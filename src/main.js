/**
 * main.js — Company News & Events Miner
 *
 * Supports:
 *  • Single company (company_name input)
 *  • Bulk mode    (companies_csv input → up to 20 companies from a CSV file)
 *
 * Pipeline per company:
 *  1. Collect   — gather articles from all configured sources
 *  2. Deduplicate
 *  3. Classify  — rule-based keyword classifier (expansion / mergers_acquisitions / …)
 *  4. Score     — impact scoring
 *  5. Summarise — Groq LLM (grounded, anti-hallucination)
 *  6. Output    — push to Apify dataset + KV summary stats
 */

import { Actor, log } from 'apify';
import { parse as parseCsv } from 'csv-parse/sync';
import fs from 'fs';
import path from 'path';

import { NewsCollector  } from './collector.js';
import { EventClassifier } from './classifier.js';
import { Deduplicator   } from './deduplicator.js';
import { ImpactScorer   } from './scorer.js';
import { GroqSummarizer } from './summarizer.js';

await Actor.init();
const input = await Actor.getInput();

// ── Input parsing ────────────────────────────────────────────────────────────

const {
    company_name,
    companies_csv,           // Apify key-value store resource key for a CSV file
    time_window       = '7d',
    intent_categories = [
        'expansion', 'mergers_acquisitions', 'product_launch',
        'funding', 'partnership', 'compliance',
    ],
    max_results       = 50,
    min_impact_score  = 3,
    language          = 'en',
    groq_api_key,            // optional; falls back to GROQ_API_KEY env var
    groq_verify       = false, // set true to enable self-verification pass
} = input;

// ── Resolve company list ─────────────────────────────────────────────────────

async function resolveCompanies() {
    // Bulk CSV mode
    if (companies_csv) {
        let csvContent;

        // companies_csv can be:
        //   (a) a plain string of CSV text,
        //   (b) a URL to a file in the default KV store,
        //   (c) a local file path (for testing)
        if (companies_csv.startsWith('http')) {
            const { default: axios } = await import('axios');
            const resp = await axios.get(companies_csv, { timeout: 15000 });
            csvContent = resp.data;
        } else if (fs.existsSync(companies_csv)) {
            csvContent = fs.readFileSync(companies_csv, 'utf8');
        } else {
            // Treat as inline CSV text
            csvContent = companies_csv;
        }

        const records = parseCsv(csvContent, {
            skip_empty_lines:   true,
            trim:               true,
            relax_column_count: true,
        });

        // Accept first column as company name, skip header rows that look like headers
        const names = records
            .map(row => {
                // row can be an array (no header) or object (with header)
                const val = Array.isArray(row) ? row[0] : Object.values(row)[0];
                return (val || '').trim();
            })
            .filter(n => n && !/^(company|name|company_name)$/i.test(n));

        if (names.length === 0) throw new Error('CSV contained no valid company names.');
        if (names.length > 20) {
            log.warning(`CSV has ${names.length} companies — processing first 20 only.`);
            return names.slice(0, 20);
        }
        return names;
    }

    // Single company mode
    if (!company_name) {
        throw new Error('Input validation failed: provide company_name OR companies_csv.');
    }
    return [company_name.trim()];
}

const companies = await resolveCompanies();
const isBulk    = companies.length > 1;

log.info(`🚀 Company News & Events Miner — ${isBulk ? `BULK mode (${companies.length} companies)` : `Single mode: "${companies[0]}"`}`);
log.info(`📅 Time window: ${time_window} | Categories: ${intent_categories.join(', ')}`);

// ── Shared services ──────────────────────────────────────────────────────────

const classifier = new EventClassifier(intent_categories);
const scorer     = new ImpactScorer();
const summarizer = new GroqSummarizer({
    apiKey: groq_api_key,
    verify: groq_verify,
});

const dataset = await Actor.openDataset();
const kvStore = await Actor.openKeyValueStore();

// ── Per-company pipeline ─────────────────────────────────────────────────────

async function processCompany(targetCompany) {
    log.info(`\n─── Processing: "${targetCompany}" ───`);

    // 1. COLLECT
    const collector   = new NewsCollector({ company_name: targetCompany, time_window, language });
    const rawArticles = await collector.collect();
    log.info(`  📡 Collected ${rawArticles.length} raw articles`);

    // 2. DEDUPLICATE
    const deduplicator  = new Deduplicator();
    const uniqueArticles = deduplicator.deduplicate(rawArticles);
    log.info(`  🗂  After dedup: ${uniqueArticles.length} unique articles`);

    // 3. CLASSIFY + SCORE (filter first so we only summarise relevant articles)
    const classified = [];
    for (const article of uniqueArticles) {
        const classification = classifier.classify(article);
        if (!classification) continue;

        const impact = scorer.score(article, classification);
        if (impact.event_impact_score < min_impact_score) continue;

        classified.push({ article, classification, impact });
    }

    log.info(`  🔍 Classified: ${classified.length} relevant events`);

    // 4. SUMMARISE — batch call with concurrency=5
    const articles   = classified.map(c => c.article);
    const summaries  = await summarizer.summariseBatch(articles, 5);

    // 5. BUILD RECORDS
    const results = classified.map(({ article, classification, impact }, i) => ({
        company_name:        targetCompany,
        event_type:          classification.event_type,
        headline:            article.title,
        summary:             summaries[i] || article.description || '',
        event_date:          article.publishedAt || article.date || null,
        source:              article.source,
        source_link:         article.url,
        intent_signal:       impact.intent_signal,
        event_impact_score:  impact.event_impact_score,
        confidence:          classification.confidence,
        keywords_matched:    classification.keywords_matched,
        scraped_at:          new Date().toISOString(),
    }));

    // Sort by impact score descending, respect per-company max_results cap
    const sorted = results
        .sort((a, b) => b.event_impact_score - a.event_impact_score)
        .slice(0, max_results);

    log.info(`  🎯 High-value events: ${sorted.length}`);

    return {
        company:          targetCompany,
        total_collected:  rawArticles.length,
        after_dedup:      uniqueArticles.length,
        high_value_events: sorted.length,
        categories_found: [...new Set(sorted.map(r => r.event_type))],
        records:          sorted,
    };
}

// ── Run all companies ────────────────────────────────────────────────────────

const allStats    = [];
let   totalEvents = 0;

for (const company of companies) {
    try {
        const result = await processCompany(company);

        if (result.records.length > 0) {
            await dataset.pushData(result.records);
        }

        allStats.push({
            company:           result.company,
            total_collected:   result.total_collected,
            after_dedup:       result.after_dedup,
            high_value_events: result.high_value_events,
            categories_found:  result.categories_found,
        });

        totalEvents += result.high_value_events;
    } catch (err) {
        log.error(`Failed to process "${company}": ${err.message}`);
        allStats.push({ company, error: err.message });
    }
}

// ── Save summary stats ───────────────────────────────────────────────────────

const summary = {
    mode:            isBulk ? 'bulk' : 'single',
    companies_count: companies.length,
    time_window,
    total_events_found: totalEvents,
    run_at:          new Date().toISOString(),
    per_company:     allStats,
};

await kvStore.setValue('SUMMARY', summary);

log.info(`\n✅ Actor completed. Total high-value events across all companies: ${totalEvents}`);
if (isBulk) {
    log.info('📊 Per-company breakdown saved to KV store key: SUMMARY');
}

await Actor.exit();
