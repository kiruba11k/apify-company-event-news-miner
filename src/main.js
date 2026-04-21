import { Actor, log } from 'apify';
import { NewsCollector } from './collector.js';
import { EventClassifier } from './classifier.js';
import { Deduplicator } from './deduplicator.js';
import { ImpactScorer } from './scorer.js';

await Actor.init();

const input = await Actor.getInput();

const {
    company_name,
    time_window = '7d',
    intent_categories = ['expansion', 'product_launch', 'funding', 'partnership', 'compliance'],
    max_results = 50,
    min_impact_score = 3,
    language = 'en',
} = input;

if (!company_name) {
    throw new Error('Input validation failed: company_name is required.');
}

log.info(`🚀 Starting Company News & Events Miner for: "${company_name}"`);
log.info(`📅 Time window: ${time_window} | Categories: ${intent_categories.join(', ')}`);

const dataset = await Actor.openDataset();
const kvStore = await Actor.openKeyValueStore();

// ── 1. COLLECT ──────────────────────────────────────────────────────────────
const collector = new NewsCollector({ company_name, time_window, language });
log.info('📡 Collecting news from all sources...');
const rawArticles = await collector.collect();
log.info(`✅ Collected ${rawArticles.length} raw articles`);

// ── 2. DEDUPLICATE ──────────────────────────────────────────────────────────
const deduplicator = new Deduplicator();
const uniqueArticles = deduplicator.deduplicate(rawArticles);
log.info(`🗂  After dedup: ${uniqueArticles.length} unique articles`);

// ── 3. CLASSIFY + SCORE ─────────────────────────────────────────────────────
const classifier = new EventClassifier(intent_categories);
const scorer = new ImpactScorer();

const results = [];

for (const article of uniqueArticles) {
    const classification = classifier.classify(article);
    if (!classification) continue; // Not a business-impact event

    const impact = scorer.score(article, classification);
    if (impact.event_impact_score < min_impact_score) continue;

    const record = {
        company_name: company_name.trim(),
        event_type: classification.event_type,
        headline: article.title,
        summary: article.summary || article.description || '',
        event_date: article.publishedAt || article.date || null,
        source: article.source,
        source_link: article.url,
        intent_signal: impact.intent_signal,
        event_impact_score: impact.event_impact_score,
        confidence: classification.confidence,
        keywords_matched: classification.keywords_matched,
        scraped_at: new Date().toISOString(),
    };

    results.push(record);
}

// ── 4. SORT & LIMIT ─────────────────────────────────────────────────────────
const sorted = results
    .sort((a, b) => b.event_impact_score - a.event_impact_score)
    .slice(0, max_results);

log.info(`🎯 Final results: ${sorted.length} high-value events identified`);

// ── 5. PUSH OUTPUT ──────────────────────────────────────────────────────────
if (sorted.length > 0) {
    await dataset.pushData(sorted);
}

// Save summary stats to KV store
await kvStore.setValue('SUMMARY', {
    company_name,
    time_window,
    total_collected: rawArticles.length,
    after_dedup: uniqueArticles.length,
    high_value_events: sorted.length,
    categories_found: [...new Set(sorted.map(r => r.event_type))],
    run_at: new Date().toISOString(),
});

log.info('✅ Actor completed successfully.');
await Actor.exit();
