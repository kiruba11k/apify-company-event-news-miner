/**
 * GroqSummarizer
 *
 * Generates grounded, factual summaries using the Groq API (llama3-70b-8192).
 *
 * Anti-hallucination strategy:
 *  1. STRICT GROUNDING  — model is told to use ONLY information present in the
 *     provided article text. Any claim not in the source text is forbidden.
 *  2. CONFIDENCE GATE   — if the article text is too short / ambiguous the
 *     summarizer returns a fallback instead of guessing.
 *  3. TEMPERATURE = 0   — deterministic output, no creative drift.
 *  4. STRUCTURED OUTPUT — model must return a JSON object; free-form prose is
 *     rejected so the caller can detect malformed responses.
 *  5. SELF-VERIFICATION — a second lightweight Groq call checks the summary
 *     against the source and flags any unsupported claims (optional, enabled
 *     via `verify: true` in options).
 *  6. FALLBACK CHAIN    — any error → rule-based excerpt → empty string.
 *     The pipeline never blocks; summaries are always "best-effort".
 */

import Groq from 'groq-sdk';
import { log } from 'apify';
import * as cheerio from 'cheerio';

function stripHtml(text) {
    if (!text || !text.includes('<')) return text || '';
    return cheerio.load(text).text().trim();
}

// Minimum article text length before we even try LLM summarisation
const MIN_TEXT_LENGTH = 80;

// Maximum characters of article text sent to the model (cost + latency guard)
const MAX_INPUT_CHARS = 3000;

const SYSTEM_PROMPT = `You are a factual business-intelligence summariser.

STRICT RULES — follow every rule or your output is invalid:
1. Use ONLY information explicitly stated in the ARTICLE TEXT provided by the user.
2. Do NOT invent names, figures, dates, percentages, or any detail absent from the article.
3. If the article text is insufficient to produce a confident summary, set "summary" to null and set "insufficient_data" to true.
4. Your summary must be 1–2 sentences maximum.
5. Do NOT add opinions, predictions, or background knowledge.
6. Respond ONLY with a valid JSON object — no markdown fences, no preamble.
7. CRITICAL FORMAT: The summary MUST begin with the time reference in the format "In [Month Year]," followed by the company name and the event. Example: "In March 2025, Acme Corp acquired XYZ Ltd for $500 million to expand its cloud services portfolio." If the exact month/year is not in the article, use the article date provided in ARTICLE_DATE.

Output schema:
{
  "summary": "<1-2 sentence factual summary starting with 'In [Month Year], [Company]...' | null>",
  "key_facts": ["<fact 1 from article>", "<fact 2 from article>"],
  "insufficient_data": <true | false>
}`;

const VERIFIER_SYSTEM_PROMPT = `You are a fact-checker. Given an ARTICLE TEXT and a SUMMARY, identify any claim in the summary that is NOT explicitly supported by the article text.

Respond ONLY with valid JSON — no markdown fences, no preamble.

Output schema:
{
  "unsupported_claims": ["<claim not in article>"],
  "verdict": "PASS" | "FAIL"
}`;

export class GroqSummarizer {
    /**
     * @param {object} options
     * @param {string}  options.apiKey      — Groq API key (or set GROQ_API_KEY env var)
     * @param {string}  [options.model]     — Groq model ID (default: llama3-70b-8192)
     * @param {boolean} [options.verify]    — run self-verification pass (default: false)
     * @param {string}  [options.companyName] — company name to anchor summaries
     */
    constructor({ apiKey, model = 'llama3-70b-8192', verify = false, companyName = '' } = {}) {
        this.client      = new Groq({ apiKey: apiKey || process.env.GROQ_API_KEY });
        this.model       = model;
        this.verify      = verify;
        this.companyName = companyName;
    }

    /** Expose the underlying Groq client (used by Deduplicator LLM pass) */
    get groqClient() { return this.client; }

    /**
     * Summarise a single article.
     *
     * @param {object} article  — { title, description, url, source, ... }
     * @returns {Promise<string>} — grounded summary or safe fallback
     */
    async summarise(article) {
        const rawText = this._buildArticleText(article);

        // Confidence gate: too little text → return trimmed excerpt directly
        if (rawText.length < MIN_TEXT_LENGTH) {
            return this._fallback(article);
        }

        const truncated = rawText.slice(0, MAX_INPUT_CHARS);

        const articleDate = article.publishedAt || article.date || '';
        const formattedDate = articleDate ? this._formatMonthYear(articleDate) : '';

        try {
            const parsed = await this._callLLM(truncated, formattedDate, this.companyName);

            if (!parsed || parsed.insufficient_data || !parsed.summary) {
                log.debug(`[GroqSummarizer] Insufficient data for: ${article.title}`);
                return this._fallback(article);
            }

            // Optional self-verification pass
            if (this.verify) {
                const verdict = await this._verify(truncated, parsed.summary);
                if (verdict === 'FAIL') {
                    log.warning(`[GroqSummarizer] Hallucination detected — using fallback for: ${article.title}`);
                    return this._fallback(article);
                }
            }

            return parsed.summary.trim();

        } catch (err) {
            log.warning(`[GroqSummarizer] LLM error for "${article.title}": ${err.message}`);
            return this._fallback(article);
        }
    }

    /**
     * Batch-summarise an array of articles with concurrency control.
     *
     * @param {object[]} articles
     * @param {number}   [concurrency=5]
     * @returns {Promise<string[]>}
     */
    async summariseBatch(articles, concurrency = 5) {
        const results = new Array(articles.length).fill('');
        const queue   = articles.map((a, i) => ({ article: a, index: i }));

        const worker = async () => {
            while (queue.length > 0) {
                const { article, index } = queue.shift();
                results[index] = await this.summarise(article);
            }
        };

        await Promise.all(Array.from({ length: concurrency }, worker));
        return results;
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    _buildArticleText(article) {
        let title = stripHtml(article.title || '');
        // Strip " - Source Name" suffix (Google News RSS format) before sending to LLM
        title = title.replace(/\s[-–—]\s[^-–—]{2,50}$/, '').trim();
        const parts = [
            title               ? `Title: ${title}`                          : '',
            article.description ? `Body: ${stripHtml(article.description)}` : '',
        ];
        return parts.filter(Boolean).join('\n').trim();
    }

    async _callLLM(articleText, articleDate = '', companyName = '') {
        const contextLines = [];
        if (companyName) contextLines.push(`COMPANY: ${companyName}`);
        if (articleDate) contextLines.push(`ARTICLE_DATE: ${articleDate}`);
        const contextBlock = contextLines.length ? contextLines.join('\n') + '\n\n' : '';

        const response = await this.client.chat.completions.create({
            model:       this.model,
            temperature: 0,
            max_tokens:  256,
            messages: [
                { role: 'system', content: SYSTEM_PROMPT },
                {
                    role: 'user',
                    content: `${contextBlock}ARTICLE TEXT:\n${articleText}\n\nProduce the JSON summary now.`,
                },
            ],
        });

        const raw = response.choices?.[0]?.message?.content || '';
        return this._parseJSON(raw);
    }

    async _verify(articleText, summary) {
        try {
            const response = await this.client.chat.completions.create({
                model:       this.model,
                temperature: 0,
                max_tokens:  128,
                messages: [
                    { role: 'system', content: VERIFIER_SYSTEM_PROMPT },
                    {
                        role: 'user',
                        content: `ARTICLE TEXT:\n${articleText}\n\nSUMMARY:\n${summary}\n\nVerify now.`,
                    },
                ],
            });
            const raw    = response.choices?.[0]?.message?.content || '';
            const parsed = this._parseJSON(raw);
            return parsed?.verdict || 'PASS';
        } catch {
            return 'PASS'; // Verifier failure → don't block pipeline
        }
    }

    _parseJSON(raw) {
        try {
            // Strip accidental markdown fences if model misbehaves
            const clean = raw
                .replace(/^```(?:json)?\s*/i, '')
                .replace(/\s*```$/,          '')
                .trim();
            return JSON.parse(clean);
        } catch {
            return null;
        }
    }

    /**
     * Rule-based fallback — formats as "In [Month Year], [Company] [event]."
     * Works without a Groq API key.
     */
    _fallback(article) {
        const rawDate = article.publishedAt || article.date || '';
        const monthYear = rawDate ? this._formatMonthYear(rawDate) : '';
        const company = this.companyName || '';
        let title = stripHtml(article.title || '');

        // Strip " - Source Name" suffix appended by Google News RSS
        // e.g. "Leonardo acquires Becrypt - UK Defence Journal" → "Leonardo acquires Becrypt"
        title = title.replace(/\s[-–—]\s[^-–—]{2,50}$/, '').trim();

        if (monthYear && company) {
            // Strip leading "Company - " or "Company: " prefix if present (full name match)
            const escaped = company.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            let event = title.replace(new RegExp(`^${escaped}[\\s\\-–—:,]+`, 'i'), '').trim();

            // If full name didn't match, try stripping just the first word of the company name
            if (event === title) {
                const firstWord = company.split(/\s+/)[0] || '';
                if (firstWord.length > 2) {
                    const escapedFirst = firstWord.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                    event = title.replace(new RegExp(`^${escapedFirst}[\\s\\-–—:,]+`, 'i'), '').trim();
                }
            }

            // If the event clause still starts with a company-name token, the title is
            // already self-contained — emit without prepending the company name again.
            const companyFirstToken = (company.split(/\s+/)[0] || '').toLowerCase();
            if (event.toLowerCase().startsWith(companyFirstToken) && companyFirstToken.length > 2) {
                // Capitalize first char (it was stripped/lowercased) and return as-is
                const cap = event.charAt(0).toUpperCase() + event.slice(1);
                return `In ${monthYear}, ${cap}`.replace(/\s{2,}/g, ' ');
            }

            // Lowercase the very first character of the event clause
            if (event.length > 0) event = event.charAt(0).toLowerCase() + event.slice(1);
            return `In ${monthYear}, ${company} ${event}`.replace(/\s{2,}/g, ' ');
        }
        if (monthYear) return `In ${monthYear}, ${title}`;
        return title.length > 220 ? title.slice(0, 217) + '…' : title;
    }

    /** Format a date string as "Month YYYY" for the LLM context */
    _formatMonthYear(dateStr) {
        try {
            const d = new Date(dateStr);
            if (isNaN(d)) return dateStr;
            return d.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
        } catch {
            return dateStr;
        }
    }
}
