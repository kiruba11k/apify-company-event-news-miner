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
4. Your summary must be 2–3 sentences maximum.
5. Do NOT add opinions, predictions, or background knowledge.
6. Respond ONLY with a valid JSON object — no markdown fences, no preamble.

Output schema:
{
  "summary": "<2-3 sentence factual summary | null>",
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
     * @param {string}  options.apiKey   — Groq API key (or set GROQ_API_KEY env var)
     * @param {string}  [options.model]  — Groq model ID (default: llama3-70b-8192)
     * @param {boolean} [options.verify] — run self-verification pass (default: false)
     */
    constructor({ apiKey, model = 'llama3-70b-8192', verify = false } = {}) {
        this.client  = new Groq({ apiKey: apiKey || process.env.GROQ_API_KEY });
        this.model   = model;
        this.verify  = verify;
    }

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

        try {
            const parsed = await this._callLLM(truncated);

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
        const parts = [
            article.title       ? `Title: ${stripHtml(article.title)}`       : '',
            article.description ? `Body: ${stripHtml(article.description)}`  : '',
        ];
        return parts.filter(Boolean).join('\n').trim();
    }

    async _callLLM(articleText) {
        const response = await this.client.chat.completions.create({
            model:       this.model,
            temperature: 0,           // deterministic — no creative drift
            max_tokens:  256,
            messages: [
                { role: 'system', content: SYSTEM_PROMPT },
                {
                    role: 'user',
                    content: `ARTICLE TEXT:\n${articleText}\n\nProduce the JSON summary now.`,
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

    /** Rule-based fallback: first ~220 chars of plain-text description, no LLM */
    _fallback(article) {
        const text = stripHtml(article.description || article.title || '');
        return text.length > 220 ? text.slice(0, 217) + '…' : text;
    }
}
