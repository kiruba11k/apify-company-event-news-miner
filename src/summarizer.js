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

const MIN_TEXT_LENGTH = 80;
const MAX_INPUT_CHARS = 3000;

const SYSTEM_PROMPT = `You are a factual business-intelligence summariser.

STRICT RULES — violating any rule makes your output invalid:
1. Use ONLY information explicitly stated in the ARTICLE TEXT. Do NOT add background knowledge, opinions, or predictions.
2. Do NOT invent names, figures, dates, percentages, or any detail absent from the article.
3. Your summary MUST be exactly 1–2 sentences.
4. Respond ONLY with a valid JSON object — no markdown fences, no preamble, no extra text.
5. If the article text is too short or vague to summarise confidently, set "summary" to null and "insufficient_data" to true.

MANDATORY SUMMARY FORMAT — every summary must follow this exact structure:
  "In [Month YYYY], [Company] [did what] [deal size / key detail if present] [to achieve what / resulting in what]."

GOOD EXAMPLES (copy this style exactly):
  "In March 2025, Arthur J. Gallagher & Co. announced a $1.2 billion acquisition of Woodruff Sawyer to strengthen its management liability, cyber, construction, and real estate insurance capabilities."
  "In December 2025, Arthur J. Gallagher & Co. acquired First Actuarial to expand its pensions, employee benefits, and actuarial consulting business in the UK."
  "In January 2026, Soho House completed its merger with MCR Hotels in a $2.7 billion take-private deal, delisting from the NYSE and becoming a private company."

RULES FOR THE DATE:
- Use the month and year from ARTICLE_DATE if available.
- If ARTICLE_DATE is missing, extract the date from the article text itself.
- Never omit the date — it is required.

Output schema:
{
  "summary": "<1-2 sentence summary in the mandatory format above | null>",
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
    constructor({ apiKey, model = 'llama-3.3-70b-versatile', verify = false, companyName = '' } = {}) {
        this.client      = new Groq({ apiKey: apiKey || process.env.GROQ_API_KEY });
        this.model       = model;
        this.verify      = verify;
        this.companyName = companyName;
    }

    get groqClient() { return this.client; }

    async filterByEntityRelevance(articles, companyName, companyDesc = '') {
        if (!articles.length) return articles;

        const context = companyDesc
            ? `"${companyName}" (${companyDesc})`
            : `"${companyName}"`;

        const BATCH = 10;
        const kept = [];

        for (let i = 0; i < articles.length; i += BATCH) {
            const batch = articles.slice(i, i + BATCH);
            const numbered = batch.map((a, idx) =>
                `${idx + 1}. ${stripHtml(a.title || '')} — ${stripHtml((a.description || '').slice(0, 200))}`
            ).join('\n');

            const prompt = `You are a relevance filter. The target company is ${context}.

For each article below, reply true if the article is specifically about the target company ${context}, or false if it is about a different entity that happens to share the same name (e.g. a hotel chain, a person, a film, a different company).

Articles:
${numbered}

Respond ONLY with a JSON array of true/false values matching the order of the articles. Example for 3 articles: [true, false, true]`;

            try {
                const response = await this.client.chat.completions.create({
                    model:       this.model,
                    temperature: 0,
                    max_tokens:  64,
                    messages: [{ role: 'user', content: prompt }],
                });
                const raw   = (response.choices?.[0]?.message?.content || '').trim();
                const flags = this._parseJSON(raw);

                if (Array.isArray(flags) && flags.length === batch.length) {
                    batch.forEach((a, idx) => { if (flags[idx]) kept.push(a); });
                } else {
                    kept.push(...batch);
                }
            } catch (err) {
                log.warning(`[GroqSummarizer] Entity filter error: ${err.message} — keeping batch as-is`);
                kept.push(...batch);
            }
        }

        return kept;
    }

    async summarise(article) {
        const rawText = this._buildArticleText(article);

        if (rawText.length < MIN_TEXT_LENGTH) {
            return this._fallback(article);
        }

        const truncated     = rawText.slice(0, MAX_INPUT_CHARS);
        const articleDate   = article.publishedAt || article.date || '';
        const formattedDate = articleDate ? this._formatMonthYear(articleDate) : '';

        try {
            const parsed = await this._callLLM(truncated, formattedDate, this.companyName);

            if (!parsed || parsed.insufficient_data || !parsed.summary) {
                log.debug(`[GroqSummarizer] Insufficient data for: ${article.title}`);
                return this._fallback(article);
            }

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

    _buildArticleText(article) {
        const parts = [
            article.title       ? `Title: ${stripHtml(article.title)}`      : '',
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
            return 'PASS';
        }
    }

    _parseJSON(raw) {
        try {
            const clean = raw
                .replace(/^```(?:json)?\s*/i, '')
                .replace(/\s*```$/,          '')
                .trim();
            return JSON.parse(clean);
        } catch {
            return null;
        }
    }

    _fallback(article) {
        const text = stripHtml(article.description || article.title || '');
        return text.length > 220 ? text.slice(0, 217) + '…' : text;
    }

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
