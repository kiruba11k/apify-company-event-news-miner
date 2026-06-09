/**
 * Deduplicator
 *
 * Removes near-duplicate articles using:
 *  1. Exact URL match
 *  2. Normalized title similarity (Jaccard coefficient on word sets)
 *  3. LLM semantic dedup — Groq reads all headlines and groups same-event stories
 *     (optional; runs only when a Groq client is provided)
 *
 * Threshold: articles with title similarity > 0.65 are considered duplicates.
 * Keeps the version with the richest description.
 */
import { log } from 'apify';

export class Deduplicator {
    constructor(threshold = 0.65) {
        this.threshold = threshold;
    }

    deduplicate(articles) {
        // Step 1: Exact URL dedupe
        const byUrl = new Map();
        for (const a of articles) {
            const key = this._normalizeUrl(a.url);
            if (!byUrl.has(key) || this._richer(a, byUrl.get(key))) {
                byUrl.set(key, a);
            }
        }

        // Step 2: Fuzzy title dedupe
        const unique = [];
        for (const candidate of byUrl.values()) {
            const isDup = unique.some(existing =>
                this._titleSimilarity(candidate.title, existing.title) > this.threshold
            );
            if (!isDup) unique.push(candidate);
        }

        return unique;
    }

    /**
     * LLM-based semantic deduplication.
     * Sends all headlines to Groq in one call and asks it to identify groups
     * that cover the same news event. Keeps the richest article per group.
     *
     * @param {object[]} articles
     * @param {import('groq-sdk').default} groqClient
     * @param {string} model
     * @returns {Promise<object[]>}
     */
    async deduplicateWithLLM(articles, groqClient, model = 'llama3-70b-8192') {
        if (articles.length <= 1) return articles;

        const headlineList = articles
            .map((a, i) => `${i + 1}. ${a.title}`)
            .join('\n');

        const systemPrompt = `You are a news deduplication assistant. Given a numbered list of article headlines, identify groups of headlines that report on the SAME underlying news event (same event, possibly from different sources or with slightly different wording).

Rules:
- Only group headlines that are clearly about the SAME specific event (same company, same action, same time period).
- Do NOT group headlines that are related but about different events.
- If a headline is unique, do not include it in any group.
- Respond ONLY with valid JSON — no markdown, no preamble.

Output schema:
{
  "duplicate_groups": [
    [1, 3, 7],
    [2, 5]
  ]
}

Where each inner array contains the 1-based indices of headlines that are duplicates of each other. Return an empty array if no duplicates found.`;

        try {
            const response = await groqClient.chat.completions.create({
                model,
                temperature: 0,
                max_tokens: 512,
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: `HEADLINES:\n${headlineList}\n\nIdentify duplicate groups now.` },
                ],
            });

            const raw = response.choices?.[0]?.message?.content || '';
            const parsed = this._parseJSON(raw);
            const groups = parsed?.duplicate_groups || [];

            if (!groups.length) return articles;

            // Build set of indices to remove (keep first/richest in each group)
            const toRemove = new Set();
            for (const group of groups) {
                if (!Array.isArray(group) || group.length < 2) continue;
                // Find richest article in the group (by description length)
                const groupArticles = group.map(idx => ({ idx, article: articles[idx - 1] })).filter(x => x.article);
                const richest = groupArticles.reduce((best, cur) =>
                    (cur.article.description || '').length > (best.article.description || '').length ? cur : best
                );
                for (const { idx } of groupArticles) {
                    if (idx !== richest.idx) toRemove.add(idx - 1);
                }
            }

            const result = articles.filter((_, i) => !toRemove.has(i));
            log.info(`  🤖 LLM dedup removed ${toRemove.size} semantic duplicates (${articles.length} → ${result.length})`);
            return result;

        } catch (err) {
            log.warning(`[Deduplicator] LLM dedup failed, skipping: ${err.message}`);
            return articles;
        }
    }

    _normalizeUrl(url = '') {
        return url
            .replace(/^https?:\/\/(www\.)?/, '')
            .replace(/\/$/, '')
            .toLowerCase();
    }

    _richer(a, b) {
        return (a.description || '').length > (b.description || '').length;
    }

    _titleSimilarity(t1 = '', t2 = '') {
        const words1 = new Set(this._tokenize(t1));
        const words2 = new Set(this._tokenize(t2));
        const intersection = new Set([...words1].filter(w => words2.has(w)));
        const union        = new Set([...words1, ...words2]);
        return union.size === 0 ? 0 : intersection.size / union.size;
    }

    _tokenize(text) {
        return text
            .toLowerCase()
            .replace(/[^a-z0-9\s]/g, ' ')
            .split(/\s+/)
            .filter(w => w.length > 2);
    }

    _parseJSON(raw) {
        try {
            const clean = raw
                .replace(/^```(?:json)?\s*/i, '')
                .replace(/\s*```$/, '')
                .trim();
            return JSON.parse(clean);
        } catch {
            return null;
        }
    }
}
