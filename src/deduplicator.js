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
     * Sends headlines to Groq in chunks and asks it to identify groups
     * that cover the same news event by meaning/context, not just wording.
     * Keeps the richest article per group.
     */
    async deduplicateWithLLM(articles, groqClient, model = 'llama3-70b-8192') {
        if (articles.length <= 1) return articles;

        // Process in chunks of 40 to avoid exceeding token limits
        const CHUNK = 40;
        const globalToRemove = new Set();

        for (let start = 0; start < articles.length; start += CHUNK) {
            const chunk       = articles.slice(start, start + CHUNK);
            const chunkGroups = await this._llmDeduplicateChunk(chunk, groqClient, model);

            // Translate chunk-local indices back to global indices
            for (const group of chunkGroups) {
                if (!Array.isArray(group) || group.length < 2) continue;
                const globalGroup = group.map(localIdx => start + localIdx);
                const richest = globalGroup.reduce((best, gi) =>
                    (articles[gi]?.description || '').length > (articles[best]?.description || '').length ? gi : best
                , globalGroup[0]);
                for (const gi of globalGroup) {
                    if (gi !== richest) globalToRemove.add(gi);
                }
            }
        }

        const result = articles.filter((_, i) => !globalToRemove.has(i));
        log.info(`  🤖 LLM dedup removed ${globalToRemove.size} semantic duplicates (${articles.length} → ${result.length})`);
        return result;
    }

    async _llmDeduplicateChunk(chunk, groqClient, model) {
        const headlineList = chunk
            .map((a, i) => `${i + 1}. ${a.title}`)
            .join('\n');

        const systemPrompt = `You are a news deduplication assistant. Given a numbered list of article headlines, your job is to find groups of headlines that cover the SAME underlying news event — even if the wording, phrasing, or details differ significantly between sources.

KEY PRINCIPLE: Focus on the MEANING and CONTEXT, not the words.
- "Acme buys Widgets Inc" and "Acme completes acquisition of Widgets Inc" → SAME EVENT
- "Leonardo wins £1bn helicopter contract" and "UK awards Leonardo deal for New Medium Helicopter" → SAME EVENT
- "CEO of Stripe resigns" and "Stripe announces leadership change" → SAME EVENT (if same person/time)
- "Soho House merges with MCR" and "Soho House completes merger, goes private" → SAME EVENT

Rules:
- Group headlines that describe the same specific event (same company, same action, same time period).
- Use context clues — even if company names or deal names are abbreviated differently.
- Do NOT group headlines that are about genuinely different events (different acquisitions, different dates).
- If a headline has no match, do not include it in any group.
- Respond ONLY with valid JSON — no markdown, no preamble.

Output schema:
{
  "duplicate_groups": [
    [1, 3, 7],
    [2, 5]
  ]
}

Each inner array = 1-based indices of headlines covering the same event. Return empty array if no duplicates.`;

        try {
            const response = await groqClient.chat.completions.create({
                model,
                temperature: 0,
                max_tokens:  768,
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user',   content: `HEADLINES:\n${headlineList}\n\nIdentify semantic duplicate groups now.` },
                ],
            });

            const raw    = response.choices?.[0]?.message?.content || '';
            const parsed = this._parseJSON(raw);
            return parsed?.duplicate_groups || [];

        } catch (err) {
            log.warning(`[Deduplicator] LLM dedup chunk failed: ${err.message}`);
            return [];
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
