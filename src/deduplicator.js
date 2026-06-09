/**
 * Deduplicator
 *
 * Three-stage pipeline:
 *  1. deduplicate()                  — exact URL + Jaccard title similarity (no API)
 *  2. deduplicateClassifiedWithLLM() — Groq reads headline + context per event_type
 *                                      group; only merges on "certain" confidence
 *  3. deduplicateClassified()        — fallback when no Groq key; shared-specific-
 *                                      tokens heuristic (no API)
 */
import { log } from 'apify';

export class Deduplicator {
    constructor(threshold = 0.65) {
        this.threshold = threshold;
    }

    // ── Stage 1: URL + Jaccard dedup (pre-classification) ───────────────────

    deduplicate(articles) {
        // Exact URL dedupe
        const byUrl = new Map();
        for (const a of articles) {
            const key = this._normalizeUrl(a.url);
            if (!byUrl.has(key) || this._richer(a, byUrl.get(key))) {
                byUrl.set(key, a);
            }
        }

        // Fuzzy title dedupe
        const unique = [];
        for (const candidate of byUrl.values()) {
            const isDup = unique.some(existing =>
                this._titleSimilarity(candidate.title, existing.title) > this.threshold
            );
            if (!isDup) unique.push(candidate);
        }

        return unique;
    }

    // ── Stage 2: Groq post-classification dedup ──────────────────────────────

    /**
     * Deduplicates already-classified events using Groq.
     * Processes each event_type group separately (small, focused batches).
     * Only merges when Groq returns confidence = "certain" — conservative by design.
     *
     * @param {Array<{article, classification, impact}>} classified
     * @param {object} groqClient  — Groq SDK client instance
     * @param {string} companyName — used in the system prompt for context
     * @param {string} model
     * @returns {Promise<Array>}
     */
    async deduplicateClassifiedWithLLM(
        classified,
        groqClient,
        companyName = '',
        model = 'llama3-70b-8192'
    ) {
        // Group by event_type — each group is a focused, small batch
        const byType = new Map();
        for (const item of classified) {
            const t = item.classification.event_type;
            if (!byType.has(t)) byType.set(t, []);
            byType.get(t).push(item);
        }

        const kept = [];
        for (const [eventType, group] of byType.entries()) {
            if (group.length <= 1) {
                kept.push(...group);
                continue;
            }
            const dedupedGroup = await this._dedupGroup(
                group, groqClient, companyName, eventType, model
            );
            kept.push(...dedupedGroup);
        }

        return kept;
    }

    async _dedupGroup(group, groqClient, companyName, eventType, model) {
        // Build numbered list: headline + up to 180 chars of description
        const articleList = group.map((item, i) => {
            const title = (item.article.title || '').trim();
            // Strip " - Source Name" suffix common in RSS feeds
            const cleanTitle = title.replace(/\s[-–—]\s[^-–—]{2,50}$/, '');
            const desc = (item.article.description || '').slice(0, 180).trim();
            const date = item.article.publishedAt
                ? new Date(item.article.publishedAt).toDateString()
                : 'unknown date';
            return `${i + 1}.\n   HEADLINE: ${cleanTitle}\n   CONTEXT: ${desc || '(no description)'}\n   DATE: ${date}`;
        }).join('\n\n');

        const systemPrompt = `You are a strict news deduplication assistant.

Company: "${companyName}"
Event category: "${eventType}"

TASK: Identify which articles describe the EXACT SAME specific event — meaning the same company action, the same target, and the same time frame.

RULES (read carefully to avoid false positives):
1. Only group articles if you are CERTAIN they cover the same single event.
2. Two different acquisitions by the same company are NOT duplicates.
3. A contract award and an acquisition are NOT duplicates even if they share keywords.
4. Articles from different months are almost certainly different events.
5. If there is ANY doubt, keep articles separate. Missing a duplicate is better than wrongly merging two different events.
6. Respond ONLY with valid JSON — no markdown, no explanation outside the JSON.

Output schema:
{
  "groups": [
    {
      "indices": [1, 3, 4],
      "confidence": "certain",
      "reason": "all three report Leonardo acquiring Becrypt in March 2026"
    }
  ]
}

Only include groups with confidence "certain". Return { "groups": [] } if no certain duplicates exist.`;

        try {
            const response = await groqClient.chat.completions.create({
                model,
                temperature: 0,
                max_tokens: 600,
                messages: [
                    { role: 'system', content: systemPrompt },
                    {
                        role: 'user',
                        content: `ARTICLES:\n\n${articleList}\n\nIdentify duplicate groups now.`,
                    },
                ],
            });

            const raw = response.choices?.[0]?.message?.content || '';
            const parsed = this._parseJSON(raw);
            const groups = (parsed?.groups || []).filter(
                g => g.confidence === 'certain'
                    && Array.isArray(g.indices)
                    && g.indices.length >= 2
            );

            if (!groups.length) return group;

            const toRemove = new Set();
            for (const g of groups) {
                const items = g.indices
                    .map(idx => ({ idx, item: group[idx - 1] }))
                    .filter(x => x.item);
                if (items.length < 2) continue;

                // Keep highest-scored; tie-break by richer description
                const best = items.reduce((a, b) => {
                    if (b.item.impact.event_impact_score !== a.item.impact.event_impact_score) {
                        return b.item.impact.event_impact_score > a.item.impact.event_impact_score ? b : a;
                    }
                    return (b.item.article.description || '').length >
                           (a.item.article.description || '').length ? b : a;
                });

                for (const { idx } of items) {
                    if (idx !== best.idx) toRemove.add(idx - 1);
                }

                log.info(`  🤖 LLM merged [${g.indices.join(', ')}] → kept #${best.idx} (${eventType}): ${g.reason}`);
            }

            return group.filter((_, i) => !toRemove.has(i));

        } catch (err) {
            log.warning(`[Deduplicator] LLM group dedup failed for "${eventType}": ${err.message} — falling back to heuristic`);
            return this._heuristicDedup(group, companyName);
        }
    }

    // ── Stage 3: Heuristic fallback (no API) ────────────────────────────────

    /**
     * Fallback dedup when no Groq key is available.
     * Merges articles within the same event_type that share ≥2 specific tokens
     * (tokens not in the company name).
     *
     * @param {Array} classified
     * @param {string} companyName
     */
    deduplicateClassified(classified, companyName = '') {
        const byType = new Map();
        for (const item of classified) {
            const t = item.classification.event_type;
            if (!byType.has(t)) byType.set(t, []);
            byType.get(t).push(item);
        }

        const kept = [];
        for (const group of byType.values()) {
            kept.push(...this._heuristicDedup(group, companyName));
        }
        return kept;
    }

    _heuristicDedup(group, companyName) {
        const companyTokens = this._tokenSet(companyName);

        const clusters = [];
        for (const item of group) {
            const words = this._tokenSet(item.article.title);
            const match = clusters.find(c => {
                const shared = [...words].filter(
                    w => this._tokenSet(c.best.article.title).has(w) && !companyTokens.has(w)
                ).length;
                return shared >= 2;
            });
            if (match) {
                if (item.impact.event_impact_score > match.best.impact.event_impact_score) {
                    match.best = item;
                }
            } else {
                clusters.push({ best: item });
            }
        }
        return clusters.map(c => c.best);
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    _tokenSet(text) {
        return new Set(
            (text || '').toLowerCase()
                .replace(/[^a-z0-9\s]/g, ' ')
                .split(/\s+/)
                .filter(w => w.length > 2)
        );
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
        const words1 = this._tokenSet(t1);
        const words2 = this._tokenSet(t2);
        const intersection = new Set([...words1].filter(w => words2.has(w)));
        const union        = new Set([...words1, ...words2]);
        return union.size === 0 ? 0 : intersection.size / union.size;
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
