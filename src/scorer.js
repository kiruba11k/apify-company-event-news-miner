/**
 * ImpactScorer
 *
 * Produces:
 *  - event_impact_score  (1–10 integer)
 *  - intent_signal       (Low | Medium | High)
 *
 * Scoring model (additive, capped at 10):
 *
 *  Base score by event type:
 *   funding              → 7 (high-intent signal)
 *   mergers_acquisitions → 7 (major corporate event)
 *   expansion            → 6
 *   leadership_change    → 6 (strong buying/relationship trigger)
 *   product_launch       → 5
 *   partnership          → 5
 *   compliance           → 4 (depends on magnitude)
 *   layoffs              → 5 (restructuring signal)
 *
 *  Modifiers (+/-):
 *   + Company name in article title                            → +2
 *   + Credible source (Reuters, Bloomberg, FT, SEC, etc.)     → +1
 *   + Dollar amount mentioned (millions/billions)             → +1
 *   + Recent (<48 hrs)                                        → +1
 *   + High-confidence classification (≥2 strong keywords)    → +1
 *   + 3+ strong keyword matches                               → +1
 *   - Source is only PR wire (not corroborated)               → -1
 *   - Low confidence classification                           → -1
 *   - Company only in description, not title                  → -1
 */

const EVENT_BASE_SCORES = {
    funding:             7,
    mergers_acquisitions: 7,
    expansion:           6,
    leadership_change:   6,
    product_launch:      5,
    partnership:         5,
    layoffs:             5,
    compliance:          4,
};

const CREDIBLE_SOURCES = [
    'reuters', 'bloomberg', 'ft.com', 'financial times', 'wsj', 'wall street journal',
    'sec edgar', 'techcrunch', 'the verge', 'wired', 'forbes', 'fortune',
    'associated press', 'ap news', 'bbc', 'cnbc', 'nytimes', 'new york times',
    'business insider', 'venturebeat', 'crunchbase', 'hacker news',
    'yahoo finance', 'marketwatch', 'barrons', 'seeking alpha',
];

const PR_WIRES = ['pr newswire', 'businesswire', 'globenewswire', 'accesswire'];

const MONEY_RE = /\$[\d.,]+\s*(million|billion|m\b|b\b)|[\d.,]+\s*(million|billion)\s*(dollar|usd|€|£)/i;

export class ImpactScorer {
    score(article, classification) {
        const base = EVENT_BASE_SCORES[classification.event_type] || 4;
        let modifier = 0;

        const src  = (article.source || '').toLowerCase();
        const text = `${article.title} ${article.description || ''}`;
                // Relevance: company name in title is a strong signal
        const relevance = article.relevanceScore ?? 0;
        if (relevance >= 3) modifier += 2;       // company in title
        else if (relevance === 0) modifier -= 1; // company barely mentioned

        // Credible source bonus
        if (CREDIBLE_SOURCES.some(s => src.includes(s))) modifier += 1;

// PR wire penalty (self-reported, no independent corroboration)
        if (PR_WIRES.some(s => src.includes(s))) modifier -= 1;

        // Dollar amount bonus 
        if (MONEY_RE.test(text)) modifier += 1;

        // Recency bonus
        if (this._isRecent(article.publishedAt, 48)) modifier += 1;

        // Classification strength
        if (classification.confidence === 'High') modifier += 1;
        if (classification.confidence === 'Low')  modifier -= 1;
        if ((classification.keywords_matched || []).length >= 3) modifier += 1;

        const score = Math.min(10, Math.max(1, base + modifier));

        return {
            event_impact_score: score,
            intent_signal: score >= 7 ? 'High' : score >= 4 ? 'Medium' : 'Low',
        };
    }

    _isRecent(dateStr, hours) {
        if (!dateStr) return false;
        return (Date.now() - new Date(dateStr).getTime()) < hours * 3_600_000;

    }
}
