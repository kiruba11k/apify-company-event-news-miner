/**
 * EventClassifier
 *
 * Rule-based NLP classifier using keyword patterns per intent category.
 * Designed to be lightweight with zero external API calls (runs locally).
 * Each event_type has a ranked list of strong + supporting signals.
 *
 * Categories:
 *  - expansion          (organic growth, new offices, new markets, new regions)
 *  - mergers_acquisitions (M&A: acquisitions, mergers, takeovers, IPOs)
 *  - product_launch
 *  - funding
 *  - partnership
 *  - compliance
 */

const CATEGORY_RULES = {
    expansion: {
        strong: [
            'expand', 'expansion', 'new market', 'new office', 'new region',
            'new country', 'new location', 'open office', 'opened office',
            'entering', 'launches in', 'entering market', 'global rollout',
            'new headquarters', 'new facility', 'new plant', 'new warehouse',
            'new store', 'international expansion', 'geographic expansion',
            'opens in', 'setting up operations', 'new branch', 'new division',
            'expands to', 'expands into', 'enters', 'footprint',
        ],
        supporting: [
            'growth', 'scale', 'international', 'regional', 'hire', 'hiring surge',
            'headcount', 'workforce expansion', 'job openings', 'global presence',
        ],
    },

    mergers_acquisitions: {
        strong: [
            'acqui', 'acquires', 'acquired', 'acquisition', 'merger', 'merges',
            'merged with', 'takeover', 'buyout', 'buy out', 'purchased',
            'ipo', 'went public', 'listed on', 'initial public offering',
            'spac', 'reverse merger', 'going public', 'stock market debut',
            'hostile takeover', 'friendly takeover', 'strategic acquisition',
            'divest', 'divestiture', 'spin-off', 'spinoff', 'carve-out',
            'agreed to buy', 'agreed to acquire', 'deal to buy', 'deal to acquire',
            'combined company', 'post-merger', 'integration complete',
        ],
        supporting: [
            'deal', 'transaction', 'shareholder', 'board approval',
            'regulatory approval', 'enterprise value', 'premium',
            'all-stock', 'all-cash', 'bid', 'offer', 'valuation',
        ],
    },

    product_launch: {
        strong: [
            'launches', 'launch', 'launches new', 'new product', 'product launch',
            'unveils', 'unveil', 'introduces', 'introduces new', 'release',
            'releases', 'debuts', 'now available', 'generally available', 'ga release',
            'beta launch', 'early access', 'goes live', 'ships', 'rolling out',
            'new feature', 'new version', 'v2', 'update', 'major update',
        ],
        supporting: [
            'innovation', 'technology', 'platform', 'solution', 'service',
            'app', 'software', 'product', 'api', 'sdk',
        ],
    },

    funding: {
        strong: [
            'raises', 'funding', 'series a', 'series b', 'series c', 'series d',
            'seed round', 'investment', 'venture capital', 'vc backed', 'backed by',
            'million', 'billion', 'valuation', 'capital raise', 'round closed',
            'fundraising', 'grant awarded', 'grant received', 'financial backing',
            'stock offering', 'debt financing', 'pre-seed', 'series e', 'series f',
            'growth equity', 'private equity', 'crowdfunding', 'convertible note',
        ],
        supporting: [
            'investor', 'investors', 'equity', 'startup', 'growth capital',
            'fundraise', 'revenue', 'revenue milestone', 'profitable',
        ],
    },

    partnership: {
        strong: [
            'partnership', 'partners with', 'partnered with', 'joint venture',
            'collaboration', 'collaborates', 'strategic alliance', 'alliance',
            'agreement', 'deal', 'deal signed', 'mou', 'memorandum',
            'contract awarded', 'contract signed', 'supplier agreement',
            'distribution agreement', 'licensing deal', 'reseller agreement',
        ],
        supporting: [
            'integrates', 'integration', 'ecosystem', 'together', 'combined',
            'co-develop', 'co-create', 'official partner',
        ],
    },

    compliance: {
        strong: [
            'compliance', 'regulation', 'regulatory', 'fined', 'fine',
            'penalty', 'lawsuit', 'litigation', 'legal action', 'settlement',
            'approved by', 'fda approval', 'sec filing', 'audit', 'violation',
            'gdpr', 'ccpa', 'iso certified', 'soc 2', 'certification',
            'sanctioned', 'investigation', 'probe', 'subpoena',
        ],
        supporting: [
            'legal', 'government', 'authority', 'court', 'ruling',
            'mandate', 'enforcement', 'policy change', 'standard',
        ],
    },
};

// PR fluff patterns to REJECT
const FLUFF_PATTERNS = [
    /congratulat/i, /award.*winner/i, /best.*place.*to.*work/i,
    /culture.*award/i, /listed.*as.*top/i, /happy.*to.*announce.*team/i,
    /proud.*to.*welcome/i, /birthday/i, /anniversary.*celebrat/i,
    /thought.*leader/i, /keynote.*speaker/i,
];

export class EventClassifier {
    constructor(enabledCategories = Object.keys(CATEGORY_RULES)) {
        this.rules = {};
        for (const cat of enabledCategories) {
            if (CATEGORY_RULES[cat]) this.rules[cat] = CATEGORY_RULES[cat];
        }
    }

    classify(article) {
        const text = `${article.title} ${article.description || ''}`.toLowerCase();

        // Reject PR fluff first
        if (FLUFF_PATTERNS.some(p => p.test(text))) return null;

        let bestMatch = null;
        let bestScore = 0;

        for (const [event_type, { strong, supporting }] of Object.entries(this.rules)) {
            const strongMatches = strong.filter(kw => text.includes(kw));
            const supportingMatches = supporting.filter(kw => text.includes(kw));

            if (strongMatches.length === 0) continue; // Must have at least 1 strong signal

            const score = strongMatches.length * 3 + supportingMatches.length;

            if (score > bestScore) {
                bestScore = score;
                bestMatch = {
                    event_type,
                    confidence: this._confidence(strongMatches.length, supportingMatches.length),
                    keywords_matched: [...strongMatches, ...supportingMatches],
                };
            }
        }

        return bestMatch;
    }

    _confidence(strong, supporting) {
        if (strong >= 3) return 'High';
        if (strong >= 2 || (strong >= 1 && supporting >= 2)) return 'Medium';
        return 'Low';
    }
}
