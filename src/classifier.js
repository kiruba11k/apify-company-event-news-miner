/**
 * EventClassifier
 *
 * Rule-based NLP classifier using keyword patterns per intent category.
 * Zero external API calls — runs locally.

 *
 * Categories:
 *  - expansion            (new markets, offices, regions, facilities)
 *  - mergers_acquisitions (M&A, IPO, takeovers, divestitures)
 *  - product_launch       (new products, features, releases)
 *  - funding              (VC rounds, grants, debt financing)
 *  - partnership          (alliances, JVs, contracts, integrations)
 *  - compliance           (fines, lawsuits, regulatory, certifications)
 *  - leadership_change    (CEO changes, board appointments, exec hires/exits)
 *  - layoffs              (workforce reductions, restructuring, job cuts)
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
            'new campus', 'new hub', 'new site', 'new territory',
            'scale internationally', 'cross-border expansion',
        ],
        supporting: [
            'growth', 'scale', 'international', 'regional', 'hire', 'hiring surge',
            'headcount', 'workforce expansion', 'job openings', 'global presence',
            'market entry', 'new customers', 'customer base',

        ],
    },

    mergers_acquisitions: {
        strong: [
            'acqui', 'acquires', 'acquired', 'acquisition', 'merger', 'merges',
            'merged with', 'takeover', 'buyout', 'buy out', 'purchased',
            'ipo', 'went public', 'listed on', 'initial public offering',
'            reverse merger', 'going public', 'stock market debut',            
            'hostile takeover', 'friendly takeover', 'strategic acquisition',
            'divest', 'divestiture', 'spin-off', 'spinoff', 'carve-out',
            'agreed to buy', 'agreed to acquire', 'deal to buy', 'deal to acquire',
            'combined company', 'post-merger', 'integration complete',
            'tender offer', 'all-cash deal', 'all-stock deal', 'take private',
            'special purpose acquisition', 'de-spac', 'merge with',
        ],
        supporting: [
            'deal', 'transaction', 'shareholder', 'board approval',
            'regulatory approval', 'enterprise value', 'premium',
            'all-stock', 'all-cash', 'bid', 'offer', 'valuation',
            'public listing', 'nasdaq', 'nyse', 'stock exchange',

        ],
    },

    product_launch: {
        strong: [
            'launches', 'launch', 'launches new', 'new product', 'product launch',
            'unveils', 'unveil', 'introduces', 'introduces new', 'release',
            'releases', 'debuts', 'now available', 'generally available', 'ga release',
            'beta launch', 'early access', 'goes live', 'ships', 'rolling out',
            'new feature', 'new version', 'v2', 'major update', 'new model',
            'new platform', 'new tool', 'new service', 'new app', 'new software',
            'new api', 'new offering', 'new solution', 'product update',
            'released today', 'launching today', 'announcing',
        ],
        supporting: [
            'innovation', 'technology', 'platform', 'solution', 'service',
            'app', 'software', 'product', 'api', 'sdk', 'customers',
            'users', 'market', 'feature set',
        ],
    },

    funding: {
        strong: [
            'raises', 'raised', 'funding', 'series a', 'series b', 'series c',
            'series d', 'series e', 'series f', 'seed round', 'pre-seed',
            'investment round', 'venture capital', 'vc backed', 'backed by',
            'capital raise', 'round closed', 'fundraising', 'grant awarded',
            'grant received', 'financial backing', 'stock offering',
            'debt financing', 'growth equity', 'private equity', 'crowdfunding',
            'convertible note', 'bridge round', 'follow-on funding',
            'secures funding', 'closes funding', 'completes fundraise',
            'new funding', 'raises capital', 'secures investment',
        ],
       
        supporting: [
             'million', 'billion', 'valuation', 'investor', 'investors', 'equity',
            'startup', 'growth capital', 'fundraise', 'revenue milestone',
            'profitable', 'unicorn', 'decacorn',
        ],
    },

    partnership: {
        strong: [
            'partnership', 'partners with', 'partnered with', 'joint venture',
            'collaboration', 'collaborates', 'strategic alliance', 'alliance',
            'mou', 'memorandum of understanding', 'agreement signed',
            'contract awarded', 'contract signed', 'supplier agreement',
            'distribution agreement', 'licensing deal', 'reseller agreement',
            'technology partnership', 'co-develop', 'co-create',
            'official partner', 'preferred partner', 'exclusive agreement',
            'teaming agreement', 'framework agreement', 'global agreement',
        ],
        supporting: [
           'deal', 'integrates', 'integration', 'ecosystem', 'together', 'combined',
            'connect', 'interoperate', 'works with', 'powered by',
        ],
    },

    compliance: {
        strong: [
            'fined', 'fine', 'penalty', 'lawsuit', 'litigation', 'legal action',
            'settlement', 'approved by', 'fda approval', 'sec filing', 'audit',
            'violation', 'gdpr', 'ccpa', 'iso certified', 'soc 2', 'certification',
            'sanctioned', 'investigation', 'probe', 'subpoena', 'indictment',
            'class action', 'regulatory action', 'enforcement action',
            'consent decree', 'compliance order', 'cease and desist',
            'whistleblower', 'anti-trust', 'antitrust', 'data breach',
            'regulatory approval', 'cleared by', 'fcc approval', 'ftc investigation',
        ],
        supporting: [
            'compliance', 'regulation', 'regulatory', 'legal', 'government',
            'authority', 'court', 'ruling', 'mandate', 'enforcement',
            'policy change', 'standard', 'breach', 'privacy',
        ],
    },
      leadership_change: {
        strong: [
            'appoints', 'appointed', 'names new', 'names as', 'hires as',
            'new ceo', 'new cfo', 'new cto', 'new coo', 'new cmo', 'new cso',
            'new president', 'new chairman', 'new director', 'new vp',
            'chief executive', 'chief financial officer', 'chief technology officer',
            'steps down', 'stepping down', 'resigns', 'resigned', 'resignation',
            'retires', 'retiring', 'retirement', 'departing', 'departs',
            'board appoints', 'board elects', 'new board member', 'joins as',
            'promoted to', 'elevation to', 'succeeds as', 'replaces as',
            'executive transition', 'leadership transition', 'management change',
        ],
        supporting: [
            'executive', 'leadership', 'management', 'board', 'c-suite',
            'officer', 'director', 'president', 'founder', 'co-founder',
        ],
    },

    layoffs: {
        strong: [
            'layoffs', 'layoff', 'lays off', 'laid off', 'job cuts', 'cutting jobs',
            'workforce reduction', 'reduce workforce', 'headcount reduction',
            'restructuring', 'restructures', 'downsizing', 'downsizes',
            'redundancies', 'reductions in force', 'rif', 'eliminates positions',
            'cuts staff', 'lets go', 'dismisses', 'furlough', 'furloughs',
            'mass layoff', 'batch layoffs', 'workforce cuts', 'team cuts',
            'job losses', 'eliminating roles', 'streamlining operations',
        ],
        supporting: [
            'employees', 'workers', 'staff', 'jobs', 'positions',
            'cost-cutting', 'cost reduction', 'efficiency', 'restructure',
            'operational changes', 'organizational changes',
        ],
    },

    technology: {
        strong: [
            'artificial intelligence', 'ai adoption', 'ai integration', 'machine learning',
            'deep learning', 'generative ai', 'large language model', 'llm', 'gpt',
            'digital transformation', 'digitization', 'digitalization',
            'cloud migration', 'cloud adoption', 'cloud infrastructure',
            'automation', 'automates', 'automated', 'robotic process automation', 'rpa',
            'blockchain', 'web3', 'smart contract', 'iot', 'internet of things',
            'cybersecurity', 'data breach', 'ransomware', 'zero trust',
            'research and development', 'r&d investment', 'patent filed', 'patent granted',
            'tech stack', 'infrastructure upgrade', 'platform migration', 'api integration',
            'data analytics', 'big data', 'machine intelligence', 'neural network',
            'quantum computing', 'edge computing', '5g deployment', 'digital strategy',
            'technology investment', 'it modernization', 'devops', 'open source',
        ],
        supporting: [
            'technology', 'tech', 'software', 'hardware', 'platform', 'digital',
            'innovation', 'data', 'cloud', 'saas', 'api', 'developer', 'engineering',
            'infrastructure', 'system', 'solution', 'algorithm', 'model', 'compute',
        ],
    },
};

// PR fluff patterns to REJECT
const FLUFF_PATTERNS = [
    /congratulat/i, /award.*winner/i, /best.*place.*to.*work/i,
    /culture.*award/i, /listed.*as.*top/i, /happy.*to.*announce.*team/i,
    /proud.*to.*welcome/i, /birthday/i, /anniversary.*celebrat/i,
    /thought.*leader/i, /keynote.*speaker/i, /featured.*in.*list/i,
    /\bCSR\b.*award/i, /charity.*donation/i, /volunteer/i,
];

export class EventClassifier {
    constructor(enabledCategories = Object.keys(CATEGORY_RULES), customIntent = '') {
        this.rules = {};
        for (const cat of enabledCategories) {
            if (CATEGORY_RULES[cat]) this.rules[cat] = CATEGORY_RULES[cat];
        }
        // Inject custom intent as a dynamic category
        if (customIntent && customIntent.trim()) {
            const terms = customIntent.trim().toLowerCase().split(/[\s,;]+/).filter(Boolean);
            this.rules['custom'] = { strong: terms, supporting: [] };
        }
    }

    classify(article) {
        const text = `${article.title} ${article.description || ''}`.toLowerCase();

        // Reject PR fluff first
        if (FLUFF_PATTERNS.some(p => p.test(text))) return null;

        let bestMatch = null;
        let bestScore = 0;

        for (const [event_type, { strong, supporting }] of Object.entries(this.rules)) {
            const strongMatches    = strong.filter(kw => text.includes(kw));
            const supportingMatches = supporting.filter(kw => text.includes(kw));

            if (strongMatches.length === 0) continue;

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
