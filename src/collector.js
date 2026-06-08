/**
 * NewsCollector — pulls articles from multiple free/freemium sources
 *
* Free sources (no API key required):
 *  1.  Google News RSS           — real-time, company + category targeted queries
 *  2.  Bing News RSS             — real-time
 *  3.  PR Newswire RSS           — press releases
 *  4.  BusinessWire RSS          — press releases
 *  5.  GlobeNewswire RSS         — press releases
 *  6.  SEC EDGAR full-text       — 8-K, S-1, 10-Q filings (US companies)
 *  7.  Hacker News (Algolia API) — free, great for tech companies
 *  8.  Reddit JSON search        — r/investing, r/stocks, r/business
 *  9.  AP Business RSS           — Associated Press business news
 *  10. Yahoo Finance RSS         — company-level headline feed
 *  11. MarketWatch RSS           — financial/business news
 *
 * Freemium (free tier, API key optional):
 *  12. NewsAPI.org               — 100 req/day free tier
 *  13. GNews.io                  — 100 req/day free tier
 *  14. TheNewsAPI.com            — 100 req/day free tier
 *  15. MediaStack                — 500 req/month free tier
 */

import { log } from 'apify';
import axios from 'axios';
import { parseStringPromise } from 'xml2js';
import * as cheerio from 'cheerio';
/**
 * Strip HTML tags and return plain text. Also extracts the first <a href>
 * as the real URL (needed for Google News RSS which wraps real links in HTML).
 */
function parseHtmlField(html) {
    if (!html || typeof html !== 'string') return { text: html || '', realUrl: null };
    if (!html.includes('<')) return { text: html, realUrl: null };
    const $ = cheerio.load(html);
    const realUrl = $('a[href]').first().attr('href') || null;
    const text = $.text().trim();
    return { text, realUrl };
}
const TIME_WINDOW_MAP = {
    '1d':  1,
    '3d':  3,
    '7d':  7,
    '14d': 14,
    '30d': 30,
    '90d': 90,
    '6m':  180,
    '1y':  365,
};

// Category-specific search query modifiers for targeted Google/Bing queries
const CATEGORY_QUERY_TERMS = {
    expansion:           'expansion OR "new market" OR "new office" OR "opens in" OR "new location"',
    mergers_acquisitions:'acquisition OR merger OR acquires OR "goes public" OR IPO OR buyout',
    product_launch:      'launches OR "new product" OR "product launch" OR unveils OR releases',
    funding:             'funding OR "series a" OR "series b" OR raises OR investment OR "venture capital"',
    partnership:         'partnership OR "joint venture" OR collaboration OR "strategic alliance" OR agreement',
    compliance:          'regulation OR compliance OR lawsuit OR fine OR penalty OR approval OR settlement',
    leadership_change:   'CEO OR "chief executive" OR appoints OR "new president" OR "board of directors" OR "executive"',
    layoffs:             'layoffs OR "job cuts" OR restructuring OR "workforce reduction" OR redundancies OR downsizing',
};

export class NewsCollector {
    constructor({ company_name, time_window, language = 'en', intent_categories = [] }) {
        this.company_name = company_name;
        this.days = TIME_WINDOW_MAP[time_window] ?? 7;
        this.language = language;
        this.intent_categories = intent_categories;
        this.cutoff = new Date(Date.now() - this.days * 86_400_000);
        this.encodedQuery = encodeURIComponent(`"${company_name}"`);
        this.nameVariants = this._buildNameVariants(company_name);

        this.apiKeys = {
            newsapi:    process.env.NEWSAPI_KEY    || '',
            gnews:      process.env.GNEWS_KEY      || '',
            thenewsapi: process.env.THENEWSAPI_KEY || '',
            mediastack: process.env.MEDIASTACK_KEY || '',
        };
    }

    async collect() {
        const tasks = [
            this._googleNewsRSS(),
            this._googleNewsCategoryQueries(),

            this._bingNewsRSS(),
            this._prNewswireRSS(),
            this._businessWireRSS(),
            this._globeNewswireRSS(),
            this._secEdgar(),
            this._hackerNews(),
            this._redditSearch(),
            this._apBusinessRSS(),
            this._yahooFinanceRSS(),
        ];

        if (this.apiKeys.newsapi)    tasks.push(this._newsapi());
        if (this.apiKeys.gnews)      tasks.push(this._gnews());
        if (this.apiKeys.thenewsapi) tasks.push(this._thenewsapi());
        if (this.apiKeys.mediastack) tasks.push(this._mediastack());

        const settled = await Promise.allSettled(tasks);
        const articles = [];

        for (const result of settled) {
            if (result.status === 'fulfilled') {
                articles.push(...result.value);
            } else {
                log.warning(`Source failed: ${result.reason?.message}`);
            }
        }

                // Filter by time window, then score relevance
        return articles
            .filter(a => this._withinWindow(a.publishedAt || a.date))
            .map(a => ({ ...a, relevanceScore: this._relevanceScore(a) }))
            .filter(a => a.relevanceScore > 0);
    }
        // ── Relevance scoring ──────────────────────────────────────────────────────

    _buildNameVariants(name) {
        const variants = [name.toLowerCase()];
        // Strip common legal suffixes for matching
        const stripped = name
            .replace(/\b(inc\.?|corp\.?|ltd\.?|llc\.?|plc\.?|co\.?|group|holdings?|technologies|tech|solutions|services|international|global)\b/gi, '')
            .trim()
            .toLowerCase();
        if (stripped && stripped !== variants[0]) variants.push(stripped);
        // Add abbreviated version (first word) if multi-word
        const words = stripped.split(/\s+/).filter(Boolean);
        if (words.length > 1 && words[0].length >= 4) variants.push(words[0]);
        return [...new Set(variants)];
    }

    _relevanceScore(article) {
        const title = (article.title || '').toLowerCase();
        const desc  = (article.description || '').toLowerCase();

        let score = 0;
        for (const variant of this.nameVariants) {
            if (title.includes(variant)) {
                score += 3; // Strong signal: company in title
                break;
            }
        }
        for (const variant of this.nameVariants) {
            if (desc.includes(variant)) {
                score += 1; // Weaker signal: only in description
                break;
            }
        }
        return score;
    }

    _withinWindow(dateStr) {
        if (!dateStr) return true;
        const d = new Date(dateStr);
        return !isNaN(d) && d >= this.cutoff;
    }

    async _parseRSS(url, sourceLabel) {
        const resp = await axios.get(url, {
            timeout: 15000,
            headers: { 'User-Agent': 'Mozilla/5.0 (compatible; CompanyNewsMiner/2.0)' },
        });
        const parsed = await parseStringPromise(resp.data, { explicitArray: false });
        const items = parsed?.rss?.channel?.item || parsed?.feed?.entry || [];
        const arr = Array.isArray(items) ? items : [items];

        return arr.map(item => {
            const rawDesc = item.description?._ || item.description || item.summary?._ || item.summary || '';
            const { text: descText, realUrl: descUrl } = parseHtmlField(rawDesc);
            const rawTitle = item.title?._ || item.title || '';
            const { text: titleText } = parseHtmlField(rawTitle);
            const rawUrl = item.link?.href || item.link || item.guid?._ || item.guid || '';
            // Prefer real article URL extracted from HTML description over redirect/guid URL
            const url = descUrl || rawUrl || '';
            return {
                title:       titleText,
                description: descText,
                url,
                publishedAt: item.pubDate || item.published || item.updated || null,
                source:      sourceLabel,
            };
        });
    }

    async _googleNewsRSS() {
        const url = `https://news.google.com/rss/search?q=${this.encodedQuery}&hl=${this.language}&gl=US&ceid=US:${this.language}`;
        return this._parseRSS(url, 'Google News');
    }
    // Run targeted Google News queries for each active category to surface
    // articles that are event-relevant but may not rank in the generic search
    async _googleNewsCategoryQueries() {
        const categories = this.intent_categories.length > 0
            ? this.intent_categories
            : Object.keys(CATEGORY_QUERY_TERMS);

        const results = await Promise.allSettled(
            categories.map(cat => {
                const terms = CATEGORY_QUERY_TERMS[cat];
                if (!terms) return Promise.resolve([]);
                const q = encodeURIComponent(`"${this.company_name}" (${terms})`);
                const url = `https://news.google.com/rss/search?q=${q}&hl=${this.language}&gl=US&ceid=US:${this.language}`;
                return this._parseRSS(url, `Google News / ${cat}`);
            })
        );

        const articles = [];
        for (const r of results) {
            if (r.status === 'fulfilled') articles.push(...r.value);
        }
        return articles;
    }
    async _bingNewsRSS() {
        const url = `https://www.bing.com/news/search?q=${this.encodedQuery}&format=rss`;
        return this._parseRSS(url, 'Bing News');
    }

    async _prNewswireRSS() {
        const url = `https://www.prnewswire.com/rss/news-releases-list.rss`;
        const articles = await this._parseRSS(url, 'PR Newswire');
        return this._filterByCompany(articles);

    }

    async _businessWireRSS() {
        const url = `https://feed.businesswire.com/rss/home/?rss=G22`;
        const articles = await this._parseRSS(url, 'BusinessWire');
        return this._filterByCompany(articles);
    }

    async _globeNewswireRSS() {
        const url = `https://www.globenewswire.com/RssFeed/subjectCode/15`;
        const articles = await this._parseRSS(url, 'GlobeNewswire');
        return this._filterByCompany(articles);

    }

    async _secEdgar() {
        // SEC EDGAR full-text search — free government API
        const url = `https://efts.sec.gov/LATEST/search-index?q="${encodeURIComponent(this.company_name)}"&dateRange=custom&startdt=${this._isoDate(this.cutoff)}&forms=8-K,S-1,10-Q`;
        const resp = await axios.get(url, {
            timeout: 15000,
            headers: { 'User-Agent': 'CompanyNewsMiner contact@example.com' },
        });
        const hits = resp.data?.hits?.hits || [];
        return hits.map(h => ({
            title:       `SEC Filing: ${h._source?.form_type || '8-K'} — ${this.company_name}`,
            description: h._source?.file_date ? `Filed: ${h._source.file_date}. Form type: ${h._source?.form_type}` : '',
            url:         `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=${encodeURIComponent(this.company_name)}&type=8-K`,
            publishedAt: h._source?.file_date || null,
            source:      'SEC EDGAR',
        }));
    }
    // Hacker News via Algolia — completely free, no key needed
    async _hackerNews() {
        const query = encodeURIComponent(this.company_name);
        const cutoffTs = Math.floor(this.cutoff.getTime() / 1000);
        const url = `https://hn.algolia.com/api/v1/search?query=${query}&tags=story&numericFilters=created_at_i>${cutoffTs}&hitsPerPage=30`;
        const resp = await axios.get(url, { timeout: 15000 });
        return (resp.data.hits || []).map(h => ({
            title:       h.title || '',
            description: h.story_text || h.comment_text || '',
            url:         h.url || `https://news.ycombinator.com/item?id=${h.objectID}`,
            publishedAt: h.created_at || null,
            source:      'Hacker News',
        }));
    }

    // Reddit JSON search — free, no key, business/investing subreddits
    async _redditSearch() {
        const query = encodeURIComponent(`"${this.company_name}"`);
        const cutoffTs = Math.floor(this.cutoff.getTime() / 1000);
        const url = `https://www.reddit.com/search.json?q=${query}&sort=new&t=month&limit=25&restrict_sr=false`;
        const resp = await axios.get(url, {
            timeout: 15000,
            headers: {
                'User-Agent': 'CompanyNewsMiner/2.0 (by /u/newsminer_bot)',
                'Accept': 'application/json',
            },
        });
        const posts = resp.data?.data?.children || [];
        return posts
            .filter(p => {
                const subreddit = (p.data.subreddit || '').toLowerCase();
                const relevantSubs = ['investing', 'stocks', 'finance', 'business', 'wallstreetbets',
                    'news', 'technology', 'tech', 'economy', 'entrepreneur', 'startups'];
                return relevantSubs.some(s => subreddit.includes(s));
            })
            .filter(p => p.data.created_utc >= cutoffTs)
            .map(p => ({
                title:       p.data.title || '',
                description: p.data.selftext?.slice(0, 500) || '',
                url:         p.data.url || `https://reddit.com${p.data.permalink}`,
                publishedAt: new Date(p.data.created_utc * 1000).toISOString(),
                source:      `Reddit / r/${p.data.subreddit}`,
            }));
    }

    // AP Business RSS — free, no key
    async _apBusinessRSS() {
        const url = `https://feeds.apnews.com/rss/apf-business`;
        const articles = await this._parseRSS(url, 'AP News');
        return this._filterByCompany(articles);
    }

    // Yahoo Finance RSS — financial news, free
    async _yahooFinanceRSS() {
        // Yahoo Finance company search via their news RSS
        const query = encodeURIComponent(this.company_name);
        const url = `https://finance.yahoo.com/news/rssindex`;
        try {
            const articles = await this._parseRSS(url, 'Yahoo Finance');
            return this._filterByCompany(articles);
        } catch {
            // Fallback: query via Yahoo search RSS
            const fallbackUrl = `https://feeds.finance.yahoo.com/rss/2.0/headline?s=${query}&region=US&lang=en-US`;
            const articles = await this._parseRSS(fallbackUrl, 'Yahoo Finance');
            return articles;
        }
    }
    async _newsapi() {
        const from = this._isoDate(this.cutoff);
        const url  = `https://newsapi.org/v2/everything?q=${this.encodedQuery}&from=${from}&sortBy=relevancy&language=${this.language}&apiKey=${this.apiKeys.newsapi}`;
        const resp = await axios.get(url, { timeout: 15000 });
        return (resp.data.articles || []).map(a => ({
            title:       a.title || '',
            description: a.description || a.content || '',
            url:         a.url,
            publishedAt: a.publishedAt,
            source:      `NewsAPI / ${a.source?.name || 'Unknown'}`,
        }));
    }

    async _gnews() {
        const url  = `https://gnews.io/api/v4/search?q=${this.encodedQuery}&lang=${this.language}&max=10&apikey=${this.apiKeys.gnews}`;
        const resp = await axios.get(url, { timeout: 15000 });
        return (resp.data.articles || []).map(a => ({
            title:       a.title || '',
            description: a.description || a.content || '',
            url:         a.url,
            publishedAt: a.publishedAt,
            source:      `GNews / ${a.source?.name || 'Unknown'}`,
        }));
    }

    async _thenewsapi() {
        const url  = `https://api.thenewsapi.com/v1/news/all?search=${this.encodedQuery}&language=${this.language}&api_token=${this.apiKeys.thenewsapi}`;
        const resp = await axios.get(url, { timeout: 15000 });
        return (resp.data.data || []).map(a => ({
            title:       a.title || '',
            description: a.description || '',
            url:         a.url,
            publishedAt: a.published_at,
            source:      `TheNewsAPI / ${a.source || 'Unknown'}`,
        }));
    }

    async _mediastack() {
        const url  = `http://api.mediastack.com/v1/news?keywords=${this.encodedQuery}&languages=${this.language}&access_key=${this.apiKeys.mediastack}`;
        const resp = await axios.get(url, { timeout: 15000 });
        return (resp.data.data || []).map(a => ({
            title:       a.title || '',
            description: a.description || '',
            url:         a.url,
            publishedAt: a.published_at,
            source:      `MediaStack / ${a.source || 'Unknown'}`,
        }));
    }
    _filterByCompany(articles) {
        return articles.filter(a => {
            const text = `${a.title} ${a.description}`.toLowerCase();
            return this.nameVariants.some(v => text.includes(v));
        });
    }
    _isoDate(d) {
        return d.toISOString().split('T')[0];
    }
}
