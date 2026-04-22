/**
 * NewsCollector — pulls articles from multiple free/freemium sources
 *
 * Sources used (all free or freemium):
 *  1. Google News RSS          — free, no key
 *  2. Bing News RSS            — free, no key
 *  3. NewsAPI.org              — free tier (100 req/day)
 *  4. GNews.io                 — free tier (100 req/day)
 *  5. TheNewsAPI.com           — free tier (100 req/day)
 *  6. MediaStack               — free tier (500 req/month)
 *  7. SEC EDGAR full-text      — free, government
 *  8. PR Newswire RSS          — free RSS feed
 *  9. BusinessWire RSS         — free RSS feed
 * 10. Globe Newswire RSS       — free RSS feed
 */

import { log } from 'apify';
import axios from 'axios';
import { parseStringPromise } from 'xml2js';

const TIME_WINDOW_MAP = {
    '1d':  1,
    '3d':  3,
    '7d':  7,
    '14d': 14,
    '30d': 30,
    '90d': 90,
    '6m':  180,   // 6 months
    '1y':  365,   // 1 year
};

export class NewsCollector {
    constructor({ company_name, time_window, language = 'en' }) {
        this.company_name = company_name;
        this.days = TIME_WINDOW_MAP[time_window] ?? 7;
        this.language = language;
        this.cutoff = new Date(Date.now() - this.days * 86_400_000);
        this.encodedQuery = encodeURIComponent(`"${company_name}"`);
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
            this._bingNewsRSS(),
            this._prNewswireRSS(),
            this._businessWireRSS(),
            this._globeNewswireRSS(),
            this._secEdgar(),
        ];

        // Conditionally add paid APIs if keys exist
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

        return articles.filter(a => this._withinWindow(a.publishedAt || a.date));
    }

    _withinWindow(dateStr) {
        if (!dateStr) return true; // Include if no date (let classifier decide)
        const d = new Date(dateStr);
        return !isNaN(d) && d >= this.cutoff;
    }

    async _parseRSS(url, sourceLabel) {
        const resp = await axios.get(url, {
            timeout: 15000,
            headers: { 'User-Agent': 'Mozilla/5.0' },
        });
        const parsed = await parseStringPromise(resp.data, { explicitArray: false });
        const items = parsed?.rss?.channel?.item || parsed?.feed?.entry || [];
        const arr = Array.isArray(items) ? items : [items];

        return arr.map(item => ({
            title:       item.title?._ || item.title || '',
            description: item.description?._ || item.description || item.summary?._ || item.summary || '',
            url:         item.link?.href || item.link || item.guid?._ || item.guid || '',
            publishedAt: item.pubDate || item.published || item.updated || null,
            source:      sourceLabel,
        }));
    }

    async _googleNewsRSS() {
        const url = `https://news.google.com/rss/search?q=${this.encodedQuery}&hl=${this.language}&gl=US&ceid=US:${this.language}`;
        return this._parseRSS(url, 'Google News');
    }

    async _bingNewsRSS() {
        const url = `https://www.bing.com/news/search?q=${this.encodedQuery}&format=rss`;
        return this._parseRSS(url, 'Bing News');
    }

    async _prNewswireRSS() {
        const url = `https://www.prnewswire.com/rss/news-releases-list.rss`;
        const articles = await this._parseRSS(url, 'PR Newswire');
        const q = this.company_name.toLowerCase();
        return articles.filter(a =>
            a.title.toLowerCase().includes(q) ||
            a.description.toLowerCase().includes(q)
        );
    }

    async _businessWireRSS() {
        const url = `https://feed.businesswire.com/rss/home/?rss=G22`;
        const articles = await this._parseRSS(url, 'BusinessWire');
        const q = this.company_name.toLowerCase();
        return articles.filter(a =>
            a.title.toLowerCase().includes(q) ||
            a.description.toLowerCase().includes(q)
        );
    }

    async _globeNewswireRSS() {
        const url = `https://www.globenewswire.com/RssFeed/subjectCode/15`;
        const articles = await this._parseRSS(url, 'GlobeNewswire');
        const q = this.company_name.toLowerCase();
        return articles.filter(a =>
            a.title.toLowerCase().includes(q) ||
            a.description.toLowerCase().includes(q)
        );
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
            title:       h._source?.period_of_report
                ? `SEC Filing: ${h._source?.form_type} — ${this.company_name}`
                : `SEC Filing: ${h._source?.form_type || '8-K'}`,
            description: h._source?.file_date
                ? `Filed: ${h._source.file_date}. Form type: ${h._source?.form_type}`
                : '',
            url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=${encodeURIComponent(this.company_name)}&type=8-K`,
            publishedAt: h._source?.file_date || null,
            source:      'SEC EDGAR',
        }));
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

    _isoDate(d) {
        return d.toISOString().split('T')[0];
    }
}
