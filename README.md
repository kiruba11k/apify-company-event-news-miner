 # Company News and Event Miner

An Apify actor that monitors any company across 11+ free news sources and classifies articles into structured business trigger events - funding rounds, expansions, M&A, product launches, partnerships, compliance actions, leadership changes and layoffs. Designed for sales teams, recruiters, investors and competitive intelligence analysts who need to act on business events before their competitors do.

---

## What It Does

For each company you provide, the actor runs a five-stage pipeline:

1. **Collect** - Fetches articles from 11 free sources simultaneously (no API keys required by default)
2. **Deduplicate** - Removes near-duplicate stories using Jaccard title similarity so the same event does not appear twice
3. **Classify** - Categorizes each article into one of 8 structured event types using rule-based keyword NLP with zero external API calls
4. **Score** - Rates each event 1–10 on business impact using source credibility, recency, company-name prominence, dollar amounts, and classification confidence
5. **Summarize** - Optionally generates a grounded 2–3 sentence summary using Groq LLM (free Groq API key required; step is skipped if no key provided)

All classification and scoring runs locally with no paid API dependency. The actor is fully functional with zero API keys.

---

## Event Categories

| Category | What It Detects |
|---|---|
| expansion | New markets, offices, facilities, geographic rollouts, hiring surges |
| mergers_acquisitions | Acquisitions, mergers, IPOs, SPACs, divestitures, spin-offs, take-privates |
| product_launch | New products, features, platform releases, beta launches, major updates |
| funding | VC rounds, seed to Series F, grants, debt financing, growth equity |
| partnership | Joint ventures, strategic alliances, licensing deals, distribution agreements, MOUs |
| compliance | Regulatory approvals, fines, lawsuits, audits, data breaches, certifications |
| leadership_change | CEO/CFO/CTO appointments, resignations, board changes, executive transitions |
| layoffs | Workforce reductions, restructuring, furloughs, headcount cuts |

---

## News Sources


| Source | Coverage |
|---|---|
| Google News RSS | Real-time global news - one generic query per company |
| Google News RSS (category queries) | One targeted query per active event category - surfaces event-specific articles that a generic search misses |
| Bing News RSS | Real-time global news |
| PR Newswire RSS | Official company press releases |
| BusinessWire RSS | Official press releases |
| GlobeNewswire RSS | Official press releases |
| SEC EDGAR Full-Text | 8-K, S-1, and 10-Q filings (US public companies) |
| Hacker News (Algolia API) | Tech company news and discussions - completely free, no rate limits |
| Reddit JSON Search | Business and investing subreddits: r/investing, r/stocks, r/finance, r/business, r/startups |
| AP Business RSS | Associated Press business news feed |
| Yahoo Finance RSS | Financial headline feed |

---

## Input Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `company_name` | string | - | Single company to monitor |
| `companies_csv` | string | - | Bulk mode: CSV text or URL with one company name per row, first column. Supports up to 20 companies. Overrides `company_name`. |
| `time_window` | string | `7d` | How far back to look: `1d`, `3d`, `7d`, `14d`, `30d`, `90d`, `6m`, `1y` |
| `intent_categories` | array | All 8 | Which event types to track. Leave default to monitor all. |
| `max_results` | integer | `50` | Maximum events returned per company (1–500) |
| `min_impact_score` | integer | `3` | Minimum impact score threshold (1–10). Increase to reduce noise. |
| `language` | string | `en` | Language code for news filtering |
| `groq_api_key` | string | - | Optional. Groq API key for LLM summarization. Free at console.groq.com. |
| `groq_verify` | boolean | `false` | Run a second Groq pass to verify summaries against source text. Recommended for high-accuracy runs. |

### Bulk CSV Format

The `companies_csv` field accepts inline CSV text, a URL to a CSV file, or a local file path. Any of the following work:

```
Stripe
OpenAI
Salesforce
Shopify
```

Or with a header row:

```
company_name
Stripe
OpenAI
Salesforce
```

---

## Output Fields

Each record in the output dataset contains:

| Field | Description |
|---|---|
| `company_name` | The monitored company |
| `event_type` | One of the 8 classified event categories |
| `headline` | Article title (plain text, HTML stripped) |
| `description` | Article excerpt or lead paragraph |
| `summary` | Groq-generated 2–3 sentence summary (empty if no Groq key provided) |
| `event_date` | Article publication date (ISO string) |
| `source` | News source name |
| `source_link` | Direct URL to the original article (not a redirect) |
| `intent_signal` | `High`, `Medium`, or `Low` based on impact score |
| `event_impact_score` | Integer 1–10 |
| `relevance_score` | How prominently the company is mentioned: 4 = in title, 1 = description only |
| `confidence` | Classifier confidence: `High`, `Medium`, or `Low` |
| `keywords_matched` | Array of matched classification keywords |
| `scraped_at` | ISO timestamp when the event was extracted |

Results are sorted by `event_impact_score` descending within each company.

### Example Output Record

```json
{
  "company_name": "Stripe",
  "event_type": "funding",
  "headline": "Stripe closes $694 million funding round at $65 billion valuation",
  "description": "Payments giant Stripe has closed a new funding round, raising $694 million at a valuation of $65 billion, the company confirmed Wednesday.",
  "summary": "Stripe raised $694 million in a new funding round that values the payments company at $65 billion. The round was led by existing investors and will be used to cover taxes related to employee stock awards.",
  "event_date": "2023-03-15T00:00:00.000Z",
  "source": "TechCrunch",
  "source_link": "https://techcrunch.com/2023/03/15/stripe-funding-65-billion/",
  "intent_signal": "High",
  "event_impact_score": 10,
  "relevance_score": 4,
  "confidence": "High",
  "keywords_matched": ["funding", "raises", "million", "valuation", "investor"],
  "scraped_at": "2024-01-20T14:32:11.000Z"
}
```

---

## Impact Scoring Model

Scores are computed additively and capped at 10.

**Base scores by event type:**

| Event Type | Base Score | Rationale |
|---|---|---|
| funding | 7 | Strong purchase and vendor-change signal |
| mergers_acquisitions | 7 | Major corporate change; new budget owners |
| expansion | 6 | Growth signal; new geographic or hiring need |
| leadership_change | 6 | New decision-maker; relationship reset opportunity |
| product_launch | 5 | Investment in new direction |
| partnership | 5 | Ecosystem change |
| layoffs | 5 | Cost-cutting or restructuring; technology replacement signal |
| compliance | 4 | Context-dependent; high only with major enforcement actions |

**Modifiers:**

| Condition | Modifier |
|---|---|
| Company name appears in article title | +2 |
| Source is Reuters, Bloomberg, AP News, WSJ, TechCrunch, or similar | +1 |
| Dollar amount mentioned (millions or billions) | +1 |
| Article published within 48 hours | +1 |
| High-confidence classification (3+ strong keywords) | +1 |
| 3 or more keyword matches total | +1 |
| Source is only a PR wire (no independent corroboration) | -1 |
| Low-confidence classification | -1 |
| Company only in description, not in title | -1 |

---

## Relevance Filtering

Every article is scored for company relevance before classification. Articles where the company name (or a stripped variant without legal suffixes like Inc., Corp., Ltd.) does not appear in either the title or description are dropped entirely.

A `relevance_score` of 4 means the company name appeared in the article title - these are the most targeted results. A score of 1 means the company appeared only in the description or body - useful but lower confidence.

The `event_impact_score` incorporates the relevance score, so high-relevance articles rank higher in the output automatically.

---

## Fluff Rejection

The classifier automatically rejects articles matching common PR fluff patterns that have no business trigger value:

- Awards and "best place to work" lists
- Executive speaking engagements and keynote announcements
- Birthday and anniversary celebrations
- CSR donations and volunteer initiatives
- "Proud to announce" culture posts

---

## Groq LLM Summarization (Optional)

When a Groq API key is provided, the actor generates a grounded 2–3 sentence factual summary for each event using `llama3-70b-8192`. The summarizer is designed to avoid hallucination:

- Temperature is set to 0 (deterministic output)
- The model is instructed to use only information present in the article text
- If the article text is too short (under 80 characters), the LLM step is skipped and the raw excerpt is used instead
- With `groq_verify: true`, a second Groq call checks the summary against the source and discards it if any unsupported claims are detected


---

## Run Summary

After each run, a summary object is saved to the key-value store under the key `SUMMARY`:

```json
{
  "mode": "bulk",
  "companies_count": 3,
  "time_window": "7d",
  "total_events_found": 47,
  "run_at": "2024-01-20T14:35:00.000Z",
  "per_company": [
    {
      "company": "Stripe",
      "total_collected": 84,
      "after_dedup": 61,
      "high_value_events": 18,
      "categories_found": ["funding", "product_launch", "partnership"]
    }
  ]
}
```

---

## Use Cases

**Sales prospecting and trigger-based outreach**
Monitor target accounts for funding rounds, leadership changes, and expansions. Reach out when a company just hired a new VP of Engineering or opened a new regional office - before your competitors do.

**Competitive intelligence**
Track competitor product launches, partnerships, and compliance actions. Get structured alerts the moment a competitor announces a new market entry or distribution deal.

**Recruiting and talent intelligence**
Monitor companies for layoff announcements and restructuring events to identify displaced candidates. Track expansion events for companies likely to be hiring.

**Investment research**
Monitor portfolio companies and prospects for funding rounds, M&A activity, and regulatory developments. The SEC EDGAR source surfaces 8-K, S-1, and 10-Q filings for US public companies automatically.

**Account-based marketing**
Enrich your CRM with structured event data to trigger campaign sequences based on company milestones.

---

## Limitations

- The actor monitors publicly available news and press releases. It does not access paywalled content.
- Google News and Bing News RSS feeds return the last 10–30 articles per query by design. For older events, increase the `time_window` 
- SEC EDGAR results cover US-listed public companies only.
- Hacker News results skew toward technology companies. For manufacturing, healthcare, or other sectors, the RSS wire sources provide better coverage.
- Groq summarization is disabled by default. The `description` field always contains the raw article excerpt regardless of whether Groq is configured.

---
