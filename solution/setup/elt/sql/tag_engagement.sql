{% extends "base.sql" %}

{% block target %}analytics.scraped_quotes.tag_engagement{% endblock %}

{% block select %}
, unnested_tags AS (
    SELECT
        author,
        quote,
        t AS tag_value
    FROM (
        SELECT q.author, q.quote, t
        FROM "raw".scraped_quotes.quotes q, q.tags AS t
    )
),
tag_metrics AS (
    SELECT
        t.tag_value,
        COUNT(*)                      AS quote_count,
        COUNT(DISTINCT t.author)      AS distinct_authors,
        SUM(COALESCE(p.pageviews, 0)) AS total_pageviews
    FROM unnested_tags t
    LEFT JOIN pageviews p
        ON p.quote = t.quote
    GROUP BY t.tag_value
)
SELECT
    tag_value,
    quote_count,
    distinct_authors,
    total_pageviews,
    -- Normalise by quote count so a tag used once by a high-traffic
    -- quote doesn't automatically outscore a broad, popular tag
    CAST(total_pageviews AS FLOAT) / NULLIF(quote_count, 0) AS pageview_weighted_score
FROM tag_metrics
ORDER BY pageview_weighted_score DESC
{% endblock %}