{% extends "base.sql" %}

{% block target %}analytics.scraped_quotes.author_geo_stats{% endblock %}

{% block select %}
SELECT
    a.author_name,
    a.author_birthdate,
    a.author_birthplace,
    c.country_name,
    c.region,
    c.subregion,
    c.latitude,
    c.longitude,
    c.population                  AS country_population,
    COUNT(q.quote)                AS quote_count,
    COALESCE(SUM(p.pageviews), 0) AS pageviews
FROM "raw".scraped_quotes.authors a
LEFT JOIN "raw".scraped_quotes.quotes q
    ON UPPER(q.author) = UPPER(a.author_name)
LEFT JOIN pageviews p
    ON p.quote = q.quote
LEFT JOIN "raw".scraped_quotes.countries c
    ON UPPER(a.country_name) = UPPER(c.country_name)
GROUP BY
    a.author_name,
    a.author_birthdate,
    a.author_birthplace,
    c.country_name,
    c.region,
    c.subregion,
    c.latitude,
    c.longitude,
    c.population
{% endblock %}