DROP TABLE IF EXISTS {% block target %}{% endblock %};

CREATE TABLE {{ self.target() }} AS
WITH pageviews AS (
    SELECT
        quote,
        pageviews
    FROM "raw".scraped_quotes.pageviews
){% block select %}{% endblock %}