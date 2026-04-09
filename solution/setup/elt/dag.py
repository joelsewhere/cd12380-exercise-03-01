from airflow.sdk import dag, task_group
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
    SQLCheckOperator,
)
from datetime import datetime
import pathlib

DAG_ROOT = pathlib.Path(__file__).parent

ANALYTICS_CONN_ID = 'redshift_analytics'

AUTHOR_GEO_STATS = 'analytics.scraped_quotes.author_geo_stats'
TAG_ENGAGEMENT = 'analytics.scraped_quotes.tag_engagement'


@dag(
    schedule='@daily',
    start_date=datetime(2026, 3, 8),
    template_searchpath=[str(DAG_ROOT / 'sql')],
)
def quotes_analytics():

    # ── Transform ─────────────────────────────────────────────────────────────

    author_geo_stats = SQLExecuteQueryOperator(
        task_id='author_geo_stats',
        conn_id=ANALYTICS_CONN_ID,
        sql='author_geo_stats.sql',
        split_statements=True,
    )

    tag_engagement = SQLExecuteQueryOperator(
        task_id='tag_engagement',
        conn_id=ANALYTICS_CONN_ID,
        sql='tag_engagement.sql',
        split_statements=True,
    )

    # ── Checks ────────────────────────────────────────────────────────────────
    @task_group
    def check_author_geo_stats():

        # Column-level checks — constraints that should hold for every row.
        column_checks = SQLColumnCheckOperator(
            task_id='column_checks',
            conn_id=ANALYTICS_CONN_ID,
            table=AUTHOR_GEO_STATS,
            column_mapping={
                'author_name': {
                    # Zero authors have a null name
                    'null_check': {'equal_to': 0},
                    # Zero authors appear more than once
                    'unique_check': {'equal_to': 0},
                },
                'quote_count': {
                    # Zero quote counts are null
                    'null_check': {'equal_to': 0},
                    # Zero quote counts are negative
                    'min': {'geq_to': 0},
                },
                'pageviews': {
                    # Zero pageviews are null
                    'null_check': {'equal_to': 0},
                    # Zero pageviews are negative
                    'min': {'geq_to': 0},
                },
            },
        )

        # Table-level checks — business rules expressed as SQL CASE statements.
        # Each check must return 1 (pass) or 0 (fail).
        # 1. The table must not be empty.
        # 2. Every author with a country_name must also have a region —
        #    a partial join result indicates a countries table problem.
        # 3. No author should have more pageviews than is plausible given
        #    the log-normal distribution used to generate the fake data
        #    (upper bound: 10 authors * max lognormal value ~1,000,000).
        table_checks = SQLTableCheckOperator(
            task_id='table_checks',
            conn_id=ANALYTICS_CONN_ID,
            table=AUTHOR_GEO_STATS,
            checks={
                'table_not_empty': {
                    'check_statement': 'COUNT(*) > 0',
                },
                'country_implies_region': {
                    'check_statement': """
                        SUM(CASE
                            WHEN country_name IS NOT NULL AND region IS NULL
                            THEN 1 ELSE 0
                        END) = 0
                    """,
                },
                'pageviews_within_bounds': {
                    'check_statement': 'MAX(pageviews) < 10000000',
                },
            },
        )

        column_checks, table_checks

    @task_group
    def check_tag_engagement():

        column_checks = SQLColumnCheckOperator(
            task_id='column_checks',
            conn_id=ANALYTICS_CONN_ID,
            table=TAG_ENGAGEMENT,
            column_mapping={
                'tag_value': {
                    'null_check': {'equal_to': 0},
                    'unique_check': {'equal_to': 0},
                },
                'quote_count': {
                    'null_check': {'equal_to': 0},
                    'min': {'geq_to': 1},
                },
                'distinct_authors': {
                    'null_check': {'equal_to': 0},
                    'min': {'geq_to': 1},
                },
                'pageview_weighted_score': {
                    'null_check': {'equal_to': 0},
                    'min': {'geq_to': 0},
                },
            },
        )

        table_checks = SQLTableCheckOperator(
            task_id='table_checks',
            conn_id=ANALYTICS_CONN_ID,
            table=TAG_ENGAGEMENT,
            checks={
                'table_not_empty': {
                    'check_statement': 'COUNT(*) > 0',
                },
                # distinct_authors can never exceed quote_count —
                # you can't have more unique authors than quotes for a tag.
                'authors_leq_quotes': {
                    'check_statement': """
                        SUM(CASE
                            WHEN distinct_authors > quote_count
                            THEN 1 ELSE 0
                        END) = 0
                    """,
                },
                # Weighted score is total_pageviews / quote_count so it
                # must always be >= total_pageviews / quote_count minimum.
                'weighted_score_consistent': {
                    'check_statement': """
                        SUM(CASE
                            WHEN total_pageviews > 0
                            AND pageview_weighted_score <= 0
                            THEN 1 ELSE 0
                        END) = 0
                    """,
                },
            },
        )

        column_checks, table_checks

    # ── Cross-table check ──────────────────────────────────────────
    # Every author in "raw.scraped_quotes.authors" must appear in author_geo_stats.
    # If this fails it means the join is failing
    cross_table_check = SQLCheckOperator(
        task_id='cross_table_author_consistency',
        conn_id=ANALYTICS_CONN_ID,
        sql=f"""
            SELECT COUNT(*) = 0
            FROM (
                SELECT DISTINCT author_name
                FROM "raw".scraped_quotes.authors
                WHERE author_name NOT IN (
                    SELECT author_name FROM analytics.scraped_quotes.author_geo_stats
                )
            )
        """,
    )

    # ── Dependencies ──────────────────────────────────────────────────
    # Cross-table check runs after both tables are verified individually
    author_geo_stats >> check_author_geo_stats() >> cross_table_check
    tag_engagement   >> check_tag_engagement() >> cross_table_check

quotes_analytics()