from airflow.sdk import dag, task, task_group, Param
import pathlib
from datetime import datetime
import os

SCHEMA='scraped_quotes'
DAG_ROOT=pathlib.Path(__file__).parent
BUCKET="{{ 'l3-external-storage-753900908173' if params.environment == 'production' else 'l3-external-storage-753900908173' }}"
S3_KEYS={
    'extract': '{{ dag.dag_id }}/extract/{{ ds }}',
    'transform': '{{ dag.dag_id }}/transform/unprocessed/{{ ds }}',
    'processed': '{{ dag.dag_id }}/transform/processed/{{ ds }}'
    }

@dag(
    schedule='@daily',
    start_date=datetime(2026, 3, 8),
    end_date=datetime(2026, 3, 16),
    catchup=True,
    max_active_runs=1,
    params={
        'environment': Param(
            os.getenv('environment', 'production'),
            dtype='string',
            enum=['development', 'production']
            )
        }
    )
def quotes_scraper():

    @task_group
    def extract():

        @task
        def quotes(filepath, extract_key, BUCKET):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from bs4 import BeautifulSoup
            
            # Collect quotes
            html = pathlib.Path(filepath).read_text()
            
            # Push quotes to S3
            hook = S3Hook()
            hook.load_string(
                string_data=html,
                key=extract_key + '/quotes.html',
                bucket_name=BUCKET,
                replace=True,
                )
            
            # Scrape author urls
            soup = BeautifulSoup(html, features="lxml")
            author_containers = soup.find_all('small', {'class': 'author'})
            author_urls = [x.parent.find('a').attrs['href'] for x in author_containers]

            # Push author urls
            return author_urls
        
        @task
        def authors(author_links, extract_key, BUCKET, ds):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook = S3Hook()

            for link in author_links:

                author_name = link.split('/')[-1]

                filepath = (
                    pathlib.Path(__file__).parent / 
                    'authors' / 
                    (ds + '-' + author_name + '.html')
                    )
                
                html = filepath.read_text()
                key = extract_key + f'/authors/{author_name}.html'

                hook.load_string(
                    string_data=html,
                    key=key,
                    bucket_name=BUCKET,
                    replace=True
                    )
        
        @task
        def countries(bucket, s3_key):
            """
            Fetch every country from the REST Countries API and stage as CSV.
            Gives us region, subregion, population, and lat/lng for author
            birthplace enrichment in the ELT transform step.
            """
            import requests
            import pandas as pd
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            resp = requests.get(
                'https://restcountries.com/v3.1/all',
                params={'fields': 'name,region,subregion,latlng,population'},
                timeout=30,
            )
            resp.raise_for_status()

            records = []
            for c in resp.json():
                latlng = c.get('latlng') or [None, None]
                records.append({
                    'country_name':  c['name']['common'],
                    'official_name': c['name']['official'],
                    'region':        c.get('region', ''),
                    'subregion':     c.get('subregion', ''),
                    'latitude':      latlng[0],
                    'longitude':     latlng[1],
                    'population':    c.get('population'),
                })

            S3Hook().load_string(
                string_data=pd.DataFrame(records).to_csv(index=False),
                key=s3_key + '/countries.csv',
                bucket_name=bucket,
                replace=True,
            )

        filepath = (DAG_ROOT / 'quotes' / 'quotes-{{ ds }}.html').as_posix()

        author_links = quotes(filepath, S3_KEYS['extract'], BUCKET)
        authors(author_links, S3_KEYS['extract'], BUCKET)
        countries(BUCKET, S3_KEYS['extract'])

    @task_group
    def transform():

        @task
        def quotes(extract_key, transform_key, BUCKET):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from bs4 import BeautifulSoup
            import pandas as pd
            import json

            hook = S3Hook()
            html = hook.read_key(
                key=extract_key + '/quotes.html',
                bucket_name=BUCKET
                )
            
            soup = BeautifulSoup(html, features='lxml')
            quote_containers = soup.find_all('div', {'class': 'quote'})

            data = []
            for container in quote_containers:
                quote = container.find('span', {'class': 'text'}).text
                author = container.find('small', {'class': 'author'}).text
                tags = [
                    tag.text for tag in 
                    container.find('div', {'class': 'tags'}).find_all('a', {'class': 'tag'})
                ]
                data.append(
                    {
                        "quote": quote,
                        "author": author,
                        "tags": json.dumps(tags)
                    }
                )
            
            csv = pd.DataFrame(data).to_csv(index=False)
            hook.load_string(
                string_data=csv,
                key=transform_key + '/quotes.csv',
                bucket_name=BUCKET,
                replace=True
            )

        @task  
        def authors(extract_key, transform_key, BUCKET):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from bs4 import BeautifulSoup
            import pandas as pd

            hook = S3Hook()
            author_keys = hook.list_keys(
                prefix=extract_key + '/authors/',
                bucket_name=BUCKET
                )

            data = []
            for key in author_keys:
                html = hook.read_key(key=key, bucket_name=BUCKET)
                soup = BeautifulSoup(html)
                author_details = soup.find('div', {'class': 'author-details'})
                birthplace = author_details.find('span', {'class': 'author-born-location'}).text

                # Strip the leading "in " the site prepends to all birthplaces
                if birthplace.startswith('in '):
                    birthplace = birthplace[3:]

                # Extract a clean country name from the birthplace string.
                # Birthplace is formatted as "in City, Region, Country" or
                # "in Country" — take the last comma-separated token, then
                # strip the leading "in " and "The " prefixes so the value
                # joins cleanly against the REST Countries reference table.
                country_name = birthplace.split(',')[-1].strip()
                if country_name.startswith('The '):
                    country_name = country_name[4:]

                data.append(
                    {
                        'author_name':        author_details.find('h3', {'class': 'author-title'}).text,
                        'author_birthdate':   author_details.find('span', {'class': 'author-born-date'}).text,
                        'author_birthplace':  birthplace,
                        'author_description': author_details.find('div', {'class': 'author-description'}).text,
                        'country_name':       country_name,
                    }
                )

            csv = pd.DataFrame(data).to_csv(index=False)
            hook.load_string(
                string_data=csv,
                key=transform_key + '/authors.csv',
                bucket_name=BUCKET,
                replace=True,
                )

        @task
        def pageviews(bucket, s3_key):
            """
            Generate a fake pageviews dataset at the quote level. In a real
            pipeline this would come from an analytics platform (GA4, Plausible,
            etc.). We simulate it by querying quotes already in Redshift and
            assigning random pageview counts drawn from a log-normal distribution
            — a small number of quotes get outsized traffic, most get modest
            traffic, mirroring real-world content popularity curves.

            The RNG is seeded so reruns and retries produce identical numbers.
            To vary by run date instead, replace 42 with int(ds_nodash).
            """
            from io import StringIO
            import random
            import pandas as pd
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            quotes = S3Hook().read_key(bucket_name=bucket, key=s3_key + '/quotes.csv')
            quotes = pd.read_csv(StringIO(quotes)).quote.to_list()

            rng = random.Random(42)
            records = [
                {
                    'quote':     quote,
                    'pageviews': int(rng.lognormvariate(mu=7, sigma=1.5))
                }
                for quote in quotes
            ]

            S3Hook().load_string(
                string_data=pd.DataFrame(records).to_csv(index=False),
                key=s3_key + '/pageviews.csv',
                bucket_name=bucket,
                replace=True,
            )
        
        args = [S3_KEYS['extract'], S3_KEYS['transform'], BUCKET]
        quotes(*args) >> pageviews(BUCKET, S3_KEYS['transform'])
        authors(*args)

    @task_group
    def redshift_init():
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

        schema = SQLExecuteQueryOperator(
            task_id='schema',
            conn_id="redshift_raw",
            sql="CREATE SCHEMA IF NOT EXISTS " + SCHEMA,
            )

        create_quotes = SQLExecuteQueryOperator(
            task_id="quotes",
            conn_id="redshift_raw",
            sql=f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.quotes (
                quote  VARCHAR(MAX),
                author VARCHAR(255),
                tags   SUPER
                )""",
            )

        create_authors = SQLExecuteQueryOperator(
            task_id="authors",
            conn_id="redshift_raw",
            sql=f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.authors (
                author_name        VARCHAR(255),
                author_birthdate   VARCHAR(255),
                author_birthplace  VARCHAR(255),
                author_description VARCHAR(MAX),
                country_name       VARCHAR(255)
                )""",
            )

        create_countries = SQLExecuteQueryOperator(
            task_id='countries',
            conn_id='redshift_raw',
            sql=f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.countries (
                    country_name   VARCHAR(255),
                    official_name  VARCHAR(255),
                    region         VARCHAR(100),
                    subregion      VARCHAR(100),
                    latitude       FLOAT,
                    longitude      FLOAT,
                    population     BIGINT
                )
            """,
        )

        create_pageviews = SQLExecuteQueryOperator(
            task_id='pageviews',
            conn_id='redshift_raw',
            sql=f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.pageviews (
                    quote     VARCHAR(MAX),
                    pageviews INT
                )
            """,
        )

        create_analytics_schema = SQLExecuteQueryOperator(
            task_id='analytics_schema',
            conn_id='redshift_analytics',
            sql=f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        )
        
        create_analytics_schema
        schema >> [create_quotes, create_authors, create_countries, create_pageviews]

    @task_group
    def load():

        from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

        quotes = S3ToRedshiftOperator(
            task_id="quotes",
            table='quotes',
            schema=SCHEMA,
            s3_bucket=BUCKET,
            redshift_conn_id='redshift_raw',
            s3_key=S3_KEYS['transform'] + '/quotes.csv',
            copy_options=["CSV", "IGNOREHEADER 1"],
         )

        authors = S3ToRedshiftOperator(
            task_id="authors",
            table='authors',
            schema=SCHEMA,
            s3_bucket=BUCKET,
            redshift_conn_id='redshift_raw',
            s3_key=S3_KEYS['transform'] + '/authors.csv',
            method='UPSERT',
            upsert_keys=['author_name'],
            copy_options=["CSV", "IGNOREHEADER 1"],
            )
         
        load_countries = S3ToRedshiftOperator(
            task_id='countries',
            schema=SCHEMA,
            table='countries',
            redshift_conn_id='redshift_raw',
            s3_bucket=BUCKET,
            s3_key=S3_KEYS['extract'] + '/countries.csv',
            method='UPSERT',
            upsert_keys=['country_name'],
            copy_options=['CSV', 'IGNOREHEADER 1'],
        )

        load_pageviews = S3ToRedshiftOperator(
            task_id='pageviews',
            schema=SCHEMA,
            table='pageviews',
            redshift_conn_id='redshift_raw',
            s3_bucket=BUCKET,
            s3_key=S3_KEYS['transform'] + '/pageviews.csv',
            method='UPSERT',
            upsert_keys=['quote'],
            copy_options=['CSV', 'IGNOREHEADER 1'],
        )

        quotes, authors, load_countries, load_pageviews
    
    def cleanup(name, BUCKET, unprocessed_key, processed_key):

        @task(task_id=f"cleanup_{name}")
        def _(BUCKET, unprocessed_key, processed_key):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook = S3Hook()
            processed_files = hook.list_keys(
                bucket_name=BUCKET,
                prefix=unprocessed_key
                )
            
            for file in processed_files:
                hook.copy_object(
                    source_bucket_name=BUCKET,
                    dest_bucket_name=BUCKET,
                    source_bucket_key=file,
                    dest_bucket_key=processed_key,
                    )
                
            hook.delete_objects(bucket=BUCKET, keys=processed_files)
        
        return _(BUCKET, unprocessed_key, processed_key)
        
    (
        extract() >> transform() >> redshift_init() >> 
        load() >> cleanup('transform', BUCKET, S3_KEYS['transform'], S3_KEYS['processed'])
        >> cleanup('extract', BUCKET, S3_KEYS['extract'], S3_KEYS['processed'])
    )
    
quotes_scraper()