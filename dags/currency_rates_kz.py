from __future__ import annotations

import logging
import re
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import pendulum
import requests
import xml.etree.ElementTree as ET
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "currency_rates_kz"
API_URL = "https://www.nationalbank.kz/rss/rates_all.xml"
POSTGRES_CONN_ID = "postgres_default"
TZ = pendulum.timezone("Asia/Almaty")

logger = logging.getLogger(__name__)


def _text(node: ET.Element | None, default: str = "") -> str:
    if node is None or node.text is None:
        return default
    return node.text.strip()


def _parse_rate(raw: str) -> Decimal:
    normalized = raw.replace(" ", "").replace(",", ".")
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise AirflowException(f"Invalid rate value: {raw}") from exc


def _extract_date(xml_root: ET.Element) -> str:
    candidates = [
        _text(xml_root.find(".//date")),
        _text(xml_root.find(".//Date")),
        _text(xml_root.find(".//pubDate")),
    ]

    for value in candidates:
        if not value:
            continue
        clean = value.strip()
        for fmt in ("DD.MM.YYYY", "YYYY-MM-DD"):
            try:
                return pendulum.from_format(clean, fmt, tz=TZ).to_date_string()
            except Exception:
                pass
        try:
            return pendulum.parse(clean, tz=TZ).to_date_string()
        except Exception:
            pass

    return pendulum.now(TZ).to_date_string()


def _extract_numeric_rate(description_text: str) -> str:
    # API may return either "511.23" or text with number inside.
    match = re.search(r"(\d+[.,]?\d*)", description_text.replace(" ", ""))
    if not match:
        raise AirflowException(f"Cannot parse numeric rate from: {description_text}")
    return match.group(1)


def parse_xml_rates(xml_text: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise AirflowException(f"XML parse error: {exc}") from exc

    rate_date = _extract_date(root)
    rows: list[dict[str, Any]] = []

    for item in root.findall(".//item"):
        code = _text(item.find("title"))
        name = _text(item.find("fullname")) or _text(item.find("fullName"))
        description = _text(item.find("description"))

        if not code or not description:
            logger.warning("Skip invalid XML item: code=%s, description=%s", code, description)
            continue

        numeric_rate = _extract_numeric_rate(description)
        rate = _parse_rate(numeric_rate)

        rows.append(
            {
                "currency_code": code,
                "currency_name": name,
                "rate": rate,
                "rate_date": rate_date,
            }
        )

    if not rows:
        raise AirflowException("No currency rates extracted from XML.")

    return rows


@dag(
    dag_id=DAG_ID,
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=TZ),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["currency", "kz", "nationalbank"],
)
def currency_rates_kz() -> None:
    @task
    def fetch_xml() -> str:
        logger.info("Requesting XML from %s", API_URL)
        try:
            response = requests.get(API_URL, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.exception("National Bank KZ API is unavailable.")
            raise AirflowException(f"Failed to fetch API data: {exc}") from exc
        logger.info("XML fetched, response size=%d bytes", len(response.text))
        return response.text

    @task
    def parse_xml(xml_text: str) -> list[dict[str, Any]]:
        logger.info("Parsing currency XML data")
        rows = parse_xml_rates(xml_text)
        logger.info("Parsed %d rows", len(rows))
        return rows

    @task
    def load_postgres(rows: list[dict[str, Any]]) -> str:
        if not rows:
            logger.info("No rows to load")
            return "NO_DATA"

        rate_date = rows[0]["rate_date"]
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS currency_rates_kz (
            rate_date DATE NOT NULL,
            currency_code VARCHAR(10) NOT NULL,
            currency_name TEXT,
            rate NUMERIC(18,6) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (rate_date, currency_code)
        );
        """
        exists_sql = "SELECT 1 FROM currency_rates_kz WHERE rate_date = %s LIMIT 1;"
        insert_sql = """
        INSERT INTO currency_rates_kz (rate_date, currency_code, currency_name, rate)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (rate_date, currency_code) DO NOTHING;
        """

        conn = hook.get_conn()
        conn.autocommit = False

        try:
            with conn.cursor() as cursor:
                logger.info("Ensuring target table exists")
                cursor.execute(create_table_sql)

                logger.info("Checking if data for %s already exists", rate_date)
                cursor.execute(exists_sql, (rate_date,))
                if cursor.fetchone() is not None:
                    logger.info("Data for %s already loaded, skipping", rate_date)
                    conn.commit()
                    return "ALREADY_EXISTS"

                payload = [
                    (r["rate_date"], r["currency_code"], r["currency_name"], r["rate"])
                    for r in rows
                ]
                cursor.executemany(insert_sql, payload)
                conn.commit()
                logger.info("Inserted %d currency rows for %s", len(payload), rate_date)
                return f"INSERTED_{len(payload)}"
        except Exception as exc:
            conn.rollback()
            logger.exception("PostgreSQL load failed")
            raise AirflowException(f"PostgreSQL load error: {exc}") from exc
        finally:
            conn.close()

    load_postgres(parse_xml(fetch_xml()))


currency_rates_kz()
