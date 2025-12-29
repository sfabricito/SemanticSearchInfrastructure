import logging
import os
import time
import uuid
from typing import Iterator, Tuple

import requests
from pyspark.sql import SparkSession
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("spark-qdrant-ingest")

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "spark-qdrant-ingest")
INPUT_PATH = os.getenv("INPUT_PATH", "")
INPUT_FORMAT = os.getenv("INPUT_FORMAT", "parquet").lower()
ID_COLUMN = os.getenv("ID_COLUMN", "id")
TEXT_COLUMN = os.getenv("TEXT_COLUMN", "text")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "64"))
VECTOR_SIZE = int(os.getenv("VECTOR_SIZE", "768"))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "embeddings")
QDRANT_DISTANCE = os.getenv("QDRANT_DISTANCE", "cosine")
QDRANT_HOST = os.getenv("QDRANT_HOST", "databases-qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_URL = os.getenv("QDRANT_URL", "")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
EMBEDDING_API_URL = os.getenv("EMBEDDING_API_URL", "http://embedding-api:8000")
EMBEDDING_ENDPOINT = EMBEDDING_API_URL.rstrip("/") + "/encode"
RUN_INTERVAL_SECONDS = int(os.getenv("RUN_INTERVAL_SECONDS", "600"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))


def resolve_distance(value: str) -> qmodels.Distance:
    normalized = (value or "").strip().lower()
    if normalized in {"dot", "dotproduct", "dot_product"}:
        return qmodels.Distance.DOT
    if normalized in {"l2", "euclid", "euclidean"}:
        return qmodels.Distance.EUCLID
    return qmodels.Distance.COSINE


def qdrant_client_kwargs() -> dict:
    if QDRANT_URL:
        return {"url": QDRANT_URL, "api_key": QDRANT_API_KEY or None}
    return {"host": QDRANT_HOST, "port": QDRANT_PORT, "api_key": QDRANT_API_KEY or None}


def ensure_collection(client: QdrantClient) -> None:
    try:
        collections = client.get_collections().collections or []
        if any(item.name == QDRANT_COLLECTION for item in collections):
            return
    except Exception as exc:
        logger.warning("Unable to list Qdrant collections: %s", exc)
    logger.info(
        "Creating collection %s (size=%s, distance=%s)",
        QDRANT_COLLECTION,
        VECTOR_SIZE,
        resolve_distance(QDRANT_DISTANCE).value,
    )
    client.create_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config=qmodels.VectorParams(size=VECTOR_SIZE, distance=resolve_distance(QDRANT_DISTANCE)),
    )


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(SPARK_APP_NAME)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_dataframe(spark: SparkSession):
    if not INPUT_PATH:
        raise ValueError("INPUT_PATH environment variable is required.")

    fmt = INPUT_FORMAT.lower()
    if fmt == "csv":
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
    elif fmt == "parquet":
        df = spark.read.parquet(INPUT_PATH)
    else:
        raise ValueError(f"Unsupported INPUT_FORMAT '{INPUT_FORMAT}'. Use 'csv' or 'parquet'.")

    missing = [col for col in (ID_COLUMN, TEXT_COLUMN) if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in dataset: {missing}")
    return df


def fetch_embedding(session: requests.Session, text: str):
    try:
        response = session.post(EMBEDDING_ENDPOINT, json={"text": text}, timeout=REQUEST_TIMEOUT)
        if response.ok:
            body = response.json()
            embedding = body.get("embedding")
            if embedding:
                return embedding
            logger.error("Embedding API response missing 'embedding' field.")
            return None
        logger.error("Embedding API returned status %s: %s", response.status_code, response.text)
    except Exception as exc:
        logger.error("Error calling embedding API: %s", exc)
    return None


def flush_points(client: QdrantClient, points) -> None:
    if not points:
        return
    client.upsert(collection_name=QDRANT_COLLECTION, wait=True, points=points)


def process_partition(rows: Iterator) -> Iterator[Tuple[int, int]]:
    session = requests.Session()
    client = QdrantClient(**qdrant_client_kwargs())
    processed = 0
    failed = 0
    buffer = []

    for row in rows:
        payload = row.asDict(recursive=True)
        text = payload.get(TEXT_COLUMN)
        if text is None:
            failed += 1
            continue
        text_value = str(text).strip()
        if not text_value:
            failed += 1
            continue

        record_id = payload.get(ID_COLUMN) or str(uuid.uuid4())
        embedding = fetch_embedding(session, text_value)
        if embedding is None:
            failed += 1
            continue

        payload_clean = {k: v for k, v in payload.items() if v is not None}
        buffer.append(qmodels.PointStruct(id=str(record_id), vector=embedding, payload=payload_clean))

        if len(buffer) >= BATCH_SIZE:
            try:
                flush_points(client, buffer)
                processed += len(buffer)
            except Exception as exc:
                logger.error("Error writing batch to Qdrant: %s", exc)
                failed += len(buffer)
            finally:
                buffer.clear()

    if buffer:
        try:
            flush_points(client, buffer)
            processed += len(buffer)
        except Exception as exc:
            logger.error("Error writing final batch to Qdrant: %s", exc)
            failed += len(buffer)

    return iter([(processed, failed)])


def run_ingest() -> Tuple[int, int]:
    start = time.time()
    spark = build_spark_session()
    df = load_dataframe(spark)
    logger.info("Loaded dataset from %s with schema: %s", INPUT_PATH, df.schema.simpleString())

    client = QdrantClient(**qdrant_client_kwargs())
    ensure_collection(client)

    stats = df.rdd.mapPartitions(process_partition).collect()
    spark.stop()

    processed = sum(item[0] for item in stats)
    failed = sum(item[1] for item in stats)

    duration = time.time() - start
    logger.info("Ingest completed. processed=%s failed=%s duration_s=%.2f", processed, failed, duration)
    return processed, failed


def main():
    while True:
        try:
            processed, failed = run_ingest()
            logger.info("Ingest run finished: processed=%s failed=%s", processed, failed)
        except Exception as exc:
            logger.exception("Ingest run failed: %s", exc)

        if RUN_INTERVAL_SECONDS <= 0:
            logger.info("RUN_INTERVAL_SECONDS<=0, keeping process alive.")
            while True:
                time.sleep(3600)

        logger.info("Sleeping %s seconds before next ingest run", RUN_INTERVAL_SECONDS)
        time.sleep(RUN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
