"""
Lambda: YouTube Data API Ingestion (Bronze Layer)
──────────────────────────────────────────────────

Triggered by EventBridge on a schedule.

Pulls trending videos from the YouTube Data API for each configured
region and writes raw JSON responses to the Bronze S3 bucket.

Bronze Bucket:
yt-datapipeline-bharath

Silver Bucket:
yt-datapipeline-bharath-2

This Lambda only writes to the Bronze bucket.

Environment Variables:
YOUTUBE_API_KEY
S3_BUCKET_BRONZE
YOUTUBE_REGIONS
SNS_ALERT_TOPIC_ARN
"""

import json
import os
import logging

from datetime import datetime, timezone

from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

import boto3


# ── Logging ──────────────────────────────────────────────────────────────────

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ── AWS Clients ──────────────────────────────────────────────────────────────

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


# ── Configuration ────────────────────────────────────────────────────────────

API_KEY = os.environ["YOUTUBE_API_KEY"].strip()

# IMPORTANT:
# .strip() removes accidental spaces before/after the bucket name.

BUCKET = os.environ["S3_BUCKET_BRONZE"].strip()

REGIONS = [
    region.strip().lower()
    for region in os.environ.get(
        "YOUTUBE_REGIONS",
        "US,GB,CA,IN"
    ).split(",")
    if region.strip()
]

SNS_TOPIC = os.environ.get(
    "SNS_ALERT_TOPIC_ARN",
    ""
).strip()

API_BASE = "https://www.googleapis.com/youtube/v3"

MAX_RESULTS = 50


# ── Log Configuration ────────────────────────────────────────────────────────

logger.info(
    f"Bronze bucket: '{BUCKET}'"
)

logger.info(
    f"Regions: {REGIONS}"
)

if SNS_TOPIC:
    logger.info(
        "SNS alerting is configured."
    )
else:
    logger.info(
        "SNS alerting is not configured."
    )


# ── Fetch Trending Videos ────────────────────────────────────────────────────

def fetch_trending_videos(region_code: str) -> dict:
    """
    Fetch current trending videos from YouTube Data API v3.
    """

    params = urlencode({
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": region_code,
        "maxResults": MAX_RESULTS,
        "key": API_KEY
    })

    url = f"{API_BASE}/videos?{params}"

    request = Request(
        url,
        headers={
            "Accept": "application/json"
        }
    )

    with urlopen(
        request,
        timeout=30
    ) as response:

        response_data = response.read().decode(
            "utf-8"
        )

    return json.loads(response_data)


# ── Fetch Video Categories ───────────────────────────────────────────────────

def fetch_video_categories(region_code: str) -> dict:
    """
    Fetch YouTube video categories for a region.
    """

    params = urlencode({
        "part": "snippet",
        "regionCode": region_code,
        "key": API_KEY
    })

    url = f"{API_BASE}/videoCategories?{params}"

    request = Request(
        url,
        headers={
            "Accept": "application/json"
        }
    )

    with urlopen(
        request,
        timeout=30
    ) as response:

        response_data = response.read().decode(
            "utf-8"
        )

    return json.loads(response_data)


# ── Write JSON to S3 ─────────────────────────────────────────────────────────

def write_to_s3(
    data: dict,
    bucket: str,
    key: str
) -> dict:
    """
    Write JSON data to S3.
    """

    body = json.dumps(
        data,
        ensure_ascii=False,
        indent=2
    )

    response = s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "ingestion_timestamp": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),
            "source": "youtube_data_api_v3"
        }
    )

    return response


# ── SNS Alert ────────────────────────────────────────────────────────────────

def send_alert(
    subject: str,
    message: str
):
    """
    Send an SNS alert.

    SNS errors are caught so that an SNS configuration problem
    does not cause the Lambda itself to fail.
    """

    if not SNS_TOPIC:

        logger.warning(
            "SNS_ALERT_TOPIC_ARN is not configured. "
            "Skipping alert."
        )

        return


    # SNS Topic ARN should look like:
    #
    # arn:aws:sns:us-east-1:123456789012:my-topic

    if not SNS_TOPIC.startswith("arn:aws:sns:"):

        logger.warning(
            "SNS_ALERT_TOPIC_ARN is not a valid SNS Topic ARN. "
            f"Received: '{SNS_TOPIC}'. "
            "Skipping SNS alert."
        )

        return


    try:

        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject[:100],
            Message=message
        )

        logger.info(
            "SNS alert sent successfully."
        )

    except Exception as error:

        logger.error(
            f"Failed to send SNS alert: {error}",
            exc_info=True
        )


# ── Lambda Handler ──────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Main Lambda handler.

    For every configured region:

    1. Fetch trending videos.
    2. Save raw trending data to Bronze S3.
    3. Fetch video categories.
    4. Save category data to Bronze S3.
    """

    # ── Generate ingestion metadata ──────────────────────────────────

    now = datetime.now(timezone.utc)

    date_partition = now.strftime(
        "%Y-%m-%d"
    )

    hour_partition = now.strftime(
        "%H"
    )

    ingestion_id = now.strftime(
        "%Y%m%d_%H%M%S"
    )


    results = {
        "success": [],
        "failed": []
    }


    logger.info(
        f"Starting ingestion: {ingestion_id}"
    )

    logger.info(
        f"Using Bronze bucket: s3://{BUCKET}"
    )


    # ── Process Each Region ──────────────────────────────────────────

    for region in REGIONS:
        region = region.strip().lower()
        logger.info(
            f"Processing region: {region}"
        )


        # ── Fetch Trending Videos ─────────────────────────────────────

        try:

            trending_data = fetch_trending_videos(
                region
            )

            video_count = len(
                trending_data.get(
                    "items",
                    []
                )
            )


            # Add pipeline metadata

            trending_data[
                "_pipeline_metadata"
            ] = {

                "ingestion_id": ingestion_id,

                "region": region.lower(),

                "ingestion_timestamp": (
                    now.isoformat()
                ),

                "video_count": video_count,

                "source": "youtube_data_api_v3"
            }


            # ── Bronze S3 Key ─────────────────────────────────────────

            s3_key = (
                "youtube/raw_statistics/"
                f"region={region}/"
                f"date={date_partition}/"
                f"hour={hour_partition}/"
                f"{ingestion_id}.json"
            )


            # Write to Bronze S3

            write_to_s3(
                trending_data,
                BUCKET,
                s3_key
            )


            logger.info(
                f"Wrote {video_count} videos to:"
            )

            logger.info(
                f"s3://{BUCKET}/{s3_key}"
            )


        except (HTTPError, URLError) as error:

            logger.error(
                f"YouTube API error for "
                f"{region} trending: {error}"
            )

            results["failed"].append(
                {
                    "region": region,
                    "type": "trending",
                    "error": str(error)
                }
            )

            continue


        except Exception as error:

            logger.error(
                f"Unexpected error for "
                f"{region} trending: {error}",
                exc_info=True
            )

            results["failed"].append(
                {
                    "region": region,
                    "type": "trending",
                    "error": str(error)
                }
            )

            continue


        # ── Fetch Category Reference Data ─────────────────────────────

        try:

            category_data = fetch_video_categories(
                region
            )


            # Add pipeline metadata

            category_data[
                "_pipeline_metadata"
            ] = {

                "ingestion_id": ingestion_id,

                "region": region.lower(),

                "ingestion_timestamp": (
                    now.isoformat()
                ),

                "source": "youtube_data_api_v3"
            }


            # ── Category S3 Key ───────────────────────────────────────

            reference_key = (
                "youtube/reference_data/"
                f"region={region}/"
                f"date={date_partition}/"
                f"{region}_category_id.json"
            )


            # Write category data to Bronze

            write_to_s3(
                category_data,
                BUCKET,
                reference_key
            )


            logger.info(
                f"Wrote categories to:"
            )

            logger.info(
                f"s3://{BUCKET}/{reference_key}"
            )


        except (HTTPError, URLError) as error:

            logger.error(
                f"YouTube API error for "
                f"{region} categories: {error}"
            )

            results["failed"].append(
                {
                    "region": region,
                    "type": "categories",
                    "error": str(error)
                }
            )

            continue


        except Exception as error:

            logger.error(
                f"Unexpected error for "
                f"{region} categories: {error}",
                exc_info=True
            )

            results["failed"].append(
                {
                    "region": region,
                    "type": "categories",
                    "error": str(error)
                }
            )

            continue


        # ── Region Successfully Processed ────────────────────────────

        results["success"].append(
            region
        )

        logger.info(
            f"Region {region} completed successfully."
        )


    # ── Summary ───────────────────────────────────────────────────────

    summary = (
        f"Ingestion {ingestion_id} complete. "
        f"Success: {len(results['success'])}/"
        f"{len(REGIONS)} regions. "
        f"Failed: {len(results['failed'])}."
    )

    logger.info(summary)


    # ── SNS Alert ─────────────────────────────────────────────────────

    if results["failed"]:

        send_alert(
            subject=(
                "[YT Pipeline] "
                f"Ingestion partial failure - "
                f"{ingestion_id}"
            ),
            message=json.dumps(
                results,
                indent=2
            )
        )


    # ── Lambda Response ──────────────────────────────────────────────

    return {
        "statusCode": 200,
        "ingestion_id": ingestion_id,
        "results": results
    }

