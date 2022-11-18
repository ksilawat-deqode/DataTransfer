import json
import os
from urllib.parse import urlparse

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

import logging
from datetime import datetime
from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(
            log_record,
            record,
            message_dict,
        )
        if not log_record.get('timestamp'):
            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['time'] = now
        if log_record.get('level'):
            log_record['level'] = log_record['level'].lower()
        else:
            log_record['level'] = record.levelname.lower()
        log_record['source'] = "DataTransfer"


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = CustomJsonFormatter('%(level)s %(msg)s %(time)s %(source)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.propagate = False


def lambda_handler(event, context):
    if event["source"] == "aws.emr-serverless":
        emr_job_id = event["detail"]["jobRunId"]

        connection = psycopg2.connect(
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT"),
            database=os.environ.get("DB_NAME"),
        )

        cursor = connection.cursor(cursor_factory=RealDictCursor)
        select_query = "SELECT * FROM emr_job_details WHERE jobid=%s"

        logger.info(
            f"Performing SELECT query for details with EMR job_id: {emr_job_id}",
        )
        cursor.execute(select_query, (emr_job_id,))

        data = cursor.fetchone()
        if not data:
            logger.info(f"No rows found for job_id: {emr_job_id}")
            return

        job_id = data.get("id")
        cross_bucket_region = data.get("cross_bucket_region")
        parsed_destination_uri = urlparse(data.get("destination"))
        destination_bucket_name = parsed_destination_uri.netloc
        destination_path = f"{parsed_destination_uri.path}/{job_id}"

        extra_log_info = {
            "clientIp": data.get("client_ip"),
            "destinationBucket": data.get("destination"),
            "queryId": data.get("id"),
            "jti": data.get("jti"),
            "query": data.get("query"),
            "region": data.get("cross_bucket_region"),
            "skyflowRequestId": data.get("requestid"),
        }

        logger.info(
            f"Found id: {job_id} corresponding to job_id:"
            f" {emr_job_id}",
            extra=extra_log_info,
        )

        secrets_client = boto3.client("secretsmanager")
        polling_function_arn = json.loads(
            secrets_client.get_secret_value(
                SecretId=os.environ.get("SECRETS"),
            ).get("SecretString")
        ).get("POLLING_FUNCTION_ARN")

        data_sync_client = boto3.client("datasync")
        external_data_sync_client = boto3.client(
            "datasync",
            region_name=cross_bucket_region,
        )

        try:
            bucket_access_role_arn = \
                os.environ.get("CROSS_ACCOUNT_BUCKET_ACCESS_ROLE_ARN")

            source_bucket_arn = os.environ.get("INTERNAL_OUTPUT_BUCKET_ARN")
            source_subdirectory = f"output/{job_id}/"

            logger.info(
                f"Creating Source Location with source_bucket_arn:"
                f" {source_bucket_arn}, source_subdirectory:"
                f" {source_subdirectory}",
                extra=extra_log_info,
            )

            source_location_arn = data_sync_client.create_location_s3(
                S3BucketArn=source_bucket_arn,
                Subdirectory=source_subdirectory,
                S3StorageClass="STANDARD",
                S3Config={
                    "BucketAccessRoleArn": bucket_access_role_arn,
                },
            ).get("LocationArn")

            logger.info(
                f"Created Source Location ARN: {source_location_arn}",
                extra=extra_log_info,
            )

            destination_bucket_arn = f"arn:aws:s3:::{destination_bucket_name}"
            destination_subdirectory = destination_path

            logger.info(
                f"Creating Destination Location with "
                f"destination_bucket_arn: {destination_bucket_arn}, "
                f"destination_subdirectory: {destination_subdirectory}",
                extra=extra_log_info,
            )

            destination_location_arn = \
                external_data_sync_client.create_location_s3(
                    S3BucketArn=destination_bucket_arn,
                    Subdirectory=destination_subdirectory,
                    S3StorageClass="STANDARD",
                    S3Config={
                        "BucketAccessRoleArn": bucket_access_role_arn,
                    },
                ).get("LocationArn")

            logger.info(
                f"Created Destination Location ARN: "
                f"{destination_location_arn}",
                extra=extra_log_info,
            )

            task_arn = external_data_sync_client.create_task(
                SourceLocationArn=source_location_arn,
                DestinationLocationArn=destination_location_arn,
            ).get("TaskArn")

            logger.info(
                f"Created Task ARN: {task_arn}",
                extra=extra_log_info,
            )

            task_execution_arn = external_data_sync_client.start_task_execution(
                TaskArn=task_arn,
            ).get("TaskExecutionArn")

            logger.info(
                f"Created Task Execution ARN: {task_execution_arn}",
                extra=extra_log_info,
            )

            payload = {
                "id": job_id,
                "region_name": cross_bucket_region,
                "task_execution_arn": task_execution_arn,
            }
            payload_bytes = json.dumps(payload).encode('utf-8')
            boto3.client("lambda").invoke_async(
                FunctionName=polling_function_arn,
                InvokeArgs=payload_bytes,
            )
        except Exception as e:
            logger.error(
                f"Data Transfer initialization failed with error:{e}",
                extra=extra_log_info,
            )

            logger.info(
                f"Updating jobstatus: FAILED, failed to initiate data transfer",
                extra=extra_log_info,
            )
            failure_update_query = "UPDATE emr_job_details SET jobstatus=%s " \
                                   "WHERE id=%s"

            cursor.execute(
                failure_update_query,
                ("FAILED", job_id)
            )
            connection.commit()
            cursor.close()

            logger.info(
                f"Successfully updated jobstatus: FAILED",
                extra=extra_log_info,
            )

        update_query = "UPDATE emr_job_details SET task_execution_arn=%s, " \
                       "data_transfer_state=%s WHERE id=%s"

        logger.info(
            f"Performing UPDATE query for data transfer details update",
            extra=extra_log_info,
        )

        cursor.execute(
            update_query,
            (task_execution_arn, "INITIATED", job_id),
        )
        connection.commit()
        cursor.close()

        logger.info(
            f"Successfully updated data transfer details",
            extra=extra_log_info,
        )
