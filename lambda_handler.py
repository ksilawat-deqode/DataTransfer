import os
from urllib.parse import urlparse

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor


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

        print(
            f"Performing SELECT query for details with EMR job_id: {emr_job_id}"
        )
        cursor.execute(select_query, (emr_job_id, ))

        data = cursor.fetchone()
        if not data:
            print(f"No rows found for job_id: {emr_job_id}")
            return

        job_id = data.get("id")
        parsed_destination_uri = urlparse(data.get("destination"))
        destination_bucket_name = parsed_destination_uri.netloc
        destination_path = f"{parsed_destination_uri.path}/{job_id}"

        print(f"{job_id}-> Found id: {job_id} corresponding to job_id:"
              f" {emr_job_id}")

        client = boto3.client("datasync")

        bucket_access_role_arn = \
            os.environ.get("CROSS_ACCOUNT_BUCKET_ACCESS_ROLE_ARN")

        source_bucket_arn = os.environ.get("INTERNAL_OUTPUT_BUCKET_ARN")
        source_subdirectory = f"output/{job_id}/"
        print(
            f"{job_id} Creating Source Location with source_bucket_arn:"
            f" {source_bucket_arn}, source_subdirectory: {source_subdirectory}"
        )
        try:
            source_location_arn = client.create_location_s3(
                S3BucketArn=source_bucket_arn,
                Subdirectory=source_subdirectory,
                S3StorageClass="STANDARD",
                S3Config={
                    "BucketAccessRoleArn": bucket_access_role_arn,
                },
            ).get("LocationArn")
            print(f"{job_id}-> Created Source Location ARN")

            destination_bucket_arn = f"arn:aws:s3:::{destination_bucket_name}"
            destination_subdirectory = destination_path
            print(
                f"{job_id} Creating Destination Location with "
                f"destination_bucket_arn: {destination_bucket_arn}, "
                f"destination_subdirectory: {destination_subdirectory}"
            )
            destination_location_arn = client.create_location_s3(
                S3BucketArn=destination_bucket_arn,
                Subdirectory=destination_subdirectory,
                S3StorageClass="STANDARD",
                S3Config={
                    "BucketAccessRoleArn": bucket_access_role_arn,
                },
            ).get("LocationArn")
            print(f"{job_id}-> Created Destination Location ARN")

            task_arn = client.create_task(
                SourceLocationArn=source_location_arn,
                DestinationLocationArn=destination_location_arn,
            ).get("TaskArn")
            print(f"{job_id}-> Created Task ARN")

            task_execution_arn = client.start_task_execution(
                TaskArn=task_arn,
            ).get("TaskExecutionArn")
            print(f"{job_id}-> Created Task Execution ARN")
        except Exception as e:
            print(
                f"{job_id}-> Updating jobstatus: FAILED, failed to initiate "
                f"data transfer"
            )
            failure_update_query = "UPDATE emr_job_details SET jobstatus=%s " \
                                   "WHERE id=%s"

            cursor.execute(
                failure_update_query,
                ("FAILED", job_id)
            )
            connection.commit()
            cursor.close()
            print(f"{job_id}-> Successfully updated jobstatus: FAILED")
            return

        update_query = "UPDATE emr_job_details SET task_execution_arn=%s, " \
                       "data_transfer_state=%s WHERE id=%s"

        print(
            f"{job_id}-> Performing UPDATE query for data transfer details"
            f" update"
        )
        cursor.execute(
            update_query,
            (task_execution_arn, "INITIATED", job_id),
        )
        connection.commit()
        cursor.close()
        print(f"{job_id}-> Successfully updated data transfer details")
