import os

import boto3
import psycopg2


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

        cursor = connection.cursor()
        query = "SELECT * FROM emr_job_details WHERE jobid=%(emr_job_id)s"
        cursor.execute(query, {"emr_job_id": emr_job_id})

        data = cursor.fetchone()
        if not data:
            print(f"No rows found for job_id: {emr_job_id}")
            return

        job_id = data[0]
        print(f"data-> {data}")
        print(f"{job_id}-> Found id: {job_id} corresponding to job_id:"
              f" {emr_job_id}")

        client = boto3.client("datasync")
        source_location_arn = client.create_location_s3(
            S3BucketArn=os.environ.get("INTERNAL_OUTPUT_BUCKET_ARN"),
            Subdirectory=f"output/{job_id}/",
            S3StorageClass="STANDARD",
            S3Config={
                "BucketAccessRoleArn":
                    os.environ.get("CROSS_ACCOUNT_BUCKET_ACCESS_ROLE_ARN"),
            },
        ).get("LocationArn")
        print(f"{job_id}-> Created Source Location ARN")

        destination_location_arn = client.create_location_s3(
            S3BucketArn=os.environ.get("EXTERNAL_OUTPUT_BUCKET_ARN"),
            Subdirectory=f"{job_id}/",
            S3StorageClass="STANDARD",
            S3Config={
                "BucketAccessRoleArn":
                    os.environ.get("CROSS_ACCOUNT_BUCKET_ACCESS_ROLE_ARN"),
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
        )
        print(f"{job_id}-> Created Task Execution ARN")
