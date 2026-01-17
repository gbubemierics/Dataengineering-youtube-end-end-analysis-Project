Data Engineering YouTube End to End Analysis Project by Gbubemi Erics

Overview
This project is about securely managing, streamlining, and analysing YouTube video data. The data includes both structured and semi-structured formats.
The focus is on video categories and trending metrics, and the goal is to make sense of how videos perform across regions and over time.

Project Goals
The first goal is data ingestion. We need a way to bring data in from different sources in a reliable way.
Next is the ETL system. The data arrives in raw format, so we transform it into a clean and usable structure.
Then comes the data lake. Since the data comes from multiple sources, we need one central place to store everything.
Scalability is also key. As the data grows, the system must scale without breaking or slowing down.
We also rely on the cloud. Processing large volumes of data on a local machine is not practical, so AWS is used instead.
Finally, reporting. We build dashboards that help answer the questions we started with and turn data into insights.

Services we will be using
Amazon S3 is used as object storage. It gives us massive scalability, strong security, high availability, and good performance.
AWS IAM handles identity and access management. It controls who can access which AWS services and resources, and keeps things secure.
Amazon QuickSight is used for reporting. It is a serverless BI service that lets us build dashboards and visualise insights at scale.
AWS Glue is the data integration layer. It helps us discover, prepare, and transform data for analytics and machine learning.
AWS Lambda is used for compute. It lets us run code without managing servers, which keeps the pipeline simple and flexible.
AWS Athena is used for querying. It allows us to run SQL queries directly on data stored in S3 without loading it into a database.

Dataset Used
The dataset comes from Kaggle and contains statistics on daily trending YouTube videos over several months. Each day includes up to 200 trending videos for many different locations.
Each region has its own file. The data includes video title, channel title, publication time, tags, views, likes, dislikes, description, and comment count.
There is also a category_id field. This field varies by region and is mapped using a JSON file linked to each area.

The dataset is available here:
https://www.kaggle.com/datasets/datasnaek/youtube-new

Architectural Diagram





Architecture Overview

This diagram shows an AWS-based data platform built around a data lake.
Data flows from source systems into Amazon S3, is processed in stages, catalogued, and then queried or visualised using analytics tools. 
Security, orchestration, and monitoring support the full pipeline.

Step-by-step Data Flow
1. Source Systems

These are the original producers of data.
They can be applications, APIs, logs, or external datasets such as Kaggle files.
Data can be structured or semi-structured.

2. Ingestion into S3 (Landing Area)

Data is ingested in bulk into Amazon S3.
The landing area stores raw data exactly as received, without any transformation.

Why S3
It is highly scalable, durable, cost-effective, and separates storage from compute.

3. Data Lake Layers (S3 Zones)

The data lake is organised into logical layers:

Landing Area
Raw data for traceability and reprocessing.

Cleansed / Enriched
Data is cleaned, standardised, and lightly enriched.

Deduped
Duplicate records are removed to ensure consistency.

Conformed
Analytics-ready data with stable schemas used by downstream tools.

This layered approach improves data quality and makes pipelines easier to manage.

4. Data Processing

AWS Glue
Used for batch ETL jobs such as cleaning, transforming, and converting data into Parquet format.

AWS Lambda
Used for lightweight or event-driven processing, often triggered by S3 events.

Glue handles heavy processing.
Lambda handles small, fast tasks.

5. Workflow Orchestration

AWS Step Functions control the execution order of Glue jobs and Lambda functions.
They manage retries, failures, and dependencies across the pipeline.

6. Data Catalog and Metadata

AWS Glue Data Catalog stores table definitions, schemas, and S3 locations.
This allows data in S3 to be discovered and queried like database tables.

7. Security and Access Control

AWS IAM manages permissions across all services.
It ensures only authorised users and services can access data and resources.

8. Analytical Access

AWS Athena
Enables SQL queries directly on data stored in S3.

Amazon Redshift
Optional data warehouse for higher performance or complex analytics workloads.

9. Reporting and Analytics

BI and analytics tools such as QuickSight, Power BI, Qlik, and notebooks connect to Athena or Redshift to build dashboards and reports.

10. Monitoring and Alerting

Amazon CloudWatch monitors logs, metrics, and pipeline health.
It helps detect failures and performance issues.

Summary

Data flows from source systems into S3, moves through structured data lake layers, is processed and catalogued, and then queried or visualised.
The architecture is scalable, secure, and designed for analytics in the cloud.
