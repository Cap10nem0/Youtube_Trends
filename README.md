# Youtube Trends on AWS 

Overview
--------

This project aims to build a system for securely managing, processing, and analyzing YouTube video data based on categories and popularity trends. 

Objectives
--------
The main goals are:

1. Ingest data from multiple sources into a centralized data repository

2. Transform the raw data into a structured format suitable for analysis 

3. Store the processed video data in a scalable data lake on the cloud 

4. Leverage cloud services to scale data processing as the dataset grows

5. Build analytics dashboards and reports to derive insights from the data

AWS TechStack
---------

To achieve these goals, we plan to utilize the following AWS services:

1. Amazon S3 for scalable object storage

2. AWS IAM for secure identity and resource access management

3. Amazon QuickSight for building business intelligence dashboards 

4. AWS Glue for scalable data integration and ETL

5. AWS Lambda for serverless computing 

6. Amazon Athena for querying S3 data directly

The dataset consists of daily trending YouTube video statistics scraped from the YouTube API. The metrics include view counts, likes, comments and other statistics on a per video and channel basis. As the data is generated separately for different regions, it provides an opportunity to compare geographic trends.

By leveraging the managed AWS services for data engineering and analytics, the goal is to build a secure, scalable and cost-efficient pipeline to gain valuable insights from YouTube trend data.
