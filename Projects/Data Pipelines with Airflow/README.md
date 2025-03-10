# Project: Data Pipelines with Airflow

## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring us into the project and expect us to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Overview

This project will make use of the core concepts of Apache Airflow. To complete the project, I created my own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.


![image](https://github.com/user-attachments/assets/cb567afc-a7fb-480a-9dc8-a9032e742b60)

The Project DAG

