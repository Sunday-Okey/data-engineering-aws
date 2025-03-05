# Project: STEDI Human Balance Analytics

## Introduction

In this project we'll act as a data engineer for the STEDI team to build a Data Lakehouse solution for sensor data that trains a machine learning model.
We will use Apache Spark and AWS Glue to process data from multiple sources, transform it, categorize the data, and curate it to be queried in the future for multiple purposes.

## Project Details

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The `Step Trainer` is just a motion sensor that records the distance of the object detected. The app uses a mobile phone `accelerometer` to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. ***Privacy will be a primary consideration in deciding what data can be used.***

Some of the early adopters have agreed to share their data for research purposes. **Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.**

## The Device
There are sensors on the device that collect data to train a machine learning algorithm to detect steps. It also has a companion mobile app that collects customer data and interacts with the device sensors. The step trainer is just a motion sensor that records the distance of the object detected.

![](https://raw.githubusercontent.com/Sunday-Okey/data-engineering-aws/refs/heads/main/Projects/STEDI%20Human%20Balance%20Analytics/gif.gif)



## Tools and Environment

We'll build data pipelines that utilize the following tools to store, filter, process, and transform the data from STEDI users for data analytics and machine learning applications.

- Apache Spark
- Simple Storage Service (Amazon S3)
- AWS Glue, Glue Studio
- AWS Glue Catalog, Glue Tables
- Athena
- Python

## Project Data

STEDI has three JSON data sources to use from the Step Trainer. Check out the JSON data in the following folders [here](https://github.com/Sunday-Okey/data-engineering-aws/tree/main/Projects/STEDI%20Human%20Balance%20Analytics/project%20data):

- customer
- step_trainer
- accelerometer

### 1. Customer Records
This is the data from fulfillment and the STEDI website.

[Data Download URL](https://github.com/Sunday-Okey/data-engineering-aws/tree/main/Projects/STEDI%20Human%20Balance%20Analytics/project%20data/customer/landing)

*AWS S3 Bucket URI - s3://cd0030bucket/customers/*

contains the following fields:

- serialnumber
- sharewithpublicasofdate
- birthday
- registrationdate
- sharewithresearchasofdate
- customername
- email
- lastupdatedate
- phone
- sharewithfriendsasofdate

### 2. Step Trainer Records
This is the data from the motion sensor.

[Data Download URL](https://github.com/Sunday-Okey/data-engineering-aws/tree/main/Projects/STEDI%20Human%20Balance%20Analytics/project%20data/step_trainer/landing)

*AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/*

contains the following fields:

- sensorReadingTime
- serialNumber
- distanceFromObject


### 3. Accelerometer Records
This is the data from the mobile app.

[Data Download URL](https://github.com/Sunday-Okey/data-engineering-aws/tree/main/Projects/STEDI%20Human%20Balance%20Analytics/project%20data/accelerometer/landing)

*AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/*

contains the following fields:

- timeStamp
- user
- x
- y
- z

## Project Instructions

Using AWS Glue, AWS S3, Python, and Spark, create or generate Python scripts to build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists.

Refer to the flowchart below to better understand the workflow.

![image](https://github.com/user-attachments/assets/3807e194-1530-40d7-a144-581d8814c741)

A flowchart displaying the workflow.

## Requirements

To simulate the data coming from the various sources, we will need to create our own S3 directories for customer_landing, step_trainer_landing, and accelerometer_landing zones, and copy the data there as a starting point.

We have decided we want to get a feel for the data we are dealing with in a semi-structured format, so we decide to create **three Glue tables** for the three landing zones. `customer_landing.sql`, `accelerometer_landing.sql`, and `step_trainer_landing.sql` scripts.

The Data Science team has done some preliminary data analysis and determined that the **Accelerometer Records** each match one of the **Customer Records**. They would like us to create 2 AWS Glue Jobs that do the following:

1. Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called **customer_trusted**.

2. Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called **accelerometer_trusted**.

3. We will need to verify our Glue job is successful and only contains Customer Records from people who agreed to share their data. We will query our Glue customer_trusted table with Athena to confirm this.

Data Scientists have discovered a data quality issue with the Customer Data. The serial number should be a unique identifier for the STEDI Step Trainer they purchased. However, there was a defect in the fulfillment website, and it used the same 30 serial numbers over and over again for millions of customers! Most customers have not received their Step Trainers yet, but those who have, are submitting Step Trainer data over the IoT network (Landing Zone). The data from the Step Trainer Records has the correct serial numbers.

The problem is that because of this serial number bug in the fulfillment data (Landing Zone), we don’t know which customer the Step Trainer Records data belongs to.

The Data Science team would like you to write a Glue job that does the following:

1. Sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called **customers_curated**.

Finally, we need to create two Glue Studio jobs that do the following tasks:

2. Read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called **step_trainer_trusted** that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).
3. Create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called **machine_learning_curated.**

Refer to the relationship diagram below to understand the desired state.


![image](https://github.com/user-attachments/assets/d523e090-471c-406a-a4d6-02b93e8ae5a2)

A diagram displaying the relationship between entities.

