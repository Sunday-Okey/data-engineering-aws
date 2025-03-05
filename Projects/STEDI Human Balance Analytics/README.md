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


