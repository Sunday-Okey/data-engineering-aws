# Project: Data Warehouse
Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, we are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

![image](https://github.com/user-attachments/assets/5600a398-45ed-4ddf-ac01-a479daaf12e6)

<img width="511" alt="image" src="https://github.com/user-attachments/assets/1adb6408-3a2e-4d67-9932-fc5b048b10e6" />
<img width="473" alt="image" src="https://github.com/user-attachments/assets/569da16c-7c6c-4c75-9212-a846f27c6f64" />

<img width="496" alt="image" src="https://github.com/user-attachments/assets/6d3e2a51-1196-404f-bea8-9f52ee905fb4" />

<img width="491" alt="image" src="https://github.com/user-attachments/assets/e1ded9fa-0915-43c1-8d20-da54897073e8" />


## Redshift Solution Architecture and ETL

<img width="517" alt="image" src="https://github.com/user-attachments/assets/d9324c54-a9aa-4b95-b947-4a373bc4d89a" />

The project folder includes four files:

- create_table.py contains fact and dimension tables for the star schema in Redshift.
- etl.py we load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
- sql_queries.py is we define our SQL statements, which will be imported into the two other files above.
- README.md provides discussions on the process and decisions for this ETL pipeline.





