# DWH-AWS-RedShift

This folder contains the necessary program files to create Sparkify database and
corresponding etl pipeline to load the database on AWS Redshift.

## Contents

1. sql_queries.py
    * Contains all the sql queries against the database used in etl.py
2. create_tables.py
    * Drops and recreates tables.
3. etl.py
    * reads and processes files from song_data and log_data and
      loads them into the tables.
4. etl.ipynb
    * Jupyter notebook file used to build the etl process step by step.
5. README.md
6. dwh.cfg
    * Configuration file used to connect to the Redshift Cluster.
7. redshift.cfg
    * Configuration file used to create Redshift Cluster.
8. environment.yaml
    * conda environment file to import the python environment used by the project.


## Installation

1. Use the following command to clone the project repository.

    ```
    git clone https://github.com/shilpamadini/DWH-AWS-RedShift.git
    ```

2. Create the environment using below command

    ```
    conda env create -f environment.yaml
    ```

3. Activate the conda environment

    ```
    source activate dand_py3
    ```

4. Follow the instructions in etl.ipynb to create your own Redshift cluster
 using Infrastructure as code (Iac).

5. Navigate to the project directory and run the following to create tables
     ```
     python create_tables.py
     ```
6. Run the following to load the fact and dimension tables
         ```
         python etl.py
         ```
7. Test the etl load at any time by using etl.ipynb. Run the following command to launch jupyter notebook.
    ```
    jupyter notebook
    ```


## Functionality

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project aims to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights on the data.

Since the analytics team is interested in knowing what songs the users are listening to and probably interested in performing ranking ,aggregation to determine which song is played the most, what is most popular song, which artist released most popular songs. Analytics may also be interested in looking at the trends over a period of time.

In order to support the required analytics a star schema design is implemented to design the data warehouse. Songplay table is the fact table and song, user,artist and time are dimension tables. Database integrity is maintained by using Primary key and foreign key constraints in the table definitions.

Here is the ER diagram explaining the schema design.

![Screen Shot 2019-06-10 at 5 40 47 PM](https://user-images.githubusercontent.com/16230330/59241519-d844a280-8bbc-11e9-894e-0dca550dc6ca.png)

 Users and time tables are loaded with distribution All strategy as they contain small datasets and used in queries that aggregate data over a period of time. Time table also loaded with start_time as sort key. Having these two tables in all the nodes will reduce shuffling and query performance will increase.Artists and songs table are distributed by artist_id. Distributing the  songplay table ,song and artist_id tables with artist_id will also reduce data shuffling between the nodes as records from the three tables for the same artist_id will reside in the same node. 
