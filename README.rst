======
gnuper
======

**gnuper** is an open source python package for the preprocessing of mobile phone metadata whilst keeping privacy in mind. It is being developed by the spin-off project `knuper <https://www.knuper.com>`_ of the Freie Universität Berlin.
The package creates features on antenna level off of mobile phone metadata like simple incoming to outgoing ratios of calls or sms as well as entropy and isolation indicators of antennas. Additionally one can include antenna averages of the bandicoot individual indicators developed by the MIT. Further information on this toolkit can be found `here <http://bandicoot.mit.edu/>`_.

Prerequisites
-------------
- Python3
- Apache Spark

Structural Overview
-------------------
The following tree diagram shows the entire process from raw mobile phone meta data to final features on antenna level. In order to keep the process fast and the intermediate tables as small as possible, several mid-aggregations are taking place. We call these intermediate steps levels.
Each level represents another aggregation or transformation step. The color coding signifies the privacy risk. Orange is on user level, blue on antenna (single tower GPS coordinate) level.

.. figure:: docs/Raw_Data_Levels.png
   :alt: Data Levels
   :scale: 60 %

- **Level 0**
This level simply unifies and preprocesses (kick out unnecessary information) the raw format. Also obvious machines are being filtered as well as the files restructured into chunks of users.

- **Level 1**
For each user a home antenna is being estimated and then interactions aggregated on user level per day and hour. Also bandicoot variables might be calculated if the flag is set.

- **Level 2**
Antenna level. Users are being allocated to their home antenna and then aggregated in three different categories. First, for each week between weekend or holidays and working days. Second, for every hour of the day irrespective of the date. And third, for all interactions between each pair of antennas.

- **Level 3**
Features are being created for several categories:
*alltime* (ratios & scaled) - over the whole time period
*variance* (variances over weeks) - to catch regional variation
*daily* (outgoing vs. incoming, week parts) - differences between working days and weekends/holidays
*hourly* (workday, peaks) - differences between the usual working day and several peaks
*interactions* (distance, isolation, entropy) - network and geolocation related
*active users* (per home antenna) - number of users allocated to an antenna as their home antenna
For the exact formulas, have a look at the specific queries.

- **Level 4**
Unite the feature categories with the bandicoot features (aggregated on antenna level) to one final dataset, ready for analysis.

Sample Data
-----------
In order to create a small synthetic CDR sample data set (<20MB) run the ipython
script `cdr_mockup.ipynb <cdr_mockup.ipynb>`_ e.g. with Jupyter.

Spark
-----
The package has been built using SPARK version 2.2.1.

Instructions on how to install Spark in Ubuntu (tested with 18.04):

1. install java environment ``sudo apt-get install default-jre``
2. install scala ``sudo apt-get install scala``
3. download spark
   ``wget http://archive.apache.org/dist/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz``
4. unzip ``sudo tar -zxvf spark-2.2.1-bin-hadoop2.7.tgz``
5. and remove ``rm spark-2.2.1-bin-hadoop2.7.tgz``
