
# Spark

## Dockerfile

Here is a quick walk through of the Dockerfile which is used to build the Spark container **locally** by compose file.

It starts with the base image of **Python 3.9.1** and some essential tools such as **curl**, **wget**, and **nano** are installed. Then, it installs the required **Python packages** using **pip**, including **pandas**, **pyarrow**, **numpy**, **pyspark**, **findspark**, and **nltk**. Also it downloads the necessary **nltk data** using the **nltk.downloader** command.
Next, Dockerfile downloads and installs the **Java Development Kit (JDK) version 17** from Oracle, and sets the `JAVA_HOME` and `PATH` environment variables accordingly.
Then it downloads and installs **Apache Spark version 3.3.2** for **Hadoop 3**, and sets the `SPARK_HOME`, `PATH`, and `PYTHONPATH`environment variables to point to the Spark installation directories.
Finally, it creates a directory for the Spark application, copy the **app.py** file (which has main scripts of the application) to it, and set the entry point for our Docker container as **spark-submit** command, passing the required **Spark packages** for **Kafka** and **Elasticsearch**, and the name of our app.py file as the command.
This Dockerfile allows to create a containerized Spark application with all the necessary dependencies and packages and also it sets up the Spark environment as **locally**.

## Source Code
Spark code performs data preprocessing and sentiment analysis on incoming messages from a Kafka topic. Here is a brief of the different parts:

- ### Reading real time messages from Kafka
It reads data from a Kafka topic named "**twitch-message**" using **Spark's Structured Streaming API**. The code reads data by using the **"readStream"** method of the DataFrameReader API in Spark. It specifies the Kafka broker's address with the option("kafka.bootstrap.servers", "broker:29092") option, where "broker" is the hostname of the Kafka broker which is a container that is running in the same network as the Spark.

* ### DataFrame

After reading data from the Kafka topic using readStream, the code creates a DataFrame named df by selecting the value column of the incoming data and casting it to a String.
Then, the code defines a schema for the incoming data with the **StructType API** from PySpark. The schema includes **five fields**: **streamer**, **order**, **time**, **username**, and **message**.

Next, the code applies some transformations on the DataFrame to perform data preprocessing and sentiment analysis.

Finally, the code creates a new DataFrame by adding a new column to indicate sentiment value(Neutral/Positive/Negative). This new DataFrame is then written to an **Elasticsearch index named "twitch"** using the **writeStream** method of the DataFrameWriter API.

* ### User Defined Functions (UDFs)
`User-Defined Functions (UDFs) are user-programmable routines that act on one row.`

#### Data preprocessing and Sentiment Analysis with VADER

The code defines several user-defined functions (UDFs) for **data preprocessing**, such as **removing whitespace**, **converting text to lowercase**, **removing numbers and punctuation**, and **removing stop words**. These functions are then used to preprocess the incoming messages before sentiment analysis.
The code defines another UDF for sentiment analysis using the **VADER sentiment analysis** tool. It classifies each message as Positive, Negative or Neutral based on the sentiment score returned by the tool.

* ### Elasticsearch

In the code, the data is sent to Elasticsearch for indexing and visualization. This is done using the **"writeStream"** function of Spark.
Once the stream is started, data will be **continuously** written to Elasticsearch **in real-time**. This data will be used for as monitoring, visualization, and analysis.
