# Amazon Data Pipeline  

Welcome to the Amazon Data Pipeline project! This is a data engineering pipeline that processes product reviews, cleans the data, stores it in PostgreSQL, and logs events using Kafka.  

To get started, first, make sure you have the required dependencies installed. The project uses Python with libraries like Pandas, PySpark, Kafka, and PostgreSQL. You’ll also need to set up a PostgreSQL database and a Kafka broker.  

Clone this repository and install the required dependencies using:  
`pip install -r requirements.txt`  

You'll need to configure the `config.ini` file with your PostgreSQL database details, Kafka settings, and input file path. Once that's set up, you can run the pipeline using:  
`python pipeline.py`  

This script reads product review data from a CSV file, processes it using Spark, and filters reviews with a rating of 4 or higher. The cleaned data is stored in PostgreSQL, and Kafka is used for logging events.  

If you want to check if Kafka is installed, you can run `kafka-server-start.sh` and see if it starts without errors. Make sure your Kafka broker is running before executing the pipeline.  

This project demonstrates data processing with Spark, database integration with PostgreSQL, and event streaming with Kafka. Feel free to modify or improve it based on your needs!

Currently, only sample data is kept in the csv file(only 200 rows). The data files are to be downloaded from kaggle and placed in the amazon_data directory to replace the current sample data. To download them, use this link 'https://www.kaggle.com/datasets/saurav9786/amazon-product-reviews'. If the link isn't working, search google for 'Amazon Product Reviews Saurav Anand'  and you'r very likely to find it.

The below commands are also an easy way to dowload and prepare the data.
'kaggle datasets download -d saurav9786/amazon-product-reviews
unzip amazon-product-reviews.zip -d amazon_data'


Warning! The data files don't have built in headers, you need to set that up by making the firse row of data as follows (review_id,product_id,rating,timestamp).

Good luckl!

