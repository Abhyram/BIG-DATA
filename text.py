import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import boto3
import pandas as pd
import matplotlib.pyplot as plt

# Initialize Spark session
@st.cache_resource
def init_spark():
    return SparkSession.builder \
        .appName("Big Data and Cloud Computing Capstone") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

spark = init_spark()

# AWS S3 Configuration
def configure_aws(ACCESS_KEY, SECRET_KEY):
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESS_KEY)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", SECRET_KEY)
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

# Streamlit App
st.title("Big Data and Cloud Computing for Scalable Data Analytics")

# AWS Credentials Input
st.sidebar.header("AWS Configuration")
ACCESS_KEY = st.sidebar.text_input("AWS Access Key", type="password")
SECRET_KEY = st.sidebar.text_input("AWS Secret Key", type="password")
S3_BUCKET = st.sidebar.text_input("S3 Bucket Name")

# File Upload
st.header("Upload Dataset")
uploaded_file = st.file_uploader("Upload your CSV file for analysis", type=["csv"])

if uploaded_file and ACCESS_KEY and SECRET_KEY:
    # Configure AWS
    configure_aws(ACCESS_KEY, SECRET_KEY)

    # Read Uploaded File
    st.write("Uploading and Processing Data...")
    df = pd.read_csv(uploaded_file)
    spark_df = spark.createDataFrame(df)

    st.write("### Preview of Uploaded Data")
    st.dataframe(df.head())

    # Data Processing (Group By and Average Calculation)
    st.write("### Processing Data...")
    processed_df = spark_df.groupBy("category").agg(avg("value").alias("average_value"))
    processed_pd_df = processed_df.toPandas()

    st.write("### Processed Data")
    st.dataframe(processed_pd_df)

    # Save to S3
    output_path = f"s3a://{S3_BUCKET}/output_data/processed_data.csv"
    st.write(f"Saving processed data to {output_path}...")
    processed_df.write.csv(output_path, mode="overwrite", header=True)
    st.success("Data saved to S3 successfully!")

    # Visualization
    st.write("### Data Visualization")
    plt.figure(figsize=(10, 5))
    plt.bar(processed_pd_df["category"], processed_pd_df["average_value"], color='skyblue')
    plt.xlabel("Category")
    plt.ylabel("Average Value")
    plt.title("Average Value by Category")
    st.pyplot(plt)

else:
    st.warning("Please upload a dataset and configure AWS credentials to proceed.")

