import hsfs
import pandas as pd
from pyspark.sql.functions import pandas_udf, col, current_timestamp
from pyspark.sql.session import SparkSession
from pyspark.sql.types import FloatType
from great_expectations.core import ExpectationSuite, ExpectationConfiguration


# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Establish a connection with the feature store
connection = hsfs.connection()
fs = connection.get_feature_store()

# Simple example of feature engineering using Pandas UDF

from fraud.features.profiles.profiles import compute_age 
compute_age_udf = pandas_udf(compute_age, FloatType())

# Read the profile data from the raw feature group
df = spark.read \
    .format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .load('/Projects/live_coding/RawData/profiles.csv')


# Invoke feature engineering
df = df.withColumn('age', compute_age_udf(col('birthdate'))) \
        .withColumn('event_time', current_timestamp()) \
        .select('cc_num', 'event_time', 'country', 'age')

# Setup Feature Group metadata
# Setup Expectation suite
expectation_suite = ExpectationSuite(expectation_suite_name="profiles_suite")

expectation_suite.add_expectation(
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column":"age", 
            "min_value":0,
            "max_value":120,
        }
    )
)

profiles = fs.get_or_create_feature_group(
    name="profiles",
    version=1,
    description="Credit card owner profile",
    primary_key=['cc_num'],
    event_time="event_time",
    online_enabled=True,
    statistics_config={'histograms': True, 'correlations': True},
    expectation_suite=expectation_suite
)

# Materialize the feature group on the feature store
profiles.insert(df)

# Persist additional metadata
feature_descriptions = [
    {"name": "cc_num", "description": "Number of the credit card performing the transaction"},
    {"name": "country", "description": "Country of residence of the card holder"},
    {"name": "age", "description": "Age of the card holder at the event time"},
]

for desc in feature_descriptions: 
    profiles.update_feature_description(desc["name"], desc["description"])
