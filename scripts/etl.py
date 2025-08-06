import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf, year, month
from pyspark.sql.types import StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from S3 in Json format
b_df = spark.read.json("s3://yelp-business-analytics/Dataset/yelp_academic_dataset_business.json")
r_df = spark.read.json("s3://yelp-business-analytics/Dataset/yelp_academic_dataset_review.json")
u_df = spark.read.json("s3://yelp-business-analytics/Dataset/yelp_academic_dataset_user.json")

# Rename columns for clarity
b_df = b_df.withColumnRenamed("name", "b_name")\
           .withColumnRenamed("stars", "b_stars")\
           .withColumnRenamed("review_count", "b_review_count")

r_df = r_df.withColumnRenamed("cool", "r_cool")\
           .withColumnRenamed("date", "r_date")\
           .withColumnRenamed("useful", "r_useful")\
           .withColumnRenamed("funny", "r_funny")

# Join the datasets
review_user_df = r_df.join(u_df, on="user_id", how="inner")
final_df = review_user_df.join(b_df, on="business_id", how="inner")

# Select only the required columns
columns_to_keep = [
    "business_id", "user_id", "name", "cool", "r_date", "review_id",
    "funny", "stars", "useful", "city", "review_count", "fans",
    "b_name", "state", "categories"
]
final_df = final_df.select(*columns_to_keep)

# Remove duplicates
final_df = final_df.dropDuplicates()

# UDF to map categories to super categories
super_categories = {
    "Restaurants": ["Restaurants", "Food"],
    "Shopping": ["Shopping", "Fashion", "Books", "Department Stores"],
    "Beauty & Spas": ["Hair Salons", "Beauty & Spas", "Nail Salons", "Massage"],
    "Health & Medical": ["Dentists", "Health & Medical", "Chiropractors"],
    "Nightlife": ["Bars", "Nightlife", "Clubs", "Pubs"],
    "Automotive": ["Auto Repair", "Automotive", "Car Dealers"],
    "Fitness": ["Gyms", "Fitness & Instruction", "Yoga", "Trainers"],
    "Home Services": ["Home Services", "Plumbing", "Electricians"],
    "Education": ["Education", "Tutoring Centers"],
    "Pets": ["Pet Services", "Veterinarians", "Pet Stores"]
}

def map_super_category(categories):
    if categories is None:
        return "Other"
    for super_cat, keywords in super_categories.items():
        for keyword in keywords:
            if keyword in categories:
                return super_cat
    return "Other"

map_super_category_udf = udf(map_super_category, StringType())
final_df = final_df.withColumn("super_category", map_super_category_udf(final_df["categories"]))

# Extract year and month from the date column and drop the original date column
final_df = final_df.withColumn("year", year("r_date"))\
                   .withColumn("month", month("r_date"))
final_df = final_df.drop("r_date", "categories")

# define and validate output path
output_path = "s3://glue-file-1/final-output/"  # Set your actual S3 output path

if not output_path.strip():
    raise ValueError("Output path cannot be empty. Please set a valid S3 path.")

# Save the final dataframe to S3 as CSV
final_df.coalesce(1) \
       .write \
       .mode("overwrite") \
       .option("header", True) \
       .csv(output_path)

job.commit()
