import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf, year, month
from pyspark.sql.types import StringType

# Get job args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 JSON input paths
business_path = "s3://yelp-business-analytics/Dataset/yelp_academic_dataset_business.json"
review_path = "s3://yelp-business-analytics/Dataset/yelp_academic_dataset_review.json"
user_path = "s3://yelp-business-analytics/Dataset/yelp_academic_dataset_user.json"

# Read the JSON datasets
b_df = spark.read.json(business_path)
r_df = spark.read.json(review_path)
u_df = spark.read.json(user_path)

# Rename columns to avoid conflicts
b_df = b_df.withColumnRenamed("name", "b_name") \
           .withColumnRenamed("stars", "b_stars") \
           .withColumnRenamed("review_count", "b_review_count")

r_df = r_df.withColumnRenamed("cool", "r_cool") \
           .withColumnRenamed("date", "r_date") \
           .withColumnRenamed("useful", "r_useful") \
           .withColumnRenamed("funny", "r_funny")

# Join: review + user → then with business
review_user_df = r_df.join(u_df, on="user_id", how="inner")
final_df = review_user_df.join(b_df, on="business_id", how="inner")

# Keep only selected columns
columns_to_keep = [
    "business_id", "user_id", "name", "r_cool", "r_date", "review_id",
    "r_funny", "stars", "r_useful", "city", "review_count", "fans",
    "b_name", "state", "categories"
]
final_df = final_df.select(*columns_to_keep)

# Drop duplicate rows
final_df = final_df.dropDuplicates()

# Super-category mapping logic
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

# Register UDF
map_super_category_udf = udf(map_super_category, StringType())

# Add super_category column
final_df = final_df.withColumn("super_category", map_super_category_udf(final_df["categories"]))

# Extract year and month, drop unused columns
final_df = final_df.withColumn("year", year("r_date")) \
                   .withColumn("month", month("r_date")) \
                   .drop("r_date", "categories")

# Output S3 path
output_path = "s3://glue-file-1/final-output/"  # Change if needed

if not output_path.strip():
    raise ValueError("Output path cannot be empty. Please set a valid S3 path.")

# Save final dataset as single CSV file in S3
final_df.coalesce(1) \
       .write \
       .mode("overwrite") \
       .option("header", True) \
       .csv(output_path)

# End Glue job
job.commit()

