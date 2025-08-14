import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


b_df = spark.read.json("s3://yelp-project123/project_json/yelp_academic_dataset_business.json")
r_df = spark.read.json("s3://yelp-project123/project_json/yelp_academic_dataset_review.json")
u_df = spark.read.json("s3://yelp-project123/project_json/yelp_academic_dataset_user.json")

b_df.coalesce(4).write.mode("overwrite").parquet("s3://yelp-project123/Parquet_data/business/")
r_df.coalesce(4).write.mode("overwrite").parquet("s3://yelp-project123/Parquet_data/review/")
u_df.coalesce(4).write.mode("overwrite").parquet("s3://yelp-project123/Parquet_data/user/")


b_df = spark.read.parquet("s3://yelp-project123/Parquet_data/business/")
r_df = spark.read.parquet("s3://yelp-project123/Parquet_data/review/")
u_df = spark.read.parquet("s3://yelp-project123/Parquet_data/user/")


b_df = b_df.withColumnRenamed("name", "b_name")\
           .withColumnRenamed("stars","b_stars")\
           .withColumnRenamed("review_count","b_review_count")\

r_df = r_df.withColumnRenamed("cool","r_cool")\
       .withColumnRenamed("date","r_date")\
      .withColumnRenamed("useful","r_useful")\
      .withColumnRenamed("funny","r_funny")\


# 1. review ⨝ user on user_id
review_user_df = r_df.join(u_df, on="user_id", how="inner")
# 2. (review + user) ⨝ business on business_id
final_df = review_user_df.join(b_df, on="business_id", how="inner")


columns_to_keep = ["business_id","user_id","name","cool","r_date","review_id","funny", "stars","useful" ,"city", "review_count","fans","b_name","state","categories"]
final_df = final_df.select(*columns_to_keep)

 final_df.groupBy(final_df.columns).count().filter("count > 1").show()

final_df = final_df.dropDuplicates()

final_df = final_df.na.drop()

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

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
    "Pets": ["Pet Services", "Veterinarians", "Pet Stores"],
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

final_df.groupBy("super_category").count().orderBy("count", ascending=False).show()

# ----- Step 19: Notebook Cell 19 -----
# transformation on date 
from pyspark.sql import functions as F

final_df = final_df.withColumn(
    "sentiment",
    F.when(F.col("stars") <= 2, "negative")
     .when(F.col("stars") == 3, "neutral")
     .otherwise("positive")
)

from pyspark.sql import functions as F

final_df = final_df.withColumn("only_date", F.to_date("r_date"))

final_df.select("r_date", "only_date").show(5, False)

final_df = final_df.drop("r_date", "categories")

from pyspark.sql.functions import when, col

state_mapping = {
    "DE": "Delaware",
    "MO": "Missouri",
    "VI": "Virgin Islands",
    "IL": "Illinois",
    "SD": "South Dakota",
    "UT": "Utah",
    "HI": "Hawaii",
    "CA": "California",
    "NC": "North Carolina",
    "AZ": "Arizona",
    "LA": "Louisiana",
    "NJ": "New Jersey",
    "MT": "Montana",
    "FL": "Florida",
    "MI": "Michigan",
    "NV": "Nevada",
    "ID": "Idaho",
    "VT": "Vermont",
    "WA": "Washington",
    "IN": "Indiana",
    "TN": "Tennessee",
    "TX": "Texas",
    "CO": "Colorado",
    "PA": "Pennsylvania",
    "AB": "Alberta",
    "MA": "Massachusetts",
    "XMS": "Mississippi"  # Placeholder for XMS; adjust if you identify the correct state
}

# Transform the state column
df = final_df.withColumn(
    "state",
    when(col("state") == "DE", "Delaware")
    .when(col("state") == "MO", "Missouri")
    .when(col("state") == "VI", "Virgin Islands")
    .when(col("state") == "IL", "Illinois")
    .when(col("state") == "SD", "South Dakota")
    .when(col("state") == "UT", "Utah")
    .when(col("state") == "HI", "Hawaii")
    .when(col("state") == "CA", "California")
    .when(col("state") == "NC", "North Carolina")
    .when(col("state") == "AZ", "Arizona")
    .when(col("state") == "LA", "Louisiana")
    .when(col("state") == "NJ", "New Jersey")
    .when(col("state") == "MT", "Montana")
    .when(col("state") == "FL", "Florida")
    .when(col("state") == "MI", "Michigan")
    .when(col("state") == "NV", "Nevada")
    .when(col("state") == "ID", "Idaho")
    .when(col("state") == "VT", "Vermont")
    .when(col("state") == "WA", "Washington")
    .when(col("state") == "IN", "Indiana")
    .when(col("state") == "TN", "Tennessee")
    .when(col("state") == "TX", "Texas")
    .when(col("state") == "CO", "Colorado")
    .when(col("state") == "PA", "Pennsylvania")
    .when(col("state") == "AB", "Alberta")
    .when(col("state") == "MA", "Massachusetts")
    .when(col("state") == "XMS", "Mississippi")
    .otherwise(col("state"))  # Keeps any unmapped states as-is
)

final_df.coalesce(1) \
    .write.mode("overwrite") \
    .parquet("s3://yelp-project123/final_data/final_parq/")

job.commit()
