import csv
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession \
    .builder \
    .appName("SparkDataFrame") \
    .getOrCreate()

#Importing data and creating dataframe
df = spark.read.csv(r"survey.csv", header=True)
df.createOrReplaceTempView("Survey")

#writing the data into a file
df.show()
df.write.option("header", "true").csv("newSurvey.csv")

#Checking for duplicates and drop it from the data
df.dropDuplicates()


spark.sql("select * from Survey where tech_company = 'Yes'").createTempView("TechCompany")
spark.sql("select * from Survey where tech_company = 'No'").createTempView("NonTechCompany")
spark.sql("select * from TechCompany union select * from NonTechCompany order by Country").show()

spark.sql("select treatment, count(*) as TreatmentCount from Survey group by treatment").show()

#part 2

spark.sql("select * from TechCompany as tc left join NonTechCompany as ntc on tc.Gender == ntc.Gender ").show()
spark.sql("select Country, count(*) as Count from Survey group by Country order by Count desc").show()

#display 13th row
row13 = df.limit(13)
expr = [f.last(col).alias(col) for col in row13.columns]
row13.agg(*expr).show()