from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format,  dayofmonth, year,  avg, \
    stddev, count

# Path to the MySQL JDBC driver JAR file
mysql_jar_path = "/home/ashok/PycharmProjects/Aidetic/mysql-connector-java-8.0.26.jar"

# Define the MySQL connection properties
mysql_properties = {
    "url": "jdbc:mysql://localhost:3306/Aidetic",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "root",
    "password": "mysql",
    "dbtable": "neic_earthquakes",
}

# Create Spark session with the MySQL JDBC driver added to the Spark configuration
spark = SparkSession.builder.appName("example").config("spark.jars", mysql_jar_path).getOrCreate()

# Read data from MySQL into a DataFrame
df = (spark.read.format("jdbc").option("url", mysql_properties["url"])
      .option("dbtable", mysql_properties["dbtable"])
      .option("user", mysql_properties["user"])
      .option("password", mysql_properties["password"])
      .option("driver", mysql_properties["driver"]).load())


df = df.withColumn("Date", to_date(df['Date'], "MM/dd/yyyy"))


df.show()
print("Q1.How does the Day of a Week affect the number of earthquakes?")
# Extract the day of the week and create a new column
Q1_day_of_week = df.withColumn("DayOfWeek", date_format(col("Date"), "E"))
Q1_day_of_week = Q1_day_of_week.groupBy("DayOfWeek").count()
Q1_day_of_week =Q1_day_of_week.filter(col("DayOfWeek").isNotNull())
Q1_day_of_week.show()

print("Q2.What is the relation between Day of the month and Number of earthquakes that happened in a year?")
Q2_day_of_month = df.withColumn("DayOfMonth",dayofmonth("Date"))
Q2_day_of_month = Q2_day_of_month.groupBy(year("Date").alias("year"),"DayOfMonth").agg(count("*").alias("Number of earthquakes in a year"))
Q2_day_of_month =Q2_day_of_month.orderBy("year","DayOfMonth")
Q2_day_of_month =Q2_day_of_month.filter(col("year").isNotNull()&col("DayOfMonth").isNotNull())
Q2_day_of_month.show()

print("Q3.What does the average frequency of earthquakes in a month from the year 1965 to 2016 tell us?")
Q3_avg_of_month = df.filter((year("Date")>="1965") & (year("Date")<"2016"))
Q3_avg_of_month =Q3_avg_of_month.groupBy(year("Date").alias("year"),date_format("Date", "MMM").alias("month")).agg(count("*").alias("EarthquakesCount"))
Q3_avg_of_month.groupBy("month").agg(avg("EarthquakesCount").alias("AverageEarthquakesCount")).show()

print("Q4.What is the relation between Year and Number of earthquakes that happened in that year?")
Q4_eq_in_year = df.groupBy(year("Date").alias("Year")).agg(count("*").alias("Number of earthquakes in year"))
Q4_eq_in_year.show()

print("Q5.How has the earthquake magnitude on average been varied over the years?")
Q5_average_magnitude_by_year = df.groupBy(year("Date").alias("Year")).agg(avg("Magnitude").alias("AverageMagnitude"))
Q5_average_magnitude_by_year.show()

print("Q6.How does year impact the standard deviation of the earthquakes?")
Q6_std_dev_by_year = df.groupBy(year("Date").alias("Year")).agg(stddev("Magnitude").alias("StdDevMagnitude"))
Q6_std_dev_by_year.show()

print("Q6.Does geographic location have anything to do with earthquakes?")
print("we can find the location where earthwuakes occur")
Q7_geo_loc = df.groupBy("Latitude","Longitude").agg(count("*").alias("EarthquakeCount"))

Q7_geo_loc = Q7_geo_loc.orderBy("EarthquakeCount",ascending=False)
Q7_geo_loc.show()

print("Q8.Where do earthquakes occur very frequently?")
Q8_freq_eq = Q7_geo_loc.first()
print(Q8_freq_eq)

print("Q9.What is the relation between Magnitude, Magnitude Type , Status and Root Mean Square of the earthquakes?")
Q9 = df.groupBy("Magnitude Type", "Status").agg(
    avg("Magnitude").alias("Avg_Magnitude"),
    avg("Root Mean Square").alias("Avg_RMS"),
    count("*").alias("Count")
).orderBy("Magnitude Type", "Status")

Q9.show()

# Stop the Spark session when done
spark.stop()