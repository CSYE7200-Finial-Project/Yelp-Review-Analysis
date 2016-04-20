//these codes need to run in zeppelin
import org.apache.spark.sql.hive.HiveContext
import sqlContext.implicits._
import sys.process._

val inputFile = "/Users/wanlima/Documents/Scala/yelp_dataset_challenge_academic_dataset/reviews/xaa1.json"
val df = sqlContext.read.json(inputFile)
df.printSchema()
df.registerTempTable("temp")  

%sql
select stars, count(stars) as Number from temp group by stars

%sql
select stars, count(stars) as Number from temp where date LIKE "${year=2008,2008|2009|2010|2011|2013|2014|2015}%"  group by stars

%sql select subString(date,1,4) as year, avg(stars) as Avg from temp group by subString(date, 1,4)