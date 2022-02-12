import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  IntegerType,
  DateType
};
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val bitcoin_data_path =
  "file:///home/striker/Desktop/My-Git-Repositories/Exploratory-Data-Analysis/Analysis-of-Cryptocurrency-Using-Apache-Spark/Datasets/bitcoin.csv"
val ethereum_data_path =
  "file:///home/striker/Desktop/My-Git-Repositories/Exploratory-Data-Analysis/Analysis-of-Cryptocurrency-Using-Apache-Spark/Datasets/ethereum.csv"
val cardano_data_path =
  "file:///home/striker/Desktop/My-Git-Repositories/Exploratory-Data-Analysis/Analysis-of-Cryptocurrency-Using-Apache-Spark/Datasets/cardano.csv"

val bitcoin_df = spark.read
  .options(Map("header" -> "true", "inferSchema" -> "true"))
  .csv(bitcoin_data_path)
val ethereum_df = spark.read
  .options(Map("header" -> "true", "inferSchema" -> "true"))
  .csv(ethereum_data_path)
val cardano_df = spark.read
  .options(Map("header" -> "true", "inferSchema" -> "true"))
  .csv(cardano_data_path)

bitcoin_df.printSchema()
ethereum_df.printSchema()
cardano_df.printSchema()

bitcoin_df.show()
ethereum_df.show()
cardano_df.show()

val to_date_pattern = "MM/dd/yyyy"
val bitcoin_final_df = bitcoin_df
  .withColumn("Date", to_date($"Date", to_date_pattern))
  .withColumn("Year", year(col("Date")))
  .withColumn("Month", month(col("Date")))
  .withColumn("Day_of_Week", dayofweek(col("Date")))
  .withColumn(
    "pct_change",
    ((col("Close") - lag("Close", 1)
      .over(Window.partitionBy().orderBy("SNo"))) / col("Close")) * 100.0
  )
  .drop("SNo")
val ethereum_final_df = ethereum_df
  .withColumn("Date", to_date($"Date", to_date_pattern))
  .withColumn("Year", year(col("Date")))
  .withColumn("Month", month(col("Date")))
  .withColumn("Day_of_Week", dayofweek(col("Date")))
  .withColumn(
    "pct_change",
    ((col("Close") - lag("Close", 1)
      .over(Window.partitionBy().orderBy("SNo"))) / col("Close")) * 100.0
  )
  .drop("SNo")
val cardano_final_df = cardano_df
  .withColumn("Date", to_date($"Date", to_date_pattern))
  .withColumn("Year", year(col("Date")))
  .withColumn("Month", month(col("Date")))
  .withColumn("Day_of_Week", dayofweek(col("Date")))
  .withColumn(
    "pct_change",
    ((col("Close") - lag("Close", 1)
      .over(Window.partitionBy().orderBy("SNo"))) / col("Close")) * 100.0
  )
  .drop("SNo")

bitcoin_final_df.printSchema()
ethereum_final_df.printSchema()
cardano_final_df.printSchema()

bitcoin_final_df.show()
ethereum_final_df.show()
cardano_final_df.show()

println("Total Number of Observations In Bitcoin Dataset\n")
bitcoin_final_df.count

println("Total Number of Observations In Ethereum Dataset\n")
ethereum_final_df.count

println("Total Number of Observations In Cardano Dataset\n")
cardano_final_df.count

val sql_context = new org.apache.spark.sql.SQLContext(sc)

// Converting all the final dataframes into table view so that we can execute SQL queries on it.
bitcoin_final_df.createOrReplaceTempView("bitcoin")
ethereum_final_df.createOrReplaceTempView("ethereum")
cardano_final_df.createOrReplaceTempView("cardano")

bitcoin_final_df.stat.corr("Close", "Marketcap")
ethereum_final_df.stat.corr("Close", "Marketcap")
cardano_final_df.stat.corr("Close", "Marketcap")

val bitcoin_year_avg_df = bitcoin_final_df
  .groupBy("Year")
  .agg(mean("Close"), mean("Marketcap"))
  .sort("Year")
bitcoin_year_avg_df.show()

val bitcoin_month_avg_df = bitcoin_final_df
  .groupBy("Month")
  .agg(mean("Close"), mean("Marketcap"))
  .sort("Month")
bitcoin_month_avg_df.show()

val ethereum_year_avg_df = ethereum_final_df
  .groupBy("Year")
  .agg(mean("Close"), mean("Marketcap"))
  .sort("Year")
ethereum_year_avg_df.show()

val ethereum_month_avg_df = ethereum_final_df
  .groupBy("Month")
  .agg(mean("Close"), mean("Marketcap"))
  .sort("Month")
ethereum_month_avg_df.show()

val cardano_year_avg_df = cardano_final_df
  .groupBy("Year")
  .agg(mean("Close"), mean("Marketcap"))
  .sort("Year")
cardano_year_avg_df.show()

val cardano_month_avg_df = cardano_final_df
  .groupBy("Month")
  .agg(mean("Close"), mean("Marketcap"))
  .sort("Month")
cardano_month_avg_df.show()

val start_date = "2020-01-01"
val bitcoin_fy21_df = bitcoin_final_df.filter($"Date" >= start_date)
val ethereum_fy21_df = ethereum_final_df.filter($"Date" >= start_date)
val cardano_fy21_df = cardano_final_df.filter($"Date" >= start_date)

bitcoin_fy21_df.show()
ethereum_fy21_df.show()
cardano_fy21_df.show()

val crypto_fy21_df = sql_context
  .sql(
    "SELECT ETH.Date as Date, BTC.Close as BTC_Close, ETH.Close as ETH_Close, CAD.Close as CAD_Close FROM Bitcoin_FY21 BTC INNER JOIN Ethereum_FY21 ETH ON BTC.Date == ETH.Date INNER JOIN Cardano_FY21 CAD ON ETH.Date == CAD.Date"
  )
  .withColumnRenamed("Close", "BTC_Close")
crypto_fy21_df.createOrReplaceTempView("Crypto_FY21")
crypto_fy21_df.show()

crypto_fy21_df.stat.corr("BTC_Close", "ETH_Close")
crypto_fy21_df.stat.corr("ETH_Close", "CAD_Close")
crypto_fy21_df.stat.corr("CAD_Close", "BTC_Close")
