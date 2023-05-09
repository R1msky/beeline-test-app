import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._


object TestApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("App")
      .getOrCreate()

    import spark.implicits._


    val orderPath: String = spark.sparkContext.getConf.get("spark.app.order_path")
    val customerPath: String = spark.sparkContext.getConf.get("spark.app.customer_path")
    val productPath: String = spark.sparkContext.getConf.get("spark.app.product_path")


    val orderSchema = new StructType()
      .add("customerID", IntegerType, false)
      .add("orderID", IntegerType, false)
      .add("productID", IntegerType, false)
      .add("numberOfProduct", IntegerType, true)
      .add("orderDate", DateType, true)
      .add("status", StringType, true)

    val customerSchema = new StructType()
      .add("customerID", IntegerType, false)
      .add("name", StringType, true)
      .add("email", StringType, true)
      .add("joinDate", DateType, true)
      .add("status", StringType, true)

    val productSchema = new StructType()
      .add("productID", IntegerType, false)
      .add("name", StringType, true)
      .add("price", DoubleType, true)
      .add("numberOfProducts", IntegerType, true)


    val orderDF = spark.read.format("csv").option("delimiter", "\t").schema(orderSchema).load(orderPath)
    val customerDF = spark.read.format("csv").option("delimiter", "\t").schema(customerSchema).load(customerPath)
    val productDF = spark.read.format("csv").option("delimiter", "\t").schema(productSchema).load(productPath)


    val orderWithTotalSumDF = orderDF
      .where($"status" === "delivered")
      .groupBy("customerID", "productID")
      .sum("numberOfProduct")
      .withColumnRenamed("sum(numberOfProduct)", "totalSumOfProduct")

    val maxSizeOfOrderDF = orderWithTotalSumDF
      .groupBy("customerID")
      .max("totalSumOfProduct")
      .withColumnRenamed("max(totalSumOfProduct)", "maxSizeOfOrder")

    val mostPopularProductDF = maxSizeOfOrderDF
      .join(orderWithTotalSumDF, Seq("customerID"))
      .where($"maxSizeOfOrder" === $"totalSumOfProduct")
      .join(customerDF, Seq("customerID"))
      .withColumnRenamed("name", "customerName")
      .join(productDF, Seq("productID"))
      .withColumnRenamed("name", "productName")
      .select("customerName", "productName")

    mostPopularProductDF.coalesce(1).write.option("header", true).csv("result")

  }
}

