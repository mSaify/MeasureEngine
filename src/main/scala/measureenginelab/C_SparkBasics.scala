package measureenginelab

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class MyRecord (id: Int, description: String)
case class Size(size_id: Int, desc: String)
case class Product(product_id: Int, size_id: Int, desc: String)
case class ProductItem(product_id: Int, product_desc: String)
case class SizeEntity(size_id: Int, size_desc: String, products: Seq[ProductItem])

object C_SparkBasics {
  def sparkStuff(): Unit = {

    // First we build a session.
    val spark = SparkSession
      .builder()
      .appName("My Test App")
      .master("local[2]") // Setting master is good for testing, don't use for production code
      .getOrCreate()

    // Set the current database
    spark.catalog.setCurrentDatabase("my_db")

    // ====== DataFrames
    // DataFrames -- how do we get one?

    // Get a DataFrame from Hive
    val dataframeFromHive = spark.table("optional_hive_database_name.hive_table_name")

    // Create a DataFrame from local data
    import spark.implicits._
    val myRecords = Seq(MyRecord(1, "Peanut"), MyRecord(2, "Butter"))
    val dataFrameFromLocal = myRecords.toDF

    // Things you can do with a DataFrame:

    // ====== Projections
    val projection = dataFrameFromLocal.select($"id", $"description", $"id" * 2 as "id_doubled")
    projection.show()
    //    +---+-----------+----------+
    //    | id|description|id_doubled|
    //    +---+-----------+----------+
    //    |  1|     Peanut|         2|
    //    |  2|     Butter|         4|
    //    +---+-----------+----------+

    val projection2 = dataFrameFromLocal.withColumn("id_doubled", $"id" * 2)

    // ===  === Expressions
    // The special syntax $"id" * 2 as "id_doubled" is just an internal DSL. Compare the following two expressions:

    val sugared : Column = $"id" * 2 as "id_doubled"
    val desugared : Column = StringContext("id").$().*(lit(2)).as("id_doubled")

    dataFrameFromLocal.select(sugared).show()
    dataFrameFromLocal.select(desugared).show()

    // Because DataFrames and Columns are just objects, you can compose with functions to keep your code DRY:
    def adjustDataFrame(df: DataFrame): DataFrame = {
      // Do interesting things to your dataframe: project, filter, join, sort, group by etc.
      df
    }

    def columnDoubler(inputColumnName: String): Column = col(inputColumnName) * 2

    dataFrameFromLocal.select(columnDoubler("id")).show()

    // More expressions
    // see also the function reference: https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
    val expr1 = $"id".isNotNull
    val expr2 = trim($"description")
    val expr3 = concat($"description", $"id", lit("-"), lit(5)) as "sku"

    dataFrameFromLocal.select(expr3).show()
    //    +---------+
    //    |      sku|
    //    +---------+
    //    |Peanut1-5|
    //    |Butter2-5|
    //    +---------+

    // ====== Filtering
    val filtered = projection.filter($"id" % 2 === 0)
    filtered.show()
    //    +---+-----------+----------+
    //    | id|description|id_doubled|
    //    +---+-----------+----------+
    //    |  2|     Butter|         4|
    //    +---+-----------+----------+

    // ====== Joins
    val sizes = Seq(Size(1, "Small"), Size(2, "Medium"), Size(3, "Large")).toDF
    val products = Seq(
      Product(1, 2, "Avacado"),
      Product(2, 1, "Lemon"),
      Product(3, 4, "Grapefruit"),
      Product(4, 1, "Cookies")).toDF

    val joined = products.join(sizes, products("size_id") === sizes("size_id"))
    joined.show()
    //    +----------+-------+-------+-------+------+
    //    |product_id|size_id|   desc|size_id|  desc|
    //    +----------+-------+-------+-------+------+
    //    |         1|      2|Avacado|      2|Medium|
    //    |         2|      1|  Lemon|      1| Small|
    //    |         4|      1|Cookies|      1| Small|
    //    +----------+-------+-------+-------+------+

    // by default, join is an inner join. To do a left join do:
    val left_joined = products.join(sizes, products("size_id") === sizes("size_id"), "left")
    left_joined.show()
    //    +----------+-------+----------+-------+------+
    //    |product_id|size_id|      desc|size_id|  desc|
    //    +----------+-------+----------+-------+------+
    //    |         1|      2|   Avacado|      2|Medium|
    //    |         2|      1|     Lemon|      1| Small|
    //    |         3|      4|Grapefruit|   null|  null|
    //    |         4|      1|   Cookies|      1| Small|
    //    +----------+-------+----------+-------+------+


    // ====== Group by
    val grouped = products
      .groupBy($"size_id")
      .agg(count($"*"))

    grouped.show()
    //    +-------+--------+
    //    |size_id|count(1)|
    //    +-------+--------+
    //    |      1|       2|
    //    |      4|       1|
    //    |      2|       1|
    //    +-------+--------+

    // ====== Creating a Dataset[T]

    // Step 1: Shape the data
    val grouped_products =
      products
        .groupBy($"size_id")
        .agg(collect_list(struct($"product_id", $"desc" as "product_desc")) as "products")
    val shaped =
      sizes
      .join(grouped_products, sizes("size_id") === grouped_products("size_id"), "left")
      .select(sizes("size_id"), $"desc" as "size_desc", $"products")

    // Step 2: deserialize into the case class model
    val shaped_dataset: Dataset[SizeEntity] = shaped.as[SizeEntity]

    shaped_dataset.show()
    //    +-------+---------+--------------------+
    //    |size_id|size_desc|            products|
    //    +-------+---------+--------------------+
    //    |      1|    Small|[[2,Lemon], [4,Co...|
    //    |      2|   Medium|       [[1,Avacado]]|
    //    |      3|    Large|                null|
    //    +-------+---------+--------------------+

    // ====== Lazy vs. Actions
    // Things that are lazy:
    // projections, filters, joins, group bys, etc.
    // E.g. anything that can modify a query plan without requiring any evaluation.

    // Things that are actions:
    // Anything requiring evaluation to get the answer.
    //  df.count()
    //  df.write.parquet("s3a://my-bucket/my/path")
    //  df.collect()
    //  df.take(10)
    //  df.show()
    //  df.head()
    //  etc...
  }
}
