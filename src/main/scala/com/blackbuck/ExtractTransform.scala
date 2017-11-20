package com.blackbuck

import java.text.SimpleDateFormat

import gudusoft.gsqlparser.{EDbVendor, TGSqlParser}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType}


/**
  * Created by gopeshtulsyan on 14/11/17 at 5:23 PM.
  */
object ExtractTransform {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ExtractTransform")
      .getOrCreate()

    val format = new SimpleDateFormat("YYYY-MM-DD HH:MM:SS")
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key","AKIAJLARELZYSV42X6RA")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")

    val ddl = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.31.21.71:9092")
      .option("subscribePattern", "supply_microservice")
      .option("startingOffsets", "earliest").load()

    val ddlStatements  = ddl.withColumn("ddl", get_json_object($"value".cast("string"), "$.ddl")).select("ddl")

    val sqlparser = new TGSqlParser(EDbVendor.dbvmssql)
    val sqlDataMap = collection.mutable.Map[String, Array[SqlParser.SQLDataType]]()

    ddlStatements.collect().foreach( ddlStatement => {
      val typeTuple = SqlParser.parseCreateStatement(ddlStatement.getString(0))
      if (typeTuple != null){
        sqlDataMap += (typeTuple._1 -> typeTuple._2)
      }
    })
  val lines = spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "172.31.21.71:9092")
    .option("subscribePattern", "supply.supply_microservice.*")
    .option("startingOffsets", "earliest").load()

  val topics = lines.select("topic").distinct().collect();
  for (elem <- topics) {
    try {
      val topic : Array[String] = elem.getString(0).split('.')
      val table = topic.apply(2)
      val oldDb = topic.apply(1)
      val oldTable = oldDb.concat(".").concat(table)
      val newTable = "etl_test".concat(".".concat(table))
      RedShiftUtil.execute(RedShiftUtil.getCreateQuery(newTable, oldTable))
      val rows = lines.select(lines.col("offset"), lines.col("value")).where(lines.col("topic").equalTo(elem.getString(0)))
      val lines1  = rows.withColumn("after", get_json_object($"value".cast("string"), "$.after")).select("after")
      val rdd = lines1.rdd.map{case Row(json: String) => json}
      var outDF = spark.sqlContext.read.json(rdd)
      val colsMap = collection.mutable.Map[String, DataType]()

      val cols = outDF.schema.fields
      cols.foreach(col => colsMap += (col.name -> col.dataType))

      val sqlTypes : Option[Array[SqlParser.SQLDataType]] = sqlDataMap.get(table)
      sqlTypes.foreach(sqlType => {
        val colName :String = sqlType.apply(0).name
        val debeziumType :Option[DataType] = colsMap.get(sqlType.apply(0).name)
        if(sqlType.apply(0).dataType.contains("datetime") && !debeziumType.get.equals(StringType)){
          //assuming to be long if not string
          outDF = outDF.withColumn(colName.concat("_tmp"), from_unixtime(outDF(colName).divide(1000))).
            drop(colName).withColumnRenamed(colName.concat("_tmp"), colName)
        }
      })

      outDF.write.format("json").mode(SaveMode.Overwrite).save(s"s3a://kafkaredshift/temp/${newTable}/")

      RedShiftUtil.execute(RedShiftUtil.getUpsertQuery(newTable, cols))
    }
    catch {
      case foo: Throwable =>
        println("Some error occurred"+foo)
    }

  }
//    tables.collect().foreach(r => tables.)


//
//val lines1  = lines.withColumn("after", get_json_object($"value".cast("string"), "$.after")).select("after")
//  val rdd = lines1.rdd.map{case Row(json: String) => json}
//  val outDF = spark.sqlContext.read.json(rdd)
//  outDF.write.format("json").mode(SaveMode.Overwrite).save("s3a://kafkaredshift/temp/3/")
//    lines1.map(lines => lines.get(0).asInstanceOf[String]).foreach(lines => print(lines))
//      val schemaString = "id created_by created_on last_updated_by last_updated_on version end_time entity_id entity_type growth_business_supply_partner_id new_business_supply_partner_id procurement_time start_time"
//      val schema =
//        StructType(
//          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
//  val obj = lines1.select(from_json($"after".cast("string"), schema) as "data").select("data.*")

  //  val words = lines.as[String].flatMap(_.split(" "))
//  val json = lines.map{case Row(json: String) => json}
//
//  json.writeStream.format("console")





//val lines = spark.readStream
//  .format("socket")
//  .option("host", "localhost")
//  .option("port", 9999)
//  .load()


//    val reader = spark.read
//
//    Option(schema).foreach(reader.schema)
//    reader.json(spark.sparkContext.parallelize(Array(json)))
//    val newDataFrame = spark.createDataFrame(lines1.rdd, schema)

//   val col = get_json_object($"after".cast("string"), "$")


//    lines1.select("after").as(String).writeStream.format("json").option("checkpointLocation", "/tmp/check1").start("/tmp/par")



//    lines1.select("after").writeStream.format("parquet").start("/tmp/par")

//
// lines1.select("after").write.format("json").option("header", false).mode(SaveMode.Overwrite)
//    .save("s3://AKIAJLARELZYSV42X6RA:kkJ1TsBcfOdta5eP8tvS1CxiBQMtBihIbzym4PYG@kafkaredshift/transient2.json")
//    val inserts = lines1.filter(line => line.getAs("op").equals("c"))

//    .mode("error")
//    .save()
//
////    inserts.reduce()

//    lines.foreach(r => print(r.schema))


//    val insertTask = lines1.select("after").select("id").writeStream.
//      format("console").
//      start()
//
//    insertTask.awaitTermination()



//    val query = op.writeStream
//      .format("console")
//      .start()
//
//    query.awaitTermination()


  }

}
