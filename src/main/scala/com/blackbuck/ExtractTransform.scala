package com.blackbuck

import com.google.gson.JsonObject
import gudusoft.gsqlparser.{EDbVendor, TGSqlParser}
import kafka.utils.{ ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer


/**
  * Created by gopeshtulsyan on 14/11/17 at 5:23 PM.
  */
object ExtractTransform {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ExtractTransform")
      .getOrCreate()
    import spark.implicits._
//    val sc = new SparkContext(new SparkConf)
//    // hostname:port for Kafka brokers, not Zookeeper
//    val kafkaParams : java.util.Map[String, Object] =  new util.HashMap[String, Object]()
//    kafkaParams.put("bootstrap.servers", "172.31.21.71:909")
//    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
//    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
//    kafkaParams.put("group.id","base_order_views")

//    val offsetRanges = Array(OffsetRange("supply.supply_microservice.base_order_views", 0, 110, 220))
//
//    val rdd = KafkaUtils.createRDD[String, String](sc ,kafkaParams, offsetRanges, LocationStrategies.PreferConsistent)
//
//
//    val format = new SimpleDateFormat("YYYY-MM-DD HH:MM:SS")



    val zk = ZkUtils.createZkClientAndConnection("localhost:2181/", 1000, 1000)

    val zkUtils = new ZkUtils(zk._1, zk._2, false)
    val topicNames = zkUtils.getAllTopics()
    val offsets : JsonObject = new JsonObject
    topicNames.foreach(topic => {
      val offsetJson : JsonObject = new JsonObject
      if(topic.startsWith("supply.supply_microservice")){
        offsetJson.addProperty("0",-2)
        offsets.add(topic, offsetJson)
      }
    })
    val basePath = "/consumers/spark-etl/offsets"

    val acls = new ListBuffer[ACL]()
    val acl = new ACL
    acl.setId(ZooDefs.Ids.ANYONE_ID_UNSAFE)
    acl.setPerms(ZooDefs.Perms.ALL)
    acls += acl


    val children : Seq[String] = zkUtils.getChildren("/consumers/spark-etl/offsets")


    children.foreach(child => {
      val topic = "supply.supply_microservice." + child
      val offsetJson : JsonObject = new JsonObject
      try{
        val path = basePath + "/" + child + "/fromOffset"
        val offset = zkUtils.readData(path)._1
        offsetJson.addProperty("0", offset)
      }catch {
        case foo: ZkNoNodeException =>
          offsetJson.addProperty("0", 0)
      }
      offsets.add(topic, offsetJson)
    })

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
      val typeTuple = SqlParser.parseCreateStatement(ddlStatement.getString(0).replace("`",""))
      if (typeTuple != null){
        sqlDataMap += (typeTuple._1 -> typeTuple._2)
      }
    })

  val lines = spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "172.31.21.71:9092")
    .option("subscribePattern", "supply.supply_microservice.*")
      .option("startingOffsets",s"${offsets.toString}")
    .load()

  val topics = lines.select("topic").distinct().collect();

  for (elem <- topics) {
    try {
      val topic : Array[String] = elem.getString(0).split('.')
      val table = topic.apply(2)
      val oldDb = topic.apply(1)
      val oldTable = oldDb.concat(".").concat(table)
      val newTable = "etl_test".concat(".".concat(table))
      RedShiftUtil.execute(RedShiftUtil.getCreateQuery(newTable, oldTable))
      val rows = lines.select(lines.col("offset"), lines.col("value"), lines.col("topic")).where(lines.col("topic").equalTo(elem.getString(0)))
      val lines1  = rows.withColumn("after", get_json_object($"value".cast("string"), "$.after")).select("after")
      val rdd = lines1.rdd.map{case Row(json: String) => json}
      var outDF = spark.sqlContext.read.json(rdd)
      val colsMap = collection.mutable.Map[String, DataType]()

      val cols = outDF.schema.fields
      cols.foreach(col => colsMap += (col.name -> col.dataType))

      val sqlTypes  = sqlDataMap.get(table)

      sqlTypes.get.foreach(sqlType => {
        val colName :String = sqlType.name
        val debeziumType :Option[DataType] = colsMap.get(sqlType.name)
        if(sqlType.dataType.equals("datetime") && !debeziumType.get.equals(StringType)){
          //assuming to be long if not string
          outDF = outDF.withColumn(colName.concat("_tmp"), from_unixtime(outDF(colName).divide(1000))).
            drop(colName).withColumnRenamed(colName.concat("_tmp"), colName)
        }
        else if(sqlType.dataType.equals("date") && !debeziumType.get.equals(StringType)){
          outDF = outDF.withColumn(colName.concat("_tmp"), to_date(from_unixtime(outDF(colName).multiply(86400)))).
            drop(colName).withColumnRenamed(colName.concat("_tmp"), colName)
        }
      })

      outDF.write.format("json").mode(SaveMode.Overwrite).save(s"s3a://kafkaredshift/temp/${newTable}/")

      RedShiftUtil.execute(RedShiftUtil.getUpsertQuery(newTable, cols))
      rows.groupBy("topic").max("offset")
      val zkdirs = new ZKGroupTopicDirs("spark-etl",table)

      val offsetPath = zkdirs.consumerOffsetDir + "/" + "fromOffset"

      val max : Any = rows.groupBy("topic").max("offset").select("max(offset)").first.get(0)
      zkUtils.updatePersistentPath(offsetPath, max.toString , JavaConversions.bufferAsJavaList(acls))
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
