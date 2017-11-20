package com.blackbuck

import com.blackbuck.Configurations.DBConfiguration
import java.sql.{Connection, DriverManager}
import java.util.Properties
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer


/**
  * Created by gopeshtulsyan on 15/11/17 at 11:07 AM.
  */
object RedShiftUtil {


  def getConnection(conf: DBConfiguration): Connection = {
    val connectionProps = new Properties()
    connectionProps.put("user", conf.userName)
    connectionProps.put("password", conf.password)
    connectionProps.put("user","lookerread")
    connectionProps.put("password","B1hiewd86yu")
    val connectionString = getJDBCUrl(conf)
    Class.forName("com.amazon.redshift.jdbc.Driver")
    DriverManager.getConnection(connectionString, connectionProps)
  }

  def getJDBCUrl(conf: DBConfiguration): String = {
    s"jdbc:redshift://${conf.hostname}:${conf.portNo}/${conf.db}"
  }


  def execute(query: String): Unit = {
    try {
      val conf: DBConfiguration = DBConfiguration("prod_bb_replica","","jarvis-prod.cdw4xlqaiotu.ap-southeast-1.redshift.amazonaws.com",
        5439,"lookerread","B1hiewd86yu")
      val con = getConnection(conf)
      val stmt = con.createStatement()
      stmt.execute(query)
      stmt.close()
      con.close()
    }catch {
      case foo: Exception =>
        println("Some error occurred while executing -----------------------------------"+foo)
    }

  }

  def getUpsertQuery(table: String, columns: Array[StructField]): String ={
    val newtable = table.concat("_staging")

    var cols = new ListBuffer[String]()
    for (elem <- columns) {
      cols += s"${elem.name} = s.${elem.name}"
    }

    val columnsAdded = cols.toArray.mkString(",")
    s"""
       |begin;
       |
       |CREATE TABLE $newtable (LIKE $table);
       |
       |copy $newtable from 's3://kafkaredshift/temp/$table/'
       |iam_role 'arn:aws:iam::735317561518:role/kafka_redshift'
       |format as json 'auto'
       |timeformat as 'auto'
       |dateformat as 'auto';
       |
       |update ${table} set ${columnsAdded}
       |from ${newtable} s where ${table}.id = s.id;
       |
       |INSERT INTO ${table}
       |SELECT s.* FROM ${newtable} s LEFT JOIN ${table}
       |ON s.id = ${table}.id
       |WHERE ${table}.id IS NULL;
       |
       |DROP TABLE ${newtable};
       |
       |END;
     """.stripMargin
  }

  def getCreateQuery(newTable: String, oldTable :String): String = {
    s"""
       |create table IF NOT EXISTS ${newTable} (LIKE ${oldTable});
     """.stripMargin
  }

}
