package com.blackbuck

/**
  * Created by gopeshtulsyan on 15/11/17 at 11:09 AM.
  */
private[blackbuck] sealed trait Configuration

private[blackbuck] object Configurations {

  case class DBConfiguration(database: String,
                             hostname: String,
                             portNo: Int,
                             userName: String,
                             password: String,
                             preLoadCmd:Option[String] = None,
                             postLoadCmd:Option[String] = None) extends Configuration {

    override def toString: String = {
      s"""{
         |   Database Type: $database,
         |   hostname: $hostname,
         |   port no: $portNo,
         |   username: $userName,
         |   password: $password
         |}""".stripMargin
    }
  }

  case class S3Config(s3Location: String,
                      accessKey: String,
                      secretKey: String) extends Configuration




}