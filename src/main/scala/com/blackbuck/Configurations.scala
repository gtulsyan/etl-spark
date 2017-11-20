package com.blackbuck

/**
  * Created by gopeshtulsyan on 15/11/17 at 11:09 AM.
  */
private[blackbuck] sealed trait Configuration

private[blackbuck] object Configurations {

  case class DBConfiguration(db: String,
                             schema: String,
                             hostname: String,
                             portNo: Int,
                             userName: String,
                             password: String,
                             preLoadCmd:Option[String] = None,
                             postLoadCmd:Option[String] = None)  {

    override def toString: String = {
      s"""{
         |
         |   Database Name: $db,
         |
         |   Schema: $schema
         |}""".stripMargin
    }
  }

  case class S3Config(s3Location: String,
                      accessKey: String,
                      secretKey: String) extends Configuration




}