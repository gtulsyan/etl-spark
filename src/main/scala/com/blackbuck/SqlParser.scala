package com.blackbuck

import gudusoft.gsqlparser._
import gudusoft.gsqlparser.nodes._
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement


/**
  * Created by gopeshtulsyan on 20/11/17 at 3:28 PM.
  */
object SqlParser {

  case class SQLDataType(name: String, dataType: String, isNotNull: Boolean) {

    override def toString: String = {
      s"""{
         |   name: $name,
         |   type: $dataType
         |   isNotNull: $isNotNull
         |}""".stripMargin
    }
  }

  val sqlparser = new TGSqlParser(EDbVendor.dbvmysql)


  def parseCreateStatement(stmt: String): Tuple2[String, Array[SQLDataType]] = {
    sqlparser.sqltext = stmt
    val ret = sqlparser.parse
    var sQLDataType: Tuple2[String, Array[SQLDataType]] = null
    if (ret == 0) {
      var i = 0
      while ( {
        i < sqlparser.sqlstatements.size
      }) {
        if (sqlparser.sqlstatements.get(i).sqlstatementtype.equals(ESqlStatementType.sstcreatetable)) {
          sQLDataType = analyzeCreateTableStmt(sqlparser.sqlstatements.get(i).asInstanceOf[TCreateTableSqlStatement])
        }
        i += 1;
        i - 1
      }
    }
    else {
      System.out.println(sqlparser.getErrormessage)
    }
    sQLDataType
  }

  protected def analyzeCreateTableStmt(pStmt: TCreateTableSqlStatement): Tuple2[String, Array[SQLDataType]] = {
    //    System.out.println("Table Name:" + pStmt.getTargetTable.toString)
    //    System.out.println("Columns:")
    var column: TColumnDefinition = null
    val listTypes: Array[SQLDataType] = new Array[SQLDataType](pStmt.getColumnList.size())
    var i = 0
    while ( {
      i < pStmt.getColumnList.size
    }) {
      column = pStmt.getColumnList.getColumn(i)

            var notNull = false
            if (column.getConstraints != null) {
              var j = 0
              while ( {
                j < column.getConstraints.size
              }) {
                if(column.getConstraints.getConstraint(j).getConstraint_type.equals(EConstraintType.notnull)){
                  notNull = true
                }

                  j += 1
                  j - 1

              }

            }
      listTypes.update(i, SQLDataType(column.getColumnName.toString, column.getDatatype.toString, notNull))
        i += 1
        i - 1
    }
    Tuple2(pStmt.getTargetTable.toString, listTypes)
    //    if (pStmt.getTableConstraints.size > 0) {
    //      System.out.println("\toutline constraints:")
    //      var i = 0
    //      while ( {
    //        i < pStmt.getTableConstraints.size
    //      }) {
    //        printConstraint(pStmt.getTableConstraints.getConstraint(i), true)
    //        System.out.println("")
    //
    //        {
    //          i += 1; i - 1
    //        }
    //      }
    //    }
  }
}

//  protected def analyzeStmt(stmt: TCustomSqlStatement): Unit = {
//    stmt.sqlstatementtype match {
//      case sstupdate =>
//        analyzeUpdateStmt(stmt.asInstanceOf[TUpdateSqlStatement])
//
//      case sstcreatetable =>
//        analyzeCreateTableStmt(stmt.asInstanceOf[TCreateTableSqlStatement])
//
//      case sstaltertable =>
//        analyzeAlterTableStmt(stmt.asInstanceOf[TAlterTableStatement])
//
//      case _ =>
//        System.out.println(stmt.sqlstatementtype.toString)
//    }
//  }
//
//  protected def analyzeUpdateStmt(pStmt: TUpdateSqlStatement): Unit = {
//    System.out.println("Table Name:" + pStmt.getTargetTable.toString)
//    System.out.println("set clause:")
//    var i = 0
//    while ( {
//      i < pStmt.getResultColumnList.size
//    }) {
//      val resultColumn = pStmt.getResultColumnList.getResultColumn(i)
//      val expression = resultColumn.getExpr
//      System.out.println("\tcolumn:" + expression.getLeftOperand.toString + "\tvalue:" + expression.getRightOperand.toString)
//
//      {
//        i += 1; i - 1
//      }
//    }
//    if (pStmt.getWhereClause != null) System.out.println("where clause:\n" + pStmt.getWhereClause.getCondition.toString)
//  }



//  protected def printConstraint(constraint: TConstraint, outline: Boolean): Unit = {
//    if (constraint.getConstraintName != null) System.out.println("\t\tconstraint name:" + constraint.getConstraintName.toString)
//    constraint.getConstraint_type match {
//      case notnull =>
//        System.out.println("\t\tnot null")
//
//      case primary_key =>
//        System.out.println("\t\tprimary key")
//        if (outline) {
//          var lcstr = ""
//          if (constraint.getColumnList != null) {
//            var k = 0
//            while ( {
//              k < constraint.getColumnList.size
//            }) {
//              if (k != 0) lcstr = lcstr + ","
//              lcstr = lcstr + constraint.getColumnList.getElement(k).toString
//
//              {
//                k += 1; k - 1
//              }
//            }
//            System.out.println("\t\tprimary key columns:" + lcstr)
//          }
//        }
//
//      case unique =>
//        System.out.println("\t\tunique key")
//        if (outline) {
//          var lcstr = ""
//          if (constraint.getColumnList != null) {
//            var k = 0
//            while ( {
//              k < constraint.getColumnList.size
//            }) {
//              if (k != 0) lcstr = lcstr + ","
//              lcstr = lcstr + constraint.getColumnList.getElement(k).toString
//
//              {
//                k += 1; k - 1
//              }
//            }
//          }
//          System.out.println("\t\tcolumns:" + lcstr)
//        }
//
//      case check =>
//        System.out.println("\t\tcheck:" + constraint.getCheckCondition.toString)
//
//      case foreign_key =>
//      case reference =>
//        System.out.println("\t\tforeign key")
//        if (outline) {
//          var lcstr = ""
//          if (constraint.getColumnList != null) {
//            var k = 0
//            while ( {
//              k < constraint.getColumnList.size
//            }) {
//              if (k != 0) lcstr = lcstr + ","
//              lcstr = lcstr + constraint.getColumnList.getElement(k).toString
//
//              {
//                k += 1; k - 1
//              }
//            }
//          }
//          System.out.println("\t\tcolumns:" + lcstr)
//        }
//        System.out.println("\t\treferenced table:" + constraint.getReferencedObject.toString)
//        if (constraint.getReferencedColumnList != null) {
//          var lcstr = ""
//          var k = 0
//          while ( {
//            k < constraint.getReferencedColumnList.size
//          }) {
//            if (k != 0) lcstr = lcstr + ","
//            lcstr = lcstr + constraint.getReferencedColumnList.getObjectName(k).toString
//
//            {
//              k += 1; k - 1
//            }
//          }
//          System.out.println("\t\treferenced columns:" + lcstr)
//        }
//
//      case _ =>
//
//    }
//  }
//
//  protected def analyzeAlterTableStmt(pStmt: TAlterTableStatement): Unit = {
//    System.out.println("Table Name:" + pStmt.getTableName.toString)
//    System.out.println("Alter table options:")
//    var i = 0
//    while ( {
//      i < pStmt.getAlterTableOptionList.size
//    }) {
//      printAlterTableOption(pStmt.getAlterTableOptionList.getAlterTableOption(i))
//
//      {
//        i += 1; i - 1
//      }
//    }
//  }
//
//  protected def printAlterTableOption(ato: TAlterTableOption): Unit = {
//    System.out.println(ato.getOptionType)
//    ato.getOptionType match {
//      case EAlterTableOptionType.AddColumn =>
//        printColumnDefinitionList(ato.getColumnDefinitionList)
//
//      case EAlterTableOptionType.ModifyColumn =>
//        printColumnDefinitionList(ato.getColumnDefinitionList)
//
//      case EAlterTableOptionType.AlterColumn =>
//        System.out.println(ato.getColumnName.toString)
//
//      case EAlterTableOptionType.DropColumn =>
//        System.out.println(ato.getColumnName.toString)
//
//      case EAlterTableOptionType.SetUnUsedColumn => //oracle
//
//        printObjectNameList(ato.getColumnNameList)
//
//      case EAlterTableOptionType.DropUnUsedColumn =>
//
//      case EAlterTableOptionType.DropColumnsContinue =>
//
//      case EAlterTableOptionType.RenameColumn =>
//        System.out.println("rename " + ato.getColumnName.toString + " to " + ato.getNewColumnName.toString)
//
//      case EAlterTableOptionType.ChangeColumn => //MySQL
//
//        System.out.println(ato.getColumnName.toString)
//        printColumnDefinitionList(ato.getColumnDefinitionList)
//
//      case EAlterTableOptionType.RenameTable =>
//        System.out.println(ato.getColumnName.toString)
//
//      case EAlterTableOptionType.AddConstraint =>
//        printConstraintList(ato.getConstraintList)
//
//      case EAlterTableOptionType.AddConstraintIndex =>
//        if (ato.getColumnName != null) System.out.println(ato.getColumnName.toString)
//        printObjectNameList(ato.getColumnNameList)
//
//      case EAlterTableOptionType.AddConstraintPK =>
//      case EAlterTableOptionType.AddConstraintUnique =>
//      case EAlterTableOptionType.AddConstraintFK =>
//        if (ato.getConstraintName != null) System.out.println(ato.getConstraintName.toString)
//        printObjectNameList(ato.getColumnNameList)
//
//      case EAlterTableOptionType.ModifyConstraint =>
//        System.out.println(ato.getConstraintName.toString)
//
//      case EAlterTableOptionType.RenameConstraint =>
//        System.out.println("rename " + ato.getConstraintName.toString + " to " + ato.getNewConstraintName.toString)
//
//      case EAlterTableOptionType.DropConstraint =>
//        System.out.println(ato.getConstraintName.toString)
//
//      case EAlterTableOptionType.DropConstraintPK =>
//
//      case EAlterTableOptionType.DropConstraintFK =>
//        System.out.println(ato.getConstraintName.toString)
//
//      case EAlterTableOptionType.DropConstraintUnique =>
//        if (ato.getConstraintName != null) { //db2
//          System.out.println(ato.getConstraintName)
//        }
//        if (ato.getColumnNameList != null) printObjectNameList(ato.getColumnNameList)
//
//      case EAlterTableOptionType.DropConstraintCheck =>
//        System.out.println(ato.getConstraintName)
//
//      case EAlterTableOptionType.DropConstraintPartitioningKey =>
//
//      case EAlterTableOptionType.DropConstraintRestrict =>
//
//      case EAlterTableOptionType.DropConstraintIndex =>
//        System.out.println(ato.getConstraintName)
//
//      case EAlterTableOptionType.DropConstraintKey =>
//        System.out.println(ato.getConstraintName)
//
//      case EAlterTableOptionType.AlterConstraintFK =>
//        System.out.println(ato.getConstraintName)
//
//      case EAlterTableOptionType.AlterConstraintCheck =>
//        System.out.println(ato.getConstraintName)
//
//      case EAlterTableOptionType.CheckConstraint =>
//
//      case EAlterTableOptionType.OraclePhysicalAttrs =>
//      case toOracleLogClause =>
//      case EAlterTableOptionType.OracleTableP =>
//      case EAlterTableOptionType.MssqlEnableTrigger =>
//      case EAlterTableOptionType.MySQLTableOptons =>
//      case EAlterTableOptionType.Db2PartitioningKeyDef =>
//      case EAlterTableOptionType.Db2RestrictOnDrop =>
//      case EAlterTableOptionType.Db2Misc =>
//      case EAlterTableOptionType.Unknown =>
//
//    }
//  }
//
//  protected def printObjectNameList(objList: TObjectNameList): Unit = {
//    var i = 0
//    while ( {
//      i < objList.size
//    }) {
//      System.out.println(objList.getObjectName(i).toString)
//
//      {
//        i += 1; i - 1
//      }
//    }
//  }
//
//  protected def printColumnDefinitionList(cdl: TColumnDefinitionList): Unit = {
//    var i = 0
//    while ( {
//      i < cdl.size
//    }) {
//      System.out.println(cdl.getColumn(i).getColumnName)
//
//      {
//        i += 1; i - 1
//      }
//    }
//  }
//
//  protected def printConstraintList(cnl: TConstraintList): Unit = {
//    var i = 0
//    while ( {
//      i < cnl.size
//    }) {
//      printConstraint(cnl.getConstraint(i), true)
//
//      {
//        i += 1; i - 1
//      }
//    }
//  }

