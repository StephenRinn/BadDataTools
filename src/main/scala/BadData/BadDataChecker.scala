package BadData

import java.io.File
import java.nio.file.Paths
import scala.io.Source
import scala.util.{Try, Using}

/**
 * This object is specifically to create psql queries to find bad data.
 *
 * In this case bad data is data which can not be null as it is not an option.
 * The early schemas did not enforce not null.
 *
 * TODO Scaladoc methods
 */
class BadDataChecker{
  private def readFile(file: File): Option[String] = {
    Using(Source.fromFile(file)) { sourceFile => sourceFile.mkString}.toOption
  }

  private def getColumns(tableDefinition: String): Seq[String] = {
    val columnRegex = """\bcolumn\[(?!Option)([^\]]+)\]\("([^"]+)"""".r
    columnRegex.findAllMatchIn(tableDefinition).map(_.group(2)).toSeq
  }

  private def getSchemaTableName(tableDefinition: String): Option[String] = {
    Try {
      val tableRegex = "(Some|Option)\\(\"[a-z,A-Z]*\"\\),*\r*\n*\\s*[_tableName = ]*\"[a-z,A-_]*\"".r
      val firstMatchTable = tableRegex.findFirstIn(tableDefinition)
      val firstTableSplit = firstMatchTable.map(st => st.split("\""))
      firstTableSplit.get(1) ++ "." ++ firstTableSplit.get(3)
    }.toOption
  }

  private def createQuery(tableName: String, columns: Seq[String], outputPath: Option[String] = None): String = {
    val select = (columns :+ "*").mkString(", ")
    val where = columns.map(col => s"""$col IS NULL""").mkString(" OR ")
    outputPath match {
      case Some(output) =>
        s"""COPY (SELECT $select FROM $tableName WHERE $where) TO '$output$tableName' CSV HEADER;"""
      case None =>
        s"""SELECT $select FROM $tableName WHERE $where"""
    }
  }

  def resourceQueryBuilder(outputPath: Option[String] = None): Unit = {
    val uri = getClass.getClassLoader.getResource("")
    val path = uri.getPath
    val directory = Paths.get(path).toFile
    println("\n TESTING V2 Resource\n")
    directory match {
      case folder if folder.exists() || folder.isDirectory =>
        folder.listFiles().foreach{ file =>
          readFile(file).foreach{ definitionString =>
            val tableName = getSchemaTableName(definitionString)
            val columns = getColumns(definitionString)
            tableName.map{ name =>
              println {
                createQuery(name, columns, outputPath)
              }
            }
          }
        }
      case _ => None
    }
  }

  def queryBuilder(path: String, outputPath: Option[String] = None): Unit = {
    val directory = Paths.get(path).toFile
    println("\n TESTING V2 Dir\n")
    directory match {
      case folder if folder.exists() && folder.isDirectory =>
        folder.listFiles().foreach{ file =>
          readFile(file).foreach{ definitionString =>
            for{
              tableName <- getSchemaTableName(definitionString)
            } {
              val columns = getColumns(definitionString)
              println(createQuery(tableName, columns, outputPath))}
            }
          }
      case file if file.exists() && file.isFile =>
        readFile(file).foreach{ definitionString =>
          for{
            tableName <- getSchemaTableName(definitionString)
          } {
            val columns = getColumns(definitionString)
            println(createQuery(tableName, columns, outputPath))}
        }
      case _ => None
    }
  }
}


