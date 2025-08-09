package BadData

import java.io.{BufferedReader, File, FileReader}
import scala.collection.mutable
import scala.io.Source

/**
 * This object is specifically to create psql queries to find bad data.
 *
 * In this case bad data is data which can not be null as it is not an option.
 * The early schemas did not enforce not null.
 */
class BadDataCheck(){
  def run(inputDir: String, outputDir: String): Unit = {
    val dir = ""
    new File(dir).listFiles().foreach{ file =>
      // Fails on db.scala, excluded in case of accidental copying
      if(file.getName != "db.scala"){
        val name = getName(file)
        //This is the folder the psql query will dump all bad data into.
        val badDataOutputDir = ""
        check(name,badDataOutputDir, file)
      }
    }
  }

  /**
   * Finds the table name from the scala file based on a simple regex.
   *
   * Provided tables must have the name in a form that follows
   * Some|Option ("schemaName"), "tableName"
   *
   * @param file: Table file to parse for possible missing required fields
   * @return      Table name to complete future SQL queries
   */
  def getName(file: File): String = {
    var customTableName = ""

    val bufferedReader = new BufferedReader(new FileReader(file))
    bufferedReader.lines().forEach(s => customTableName += s ++ "\n")
    bufferedReader.close()

    val tableRegex = "(Some|Option)\\(\"[a-z,A-Z]*\"\\),*\r*\n*\\s*[_tableName = ]*\"[a-z,A-_]*\"".r
    val firstMatchTable = tableRegex.findFirstIn(customTableName)
    val firstTableSplit = firstMatchTable.map(st => st.split("\""))
    firstTableSplit.get(1) ++ "." ++ firstTableSplit.get(3)
  }

  private def getNameResource() = {
    var customTableName = ""

    val in = Source.fromResource("BadDataTableTemp")
    in.getLines.foreach(s => customTableName += s ++ "\n")

    val tableRegex = "(Some|Option)\\(\"[a-z,A-Z]*\"\\),*\r*\n*\\s*[_tableName = ]*\"[a-z,A-_]*\"".r
    val firstMatchTable = tableRegex.findFirstIn(customTableName)
    val firstTableSplit = firstMatchTable.map(st => st.split("\""))
    firstTableSplit.get(1) ++ "." ++ firstTableSplit.get(3)
  }

  def check(name: String,
            path: String,
            file: File
           ): Unit= {
    val bufferedReader = new BufferedReader(new FileReader(file))
    val tableName = name
    val optionRegex = "column\\[[a-z,A-Z]*]\\(\".*\".*\\)".r
    val columnRegex = "\".*\"".r
    val sqlNonOptions = new mutable.StringBuilder()
    val findQuery = new mutable.StringBuilder("Select ")
    var columns = List[String]("")
    /*
     Since we don't have to worry about multiple matches on one line I simplified the regex
     however this will fail if we even have multiple lines for a single column, or
     if we have multiple on one line. Not a super simple fix, so I don't want to.
    */
    bufferedReader.lines().forEach { line =>
      val regmatch = optionRegex.findFirstIn(line)
      regmatch.map(matches => columns = columns ++ List(matches))
    }
    columns = columns.drop(1)
    val maybeNonOptionalColumn = columns.map { str =>
      val columnname = columnRegex.findFirstIn(str)
      findQuery ++= columnname.get.dropRight(1).drop(1) + ","
      columnname
    }
    // Gets full object so you can id offending record if PK is optional in code
    findQuery ++= "* "
    maybeNonOptionalColumn.foreach{
      case Some(string) => sqlNonOptions++=(string.substring(1,string.length - 1))
        sqlNonOptions++=" is null OR "
      case None =>
    }
    val orCases = sqlNonOptions.mkString
    val finalSql = orCases.substring(0,orCases.length -3)
    val finalQuery = s"${findQuery.mkString.dropRight(1)} from $tableName where $finalSql"
    println("\\copy (" +finalQuery + s") TO '$path$name' HEADER;")
  }

  /**
   * Version of the checkBadData intended to be used
   * when copying the file contents of a single table is
   * easier than copying all the files to a new dir.
   *
   * Copy the table information (whole file is fine) into the
   * resources.BadDataTableTemp file before running.
   *
   * @param outputPath  This should be a directory for storing the
   *                    output of the sql query created by the method
   */
  def checkResource(outputPath: String)={
    val in = Source.fromResource("BadDataTableTemp")
    val tableName = getNameResource()
    val optionRegex = "column\\[[a-z,A-Z]*]\\(\".*\".*\\)".r
    val columnRegex = "\".*\"".r
    val sqlNonOptions =new mutable.StringBuilder()
    val findQuery = new mutable.StringBuilder("Select ")
    var columns = List[String]("")
    val strings = in.getLines.foreach { line =>
      val regmatch = optionRegex.findFirstIn(line)
      regmatch.map(matches => columns = columns ++ List(matches))
    }
    columns = columns.drop(1)
    val maybeNonOptionalColumn = columns.map { str =>
      val columnname = columnRegex.findFirstIn(str)
      findQuery ++= columnname.get.dropRight(1).drop(1) + ","
      columnname
    }
    // Gets full object so you can id offending record if PK is optional in code
    findQuery ++= "* "
    maybeNonOptionalColumn.foreach{
      case Some(string) => sqlNonOptions++=(string.substring(1,string.length - 1))
        sqlNonOptions++=" is null OR "
      case None =>
    }
    val orCases = sqlNonOptions.mkString
    val finalSql = orCases.substring(0,orCases.length -3)
    val finalQuery = s"${findQuery.mkString.dropRight(1)} from $tableName where $finalSql"
    println("\\copy (" +finalQuery + s") TO '$outputPath$tableName' HEADER;")
  }

  /**
   * Creates a bad data query with an output to the pathOutput directory.
   * Best use when you can quickly grab the absolute path for a single table to
   * quickly check for bad data
   *
   * @param absolutePathInput Table to be parsed for the query
   * @param pathOutput        Where the files will be placed for review
   *                          defaults to /Users/Shared/BadDataOutput/
   */
  def checkSingleFile(absolutePathInput: String,
                      pathOutput: String = "/Users/Shared/BadDataOutput/"
                     ): Unit = {
    val file = new File(absolutePathInput)
    val name = getName(file)
    check(name, pathOutput, file)
  }
}


