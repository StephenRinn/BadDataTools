package BadData

import java.io.{BufferedReader, File, FileReader}
import scala.collection.mutable

/**
 * This object is specifically to create psql queries to find bad data.
 *
 * In this case bad data is data which can not be null as it is not an option.
 * The early schemas did not enforce not null, so we occasionally have bad data
 * inserted by either salesforce users, or engineer mistake.
 *
 * To use -
 *  1.  Set variables that are computer specific
 *      a.  dir               - Where the tables to parse are
 *      b.  badDataOutputDir  - Where to print the bad data files
 *  2.  Create a copy of all base tables you want to check and place in dir
 *  3.  Run Main method
 *  4.  Instead of running a query in PgAdmin, open a psql query in PGAdmin
 *  5.  Run Query copied from output line
 */
class BadDataCheck(){
  def run(inputDir: String, outputDir: String):Unit ={
    /*
    This is the folder you should copy current DB tables to check for bad data.
    I suggest only pulling a few files in to check for the data to ensure you are
    not bogging down the system.

    THIS IS UNTESTED FOR ALL TABLES AT ONCE -> IF YOU BREAK SOMETHING USING THIS
    ON ALL TABLES I TAKE NO RESPONSIBILITY
     */
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
    bufferedReader.lines().forEach(s => customTableName = customTableName ++ s ++ "\n")
    bufferedReader.close()

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

     Even still, it will only cause a problem if the broken up line is non optional
     or if both of the columns on a single line are non optional.
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
}


