import BadData._

object main {
  private val badDataChecker = new BadDataChecker

  /**
   * BadDataTools is designed to create sql statements by parsing
   * table definitions to quickly find null values which are not set
   * as options.
   *
   * BadDataTools will also check the resource file "BadDataTableTemp" if it exists
   * and follows the format provided
   *
   * TODO find better solution than printing the query
   *
   * This tool was designed to be used with Slick, and creating tables
   * with a format which roughly fits
   *
   * Table -
   * Some|Option ("schemaName"), "tableName"
   *
   * Column -
   * def foo = column[Option[String]]("foo")
   *
   * @param args Command-line arguments:
   *             -  inputDir - Path to the directory containing all table definitions
   *             -  outputDir - Path generated query will write results to
   *             -  absolutePath - Absolute path to single table definition to limit query size
   *
   * Example:
   *    {{{
   *      scala BadDataTools --inputDir ./foo --outputDir ./foo/bar --absolutePath /abs/foobar.scala
   *    }}}
   */
  def main(args: Array[String]): Unit = {
    def getArg(name: String, default: Option[String] = None): Option[String] = {
      val index = args.indexOf(s"--$name")
      index match {
        case -1 => default
        case idx if idx + 1 >= args.length => default
        case idx => Some(args(idx + 1))
      }
    }
    val inputDir = getArg("inputDir")
    val outputDir = getArg("outputDir")
    val absolutePathInput = getArg("absolutePath")

    runBadData(inputDir = inputDir, outputDir = outputDir)
    runBadDataResourceFile(outputDir = outputDir)
    runBadDataSingleFile(outputDir = outputDir, absolutePathInput = absolutePathInput)
  }

  /**
   * Creates query to look for bad data from table files
   *
   * @param inputDir  Directory where the copied tables are located
   * @param outputDir Directory the bad data will be exported to
   */
  private def runBadData(inputDir: Option[String] = None,
                         outputDir: Option[String] = None
                ): Unit = {
    (inputDir,outputDir) match {
      case (Some(input), Some(output)) =>
        println(s"\n\nrunBadData - Setting up Query with inputDir: $input outputDir: $output\n\n")
        badDataChecker.run(inputDir = input, outputDir = output)
        println(s"\n\n END OF QUERY for inputDir: $input outputDir: $output")
      case _ => None
    }
  }

  /**
   * Creates query to look for bad data from table files
   *
   * @param outputDir Directory the bad data will be exported to
   */
  private def runBadDataResourceFile(outputDir: Option[String] = None): Unit = {
    outputDir match {
      case Some(output) =>
        badDataChecker.checkResource(output)
      case None =>
        badDataChecker.checkResource()
    }

  }

  /**
   * Creates a query to find bad data from single input file
   *
   * @param outputDir Directory the bad data will be exported to
   * @param absolutePathInput Absolute path to single table file
   */
  private def runBadDataSingleFile(outputDir: Option[String] = None,
                                   absolutePathInput: Option[String] = None
                                  ): Unit = {
    (absolutePathInput, outputDir) match {
      case (Some(absolutePath),Some(output)) =>
        badDataChecker.checkSingleFile(absolutePath, output)
      case (Some(absolutePath),None) =>
        badDataChecker.checkSingleFile(absolutePath)
      case _ => None
    }

  }
}
