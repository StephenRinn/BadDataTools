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
   * {{{def foo = column[Option[String]]("foo")}}}
   *
   * @param args Command-line arguments:
   *             -  inputPath - Path to the directory containing all table definitions
   *                            or to Single table definition file
   *             -  outputDir - Path generated query will write results to
   *
   * @example
   * {{{
   *   scala BadDataTools --inputPath ./foo --outputDir ./foo/bar
   * }}}
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

    val outputDir = getArg("outputDir")
    val pathInput = getArg("inputPath")

    badDataChecker.resourceQueryBuilder(outputPath = outputDir)
    // Only perform queryBuilder if a path is defined
    pathInput match {
      case Some(path) =>
        badDataChecker.queryBuilder(path = path, outputPath = outputDir)
      case None => -1
    }
  }
}
