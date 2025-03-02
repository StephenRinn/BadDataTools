import BadData._

/**
 * To use, uncomment method you want to run, then run main.
 */
object main {
  private val badData = new BadDataCheck

  def main(args: Array[String]): Unit = {
    //runBadData(inputDir = ???, outputDir = ???)
    //runBadDataResourceFile(outputDir = ???)
    Right(true)
  }

  /**
   * Creates query to look for bad data from table files
   *
   * PLEASE READ the run function scala docs before using!
   *
   * @param inputDir  Directory where the copied tables are located
   * @param outputDir Directory the bad data will be exported to
   */
  def runBadData(inputDir: String, outputDir: String): Unit = {
    badData.run(inputDir = inputDir, outputDir = outputDir)
  }

  /**
   * Creates query to look for bad data from table files
   *
   * PLEASE READ the run function scala docs before using!
   *
   * @param outputDir Directory the bad data will be exported to
   */
  def runBadDataResourceFile(outputDir: String): Unit = {
    badData.checkResource(outputDir)
  }
}