import BadData._

/**
 * To use, uncomment method you want to run, then run main.
 *
 * TODO fix inputs and outputs with env vars or default values to remove need for commenting
 */
object main {
  private val badData = new BadDataCheck

  def main(args: Array[String]): Unit = {
    //runBadData(inputDir = ???, outputDir = ???)
    //runBadDataResourceFile(outputDir = ???)
    //runBadDataSingleFile(absolutePathInput = ???)
    //runBadDataSingleFile(outputDir = ???, absolutePathInput = ???)
    Right(true)
  }

  /**
   * Creates query to look for bad data from table files
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
   * @param outputDir Directory the bad data will be exported to
   */
  def runBadDataResourceFile(outputDir: String): Unit = {
    badData.checkResource(outputDir)
  }

  def runBadDataSingleFile(outputDir: String, absolutePathInput: String) = {
    badData.checkSingleFile(absolutePathInput, outputDir)
  }

  def runBadDataSingleFile(absolutePathInput: String) = {
    badData.checkSingleFile(absolutePathInput)
  }
}
