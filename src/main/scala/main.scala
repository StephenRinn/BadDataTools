import BadData._

object main {
  def main(args: Array[String]): Unit = {
    //runBadData(inputDir = ???, outputDir = ???)
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
    val badData = new BadDataCheck()
    badData.run(inputDir = inputDir, outputDir = outputDir)
  }
}