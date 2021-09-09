import java.io.{File, FileWriter, PrintWriter}

object OutputCombinerTester extends App {

  def outputCombiner(inPath: String, outPath: String, title: String, header: Boolean = true): Unit = {
    println(s"Moving temp data files to: $outPath/$title.csv")

    // Opening the input directory
    val directory = new File(inPath)

    // Creating a list of all .csv files in the input directory
    val csv_files = directory
      .listFiles()
      .filter(_.toString.endsWith(".csv"))

    // creating a file writer object to write each partial csv into one temp file
    // NOTE: here a .txt file is being used for coalescing the lines for easier formatting
    val file = new File(s"$inPath/temp.txt")
    val writer = new PrintWriter(new FileWriter(file))

    // whether or not the csv files have headers, the first needs to be written
    // line for line (i.e. if it has headers, the headers need to be written)
    val bufferedSource = scala.io.Source.fromFile(csv_files(0))
    val lines = bufferedSource.getLines()
    for (line <- lines) {
      writer.write(line)
      writer.write('\n')
    }
    bufferedSource.close()

    // now loop through every line in every csv_file other than the first one
    for (csv_file <- csv_files.drop(1)) {
      // opening the file as a buffered source, reading the lines from it, then writing
      // the lines to the buffered writer object
      val bufferedSource = scala.io.Source.fromFile(csv_file)

      // if the header flag is active, then the first line of every csv file can be
      // dropped/skipped, otherwise just write every line to the composite
      if (header) {
        for (line <- bufferedSource.getLines().drop(1)) {
          writer.write(line)
          writer.write('\n')
        }
      } else {
        for (line <- bufferedSource.getLines()) {
          writer.write(line)
          writer.write('\n')
        }
      }
      bufferedSource.close()
    }
    writer.close()

    // creating the desired output directory then moving the temp file to that directory
    // and renaming the temp file to the desired title
    new File(outPath).mkdirs()
    val temp_output = new File(s"$inPath/temp.txt")
    temp_output.renameTo(new File(s"$outPath/$title.csv"))
  }

  outputCombiner("output/temp/Q2_results", "output/question_02", "results_alt", header = true)
}
