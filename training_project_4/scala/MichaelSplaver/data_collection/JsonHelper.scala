package MichaelSplaver.data_collection

import java.io.PrintWriter

import net.liftweb.json.{DefaultFormats, JValue, parse}

import scala.io.Source

object JsonHelper {

  private implicit val formats: DefaultFormats.type = DefaultFormats

  def getValue(filePath: String, key: String): String = {
    val configSource = Source.fromFile(filePath)
    val config: JValue = parse(configSource.mkString)
    val value = (config \ key).extractOpt[String].get
    configSource.close()
    value
  }

  def writeToFile(fileName: String, json: String): Unit = {
    var writer: PrintWriter = null
    try {
      writer = new PrintWriter(fileName)
      writer.write(json)
    }
    catch { case e: Exception => e.printStackTrace() }
    finally {
      writer.close()
    }
  }
}
