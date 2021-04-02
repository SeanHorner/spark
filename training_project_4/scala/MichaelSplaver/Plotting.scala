package MichaelSplaver

import java.io.File

import com.cibo.evilplot.plot.aesthetics.DefaultTheme.defaultTheme
import com.cibo.evilplot.plot.{Bar, BarChart}
import org.apache.spark.sql.DataFrame

object Plotting {

  def plotBar(df: DataFrame, title: String, fullPath: String): Unit = {
    // Convert dataframe into sequence of doubles for axes
    val seq = df.collect().map(row => row.toSeq.map(_.toString)).toSeq.flatten

    var yAxis = Seq[Double]()
    var xAxis = Seq[String]()

    // Populate x-axis and y-axis
    for (i <- 0 until seq.length) {
      if (i % 2 == 0) {
        xAxis = xAxis :+ (seq(i))
      }
    }
    for (i <- 0 until seq.length) {
      if (i % 2 != 0) {
        yAxis = yAxis :+ seq(i).toDouble
      }
    }

    // Plot a bar chart
    BarChart.custom(yAxis.map(Bar.apply), spacing = Some(10))
      .title(title)
      .standard(xLabels = xAxis)
      .ybounds(lower = 0)
      .render()
      .write(new File(fullPath))
  }
}
