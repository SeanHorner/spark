package LiamHood

import org.apache.spark.sql.DataFrame
import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}

import com.cibo.evilplot.colors.{Color, HSL}
import com.cibo.evilplot.numeric.Point
import com.cibo.evilplot.plot.LinePlot._
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import com.cibo.evilplot.plot.renderers.PathRenderer

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object Plots {
  // Plot bar charts using Dataframe results
//  def q14_plots(df: DataFrame, title: String, fname: String): Unit ={
//    val data = df.collect().map(row =>
//      Seq(row(1).toString.toDouble, row(2).toString.toDouble))
//    val x_labels = df.collect().map(_(0).toString).map(_.drop(2))
//    BarChart
//      .stacked(
//        data,
//        labels = Seq("online", "in-person")
//      )
//      .title(s"$title")
//      .xAxis(x_labels)
//      .yAxis()
//      .frame()
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}.png"))
//
//    BarChart
//      .stacked(
//        data.slice(data.length - 18, data.length),
//        labels = Seq("online", "in-person")
//      )
//      .title(s"$title")
//      .xAxis(x_labels.slice(data.length - 18, data.length))
//      .yAxis()
//      .frame()
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}_zoomed.png"))
//  }

  def q14_line_plots(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(1), row(2)).map(_.toString.toDouble))
    val x_labels = df.collect().map(_(0).toString)
    val months = x_labels.map(date_month_to_double(_))
    line_plot(data, months, Seq("Online", "In Person"), "Years", "Number of Events", title, fname, 20)
//    val points: ListBuffer[Seq[Point]] = ListBuffer()
//    for (ii <- 0 to data(0).length-1) {
//      points.append(months.zip(data.map(_(ii)))
//        .toSeq
//        .map(row => Point(row._1, row._2)))
//    }
//
//    val fsize = 20
//
//    Overlay(
//      LinePlot.series(points(0), "Online", colors(0)),
//      LinePlot.series(points(1), "In Person", colors(1))
//    )
//      .xAxis().yAxis()
//      .xGrid().yGrid()
//      .xLabel("Years", size = Some(fsize))
//      .yLabel("Number of Events", size = Some(fsize))
//      .title(s"$title")
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}.png"))
//
//    Overlay(
//      LinePlot.series(points(0).slice(points(0).length - 24, points(0).length), "Online", colors(0)),
//      LinePlot.series(points(1).slice(points(0).length - 24, points(0).length), "In Person", colors(1))
//    )
//      .xAxis().yAxis()
//      .xGrid().yGrid()
//      .xLabel("Years", size = Some(fsize))
//      .yLabel("Number of Events", size = Some(fsize))
//      .title(s"$title")
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}_zoomed.png"))
  }

//  def q11a_plot(df: DataFrame, title: String, fname: String): Unit ={
//    val data = df.collect().map(row => Seq(row(1), row(2), row(3), row(4)).map(_.toString.toDouble))
//    val x_labels = df.collect().map(_(0).toString).map(_.drop(2))
//
//    BarChart
//      .stacked(
//        data,
//        labels = Seq("free", "cash", "paypal", "wepay")
//      )
//      .title(s"$title")
//      .xAxis(x_labels)
//      .yAxis()
//      .frame()
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}.png"))
//
//    BarChart
//      .stacked(
//        data.map(row => Seq(row(1), row(2), row(3))),
//        labels = Seq("cash", "paypal", "wepay")
//      )
//      .title(s"$title")
//      .xAxis(x_labels)
//      .yAxis()
//      .frame()
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}_nf.png"))
//
//    BarChart
//      .stacked(
//        data.slice(data.length - 18, data.length),
//        labels = Seq("free", "cash", "paypal", "wepay")
//      )
//      .title(s"$title")
//      .xAxis(x_labels.slice(data.length - 18, data.length))
//      .yAxis()
//      .frame()
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}_zoomed.png"))
//
//    BarChart
//      .stacked(
//        data.slice(data.length - 18, data.length).map(row => Seq(row(1), row(2), row(3))),
//        labels = Seq("cash", "paypal", "wepay")
//      )
//      .title(s"$title")
//      .xAxis(x_labels.slice(data.length - 18, data.length))
//      .yAxis()
//      .frame()
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}_nf_zoomed.png"))
//  }

  def q11a_line_plot(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(1), row(2), row(3), row(4)).map(_.toString.toDouble))
    val x_labels = df.collect().map(_(0).toString).map(date_month_to_double(_))
//    val x_labels = months_temp.map(_ - months_temp.min)
    val labels = Seq("free", "cash", "paypal", "wepay")
    line_plot(data, x_labels, labels, "Years", "Number of Events", title, fname, 20)
    line_plot(data.map(row => Seq(row(1), row(2), row(3))), x_labels, labels.slice(1, 4), "Years", "Number of Events", s"${title}_nf", s"${fname}_nf", 20)
//    val points: ListBuffer[Seq[Point]] = ListBuffer()
//    for (ii <- 0 to data(0).length-1) {
//      points.append(x_labels.zip(data.map(_(ii)))
//        .toSeq
//        .map(row => Point(row._1, row._2)))
//    }
//
//    var plots: Seq[Plot] = Seq()
//    for (ii <- 0 to 3) {
//      plots = plots :+ LinePlot.series(points(ii), labels(ii), colors(ii))
//    }
//
//    val fsize = 20
//
//    Overlay.fromSeq(
//      plots
//    )
//      .xAxis().yAxis()
//      .xGrid().yGrid()
//      .xLabel("Years", size = Some(fsize))
//      .yLabel("Number of Events", size = Some(fsize))
//      .title(s"$title")
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}.png"))
//
//    Overlay.fromSeq(
//      plots.slice(1, 4)
//    )
//      .xAxis().yAxis()
//      .xGrid().yGrid()
//      .xLabel("Years", size = Some(fsize))
//      .yLabel("Number of Events", size = Some(fsize))
//      .title(s"$title")
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}_nf.png"))
    }

//  def q11b_plot(df: DataFrame, title: String, fname: String): Unit ={
//    val data = df.collect().map(row => row(1).toString.toDouble).toSeq
//    val x_labels = df.collect().map(_(0).toString).map(_.drop(2))
//    BarChart
//      .custom(data.map(Bar.apply))
//      .title(s"$title")
//      .standard(xLabels = x_labels)
//      .render()
//      .write(new File(s"${fname}.png"))
//
//    BarChart
//      .custom(data.slice(data.length - 18, data.length).map(Bar.apply))
//      .title(s"$title")
//      .standard(xLabels = x_labels.slice(data.length - 18, data.length))
//      .render()
//      .write(new File(s"${fname}_zoomed.png"))
//  }

  def q11b_line_plot(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(1), row(2), row(3), row(4)).map(_.toString.toDouble))
    val x_labels = df.collect().map(_(0).toString).map(date_month_to_double(_))
    val labels = Seq("Cash", "PayPal", "WePay", "Average")
    line_plot(data, x_labels, labels, "Years", "Fee [Dollars]", title, fname, 20)
//    val points: ListBuffer[Seq[Point]] = ListBuffer()
//    for (ii <- 0 to data(0).length-1) {
//      points.append(x_labels.zip(data.map(_(ii)))
//        .toSeq
//        .map(row => Point(row._1, row._2)))
//    }
//    var plots: Seq[Plot] = Seq()
//    var plots_zoomed: Seq[Plot] = Seq()
//    for (ii <- 0 to 3) {
//      plots = plots :+ LinePlot.series(points(ii), labels(ii), colors(ii))
//      plots_zoomed = plots_zoomed :+ LinePlot.series(points(ii).slice(points(0).length - 24, points(0).length), labels(ii), colors(ii))
//    }
//
//    val fsize = 20
//
//    Overlay.fromSeq(
//      plots_zoomed
//    )
//      .xAxis().yAxis()
//      .xGrid().yGrid()
//      .xLabel("Years", size = Some(fsize))
//      .yLabel("Fee [Dollars]", size = Some(fsize))
//      .title(s"$title")
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}_zoomed.png"))
//
//    Overlay.fromSeq(
//      plots
//    )
//      .xAxis().yAxis()
//      .xGrid().yGrid()
//      .xLabel("Years", size = Some(fsize))
//      .yLabel("Fee [Dollars]", size = Some(fsize))
//      .title(s"$title")
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}.png"))
  }

//  def q7_plots(df: DataFrame, title: String, fname: String): Unit ={
//    val data = df.collect().map(row => Seq(row(3), row(4), row(5), row(6), row(7), row(8)).map(_.toString.toDouble))
//    val x_labels = df.collect().map(_(0).toString)
//
//    BarChart
//      .stacked(
//        data,
//        labels = Seq("Career & Business (2)", "Education (6)", "Movements (4)", "Hobbies & Crafts(15)", "Writing (36)")
//      )
//      .title(s"$title")
//      .xAxis(x_labels)
//      .yAxis()
//      .frame()
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}.png"))
//
//    BarChart
//      .stacked(
//        data.slice(data.length - 18, data.length),
//        labels = Seq("Career & Business (2)", "Education (6)", "Movements (4)", "Hobbies & Crafts(15)", "Writing (36)")
//      )
//      .title(s"$title")
//      .xAxis(x_labels.slice(data.length - 18, data.length))
//      .yAxis()
//      .frame()
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}_zoomed.png"))
//  }

  def q7_line_plots(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(3), row(4), row(5), row(7), row(8)).map(_.toString.toDouble))
    val x_labels = df.collect().map(_(0).toString).map(date_month_to_double(_))
    val labels = Seq("Career & Business (2)", "Education (6)", "Movements (4)", "Hobbies & Crafts(15)", "Writing (36)")
    line_plot(data, x_labels, labels, "Years", "Percent of Events with this Topic", title, fname, 20)
//    val points: ListBuffer[Seq[Point]] = ListBuffer()
//    for (ii <- 0 to data(0).length-1) {
//      points.append(x_labels.zip(data.map(_(ii)))
//        .toSeq
//        .map(row => Point(row._1, row._2)))
//    }
//
//    var plots: Seq[Plot] = Seq()
//    for (ii <- 0 to 4) {
//      plots = plots :+ LinePlot.series(points(ii), labels(ii), colors(ii))
//    }
//
//    val fsize = 20
//
//    Overlay.fromSeq(
//      plots
//    )
//      .xAxis().yAxis()
//      .xGrid().yGrid()
//      .xLabel("Years", size = Some(fsize))
//      .yLabel("Percent of Events with this Topic", size = Some(fsize))
//      .title(s"$title")
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}.png"))
//
//
//    var plots_zoomed: Seq[Plot] = Seq()
//    for (ii <- 0 to 4) {
//      plots_zoomed = plots_zoomed :+ LinePlot.series(points(ii).slice(points(0).length - 24, points(0).length), labels(ii), colors(ii))
//    }
//
//    Overlay.fromSeq(
//      plots_zoomed
//    )
//      .xAxis().yAxis()
//      .xGrid().yGrid()
//      .xLabel("Years", size = Some(fsize))
//      .yLabel("Percent of Events with this Topic", size = Some(fsize))
//      .title(s"$title")
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}_zoomed.png"))
    }

  def q7_wa_line_plots(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(3), row(4), row(6)).map(_.toString.toDouble))
    val x_labels = df.collect().map(_(0).toString).map(date_month_to_double(_))
    val labels = Seq("Career & Business (2)", "Sports & Fitness (32)", "Education (6)")
    line_plot(data, x_labels, labels, "Years", "Percent of Events with this Topic", title, fname, 20)
//
//    val points: ListBuffer[Seq[Point]] = ListBuffer()
//    for (ii <- 0 to data(0).length-1) {
//      points.append(x_labels.zip(data.map(_(ii)))
//        .toSeq
//        .map(row => Point(row._1, row._2)))
//    }
//
//    var plots: Seq[Plot] = Seq()
//    for (ii <- 0 to 2) {
//      plots = plots :+ LinePlot.series(points(ii), labels(ii), colors(ii))
//    }
//
//    val fsize = 20
//
//    Overlay.fromSeq(
//      plots
//    )
//      .xAxis().yAxis()
//      .xGrid().yGrid()
//      .xLabel("Years", size = Some(fsize))
//      .yLabel("Percent of Events with this Topic", size = Some(fsize))
//      .title(s"$title")
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}.png"))
//
//    var plots_zoomed: Seq[Plot] = Seq()
//    for (ii <- 0 to 2) {
//      plots_zoomed = plots_zoomed :+ LinePlot.series(points(ii).slice(points(0).length - 24, points(0).length), labels(ii), colors(ii))
//    }
//
//    LinePlot.series(points(0).slice(points(0).length - 24, points(0).length), labels(0), colors(0))
//    Overlay.fromSeq(
//      plots_zoomed
//    )
//      .xAxis().yAxis()
//      .xGrid().yGrid()
//      .xLabel("Years", size = Some(fsize))
//      .yLabel("Percent of Events with this Topic", size = Some(fsize))
//      .title(s"$title", size = Some(fsize))
//      .bottomLegend()
//      .render()
//      .write(new File(s"${fname}_zoomed.png"))
  }

  def q7_ny_line_plots(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(3), row(4), row(5)).map(_.toString.toDouble))
    val x_labels = df.collect().map(_(0).toString).map(date_month_to_double(_))
    val labels = Seq("LGBTQ (12)", "Career & Business (2)", "Education (6)")
    line_plot(data, x_labels, labels, "Years", "Percent of Events with this Topic", title, fname, 20)
  }

  def total_events_line_plots(df: DataFrame, title: String, fname: String): Unit ={
    val data = df.collect().map(row => Seq(row(1)).map(_.toString.toDouble))
    val x_labels = df.collect().map(_(0).toString).map(date_month_to_double(_))
    val labels = Seq("Events")
    line_plot(data, x_labels, labels, "Years", "Event Count", title, fname, 20)
  }

  def date_month_to_double(date: String): Double = {
    val pattern: Regex = "(\\d+)-(\\d+)".r
    date match {
      case pattern(year, month) if year == "2021" => 0
      case pattern(year, month) if year < "2021" => ((year.toInt - 2020)*12 + (month.toInt - 12) - 1).toDouble / 12
      case _ => 100
    }
  }

  def line_plot(data: Seq[Seq[Double]], x_vals: Seq[Double], labels: Seq[String], xlabel: String, ylabel: String, title: String, name: String, fsize: Int): Unit ={
    val n = labels.length
    val colors = Color.getGradientSeq(n)
    val points: ListBuffer[Seq[Point]] = ListBuffer()
    for (ii <- 0 to data(0).length-1) {
      points.append(x_vals.zip(data.map(_(ii)))
        .toSeq
        .map(row => Point(row._1, row._2)))
    }

    var plots: Seq[Plot] = Seq()
    var plots_zoomed: Seq[Plot] = Seq()
    for (ii <- 0 to n-1) {
      plots = plots :+ LinePlot.series(points(ii), labels(ii), colors(ii))
      plots_zoomed = plots_zoomed :+ LinePlot.series(points(ii).slice(points(0).length - 48, points(0).length), labels(ii), colors(ii))
    }

    Overlay.fromSeq(
      plots
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .xLabel(xlabel, size = Some(fsize))
      .yLabel(ylabel, size = Some(fsize))
      .title(s"$title")
      .bottomLegend()
      .render()
      .write(new File(s"${name}.png"))

    Overlay.fromSeq(
      plots_zoomed
    )
      .xAxis().yAxis()
      .xGrid().yGrid()
      .xLabel(xlabel, size = Some(fsize))
      .yLabel(ylabel, size = Some(fsize))
      .title(s"$title", size = Some(fsize))
      .bottomLegend()
      .render()
      .write(new File(s"${name}_zoomed.png"))
  }
}
