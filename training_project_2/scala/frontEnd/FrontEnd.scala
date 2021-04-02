package frontEnd

import java.io.{BufferedWriter, File, FileWriter}

import analyses.analysisEngine
import com.danielasfregola.twitter4s.TwitterRestClient
import net.liftweb.json.{DefaultFormats, parse}
import net.liftweb.json.Serialization.write

import scala.util.{Failure, Success}
import twitterdao.TwitterDAO
import twitterobjects.Tweet

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

object FrontEnd {
  private val restClient = TwitterRestClient()
  private val MINUTES_BETWEEN_LOOPS = 5
  private val twitterDAO = new TwitterDAO(System.getenv("BEARER_TOKEN"))
  private val analysisEngine = new analysisEngine()

  def main(args: Array[String]): Unit = {
    mainLoop()
  }

  /**
   * A simple function that will make the authenticated user send a tweet.
   * @param message String content of the tweet.
   * @param replyingToID Tweet ID to be used in the event of making a reply.
   */
  def tweetOut(message: String, replyingToID: Option[Long] = None): Unit = {
    restClient.createTweet(status = message, in_reply_to_status_id = replyingToID)
  }

  /**
   * The main loop of the program. Retrieves every mention of the bot since the
   * last one interpreted, and passes them to the executeQuery function in order
   * of creation. When one completes, the Save File is updated to note that no
   * mention older than it should be obtained from the search query.
   */
  def mainLoop(): Unit = {
    var continueLoop = true
    while (continueLoop) {
      println("Beginning query:")
      val saveData = readSave("saveData.json")
      val mentions = restClient.mentionsTimeline(since_id = Option(saveData.LastMention))
      mentions onComplete {
        case Success(tweets) =>
          val commands = tweets.data.map(x => Tweet(x.id, x.user.get.id, x.text)).reverse
          for (command <- commands) {
            println(s"Interpreting $command...")
            executeQuery(command)
            saveData.LastMention = command.tweetId
            writeSave(saveData, "saveData.json")
          }
        case Failure(exception) =>
          println(exception)
      }
      for (minute <- MINUTES_BETWEEN_LOOPS to 1 by -1) {
        println(s"$minute minute(s) left before the next query...")
        Thread.sleep(1 * 60 * 1000)
      }
    }
  }


  def executeQuery(query: Tweet): Unit = {
    val commandNoArgPattern = """@\S+\s+(\w+)""".r
    val commandPattern = """@\S+\s+(\w+)\s+@?(\w+)""".r
    val TEMP_PATH = "temp"

    var response: Option[String] = None
    query.text match {
      case commandNoArgPattern(cmd) if cmd.equalsIgnoreCase("estimateTweetCount") =>
        println("Building next temp file...")
        twitterDAO.collectStreamedTweets(TEMP_PATH, 1000)
        println("Operating on temp file...")
        response = Option(s"We estimate ${analysisEngine.tweetCountEstimate(TEMP_PATH)} tweets were made in the last 24 hours.")

      case commandPattern(cmd, user) if cmd.equalsIgnoreCase("mostPopularHashtag") =>
        val userObject = twitterDAO.getUser(user)
        writeList(twitterDAO.getUserTimeline(userObject, count = 1000), TEMP_PATH)
        response = Option(analysisEngine.hashtagStripper(TEMP_PATH))

      case commandPattern(cmd, user) if cmd.equalsIgnoreCase("wordCount") =>
        val userObject = twitterDAO.getUser(user)
        writeList(twitterDAO.getUserTimeline(userObject, count = 1000), TEMP_PATH)
        response = Option(analysisEngine.wordCounter(TEMP_PATH))

      case commandPattern(cmd, char) if cmd.equalsIgnoreCase("followingCharacter") =>
        println("Building next temp file...")
        twitterDAO.collectStreamedTweets(TEMP_PATH, 1000)
        println("Operating on temp file...")
        response = Option(analysisEngine.followingCharacter(TEMP_PATH, char.head))

      case commandPattern(cmd, user) if cmd.equalsIgnoreCase("retweetPercentage") =>
        val userObject = twitterDAO.getUser(user)
        writeList(twitterDAO.getUserTimeline(userObject, count = 1000, includeRetweets = true), TEMP_PATH)
        response = Option(s"$user's retweet percentage is ${analysisEngine.retweetPercentage(TEMP_PATH)}%")

      case _ =>
    }
    response match {
      case Some(content) => tweetOut(content, Option(query.tweetId))
      case None =>
    }
  }

  private def writeSave[T](saveData: T, path: String): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val json = write(saveData)

    println(s"Writing to $path")
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(json)
    bw.close()
  }

  private def readSave(path: String): SaveData = {
    implicit val formats: DefaultFormats.type = net.liftweb.json.DefaultFormats
    val source = Source.fromFile(path)
    val json = try parse(source.mkString) finally source.close()
    json.extract[SaveData]
  }

  private def writeList(tweetList: List[Tweet], path: String): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val json = tweetList.map(x => write(x)).mkString("\n")

    println(s"Writing to $path")
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(json)
    bw.close()
  }
}
