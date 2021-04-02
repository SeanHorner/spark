package twitterdao

import java.io.{BufferedReader, InputStreamReader}

import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import net.liftweb.json._
import org.apache.http.NameValuePair
import org.apache.http.message.BasicNameValuePair
import twitterobjects.Tweet

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ArrayBuffer
import scala.sys.exit

class StreamingTwitterDAO(bearerToken: String) {


  /** Streams data and returns a list of Tweet objects
   * @param totalTweets Number of tweets you want to sample capped at 10k 
   * @return            A list of Tweet objects*/
  def getTweetStreamToList(totalTweets: Int): List[Tweet] = {

    if(totalTweets>10000) {
      println("Don't request more than 10000 tweets please")
      exit(1)
    }
    var jsonStrings = new ArrayBuffer[String]()
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream")
    var queryParameters = new ArrayBuffer[NameValuePair]
    queryParameters += new BasicNameValuePair("expansions", "author_id")
    uriBuilder.addParameters(queryParameters.asJava)
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    httpGet.setHeader("Content-Type", "application/json")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    if (null != entity) {
      val reader = new BufferedReader(new InputStreamReader(entity.getContent))
      var line = reader.readLine()
      var lineNumber = 1
      while(line!=null&&lineNumber<=totalTweets) {
        if(line!="") {
          jsonStrings += line
          lineNumber += 1
        }
        line = reader.readLine()
      }
    }
    parseJSONStringListToTweetList(jsonStrings.toList)
  }

  /** Don't worry about this guy he just parses strings */
  private def parseJSONStringListToTweetList(jsonStringList: List[String]): List[Tweet] = {
    implicit val formats = DefaultFormats
    var tweetList = new ArrayBuffer[Tweet]()
    for(jsonString <- jsonStringList) {
      val parsedJsonString = parse(jsonString)
      val tweetId = (parsedJsonString \ "data" \ "id").extract[String].toLong

      val authorId = (parsedJsonString \ "data" \ "author_id").extract[String].toLong

      val text = (parsedJsonString \ "data" \ "text").extract[String]

      tweetList += Tweet(tweetId, authorId, text)
    }
    tweetList.toList
  }

}

