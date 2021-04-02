package twitterdao

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.nio.file.Paths

import net.liftweb.json._
import org.apache.http.NameValuePair
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.joda.time.DateTime
import twitterobjects.{Tweet, TwitterUser}

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ArrayBuffer




class TwitterDAO(bearerToken: String) {

  /** Feed this function a unique screen name for a user and it will return a twitter user with some basic properties
   * and metrics
   * @param users A string specifying the user's screen name
   * @return      An object of type TwitterUser with basic information and metrics*/
  def getUser(users: String): TwitterUser = {
    var userResponse: String = null

    val httpClient = generateHttpClient()

    val uriBuilder = new URIBuilder("https://api.twitter.com/2/users/by")
    var queryParameters = new ArrayBuffer[NameValuePair]
    queryParameters += new BasicNameValuePair("usernames", users)
    queryParameters += new BasicNameValuePair("user.fields", "created_at,description,public_metrics")
    uriBuilder.addParameters(queryParameters.asJava)

    val httpGet = new HttpGet(uriBuilder.build())
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")
    httpGet.setHeader("Content-Type", "application/json")

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity

    if(null != entity)
      userResponse = EntityUtils.toString(entity, "UTF-8")

    //userResponse
    JSONUserResponseToUserObject(userResponse)

  }

  /** Feed this function a unique user ID for a user and it will return a twitter user with some basic properties
   * and metrics
   * @param userId A string specifying the user's screen name
   * @return      An object of type TwitterUser with basic information and metrics*/
  def getUser(userId: Long): TwitterUser = {
    var userResponse: String = null

    val httpClient = generateHttpClient()

    val uriBuilder = new URIBuilder("https://api.twitter.com/2/users")
    var queryParameters = new ArrayBuffer[NameValuePair]
    queryParameters += new BasicNameValuePair("ids", userId.toString)
    queryParameters += new BasicNameValuePair("user.fields", "created_at,description,public_metrics")
    uriBuilder.addParameters(queryParameters.asJava)

    val httpGet = new HttpGet(uriBuilder.build())
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")
    httpGet.setHeader("Content-Type", "application/json")

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity

    if(null != entity)
      userResponse = EntityUtils.toString(entity, "UTF-8")

    JSONUserResponseToUserObject(userResponse)
  }

  /** Use this function if you want to get a Twitter user's timeline
   * @param user            the user whose timeline you wish to see
   * @param count           number of tweets you want to see from the timeline. Maximum is 200 (according to Twitter API)
   * @param excludeReplies  When set to true, excludes tweets where the given user is replying to other tweets
   * @param includeRetweets When set to true, includes tweets where the given user is replying to another tweet
   * @return                Returns a list of type Tweet from the given user's timeline*/
  def getUserTimeline(user: TwitterUser, count: Int = 200, excludeReplies: Boolean = true, includeRetweets: Boolean = false): List[Tweet] = {
    var tweetResponse: String = null

    val httpClient = generateHttpClient()

    val uriBuilder = new URIBuilder("https://api.twitter.com/1.1/statuses/user_timeline.json")
    var queryParameters = new ArrayBuffer[NameValuePair]
    queryParameters += new BasicNameValuePair("user_id", s"${user.userId}")
    queryParameters += new BasicNameValuePair("count", s"$count")
    queryParameters += new BasicNameValuePair("trim_user", s"false")
    queryParameters += new BasicNameValuePair("tweet_mode", "extended")
    queryParameters += new BasicNameValuePair("exclude_replies", s"$excludeReplies")
    queryParameters += new BasicNameValuePair("include_rts", s"$includeRetweets")
    uriBuilder.addParameters(queryParameters.asJava)

    val httpGet = new HttpGet(uriBuilder.build())
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity

    if(null != entity)
      tweetResponse = EntityUtils.toString(entity, "UTF-8")

    JSONTweetResponseToTweetObject(tweetResponse)
  }

  /** Gets replies to a given tweet by searching twitter where the author of the given tweet is mentioned and checks
   * those tweets to see if they are a reply to the given tweet. IMPORTANT: This will likely never return ALL replies to
   * a tweet. This is due to Twitter capping the amount of returns at 100. If you want a good sampling of replies,
   * pass a tweet from a user who gets a lot of replies.
   * @param tweet Tweet object who you want replies to
   * @param count maximum number of potential replies
   * @return      returns a list of tweets that are replies to given tweets*/
  def getRepliesToTweet(tweet: Tweet, count: Int = 100): List[Tweet] = {

    val tweetId = tweet.tweetId
    val user = getUser(tweet.authorId)
    var tweetResponse: String = null
    val httpClient = generateHttpClient()

    val uriBuilder = new URIBuilder("https://api.twitter.com/1.1/search/tweets.json")
    var queryParameters = new ArrayBuffer[NameValuePair]
    queryParameters += new BasicNameValuePair("q", s"@${user.userName} ")
    queryParameters += new BasicNameValuePair("since_id", s"$tweetId")
    queryParameters += new BasicNameValuePair("count", s"$count")
    queryParameters += new BasicNameValuePair("tweet_mode", "extended")
    uriBuilder.addParameters(queryParameters.asJava)
    val httpGet = new HttpGet(uriBuilder.build())
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    if(null != entity)
      tweetResponse = EntityUtils.toString(entity, "UTF-8")

    JSONTweetSearchResponseAndFilterByTweetId(tweetId,tweetResponse)

  }

  /** Give this a list of tweets and a maximum number of replies per tweet. This function just loops over the above
   * function
   * @param tweets  A list of tweets for which you want replies
   * @param count   Maximum number of replies per tweet
   * @return        Returns a list of tweets which are replies based off the tweets given*/
  def getRepliesToListOfTweets(tweets: List[Tweet], count: Int = 100): List[Tweet] = {
    var replies = new ArrayBuffer[List[Tweet]]()
    for (e <- tweets) {
      replies += getRepliesToTweet(e,count)
    }
    replies.toList.flatten[Tweet]
  }

  /** Don't worry about the functions below. They are simply wrappers for other functions or JSON parsing utilities
   * that are used by the above functions */
  private def generateHttpClient() = {
    val httpClient = HttpClients.custom()
      .setDefaultRequestConfig(RequestConfig.custom()
        .setCookieSpec(CookieSpecs.STANDARD).build())
      .build()
    httpClient
  }

  private def JSONUserResponseToUserObject(response: String): TwitterUser = {

    val jsonResponse = parse(response)
    implicit val formats: DefaultFormats.type = DefaultFormats

    val userId = (jsonResponse \ "data" \ "id").extract[String].toLong

    val userName = (jsonResponse \ "data" \ "username").extract[String]

    val name = (jsonResponse \ "data" \ "name").extract[String]

    val createDate = DateTime.parse((jsonResponse \ "data" \ "created_at").extract[String])

    val description = (jsonResponse \ "data" \ "description").extract[String]

    val followersCount = (jsonResponse \ "data" \ "public_metrics" \ "followers_count").extract[Int]

    val followingCount = (jsonResponse \ "data" \ "public_metrics" \ "following_count").extract[Int]

    TwitterUser(userId,userName,name,createDate,description,followersCount,followingCount)
  }

  def collectStreamedTweets(path:String, count:Int=1000): Unit = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream")
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    if (null != entity) {
      val reader = new BufferedReader(new InputStreamReader(entity.getContent))
      var line = reader.readLine
      val fileWriter = new PrintWriter(Paths.get(path).toFile)
      var lineNumber = 1 //track line number to know when to move to new file
      while (line != null && lineNumber % count != 0) {
        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }
      fileWriter.close()
    }
  }

  private def JSONTweetResponseToTweetObject(response: String): List[Tweet] = {

    val jsonResponse = parse(response)
    implicit val formats: DefaultFormats.type = DefaultFormats

    var tweetIdList = ArrayBuffer[Long]()
    var authorIdList = ArrayBuffer[Long]()
    var textList = ArrayBuffer[String]()
    var tweetList = ArrayBuffer[Tweet]()

    val tweetId = (jsonResponse \ "id").children
    tweetId.foreach(tweetIdList += _.extract[Long])

    val authorId = (jsonResponse \ "user" \ "id").children
    authorId.foreach(authorIdList += _.extract[Long])

    val text = (jsonResponse \ "full_text").children
    text.foreach(textList += _.extract[String])


    for (i <- textList.indices)
      tweetList+=Tweet(tweetIdList(i), authorIdList(i), textList(i))

    tweetList.toList

  }


  private def JSONTweetSearchResponseAndFilterByTweetId(idToFilterBy: Long, response: String): List[Tweet] = {

    val jsonResponse = parse(response)
    implicit val formats: DefaultFormats.type = DefaultFormats

    var tweetIdList = ArrayBuffer[Long]()
    var authorIdList = ArrayBuffer[Long]()
    var textList = ArrayBuffer[String]()
    var idToCompareList = ArrayBuffer[Long]()
    var tweetList = ArrayBuffer[Tweet]()

    val tweetId = (jsonResponse \ "statuses" \ "id").children
    tweetId.foreach(tweetIdList += _.extract[Long])

    val authorId = (jsonResponse \ "statuses" \ "user" \ "id").children
    authorId.foreach(authorIdList += _.extract[Long])

    val text = (jsonResponse \ "statuses" \ "full_text").children
    text.foreach(textList += _.extract[String])

    val idToCompare = (jsonResponse \ "statuses" \ "in_reply_to_status_id").children
    idToCompare.foreach(idToCompareList += _.extract[Long])

    for (i <- textList.indices) {
      if(idToCompareList(i)==idToFilterBy)
        tweetList+=Tweet(tweetIdList(i), authorIdList(i), textList(i))
    }

    tweetList.toList

  }

}
