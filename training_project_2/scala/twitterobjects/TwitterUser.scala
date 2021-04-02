package twitterobjects
import org.joda.time.DateTime

case class TwitterUser(userId: Long, userName: String, name: String, createDate: DateTime, description: String, followers: Int, following: Int) {}
