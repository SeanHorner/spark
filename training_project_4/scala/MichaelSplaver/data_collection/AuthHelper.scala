package MichaelSplaver.data_collection

import scalaj.http.Http

object AuthHelper {


  private def getAuthorization(apiKey: String, redirectUri: String): String = {
    val result = Http(s"https://secure.meetup.com/oauth2/authorize?client_id=$apiKey&redirect_uri=$redirectUri&response_type=anonymous_code")
      .header("Accept", "application/json").asString

    result.body
  }

  private def getBearerToken(apiKey: String, apiSecret: String, redirectUri: String, authCode: String): String = {
    val result = Http(s"https://secure.meetup.com/oauth2/access?client_id=$apiKey&client_secret=$apiSecret&grant_type=anonymous_code&redirect_uri=$redirectUri&code=$authCode").postForm
      .header("Accept", "application/json").asString

    result.body
  }

  private def getAccessToken(email: String, password: String, bearer: String): String = {

    val result = Http(s"https://api.meetup.com/sessions?&email=$email&password=$password").postForm
      .header("Accept", "application/json")
      .header("Authorization", s"Bearer $bearer").asString

    result.body
  }

  def requestAccessToken(): String = {
    val apikey = JsonHelper.getValue("credentials.json", "api-key")
    val apisecret = JsonHelper.getValue("credentials.json", "secret-key")
    val email = JsonHelper.getValue("credentials.json", "email")
    val password = JsonHelper.getValue("credentials.json", "password")
    //println(getAuthorization(apikey))
    //println(getBearerToken(apikey, apisecret))
    val bearer = ""
    getAccessToken(email, password, bearer)
  }


}
