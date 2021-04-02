package MichaelSplaver

object Categories {
  def getCategory(id: Int): String = {
    id match {
      case 1 => "Arts"
      case 2 => "Career & Business"
      case 3 => "Outdoors & Adventure"
      case 23 => "Outdoors & Adventure"
      case 4 => "Movements"
      case 13 => "Movements"
      case 5 => "Dance"
      case 6 => "Learning"
      case 8 => "Fashion & Beauty"
      case 9 => "Sports & Fitness"
      case 32 => "Sports & Fitness"
      case 10 => "Food & Drink"
      case 11 => "Sci-Fi & Games"
      case 29 => "Sci-Fi & Games"
      case 12 => "LGBTQ"
      case 14 => "Health & Wellness"
      case 33 => "Health & Wellness"
      case 15 => "Hobbies & Crafts"
      case 16 => "Language & Culture"
      case 18 => "Book Clubs"
      case 20 => "Film"
      case 21 => "Music"
      case 22 => "Beliefs"
      case 24 => "Beliefs"
      case 28 => "Beliefs"
      case 25 => "Family"
      case 26 => "Pets"
      case 27 => "Photography"
      case 31 => "Social"
      case 34 => "Tech"
      case 36 => "Writing"
      case _ => "Unknown"
    }
  }
}
