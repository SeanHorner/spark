import scala.io.StdIn
import scala.util.matching.Regex

object MainRunner {
  def main(args: Array[String]): Unit = {
    val commandArgPattern: Regex = "(\\w+)\\s*(.*)".r

    println("***** Welcome to Meetup Tech Events Analysis! *****")
    println("*        What would you like to do today?         *")
    println("")


    var continue = true
    while(continue){
      Menu.printMenu()
      StdIn.readLine() match {
        case commandArgPattern(cmd, args) if cmd.equalsIgnoreCase("Q") => {
          println("Would you like execute the question(s): ")
          println("How many events were created for each month/year?")
          println("How has the event capacity (total rsvp_limit) changed over time?")
          println("Enter y to execute...")
          StdIn.readLine() match {
            case e if e.equalsIgnoreCase("y") => {
              QuanVu.Runner.mainRunner()
              //continue = false
            }
            case _ => {
              println("------------------Invalid command!-------------------")
              println("")
            }
          }
        }

        case commandArgPattern(cmd, args) if cmd.equalsIgnoreCase("E") => {
          println("Would you like execute the question(s): ")
          println("Which cities hosted the most tech based events (if we can get good historical data)?")
          println("Where are events hosted the most?")
          println("Enter y to execute...")
          StdIn.readLine() match {
            case e if e.equalsIgnoreCase("y") => {
              EdwardReed.testQueries.mainRunner()
              //continue = false
            }
            case _ => {
              println("------------------Invalid command!-------------------")
              println("")
            }
          }
        }

        case commandArgPattern(cmd, args) if cmd.equalsIgnoreCase("K") => {
          println("Would you like execute the question(s): ")
          println("Which event has the most RSVPs?")
          println("How many upcoming events are online compared to in person ones?")
          println("Enter y to execute...")
          StdIn.readLine() match {
            case e if e.equalsIgnoreCase("y") => {
              KylePacheco.QuestionFour.mainRunner()
              KylePacheco.QuestionNinePartOne.mainRunner()
              KylePacheco.QuestionNinePartTwo.mainRunner()
              //continue = false
            }
            case _ => {
              println("------------------Invalid command!-------------------")
              println("")
            }
          }
        }

        case commandArgPattern(cmd, args) if cmd.equalsIgnoreCase("L") => {
          println("Would you like execute the question(s): ")
          println("What is the growth rate of some specified topics in a city over time?")
          println("Prevalence of different payment options?")
          println("Has the number of in person events been increasing (relative to the total number of events) over time?")
          println("Enter y to execute...")
          StdIn.readLine() match {
            case e if e.equalsIgnoreCase("y") => {
              LiamHood.LiamRunner.mainRunner()
              //continue = false
            }
            case _ => {
              println("------------------Invalid command!-------------------")
              println("")
            }
          }
        }

        case commandArgPattern(cmd, args) if cmd.equalsIgnoreCase("M") => {
          println("Would you like execute the question(s): ")
          println("What are some of the most common meetup topics?")
          println("Enter y to execute...")
          StdIn.readLine() match {
            case e if e.equalsIgnoreCase("y") => {
              MichaelSplaver.Question16.question16()
              //continue = false
            }
            case _ => {
              println("------------------Invalid command!-------------------")
              println("")
            }
          }
        }

        case commandArgPattern(cmd, args) if cmd.equalsIgnoreCase("S") => {
          println("Would you like execute the question(s): ")
          println("What is the trend of new tech vs established tech meetups?")
          println("What is the most popular time when events are created?")
          println("Are events with longer durations more popular vs shorter ones?")
          println("Has there been a change in planning times for events?")
          println("Enter y to execute...")
          StdIn.readLine() match {
            case e if e.equalsIgnoreCase("y") => {
              SeanHorner.spark_runner.mainRunner()
              //continue = false
            }
            case _ => {
              println("------------------Invalid command!-------------------")
              println("")
            }
          }
        }

        case commandArgPattern(cmd, args) if cmd.equalsIgnoreCase("exit") => {
          println("Terminating the program...................................")
          continue = false
        }
        case _ => {
          println("------------------Invalid command!-------------------")
          println("")
        }
      }
    }

  }
}
