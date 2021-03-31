## Project

- Create a Spark application to determine the trends of Tech related meetings in specific cities using the Meetup.com API (and other subject materials if preferred).
- Explore the feasibility of Streaming + Kafka as well with this.

## Tech Stack
- Python 3.9.0 / Scala 2.13.2
- Spark SQL
- Apache Spark
- Git + Github (boards as well)

## Trend Questions
1. What is the trend of new tech vs established tech meetups? (e.g. number of meetups about C++ vs. Rust) 
  	- description (broken down into keyword occurences)
2. What are some of the most common meetup topics? (e.g. Gaming vs. Professional Development, Big Data vs. Automation)
	- category_ids or id
3. What is the most popular time when events are created? (Find local_time/date)
	- created
4. Which event has the most RSVPs?
	- yes_rsvp_count
5. Are events with longer durations more popular vs shorter ones?
	- duration and yes_rsvp_count
6. How many events were created for each month/year?
	- local_date
7. (if good historical data is available) what is the growth rate of some specified topics in a city over time?
	- category_ids
8. Which cities hosted the most tech based events (if we can get good historical data)?
	- group.localized_location
9. How many upcoming events are online compared to in person ones (and what cities have the most upcoming in person events)?
	- is_online_event
10. Prevalence of different payment options.
	- fee.accepts, fee.amount
11. How has the event capacity (total rsvp_limit) changed over time?
	- rsvp_limit
12. Has there been a change in planning times for events? (time - created)
	- time, created
13. Has the number of in person events been increasing (relative to the total number of events) over time?
	- is_online_event
14. Where are events hosted the most?
	- venue.id, venue.name

