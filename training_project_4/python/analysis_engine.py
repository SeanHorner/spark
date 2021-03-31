import os
from datetime import datetime
import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
import numpy as np
import matplotlib as mpl

import ae_helper


# ----------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------- Menu --------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

def menu() -> int:
    # Printing the options menu
    print("*************************************************************************")
    print("|   1:    How many events were created for each month/year?             |")
    print("|   2:    How do the number of events online compare to in-person?      |")
    print("|   3:    What is the trend of new tech vs. established tech meetings?  |")
    print("|   4:    What city hosted the most tech based events?                  |")
    print("|   5:    Which venue has hosted the most events?                       |")
    print("|   6:    Which state has the most venues?                              |")
    print("|   7:    What are some of the most common meetup topics?               |")
    print("|   8:    What extra topics are associated with tech meetups?           |")
    print("|   9:    What is the most popular time when events are created?        |")
    print("|  10:    Are events with longer durations more popular than shorter?   |")
    print("|  11:    Has there been a change in planning times for events?         |")
    print("|  12:    Which event has the most RSVPs?                               |")
    print("|  13:    How has event capacity changed over time?                     |")
    print("|  14:    What is the preferred payment method for events?              |")
    print("|  15:    How has the average cost of events changed over time?         |")
    print("|  16:    What are the largest tech groups by members?                  |")
    print("|  17:    Run all of the analyses                                       |")
    print("|   0:    Exit the analysis program                                     |")
    print("*************************************************************************\n")

    # Creating a repeating loop to get user input then report it back.
    repeat = True
    result = 0
    while repeat:
        repeat = False
        try:
            result = int(input("Enter your choice:  "))
        except ValueError:
            print("Something went wrong, please try again...")
            repeat = True

    return result


# ----------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------Analyses-------------------------------------------------------
#                Analysis runners that ingest the raw data and produce result output (graphs and TSVs)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# ------------------------------- Q1:  How many events were created for each month/year? -------------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q1(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 1 started...")
    q1_data: ArrayType(list) = []
    for year in range(2003, 2021):
        year_data = [year]

        for month in range(1, 13):
            if month != 12:
                month_start = datetime(year, month, 1).timestamp()
                month_end = datetime(year, month + 1, 1).timestamp()
            else:
                month_start = datetime(year, month, 1).timestamp()
                month_end = datetime(year + 1, 1, 1).timestamp()

            month_total = df                            \
                .filter(df['created'] > month_start)    \
                .filter(df['created'] <= month_end)     \
                .count()

            year_data.append(month_total)

        q1_data.append(year_data)

        # Still need to create and append totals row

        q1_df = spark.createDataFrame(q1_data)

        print(q1_df.take(10))


# ----------------------------------------------------------------------------------------------------------------------
# --------------------------- Q2:  How do the number of events online compare to in-person? ----------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q2(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 2 started...")
    online_events_df = df \
        .select('date_month')


# ----------------------------------------------------------------------------------------------------------------------
# ------------------------- Q3:  What is the trend of new tech vs. established tech meetings? --------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q3(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 3 started...")

    # Aux function to find all mentions of a given technology in the descriptions of meetings spanning the given years.
    def tech_mentions(technology, year_start, year_end) -> list[int]:
        interim_1 = df.select('description', 'time').filter(df['description'].contains(technology))
        working_list: list[int] = []
        for y in range(year_start, year_end + 1):
            print(f"\t\tcalculating for year {y}...")
            start_millis = datetime(y, 1, 1).timestamp()
            end_millis = datetime(y + 1, 1, 1).timestamp()

            result = interim_1 \
                .filter(start_millis <= df['time']) \
                .filter(df['time'] <= end_millis) \
                .count()
            working_list.append(result)

        return working_list

    # Dictionary containing all of the technologies and their era (<, >, =90's)
    tech_dict = {"ada": "<90's", "android": ">90's", "clojure": ">90's", "cobol": "<90's",
                 "dart": ">90's", "delphi": "=90's", "fortran": "<90's", "ios": ">90's",
                 "java": "=90's", "javascript": "=90's", "kotlin": ">90's", "labview": "<90's",
                 "matlab": "<90's", "pascal": "<90's", "perl": "<90's", "php": "90's",
                 "powershell": ">90's", "python": "=90's", "ruby": "=90's", "rust": ">90's",
                 "scala": ">90's", "sql": "<90's", "typescript": ">90's", "visual basic ": "=90's",
                 "wolfram": "<90's"}

    # Creating the schema for the results DataFrame
    q3_schema = StructType([StructField('Technology', StringType(), True), StructField('Era', StringType(), True),
                            StructField('2003', IntegerType(), False), StructField('2004', IntegerType(), False),
                            StructField('2005', IntegerType(), False), StructField('2006', IntegerType(), False),
                            StructField('2007', IntegerType(), False), StructField('2008', IntegerType(), False),
                            StructField('2009', IntegerType(), False), StructField('2010', IntegerType(), False),
                            StructField('2011', IntegerType(), False), StructField('2012', IntegerType(), False),
                            StructField('2013', IntegerType(), False), StructField('2014', IntegerType(), False),
                            StructField('2015', IntegerType(), False), StructField('2016', IntegerType(), False),
                            StructField('2017', IntegerType(), False), StructField('2018', IntegerType(), False),
                            StructField('2019', IntegerType(), False), StructField('2020', IntegerType(), False)])

    q3_cols = ['Technology', 'Era', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010',
               '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']

    # Temporary holding pin for the results before being converted to a DataFrame
    q3_data: ArrayType(list) = []
    # Looping through every technology listed in the tech_dictionary
    for tech, era in tech_dict.items():
        temp = [tech, era, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        print(f"Calculating yearly data for {tech}...")
        years = tech_mentions(tech, 2003, 2020)
        i = 2
        for year in years:
            temp[i] = year
            i += 1
        print(temp)
        row = (temp[0], temp[1], int(temp[2]), int(temp[3]), int(temp[4]), int(temp[5]), int(temp[6]),
               int(temp[7]), int(temp[8]), int(temp[9]), int(temp[10]), int(temp[11]), int(temp[12]),
               int(temp[13]), int(temp[14]), int(temp[15]), int(temp[16]), int(temp[17]), int(temp[18]),
               int(temp[19]))
        q3_data.append(row)

    print(q3_data)

    # Creating a Spark DataFrame from the raw data
    q3_df = spark.createDataFrame(q3_data, q3_schema)

    # Printing for quality assurance
    print(q3_df.show(10))

    # Saving the result data to a save file
    q3_df                                               \
        .write                                          \
        .mode('overwrite')                              \
        .csv("output/q3_tech_mentions.tsv", sep='\t')

    # Creating graphs from the produced data

    # Ensuring memory cleaning (in case of full analysis) by manually unpersisting
    q3_df.unpersist()


# ----------------------------------------------------------------------------------------------------------------------
# --------------------------------- Q4:  What city hosted the most tech based events? ----------------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q4(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 4 started...")
    result_df = df                         \
        .groupby('localized_location')  \
        .count()                        \
        .orderby('count')
    row = result_df[0:1]

    print(row)


# ----------------------------------------------------------------------------------------------------------------------
# ------------------------------------ Q5:  Which venue has hosted the most events? ------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q5(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 5 started...")
    result_df = df          \
        .groupby('v_id')    \
        .count()            \
        .orderBy('count')

    result_df                                   \
        .write                                  \
        .mode('overwrite')                      \
        .csv("output/q5_result.tsv", sep='\t')

    print("The venue with the most events was:")
    print(f"\t{str(result_df.select('v_name'))}")
    print(f"\tlocated in {str(result_df.select('localized_location'))}")
    print(f"\twith {str(result_df.select('count'))} events.")

    result_df.unpersist()


# ----------------------------------------------------------------------------------------------------------------------
# ---------------------------------------- Q6: Which state has the most venues? ----------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q6(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 6 started...")
    result = df                                 \
        .select('v_id', 'localized_location')   \
        .groupby('v_id')                        \
        .count()                                \
        .groupby('localized_location')          \
        .sum('count')
    row = result.max('count')

    print(row)


# ----------------------------------------------------------------------------------------------------------------------
# -------------------------------- Q7:  What are some of the most common meetup topics? --------------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q7(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 7 started...")


# ----------------------------------------------------------------------------------------------------------------------
# ------------------------------ Q8:  What extra topics are associated with tech meetups? ------------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q8(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 8 started...")


# ----------------------------------------------------------------------------------------------------------------------
# ---------------------------- Q9:  What is the most popular time when events are created? -----------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q9(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 9 started...")

    q9_df = df                                                          \
        .filter(df['created'].isNotNull())                              \
        .filter(df['localized_location'].isNotNull())                   \
        .select('adj_creation_time')                                    \
        .withColumn('mod_c_time', df['adj_creation_time'] % 86400000)   \
        .withColumn('min_c', df['adj_creation_time']/60000)             \
        .select('min_c', 'count')                                       \
        .groupby('min_c')                                               \
        .count()                                                        \
        .orderBy('min_c')

    q9_df                                           \
        .write                                      \
        .mode('overwrite')                          \
        .csv('output/q9_full.tsv', sep='\t')

    q9_by_count = q9_df     \
        .orderBy('count')   \
        .limit(20)

    q9_by_count                                     \
        .write                                      \
        .mode('overwrite')                          \
        .csv('output/q9_by_count.tsv', sep='\t')

    q9_by_count.unpersist()
    q9_df.unpersist()


# ----------------------------------------------------------------------------------------------------------------------
# ---------------------- Q10:  Are events with longer durations more popular than shorter events? ----------------------
# ----------------------------------------------------------------------------------------------------------------------
def q10(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 10 started...")

    # Creating initial DataFrame for the analysis to work with
    q10_df = df                                         \
        .select('duration')                             \
        .filter(df['duration'].isNotNull())             \
        .withColumn('mins', f.col('duration') / 60000)  \
        .groupby('min')                                 \
        .count()

    # First analysis: count up occurrences of each duration and order by count (i.e. most common)
    print("\trunning analysis by count...")
    q10_by_count = q10_df       \
        .sort(f.desc('count'))  \
        .limit(20)

    q10_by_count                                    \
        .write                                      \
        .mode('overwrite')                          \
        .csv("output/q10_by_count.tsv", sep='\t')

    q10_by_count.unpersist()

    # Second analysis: count up occurrences oif each duration in the first 24 hours (1440 minutes)
    print("\trunning analysis for first day...")
    q10_day_one = q10_df            \
        .filter(df['min'] < 1440)   \
        .orderBy('min')

    q10_day_one                                     \
        .write                                      \
        .mode('overwrite')                          \
        .csv("output/q10_day_one.tsv", sep='\t')

    q10_day_one.unpersist()

    # Third analysis: count up the occurrences of each duration after the first 24 hours
    print("\trunning analysis for remaining days...")
    q10_beyond_day_one = q10_df     \
        .filter(df['min'] <= 1440)  \
        .orderBy('min')

    q10_beyond_day_one                                  \
        .write                                          \
        .mode('overwrite')                              \
        .csv("output/q10_beyond_day_one.tsv", sep='\t')

    q10_beyond_day_one.unpersist()

    q10_df.unpersist()


# ----------------------------------------------------------------------------------------------------------------------
# ---------------------------- Q11:  Has there been a change in planning times for events? -----------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q11(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 11 started...")

    # Creating initial DataFrame for the analysis to work with
    q11_df = df                                                     \
        .select('created', 'time')                                  \
        .filter(df['created'].isNotNull())                          \
        .filter(df['time'].isNotNull())                             \
        .withColumn('prepping_period', df['time'] - df['created'])  \
        .withColumn('prep_min', df['prepping_period'] / 60000)      \
        .filter(df['prep_min'] > 0)

    # Creating temporary data holding structure
    q11_data: ArrayType(list) = []

    # Looping through years available in the dataset
    for yr in range(2003, 2021):
        # Creating a beginning and ending time to filter the dataset to
        start_millis = datetime(yr, 1, 1).timestamp()
        end_millis = datetime(yr + 1, 1, 1).timestamp()

        # Creating a temporary DataFrame of filtered results to assess
        prep_df = q11_df                                            \
            .filter(df['time'] > start_millis)                      \
            .filter(df['time'] < end_millis)                        \
            .groupby('prep_min')                                    \
            .count()                                                \
            .withColumn('prep_time', df['prep_min'] * df['count'])

        # Calculating total prep time for all events, total events, and average
        total_prep_time = prep_df.agg(sum('prep_time'))
        total_events = prep_df.agg(sum('count'))
        avg_prep_time = total_prep_time / total_events

        # Manually unpersisting the DataFrame to free up memory
        prep_df.unpersist()

        # Creating a temporary list and appending that list to the data
        temp_entry = (yr, avg_prep_time)
        q11_data.append(temp_entry)

    q11_schema = StructType([StructField('Year', StringType(), True),
                             StructField('Average Prep Time', IntegerType(), True)])
    q11_results_df = spark.createDataFrame(q11_data, q11_schema)

    q11_results_df                                      \
        .write                                          \
        .mode('overwrite')                              \
        .csv("output/q11_avg_prep_time.tsv", sep='\t')


# ----------------------------------------------------------------------------------------------------------------------
# --------------------------------------- Q12:  Which event has the most RSVPs? ----------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q12(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 12 started...")
    result_df = df                                                          \
        .select("id", f.expr("struct(yes_rsvp_count as cmp, *) as row"))    \
        .groupby("id")                                                      \
        .agg(max("row").alias("row"))                                       \
        .select("row.*")                                                    \
        .drop("cmp")

    print(result_df)

    result_df                                   \
        .write                                  \
        .mode('overwrite')                      \
        .csv("output/q12_result.tsv", sep='\t')


# ----------------------------------------------------------------------------------------------------------------------
# ---------------------------------- Q13:  How has event capacity changed over time? -----------------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q13(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 13 started...")


# ----------------------------------------------------------------------------------------------------------------------
# ------------------------------- Q14:  What is the preferred payment method for events? -------------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q14(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 14 started...")


# ----------------------------------------------------------------------------------------------------------------------
# ---------------------------- Q15:  How has the average cost of events changed over time? -----------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q15(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 15 started...")


# ----------------------------------------------------------------------------------------------------------------------
# --------------------------------- Q16:  What are the largest tech groups by members? ---------------------------------
# ----------------------------------------------------------------------------------------------------------------------
def q16(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 16 started...")


# ----------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------Main---------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------


def main():
    # Setting up the SparkSession for the analysis engine
    spark = SparkSession \
        .builder \
        .master("local[4]") \
        .appName("MeetUp Trend Analysis") \
        .getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.sparkContext.setLogLevel("ERROR")

    # ----------------------------------------------------------------------------------------------------------
    # ---------------------------------------- UDFs and Base Functions -----------------------------------------
    # | Creating DataFrame UDFs to properly format data and type each column                                   |
    # ----------------------------------------------------------------------------------------------------------

    # Function and UDF to convert string dates in DF into millisecond timestamps
    def date_parser(date_string: str):
        return datetime.strptime(date_string, "%Y-%m-%d")

    date_parser_udf = f.udf(date_parser, DateType())

    def status_parser(status: str) -> bool:
        if status == "past":
            return True
        else:
            return False

    status_parser_udf = f.udf(status_parser, BooleanType())

    # UDF created from the tz_offset_calculator function in the ae_helper
    tz_parser_udf = f.udf(ae_helper.tz_offset_calc, LongType())

    # def cat_ids_parser(cat_string: str):
    #     hold: list[int] = []
    #     if len(cat_string) == 0:
    #         return hold
    #
    #     ids = cat_string.replace(" ", "").split(',')
    #     for i in ids:
    #         hold.append(int(i))
    #     return hold
    #
    # cat_parser_udf = F.udf(cat_ids_parser, ArrayType(IntegerType()))

    # def state_venue(loc_loc: str, v_id: str) -> str:
    #     return loc_loc[-2:] + "-" + v_id
    #
    # state_venue_udf = f.udf(state_venue, StringType())

    base_df = spark.read.load('all_cities.tsv', format='csv', header='True', sep='\t', inferSchema="true")
    # df_cols = ["id", "name", "group_name", "urlname", "v_id", "v_name", "local_date", "date_month", "local_time",
    #            "localized_location", "is_online_event", "status", "cat_ids", "duration", "time", "created",
    #            "yes_rsvp_count", "rsvp_limit", "accepts", "amount", "description"]

    print(base_df.show(10))
    print("\n\n")

    # Modifying the column data types to match data
    base_df = base_df                                                                   \
        .withColumn('local_date', date_parser_udf('local_date'))                        \
        .withColumn('time_offset', tz_parser_udf('localized_location'))                 \
        .withColumn('adj_creation_time', f.col('created') + f.col('time_offset'))       \
        .withColumn('past', status_parser_udf('status'))                                \
        .withColumn('is_online_event', f.col('is_online_event').cast(BooleanType()))
    # .withColumn('state_venue', state_venue_udf('localized_location', 'v_id'))
    # .withColumn('cat_ids', cat_parser_udf('cat_ids'))

    # new_cols_list = ["time_offset", "adj_creation_time", "past"]

    # ----------------------------------------------------------------------------------------------------------
    # ----------------------------------------------- Menu Loop ------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------
    while True:
        choice = menu()
        if choice == 1:
            q1(spark, base_df)
        elif choice == 2:
            q2(spark, base_df)
        elif choice == 3:
            q3(spark, base_df)
        elif choice == 4:
            q4(spark, base_df)
        elif choice == 5:
            q5(spark, base_df)
        elif choice == 6:
            q6(spark, base_df)
        elif choice == 7:
            q7(spark, base_df)
        elif choice == 8:
            q8(spark, base_df)
        elif choice == 9:
            q9(spark, base_df)
        elif choice == 10:
            q10(spark, base_df)
        elif choice == 11:
            q11(spark, base_df)
        elif choice == 12:
            q12(spark, base_df)
        elif choice == 13:
            q13(spark, base_df)
        elif choice == 14:
            q14(spark, base_df)
        elif choice == 15:
            q15(spark, base_df)
        elif choice == 16:
            q16(spark, base_df)
        elif choice == 17:
            q1(spark, base_df)
            q2(spark, base_df)
            q3(spark, base_df)
            q4(spark, base_df)
            q5(spark, base_df)
            q6(spark, base_df)
            q7(spark, base_df)
            q8(spark, base_df)
            q9(spark, base_df)
            q10(spark, base_df)
            q11(spark, base_df)
            q12(spark, base_df)
            q13(spark, base_df)
            q14(spark, base_df)
            q15(spark, base_df)
            q16(spark, base_df)
        elif choice == 0:
            break
        else:
            print("Invalid option, please choose from the available options.")


if __name__ == "__main__":
    main()
