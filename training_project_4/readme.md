## Datasets
 - IMF
   - https://www.imf.org/en/Publications/SPROLLS/world-economic-outlook-databases#sort=%40imfdate%20descending
   - October 2019/2020 & April 2020
 - COVID-19
   - https://ourworldindata.org/coronavirus-source-data
 - Twitter COVID-19 Stream
   - https://developer.twitter.com/en/docs/labs/covid19-stream/overview

## Problem Statement
 - Which Regions handled COVID-19 the best assuming our metrics are change in GDP by percentage and COVID-19 infection rate per capita. (Jan 1 2020 - Oct 31 2020)
 - Find the top 5 pairs of countries that share a land border and have the highest discrepancy in covid-19 infection rate per capita. Additionally find the top 5 landlocked countries that have the highest discrepancy in covid-19 infection rate per capita.
 - Live update by Region of current relevant totals from COVID-19 data.
 - Is the trend of the global COVID-19 discussion going up or down? Do spikes in infection rates of the 5-30 age range affect the volume of discussion?
 - When was COVID-19 being discussed the most?
 - What percentage of countries have an increasing COVID-19 Infection rate?
 - What are the hashtags used to describe COVID-19 by Region (e.g. #covid, #COVID-19, #Coronavirus, #NovelCoronavirus)? Additionally what are the top 10 commonly used hashtags used alongside COVID hashtags?
 - Is there a significant relationship between a Region’s cumulative GDP and Infection Rate per capita? What is the average amount of time it took for each region to reach its first peak in infection rate per capita?

 - Project monitoring through ELK Stack

## Relevant Totals
 - Infection Numbers
 - Deaths
 - Recoveries

## Regions
 - Africa
 - Asia
 - The Caribbean
 - Central America
 - Europe
 - North America
 - South America
 - Oceania

## Helm Charts Used
 - Kafka: bitnami/kafka
 - Zookeeper: bitnami/zookeeper
 - Spark: bitnami/spark
 - Ingress: nginx
 - Elastic Stack: Custom
