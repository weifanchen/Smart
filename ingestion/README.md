# Ingestion

## history 
Build the profile of each household. <br>
There are three types of household - residential, industrial, public. <br>
For each household, The electric appliances with smart meter are listed in the profile. <br>

## streaming 

Using Apache Kafka to stimulate real time usage events of each electric appliance. There might also have anomalies in usage. <br>
Using Apache structured streaming to consume events with micro-batch rdd and sum up five 1 sec events to one 5 sec events to save 80% of the space in database, to detect anomalies usage. 
