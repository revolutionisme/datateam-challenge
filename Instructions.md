# Instructions for running the Spark Application

Before running the application, the user needs to do the following:
### For running the spark application locally
1. Download Spark 1.6.1 on the system where you plan to execute the code.
2. Extract the package spark binaries

##### If running via IDE like Intellij
1. Install Scala plugin within Intellij or Install Scala (2.11.x) on your machine. The sbt prject should be automatically detected, if not set up the corresponding SDK.

2.	Once Spark is setup, the application can be run directly by running `NearestAirportFinder`, the location for input data can be changed using the application.conf in the resources directory.

##### If running from the command line
Use Scala shell to run `assembly` or do `sbt assembly` from the command line to create a fat jar, its location would be `datateam-challenge\target\scala-2.11\Travel-Audience Challenge-assembly-0.1-SNAPSHOT.jar`. Then run it as follows

        ./spark-1.6.1-bin-hadoop2.6/bin/spark-submit  --class com.travelAudience.locator.NearestAirportFinder --master local[*] <fat-jar-created-using-assembly>

### For running the spark application on YARN
1. Download the fat jar on the cluster machine from where you have permission to run spark-submit.
2. Run the following command:

		spark-1.6.1-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class com.travelAudience.locator.NearestAirportFinder <fat-jar-created-using-assembly>


>For running the tests, just run `sbt test`.