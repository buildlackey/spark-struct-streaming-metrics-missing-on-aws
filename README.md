# spark-struct-streaming-metrics-missing-on-aws
Demo of how when we deploy on AWS/EMR the structured streaming metrics tab shows no batches processed and no graphs

# Running Locally (where the streaming metrics do seem to appear as expected)

Build and run the project's Spark app locally as follows:

    ./gradlew clean ; ./gradlew shadowJar
    bash -x  launchApp.sh   localTest earliest 2>&1 | tee /tmp/out

Once the application starts dumping data to the console you should be able to point your browser
to the locally running Spark UI:

     http://localhost:4040/StreamingQuery

Click on the link in the 'RunId' column and you should see nice graphs and a message of the form:

    Running batches for M seconds .... ( N completed batches)


# Running on an AWS EMR cluster (where the streaming metrics do not appear as expected)

Running the same program on an AWS EMR does show little files being created for each Kafka message read in, but 
for some reason when you launch the Spark UI for your running app and go to the StreamingQuery tab of the UI
you will 0 completed batches reported, and no graphs.

To repro in your AWS environment you will need:

    * the IP or public DNS of the master node of your EMR cluster
    * an ssh key that allows you to scp files and ssh into a terminal session on that cluster
    * a kafka broker to connect to, and a topic to which a producer is actively sending records

Build and deploy the app as follows:

    ./gradlew clean ; ./gradlew shadowJar
    bash deployToAws.sh $tc ~/.ssh/spark-cluster.pem

This will upload the .jar and launch script (launchApp.sh) to  /home/hadoop/bug

    cd bug
    # launch the app on your cluster:
    brokerAddr=brokerUrlOfYourKafkaCluster
    topic=some_topic_actively_receiving_records
    sparkMaster=yarn
    bash -x launchApp.sh  $sparkMaster [earliest|latest] $brokerAddr $topic

Once the app is launched you can see that each Kafka record read in is being dumped to a distinct file.
You can open up the file and see your kafka records (they are assumed to be binary format) so batches are 
being processed. However, on AWS EMR, when you go to the Spark UI instance for your running job, you will 
see that the batch metrics on the Streaming tab are never updated, and the number of processed bathches 
is always reported to be zero. This is despite the fact that  the Kafka records captured in 
individual files clearly show that batches ARE being processed.



