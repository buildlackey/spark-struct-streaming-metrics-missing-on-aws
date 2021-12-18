
#   Once you have an EMR cluster that you know how to ssh into, you can use this script to deploy the 'fat jar' 
#   that runs the spark app

host=$1
sshKeyPath=$2


gradlew shadowJar


sshArgs="-o StrictHostKeyChecking=no -i $sshKeyPath"
unpackDir=/home/hadoop/bug
fatJar=./build/libs/spark-struct-streaming-metrics-missing-on-aws-all.jar
ssh $sshArgs hadoop@$host "rm -rf $unpackDir ; mkdir $unpackDir"
scp $sshArgs $fatJar hadoop@$host:$unpackDir
scp $sshArgs launchApp.sh hadoop@$host:$unpackDir
