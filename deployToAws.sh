host=$1
sshKeyPath=$2


gradlew shadowJar


sshArgs="-o StrictHostKeyChecking=no -i $sshKeyPath"
unpackDir=/home/hadoop/bug
fatJar=./build/libs/spark-struct-streaming-metrics-missing-on-aws-all.jar
ssh $sshArgs hadoop@$host "rm -rf $unpackDir ; mkdir $unpackDir"
scp $sshArgs $fatJar hadoop@$host:$unpackDir
scp $sshArgs launchApp.sh hadoop@$host:$unpackDir
