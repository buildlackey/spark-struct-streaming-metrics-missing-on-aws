# usage
#
#   launch-app.sh localTest [earliest|latest]
#               OR
#   launch-app.sh cluster  [earliest|latest] [kafka-broker-address] [kafka-topic]

mode=$1
startingOffsets=$2
brokerAddr=$3
topic=$4



if [ "$mode" = "cluster" ] ; then
        if [ -z "$brokerAddr" ] || [ -z "$topic" ] ; then
            echo  you must supply 2 final arguments to this script, [kafka-broker-address], followed by [kafka-topic]
            exit 1
        fi
        jar=./spark-struct-streaming-metrics-missing-on-aws-all.jar
else
        if [ -e ./build/libs/spark-struct-streaming-metrics-missing-on-aws-all.jar ] ; then
                jar=./build/libs/spark-struct-streaming-metrics-missing-on-aws-all.jar
        elif [ -e ./spark-struct-streaming-metrics-missing-on-aws-all.jar ] ; then
                echo "************************************************************************************************"
                echo "running localTest mode on cluster means you need localhost:9092 kafka broker else it wont work"
                echo "************************************************************************************************"
                jar=./spark-struct-streaming-metrics-missing-on-aws-all.jar
        else
                echo no jar file found for localTest mode
                exit 1
        fi
fi

if [ ! -e $jar ] ; then
        echo "could not find .jar - are you using correct mode? - $mode"
        exit 1
fi

class=com.example.AwsSupportCaseFailsToYieldLogs
spark-submit   --name demo \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
        --class $class $jar  $mode $startingOffsets $brokerAddr $topic
