# usage
#
#   launch-app.sh localTest [earliest|latest]
#               OR
#   launch-app.sh cluster  [earliest|latest] [kafka-broker-address] [kafka-topic]

brokerAddr=$1
topic=$2

if [ -z "$brokerAddr" ] || [ -z "$topic" ] ; then 
    echo  you must supply 2 arguments to this script, [kafka-broker-address], followed by [kafka-topic]
    exit 1
fi
    

class=AwsSupportCaseFailsToYieldLogs
spark-submit   --name demo \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
        --class $class $jar  localTest earliest brokerAddr=$1 topic=$2
