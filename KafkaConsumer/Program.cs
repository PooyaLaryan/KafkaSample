using Common;
using KafkaConsumer;


var consumer = new KafkaConsumerClass(CommonData.bootstrapServers, CommonData.topic, CommonData.groupId);
consumer.StartConsuming();
//consumer.CountOfMessageInTopic();