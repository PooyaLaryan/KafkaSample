using KafkaConsumer;

var bootstrapServers = "localhost:9092"; // Replace with your Kafka server
var topic = "test-topic"; // Replace with your topic name
var groupId = "test-group"; // Replace with your group ID

var consumer = new KafkaConsumerClass(bootstrapServers, topic, groupId);
consumer.StartConsuming();