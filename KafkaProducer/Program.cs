using KafkaProducer;

var bootstrapServers = "localhost:9092"; // Replace with your Kafka server
var topic = "test-topic"; // Replace with your topic name

var producer = new KafkaProducerClass(bootstrapServers, topic);

Console.WriteLine("Enter messages to send to Kafka (type 'exit' to quit):");
string input;
while ((input = Console.ReadLine()) != "exit")
{
    await producer.PublishMessageAsync(input);
}