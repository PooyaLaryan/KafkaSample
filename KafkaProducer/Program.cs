using Common;
using KafkaProducer;



var producer = new KafkaProducerClass(CommonData.bootstrapServers, CommonData.topic);

Console.WriteLine("Enter messages to send to Kafka (type 'exit' to quit):");
string input;
while ((input = Console.ReadLine()) != "exit")
{
    await producer.PublishMessageAsync(input);
}
