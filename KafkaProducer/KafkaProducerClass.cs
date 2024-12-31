using Confluent.Kafka;

namespace KafkaProducer;

public class KafkaProducerClass
{
    private readonly string _bootstrapServers;
    private readonly string _topic;

    public KafkaProducerClass(string bootstrapServers, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
    }

    public async Task PublishMessageAsync(string message)
    {
        var config = new ProducerConfig {
            BootstrapServers = _bootstrapServers,
            Acks = Acks.All,
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        try
        {
            var deliveryResult = await producer.ProduceAsync(_topic, new Message<Null, string> { Value = message });

            Console.WriteLine($"Message delivered to {deliveryResult.TopicPartitionOffset}");
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
    }
}
