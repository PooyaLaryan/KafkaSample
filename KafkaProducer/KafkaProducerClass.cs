using Confluent.Kafka;

namespace KafkaProducer;

public class KafkaProducerClass
{
    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly ProducerConfig _config;
    private readonly IProducer<Null, string> _producer;

    public KafkaProducerClass(string bootstrapServers, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
        _config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            Acks = Acks.All,
        };
        _producer = new ProducerBuilder<Null, string>(_config).Build();
    }

    public async Task PublishMessageAsync(string message)
    {
        try
        {
            var deliveryResult = await _producer.ProduceAsync(_topic, new Message<Null, string> { Value = message });

            Console.WriteLine($"Message delivered to {deliveryResult.TopicPartitionOffset}-{deliveryResult.Topic}");
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
    }
}
