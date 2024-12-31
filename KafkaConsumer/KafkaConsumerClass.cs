using Confluent.Kafka;

namespace KafkaConsumer;
public class KafkaConsumerClass
{
    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly string _groupId;

    public KafkaConsumerClass(string bootstrapServers, string topic, string groupId)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
        _groupId = groupId;
    }

    public void StartConsuming()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Null, string>(config).Build();

        consumer.Subscribe(_topic);

        Console.WriteLine("Consumer started. Waiting for messages...");

        try
        {
            while (true)
            {
                var consumeResult = consumer.Consume();

                Console.WriteLine($"Message received: {consumeResult.Message.Value}");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Consumer stopped.");
            consumer.Close();
        }
    }
}
