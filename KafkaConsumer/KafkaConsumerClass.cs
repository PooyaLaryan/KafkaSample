using Common;
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
            BootstrapServers = CommonData.bootstrapServers,
            GroupId = CommonData.groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            //EnableAutoCommit = true
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe(CommonData.topic);

            Console.WriteLine("Kafka consumer is running. Press Ctrl+C to exit...");

            try
            {
                while (true)
                {
                    var result = consumer.Consume();
                    Console.WriteLine($"Message: {result.Message.Value}, Partition: {result.Partition}, Offset: {result.Offset}");
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Consumer was cancelled.");
            }
            finally
            {
                consumer.Close();
            }
        }
    }

    public void IsTopicExists()
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = CommonData.bootstrapServers,
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        string topicName = CommonData.topic; // Replace with the topic you want to check

        try
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            bool topicExists = metadata.Topics.Exists(t => t.Topic == topicName);

            if (topicExists)
            {
                Console.WriteLine($"Topic '{topicName}' exists.");
            }
            else
            {
                Console.WriteLine($"Topic '{topicName}' does not exist.");
            }

            Console.ReadKey();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error while checking topic: {ex.Message}");
        }
    }

    public void CountOfMessageInTopic()
    {
        string bootstrapServers = CommonData.bootstrapServers;
        string topicName = CommonData.topic; // Replace with your topic name

        // AdminClient to fetch metadata
        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };

        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        try
        {
            // Fetch topic metadata to get the list of partitions
            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            if (metadata.Topics.Count == 0 || metadata.Topics[0].Error.IsError)
            {
                Console.WriteLine($"Topic '{topicName}' does not exist.");
                return;
            }

            int partitionCount = metadata.Topics[0].Partitions.Count;
            Console.WriteLine($"Topic '{topicName}' has {partitionCount} partitions.");

            long totalMessageCount = 0;

            // Consumer to query offsets
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = CommonData.groupId,
                EnableAutoCommit = false
            };

            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

            foreach (var partitionMetadata in metadata.Topics[0].Partitions)
            {
                var topicPartition = new TopicPartition(topicName, partitionMetadata.PartitionId);

                // Get earliest and latest offsets for the partition
                var watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(10));

                long messageCount = watermarkOffsets.High - watermarkOffsets.Low;
                Console.WriteLine($"Partition {partitionMetadata.PartitionId}: Message Count = {messageCount}");
                totalMessageCount += messageCount;
            }

            Console.WriteLine($"Total Messages in Topic '{topicName}': {totalMessageCount}");
            Console.ReadKey();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }
}
