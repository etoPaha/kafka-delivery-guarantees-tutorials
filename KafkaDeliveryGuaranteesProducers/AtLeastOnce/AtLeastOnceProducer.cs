// Используется Confluent.Kafka nuget-package
using Confluent.Kafka;
using System;
using System.Threading.Tasks;

public class AtLeastOnceProducer
{
    public async Task ProduceAsync(string brokerList, string topicName, string message)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = brokerList,
            // All: Ждем подтверждения от лидера и всех синхронизированных реплик
            Acks = Acks.All
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            try
            {
                var deliveryResult = await producer.ProduceAsync(topicName, new Message<Null, string> { Value = message });
                Console.WriteLine($"Сообщение доставлено в: {deliveryResult.TopicPartitionOffset}");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Доставка не удалась: {e.Error.Reason}");
            }
        }
    }
}