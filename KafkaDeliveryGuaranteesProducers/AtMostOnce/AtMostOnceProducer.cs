// Используется Confluent.Kafka nuget-пакет
using Confluent.Kafka;
using System;
using System.Threading.Tasks;

public class AtMostOnceProducer
{
    public async Task ProduceAsync(string brokerList, string topicName)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = brokerList,
            // 0: Не ждем подтверждения от брокера.
            Acks = Acks.None
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            try
            {
                await producer.ProduceAsync(topicName, new Message<Null, string> { Value = "сообщение с at-most-once" });
                Console.WriteLine("Сообщение отправлено (без ожидания подтверждения)");
            }
            catch (Exception e)
            {
                // Ошибка может возникнуть до отправки, например, при сериализации
                Console.WriteLine($"Ошибка при отправке: {e.Message}");
            }
        }
    }
}