// Используется Confluent.Kafka nuget-package

using Confluent.Kafka;

namespace KafkaDeliveryGuaranteesConsumers.AtLeastOnce;

public class AtLeastOnceConsumer
{
    public void Consume(string brokerList, string topicName, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Консьюмер запущен");
        
        var config = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = "at-least-once-group",
            // Отключаем автоматическую фиксацию смещения
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe(topicName);
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(cancellationToken);
                    
                    // 1. Логика обработки сообщения
                    Console.WriteLine($"Обработано сообщение: '{consumeResult.Message.Value}'");
                    
                    // Моделируем возможный сбой до фиксации смещения
                    if (consumeResult.Message.Value.Contains("сбой"))
                    {
                        throw new Exception("Произошел сбой во время обработки!");
                    }
                    
                    // 2. Фиксируем смещение вручную только после успешной обработки
                    consumer.Commit(consumeResult);
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Потребление остановлено.");
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}