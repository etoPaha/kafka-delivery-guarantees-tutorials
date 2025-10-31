// Используется Confluent.Kafka nuget-package

using Confluent.Kafka;

namespace KafkaDeliveryGuaranteesConsumers.AtLeastOnce.ErrorsHandling;

public class AtLeastOnceConsumer_SkipAndLog
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
            
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(cancellationToken);
                    
                    try
                    {
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
                    catch (Exception ex)
                    {
                        // Ловим ошибку
                        Console.WriteLine($"КРИТИЧЕСКАЯ ОШИБКА: {ex.Message}. Сообщение будет пропущено.");
                        // ВАЖНО: здесь нужно логировать всё: ex.ToString(), consumeResult.Message.Value и т.д.
                
                        consumer.Commit(consumeResult);
    
                        // Коммитим смещение, чтобы пропустить сообщение.
                        // В данном примере consumer.Consume() уже вернул результат, поэтому мы можем его закоммитить.
                        // consumer.Commit(); // В Confluent.Kafka 1.x.x можно было делать так.
                        // В новых версиях лучше коммитить конкретный результат, если он есть.
                        // Если consumeResult получен, можно его закоммитить, чтобы сдвинуть offset.
                        // Но лучше убедиться, что вы не коммитите по ошибке успешный результат.
                        // Безопаснее всего просто продолжить цикл, если ошибка не критична для потока.
                        // Однако, чтобы сдвинуть offset, коммит необходим.
                    }
                }
           
        }
    }
}