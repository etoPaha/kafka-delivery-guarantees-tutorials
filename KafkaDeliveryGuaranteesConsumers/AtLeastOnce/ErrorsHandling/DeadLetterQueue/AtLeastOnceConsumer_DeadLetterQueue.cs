// Используется Confluent.Kafka nuget-package

using Confluent.Kafka;

namespace KafkaDeliveryGuaranteesConsumers.AtLeastOnce.ErrorsHandling.DeadLetterQueue;

public class AtLeastOnceConsumer_DeadLetterQueue
{
    private int MAX_RETRIES = 3;
    
    public async Task ConsumeAsync(string brokerList, string topicName, CancellationToken cancellationToken = default)
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
        
        // Настроим его на надежную доставку
        var producerConfig = new ProducerConfig { BootstrapServers = brokerList, Acks = Acks.All };
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        string dlqTopicName = topicName + "-dlq"; // Например: my-test-topic-dlq

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe(topicName);
            
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(cancellationToken);
                bool processedSuccessfully = false;
                
                for (int i = 0; i <= MAX_RETRIES; i++)
                {
                    try
                    {
                        // --- Логика обработки ---
                        Console.WriteLine($"Попытка {i + 1}. Обработка: '{consumeResult.Message.Value}'");
                        if (consumeResult.Message.Value.Contains("сбой"))
                        {
                            throw new Exception("Постоянный сбой бизнес-логики!");
                        }
                        
                        // Если дошли сюда, все хорошо
                        processedSuccessfully = true;
                        break; // Выходим из цикла повторов
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Ошибка на попытке {i + 1}: {ex.Message}");
                        if (i == MAX_RETRIES)
                        {
                            // Попытки исчерпаны, отправляем в DLQ
                            Console.WriteLine("Все попытки провалены. Отправка в DLQ...");
                            var dlqMessage = new Message<Null, string> { Value = consumeResult.Message.Value };
                            // Добавляем полезную информацию об ошибке в заголовки
                            dlqMessage.Headers = new Headers
                            {
                                { "error-reason", System.Text.Encoding.UTF8.GetBytes(ex.Message) },
                                { "original-topic", System.Text.Encoding.UTF8.GetBytes(consumeResult.Topic) }
                            };
                            
                            // Отправляем в DLQ и ждем подтверждения
                            await producer.ProduceAsync(dlqTopicName, dlqMessage);
                            Console.WriteLine("Сообщение успешно отправлено в DLQ.");
                        }
                        else
                        {
                            // Ждем перед следующей попыткой
                            await Task.Delay(1000); // Задержка 1 секунда
                        }
                    }
                }
                
                // Коммитим смещение в любом случае:
                // - Либо потому что обработали успешно.
                // - Либо потому что успешно отправили в DLQ.
                consumer.Commit(consumeResult);
            }
           
        }
    }
}