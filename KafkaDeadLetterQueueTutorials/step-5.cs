using Confluent.Kafka;
using System.Text;

/// <summary>
/// Шаг 5: Добавляем повторные попытки (Retries)
/// </summary>
public class DlqConsumer_Lesson_Step_5
{
    public async Task ConsumeAsync(string brokerList, string topicName, CancellationToken cancellationToken = default)
    {
        // ... В начало метода ConsumeAsync добавляем создание продюсера ...
        var producerConfig = new ProducerConfig { BootstrapServers = brokerList, Acks = Acks.All };
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        string dlqTopicName = topicName + "-dlq";
        
        Console.WriteLine("Консьюмер-урок запущен");

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = "lesson-group",
            EnableAutoCommit = false, // Важно для at-least-once
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        consumer.Subscribe(topicName);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(cancellationToken);
                bool processedSuccessfully = false; // Флаг, чтобы отследить успех

                for (int attempt = 1; attempt <= 3; attempt++)
                {
                    try
                    {
                        Console.WriteLine($"Попытка {attempt}/3. Обработка: '{consumeResult.Message.Value}'");
                        if (consumeResult.Message.Value.Contains("сбой"))
                        {
                            throw new Exception("Сбой бизнес-логики!");
                        }

                        // Успех!
                        consumer.Commit(consumeResult);
                        processedSuccessfully = true;
                        break; // Выходим из цикла for
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Ошибка на попытке {attempt}: {ex.Message}");
                        if (attempt == 3) // Если это была последняя попытка
                        {
                            Console.WriteLine("Попытки исчерпаны. Отправляем в DLQ...");
                            var dlqMessage = new Message<Null, string> { Value = consumeResult.Message.Value };
                            // ... (код отправки в DLQ, как на шаге 4) ...
                            await producer.ProduceAsync(dlqTopicName, dlqMessage);
                            consumer.Commit(consumeResult); // Коммитим после отправки в DLQ
                        }
                        else
                        {
                            await Task.Delay(1000); // Ждем секунду перед повтором
                        }
                    }
                }
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