using Confluent.Kafka;
using System.Text;

/// <summary>
/// Шаг 4: Решение проблемы потери данных. Отправка в DLQ
/// </summary>
public class DlqConsumer_Lesson_Step_4
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

                try
                {
                    // ... та же логика обработки ...
                    Console.WriteLine($"Обрабатывается: '{consumeResult.Message.Value}'");
                    if (consumeResult.Message.Value.Contains("сбой"))
                    {
                        throw new Exception("Сбой бизнес-логики!");
                    }
                    consumer.Commit(consumeResult);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ОШИБКА: {ex.Message}. Отправляем в DLQ...");
    
                    // 1. Создаем сообщение для DLQ
                    var dlqMessage = new Message<Null, string> { Value = consumeResult.Message.Value };
                    dlqMessage.Headers = new Headers
                    {
                        { "error-reason", Encoding.UTF8.GetBytes(ex.Message) }
                    };

                    // 2. Отправляем его
                    await producer.ProduceAsync(dlqTopicName, dlqMessage);
                    Console.WriteLine("Успешно отправлено в DLQ.");

                    // 3. Коммитим смещение в основной очереди
                    consumer.Commit(consumeResult);
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