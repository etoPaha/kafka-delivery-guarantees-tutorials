using Confluent.Kafka;
using System.Text;

/// <summary>
/// Шаг 6: Финал. Рефакторинг в отдельные методы
/// </summary>
public class DlqConsumer_Lesson_Step_6
{
    
    private const int MAX_RETRIES = 3;
    
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
                
                // Просто вызываем наш новый, аккуратный метод
                await HandleMessageWithRetryAndDlq(consumer, producer, consumeResult, dlqTopicName);
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
    
    // --- Метод, в который мы перенесли всю сложную логику из Шага 5 ---
    private async Task HandleMessageWithRetryAndDlq(
        IConsumer<Ignore, string> consumer, 
        IProducer<Null, string> dlqProducer, 
        ConsumeResult<Ignore, string> consumeResult, 
        string dlqTopicName)
    {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++)
        {
            try
            {
                Console.WriteLine($"Попытка {attempt}/{MAX_RETRIES}. Обработка: '{consumeResult.Message.Value}'");
                if (consumeResult.Message.Value.Contains("сбой"))
                {
                    throw new Exception("Сбой бизнес-логики!");
                }
                
                consumer.Commit(consumeResult);
                return; // Успех, выходим из метода
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка на попытке {attempt}: {ex.Message}");
                
                if (attempt == MAX_RETRIES)
                {
                    Console.WriteLine("Попытки исчерпаны. Отправка в DLQ...");
                    // Можно еще дальше вынести в метод SendToDlq() для чистоты
                    var dlqMessage = new Message<Null, string> { Value = consumeResult.Message.Value };
                    await dlqProducer.ProduceAsync(dlqTopicName, dlqMessage);
                    
                    consumer.Commit(consumeResult);
                    return;
                }
                
                await Task.Delay(1000);
            }
        }
    }
}