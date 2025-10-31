using Confluent.Kafka;
using System.Text;

/// <summary>
/// Шаг 1: Основа. Простой AtLeastOnceConsumer
/// </summary>
public class DlqConsumer_Lesson_Step_1
{
    public async Task ConsumeAsync(string brokerList, string topicName, CancellationToken cancellationToken = default)
    {
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
            
                // --- НАША БИЗНЕС-ЛОГИКА ---
                Console.WriteLine($"Обработано: '{consumeResult.Message.Value}'");
                // -------------------------

                // Коммитим смещение ПОСЛЕ успешной обработки
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