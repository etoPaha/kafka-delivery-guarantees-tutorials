using Confluent.Kafka;
using System.Text;

/// <summary>
/// Шаг 3: Первое решение. Пропускаем сообщение с помощью try-catch
/// </summary>
public class DlqConsumer_Lesson_Step_3
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

                try
                {       
                    // --- НАША БИЗНЕС-ЛОГИКА ---
                    Console.WriteLine($"Обрабатывается: '{consumeResult.Message.Value}'");
                    if (consumeResult.Message.Value.Contains("сбой"))
                    {
                        throw new Exception("Сбой бизнес-логики!");
                    }
                    // -------------------------

                    // Коммитим только при успехе
                    consumer.Commit(consumeResult);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ОШИБКА: {ex.Message}. Сообщение будет пропущено.");
                    // Коммитим, чтобы передвинуть смещение и не застрять
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