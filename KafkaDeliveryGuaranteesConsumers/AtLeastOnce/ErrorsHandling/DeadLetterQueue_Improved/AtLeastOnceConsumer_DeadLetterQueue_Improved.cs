using System.Text;
using Confluent.Kafka;

namespace KafkaDeliveryGuaranteesConsumers.AtLeastOnce.ErrorsHandling.DeadLetterQueue_Improved;

public class AtLeastOnceConsumer_DeadLetterQueue_Improved
{
    // Сделаем константой, так как это не меняется
    private const int MAX_RETRIES = 3; // 1 начальная попытка + 2 повтора

    public async Task ConsumeAsync(string brokerList, string topicName, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Консьюмер запущен");

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = "at-least-once-group-dlq",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var producerConfig = new ProducerConfig { BootstrapServers = brokerList, Acks = Acks.All };

        // Оборачиваем все в try-finally для гарантированного закрытия ресурсов
        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        
        consumer.Subscribe(topicName);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Выносим всю сложную логику в отдельный метод для чистоты
                var consumeResult = consumer.Consume(cancellationToken);
                await HandleMessageWithRetryAndDlq(consumer, producer, consumeResult, topicName + "-dlq");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Потребление остановлено.");
        }
        finally
        {
            // Гарантируем, что консьюмер корректно закроет соединение
            consumer.Close();
        }
    }

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
                // --- Логика обработки ---
                Console.WriteLine($"Попытка {attempt}/{MAX_RETRIES}. Обработка: '{consumeResult.Message.Value}'");
                if (consumeResult.Message.Value.Contains("сбой"))
                {
                    throw new Exception("Постоянный сбой бизнес-логики!");
                }
                
                // Успешная обработка, коммитим и выходим
                consumer.Commit(consumeResult);
                return; // <-- Выход из метода, так как все прошло успешно
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка на попытке {attempt}/{MAX_RETRIES}: {ex.Message}");
                
                if (attempt == MAX_RETRIES)
                {
                    // Попытки исчерпаны, отправляем в DLQ
                    Console.WriteLine("Все попытки провалены. Отправка в DLQ...");
                    await SendToDlq(dlqProducer, consumeResult, ex, dlqTopicName);
                    
                    // Коммитим смещение, так как сообщение ушло в DLQ
                    consumer.Commit(consumeResult);
                    return; // <-- Выход из метода
                }
                
                // Ждем перед следующей попыткой
                await Task.Delay(1000); // Задержка 1 секунда
            }
        }
    }

    private async Task SendToDlq(
        IProducer<Null, string> dlqProducer, 
        ConsumeResult<Ignore, string> originalMessage, 
        Exception error, 
        string dlqTopicName)
    {
        try
        {
            var dlqMessage = new Message<Null, string> { Value = originalMessage.Message.Value };
            dlqMessage.Headers = new Headers
            {
                { "error-reason", Encoding.UTF8.GetBytes(error.Message) },
                { "error-stacktrace", Encoding.UTF8.GetBytes(error.ToString()) },
                { "original-topic", Encoding.UTF8.GetBytes(originalMessage.Topic) },
                { "original-partition", Encoding.UTF8.GetBytes(originalMessage.Partition.ToString()) },
                { "original-offset", Encoding.UTF8.GetBytes(originalMessage.Offset.ToString()) }
            };

            await dlqProducer.ProduceAsync(dlqTopicName, dlqMessage);
            Console.WriteLine("Сообщение успешно отправлено в DLQ.");
        }
        catch (Exception dlqEx)
        {
            // ЭТО КРИТИЧЕСКАЯ СИТУАЦИЯ: мы не можем ни обработать сообщение, ни отправить его в DLQ.
            // Единственный безопасный вариант - остановить приложение и ждать вмешательства инженера.
            Console.WriteLine($"КАТАСТРОФА! Не удалось отправить сообщение в DLQ: {dlqEx.Message}. Останавливаем консьюмер.");
            // В реальном приложении здесь был бы вызов системы мониторинга (Prometheus, Zabbix) и/или
            // преднамеренное падение процесса, чтобы Kubernetes/Docker его перезапустил.
            throw; // Пробрасываем исключение наверх, что приведет к остановке приложения.
        }
    }
}