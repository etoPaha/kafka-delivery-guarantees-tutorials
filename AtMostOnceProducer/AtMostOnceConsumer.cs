using Confluent.Kafka;

namespace AtMostOnceProducer;

public class AtMostOnceConsumer
{
    public void Consume(string brokerList, string topicName, CancellationToken ct)
    {
        // Создает настройки для консьюмера / потребителя
        // основные моменты для at-most-once это
        // EnableAutoCommit = true; и AutoOffsetReset = AutoOffsetReset.Latest;
        var config = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = "at-most-once-group",
            // !!! важные настройки
            EnableAutoCommit = true,
            AutoOffsetReset = AutoOffsetReset.Latest
            // !!!
        };
        
        // создаем консьюмер
        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        
        // подписываемся на топик
        consumer.Subscribe(topicName);

        try
        {
            // основной цикл обработки
            while (ct.IsCancellationRequested == false)
            {
                var consumeResult = consumer.Consume(ct);

                var messageVal = consumeResult.Message.Value;
                Console.WriteLine($"Обработка сообщения: {messageVal}");

                // ручной коммит не нужен
                // при настройке EnableAutoCommit работает фоновый поток
                // который фиксирует offset каждые 5 сек
            }
        }
        catch (OperationCanceledException)
        {
            // обработка токена отмены
            // завершение цикла обработки сообщений из топика
        }
        finally
        {
            // закрываем консьюмер
            consumer.Close();
        }
    }
    
    // простой пример метода без лишних деталей
    public void Consume_SimpleVersion(string brokerList, string topicName, CancellationToken ct)
    {
        // Создает настройки для консьюмера / потребителя
        // основные моменты для at-most-once это
        // EnableAutoCommit = true; и AutoOffsetReset = AutoOffsetReset.Latest;
        var config = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = "at-most-once-group",
            // !!! важные настройки
            EnableAutoCommit = true,
            AutoOffsetReset = AutoOffsetReset.Latest
            // !!!
        };
        
        // создаем консьюмер
        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        
        // подписываемся на топик
        consumer.Subscribe(topicName);
        
        // основной цикл обработки
        while (ct.IsCancellationRequested == false)
        {
            var consumeResult = consumer.Consume(ct);

            // ручной коммит не нужен
            // при настройке EnableAutoCommit работает фоновый поток
            // который фиксирует offset каждые 5 сек
        }
    }
}