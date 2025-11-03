using Confluent.Kafka;

namespace KafkaAtLeastOnceConsumerTutorial;

// at-least-once консьюмер подразумевает что он старается обязательно обработать сообщение
// или положить его в очередь мертвых сообщений, если там неправильный json и его не получается обработать

// шаг 1 - Основа консьюмера
// наполняем метод логикой
// создаем настройки
// создаем консьюмер
// подписываемся на топик
// создаем цикл обработки
// и добавляем обработка ошибок через try catch

public class AtLeastOnceConsumer_Step_1
{
    public async Task ConsumeAsync(string brokerList, string topicName, CancellationToken ct = default)
    {
        Console.WriteLine("at-least-once консьюмер запущен");

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = "at-least-consumer-group",
            EnableAutoCommit = false, // Важно для at-least-once консьюмера
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        consumer.Subscribe(topicName);

        try
        {
            while (ct.IsCancellationRequested == false)
            {
                // синхронное получение значения из кафки
                var consumeResult = consumer.Consume(ct);
            
                // обработка
                var valueFromTopic = consumeResult.Message.Value;
                Console.WriteLine($"Обработано: {valueFromTopic}");
            
                // фиксация коммита
                consumer.Commit(consumeResult);
            }
        }
        catch (OperationCanceledException)
        {
            // тут обработка токена отмены 
            
            Console.WriteLine("Потребление остановлено.");
        }
        finally
        {
            // и завершение процесса обработки сообщений из топика
            // отписка от топика и перебалансировка консьюмеров
            
            consumer.Close();
        }
    }
    
    // вариант методе без обработки ошибок
    // просто настройки
    // создание консьюмера
    // подписка
    // и цикл обработки с ручным коммитом смещения
    public async Task ConsumeAsync_WithoutTryCatch(string brokerList, string topicName, CancellationToken ct = default)
    {
        Console.WriteLine("at-least-once консьюмер запущен");

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = "at-least-consumer-group",
            EnableAutoCommit = false, // Важно для at-least-once консьюмера
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        consumer.Subscribe(topicName);

        while (ct.IsCancellationRequested == false)
        {
            // синхронное получение значения из кафки
            var consumeResult = consumer.Consume(ct);
            
            // обработка
            var valueFromTopic = consumeResult.Message.Value;
            Console.WriteLine($"Обработано: {valueFromTopic}");
            
            // фиксация коммита
            consumer.Commit(consumeResult);
        }
    }
}