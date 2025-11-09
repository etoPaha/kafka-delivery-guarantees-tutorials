using Confluent.Kafka;

namespace AtMostOnceProducer;

// Пример создания продюсера кафки с гарантией доставки at-most-once
// at-most-once гарантия доставки подразумевает доставку не более одного раза
// т.е. какие-то сообщения могут быть утеряны

// at-most-once - не более одного раза

public class AtMostOnceProducer
{
    public async Task ProduceAsync(string brokerList, string topicName)
    {
        // нужно сконфигурировать продюсера
        // создать продюсера
        // отправить сообщение
        // ! продюсер не ждет от брокера кафки что он запишет сообщение !
        // работа в формате отправил и забыл

        // Acks - параметр настраивает подтверждение от пользователя
        var config = new ProducerConfig
        {
            BootstrapServers = brokerList,
            Acks = Acks.None // 0 - Мы не ждем подтверждения
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        try
        {
            var message = new Message<Null, string>
            {
                Value = "cообщение - at-most-once producer"
            };
            await producer.ProduceAsync(topicName, message);

            Console.WriteLine("Сообщение отправлено (без ожидания подтверждения)");
        }
        catch (Exception ex)
        {
            // обработка ошибок при отправке
            // чтобы быть вкурсе проблем
            Console.WriteLine();
        }
        
    }
    
    // простая версия без лишних деталей
    public async Task ProduceAsync_SimpleVersion(string brokerList, string topicName)
    {
        // нужно сконфигурировать продюсера
        // создать продюсера
        // отправить сообщение
        // ! продюсер не ждет от брокера кафки что он запишет сообщение !
        // работа в формате отправил и забыл

        // Acks - параметр настраивает подтверждение от пользователя
        var config = new ProducerConfig
        {
            BootstrapServers = brokerList,
            Acks = Acks.None // 0 - Мы не ждем подтверждения
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        var message = new Message<Null, string>
        {
            Value = "cообщение - at-most-once producer"
        };
        await producer.ProduceAsync(topicName, message);
    }
}