using Confluent.Kafka;

namespace AtMostOnceProducer;

public class AtMostOnceConsumer_Example
{
    public void Consume(string brokerList, string topicName, CancellationToken cancellationToken)
    {
        // БЛОК A: Конфигурация "Доверяй, но не проверяй"
        var config = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = "at-most-once-group",

            // EnableAutoCommit = true - КЛЮЧЕВАЯ НАСТРОЙКА
            // Что это значит: "Я доверяю библиотеке Confluent.Kafka самой решать,
            // когда делать коммит". Библиотека будет делать коммиты периодически
            // в фоновом режиме (по умолчанию каждые 5 секунд).
            // Коммит будет сделан для самого последнего смещения, которое вернул
            // метод Consume().
            EnableAutoCommit = true,
            // AutoCommitIntervalMs = 5000, // Это интервал по умолчанию

            AutoOffsetReset = AutoOffsetReset.Latest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe(topicName);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    // 1. ПОЛУЧЕНИЕ СООБЩЕНИЯ
                    // Метод Consume() возвращает нам сообщение. В этот момент
                    // библиотека уже считает его "кандидатом на авто-коммит".
                    var consumeResult = consumer.Consume(cancellationToken);

                    // 2. ФОНОВЫЙ АВТО-КОММИТ (может произойти в любой момент)
                    // Представьте, что здесь, в параллельном потоке, таймер
                    // библиотеки срабатывает и отправляет коммит для смещения,
                    // которое мы ТОЛЬКО ЧТО получили.

                    // 3. НАША БИЗНЕС-ЛОГИКА
                    Console.WriteLine($"Начинаю обработку: '{consumeResult.Message.Value}'");
                    // Имитируем долгую работу или сбой
                    if (consumeResult.Message.Value.Contains("сбой"))
                    {
                        Console.WriteLine("Критический сбой во время обработки!");
                        // Приложение падает ЗДЕСЬ
                        throw new Exception("Сбой!"); 
                    }
                    Console.WriteLine("Обработка успешно завершена.");
                    
                    // НЕТ РУЧНОГО ВЫЗОВА consumer.Commit()!
                }
            }
            catch (OperationCanceledException) { /* ... */ }
            finally { consumer.Close(); }
        }
    }
}