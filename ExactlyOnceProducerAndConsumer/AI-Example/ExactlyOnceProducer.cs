using Confluent.Kafka;

namespace ExactlyOnceProducerAndConsumer.AI_Example;

public class ExactlyOnceProducer
{
    public void ProduceInTransaction(string brokerList, string topicName)
    {
        // БЛОК A: Конфигурация для "пуленепробиваемой" доставки
        var config = new ProducerConfig
        {
            BootstrapServers = brokerList,

            // 1. EnableIdempotence = true - "Включить защиту от дублей"
            // Что это делает? Продюсер получает уникальный ID (PID) и начинает
            // нумеровать каждое отправляемое сообщение (sequence number).
            // Брокер отслеживает последний полученный номер для каждого PID.
            // Если продюсер из-за сбоя сети отправит сообщение №5 дважды,
            // брокер примет его в первый раз, а во второй раз увидит "ага,
            // я уже видел сообщение №5 от этого продюсера" и просто отбросит дубликат.
            // ВАЖНО: Эта настройка автоматически устанавливает Acks = All для максимальной надежности.
            EnableIdempotence = true,

            // 2. TransactionalId = "unique-transactional-id" - "Паспорт для транзакций"
            // Что это делает? Это уникальное имя, которое позволяет продюсеру
            // "пережить" перезапуск. Если ваше приложение упадет посреди транзакции,
            // после перезапуска оно сможет подключиться с тем же TransactionalId,
            // и Kafka поможет ему либо завершить, либо откатить зависшую транзакцию.
            // Это гарантирует, что не останется "полузавершенных" дел.
            TransactionalId = "unique-transactional-id-for-this-producer-instance"
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            // БЛОК B: Инициализация. "Предъявите ваш паспорт"
            // Перед началом работы продюсер должен "зарегистрировать" свой TransactionalId в Kafka.
            producer.InitTransactions(TimeSpan.FromSeconds(10));

            try
            {
                // БЛОК C: Атомарная операция. "Начинаем банковский перевод"
                // 1. Начало транзакции
                producer.BeginTransaction();

                // 2. Отправка сообщений ВНУТРИ транзакции.
                // Эти сообщения еще не видны консьюмерам. Они как бы в "режиме ожидания".
                producer.Produce(topicName, new Message<Null, string> { Value = "сообщение 1 в транзакции" });
                producer.Produce(topicName, new Message<Null, string> { Value = "сообщение 2 в транзакции" });
                
                // --- Здесь может быть сбой! ---
                // Если приложение упадет здесь, транзакция останется незавершенной.
                // Kafka через некоторое время автоматически ее откатит (Abort).

                // 3. Фиксация транзакции. "Подтвердить перевод"
                // Это команда "Все прошло успешно, делаем изменения видимыми".
                // Только после этого вызова сообщения станут доступны для чтения консьюмерам.
                producer.CommitTransaction(TimeSpan.FromSeconds(10));
                Console.WriteLine("Транзакция успешно зафиксирована.");
            }
            catch (KafkaException e)
            {
                // БЛОК D: Откат. "Отменить перевод"
                Console.WriteLine($"Ошибка транзакции: {e.Message}");
                // Если что-то пошло не так (например, один из топиков недоступен),
                // мы явно отменяем всю операцию. Все отправленные сообщения
                // внутри этой транзакции будут удалены.
                producer.AbortTransaction();
            }
        }
    }
}