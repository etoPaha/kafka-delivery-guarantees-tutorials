using Confluent.Kafka;

namespace ExactlyOnceProducerAndConsumer.AI_Example;

public class ExactlyOnceConsumer
{
    public void Consume(string brokerList, string topicName, CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = "exactly-once-group",
            EnableAutoCommit = false, // Мы по-прежнему управляем коммитами вручную!

            // IsolationLevel = IsolationLevel.ReadCommitted - КЛЮЧЕВАЯ НАСТРОЙКА
            // Что это значит: "Показывать мне только те сообщения, которые являются
            // частью УСПЕШНО ЗАВЕРШЕННОЙ (закоммиченной) транзакции".
            //
            // По умолчанию стоит ReadUncommitted, что означает "показывать все".
            // ReadCommitted - это фильтр на стороне брокера. Брокер просто не будет
            // отдавать этому консьюмеру сообщения из открытых или откаченных транзакций.
            IsolationLevel = IsolationLevel.ReadCommitted
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe(topicName);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(cancellationToken);

                    // Логика обработки здесь выглядит ТОЧНО ТАК ЖЕ,
                    // как в AtLeastOnceConsumer. Магия происходит на стороне брокера,
                    // который отфильтровывает для нас "мусор".
                    Console.WriteLine($"Обработано (из закоммиченной транзакции): '{consumeResult.Message.Value}'");

                    // Мы все еще должны делать ручной коммит, чтобы сообщить брокеру,
                    // что НАША обработка прошла успешно.
                    consumer.Commit(consumeResult);
                }
            }
            // ... (catch и finally как в AtLeastOnceConsumer) ...
            catch (Exception e)
            {

            }
            finally
            {
                
            }
        }
    }
}