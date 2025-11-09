using Confluent.Kafka;

namespace ExactlyOnceProducerAndConsumer.my_examples;

public class ExactlyOnceProducer_MyVersion
{
    public void ProduceInTransaction(string brokerList, string topicName)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = brokerList,
            //  идемпотентность продюсера
            // id продюсера - PID (producer id)
            EnableIdempotence = true,
            // уникальный id на случай падения
            TransactionalId = "unique-trasactional-id"
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();
        
        // инициализация транзакций
        producer.InitTransactions(TimeSpan.FromSeconds(10));

        try
        {
            producer.BeginTransaction();

            var message1 = new Message<Null, string>
            {
                Value = "cooбщение 1"
            };
            producer.Produce(topicName, message1);

            var message2 = new Message<Null, string>
            {
                Value = "сообщение 2"
            };
            producer.Produce(topicName, message2);
            
            producer.CommitTransaction(TimeSpan.FromSeconds(10));
            
            Console.WriteLine("Транзакция успешно зафиксирована.");
        }
        catch (KafkaException e)
        {
            Console.WriteLine($"Ошибка транзакации кафки: {e.Message}");
            
            producer.AbortTransaction();
        }
    }
}