using KafkaDeliveryGuaranteesConsumers.AtLeastOnce.ErrorsHandling;

namespace KafkaDeliveryGuaranteesConsumers.AtLeastOnce;

public class AtLeastOnceExecuteConsumer_SkipAndLog
{
    public static void Execute()
    {
        // Установите Confluent.Kafka через NuGet, если еще не сделали это
        // dotnet add package Confluent.Kafka

        // Адрес для подключения с вашего компьютера
        string bootstrapServers = "localhost:29092";

        // Название топика, который вы создали в Kafka-UI
        string topicName = "my-test-topic";

        // Создаем и запускаем продюсер
        var producer = new AtLeastOnceConsumer_SkipAndLog();
        producer.Consume(bootstrapServers, topicName);

        Console.WriteLine("Программа завершена. Проверьте сообщения в Kafka-UI!");
    }
}