namespace KafkaDeliveryGuaranties.AtMostOnce;

public class AtMostOnceExample
{
    public static async Task Execute()
    {
        // Установите Confluent.Kafka через NuGet, если еще не сделали это
        // dotnet add package Confluent.Kafka

        // Адрес для подключения с вашего компьютера
        string bootstrapServers = "localhost:29092";

        // Название топика, который вы создали в Kafka-UI
        string topicName = "my-test-topic";

        // Создаем и запускаем продюсер
        var producer = new AtMostOnceProducer(); // или AtLeastOnceProducer
        await producer.ProduceAsync(bootstrapServers, topicName);

        Console.WriteLine("Программа завершена. Проверьте сообщения в Kafka-UI!");
    }
}