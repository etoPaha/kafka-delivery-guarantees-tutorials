namespace KafkaDeliveryGuaranties.AtLeastOnce;

public class AtLeastOnceExecuteProducer
{
    public static async Task Execute(string message)
    {
        // Установите Confluent.Kafka через NuGet, если еще не сделали это
        // dotnet add package Confluent.Kafka

        // Адрес для подключения с вашего компьютера
        string bootstrapServers = "localhost:29092";

        // Название топика, который вы создали в Kafka-UI
        string topicName = "my-test-topic";

        // Создаем и запускаем продюсер
        var producer = new AtLeastOnceProducer(); // или AtLeastOnceProducer
        await producer.ProduceAsync(bootstrapServers, topicName, message);

        Console.WriteLine("Программа завершена. Проверьте сообщения в Kafka-UI!");
    }
}