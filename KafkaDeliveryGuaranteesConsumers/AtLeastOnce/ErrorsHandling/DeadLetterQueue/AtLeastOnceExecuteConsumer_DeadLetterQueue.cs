namespace KafkaDeliveryGuaranteesConsumers.AtLeastOnce.ErrorsHandling.DeadLetterQueue;

public class AtLeastOnceExecuteConsumer_DeadLetterQueue
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
        var producer = new AtLeastOnceConsumer_DeadLetterQueue();
        await producer.ConsumeAsync(bootstrapServers, topicName);

        Console.WriteLine("Программа завершена. Проверьте сообщения в Kafka-UI!");
    }
}