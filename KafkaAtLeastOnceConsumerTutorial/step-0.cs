namespace KafkaAtLeastOnceConsumerTutorial;

// шаг 0 - база
// скелет класса
// пока без внутряки просто метод, параметры и заглушки

public class AtLeastOnce_Consumer_Step_0
{
    public async Task ConsumeAsync(string brokerList, string topicName, CancellationToken ct = default)
    {
        Console.WriteLine("at-least-once консьюмер запущен");
        
        // тут должна быть обработка

        await Task.CompletedTask; // заглушка для асинхронного метода
    }
}