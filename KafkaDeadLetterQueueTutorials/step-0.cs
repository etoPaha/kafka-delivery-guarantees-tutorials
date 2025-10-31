using Confluent.Kafka;
using System.Text;

/// <summary>
/// Шаг 0: Подготовка. Создаем "скелет" класса
/// </summary>
public class DlqConsumer_Lesson_Step_0
{
    public async Task ConsumeAsync(string brokerList, string topicName, CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Консьюмер-урок запущен");
        
        // Здесь будет наш код
        
        await Task.CompletedTask; // Временная заглушка
    }
}