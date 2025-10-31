using KafkaDeliveryGuaranties.AtLeastOnce;

var message1 = "сообщение с at-least-once - сбой";
var message2 = "сообщение с at-least-once";

await AtLeastOnceExecuteProducer.Execute(message2);