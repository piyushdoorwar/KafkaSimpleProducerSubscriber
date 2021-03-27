using Confluent.Kafka;
using System;

namespace KafkaSimpleProducerSubscriber.KafkaOperators
{
    public class ConsumeMessageFromKafka
    {
        private readonly IConsumer<int, string> _kafkaConsumer;
        public ConsumeMessageFromKafka(IConsumer<int, string> kafkaConsumer)
        {
            _kafkaConsumer = kafkaConsumer;
        }

        public void Consume(string kafkaTopic)
        {

            _kafkaConsumer.Subscribe(kafkaTopic);
            while (true)
            {
                try
                {
                    var messageFetchedfromTopic = _kafkaConsumer.Consume();
                    Console.WriteLine(messageFetchedfromTopic.Message.Value);
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"Kafka Consumer on Topic {kafkaTopic} failed to consume message {exception.Message}");
                }
            }
        }
    }
}
