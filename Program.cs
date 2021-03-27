using Confluent.Kafka;
using KafkaSimpleProducerSubscriber.KafkaOperators;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;

namespace KafkaSimpleProducerSubscriber
{
    public class Program
    {
        /// <summary>
        /// Gets the configuration.
        /// </summary>
        /// <value>The configuration.</value>
        public IConfiguration Configuration { get; }
        public Program(IConfiguration configuration)
        {
            Configuration = configuration;
        }
        static async Task Main()
        {
            var topic = "OrderEventQA2";
            var bootstrapServers = "127.0.0.1:9092";
            CreateKafkaTopics.Create(false);
            PublishMessageToKafka publishMessageToKafka = KafkaProducerBuilder(bootstrapServers);
            await publishMessageToKafka.Publish(topic);

            ConsumeMessageFromKafka consumeMessageFromKafka = KafkaConsumerBuilder(bootstrapServers);
            consumeMessageFromKafka.Consume(topic);

        }

        private static ConsumeMessageFromKafka KafkaConsumerBuilder(string bootstrapServers)
        {
            var config = new ConsumerConfig
            {
                GroupId = "ConsumerGroup01",
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            var kafkaConsumer = new ConsumerBuilder<int, string>(config).Build();
            var consumeMessageFromKafka = new ConsumeMessageFromKafka(kafkaConsumer);
            return consumeMessageFromKafka;
        }

        private static PublishMessageToKafka KafkaProducerBuilder(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                Acks = Acks.All
            };
            var kafkaProducer = new ProducerBuilder<int, string>(producerConfig).Build();
            var publishMessageToKafka = new PublishMessageToKafka(kafkaProducer);
            return publishMessageToKafka;
        }
    }
}
