using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;


namespace KafkaSimpleProducerSubscriber.KafkaOperators
{
    public class CreateKafkaTopics
    {
        public static void Create(bool create)
        {
            if (create)
            {
                var kafkaAdminClientConfig = new AdminClientConfig
                {
                    BootstrapServers = "127.0.0.1:9092"
                };

                var adminClient = new AdminClientBuilder(kafkaAdminClientConfig)
                    .Build();
                CreateTopicWithAdminClient(adminClient);
            }
        }

        private static void CreateTopicWithAdminClient(IAdminClient adminClient)
        {
            try
            {
                adminClient.CreateTopicsAsync(new TopicSpecification[]
                {
                        new TopicSpecification
                        {
                            Name = "topic01",
                            ReplicationFactor = 2,
                            NumPartitions = 4
                        },
                        new TopicSpecification
                        {
                            Name = "topic02",
                            ReplicationFactor = 2,
                            NumPartitions = 4
                        }
                });
                Console.WriteLine("Topic(s) created Successfully");
                Console.WriteLine();
            }
            catch (CreateTopicsException e)
            {
                Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
            }
        }
    }
}
