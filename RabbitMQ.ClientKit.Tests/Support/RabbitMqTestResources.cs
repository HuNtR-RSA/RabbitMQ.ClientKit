using RabbitMQ.Client;
using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit.Tests.Support;

internal static class RabbitMqTestResources
{
    public static string CreateUniqueName(string prefix) => $"{prefix}.{Guid.NewGuid():N}";

    public static RabbitMqTopologyOptions CreateQueueTopology(string queueName) => new()
    {
        Queue = new RabbitMqQueueOptions
        {
            Name = queueName,
            Durable = false,
            AutoDelete = true
        }
    };

    public static RabbitMqTopologyOptions CreateDirectTopology(string exchangeName, string queueName, string routingKey) => new()
    {
        Exchange = new RabbitMqExchangeOptions
        {
            Name = exchangeName,
            Type = ExchangeType.Direct,
            Durable = false,
            AutoDelete = true
        },
        Queue = new RabbitMqQueueOptions
        {
            Name = queueName,
            Durable = false,
            AutoDelete = true
        },
        Bindings =
        [
            new RabbitMqQueueBindingOptions
            {
                ExchangeName = exchangeName,
                QueueName = queueName,
                RoutingKey = routingKey
            }
        ]
    };
}
