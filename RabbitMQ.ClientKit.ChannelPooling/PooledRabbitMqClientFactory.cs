using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Connection;
using RabbitMQ.ClientKit.Serialization;

namespace RabbitMQ.ClientKit.ChannelPooling;

/// <summary>
/// Creates <see cref="RabbitMqClient" /> instances backed by pooled producer channels and reusable consumer channels.
/// </summary>
public static class PooledRabbitMqClientFactory
{
    /// <summary>
    /// Creates a client using the channel pooling extension strategy.
    /// </summary>
    /// <param name="connectionOptions">The broker connection options.</param>
    /// <param name="poolingOptions">The producer channel pooling options.</param>
    /// <param name="serializer">The optional payload serializer. JSON is used by default.</param>
    /// <returns>A configured <see cref="RabbitMqClient" />.</returns>
    public static RabbitMqClient Create
    (
        RabbitMqConnectionOptions connectionOptions,
        RabbitMqChannelPoolingOptions? poolingOptions = null,
        IRabbitMqSerializer? serializer = null
    )
    {
        ArgumentNullException.ThrowIfNull(connectionOptions);

        var connectionManager = new RabbitMqConnectionManager(connectionOptions);
        var resolvedSerializer = serializer ?? new JsonRabbitMqSerializer();
        IRabbitMqProducerChannelProvider producerChannelProvider = new PooledProducerChannelProvider(connectionManager, poolingOptions);
        IRabbitMqConsumerChannelProvider consumerChannelProvider = new ReusableConsumerChannelProvider(connectionManager);

        return new RabbitMqClient
        (
            connectionManager,
            resolvedSerializer,
            producerChannelProvider,
            consumerChannelProvider,
            ownsDependencies: true
        );
    }
}
