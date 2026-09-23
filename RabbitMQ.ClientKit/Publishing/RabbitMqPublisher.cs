using RabbitMQ.Client;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Serialization;
using RabbitMQ.ClientKit.Topology;

namespace RabbitMQ.ClientKit.Publishing;

/// <summary>
/// Publishes strongly typed messages to RabbitMQ.
/// </summary>
public sealed class RabbitMqPublisher
(
    IRabbitMqProducerChannelProvider producerChannelProvider,
    IRabbitMqSerializer serializer,
    RabbitMqTopologyInitializer topologyInitializer
)
{
    private readonly IRabbitMqProducerChannelProvider _producerChannelProvider = producerChannelProvider ?? throw new ArgumentNullException(nameof(producerChannelProvider));
    private readonly IRabbitMqSerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly RabbitMqTopologyInitializer _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));

    /// <summary>
    /// Publishes a payload using the supplied options.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="message">The payload to publish.</param>
    /// <param name="options">The publish options describing routing, topology, and properties.</param>
    /// <param name="cancellationToken">The cancellation token for the publish operation.</param>
    public async Task PublishAsync<T>(T message, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        await using var lease = await _producerChannelProvider.RentAsync(cancellationToken).ConfigureAwait(false);

        if (options.Topology is not null)
        {
            await RabbitMqTopologyInitializer.InitializeAsync(lease.Channel, options.Topology, cancellationToken).ConfigureAwait(false);
        }

        var body = _serializer.Serialize(message);
        var properties = options.Properties?.ToBasicProperties() ?? new BasicProperties { Persistent = true };

        if (string.IsNullOrWhiteSpace(properties.ContentType))
        {
            properties.ContentType = _serializer.ContentType;
        }

        await lease.Channel.BasicPublishAsync(
                options.ExchangeName,
                options.ResolveRoutingKey(),
                options.Mandatory,
                properties,
                body,
                cancellationToken)
            .ConfigureAwait(false);
    }
}
