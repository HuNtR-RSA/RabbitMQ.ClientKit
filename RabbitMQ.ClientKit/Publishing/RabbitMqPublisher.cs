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
        ValidateOptions(options);

        await using var lease = await _producerChannelProvider.RentAsync(cancellationToken).ConfigureAwait(false);
        await InitializeTopologyAsync(lease.Channel, options, cancellationToken).ConfigureAwait(false);
        await PublishCoreAsync(lease.Channel, message, options, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Publishes multiple payloads using the supplied options.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="messages">The payloads to publish.</param>
    /// <param name="options">The publish options describing routing, topology, and properties shared by the batch.</param>
    /// <param name="cancellationToken">The cancellation token for the publish operation.</param>
    public async Task PublishBatchAsync<T>(IEnumerable<T> messages, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        ValidateOptions(options);

        using var enumerator = messages.GetEnumerator();
        if (!enumerator.MoveNext())
        {
            return;
        }

        await using var lease = await _producerChannelProvider.RentAsync(cancellationToken).ConfigureAwait(false);
        await InitializeTopologyAsync(lease.Channel, options, cancellationToken).ConfigureAwait(false);

        do
        {
            await PublishCoreAsync(lease.Channel, enumerator.Current, options, cancellationToken).ConfigureAwait(false);
        }
        while (enumerator.MoveNext());
    }

    private async Task InitializeTopologyAsync(IChannel channel, RabbitMqPublishOptions options, CancellationToken cancellationToken)
    {
        if (options.Topology is not null)
        {
            await RabbitMqTopologyInitializer.InitializeAsync(channel, options.Topology, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task PublishCoreAsync<T>(IChannel channel, T message, RabbitMqPublishOptions options, CancellationToken cancellationToken)
    {
        var body = _serializer.Serialize(message);
        var properties = options.Properties?.ToBasicProperties() ?? new BasicProperties { Persistent = true };

        if (string.IsNullOrWhiteSpace(properties.ContentType))
        {
            properties.ContentType = _serializer.ContentType;
        }

        await channel.BasicPublishAsync(
                options.ExchangeName,
                options.ResolveRoutingKey(),
                options.Mandatory,
                properties,
                body,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static void ValidateOptions(RabbitMqPublishOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
    }
}
