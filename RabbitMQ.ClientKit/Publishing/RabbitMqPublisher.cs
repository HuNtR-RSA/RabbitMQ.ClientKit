using RabbitMQ.Client;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Models;
using RabbitMQ.ClientKit.Serialization;
using RabbitMQ.ClientKit.Topology;

namespace RabbitMQ.ClientKit.Publishing;

/// <summary>
/// Publishes strongly typed messages to RabbitMQ.
/// </summary>
public sealed class RabbitMqPublisher
(
    IRabbitMqProducerChannelProvider producerChannelProvider,
    IRabbitMqSerializer serializer
)
{
    private readonly IRabbitMqProducerChannelProvider _producerChannelProvider = producerChannelProvider ?? throw new ArgumentNullException(nameof(producerChannelProvider));
    private readonly IRabbitMqSerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));

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

        await using var lease = await _producerChannelProvider.RentAsync
        (
            options.PublisherConfirms,
            options.ConfirmTimeout,
            cancellationToken
        ).ConfigureAwait(false);
        
        await InitializeTopologyAsync(lease.Channel, options, cancellationToken).ConfigureAwait(false);
        
        await PublishCoreAsync
        (
            lease.Channel,
            _serializer.Serialize(message),
            _serializer.ContentType,
            options,
            cancellationToken
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Publishes a raw byte payload without running serialization.
    /// </summary>
    /// <param name="body">The raw payload to publish.</param>
    /// <param name="options">The publish options describing routing, topology, and properties.</param>
    /// <param name="cancellationToken">The cancellation token for the publish operation.</param>
    public async Task PublishAsync(ReadOnlyMemory<byte> body, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
    {
        ValidateOptions(options);

        await using var lease = await _producerChannelProvider.RentAsync(
            options.PublisherConfirms,
            options.ConfirmTimeout,
            cancellationToken).ConfigureAwait(false);
        
        await InitializeTopologyAsync(lease.Channel, options, cancellationToken).ConfigureAwait(false);
        
        await PublishCoreAsync
        (
            lease.Channel,
            body,
            null,
            options,
            cancellationToken
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Publishes multiple payloads using the supplied options.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="messages">The payloads to publish.</param>
    /// <param name="options">The publish options describing routing, topology, and properties shared by the batch.</param>
    /// <param name="cancellationToken">The cancellation token for the publish operation.</param>
    public async Task<RabbitMqPublishBatchResult> PublishBatchAsync<T>(IEnumerable<T> messages, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        ValidateOptions(options);

        using var enumerator = messages.GetEnumerator();
        if (!enumerator.MoveNext())
        {
            return RabbitMqPublishBatchResult.Empty;
        }

        IRabbitMqChannelLease lease;
        try
        {
            lease = await _producerChannelProvider.RentAsync
            (
                options.PublisherConfirms,
                options.ConfirmTimeout,
                cancellationToken
            ).ConfigureAwait(false);
        }
        catch
        {
            return RabbitMqPublishBatchResult.NotSent;
        }

        await using (lease)
        {
            try
            {
                await InitializeTopologyAsync(lease.Channel, options, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                return RabbitMqPublishBatchResult.NotSent;
            }

            var attempted = 0;
            var confirmed = 0;
            try
            {
                do
                {
                    var body = _serializer.Serialize(enumerator.Current);
                    attempted++;
                    await PublishCoreAsync
                    (
                        lease.Channel,
                        body,
                        _serializer.ContentType,
                        options,
                        cancellationToken
                    ).ConfigureAwait(false);
                    confirmed++;
                }
                while (enumerator.MoveNext());

                return RabbitMqPublishBatchResult.Confirmed(confirmed);
            }
            catch
            {
                return attempted == 0
                    ? RabbitMqPublishBatchResult.NotSent
                    : RabbitMqPublishBatchResult.Unconfirmed(attempted, confirmed);
            }
        }
    }

    /// <summary>
    /// Publishes multiple raw byte payloads using the supplied options.
    /// </summary>
    /// <param name="messages">The raw payloads to publish.</param>
    /// <param name="options">The publish options describing routing, topology, and properties shared by the batch.</param>
    /// <param name="cancellationToken">The cancellation token for the publish operation.</param>
    public async Task<RabbitMqPublishBatchResult> PublishBatchAsync(IEnumerable<ReadOnlyMemory<byte>> messages, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        ValidateOptions(options);

        using var enumerator = messages.GetEnumerator();
        if (!enumerator.MoveNext())
        {
            return RabbitMqPublishBatchResult.Empty;
        }

        IRabbitMqChannelLease lease;
        try
        {
            lease = await _producerChannelProvider.RentAsync
            (
                options.PublisherConfirms,
                options.ConfirmTimeout,
                cancellationToken
            ).ConfigureAwait(false);
        }
        catch
        {
            return RabbitMqPublishBatchResult.NotSent;
        }

        await using (lease)
        {
            try
            {
                await InitializeTopologyAsync(lease.Channel, options, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                return RabbitMqPublishBatchResult.NotSent;
            }

            var attempted = 0;
            var confirmed = 0;
            try
            {
                do
                {
                    attempted++;
                    await PublishCoreAsync
                    (
                        lease.Channel,
                        enumerator.Current,
                        null,
                        options,
                        cancellationToken
                    ).ConfigureAwait(false);
                    confirmed++;
                }
                while (enumerator.MoveNext());

                return RabbitMqPublishBatchResult.Confirmed(confirmed);
            }
            catch
            {
                return RabbitMqPublishBatchResult.Unconfirmed(attempted, confirmed);
            }
        }
    }

    /// <summary>
    /// Declares the specified topology on a producer channel without publishing any messages.
    /// </summary>
    /// <param name="topology">The topology to declare.</param>
    /// <param name="cancellationToken">The cancellation token for the declaration operation.</param>
    public async Task DeclareAsync(RabbitMqTopologyOptions topology, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topology);

        await using var lease = await _producerChannelProvider.RentAsync
        (
            publisherConfirmationsEnabled: false,
            confirmTimeout: null,
            cancellationToken
        ).ConfigureAwait(false);

        await RabbitMqTopologyInitializer.InitializeAsync(lease.Channel, topology, cancellationToken).ConfigureAwait(false);
    }

    private async Task InitializeTopologyAsync(IChannel channel, RabbitMqPublishOptions options, CancellationToken cancellationToken)
    {
        if (options.Topology is not null)
        {
            await RabbitMqTopologyInitializer.InitializeAsync(channel, options.Topology, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task PublishCoreAsync
    (
        IChannel channel,
        ReadOnlyMemory<byte> body,
        string? defaultContentType,
        RabbitMqPublishOptions options,
        CancellationToken cancellationToken
    )
    {
        var properties = options.Properties?.ToBasicProperties() ?? new BasicProperties { Persistent = true };

        if (string.IsNullOrWhiteSpace(properties.ContentType) && !string.IsNullOrWhiteSpace(defaultContentType))
        {
            properties.ContentType = defaultContentType;
        }

        await channel.BasicPublishAsync
        (
            options.ExchangeName,
            options.ResolveRoutingKey(),
            options.Mandatory,
            properties,
            body,
            cancellationToken
        ).ConfigureAwait(false);
    }

    private static void ValidateOptions(RabbitMqPublishOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
    }
}
