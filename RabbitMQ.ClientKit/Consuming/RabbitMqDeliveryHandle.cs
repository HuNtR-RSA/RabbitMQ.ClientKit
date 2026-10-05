using RabbitMQ.Client;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Serialization;
using RabbitMQ.ClientKit.Topology;

namespace RabbitMQ.ClientKit.Consuming;

/// <summary>
/// Default delivery handle that publishes replacements and settles on the consumer channel.
/// </summary>
public sealed class RabbitMqDeliveryHandle
(
    IChannel channel,
    ulong deliveryTag,
    IRabbitMqSerializer serializer
) : IRabbitMqDeliveryHandle
{
    private readonly IChannel _channel = channel ?? throw new ArgumentNullException(nameof(channel));
    private readonly IRabbitMqSerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private int _settled;

    /// <inheritdoc />
    public bool IsSettled => Volatile.Read(ref _settled) == 1;

    /// <inheritdoc />
    public Task PublishReplacementAsync<T>(T message, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return PublishCoreAsync(_serializer.Serialize(message), options, cancellationToken);
    }

    /// <inheritdoc />
    public Task PublishReplacementAsync(ReadOnlyMemory<byte> body, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return PublishCoreAsync(body, options, cancellationToken);
    }

    /// <inheritdoc />
    public async Task AckAsync(CancellationToken cancellationToken = default)
    {
        await _channel.BasicAckAsync(deliveryTag, false, cancellationToken).ConfigureAwait(false);
        Interlocked.Exchange(ref _settled, 1);
    }

    /// <inheritdoc />
    public async Task RejectAsync(CancellationToken cancellationToken = default)
    {
        await _channel.BasicRejectAsync(deliveryTag, false, cancellationToken).ConfigureAwait(false);
        Interlocked.Exchange(ref _settled, 1);
    }

    /// <inheritdoc />
    public async Task NackAsync(bool requeue = true, CancellationToken cancellationToken = default)
    {
        await _channel.BasicNackAsync(deliveryTag, false, requeue, cancellationToken).ConfigureAwait(false);
        Interlocked.Exchange(ref _settled, 1);
    }

    /// <inheritdoc />
    public Task ReplaceAndAckAsync<T>(T message, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return ReplaceAndAckCoreAsync(_serializer.Serialize(message), _serializer.ContentType, options, cancellationToken);
    }

    /// <inheritdoc />
    public Task ReplaceAndAckAsync(ReadOnlyMemory<byte> body, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        return ReplaceAndAckCoreAsync(body, null, options, cancellationToken);
    }

    private async Task PublishCoreAsync(ReadOnlyMemory<byte> body, RabbitMqPublishOptions options, CancellationToken cancellationToken)
    {
        await InitializeTopologyAsync(options, cancellationToken).ConfigureAwait(false);

        var properties = options.Properties?.ToBasicProperties() ?? new BasicProperties { Persistent = true };
        if (string.IsNullOrWhiteSpace(properties.ContentType))
        {
            properties.ContentType = _serializer.ContentType;
        }

        await _channel.BasicPublishAsync
        (
            options.ExchangeName,
            options.ResolveRoutingKey(),
            options.Mandatory,
            properties,
            body,
            cancellationToken
        ).ConfigureAwait(false);
    }

    private async Task ReplaceAndAckCoreAsync(
        ReadOnlyMemory<byte> body,
        string? defaultContentType,
        RabbitMqPublishOptions options,
        CancellationToken cancellationToken)
    {
        var replacementAttempted = false;
        var replacementConfirmed = false;

        try
        {
            replacementAttempted = true;
            await InitializeTopologyAsync(options, cancellationToken).ConfigureAwait(false);
            var properties = options.Properties?.ToBasicProperties() ?? new BasicProperties { Persistent = true };
            if (string.IsNullOrWhiteSpace(properties.ContentType) && !string.IsNullOrWhiteSpace(defaultContentType))
            {
                properties.ContentType = defaultContentType;
            }

            await _channel.BasicPublishAsync
            (
                options.ExchangeName,
                options.ResolveRoutingKey(),
                options.Mandatory,
                properties,
                body,
                cancellationToken
            ).ConfigureAwait(false);

            replacementConfirmed = true;
            await AckAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            if (replacementConfirmed)
            {
                try
                {
                    await AckAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // The confirmed replacement is durable. Requeueing the original would duplicate it.
                }
                finally
                {
                    Interlocked.Exchange(ref _settled, 1);
                }

                return;
            }

            if (replacementAttempted && _channel.IsOpen)
            {
                await NackAsync(requeue: true, CancellationToken.None).ConfigureAwait(false);
            }

            throw;
        }
    }

    private Task InitializeTopologyAsync(RabbitMqPublishOptions options, CancellationToken cancellationToken)
        => options.Topology is null
            ? Task.CompletedTask
            : RabbitMqTopologyInitializer.InitializeAsync(_channel, options.Topology, cancellationToken);
}
