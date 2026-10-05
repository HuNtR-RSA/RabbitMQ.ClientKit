using RabbitMQ.ClientKit.Connection;

namespace RabbitMQ.ClientKit.Channel;

/// <summary>
/// Creates a fresh producer channel for every lease request.
/// </summary>
public sealed class TransientProducerChannelProvider(IRabbitMqConnectionManager connectionManager) : IRabbitMqProducerChannelProvider
{
    private readonly IRabbitMqConnectionManager _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));

    /// <inheritdoc />
    public async ValueTask<IRabbitMqChannelLease> RentAsync
    (
        bool publisherConfirmationsEnabled = false,
        TimeSpan? confirmTimeout = null,
        CancellationToken cancellationToken = default
    )
    {
        var options = RabbitMqChannelOptionsFactory.CreateChannelOptions(publisherConfirmationsEnabled, confirmTimeout);
        var channel = await _connectionManager.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);
        RabbitMqChannelOptionsFactory.ConfigureChannel(channel, publisherConfirmationsEnabled, confirmTimeout);
        
        return new AsyncDisposableChannelLease(channel);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
