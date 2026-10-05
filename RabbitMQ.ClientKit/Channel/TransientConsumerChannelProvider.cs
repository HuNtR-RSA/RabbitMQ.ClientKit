using RabbitMQ.ClientKit.Connection;

namespace RabbitMQ.ClientKit.Channel;

/// <summary>
/// Creates a fresh consumer channel for every lease request.
/// </summary>
public sealed class TransientConsumerChannelProvider(IRabbitMqConnectionManager connectionManager) : IRabbitMqConsumerChannelProvider
{
    private readonly IRabbitMqConnectionManager _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));

    /// <inheritdoc />
    public async ValueTask<IRabbitMqChannelLease> RentAsync(string consumerName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerName);

        var options = RabbitMqChannelOptionsFactory.CreateChannelOptions(publisherConfirmationsEnabled: true);
        var channel = await _connectionManager.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);
        
        return new AsyncDisposableChannelLease(channel);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
