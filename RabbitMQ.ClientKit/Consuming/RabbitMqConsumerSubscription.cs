using RabbitMQ.ClientKit.Channel;
using RabbitMQ.Client;

namespace RabbitMQ.ClientKit.Consuming;

/// <summary>
/// Represents an active consumer subscription that can be stopped and disposed.
/// </summary>
public sealed class RabbitMqConsumerSubscription(
    IRabbitMqChannelLease lease,
    IChannel channel,
    string consumerTag,
    CancellationTokenSource cancellationTokenSource) : IAsyncDisposable
{
    private readonly IRabbitMqChannelLease _lease = lease ?? throw new ArgumentNullException(nameof(lease));
    private readonly IChannel _channel = channel ?? throw new ArgumentNullException(nameof(channel));
    private readonly CancellationTokenSource _cancellationTokenSource = cancellationTokenSource ?? throw new ArgumentNullException(nameof(cancellationTokenSource));
    private int _disposed;

    /// <summary>
    /// Gets the RabbitMQ consumer tag associated with the subscription.
    /// </summary>
    private string ConsumerTag { get; } = consumerTag ?? throw new ArgumentNullException(nameof(consumerTag));

    /// <summary>
    /// Gets a value indicating whether the subscription is active and its underlying channel is open.
    /// </summary>
    public bool IsHealthy => Volatile.Read(ref _disposed) == 0 && _channel.IsOpen;

    /// <summary>
    /// Stops the subscription, cancels the consumer, and releases the leased channel.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the stop operation.</param>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await _cancellationTokenSource.CancelAsync();

        try
        {
            if (_channel.IsOpen && !string.IsNullOrWhiteSpace(ConsumerTag))
            {
                await _channel.BasicCancelAsync(ConsumerTag, false, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _cancellationTokenSource.Dispose();
            await _lease.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => new(StopAsync());
}
