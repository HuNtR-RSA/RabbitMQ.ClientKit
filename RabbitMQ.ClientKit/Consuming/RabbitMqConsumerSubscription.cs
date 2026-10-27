using RabbitMQ.ClientKit.Channel;
using RabbitMQ.Client;

namespace RabbitMQ.ClientKit.Consuming;

/// <summary>
/// Represents an active consumer subscription that can be stopped and disposed.
/// </summary>
public sealed class RabbitMqConsumerSubscription : IAsyncDisposable
{
    private readonly IRabbitMqConsumerSession _session;
    private int _disposed;

    /// <summary>
    /// Initializes a subscription over an AMQP 0-9-1 channel lease.
    /// </summary>
    /// <param name="lease">The leased consumer channel.</param>
    /// <param name="channel">The AMQP 0-9-1 channel used by the consumer.</param>
    /// <param name="consumerTag">The RabbitMQ consumer tag.</param>
    /// <param name="cancellationTokenSource">The subscription cancellation source.</param>
    public RabbitMqConsumerSubscription(
        IRabbitMqChannelLease lease,
        IChannel channel,
        string consumerTag,
        CancellationTokenSource cancellationTokenSource)
        : this(new Amqp091ConsumerSession(lease, channel, consumerTag, cancellationTokenSource))
    {
    }

    internal RabbitMqConsumerSubscription(IRabbitMqConsumerSession session)
    {
        _session = session ?? throw new ArgumentNullException(nameof(session));
    }

    /// <summary>
    /// Gets a value indicating whether the subscription is active and its underlying consumer is open.
    /// </summary>
    public bool IsHealthy => Volatile.Read(ref _disposed) == 0 && _session.IsHealthy;

    /// <summary>
    /// Stops the subscription, cancels the consumer, and releases the leased resources.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the stop operation.</param>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await _session.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => new(StopAsync());
}
