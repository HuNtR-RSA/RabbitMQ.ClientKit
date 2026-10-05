namespace RabbitMQ.ClientKit.Channel;

/// <summary>
/// Provides channels for publish operations.
/// </summary>
public interface IRabbitMqProducerChannelProvider : IAsyncDisposable
{
    /// <summary>
    /// Rents a channel for a producer operation.
    /// </summary>
    /// <param name="publisherConfirmationsEnabled">Whether publisher confirmations should be enabled on the rented channel.</param>
    /// <param name="confirmTimeout">The optional publisher confirmation timeout.</param>
    /// <param name="cancellationToken">The cancellation token for the rent operation.</param>
    /// <returns>A lease wrapping the rented channel.</returns>
    ValueTask<IRabbitMqChannelLease> RentAsync
    (
        bool publisherConfirmationsEnabled = false,
        TimeSpan? confirmTimeout = null,
        CancellationToken cancellationToken = default
    );
}
