namespace RabbitMQ.ClientKit.Channel;

/// <summary>
/// Provides channels for publish operations.
/// </summary>
public interface IRabbitMqProducerChannelProvider : IAsyncDisposable
{
    /// <summary>
    /// Rents a channel for a producer operation.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the rent operation.</param>
    /// <returns>A lease wrapping the rented channel.</returns>
    ValueTask<IRabbitMqChannelLease> RentAsync(CancellationToken cancellationToken = default);
}
