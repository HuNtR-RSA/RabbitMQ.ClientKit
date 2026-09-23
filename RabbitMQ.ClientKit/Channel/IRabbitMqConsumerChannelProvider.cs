namespace RabbitMQ.ClientKit.Channel;

/// <summary>
/// Provides channels for consumer registrations.
/// </summary>
public interface IRabbitMqConsumerChannelProvider : IAsyncDisposable
{
    /// <summary>
    /// Rents a channel for a consumer identified by name.
    /// </summary>
    /// <param name="consumerName">The logical consumer name used by the provider strategy.</param>
    /// <param name="cancellationToken">The cancellation token for the rent operation.</param>
    /// <returns>A lease wrapping the rented channel.</returns>
    ValueTask<IRabbitMqChannelLease> RentAsync(string consumerName, CancellationToken cancellationToken = default);
}
