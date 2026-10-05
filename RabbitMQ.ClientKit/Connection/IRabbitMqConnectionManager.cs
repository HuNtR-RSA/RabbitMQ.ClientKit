using RabbitMQ.Client;

namespace RabbitMQ.ClientKit.Connection;

/// <summary>
/// Manages the shared RabbitMQ connection used to create channels.
/// </summary>
public interface IRabbitMqConnectionManager : IAsyncDisposable
{
    /// <summary>
    /// Gets an open connection, creating one if required.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the connection acquisition.</param>
    /// <returns>An open RabbitMQ connection.</returns>
    ValueTask<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a channel from the managed connection.
    /// </summary>
    /// <param name="options">The channel creation options specifying publisher confirmations and tracking.</param>
    /// <param name="cancellationToken">The cancellation token for the channel creation.</param>
    /// <returns>A newly created RabbitMQ channel.</returns>
    Task<IChannel> CreateChannelAsync
    (
        CreateChannelOptions? options = null,
        CancellationToken cancellationToken = default
    );
}
