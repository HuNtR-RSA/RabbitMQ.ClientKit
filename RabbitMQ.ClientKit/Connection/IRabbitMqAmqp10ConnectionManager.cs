using RabbitMQ.AMQP.Client;

namespace RabbitMQ.ClientKit.Connection;

/// <summary>
/// Manages the dedicated AMQP 1.0 connection used by AMQP 1.0 consumers.
/// AMQP 0-9-1 and AMQP 1.0 cannot share a TCP connection.
/// </summary>
public interface IRabbitMqAmqp10ConnectionManager : IAsyncDisposable
{
    /// <summary>
    /// Gets an open AMQP 1.0 connection, creating one if required.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the connection acquisition.</param>
    /// <returns>An open AMQP 1.0 connection.</returns>
    ValueTask<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);
}
