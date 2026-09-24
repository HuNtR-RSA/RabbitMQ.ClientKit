namespace RabbitMQ.ClientKit.DynamicConfiguration;

/// <summary>
/// Resolves RabbitMQ clients and named endpoint handles from a reloadable configuration source.
/// </summary>
public interface IRabbitMqDynamicEndpointResolver
{
    /// <summary>
    /// Forces a refresh from the underlying configuration source.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the refresh operation.</param>
    ValueTask RefreshAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the latest loaded configuration snapshot.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the load operation.</param>
    ValueTask<RabbitMqDynamicConfigurationSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves a client for the supplied connection name, or the default connection when omitted.
    /// </summary>
    /// <param name="connectionName">The optional connection name.</param>
    /// <param name="cancellationToken">The cancellation token for the load operation.</param>
    ValueTask<RabbitMqClient> GetClientAsync(string? connectionName = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves a producer handle that reads the latest configuration on each use.
    /// </summary>
    /// <param name="name">The producer name.</param>
    RabbitMqDynamicProducer GetProducer(string name);

    /// <summary>
    /// Resolves a consumer handle that reads the latest configuration on each use.
    /// </summary>
    /// <param name="name">The consumer name.</param>
    RabbitMqDynamicConsumer GetConsumer(string name);
}
