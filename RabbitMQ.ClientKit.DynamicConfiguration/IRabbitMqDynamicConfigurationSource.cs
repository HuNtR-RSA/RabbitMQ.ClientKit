namespace RabbitMQ.ClientKit.DynamicConfiguration;

/// <summary>
/// Loads reloadable RabbitMQ configuration snapshots from an external source such as a database or configuration provider.
/// </summary>
public interface IRabbitMqDynamicConfigurationSource
{
    /// <summary>
    /// Loads the latest available RabbitMQ configuration snapshot.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the load operation.</param>
    /// <returns>The latest configuration snapshot.</returns>
    ValueTask<RabbitMqDynamicConfigurationSnapshot> GetConfigurationAsync(CancellationToken cancellationToken = default);
}
