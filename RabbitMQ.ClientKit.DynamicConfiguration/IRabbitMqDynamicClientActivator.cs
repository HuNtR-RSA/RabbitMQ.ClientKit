using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit.DynamicConfiguration;

/// <summary>
/// Creates RabbitMQ clients for dynamically loaded connection definitions.
/// </summary>
public interface IRabbitMqDynamicClientActivator
{
    /// <summary>
    /// Creates a client for the supplied connection definition.
    /// </summary>
    /// <param name="connectionName">The optional logical connection name.</param>
    /// <param name="connectionOptions">The connection definition.</param>
    /// <returns>A client configured for the supplied connection.</returns>
    RabbitMqClient CreateClient(string? connectionName, RabbitMqConnectionOptions connectionOptions);
}
