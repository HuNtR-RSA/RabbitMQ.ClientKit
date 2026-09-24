using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Consuming;
using RabbitMQ.ClientKit.Publishing;

namespace RabbitMQ.ClientKit;

/// <summary>
/// Resolves RabbitMQ services and configured endpoints from the current service provider.
/// </summary>
public interface IRabbitMqEndpointResolver
{
    /// <summary>
    /// Gets the client registered for the supplied connection name, or the default client when no name is supplied.
    /// </summary>
    /// <param name="connectionName">The optional connection name.</param>
    RabbitMqClient GetClient(string? connectionName = null);

    /// <summary>
    /// Gets the publisher registered for the supplied connection name, or the default publisher when no name is supplied.
    /// </summary>
    /// <param name="connectionName">The optional connection name.</param>
    RabbitMqPublisher GetPublisher(string? connectionName = null);

    /// <summary>
    /// Gets the consumer registered for the supplied connection name, or the default consumer when no name is supplied.
    /// </summary>
    /// <param name="connectionName">The optional connection name.</param>
    RabbitMqConsumer GetConsumer(string? connectionName = null);

    /// <summary>
    /// Gets a configured producer definition together with its resolved RabbitMQ services.
    /// </summary>
    /// <param name="name">The configured producer name.</param>
    RabbitMqConfiguredProducer GetRequiredProducer(string name);

    /// <summary>
    /// Gets a configured consumer definition together with its resolved RabbitMQ services.
    /// </summary>
    /// <param name="name">The configured consumer name.</param>
    RabbitMqConfiguredConsumer GetRequiredConsumer(string name);
}
