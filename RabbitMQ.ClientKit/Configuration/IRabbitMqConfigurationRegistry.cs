namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Exposes configured producer and consumer definitions loaded into the service container.
/// </summary>
public interface IRabbitMqConfigurationRegistry
{
    /// <summary>
    /// Gets the configured producer definitions.
    /// </summary>
    IReadOnlyCollection<RabbitMqProducerRegistration> Producers { get; }

    /// <summary>
    /// Gets the configured consumer definitions.
    /// </summary>
    IReadOnlyCollection<RabbitMqConsumerRegistration> Consumers { get; }

    /// <summary>
    /// Returns the configured producer definition for the supplied name.
    /// </summary>
    /// <param name="name">The producer name.</param>
    RabbitMqProducerRegistration GetRequiredProducer(string name);

    /// <summary>
    /// Returns the configured consumer definition for the supplied name.
    /// </summary>
    /// <param name="name">The consumer name.</param>
    RabbitMqConsumerRegistration GetRequiredConsumer(string name);

    /// <summary>
    /// Attempts to resolve a configured producer definition.
    /// </summary>
    /// <param name="name">The producer name.</param>
    /// <param name="producer">The resolved producer definition when found.</param>
    /// <returns><see langword="true" /> when a matching producer exists; otherwise <see langword="false" />.</returns>
    bool TryGetProducer(string name, out RabbitMqProducerRegistration? producer);

    /// <summary>
    /// Attempts to resolve a configured consumer definition.
    /// </summary>
    /// <param name="name">The consumer name.</param>
    /// <param name="consumer">The resolved consumer definition when found.</param>
    /// <returns><see langword="true" /> when a matching consumer exists; otherwise <see langword="false" />.</returns>
    bool TryGetConsumer(string name, out RabbitMqConsumerRegistration? consumer);
}
