using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit.DynamicConfiguration;

/// <summary>
/// Represents a fully materialized RabbitMQ configuration snapshot that can be refreshed at runtime.
/// </summary>
public sealed class RabbitMqDynamicConfigurationSnapshot
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqDynamicConfigurationSnapshot" /> class.
    /// </summary>
    /// <param name="defaultConnection">The optional default connection.</param>
    /// <param name="namedConnections">The named connections keyed by name.</param>
    /// <param name="producers">The named producer definitions keyed by name.</param>
    /// <param name="consumers">The named consumer definitions keyed by name.</param>
    public RabbitMqDynamicConfigurationSnapshot(
        RabbitMqConnectionOptions? defaultConnection,
        IReadOnlyDictionary<string, RabbitMqConnectionOptions>? namedConnections = null,
        IReadOnlyDictionary<string, RabbitMqProducerRegistration>? producers = null,
        IReadOnlyDictionary<string, RabbitMqConsumerRegistration>? consumers = null)
    {
        DefaultConnection = defaultConnection;
        NamedConnections = namedConnections ?? new Dictionary<string, RabbitMqConnectionOptions>(StringComparer.OrdinalIgnoreCase);
        Producers = producers ?? new Dictionary<string, RabbitMqProducerRegistration>(StringComparer.OrdinalIgnoreCase);
        Consumers = consumers ?? new Dictionary<string, RabbitMqConsumerRegistration>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Gets the optional default connection.
    /// </summary>
    public RabbitMqConnectionOptions? DefaultConnection { get; }

    /// <summary>
    /// Gets the named connections keyed by name.
    /// </summary>
    public IReadOnlyDictionary<string, RabbitMqConnectionOptions> NamedConnections { get; }

    /// <summary>
    /// Gets the producer registrations keyed by producer name.
    /// </summary>
    public IReadOnlyDictionary<string, RabbitMqProducerRegistration> Producers { get; }

    /// <summary>
    /// Gets the consumer registrations keyed by consumer name.
    /// </summary>
    public IReadOnlyDictionary<string, RabbitMqConsumerRegistration> Consumers { get; }
}
