namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Represents a named consumer configuration loaded from application settings.
/// </summary>
public sealed class RabbitMqConsumerRegistration
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqConsumerRegistration" /> class.
    /// </summary>
    /// <param name="name">The consumer name.</param>
    /// <param name="connectionName">The optional named connection that should back the consumer.</param>
    /// <param name="options">The consume options bound for the consumer.</param>
    public RabbitMqConsumerRegistration(string name, string? connectionName, RabbitMqConsumerOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        Name = name;
        ConnectionName = string.IsNullOrWhiteSpace(connectionName) ? null : connectionName;
        Options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Gets the consumer name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the optional named connection associated with the consumer.
    /// </summary>
    public string? ConnectionName { get; }

    /// <summary>
    /// Gets the consume options bound for the consumer.
    /// </summary>
    public RabbitMqConsumerOptions Options { get; }
}
