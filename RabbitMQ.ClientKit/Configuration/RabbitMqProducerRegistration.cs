namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Represents a named producer configuration loaded from application settings.
/// </summary>
public sealed class RabbitMqProducerRegistration
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqProducerRegistration" /> class.
    /// </summary>
    /// <param name="name">The producer name.</param>
    /// <param name="connectionName">The optional named connection that should publish this producer's messages.</param>
    /// <param name="options">The publish options bound for the producer.</param>
    public RabbitMqProducerRegistration(string name, string? connectionName, RabbitMqPublishOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        Name = name;
        ConnectionName = string.IsNullOrWhiteSpace(connectionName) ? null : connectionName;
        Options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Gets the producer name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the optional named connection associated with the producer.
    /// </summary>
    public string? ConnectionName { get; }

    /// <summary>
    /// Gets the publish options bound for the producer.
    /// </summary>
    public RabbitMqPublishOptions Options { get; }
}
