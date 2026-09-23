namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Defines how a message should be published.
/// </summary>
public sealed class RabbitMqPublishOptions
{
    /// <summary>
    /// Gets the exchange to publish to. Leave empty to use the default exchange.
    /// </summary>
    public string ExchangeName { get; init; } = string.Empty;

    /// <summary>
    /// Gets the explicit routing key to use for publishing.
    /// </summary>
    public string? RoutingKey { get; init; }

    /// <summary>
    /// Gets the queue name to use as the routing key when one is not supplied.
    /// </summary>
    public string? QueueName { get; init; }

    /// <summary>
    /// Gets a value indicating whether unroutable messages should be returned by the broker.
    /// </summary>
    public bool Mandatory { get; init; }

    /// <summary>
    /// Gets the message properties applied to the outgoing publication.
    /// </summary>
    public RabbitMqMessageProperties? Properties { get; init; }

    /// <summary>
    /// Gets the topology that should be declared before publishing.
    /// </summary>
    public RabbitMqTopologyOptions? Topology { get; init; }

    internal string ResolveRoutingKey()
    {
        if (!string.IsNullOrWhiteSpace(RoutingKey))
        {
            return RoutingKey;
        }

        if (!string.IsNullOrWhiteSpace(QueueName))
        {
            return QueueName;
        }

        return string.Empty;
    }

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(ExchangeName) && string.IsNullOrWhiteSpace(RoutingKey) && string.IsNullOrWhiteSpace(QueueName))
        {
            throw new InvalidOperationException("Publishing requires an exchange, a routing key, or a queue name.");
        }
    }
}
