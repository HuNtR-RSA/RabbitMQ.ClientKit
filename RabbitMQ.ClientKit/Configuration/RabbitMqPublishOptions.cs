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

    /// <summary>
    /// Gets a value indicating whether the leased producer channel should track publisher confirmations.
    /// Client 7 enables this at channel creation; there is no later ConfirmSelect.
    /// </summary>
    public bool PublisherConfirms { get; init; }

    /// <summary>
    /// Gets the timeout used when <see cref="PublisherConfirms" /> is enabled.
    /// Defaults to 30 seconds.
    /// </summary>
    public TimeSpan ConfirmTimeout { get; init; } = TimeSpan.FromSeconds(30);

    internal string ResolveRoutingKey()
    {
        if (!string.IsNullOrWhiteSpace(RoutingKey))
        {
            return RoutingKey;
        }

        return !string.IsNullOrWhiteSpace(QueueName)
            ? QueueName
            : string.Empty;
    }

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(ExchangeName) && string.IsNullOrWhiteSpace(RoutingKey) && string.IsNullOrWhiteSpace(QueueName))
        {
            throw new InvalidOperationException("Publishing requires an exchange, a routing key, or a queue name.");
        }
    }
}
