namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Defines how a consumer subscription should be created.
/// </summary>
public sealed class RabbitMqConsumerOptions
{
    /// <summary>
    /// Gets the queue name to consume from.
    /// </summary>
    public string QueueName { get; init; } = string.Empty;

    /// <summary>
    /// Gets the logical consumer name used by reusable channel providers.
    /// </summary>
    public string? ConsumerName { get; init; }

    /// <summary>
    /// Gets the explicit RabbitMQ consumer tag.
    /// </summary>
    public string ConsumerTag { get; init; } = string.Empty;

    /// <summary>
    /// Gets a value indicating whether deliveries are auto-acknowledged by RabbitMQ.
    /// </summary>
    public bool AutoAck { get; init; }

    /// <summary>
    /// Gets the prefetch count applied to the channel before consuming.
    /// </summary>
    public ushort PrefetchCount { get; init; } = 1;

    /// <summary>
    /// Gets a value indicating whether the prefetch setting is global to the channel.
    /// </summary>
    public bool GlobalPrefetch { get; init; }

    /// <summary>
    /// Gets a value indicating whether the consumer is exclusive.
    /// </summary>
    public bool Exclusive { get; init; }

    /// <summary>
    /// Gets a value indicating whether the consumer should ignore messages published by the same connection.
    /// </summary>
    public bool NoLocal { get; init; }

    /// <summary>
    /// Gets a value indicating whether failed message handling should requeue the delivery.
    /// </summary>
    public bool RequeueOnFailure { get; init; } = true;

    /// <summary>
    /// Gets the optional consumer arguments passed to RabbitMQ.
    /// </summary>
    public IDictionary<string, object?>? Arguments { get; init; }

    /// <summary>
    /// Gets the topology that should be declared before consuming.
    /// </summary>
    public RabbitMqTopologyOptions? Topology { get; init; }

    internal string ResolveConsumerName() => string.IsNullOrWhiteSpace(ConsumerName) ? QueueName : ConsumerName;

    internal void Validate() => ArgumentException.ThrowIfNullOrWhiteSpace(QueueName);
}
