namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Describes a queue binding that should exist before publishing or consuming.
/// </summary>
public sealed class RabbitMqQueueBindingOptions
{
    /// <summary>
    /// Gets the queue name to bind.
    /// </summary>
    public string QueueName { get; init; } = string.Empty;

    /// <summary>
    /// Gets the exchange name to bind from.
    /// </summary>
    public string ExchangeName { get; init; } = string.Empty;

    /// <summary>
    /// Gets the routing key for the binding.
    /// </summary>
    public string RoutingKey { get; init; } = string.Empty;

    /// <summary>
    /// Gets the optional binding arguments.
    /// </summary>
    public IDictionary<string, object?>? Arguments { get; init; }
}
