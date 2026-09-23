namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Aggregates the exchange, queue, and bindings that should be declared before use.
/// </summary>
public sealed class RabbitMqTopologyOptions
{
    /// <summary>
    /// Gets the exchange declaration options.
    /// </summary>
    public RabbitMqExchangeOptions? Exchange { get; init; }

    /// <summary>
    /// Gets the queue declaration options.
    /// </summary>
    public RabbitMqQueueOptions? Queue { get; init; }

    /// <summary>
    /// Gets the queue bindings to declare.
    /// </summary>
    public IReadOnlyCollection<RabbitMqQueueBindingOptions> Bindings { get; init; } = Array.Empty<RabbitMqQueueBindingOptions>();
}
