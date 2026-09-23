using RabbitMQ.Client;

namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Describes an exchange that should exist before publishing or consuming.
/// </summary>
public sealed class RabbitMqExchangeOptions
{
    /// <summary>
    /// Gets the exchange name.
    /// </summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// Gets the exchange type.
    /// </summary>
    public string Type { get; init; } = ExchangeType.Direct;

    /// <summary>
    /// Gets a value indicating whether the exchange is durable.
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    /// Gets a value indicating whether the exchange is automatically deleted when unused.
    /// </summary>
    public bool AutoDelete { get; init; }

    /// <summary>
    /// Gets the optional exchange declaration arguments.
    /// </summary>
    public IDictionary<string, object?>? Arguments { get; init; }
}
