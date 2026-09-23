namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Describes a queue that should exist before publishing or consuming.
/// </summary>
public sealed class RabbitMqQueueOptions
{
    /// <summary>
    /// Gets the queue name.
    /// </summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// Gets a value indicating whether the queue is durable.
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    /// Gets a value indicating whether the queue is exclusive to the creating connection.
    /// </summary>
    public bool Exclusive { get; init; }

    /// <summary>
    /// Gets a value indicating whether the queue is automatically deleted when unused.
    /// </summary>
    public bool AutoDelete { get; init; }

    /// <summary>
    /// Gets the optional queue declaration arguments.
    /// </summary>
    public IDictionary<string, object?>? Arguments { get; init; }
}
