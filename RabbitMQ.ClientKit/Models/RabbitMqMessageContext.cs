namespace RabbitMQ.ClientKit.Models;

/// <summary>
/// Exposes broker metadata for a received RabbitMQ message.
/// </summary>
public sealed class RabbitMqMessageContext
{
    /// <summary>
    /// Gets the broker delivery tag.
    /// </summary>
    public ulong DeliveryTag { get; init; }

    /// <summary>
    /// Gets a value indicating whether the delivery was previously redelivered.
    /// </summary>
    public bool Redelivered { get; init; }

    /// <summary>
    /// Gets the RabbitMQ consumer tag.
    /// </summary>
    public string ConsumerTag { get; init; } = string.Empty;

    /// <summary>
    /// Gets the exchange the message was published to.
    /// </summary>
    public string Exchange { get; init; } = string.Empty;

    /// <summary>
    /// Gets the routing key the message was published with.
    /// </summary>
    public string RoutingKey { get; init; } = string.Empty;

    /// <summary>
    /// Gets the application identifier.
    /// </summary>
    public string? AppId { get; init; }

    /// <summary>
    /// Gets the content type.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// Gets the correlation identifier.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Gets the message identifier.
    /// </summary>
    public string? MessageId { get; init; }

    /// <summary>
    /// Gets the reply-to address.
    /// </summary>
    public string? ReplyTo { get; init; }

    /// <summary>
    /// Gets the UTC timestamp if one was supplied by the publisher.
    /// </summary>
    public DateTimeOffset? TimestampUtc { get; init; }

    /// <summary>
    /// Gets the AMQP headers.
    /// </summary>
    public IReadOnlyDictionary<string, object?> Headers { get; init; } = new Dictionary<string, object?>();
}
