using RabbitMQ.Client;

namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Represents publish-time AMQP message properties exposed by the wrapper.
/// </summary>
public sealed class RabbitMqMessageProperties
{
    /// <summary>
    /// Gets the application identifier.
    /// </summary>
    public string? AppId { get; init; }

    /// <summary>
    /// Gets the content encoding.
    /// </summary>
    public string? ContentEncoding { get; init; }

    /// <summary>
    /// Gets the content type.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// Gets the correlation identifier.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Gets the message expiration string.
    /// </summary>
    public string? Expiration { get; init; }

    /// <summary>
    /// Gets the AMQP headers.
    /// </summary>
    public IDictionary<string, object?>? Headers { get; init; }

    /// <summary>
    /// Gets the message identifier.
    /// </summary>
    public string? MessageId { get; init; }

    /// <summary>
    /// Gets a value indicating whether the message should be persisted.
    /// </summary>
    public bool Persistent { get; init; } = true;

    /// <summary>
    /// Gets the optional priority.
    /// </summary>
    public byte? Priority { get; init; }

    /// <summary>
    /// Gets the reply-to address.
    /// </summary>
    public string? ReplyTo { get; init; }

    /// <summary>
    /// Gets the optional UTC timestamp for the message.
    /// </summary>
    public DateTimeOffset? TimestampUtc { get; init; }

    /// <summary>
    /// Gets the application-specific message type.
    /// </summary>
    public string? Type { get; init; }

    /// <summary>
    /// Gets the validated user identifier.
    /// </summary>
    public string? UserId { get; init; }

    internal BasicProperties ToBasicProperties()
    {
        var properties = new BasicProperties
        {
            AppId = AppId,
            ContentEncoding = ContentEncoding,
            ContentType = ContentType,
            CorrelationId = CorrelationId,
            Expiration = Expiration,
            Headers = Headers is null ? null : new Dictionary<string, object?>(Headers),
            MessageId = MessageId,
            Persistent = Persistent,
            ReplyTo = ReplyTo,
            Type = Type,
            UserId = UserId
        };

        if (Priority.HasValue)
        {
            properties.Priority = Priority.Value;
        }

        if (TimestampUtc.HasValue)
        {
            properties.Timestamp = new AmqpTimestamp(TimestampUtc.Value.ToUnixTimeSeconds());
        }

        return properties;
    }
}
