using RabbitMQ.ClientKit.Consuming;

namespace RabbitMQ.ClientKit.Models;

/// <summary>
/// Represents a deserialized message body together with its broker metadata.
/// </summary>
/// <typeparam name="T">The payload type.</typeparam>
public sealed class RabbitMqReceivedMessage<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqReceivedMessage{T}" /> class.
    /// </summary>
    /// <param name="payload">The deserialized payload.</param>
    /// <param name="context">The broker metadata for the delivery.</param>
    /// <param name="body">The raw message body.</param>
    /// <param name="delivery">The delivery handle for manual acknowledgement or replace-then-ack operations.</param>
    public RabbitMqReceivedMessage(T payload, RabbitMqMessageContext context, ReadOnlyMemory<byte> body, IRabbitMqDeliveryHandle? delivery = null)
    {
        Payload = payload;
        Context = context;
        Body = body;
        Delivery = delivery;
    }

    /// <summary>
    /// Gets the deserialized payload.
    /// </summary>
    public T Payload { get; }

    /// <summary>
    /// Gets the broker metadata for the delivery.
    /// </summary>
    public RabbitMqMessageContext Context { get; }

    /// <summary>
    /// Gets the raw message body.
    /// </summary>
    public ReadOnlyMemory<byte> Body { get; }

    /// <summary>
    /// Gets the delivery handle for manual acknowledgement or replace-then-ack operations.
    /// </summary>
    public IRabbitMqDeliveryHandle? Delivery { get; }
}
