using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit.Consuming;

/// <summary>
/// Settles a delivery and optionally publishes a replacement on the same consumer channel.
/// Do not close or dispose the channel from a handler; Client 7 can deadlock on that.
/// </summary>
public interface IRabbitMqDeliveryHandle
{
    /// <summary>
    /// Gets a value indicating whether this delivery has already been settled.
    /// </summary>
    bool IsSettled { get; }

    /// <summary>
    /// Publishes a typed replacement on the consumer channel and waits for a publisher confirm
    /// when the channel was created with confirmation tracking.
    /// </summary>
    Task PublishReplacementAsync<T>(T message, RabbitMqPublishOptions options, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a raw replacement body on the consumer channel.
    /// </summary>
    Task PublishReplacementAsync(ReadOnlyMemory<byte> body, RabbitMqPublishOptions options, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledges the original delivery.
    /// </summary>
    Task AckAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Rejects the original delivery without requeueing it.
    /// </summary>
    Task RejectAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Negatively acknowledges the original delivery.
    /// </summary>
    Task NackAsync(bool requeue = true, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a typed replacement, waits for the confirm, and only then acknowledges the original.
    /// If the confirm never arrives the original is nacked with requeue. If the confirm landed and
    /// the ack then fails, the original is not nacked, so a second copy is not created on purpose.
    /// A crash between confirm and ack can still redeliver the original while the replacement is queued;
    /// handlers must stay idempotent.
    /// </summary>
    Task ReplaceAndAckAsync<T>(T message, RabbitMqPublishOptions options, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a raw replacement, waits for the confirm, and only then acknowledges the original.
    /// </summary>
    Task ReplaceAndAckAsync(ReadOnlyMemory<byte> body, RabbitMqPublishOptions options, CancellationToken cancellationToken = default);
}
