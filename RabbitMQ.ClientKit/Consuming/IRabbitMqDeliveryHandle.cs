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
    /// Publishes a typed replacement, waits for the confirmation, and only then acknowledges the original.
    /// If the confirmation never arrives the original is nacked with requeue. If the confirmation landed and
    /// the ack then fails, the original is not nacked, so a second copy is not created on purpose.
    /// A crash between confirm and ack can still redeliver the original while the replacement is queued;
    /// handlers must stay idempotent.
    /// </summary>
    Task ReplaceAndAckAsync<T>(T message, RabbitMqPublishOptions options, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a raw replacement, waits for the confirmation, and only then acknowledges the original.
    /// </summary>
    Task ReplaceAndAckAsync(ReadOnlyMemory<byte> body, RabbitMqPublishOptions options, CancellationToken cancellationToken = default);

    /// <summary>
    /// Settles the delivery with AMQP 1.0 <c>modified</c> and holds the same message until
    /// <c>UtcNow + delay</c> via the <c>x-opt-delivery-time</c> annotation.
    /// Requires an AMQP 1.0 consumer on a quorum queue (RabbitMQ 4.3+). Classic queues cannot
    /// apply a modified annotation. The body is not republished.
    /// When <paramref name="deliveryFailed"/> is <see langword="true"/>, the quorum delivery count is incremented.
    /// </summary>
    /// <param name="delay">How long the broker should wait before redelivering.</param>
    /// <param name="deliveryFailed">
    /// When <see langword="true"/>, RabbitMQ treats the settlement as a failed delivery and increments the delivery count.
    /// </param>
    /// <param name="cancellationToken">The cancellation token observed before sending the outcome.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this handle belongs to an AMQP 0-9-1 delivery.
    /// </exception>
    Task DelayRetryAsync(TimeSpan delay, bool deliveryFailed, CancellationToken cancellationToken = default);

    /// <summary>
    /// Settles the delivery with AMQP 1.0 <c>modified</c> and combines the supplied annotations
    /// with the existing message annotations. Callers who pass <c>x-opt-delivery-time</c> must supply
    /// an absolute UTC Unix timestamp in milliseconds as a <see cref="long"/>.
    /// Requires an AMQP 1.0 consumer. Classic queues cannot apply a modified annotation.
    /// When <paramref name="deliveryFailed"/> is <see langword="true"/>, the quorum delivery count is incremented.
    /// </summary>
    /// <param name="annotations">Message annotations to combine with the existing ones. Keys the broker understands start with <c>x-</c>.</param>
    /// <param name="deliveryFailed">
    /// When <see langword="true"/>, RabbitMQ treats the settlement as a failed delivery and increments the delivery count.
    /// </param>
    /// <param name="cancellationToken">The cancellation token observed before sending the outcome.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this handle belongs to an AMQP 0-9-1 delivery.
    /// </exception>
    Task RequeueAsync(IReadOnlyDictionary<string, object> annotations, bool deliveryFailed, CancellationToken cancellationToken = default);
}
