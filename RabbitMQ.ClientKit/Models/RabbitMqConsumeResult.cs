namespace RabbitMQ.ClientKit.Models;

/// <summary>
/// Represents the acknowledgement decision returned by a consumer handler.
/// </summary>
/// <param name="Disposition">The acknowledgement action to apply.</param>
public sealed record RabbitMqConsumeResult(RabbitMqConsumeDisposition Disposition)
{
    /// <summary>
    /// Gets a consume result that acknowledges the delivery.
    /// </summary>
    public static RabbitMqConsumeResult Ack { get; } = new(RabbitMqConsumeDisposition.Ack);

    /// <summary>
    /// Gets a consume result that rejects the delivery without requeueing it.
    /// </summary>
    public static RabbitMqConsumeResult Reject { get; } = new(RabbitMqConsumeDisposition.Reject);

    /// <summary>
    /// Gets a consume result that requeues the delivery.
    /// </summary>
    public static RabbitMqConsumeResult Requeue { get; } = new(RabbitMqConsumeDisposition.Requeue);
}
