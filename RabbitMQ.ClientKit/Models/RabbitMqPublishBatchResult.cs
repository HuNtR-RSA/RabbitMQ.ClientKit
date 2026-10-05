namespace RabbitMQ.ClientKit.Models;

/// <summary>
/// Reports the outcome of a batch publish so callers can tell a pre-send failure
/// from an ambiguous confirm timeout.
/// </summary>
public sealed class RabbitMqPublishBatchResult
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqPublishBatchResult" /> class.
    /// </summary>
    public RabbitMqPublishBatchResult(RabbitMqPublishBatchStatus status, int attemptedCount, int confirmedCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(attemptedCount);

        if (confirmedCount < 0 || confirmedCount > attemptedCount)
        {
            throw new ArgumentOutOfRangeException(nameof(confirmedCount));
        }

        Status = status;
        AttemptedCount = attemptedCount;
        ConfirmedCount = confirmedCount;
    }

    /// <summary>
    /// Gets how far the batch progressed.
    /// </summary>
    public RabbitMqPublishBatchStatus Status { get; }

    /// <summary>
    /// Gets the number of messages the publisher attempted to send.
    /// </summary>
    public int AttemptedCount { get; }

    /// <summary>
    /// Gets the number of messages that received a broker confirmation.
    /// </summary>
    public int ConfirmedCount { get; }

    /// <summary>
    /// Gets a result for an empty batch.
    /// </summary>
    public static RabbitMqPublishBatchResult Empty { get; } = new(RabbitMqPublishBatchStatus.Confirmed, 0, 0);

    /// <summary>
    /// Gets a result for a failure before any publish frame was written.
    /// </summary>
    public static RabbitMqPublishBatchResult NotSent { get; } = new(RabbitMqPublishBatchStatus.NotSent, 0, 0);

    /// <summary>
    /// Creates a confirmed result for the supplied count.
    /// </summary>
    public static RabbitMqPublishBatchResult Confirmed(int count)
        => new(RabbitMqPublishBatchStatus.Confirmed, count, count);

    /// <summary>
    /// Creates an unconfirmed result after one or more publish attempts.
    /// </summary>
    public static RabbitMqPublishBatchResult Unconfirmed(int attemptedCount, int confirmedCount)
        => new(RabbitMqPublishBatchStatus.Unconfirmed, attemptedCount, confirmedCount);
}
