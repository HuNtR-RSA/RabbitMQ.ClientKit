namespace RabbitMQ.ClientKit.Models;

/// <summary>
/// Describes how far a confirmed batch publish progressed.
/// </summary>
public enum RabbitMqPublishBatchStatus
{
    /// <summary>
    /// No publish frame was written. Callers may safely roll back local state.
    /// </summary>
    NotSent,

    /// <summary>
    /// At least one publish was attempted, but not every message was confirmed.
    /// The broker may already have accepted some or all of those messages.
    /// </summary>
    Unconfirmed,

    /// <summary>
    /// Every published message received a broker confirmation.
    /// </summary>
    Confirmed
}
