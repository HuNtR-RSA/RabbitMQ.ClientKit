namespace RabbitMQ.ClientKit.Models;

/// <summary>
/// Defines how a processed delivery should be acknowledged back to RabbitMQ.
/// </summary>
public enum RabbitMqConsumeDisposition
{
    /// <summary>
    /// Acknowledge the message as successfully processed.
    /// </summary>
    Ack,

    /// <summary>
    /// Reject the message without requeueing it.
    /// </summary>
    Reject,

    /// <summary>
    /// Negatively acknowledge the message and requeue it.
    /// </summary>
    Requeue
}
