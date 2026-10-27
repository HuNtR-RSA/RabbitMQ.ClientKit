namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Identifies the AMQP protocol used to consume from a queue.
/// One TCP connection speaks one protocol, so AMQP 1.0 subscriptions use a dedicated connection.
/// </summary>
public enum RabbitMqConsumerProtocol
{
    /// <summary>
    /// AMQP 0-9-1 via RabbitMQ.Client. This is the default and supports ack, nack, reject, and replace-then-ack.
    /// It cannot apply the AMQP 1.0 modified outcome used by delayed retry.
    /// </summary>
    Amqp091 = 0,

    /// <summary>
    /// AMQP 1.0 via RabbitMQ.AMQP.Client. Required for <c>DelayRetryAsync</c> / <c>x-opt-delivery-time</c>
    /// on quorum queues (RabbitMQ 4.3+). Classic queues cannot apply a modified annotation.
    /// </summary>
    Amqp10 = 1
}
