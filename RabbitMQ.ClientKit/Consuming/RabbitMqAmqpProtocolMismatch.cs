namespace RabbitMQ.ClientKit.Consuming;

internal static class RabbitMqAmqpProtocolMismatch
{
    internal const string DelayRetryOnAmqp091 =
        "DelayRetryAsync is an AMQP 1.0 settlement and cannot be used on an AMQP 0-9-1 delivery. Subscribe with RabbitMqConsumerProtocol.Amqp10 so the broker can apply x-opt-delivery-time.";

    internal const string RequeueOnAmqp091 =
        "RequeueAsync with annotations is an AMQP 1.0 settlement and cannot be used on an AMQP 0-9-1 delivery. Subscribe with RabbitMqConsumerProtocol.Amqp10 so the broker can apply a modified outcome.";

    internal const string ReplacementOnAmqp10 =
        "Replacement publishing is an AMQP 0-9-1 operation and cannot be used on an AMQP 1.0 delivery. Use DelayRetryAsync or RequeueAsync to settle the same message in place.";
}
