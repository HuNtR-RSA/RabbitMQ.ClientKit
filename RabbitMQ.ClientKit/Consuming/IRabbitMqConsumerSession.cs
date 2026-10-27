namespace RabbitMQ.ClientKit.Consuming;

internal interface IRabbitMqConsumerSession
{
    bool IsHealthy { get; }

    Task StopAsync(CancellationToken cancellationToken);
}
