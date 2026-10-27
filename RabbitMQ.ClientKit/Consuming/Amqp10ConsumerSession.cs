using RabbitMQ.AMQP.Client;

namespace RabbitMQ.ClientKit.Consuming;

internal sealed class Amqp10ConsumerSession(
    IConsumer consumer,
    CancellationTokenSource cancellationTokenSource) : IRabbitMqConsumerSession
{
    private readonly IConsumer _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
    private readonly CancellationTokenSource _cancellationTokenSource = cancellationTokenSource ?? throw new ArgumentNullException(nameof(cancellationTokenSource));

    public bool IsHealthy => _consumer.State is State.Open;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _cancellationTokenSource.CancelAsync();

        try
        {
            if (_consumer.State is State.Open)
            {
                await _consumer.CloseAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _consumer.Dispose();
            _cancellationTokenSource.Dispose();
        }
    }
}
