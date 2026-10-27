using RabbitMQ.Client;
using RabbitMQ.ClientKit.Channel;

namespace RabbitMQ.ClientKit.Consuming;

internal sealed class Amqp091ConsumerSession(
    IRabbitMqChannelLease lease,
    IChannel channel,
    string consumerTag,
    CancellationTokenSource cancellationTokenSource) : IRabbitMqConsumerSession
{
    private readonly IRabbitMqChannelLease _lease = lease ?? throw new ArgumentNullException(nameof(lease));
    private readonly IChannel _channel = channel ?? throw new ArgumentNullException(nameof(channel));
    private readonly string _consumerTag = consumerTag ?? throw new ArgumentNullException(nameof(consumerTag));
    private readonly CancellationTokenSource _cancellationTokenSource = cancellationTokenSource ?? throw new ArgumentNullException(nameof(cancellationTokenSource));

    public bool IsHealthy => _channel.IsOpen;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _cancellationTokenSource.CancelAsync();

        try
        {
            if (_channel.IsOpen && !string.IsNullOrWhiteSpace(_consumerTag))
            {
                await _channel.BasicCancelAsync(_consumerTag, false, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _cancellationTokenSource.Dispose();
            await _lease.DisposeAsync().ConfigureAwait(false);
        }
    }
}
