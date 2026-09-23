using RabbitMQ.Client;

namespace RabbitMQ.ClientKit.Channel;

internal sealed class AsyncDisposableChannelLease(IChannel channel) : IRabbitMqChannelLease
{
    public IChannel Channel { get; } = channel;

    public ValueTask DisposeAsync() => Channel.DisposeAsync();
}
