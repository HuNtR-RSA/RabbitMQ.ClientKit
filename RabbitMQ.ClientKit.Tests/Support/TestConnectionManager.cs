using RabbitMQ.Client;
using RabbitMQ.ClientKit.Connection;

namespace RabbitMQ.ClientKit.Tests.Support;

internal sealed class TestConnectionManager(params IChannel[] channels) : IRabbitMqConnectionManager
{
    private readonly Queue<IChannel> _channels = new(channels);

    public int CreateChannelCalls { get; private set; }

    public ValueTask<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("Connection access is not required for these unit tests.");

    public Task<IChannel> CreateChannelAsync(CreateChannelOptions? options = null, CancellationToken cancellationToken = default)
    {
        CreateChannelCalls++;
        return _channels.Count != 0 
            ? Task.FromResult(_channels.Dequeue())
            : throw new InvalidOperationException("No channels were configured for the test connection manager.");
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
