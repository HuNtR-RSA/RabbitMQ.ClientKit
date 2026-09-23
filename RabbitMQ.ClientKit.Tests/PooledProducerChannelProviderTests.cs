using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.ClientKit.ChannelPooling;
using RabbitMQ.ClientKit.Tests.Support;

namespace RabbitMQ.ClientKit.Tests;

public sealed class PooledProducerChannelProviderTests
{
    [Fact]
    public void Constructor_RejectsNonPositivePoolSize()
    {
        var manager = new TestConnectionManager(CreateOpenChannel());

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new PooledProducerChannelProvider(manager, new RabbitMqChannelPoolingOptions { ProducerPoolSize = 0 }));
    }

    [Fact]
    public async Task RentAsync_ReusesReturnedChannel()
    {
        var channel = CreateOpenChannel();
        var manager = new TestConnectionManager(channel);
        var provider = new PooledProducerChannelProvider(manager, new RabbitMqChannelPoolingOptions { ProducerPoolSize = 1 });

        IChannel firstChannel;
        await using (var lease = await provider.RentAsync())
        {
            firstChannel = lease.Channel;
        }

        await using var secondLease = await provider.RentAsync();

        Assert.Same(firstChannel, secondLease.Channel);
        Assert.Equal(1, manager.CreateChannelCalls);
    }

    private static IChannel CreateOpenChannel()
    {
        var channel = Substitute.For<IChannel>();
        channel.IsOpen.Returns(true);
        channel.DisposeAsync().Returns(ValueTask.CompletedTask);
        return channel;
    }
}
