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
        Assert.False(manager.CreateChannelOptionsHistory[0]!.PublisherConfirmationsEnabled);
    }

    [Fact]
    public async Task RentAsync_DoesNotReuseChannelAcrossDifferentConfirmationModes()
    {
        var nonConfirmChannel = CreateOpenChannel();
        var confirmChannel = CreateOpenChannel();
        var manager = new TestConnectionManager(nonConfirmChannel, confirmChannel);
        var provider = new PooledProducerChannelProvider(manager, new RabbitMqChannelPoolingOptions { ProducerPoolSize = 2 });

        await using (var firstLease = await provider.RentAsync(publisherConfirmationsEnabled: false))
        {
            Assert.Same(nonConfirmChannel, firstLease.Channel);
        }

        await using var secondLease = await provider.RentAsync(publisherConfirmationsEnabled: true);

        Assert.Same(confirmChannel, secondLease.Channel);
        Assert.Equal(2, manager.CreateChannelCalls);
        Assert.False(manager.CreateChannelOptionsHistory[0]!.PublisherConfirmationsEnabled);
        Assert.True(manager.CreateChannelOptionsHistory[1]!.PublisherConfirmationsEnabled);
    }

    [Fact]
    public async Task RentAsync_ReclaimsIdleChannelFromDifferentConfirmationPoolWhenPoolIsFull()
    {
        var firstChannel = CreateOpenChannel();
        var secondChannel = CreateOpenChannel();
        var manager = new TestConnectionManager(firstChannel, secondChannel);
        var provider = new PooledProducerChannelProvider(manager, new RabbitMqChannelPoolingOptions { ProducerPoolSize = 1 });

        await using (var firstLease = await provider.RentAsync(publisherConfirmationsEnabled: false))
        {
            Assert.Same(firstChannel, firstLease.Channel);
        }

        await using var secondLease = await provider.RentAsync(publisherConfirmationsEnabled: true);

        Assert.Same(secondChannel, secondLease.Channel);
        Assert.Equal(2, manager.CreateChannelCalls);
        await firstChannel.Received(1).DisposeAsync();
    }

    private static IChannel CreateOpenChannel()
    {
        var channel = Substitute.For<IChannel>();
        channel.IsOpen.Returns(true);
        channel.DisposeAsync().Returns(ValueTask.CompletedTask);
        return channel;
    }
}
