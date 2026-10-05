using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.ClientKit.ChannelPooling;
using RabbitMQ.ClientKit.Tests.Support;

namespace RabbitMQ.ClientKit.Tests;

public sealed class ReusableConsumerChannelProviderTests
{
    [Fact]
    public async Task RentAsync_ThrowsWhenConsumerChannelAlreadyLeased()
    {
        var manager = new TestConnectionManager(CreateOpenChannel());
        var provider = new ReusableConsumerChannelProvider(manager);

        await using var firstLease = await provider.RentAsync("orders-consumer");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => provider.RentAsync("orders-consumer").AsTask());

        Assert.Contains("already in use", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RentAsync_ReusesReturnedChannelForSameConsumer()
    {
        var channel = CreateOpenChannel();
        var manager = new TestConnectionManager(channel);
        var provider = new ReusableConsumerChannelProvider(manager);

        await using (var firstLease = await provider.RentAsync("orders-consumer"))
        {
            Assert.Same(channel, firstLease.Channel);
        }

        await using var secondLease = await provider.RentAsync("orders-consumer");

        Assert.Same(channel, secondLease.Channel);
        Assert.Equal(1, manager.CreateChannelCalls);
    }

    [Fact]
    public async Task RentAsync_ReplacesClosedChannelBeforeReusingConsumer()
    {
        var firstChannel = CreateOpenChannel();
        var secondChannel = CreateOpenChannel();
        var manager = new TestConnectionManager(firstChannel, secondChannel);
        var provider = new ReusableConsumerChannelProvider(manager);

        await using (var firstLease = await provider.RentAsync("orders-consumer"))
        {
            Assert.Same(firstChannel, firstLease.Channel);
        }

        firstChannel.IsOpen.Returns(false);
        await using var secondLease = await provider.RentAsync("orders-consumer");

        Assert.Same(secondChannel, secondLease.Channel);
        Assert.Equal(2, manager.CreateChannelCalls);
    }

    [Fact]
    public async Task RentAsync_ReplacesChannelAfterCallbackException()
    {
        var firstChannel = CreateOpenChannel();
        var secondChannel = CreateOpenChannel();
        var manager = new TestConnectionManager(firstChannel, secondChannel);
        var provider = new ReusableConsumerChannelProvider(manager);

        await using (var firstLease = await provider.RentAsync("orders-consumer"))
        {
            Assert.Same(firstChannel, firstLease.Channel);
        }

        firstChannel.CallbackExceptionAsync += Raise.Event<AsyncEventHandler<CallbackExceptionEventArgs>>
        (
            firstChannel,
            new CallbackExceptionEventArgs(new Dictionary<string, object>(), new Exception("test callback error"))
        );

        await using var secondLease = await provider.RentAsync("orders-consumer");

        Assert.Same(secondChannel, secondLease.Channel);
        Assert.Equal(2, manager.CreateChannelCalls);
    }

    private static IChannel CreateOpenChannel()
    {
        var channel = Substitute.For<IChannel>();
        channel.IsOpen.Returns(true);
        channel.DisposeAsync().Returns(ValueTask.CompletedTask);
        return channel;
    }
}
