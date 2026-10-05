using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Consuming;

namespace RabbitMQ.ClientKit.Tests;

public sealed class RabbitMqConsumerSubscriptionTests
{
    [Fact]
    public async Task StopAsync_CancelsConsumerAndDisposesLease()
    {
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var cancellationTokenSource = new CancellationTokenSource();
        var token = cancellationTokenSource.Token;

        channel.IsOpen.Returns(true);
        channel.BasicCancelAsync(default!, default, default).ReturnsForAnyArgs(Task.CompletedTask);

        var subscription = new RabbitMqConsumerSubscription(lease, channel, "consumer-tag", cancellationTokenSource);

        await subscription.StopAsync();

        Assert.True(token.IsCancellationRequested);
        await channel.Received(1).BasicCancelAsync("consumer-tag", false, Arg.Any<CancellationToken>());
        await lease.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task StopAsync_IsIdempotent()
    {
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var subscription = new RabbitMqConsumerSubscription(lease, channel, "consumer-tag", new CancellationTokenSource());

        channel.IsOpen.Returns(true);
        channel.BasicCancelAsync(default!, default, default).ReturnsForAnyArgs(Task.CompletedTask);

        await subscription.StopAsync();
        await subscription.StopAsync();

        await channel.Received(1).BasicCancelAsync("consumer-tag", false, Arg.Any<CancellationToken>());
        await lease.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task IsHealthy_ReflectsChannelStateAndDisposal()
    {
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        channel.IsOpen.Returns(true);

        var subscription = new RabbitMqConsumerSubscription(lease, channel, "consumer-tag", new CancellationTokenSource());

        Assert.True(subscription.IsHealthy);

        channel.IsOpen.Returns(false);
        Assert.False(subscription.IsHealthy);

        channel.IsOpen.Returns(true);
        Assert.True(subscription.IsHealthy);

        await subscription.StopAsync();
        Assert.False(subscription.IsHealthy);
    }
}
