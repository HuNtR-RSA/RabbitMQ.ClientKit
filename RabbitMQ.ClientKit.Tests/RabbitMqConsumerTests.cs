using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Consuming;
using RabbitMQ.ClientKit.Models;
using RabbitMQ.ClientKit.Serialization;
using RabbitMQ.ClientKit.Tests.Support;
using RabbitMQ.ClientKit.Topology;

namespace RabbitMQ.ClientKit.Tests;

public sealed class RabbitMqConsumerTests
{
    [Fact]
    public async Task SubscribeAsync_HandlerFirstOverload_CreatesSubscription()
    {
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var provider = Substitute.For<IRabbitMqConsumerChannelProvider>();
        var serializer = Substitute.For<IRabbitMqSerializer>();

        lease.Channel.Returns(channel);
        provider.RentAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<IRabbitMqChannelLease>(lease));

        channel.BasicConsumeAsync(
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<bool>(),
                Arg.Any<IDictionary<string, object?>>(),
                Arg.Any<IAsyncBasicConsumer>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult("consumer-tag"));

        var consumer = new RabbitMqConsumer(provider, serializer);

        await using var subscription = await consumer.SubscribeAsync<TestMessage>(
            (_, _) => Task.FromResult(RabbitMqConsumeResult.Ack),
            new RabbitMqConsumerOptions
            {
                QueueName = "orders.handler-first"
            });

        Assert.NotNull(subscription);
        await channel.Received(1).BasicConsumeAsync(
            Arg.Is("orders.handler-first"),
            Arg.Any<bool>(),
            Arg.Any<string>(),
            Arg.Any<bool>(),
            Arg.Any<bool>(),
            Arg.Any<IDictionary<string, object?>>(),
            Arg.Any<IAsyncBasicConsumer>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_AutoAckDelivery_DoesNotExposeDeliveryHandle()
    {
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var provider = Substitute.For<IRabbitMqConsumerChannelProvider>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        var receivedSource = new TaskCompletionSource<RabbitMqReceivedMessage<TestMessage>>(TaskCreationOptions.RunContinuationsAsynchronously);
        IAsyncBasicConsumer? basicConsumer = null;

        lease.Channel.Returns(channel);
        provider.RentAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<IRabbitMqChannelLease>(lease));
        serializer.Deserialize<TestMessage>(Arg.Any<ReadOnlyMemory<byte>>())
            .Returns(new TestMessage { Value = "auto-ack" });

        channel.BasicConsumeAsync(
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<bool>(),
                Arg.Any<IDictionary<string, object?>>(),
                Arg.Any<IAsyncBasicConsumer>(),
                Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                basicConsumer = callInfo.ArgAt<IAsyncBasicConsumer>(6);
                return Task.FromResult("consumer-tag");
            });

        var consumer = new RabbitMqConsumer(provider, serializer);

        await using var subscription = await consumer.SubscribeAsync<TestMessage>(
            (message, _) =>
            {
                receivedSource.TrySetResult(message);
                return Task.FromResult(RabbitMqConsumeResult.Ack);
            },
            new RabbitMqConsumerOptions
            {
                QueueName = "orders.auto-ack",
                AutoAck = true
            });

        var eventingConsumer = Assert.IsType<AsyncEventingBasicConsumer>(basicConsumer);
        await eventingConsumer.HandleBasicDeliverAsync(
            "consumer-tag",
            42,
            false,
            string.Empty,
            "orders.auto-ack",
            new BasicProperties(),
            new ReadOnlyMemory<byte>([1, 2, 3]),
            CancellationToken.None);

        var received = await receivedSource.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Null(received.Delivery);
    }
}
