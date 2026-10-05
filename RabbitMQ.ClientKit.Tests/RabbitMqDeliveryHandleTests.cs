using NSubstitute;
using NSubstitute.ExceptionExtensions;
using RabbitMQ.Client;
using RabbitMQ.ClientKit.Consuming;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Serialization;
using RabbitMQ.ClientKit.Tests.Support;

namespace RabbitMQ.ClientKit.Tests;

public sealed class RabbitMqDeliveryHandleTests
{
    [Fact]
    public async Task ReplaceAndAckAsync_WhenPublishAndAckSucceed_PublishesThenAcksAndSetsSettled()
    {
        var channel = Substitute.For<IChannel>();
        channel.IsOpen.Returns(true);
        var serializer = Substitute.For<IRabbitMqSerializer>();
        serializer.Serialize(Arg.Any<TestMessage>()).Returns(new byte[] { 1, 2, 3 });
        serializer.ContentType.Returns("application/json");

        var handle = new RabbitMqDeliveryHandle(channel, deliveryTag: 42, serializer);

        Assert.False(handle.IsSettled);

        await handle.ReplaceAndAckAsync(
            new TestMessage { Value = "replacement" },
            new RabbitMqPublishOptions { QueueName = "dest-queue" });

        Assert.True(handle.IsSettled);

        await channel.Received(1).BasicPublishAsync
        (
            string.Empty,
            "dest-queue",
            false,
            Arg.Any<BasicProperties>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<CancellationToken>()
        );

        await channel.Received(1).BasicAckAsync(42, false, Arg.Any<CancellationToken>());
        await channel.DidNotReceive().BasicNackAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ReplaceAndAckAsync_WhenPublishThrows_NacksWithRequeueAndRethrowsWithoutAcking()
    {
        var channel = Substitute.For<IChannel>();
        channel.IsOpen.Returns(true);
        var serializer = Substitute.For<IRabbitMqSerializer>();
        serializer.Serialize(Arg.Any<TestMessage>()).Returns(new byte[] { 1, 2, 3 });

        channel.BasicPublishAsync
        (
            Arg.Any<string>(),
            Arg.Any<string>(),
            Arg.Any<bool>(),
            Arg.Any<BasicProperties>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<CancellationToken>()
        ).Returns(_ => ValueTask.FromException(new TimeoutException("confirm timeout")));

        var handle = new RabbitMqDeliveryHandle(channel, deliveryTag: 42, serializer);

        var ex = await Assert.ThrowsAsync<TimeoutException>
        (
            () =>
                handle.ReplaceAndAckAsync
                (
                    new TestMessage { Value = "replacement" },
                    new RabbitMqPublishOptions { QueueName = "dest-queue" }
                )
        );

        Assert.Equal("confirm timeout", ex.Message);
        Assert.True(handle.IsSettled);

        await channel.Received(1).BasicNackAsync(42, false, true, Arg.Any<CancellationToken>());
        await channel.DidNotReceive().BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ReplaceAndAckAsync_WhenAckThrowsAfterConfirmedPublish_DoesNotNackAndSwallowsAckException()
    {
        var channel = Substitute.For<IChannel>();
        channel.IsOpen.Returns(true);
        var serializer = Substitute.For<IRabbitMqSerializer>();
        serializer.Serialize(Arg.Any<TestMessage>()).Returns(new byte[] { 1, 2, 3 });

        channel.BasicAckAsync
        (
            42,
            false,
            Arg.Any<CancellationToken>()
        ).Returns(_ => ValueTask.FromException(new InvalidOperationException("ack communication dropped")));

        var handle = new RabbitMqDeliveryHandle(channel, deliveryTag: 42, serializer);

        // Does not throw because confirmed replacement is durable; original must not be nacked (which would duplicate).
        await handle.ReplaceAndAckAsync
        (
            new TestMessage { Value = "replacement" },
            new RabbitMqPublishOptions { QueueName = "dest-queue" }
        );

        Assert.True(handle.IsSettled);
        await channel.DidNotReceive().BasicNackAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RawReplaceAndAckAsync_PublishesRawBytesWithoutSerializer()
    {
        var channel = Substitute.For<IChannel>();
        channel.IsOpen.Returns(true);
        var serializer = Substitute.For<IRabbitMqSerializer>();
        var body = new ReadOnlyMemory<byte>([7, 8, 9]);

        var handle = new RabbitMqDeliveryHandle(channel, deliveryTag: 100, serializer);

        await handle.ReplaceAndAckAsync
        (
            body,
            new RabbitMqPublishOptions { QueueName = "dest-queue" }
        );

        serializer.DidNotReceive().Serialize(Arg.Any<object>());
        await channel.Received(1).BasicPublishAsync
        (
            string.Empty,
            "dest-queue",
            false,
            Arg.Any<BasicProperties>(),
            body,
            Arg.Any<CancellationToken>()
        );
        
        await channel.Received(1).BasicAckAsync(100, false, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishReplacementAsync_DeclaresTopologyBeforePublishing()
    {
        var channel = Substitute.For<IChannel>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        serializer.Serialize(Arg.Any<TestMessage>()).Returns(new byte[] { 1, 2, 3 });
        serializer.ContentType.Returns("application/json");

        var handle = new RabbitMqDeliveryHandle(channel, deliveryTag: 42, serializer);

        await handle.PublishReplacementAsync(
            new TestMessage { Value = "replacement" },
            new RabbitMqPublishOptions
            {
                QueueName = "dest-queue",
                Topology = new RabbitMqTopologyOptions
                {
                    Queue = new RabbitMqQueueOptions { Name = "dest-queue", Durable = true }
                }
            });

        await channel.Received(1).QueueDeclareAsync(
            "dest-queue",
            true,
            false,
            false,
            Arg.Any<IDictionary<string, object?>>(),
            false,
            false,
            Arg.Any<CancellationToken>());
        await channel.Received(1).BasicPublishAsync(
            string.Empty,
            "dest-queue",
            false,
            Arg.Any<BasicProperties>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<CancellationToken>());
    }
}
