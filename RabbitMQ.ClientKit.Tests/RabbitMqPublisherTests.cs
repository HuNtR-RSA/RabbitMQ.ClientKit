using NSubstitute;
using NSubstitute.ExceptionExtensions;
using RabbitMQ.Client;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Models;
using RabbitMQ.ClientKit.Publishing;
using RabbitMQ.ClientKit.Serialization;
using RabbitMQ.ClientKit.Tests.Support;
using RabbitMQ.ClientKit.Topology;

namespace RabbitMQ.ClientKit.Tests;

public sealed class RabbitMqPublisherTests
{
    [Fact]
    public async Task PublishAsync_UsesQueueNameAsRoutingKeyAndSerializerContentType()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        var body = new ReadOnlyMemory<byte>([1, 2, 3]);

        lease.Channel.Returns(channel);
        provider.RentAsync(Arg.Any<bool>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<IRabbitMqChannelLease>(lease));
        serializer.Serialize(Arg.Any<TestMessage>()).Returns(body);
        serializer.ContentType.Returns("application/json");

        var publisher = new RabbitMqPublisher(provider, serializer);

        await publisher.PublishAsync(
            new TestMessage { Value = "hello" },
            new RabbitMqPublishOptions
            {
                QueueName = "orders.created",
                Properties = new RabbitMqMessageProperties
                {
                    CorrelationId = "corr-1"
                }
            });

        await channel.Received(1).BasicPublishAsync
        (
            string.Empty,
            "orders.created",
            false,
            Arg.Is<BasicProperties>
            (
                properties =>
                    properties.ContentType == "application/json" &&
                    properties.CorrelationId == "corr-1" &&
                    properties.Persistent
            ),
            body,
            Arg.Any<CancellationToken>()
        );
        
        await lease.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task PublishAsync_RawBytes_DoesNotCallSerializer()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        var body = new ReadOnlyMemory<byte>([9, 8, 7]);

        lease.Channel.Returns(channel);
        provider.RentAsync
        (
            Arg.Any<bool>(),
            Arg.Any<TimeSpan?>(),
            Arg.Any<CancellationToken>()
        ).Returns(new ValueTask<IRabbitMqChannelLease>(lease));

        var publisher = new RabbitMqPublisher(provider, serializer);

        await publisher.PublishAsync
        (
            body,
            new RabbitMqPublishOptions
            {
                QueueName = "orders.raw"
            }
        );

        serializer.DidNotReceive().Serialize(Arg.Any<object>());
        await channel.Received(1).BasicPublishAsync
        (
            string.Empty,
            "orders.raw",
            false,
            Arg.Is<BasicProperties>(p => p.Persistent),
            body,
            Arg.Any<CancellationToken>()
        );
    }

    [Fact]
    public async Task PublishAsync_RejectsOptionsWithoutExchangeRoutingKeyOrQueue()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        var publisher = new RabbitMqPublisher(provider, serializer);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            publisher.PublishAsync(new TestMessage(), new RabbitMqPublishOptions()));

        await provider.DidNotReceive().RentAsync(Arg.Any<bool>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishBatchAsync_EmptyBatch_ReturnsEmpty()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        var publisher = new RabbitMqPublisher(provider, serializer);

        var result = await publisher.PublishBatchAsync(Array.Empty<TestMessage>(), new RabbitMqPublishOptions
        {
            QueueName = "orders.created"
        });

        Assert.Equal(RabbitMqPublishBatchStatus.Confirmed, result.Status);
        Assert.Equal(0, result.AttemptedCount);
        Assert.Equal(0, result.ConfirmedCount);
        await provider.DidNotReceive().RentAsync(Arg.Any<bool>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>());
        serializer.DidNotReceive().Serialize(Arg.Any<TestMessage>());
    }

    [Fact]
    public async Task PublishBatchAsync_RentThrows_ReturnsNotSent()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        provider.RentAsync
        (
            Arg.Any<bool>(),
            Arg.Any<TimeSpan?>(),
            Arg.Any<CancellationToken>()
        ).Returns(_ => ValueTask.FromException<IRabbitMqChannelLease>(new InvalidOperationException("pool exhausted")));

        var publisher = new RabbitMqPublisher(provider, serializer);

        var result = await publisher.PublishBatchAsync
        (
            [new TestMessage { Value = "one" }],
            new RabbitMqPublishOptions { QueueName = "orders.created" }
        );

        Assert.Equal(RabbitMqPublishBatchStatus.NotSent, result.Status);
        Assert.Equal(0, result.AttemptedCount);
        Assert.Equal(0, result.ConfirmedCount);
    }

    [Fact]
    public async Task PublishBatchAsync_DeclareThrows_ReturnsNotSent()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var serializer = Substitute.For<IRabbitMqSerializer>();

        channel.QueueDeclareAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<IDictionary<string, object?>>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("declare failed"));

        lease.Channel.Returns(channel);
        provider.RentAsync
        (
            Arg.Any<bool>(),
            Arg.Any<TimeSpan?>(),
            Arg.Any<CancellationToken>()
        ).Returns(new ValueTask<IRabbitMqChannelLease>(lease));

        var publisher = new RabbitMqPublisher(provider, serializer);

        var result = await publisher.PublishBatchAsync
        (
            [new TestMessage { Value = "one" }],
            new RabbitMqPublishOptions
            {
                QueueName = "orders.created",
                Topology = new RabbitMqTopologyOptions
                {
                    Queue = new RabbitMqQueueOptions { Name = "orders.created" }
                }
            }
        );

        Assert.Equal(RabbitMqPublishBatchStatus.NotSent, result.Status);
        Assert.Equal(0, result.AttemptedCount);
        Assert.Equal(0, result.ConfirmedCount);
    }

    [Fact]
    public async Task PublishBatchAsync_FirstPublishThrows_ReturnsUnconfirmed10()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        var body = new ReadOnlyMemory<byte>([1, 2, 3]);

        channel.BasicPublishAsync
        (
            Arg.Any<string>(),
            Arg.Any<string>(),
            Arg.Any<bool>(),
            Arg.Any<BasicProperties>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<CancellationToken>()
        ).Returns(_ => ValueTask.FromException(new TimeoutException("confirm timed out")));

        lease.Channel.Returns(channel);
        provider.RentAsync
        (
            Arg.Any<bool>(),
            Arg.Any<TimeSpan?>(),
            Arg.Any<CancellationToken>()
        ).Returns(new ValueTask<IRabbitMqChannelLease>(lease));
        
        serializer.Serialize(Arg.Any<TestMessage>()).Returns(body);

        var publisher = new RabbitMqPublisher(provider, serializer);

        var result = await publisher.PublishBatchAsync
        (
            [new TestMessage { Value = "one" }, new TestMessage { Value = "two" }],
            new RabbitMqPublishOptions { QueueName = "orders.created" }
        );

        Assert.Equal(RabbitMqPublishBatchStatus.Unconfirmed, result.Status);
        Assert.Equal(1, result.AttemptedCount);
        Assert.Equal(0, result.ConfirmedCount);
    }

    [Fact]
    public async Task PublishBatchAsync_FirstSerializationThrows_ReturnsNotSent()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var serializer = Substitute.For<IRabbitMqSerializer>();

        lease.Channel.Returns(channel);
        provider.RentAsync
        (
            Arg.Any<bool>(),
            Arg.Any<TimeSpan?>(),
            Arg.Any<CancellationToken>()
        ).Returns(new ValueTask<IRabbitMqChannelLease>(lease));

        serializer.Serialize(Arg.Any<TestMessage>()).Returns(_ => throw new InvalidOperationException("serialize failed"));

        var publisher = new RabbitMqPublisher(provider, serializer);

        var result = await publisher.PublishBatchAsync
        (
            [new TestMessage { Value = "one" }],
            new RabbitMqPublishOptions { QueueName = "orders.created" }
        );

        Assert.Equal(RabbitMqPublishBatchStatus.NotSent, result.Status);
        Assert.Equal(0, result.AttemptedCount);
        Assert.Equal(0, result.ConfirmedCount);
        await channel.DidNotReceive().BasicPublishAsync
        (
            Arg.Any<string>(),
            Arg.Any<string>(),
            Arg.Any<bool>(),
            Arg.Any<BasicProperties>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<CancellationToken>()
        );
    }

    [Fact]
    public async Task PublishBatchAsync_AllSucceed_ReturnsConfirmedN()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        var body = new ReadOnlyMemory<byte>([1, 2, 3]);
        var messages = new[]
        {
            new TestMessage { Value = "one" },
            new TestMessage { Value = "two" }
        };

        lease.Channel.Returns(channel);
        provider.RentAsync
        (
            Arg.Any<bool>(),
            Arg.Any<TimeSpan?>(),
            Arg.Any<CancellationToken>()
        ).Returns(new ValueTask<IRabbitMqChannelLease>(lease));
        
        serializer.Serialize(Arg.Any<TestMessage>()).Returns(body);
        serializer.ContentType.Returns("application/json");

        var publisher = new RabbitMqPublisher(provider, serializer);

        var result = await publisher.PublishBatchAsync
        (
            messages,
            new RabbitMqPublishOptions
            {
                QueueName = "orders.created"
            }
        );

        Assert.Equal(RabbitMqPublishBatchStatus.Confirmed, result.Status);
        Assert.Equal(2, result.AttemptedCount);
        Assert.Equal(2, result.ConfirmedCount);
        await provider.Received(1).RentAsync(Arg.Any<bool>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>());
        await channel.Received(2).BasicPublishAsync
        (
            string.Empty,
            "orders.created",
            false,
            Arg.Any<BasicProperties>(),
            body,
            Arg.Any<CancellationToken>()
        );
        
        await lease.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task DeclareAsync_DeclaresQueueAndQueuesWithoutPublishing()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var serializer = Substitute.For<IRabbitMqSerializer>();

        lease.Channel.Returns(channel);
        provider.RentAsync
        (
            Arg.Any<bool>(),
            Arg.Any<TimeSpan?>(),
            Arg.Any<CancellationToken>()
        ).Returns(new ValueTask<IRabbitMqChannelLease>(lease));

        var publisher = new RabbitMqPublisher(provider, serializer);

        var topology = new RabbitMqTopologyOptions
        {
            Queue = new RabbitMqQueueOptions { Name = "main-queue", Durable = true },
            Queues =
            [
                new RabbitMqQueueOptions { Name = "failed-queue", Durable = true },
                new RabbitMqQueueOptions { Name = "invalid-queue", Durable = true }
            ]
        };

        await publisher.DeclareAsync(topology);

        await channel.Received(1).QueueDeclareAsync("main-queue", true, false, false, Arg.Any<IDictionary<string, object?>>(), false, false, Arg.Any<CancellationToken>());
        await channel.Received(1).QueueDeclareAsync("failed-queue", true, false, false, Arg.Any<IDictionary<string, object?>>(), false, false, Arg.Any<CancellationToken>());
        await channel.Received(1).QueueDeclareAsync("invalid-queue", true, false, false, Arg.Any<IDictionary<string, object?>>(), false, false, Arg.Any<CancellationToken>());
        await channel.DidNotReceive().BasicPublishAsync
        (
            Arg.Any<string>(),
            Arg.Any<string>(),
            Arg.Any<bool>(),
            Arg.Any<BasicProperties>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<CancellationToken>()
        );
    }
}
