using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
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
        var topologyInitializer = new RabbitMqTopologyInitializer();
        var body = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 });

        lease.Channel.Returns(channel);
        provider.RentAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<IRabbitMqChannelLease>(lease));
        serializer.Serialize(Arg.Any<TestMessage>()).Returns(body);
        serializer.ContentType.Returns("application/json");

        var publisher = new RabbitMqPublisher(provider, serializer, topologyInitializer);

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

        await channel.Received(1).BasicPublishAsync<BasicProperties>(
            string.Empty,
            "orders.created",
            false,
            Arg.Is<BasicProperties>(properties =>
                properties.ContentType == "application/json" &&
                properties.CorrelationId == "corr-1" &&
                properties.Persistent),
            body,
            Arg.Any<CancellationToken>());
        await lease.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task PublishAsync_RejectsOptionsWithoutExchangeRoutingKeyOrQueue()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        var publisher = new RabbitMqPublisher(provider, serializer, new RabbitMqTopologyInitializer());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            publisher.PublishAsync(new TestMessage(), new RabbitMqPublishOptions()));

        await provider.DidNotReceive().RentAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishBatchAsync_UsesSingleLeaseAndPublishesEachMessage()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var lease = Substitute.For<IRabbitMqChannelLease>();
        var channel = Substitute.For<IChannel>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        var topologyInitializer = new RabbitMqTopologyInitializer();
        var body = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 });
        var messages = new[]
        {
            new TestMessage { Value = "one" },
            new TestMessage { Value = "two" }
        };

        lease.Channel.Returns(channel);
        provider.RentAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<IRabbitMqChannelLease>(lease));
        serializer.Serialize(Arg.Any<TestMessage>()).Returns(body);
        serializer.ContentType.Returns("application/json");

        var publisher = new RabbitMqPublisher(provider, serializer, topologyInitializer);

        await publisher.PublishBatchAsync(
            messages,
            new RabbitMqPublishOptions
            {
                QueueName = "orders.created",
                Topology = new RabbitMqTopologyOptions
                {
                    Queue = new RabbitMqQueueOptions
                    {
                        Name = "orders.created",
                        Durable = true
                    }
                }
            });

        await provider.Received(1).RentAsync(Arg.Any<CancellationToken>());
        await channel.Received(2).BasicPublishAsync<BasicProperties>(
            string.Empty,
            "orders.created",
            false,
            Arg.Is<BasicProperties>(properties =>
                properties.ContentType == "application/json" &&
                properties.Persistent),
            body,
            Arg.Any<CancellationToken>());
        await lease.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task PublishBatchAsync_DoesNotRentChannelForEmptyBatch()
    {
        var provider = Substitute.For<IRabbitMqProducerChannelProvider>();
        var serializer = Substitute.For<IRabbitMqSerializer>();
        var publisher = new RabbitMqPublisher(provider, serializer, new RabbitMqTopologyInitializer());

        await publisher.PublishBatchAsync(Array.Empty<TestMessage>(), new RabbitMqPublishOptions
        {
            QueueName = "orders.created"
        });

        await provider.DidNotReceive().RentAsync(Arg.Any<CancellationToken>());
        serializer.DidNotReceive().Serialize(Arg.Any<TestMessage>());
    }
}
