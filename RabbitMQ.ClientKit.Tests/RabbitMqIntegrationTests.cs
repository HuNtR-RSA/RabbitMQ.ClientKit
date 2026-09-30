using System.Collections.Concurrent;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Models;
using RabbitMQ.ClientKit.Tests.Support;
using Xunit;

namespace RabbitMQ.ClientKit.Tests;

[Collection(RabbitMqContainerCollection.Name)]
public sealed class RabbitMqIntegrationTests(RabbitMqContainerFixture fixture)
{
    private readonly RabbitMqContainerFixture _fixture = fixture;

    [ContainerRuntimeFact]
    [Trait("Category", "Integration")]
    public async Task PublishAndSubscribeAsync_RoundTripsPayloadAndProperties()
    {
        var exchangeName = RabbitMqTestResources.CreateUniqueName("integration-exchange");
        var queueName = RabbitMqTestResources.CreateUniqueName("integration-queue");
        var routingKey = RabbitMqTestResources.CreateUniqueName("integration-route");
        var correlationId = Guid.NewGuid().ToString("N");
        var topology = RabbitMqTestResources.CreateDirectTopology(exchangeName, queueName, routingKey);
        var receivedMessageSource = new TaskCompletionSource<RabbitMqReceivedMessage<TestMessage>>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var client = new RabbitMqClient(_fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Integration.RoundTrip"));
        await using var subscription = await client.SubscribeAsync<TestMessage>(
            new RabbitMqConsumerOptions
            {
                QueueName = queueName,
                ConsumerName = queueName,
                Topology = topology
            },
            (message, _) =>
            {
                receivedMessageSource.TrySetResult(message);
                return Task.FromResult(RabbitMqConsumeResult.Ack);
            });

        await client.PublishAsync(
            new TestMessage { Value = "hello from integration test" },
            new RabbitMqPublishOptions
            {
                ExchangeName = exchangeName,
                RoutingKey = routingKey,
                Topology = topology,
                Properties = new RabbitMqMessageProperties
                {
                    CorrelationId = correlationId,
                    MessageId = RabbitMqTestResources.CreateUniqueName("message")
                }
            });

        var received = await receivedMessageSource.Task.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.Equal("hello from integration test", received.Payload.Value);
        Assert.Equal(exchangeName, received.Context.Exchange);
        Assert.Equal(routingKey, received.Context.RoutingKey);
        Assert.Equal(correlationId, received.Context.CorrelationId);
        Assert.Equal("application/json", received.Context.ContentType);
        Assert.False(received.Context.Redelivered);
    }

    [ContainerRuntimeFact]
    [Trait("Category", "Integration")]
    public async Task SubscribeAsync_RequeueResultRedeliversMessage()
    {
        var queueName = RabbitMqTestResources.CreateUniqueName("integration-requeue");
        var topology = RabbitMqTestResources.CreateQueueTopology(queueName);
        var deliveries = new ConcurrentQueue<RabbitMqReceivedMessage<TestMessage>>();
        var secondDeliverySource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;

        await using var client = new RabbitMqClient(_fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Integration.Requeue"));
        await using var subscription = await client.SubscribeAsync<TestMessage>(
            new RabbitMqConsumerOptions
            {
                QueueName = queueName,
                ConsumerName = queueName,
                PrefetchCount = 1,
                Topology = topology
            },
            (message, _) =>
            {
                deliveries.Enqueue(message);

                if (Interlocked.Increment(ref attempts) == 1)
                {
                    return Task.FromResult(RabbitMqConsumeResult.Requeue);
                }

                secondDeliverySource.TrySetResult(true);
                return Task.FromResult(RabbitMqConsumeResult.Ack);
            });

        await client.PublishAsync(
            new TestMessage { Value = "retry-me" },
            new RabbitMqPublishOptions
            {
                QueueName = queueName,
                Topology = topology
            });

        await secondDeliverySource.Task.WaitAsync(TimeSpan.FromSeconds(20));

        var receivedDeliveries = deliveries.ToArray();
        Assert.Equal(2, receivedDeliveries.Length);
        Assert.All(receivedDeliveries, message => Assert.Equal("retry-me", message.Payload.Value));
        Assert.False(receivedDeliveries[0].Context.Redelivered);
        Assert.True(receivedDeliveries[1].Context.Redelivered);
    }

    [ContainerRuntimeFact]
    [Trait("Category", "Integration")]
    public async Task SubscribeAsync_RejectResultDropsMessageWithoutBlockingNextDelivery()
    {
        var queueName = RabbitMqTestResources.CreateUniqueName("integration-reject");
        var topology = RabbitMqTestResources.CreateQueueTopology(queueName);
        var deliveries = new ConcurrentQueue<RabbitMqReceivedMessage<TestMessage>>();
        var secondDeliverySource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var client = new RabbitMqClient(_fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Integration.Reject"));
        await using var subscription = await client.SubscribeAsync<TestMessage>(
            new RabbitMqConsumerOptions
            {
                QueueName = queueName,
                ConsumerName = queueName,
                PrefetchCount = 1,
                Topology = topology
            },
            (message, _) =>
            {
                deliveries.Enqueue(message);

                if (message.Payload.Value == "drop-me")
                {
                    return Task.FromResult(RabbitMqConsumeResult.Reject);
                }

                secondDeliverySource.TrySetResult(true);
                return Task.FromResult(RabbitMqConsumeResult.Ack);
            });

        await client.PublishBatchAsync(
            new[]
            {
                new TestMessage { Value = "drop-me" },
                new TestMessage { Value = "keep-me" }
            },
            new RabbitMqPublishOptions
            {
                QueueName = queueName,
                Topology = topology
            });

        await secondDeliverySource.Task.WaitAsync(TimeSpan.FromSeconds(20));

        var receivedDeliveries = deliveries.ToArray();
        Assert.Equal(2, receivedDeliveries.Length);
        Assert.Equal("drop-me", receivedDeliveries[0].Payload.Value);
        Assert.Equal("keep-me", receivedDeliveries[1].Payload.Value);
        Assert.All(receivedDeliveries, message => Assert.False(message.Context.Redelivered));
    }
}
