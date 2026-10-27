using System.Collections.Concurrent;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Models;
using RabbitMQ.ClientKit.Tests.Support;
using Xunit;

namespace RabbitMQ.ClientKit.Tests;

[Collection(RabbitMq43ContainerCollection.Name)]
public sealed class RabbitMqAmqp10DelayedRetryTests(RabbitMq43ContainerFixture fixture) : RabbitMqAmqp10DelayedRetryTestsBase(fixture);

[Collection(RabbitMq44AlpineContainerCollection.Name)]
public sealed class RabbitMq44AlpineAmqp10DelayedRetryTests(RabbitMq44AlpineContainerFixture fixture) : RabbitMqAmqp10DelayedRetryTestsBase(fixture);

public abstract class RabbitMqAmqp10DelayedRetryTestsBase(RabbitMqQuorumContainerFixture fixture)
{
    private readonly RabbitMqQuorumContainerFixture _fixture = fixture;

    [ContainerRuntimeFact]
    [Trait("Category", "Integration")]
    public async Task DelayRetryAsync_HoldsSameBodyUntilDeliveryTimeAndIncrementsDeliveryCount()
    {
        var queueName = RabbitMqTestResources.CreateUniqueName("amqp10-delay-retry");
        var topology = RabbitMqTestResources.CreateQuorumQueueTopology(queueName);
        var deliveries = new ConcurrentQueue<(DateTimeOffset ReceivedAt, RabbitMqReceivedMessage<TestMessage> Message)>();
        var secondDeliverySource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerError = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var delay = TimeSpan.FromSeconds(2);

        await using var client = new RabbitMqClient(_fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Amqp10.DelayRetry"));
        await using var subscription = await client.SubscribeAsync<TestMessage>(
            new RabbitMqConsumerOptions
            {
                QueueName = queueName,
                ConsumerName = queueName,
                PrefetchCount = 1,
                Protocol = RabbitMqConsumerProtocol.Amqp10,
                Topology = topology
            },
            async (message, cancellationToken) =>
            {
                try
                {
                    deliveries.Enqueue((DateTimeOffset.UtcNow, message));

                    if (deliveries.Count == 1)
                    {
                        await message.Delivery!.DelayRetryAsync(delay, deliveryFailed: true, cancellationToken);
                        return RabbitMqConsumeResult.Handled;
                    }

                    await message.Delivery!.AckAsync(cancellationToken);
                    secondDeliverySource.TrySetResult(true);
                    return RabbitMqConsumeResult.Handled;
                }
                catch (Exception ex)
                {
                    handlerError.TrySetResult(ex);
                    throw;
                }
            });

        await client.PublishAsync(
            new TestMessage { Value = "hold-me" },
            new RabbitMqPublishOptions
            {
                QueueName = queueName,
                Topology = topology
            });

        var firstDeliveryDeadline = DateTimeOffset.UtcNow.Add(TimeSpan.FromSeconds(10));
        while (deliveries.IsEmpty && DateTimeOffset.UtcNow < firstDeliveryDeadline)
        {
            await Task.Delay(50);
        }

        Assert.False(deliveries.IsEmpty, handlerError.Task.IsCompleted
            ? handlerError.Task.Result.ToString()
            : "The first AMQP 1.0 delivery did not arrive.");

        await Task.Delay(TimeSpan.FromMilliseconds(1500));
        Assert.False(handlerError.Task.IsCompleted, handlerError.Task.IsCompleted ? handlerError.Task.Result.ToString() : null);
        Assert.Single(deliveries);

        var completed = await Task.WhenAny(secondDeliverySource.Task, Task.Delay(TimeSpan.FromSeconds(20)));
        if (completed != secondDeliverySource.Task)
        {
            var queueState = await _fixture.DescribeQueuesAsync();
            var brokerTime = await _fixture.EvalAsync("erlang:system_time(millisecond).");
            var hostTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            throw new TimeoutException(
                $"Second delivery did not arrive. Host unix ms={hostTime}. Broker time: {brokerTime}. Queue state: {queueState}");
        }

        var received = deliveries.ToArray();
        Assert.Equal(2, received.Length);
        Assert.All(received, item => Assert.Equal("hold-me", item.Message.Payload.Value));
        Assert.True(received[1].ReceivedAt - received[0].ReceivedAt >= TimeSpan.FromSeconds(1.8));
        Assert.True(received[1].Message.Context.DeliveryCount > received[0].Message.Context.DeliveryCount);
    }
}
