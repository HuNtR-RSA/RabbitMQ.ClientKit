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
            [
                new TestMessage { Value = "drop-me" },
                new TestMessage { Value = "keep-me" }
            ],
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

    [ContainerRuntimeFact]
    [Trait("Category", "Integration")]
    public async Task PublishBatchAsync_ConfirmedBatch_EachBodyAppearsOnce()
    {
        var queueName = RabbitMqTestResources.CreateUniqueName("integration-batch");
        var topology = RabbitMqTestResources.CreateQueueTopology(queueName);
        var received = new ConcurrentQueue<string>();
        var completionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var client = new RabbitMqClient(_fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Integration.Batch"));
        await using var subscription = await client.SubscribeAsync<TestMessage>
        (
            new RabbitMqConsumerOptions
            {
                QueueName = queueName,
                ConsumerName = queueName,
                PrefetchCount = 10,
                Topology = topology
            },
            (message, _) =>
            {
                received.Enqueue(message.Payload.Value);
                if (received.Count == 5)
                {
                    completionSource.TrySetResult(true);
                }

                return Task.FromResult(RabbitMqConsumeResult.Ack);
            }
        );

        var messages = Enumerable.Range(1, 5).Select(i => new TestMessage { Value = $"msg-{i}" }).ToList();
        var result = await client.PublishBatchAsync
        (
            messages,
            new RabbitMqPublishOptions
            {
                QueueName = queueName,
                Topology = topology,
                PublisherConfirms = true
            }
        );

        Assert.Equal(RabbitMqPublishBatchStatus.Confirmed, result.Status);
        Assert.Equal(5, result.AttemptedCount);
        Assert.Equal(5, result.ConfirmedCount);

        await completionSource.Task.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.Equal(5, received.Count);
        Assert.Equal(messages.Select(m => m.Value).OrderBy(x => x), received.OrderBy(x => x));
    }

    [ContainerRuntimeFact]
    [Trait("Category", "Integration")]
    public async Task SubscribeAsync_ReplaceAndAckAsync_PublishesToSecondQueueAndSettlesOriginal()
    {
        var sourceQueue = RabbitMqTestResources.CreateUniqueName("integration-replace-src");
        var destQueue = RabbitMqTestResources.CreateUniqueName("integration-replace-dst");
        var topology = new RabbitMqTopologyOptions
        {
            Queue = new RabbitMqQueueOptions { Name = sourceQueue, Durable = true },
            Queues = [new RabbitMqQueueOptions { Name = destQueue, Durable = true }]
        };

        var destReceivedSource = new TaskCompletionSource<RabbitMqReceivedMessage<TestMessage>>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var client = new RabbitMqClient(_fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Integration.ReplaceAndAck"));
        await using var destSubscription = await client.SubscribeAsync<TestMessage>
        (
            new RabbitMqConsumerOptions
            {
                QueueName = destQueue,
                ConsumerName = destQueue,
                Topology = topology
            },
            (message, _) =>
            {
                destReceivedSource.TrySetResult(message);
                return Task.FromResult(RabbitMqConsumeResult.Ack);
            }
        );

        await using var srcSubscription = await client.SubscribeAsync<TestMessage>
        (
            new RabbitMqConsumerOptions
            {
                QueueName = sourceQueue,
                ConsumerName = sourceQueue,
                Topology = topology
            },
            async (message, ct) =>
            {
                await message.Delivery!.ReplaceAndAckAsync(
                    new TestMessage { Value = "transformed-" + message.Payload.Value },
                    new RabbitMqPublishOptions
                    {
                        QueueName = destQueue
                    },
                    ct);

                return RabbitMqConsumeResult.Handled;
            }
        );

        await client.PublishAsync
        (
            new TestMessage { Value = "original" },
            new RabbitMqPublishOptions
            {
                QueueName = sourceQueue,
                Topology = topology,
                PublisherConfirms = true
            }
        );

        var destMsg = await destReceivedSource.Task.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.Equal("transformed-original", destMsg.Payload.Value);
    }

    [ContainerRuntimeFact]
    [Trait("Category", "Integration")]
    public async Task SubscribeAsync_RawRepublishIsByteIdenticalWithHeaders()
    {
        var sourceQueue = RabbitMqTestResources.CreateUniqueName("integration-raw-src");
        var destQueue = RabbitMqTestResources.CreateUniqueName("integration-raw-dst");
        var topology = new RabbitMqTopologyOptions
        {
            Queue = new RabbitMqQueueOptions { Name = sourceQueue, Durable = true },
            Queues = [new RabbitMqQueueOptions { Name = destQueue, Durable = true }]
        };

        var destReceivedSource = new TaskCompletionSource<RabbitMqReceivedMessage<TestMessage>>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var client = new RabbitMqClient(_fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Integration.RawRepublish"));
        await using var destSubscription = await client.SubscribeAsync<TestMessage>
        (
            new RabbitMqConsumerOptions
            {
                QueueName = destQueue,
                ConsumerName = destQueue,
                Topology = topology
            },
            (message, _) =>
            {
                destReceivedSource.TrySetResult(message);
                return Task.FromResult(RabbitMqConsumeResult.Ack);
            }
        );

        await using var srcSubscription = await client.SubscribeAsync<TestMessage>
        (
            new RabbitMqConsumerOptions
            {
                QueueName = sourceQueue,
                ConsumerName = sourceQueue,
                Topology = topology
            },
            async (message, ct) =>
            {
                // Raw replace and ack with headers and raw bytes preserved
                await message.Delivery!.ReplaceAndAckAsync
                (
                    message.Body,
                    new RabbitMqPublishOptions
                    {
                        QueueName = destQueue,
                        Properties = new RabbitMqMessageProperties
                        {
                            ContentType = message.Context.ContentType,
                            ContentEncoding = message.Context.ContentEncoding,
                            Type = message.Context.Type,
                            Headers = new Dictionary<string, object?>(message.Context.Headers)
                        }
                    },
                    ct
                );

                return RabbitMqConsumeResult.Handled;
            }
        );

        var originalMessage = new TestMessage { Value = "byte-exact-test" };
        var headerBytes = new byte[] { 0x01, 0x02, 0x03 };
        await client.PublishAsync
        (
            originalMessage,
            new RabbitMqPublishOptions
            {
                QueueName = sourceQueue,
                Topology = topology,
                PublisherConfirms = true,
                Properties = new RabbitMqMessageProperties
                {
                    ContentEncoding = "gzip",
                    Type = "custom.binary.event",
                    Headers = new Dictionary<string, object?>
                    {
                        ["X-Binary-Header"] = headerBytes,
                        ["X-String-Header"] = "test-value"
                    }
                }
            }
        );

        var destMsg = await destReceivedSource.Task.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.Equal("byte-exact-test", destMsg.Payload.Value);
        Assert.Equal("application/json", destMsg.Context.ContentType);
        Assert.Equal("gzip", destMsg.Context.ContentEncoding);
        Assert.Equal("custom.binary.event", destMsg.Context.Type);
        Assert.True(destMsg.Context.Headers.TryGetValue("X-String-Header", out var strVal));
        var stringHeaderValue = strVal is byte[] strBytes ? System.Text.Encoding.UTF8.GetString(strBytes) : strVal?.ToString();
        Assert.Equal("test-value", stringHeaderValue);
        Assert.True(destMsg.Context.Headers.TryGetValue("X-Binary-Header", out var binVal));
        Assert.Equal(headerBytes, (byte[]?)binVal);
    }

    [ContainerRuntimeFact]
    [Trait("Category", "Integration")]
    public async Task DeclareAsync_CreatesMultipleQueuesWithoutPublish()
    {
        var queue1 = RabbitMqTestResources.CreateUniqueName("integration-declare-q1");
        var queue2 = RabbitMqTestResources.CreateUniqueName("integration-declare-q2");
        var queue3 = RabbitMqTestResources.CreateUniqueName("integration-declare-q3");

        var topology = new RabbitMqTopologyOptions
        {
            Queue = new RabbitMqQueueOptions { Name = queue1, Durable = true },
            Queues =
            [
                new RabbitMqQueueOptions { Name = queue2, Durable = true },
                new RabbitMqQueueOptions { Name = queue3, Durable = true }
            ]
        };

        await using var client = new RabbitMqClient(_fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Integration.DeclareAsync"));
        await client.DeclareAsync(topology);

        var completionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await client.SubscribeAsync<TestMessage>
        (
            new RabbitMqConsumerOptions
            {
                QueueName = queue2,
                ConsumerName = queue2
            },
            (_, _) =>
            {
                completionSource.TrySetResult(true);
                return Task.FromResult(RabbitMqConsumeResult.Ack);
            }
        );

        Assert.True(sub.IsHealthy);

        await client.PublishAsync
        (
            new TestMessage { Value = "declared-ok" },
            new RabbitMqPublishOptions { QueueName = queue2 }
        );

        await completionSource.Task.WaitAsync(TimeSpan.FromSeconds(20));
    }
}
