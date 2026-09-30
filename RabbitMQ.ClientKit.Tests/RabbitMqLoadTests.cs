using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.ClientKit.ChannelPooling;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Consuming;
using RabbitMQ.ClientKit.Publishing;
using RabbitMQ.ClientKit.Tests.Support;
using Xunit;

namespace RabbitMQ.ClientKit.Tests;

[Collection(RabbitMqContainerCollection.Name)]
public sealed class RabbitMqLoadTests(RabbitMqContainerFixture fixture)
{
    private readonly RabbitMqContainerFixture _fixture = fixture;

    [RabbitMqLoadFact(Timeout = 180_000)]
    [Trait("Category", "Load")]
    public async Task PooledClient_PublishesAndConsumesConfiguredBurst()
    {
        var queueName = RabbitMqTestResources.CreateUniqueName("load-queue");
        var topology = RabbitMqTestResources.CreateQueueTopology(queueName);
        var processedMessages = 0;
        var messageCount = RabbitMqLoadTestSettings.MessageCount;
        var seenPayloads = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
        var completionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var services = new ServiceCollection();
        services.AddPooledRabbitMqClient(
            _fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Load", consumerDispatchConcurrency: 8),
            new RabbitMqChannelPoolingOptions
            {
                ProducerPoolSize = Math.Min(Environment.ProcessorCount, 8)
            });

        await using var serviceProvider = services.BuildServiceProvider();
        var publisher = serviceProvider.GetRequiredService<RabbitMqPublisher>();
        var consumer = serviceProvider.GetRequiredService<RabbitMqConsumer>();

        await using var subscription = await consumer.SubscribeAsync<TestMessage>(
            new RabbitMqConsumerOptions
            {
                QueueName = queueName,
                ConsumerName = queueName,
                PrefetchCount = 64,
                Topology = topology
            },
            (message, _) =>
            {
                seenPayloads.TryAdd(message.Payload.Value, 0);

                if (Interlocked.Increment(ref processedMessages) == messageCount)
                {
                    completionSource.TrySetResult(true);
                }

                return Task.FromResult(Models.RabbitMqConsumeResult.Ack);
            });

        var messages = Enumerable.Range(0, messageCount)
            .Select(index => new TestMessage { Value = $"load-message-{index}" })
            .ToArray();

        await publisher.PublishBatchAsync(
            messages,
            new RabbitMqPublishOptions
            {
                QueueName = queueName,
                Topology = topology
            });

        await completionSource.Task.WaitAsync(RabbitMqLoadTestSettings.Timeout);

        Assert.Equal(messageCount, processedMessages);
        Assert.Equal(messageCount, seenPayloads.Count);
    }
}
