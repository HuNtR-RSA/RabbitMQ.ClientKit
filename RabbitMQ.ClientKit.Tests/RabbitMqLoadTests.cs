using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.ClientKit.ChannelPooling;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Consuming;
using RabbitMQ.ClientKit.Publishing;
using RabbitMQ.ClientKit.Tests.Support;
using Xunit;
using Xunit.Abstractions;

namespace RabbitMQ.ClientKit.Tests;

[Collection(RabbitMqContainerCollection.Name)]
public sealed class RabbitMqLoadTests
{
    private readonly RabbitMqContainerFixture _fixture;
    private readonly ITestOutputHelper _output;

    public RabbitMqLoadTests(RabbitMqContainerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [RabbitMqLoadFact(Timeout = 180_000)]
    [Trait("Category", "Load")]
    public async Task TransientClient_PublishesAndConsumesConfiguredBurst()
    {
        var result = await RunScenarioAsync(
            "Transient/default",
            services => services.AddRabbitMqClient(_fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Load.Transient", consumerDispatchConcurrency: 8)));

        WriteScenarioResult(result);
    }

    [RabbitMqLoadFact(Timeout = 180_000)]
    [Trait("Category", "Load")]
    public async Task PooledClient_PublishesAndConsumesConfiguredBurst()
    {
        var result = await RunScenarioAsync(
            "Pooled",
            services => services.AddPooledRabbitMqClient(
                _fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Load.Pooled", consumerDispatchConcurrency: 8),
                new RabbitMqChannelPoolingOptions
                {
                    ProducerPoolSize = Math.Min(Environment.ProcessorCount, 8)
                }));

        WriteScenarioResult(result);
    }

    [RabbitMqLoadFact(Timeout = 180_000)]
    [Trait("Category", "Load")]
    public async Task TransientAndPooledClients_ReportComparativeMetrics()
    {
        var transientResult = await RunScenarioAsync(
            "Transient/default",
            services => services.AddRabbitMqClient(_fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Load.TransientComparison", consumerDispatchConcurrency: 8)));

        var pooledResult = await RunScenarioAsync(
            "Pooled",
            services => services.AddPooledRabbitMqClient(
                _fixture.CreateConnectionOptions("RabbitMQ.ClientKit.Load.PooledComparison", consumerDispatchConcurrency: 8),
                new RabbitMqChannelPoolingOptions
                {
                    ProducerPoolSize = Math.Min(Environment.ProcessorCount, 8)
                }));

        WriteComparison(transientResult, pooledResult);
    }

    private async Task<RabbitMqLoadScenarioResult> RunScenarioAsync(
        string scenarioName,
        Action<IServiceCollection> configureServices)
    {
        var queueName = RabbitMqTestResources.CreateUniqueName("load-queue");
        var topology = RabbitMqTestResources.CreateQueueTopology(queueName);
        var processedMessages = 0;
        var messageCount = RabbitMqLoadTestSettings.MessageCount;
        var seenPayloads = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
        var completionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var services = new ServiceCollection();
        configureServices(services);

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

        var publishStopwatch = Stopwatch.StartNew();
        var endToEndStopwatch = Stopwatch.StartNew();

        await publisher.PublishBatchAsync(
            messages,
            new RabbitMqPublishOptions
            {
                QueueName = queueName,
                Topology = topology
            });

        publishStopwatch.Stop();
        await completionSource.Task.WaitAsync(RabbitMqLoadTestSettings.Timeout);
        endToEndStopwatch.Stop();

        Assert.Equal(messageCount, processedMessages);
        Assert.Equal(messageCount, seenPayloads.Count);

        return new RabbitMqLoadScenarioResult(
            scenarioName,
            messageCount,
            publishStopwatch.Elapsed,
            endToEndStopwatch.Elapsed);
    }

    private void WriteScenarioResult(RabbitMqLoadScenarioResult result)
    {
        _output.WriteLine($"Scenario: {result.ScenarioName}");
        _output.WriteLine($"Messages: {result.MessageCount}");
        _output.WriteLine($"Publish duration: {result.PublishDuration.TotalMilliseconds:F2} ms ({result.PublishMessagesPerSecond:F2} msg/s)");
        _output.WriteLine($"End-to-end duration: {result.EndToEndDuration.TotalMilliseconds:F2} ms ({result.EndToEndMessagesPerSecond:F2} msg/s)");
    }

    private void WriteComparison(RabbitMqLoadScenarioResult baseline, RabbitMqLoadScenarioResult candidate)
    {
        WriteScenarioResult(baseline);
        WriteScenarioResult(candidate);

        _output.WriteLine("Comparison:");
        _output.WriteLine(
            $"Publish throughput ratio ({candidate.ScenarioName} / {baseline.ScenarioName}): {candidate.PublishMessagesPerSecond / baseline.PublishMessagesPerSecond:F2}x");
        _output.WriteLine(
            $"End-to-end throughput ratio ({candidate.ScenarioName} / {baseline.ScenarioName}): {candidate.EndToEndMessagesPerSecond / baseline.EndToEndMessagesPerSecond:F2}x");
    }

    private sealed record RabbitMqLoadScenarioResult(
        string ScenarioName,
        int MessageCount,
        TimeSpan PublishDuration,
        TimeSpan EndToEndDuration)
    {
        public double PublishMessagesPerSecond => MessageCount / Math.Max(PublishDuration.TotalSeconds, double.Epsilon);

        public double EndToEndMessagesPerSecond => MessageCount / Math.Max(EndToEndDuration.TotalSeconds, double.Epsilon);
    }
}
