using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Connection;
using RabbitMQ.ClientKit.DynamicConfiguration;
using RabbitMQ.ClientKit.Serialization;

namespace RabbitMQ.ClientKit.Tests;

public sealed class DynamicConfigurationTests
{
    [Fact]
    public async Task DynamicResolver_ReusesClientWhenConnectionDoesNotChange()
    {
        var source = new MutableDynamicConfigurationSource(CreateSnapshot("rabbit-a", "orders.created"));
        var activator = new RecordingDynamicClientActivator();
        var services = new ServiceCollection();

        services.AddSingleton<IRabbitMqDynamicConfigurationSource>(source);
        services.AddSingleton<IRabbitMqDynamicClientActivator>(activator);
        services.AddDynamicRabbitMqClientKit(sp => sp.GetRequiredService<IRabbitMqDynamicConfigurationSource>());

        await using var provider = services.BuildServiceProvider();

        var resolver = provider.GetRequiredService<IRabbitMqDynamicEndpointResolver>();
        var producer = resolver.GetProducer("orders");

        var firstClient = await producer.GetClientAsync();
        var firstRegistration = await producer.GetRegistrationAsync();

        source.Snapshot = CreateSnapshot("rabbit-a", "orders.updated");
        await resolver.RefreshAsync();

        var secondClient = await producer.GetClientAsync();
        var secondRegistration = await producer.GetRegistrationAsync();

        Assert.Same(firstClient, secondClient);
        Assert.Equal("orders.created", firstRegistration.Options.QueueName);
        Assert.Equal("orders.updated", secondRegistration.Options.QueueName);
        Assert.Equal(1, activator.CreateCount);
        Assert.Equal("rabbit-a", activator.CreatedHosts.Single());
    }

    [Fact]
    public async Task DynamicResolver_ReplacesClientWhenConnectionChanges()
    {
        var source = new MutableDynamicConfigurationSource(CreateSnapshot("rabbit-a", "orders.created"));
        var activator = new RecordingDynamicClientActivator();
        var services = new ServiceCollection();

        services.AddSingleton<IRabbitMqDynamicConfigurationSource>(source);
        services.AddSingleton<IRabbitMqDynamicClientActivator>(activator);
        services.AddDynamicRabbitMqClientKit(sp => sp.GetRequiredService<IRabbitMqDynamicConfigurationSource>());

        await using var provider = services.BuildServiceProvider();

        var resolver = provider.GetRequiredService<IRabbitMqDynamicEndpointResolver>();
        var producer = resolver.GetProducer("orders");

        var firstClient = await producer.GetClientAsync();

        source.Snapshot = CreateSnapshot("rabbit-b", "orders.created");
        await resolver.RefreshAsync();

        var secondClient = await producer.GetClientAsync();

        Assert.NotSame(firstClient, secondClient);
        Assert.Equal(2, activator.CreateCount);
        Assert.Equal(new[] { "rabbit-a", "rabbit-b" }, activator.CreatedHosts);
    }

    [Fact]
    public async Task AddDynamicRabbitMqClientKit_BindsConfigurationSection()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RabbitMq:DefaultConnection:HostName"] = "default-rabbit",
                ["RabbitMq:DefaultConnection:ClientProvidedName"] = "default-api",
                ["RabbitMq:NamedConnections:0:Name"] = "billing",
                ["RabbitMq:NamedConnections:0:HostName"] = "billing-rabbit",
                ["RabbitMq:NamedConnections:0:ClientProvidedName"] = "billing-api",
                ["RabbitMq:Producers:0:Name"] = "orders-created",
                ["RabbitMq:Producers:0:Publish:QueueName"] = "orders.created",
                ["RabbitMq:Producers:0:Publish:PublisherConfirms"] = "true",
                ["RabbitMq:Producers:0:Publish:ConfirmTimeout"] = "00:00:15",
                ["RabbitMq:Producers:0:Publish:Topology:Queues:0:Name"] = "orders.failed",
                ["RabbitMq:Producers:0:Publish:Topology:Queues:1:Name"] = "orders.invalid",
                ["RabbitMq:Consumers:0:Name"] = "billing-worker",
                ["RabbitMq:Consumers:0:ConnectionName"] = "billing",
                ["RabbitMq:Consumers:0:Subscribe:QueueName"] = "billing.charged"
            })
            .Build();

        var services = new ServiceCollection();
        services.AddDynamicRabbitMqClientKit(configuration.GetSection("RabbitMq"));

        await using var provider = services.BuildServiceProvider();

        var resolver = provider.GetRequiredService<IRabbitMqDynamicEndpointResolver>();
        var producer = resolver.GetProducer("orders-created");
        var consumer = resolver.GetConsumer("billing-worker");
        var defaultClient = await resolver.GetClientAsync();
        var billingClient = await resolver.GetClientAsync("billing");

        var producerRegistration = await producer.GetRegistrationAsync();
        var consumerRegistration = await consumer.GetRegistrationAsync();

        Assert.Equal("orders.created", producerRegistration.Options.QueueName);
        Assert.True(producerRegistration.Options.PublisherConfirms);
        Assert.Equal(TimeSpan.FromSeconds(15), producerRegistration.Options.ConfirmTimeout);
        Assert.Equal(2, producerRegistration.Options.Topology?.Queues.Count);
        Assert.Equal("billing", consumerRegistration.ConnectionName);
        Assert.Equal("default-rabbit", (await resolver.GetSnapshotAsync()).DefaultConnection?.HostName);
        Assert.Equal("billing-rabbit", (await resolver.GetSnapshotAsync()).NamedConnections["billing"].HostName);
        Assert.NotNull(defaultClient);
        Assert.NotNull(billingClient);
        Assert.NotSame(defaultClient, billingClient);
    }

    [Fact]
    public async Task DynamicResolver_RejectsUnknownConnectionReference()
    {
        var source = new MutableDynamicConfigurationSource(
            new RabbitMqDynamicConfigurationSnapshot(
                null,
                producers: new Dictionary<string, RabbitMqProducerRegistration>(StringComparer.OrdinalIgnoreCase)
                {
                    ["orders"] = new("orders", null, new RabbitMqPublishOptions { QueueName = "orders.created" })
                }));

        var services = new ServiceCollection();
        services.AddSingleton<IRabbitMqDynamicConfigurationSource>(source);
        services.AddDynamicRabbitMqClientKit(sp => sp.GetRequiredService<IRabbitMqDynamicConfigurationSource>());

        await using var provider = services.BuildServiceProvider();

        var resolver = provider.GetRequiredService<IRabbitMqDynamicEndpointResolver>();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await resolver.GetSnapshotAsync());

        Assert.Contains("default connection", exception.Message);
    }

    private static RabbitMqDynamicConfigurationSnapshot CreateSnapshot(string hostName, string queueName) =>
        new(
            new RabbitMqConnectionOptions
            {
                HostName = hostName,
                ClientProvidedName = hostName
            },
            producers: new Dictionary<string, RabbitMqProducerRegistration>(StringComparer.OrdinalIgnoreCase)
            {
                ["orders"] = new("orders", null, new RabbitMqPublishOptions { QueueName = queueName })
            },
            consumers: new Dictionary<string, RabbitMqConsumerRegistration>(StringComparer.OrdinalIgnoreCase)
            {
                ["orders-worker"] = new("orders-worker", null, new RabbitMqConsumerOptions { QueueName = queueName })
            });

    private sealed class MutableDynamicConfigurationSource(RabbitMqDynamicConfigurationSnapshot snapshot) : IRabbitMqDynamicConfigurationSource
    {
        public RabbitMqDynamicConfigurationSnapshot Snapshot { get; set; } = snapshot;

        public ValueTask<RabbitMqDynamicConfigurationSnapshot> GetConfigurationAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(Snapshot);
    }

    private sealed class RecordingDynamicClientActivator : IRabbitMqDynamicClientActivator
    {
        public List<string> CreatedHosts { get; } = [];

        public int CreateCount => CreatedHosts.Count;

        public RabbitMqClient CreateClient(string? connectionName, RabbitMqConnectionOptions connectionOptions)
        {
            CreatedHosts.Add(connectionOptions.HostName);

            var connectionManager = Substitute.For<IRabbitMqConnectionManager>();
            var serializer = Substitute.For<IRabbitMqSerializer>();
            var producerChannelProvider = Substitute.For<IRabbitMqProducerChannelProvider>();
            var consumerChannelProvider = Substitute.For<IRabbitMqConsumerChannelProvider>();

            serializer.ContentType.Returns("application/json");

            return new RabbitMqClient(
                connectionManager,
                serializer,
                producerChannelProvider,
                consumerChannelProvider);
        }
    }
}
