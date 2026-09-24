using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.ChannelPooling;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Connection;
using RabbitMQ.ClientKit.Consuming;
using RabbitMQ.ClientKit.Publishing;
using RabbitMQ.ClientKit.Serialization;

namespace RabbitMQ.ClientKit.Tests;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public async Task AddRabbitMqClient_RegistersCoreServices()
    {
        var services = new ServiceCollection();

        services.AddRabbitMqClient(new RabbitMqConnectionOptions
        {
            HostName = "localhost",
            ClientProvidedName = "orders-api"
        });

        await using var provider = services.BuildServiceProvider();

        var connectionOptions = provider.GetRequiredService<RabbitMqConnectionOptions>();
        var serializer = provider.GetRequiredService<IRabbitMqSerializer>();
        var connectionManager = provider.GetRequiredService<IRabbitMqConnectionManager>();
        var producerChannelProvider = provider.GetRequiredService<IRabbitMqProducerChannelProvider>();
        var consumerChannelProvider = provider.GetRequiredService<IRabbitMqConsumerChannelProvider>();
        var publisher = provider.GetRequiredService<RabbitMqPublisher>();
        var consumer = provider.GetRequiredService<RabbitMqConsumer>();
        var client = provider.GetRequiredService<RabbitMqClient>();

        Assert.Equal("localhost", connectionOptions.HostName);
        Assert.Equal("orders-api", connectionOptions.ClientProvidedName);
        Assert.IsType<JsonRabbitMqSerializer>(serializer);
        Assert.IsType<RabbitMqConnectionManager>(connectionManager);
        Assert.IsType<TransientProducerChannelProvider>(producerChannelProvider);
        Assert.IsType<TransientConsumerChannelProvider>(consumerChannelProvider);
        Assert.NotNull(publisher);
        Assert.NotNull(consumer);
        Assert.NotNull(client);
    }

    [Fact]
    public async Task AddPooledRabbitMqClient_RegistersPoolingServices()
    {
        var services = new ServiceCollection();

        services.AddPooledRabbitMqClient
        (
            new RabbitMqConnectionOptions
            {
                HostName = "localhost",
                ClientProvidedName = "payments-api"
            },
            new RabbitMqChannelPoolingOptions
            {
                ProducerPoolSize = 16
            });

        await using var provider = services.BuildServiceProvider();

        var connectionOptions = provider.GetRequiredService<RabbitMqConnectionOptions>();
        var poolingOptions = provider.GetRequiredService<RabbitMqChannelPoolingOptions>();
        var producerChannelProvider = provider.GetRequiredService<IRabbitMqProducerChannelProvider>();
        var consumerChannelProvider = provider.GetRequiredService<IRabbitMqConsumerChannelProvider>();
        var client = provider.GetRequiredService<RabbitMqClient>();

        Assert.Equal("localhost", connectionOptions.HostName);
        Assert.Equal("payments-api", connectionOptions.ClientProvidedName);
        Assert.Equal(16, poolingOptions.ProducerPoolSize);
        Assert.IsType<PooledProducerChannelProvider>(producerChannelProvider);
        Assert.IsType<ReusableConsumerChannelProvider>(consumerChannelProvider);
        Assert.NotNull(client);
    }

    [Fact]
    public async Task AddNamedRabbitMqClient_RegistersKeyedCoreServices()
    {
        var services = new ServiceCollection();

        services.AddNamedRabbitMqClient(
            "orders",
            new RabbitMqConnectionOptions
            {
                HostName = "orders-rabbit",
                ClientProvidedName = "orders-api"
            });

        await using var provider = services.BuildServiceProvider();

        var connectionOptions = provider.GetRequiredKeyedService<RabbitMqConnectionOptions>("orders");
        var serializer = provider.GetRequiredKeyedService<IRabbitMqSerializer>("orders");
        var connectionManager = provider.GetRequiredKeyedService<IRabbitMqConnectionManager>("orders");
        var producerChannelProvider = provider.GetRequiredKeyedService<IRabbitMqProducerChannelProvider>("orders");
        var consumerChannelProvider = provider.GetRequiredKeyedService<IRabbitMqConsumerChannelProvider>("orders");
        var publisher = provider.GetRequiredKeyedService<RabbitMqPublisher>("orders");
        var consumer = provider.GetRequiredKeyedService<RabbitMqConsumer>("orders");
        var client = provider.GetRequiredKeyedService<RabbitMqClient>("orders");

        Assert.Equal("orders-rabbit", connectionOptions.HostName);
        Assert.Equal("orders-api", connectionOptions.ClientProvidedName);
        Assert.IsType<JsonRabbitMqSerializer>(serializer);
        Assert.IsType<RabbitMqConnectionManager>(connectionManager);
        Assert.IsType<TransientProducerChannelProvider>(producerChannelProvider);
        Assert.IsType<TransientConsumerChannelProvider>(consumerChannelProvider);
        Assert.NotNull(publisher);
        Assert.NotNull(consumer);
        Assert.NotNull(client);
    }

    [Fact]
    public async Task AddNamedPooledRabbitMqClient_RegistersKeyedPoolingServices()
    {
        var services = new ServiceCollection();

        services.AddNamedPooledRabbitMqClient(
            "payments",
            new RabbitMqConnectionOptions
            {
                HostName = "payments-rabbit",
                ClientProvidedName = "payments-api"
            },
            new RabbitMqChannelPoolingOptions
            {
                ProducerPoolSize = 16
            });

        await using var provider = services.BuildServiceProvider();

        var connectionOptions = provider.GetRequiredKeyedService<RabbitMqConnectionOptions>("payments");
        var poolingOptions = provider.GetRequiredKeyedService<RabbitMqChannelPoolingOptions>("payments");
        var producerChannelProvider = provider.GetRequiredKeyedService<IRabbitMqProducerChannelProvider>("payments");
        var consumerChannelProvider = provider.GetRequiredKeyedService<IRabbitMqConsumerChannelProvider>("payments");
        var client = provider.GetRequiredKeyedService<RabbitMqClient>("payments");

        Assert.Equal("payments-rabbit", connectionOptions.HostName);
        Assert.Equal("payments-api", connectionOptions.ClientProvidedName);
        Assert.Equal(16, poolingOptions.ProducerPoolSize);
        Assert.IsType<PooledProducerChannelProvider>(producerChannelProvider);
        Assert.IsType<ReusableConsumerChannelProvider>(consumerChannelProvider);
        Assert.NotNull(client);
    }

    [Fact]
    public async Task AddNamedRabbitMqClient_AllowsMultipleIndependentRegistrations()
    {
        var services = new ServiceCollection();

        services.AddNamedRabbitMqClient(
            "orders",
            new RabbitMqConnectionOptions
            {
                HostName = "orders-rabbit",
                ClientProvidedName = "orders-api"
            });
        services.AddNamedRabbitMqClient(
            "billing",
            new RabbitMqConnectionOptions
            {
                HostName = "billing-rabbit",
                ClientProvidedName = "billing-api"
            });

        await using var provider = services.BuildServiceProvider();

        var ordersOptions = provider.GetRequiredKeyedService<RabbitMqConnectionOptions>("orders");
        var billingOptions = provider.GetRequiredKeyedService<RabbitMqConnectionOptions>("billing");
        var ordersClient = provider.GetRequiredKeyedService<RabbitMqClient>("orders");
        var billingClient = provider.GetRequiredKeyedService<RabbitMqClient>("billing");

        Assert.Equal("orders-rabbit", ordersOptions.HostName);
        Assert.Equal("billing-rabbit", billingOptions.HostName);
        Assert.NotSame(ordersOptions, billingOptions);
        Assert.NotSame(ordersClient, billingClient);
    }

    [Fact]
    public async Task AddRabbitMqClientKit_BindsConnectionsAndConfiguredEndpoints()
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
                ["RabbitMq:Producers:1:Name"] = "billing-charged",
                ["RabbitMq:Producers:1:ConnectionName"] = "billing",
                ["RabbitMq:Producers:1:Publish:ExchangeName"] = "billing",
                ["RabbitMq:Producers:1:Publish:RoutingKey"] = "charged",
                ["RabbitMq:Consumers:0:Name"] = "orders-worker",
                ["RabbitMq:Consumers:0:Consume:QueueName"] = "orders.created",
                ["RabbitMq:Consumers:1:Name"] = "billing-worker",
                ["RabbitMq:Consumers:1:ConnectionName"] = "billing",
                ["RabbitMq:Consumers:1:Consume:QueueName"] = "billing.charged",
                ["RabbitMq:Consumers:1:Consume:ConsumerName"] = "billing-worker-channel"
            })
            .Build();

        var services = new ServiceCollection();
        services.AddRabbitMqClientKit(configuration.GetSection("RabbitMq"));

        await using var provider = services.BuildServiceProvider();

        var registry = provider.GetRequiredService<IRabbitMqConfigurationRegistry>();
        var resolver = provider.GetRequiredService<IRabbitMqEndpointResolver>();
        var defaultClient = provider.GetRequiredService<RabbitMqClient>();
        var billingClient = provider.GetRequiredKeyedService<RabbitMqClient>("billing");

        var defaultProducer = resolver.GetRequiredProducer("orders-created");
        var billingProducer = resolver.GetRequiredProducer("billing-charged");
        var defaultConsumer = resolver.GetRequiredConsumer("orders-worker");
        var billingConsumer = resolver.GetRequiredConsumer("billing-worker");

        Assert.Equal("orders.created", registry.GetRequiredProducer("orders-created").Options.QueueName);
        Assert.Equal("billing", registry.GetRequiredProducer("billing-charged").ConnectionName);
        Assert.Equal("billing-worker-channel", registry.GetRequiredConsumer("billing-worker").Options.ConsumerName);
        Assert.Same(defaultClient, defaultProducer.Client);
        Assert.Same(billingClient, billingProducer.Client);
        Assert.Same(provider.GetRequiredService<RabbitMqPublisher>(), defaultProducer.Publisher);
        Assert.Same(provider.GetRequiredKeyedService<RabbitMqPublisher>("billing"), billingProducer.Publisher);
        Assert.Same(provider.GetRequiredService<RabbitMqConsumer>(), defaultConsumer.Consumer);
        Assert.Same(provider.GetRequiredKeyedService<RabbitMqConsumer>("billing"), billingConsumer.Consumer);
    }

    [Fact]
    public void AddRabbitMqClientKit_RejectsUnknownConnectionReferences()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RabbitMq:DefaultConnection:HostName"] = "default-rabbit",
                ["RabbitMq:Producers:0:Name"] = "orders-created",
                ["RabbitMq:Producers:0:ConnectionName"] = "missing",
                ["RabbitMq:Producers:0:Publish:QueueName"] = "orders.created"
            })
            .Build();

        var services = new ServiceCollection();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddRabbitMqClientKit(configuration.GetSection("RabbitMq")));

        Assert.Contains("missing", exception.Message);
    }

}
