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
}
