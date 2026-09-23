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
}
