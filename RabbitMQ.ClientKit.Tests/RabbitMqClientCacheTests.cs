using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit.Tests;

public sealed class RabbitMqClientCacheTests
{
    [Fact]
    public async Task GetOrCreate_ReturnsSameClientForSameHostPortAndVHost()
    {
        await using var cache = new RabbitMqClientCache();

        var client1 = cache.GetOrCreate(new RabbitMqConnectionOptions
        {
            HostName = "rabbit1.example.com",
            Port = 5672,
            VirtualHost = "vhost1"
        });

        var client2 = cache.GetOrCreate(new RabbitMqConnectionOptions
        {
            HostName = "rabbit1.example.com",
            Port = 5672,
            VirtualHost = "vhost1"
        });

        var client3 = cache.GetOrCreate(new RabbitMqConnectionOptions
        {
            HostName = "rabbit1.example.com",
            Port = 5672,
            VirtualHost = "vhost2"
        });

        Assert.Same(client1, client2);
        Assert.NotSame(client1, client3);
    }

    [Fact]
    public async Task GetOrCreate_UriString_ReturnsSameClientForEquivalentUri()
    {
        await using var cache = new RabbitMqClientCache();

        var client1 = cache.GetOrCreate("amqp://guest:guest@broker.local:5672/myvhost");
        var client2 = cache.GetOrCreate(new Uri("amqp://admin:secret@broker.local:5672/myvhost"));

        Assert.Same(client1, client2);
    }
}
