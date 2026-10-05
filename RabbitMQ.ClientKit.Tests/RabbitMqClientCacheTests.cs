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
    public async Task GetOrCreate_ReturnsDifferentClientsForDifferentCredentials()
    {
        await using var cache = new RabbitMqClientCache();

        var client1 = cache.GetOrCreate(new RabbitMqConnectionOptions
        {
            HostName = "rabbit1.example.com",
            Port = 5672,
            VirtualHost = "vhost1",
            UserName = "guest",
            Password = "guest"
        });

        var client2 = cache.GetOrCreate(new RabbitMqConnectionOptions
        {
            HostName = "rabbit1.example.com",
            Port = 5672,
            VirtualHost = "vhost1",
            UserName = "service-user",
            Password = "service-password"
        });

        Assert.NotSame(client1, client2);
    }

    [Fact]
    public async Task GetOrCreate_UriString_ReturnsSameClientForEquivalentUri()
    {
        await using var cache = new RabbitMqClientCache();
        const string uriString = "amqp://guest:guest@broker.local:5672/myvhost";

        var client1 = cache.GetOrCreate(uriString);
        var client2 = cache.GetOrCreate(new Uri(uriString));

        Assert.Same(client1, client2);
    }

    [Fact]
    public async Task GetOrCreate_DifferentUriCredentials_ReturnDifferentClients()
    {
        await using var cache = new RabbitMqClientCache();

        var client1 = cache.GetOrCreate("amqp://guest:guest@broker.local:5672/myvhost");
        var client2 = cache.GetOrCreate(new Uri("amqp://admin:secret@broker.local:5672/myvhost"));

        Assert.NotSame(client1, client2);
    }

    [Fact]
    public async Task GetOrCreate_DifferentConnectionSettings_ReturnDifferentClients()
    {
        await using var cache = new RabbitMqClientCache();

        var client1 = cache.GetOrCreate(new RabbitMqConnectionOptions
        {
            HostName = "rabbit1.example.com",
            Port = 5672,
            VirtualHost = "vhost1",
            RequestedHeartbeat = TimeSpan.FromSeconds(30)
        });

        var client2 = cache.GetOrCreate(new RabbitMqConnectionOptions
        {
            HostName = "rabbit1.example.com",
            Port = 5672,
            VirtualHost = "vhost1",
            RequestedHeartbeat = TimeSpan.FromSeconds(10)
        });

        Assert.NotSame(client1, client2);
    }
}
