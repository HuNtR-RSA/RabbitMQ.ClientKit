using RabbitMQ.ClientKit.ChannelPooling;
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
    public async Task GetOrCreate_DefaultCache_ReturnsSameClientForSameUri_AndDifferentForDifferentUris()
    {
        await using var cache = new RabbitMqClientCache();

        var client1 = cache.GetOrCreate("amqp://guest:guest@broker-a.local:5672/vhost");
        var client2 = cache.GetOrCreate("amqp://guest:guest@broker-a.local:5672/vhost");
        var client3 = cache.GetOrCreate("amqp://guest:guest@broker-b.local:5672/vhost");

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

    [Fact]
    public async Task GetOrCreate_CustomFactory_InvokedOncePerKey_AndReturnsFactoryInstance()
    {
        var created = new List<RabbitMqClient>();
        await using var cache = new RabbitMqClientCache(options =>
        {
            var client = new RabbitMqClient(options);
            created.Add(client);
            return client;
        });

        var options = new RabbitMqConnectionOptions
        {
            HostName = "rabbit1.example.com",
            Port = 5672,
            VirtualHost = "vhost1"
        };

        var client1 = cache.GetOrCreate(options);
        var client2 = cache.GetOrCreate(options);

        Assert.Single(created);
        Assert.Same(created[0], client1);
        Assert.Same(client1, client2);
    }

    [Fact]
    public async Task GetOrCreate_StringUriAndOptionsOverloads_AllUseCustomFactory()
    {
        var invocationCount = 0;
        await using var cache = new RabbitMqClientCache(options =>
        {
            Interlocked.Increment(ref invocationCount);
            return new RabbitMqClient(options);
        });

        const string uriString = "amqp://guest:guest@broker.local:5672/myvhost";
        var fromString = cache.GetOrCreate(uriString);
        var fromUri = cache.GetOrCreate(new Uri(uriString));
        var fromOptions = cache.GetOrCreate(new RabbitMqConnectionOptions
        {
            ConnectionUri = uriString
        });

        Assert.Equal(1, invocationCount);
        Assert.Same(fromString, fromUri);
        Assert.Same(fromString, fromOptions);
    }

    [Fact]
    public async Task CreateCache_ReturnsSameClientForSameUri_AndNewClientForNewUri()
    {
        await using var cache = PooledRabbitMqClientFactory.CreateCache();

        var client1 = cache.GetOrCreate("amqp://guest:guest@broker-a.local:5672/vhost");
        var client2 = cache.GetOrCreate("amqp://guest:guest@broker-a.local:5672/vhost");
        var client3 = cache.GetOrCreate("amqp://guest:guest@broker-b.local:5672/vhost");

        Assert.Same(client1, client2);
        Assert.NotSame(client1, client3);
    }

    [Fact]
    public async Task GetOrCreate_AfterDisposeAsync_ThrowsObjectDisposedException()
    {
        var cache = new RabbitMqClientCache();
        await cache.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() =>
            cache.GetOrCreate("amqp://guest:guest@broker.local:5672/vhost"));
    }
}
