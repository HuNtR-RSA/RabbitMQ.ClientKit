using RabbitMQ.ClientKit.Configuration;
using Testcontainers.RabbitMq;
using Xunit;

namespace RabbitMQ.ClientKit.Tests.Support;

[CollectionDefinition(Name)]
public sealed class RabbitMqContainerCollection : ICollectionFixture<RabbitMqContainerFixture>
{
    public const string Name = "RabbitMQ container";
}

public sealed class RabbitMqContainerFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:3.13-management").Build();

    public RabbitMqConnectionOptions CreateConnectionOptions(string clientProvidedName, ushort consumerDispatchConcurrency = 1) => new()
    {
        ConnectionUri = _container.GetConnectionString(),
        ClientProvidedName = clientProvidedName,
        ConsumerDispatchConcurrency = consumerDispatchConcurrency
    };

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}
