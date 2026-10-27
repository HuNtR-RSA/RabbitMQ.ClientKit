using RabbitMQ.ClientKit.Configuration;
using Testcontainers.RabbitMq;
using Xunit;

namespace RabbitMQ.ClientKit.Tests.Support;

[CollectionDefinition(Name)]
public sealed class RabbitMq43ContainerCollection : ICollectionFixture<RabbitMq43ContainerFixture>
{
    public const string Name = "RabbitMQ 4.3 container";
}

public sealed class RabbitMq43ContainerFixture() : RabbitMqQuorumContainerFixture("rabbitmq:4.3-management");

[CollectionDefinition(RabbitMq44AlpineContainerCollection.Name)]
public sealed class RabbitMq44AlpineContainerCollection : ICollectionFixture<RabbitMq44AlpineContainerFixture>
{
    public const string Name = "RabbitMQ 4.4 Alpine container";
}

public sealed class RabbitMq44AlpineContainerFixture() : RabbitMqQuorumContainerFixture("rabbitmq:4.4-alpine");

public abstract class RabbitMqQuorumContainerFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container;

    protected RabbitMqQuorumContainerFixture(string image)
    {
        _container = new RabbitMqBuilder(image).Build();
    }

    public RabbitMqConnectionOptions CreateConnectionOptions(string clientProvidedName, ushort consumerDispatchConcurrency = 1) => new()
    {
        ConnectionUri = _container.GetConnectionString(),
        ClientProvidedName = clientProvidedName,
        ConsumerDispatchConcurrency = consumerDispatchConcurrency
    };

    public async Task<string> DescribeQueuesAsync()
    {
        var result = await _container.ExecAsync(
        [
            "rabbitmqctl",
            "list_queues",
            "name",
            "type",
            "arguments",
            "messages",
            "messages_ready",
            "messages_unacknowledged",
            "consumers"
        ]);
        return $"exit={result.ExitCode}{Environment.NewLine}{result.Stdout}{result.Stderr}";
    }

    public async Task<string> EvalAsync(string expression)
    {
        var result = await _container.ExecAsync(["rabbitmqctl", "eval", expression]);
        return $"exit={result.ExitCode} stdout={result.Stdout} stderr={result.Stderr}";
    }

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}
