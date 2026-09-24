using Microsoft.Extensions.DependencyInjection;

namespace RabbitMQ.ClientKit.Configuration;

internal sealed class RabbitMqConfigurationRegistry : IRabbitMqConfigurationRegistry
{
    private readonly IReadOnlyDictionary<string, RabbitMqProducerRegistration> _producers;
    private readonly IReadOnlyDictionary<string, RabbitMqConsumerRegistration> _consumers;

    public RabbitMqConfigurationRegistry(
        IReadOnlyDictionary<string, RabbitMqProducerRegistration> producers,
        IReadOnlyDictionary<string, RabbitMqConsumerRegistration> consumers)
    {
        _producers = producers ?? throw new ArgumentNullException(nameof(producers));
        _consumers = consumers ?? throw new ArgumentNullException(nameof(consumers));
    }

    public IReadOnlyCollection<RabbitMqProducerRegistration> Producers => _producers.Values.ToArray();

    public IReadOnlyCollection<RabbitMqConsumerRegistration> Consumers => _consumers.Values.ToArray();

    public RabbitMqProducerRegistration GetRequiredProducer(string name) =>
        _producers.TryGetValue(name, out var producer)
            ? producer
            : throw new KeyNotFoundException($"No RabbitMQ producer named '{name}' is configured.");

    public RabbitMqConsumerRegistration GetRequiredConsumer(string name) =>
        _consumers.TryGetValue(name, out var consumer)
            ? consumer
            : throw new KeyNotFoundException($"No RabbitMQ consumer named '{name}' is configured.");

    public bool TryGetProducer(string name, out RabbitMqProducerRegistration? producer) =>
        _producers.TryGetValue(name, out producer);

    public bool TryGetConsumer(string name, out RabbitMqConsumerRegistration? consumer) =>
        _consumers.TryGetValue(name, out consumer);
}

internal sealed class RabbitMqEndpointResolver(
    IServiceProvider serviceProvider,
    IRabbitMqConfigurationRegistry configurationRegistry) : IRabbitMqEndpointResolver
{
    private readonly IServiceProvider _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private readonly IRabbitMqConfigurationRegistry _configurationRegistry = configurationRegistry ?? throw new ArgumentNullException(nameof(configurationRegistry));

    public RabbitMqClient GetClient(string? connectionName = null) =>
        Resolve<RabbitMqClient>(connectionName);

    public Publishing.RabbitMqPublisher GetPublisher(string? connectionName = null) =>
        Resolve<Publishing.RabbitMqPublisher>(connectionName);

    public Consuming.RabbitMqConsumer GetConsumer(string? connectionName = null) =>
        Resolve<Consuming.RabbitMqConsumer>(connectionName);

    public RabbitMqConfiguredProducer GetRequiredProducer(string name)
    {
        var registration = _configurationRegistry.GetRequiredProducer(name);
        return new RabbitMqConfiguredProducer(
            registration,
            GetPublisher(registration.ConnectionName),
            GetClient(registration.ConnectionName));
    }

    public RabbitMqConfiguredConsumer GetRequiredConsumer(string name)
    {
        var registration = _configurationRegistry.GetRequiredConsumer(name);
        return new RabbitMqConfiguredConsumer(
            registration,
            GetConsumer(registration.ConnectionName),
            GetClient(registration.ConnectionName));
    }

    private T Resolve<T>(string? connectionName) where T : notnull =>
        string.IsNullOrWhiteSpace(connectionName)
            ? _serviceProvider.GetRequiredService<T>()
            : _serviceProvider.GetRequiredKeyedService<T>(connectionName);
}

internal sealed class RabbitMqClientKitConfiguration
{
    public RabbitMqConnectionConfiguration? Connection { get; set; }

    public List<RabbitMqNamedConnectionConfiguration> Connections { get; set; } = [];

    public List<RabbitMqProducerConfiguration> Producers { get; set; } = [];

    public List<RabbitMqConsumerConfiguration> Consumers { get; set; } = [];
}

internal class RabbitMqConnectionConfiguration
{
    public string? ConnectionUri { get; set; }

    public string HostName { get; set; } = "localhost";

    public int Port { get; set; } = 5672;

    public string UserName { get; set; } = "guest";

    public string Password { get; set; } = "guest";

    public string VirtualHost { get; set; } = "/";

    public string? ClientProvidedName { get; set; }

    public bool AutomaticRecoveryEnabled { get; set; } = true;

    public bool TopologyRecoveryEnabled { get; set; } = true;

    public ushort ConsumerDispatchConcurrency { get; set; } = 1;

    public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(60);

    public TimeSpan RequestedConnectionTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(10);

    public RabbitMqConnectionOptions ToOptions() => new()
    {
        ConnectionUri = ConnectionUri,
        HostName = HostName,
        Port = Port,
        UserName = UserName,
        Password = Password,
        VirtualHost = VirtualHost,
        ClientProvidedName = ClientProvidedName,
        AutomaticRecoveryEnabled = AutomaticRecoveryEnabled,
        TopologyRecoveryEnabled = TopologyRecoveryEnabled,
        ConsumerDispatchConcurrency = ConsumerDispatchConcurrency,
        RequestedHeartbeat = RequestedHeartbeat,
        RequestedConnectionTimeout = RequestedConnectionTimeout,
        NetworkRecoveryInterval = NetworkRecoveryInterval
    };
}

internal sealed class RabbitMqNamedConnectionConfiguration : RabbitMqConnectionConfiguration
{
    public string Name { get; set; } = string.Empty;
}

internal sealed class RabbitMqProducerConfiguration
{
    public string Name { get; set; } = string.Empty;

    public string? ConnectionName { get; set; }

    public RabbitMqPublishOptionsConfiguration Publish { get; set; } = new();
}

internal sealed class RabbitMqConsumerConfiguration
{
    public string Name { get; set; } = string.Empty;

    public string? ConnectionName { get; set; }

    public RabbitMqConsumerOptionsConfiguration Consume { get; set; } = new();
}

internal sealed class RabbitMqPublishOptionsConfiguration
{
    public string ExchangeName { get; set; } = string.Empty;

    public string? RoutingKey { get; set; }

    public string? QueueName { get; set; }

    public bool Mandatory { get; set; }

    public RabbitMqMessagePropertiesConfiguration? Properties { get; set; }

    public RabbitMqTopologyOptionsConfiguration? Topology { get; set; }

    public RabbitMqPublishOptions ToOptions() => new()
    {
        ExchangeName = ExchangeName,
        RoutingKey = RoutingKey,
        QueueName = QueueName,
        Mandatory = Mandatory,
        Properties = Properties?.ToOptions(),
        Topology = Topology?.ToOptions()
    };
}

internal sealed class RabbitMqConsumerOptionsConfiguration
{
    public string QueueName { get; set; } = string.Empty;

    public string? ConsumerName { get; set; }

    public string ConsumerTag { get; set; } = string.Empty;

    public bool AutoAck { get; set; }

    public ushort PrefetchCount { get; set; } = 1;

    public bool GlobalPrefetch { get; set; }

    public bool Exclusive { get; set; }

    public bool NoLocal { get; set; }

    public bool RequeueOnFailure { get; set; } = true;

    public Dictionary<string, object?>? Arguments { get; set; }

    public RabbitMqTopologyOptionsConfiguration? Topology { get; set; }

    public RabbitMqConsumerOptions ToOptions() => new()
    {
        QueueName = QueueName,
        ConsumerName = ConsumerName,
        ConsumerTag = ConsumerTag,
        AutoAck = AutoAck,
        PrefetchCount = PrefetchCount,
        GlobalPrefetch = GlobalPrefetch,
        Exclusive = Exclusive,
        NoLocal = NoLocal,
        RequeueOnFailure = RequeueOnFailure,
        Arguments = Arguments,
        Topology = Topology?.ToOptions()
    };
}

internal sealed class RabbitMqTopologyOptionsConfiguration
{
    public RabbitMqExchangeOptionsConfiguration? Exchange { get; set; }

    public RabbitMqQueueOptionsConfiguration? Queue { get; set; }

    public List<RabbitMqQueueBindingOptionsConfiguration> Bindings { get; set; } = [];

    public RabbitMqTopologyOptions ToOptions() => new()
    {
        Exchange = Exchange?.ToOptions(),
        Queue = Queue?.ToOptions(),
        Bindings = Bindings.Select(static binding => binding.ToOptions()).ToArray()
    };
}

internal sealed class RabbitMqExchangeOptionsConfiguration
{
    public string Name { get; set; } = string.Empty;

    public string Type { get; set; } = RabbitMQ.Client.ExchangeType.Direct;

    public bool Durable { get; set; } = true;

    public bool AutoDelete { get; set; }

    public Dictionary<string, object?>? Arguments { get; set; }

    public RabbitMqExchangeOptions ToOptions() => new()
    {
        Name = Name,
        Type = Type,
        Durable = Durable,
        AutoDelete = AutoDelete,
        Arguments = Arguments
    };
}

internal sealed class RabbitMqQueueOptionsConfiguration
{
    public string Name { get; set; } = string.Empty;

    public bool Durable { get; set; } = true;

    public bool Exclusive { get; set; }

    public bool AutoDelete { get; set; }

    public Dictionary<string, object?>? Arguments { get; set; }

    public RabbitMqQueueOptions ToOptions() => new()
    {
        Name = Name,
        Durable = Durable,
        Exclusive = Exclusive,
        AutoDelete = AutoDelete,
        Arguments = Arguments
    };
}

internal sealed class RabbitMqQueueBindingOptionsConfiguration
{
    public string QueueName { get; set; } = string.Empty;

    public string ExchangeName { get; set; } = string.Empty;

    public string RoutingKey { get; set; } = string.Empty;

    public Dictionary<string, object?>? Arguments { get; set; }

    public RabbitMqQueueBindingOptions ToOptions() => new()
    {
        QueueName = QueueName,
        ExchangeName = ExchangeName,
        RoutingKey = RoutingKey,
        Arguments = Arguments
    };
}

internal sealed class RabbitMqMessagePropertiesConfiguration
{
    public string? AppId { get; set; }

    public string? ContentEncoding { get; set; }

    public string? ContentType { get; set; }

    public string? CorrelationId { get; set; }

    public string? Expiration { get; set; }

    public Dictionary<string, object?>? Headers { get; set; }

    public string? MessageId { get; set; }

    public bool Persistent { get; set; } = true;

    public byte? Priority { get; set; }

    public string? ReplyTo { get; set; }

    public DateTimeOffset? TimestampUtc { get; set; }

    public string? Type { get; set; }

    public string? UserId { get; set; }

    public RabbitMqMessageProperties ToOptions() => new()
    {
        AppId = AppId,
        ContentEncoding = ContentEncoding,
        ContentType = ContentType,
        CorrelationId = CorrelationId,
        Expiration = Expiration,
        Headers = Headers,
        MessageId = MessageId,
        Persistent = Persistent,
        Priority = Priority,
        ReplyTo = ReplyTo,
        TimestampUtc = TimestampUtc,
        Type = Type,
        UserId = UserId
    };
}
