using System.Text.Json;
using Microsoft.Extensions.Configuration;
using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit.DynamicConfiguration;

internal sealed class DefaultRabbitMqDynamicClientActivator : IRabbitMqDynamicClientActivator
{
    public RabbitMqClient CreateClient(string? connectionName, RabbitMqConnectionOptions connectionOptions)
    {
        ArgumentNullException.ThrowIfNull(connectionOptions);
        return new RabbitMqClient(connectionOptions);
    }
}

internal sealed class RabbitMqDynamicRuntime(
    IRabbitMqDynamicConfigurationSource configurationSource,
    IRabbitMqDynamicClientActivator clientActivator) : IRabbitMqDynamicEndpointResolver, IAsyncDisposable
{
    private readonly IRabbitMqDynamicConfigurationSource _configurationSource = configurationSource ?? throw new ArgumentNullException(nameof(configurationSource));
    private readonly IRabbitMqDynamicClientActivator _clientActivator = clientActivator ?? throw new ArgumentNullException(nameof(clientActivator));
    private readonly SemaphoreSlim _sync = new(1, 1);
    private readonly Dictionary<string, CachedClient> _clients = new(StringComparer.OrdinalIgnoreCase);
    private RabbitMqDynamicConfigurationSnapshot? _snapshot;
    private bool _disposed;

    public RabbitMqDynamicProducer GetProducer(string name) => new(this, name);

    public RabbitMqDynamicConsumer GetConsumer(string name) => new(this, name);

    public async ValueTask<RabbitMqDynamicConfigurationSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
    {
        await EnsureSnapshotAsync(cancellationToken).ConfigureAwait(false);
        return _snapshot!;
    }

    public async ValueTask RefreshAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var snapshot = await _configurationSource.GetConfigurationAsync(cancellationToken).ConfigureAwait(false);
        ValidateSnapshot(snapshot);

        List<RabbitMqClient> staleClients;
        await _sync.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();

            _snapshot = snapshot;
            staleClients = RemoveStaleClients(snapshot);
        }
        finally
        {
            _sync.Release();
        }

        await DisposeClientsAsync(staleClients).ConfigureAwait(false);
    }

    public async ValueTask<RabbitMqClient> GetClientAsync(string? connectionName = null, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await EnsureSnapshotAsync(cancellationToken).ConfigureAwait(false);

        RabbitMqClient? staleClient = null;
        RabbitMqClient resolvedClient;
        await _sync.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();

            var snapshot = _snapshot!;
            var cacheKey = NormalizeConnectionName(connectionName);
            var connectionOptions = ResolveConnectionOptions(snapshot, connectionName);
            var fingerprint = ComputeFingerprint(connectionOptions);

            if (_clients.TryGetValue(cacheKey, out var cached) && cached.Fingerprint == fingerprint)
            {
                return cached.Client;
            }

            var client = _clientActivator.CreateClient(connectionName, connectionOptions);

            if (cached is not null)
            {
                staleClient = cached.Client;
            }

            _clients[cacheKey] = new CachedClient(fingerprint, client);
            resolvedClient = client;
        }
        finally
        {
            _sync.Release();
        }

        if (staleClient is not null)
        {
            await staleClient.DisposeAsync().ConfigureAwait(false);
        }

        return resolvedClient;
    }

    public async ValueTask<RabbitMqProducerRegistration> GetProducerRegistrationAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var snapshot = await GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        return snapshot.Producers.TryGetValue(name, out var registration)
            ? registration
            : throw new KeyNotFoundException($"No dynamic RabbitMQ producer named '{name}' is configured.");
    }

    public async ValueTask<RabbitMqConsumerRegistration> GetConsumerRegistrationAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var snapshot = await GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        return snapshot.Consumers.TryGetValue(name, out var registration)
            ? registration
            : throw new KeyNotFoundException($"No dynamic RabbitMQ consumer named '{name}' is configured.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        List<RabbitMqClient> clients;
        await _sync.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            clients = _clients.Values.Select(static x => x.Client).ToList();
            _clients.Clear();
            _snapshot = null;
        }
        finally
        {
            _sync.Release();
            _sync.Dispose();
        }

        await DisposeClientsAsync(clients).ConfigureAwait(false);
    }

    private async Task EnsureSnapshotAsync(CancellationToken cancellationToken)
    {
        if (_snapshot is not null)
        {
            return;
        }

        await RefreshAsync(cancellationToken).ConfigureAwait(false);
    }

    private List<RabbitMqClient> RemoveStaleClients(RabbitMqDynamicConfigurationSnapshot snapshot)
    {
        var activeFingerprints = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (snapshot.DefaultConnection is not null)
        {
            activeFingerprints[string.Empty] = ComputeFingerprint(snapshot.DefaultConnection);
        }

        foreach (var connection in snapshot.NamedConnections)
        {
            activeFingerprints[connection.Key] = ComputeFingerprint(connection.Value);
        }

        var staleClients = new List<RabbitMqClient>();
        foreach (var pair in _clients.ToArray())
        {
            if (!activeFingerprints.TryGetValue(pair.Key, out var fingerprint) || pair.Value.Fingerprint != fingerprint)
            {
                staleClients.Add(pair.Value.Client);
                _clients.Remove(pair.Key);
            }
        }

        return staleClients;
    }

    private static RabbitMqConnectionOptions ResolveConnectionOptions(
        RabbitMqDynamicConfigurationSnapshot snapshot,
        string? connectionName)
    {
        if (string.IsNullOrWhiteSpace(connectionName))
        {
            return snapshot.DefaultConnection
                ?? throw new InvalidOperationException("The dynamic RabbitMQ configuration does not define a default connection.");
        }

        return snapshot.NamedConnections.TryGetValue(connectionName, out var connectionOptions)
            ? connectionOptions
            : throw new KeyNotFoundException($"No dynamic RabbitMQ connection named '{connectionName}' is configured.");
    }

    private static string NormalizeConnectionName(string? connectionName) =>
        string.IsNullOrWhiteSpace(connectionName) ? string.Empty : connectionName;

    private static string ComputeFingerprint(RabbitMqConnectionOptions connectionOptions) =>
        JsonSerializer.Serialize(connectionOptions);

    private static async Task DisposeClientsAsync(IEnumerable<RabbitMqClient> clients)
    {
        foreach (var client in clients)
        {
            await client.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static void ValidateSnapshot(RabbitMqDynamicConfigurationSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        foreach (var producer in snapshot.Producers.Values)
        {
            if (string.IsNullOrWhiteSpace(producer.ConnectionName))
            {
                if (snapshot.DefaultConnection is null)
                {
                    throw new InvalidOperationException($"The dynamic RabbitMQ producer '{producer.Name}' references the default connection, but none is configured.");
                }

                continue;
            }

            if (!snapshot.NamedConnections.ContainsKey(producer.ConnectionName))
            {
                throw new InvalidOperationException($"The dynamic RabbitMQ producer '{producer.Name}' references unknown connection '{producer.ConnectionName}'.");
            }
        }

        foreach (var consumer in snapshot.Consumers.Values)
        {
            if (string.IsNullOrWhiteSpace(consumer.ConnectionName))
            {
                if (snapshot.DefaultConnection is null)
                {
                    throw new InvalidOperationException($"The dynamic RabbitMQ consumer '{consumer.Name}' references the default connection, but none is configured.");
                }

                continue;
            }

            if (!snapshot.NamedConnections.ContainsKey(consumer.ConnectionName))
            {
                throw new InvalidOperationException($"The dynamic RabbitMQ consumer '{consumer.Name}' references unknown connection '{consumer.ConnectionName}'.");
            }
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, nameof(RabbitMqDynamicRuntime));

    private sealed record CachedClient(string Fingerprint, RabbitMqClient Client);
}

internal sealed class ConfigurationSectionRabbitMqDynamicConfigurationSource(IConfigurationSection configurationSection) : IRabbitMqDynamicConfigurationSource
{
    private readonly IConfigurationSection _configurationSection = configurationSection ?? throw new ArgumentNullException(nameof(configurationSection));

    public ValueTask<RabbitMqDynamicConfigurationSnapshot> GetConfigurationAsync(CancellationToken cancellationToken = default)
    {
        var configuration = _configurationSection.Get<RabbitMqDynamicConfigurationModel>() ?? new RabbitMqDynamicConfigurationModel();
        var defaultConnection = configuration.DefaultConnection?.ToOptions();
        var namedConnections = new Dictionary<string, RabbitMqConnectionOptions>(StringComparer.OrdinalIgnoreCase);
        var producers = new Dictionary<string, RabbitMqProducerRegistration>(StringComparer.OrdinalIgnoreCase);
        var consumers = new Dictionary<string, RabbitMqConsumerRegistration>(StringComparer.OrdinalIgnoreCase);

        foreach (var connection in configuration.NamedConnections)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(connection.Name);

            if (!namedConnections.TryAdd(connection.Name, connection.ToOptions()))
            {
                throw new InvalidOperationException($"The dynamic RabbitMQ connection '{connection.Name}' is configured more than once.");
            }
        }

        foreach (var producer in configuration.Producers)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(producer.Name);

            var registration = new RabbitMqProducerRegistration(producer.Name, producer.ConnectionName, producer.Publish.ToOptions());
            if (!producers.TryAdd(registration.Name, registration))
            {
                throw new InvalidOperationException($"The dynamic RabbitMQ producer '{registration.Name}' is configured more than once.");
            }
        }

        foreach (var consumer in configuration.Consumers)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(consumer.Name);

            var registration = new RabbitMqConsumerRegistration(consumer.Name, consumer.ConnectionName, consumer.Consume.ToOptions());
            if (!consumers.TryAdd(registration.Name, registration))
            {
                throw new InvalidOperationException($"The dynamic RabbitMQ consumer '{registration.Name}' is configured more than once.");
            }
        }

        return ValueTask.FromResult(new RabbitMqDynamicConfigurationSnapshot(defaultConnection, namedConnections, producers, consumers));
    }
}

internal sealed class RabbitMqDynamicConfigurationModel
{
    public RabbitMqConnectionModel? DefaultConnection { get; set; }

    public List<RabbitMqNamedConnectionModel> NamedConnections { get; set; } = [];

    public List<RabbitMqProducerModel> Producers { get; set; } = [];

    public List<RabbitMqConsumerModel> Consumers { get; set; } = [];
}

internal class RabbitMqConnectionModel
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

internal sealed class RabbitMqNamedConnectionModel : RabbitMqConnectionModel
{
    public string Name { get; set; } = string.Empty;
}

internal sealed class RabbitMqProducerModel
{
    public string Name { get; set; } = string.Empty;

    public string? ConnectionName { get; set; }

    public RabbitMqPublishOptionsModel Publish { get; set; } = new();
}

internal sealed class RabbitMqConsumerModel
{
    public string Name { get; set; } = string.Empty;

    public string? ConnectionName { get; set; }

    public RabbitMqConsumerOptionsModel Consume { get; set; } = new();
}

internal sealed class RabbitMqPublishOptionsModel
{
    public string ExchangeName { get; set; } = string.Empty;

    public string? RoutingKey { get; set; }

    public string? QueueName { get; set; }

    public bool Mandatory { get; set; }

    public RabbitMqMessagePropertiesModel? Properties { get; set; }

    public RabbitMqTopologyOptionsModel? Topology { get; set; }

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

internal sealed class RabbitMqConsumerOptionsModel
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

    public RabbitMqTopologyOptionsModel? Topology { get; set; }

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

internal sealed class RabbitMqTopologyOptionsModel
{
    public RabbitMqExchangeOptionsModel? Exchange { get; set; }

    public RabbitMqQueueOptionsModel? Queue { get; set; }

    public List<RabbitMqQueueBindingOptionsModel> Bindings { get; set; } = [];

    public RabbitMqTopologyOptions ToOptions() => new()
    {
        Exchange = Exchange?.ToOptions(),
        Queue = Queue?.ToOptions(),
        Bindings = Bindings.Select(static x => x.ToOptions()).ToArray()
    };
}

internal sealed class RabbitMqExchangeOptionsModel
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

internal sealed class RabbitMqQueueOptionsModel
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

internal sealed class RabbitMqQueueBindingOptionsModel
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

internal sealed class RabbitMqMessagePropertiesModel
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
