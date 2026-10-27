using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit.Connection;

/// <summary>
/// Creates and reuses a single AMQP 1.0 connection for delayed-retry consumers.
/// </summary>
public sealed class RabbitMqAmqp10ConnectionManager(RabbitMqConnectionOptions options) : IRabbitMqAmqp10ConnectionManager
{
    private readonly RabbitMqConnectionOptions _options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly SemaphoreSlim _sync = new(1, 1);
    private IEnvironment? _environment;
    private IConnection? _connection;
    private bool _disposed;

    /// <inheritdoc />
    public async ValueTask<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (_connection is { State: State.Open })
        {
            return _connection;
        }

        await _sync.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();

            if (_connection is { State: State.Open })
            {
                return _connection;
            }

            await CloseConnectionAsync().ConfigureAwait(false);

            _environment ??= AmqpEnvironment.Create(CreateConnectionSettings());
            _connection = await _environment.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
            return _connection;
        }
        finally
        {
            _sync.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await _sync.WaitAsync().ConfigureAwait(false);
        try
        {
            await CloseConnectionAsync().ConfigureAwait(false);

            if (_environment is not null)
            {
                await _environment.CloseAsync().ConfigureAwait(false);
                _environment = null;
            }
        }
        finally
        {
            _sync.Release();
            _sync.Dispose();
        }
    }

    private ConnectionSettings CreateConnectionSettings()
    {
        var recovery = new RecoveryConfiguration()
            .Activated(_options.AutomaticRecoveryEnabled)
            .Topology(_options.TopologyRecoveryEnabled);

        var containerId = string.IsNullOrWhiteSpace(_options.ClientProvidedName)
            ? "RabbitMQ.ClientKit.AMQP10"
            : $"{_options.ClientProvidedName}.AMQP10";

        var builder = ConnectionSettingsBuilder.Create()
            .ContainerId(containerId)
            .SaslMechanism(SaslMechanism.Plain)
            .RecoveryConfiguration(recovery);

        if (!string.IsNullOrWhiteSpace(_options.ConnectionUri))
        {
            return builder
                .Uri(new Uri(_options.ConnectionUri, UriKind.Absolute))
                .Build();
        }

        return builder
            .Host(_options.HostName)
            .Port(_options.Port)
            .User(_options.UserName)
            .Password(_options.Password)
            .VirtualHost(_options.VirtualHost)
            .Build();
    }

    private async Task CloseConnectionAsync()
    {
        if (_connection is null)
        {
            return;
        }

        try
        {
            await _connection.CloseAsync().ConfigureAwait(false);
        }
        finally
        {
            _connection.Dispose();
            _connection = null;
        }
    }

    private void ThrowIfDisposed()
        => ObjectDisposedException.ThrowIf(_disposed, nameof(RabbitMqAmqp10ConnectionManager));
}
