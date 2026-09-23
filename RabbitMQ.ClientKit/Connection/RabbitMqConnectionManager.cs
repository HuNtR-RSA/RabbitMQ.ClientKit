using RabbitMQ.Client;
using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit.Connection;

/// <summary>
/// Creates and reuses a single RabbitMQ connection for channel creation.
/// </summary>
public sealed class RabbitMqConnectionManager(RabbitMqConnectionOptions options) : IRabbitMqConnectionManager
{
    private readonly RabbitMqConnectionOptions _options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly SemaphoreSlim _sync = new(1, 1);
    private IConnection? _connection;
    private bool _disposed;

    /// <inheritdoc />
    public async ValueTask<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await _sync.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();

            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            if (_connection is not null)
            {
                await _connection.DisposeAsync().ConfigureAwait(false);
                _connection = null;
            }

            var factory = CreateFactory();
            _connection = await factory.CreateConnectionAsync(_options.ClientProvidedName, cancellationToken).ConfigureAwait(false);
            return _connection;
        }
        finally
        {
            _sync.Release();
        }
    }

    /// <inheritdoc />
    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
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
            if (_connection is not null)
            {
                await _connection.DisposeAsync().ConfigureAwait(false);
                _connection = null;
            }
        }
        finally
        {
            _sync.Release();
            _sync.Dispose();
        }
    }

    private ConnectionFactory CreateFactory()
    {
        var factory = new ConnectionFactory
        {
            AutomaticRecoveryEnabled = _options.AutomaticRecoveryEnabled,
            ConsumerDispatchConcurrency = _options.ConsumerDispatchConcurrency,
            NetworkRecoveryInterval = _options.NetworkRecoveryInterval,
            RequestedConnectionTimeout = _options.RequestedConnectionTimeout,
            RequestedHeartbeat = _options.RequestedHeartbeat,
            TopologyRecoveryEnabled = _options.TopologyRecoveryEnabled
        };

        if (!string.IsNullOrWhiteSpace(_options.ConnectionUri))
        {
            factory.Uri = new Uri(_options.ConnectionUri, UriKind.Absolute);
            return factory;
        }

        factory.HostName = _options.HostName;
        factory.Port = _options.Port;
        factory.UserName = _options.UserName;
        factory.Password = _options.Password;
        factory.VirtualHost = _options.VirtualHost;

        return factory;
    }

    private void ThrowIfDisposed()
        => ObjectDisposedException.ThrowIf(_disposed, nameof(RabbitMqConnectionManager));
}
