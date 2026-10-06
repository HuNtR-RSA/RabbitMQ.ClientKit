using System.Collections.Concurrent;
using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit;

/// <summary>
/// Caches and reuses <see cref="RabbitMqClient"/> instances keyed by the full connection configuration.
/// Clients returned from the cache are owned by the cache; callers must not dispose them.
/// Dispose the cache to dispose all cached clients.
/// </summary>
public sealed class RabbitMqClientCache : IAsyncDisposable
{
    private static readonly Func<RabbitMqConnectionOptions, RabbitMqClient> DefaultClientFactory =
        static options => new RabbitMqClient(options);

    private readonly ConcurrentDictionary<string, RabbitMqClient> _clients = new(StringComparer.Ordinal);
    private readonly Func<RabbitMqConnectionOptions, RabbitMqClient> _clientFactory;
    private bool _disposed;

    /// <summary>
    /// Creates a cache that builds clients with the supplied factory, or with
    /// <c>new RabbitMqClient(options)</c> when <paramref name="clientFactory"/> is <see langword="null"/>.
    /// </summary>
    /// <param name="clientFactory">
    /// Optional factory used to create clients for new cache keys.
    /// Pool size and serializer choices belong on the factory (or a factory closure), not on the cache key.
    /// </param>
    public RabbitMqClientCache(Func<RabbitMqConnectionOptions, RabbitMqClient>? clientFactory = null)
    {
        _clientFactory = clientFactory ?? DefaultClientFactory;
    }

    /// <summary>
    /// Gets or creates a <see cref="RabbitMqClient"/> for the specified connection options.
    /// Keyed by the effective broker identity, credentials, and connection settings.
    /// The returned client is owned by this cache; dispose the cache rather than the client.
    /// </summary>
    /// <param name="options">The connection options describing the broker endpoint.</param>
    /// <returns>A cached or newly created <see cref="RabbitMqClient"/> instance.</returns>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="ObjectDisposedException"></exception>
    public RabbitMqClient GetOrCreate(RabbitMqConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ThrowIfDisposed();

        var key = BuildKey(options);
        return _clients.GetOrAdd
        (
            key,
            static (_, state) => state.Factory(state.Options),
            (Factory: _clientFactory, Options: options)
        );
    }

    /// <summary>
    /// Gets or creates a <see cref="RabbitMqClient"/> for the specified URI string.
    /// The returned client is owned by this cache; dispose the cache rather than the client.
    /// </summary>
    /// <param name="uriString">The connection URI string.</param>
    /// <returns>A cached or newly created <see cref="RabbitMqClient"/> instance.</returns>
    public RabbitMqClient GetOrCreate(string uriString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(uriString);
        ThrowIfDisposed();

        return GetOrCreate(new RabbitMqConnectionOptions
        {
            ConnectionUri = uriString
        });
    }

    /// <summary>
    /// Gets or creates a <see cref="RabbitMqClient"/> for the specified URI.
    /// The returned client is owned by this cache; dispose the cache rather than the client.
    /// </summary>
    /// <param name="uri">The connection URI.</param>
    /// <returns>A cached or newly created <see cref="RabbitMqClient"/> instance.</returns>
    public RabbitMqClient GetOrCreate(Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ThrowIfDisposed();

        return GetOrCreate(new RabbitMqConnectionOptions
        {
            ConnectionUri = uri.OriginalString
        });
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        foreach (var client in _clients.Values)
        {
            await client.DisposeAsync().ConfigureAwait(false);
        }

        _clients.Clear();
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RabbitMqClientCache));
        }
    }

    private static string BuildKey(RabbitMqConnectionOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.ConnectionUri) &&
            Uri.TryCreate(options.ConnectionUri, UriKind.Absolute, out var uri))
        {
            return BuildKey(
                BuildUriIdentity(uri),
                options.ClientProvidedName,
                options.AutomaticRecoveryEnabled,
                options.TopologyRecoveryEnabled,
                options.ConsumerDispatchConcurrency,
                options.RequestedHeartbeat,
                options.RequestedConnectionTimeout,
                options.NetworkRecoveryInterval);
        }

        var vhost = string.IsNullOrWhiteSpace(options.VirtualHost) ? "/" : options.VirtualHost;
        return BuildKey(
            $"{options.HostName}:{options.Port}/{vhost}|{options.UserName}|{options.Password}",
            options.ClientProvidedName,
            options.AutomaticRecoveryEnabled,
            options.TopologyRecoveryEnabled,
            options.ConsumerDispatchConcurrency,
            options.RequestedHeartbeat,
            options.RequestedConnectionTimeout,
            options.NetworkRecoveryInterval);
    }

    private static string BuildUriIdentity(Uri uri)
    {
        var port = uri.IsDefaultPort
            ? uri.Scheme switch
            {
                "amqps" => 5671,
                "amqp" => 5672,
                _ => uri.Port
            }
            : uri.Port;

        var virtualHost = uri.AbsolutePath.Trim('/');
        if (string.IsNullOrWhiteSpace(virtualHost))
        {
            virtualHost = "/";
        }

        return $"{uri.Scheme}://{uri.UserInfo}@{uri.Host}:{port}/{virtualHost}{uri.Query}";
    }

    private static string BuildKey(
        string ConnectionIdentity,
        string? ClientProvidedName,
        bool AutomaticRecoveryEnabled,
        bool TopologyRecoveryEnabled,
        ushort ConsumerDispatchConcurrency,
        TimeSpan RequestedHeartbeat,
        TimeSpan RequestedConnectionTimeout,
        TimeSpan NetworkRecoveryInterval)
        => $"{ConnectionIdentity}|{ClientProvidedName}|{AutomaticRecoveryEnabled}|{TopologyRecoveryEnabled}|{ConsumerDispatchConcurrency}|{RequestedHeartbeat.Ticks}|{RequestedConnectionTimeout.Ticks}|{NetworkRecoveryInterval.Ticks}";
}
