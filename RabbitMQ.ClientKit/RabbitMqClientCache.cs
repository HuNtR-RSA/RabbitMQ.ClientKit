using System.Collections.Concurrent;
using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit;

/// <summary>
/// Caches and reuses <see cref="RabbitMqClient"/> instances keyed by the full connection configuration.
/// </summary>
public sealed class RabbitMqClientCache : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, RabbitMqClient> _clients = new(StringComparer.Ordinal);
    private bool _disposed;

    /// <summary>
    /// Gets or creates a <see cref="RabbitMqClient"/> for the specified connection options.
    /// Keyed by the effective broker identity, credentials, and connection settings.
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
        return _clients.GetOrAdd(key, static (_, opt) => new RabbitMqClient(opt), options);
    }

    /// <summary>
    /// Gets or creates a <see cref="RabbitMqClient"/> for the specified URI string.
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
