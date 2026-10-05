using System.Collections.Concurrent;
using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit;

/// <summary>
/// Caches and reuses <see cref="RabbitMqClient"/> instances keyed by broker endpoint (host, port, and virtual host).
/// </summary>
public sealed class RabbitMqClientCache : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, RabbitMqClient> _clients = new(StringComparer.OrdinalIgnoreCase);
    private bool _disposed;

    /// <summary>
    /// Gets or creates a <see cref="RabbitMqClient"/> for the specified connection options.
    /// Keyed by host, port, and virtual host.
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
        return GetOrCreate(new Uri(uriString));
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
            ConnectionUri = uri.ToString()
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
            return BuildKey(uri);
        }

        var vhost = string.IsNullOrWhiteSpace(options.VirtualHost) ? "/" : options.VirtualHost;
        return $"{options.HostName}:{options.Port}/{vhost}";
    }

    private static string BuildKey(Uri uri)
    {
        var host = uri.Host;
        var port = uri.Port > 0 ? uri.Port : 5672;
        var path = uri.AbsolutePath.Trim('/');
        var vhost = string.IsNullOrWhiteSpace(path) ? "/" : path;
        return $"{host}:{port}/{vhost}";
    }
}
