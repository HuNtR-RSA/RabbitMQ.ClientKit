using System.Collections.Concurrent;
using RabbitMQ.Client;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Connection;

namespace RabbitMQ.ClientKit.ChannelPooling;

/// <summary>
/// Reuses a bounded set of producer channels for publish-heavy workloads.
/// </summary>
public sealed class PooledProducerChannelProvider : IRabbitMqProducerChannelProvider
{
    private readonly IRabbitMqConnectionManager _connectionManager;
    private readonly ConcurrentQueue<IChannel> _availableChannels = new();
    private readonly SemaphoreSlim _availabilitySignal = new(0);
    private readonly int _poolSize;
    private int _createdChannels;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledProducerChannelProvider" /> class.
    /// </summary>
    /// <param name="connectionManager">The connection manager used to create channels.</param>
    /// <param name="options">The pooling options that define the pool size.</param>
    public PooledProducerChannelProvider(
        IRabbitMqConnectionManager connectionManager,
        RabbitMqChannelPoolingOptions? options = null)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _poolSize = options?.ProducerPoolSize ?? 8;

        if (_poolSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "ProducerPoolSize must be greater than zero.");
        }
    }

    /// <inheritdoc />
    public async ValueTask<IRabbitMqChannelLease> RentAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        while (true)
        {
            while (_availableChannels.TryDequeue(out var channel))
            {
                if (channel.IsOpen)
                {
                    return new PooledProducerChannelLease(this, channel);
                }

                Interlocked.Decrement(ref _createdChannels);
                await channel.DisposeAsync().ConfigureAwait(false);
            }

            if (TryReserveChannelSlot())
            {
                try
                {
                    var createdChannel = await _connectionManager.CreateChannelAsync(cancellationToken).ConfigureAwait(false);
                    return new PooledProducerChannelLease(this, createdChannel);
                }
                catch
                {
                    Interlocked.Decrement(ref _createdChannels);
                    throw;
                }
            }

            await _availabilitySignal.WaitAsync(cancellationToken).ConfigureAwait(false);
            ThrowIfDisposed();
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
        _availabilitySignal.Release(_poolSize);

        while (_availableChannels.TryDequeue(out var channel))
        {
            await channel.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async ValueTask ReturnAsync(IChannel channel)
    {
        if (_disposed || !channel.IsOpen)
        {
            Interlocked.Decrement(ref _createdChannels);
            await channel.DisposeAsync().ConfigureAwait(false);
            return;
        }

        _availableChannels.Enqueue(channel);
        _availabilitySignal.Release();
    }

    private bool TryReserveChannelSlot()
    {
        while (true)
        {
            var current = Volatile.Read(ref _createdChannels);
            if (current >= _poolSize)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _createdChannels, current + 1, current) == current)
            {
                return true;
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(PooledProducerChannelProvider));
        }
    }

    private sealed class PooledProducerChannelLease(PooledProducerChannelProvider owner, IChannel channel) : IRabbitMqChannelLease
    {
        private readonly PooledProducerChannelProvider _owner = owner;
        private IChannel? _channel = channel;

        public IChannel Channel => _channel ?? throw new ObjectDisposedException(nameof(PooledProducerChannelLease));

        public async ValueTask DisposeAsync()
        {
            var channel = Interlocked.Exchange(ref _channel, null);
            if (channel is null)
            {
                return;
            }

            await _owner.ReturnAsync(channel).ConfigureAwait(false);
        }
    }
}
