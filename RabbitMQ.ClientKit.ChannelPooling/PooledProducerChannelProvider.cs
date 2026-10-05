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
    private readonly ConcurrentDictionary<ChannelPoolKey, ConcurrentQueue<IChannel>> _availableChannels = new();
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
    public async ValueTask<IRabbitMqChannelLease> RentAsync
    (
        bool publisherConfirmationsEnabled = false,
        TimeSpan? confirmTimeout = null,
        CancellationToken cancellationToken = default
    )
    {
        ThrowIfDisposed();
        var poolKey = ChannelPoolKey.Create(publisherConfirmationsEnabled, confirmTimeout);

        while (true)
        {
            var availableChannels = _availableChannels.GetOrAdd(poolKey, static _ => new ConcurrentQueue<IChannel>());
            while (availableChannels.TryDequeue(out var channel))
            {
                if (channel.IsOpen)
                {
                    return new PooledProducerChannelLease(this, channel, poolKey);
                }

                Interlocked.Decrement(ref _createdChannels);
                await channel.DisposeAsync().ConfigureAwait(false);
            }

            if (TryReserveChannelSlot())
            {
                try
                {
                    var options = RabbitMqChannelOptionsFactory.CreateChannelOptions(publisherConfirmationsEnabled, confirmTimeout);
                    var createdChannel = await _connectionManager.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);
                    RabbitMqChannelOptionsFactory.ConfigureChannel(createdChannel, publisherConfirmationsEnabled, confirmTimeout);
                    return new PooledProducerChannelLease(this, createdChannel, poolKey);
                }
                catch
                {
                    Interlocked.Decrement(ref _createdChannels);
                    throw;
                }
            }

            if (await TryReclaimChannelSlotAsync(poolKey).ConfigureAwait(false))
            {
                continue;
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

        foreach (var queue in _availableChannels.Values)
        {
            while (queue.TryDequeue(out var channel))
            {
                await channel.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async ValueTask ReturnAsync(IChannel channel, ChannelPoolKey poolKey)
    {
        if (_disposed || !channel.IsOpen)
        {
            Interlocked.Decrement(ref _createdChannels);
            await channel.DisposeAsync().ConfigureAwait(false);
            return;
        }

        var availableChannels = _availableChannels.GetOrAdd(poolKey, static _ => new ConcurrentQueue<IChannel>());
        availableChannels.Enqueue(channel);
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

    private async ValueTask<bool> TryReclaimChannelSlotAsync(ChannelPoolKey requestedPoolKey)
    {
        foreach (var pair in _availableChannels)
        {
            if (pair.Key == requestedPoolKey)
            {
                continue;
            }

            while (pair.Value.TryDequeue(out var channel))
            {
                Interlocked.Decrement(ref _createdChannels);
                await channel.DisposeAsync().ConfigureAwait(false);
                return true;
            }
        }

        return false;
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, "The producer channel pool has already been disposed.");
    }

    private readonly record struct ChannelPoolKey(bool PublisherConfirmationsEnabled, TimeSpan? ConfirmTimeout)
    {
        public static ChannelPoolKey Create(bool publisherConfirmationsEnabled, TimeSpan? confirmTimeout)
            => publisherConfirmationsEnabled
                ? new ChannelPoolKey(true, confirmTimeout ?? RabbitMqChannelOptionsFactory.DefaultConfirmTimeout)
                : new ChannelPoolKey(false, null);
    }

    private sealed class PooledProducerChannelLease
    (
        PooledProducerChannelProvider owner,
        IChannel channel,
        ChannelPoolKey poolKey
    ) : IRabbitMqChannelLease
    {
        private readonly PooledProducerChannelProvider _owner = owner;
        private readonly ChannelPoolKey _poolKey = poolKey;
        private IChannel? _channel = channel;

        public IChannel Channel => _channel ?? throw new ObjectDisposedException(nameof(PooledProducerChannelLease));

        public async ValueTask DisposeAsync()
        {
            var channel = Interlocked.Exchange(ref _channel, null);
            if (channel is null)
            {
                return;
            }

            await _owner.ReturnAsync(channel, _poolKey).ConfigureAwait(false);
        }
    }
}
