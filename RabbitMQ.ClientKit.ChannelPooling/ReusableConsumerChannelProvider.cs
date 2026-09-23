using System.Collections.Concurrent;
using RabbitMQ.Client;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Connection;

namespace RabbitMQ.ClientKit.ChannelPooling;

/// <summary>
/// Reuses a dedicated channel per logical consumer name.
/// </summary>
public sealed class ReusableConsumerChannelProvider(IRabbitMqConnectionManager connectionManager) : IRabbitMqConsumerChannelProvider
{
    private readonly IRabbitMqConnectionManager _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
    private readonly ConcurrentDictionary<string, ConsumerChannelState> _channels = new(StringComparer.Ordinal);
    private bool _disposed;

    /// <inheritdoc />
    public async ValueTask<IRabbitMqChannelLease> RentAsync(string consumerName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerName);
        ThrowIfDisposed();

        var state = _channels.GetOrAdd(consumerName, static _ => new ConsumerChannelState());
        await state.Sync.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();

            if (state.IsLeased)
            {
                throw new InvalidOperationException($"The consumer channel '{consumerName}' is already in use.");
            }

            if (state.Channel is null || !state.Channel.IsOpen)
            {
                if (state.Channel is not null)
                {
                    await state.Channel.DisposeAsync().ConfigureAwait(false);
                }

                state.Channel = await _connectionManager.CreateChannelAsync(cancellationToken).ConfigureAwait(false);
            }

            state.IsLeased = true;
            return new ReusableConsumerChannelLease(this, consumerName, state.Channel);
        }
        finally
        {
            state.Sync.Release();
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

        foreach (var pair in _channels)
        {
            var state = pair.Value;
            await state.Sync.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!state.IsLeased && state.Channel is not null)
                {
                    await state.Channel.DisposeAsync().ConfigureAwait(false);
                    state.Channel = null;
                }
            }
            finally
            {
                state.Sync.Release();
            }
        }
    }

    internal async ValueTask ReturnAsync(string consumerName, IChannel channel)
    {
        if (!_channels.TryGetValue(consumerName, out var state))
        {
            await channel.DisposeAsync().ConfigureAwait(false);
            return;
        }

        await state.Sync.WaitAsync().ConfigureAwait(false);
        try
        {
            state.IsLeased = false;

            if (_disposed || !channel.IsOpen)
            {
                if (ReferenceEquals(state.Channel, channel))
                {
                    state.Channel = null;
                }

                _channels.TryRemove(consumerName, out _);
                await channel.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            state.Sync.Release();
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(ReusableConsumerChannelProvider));
        }
    }

    private sealed class ConsumerChannelState
    {
        public SemaphoreSlim Sync { get; } = new(1, 1);

        public IChannel? Channel { get; set; }

        public bool IsLeased { get; set; }
    }

    private sealed class ReusableConsumerChannelLease(
        ReusableConsumerChannelProvider owner,
        string consumerName,
        IChannel channel) : IRabbitMqChannelLease
    {
        private readonly ReusableConsumerChannelProvider _owner = owner;
        private readonly string _consumerName = consumerName;
        private IChannel? _channel = channel;

        public IChannel Channel => _channel ?? throw new ObjectDisposedException(nameof(ReusableConsumerChannelLease));

        public async ValueTask DisposeAsync()
        {
            var channel = Interlocked.Exchange(ref _channel, null);
            if (channel is null)
            {
                return;
            }

            await _owner.ReturnAsync(_consumerName, channel).ConfigureAwait(false);
        }
    }
}
