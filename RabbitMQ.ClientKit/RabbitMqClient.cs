using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Connection;
using RabbitMQ.ClientKit.Consuming;
using RabbitMQ.ClientKit.Models;
using RabbitMQ.ClientKit.Publishing;
using RabbitMQ.ClientKit.Serialization;
using RabbitMQ.ClientKit.Topology;

namespace RabbitMQ.ClientKit;

/// <summary>
/// Provides a convenience facade over the core RabbitMQ publishing and subscription services.
/// </summary>
public sealed class RabbitMqClient : IAsyncDisposable
{
    private readonly IRabbitMqConnectionManager? _connectionManager;
    private readonly IRabbitMqProducerChannelProvider _producerChannelProvider;
    private readonly IRabbitMqConsumerChannelProvider _consumerChannelProvider;
    private readonly bool _ownsDependencies;

    /// <summary>
    /// Initializes a new client using the built-in connection manager, serializer, and transient channel providers.
    /// </summary>
    /// <param name="options">The connection options used to reach the broker.</param>
    /// <param name="serializer">The optional payload serializer. JSON is used by default.</param>
    public RabbitMqClient(RabbitMqConnectionOptions options, IRabbitMqSerializer? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        _connectionManager = new RabbitMqConnectionManager(options);
        var serializer1 = serializer ?? new JsonRabbitMqSerializer();
        _producerChannelProvider = new TransientProducerChannelProvider(_connectionManager);
        _consumerChannelProvider = new TransientConsumerChannelProvider(_connectionManager);

        Publisher = new RabbitMqPublisher(_producerChannelProvider, serializer1);
        Consumer = new RabbitMqConsumer(_consumerChannelProvider, serializer1);
        _ownsDependencies = true;
    }

    /// <summary>
    /// Initializes a new client using externally supplied dependencies.
    /// </summary>
    /// <param name="connectionManager">The connection manager used to create channels.</param>
    /// <param name="serializer">The payload serializer.</param>
    /// <param name="producerChannelProvider">The producer channel provider.</param>
    /// <param name="consumerChannelProvider">The consumer channel provider.</param>
    /// <param name="ownsDependencies">Whether disposing the client should also dispose the supplied dependencies.</param>
    public RabbitMqClient
    (
        IRabbitMqConnectionManager connectionManager,
        IRabbitMqSerializer serializer,
        IRabbitMqProducerChannelProvider producerChannelProvider,
        IRabbitMqConsumerChannelProvider consumerChannelProvider,
        bool ownsDependencies = false
    )
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        var serializer1 = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _producerChannelProvider = producerChannelProvider ?? throw new ArgumentNullException(nameof(producerChannelProvider));
        _consumerChannelProvider = consumerChannelProvider ?? throw new ArgumentNullException(nameof(consumerChannelProvider));

        Publisher = new RabbitMqPublisher(_producerChannelProvider, serializer1);
        Consumer = new RabbitMqConsumer(_consumerChannelProvider, serializer1);
        _ownsDependencies = ownsDependencies;
    }

    /// <summary>
    /// Gets the publishing service.
    /// </summary>
    public RabbitMqPublisher Publisher { get; }

    /// <summary>
    /// Gets the subscription service.
    /// </summary>
    public RabbitMqConsumer Consumer { get; }

    /// <summary>
    /// Publishes a payload using the configured publisher service.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="message">The payload to publish.</param>
    /// <param name="options">The publish options.</param>
    /// <param name="cancellationToken">The cancellation token for the publish operation.</param>
    public Task PublishAsync<T>(T message, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
        => Publisher.PublishAsync(message, options, cancellationToken);

    /// <summary>
    /// Publishes a raw byte payload without running serialization using the configured publisher service.
    /// </summary>
    /// <param name="body">The raw payload to publish.</param>
    /// <param name="options">The publish options.</param>
    /// <param name="cancellationToken">The cancellation token for the publish operation.</param>
    public Task PublishAsync(ReadOnlyMemory<byte> body, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
        => Publisher.PublishAsync(body, options, cancellationToken);

    /// <summary>
    /// Publishes multiple payloads using the configured publisher service.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="messages">The payloads to publish.</param>
    /// <param name="options">The publish options shared by the batch.</param>
    /// <param name="cancellationToken">The cancellation token for the publish operation.</param>
    public Task<RabbitMqPublishBatchResult> PublishBatchAsync<T>(IEnumerable<T> messages, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
        => Publisher.PublishBatchAsync(messages, options, cancellationToken);

    /// <summary>
    /// Publishes multiple raw byte payloads using the configured publisher service.
    /// </summary>
    /// <param name="messages">The raw payloads to publish.</param>
    /// <param name="options">The publish options shared by the batch.</param>
    /// <param name="cancellationToken">The cancellation token for the publish operation.</param>
    public Task<RabbitMqPublishBatchResult> PublishBatchAsync(IEnumerable<ReadOnlyMemory<byte>> messages, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
        => Publisher.PublishBatchAsync(messages, options, cancellationToken);

    /// <summary>
    /// Declares the specified topology on a producer channel without publishing any messages.
    /// </summary>
    /// <param name="topology">The topology to declare.</param>
    /// <param name="cancellationToken">The cancellation token for the declaration operation.</param>
    public Task DeclareAsync(RabbitMqTopologyOptions topology, CancellationToken cancellationToken = default)
        => Publisher.DeclareAsync(topology, cancellationToken);

    /// <summary>
    /// Creates a consumer subscription using the configured consumer service.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="handler">The message handler.</param>
    /// <param name="cancellationToken">The cancellation token for the subscribe operation.</param>
    /// <returns>The active consumer subscription.</returns>
    public Task<RabbitMqConsumerSubscription> SubscribeAsync<T>
    (
        RabbitMqConsumerOptions options,
        Func<RabbitMqReceivedMessage<T>, CancellationToken, Task<RabbitMqConsumeResult>> handler,
        CancellationToken cancellationToken = default)
        => Consumer.SubscribeAsync(options, handler, cancellationToken);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (!_ownsDependencies)
        {
            return;
        }

        await _producerChannelProvider.DisposeAsync().ConfigureAwait(false);
        await _consumerChannelProvider.DisposeAsync().ConfigureAwait(false);

        if (_connectionManager is not null)
        {
            await _connectionManager.DisposeAsync().ConfigureAwait(false);
        }
    }
}
