using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Models;
using RabbitMQ.ClientKit.Publishing;

namespace RabbitMQ.ClientKit.DynamicConfiguration;

/// <summary>
/// Provides access to a dynamically configured producer definition.
/// </summary>
public sealed class RabbitMqDynamicProducer
{
    private readonly RabbitMqDynamicRuntime _runtime;

    internal RabbitMqDynamicProducer(RabbitMqDynamicRuntime runtime, string name)
    {
        _runtime = runtime ?? throw new ArgumentNullException(nameof(runtime));
        Name = string.IsNullOrWhiteSpace(name) ? throw new ArgumentException("Producer name is required.", nameof(name)) : name;
    }

    /// <summary>
    /// Gets the logical producer name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the latest producer registration.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the load operation.</param>
    public ValueTask<RabbitMqProducerRegistration> GetRegistrationAsync(CancellationToken cancellationToken = default)
        => _runtime.GetProducerRegistrationAsync(Name, cancellationToken);

    /// <summary>
    /// Gets the current publisher for the producer's configured connection.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the load operation.</param>
    public async ValueTask<RabbitMqPublisher> GetPublisherAsync(CancellationToken cancellationToken = default)
    {
        var registration = await GetRegistrationAsync(cancellationToken).ConfigureAwait(false);
        var client = await _runtime.GetClientAsync(registration.ConnectionName, cancellationToken).ConfigureAwait(false);
        return client.Publisher;
    }

    /// <summary>
    /// Gets the current client for the producer's configured connection.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the load operation.</param>
    public async ValueTask<RabbitMqClient> GetClientAsync(CancellationToken cancellationToken = default)
    {
        var registration = await GetRegistrationAsync(cancellationToken).ConfigureAwait(false);
        return await _runtime.GetClientAsync(registration.ConnectionName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Publishes a message using the latest producer definition.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="message">The message payload.</param>
    /// <param name="cancellationToken">The cancellation token for the publish operation.</param>
    public async Task PublishAsync<T>(T message, CancellationToken cancellationToken = default)
    {
        var registration = await GetRegistrationAsync(cancellationToken).ConfigureAwait(false);
        var publisher = await GetPublisherAsync(cancellationToken).ConfigureAwait(false);
        await publisher.PublishAsync(message, registration.Options, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Publishes a batch of messages using the latest producer definition.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="messages">The messages to publish.</param>
    /// <param name="cancellationToken">The cancellation token for the publish operation.</param>
    /// <returns>The outcome of the batch publish operation.</returns>
    public async Task<RabbitMqPublishBatchResult> PublishBatchAsync<T>(IEnumerable<T> messages, CancellationToken cancellationToken = default)
    {
        var registration = await GetRegistrationAsync(cancellationToken).ConfigureAwait(false);
        var publisher = await GetPublisherAsync(cancellationToken).ConfigureAwait(false);
        return await publisher.PublishBatchAsync(messages, registration.Options, cancellationToken).ConfigureAwait(false);
    }
}
