using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Consuming;
using RabbitMQ.ClientKit.Models;

namespace RabbitMQ.ClientKit.DynamicConfiguration;

/// <summary>
/// Provides access to a dynamically configured consumer definition.
/// </summary>
public sealed class RabbitMqDynamicConsumer
{
    private readonly RabbitMqDynamicRuntime _runtime;

    internal RabbitMqDynamicConsumer(RabbitMqDynamicRuntime runtime, string name)
    {
        _runtime = runtime ?? throw new ArgumentNullException(nameof(runtime));
        Name = string.IsNullOrWhiteSpace(name) ? throw new ArgumentException("Consumer name is required.", nameof(name)) : name;
    }

    /// <summary>
    /// Gets the logical consumer name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the latest consumer registration.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the load operation.</param>
    public ValueTask<RabbitMqConsumerRegistration> GetRegistrationAsync(CancellationToken cancellationToken = default) =>
        _runtime.GetConsumerRegistrationAsync(Name, cancellationToken);

    /// <summary>
    /// Gets the current consumer service for the consumer's configured connection.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the load operation.</param>
    public async ValueTask<RabbitMqConsumer> GetConsumerAsync(CancellationToken cancellationToken = default)
    {
        var registration = await GetRegistrationAsync(cancellationToken).ConfigureAwait(false);
        var client = await _runtime.GetClientAsync(registration.ConnectionName, cancellationToken).ConfigureAwait(false);
        return client.Consumer;
    }

    /// <summary>
    /// Gets the current client for the consumer's configured connection.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the load operation.</param>
    public async ValueTask<RabbitMqClient> GetClientAsync(CancellationToken cancellationToken = default)
    {
        var registration = await GetRegistrationAsync(cancellationToken).ConfigureAwait(false);
        return await _runtime.GetClientAsync(registration.ConnectionName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a subscription using the latest consumer definition.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="handler">The message handler.</param>
    /// <param name="cancellationToken">The cancellation token for the subscribe operation.</param>
    public async Task<RabbitMqConsumerSubscription> SubscribeAsync<T>(
        Func<RabbitMqReceivedMessage<T>, CancellationToken, Task<RabbitMqConsumeResult>> handler,
        CancellationToken cancellationToken = default)
    {
        var registration = await GetRegistrationAsync(cancellationToken).ConfigureAwait(false);
        var consumer = await GetConsumerAsync(cancellationToken).ConfigureAwait(false);
        return await consumer.SubscribeAsync(registration.Options, handler, cancellationToken).ConfigureAwait(false);
    }
}
