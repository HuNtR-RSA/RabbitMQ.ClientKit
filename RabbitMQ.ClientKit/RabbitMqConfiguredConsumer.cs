using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Consuming;

namespace RabbitMQ.ClientKit;

/// <summary>
/// Represents a configured consumer and the RabbitMQ services it resolves to.
/// </summary>
public sealed class RabbitMqConfiguredConsumer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqConfiguredConsumer" /> class.
    /// </summary>
    /// <param name="registration">The configured consumer registration.</param>
    /// <param name="consumer">The resolved consumer.</param>
    /// <param name="client">The resolved client.</param>
    public RabbitMqConfiguredConsumer(
        RabbitMqConsumerRegistration registration,
        RabbitMqConsumer consumer,
        RabbitMqClient client)
    {
        Registration = registration ?? throw new ArgumentNullException(nameof(registration));
        Consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        Client = client ?? throw new ArgumentNullException(nameof(client));
    }

    /// <summary>
    /// Gets the consumer registration.
    /// </summary>
    public RabbitMqConsumerRegistration Registration { get; }

    /// <summary>
    /// Gets the consumer name.
    /// </summary>
    public string Name => Registration.Name;

    /// <summary>
    /// Gets the optional named connection associated with the consumer.
    /// </summary>
    public string? ConnectionName => Registration.ConnectionName;

    /// <summary>
    /// Gets the consume options bound for the consumer.
    /// </summary>
    public RabbitMqConsumerOptions Options => Registration.Options;

    /// <summary>
    /// Gets the resolved consumer service.
    /// </summary>
    public RabbitMqConsumer Consumer { get; }

    /// <summary>
    /// Gets the resolved client.
    /// </summary>
    public RabbitMqClient Client { get; }
}
