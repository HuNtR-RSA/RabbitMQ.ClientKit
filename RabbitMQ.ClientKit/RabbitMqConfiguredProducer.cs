using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Publishing;

namespace RabbitMQ.ClientKit;

/// <summary>
/// Represents a configured producer and the RabbitMQ services it resolves to.
/// </summary>
public sealed class RabbitMqConfiguredProducer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqConfiguredProducer" /> class.
    /// </summary>
    /// <param name="registration">The configured producer registration.</param>
    /// <param name="publisher">The resolved publisher.</param>
    /// <param name="client">The resolved client.</param>
    public RabbitMqConfiguredProducer
    (
        RabbitMqProducerRegistration registration,
        RabbitMqPublisher publisher,
        RabbitMqClient client
    )
    {
        Registration = registration ?? throw new ArgumentNullException(nameof(registration));
        Publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        Client = client ?? throw new ArgumentNullException(nameof(client));
    }

    /// <summary>
    /// Gets the producer registration.
    /// </summary>
    public RabbitMqProducerRegistration Registration { get; }

    /// <summary>
    /// Gets the producer name.
    /// </summary>
    public string Name => Registration.Name;

    /// <summary>
    /// Gets the optional named connection associated with the producer.
    /// </summary>
    public string? ConnectionName => Registration.ConnectionName;

    /// <summary>
    /// Gets the publish options bound for the producer.
    /// </summary>
    public RabbitMqPublishOptions Options => Registration.Options;

    /// <summary>
    /// Gets the resolved publisher.
    /// </summary>
    public RabbitMqPublisher Publisher { get; }

    /// <summary>
    /// Gets the resolved client.
    /// </summary>
    public RabbitMqClient Client { get; }
}
