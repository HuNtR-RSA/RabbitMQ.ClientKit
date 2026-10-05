using RabbitMQ.Client;

namespace RabbitMQ.ClientKit.Channel;

/// <summary>
/// Factory for building RabbitMQ <see cref="CreateChannelOptions"/>. Publisher confirmations are enabled at channel creation;
/// they cannot be turned on later with ConfirmSelect.
/// </summary>
public static class RabbitMqChannelOptionsFactory
{
    /// <summary>
    /// Gets the default publisher confirmation timeout.
    /// </summary>
    public static readonly TimeSpan DefaultConfirmTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Creates channel options with optional publisher confirmation tracking.
    /// </summary>
    public static CreateChannelOptions CreateChannelOptions(bool publisherConfirmationsEnabled, TimeSpan? confirmTimeout = null)
        => new
        (
            publisherConfirmationsEnabled: publisherConfirmationsEnabled,
            publisherConfirmationTrackingEnabled: publisherConfirmationsEnabled
        );
}
