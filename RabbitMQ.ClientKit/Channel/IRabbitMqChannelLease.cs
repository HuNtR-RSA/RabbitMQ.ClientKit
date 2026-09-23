using RabbitMQ.Client;

namespace RabbitMQ.ClientKit.Channel;

/// <summary>
/// Represents a leased RabbitMQ channel whose lifetime is controlled by a channel provider.
/// </summary>
public interface IRabbitMqChannelLease : IAsyncDisposable
{
    /// <summary>
    /// Gets the leased channel instance.
    /// </summary>
    IChannel Channel { get; }
}
