namespace RabbitMQ.ClientKit.ChannelPooling;

/// <summary>
/// Configures the channel pooling extension package.
/// </summary>
public sealed class RabbitMqChannelPoolingOptions
{
    /// <summary>
    /// Gets the maximum number of producer channels retained by the pool.
    /// </summary>
    public int ProducerPoolSize { get; init; } = 8;
}
