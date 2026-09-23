namespace RabbitMQ.ClientKit.Configuration;

/// <summary>
/// Defines how the wrapper connects to a RabbitMQ broker.
/// </summary>
public sealed class RabbitMqConnectionOptions
{
    /// <summary>
    /// Gets the connection URI. When set, it takes precedence over the individual host settings.
    /// </summary>
    public string? ConnectionUri { get; init; }

    /// <summary>
    /// Gets the broker host name.
    /// </summary>
    public string HostName { get; init; } = "localhost";

    /// <summary>
    /// Gets the broker port.
    /// </summary>
    public int Port { get; init; } = 5672;

    /// <summary>
    /// Gets the user name used to authenticate with the broker.
    /// </summary>
    public string UserName { get; init; } = "guest";

    /// <summary>
    /// Gets the password used to authenticate with the broker.
    /// </summary>
    public string Password { get; init; } = "guest";

    /// <summary>
    /// Gets the virtual host to connect to.
    /// </summary>
    public string VirtualHost { get; init; } = "/";

    /// <summary>
    /// Gets the optional connection name shown in RabbitMQ management tooling.
    /// </summary>
    public string? ClientProvidedName { get; init; }

    /// <summary>
    /// Gets a value indicating whether automatic network recovery is enabled.
    /// </summary>
    public bool AutomaticRecoveryEnabled { get; init; } = true;

    /// <summary>
    /// Gets a value indicating whether topology recovery is enabled after reconnection.
    /// </summary>
    public bool TopologyRecoveryEnabled { get; init; } = true;

    /// <summary>
    /// Gets the number of concurrent async consumer dispatch operations per connection.
    /// </summary>
    public ushort ConsumerDispatchConcurrency { get; init; } = 1;

    /// <summary>
    /// Gets the requested heartbeat interval.
    /// </summary>
    public TimeSpan RequestedHeartbeat { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Gets the timeout used for initial connection attempts.
    /// </summary>
    public TimeSpan RequestedConnectionTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets the retry interval used by automatic network recovery.
    /// </summary>
    public TimeSpan NetworkRecoveryInterval { get; init; } = TimeSpan.FromSeconds(10);
}
