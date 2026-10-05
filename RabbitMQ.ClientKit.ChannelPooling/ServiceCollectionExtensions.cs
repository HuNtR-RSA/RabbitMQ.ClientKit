using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Consuming;
using RabbitMQ.ClientKit.Publishing;
using RabbitMQ.ClientKit.Serialization;

namespace RabbitMQ.ClientKit.ChannelPooling;

/// <summary>
/// Registers pooled RabbitMQ.ClientKit services with <see cref="IServiceCollection" />.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds RabbitMQ client services configured to use pooled producer channels and reusable consumer channels.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="connectionOptions">The broker connection options.</param>
    /// <param name="poolingOptions">The producer channel pooling options.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddPooledRabbitMqClient
    (
        this IServiceCollection services,
        RabbitMqConnectionOptions connectionOptions,
        RabbitMqChannelPoolingOptions? poolingOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(connectionOptions);

        services.AddRabbitMqClient(connectionOptions);
        services.Replace(ServiceDescriptor.Singleton(poolingOptions ?? new RabbitMqChannelPoolingOptions()));
        services.Replace
        (
            ServiceDescriptor.Singleton<IRabbitMqProducerChannelProvider>
            (
                sp =>
                    new PooledProducerChannelProvider
                    (
                sp.GetRequiredService<Connection.IRabbitMqConnectionManager>(),
                sp.GetRequiredService<RabbitMqChannelPoolingOptions>()
                    )
            )
        );
        services.Replace(ServiceDescriptor.Singleton<IRabbitMqConsumerChannelProvider, ReusableConsumerChannelProvider>());

        return services;
    }

    /// <summary>
    /// Adds named RabbitMQ client services configured to use pooled producer channels and reusable consumer channels.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="connectionName">The logical name used to resolve the keyed services.</param>
    /// <param name="connectionOptions">The broker connection options.</param>
    /// <param name="poolingOptions">The producer channel pooling options.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddNamedPooledRabbitMqClient
    (
        this IServiceCollection services,
        string connectionName,
        RabbitMqConnectionOptions connectionOptions,
        RabbitMqChannelPoolingOptions? poolingOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionName);
        ArgumentNullException.ThrowIfNull(connectionOptions);

        services.AddNamedRabbitMqClient(connectionName, connectionOptions);
        services.AddKeyedSingleton(connectionName, poolingOptions ?? new RabbitMqChannelPoolingOptions());
        services.AddKeyedSingleton<IRabbitMqProducerChannelProvider>
        (
            connectionName,
            (sp, key) =>
                new PooledProducerChannelProvider
                (
                    sp.GetRequiredKeyedService<Connection.IRabbitMqConnectionManager>(key),
                    sp.GetRequiredKeyedService<RabbitMqChannelPoolingOptions>(key)
                )
        );
        services.AddKeyedSingleton<IRabbitMqConsumerChannelProvider>
        (
            connectionName,
            (sp, key) => new ReusableConsumerChannelProvider(sp.GetRequiredKeyedService<Connection.IRabbitMqConnectionManager>(key))
        );
        services.AddKeyedSingleton<RabbitMqPublisher>
        (
            connectionName,
            (sp, key) =>
                new RabbitMqPublisher
                (
                    sp.GetRequiredKeyedService<IRabbitMqProducerChannelProvider>(key),
                    sp.GetRequiredKeyedService<IRabbitMqSerializer>(key)
                )
        );
        services.AddKeyedSingleton<RabbitMqConsumer>
        (
            connectionName,
            (sp, key) =>
                new RabbitMqConsumer
                (
                    sp.GetRequiredKeyedService<IRabbitMqConsumerChannelProvider>(key),
                    sp.GetRequiredKeyedService<IRabbitMqSerializer>(key)
                )
        );
        services.AddKeyedSingleton<RabbitMqClient>
        (
            connectionName,
            (sp, key) =>
                new RabbitMqClient
                (
                    sp.GetRequiredKeyedService<Connection.IRabbitMqConnectionManager>(key),
                    sp.GetRequiredKeyedService<IRabbitMqSerializer>(key),
                    sp.GetRequiredKeyedService<IRabbitMqProducerChannelProvider>(key),
                    sp.GetRequiredKeyedService<IRabbitMqConsumerChannelProvider>(key)
                )
        );

        return services;
    }
}
