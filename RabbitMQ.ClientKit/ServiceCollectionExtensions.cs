using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Connection;
using RabbitMQ.ClientKit.Consuming;
using RabbitMQ.ClientKit.Publishing;
using RabbitMQ.ClientKit.Serialization;
using RabbitMQ.ClientKit.Topology;

namespace RabbitMQ.ClientKit;

/// <summary>
/// Registers RabbitMQ.ClientKit services with <see cref="IServiceCollection" />.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the core RabbitMQ client services using transient producer and consumer channel providers.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="connectionOptions">The broker connection options.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddRabbitMqClient
    (
        this IServiceCollection services,
        RabbitMqConnectionOptions connectionOptions
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(connectionOptions);

        services.TryAddSingleton(connectionOptions);
        services.TryAddSingleton<IRabbitMqSerializer, JsonRabbitMqSerializer>();
        services.TryAddSingleton<RabbitMqTopologyInitializer>();
        services.TryAddSingleton<IRabbitMqConnectionManager>(sp => new RabbitMqConnectionManager(sp.GetRequiredService<RabbitMqConnectionOptions>()));
        services.TryAddSingleton<IRabbitMqProducerChannelProvider, TransientProducerChannelProvider>();
        services.TryAddSingleton<IRabbitMqConsumerChannelProvider, TransientConsumerChannelProvider>();
        services.TryAddSingleton<RabbitMqPublisher>
        (
            sp =>
                new RabbitMqPublisher
                (
                    sp.GetRequiredService<IRabbitMqProducerChannelProvider>(),
                    sp.GetRequiredService<IRabbitMqSerializer>(),
                    sp.GetRequiredService<RabbitMqTopologyInitializer>()
                )
        );
        services.TryAddSingleton<RabbitMqConsumer>
        (
            sp => 
                new RabbitMqConsumer
                (
                    sp.GetRequiredService<IRabbitMqConsumerChannelProvider>(), 
                    sp.GetRequiredService<IRabbitMqSerializer>(), 
                    sp.GetRequiredService<RabbitMqTopologyInitializer>()
                )
        );
        services.TryAddSingleton<RabbitMqClient>
        (
            sp =>
                new RabbitMqClient
                (
                    sp.GetRequiredService<IRabbitMqConnectionManager>(), 
                    sp.GetRequiredService<IRabbitMqSerializer>(), 
                    sp.GetRequiredService<IRabbitMqProducerChannelProvider>(), 
                    sp.GetRequiredService<IRabbitMqConsumerChannelProvider>()
                )
        );

        return services;
    }

    /// <summary>
    /// Adds named RabbitMQ client services using transient producer and consumer channel providers.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="connectionName">The logical name used to resolve the keyed services.</param>
    /// <param name="connectionOptions">The broker connection options.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddNamedRabbitMqClient
    (
        this IServiceCollection services,
        string connectionName,
        RabbitMqConnectionOptions connectionOptions
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionName);
        ArgumentNullException.ThrowIfNull(connectionOptions);

        services.AddKeyedSingleton(connectionName, connectionOptions);
        services.AddKeyedSingleton<IRabbitMqSerializer, JsonRabbitMqSerializer>(connectionName);
        services.AddKeyedSingleton<RabbitMqTopologyInitializer>(connectionName);
        services.AddKeyedSingleton<IRabbitMqConnectionManager>
        (
            connectionName,
            (sp, key) => new RabbitMqConnectionManager(sp.GetRequiredKeyedService<RabbitMqConnectionOptions>(key))
        );
        services.AddKeyedSingleton<IRabbitMqProducerChannelProvider>
        (
            connectionName,
            (sp, key) => new TransientProducerChannelProvider(sp.GetRequiredKeyedService<IRabbitMqConnectionManager>(key))
        );
        services.AddKeyedSingleton<IRabbitMqConsumerChannelProvider>
        (
            connectionName,
            (sp, key) => new TransientConsumerChannelProvider(sp.GetRequiredKeyedService<IRabbitMqConnectionManager>(key))
        );
        services.AddKeyedSingleton<RabbitMqPublisher>
        (
            connectionName,
            (sp, key) =>
                new RabbitMqPublisher
                (
                    sp.GetRequiredKeyedService<IRabbitMqProducerChannelProvider>(key),
                    sp.GetRequiredKeyedService<IRabbitMqSerializer>(key),
                    sp.GetRequiredKeyedService<RabbitMqTopologyInitializer>(key)
                )
        );
        services.AddKeyedSingleton<RabbitMqConsumer>
        (
            connectionName,
            (sp, key) =>
                new RabbitMqConsumer
                (
                    sp.GetRequiredKeyedService<IRabbitMqConsumerChannelProvider>(key),
                    sp.GetRequiredKeyedService<IRabbitMqSerializer>(key),
                    sp.GetRequiredKeyedService<RabbitMqTopologyInitializer>(key)
                )
        );
        services.AddKeyedSingleton<RabbitMqClient>
        (
            connectionName,
            (sp, key) =>
                new RabbitMqClient
                (
                    sp.GetRequiredKeyedService<IRabbitMqConnectionManager>(key),
                    sp.GetRequiredKeyedService<IRabbitMqSerializer>(key),
                    sp.GetRequiredKeyedService<IRabbitMqProducerChannelProvider>(key),
                    sp.GetRequiredKeyedService<IRabbitMqConsumerChannelProvider>(key)
                )
        );

        return services;
    }
}
