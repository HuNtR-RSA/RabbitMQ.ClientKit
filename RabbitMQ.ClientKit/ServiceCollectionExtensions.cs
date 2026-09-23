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
}
