using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;

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
}
