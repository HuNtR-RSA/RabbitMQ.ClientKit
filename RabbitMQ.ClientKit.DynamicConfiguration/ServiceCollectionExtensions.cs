using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace RabbitMQ.ClientKit.DynamicConfiguration;

/// <summary>
/// Registers optional dynamic RabbitMQ configuration services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds dynamic RabbitMQ configuration services using the supplied source type.
    /// </summary>
    /// <typeparam name="TSource">The source type that loads the latest snapshot.</typeparam>
    /// <param name="services">The service collection to configure.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddDynamicRabbitMqClientKit<TSource>(this IServiceCollection services)
        where TSource : class, IRabbitMqDynamicConfigurationSource
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<IRabbitMqDynamicConfigurationSource, TSource>();
        return services.AddDynamicRabbitMqClientKit();
    }

    /// <summary>
    /// Adds dynamic RabbitMQ configuration services using a configuration section as the source.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="configurationSection">The configuration section to reload from.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddDynamicRabbitMqClientKit
    (
        this IServiceCollection services,
        IConfigurationSection configurationSection
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurationSection);

        services.TryAddSingleton<IRabbitMqDynamicConfigurationSource>
        (
            _ => new ConfigurationSectionRabbitMqDynamicConfigurationSource(configurationSection)
        );
        
        return services.AddDynamicRabbitMqClientKit();
    }

    /// <summary>
    /// Adds dynamic RabbitMQ configuration services using the supplied source factory.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="sourceFactory">The factory that creates the configuration source.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddDynamicRabbitMqClientKit
    (
        this IServiceCollection services,
        Func<IServiceProvider, IRabbitMqDynamicConfigurationSource> sourceFactory
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(sourceFactory);

        services.TryAddSingleton(sourceFactory);
        return services.AddDynamicRabbitMqClientKit();
    }

    private static IServiceCollection AddDynamicRabbitMqClientKit(this IServiceCollection services)
    {
        services.TryAddSingleton<IRabbitMqDynamicClientActivator, DefaultRabbitMqDynamicClientActivator>();
        services.TryAddSingleton<RabbitMqDynamicRuntime>();
        services.TryAddSingleton<IRabbitMqDynamicEndpointResolver>(sp => sp.GetRequiredService<RabbitMqDynamicRuntime>());
        
        return services;
    }
}
