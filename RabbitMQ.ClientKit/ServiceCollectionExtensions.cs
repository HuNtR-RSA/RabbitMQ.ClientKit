using Microsoft.Extensions.Configuration;
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

    /// <summary>
    /// Adds RabbitMQ clients and configured producer/consumer definitions from an application configuration section.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="configurationSection">The configuration section containing RabbitMQ settings.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddRabbitMqClientKit(
        this IServiceCollection services,
        IConfigurationSection configurationSection)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurationSection);

        var configuration = configurationSection.Get<RabbitMqClientKitConfiguration>() ?? new RabbitMqClientKitConfiguration();
        var defaultConnection = configuration.DefaultConnection;
        var namedConnections = configuration.NamedConnections;
        var hasDefaultConnection = defaultConnection is not null;

        if (defaultConnection is not null)
        {
            services.AddRabbitMqClient(defaultConnection.ToOptions());
        }

        var knownConnections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var namedConnection in namedConnections)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(namedConnection.Name);

            if (!knownConnections.Add(namedConnection.Name))
            {
                throw new InvalidOperationException($"The RabbitMQ connection '{namedConnection.Name}' is configured more than once.");
            }

            services.AddNamedRabbitMqClient(namedConnection.Name, namedConnection.ToOptions());
        }

        var producers = CreateProducerRegistrations(configuration.Producers, knownConnections, hasDefaultConnection);
        var consumers = CreateConsumerRegistrations(configuration.Consumers, knownConnections, hasDefaultConnection);

        services.TryAddSingleton<IRabbitMqConfigurationRegistry>(_ => new RabbitMqConfigurationRegistry(producers, consumers));
        services.TryAddSingleton<IRabbitMqEndpointResolver, RabbitMqEndpointResolver>();

        return services;
    }

    private static IReadOnlyDictionary<string, RabbitMqProducerRegistration> CreateProducerRegistrations(
        IEnumerable<RabbitMqProducerConfiguration> configurations,
        ISet<string> knownConnections,
        bool hasDefaultConnection)
    {
        var producers = new Dictionary<string, RabbitMqProducerRegistration>(StringComparer.OrdinalIgnoreCase);

        foreach (var configuration in configurations)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(configuration.Name);
            ValidateConnectionReference(configuration.ConnectionName, knownConnections, hasDefaultConnection, $"producer '{configuration.Name}'");

            var registration = new RabbitMqProducerRegistration(
                configuration.Name,
                configuration.ConnectionName,
                configuration.Publish.ToOptions());

            if (!producers.TryAdd(registration.Name, registration))
            {
                throw new InvalidOperationException($"The RabbitMQ producer '{registration.Name}' is configured more than once.");
            }
        }

        return producers;
    }

    private static IReadOnlyDictionary<string, RabbitMqConsumerRegistration> CreateConsumerRegistrations(
        IEnumerable<RabbitMqConsumerConfiguration> configurations,
        ISet<string> knownConnections,
        bool hasDefaultConnection)
    {
        var consumers = new Dictionary<string, RabbitMqConsumerRegistration>(StringComparer.OrdinalIgnoreCase);

        foreach (var configuration in configurations)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(configuration.Name);
            ValidateConnectionReference(configuration.ConnectionName, knownConnections, hasDefaultConnection, $"consumer '{configuration.Name}'");

            var registration = new RabbitMqConsumerRegistration(
                configuration.Name,
                configuration.ConnectionName,
                configuration.Consume.ToOptions());

            if (!consumers.TryAdd(registration.Name, registration))
            {
                throw new InvalidOperationException($"The RabbitMQ consumer '{registration.Name}' is configured more than once.");
            }
        }

        return consumers;
    }

    private static void ValidateConnectionReference(
        string? connectionName,
        ISet<string> knownConnections,
        bool hasDefaultConnection,
        string ownerDescription)
    {
        if (string.IsNullOrWhiteSpace(connectionName))
        {
            if (!hasDefaultConnection)
            {
                throw new InvalidOperationException($"The configured {ownerDescription} references the default RabbitMQ connection, but no default connection is configured.");
            }

            return;
        }

        if (!knownConnections.Contains(connectionName))
        {
            throw new InvalidOperationException($"The configured {ownerDescription} references unknown RabbitMQ connection '{connectionName}'.");
        }
    }
}
