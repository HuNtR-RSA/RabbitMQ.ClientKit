using RabbitMQ.Client;
using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit.Topology;

/// <summary>
/// Declares exchanges, queues, and bindings before a producer or consumer uses them.
/// </summary>
public sealed class RabbitMqTopologyInitializer
{
    /// <summary>
    /// Declares the configured topology on the provided channel.
    /// </summary>
    /// <param name="channel">The channel used for declarations.</param>
    /// <param name="topology">The topology to declare.</param>
    /// <param name="cancellationToken">The cancellation token for topology operations.</param>
    public static async Task InitializeAsync(IChannel channel, RabbitMqTopologyOptions topology, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(topology);

        if (topology.Exchange is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(topology.Exchange.Name);
            ArgumentException.ThrowIfNullOrWhiteSpace(topology.Exchange.Type);

            await channel.ExchangeDeclareAsync
            (
                    topology.Exchange.Name,
                    topology.Exchange.Type,
                    topology.Exchange.Durable,
                    topology.Exchange.AutoDelete,
                    topology.Exchange.Arguments,
                    cancellationToken: cancellationToken
            ).ConfigureAwait(false);
        }

        if (topology.Queue is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(topology.Queue.Name);

            await channel.QueueDeclareAsync
            (
                    topology.Queue.Name,
                    topology.Queue.Durable,
                    topology.Queue.Exclusive,
                    topology.Queue.AutoDelete,
                    topology.Queue.Arguments,
                    cancellationToken: cancellationToken
            ).ConfigureAwait(false);
        }

        foreach (var binding in topology.Bindings)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(binding.QueueName);
            ArgumentException.ThrowIfNullOrWhiteSpace(binding.ExchangeName);

            await channel.QueueBindAsync
            (
                    binding.QueueName,
                    binding.ExchangeName,
                    binding.RoutingKey,
                    binding.Arguments,
                    cancellationToken: cancellationToken
            ).ConfigureAwait(false);
        }
    }
}
