using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Models;
using RabbitMQ.ClientKit.Serialization;
using RabbitMQ.ClientKit.Topology;

namespace RabbitMQ.ClientKit.Consuming;

/// <summary>
/// Creates RabbitMQ consumer subscriptions for strongly typed message handlers.
/// </summary>
public sealed class RabbitMqConsumer(
    IRabbitMqConsumerChannelProvider consumerChannelProvider,
    IRabbitMqSerializer serializer,
    RabbitMqTopologyInitializer topologyInitializer)
{
    private readonly IRabbitMqConsumerChannelProvider _consumerChannelProvider = consumerChannelProvider ?? throw new ArgumentNullException(nameof(consumerChannelProvider));
    private readonly IRabbitMqSerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly RabbitMqTopologyInitializer _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));

    /// <summary>
    /// Subscribes to a queue and dispatches deliveries to the provided handler.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="options">The consumer options describing queue behavior and topology.</param>
    /// <param name="handler">The async message handler.</param>
    /// <param name="cancellationToken">The cancellation token for the subscribe operation.</param>
    /// <returns>The active consumer subscription.</returns>
    public async Task<RabbitMqConsumerSubscription> SubscribeAsync<T>(
        RabbitMqConsumerOptions options,
        Func<RabbitMqReceivedMessage<T>, CancellationToken, Task<RabbitMqConsumeResult>> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(handler);

        options.Validate();

        var lease = await _consumerChannelProvider.RentAsync(options.ResolveConsumerName(), cancellationToken).ConfigureAwait(false);
        try
        {
            var channel = lease.Channel;

            if (options.Topology is not null)
            {
                await RabbitMqTopologyInitializer.InitializeAsync(channel, options.Topology, cancellationToken).ConfigureAwait(false);
            }

            if (options.PrefetchCount > 0)
            {
                await channel.BasicQosAsync(0, options.PrefetchCount, options.GlobalPrefetch, cancellationToken).ConfigureAwait(false);
            }

            var subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (_, args) => HandleMessageAsync(channel, args, options, handler, subscriptionCts.Token);

            var consumerTag = await channel.BasicConsumeAsync(
                    options.QueueName,
                    options.AutoAck,
                    options.ConsumerTag,
                    options.NoLocal,
                    options.Exclusive,
                    options.Arguments,
                    consumer,
                    cancellationToken)
                .ConfigureAwait(false);

            return new RabbitMqConsumerSubscription(lease, channel, consumerTag, subscriptionCts);
        }
        catch
        {
            await lease.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task HandleMessageAsync<T>(
        IChannel channel,
        BasicDeliverEventArgs args,
        RabbitMqConsumerOptions options,
        Func<RabbitMqReceivedMessage<T>, CancellationToken, Task<RabbitMqConsumeResult>> handler,
        CancellationToken cancellationToken)
    {
        var body = args.Body.ToArray();
        try
        {
            var payload = _serializer.Deserialize<T>(body);
            var message = new RabbitMqReceivedMessage<T>(
                payload,
                CreateMessageContext(args),
                body);

            var result = await handler(message, cancellationToken).ConfigureAwait(false);

            if (!options.AutoAck)
            {
                await ApplyConsumeResultAsync(channel, args.DeliveryTag, result, cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            if (!options.AutoAck && channel.IsOpen)
            {
                await channel.BasicNackAsync(args.DeliveryTag, false, options.RequeueOnFailure, CancellationToken.None).ConfigureAwait(false);
            }

            throw;
        }
    }

    private static async Task ApplyConsumeResultAsync
    (
        IChannel channel,
        ulong deliveryTag,
        RabbitMqConsumeResult result,
        CancellationToken cancellationToken
    )
    {
        switch (result.Disposition)
        {
            case RabbitMqConsumeDisposition.Ack:
                await channel.BasicAckAsync(deliveryTag, false, cancellationToken).ConfigureAwait(false);
                break;
            case RabbitMqConsumeDisposition.Reject:
                await channel.BasicRejectAsync(deliveryTag, false, cancellationToken).ConfigureAwait(false);
                break;
            case RabbitMqConsumeDisposition.Requeue:
                await channel.BasicNackAsync(deliveryTag, false, true, cancellationToken).ConfigureAwait(false);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(result), result.Disposition, "Unknown consume disposition.");
        }
    }

    private static RabbitMqMessageContext CreateMessageContext(BasicDeliverEventArgs args)
    {
        var headers = args.BasicProperties.Headers is null
            ? new Dictionary<string, object?>()
            : new Dictionary<string, object?>(args.BasicProperties.Headers);

        DateTimeOffset? timestamp = null;
        if (args.BasicProperties.IsTimestampPresent())
        {
            timestamp = DateTimeOffset.FromUnixTimeSeconds(args.BasicProperties.Timestamp.UnixTime);
        }

        return new RabbitMqMessageContext
        {
            AppId = args.BasicProperties.AppId,
            ConsumerTag = args.ConsumerTag,
            ContentType = args.BasicProperties.ContentType,
            CorrelationId = args.BasicProperties.CorrelationId,
            DeliveryTag = args.DeliveryTag,
            Exchange = args.Exchange,
            Headers = headers,
            MessageId = args.BasicProperties.MessageId,
            Redelivered = args.Redelivered,
            ReplyTo = args.BasicProperties.ReplyTo,
            RoutingKey = args.RoutingKey,
            TimestampUtc = timestamp
        };
    }
}
