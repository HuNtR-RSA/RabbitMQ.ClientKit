using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.ClientKit.Channel;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Connection;
using RabbitMQ.ClientKit.Models;
using RabbitMQ.ClientKit.Serialization;
using RabbitMQ.ClientKit.Topology;

namespace RabbitMQ.ClientKit.Consuming;

/// <summary>
/// Creates RabbitMQ consumer subscriptions for strongly typed message handlers.
/// </summary>
public sealed class RabbitMqConsumer
(
    IRabbitMqConsumerChannelProvider consumerChannelProvider,
    IRabbitMqSerializer serializer,
    IRabbitMqAmqp10ConnectionManager? amqp10ConnectionManager = null
)
{
    private readonly IRabbitMqConsumerChannelProvider _consumerChannelProvider = consumerChannelProvider ?? throw new ArgumentNullException(nameof(consumerChannelProvider));
    private readonly IRabbitMqSerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly IRabbitMqAmqp10ConnectionManager? _amqp10ConnectionManager = amqp10ConnectionManager;

    /// <summary>
    /// Subscribes to a queue and dispatches deliveries to the provided handler.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="options">The consumer options describing queue behavior and topology.</param>
    /// <param name="handler">The async message handler.</param>
    /// <param name="cancellationToken">The cancellation token for the subscribe operation.</param>
    /// <returns>The active consumer subscription.</returns>
    public Task<RabbitMqConsumerSubscription> SubscribeAsync<T>
    (
        RabbitMqConsumerOptions options,
        Func<RabbitMqReceivedMessage<T>, CancellationToken, Task<RabbitMqConsumeResult>> handler,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(handler);

        options.Validate();

        return options.Protocol == RabbitMqConsumerProtocol.Amqp10
            ? SubscribeAmqp10Async(options, handler, cancellationToken)
            : SubscribeAmqp091Async(options, handler, cancellationToken);
    }

    private async Task<RabbitMqConsumerSubscription> SubscribeAmqp091Async<T>
    (
        RabbitMqConsumerOptions options,
        Func<RabbitMqReceivedMessage<T>, CancellationToken, Task<RabbitMqConsumeResult>> handler,
        CancellationToken cancellationToken
    )
    {
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

            var consumerTag = await channel.BasicConsumeAsync
            (
                    options.QueueName,
                    options.AutoAck,
                    options.ConsumerTag,
                    options.NoLocal,
                    options.Exclusive,
                    options.Arguments,
                    consumer,
                    cancellationToken
            ).ConfigureAwait(false);

            return new RabbitMqConsumerSubscription(lease, channel, consumerTag, subscriptionCts);
        }
        catch
        {
            await lease.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task<RabbitMqConsumerSubscription> SubscribeAmqp10Async<T>
    (
        RabbitMqConsumerOptions options,
        Func<RabbitMqReceivedMessage<T>, CancellationToken, Task<RabbitMqConsumeResult>> handler,
        CancellationToken cancellationToken
    )
    {
        if (_amqp10ConnectionManager is null)
        {
            throw new InvalidOperationException(
                "AMQP 1.0 consumption requires an IRabbitMqAmqp10ConnectionManager. Subscribe with RabbitMqConsumerProtocol.Amqp10 on a client that owns an AMQP 1.0 connection.");
        }

        if (options.Topology is not null)
        {
            await DeclareTopologyAsync(options, cancellationToken).ConfigureAwait(false);
        }

        var connection = await _amqp10ConnectionManager.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        var subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        try
        {
            var builder = connection.ConsumerBuilder()
                .Queue(options.QueueName)
                .MessageHandler((context, message) => HandleAmqp10MessageAsync(context, message, options, handler, subscriptionCts.Token));

            if (options.PrefetchCount > 0)
            {
                builder = builder.InitialCredits(options.PrefetchCount);
            }

            builder = builder.SettleStrategy(
                options.AutoAck ? ConsumerSettleStrategy.PreSettled : ConsumerSettleStrategy.ExplicitSettle);

            var consumer = await builder.BuildAndStartAsync(cancellationToken).ConfigureAwait(false);
            return new RabbitMqConsumerSubscription(new Amqp10ConsumerSession(consumer, subscriptionCts));
        }
        catch
        {
            subscriptionCts.Dispose();
            throw;
        }
    }

    private async Task DeclareTopologyAsync(RabbitMqConsumerOptions options, CancellationToken cancellationToken)
    {
        var lease = await _consumerChannelProvider.RentAsync(options.ResolveConsumerName(), cancellationToken).ConfigureAwait(false);
        try
        {
            await RabbitMqTopologyInitializer.InitializeAsync(lease.Channel, options.Topology!, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await lease.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task HandleMessageAsync<T>
    (
        IChannel channel,
        BasicDeliverEventArgs args,
        RabbitMqConsumerOptions options,
        Func<RabbitMqReceivedMessage<T>, CancellationToken, Task<RabbitMqConsumeResult>> handler,
        CancellationToken cancellationToken
    )
    {
        var body = args.Body.ToArray();
        var delivery = options.AutoAck
            ? null
            : new RabbitMqDeliveryHandle(channel, args.DeliveryTag, _serializer);
        try
        {
            var payload = _serializer.Deserialize<T>(body);
            var message = new RabbitMqReceivedMessage<T>
            (
                payload,
                CreateMessageContext(args),
                body,
                delivery
            );

            var result = await handler(message, cancellationToken).ConfigureAwait(false);

            if (!options.AutoAck && result.Disposition != RabbitMqConsumeDisposition.Handled && !(delivery?.IsSettled ?? false))
            {
                await ApplyConsumeResultAsync(channel, args.DeliveryTag, result, cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            if (!options.AutoAck && !(delivery?.IsSettled ?? false) && channel.IsOpen)
            {
                await channel.BasicNackAsync(args.DeliveryTag, false, options.RequeueOnFailure, CancellationToken.None).ConfigureAwait(false);
            }

            throw;
        }
    }

    private async Task HandleAmqp10MessageAsync<T>
    (
        IContext context,
        IMessage message,
        RabbitMqConsumerOptions options,
        Func<RabbitMqReceivedMessage<T>, CancellationToken, Task<RabbitMqConsumeResult>> handler,
        CancellationToken cancellationToken
    )
    {
        var body = ReadBody(message);
        var delivery = options.AutoAck
            ? null
            : new RabbitMqAmqp10DeliveryHandle(context);
        try
        {
            var payload = _serializer.Deserialize<T>(body);
            var received = new RabbitMqReceivedMessage<T>
            (
                payload,
                CreateAmqp10MessageContext(message),
                body,
                delivery
            );

            var result = await handler(received, cancellationToken).ConfigureAwait(false);

            if (!options.AutoAck && result.Disposition != RabbitMqConsumeDisposition.Handled && !(delivery?.IsSettled ?? false))
            {
                await ApplyAmqp10ConsumeResultAsync(delivery!, result, cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            if (!options.AutoAck && !(delivery?.IsSettled ?? false))
            {
                if (options.RequeueOnFailure)
                {
                    context.Requeue();
                }
                else
                {
                    context.Discard();
                }
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
            case RabbitMqConsumeDisposition.Handled:
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(result), result.Disposition, "Unknown consume disposition.");
        }
    }

    private static Task ApplyAmqp10ConsumeResultAsync
    (
        IRabbitMqDeliveryHandle delivery,
        RabbitMqConsumeResult result,
        CancellationToken cancellationToken
    )
        => result.Disposition switch
        {
            RabbitMqConsumeDisposition.Ack => delivery.AckAsync(cancellationToken),
            RabbitMqConsumeDisposition.Reject => delivery.RejectAsync(cancellationToken),
            RabbitMqConsumeDisposition.Requeue => delivery.NackAsync(requeue: true, cancellationToken),
            RabbitMqConsumeDisposition.Handled => Task.CompletedTask,
            _ => throw new ArgumentOutOfRangeException(nameof(result), result.Disposition, "Unknown consume disposition.")
        };

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
            ContentEncoding = args.BasicProperties.ContentEncoding,
            Type = args.BasicProperties.Type,
            Expiration = args.BasicProperties.Expiration,
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

    private static RabbitMqMessageContext CreateAmqp10MessageContext(IMessage message)
    {
        var deliveryCount = TryGet(message.DeliveryCount);
        var headers = TryGetProperties(message);
        var creationTime = TryGet(message.CreationTime);

        return new RabbitMqMessageContext
        {
            ContentType = TryGet(message.ContentType),
            ContentEncoding = TryGet(message.ContentEncoding),
            CorrelationId = TryGet(message.CorrelationId)?.ToString(),
            DeliveryCount = deliveryCount,
            Headers = headers,
            MessageId = TryGet(message.MessageId)?.ToString(),
            Redelivered = deliveryCount > 0,
            ReplyTo = TryGet(message.ReplyTo),
            TimestampUtc = creationTime == default ? null : new DateTimeOffset(DateTime.SpecifyKind(creationTime, DateTimeKind.Utc))
        };
    }

    private static ReadOnlyMemory<byte> ReadBody(IMessage message)
    {
        try
        {
            return message.Body();
        }
        catch (InvalidCastException)
        {
            return System.Text.Encoding.UTF8.GetBytes(message.BodyAsString());
        }
        catch (InvalidOperationException)
        {
            return System.Text.Encoding.UTF8.GetBytes(message.BodyAsString());
        }
    }

    private static T? TryGet<T>(Func<T> getter)
    {
        try
        {
            return getter();
        }
        catch (FieldNotSetException)
        {
            return default;
        }
        catch (NullReferenceException)
        {
            return default;
        }
    }

    private static IReadOnlyDictionary<string, object?> TryGetProperties(IMessage message)
    {
        try
        {
            var properties = message.Properties();
            var headers = new Dictionary<string, object?>(properties.Count);
            foreach (var pair in properties)
            {
                var key = pair.Key?.ToString();
                if (key is not null)
                {
                    headers[key] = pair.Value;
                }
            }

            return headers;
        }
        catch (FieldNotSetException)
        {
            return new Dictionary<string, object?>();
        }
    }
}
