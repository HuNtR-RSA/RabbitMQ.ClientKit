# RabbitMQ.ClientKit

`RabbitMQ.ClientKit` is a small RabbitMQ client toolkit aimed at the common cases:

- publishing strongly typed messages with JSON serialization
- declaring queue or exchange topology when needed
- starting async consumers with explicit ack, reject, or requeue behavior
- swapping channel-management strategies without changing producer or consumer code

The current packaged release line targets **.NET 8** and uses **independent SemVer**.

## Packages

| Package | Purpose |
|---|---|
| `RabbitMQ.ClientKit` | Core connection, publishing, consuming, serialization, and transient channel creation |
| `RabbitMQ.ClientKit.ChannelPooling` | Reusable producer and consumer channel strategies built on the core abstractions |
| `RabbitMQ.ClientKit.DynamicConfiguration` | Optional reloadable configuration support for appsettings, databases, and other dynamic sources |

## Core usage

```csharp
using RabbitMQ.ClientKit;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.Models;

var connection = new RabbitMqConnectionOptions
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest",
    ClientProvidedName = "orders-api"
};

await using var client = new RabbitMqClient(connection);

await client.PublishAsync
(
    new { OrderId = 42, Status = "Created" },
    new RabbitMqPublishOptions
    {
        QueueName = "orders.created",
        Topology = new RabbitMqTopologyOptions
        {
            Queue = new RabbitMqQueueOptions
            {
                Name = "orders.created",
                Durable = true
            }
        }
    }
);

await using var subscription = await client.SubscribeAsync<dynamic>
(
    new RabbitMqConsumerOptions
    {
        QueueName = "orders.created",
        Topology = new RabbitMqTopologyOptions
        {
            Queue = new RabbitMqQueueOptions
            {
                Name = "orders.created",
                Durable = true
            }
        }
    },
    async (message, cancellationToken) =>
    {
        Console.WriteLine(message.Payload);
        await Task.CompletedTask;
        return RabbitMqConsumeResult.Ack;
    }
);
```

## Batch publishing and publisher confirms

RabbitMQ.Client v7+ establishes publisher confirmations at channel creation (`CreateChannelOptions`). `RabbitMQ.ClientKit` enables confirm tracking on publisher channels with a configurable timeout (default 30s).

### Batch publishing

Batch publishing reuses a single leased channel and publishes each message individually while tracking confirmations.

```csharp
var result = await client.PublishBatchAsync
(
    new[]
    {
        new { OrderId = 42, Status = "Created" },
        new { OrderId = 43, Status = "Created" }
    },
    new RabbitMqPublishOptions
    {
        QueueName = "orders.created",
        PublisherConfirms = true
    }
);
```

### Batch result status (NotSent vs Unconfirmed)

`PublishBatchAsync` returns a `RabbitMqPublishBatchResult` indicating the exact publish outcome:

| Status | Meaning | Transaction / Rollback Guidance |
|---|---|---|
| `Confirmed` | All messages were published successfully. When `PublisherConfirms = true`, this means the broker confirmed every message (or the batch was empty). Without that flag, it only means `BasicPublishAsync` completed without throwing. | Safe to commit when confirms were requested and received; otherwise treat as "publish call succeeded." |
| `NotSent` | Failure occurred before the batch wrote its first publish frame (e.g. channel lease, topology declaration, or first-message serialization threw). | **Safe to roll back** database transactions. |
| `Unconfirmed` | Failure occurred after at least one frame was written (e.g. publish or confirm timeout threw mid-batch). Contains `AttemptedCount` and `ConfirmedCount`. | **Do NOT roll back** transactions; broker may have received/queued messages. |

## Raw publishing

You can publish raw byte payloads directly without invoking serializer logic:

```csharp
ReadOnlyMemory<byte> rawBytes = new byte[] { 0x01, 0x02, 0x03 };

await client.PublishAsync
(
    rawBytes,
    new RabbitMqPublishOptions
    {
        QueueName = "events.raw",
        Properties = new RabbitMqMessageProperties
        {
            ContentType = "application/octet-stream"
        }
    }
);
```

## Replace-then-ack and delivery handles

Consumers receive an `IRabbitMqDeliveryHandle` on `RabbitMqReceivedMessage<T>.Delivery` that enables in-place message replacement and manual acknowledgment. When `AutoAck = true`, `Delivery` is `null` because the broker settles the delivery before the handler runs.

```csharp
await using var subscription = await client.SubscribeAsync<OrderMessage>
(
    new RabbitMqConsumerOptions
    {
        QueueName = "orders.input",
        ConsumerName = "orders-processor"
    },
    async (message, cancellationToken) =>
    {
        var replacement = new ProcessedOrder { Id = message.Payload.Id, Status = "Processed" };

        await message.Delivery!.ReplaceAndAckAsync(
            replacement,
            new RabbitMqPublishOptions { QueueName = "orders.output" },
            cancellationToken);

        return RabbitMqConsumeResult.Handled;
    }
);
```

### Replace-then-ack semantics

`ReplaceAndAckAsync` implements a confirm-then-ack workflow on the same consumer channel:
1. Publishes the replacement message and waits for the broker confirmation.
2. If the replacement confirm fails or throws: negatively acknowledges (`nack`) the original message with requeue.
3. If the replacement confirm succeeds: acknowledges (`ack`) the original message. If the subsequent `ack` throws, the exception is swallowed and the original is **not** nacked (preventing duplicate replacements).

When using `ReplaceAndAckAsync` or manual delivery settlement, return `RabbitMqConsumeResult.Handled` so the consumer wrapper does not double-ack or double-nack.

Replacement publishes honor the supplied `RabbitMqPublishOptions`, including `Topology`, so a handler can declare the destination exchange and queues before republishing.

### Confirm-then-ack duplicate window

If a consumer crashes or network connectivity is lost in the brief window between a successful replacement confirm and the broker processing the original message's ack, RabbitMQ will redeliver the original message upon reconnect. **Message handlers must be designed to be idempotent.**

## Topology and multi-queue declarations

Topology declarations can be initialized automatically on publish/consume or executed standalone via `DeclareAsync`:

```csharp
var topology = new RabbitMqTopologyOptions
{
    Queue = new RabbitMqQueueOptions { Name = "orders.work", Durable = true },
    Queues =
    [
        new RabbitMqQueueOptions { Name = "orders.failed", Durable = true },
        new RabbitMqQueueOptions { Name = "orders.invalid", Durable = true }
    ]
};

// Declare topology without publishing
await client.DeclareAsync(topology);
```

## Advanced topology

Topology declarations are optional, but when you supply them the toolkit will declare the configured exchange, queue, and bindings before publishing or consuming.

That makes the topology model the place to express broker-native queue and binding features such as:

- durable vs transient exchanges and queues
- auto-delete and exclusive queues
- queue bindings with explicit routing keys
- queue arguments such as TTL, max length, quorum queue settings, and dead-letter routing

Dead-letter queues are configured through normal RabbitMQ queue arguments rather than a special wrapper-specific API. For example:

```csharp
var topology = new RabbitMqTopologyOptions
{
    Exchange = new RabbitMqExchangeOptions
    {
        Name = "orders",
        Type = "direct",
        Durable = true
    },
    Queue = new RabbitMqQueueOptions
    {
        Name = "orders.primary",
        Durable = true,
        Arguments = new Dictionary<string, object?>
        {
            ["x-dead-letter-exchange"] = "orders.dlx",
            ["x-dead-letter-routing-key"] = "orders.dead",
            ["x-message-ttl"] = 30_000
        }
    },
    Bindings =
    [
        new RabbitMqQueueBindingOptions
        {
            ExchangeName = "orders",
            QueueName = "orders.primary",
            RoutingKey = "orders.created"
        }
    ]
};
```

Once a queue is configured with dead-lettering, returning `RabbitMqConsumeResult.Reject` or disabling requeue-on-failure will let RabbitMQ route the message to the broker-configured DLX/DLQ.

## Consumer behavior

`RabbitMqConsumerOptions` gives you the common RabbitMQ consumption controls without forcing you down a single acknowledgment model.

| Option | Purpose |
|---|---|
| `QueueName` | The queue to consume from |
| `ConsumerName` | Stable logical consumer identity used by reusable consumer channel providers |
| `ConsumerTag` | Explicit RabbitMQ consumer tag |
| `AutoAck` | Let RabbitMQ acknowledge deliveries automatically |
| `PrefetchCount` / `GlobalPrefetch` | Control QoS and delivery batching on the channel |
| `Exclusive` | Request an exclusive consumer |
| `NoLocal` | Ask the broker not to deliver publications from the same connection |
| `RequeueOnFailure` | On unhandled exceptions, choose whether the toolkit nacks with requeue or without requeue |
| `Arguments` | Pass broker-specific consumer arguments |
| `Topology` | Declare the required exchange/queue/bindings before consuming |

The handler result controls normal acknowledgment flow:

- `RabbitMqConsumeResult.Ack` acknowledges the delivery
- `RabbitMqConsumeResult.Reject` rejects it without requeue
- `RabbitMqConsumeResult.Requeue` nacks it and requeues it

Unhandled exceptions are also surfaced to the caller. When `AutoAck` is `false`, the toolkit will `BasicNack` using `RequeueOnFailure`.

Example with explicit prefetch and dead-letter-friendly failure behavior:

```csharp
await using var subscription = await client.SubscribeAsync<OrderCreated>
(
    new RabbitMqConsumerOptions
    {
        QueueName = "orders.primary",
        ConsumerName = "orders-worker",
        PrefetchCount = 32,
        RequeueOnFailure = false,
        Topology = topology
    },
    async (message, cancellationToken) =>
    {
        if (!await TryProcessAsync(message.Payload, cancellationToken))
        {
            return RabbitMqConsumeResult.Reject;
        }

        return RabbitMqConsumeResult.Ack;
    }
);
```

## Message properties and message context

Publishing supports the most commonly used AMQP properties through `RabbitMqMessageProperties`, including:

- `Headers`
- `CorrelationId`
- `MessageId`
- `ReplyTo`
- `Expiration`
- `Priority`
- `Type`
- `UserId`
- `AppId`
- `TimestampUtc`
- `ContentType` / `ContentEncoding`
- `Persistent`

Example:

```csharp
await client.PublishAsync
(
    new { OrderId = 42, Status = "Created" },
    new RabbitMqPublishOptions
    {
        ExchangeName = "orders",
        RoutingKey = "orders.created",
        Properties = new RabbitMqMessageProperties
        {
            CorrelationId = "order-42",
            MessageId = Guid.NewGuid().ToString("N"),
            ReplyTo = "orders.reply",
            Type = "orders.created",
            Headers = new Dictionary<string, object?>
            {
                ["tenant"] = "acme",
                ["source"] = "orders-api"
            },
            TimestampUtc = DateTimeOffset.UtcNow
        }
    }
);
```

On the consume side, each `RabbitMqReceivedMessage<T>` includes:

- `Payload` for the deserialized body
- `Body` for the raw bytes
- `Context` for broker metadata such as `DeliveryTag`, `Redelivered`, `Exchange`, `RoutingKey`, `CorrelationId`, `MessageId`, `ReplyTo`, `TimestampUtc`, and `Headers`

That lets you keep strongly typed handlers while still inspecting the transport-level metadata when you need it.

## Connection tuning

`RabbitMqConnectionOptions` intentionally exposes the connection settings that most applications end up tuning in production:

| Option | Purpose |
|---|---|
| `ConnectionUri` | Use a single URI instead of individual host settings |
| `HostName` / `Port` / `UserName` / `Password` / `VirtualHost` | Standard connection parameters |
| `ClientProvidedName` | Connection name shown in RabbitMQ management tooling |
| `AutomaticRecoveryEnabled` | Reconnect automatically after network interruption |
| `TopologyRecoveryEnabled` | Re-declare broker topology after reconnect |
| `ConsumerDispatchConcurrency` | Number of concurrent async consumer dispatches per connection |
| `RequestedHeartbeat` | Heartbeat interval |
| `RequestedConnectionTimeout` | Initial connection timeout |
| `NetworkRecoveryInterval` | Retry interval used by automatic recovery |

Example:

```csharp
var connection = new RabbitMqConnectionOptions
{
    ConnectionUri = "amqp://guest:guest@localhost:5672/",
    ClientProvidedName = "orders-api",
    AutomaticRecoveryEnabled = true,
    TopologyRecoveryEnabled = true,
    ConsumerDispatchConcurrency = 8,
    RequestedHeartbeat = TimeSpan.FromSeconds(30),
    RequestedConnectionTimeout = TimeSpan.FromSeconds(15),
    NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
};
```

## Client caching

`RabbitMqClientCache` provides thread-safe caching and reuse of `RabbitMqClient` instances keyed by the effective connection configuration (broker identity, credentials, and connection settings), avoiding duplicate connection management overhead when connecting dynamically across multiple services.

Clients returned from the cache are owned by the cache. Do not dispose individual clients; dispose the cache when finished.

```csharp
await using var cache = new RabbitMqClientCache();

// Resolve or create a client from connection options
var client1 = cache.GetOrCreate
(
    new RabbitMqConnectionOptions
    {
        HostName = "rabbit-cluster",
        Port = 5672,
        VirtualHost = "orders"
    }
);

// Or resolve directly by URI
var client2 = cache.GetOrCreate(new Uri("amqp://guest:guest@rabbit-cluster:5672/orders"));
```

## Pooled client cache

When broker URIs are only known at send time (for example a per-brand connection string), register a pooled client cache as a singleton and resolve clients by URI. Pool size and serializer are fixed on the cache instance and are not part of the cache key.

```csharp
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.ClientKit.ChannelPooling;
using RabbitMQ.ClientKit.Configuration;

services.AddSingleton(_ => PooledRabbitMqClientFactory.CreateCache
(
    new RabbitMqChannelPoolingOptions { ProducerPoolSize = 16 })
);

// At send time:
var client = cache.GetOrCreate(brandUri);
var result = await client.PublishBatchAsync
(
    messages,
    new RabbitMqPublishOptions
    {
        QueueName = "orders.created",
        PublisherConfirms = true
    }
);
```

Do not dispose the client returned by `GetOrCreate`; dispose the cache (typically via DI container shutdown).

## Core DI usage

`RabbitMQ.ClientKit` now includes `IServiceCollection` extensions for the built-in transient channel strategy.

```csharp
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.ClientKit;
using RabbitMQ.ClientKit.Configuration;

var services = new ServiceCollection();

services.AddRabbitMqClient
(
    new RabbitMqConnectionOptions
    {
        HostName = "localhost",
        UserName = "guest",
        Password = "guest",
        ClientProvidedName = "orders-api"
    }
);
```

You can then inject:

- `RabbitMqClient` for the convenience facade
- `RabbitMqPublisher` if you only publish
- `RabbitMqConsumer` if you only subscribe
- `IRabbitMqSerializer`, `IRabbitMqConnectionManager`, and the channel-provider abstractions if you need lower-level control

Example consumer-facing service:

```csharp
using RabbitMQ.ClientKit;
using RabbitMQ.ClientKit.Configuration;

public sealed class OrderPublisher(RabbitMqClient rabbitMqClient)
{
    public Task PublishCreatedAsync(int orderId, CancellationToken cancellationToken = default)
        => rabbitMqClient.PublishAsync
        (
            new { OrderId = orderId, Status = "Created" },
            new RabbitMqPublishOptions
            {
                QueueName = "orders.created",
                Topology = new RabbitMqTopologyOptions
                {
                    Queue = new RabbitMqQueueOptions
                    {
                        Name = "orders.created",
                        Durable = true
                    }
                }
            },
            cancellationToken
        );
}
```

## Pooling extension usage

```csharp
using RabbitMQ.ClientKit.ChannelPooling;
using RabbitMQ.ClientKit.Configuration;

var pooledClient = PooledRabbitMqClientFactory.Create
(
    new RabbitMqConnectionOptions
    {
        HostName = "localhost",
        ClientProvidedName = "payments-api"
    },
    new RabbitMqChannelPoolingOptions
    {
        ProducerPoolSize = 16
    }
);
```

## Pooling DI usage

`RabbitMQ.ClientKit.ChannelPooling` also includes `IServiceCollection` extensions that swap in the pooled producer and reusable consumer channel providers.

```csharp
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.ChannelPooling;

var services = new ServiceCollection();

services.AddPooledRabbitMqClient
(
    new RabbitMqConnectionOptions
    {
        HostName = "localhost",
        ClientProvidedName = "payments-api"
    },
    new RabbitMqChannelPoolingOptions
    {
        ProducerPoolSize = 16
    }
);
```

That registration still resolves `RabbitMqClient`, `RabbitMqPublisher`, and `RabbitMqConsumer`, but the underlying channel strategy changes to:

- a bounded producer-channel pool for publish-heavy workloads
- a reusable consumer-channel store keyed by consumer name, so long-lived consumer channels can be retained and reused between subscriptions

## Named DI registrations

You can also register multiple RabbitMQ clients side-by-side by name with .NET 8 keyed services.

```csharp
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.ClientKit;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.ChannelPooling;

var services = new ServiceCollection();

services.AddNamedRabbitMqClient
(
    "orders",
    new RabbitMqConnectionOptions
    {
        HostName = "orders-rabbit",
        ClientProvidedName = "orders-api"
    }
);

services.AddNamedPooledRabbitMqClient
(
    "payments",
    new RabbitMqConnectionOptions
    {
        HostName = "payments-rabbit",
        ClientProvidedName = "payments-api"
    },
    new RabbitMqChannelPoolingOptions
    {
        ProducerPoolSize = 16
    }
);
```

Resolve them either directly from the service provider:

```csharp
var provider = services.BuildServiceProvider();

var ordersClient = provider.GetRequiredKeyedService<RabbitMqClient>("orders");
var paymentsPublisher = provider.GetRequiredKeyedService<RabbitMqPublisher>("payments");
var paymentsConsumer = provider.GetRequiredKeyedService<RabbitMqConsumer>("payments");
```

Or inject them into application services with keyed DI:

```csharp
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.ClientKit;

public sealed class BillingPublisher([FromKeyedServices("payments")] RabbitMqPublisher publisher)
{
    public RabbitMqPublisher Publisher { get; } = publisher;
}
```

## appsettings.json registration

You can also register a default connection, named connections, and arrays of named producer/consumer definitions from configuration.

```json
{
  "RabbitMq": {
    "DefaultConnection": {
      "HostName": "default-rabbit",
      "UserName": "guest",
      "Password": "guest",
      "ClientProvidedName": "default-api"
    },
    "NamedConnections": [
      {
        "Name": "billing",
        "HostName": "billing-rabbit",
        "UserName": "guest",
        "Password": "guest",
        "ClientProvidedName": "billing-api"
      }
    ],
    "Producers": [
      {
        "Name": "orders-created",
        "Publish": {
          "QueueName": "orders.created"
        }
      },
      {
        "Name": "billing-charged",
        "ConnectionName": "billing",
        "Publish": {
          "ExchangeName": "billing",
          "RoutingKey": "charged"
        }
      }
    ],
    "Consumers": [
      {
        "Name": "orders-worker",
        "Subscribe": {
          "QueueName": "orders.created"
        }
      },
      {
        "Name": "billing-worker",
        "ConnectionName": "billing",
        "Subscribe": {
          "QueueName": "billing.charged",
          "ConsumerName": "billing-worker-channel"
        }
      }
    ]
  }
}
```

Register that section like this:

```csharp
using RabbitMQ.ClientKit;

builder.Services.AddRabbitMqClientKit(builder.Configuration.GetSection("RabbitMq"));
```

Then resolve configured endpoints through `IRabbitMqEndpointResolver`:

```csharp
using RabbitMQ.ClientKit;

public sealed class OrderPublisherService(IRabbitMqEndpointResolver rabbitMq)
{
    public Task PublishCreatedAsync(object message, CancellationToken cancellationToken = default)
    {
        var producer = rabbitMq.GetRequiredProducer("orders-created");
        return producer.Publisher.PublishAsync(message, producer.Options, cancellationToken);
    }
}
```

`ConnectionName` is optional on producers and consumers. When omitted, the definition uses `DefaultConnection`; when supplied, it resolves against one of the `NamedConnections`.

Configured endpoints resolve to richer handles than just raw options:

- `IRabbitMqEndpointResolver.GetRequiredProducer(name)` returns a `RabbitMqConfiguredProducer` with the producer registration, resolved publisher, resolved client, and bound `RabbitMqPublishOptions`
- `IRabbitMqEndpointResolver.GetRequiredConsumer(name)` returns a `RabbitMqConfiguredConsumer` with the consumer registration, resolved consumer, resolved client, and bound `RabbitMqConsumerOptions`

## Dynamic configuration package

If you want settings to come from a database or another reloadable source instead of being fixed at startup, use the optional `RabbitMQ.ClientKit.DynamicConfiguration` package.

It adds:

- `IRabbitMqDynamicConfigurationSource` for loading the latest snapshot
- `IRabbitMqDynamicEndpointResolver` for resolving the current client/producer/consumer
- `RabbitMqDynamicProducer` and `RabbitMqDynamicConsumer` handles that read the latest configuration on each use

Example registration with a custom source:

```csharp
using RabbitMQ.ClientKit.DynamicConfiguration;

builder.Services.AddSingleton<IRabbitMqDynamicConfigurationSource, DatabaseRabbitMqConfigurationSource>();
builder.Services.AddDynamicRabbitMqClientKit<DatabaseRabbitMqConfigurationSource>();
```

Or from a reloadable configuration section:

```csharp
using RabbitMQ.ClientKit.DynamicConfiguration;

builder.Services.AddDynamicRabbitMqClientKit(builder.Configuration.GetSection("RabbitMq"));
```

Example source shape:

```csharp
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.DynamicConfiguration;

public sealed class DatabaseRabbitMqConfigurationSource : IRabbitMqDynamicConfigurationSource
{
    public ValueTask<RabbitMqDynamicConfigurationSnapshot> GetConfigurationAsync(CancellationToken cancellationToken = default)
    {
        var defaultConnection = new RabbitMqConnectionOptions
        {
            HostName = "rabbit-a",
            ClientProvidedName = "orders-api"
        };

        var producers = new Dictionary<string, RabbitMqProducerRegistration>
        {
            ["orders-created"] = new(
                "orders-created",
                null,
                new RabbitMqPublishOptions
                {
                    QueueName = "orders.created"
                })
        };

        return ValueTask.FromResult(new RabbitMqDynamicConfigurationSnapshot(defaultConnection, producers: producers));
    }
}
```

Using the dynamic resolver:

```csharp
using RabbitMQ.ClientKit.DynamicConfiguration;

public sealed class OrderPublisherService(IRabbitMqDynamicEndpointResolver rabbitMq)
{
    public async Task PublishCreatedAsync(object message, CancellationToken cancellationToken = default)
    {
        var producer = rabbitMq.GetProducer("orders-created");
        await producer.PublishAsync(message, cancellationToken);
    }
}
```

Refreshing configuration:

```csharp
await rabbitMq.RefreshAsync(cancellationToken);
```

Future producer calls will use the latest loaded options and connection mapping. Existing active consumer subscriptions are not hot-swapped automatically; refresh affects future `SubscribeAsync(...)` calls.

For configuration binding, `Subscribe` is the preferred consumer section name. The older `Consume` section name is still accepted for compatibility.

The pooling package uses:

- a bounded producer-channel pool for publish-heavy workloads
- a reusable consumer-channel store keyed by consumer name, so long-lived consumer channels can be retained and reused between subscriptions

## Customization and extensibility

The README examples use the default JSON serializer and the built-in connection/channel implementations, but the public abstractions are intentionally open for customization.

Useful extension points include:

- `IRabbitMqSerializer` when you want a serializer other than the built-in `JsonRabbitMqSerializer`
- `IRabbitMqConnectionManager` when connection creation/lifetime needs to be controlled externally
- `IRabbitMqProducerChannelProvider` and `IRabbitMqConsumerChannelProvider` when you want a different channel-leasing strategy
- `IRabbitMqEndpointResolver` for resolving configured named endpoints from DI
- `IRabbitMqDynamicConfigurationSource` for loading dynamic configuration snapshots from a database, API, or custom provider
- `IRabbitMqDynamicClientActivator` when dynamically resolved clients need a custom construction strategy

For example, a custom serializer can be registered in DI before the higher-level services are consumed:

```csharp
public sealed class CustomSerializer : IRabbitMqSerializer
{
    public string ContentType => "application/json";

    public ReadOnlyMemory<byte> Serialize<T>(T message)
    {
        throw new NotImplementedException();
    }

    public T Deserialize<T>(ReadOnlyMemory<byte> body)
    {
        throw new NotImplementedException();
    }
}

var services = new ServiceCollection();
services.AddRabbitMqClient(new RabbitMqConnectionOptions
{
    HostName = "localhost"
});
services.AddSingleton<IRabbitMqSerializer, CustomSerializer>();
```

If you replace default services after calling `AddRabbitMqClient(...)`, use the usual .NET DI override patterns so the final registration order matches the behavior you want.

## Testing

Unit tests can run entirely in-process because the client toolkit surface is mostly channel-management and serialization logic.

For integration tests, you will want:

- Docker Desktop, Podman, or another way to run a disposable RabbitMQ broker
- a RabbitMQ image such as `rabbitmq:management`
- ports `5672` for AMQP and optionally `15672` for the management UI
- deterministic test credentials and vhost setup
- a test fixture that creates unique queue and exchange names per test run

A simple starting point is:

```bash
docker run --rm -d --name rabbitmq-test -p 5672:5672 -p 15672:15672 rabbitmq:management
```

Once that is available, integration tests should verify publish/consume round trips, topology declaration, ack/reject/requeue flows, and pooled channel behavior against the real broker.

The test project now includes container-backed coverage for those scenarios:

- integration tests are marked with `Trait("Category", "Integration")` and auto-skip when neither Docker nor Podman is available
- load tests are marked with `Trait("Category", "Load")` and auto-skip when neither Docker nor Podman is available
- the load suite defaults to `1000` messages and a `60` second completion window, configurable via `RABBITMQ_CLIENTKIT_LOAD_MESSAGE_COUNT` and `RABBITMQ_CLIENTKIT_LOAD_TIMEOUT_SECONDS`
- the load suite now covers both the default transient channel strategy and the pooled channel strategy with matched publish/consume scenarios
- `RabbitMqLoadTests.TransientAndPooledClients_ReportComparativeMetrics` runs both strategies back-to-back and writes comparable publish and end-to-end throughput ratios to the xUnit test output

Examples:

```bash
dotnet test --filter "Category=Integration"
dotnet test --filter "Category=Load"
```

The load scenarios are:

| Test | Strategy | Output |
|---|---|---|
| `RabbitMqLoadTests.TransientClient_PublishesAndConsumesConfiguredBurst` | Core `AddRabbitMqClient(...)` transient producer/consumer channels | Publish duration and end-to-end throughput |
| `RabbitMqLoadTests.PooledClient_PublishesAndConsumesConfiguredBurst` | `AddPooledRabbitMqClient(...)` pooled producer/reusable consumer channels | Publish duration and end-to-end throughput |
| `RabbitMqLoadTests.TransientAndPooledClients_ReportComparativeMetrics` | Runs both strategies under the same settings | Per-scenario metrics plus pooled/transient throughput ratios |

## Performance snapshot

Current local baseline from `RabbitMqLoadTests.TransientAndPooledClients_ReportComparativeMetrics` with `RABBITMQ_CLIENTKIT_LOAD_MESSAGE_COUNT=100000`:

| Strategy | Publish duration | Publish throughput | End-to-end duration | End-to-end throughput | Notes |
|---|---|---:|---|---:|---|
| Transient/default | `1.928s` | `51,876 msg/s` | `5.349s` | `18,695 msg/s` | Windows 11, Ryzen 7 5800X, 32 GB RAM, Docker 29.8.2, `rabbitmq:3.13-management`, `dotnet test --no-build --filter "Category=Load" --logger "console;verbosity=detailed"` |
| Pooled | `1.556s` | `64,264 msg/s` | `5.567s` | `17,963 msg/s` | Same host and broker settings as transient/default |

In this comparative run, pooled throughput was about **`1.24x` faster for publish throughput** and **`0.96x` of the transient/default end-to-end throughput**.

The isolated single-scenario stress runs from the same 100k-message suite were still favorable to pooling end-to-end:

| Scenario run | Publish throughput | End-to-end throughput |
|---|---:|---:|
| `TransientClient_PublishesAndConsumesConfiguredBurst` | `43,379 msg/s` | `17,632 msg/s` |
| `PooledClient_PublishesAndConsumesConfiguredBurst` | `64,419 msg/s` | `18,600 msg/s` |

Treat these as reproducible local baselines rather than formal benchmarks: the numbers still include test-host, test-runner, and broker-container overhead, so absolute throughput and even the relative end-to-end winner can vary by machine and run order.

## License

Licensed under the Apache License, Version 2.0. See `LICENSE` for details.

Maintained by Colin Campbell.
