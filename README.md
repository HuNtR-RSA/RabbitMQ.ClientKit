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

await client.PublishAsync(
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
    });

await using var subscription = await client.SubscribeAsync<dynamic>(
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
    });
```

## Batch publishing

RabbitMQ.Client v7+ no longer exposes the old bulk-publish API, so `RabbitMQ.ClientKit` batch publishing reuses a single leased channel and publishes each message individually.

```csharp
await client.PublishBatchAsync(
    new[]
    {
        new { OrderId = 42, Status = "Created" },
        new { OrderId = 43, Status = "Created" }
    },
    new RabbitMqPublishOptions
    {
        QueueName = "orders.created"
    });
```

Batch publishing uses one shared `RabbitMqPublishOptions` instance for the batch.

## Core DI usage

`RabbitMQ.ClientKit` now includes `IServiceCollection` extensions for the built-in transient channel strategy.

```csharp
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.ClientKit;
using RabbitMQ.ClientKit.Configuration;

var services = new ServiceCollection();

services.AddRabbitMqClient(new RabbitMqConnectionOptions
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest",
    ClientProvidedName = "orders-api"
});
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
    public Task PublishCreatedAsync(int orderId, CancellationToken cancellationToken = default) =>
        rabbitMqClient.PublishAsync(
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
            cancellationToken);
}
```

## Pooling extension usage

```csharp
using RabbitMQ.ClientKit.ChannelPooling;
using RabbitMQ.ClientKit.Configuration;

var pooledClient = PooledRabbitMqClientFactory.Create(
    new RabbitMqConnectionOptions
    {
        HostName = "localhost",
        ClientProvidedName = "payments-api"
    },
    new RabbitMqChannelPoolingOptions
    {
        ProducerPoolSize = 16
    });
```

## Pooling DI usage

`RabbitMQ.ClientKit.ChannelPooling` also includes `IServiceCollection` extensions that swap in the pooled producer and reusable consumer channel providers.

```csharp
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.ClientKit.Configuration;
using RabbitMQ.ClientKit.ChannelPooling;

var services = new ServiceCollection();

services.AddPooledRabbitMqClient(
    new RabbitMqConnectionOptions
    {
        HostName = "localhost",
        ClientProvidedName = "payments-api"
    },
    new RabbitMqChannelPoolingOptions
    {
        ProducerPoolSize = 16
    });
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

services.AddNamedRabbitMqClient(
    "orders",
    new RabbitMqConnectionOptions
    {
        HostName = "orders-rabbit",
        ClientProvidedName = "orders-api"
    });

services.AddNamedPooledRabbitMqClient(
    "payments",
    new RabbitMqConnectionOptions
    {
        HostName = "payments-rabbit",
        ClientProvidedName = "payments-api"
    },
    new RabbitMqChannelPoolingOptions
    {
        ProducerPoolSize = 16
    });
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
        "Consume": {
          "QueueName": "orders.created"
        }
      },
      {
        "Name": "billing-worker",
        "ConnectionName": "billing",
        "Consume": {
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

The pooling package uses:

- a bounded producer-channel pool for publish-heavy workloads
- a reusable consumer-channel store keyed by consumer name, so long-lived consumer channels can be retained and reused between subscriptions

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

Current local baseline from `RabbitMqLoadTests.TransientAndPooledClients_ReportComparativeMetrics` with `RABBITMQ_CLIENTKIT_LOAD_MESSAGE_COUNT=50000`:

| Strategy | Publish duration | Publish throughput | End-to-end duration | End-to-end throughput | Notes |
|---|---|---:|---|---:|---|
| Transient/default | `1.385s` | `36,111 msg/s` | `3.226s` | `15,497 msg/s` | Windows 11, Ryzen 7 5800X, 32 GB RAM, Docker 29.8.1, `rabbitmq:3.13-management`, `dotnet test --no-build --filter "FullyQualifiedName~RabbitMqLoadTests.TransientAndPooledClients_ReportComparativeMetrics" --logger "console;verbosity=detailed"` |
| Pooled | `0.925s` | `54,063 msg/s` | `2.963s` | `16,873 msg/s` | Same host and broker settings as transient/default |

In this run, pooled throughput was about **`1.50x` faster for publish throughput** and **`1.09x` faster end-to-end** than the transient/default strategy.

Treat this as a reproducible local baseline rather than a formal benchmark: the numbers still include test-host and broker-container overhead, so absolute throughput will vary by machine and runtime configuration.

## License

Licensed under the Apache License, Version 2.0. See `LICENSE` for details.

Maintained by Colin Campbell.
