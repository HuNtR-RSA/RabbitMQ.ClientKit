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
    "Connection": {
      "HostName": "default-rabbit",
      "UserName": "guest",
      "Password": "guest",
      "ClientProvidedName": "default-api"
    },
    "Connections": [
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

`ConnectionName` is optional on producers and consumers. When omitted, the definition uses the default `Connection`; when supplied, it resolves against one of the named `Connections`.

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

Maintained by Colin Campbell.
