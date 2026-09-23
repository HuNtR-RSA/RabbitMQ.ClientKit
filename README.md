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
