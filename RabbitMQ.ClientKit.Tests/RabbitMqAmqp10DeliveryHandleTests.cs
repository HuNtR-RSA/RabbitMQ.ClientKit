using NSubstitute;
using RabbitMQ.AMQP.Client;
using RabbitMQ.ClientKit.Consuming;

namespace RabbitMQ.ClientKit.Tests;

public sealed class RabbitMqAmqp10DeliveryHandleTests
{
    [Fact]
    public async Task DelayRetryAsync_CallsContextDelayedRetryAndSettles()
    {
        var context = Substitute.For<IContext>();
        var handle = new RabbitMqAmqp10DeliveryHandle(context);

        await handle.DelayRetryAsync(TimeSpan.FromSeconds(2), deliveryFailed: true);

        Assert.True(handle.IsSettled);
        context.Received(1).DelayedRetry(TimeSpan.FromSeconds(2), true);
    }

    [Fact]
    public async Task RequeueAsync_CallsContextRequeueWithCopiedAnnotations()
    {
        var context = Substitute.For<IContext>();
        var handle = new RabbitMqAmqp10DeliveryHandle(context);
        var deliveryTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2_000;
        IReadOnlyDictionary<string, object> annotations = new Dictionary<string, object>
        {
            ["x-opt-delivery-time"] = deliveryTime
        };

        await handle.RequeueAsync(annotations, deliveryFailed: true);

        Assert.True(handle.IsSettled);
        context.Received(1).Requeue(
            Arg.Is<Dictionary<string, object>>(dict =>
                dict.ContainsKey("x-opt-delivery-time") &&
                Equals(dict["x-opt-delivery-time"], deliveryTime)),
            true);
    }
}
