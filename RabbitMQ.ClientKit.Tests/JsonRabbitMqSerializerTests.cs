using System.Text;
using RabbitMQ.ClientKit.Serialization;
using RabbitMQ.ClientKit.Tests.Support;

namespace RabbitMQ.ClientKit.Tests;

public sealed class JsonRabbitMqSerializerTests
{
    [Fact]
    public void SerializeAndDeserialize_RoundTripsPayload()
    {
        var serializer = new JsonRabbitMqSerializer();
        var message = new TestMessage { Value = "hello" };

        var body = serializer.Serialize(message);
        var roundTripped = serializer.Deserialize<TestMessage>(body);

        Assert.Equal("hello", roundTripped.Value);
        Assert.Equal("application/json", serializer.ContentType);
    }

    [Fact]
    public void Deserialize_ThrowsForNullPayload()
    {
        var serializer = new JsonRabbitMqSerializer();
        var body = Encoding.UTF8.GetBytes("null");

        var exception = Assert.Throws<InvalidOperationException>(() => serializer.Deserialize<TestMessage>(body));

        Assert.Contains(nameof(TestMessage), exception.Message, StringComparison.Ordinal);
    }
}
