using System.Text.Json;

namespace RabbitMQ.ClientKit.Serialization;

/// <summary>
/// Serializes payloads with <see cref="JsonSerializer" /> using UTF-8 JSON.
/// </summary>
public sealed class JsonRabbitMqSerializer(JsonSerializerOptions? serializerOptions = null) : IRabbitMqSerializer
{
    private readonly JsonSerializerOptions _serializerOptions = serializerOptions ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);

    /// <inheritdoc />
    public string ContentType => "application/json";

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Serialize<T>(T message) => JsonSerializer.SerializeToUtf8Bytes(message, _serializerOptions);

    /// <inheritdoc />
    public T Deserialize<T>(ReadOnlyMemory<byte> body)
    {
        var value = JsonSerializer.Deserialize<T>(body.Span, _serializerOptions);
        
        return value ?? throw new InvalidOperationException($"The message body could not be deserialized to {typeof(T).FullName}.");
    }
}
