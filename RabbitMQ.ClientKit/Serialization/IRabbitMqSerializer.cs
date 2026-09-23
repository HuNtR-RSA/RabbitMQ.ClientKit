namespace RabbitMQ.ClientKit.Serialization;

/// <summary>
/// Serializes and deserializes message payloads for RabbitMQ operations.
/// </summary>
public interface IRabbitMqSerializer
{
    /// <summary>
    /// Gets the MIME content type emitted for serialized messages.
    /// </summary>
    string ContentType { get; }

    /// <summary>
    /// Serializes a payload into a binary body.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="message">The payload to serialize.</param>
    /// <returns>The serialized message body.</returns>
    ReadOnlyMemory<byte> Serialize<T>(T message);

    /// <summary>
    /// Deserializes a binary body into a payload.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="body">The message body to deserialize.</param>
    /// <returns>The deserialized payload.</returns>
    T Deserialize<T>(ReadOnlyMemory<byte> body);
}
