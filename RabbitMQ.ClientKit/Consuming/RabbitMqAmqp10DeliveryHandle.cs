using RabbitMQ.AMQP.Client;
using RabbitMQ.ClientKit.Configuration;

namespace RabbitMQ.ClientKit.Consuming;

/// <summary>
/// Settles an AMQP 1.0 delivery through <see cref="IContext"/>, including delayed retry via
/// <c>x-opt-delivery-time</c>.
/// </summary>
public sealed class RabbitMqAmqp10DeliveryHandle(IContext context) : IRabbitMqDeliveryHandle
{
    private readonly IContext _context = context ?? throw new ArgumentNullException(nameof(context));
    private int _settled;

    /// <inheritdoc />
    public bool IsSettled => Volatile.Read(ref _settled) == 1;

    /// <inheritdoc />
    public Task PublishReplacementAsync<T>(T message, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException(RabbitMqAmqpProtocolMismatch.ReplacementOnAmqp10);

    /// <inheritdoc />
    public Task PublishReplacementAsync(ReadOnlyMemory<byte> body, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException(RabbitMqAmqpProtocolMismatch.ReplacementOnAmqp10);

    /// <inheritdoc />
    public Task AckAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _context.Accept();
        MarkSettled();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task RejectAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _context.Discard();
        MarkSettled();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task NackAsync(bool requeue = true, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (requeue)
        {
            _context.Requeue();
        }
        else
        {
            _context.Discard();
        }

        MarkSettled();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ReplaceAndAckAsync<T>(T message, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException(RabbitMqAmqpProtocolMismatch.ReplacementOnAmqp10);

    /// <inheritdoc />
    public Task ReplaceAndAckAsync(ReadOnlyMemory<byte> body, RabbitMqPublishOptions options, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException(RabbitMqAmqpProtocolMismatch.ReplacementOnAmqp10);

    /// <inheritdoc />
    public Task DelayRetryAsync(TimeSpan delay, bool deliveryFailed, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _context.DelayedRetry(delay, deliveryFailed);
        MarkSettled();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task RequeueAsync(IReadOnlyDictionary<string, object> annotations, bool deliveryFailed, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(annotations);
        cancellationToken.ThrowIfCancellationRequested();
        _context.Requeue(ToAnnotationDictionary(annotations), deliveryFailed);
        MarkSettled();
        return Task.CompletedTask;
    }

    private static Dictionary<string, object> ToAnnotationDictionary(IReadOnlyDictionary<string, object> annotations)
    {
        var copy = new Dictionary<string, object>(annotations.Count);
        foreach (var pair in annotations)
        {
            copy[pair.Key] = pair.Value;
        }

        return copy;
    }

    private void MarkSettled() => Interlocked.Exchange(ref _settled, 1);
}
