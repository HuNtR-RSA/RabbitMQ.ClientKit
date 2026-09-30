using System.Diagnostics;
using Xunit;

namespace RabbitMQ.ClientKit.Tests.Support;

public sealed class ContainerRuntimeFactAttribute : FactAttribute
{
    public ContainerRuntimeFactAttribute()
    {
        Skip = ContainerRuntimeAvailability.SkipReason;
    }
}

public sealed class RabbitMqLoadFactAttribute : FactAttribute
{
    public RabbitMqLoadFactAttribute()
    {
        Skip = ContainerRuntimeAvailability.SkipReason;
    }
}

internal static class ContainerRuntimeAvailability
{
    private static readonly Lazy<string?> SkipReasonFactory = new(DetectSkipReason, LazyThreadSafetyMode.ExecutionAndPublication);

    public static string? SkipReason => SkipReasonFactory.Value;

    private static string? DetectSkipReason()
    {
        foreach (var runtimeName in new[] { "docker", "podman" })
        {
            if (CanReachContainerRuntime(runtimeName))
            {
                return null;
            }
        }

        return "Container-backed RabbitMQ tests require Docker or Podman to be installed and running.";
    }

    private static bool CanReachContainerRuntime(string runtimeName)
    {
        try
        {
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = runtimeName,
                    Arguments = "version",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                }
            };

            if (!process.Start())
            {
                return false;
            }

            if (process.WaitForExit(milliseconds: 5000))
            {
                return process.ExitCode == 0;
            }
            
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch
            {
                // Best-effort cleanup for a timed-out probe.
            }

            return false;

        }
        catch
        {
            return false;
        }
    }
}

internal static class RabbitMqLoadTestSettings
{
    public const string MessageCountEnvironmentVariable = "RABBITMQ_CLIENTKIT_LOAD_MESSAGE_COUNT";
    public const string TimeoutSecondsEnvironmentVariable = "RABBITMQ_CLIENTKIT_LOAD_TIMEOUT_SECONDS";

    public static int MessageCount => ReadPositiveInt(MessageCountEnvironmentVariable, 1000);

    public static TimeSpan Timeout => TimeSpan.FromSeconds(ReadPositiveInt(TimeoutSecondsEnvironmentVariable, 60));

    private static int ReadPositiveInt(string environmentVariableName, int defaultValue)
    {
        var rawValue = Environment.GetEnvironmentVariable(environmentVariableName);
        return int.TryParse(rawValue, out var parsedValue) && parsedValue > 0
            ? parsedValue
            : defaultValue;
    }
}
