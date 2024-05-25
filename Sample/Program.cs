using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;

class Program
{
    public static ConcurrentDictionary<long, (DateTimeOffset sentAt, long batch)> sent = new ConcurrentDictionary<long, (DateTimeOffset, long)>();
    public static ConcurrentDictionary<long, (DateTimeOffset sentAt, long batch)> received = new ConcurrentDictionary<long, (DateTimeOffset, long)>();
    static long receiveCount;
    static long sentCount;

    public static ConcurrentBag<StatsEntry> stats = new ConcurrentBag<StatsEntry>();
    static string EndpointName = "Sqs.Concurrency.Tests";
    static string QueueName = EndpointName.Replace(".", "-");

    public struct StatsEntry
    {
        public long Id;
        public DateTimeOffset ReceivedAt;
        public DateTimeOffset SentAt;
        public long Batch;

        public StatsEntry(long id, long batch, DateTimeOffset sentAt, DateTimeOffset receivedAt)
        {
            Id = id;
            SentAt = sentAt;
            ReceivedAt = receivedAt;
            Batch = batch;
        }
    }

    static async Task Main()
    {
        AWSConfigs.InitializeCollections = true;

        Console.Title = EndpointName;

        Console.WriteLine("Purging queues");

        string queueUrl = null;

        var client = new AmazonSQSClient(new AmazonSQSConfig());

        try
        {
            queueUrl = await CreateQueue(client, QueueName);
        }
        catch (QueueNameExistsException)
        {
        }

        try
        {
            var inputQueue = await client.GetQueueUrlAsync(QueueName);
            await client.PurgeQueueAsync(inputQueue.QueueUrl);
        }
        catch (PurgeQueueInProgressException)
        {
        }
        catch (QueueDoesNotExistException)
        {
        }

        Console.WriteLine("Queues purged.");
        Console.ReadLine();

        var concurrencyLevel = 4;
        var testTimeout = TimeSpan.FromMinutes(2);
        var cts = new CancellationTokenSource(testTimeout);
        var consumerCts = new CancellationTokenSource(testTimeout.Add(TimeSpan.FromSeconds(40)));
        var syncher = new TaskCompletionSource<bool>();

        var consumerTasks = new List<Task>();

        for (var i = 0; i < concurrencyLevel; i++)
        {
            consumerTasks.Add(ConsumeMessage(client, queueUrl, i, consumerCts.Token));
        }

        var sendTask = Task.Run(() => Sending(client, queueUrl, cts.Token, syncher), CancellationToken.None);
        var checkTask = Task.Run(() => DumpCurrentState(cts.Token), CancellationToken.None);

        await Task.WhenAll(consumerTasks.Union(new[]
        {
            sendTask, checkTask,
            CheckState(syncher)
        }));
   

        client.Dispose();

        Console.WriteLine("Press any key to exit.");
        Console.ReadKey();
    }

    static async Task Sending(IAmazonSQS client, string queueUrl, CancellationToken token, TaskCompletionSource<bool> syncher)
    {
        var attempt = 0L;
        var batchSend = 0L;
        var random = new Random();

        try
        {
            while (!token.IsCancellationRequested)
            {
                var numberOfMessagesToSend = random.Next(1, 256);
                var sendTask = new List<Task>(numberOfMessagesToSend);
                batchSend++;
                for (var i = 0; i < numberOfMessagesToSend; i++)
                {
                    async Task SendMessage(long localAttempt, long batchNr)
                    {
                        var now = DateTimeOffset.UtcNow;


                        try
                        {
                            sent.AddOrUpdate(localAttempt, (now, batchNr), (_,__) => (now, batchNr));

                            await client.SendMessageAsync(new SendMessageRequest(queueUrl, $"{localAttempt};{now.ToUnixTimeMilliseconds()};{batchNr}"), CancellationToken.None);

                            Interlocked.Increment(ref sentCount);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Sending error {ex.Message}. Aborting");
                            throw;
                        }
                    }

                    sendTask.Add(SendMessage(attempt++, batchSend));
                }

                await Task.WhenAll(sendTask);
                await Task.Delay(TimeSpan.FromSeconds(random.Next(1, 2)), token);
            }
        }
        catch (OperationCanceledException)
        {
            // ignore
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Sending error {ex.Message}. Aborting");
        }
        finally
        {
            Console.WriteLine();
            Console.WriteLine("--- Sending ---");
            Console.WriteLine("Done sending...");
            Console.WriteLine("--- Sending ---");

            await File.WriteAllTextAsync("sent.txt", $"{DateTimeOffset.UtcNow}: Attempts {attempt} / Sent {Interlocked.Read(ref sentCount)} / Batches {batchSend}", CancellationToken.None);

            syncher.TrySetResult(true);
        }
    }

    static async Task ConsumeMessage(IAmazonSQS sqsClient, string queueUrl, int pumpNumber, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                var receiveResult = await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
                    {
                        MaxNumberOfMessages = 10,
                        QueueUrl = queueUrl,
                        WaitTimeSeconds = 20,
                        AttributeNames = new List<string>
                        {
                            "SentTimestamp",
                            "ApproximateFirstReceiveTimestamp",
                            "ApproximateReceiveCount"
                        },
                    },
                    token).ConfigureAwait(false);

                var concurrentReceives = new List<Task>(receiveResult.Messages.Count);
                foreach (var message in receiveResult.Messages)
                {
                    concurrentReceives.Add(Consume(sqsClient, queueUrl, message));
                }

                await Task.WhenAll(concurrentReceives)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - cancelled");
            }
            catch (OverLimitException)
            {
                Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - throttled");
            }
            catch (AmazonSQSException)
            {
                Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - error");
            }
        }
    }

    static async Task Consume(IAmazonSQS sqsClient, string queueUrl, Message message)
    {
        Interlocked.Increment(ref receiveCount);

        var sentTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(message.Attributes["SentTimestamp"]));
        var firstReceiveTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(message.Attributes["ApproximateFirstReceiveTimestamp"]));

        if (Convert.ToInt32(message.Attributes["ApproximateReceiveCount"]) > 1)
        {
            firstReceiveTimestamp = DateTimeOffset.UtcNow;
        }

        var elapsed = firstReceiveTimestamp - sentTimestamp;

        var content = message.Body.Split(';');
        var attempt = Convert.ToInt64(content[0]);
        var batch = Convert.ToInt64(content[2]);
        var sentAt = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(content[1]));

        received.AddOrUpdate(attempt, (sentAt, batch), (_, __) => (sentAt, batch));
        stats.Add(new StatsEntry(attempt, batch, sentAt, DateTime.UtcNow));

        await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle, CancellationToken.None).ConfigureAwait(false);
    }

    static async Task DumpCurrentState(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            Console.Clear();
            Console.WriteLine("--- Current state ---");

            var allNotReceived = sent.ToArray().OrderBy(x => x.Key).Select(x => x.Key).Except(received.ToArray().OrderBy(x => x.Key).Select(x => x.Key).ToArray()).ToArray();

            if (allNotReceived.Length != 0)
            {
                foreach (var entry in allNotReceived)
                {
                    Console.WriteLine($"'{entry}'");
                }
            }
            else
            {
                Console.WriteLine("empty.");
            }

            Console.WriteLine("--- Current state ---");

            await WriteStats();

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(10), token);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    static async Task<string> CreateQueue(AmazonSQSClient client, string queueName)
    {
        var sqsRequest = new CreateQueueRequest
        {
            QueueName = queueName
        };
        var createQueueResponse = await client.CreateQueueAsync(sqsRequest).ConfigureAwait(false);
        var queueUrl = createQueueResponse.QueueUrl;
        var sqsAttributesRequest = new SetQueueAttributesRequest
        {
            QueueUrl = queueUrl
        };
        sqsAttributesRequest.Attributes.Add(QueueAttributeName.MessageRetentionPeriod,
            TimeSpan.FromDays(4).TotalSeconds.ToString());

        await client.SetQueueAttributesAsync(sqsAttributesRequest).ConfigureAwait(false);
        return queueUrl;
    }

    static async Task CheckState(TaskCompletionSource<bool> syncher)
    {
        await syncher.Task;

        while (true)
        {
            Console.Clear();
            Console.WriteLine("--- Not yet received ---");

            var allNotReceived = sent.ToArray().OrderBy(x => x.Key).Select(x => x.Key).Except(received.ToArray().OrderBy(x => x.Key).Select(x => x.Key).ToArray()).ToArray();

            if (allNotReceived.Length == 0)
            {
                break;
            }

            foreach (var entry in allNotReceived)
            {
                Console.WriteLine($"'{entry}' to be received.'");
            }

            Console.WriteLine("--- Not yet received ---");

            await File.WriteAllTextAsync("received.txt", $"{DateTimeOffset.UtcNow}: {Interlocked.Read(ref receiveCount)}", CancellationToken.None);

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
            }
            catch (OperationCanceledException)
            {
            }
        }

        await WriteStats();

        Console.WriteLine();
        Console.WriteLine("--- Summary ---");
        Console.WriteLine("Received everything. Done");
        Console.WriteLine("--- Summary ---");
    }

    static async Task WriteStats()
    {
        using (var writer = new StreamWriter("stats.csv", false))
        {
            await writer.WriteLineAsync($"{nameof(StatsEntry.Id)},{nameof(StatsEntry.Batch)},{nameof(StatsEntry.SentAt)},{nameof(StatsEntry.ReceivedAt)},Delta");
            foreach (var statsEntry in stats.OrderBy(s => s.SentAt))
            {
                var delta = statsEntry.ReceivedAt - statsEntry.SentAt;
                await writer.WriteLineAsync($"{statsEntry.Id},{statsEntry.Batch},{statsEntry.SentAt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)},{statsEntry.ReceivedAt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)},{delta.ToString(@"hh\:mm\:ss\.fff", CultureInfo.InvariantCulture)}");
            }

            await writer.FlushAsync();
            writer.Close();
        }
    }
}