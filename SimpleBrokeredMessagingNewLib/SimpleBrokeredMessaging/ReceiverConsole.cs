using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace SimpleBrokeredMessaging
{
    class ReceiverConsole
    {
        static string QueuePath = "queue-test";

        static async Task Main(string[] args)
        {
            // Create a queue client
            string busSecret = "YOUR_CONNECTION_STRING";
            ServiceBusClient client = new ServiceBusClient(busSecret);

            // Create a message handler to receive messages
            ServiceBusProcessorOptions messageProcessorOptions = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 1
            };

            ServiceBusProcessor processor = client.CreateProcessor(QueuePath, messageProcessorOptions);
            processor.ProcessMessageAsync += ProcessMessagesAsyc;
            processor.ProcessErrorAsync += HandleExceptionsAsync;

            await processor.StartProcessingAsync();

            // Note that we will receive the messages after this next block executes.
            // The messages are received on a different thread.
            Console.WriteLine("Finished processing messages. Press enter to exit");
            Console.ReadLine();

            // Close the client
            await client.DisposeAsync();
        }

        private static async Task ProcessMessagesAsyc(ProcessMessageEventArgs args)
        {
            string content = args.Message.Body.ToString();
            Console.WriteLine($"Recieved message: {content}");
            await args.CompleteMessageAsync(args.Message);
        }

        private static Task HandleExceptionsAsync(ProcessErrorEventArgs args)
        {
            // the error source tells me at what point in the processing an error occurred
            Console.WriteLine(args.ErrorSource);
            // the fully qualified namespace is available
            Console.WriteLine(args.FullyQualifiedNamespace);
            // as well as the entity path
            Console.WriteLine(args.EntityPath);
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }
    }
}
