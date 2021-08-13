using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace SimpleBrokeredMessageing.ChatConsole
{
    class ChatApplication
    {
        static string TopicPath = "topictestpath";


        static async Task Main(string[] args)
        {
            Console.WriteLine("Enter name: ");
            string username = Console.ReadLine();
            string busSecret = "YOUR_CONNECTION_STRING";

            ServiceBusAdministrationClient manager = new ServiceBusAdministrationClient(busSecret);
            ServiceBusClient client = new ServiceBusClient(busSecret);

            var topicOptions = new CreateTopicOptions(TopicPath)
            {
                AutoDeleteOnIdle = TimeSpan.FromDays(7),
                DefaultMessageTimeToLive = TimeSpan.FromDays(2),
                DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(1),
                EnableBatchedOperations = false,
                EnablePartitioning = false,
                MaxSizeInMegabytes = 2048,
                RequiresDuplicateDetection = true
            };

            topicOptions.AuthorizationRules.Add(new SharedAccessAuthorizationRule(
                "allClaims",
                new[] { AccessRights.Manage, AccessRights.Send, AccessRights.Listen }));

            // Create a topic if it does not exist
            if (!manager.TopicExistsAsync(TopicPath).Result)
                manager.CreateTopicAsync(TopicPath).Wait();

            string subscriptionName = username;
            var subscriptionOptions = new CreateSubscriptionOptions(TopicPath, subscriptionName)
            {
                AutoDeleteOnIdle = TimeSpan.FromMinutes(5),
                DefaultMessageTimeToLive = TimeSpan.FromDays(2),
                EnableBatchedOperations = false
            };
            if (!manager.SubscriptionExistsAsync(TopicPath, username).Result)
                await manager.CreateSubscriptionAsync(subscriptionOptions);

            ServiceBusProcessorOptions messageProcessorOptions = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 1
            };

            SubscriptionProperties sub = await manager.GetSubscriptionAsync(TopicPath, username);
            ServiceBusProcessor processor = client.CreateProcessor(TopicPath, username, messageProcessorOptions);
            processor.ProcessMessageAsync += ProcessMessagesAsyc;
            processor.ProcessErrorAsync += ExceptionReceivedHandler;
            await processor.StartProcessingAsync();

            // Send messages to announce yourself
            ServiceBusSender sender = client.CreateSender(TopicPath);
            SendMessage("Has entered the room...", username, sender);

            while (true)
            {
                string text = Console.ReadLine();
                if (text.Equals("exit")) break;

                // Send a chat message
                ServiceBusMessage chatMessage = new ServiceBusMessage(Encoding.UTF8.GetBytes(text));
                chatMessage.ApplicationProperties.Add("username", username);
                sender.SendMessageAsync(chatMessage).Wait();
            }

            // Send a message to say you are leaving
            SendMessage("Has left the building...", username, sender);

            // Close the clients
            sender.CloseAsync().Wait();
        }

        private static void SendMessage(string message, string username, ServiceBusSender sender)
        {
            ServiceBusMessage helloMessage = new ServiceBusMessage(Encoding.UTF8.GetBytes(message));
            helloMessage.ApplicationProperties.Add("username", username);

            //Create a queue client
            sender.SendMessageAsync(helloMessage).Wait();
        }

        private static async Task ProcessMessagesAsyc(ProcessMessageEventArgs args)
        {
            string content = args.Message.Body.ToString();
            string username = args.Message.ApplicationProperties["username"].ToString();
            Console.WriteLine($"{username}> {content}");

            await args.CompleteMessageAsync(args.Message);
        }

        private static Task ExceptionReceivedHandler(ProcessErrorEventArgs args)
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
