using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace SimpleBrokeredMessaging.Sender
{
    class SenderConsole
    {
        static string QueuePath = "queue-test";

        static async Task Main(string[] args)
        {
            string busSecret = "YOUR_CONNECTION_STRING";
            ServiceBusClient client = new ServiceBusClient(busSecret);

            //Create a queue client
            ServiceBusSender sender = client.CreateSender(QueuePath);

            //Send some messages
            for (int i = 0; i < 10; i++)
            {
                string content = $"Message: {i}";
                ServiceBusMessage message = new ServiceBusMessage(Encoding.UTF8.GetBytes(content));
                await sender.SendMessageAsync(message);

                Console.WriteLine($"Sent message with id: {i}");
            }

            // Close the client
            sender.CloseAsync().Wait();

            Console.WriteLine("Finished sending messages");
            Console.ReadLine();
        }
    }
}
