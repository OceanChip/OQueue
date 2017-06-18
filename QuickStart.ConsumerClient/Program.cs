using OceanChip.Common.Components;
using OceanChip.Common.Configurations;
using OceanChip.Common.Socketing;
using OceanChip.Common.Utilities;
using OceanChip.Queue.Clients.Consumers;
using OceanChip.Queue.Configurations;
using OceanChip.Queue.Protocols;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using OCommonConfiguration = OceanChip.Common.Configurations.Configuration;

namespace QuickStart.ConsumerClient
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();
            Console.ReadLine();
        }

        static void InitializeEQueue()
        {
            OCommonConfiguration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler()
                .RegisterQueueComponents();

            var clusterName = ConfigurationManager.AppSettings["ClusterName"];
            var consumerName = ConfigurationManager.AppSettings["ConsumerName"];
            var consumerGroup = ConfigurationManager.AppSettings["ConsumerGroup"];
            var address = ConfigurationManager.AppSettings["NameServerAddress"];
            var topic = ConfigurationManager.AppSettings["Topic"];
            var nameServerAddress = string.IsNullOrEmpty(address) ? SocketUtils.GetLocalIPV4() : IPAddress.Parse(address);
            var clientCount = int.Parse(ConfigurationManager.AppSettings["ClientCount"]);
            var setting = new ConsumerSetting
            {
                ClusterName = clusterName,
                ConsumeFromWhere = ConsumeFromWhere.FirstOffset,
                MessageHandleMode = MessageHandleMode.Sequential,
                NameServerList = new List<IPEndPoint> { new IPEndPoint(nameServerAddress, 9593) }
            };
            var messageHandler = new MessageHandler();
            for (var i = 1; i <= clientCount; i++)
            {
                new Consumer(consumerGroup, setting, consumerName)
                    .Subscribe(topic)
                    .SetMessageHandler(messageHandler)
                    .Start();
            }
        }

        class MessageHandler : IMessageHandler
        {
            private readonly IPerformanceService _performanceService;

            public MessageHandler()
            {
                _performanceService = ObjectContainer.Resolve<IPerformanceService>();
                _performanceService.Initialize("TotalReceived").Start();
            }

            public void Handle(QueueMessage message, IMessageContext context)
            {
                _performanceService.IncrementKeyCount("default", (DateTime.Now - message.CreatedTime).TotalMilliseconds);
                context.OnMessageHandled(message);
            }
        }
    }
}
