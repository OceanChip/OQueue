using OceanChip.Common.Configurations;
using OceanChip.Common.Extensions;
using OceanChip.Common.Socketing;
using OceanChip.Queue.Broker;
using OceanChip.Queue.Configurations;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using OCommonConfiguration = OceanChip.Common.Configurations.Configuration;

namespace QuickStart.BrokerServer2
{
    class Program
    {

        static void Main(string[] args)
        {
            InitializeEQueue();

            var address = ConfigurationManager.AppSettings["nameServerAddress"];
            var nameServerAddress = string.IsNullOrEmpty(address) ? SocketUtils.GetLocalIPV4() : IPAddress.Parse(address);
            var setting = new BrokerSetting(
                bool.Parse(ConfigurationManager.AppSettings["isMemoryMode"]),
                ConfigurationManager.AppSettings["fileStoreRootPath"],
                chunkCacheMaxPercent: 95,
                chunkFlushInterval: int.Parse(ConfigurationManager.AppSettings["flushInterval"]),
                messageChunkDataSize: int.Parse(ConfigurationManager.AppSettings["chunkSize"]) * 1024 * 1024,
                chunkWriteBuffer: int.Parse(ConfigurationManager.AppSettings["chunkWriteBuffer"]) * 1024,
                enableCache: bool.Parse(ConfigurationManager.AppSettings["enableCache"]),
                chunkCacheMinPercent: int.Parse(ConfigurationManager.AppSettings["chunkCacheMinPercent"]),
                syncFlush: bool.Parse(ConfigurationManager.AppSettings["syncFlush"]),
                messageChunkLocalCacheSize: 30 * 10000,
                queueChunkLocalCacheSize: 10000)
            {
                NotifyWhenMessageArrived = bool.Parse(ConfigurationManager.AppSettings["notifyWhenMessageArrived"]),
                MessageWriteQueueThreshold = int.Parse(ConfigurationManager.AppSettings["messageWriteQueueThreshold"])
            };
            setting.NameServerList = new List<IPEndPoint> { new IPEndPoint(nameServerAddress, 9593) };
            setting.BrokerInfo.BrokerName = ConfigurationManager.AppSettings["brokerName"];
            setting.BrokerInfo.GroupName = ConfigurationManager.AppSettings["groupName"];
            setting.BrokerInfo.ProducerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), int.Parse(ConfigurationManager.AppSettings["producerPort"])).ToAddress();
            setting.BrokerInfo.ConsumerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), int.Parse(ConfigurationManager.AppSettings["consumerPort"])).ToAddress();
            setting.BrokerInfo.AdminAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), int.Parse(ConfigurationManager.AppSettings["adminPort"])).ToAddress();
            BrokerController.Create(setting).Start();
            Console.ReadLine();
        }

        static void InitializeEQueue()
        {
            var configuration = OCommonConfiguration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler()
                .RegisterQueueComponents()
                .UseDeleteMessageByCountStrategy(10);
        }
    }
}
