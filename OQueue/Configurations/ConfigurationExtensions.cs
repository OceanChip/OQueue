using OceanChip.Common.Configurations;
using OceanChip.Queue.Broker;
using OceanChip.Queue.Broker.Client;
using OceanChip.Queue.Broker.DeleteMessageStrategies;
using OceanChip.Queue.Broker.LongPolling;
using OceanChip.Queue.Clients.Consumers;
using OceanChip.Queue.Clients.Producers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Configurations
{
    public static class ConfigurationExtensions
    {
        public static Configuration RegisterQueueComponents(this Configuration configuration)
        {
            configuration.SetDefault<IDeleteMessageStrategy, DeleteMessageByCountStrategy>();
            configuration.SetDefault<IAllocateMessageQueueStrategy, AverageAllocateMessageQueueStrategy>();
            configuration.SetDefault<IQueueSelector, QueueAverageSelector>();
            configuration.SetDefault<IMessageStore, DefaultMessageStore>();
            configuration.SetDefault<ProducerManager, ProducerManager>();
            configuration.SetDefault<IQueueStore, DefaultQueueStore>();
            configuration.SetDefault<ConsumerManager, ConsumerManager>();
            configuration.SetDefault<IConsumeOffsetStore, DefaultConsumeOffsetStore>();
            configuration.SetDefault<GetConsumerListService, GetConsumerListService>();
            configuration.SetDefault<GetTopicConsumeInfoListService, GetTopicConsumeInfoListService>();
            configuration.SetDefault<SuspendedPullRequestManager, SuspendedPullRequestManager>();
            configuration.SetDefault<ITpsStatisticService, DefaultTpsStatisticService>();
            configuration.SetDefault<IChunkStatisticService, DefaultChunkStatisticService>();
            return configuration;
        }
        public static Configuration UseDeleteMessageByTimeStrategy(this Configuration configuration,int maxStoreageHours = 24 * 30)
        {
            configuration.SetDefault <IDeleteMessageStrategy, DeleteMessageByTimeStrategy>(new DeleteMessageByTimeStrategy(maxStoreageHours));
            return configuration;
        }
        public static Configuration UseDeleteMessageByCountStrategy(this Configuration configuration,int maxChunkCount = 100)
        {
            configuration.SetDefault<IDeleteMessageStrategy, DeleteMessageByCountStrategy>(new DeleteMessageByCountStrategy(maxChunkCount));
            return configuration;
        }
    }
}
