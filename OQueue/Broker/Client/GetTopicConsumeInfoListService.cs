using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.Client
{
    public class GetTopicConsumeInfoListService
    {
        private readonly ConsumerManager _consumerManager;
        private readonly IQueueStore _queueStore;
        private readonly IConsumeOffsetStore _consumeOffsetStore;
        private readonly ITpsStatisticService _tpsStatisticService;


        public GetTopicConsumeInfoListService(ConsumerManager consumerManager,IConsumeOffsetStore consumerOffsetStore,IQueueStore queueStore,ITpsStatisticService tpsStatService)
        {
            this._consumeOffsetStore = consumerOffsetStore;
            this._consumerManager = consumerManager;
            this._queueStore = queueStore;
            this._tpsStatisticService = tpsStatService;
        }
        public IEnumerable<TopicConsumeInfo> GetAllTopicConsumeInfoList()
        {
            var topicConsumeList = _consumeOffsetStore.GetAllTopicConsumeInfoList();

            foreach(var topicConsume in topicConsumeList)
            {
                var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topicConsume.Topic, topicConsume.QueueId);
                topicConsume.QueueCurrentOffset = queueCurrentOffset;
                topicConsume.QueueNotConsumeCount = topicConsume.CalculateQueueNotConsumeCount();
                topicConsume.OnlineConsumerCount = _consumerManager.GetConsumerCount(topicConsume.ConsumerGroup);
                topicConsume.ClientCachedMessageCount = _consumerManager.GetClientCacheMessageCount(topicConsume.ConsumerGroup, topicConsume.Topic, topicConsume.QueueId);
                topicConsume.ConsumeThroughput = _tpsStatisticService.GetTopicConsumeThroughput(topicConsume.Topic, topicConsume.QueueId, topicConsume.ConsumerGroup);
            }
            return topicConsumeList;
        }
        public IEnumerable<TopicConsumeInfo> GetTopicConsumeInfoList(string groupName,string topic)
        {
            var topicConsumeList = _consumeOffsetStore.GetTopicConsumeInfoList(groupName, topic);

            foreach (var topicConsume in topicConsumeList)
            {
                var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topicConsume.Topic, topicConsume.QueueId);
                topicConsume.QueueCurrentOffset = queueCurrentOffset;
                topicConsume.QueueNotConsumeCount = topicConsume.CalculateQueueNotConsumeCount();
                topicConsume.OnlineConsumerCount = _consumerManager.GetConsumerCount(topicConsume.ConsumerGroup);
                topicConsume.ClientCachedMessageCount = _consumerManager.GetClientCacheMessageCount(topicConsume.ConsumerGroup, topicConsume.Topic, topicConsume.QueueId);
                topicConsume.ConsumeThroughput = _tpsStatisticService.GetTopicConsumeThroughput(topicConsume.Topic, topicConsume.QueueId, topicConsume.ConsumerGroup);
            }
            return topicConsumeList;
        }
    }
}
