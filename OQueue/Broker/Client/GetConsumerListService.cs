using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Queue.Protocols;

namespace OceanChip.Queue.Broker.Client
{
    public class GetConsumerListService
    {
        private readonly ConsumerManager _consumerManager;
        private readonly IQueueStore _queueStore;
        private readonly IConsumeOffsetStore _consumeOffsetStore;

        public GetConsumerListService(ConsumerManager consumerManager,IConsumeOffsetStore consumeOffsetStore,IQueueStore queueStore)
        {
            this._consumeOffsetStore = consumeOffsetStore;
            this._consumerManager = consumerManager;
            this._queueStore = queueStore;
        }
        public IEnumerable<ConsumerInfo> GetAllConsumerList()
        {
            var consumerList = new List<ConsumerInfo>();
            var consumerGroupList = _consumerManager.GetAllConsumerGroups();
            if(consumerGroupList==null || consumerGroupList.Count() == 0)
            {
                return consumerList;
            }
            foreach(var group in consumerGroupList)
            {
                var consumerIdList = group.GetAllConsumerIds().ToList();
                consumerIdList.Sort();

                foreach(var consumerId in consumerIdList)
                {
                    var messageQueueList = group.GetConsumingQueueList(consumerId);
                    foreach(var mq in messageQueueList)
                    {
                        consumerList.Add(BuildConsumerInfo(group.GroupName, consumerId, mq));
                    }
                }
            }
            consumerList.Sort(SortConsumerInfo);
            return consumerList;
        }
        public IEnumerable<ConsumerInfo> GetConsumerList(string groupName,string topic)
        {
            var consumerList = new List<ConsumerInfo>();

            if(!string.IsNullOrEmpty(groupName) && !string.IsNullOrEmpty(topic))
            {
                var group = _consumerManager.GetConsumerGroup(groupName);
                if (group == null)
                    return consumerList;
                var consumerIdList = group.GetConsumerIdsForTopic(topic).ToList();
                consumerIdList.Sort();

                foreach(var consumerId in consumerIdList)
                {
                    var messageQueueList = group.GetConsumingQueueList(consumerId);
                    foreach(var mq in messageQueueList)
                    {
                        consumerList.Add(BuildConsumerInfo(group.GroupName, consumerId, mq));
                    }
                }
                consumerList.Sort(SortConsumerInfo);
            }
            return consumerList;
        }
        private int SortConsumerInfo(ConsumerInfo x, ConsumerInfo y)
        {
            var result = string.Compare(x.ConsumerGroup, y.ConsumerGroup);
            if (result != 0)
                return result;
            result = string.Compare(x.ConsumerId, y.ConsumerId);
            if (result != 0)
                return result;
            if(x.QueueId>y.QueueId)
                return -1;
            else if (x.QueueId < y.QueueId)
                return -1;
            return 0;
        }

        private ConsumerInfo BuildConsumerInfo(string groupName, string consumerId, MessageQueueEx mq)
        {
            var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(mq.Topic, mq.QueueId);
            var consumer = new ConsumerInfo
            {
                ConsumerGroup=groupName,
                ConsumerId=consumerId,
                Topic=mq.Topic,
                QueueId=mq.QueueId,
                ClientCachedMessageCount=mq.ClientCachedMessageCount,
                QueueCurrentOffset=queueCurrentOffset,
                ConsumedOffset=_consumeOffsetStore.GetConsumeOffset(mq.Topic,mq.QueueId,groupName)
            };
            consumer.QueueNotConsumeCount = consumer.CalculatedQueueNotConsumeCount();
            return consumer;
        }
    }
}
