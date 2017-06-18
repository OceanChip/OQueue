using OceanChip.Common.Components;
using OceanChip.Common.Extensions;
using OceanChip.Common.Logging;
using OceanChip.Common.Socketing;
using OceanChip.Queue.Protocols;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.Client
{
    public class ConsumerGroup
    {
        class ConsumerInfo
        {
            public string ConsumerId;
            public ClientHeartbeatInfo HeartbeatInfo;
            public IList<string> SubscriptionTopics = new List<string>();
            public IList<MessageQueueEx> ConsumingQueues = new List<MessageQueueEx>();
        }
        private readonly string _groupName;
        private readonly ConcurrentDictionary<string, ConsumerInfo> _consumerInfoDict = new ConcurrentDictionary<string, ConsumerInfo>();
        private readonly ILogger _logger;

        public string GroupName => _groupName;
        public ConsumerGroup(string groupName)
        {
            this._groupName = groupName;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }
        public void RegisterConsumer(ITcpConnection connection,string consumerId,IList<string> subscriptionTopics,IList<MessageQueueEx> consumingMessageQueue)
        {
            var connectionId = connection.RemoteEndPoint.ToAddress();

            _consumerInfoDict.AddOrUpdate(connectionId, key =>
            {
                var newConsumerInfo = new ConsumerInfo
                {
                    ConsumerId = consumerId,
                    HeartbeatInfo = new ClientHeartbeatInfo(connection) { LastHeartbeartTime = DateTime.Now },
                    SubscriptionTopics = subscriptionTopics,
                    ConsumingQueues = consumingMessageQueue
                };
                _logger.InfoFormat("Consumer registered to group,groupName:{0},consumerId:{1},connectionId:{2},subscriptionTopics:{3},ConsumingQueues:{4}", 
                    _groupName, consumerId, key, string.Join("|", subscriptionTopics), 
                    string.Join("|", consumingMessageQueue));
                return newConsumerInfo;
            }, (key, existingConsumerInfo) =>
            {
                existingConsumerInfo.HeartbeatInfo.LastHeartbeartTime = DateTime.Now;

                var oldSubscriptionList = existingConsumerInfo.SubscriptionTopics.ToList();
                var newSubscriptionList = subscriptionTopics.ToList();
                if (IsStringCollectionChanged(oldSubscriptionList, newSubscriptionList))
                {
                    existingConsumerInfo.SubscriptionTopics = newSubscriptionList;
                    _logger.InfoFormat("Consumer subscriptionTopics changed.groupName:{0},consumerId:{1},connectionId:{2},old:{3},new:{4}",
                        _groupName, consumerId, key, string.Join("|", oldSubscriptionList), string.Join("|", newSubscriptionList));
                }

                var oldConsumingQueues = existingConsumerInfo.ConsumingQueues;
                var newConsumingQueues = consumingMessageQueue;
                if (IsMessageQueueChanged(oldConsumingQueues, newConsumingQueues))
                {
                    existingConsumerInfo.ConsumingQueues = newConsumingQueues;
                    _logger.InfoFormat("Consumer newConsumingQueues changed.groupName:{0},consumerId:{1},connectionId:{2},old:{3},new:{4}",
                        _groupName, consumerId, key, string.Join("|", oldSubscriptionList), string.Join("|", newSubscriptionList));
                }
                return existingConsumerInfo;
            }            
            );
        }
        public bool IsConsumerActive(string consumerId)
        {
            return _consumerInfoDict.Values.Any(x => x.ConsumerId == consumerId);
        }
        public void RemoveConsumer(string connectionId)
        {
            ConsumerInfo consumer;
            if(_consumerInfoDict.TryRemove(connectionId,out consumer))
            {
                try
                {
                    consumer.HeartbeatInfo.Connection.Close();
                }catch(Exception ex)
                {
                    _logger.Error($"关闭消费者连接失败,consumerId:{consumer.ConsumerId},connectionId:{connectionId}",ex);
                }
                _logger.InfoFormat("移除消费者组:{0},consumerId:{1},connectionId:{2},lastHeartbeat:{3},subscriptionTopics:{4},consumingQueues:{5}",
                    _groupName,
                    consumer.ConsumerId,
                    connectionId,
                    consumer.HeartbeatInfo.LastHeartbeartTime,
                    string.Join("|",consumer.SubscriptionTopics),
                    string.Join("|",consumer.ConsumingQueues));
            }
        }
        public void RemoveNotActiveConsumers()
        {
            foreach(var entry in _consumerInfoDict)
            {
                if (entry.Value.HeartbeatInfo.IsTimeout(BrokerController.Instance.Setting.ConsumerExpiredTimeout))
                {
                    RemoveConsumer(entry.Key);
                }
            }
        }
        public IEnumerable<string> GetAllConsumerIds()
        {
            return _consumerInfoDict.Values.Select(x => x.ConsumerId).ToList();
        }
        public int GetConsumerCount()
        {
            return _consumerInfoDict.Count;
        }
        public int GetClientCacheMessageCount(string topic,int queueId)
        {
            var count = 0;
            foreach(var consumer in _consumerInfoDict.Values)
            {
                foreach(var messageQueue in consumer.ConsumingQueues)
                {
                    if(messageQueue.Topic==topic && messageQueue.QueueId== queueId)
                    {
                        count += messageQueue.ClientCachedMessageCount;
                    }
                }
            }
            return count;
        }
        public IEnumerable<string> GetConsumerIdsForTopic(string topic)
        {
            return _consumerInfoDict.Where(x => x.Value.SubscriptionTopics.Any(y => y == topic)).Select(z => z.Value.ConsumerId);
        }
        public bool IsConsumerExistForQueue(string topic,int queueId)
        {
            return _consumerInfoDict.Values.Any(x => x.ConsumingQueues.Any(y => y.Topic == topic && y.QueueId == queueId));
        }
        public IEnumerable<MessageQueueEx> GetConsumingQueueList(string consumerId)
        {
            var consumer = _consumerInfoDict.Values.SingleOrDefault(x => x.ConsumerId == consumerId);
            if (consumer != null)
            {
                return consumer.ConsumingQueues.ToList();
            }
            return new List<MessageQueueEx>();
        }
        private bool IsMessageQueueChanged(IList<MessageQueueEx> list1,IList<MessageQueueEx> list2)
        {
            if (list1.Count != list2.Count)
                return true;
            for(int i = 0; i < list2.Count; i++)
            {
                var item1 = list1[i];
                var item2 = list2[i];
                if (item1.Topic != item2.Topic)
                    return true;
                if (item1.QueueId != item2.QueueId)
                    return true;
            }
            return false;
        }
        private bool IsStringCollectionChanged(IList<string> orginal,IList<string> current)
        {
            if (orginal.Count != current.Count)
                return true;
            for(int idx = 0; idx < orginal.Count; idx++)
            {
                if (orginal[idx] != current[idx])
                {
                    return true;
                }
            }
            return false;
        }
    }
}
