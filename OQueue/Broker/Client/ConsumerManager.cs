using OceanChip.Common.Components;
using OceanChip.Common.Scheduling;
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
    public class ConsumerManager
    {
        private readonly ConcurrentDictionary<string, ConsumerGroup> _consumerGroupDict = new ConcurrentDictionary<string, ConsumerGroup>();
        private readonly IScheduleService _scheduleService;

        public ConsumerManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
        }
        public void Start()
        {
            _consumerGroupDict.Clear();
            _scheduleService.StartTask("ScanNotActiveConsumer", ScanNotActiveConsumer, 1000, 1000);
        }
        public void Shutdown()
        {
            _consumerGroupDict.Clear();
            _scheduleService.StopTask("ScanNotActiveConsumer");
        }
        public void RegisterConsumer(string groupName,string consumerId,IEnumerable<string> subscriptionTopics,IEnumerable<MessageQueueEx> consumingqQueueList,ITcpConnection connection)
        {
            var consumerGroup = _consumerGroupDict.GetOrAdd(groupName, key => new ConsumerGroup(key));
            consumerGroup.RegisterConsumer(connection, consumerId, subscriptionTopics.ToList(), consumingqQueueList.ToList());
        }
        public void RemoveConsumer(string connectionId)
        {
            foreach(var group in _consumerGroupDict.Values)
            {
                group.RemoveConsumer(connectionId);
            }
        }
        public int GetConsumerGroupCount()
        {
            return _consumerGroupDict.Count;
        }
        public IEnumerable<ConsumerGroup> GetAllConsumerGroups()
        {
            return _consumerGroupDict.Values.ToList();
        }
        public int GetAllConsumerCount()
        {
            return GetAllConsumerGroups().Sum(x => x.GetConsumerCount());
        }
        public ConsumerGroup GetConsumerGroup(string groupName)
        {
            ConsumerGroup group;
            if(_consumerGroupDict.TryGetValue(groupName,out group))
            {
                return group;
            }
            return null;
        }
        public int GetConsumerCount(string groupName)
        {
            ConsumerGroup group;
            if (_consumerGroupDict.TryGetValue(groupName, out group))
            {
                return group.GetConsumerCount();
            }
            return 0 ;
        }
        public int GetClientCacheMessageCount(string groupName,string topic,int queueId)
        {
            ConsumerGroup group;
            if (_consumerGroupDict.TryGetValue(groupName, out group))
            {
                return group.GetClientCacheMessageCount(topic,queueId);
            }
            return 0;
        }
        public bool IsConsumerActive(string consumerGroup,string consumerId)
        {
            var group = GetConsumerGroup(consumerGroup);
            return group!=null &&group.IsConsumerActive(consumerId);
        }
        public bool IsConsumerExistForQueue(string topic,int queueId)
        {
            var groups = GetAllConsumerGroups();
            foreach(var group in groups)
            {
                if (group.IsConsumerExistForQueue(topic, queueId))
                {
                    return true;
                }
            }
            return false;
        }
        private void ScanNotActiveConsumer()
        {
            foreach(var group in _consumerGroupDict.Values)
            {
                group.RemoveNotActiveConsumers();
            }
        }
    }
}
