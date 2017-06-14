using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers
{
    /// <summary>
    /// 消费心跳
    /// </summary>
    [Serializable]
    public class ConsumerHeatbeatData
    {
        public string ConsumerId { get; private set; }
        public string GroupName { get; private set; }
        public IEnumerable<string> SubscriptionTopics { get; private set; }
        public IEnumerable<MessageQueueEx> ConsumingQueues { get; private set; }
        public ConsumerHeatbeatData(string consumerId,string groupName,IEnumerable<string> subtopics,IEnumerable<MessageQueueEx> consumingQueues)
        {
            this.ConsumerId = consumerId;
            this.GroupName = groupName;
            this.SubscriptionTopics = subtopics;
            this.ConsumingQueues = consumingQueues;
        }
        public override string ToString()
        {
            return $"[ConsumerId={ConsumerId},GroupName={GroupName},SubscriptionTopicsL={string.Join("|",SubscriptionTopics)},ConsumingQueues={string.Join("|", ConsumingQueues)}]";
        }
    }
}
