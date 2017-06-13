using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols
{
    [Serializable]
    public class MessageQueueEx:MessageQueue
    {
        public int ClientCachedMessageCount { get; set; }
        public MessageQueueEx() : base() { }
        public MessageQueueEx(string brokeName,string topic,int queueId) : base(brokeName, topic, queueId)
        {

        }
        public override string ToString()
        {
            return $"[BrokerName={BrokerName},Topic={Topic},QueueId={QueueId},ClientCachedMessageCount={ClientCachedMessageCount}]";
        }
    }
}
