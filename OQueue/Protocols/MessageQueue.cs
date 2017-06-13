using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols
{
    [Serializable]
    public class MessageQueue
    {
        public string BrokerName { get; private set; }
        public string Topic { get; private set; }
        public int QueueId { get; private set; }

        public MessageQueue() { }

        public MessageQueue(string brokerName,string topic,int queueId)
        {
            this.BrokerName = brokerName;
            this.Topic = topic;
            this.QueueId = queueId;
        }
        public override string ToString()
        {
            return $"[BrokerName={BrokerName},Topic={Topic},QueueId={QueueId}]";
        }
    }
}
