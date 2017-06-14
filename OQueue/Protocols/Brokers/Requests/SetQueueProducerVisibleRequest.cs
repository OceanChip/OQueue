using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Requests
{

    [Serializable]
    public class SetQueueProducerVisibleRequest
    {
        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public bool Visible { get; private set; }
        public SetQueueProducerVisibleRequest(string topic, int queueId, bool visible)
        {
            this.Topic = topic;
            this.QueueId = queueId;
            this.Visible = visible;
        }
    }
}
