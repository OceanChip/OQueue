using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Requests
{
    [Serializable]
    public class DeleteQueueRequest
    {
        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public DeleteQueueRequest(string topic,int queueId)
        {
            this.Topic = topic;
            this.QueueId = queueId;
        }
    }
}
