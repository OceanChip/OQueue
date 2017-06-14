using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Requests
{
    [Serializable]
    public class UpdateQueueOffsetRequest
    {
        public string ConsumerGroup { get; set; }
        public MessageQueue MessageQueue { get; set; }
        public long QueueOffset { get; set; }
        public UpdateQueueOffsetRequest(string consumerGroup,MessageQueue messageQueue,long queueOffset)
        {
            this.ConsumerGroup = consumerGroup;
            this.MessageQueue = messageQueue;
            this.QueueOffset = queueOffset;
        }
        public override string ToString()
        {
            return $"[ConsumerGroup={ConsumerGroup},MessageQueue=[{MessageQueue.ToString()}],QueueOffset={QueueOffset}]";
        }
    }
}
