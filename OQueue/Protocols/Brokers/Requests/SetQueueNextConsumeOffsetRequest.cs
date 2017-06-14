using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Requests
{
    [Serializable]
    public class SetQueueNextConsumeOffsetRequest
    {
        public string ConsumerGroup { get; private set; }
        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long NextOffset { get; private set; }
        public SetQueueNextConsumeOffsetRequest(string consumeGroup,string topic,int queueId,long nextOffset)
        {
            this.ConsumerGroup = consumeGroup;
            this.Topic = topic;
            this.QueueId = queueId;
            this.NextOffset = nextOffset;
        }
        public override string ToString()
        {
            return $"[ConsumerGroup={ConsumerGroup},Topic={Topic},QueueId={QueueId},NextOffset={NextOffset}]";
        }
    }
}
