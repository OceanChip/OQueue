using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers
{
    [Serializable]
    public class TopicConsumeInfo
    {
        public string ConsumerGroup { get; set; }
        public string Topic { get; set; }
        public int QueueId { get; set; }
        public long QueueCurrentOffset { get; set; }
        public long ConsumedOffset { get; set; }
        public int ClientCachedMessageCount { get; set; }
        public long QueueNotConsumeCount { get; set; }
        public int OnlineConsumerCount { get; set; }
        public long ConsumeThroughput { get; set; }
        public long CalculateQueueNotConsumeCount()
        {
            if (ConsumedOffset >= 0)
            {
                return QueueCurrentOffset - ConsumedOffset;
            }
            else if (QueueCurrentOffset >= 0)
                return QueueCurrentOffset + 1;
            return 0;
        }
    }
}
