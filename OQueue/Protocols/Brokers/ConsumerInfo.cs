using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers
{
    [Serializable]
    public class ConsumerInfo
    {
        public string ConsumerGroup { get; set; }
        public string ConsumerId { get; set; }
        public string Topic { get; set; }
        public int QueueId { get; set; }
        /// <summary>
        /// 队列当前位置
        /// </summary>
        public long QueueCurrentOffset { get; set; }
        /// <summary>
        /// 队列消息位置
        /// </summary>
        public long ConsumedOffset { get; set; }
        public long QueueNotConsumeCount { get; set; }
        /// <summary>
        /// 客户端缓存的消息树
        /// </summary>
        public int ClientCachedMessageCount { get; set; }

        public long CalculatedQueueNotConsumeCount()
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
