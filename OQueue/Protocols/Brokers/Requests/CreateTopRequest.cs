using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Requests
{
    /// <summary>
    /// 创建主题
    /// </summary>
    [Serializable]
    public class CreateTopicRequest
    {
        public string Topic { get; private set; }
        public int? InitialQueueCount { get; private set; }
        public CreateTopicRequest(string topic,int? initQueueCount)
        {
            this.Topic = topic;
            this.InitialQueueCount = initQueueCount;
        }
    }
}
