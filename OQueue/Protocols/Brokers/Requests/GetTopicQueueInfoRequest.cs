using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Requests
{
    [Serializable]
    public class GetTopicQueueInfoRequest
    {
        public string Topic { get; private set; }
        public GetTopicQueueInfoRequest(string topic)
        {
            this.Topic = topic;
        }
    }
}
