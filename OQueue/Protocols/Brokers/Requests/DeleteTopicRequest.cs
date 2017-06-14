using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Requests
{
    [Serializable]
    public class DeleteTopicRequest
    {
        public string Topic { get; private set; }
        public DeleteTopicRequest(string topic)
        {
            this.Topic = topic;
        }
    }
}
