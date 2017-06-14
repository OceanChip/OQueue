using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.NameServers
{
    [Serializable]
    public class BrokerTopQueueInfo
    {
        public BrokerInfo BrokerInfo { get; set; }
        public IList<TopicQueueInfo> TopicConsumerInfo { get; set; }
        public BrokerTopQueueInfo()
        {
            TopicConsumerInfo = new List<TopicQueueInfo>();
        }
    }
}
