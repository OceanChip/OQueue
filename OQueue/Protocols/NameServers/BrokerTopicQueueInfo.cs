using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers
{
    [Serializable]
    public class BrokerTopicQueueInfo
    {
        public BrokerInfo BrokerInfo { get; set; }
        public IList<TopicQueueInfo> TopicConsumerInfoList { get; set; }
        public BrokerTopicQueueInfo()
        {
            TopicConsumerInfoList = new List<TopicQueueInfo>();
        }
    }
}
