using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.NameServers
{
    [Serializable]
    public class BrokerTopicConsumeInfo
    {
        public BrokerInfo BrokerInfo { get; set; }
        public IList<TopicConsumeInfo> TopicConsumerInfo { get; set; }
        public BrokerTopicConsumeInfo()
        {
            TopicConsumerInfo = new List<TopicConsumeInfo>();
        }
    }
}
