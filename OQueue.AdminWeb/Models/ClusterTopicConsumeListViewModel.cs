using System.Collections.Generic;
using OceanChip.Queue.Protocols.Brokers;

namespace OQueue.AdminWeb.Models
{
    public class ClusterTopicConsumeListViewModel
    {
        public string ClusterName { get; set; }
        public string Group { get; set; }
        public string Topic { get; set; }
        public IEnumerable<BrokerTopicConsumeInfo> TopicConsumeInfoList { get; set; }
    }
}