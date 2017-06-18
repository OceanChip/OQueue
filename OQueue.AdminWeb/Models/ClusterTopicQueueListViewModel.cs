using System.Collections.Generic;
using OceanChip.Queue.Protocols.Brokers;

namespace OQueue.AdminWeb.Models
{
    public class ClusterTopicQueueListViewModel
    {
        public string ClusterName { get; set; }
        public string Topic { get; set; }
        public IEnumerable<BrokerTopicQueueInfo> TopicQueueInfoList { get; set; }
    }
}