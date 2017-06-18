using System.Collections.Generic;
using OceanChip.Queue.Protocols.Brokers;

namespace OQueue.AdminWeb.Models
{
    public class BrokerTopicQueueListViewModel
    {
        public string Topic { get; set; }
        public IEnumerable<TopicQueueInfo> TopicQueueInfoList { get; set; }
    }
}