using System.Collections.Generic;
using OceanChip.Queue.Protocols.Brokers;

namespace OQueue.AdminWeb.Models
{
    public class ClusterProducerListViewModel
    {
        public string ClusterName { get; set; }
        public IEnumerable<BrokerProducerListInfo> ProducerList { get; set; }
    }
}