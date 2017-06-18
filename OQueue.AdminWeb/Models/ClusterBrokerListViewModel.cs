using System.Collections.Generic;
using OceanChip.Queue.Protocols.Brokers;

namespace OQueue.AdminWeb.Models
{
    public class ClusterBrokerListViewModel
    {
        public string ClusterName { get; set; }
        public IEnumerable<BrokerStatusInfo> BrokerList { get; set; }
    }
}