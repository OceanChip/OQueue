using System.Collections.Generic;
using OceanChip.Queue.Protocols.Brokers;

namespace OQueue.AdminWeb.Models
{
    public class BrokerConsumerListViewModel
    {
        public string Group { get; set; }
        public string Topic { get; set; }
        public IEnumerable<ConsumerInfo> ConsumerList { get; set; }
    }
}