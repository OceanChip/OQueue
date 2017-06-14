using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.NameServers
{
    [Serializable]
    public class BrokerProducerListInfo
    {
        public BrokerInfo BrokerInfo { get; set; }
        public IList<string> ProducerList { get; set; }
        public BrokerProducerListInfo()
        {
            ProducerList = new List<string>();
        }
    }
}
