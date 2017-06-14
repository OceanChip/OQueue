using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.NameServers.Requests
{
    [Serializable]
    public class BrokerUnRegistrationRequest
    {
        public BrokerInfo BrokerInfo { get; set; }
    }
}
