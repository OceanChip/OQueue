using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.NameServers
{
    [Serializable]
    public class BrokerConsumerListInfo
    {
        public BrokerInfo BrokerInfo { get; set; }
        public IList<ConsumerInfo> ConsumerList { get; set; }
        public BrokerConsumerListInfo()
        {
            this.ConsumerList = new List<ConsumerInfo>();
        }
    }
}
