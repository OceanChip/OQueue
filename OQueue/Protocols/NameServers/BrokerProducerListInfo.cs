using System;
using System.Collections.Generic;

namespace OceanChip.Queue.Protocols.Brokers
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
