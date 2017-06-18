using System;
using System.Collections.Generic;

namespace OceanChip.Queue.Protocols.Brokers
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
