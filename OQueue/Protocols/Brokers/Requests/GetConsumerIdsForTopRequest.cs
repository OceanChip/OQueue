using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Requests
{
    [Serializable]
    public class GetConsumerIdsForTopRequest
    {
        public string GroupName { get; private set; }
        public string Topic { get; private set; }
        public GetConsumerIdsForTopRequest(string groupName,string topic)
        {
            this.GroupName = groupName;
            this.Topic = topic;
        }
        public override string ToString()
        {
            return $"[GroupName={GroupName},Topic={Topic}]";
        }
    }
}
