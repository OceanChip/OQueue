using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Requests
{
    [Serializable]
    public class DeleteConsumerGroupRequest
    {
        public string GroupName { get; private set; }
        public DeleteConsumerGroupRequest(string name)
        {
            this.GroupName = name;
        }
        public override string ToString()
        {
            return $"[GroupName={GroupName}]";
        }
    }
}
