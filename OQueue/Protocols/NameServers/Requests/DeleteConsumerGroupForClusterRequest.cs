using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.NameServers.Requests
{

    [Serializable]
    public class DeleteConsumerGroupForClusterRequest
    {
        public string ClusterName { get; set; }
        public string GroupName { get; set; }
    }
}
