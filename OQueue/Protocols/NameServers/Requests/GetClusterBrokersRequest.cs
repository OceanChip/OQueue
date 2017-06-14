using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.NameServers.Requests
{

    [Serializable]
    public class GetClusterBrokersRequest
    {
        public string ClusterName { get; set; }
        public string Topic { get; set; }
        /// <summary>
        /// 是否只查找主集群
        /// </summary>
        public bool OnlyFindMaster { get; set; }
    }
}
