using OceanChip.Common.Socketing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Clients
{
    public class ClientSetting
    {
        public string ClientName { get; set; }
        public string ClusterName { get; set; }
        public IEnumerable<IPEndPoint> NameServerList { get; set; }
        public SocketSetting SocketSetting { get; set; }
        public int SendHeartbeatInterval { get; set; }
        public bool OnlyFindMasterBroker { get; set; }
        public int RefreshBrokerAndTopicRouteInfoInterval { get; set; }
    }
}
