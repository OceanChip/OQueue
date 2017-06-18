using OceanChip.Common.Socketing;
using System.Collections.Generic;
using System.Net;

namespace OceanChip.Queue.Clients.Producers
{
    public class ProducerSetting
    {
        public string ClusterName { get; set; }
        public IEnumerable<IPEndPoint> NameServerList { get; set; }
        public SocketSetting SocketSetting { get; set; }
        public int RefreshBrokerAndTopicRouteInfoInterval { get; set; }
        public int HeartbeatBrokerInterval { get; set; }
        public int MessageMaxSize { get; set; }
        public int SendMessageMaxRetryCount { get; set; }

        public ProducerSetting()
        {
            ClusterName = "DefaultCluster";
            NameServerList = new List<IPEndPoint>()
            {
                new IPEndPoint(SocketUtils.GetLocalIPV4(),9593)
            };
            SocketSetting = new SocketSetting();
            RefreshBrokerAndTopicRouteInfoInterval = 1000 * 5;
            HeartbeatBrokerInterval = 1000;
            MessageMaxSize = 1024 * 1024 * 4;
            SendMessageMaxRetryCount = 5;
        }
    }
}