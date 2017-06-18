using OceanChip.Common.Extensions;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.NameServers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.NameServer
{
    public class BrokerRequestService
    {
        private NameServerController _nameServerController;
        public BrokerRequestService(NameServerController nameServerController)
        {
            _nameServerController = nameServerController;
        }
        public void ExecuteActionToAllClusterBrokers(string clusterName,Action<SocketRemotingClient> action)
        {
            var request = new GetClusterBrokersRequest
            {
                ClusterName = clusterName,
                OnlyFindMaster = true
            };
            var endpointList = _nameServerController.ClusterManager.GetClusterBrokers(request).Select(x => x.AdminAddress.ToEndPoint());
            var remotingClientList = endpointList.ToRomotingClientList(_nameServerController.Setting.SocketSetting);

            foreach(var client in remotingClientList)
            {
                client.Start();
            }
            try
            {
                foreach(var client in remotingClientList)
                {
                    action(client);
                }
            }
            finally
            {
                foreach (var client in remotingClientList)
                    client.Shutdown();
            }
        }
    }
}
