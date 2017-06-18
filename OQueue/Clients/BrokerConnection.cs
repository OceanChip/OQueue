using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.Brokers;

namespace OceanChip.Queue.Clients
{
    public class BrokerConnection
    {
        private readonly BrokerInfo _brokerInfo;
        private readonly SocketRemotingClient _remotingClient;
        private readonly SocketRemotingClient _adminRemotingClient;
        public BrokerInfo BrokerInfo => _brokerInfo;
        public SocketRemotingClient RemotingClient => _remotingClient;
        public SocketRemotingClient AdminRemotingClient => _adminRemotingClient;

        public BrokerConnection(BrokerInfo broker,SocketRemotingClient remotingClient,SocketRemotingClient adminRemotingClient)
        {
            _brokerInfo = broker;
            _remotingClient = remotingClient;
            _adminRemotingClient = adminRemotingClient;
        }
        public void Start()
        {
            _remotingClient.Start();
            _adminRemotingClient.Start();
        }
        public void Stop()
        {
            _remotingClient.Shutdown();
            _adminRemotingClient.Shutdown();
        }
    }
}