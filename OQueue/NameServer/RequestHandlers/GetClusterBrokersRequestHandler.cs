using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.NameServers.Requests;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class GetClusterBrokersRequestHandler : AbstractRequestHandler<GetClusterBrokersRequest>
    {
        public GetClusterBrokersRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, GetClusterBrokersRequest request)
        {
            var brokerInfoList = _nameServerController.ClusterManager.GetClusterBrokers(request);
            return _binarySerializer.Serialize(brokerInfoList);
        }
    }
}
