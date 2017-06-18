using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.NameServers.Requests;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class GetConsumerListRequestHandler : AbstractRequestHandler<GetConsumerListRequest>
    {
        public GetConsumerListRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, GetConsumerListRequest request)
        {
            var consumerList = _nameServerController.ClusterManager.GetConsumerList(request);
            return _binarySerializer.Serialize(consumerList);
        }
    }
}
