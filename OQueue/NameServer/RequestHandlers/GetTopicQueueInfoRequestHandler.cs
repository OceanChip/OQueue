//using OceanChip.Queue.Protocols.Brokers.Requests;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.NameServers.Requests;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class GetTopicQueueInfoRequestHandler : AbstractRequestHandler<GetTopicQueueInfoRequest>
    {
        public GetTopicQueueInfoRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, GetTopicQueueInfoRequest request)
        {
            var topicQueueInfoList = _nameServerController.ClusterManager.GetTopicQueueInfo(request);
            return _binarySerializer.Serialize(topicQueueInfoList);
        }
    }
}
