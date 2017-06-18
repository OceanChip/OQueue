using OceanChip.Queue.Protocols.NameServers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;

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
