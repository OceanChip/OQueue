using OceanChip.Queue.Protocols.NameServers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class GetTopicRouteInfoRequestHandler : AbstractRequestHandler<GetTopicRouteInfoRequest>
    {
        public GetTopicRouteInfoRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, GetTopicRouteInfoRequest request)
        {
            var topicRouterInfoList = _nameServerController.ClusterManager.GetTopicRouteInfo(request);
            return _binarySerializer.Serialize(topicRouterInfoList);
        }
    }
}
