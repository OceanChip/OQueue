using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.NameServers.Requests;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class GetTopicAccumulateInfoListRequestHandler : AbstractRequestHandler<GetTopicAccumulateInfoListRequest>
    {
        public GetTopicAccumulateInfoListRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, GetTopicAccumulateInfoListRequest request)
        {
            var topicAccumulateInfoList = _nameServerController.ClusterManager.GetTopicAccumulateInfoList(request);
            return _binarySerializer.Serialize(topicAccumulateInfoList);
        }
    }
}
