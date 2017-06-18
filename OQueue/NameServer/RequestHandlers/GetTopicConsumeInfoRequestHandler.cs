
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.NameServers.Requests;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class GetTopicConsumeInfoRequestHandler : AbstractRequestHandler<GetTopicConsumeInfoRequest>
    {
        public GetTopicConsumeInfoRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, GetTopicConsumeInfoRequest request)
        {
            var topicConsumeInfoList = _nameServerController.ClusterManager.GetTopicConsumeInfo(request);
            return _binarySerializer.Serialize(topicConsumeInfoList);
        }
    }
}
