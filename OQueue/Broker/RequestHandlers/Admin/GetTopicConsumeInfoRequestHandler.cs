using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Broker.Client;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class GetTopicConsumeInfoRequestHandler : AbstractRequestHanlder<GetTopicConsumeInfoRequest, GetTopicConsumeInfoListService>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, GetTopicConsumeInfoRequest request)
        {
            var topicConsumeList = _service.GetTopicConsumeInfoList(request.GroupName, request.Topic);
            return _binarySerializer.Serialize(topicConsumeList);
        }
    }
}
