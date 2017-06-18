using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.Brokers.Requests;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class GetTopicQueueInfoRequestHandler : AbstractRequestHanlder<GetTopicQueueInfoRequest, IQueueStore>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, GetTopicQueueInfoRequest request)
        {
            var topicQueueList = _service.GetTopicQueueInfoList(request.Topic);
            return _binarySerializer.Serialize(topicQueueList);
        }
    }
}
