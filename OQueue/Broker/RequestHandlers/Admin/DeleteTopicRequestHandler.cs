using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class DeleteTopicRequestHandler : AbstractRequestHanlder<DeleteTopicRequest, IQueueStore>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, DeleteTopicRequest request)
        {
            _service.DeleteTopic(request.Topic);
            return EmptyBytes;
        }
    }
}
