using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class CreateTopicRequestHandler : AbstractRequestHanlder<CreateTopicRequest, IQueueStore>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, CreateTopicRequest request)
        {
            IEnumerable<int> queueIds = _service.CreateTopic(request.Topic, request.InitialQueueCount);
            return  _binarySerializer.Serialize(queueIds);
        }
    }
}
