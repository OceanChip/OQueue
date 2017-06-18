using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.Brokers.Requests;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class SetQueueConsumerVisibleRequestHandler : AbstractRequestHanlder<SetQueueConsumerVisibleRequest, IQueueStore>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, SetQueueConsumerVisibleRequest request)
        {
            _service.SetConsumerVisibile(request.Topic, request.QueueId, request.Visible);
            return EmptyBytes;
        }
    }
}
