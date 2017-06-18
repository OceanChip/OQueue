using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.Brokers.Requests;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class SetQueueProducerVisibleRequestHandler : AbstractRequestHanlder<SetQueueProducerVisibleRequest, IQueueStore>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, SetQueueProducerVisibleRequest request)
        {
            _service.SetProducerVisibile(request.Topic, request.QueueId, request.Visible);
            return EmptyBytes;
        }
    }
}
