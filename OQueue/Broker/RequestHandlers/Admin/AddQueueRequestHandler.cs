using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Common.Components;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class AddQueueRequestHandler : AbstractRequestHanlder<AddQueueRequest, IQueueStore>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, AddQueueRequest request)
        {
            _service.AddQueue(request.Topic);
            return EmptyBytes;
        }
    }
}
