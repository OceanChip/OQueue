using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class DeleteQueueRequestHandler : AbstractRequestHanlder<DeleteQueueRequest, IQueueStore>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, DeleteQueueRequest request)
        {
            _service.DeleteQueue(request.Topic, request.QueueId);
            return EmptyBytes;
        }
    }
}
