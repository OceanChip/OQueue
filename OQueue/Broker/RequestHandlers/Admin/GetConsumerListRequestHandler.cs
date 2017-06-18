using OceanChip.Queue.Broker.Client;
using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class GetConsumerListRequestHandler : AbstractRequestHanlder<GetConsumerListRequest, GetConsumerListService>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, GetConsumerListRequest request)
        {
            var consumerList = _service.GetConsumerList(request.GroupName, request.Topic);
            return _binarySerializer.Serialize(consumerList);
        }
    }
}
