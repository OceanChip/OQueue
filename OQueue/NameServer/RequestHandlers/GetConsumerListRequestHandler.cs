using OceanChip.Queue.Protocols.NameServers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class GetConsumerListRequestHandler : AbstractRequestHandler<GetConsumerListRequest>
    {
        public GetConsumerListRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, GetConsumerListRequest request)
        {
            var consumerList = _nameServerController.ClusterManager.GetConsumerList(request);
            return _binarySerializer.Serialize(consumerList);
        }
    }
}
