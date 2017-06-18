using OceanChip.Queue.Protocols.NameServers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class GetProducerListRequestHandler : AbstractRequestHandler<GetProducerListRequest>
    {
        public GetProducerListRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, GetProducerListRequest request)
        {
            var producerList = _nameServerController.ClusterManager.GetProducerList(request);
            return _binarySerializer.Serialize(producerList);
        }
    }
}
