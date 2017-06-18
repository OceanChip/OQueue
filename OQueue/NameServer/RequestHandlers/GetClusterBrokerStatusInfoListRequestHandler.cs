using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.NameServers.Requests;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class GetClusterBrokerStatusInfoListRequestHandler : AbstractRequestHandler<GetClusterBrokersRequest>
    {
        public GetClusterBrokerStatusInfoListRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, GetClusterBrokersRequest request)
        {
            var brokerInfoList = _nameServerController.ClusterManager.GetClusterBrokerStatusInfos(request);
            return _binarySerializer.Serialize(brokerInfoList);
        }
    }
}
