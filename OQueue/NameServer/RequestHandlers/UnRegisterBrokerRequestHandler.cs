using System;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.NameServers.Requests;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class UnRegisterBrokerRequestHandler : AbstractRequestHandler<BrokerUnRegistrationRequest>
    {

        public UnRegisterBrokerRequestHandler(NameServerController nameServerController):base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, BrokerUnRegistrationRequest request)
        {
            _nameServerController.ClusterManager.UnRegisterBroker(request);
            return EmptyBytes;
        }
    }
}