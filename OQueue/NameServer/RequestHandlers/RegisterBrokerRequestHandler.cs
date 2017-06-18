using OceanChip.Common.Components;
using OceanChip.Common.Remoting;
using OceanChip.Common.Serializing;
using OceanChip.Queue.Protocols.NameServers.Requests;
using OceanChip.Queue.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class RegisterBrokerRequestHandler:AbstractRequestHandler<BrokerRegistrationRequest>
    {

        public RegisterBrokerRequestHandler(NameServerController nameServerController)
            :base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context,BrokerRegistrationRequest request)
        {
            _nameServerController.ClusterManager.RegisterBroker(context.Connection, request);
            return EmptyBytes;
        }
    }
}
