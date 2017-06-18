using OceanChip.Common.Components;
using OceanChip.Common.Remoting;
using OceanChip.Common.Serializing;
using OceanChip.Queue.Broker.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.RequestHandlers
{
    public class ProducerHeartbeatRequestHandler : IRequestHandler
    {
        private ProducerManager _producerManager;
        private IBinarySerializer _binarySerializer;

        public ProducerHeartbeatRequestHandler()
        {
            _producerManager = ObjectContainer.Resolve<ProducerManager>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var producerId = Encoding.UTF8.GetString(remotingRequest.Body);
            _producerManager.RegisterProducer(context.Connection, producerId);
            return null;

        }
    }
}
