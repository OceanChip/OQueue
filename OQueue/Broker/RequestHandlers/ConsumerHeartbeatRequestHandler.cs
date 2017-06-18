using OceanChip.Common.Components;
using OceanChip.Common.Remoting;
using OceanChip.Common.Serializing;
using OceanChip.Queue.Broker.Client;
using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.RequestHandlers
{
    public class ConsumerHeartbeatRequestHandler : IRequestHandler
    {
        private ConsumerManager _consumerManager;
        private IBinarySerializer _binarySerializer;

        public ConsumerHeartbeatRequestHandler()
        {
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }
        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var data = _binarySerializer.Deserialize<ConsumerHeatbeatData>(remotingRequest.Body);
            _consumerManager.RegisterConsumer(data.GroupName, data.ConsumerId, data.SubscriptionTopics, data.ConsumingQueues, context.Connection);
            return null;
        }
    }
}
