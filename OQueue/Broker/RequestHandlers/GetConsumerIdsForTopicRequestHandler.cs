using OceanChip.Common.Components;
using OceanChip.Common.Remoting;
using OceanChip.Common.Serializing;
using OceanChip.Queue.Broker.Client;
using OceanChip.Queue.Protocols.Brokers.Requests;
using OceanChip.Queue.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.RequestHandlers
{
    public class GetConsumerIdsForTopicRequestHandler:IRequestHandler
    {
        private ConsumerManager _consumerManager;
        private IBinarySerializer _binarySerializer;
        public GetConsumerIdsForTopicRequestHandler()
        {
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                return RemotingResponseFactory.CreateResponse(remotingRequest, Encoding.UTF8.GetBytes(string.Empty));
            }

            var request = _binarySerializer.Deserialize<GetConsumerIdsForTopRequest>(remotingRequest.Body);
            var group = _consumerManager.GetConsumerGroup(request.GroupName);
            var consumerIdList = new List<string>();
            if (group != null)
            {
                consumerIdList = group.GetConsumerIdsForTopic(request.Topic).ToList();
                consumerIdList.Sort();
            }
            return RemotingResponseFactory.CreateResponse(remotingRequest, Encoding.UTF8.GetBytes(string.Join(",", consumerIdList)));
        }
    }
}
