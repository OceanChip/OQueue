using OceanChip.Common.Components;
using OceanChip.Common.Remoting;
using OceanChip.Common.Serializing;
using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.RequestHandlers
{
    public class UpdateQueueConsumeOffsetRequestHandler : IRequestHandler
    {
        private IConsumeOffsetStore _offsetStore;
        private IBinarySerializer _binarySerializer;
        private readonly ITpsStatisticService _tpsStatisticService;

        public UpdateQueueConsumeOffsetRequestHandler()
        {
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _tpsStatisticService = ObjectContainer.Resolve<ITpsStatisticService>();
        }
        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
                return null;

            var request = _binarySerializer.Deserialize<UpdateQueueOffsetRequest>(remotingRequest.Body);
            _offsetStore.UpdateConsumeOffset(
                request.MessageQueue.Topic,
                request.MessageQueue.QueueId,
                request.ConsumerGroup,
                request.QueueOffset
                );
            _tpsStatisticService.UpdateTopicConsumeOffset(
                request.MessageQueue.Topic,
                request.MessageQueue.QueueId,
                request.ConsumerGroup,
                request.QueueOffset);
            return null;
        }
    }
}
