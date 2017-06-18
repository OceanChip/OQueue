using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Broker.LongPolling;
using OceanChip.Queue.Protocols.Brokers.Requests;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class SetQueueNextConsumeOffsetRequestHandler : AbstractRequestHanlder<SetQueueNextConsumeOffsetRequest, IConsumeOffsetStore, SuspendedPullRequestManager>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, SetQueueNextConsumeOffsetRequest request)
        {
            _service1.SetConsumeNextOffset(
                request.Topic,
                request.QueueId,
                request.ConsumerGroup,
                request.NextOffset);
            _service2.RemovePullRequest(request.ConsumerGroup, request.Topic, request.QueueId);
            return EmptyBytes;
        }
    }
}
