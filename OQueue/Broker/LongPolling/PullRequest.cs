using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.LongPolling
{
    public class PullRequest
    {
        public RemotingRequest RemotingRequest { get; private set; }
        public PullMessageRequest PullMessageRequest { get; private set; }
        public IRequestHandlerContext RequestHandlerContext { get; private set; }
        public DateTime SuspendStartTime { get; private set; }
        public long SuspendMilliseconds { get; private set; }
        public Action<PullRequest> NewMessageArrivedAction { get; private set; }
        public Action<PullRequest> TimeoutAction { get; private set; }
        public Action<PullRequest> NoNewMessageAction { get; private set; }
        public Action<PullRequest> ReplaceAction { get; private set; }
        public PullRequest(
            RemotingRequest remotingRequest,
            PullMessageRequest messageRequest,
            IRequestHandlerContext handlerContext,
            DateTime suspendStartTime,
            long suspendMilliseconds,
            Action<PullRequest> newMessageArrivedAction,
            Action<PullRequest> timeoutAction,
            Action<PullRequest> noNewMessagedAction,
            Action<PullRequest> replacedAction
            )
        {
            this.RemotingRequest = remotingRequest;
            this.PullMessageRequest = messageRequest;
            this.RequestHandlerContext = handlerContext;
            this.SuspendMilliseconds = suspendMilliseconds;
            this.SuspendStartTime = suspendStartTime;
            this.NewMessageArrivedAction = newMessageArrivedAction;
            this.TimeoutAction = timeoutAction;
            this.NoNewMessageAction = noNewMessagedAction;
            this.ReplaceAction = replacedAction;
        }
        public bool isTimeout()
        {
            return (DateTime.Now - SuspendStartTime).TotalMilliseconds >= SuspendMilliseconds;
        }
    }
}
