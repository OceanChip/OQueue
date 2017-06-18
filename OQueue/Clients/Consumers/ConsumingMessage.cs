using OceanChip.Queue.Protocols;

namespace OceanChip.Queue.Clients.Consumers
{
    public class ConsumingMessage
    {
        public QueueMessage Message { get; private set; }
        public PullRequest PullRequest { get; private set; }
        public bool IsIgnored { get; set; }

        public ConsumingMessage(QueueMessage message,PullRequest pullRequest)
        {
            this.Message = message;
            this.PullRequest = pullRequest;
            message.BrokerName = pullRequest.MessageQueue.BrokerName;
        }
    }
}