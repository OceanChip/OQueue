using System;
using OceanChip.Queue.Protocols;

namespace OceanChip.Queue.Clients.Consumers
{
    public class MessageContext : IMessageContext
    {
        public Action<QueueMessage> MessageHandledAction { get; private set; }

        public MessageContext(Action<QueueMessage> messageHandledAction)
        {
            this.MessageHandledAction = messageHandledAction;
        }

        public void OnMessageHandled(QueueMessage message)
        {
            MessageHandledAction(message);
        }
    }
}