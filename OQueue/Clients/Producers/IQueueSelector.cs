using OceanChip.Queue.Protocols;
using System.Collections.Generic;

namespace OceanChip.Queue.Clients.Producers
{
    public interface IQueueSelector
    {
        MessageQueue SelectMessageQueue(IList<MessageQueue> availableMessageQueues, Message message, string routingKey);
    }
}