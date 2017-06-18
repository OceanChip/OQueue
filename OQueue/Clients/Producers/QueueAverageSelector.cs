using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Queue.Protocols;
using System.Threading;

namespace OceanChip.Queue.Clients.Producers
{
    public class QueueAverageSelector : IQueueSelector
    {
        private long _index;
        public MessageQueue SelectMessageQueue(IList<MessageQueue> availableMessageQueues, Message message, string routingKey)
        {
            if (availableMessageQueues.Count == 0)
                return null;

            return availableMessageQueues[(int)(Interlocked.Increment(ref _index) % availableMessageQueues.Count)];
        }
    }
}
