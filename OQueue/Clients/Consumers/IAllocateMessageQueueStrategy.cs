using OceanChip.Queue.Protocols;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Clients.Consumers
{
    public interface IAllocateMessageQueueStrategy
    {
        IEnumerable<MessageQueue> Allocate(string currentConsumerId, IList<MessageQueue> messageQueueList, IList<string> totalConsumerIds);
    }
}
