using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker
{
    public interface IQueue
    {
        string Topic { get; }
        int QueueId { get; }
        long NextOffset { get; }
        long IncrementNextOffset();
    }
}
