using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Request
{
    [Serializable]
    public class BatchSendMessageRequest
    {
        public int QueueId { get; set; }
        public IEnumerable<Message> Messages { get; set; }
        public string ProducerAddress { get; set; }
    }
}
