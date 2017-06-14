using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Requests
{
    [Serializable]
    public class GetMessageDetailRequest
    {
        public string MessageId { get; set; }
        public GetMessageDetailRequest(string messageId)
        {
            this.MessageId = messageId;
        }
        public override string ToString()
        {
            return $"[Message={MessageId}]";
        }
    }
}
