using OceanChip.Queue.Protocols;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Clients.Producers
{
    public class BatchSendResult
    {
        public SendStatus SendStatus { get; private set; }
        public BatchMessageStoreResult MessageStoreResult { get; private set; }
        public string ErrorMessage { get; private set; }

        public BatchSendResult(SendStatus status,BatchMessageStoreResult result,string errorMessage)
        {
            this.SendStatus = status;
            this.MessageStoreResult = result;
            this.ErrorMessage = errorMessage;
        }
        public override string ToString()
        {
            return $"[SendStatus:{SendStatus},MessageStoreResult:{MessageStoreResult},ErrorMessage:{ErrorMessage}]";
        }
    }
}
