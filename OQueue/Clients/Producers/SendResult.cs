using OceanChip.Queue.Protocols;

namespace OceanChip.Queue.Clients.Producers
{
    public class SendResult
    {
        public SendStatus SendStatus { get; private set; }
        public MessageStoreResult MessageStoreResult { get; private set; }
        public string ErrorMessage { get; private set; }


        public SendResult(SendStatus status, MessageStoreResult result, string errorMessage)
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