using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols
{
    [Serializable]
    public class BatchMessageItemResult
    {
        public string MessageId { get; set; }
        public int Code { get; set; }
        public long QueueOffset { get; set; }

        public string Tag { get; set; }
        public DateTime CreatedTime { get; set; }
        public DateTime StoredTime { get; set; }
        public BatchMessageItemResult() { }
        public BatchMessageItemResult(string messageId, int code,   long queueOffset, string tag, DateTime createdTime, DateTime storedTime)
        {
            this.MessageId = messageId;
            this.Code = code;
            this.Tag = tag;
            this.QueueOffset = queueOffset;
            this.CreatedTime = createdTime;
            this.StoredTime = storedTime;
        }
        public override string ToString()
        {
            return $"[MessageId={MessageId},Code={Code},QueueOffset={QueueOffset},Tag={Tag},CreatedTime={CreatedTime},StoredTime={StoredTime}]";
        }
    }
}
