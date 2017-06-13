using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols
{
    [Serializable]
    public class MessageStoreResult
    {
        public string MessageId { get; set; }
        public int QueueId { get; set; }
        public long QueueOffset { get; set; }

        public string Tag { get; set; }
        public int Code { get; set; }
        public DateTime CreatedTime { get; set; }
        public DateTime StoredTime { get; set; }
        public string Topic { get; set; }

        public MessageStoreResult()
        {

        }
        public MessageStoreResult(string messageId,int code,string topic,int queueId,long queueOffset,string tag,DateTime createdTime,DateTime storedTime)
        {
            this.MessageId = messageId;
            this.Code = code;
            this.Topic = topic;
            this.QueueId = queueId;
            this.Tag = tag;
            this.QueueOffset = queueOffset;
            this.CreatedTime = createdTime;
            this.StoredTime = storedTime;
        }
        public override string ToString()
        {
            return $"[MessageId={MessageId},Code={Code},Topic={Topic},QueueId={QueueId},QueueOffset={QueueOffset},Tag={Tag},CreatedTime={CreatedTime},StoredTime={StoredTime}]";
        }
    }
}
