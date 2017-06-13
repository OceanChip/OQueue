using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols
{
    [Serializable]
    public class BatchMessageStoreResult
    {
        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public IEnumerable<BatchMessageItemResult> MessageResults { get; private set; }

        public BatchMessageStoreResult() { }
        public BatchMessageStoreResult(string topic,int queueId,IEnumerable<BatchMessageItemResult> results)
        {
            this.Topic = topic;
            this.QueueId = queueId;
            this.MessageResults = results;
        }
        public override string ToString()
        {
            return $"[Topic={Topic},QueueId={QueueId},MessageResults=[{string.Join(",", MessageResults)}]]";
        }
    }
}
