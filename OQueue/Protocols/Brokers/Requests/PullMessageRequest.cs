using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers.Requests
{
    [Serializable]
    public class PullMessageRequest
    {
        public string ConsumerId { get; set; }
        public string ConsumerGroup { get; set; }
        public MessageQueue MessageQueue { get; set; }
        public string Tags { get; set; }
        public long QueueOffset { get; set; }
        public int PullMessageBatchSize { get; set; }
        public long SuspendPullRequestMilliseconds { get; set; }
        public ConsumeFromWhere ConsumeFromWhere { get; set; }

        public static void WriteToStream(PullMessageRequest request,Stream stream)
        {
            using (var writer = new BinaryWriter(stream))
            {
                writer.Write(request.ConsumerId);
                writer.Write(request.ConsumerGroup);
                writer.Write(request.MessageQueue.BrokerName);
                writer.Write(request.MessageQueue.Topic);
                writer.Write(request.MessageQueue.QueueId);
                writer.Write(request.Tags);
                writer.Write(request.PullMessageBatchSize);
                writer.Write(request.SuspendPullRequestMilliseconds);
                writer.Write((int)request.ConsumeFromWhere);
            }
        }
        public static PullMessageRequest ReadFromStream(Stream stream)
        {
            using (var reader = new BinaryReader(stream))
            {
                return new PullMessageRequest()
                {
                    ConsumerId = reader.ReadString(),
                    ConsumerGroup = reader.ReadString(),
                    MessageQueue = new MessageQueue(reader.ReadString(), reader.ReadString(), reader.ReadInt32()),
                    Tags = reader.ReadString(),
                    QueueOffset=reader.ReadInt64(),
                    PullMessageBatchSize=reader.ReadInt32(),
                    SuspendPullRequestMilliseconds=reader.ReadInt64(),
                    ConsumeFromWhere=(ConsumeFromWhere)reader.ReadInt32(),
                };
            }
        }
    }
}
