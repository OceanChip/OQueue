using OceanChip.Queue.Protocols;
using System;
using System.Collections.Generic;

namespace OceanChip.Queue.Clients.Consumers
{
    public class PullRequest
    {
        public string ConsumerId { get; private set; }
        public string GroupName { get; private set; }
        public MessageQueue MessageQueue { get; private set; }
        public ProcessQueue ProcessQueue { get; private set; }
        public long NextConsumeOffset { get; set; }
        public DateTime PullStartTime { get; set; }
        public HashSet<string> Tags { get; set; }
        public bool IsDropped { get; set; }

        public PullRequest(string consumerId,string groupName,MessageQueue mq,long nextConsumeOffset,HashSet<string> tags)
        {
            this.ConsumerId = consumerId;
            this.GroupName = groupName;
            this.MessageQueue = mq;
            this.NextConsumeOffset = nextConsumeOffset;
            this.ProcessQueue = new ProcessQueue();
            this.Tags = tags ?? new HashSet<string>();
        }
        public override string ToString()
        {
            return $"[ConsumeId:{ConsumerId},Group:{GroupName},MessageQueue:{MessageQueue},NextConsumeOffset:{NextConsumeOffset},Tags:{string.Join("|",Tags)},IsDropped:{IsDropped}]";
        }
    }
}