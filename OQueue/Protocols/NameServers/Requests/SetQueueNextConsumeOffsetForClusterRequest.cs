﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.NameServers.Requests
{
    [Serializable]
    public class SetQueueNextConsumeOffsetForClusterRequest
    {
        public string ClusterName { get; set; }
        public string ConsumerGroup { get; set; }
        public string Topic { get; set; }
        public int QueueId { get; set; }
        public long NextOffset { get; set; }
    }
}
