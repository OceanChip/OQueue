using OceanChip.Common.Components;
using OceanChip.Common.Logging;
using OceanChip.Common.Serializing;
using OceanChip.Common.Storage;
using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker
{
    public class Queue:IQueue
    {
        private const string QueueSettingFileName = "queue.setting";
        private readonly ChunkWriter _chunkWriter;
        private readonly ChunkReader _chunkReader;
        private readonly ChunkManager _chunkManager;
        private readonly IJsonSerializer _json;
        private readonly string _queueSettingFile;
        private QueueSetting _setting;
        private long _nextOffset = 0;
        private ILogger _logger;

        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long NextOffset => _nextOffset;
        public QueueSetting Setting => _setting;
        public QueueKey Key { get; private set; }

        public Queue(string topic,int queueId)
        {
            Topic = topic;
            this.QueueId = queueId;
            Key = new QueueKey(topic, queueId);

            _json = ObjectContainer.Resolve<IJsonSerializer>();
           // _chunkManager=new ChunkManager("QueueChunk-"+Key.ToString(),)
           
        }
    }
    public class QueueSetting
    {
        public bool ProducerVisible;
        public bool ConsumerVisible;
        public bool IsDeleted;

        public QueueSetting()
        {
            ProducerVisible = true;
            ConsumerVisible = true;
            IsDeleted = false;
        }
    }
}
