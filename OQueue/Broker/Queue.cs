using OceanChip.Common.Components;
using OceanChip.Common.Extensions;
using OceanChip.Common.Logging;
using OceanChip.Common.Serializing;
using OceanChip.Common.Storage;
using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.IO;
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
            _chunkManager = new ChunkManager("QueueChunk-" + Key.ToString(),
                BrokerController.Instance.Setting.QueueChunkConfig,
                BrokerController.Instance.Setting.IsMessageStoreMemoryMode, Topic + @"\" + QueueId);
            _chunkWriter = new ChunkWriter(_chunkManager);
            _chunkReader = new ChunkReader(_chunkManager,_chunkWriter);
            _queueSettingFile = Path.Combine(_chunkManager.ChunkPath, QueueSettingFileName);
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
           
        }

        public long GetMinQueueOffset()
        {
            if (_nextOffset == 0L)
                return -1L;
            return _chunkManager.GetFirstChunk().ChunkHeader.ChunkDataStartPosition / _chunkManager.Config.ChunkDataUnitSize;
        }

        public long GetMessagePosition(long queueOffset, out int tagCode, bool autoCache=true)
        {
            tagCode = 0;

            var position = queueOffset * _chunkManager.Config.ChunkDataUnitSize;
            var record = _chunkReader.TryReadAt(position, ReadMessageIndex, autoCache);
            if (record == null)
                return -1L;
            tagCode = record.TagCode;
            return record.MessageLogPosition - 1;
        }

        public void SetConsumerVisibile(bool visible)
        {
            _setting.ConsumerVisible = visible;
            SaveQueueSetting();
        }
        public void SetProducerVisibile(bool visible)
        {
            _setting.ProducerVisible = visible;
            SaveQueueSetting();
        }

        public void Delete()
        {
            _setting.IsDeleted = true;
            SaveQueueSetting();

            Close();

            if (!_chunkManager.IsMemoryMode)
            {
                Directory.Delete(_chunkManager.ChunkPath, true);
            }
        }

        public void Load()
        {
            _setting = LoadQuerySetting();
            if (_setting == null)
            {
                _setting = new QueueSetting();
                SaveQueueSetting();
            }
            if (_setting.IsDeleted)
                return;
            _chunkManager.Load(ReadMessageIndex);
            _chunkWriter.Open();

            var lastChunk = _chunkManager.GetLastChunk();
            var lastOffsetGlobalPosition = lastChunk.DataPosition + lastChunk.ChunkHeader.ChunkDataStartPosition;
            if (lastOffsetGlobalPosition > 0)
            {
                _nextOffset = lastOffsetGlobalPosition / _chunkManager.Config.ChunkDataUnitSize;
            }
        }

        public void Close()
        {
            _chunkWriter.Close();
            _chunkManager.Close();
        }

        public void AddMessage(long messagePosition,string messageTag)
        {
            _chunkWriter.Write(new QueueLogRecord(messagePosition + 1, messageTag.GetStringHashcode()));
        }
        private QueueLogRecord ReadMessageIndex(byte[] recordBuffer)
        {
            var record = new QueueLogRecord();
            record.ReadFrom(recordBuffer);
            if (record.MessageLogPosition <= 0)
                return null;
            return record;
        }

        private void SaveQueueSetting()
        {
            if (_chunkManager.IsMemoryMode)
                return;

            if (!Directory.Exists(_chunkManager.ChunkPath))
            {
                Directory.CreateDirectory(_chunkManager.ChunkPath);
            }
            using (var stream = new FileStream(_queueSettingFile, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(_json.Serialize(_setting));
                }
            }
        }

        private QueueSetting LoadQuerySetting()
        {
            if (_chunkManager.IsMemoryMode)
                return null;

            if (!Directory.Exists(_chunkManager.ChunkPath))
            {
                Directory.CreateDirectory(_chunkManager.ChunkPath);
            }
            using (var stream = new FileStream(_queueSettingFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                using (var reader = new StreamReader(stream))
                {
                    var text = reader.ReadToEnd();
                    if (!string.IsNullOrEmpty(text))
                    {
                        return _json.Deserialize<QueueSetting>(text);
                    }
                    return null;
                }
            }
        }


        public void DeleteMessages(long minMessagePosition)
        {
            var chunks = _chunkManager.GetAllChunks().Where(x => x.IsCompleted).OrderBy(x => x.ChunkHeader.ChunkNumber);
            foreach(var chunk in chunks)
            {
                var maxPosition = chunk.ChunkHeader.ChunkDataEndPosition - _chunkManager.Config.ChunkDataUnitSize;
                var record = _chunkReader.TryReadAt(maxPosition, ReadMessageIndex, false);
                if (record == null)
                    continue;

                var chunkLastMessagePosition = record.MessageLogPosition - 1;
                if(chunkLastMessagePosition<minMessagePosition)
                {
                    if (_chunkManager.RemoveChunk(chunk))
                    {
                        _logger.InfoFormat("Queue[{0}，{1}] chunk:#{2}已经删除，ChunkLastMessagePosition:{3},MessageStoreMinMessagePosition:{4}",
                            Topic, QueueId, chunk.ChunkHeader.ChunkNumber, chunkLastMessagePosition, minMessagePosition);
                    }
                }
            }
        }

        public long IncrementNextOffset()
        {
            return _nextOffset++;
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
