using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Queue.Protocols;
using OceanChip.Common.Storage;
using OceanChip.Queue.Broker.DeleteMessageStrategies;
using OceanChip.Common.Scheduling;
using OceanChip.Common.Utilities;
using OceanChip.Common.Logging;

namespace OceanChip.Queue.Broker
{
    public class DefaultMessageStore : IMessageStore, IDisposable
    {
        private const string TaskName = "DeleteMessages";
        private ChunkManager _chunkManager;
        private ChunkWriter _chunkWriter;
        private ChunkReader _chunkReader;
        private readonly IDeleteMessageStrategy _deleteMessageStragegy;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private long _minConsumedMessagePosition = -1;
        private BufferQueue<MessageLogRecord> _bufferQueue;
        private BufferQueue<BatchMessageLogRecord> _batchMessageBufferQueue;
        private readonly object _lockObj = new object();
        
        public int MaxChunkNum => _chunkManager.GetLastChunk().ChunkHeader.ChunkNumber;

        public long MinMessagePosition => _chunkManager.GetFirstChunk().ChunkHeader.ChunkDataStartPosition;

        public long CurrentMessagePosition => _chunkWriter.CurrentChunk.GlobalDataPosition;

        public int ChunkCount => _chunkManager.GetChunkCount();

        public int MinChunkNum => _chunkManager.GetFirstChunk().ChunkHeader.ChunkNumber;

        public DefaultMessageStore(IDeleteMessageStrategy deleteMessageStategy,IScheduleService scheduleService,ILoggerFactory loggerFactory)
        {
            _deleteMessageStragegy = deleteMessageStategy;
            _scheduleService = scheduleService;
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public void BatchStoreMessageAsync(IQueue queue, IEnumerable<Message> messages, Action<BatchMessageLogRecord, object> callback, object parameter, string producerAddress)
        {
            lock (_lockObj)
            {
                var recordList = new List<MessageLogRecord>();
                foreach (var message in messages)
                {
                    var record = new MessageLogRecord(
                        queue.Topic,
                        message.Code,
                        message.Body,
                        queue.QueueId,
                        queue.NextOffset,
                        message.CreatedTime,
                        DateTime.Now,
                        message.Tag,
                        producerAddress ?? string.Empty,
                        null,
                        null);
                    recordList.Add(record);
                    queue.IncrementNextOffset();
                }
                var batchRecord = new BatchMessageLogRecord(recordList, callback, parameter);
                _batchMessageBufferQueue.EnqueueMessage(batchRecord);
            }
        }

        public void Dispose()
        {
            Shutdown();
        }

        public QueueMessage GetMessage(long position)
        {
            var buffer = GetMessageBuffer(position);
            if(buffer != null)
            {
                var nextOffset = 0;
                var messageLength = ByteUtil.DecodeInt(buffer, nextOffset, out nextOffset);
                if (messageLength > 0)
                {
                    var message = new QueueMessage();
                    var messageBytes = new byte[messageLength];
                    Buffer.BlockCopy(buffer, nextOffset, messageBytes, 0, messageLength);
                    message.ReadForm(messageBytes);
                    return message;
                }
            }
            return null;
        }

        public byte[] GetMessageBuffer(long position)
        {
            var record = _chunkReader.TryReadRecrodBufferAt(position);
            if (record != null)
                return record.RecordBuffer;
            return null;
        }

        public bool IsMessagePositionExists(long position)
        {
            var chunk = _chunkManager.GetChunkFor(position);
            return chunk != null;
        }

        public void Load()
        {
            _bufferQueue = new BufferQueue<MessageLogRecord>("MessageBufferQueue", BrokerController.Instance.Setting.MessageWriteQueueThreshold, PersistMessages, _logger);
            _batchMessageBufferQueue = new BufferQueue<BatchMessageLogRecord>("BatchMessageBufferQueue", BrokerController.Instance.Setting.BatchMessageWriteQueueThreshold, BatchPersistMessages, _logger);
            _chunkManager = new ChunkManager("MessageChunk", BrokerController.Instance.Setting.MessageChunkConfig, BrokerController.Instance.Setting.IsMessageStoreMemoryMode);
            _chunkWriter = new ChunkWriter(_chunkManager);
            _chunkReader = new ChunkReader(_chunkManager, _chunkWriter);
            _chunkManager.Load(ReadMessage);
        }

        public void Start()
        {
            _chunkWriter.Open();
            _scheduleService.StartTask(TaskName, DeleteMessages, 5 * 1000, BrokerController.Instance.Setting.DeleteMessageInterval);
        }

        private void DeleteMessages()
        {
            var chunks = _deleteMessageStragegy.GetAllowDeleteChunks(_chunkManager, _minConsumedMessagePosition);
            foreach(var chunk in chunks)
            {
                if (_chunkManager.RemoveChunk(chunk))
                {
                    _logger.Info($"Message Chunk:#{chunk.ChunkHeader.ChunkNumber} is deleted,ChunkPositionScale:[{chunk.ChunkHeader.ChunkDataStartPosition},{chunk.ChunkHeader.ChunkDataEndPosition}],MinConsumeMessagePosition:{_minConsumedMessagePosition}");
                }
            }
        }

        public void Shutdown()
        {
            _scheduleService.StopTask(TaskName);
            _chunkWriter.Close();
            _chunkManager.Close();
        }

        public void StoreMessageAsync(IQueue queue, Message message, Action<MessageLogRecord, object> callback, object parameter, string producerAddress)
        {
            lock (_lockObj)
            {
                var record = new MessageLogRecord(
                    message.Topic,
                    message.Code,
                    message.Body,
                    queue.QueueId,
                    queue.NextOffset,
                    message.CreatedTime,
                    DateTime.Now,
                    message.Tag,
                    producerAddress ?? string.Empty,
                    callback,
                    parameter
                    );
                _bufferQueue.EnqueueMessage(record);
                queue.IncrementNextOffset();
            }
        }

        public void UpdateMinConsumedMessagePosition(long minConsumedMessagePosition)
        {
            _minConsumedMessagePosition = minConsumedMessagePosition;
        }
        private ILogRecord ReadMessage(byte[] recordBuffer)
        {
            var record = new MessageLogRecord();
            record.ReadForm(recordBuffer);
            return record;
        }

        private void BatchPersistMessages(BatchMessageLogRecord messages)
        {
            foreach(var record in messages.Records)
            {
                _chunkWriter.Write(record);
            }
            messages.OnPersisted();
        }

        private void PersistMessages(MessageLogRecord record)
        {
            _chunkWriter.Write(record);
            record.OnPersisted();
        }
    }
}
