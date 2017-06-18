using OceanChip.Common.Extensions;
using OceanChip.Common.Logging;
using OceanChip.Common.Scheduling;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker
{
    public class DefaultChunkStatisticService : IChunkStatisticService
    {
        private const string TaskName = "LogChunkStatisticStatus";
        private readonly ILogger _logger;
        private readonly IMessageStore _messageStore;
        private readonly IScheduleService _scheduleService;
        private ConcurrentDictionary<int, ByteInfo> _bytesWriteDict;
        private ConcurrentDictionary<int, CountInfo> _fileReadDict;
        private ConcurrentDictionary<int, CountInfo> _unManagedReadDict;
        private ConcurrentDictionary<int, CountInfo> _cachedReadDict;

        public DefaultChunkStatisticService(IMessageStore messageStore,IScheduleService scheduleService,ILoggerFactory logFactory)
        {
            this._messageStore = messageStore;
            this._scheduleService = scheduleService;
            this._logger = logFactory.Create("ChunkStatistic");
            this._bytesWriteDict = new ConcurrentDictionary<int, ByteInfo>();
            this._fileReadDict = new ConcurrentDictionary<int, CountInfo>();
            this._unManagedReadDict = new ConcurrentDictionary<int, CountInfo>();
            this._cachedReadDict = new ConcurrentDictionary<int, CountInfo>();
        }
        public void AddCachedReadCount(int chunkNum)
        {
            _cachedReadDict.AddOrUpdate(chunkNum, GetDefaultCountInfo, UpdateCountInfo);
        }

        public void AddFileReadCount(int chunkNum)
        {
            _fileReadDict.AddOrUpdate(chunkNum, GetDefaultCountInfo, UpdateCountInfo);
        }

        public void AddUnManagedReadCount(int chunkNum)
        {
            _unManagedReadDict.AddOrUpdate(chunkNum, GetDefaultCountInfo, UpdateCountInfo);
        }

        public void AddWriteBytes(int chunkNum, int byteCount)
        {
            _bytesWriteDict.AddOrUpdate(chunkNum, GetDefaultBytesInfo, (chunkNumber, current) => UpdateBytesInfo(chunkNumber, current, byteCount));
      }


        public void Shutdown()
        {
            if (!BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
            {
                _scheduleService.StopTask(TaskName);
            }
        }

        public void Start()
        {
            if (!BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
            {
                _scheduleService.StartTask(TaskName, LogChunkStatisticStatus,1000,1000);
            }
        }
        private CountInfo GetDefaultCountInfo(int chunkNum)
        {
            return new CountInfo { CurrentCount = 1 };
        }
        private CountInfo UpdateCountInfo(int chunkNum,CountInfo countInfo)
        {
            Interlocked.Increment(ref countInfo.CurrentCount);
            return countInfo;
        }
        private ByteInfo GetDefaultBytesInfo(int chunkNum)
        {
            return new ByteInfo();
        }
        private ByteInfo UpdateBytesInfo(int chunkNum, ByteInfo current, int byteCount)
        {
            Interlocked.Add(ref current.CurrentBytes, byteCount);
            return current;
        }
        private void LogChunkStatisticStatus()
        {
            if (_logger.IsDebugEnabled)
            {
                var bytesWriteStatus = UpdateWriteStatus(_bytesWriteDict);
                var unmaagedReadStatus = UpdateReadStatus(_unManagedReadDict);
                var fileReadStatus = UpdateReadStatus(_fileReadDict);
                var cachedReadStatus = UpdateReadStatus(_cachedReadDict);
                _logger.Debug($"maxChunk:#{_messageStore.MaxChunkNum},write:{bytesWriteStatus},unmanagedCacheRead:{unmaagedReadStatus},localCacheRead:{cachedReadStatus},fileRead:{fileReadStatus}");
            }
        }

        private string UpdateWriteStatus(ConcurrentDictionary<int, ByteInfo> bytesWriteDict)
        {
            var list = new List<string>();
            var toremoveKeys = new List<int>();

            foreach(var entry in bytesWriteDict)
            {
                var chunkNum = entry.Key;
                var throughput = entry.Value.UpgradeBytes() / 1024;
                if (throughput > 0)
                    list.Add($"[Chunk:#{chunkNum},bytes:{throughput}KB]");
                else
                    toremoveKeys.Add(chunkNum);
            }
            foreach(var key in toremoveKeys)
            {
                _bytesWriteDict.Remove(key);
            }
            return list.Count == 0 ? "[]" : string.Join(",", list);
        }
        private string UpdateReadStatus(ConcurrentDictionary<int,CountInfo> dict)
        {
            var list = new List<string>();
            foreach(var entry in dict)
            {
                var chunkNum = entry.Key;
                var throughput = entry.Value.UpgradeCount();
                if(throughput>0)
                    list.Add($"[Chunk:#{chunkNum},Count:{throughput}]");
            }
            return list.Count == 0 ? "[]" : string.Join(",", list);
        }

        class ByteInfo
        {
            public long PreviousBytes;
            public long CurrentBytes;

            public long UpgradeBytes()
            {
                var incrementBytes = CurrentBytes - PreviousBytes;
                PreviousBytes = CurrentBytes;
                return incrementBytes;
            }
        }
        class CountInfo
        {
            public long PreviousCount;
            public long CurrentCount;

            public long UpgradeCount()
            {
                var incrementBytes = CurrentCount - PreviousCount;
                PreviousCount = CurrentCount;
                return incrementBytes;
            }
        }
    }
}
