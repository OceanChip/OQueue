using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Queue.Protocols.Brokers;
using OceanChip.Common.Scheduling;
using OceanChip.Common.Serializing;
using OceanChip.Common.Logging;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using OceanChip.Common.Extensions;

namespace OceanChip.Queue.Broker
{
    public class DefaultConsumeOffsetStore : IConsumeOffsetStore
    {
        private const string TaskName = "PersistConsumeOffsetInfo";
        private const string ConsumeOffsetFileName = "consume-offsets.json";
        private const string ConsumeOffsetBackupFileName = "consume-offsets-backup.json";
        private readonly IScheduleService _scheduleService;
        private readonly IJsonSerializer _json;
        private readonly ILogger _logger;
        private string _consumeOffsetFile;
        private string _consumeOffsetBackupFile;
        private ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>> _groupConsumeOffsetDict;
        private ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>> _groupNextConsumeOffsetDict;
        private int _isPersistingOffsets;

        public DefaultConsumeOffsetStore(IScheduleService scheduleService,IJsonSerializer json,ILoggerFactory loggerFactory)
        {
            this._groupConsumeOffsetDict = new ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>>();
            this._groupNextConsumeOffsetDict = new ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>>();
            this._scheduleService = scheduleService;
            this._json = json;
            _logger = loggerFactory.Create(GetType().FullName);
        }
        public void DeleteConsumeOffset(QueueKey queueKey)
        {
            foreach(var dict in _groupConsumeOffsetDict.Values)
            {
                var keys = dict.Keys.Where(x => x == queueKey);
                foreach(var key in keys)
                {
                    dict.Remove(key);
                }
            }
            PersistConsumeOffsetInfo();
        }

        public bool DeleteConsumerGroup(string group)
        {
            return _groupConsumeOffsetDict.Remove(group);
        }

        public IEnumerable<string> GetAllConsumeGroupNames()
        {
            return _groupConsumeOffsetDict.Keys.ToList();
        }

        public IEnumerable<TopicConsumeInfo> GetAllTopicConsumeInfoList()
        {
            var topicConsumerInfoList = new List<TopicConsumeInfo>();
            foreach(var entry in _groupConsumeOffsetDict)
            {
                var groupName = entry.Key;
                var consumeInfoDict = entry.Value;
                foreach(var key in consumeInfoDict)
                {
                    var queueKey = key.Key;
                    var consumedOffset = key.Value;
                    topicConsumerInfoList.Add(new TopicConsumeInfo
                    {
                        ConsumerGroup = groupName,
                        Topic = queueKey.Topic,
                        QueueId = queueKey.QueueId,
                        ConsumedOffset = consumedOffset,
                    });
                }
            }
            topicConsumerInfoList.Sort(SortTopicConsumeInfo);
            return topicConsumerInfoList;
        }

        public IEnumerable<QueueKey> GetConsumeKeys()
        {
            var keyList = new List<QueueKey>();
            foreach (var dict in _groupConsumeOffsetDict.Values)
            {
                foreach (var key in dict.Keys)
                {
                    if (!keyList.Contains(key))
                        keyList.Add(key);
                }
            }
            return keyList;
        }

        public long GetConsumeOffset(string topic, int queueId, string group)
        {
            ConcurrentDictionary<QueueKey, long> queueOffsetDict;
            if(_groupConsumeOffsetDict.TryGetValue(group,out queueOffsetDict))
            {
                long offset;
                var key = new QueueKey(topic, queueId);
                if (queueOffsetDict.TryGetValue(key, out offset))
                    return offset;
            }
            return -1L;
        }

        public int GetConsumerGroupCount()
        {
            return _groupConsumeOffsetDict.Count;
        }

        public long GetMinConsumeOffset(string topic, int queueId)
        {
            var key = new QueueKey(topic, queueId);
            var minOffset = -1L;
            foreach(var dict in _groupConsumeOffsetDict.Values)
            {
                long offset;
                if(dict.TryGetValue(key,out offset))
                {
                    if (minOffset == -1)
                        minOffset = offset;
                    else if (offset < minOffset)
                        minOffset = offset;
                }
            }
            return minOffset;
        }

        public IEnumerable<TopicConsumeInfo> GetTopicConsumeInfoList(string groupName, string topic)
        {
            var topicConsumeInfoList = new List<TopicConsumeInfo>();
            if(!string.IsNullOrEmpty(groupName) && !string.IsNullOrEmpty(topic))
            {
                ConcurrentDictionary<QueueKey, long> found;
                if(_groupConsumeOffsetDict.TryGetValue(groupName,out found))
                {
                    foreach(var entry in found.Where(x => x.Key.Topic == topic))
                    {
                        var queueKey = entry.Key;
                        var consumedOffset = entry.Value;
                        topicConsumeInfoList.Add(new TopicConsumeInfo
                        {
                            ConsumerGroup = groupName,
                            Topic = queueKey.Topic,
                            QueueId = queueKey.QueueId,
                            ConsumedOffset = consumedOffset,
                        });
                    }
                }
                topicConsumeInfoList.Sort(SortTopicConsumeInfo);
            }
            return topicConsumeInfoList;
        }

        public void SetConsumeNextOffset(string topic, int queueId, string group, long nextOffset)
        {
            var queueOffsetDict = _groupConsumeOffsetDict.GetOrAdd(group, k =>
            {
                return new ConcurrentDictionary<QueueKey, long>();
            });
            var key = new QueueKey(topic, queueId);
            queueOffsetDict[key] = nextOffset;
        }

        public void Shutdown()
        {
            if (BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
                return;
            PersistConsumeOffsetInfo();
            _scheduleService.StopTask(TaskName);
        }

        public void Start()
        {
            if (BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
                return;

            var path = BrokerController.Instance.Setting.FileStoreRootPath;
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            _consumeOffsetFile = Path.Combine(path, ConsumeOffsetFileName);
            _consumeOffsetBackupFile = Path.Combine(path, ConsumeOffsetBackupFileName);

            LoadConsumeOffsetInfo();
            _scheduleService.StartTask(TaskName, PersistConsumeOffsetInfo, 1000 * 5, BrokerController.Instance.Setting.PersistConsumeOffsetInterval);
        }

     

        public bool TryFetchNextConsumeOffset(string topic, int queueId, string group, out long nextOffset)
        {
            nextOffset = 0L;
            ConcurrentDictionary<QueueKey, long> found;
            if(_groupConsumeOffsetDict.TryGetValue(group,out found))
            {
                long offset;
                var key = new QueueKey(topic, queueId);
                if (found.TryRemove(key, out offset))
                {
                    nextOffset = offset;
                    return true;
                }
            }
            return false;
        }

        public void UpdateConsumeOffset(string topic, int queueId, string group, long offset)
        {
            var queueOffsetDict = _groupConsumeOffsetDict.GetOrAdd(group, k => new ConcurrentDictionary<QueueKey, long>());
            var key = new QueueKey(topic, queueId);
            queueOffsetDict[key] = offset;
        }
        private int SortTopicConsumeInfo(TopicConsumeInfo x,TopicConsumeInfo y)
        {
            var result = string.Compare(x.ConsumerGroup, y.ConsumerGroup);
            if (result != 0)
                return result;
            result = string.Compare(x.Topic, y.Topic);
            if (result != 0)
                return result;
            if (x.QueueId > y.QueueId)
                return 1;
            else if (x.QueueId < y.QueueId)
                return -1;
            return 0;
        }
        private void PersistConsumeOffsetInfo()
        {
            if (string.IsNullOrEmpty(_consumeOffsetFile))
            {
                return;
            }
            if(Interlocked.CompareExchange(ref _isPersistingOffsets, 1, 0) == 0)
            {
                try
                {
                    using (var stream = new FileStream(_consumeOffsetFile, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite))
                    {
                        using (var writer = new StreamWriter(stream))
                        {
                            var toDict = ConvertDictTo(_groupConsumeOffsetDict);
                            var json = _json.Serialize(toDict);
                            writer.Write(json);
                            writer.Flush();
                            stream.Flush(true);
                        }
                    }
                    File.Copy(_consumeOffsetFile, _consumeOffsetBackupFile, true);
                }catch(Exception ex)
                {
                    _logger.Error("持久化消费偏移发生异常", ex);
                }
                finally
                {
                    Interlocked.Exchange(ref _isPersistingOffsets, 0);
                }
            }
        }
        private ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, long>>> ConvertDictTo(ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>> source)
        {
            var toDict = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, long>>>();

            foreach(var entry in source)
            {
                var key = entry.Key;
                var dict = toDict.GetOrAdd(key, x => new ConcurrentDictionary<string, ConcurrentDictionary<int, long>>());

                foreach(var entry2 in entry.Value)
                {
                    var topic = entry2.Key.Topic;
                    var queueId = entry2.Key.QueueId;
                    var offset = entry2.Value;
                    var dict2 = dict.GetOrAdd(topic, x => new ConcurrentDictionary<int, long>());
                    dict2.TryAdd(queueId, offset);
                }
            }
            return toDict;
        }
        private ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>> ConvertDictFrom(ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, long>>> source)
        {
            var toDict = new ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>>();
            foreach(var entry in source)
            {
                var key = entry.Key;
                var dict = toDict.GetOrAdd(key, x => new ConcurrentDictionary<QueueKey, long>());
                foreach(var entry2 in entry.Value)
                {
                    var topic = entry2.Key;
                    foreach(var entry3 in entry2.Value)
                    {
                        var queueId = entry3.Key;
                        var offset = entry3.Value;
                        var queueKey = new QueueKey(topic, queueId);
                        dict.TryAdd(queueKey, offset);
                    }
                }
            }
            return toDict;
        }
        private void LoadConsumeOffsetInfo()
        {
            try
            {
                LoadConsumeOffsetInfo(_consumeOffsetFile);
            }
            catch
            {
                LoadConsumeOffsetInfo(_consumeOffsetBackupFile);
            }
        }
        private void LoadConsumeOffsetInfo(string file)
        {
            using(var stream = new FileStream(file, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                using (var reader = new StreamReader(stream))
                {
                    var json = reader.ReadToEnd();
                    if (!string.IsNullOrEmpty(json))
                    {
                        try
                        {
                            var dict = _json.Deserialize<ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, long>>>>(json);
                            _groupConsumeOffsetDict = ConvertDictFrom(dict);
                        }catch(Exception ex)
                        {
                            _logger.Error("反序列化消费偏移失败,文件:"+file,ex);
                            throw;
                        }
                    }
                    else
                    {
                        _groupConsumeOffsetDict = new ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>>();
                    }
                }
            }
        }
    }
}
