using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Queue.Protocols.Brokers;
using System.Collections.Concurrent;
using OceanChip.Common.Scheduling;
using OceanChip.Common.Logging;
using OceanChip.Common.Utilities;
using System.IO;
using OceanChip.Common.Extensions;
using System.Threading;

namespace OceanChip.Queue.Broker
{
    public class DefaultQueueStore : IQueueStore
    {
        private const string TaskName = "DeleteQueueMessage";
        public readonly ConcurrentDictionary<QueueKey, Queue> _queueDict;
        private readonly IMessageStore _messageStore;
        private readonly IConsumeOffsetStore _consumeOffsetStore;
        private readonly IScheduleService _scheduleService;
        private readonly ITpsStatisticService _tpsStaticticService;
        private readonly ILogger _logger;
        private readonly object _lockObj = new object();
        private int _isUpdatingMinConsumedMessagePosition;
        private int _isDeletingQueueMessage;

        public DefaultQueueStore(IMessageStore messageStore,
            IConsumeOffsetStore consumeOffsetStore,
            IScheduleService scheduleService,
            ITpsStatisticService tpsStatService,
            ILoggerFactory loggerFactory)
        {
            this._messageStore = messageStore;
            this._consumeOffsetStore = consumeOffsetStore;
            this._scheduleService = scheduleService;
            this._tpsStaticticService = tpsStatService;
            this._logger = loggerFactory.Create(GetType().FullName);
            this._queueDict = new ConcurrentDictionary<QueueKey, Queue>();
        }
        public void AddQueue(string topic)
        {
            lock (_lockObj)
            {
                Check.NotNullOrEmpty(topic, nameof(topic));

                var queues = _queueDict.Values.Where(x => x.Topic == topic);
                if (queues.Count() >= BrokerController.Instance.Setting.TopicMaxQueueCount)
                {
                    throw new ArgumentException($"队列数量不能超过{BrokerController.Instance.Setting.TopicMaxQueueCount}");
                }
                var queueId = queues.Count() == 0 ? 0 : queues.Max(x => x.QueueId) + 1;
                if (!IsQueueExists(topic, queueId))
                {
                    LoadQueue(topic, queueId);
                }
            }
        }

        public IEnumerable<int> CreateTopic(string topic, int? initialQueueCount)
        {
            lock (_lockObj)
            {
                Check.NotNullOrEmpty(topic, nameof(topic));

                if (initialQueueCount != null)
                {
                    Check.Positive(initialQueueCount.Value, nameof(initialQueueCount));
                }
                else
                {
                    initialQueueCount = BrokerController.Instance.Setting.TopicDefaultQueueCount;
                }
                if (IsTopicExists(topic))
                {
                    return _queueDict.Values.Where(x => x.Topic == topic && !x.Setting.IsDeleted).Select(x => x.QueueId).ToList();
                }
                //若大于最大值
                if (initialQueueCount > BrokerController.Instance.Setting.TopicMaxQueueCount)
                {
                    throw new ArgumentException($"初始化队列数量[{initialQueueCount}]不能大于最大队列数量[{BrokerController.Instance.Setting.TopicMaxQueueCount}]");
                }
                for(int idx = 0; idx < initialQueueCount; idx++)
                {
                    LoadQueue(topic, idx);
                }
                return _queueDict.Values.Where(x => x.Topic == topic && !x.Setting.IsDeleted).Select(x=>x.QueueId).ToList(); ;
            }
        }

        private void LoadQueue(string topic, int queueId)
        {
            var queue = new Queue(topic, queueId);
            queue.Load();
            if (queue.Setting.IsDeleted)
                return;
            var key = new QueueKey(topic, queueId);
            _queueDict.TryAdd(key, queue);
        }

        public void DeleteQueue(string topic, int queueId)
        {
            lock (_lockObj)
            {
                var key = new QueueKey(topic, queueId);
                Queue queue;
                if(!_queueDict.TryGetValue(key,out queue))
                {
                    return;
                }

                //检查队列是否可删除
                CheckQueueAllToDelete(queue);

                //删除队列的消费进度信息
                _consumeOffsetStore.DeleteConsumeOffset(queue.Key);

                //删除队列本身，包括所有的文件
                queue.Delete();

                //将队列从字典中移除
                _queueDict.Remove(key);

                //如果当前Broker上的一个队列都没有了，则清空整个Brokerx下的所有文件
                if (_queueDict.IsEmpty)
                {
                    BrokerController.Instance.Clean();
                }
            }
        }

        public void DeleteTopic(string topic)
        {
            lock (_lockObj)
            {
                Check.NotNullOrEmpty(topic, nameof(topic));

                var queues = _queueDict.Values.Where(x => x.Topic == topic).OrderBy(x => x.QueueId);
                foreach(var q in queues)
                {
                    CheckQueueAllToDelete(q);
                }
                foreach(var q in queues)
                {
                    DeleteQueue(q.Topic, q.QueueId);
                }

                if (!BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
                {
                    var topicPath = Path.Combine(BrokerController.Instance.Setting.QueueChunkConfig.BasePath, topic);
                    if (Directory.Exists(topicPath))
                        Directory.Delete(topicPath);
                }
            }
        }

        private void CheckQueueAllToDelete(Queue queue)
        {
            if(queue.Setting.ProducerVisible || queue.Setting.ConsumerVisible)
            {
                throw new Exception($"队列[topic:{queue.Topic},queueId:{queue.QueueId}]的Producer或Consumer可见，所以不能进行删除操作");
            }
            //检查是否有未消费完的消息
            var minConsumeOffset = _consumeOffsetStore.GetMinConsumeOffset(queue.Topic, queue.QueueId);
            var queueCurrentOffset = queue.NextOffset - 1;
            if (minConsumeOffset < queueCurrentOffset)
            {
                throw new Exception($"队列[topic:{queue.Topic},queueId:{queue.QueueId}]还有未处理的消息，消息数量：{queueCurrentOffset-minConsumeOffset}，不能进行删除操作");
            }
        }

        public int GetAllQueueCount()
        {
            return _queueDict.Count;
        }

        public IEnumerable<Queue> GetAllQueues()
        {
            return _queueDict.Values.Where(x => !x.Setting.IsDeleted).ToList();
        }

        public IEnumerable<string> GetAllTopics()
        {
            return _queueDict.Values.Select(x => x.Topic ).Distinct();
        }

        public long GetMinConsumeMessagePosition()
        {
            var minConsumedQueueOffset = -1L;
            var queue = default(Queue);
            var hasConsumerQueues = _queueDict.Values.Where(x => BrokerController.Instance.ConsumerManager.IsConsumerExistForQueue(x.Topic, x.QueueId)).ToList();

            foreach(var currentQueue in hasConsumerQueues)
            {
                var offset = _consumeOffsetStore.GetMinConsumeOffset(currentQueue.Topic, currentQueue.QueueId);
                var queueCurrentOffset = currentQueue.NextOffset - 1;
                if (offset > queueCurrentOffset)
                    offset = queueCurrentOffset;
                if((minConsumedQueueOffset==-1L && offset >= 0) 
                    || offset<minConsumedQueueOffset)
                {
                    minConsumedQueueOffset = offset;
                    queue = currentQueue;
                }
            }
            if(queue !=null && minConsumedQueueOffset>=0)
            {
                int tagCode;
                return queue.GetMessagePosition(minConsumedQueueOffset, out tagCode, false);
            }
            return -1L;
        }

        public Queue GetQueue(string topic, int queueId)
        {
            return GetQueue(new QueueKey(topic, queueId));
        }

        private Queue GetQueue(QueueKey queueKey)
        {
            Queue queue;
            if(_queueDict.TryGetValue(queueKey, out queue) && !queue.Setting.IsDeleted)
            {
                return queue;
            }
            return null;
        }

        public long GetQueueCurrentOffset(string topic, int queueId)
        {
            var key = new QueueKey(topic, queueId);
            Queue queue;
            if (_queueDict.TryGetValue(key, out queue))
            {
                return queue.NextOffset - 1;
            }
            return -1;
        }

        public long GetQueueMinOffset(string topic, int queueId)
        {
            var key = new QueueKey(topic, queueId);
            Queue queue;
            if(_queueDict.TryGetValue(key,out queue))
            {
                return queue.GetMinQueueOffset();
            }
            return -1;
        }

        public IEnumerable<Queue> GetQueues(string topic, bool autoCreate = false)
        {
            lock (_lockObj)
            {
                var queues = _queueDict.Values.Where(x => x.Topic == topic && !x.Setting.IsDeleted);
                if(queues.IsEmpty() && autoCreate)
                {
                    CreateTopic(topic, BrokerController.Instance.Setting.TopicDefaultQueueCount);
                    queues = _queueDict.Values.Where(x => x.Topic == topic && !x.Setting.IsDeleted);
                }
                return queues;
            }
        }

        public IList<TopicQueueInfo> GetTopicQueueInfoList(string topic = null)
        {
            var topicQueueInfoList = new List<TopicQueueInfo>();
            var queueList=default(IList<Queue>);
            if (string.IsNullOrEmpty(topic))
            {
                queueList = GetAllQueues().ToList();
            }
            else
            {
                queueList = GetQueues(topic).ToList();
            }
            var foundQueues = queueList.OrderBy(x => x.Topic).ThenBy(x => x.QueueId);
            foreach(var queue in foundQueues)
            {
                var topicQueueInfo = new TopicQueueInfo()
                {
                    Topic = queue.Topic,
                    QueueId = queue.QueueId,
                    QueueCurrentOffset = queue.NextOffset - 1,
                    QueueMinOffset = queue.GetMinQueueOffset(),
                    QueueMinConsumedOffset = _consumeOffsetStore.GetMinConsumeOffset(queue.Topic, queue.QueueId),
                    ProducerVisible = queue.Setting.ProducerVisible,
                    ConsumerVisible = queue.Setting.ConsumerVisible,
                    SendThroughput=_tpsStaticticService.GetTopicSendThroughput(queue.Topic,queue.QueueId),
                };
                topicQueueInfo.QueueNotConsumeCount = topicQueueInfo.CalculateQueueNotConsumeCount();
                topicQueueInfoList.Add(topicQueueInfo);
            }
            return topicQueueInfoList;
        }

        public long GetTotalUnConsumeMessageCount()
        {
            var totalCount = 0L;
            foreach(var q in _queueDict.Values)
            {
                var minConsumedOffset = _consumeOffsetStore.GetMinConsumeOffset(q.Topic, q.QueueId);
                var queueCurrentOffset = q.NextOffset - 1;
                if (queueCurrentOffset > minConsumedOffset)
                {
                    var count = queueCurrentOffset - minConsumedOffset;
                    totalCount += count;
                }
            }
            return totalCount;
        }

        public bool IsQueueExists(QueueKey key)
        {
            return _queueDict.Values.Any(x => x.Topic == key.Topic && x.QueueId == key.QueueId&&!x.Setting.IsDeleted);

        }

        public bool IsQueueExists(string topic, int queueId)
        {
            return _queueDict.Values.Any(x => x.Topic == topic && x.QueueId == queueId &&!x.Setting.IsDeleted);
        }

        public bool IsTopicExists(string topic)
        {
            return _queueDict.Values.Any(x => x.Topic == topic);
        }

        public void Load()
        {
            LoadQueues();
        }

        private void LoadQueues()
        {
            _queueDict.Clear();
            if (BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
                return;

            var chunkConfig = BrokerController.Instance.Setting.QueueChunkConfig;
            if (!Directory.Exists(chunkConfig.BasePath))
            {
                Directory.CreateDirectory(chunkConfig.BasePath);
            }
            var topicPathList = Directory
                .EnumerateDirectories(chunkConfig.BasePath, "*", SearchOption.TopDirectoryOnly)
                .OrderBy(x => x, StringComparer.CurrentCultureIgnoreCase)
                .ToArray();

            foreach(var topicPath in topicPathList)
            {
                var queuePathList = Directory
                    .EnumerateDirectories(topicPath, "*", SearchOption.TopDirectoryOnly)
                    .OrderBy(x => x, StringComparer.CurrentCultureIgnoreCase)
                    .ToArray();
                foreach(var queuePath in queuePathList)
                {
                    var items = queuePath.Split('\\');
                    var queueId = int.Parse(items[items.Length - 1]);
                    var topic = items[items.Length - 2];
                    LoadQueue(topic, queueId);
                }
            }
        }

        public void SetConsumerVisibile(string topic, int queueId, bool visible)
        {
            lock (_lockObj)
            {
                var queue = GetQueue(topic, queueId);
                queue?.SetConsumerVisibile(visible);
            }
        }

        public void SetProducerVisibile(string topic, int queueId, bool visible)
        {
            lock (_lockObj)
            {
                var queue = GetQueue(topic, queueId);
                queue?.SetProducerVisibile(visible);
            }
        }

        public void Shutdown()
        {
            CloseQueues();
            _scheduleService.StopTask("UpdateMinConsumedMessagePosition");
            _scheduleService.StopTask(TaskName);
        }

        private void CloseQueues()
        {
            foreach(var queue in _queueDict.Values)
            {
                try
                {
                    queue.Close();
                }catch(Exception ex)
                {
                    _logger.Error($"关闭队列[{queue.Topic},{queue.QueueId}]失败", ex);
                }
            }
            _queueDict.Clear();
        }

        public void Start()
        {
            _scheduleService.StartTask("UpdateMinConsumedMessagePosition", UpdateMinConsumedMessagePosition, 5000, 5000);
            _scheduleService.StartTask(TaskName, DeleteQueueMessages, 5000, BrokerController.Instance.Setting.DeleteQueueMessageInterval);
        }

        private void DeleteQueueMessages()
        {
            if(Interlocked.CompareExchange(ref _isDeletingQueueMessage, 1, 0) == 0)
            {
                try
                {
                    var queues = _queueDict.OrderBy(x => x.Key).Select(x => x.Value).ToList();
                    foreach(var queue in queues)
                    {
                        try
                        {
                            queue.DeleteMessages(_messageStore.MinMessagePosition);
                        }catch(Exception ex)
                        {
                            _logger.Error($"删除队列[{queue.Topic},{queue.QueueId}]消息发生异常", ex);
                        }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _isDeletingQueueMessage, 0);
                }
            }
        }

        private void UpdateMinConsumedMessagePosition()
        {
            if(Interlocked.CompareExchange(ref _isUpdatingMinConsumedMessagePosition, 1, 0) == 0)
            {
                try
                {
                    var minConsumedMessagePosition = GetMinConsumeMessagePosition();
                    if (minConsumedMessagePosition >= 0)
                    {
                        _messageStore.UpdateMinConsumedMessagePosition(minConsumedMessagePosition);
                    }
                }catch(Exception ex)
                {
                    _logger.Error("更新最小消费消息发生异常", ex);
                }
                finally
                {
                    Interlocked.Exchange(ref _isUpdatingMinConsumedMessagePosition, 0);
                }
            }
        }
    }
}
