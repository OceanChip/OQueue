using OceanChip.Common.Components;
using OceanChip.Common.Logging;
using OceanChip.Common.Scheduling;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.LongPolling
{
    public class SuspendedPullRequestManager
    {
        private const string Separator = "@";
        private readonly BlockingCollection<NotifyItem> _notifyQueue;// = new BlockingCollection<NotifyItem>(new ConcurrentQueue<NotifyItem>());
        private readonly ConcurrentDictionary<string, PullRequest> _queueRequestDict = new ConcurrentDictionary<string, PullRequest>();
        private readonly IScheduleService _scheduleService;
        private readonly IQueueStore _queueStore;
        private readonly ILogger _logger;
        private readonly Worker _notifyMessageArrivedWorker;

        public SuspendedPullRequestManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _notifyQueue = new BlockingCollection<NotifyItem>(new ConcurrentQueue<NotifyItem>());
            _notifyMessageArrivedWorker = new Worker("NotifyMessageArrived", () =>
              {
                  var notifyItem = _notifyQueue.Take();
                  if (notifyItem == null) return;
                  NotifyMessageArrived(notifyItem.Topic, notifyItem.QueueId, notifyItem.QueueOffset);
              });
        }
        public void Clean()
        {
            var keys = _queueRequestDict.Keys.ToList();
            foreach(var key in keys)
            {
                PullRequest request;
                if(_queueRequestDict.TryRemove(key,out request))
                {
                    Task.Factory.StartNew(() => request.NoNewMessageAction(request));
                }
            }
        }
        public void RemovePullRequest(string consumerGroup,string topic,int queueId)
        {
            var key = BuildKey(topic, queueId, consumerGroup);
            PullRequest request;
            if(_queueRequestDict.TryRemove(key,out request))
            {
                Task.Factory.StartNew(() => request.NoNewMessageAction(request));
            }
        }
        public void SuspendPullRequest(PullRequest request)
        {
            var messageRequest = request.PullMessageRequest;
            var key = BuildKey(messageRequest.MessageQueue.Topic, messageRequest.MessageQueue.QueueId, messageRequest.ConsumerGroup);

            var existingRequest = default(PullRequest);
            var currentPullRequest = _queueRequestDict.AddOrUpdate(key, x =>
            {
                return request;
            }, (x, req) =>
             {
                 existingRequest = req;
                 return request;
             });
            if(existingRequest != null)
            {
                var currentRequest = existingRequest;
                Task.Factory.StartNew(() => currentPullRequest.ReplaceAction(currentPullRequest));
            }
        }
        public void NotifyNewMessage(string topic,int queueId,long queueOffset)
        {
            _notifyQueue.Add(new NotifyItem { Topic = topic, QueueId = queueId, QueueOffset = queueOffset });
        }
        public void Start()
        {
            StartCheckBlockingPullRequestTask();
            if (BrokerController.Instance.Setting.NotifyWhenMessageArrived)
            {
                StartCheckBlockingPullRequestTask();
            }
        }
        public void Shutdown()
        {
            StopCheckBlockingPullRequestTask();
            if (BrokerController.Instance.Setting.NotifyWhenMessageArrived)
            {
                StopNotifyMessageArrayWorker();
            }
        }
        private void StartCheckBlockingPullRequestTask()
        {
            _scheduleService.StartTask("CheckBlockingPullRequest", CheckBlockingPullRequest, 5000, BrokerController.Instance.Setting.CheckBlockingPullRequestMillseconds);
        }
        private void StopCheckBlockingPullRequestTask()
        {
            _scheduleService.StopTask("CheckBlockingPullRequest");
        }
        private void StartNotifyMessageArrivedWorker()
        {
            _notifyMessageArrivedWorker.Start();
        }
        private void StopNotifyMessageArrayWorker()
        {
            _notifyMessageArrivedWorker.Stop();
            if (_notifyQueue != null && _notifyQueue.Count == 0)
                _notifyQueue.Add(null);
        }
        private void CheckBlockingPullRequest()
        {
            var watch = Stopwatch.StartNew();
            foreach(var entry in _queueRequestDict)
            {
                var items = entry.Key.Split(new string[] { Separator }, StringSplitOptions.None);
                var topic = items[0];
                var queueId = int.Parse(items[1]);
                var queueOffset = _queueStore.GetQueueCurrentOffset(topic, queueId);
                NotifyMessageArrived(topic, queueId, queueOffset);
            }
            var timeSpent = watch.ElapsedMilliseconds;
            if (timeSpent > 1000)
            {
                _logger.WarnFormat("Check blocking pull request use time too long,time spent:{0}", timeSpent);
            }
        }
        private void NotifyMessageArrived(string topic,int queueId,long queueOffset)
        {
            var keyPrefix = BuildKeyPrefix(topic, queueId);
            var keys = _queueRequestDict.Keys.Where(x => x.StartsWith(keyPrefix));

            foreach(var key in keys)
            {
                PullRequest request;
                if(_queueRequestDict.TryGetValue(key,out request))
                {
                    if (queueOffset >= request.PullMessageRequest.QueueOffset)
                    {
                        PullRequest currentRequest;
                        if(_queueRequestDict.TryRemove(key,out currentRequest))
                        {
                            Task.Factory.StartNew(() => currentRequest.NewMessageArrivedAction(currentRequest));
                        }
                    }else if (request.isTimeout())
                    {
                        PullRequest currentRequest;
                        if (_queueRequestDict.TryRemove(key, out currentRequest))
                        {
                            Task.Factory.StartNew(() => currentRequest.TimeoutAction(currentRequest));
                        }
                    }
                }
            }
        }

        public static string BuildKeyPrefix(string topic,int queueId)
        {
            return $"{topic}{Separator}{queueId}{Separator}";
        }
        private static string BuildKey(string topic,int queueId,string group)
        {
            return $"{topic}{Separator}{queueId}{Separator}{group}";
        }
        class NotifyItem
        {
            public string Topic { get; set; }
            public int QueueId { get; set; }
            public long QueueOffset { get; set; }
        }
    }
}
