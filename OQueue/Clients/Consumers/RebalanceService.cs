using System;
using System.Collections.Generic;
using OceanChip.Queue.Protocols;
using OceanChip.Common.Serializing;
using OceanChip.Common.Scheduling;
using System.Collections.Concurrent;
using OceanChip.Common.Logging;
using OceanChip.Common.Components;
using System.Linq;
using OceanChip.Queue.Protocols.Brokers.Requests;
using OceanChip.Queue.Protocols.Brokers;
using OceanChip.Common.Remoting;
using OceanChip.Common.Extensions;
using System.Text;

namespace OceanChip.Queue.Clients.Consumers
{
    internal class RebalanceService
    {
        private readonly string _clientId;
        private readonly Consumer _consumer;
        private readonly ClientService _clientService;
        private readonly IAllocateMessageQueueStrategy _allocateMessageQueue;
        private readonly PullMessageService _pullMessageService;
        private readonly CommitConsumeOffsetService _commitConsumeOffsetService;
        private readonly IBinarySerializer _binarySerializer;
        private readonly ConcurrentDictionary<string, PullRequest> _pullRequestDicts;
        private IScheduleService _scheduleService;
        private readonly ILogger _logger;

        public RebalanceService(Consumer consumer, ClientService clientService, PullMessageService pullMessageService, CommitConsumeOffsetService commitConsumeOffsetService)
        {
            this._consumer = consumer;
            _clientService = clientService;
            _pullMessageService = pullMessageService;
            _clientId = clientService.GetClientId();
            _pullRequestDicts = new ConcurrentDictionary<string, PullRequest>();
            _commitConsumeOffsetService = commitConsumeOffsetService;
            _allocateMessageQueue = ObjectContainer.Resolve<IAllocateMessageQueueStrategy>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }
        public void Start()
        {
            _scheduleService.StartTask("RebalanceService", Rebalance, 1000, _consumer.Setting.RebalanceInterval);
            if (_consumer.Setting.AutoPull)
            {
                _scheduleService.StartTask("CommotOffsets", CommitOffsets, 100, _consumer.Setting.CommitConsumerOffsetInterval);
            }
            _logger.InfoFormat("{0} startted.", GetType().Name);
        }

        public void Stop()
        {
            _scheduleService.StopTask("RebalanceService");
            if (_consumer.Setting.AutoPull)
            {
                _scheduleService.StopTask("CommotOffsets");
            }
            _logger.InfoFormat("{0} stopped.", GetType().Name);
        }
        public IEnumerable<MessageQueueEx> GetCurrentQueues()
        {
            return _pullRequestDicts.Values.Select(x =>
            {
                return new MessageQueueEx(x.MessageQueue.BrokerName, x.MessageQueue.Topic, x.MessageQueue.QueueId)
                {
                    ClientCachedMessageCount = x.ProcessQueue.GetMessageCount()
                };
            }).ToList();
        }
        private void Rebalance()
        {
            foreach(var pair in _consumer.SubscriptionTopics)
            {
                var topic = pair.Key;
                try
                {
                    RebalanceClustering(pair);
                }catch(Exception ex)
                {
                    _logger.Error($"RebalanceClustering发生异常，consumerGroup:{_consumer.GroupName},consumerId:{_clientId},topic:{topic}", ex);
                }
            }
        }
        private void RebalanceClustering(KeyValuePair<string,HashSet<string>> pair)
        {
            var topic = pair.Key;
            try
            {
                var consumerIdList = GetConsumerIdsForTopic(topic);
                if(consumerIdList==null || consumerIdList.Count == 0)
                {
                    _logger.Warn("无消费者");
                    UpdatePullRequestDict(pair, new List<MessageQueue>());
                    return;
                }
                var messageQueueList = _clientService.GetTopicMessageQueues(topic);
                if(messageQueueList==null && messageQueueList.Count == 0)
                {
                    _logger.Warn("无消息队列");
                    UpdatePullRequestDict(pair, new List<MessageQueue>());
                    return;
                }
                var allocateMessageQueueList = _allocateMessageQueue.Allocate(_clientId, messageQueueList, consumerIdList).ToList();
                UpdatePullRequestDict(pair, allocateMessageQueueList);
            }catch(Exception ex)
            {
                _logger.Error($"RebalanceClustering发生异常，consumerGroup:{_consumer.GroupName},consumerId:{_clientId},topic:{topic}", ex);
                UpdatePullRequestDict(pair, new List<MessageQueue>());
            }
        }

        private void UpdatePullRequestDict(KeyValuePair<string, HashSet<string>> pair, List<MessageQueue> messaggeQueues)
        {
            var topic = pair.Key;
            var toRemovePullRequestKeys = new List<string>();
            foreach(var request in _pullRequestDicts.Values.Where(x => x.MessageQueue.Topic == topic))
            {
                var key = request.MessageQueue.ToString();
                if (!messaggeQueues.Any(x => x.ToString() == key)){
                    toRemovePullRequestKeys.Add(key);
                }
            }
            foreach(var key in toRemovePullRequestKeys)
            {
                PullRequest request;
                if(_pullRequestDicts.TryRemove(key,out request))
                {
                    request.IsDropped = true;
                    _commitConsumeOffsetService.CommitConsumeOffset(request);
                    _logger.InfoFormat("Dropped pull request,consumerGroup:{0}，consumerId:{1}，queue:{2},tags:{3}",
                        _consumer.GroupName,
                        _clientId,
                        request.MessageQueue,
                        string.Join("|", request.Tags));
                }
            }
            foreach(var mq in messaggeQueues)
            {
                var key = mq.ToString();
                PullRequest exist;
                if(!_pullRequestDicts.TryGetValue(key,out exist))
                {
                    var request = new PullRequest(_clientId, _consumer.GroupName, mq, -1, pair.Value);
                    if (_pullRequestDicts.TryAdd(key, request))
                    {
                        _pullMessageService.SchedulePullRequest(request);
                        _logger.InfoFormat("Added pull request,consumerGroup:{0},consumerid：{1}，queue:{2},tags:{3}",
                            _consumer.GroupName,
                            _clientId,
                            request.MessageQueue,
                            string.Join("|", request.Tags));
                    }
                }
            }
        }

        private IList<string> GetConsumerIdsForTopic(string topic)
        {
            var brokerConnection = _clientService.GetFirstBrokerConnection();
            var request = _binarySerializer.Serialize(new GetConsumerIdsForTopRequest(_consumer.GroupName, topic));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.GetConsumerIdsForTopic, request);
            var remotingResponse = brokerConnection.AdminRemotingClient.InvokeSync(remotingRequest, 5000);
            if (remotingResponse.ResponseCode != ResponseCode.Success)
            {
                throw new Exception(string.Format("GetConsumerIdsForTopic发生异常,consumeGroup:{0},topic:{1},adminADDress{2},Remoting ResponseCode:{3},errorMessage{4}",
                    _consumer.GroupName,
                    topic,
                    brokerConnection.AdminRemotingClient.ServerEndPoint.ToAddress(),
                    remotingResponse.ResponseCode,
                    Encoding.UTF8.GetString(remotingResponse.ResponseBody)
                    ));
            }
            var consumerIds = Encoding.UTF8.GetString(remotingResponse.ResponseBody);
            var consumerIdList = consumerIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries).ToList();
            consumerIdList.Sort();
            return consumerIdList;
        }

        private void CommitOffsets()
        {
            foreach(var request in _pullRequestDicts.Values)
            {
                _commitConsumeOffsetService.CommitConsumeOffset(request);
            }
        }
    }
}