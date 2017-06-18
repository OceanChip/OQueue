using OceanChip.Common.Components;
using OceanChip.Common.Logging;
using OceanChip.Common.Serializing;
using OceanChip.Common.Utilities;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Protocols.Brokers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OceanChip.Queue.Clients.Consumers
{
    public class Consumer
    {
        private readonly ClientService _clientService;
        private readonly PullMessageService _pullMessageService;
        private readonly CommitConsumeOffsetService _commitConsumeOffsetService;
        private readonly RebalanceService _rebalanceService;
        private readonly IBinarySerializer _binarySerializer;
        private readonly IDictionary<string, HashSet<string>> _subscriptionTopics;
        private readonly ILogger _logger;
        private bool _stopped;

        public ConsumerSetting Setting { get; private set; }
        public string GroupName { get; private set; }
        public string Name { get; private set; }
        public IDictionary<string, HashSet<string>> SubscriptionTopics => _subscriptionTopics;
        public bool Stopped => _stopped;

        public Consumer(string groupName,string consumerName=null):this(groupName,new ConsumerSetting(), consumerName)
        {

        }
        public Consumer(string groupName,ConsumerSetting setting,string consumerName = null)
        {
            Check.NotNull(groupName, nameof(groupName));

            this.Name = consumerName;
            this.GroupName = groupName;
            this.Setting = setting ?? new ConsumerSetting();
            
            if(this.Setting.NameServerList==null || this.Setting.NameServerList.Count() == 0)
            {
                throw new Exception("地址列表不能为空");
            }
            _subscriptionTopics = new Dictionary<string, HashSet<string>>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            var clientSetting = new ClientSetting
            {
                ClientName = this.Name,
                ClusterName = this.Setting.ClusterName,
                NameServerList = this.Setting.NameServerList,
                SocketSetting = this.Setting.SocketSetting,
                OnlyFindMasterBroker = true,
                SendHeartbeatInterval = this.Setting.HeartbeatBrokerInterval,
                RefreshBrokerAndTopicRouteInfoInterval = this.Setting.RefreshBrokerAndTopicRouteInfoInterval
            };
            _clientService = new ClientService(clientSetting, null, this);
            _pullMessageService = new PullMessageService(this, _clientService);
            _commitConsumeOffsetService = new CommitConsumeOffsetService(this, _clientService);
            _rebalanceService = new RebalanceService(this, _clientService, _pullMessageService,_commitConsumeOffsetService);
        }
        public Consumer SetMessageHandler(IMessageHandler messageHandler)
        {
            _pullMessageService.SetMessageHandler(messageHandler);
            return this;
        }
        public Consumer Start()
        {
            _clientService.Start();
            _pullMessageService.Start();
            _rebalanceService.Start();
            _commitConsumeOffsetService.Start();
            _logger.InfoFormat("{0} startted.", GetType().Name);
            return this;
        }
        public Consumer Stop()
        {
            _stopped = true;
            _commitConsumeOffsetService.Stop();
            _rebalanceService.Stop();
            _pullMessageService.Stop();
            _commitConsumeOffsetService.Start();
            _clientService.Stop();
            _logger.InfoFormat("{0} stopped.", GetType().Name);
            return this;
        }
        public Consumer SubScribe(string topic,params string[] tags)
        {
            if (!_subscriptionTopics.ContainsKey(topic))
            {
                _subscriptionTopics.Add(topic, tags == null ? new HashSet<string>():new HashSet<string>(tags));
            }
            else
            {
                var tagSet = _subscriptionTopics[topic];
                if(tags != null)
                {
                    foreach(var tag in tags)
                    {
                        tagSet.Add(tag);
                    }
                }
            }
            _clientService.RegisterSubscriptionTopic(topic);
            return this;
        }
        public IEnumerable<MessageQueueEx> GetCurrentQueues()
        {
            return _rebalanceService.GetCurrentQueues();
        }
        public IEnumerable<QueueMessage> PullMessages(int maxCount,int timeoutMilliseconds,CancellationToken cancellation)
        {
            return _pullMessageService.PullMessage(maxCount, timeoutMilliseconds, cancellation);
        }
        public void CommitComsumeOffset(string brokerName,string topic,int queueId,long consumeOffset)
        {
            _commitConsumeOffsetService.CommitConsumeOffset(brokerName, topic, queueId, consumeOffset);
        }
        internal void SendHeartbeat()
        {
            var brokerConnections = _clientService.GetAllBrokerConnections();
            var queueGroups = GetCurrentQueues().GroupBy(x => x.BrokerName);

            foreach(var brokerConnection in brokerConnections)
            {
                var remotingClient = brokerConnection.RemotingClient;
                var clientId = _clientService.GetClientId();

                try
                {
                    var messageQueues = new List<MessageQueueEx>();
                    var group = queueGroups.SingleOrDefault(x => x.Key == brokerConnection.BrokerInfo.BrokerName);
                    if(group != null)
                    {
                        messageQueues.AddRange(group);
                    }
                    var heartbeatData = new ConsumerHeatbeatData(clientId, GroupName, _subscriptionTopics.Keys, messageQueues);
                    var data = _binarySerializer.Serialize(heartbeatData);
                    remotingClient.InvokeOnway(new Common.Remoting.RemotingRequest((int)BrokerRequestCode.ConsumerHeartbeat, data));

                }catch(Exception ex)
                {
                    if (remotingClient.IsConnected)
                    {
                        _logger.Error($"发生消费者心跳发生异常，brokerInfo:{brokerConnection.BrokerInfo}", ex);
                    }
                }
            }
        }
    }
}
