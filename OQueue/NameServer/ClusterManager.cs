using OceanChip.Common.Components;
using OceanChip.Common.Extensions;
using OceanChip.Common.Logging;
using OceanChip.Common.Remoting;
using OceanChip.Common.Scheduling;
using OceanChip.Common.Serializing;
using OceanChip.Common.Socketing;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Protocols.Brokers;
using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using OceanChip.Queue.Protocols.NameServers.Requests;

namespace OceanChip.Queue.NameServer
{
    /// <summary>
    /// 集群管理
    /// </summary>
    public class ClusterManager
    {
        private const string ScanNotActiveBrokerName = "ScanNotActiveBroker";

        private readonly ConcurrentDictionary<string, Cluster> _clusterDict;
        private readonly object _lockObj = new object();
        private readonly IScheduleService _scheduleService;
        private readonly NameServerController _nameServerController;
        private readonly ILogger _logger;
        private readonly IJsonSerializer _json;
        private readonly IBinarySerializer _binarySerializer;

        public ClusterManager(NameServerController nameServerController)
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _json = ObjectContainer.Resolve<IJsonSerializer>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _clusterDict = new ConcurrentDictionary<string, Cluster>();
            this._nameServerController = nameServerController;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }
        public void Start()
        {
            _clusterDict.Clear();
            _scheduleService.StartTask(ScanNotActiveBrokerName, ScanNotActiveBroker, 1000, 1000);
        }
        public void Shutdown()
        {
            _clusterDict.Clear();
            _scheduleService.StopTask(ScanNotActiveBrokerName);
        }
        /// <summary>
        /// 注册Broker
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="request"></param>
        public void RegisterBroker(ITcpConnection connection,BrokerRegistrationRequest request)
        {
            lock (_lockObj)
            {
                var brokerInfo = request.BrokerInfo;
                var cluster = _clusterDict.GetOrAdd(brokerInfo.ClusterName, x => new Cluster { ClusterName = x });
                var brokerGroup = cluster.BrokerGroups.GetOrAdd(brokerInfo.GroupName, x => new BrokerGroup { GroupName = x });
                Broker broker;
                if(!brokerGroup.Brokers.TryGetValue(brokerInfo.BrokerName,out broker))
                {
                    var connectionId = connection.RemoteEndPoint.ToAddress();
                    broker = new Broker()
                    {
                        BrokerInfo = request.BrokerInfo,
                        TotalSendThroughput=request.TotalSendThroughput,
                        TotalConsumeThroughput=request.TotalConsumeThroughput,
                        TotalUnConsumedMessageCount=request.TotalUnConsumedMessageCount,
                        TopicQueueInfoList=request.TopicQueueInfoList,
                        TopicConsumeInfoList=request.TopicConsumeInfoList,
                        ProducerList=request.ProducerList,
                        ConsumerList=request.ConsumerList,
                        Connection=connection,
                        ConnectionId=connectionId,
                        LastActiveTime=DateTime.Now,
                        FirstRegisteredTime=DateTime.Now,
                        Group=brokerGroup
                    };
                    if (brokerGroup.Brokers.TryAdd(brokerInfo.BrokerName, broker))
                    {
                        _logger.Info($"注册新Broker，BrokerInfo:{_json.Serialize(brokerInfo)}");
                    }
                }
                else
                {
                    broker.LastActiveTime = DateTime.Now;
                    broker.TotalSendThroughput = request.TotalSendThroughput;
                    broker.TotalConsumeThroughput = request.TotalConsumeThroughput;
                    broker.TotalUnConsumedMessageCount = request.TotalUnConsumedMessageCount;

                    if (!broker.BrokerInfo.IsEqualsWith(request.BrokerInfo))
                    {
                        var logInfo = $"Broker基础信息改变,旧：{broker.BrokerInfo},新:{request.BrokerInfo}";
                        broker.BrokerInfo = request.BrokerInfo;
                        _logger.Info(logInfo);
                    }
                    broker.TopicQueueInfoList = request.TopicQueueInfoList;
                    broker.TopicConsumeInfoList = request.TopicConsumeInfoList;
                    broker.ProducerList = request.ProducerList;
                    broker.ConsumerList = request.ConsumerList;
                }
            }
        }
        public void UnRegisterBroker(BrokerUnRegistrationRequest request)
        {
            lock (_lockObj)
            {
                var brokerInfo = request.BrokerInfo;
                var cluster = _clusterDict.GetOrAdd(brokerInfo.ClusterName, x => new Cluster { ClusterName = x });
                var brokerGroup = cluster.BrokerGroups.GetOrAdd(brokerInfo.GroupName, x => new BrokerGroup { GroupName = x });
                Broker removed;
                if(brokerGroup.Brokers.TryRemove(brokerInfo.BrokerName,out removed))
                {
                    _logger.Info("注销Broker，BrokerInfo:" + _json.Serialize(removed.BrokerInfo));
                }
            }
        }
        public void RemoveBroker(ITcpConnection connection)
        {
            lock (_lockObj)
            {
                var connectionId = connection.RemoteEndPoint.ToAddress();
                var broker = FindBroker(connectionId);
                if(broker != null)
                {
                    Broker removed;
                    if (broker.Group.Brokers.TryRemove(broker.BrokerInfo.BrokerName, out removed))
                    {
                        _logger.Info("移除Broker，BorderInfo:" + _json.Serialize(broker.BrokerInfo));
                    }
                }
            }
        }

        public IList<TopicRouteInfo> GetTopicRouteInfo(GetTopicRouteInfoRequest request)
        {
            lock (_lockObj)
            {
                var result = new List<TopicRouteInfo>();
                Cluster cluster;
                if(string.IsNullOrEmpty(request.ClusterName)|| !_clusterDict.TryGetValue(request.ClusterName,out cluster))
                {
                    return result;
                }
                foreach(var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach(var broker in brokerGroup.Brokers.Values)
                    {
                        if(request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }

                        var queueList = new List<int>();
                        var topicQueueInfoList = broker.TopicQueueInfoList.Where(p => p.Topic == request.Topic).ToList();
                        if (topicQueueInfoList.Count > 0)
                        {
                            if(request.ClientRole == ClientRole.Producer)
                            {
                                queueList = topicQueueInfoList.Where(p => p.ProducerVisible).Select(p=>p.QueueId).ToList();
                            }else if(request.ClientRole == ClientRole.Consumer)
                            {
                                queueList = topicQueueInfoList.Where(p => p.ConsumerVisible).Select(p => p.QueueId).ToList();
                            }
                        }else if (_nameServerController.Setting.AutoCreateTopic)
                        {
                            queueList = CreateTopicOnBroker(request.Topic, broker).ToList();
                        }
                        result.Add(new TopicRouteInfo
                        {
                            BrokerInfo = broker.BrokerInfo,
                            QueueInfo = queueList
                        });
                    }
                }
                return result;
            }
        }
        /// <summary>
        /// 获取Topic队列信息列表
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public IList<BrokerTopicQueueInfo> GetTopicQueueInfo(Protocols.NameServers.Requests.GetTopicQueueInfoRequest request)
        {
            lock (_lockObj)
            {
                var rtnList = new List<BrokerTopicQueueInfo>();
                Cluster cluster;
                if(string.IsNullOrEmpty(request.ClusterName)||!_clusterDict.TryGetValue(request.ClusterName,out cluster))
                {
                    return rtnList;
                }
                foreach(var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach(var broker in brokerGroup.Brokers.Values)
                    {
                        //若设定了只查找Master,则，判断Broker是否为Master
                        if(request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        var topicQueueInfoList=broker.TopicQueueInfoList.Where(p=>p.Topic==request.Topic).ToList();
                        rtnList.Add(new BrokerTopicQueueInfo
                        {
                            BrokerInfo = broker.BrokerInfo,
                            TopicConsumerInfoList = topicQueueInfoList,
                        });
                    }
                }
                rtnList.Sort((x, y) =>
                {
                    return string.Compare(x.BrokerInfo.BrokerName, y.BrokerInfo.BrokerName);
                });
                return rtnList;
            }
        }

        public IList<BrokerTopicConsumeInfo> GetTopicConsumeInfo(Protocols.NameServers.Requests.GetTopicConsumeInfoRequest request)
        {
            lock (_lockObj)
            {
                var rtnList = new List<BrokerTopicConsumeInfo>();
                Cluster cluster;
                if(string.IsNullOrEmpty(request.ClusterName)||!_clusterDict.TryGetValue(request.ClusterName,out cluster))
                {
                    return rtnList;
                }
                foreach(var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach(var broker in brokerGroup.Brokers.Values)
                    {
                        if(request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        var topicConsumeInfoList = broker.TopicConsumeInfoList.Where(p => p.Topic == request.Topic && p.ConsumerGroup == request.ConsumerGroup).ToList();
                        rtnList.Add(new BrokerTopicConsumeInfo
                        {
                            BrokerInfo = broker.BrokerInfo,
                            TopicConsumerInfo = topicConsumeInfoList,
                        });
                    }                    
                }
                rtnList.Sort((x, y) =>
                {
                    return string.Compare(x.BrokerInfo.BrokerName, y.BrokerInfo.BrokerName);
                });
                return rtnList;
            }
        }
        public IList<BrokerProducerListInfo> GetProducerList(GetProducerListRequest request)
        {
            lock (_lockObj)
            {
                var rtnList = new List<BrokerProducerListInfo>();
                Cluster cluster;
                if (string.IsNullOrEmpty(request.ClusterName) || !_clusterDict.TryGetValue(request.ClusterName, out cluster))
                {
                    return rtnList;
                }
                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        rtnList.Add(new BrokerProducerListInfo
                        {
                            BrokerInfo = broker.BrokerInfo,
                            ProducerList = broker.ProducerList,
                        });
                    }
                }
                rtnList.Sort((x, y) =>
                {
                    return string.Compare(x.BrokerInfo.BrokerName, y.BrokerInfo.BrokerName);
                });
                return rtnList;
            }
        }

        public IList<BrokerConsumerListInfo> GetConsumerList(Protocols.NameServers.Requests.GetConsumerListRequest request)
        {
            lock (_lockObj)
            {
                var rtnList = new List<BrokerConsumerListInfo>();
                Cluster cluster;
                if (string.IsNullOrEmpty(request.ClusterName) || !_clusterDict.TryGetValue(request.ClusterName, out cluster))
                {
                    return rtnList;
                }
                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        rtnList.Add(new BrokerConsumerListInfo
                        {
                            BrokerInfo = broker.BrokerInfo,
                            ConsumerList = broker.ConsumerList,
                        });
                    }
                }
                rtnList.Sort((x, y) =>
                {
                    return string.Compare(x.BrokerInfo.BrokerName, y.BrokerInfo.BrokerName);
                });
                return rtnList;
            }
        }
        public IList<string> GetAllClusters()
        {
            var clusterList = _clusterDict.Keys.ToList();
            clusterList.Sort();
            return clusterList;
        }
        public IList<BrokerInfo> GetClusterBrokers(GetClusterBrokersRequest request)
        {
            lock (_lockObj)
            {
                var rtnList = new List<Broker>();
                Cluster cluster;
                if (string.IsNullOrEmpty(request.ClusterName) || !_clusterDict.TryGetValue(request.ClusterName, out cluster))
                {
                    return rtnList.Select(x=>x.BrokerInfo).ToList();
                }
                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        if (!string.IsNullOrEmpty(request.Topic))
                        {
                            if(broker.TopicQueueInfoList.Any(x=>x.Topic==request.Topic))
                                rtnList.Add(broker);
                        }
                        else
                        {
                            rtnList.Add(broker);
                        }
                    }
                }
                rtnList.Sort((x, y) =>
                {
                    if (x.FirstRegisteredTime.Ticks > y.FirstRegisteredTime.Ticks)
                        return 1;
                    else if (x.FirstRegisteredTime.Ticks < y.FirstRegisteredTime.Ticks)
                        return -1;
                    return 0;
                });
                return rtnList.Select(x=>x.BrokerInfo).ToList();
            }
        }
        public IList<BrokerStatusInfo> GetClusterBrokerStatusInfos(GetClusterBrokersRequest request)
        {
            lock (_lockObj)
            {
                var rtnList = new List<Broker>();
                Cluster cluster;
                if (string.IsNullOrEmpty(request.ClusterName) || !_clusterDict.TryGetValue(request.ClusterName, out cluster))
                {
                    return rtnList.Select(x => new BrokerStatusInfo
                    {
                        BrokerInfo=x.BrokerInfo,
                        TotalConsumeThroughput=x.TotalConsumeThroughput,
                        TotalSendThroughput=x.TotalSendThroughput,
                        TotalUnConsumedMessageCount=x.TotalUnConsumedMessageCount
                    }).ToList();
                }
                foreach (var brokerGroup in cluster.BrokerGroups.Values)
                {
                    foreach (var broker in brokerGroup.Brokers.Values)
                    {
                        if (request.OnlyFindMaster && broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                        {
                            continue;
                        }
                        if (!string.IsNullOrEmpty(request.Topic))
                        {
                            if (broker.TopicQueueInfoList.Any(x => x.Topic == request.Topic))
                                rtnList.Add(broker);
                        }
                        else
                        {
                            rtnList.Add(broker);
                        }
                    }
                }
                rtnList.Sort((x, y) =>
                {
                    return string.Compare(x.BrokerInfo.BrokerName, y.BrokerInfo.BrokerName);
                });
                return rtnList.Select(x => new BrokerStatusInfo
                {
                    BrokerInfo = x.BrokerInfo,
                    TotalConsumeThroughput = x.TotalConsumeThroughput,
                    TotalSendThroughput = x.TotalSendThroughput,
                    TotalUnConsumedMessageCount = x.TotalUnConsumedMessageCount
                }).ToList();
            }
        }
        public IList<TopicAccumulateInfo> GetTopicAccumulateInfoList(GetTopicAccumulateInfoListRequest request)
        {
            lock (_lockObj)
            {
                var rtnList = new List<TopicAccumulateInfo>();
                var tempDict = new ConcurrentDictionary<string, IList<TopicConsumeInfo>>();

                foreach(var cluster in _clusterDict)
                {
                    foreach(var brokerGroup in cluster.Value.BrokerGroups.Values)
                    {
                        foreach(var broker in brokerGroup.Brokers.Values)
                        {
                            if (broker.BrokerInfo.BrokerRole != (int)BrokerRole.Master)
                                continue;

                            foreach(var topicConsumeInfo in broker.TopicConsumeInfoList)
                            {
                                var key = $"{topicConsumeInfo.Topic}_{topicConsumeInfo.ConsumerGroup}";
                                var list = tempDict.GetOrAdd(key, x => new List<TopicConsumeInfo>());
                                list.Add(topicConsumeInfo);
                            }
                        }
                    }
                }
                foreach(var list in tempDict.Values)
                {
                    if (list.Count == 0)
                        continue;

                    var consumeGroup = list[0].ConsumerGroup;
                    var topic = list[0].Topic;
                    var queueCount = list.Count;
                    var onlineConsumerCount = list[0].OnlineConsumerCount;
                    long accumulateCount = 0;
                    long consumeThroughput = 0;

                    foreach(var item in list)
                    {
                        accumulateCount += item.QueueNotConsumeCount;
                        consumeThroughput += item.ConsumeThroughput;
                    }
                    var topicAccumulateInfo = new TopicAccumulateInfo
                    {
                        ConsumerGroup = consumeGroup,
                        Topic = topic,
                        QueueCount = queueCount,
                        AccumulateCount = accumulateCount,
                        ConsumeThroughput = consumeThroughput,
                        OnlineConsumerCount = onlineConsumerCount,
                    };
                    rtnList.Add(topicAccumulateInfo);
                }
                rtnList = rtnList.Where(x => x.AccumulateCount >= request.AccumulateThreshold).ToList();
                return rtnList;
            }
        }
        private bool IsTopicQueueInfoChanged(IList<TopicQueueInfo> list1,IList<TopicQueueInfo> list2)
        {
            if (list1.Count != list2.Count)
                return true;

            for(var i = 0; i < list1.Count; i++)
            {
                var item1 = list1[i];
                var item2 = list2[i];

                if( item1.Topic != item2.Topic
                    || item1.QueueId != item2.QueueId
                    || item1.ProducerVisible != item2.ProducerVisible
                    || item1.ConsumerVisible != item2.ConsumerVisible)
                {
                    return true;
                }
                    
            }
            return false;
        }
        private bool IsTopicConsumeInfoChanged(IList<TopicConsumeInfo> list1, IList<TopicConsumeInfo> list2)
        {
            if (list1.Count != list2.Count)
                return true;

            for (var i = 0; i < list1.Count; i++)
            {
                var item1 = list1[i];
                var item2 = list2[i];

                if (item1.Topic != item2.Topic 
                    || item1.ConsumerGroup != item2.ConsumerGroup
                    || item1.QueueId != item2.QueueId
                    || item1.OnlineConsumerCount != item2.OnlineConsumerCount
                    )
                {
                    return true;
                }

            }
            return false;
        }
        private bool IsConsumerInfoChanged(IList<ConsumerInfo> list1, IList<ConsumerInfo> list2)
        {
            if (list1.Count != list2.Count)
            {
                return true;
            }
            for (var i = 0; i < list1.Count; i++)
            {
                var item1 = list1[i];
                var item2 = list2[i];

                if (item1.Topic != item2.Topic
                    || item1.ConsumerGroup != item2.ConsumerGroup
                    || item1.QueueId != item2.QueueId
                    || item1.ConsumerId != item2.ConsumerId
                    )
                {
                    return true;
                }
                
            }

            return false;
        }
        private Broker FindBroker(string connectionId)
        {
            foreach(var cluster in _clusterDict.Values)
            {
                foreach(var brokerGroup in cluster.BrokerGroups.Values)
                {
                    return brokerGroup.Brokers.Values.FirstOrDefault(p => p.ConnectionId.Equals(connectionId));
                }
            }
            return null;
        }
        private IEnumerable<int> CreateTopicOnBroker(string topic,Broker broker)
        {
            var brokerAdminEndpoint = broker.BrokerInfo.AdminAddress.ToEndPoint();
            var adminRomotingClient = new SocketRemotingClient(brokerAdminEndpoint, _nameServerController.Setting.SocketSetting);
            var requestData = _binarySerializer.Serialize(new CreateTopicRequest(topic));
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.CreateTopic, requestData);
            var remotingResponse = adminRomotingClient.InvokeSync(remotingRequest, 30000);
            if(remotingResponse.RequestCode != ResponseCode.Success)
            {
                throw new Exception($"通过Broker自动创建Topic失败，失败原因:{Encoding.UTF8.GetString(remotingResponse.ResponseBody)}");
            }
            adminRomotingClient.Shutdown();
            return _binarySerializer.Deserialize<IEnumerable<int>>(remotingResponse.ResponseBody);
        }

        private void ScanNotActiveBroker()
        {
            lock (_lockObj)
            {
                foreach(var cluster in _clusterDict.Values)
                {
                    foreach(var brokerGroup in cluster.BrokerGroups.Values)
                    {
                        var notActiveBrokers = new List<Broker>();
                        foreach(var broker in brokerGroup.Brokers.Values)
                        {
                            if (broker.IsTimeout(_nameServerController.Setting.BrokerInactiveMaxMilliseconds))
                            {
                                notActiveBrokers.Add(broker);
                            }
                        }
                        if (notActiveBrokers.Count > 0)
                        {
                            foreach(var broker in notActiveBrokers)
                            {
                                Broker removed;
                                if(brokerGroup.Brokers.TryRemove(broker.BrokerInfo.BrokerName,out removed))
                                {
                                    _logger.Info($"移除超时Broker,Broker信息：{_json.Serialize(broker)},最后更新时间：{broker.LastActiveTime}");
                                }
                            }
                        }
                    }
                }
            }
        }

        class Cluster
        {
            public string ClusterName { get; set; }
            public ConcurrentDictionary<string, BrokerGroup> BrokerGroups = new ConcurrentDictionary<string, BrokerGroup>();
        }
        class BrokerGroup
        {
            public string GroupName { get; set; }
            public ConcurrentDictionary<string, Broker> Brokers = new ConcurrentDictionary<string, Broker>();
        }
        class Broker
        {
            public BrokerInfo BrokerInfo { get; set; }
            public long TotalSendThroughput { get; set; }
            public long TotalConsumeThroughput { get; set; }
            public long TotalUnConsumedMessageCount { get; set; }
            public IList<TopicQueueInfo> TopicQueueInfoList = new List<TopicQueueInfo>();
            public IList<TopicConsumeInfo> TopicConsumeInfoList = new List<TopicConsumeInfo>();
            public IList<string> ProducerList = new List<string>();
            public IList<ConsumerInfo> ConsumerList = new List<ConsumerInfo>();
            public string ConnectionId { get; set; }
            public ITcpConnection Connection { get; set; }
            public DateTime LastActiveTime { get; set; }
            public BrokerGroup Group { get; set; }
            public DateTime FirstRegisteredTime { get; set; }
            public bool IsTimeout(double timeoutMilliseconds)
            {
                return (DateTime.Now - LastActiveTime).TotalMilliseconds >= timeoutMilliseconds;
            }
        }
        class BrokerConnection
        {
            private readonly BrokerInfo _brokerInfo;
            private readonly SocketRemotingClient _adminRemotingClient;
            public BrokerInfo BrokerInfo => _brokerInfo;
            public SocketRemotingClient AdminRemotingClient => _adminRemotingClient;

            public BrokerConnection(BrokerInfo brokerInfo,SocketRemotingClient adminRemotingClient)
            {
                this._brokerInfo = brokerInfo;
                this._adminRemotingClient = adminRemotingClient;
            }
            public void Start()
            {
                _adminRemotingClient.Start();
            }
            public void Stop()
            {
                _adminRemotingClient.Shutdown();
            }
        }
    }
}
