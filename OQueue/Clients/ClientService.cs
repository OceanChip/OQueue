using OceanChip.Common.Logging;
using OceanChip.Common.Remoting;
using OceanChip.Common.Scheduling;
using OceanChip.Common.Serializing;
using OceanChip.Queue.Protocols;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using OceanChip.Queue.Clients.Producers;
using OceanChip.Queue.Clients.Consumers;
using OceanChip.Common.Utilities;
using System.Threading;
using OceanChip.Common.Components;
using OceanChip.Common.Extensions;
using OceanChip.Queue.Protocols.Brokers;
using System.Net;
using OceanChip.Common.Socketing;
using System.Diagnostics;
using OceanChip.Queue.Protocols.NameServers.Requests;

namespace OceanChip.Queue.Clients
{
    public class ClientService
    {
        private static long _instanceNumber;
        private readonly object _lockObj = new object();
        private readonly string _clientId;
        private readonly ClientSetting _settings;
        private readonly IList<SocketRemotingClient> _nameServerRemotingClientList;
        private readonly ConcurrentDictionary<string, BrokerConnection> _brokerConnectionDick;
        private readonly ConcurrentDictionary<string, IList<MessageQueue>> _topicMessageQueueDict;
        private readonly IBinarySerializer _binarySerializer;
        private readonly IJsonSerializer _json;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private Producer _producer;
        private Consumer _consumer;
        private long _nameServerIndex;

        public ClientService(ClientSetting clientSetting, Producer producer, Consumer consumer)
        {
            Check.NotNull(clientSetting, nameof(clientSetting));
            if(producer==null && consumer == null)
            {
                throw new ArgumentException("producer or consumer 只能选择其一");
            }else if(producer !=null && consumer != null)
            {
                throw new ArgumentException("producer or consumer 只能选择其一");
            }
            Interlocked.Increment(ref _instanceNumber);
            
            this._settings = clientSetting;
            this._producer = producer;
            this._consumer = consumer;
            _clientId = BuildClientId(clientSetting.ClientName);
            _brokerConnectionDick = new ConcurrentDictionary<string, BrokerConnection>();
            _topicMessageQueueDict = new ConcurrentDictionary<string, IList<MessageQueue>>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _json = ObjectContainer.Resolve<IJsonSerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService > ();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _nameServerRemotingClientList = _settings.NameServerList.ToRomotingClientList(_settings.SocketSetting).ToList();
        }


        public virtual ClientService Start()
        {
            StartAllNameServerClients();
            RefreshClusterBrokers();
            if (_brokerConnectionDick.Count == 0)
                throw new Exception("无可用Brokers");

            _scheduleService.StartTask("SendHeartbeatToAllBrokers", SendHeartbeatToAllBrokers, 1000, _settings.SendHeartbeatInterval);
            _scheduleService.StartTask("RefreshBrokerAndTopicRouteInfo", () =>
            {
                RefreshClusterBrokers();
                RefreshTopicRouteInfo();
            }, 1000, _settings.RefreshBrokerAndTopicRouteInfoInterval);
            return this;
        }

        public virtual ClientService Stop()
        {
            _scheduleService.StopTask("SendHeartbeatToAllBrokers");
            _scheduleService.StopTask("RefreshBrokerAndTopicRouteInfo");
            StopAllNameServerClients();
            StopAllBrokerServices();
            _logger.InfoFormat("{0} stopped.", GetType().Name);
            return this;
        }

        public IList<BrokerConnection> GetAllBrokerConnections()
        {
            return _brokerConnectionDick.Values.ToList();
        }

        public string GetClientId()
        {
            return _clientId;
        }
        public ClientService RegisterSubscriptionTopic(string topic)
        {
            _topicMessageQueueDict.TryAdd(topic, new List<MessageQueue>());
            return this;
        }

        public BrokerConnection GetBrokerConnection(string brokerName)
        {
            BrokerConnection brokerConnection;
            if (_brokerConnectionDick.TryGetValue(brokerName, out brokerConnection)){
                return brokerConnection;
            }
            return null;
        }
        public BrokerConnection GetFirstBrokerConnection()
        {
            var availableList = _brokerConnectionDick.Values.Where(x => x.RemotingClient.IsConnected).ToList();
            if (availableList.Count == 0)
                throw new Exception("无可用Broker.");
            return availableList.First();
        }
        public IList<MessageQueue> GetTopicMessageQueues(string topic)
        {
            IList<MessageQueue> messageQueueList;
            if(_topicMessageQueueDict.TryGetValue(topic,out messageQueueList))
            {
                return messageQueueList;
            }
            lock (_lockObj)
            {
                if(_topicMessageQueueDict.TryGetValue(topic,out messageQueueList))
                {
                    return messageQueueList;
                }
                try
                {
                    var topicRouteList = GetTopicRouteInfoList(topic);
                    messageQueueList = new List<MessageQueue>();

                    foreach(var topicroute in topicRouteList)
                    {
                        foreach(var qid in topicroute.QueueInfo)
                        {
                            var messageQueue = new MessageQueue(topicroute.BrokerInfo.BrokerName, topic, qid);
                            messageQueueList.Add(messageQueue);
                        }
                    }
                    SortMessageQueues(messageQueueList);
                    _topicMessageQueueDict[topic] = messageQueueList;
                }catch(Exception ex)
                {
                    _logger.Error($"GetTopicRouteInfoList发生异常,topic:{topic}", ex);
                }
                return messageQueueList;
            }
        }

        private void SendHeartbeatToAllBrokers()
        {
            if(_producer != null)
            {
                _producer.SendHeartbeat();
            }else if(_consumer != null)
            {
                _consumer.SendHeartbeat();
            }
        }
        private IList<BrokerInfo> GetClusterBrokerList()
        {
            var nameServerRemotingClient = GetAvialableNameServerRemotingClient();
            var request = new GetClusterBrokersRequest
            {
                ClusterName = _settings.ClusterName,
                OnlyFindMaster = _settings.OnlyFindMasterBroker
            };
            var data = _binarySerializer.Serialize(request);
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.GetClusterBrokers, data);
            var remotingResponse = nameServerRemotingClient.InvokeSync(remotingRequest, 5000);
            if (remotingResponse.ResponseCode != ResponseCode.Success)
            {
                throw new Exception($"通过地址获取Broker集群失败，clusterName:{_settings.ClusterName},nameServer:{nameServerRemotingClient},remoting ResponseCode:{remotingResponse.ResponseCode},ErrorMessage:{Encoding.UTF8.GetString(remotingResponse.ResponseBody)}");
            }
           return  _binarySerializer.Deserialize<IList<BrokerInfo>>(remotingResponse.ResponseBody);
        }
        private void StartAllNameServerClients()
        {
            foreach(var nameServerClient in _nameServerRemotingClientList)
            {
                nameServerClient.Start();
            }
        }
        private void StopAllNameServerClients()
        {
            foreach (var nameServerClient in _nameServerRemotingClientList)
            {
                nameServerClient.Shutdown();
            }
        }
        private void StartAllBrokerServices()
        {
            foreach(var brokerService in _brokerConnectionDick.Values)
            {
                brokerService.Start();
            }
        }
        private void StopAllBrokerServices()
        {
            foreach (var brokerService in _brokerConnectionDick.Values)
            {
                brokerService.Stop();
            }
        }
        private void RefreshClusterBrokers()
        {
            lock (_lockObj)
            {
                var newBrokerInfoList = GetClusterBrokerList();
                var oldBrokerInfoList = _brokerConnectionDick.Select(x => x.Value.BrokerInfo).ToList();

                var newBrokerInfoJson = _json.Serialize(newBrokerInfoList);
                var oldBrokerInfoJson = _json.Serialize(oldBrokerInfoList);

                if(oldBrokerInfoJson != newBrokerInfoJson)
                {
                    var addedBrokerInfoList = newBrokerInfoList.Where(x => !_brokerConnectionDick.Any(y => y.Key == x.BrokerName)).ToList();
                    var removedBrokerInfoList = _brokerConnectionDick.Values.Where(x => !newBrokerInfoList.Any(y => y.BrokerName == x.BrokerInfo.BrokerName)).ToList();

                    foreach(var brokerConn in removedBrokerInfoList)
                    {
                        BrokerConnection removed;
                        if(_brokerConnectionDick.TryRemove(brokerConn.BrokerInfo.BrokerName,out removed))
                        {
                            brokerConn.Stop();
                            _logger.InfoFormat("移除Broker：{0}", brokerConn.BrokerInfo);
                        }
                    }
                    foreach(var brokerInfo in addedBrokerInfoList)
                    {
                        var brokerconn = BuildAndStartBrokerConnection(brokerInfo);
                        if (_brokerConnectionDick.TryAdd(brokerInfo.BrokerName, brokerconn))
                        {
                            _logger.InfoFormat("添加Broker:{0}", brokerInfo);
                        }
                    }
                }
            }
        }
        private void RefreshTopicRouteInfo()
        {
            lock (_lockObj)
            {
                foreach(var entry in _topicMessageQueueDict)
                {
                    var topic = entry.Key;
                    var oldMessageQueueList = entry.Value;
                    var topicRouteInfoList = GetTopicRouteInfoList(topic);
                    var newMessageQueueList = new List<MessageQueue>();

                    foreach(var route in topicRouteInfoList)
                    {
                        foreach(var queueid in route.QueueInfo)
                        {
                            var messageQueue = new MessageQueue(route.BrokerInfo.BrokerName, topic, queueid);
                            newMessageQueueList.Add(messageQueue);
                        }
                    }
                    SortMessageQueues(newMessageQueueList);

                    var newMessageQueueJson = _json.Serialize(newMessageQueueList);
                    var oldMessageQueueJson = _json.Serialize(oldMessageQueueList);

                    if(oldMessageQueueJson!= newMessageQueueJson)
                    {
                        _topicMessageQueueDict[topic] = newMessageQueueList;
                        _logger.InfoFormat("topic路由信息发生变化，topic:{0},newRouteInfo:{1},oldRouteInfo:{2}", topic, newMessageQueueJson, oldMessageQueueJson);
                    }
                }
            }
        }
        private IList<TopicRouteInfo> GetTopicRouteInfoList(string topic)
        {
            var nameServerClient = GetAvialableNameServerRemotingClient();
            var request = new GetTopicRouteInfoRequest
            {
                ClientRole = _producer != null ? ClientRole.Producer : ClientRole.Consumer,
                ClusterName = _settings.ClusterName,
                OnlyFindMaster = _settings.OnlyFindMasterBroker,
                Topic = topic
            };

            var data = _binarySerializer.Serialize(request);
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.GetTopicRouteInfo, data);
            var remotingResponse = nameServerClient.InvokeSync(remotingRequest, 5000);
            if(remotingResponse.ResponseCode != ResponseCode.Success)
            {
                throw new Exception($"通过地址获取名字路由信息失败,topic:{topic},nameServeAddress:{nameServerClient},remoting ResponseCode:{remotingResponse.ResponseCode},errorMessage:{Encoding.UTF8.GetString(remotingResponse.ResponseBody)}");
            }
            return _binarySerializer.Deserialize<IList<TopicRouteInfo>>(remotingResponse.ResponseBody);
        }

        private SocketRemotingClient GetAvialableNameServerRemotingClient()
        {
            var avaliableList = _nameServerRemotingClientList.Where(x => x.IsConnected).ToList();
            if (avaliableList.Count == 0)
                throw new Exception("无可用服务地址");

            return avaliableList[(int)(Interlocked.Increment(ref _nameServerIndex) % avaliableList.Count)];

        }
        private void SortMessageQueues(IList<MessageQueue> messageQueueList)
        {
            ((List<MessageQueue>)messageQueueList).Sort((x, y) =>
            {
                var result = string.Compare(x.BrokerName, y.BrokerName);
                if (result != 0)
                    return result;
                else if (x.QueueId > y.QueueId)
                    return 1;
                else if (x.QueueId < y.QueueId)
                    return -1;
                return 0;
            });
            
        }

        private BrokerConnection BuildAndStartBrokerConnection(BrokerInfo brokerInfo)
        {
            IPEndPoint brokerEndPoint;
            if(_producer != null)
            {
                brokerEndPoint = brokerInfo.ProducerAddress.ToEndPoint();
            }else if(_consumer != null)
            {
                brokerEndPoint = brokerInfo.ConsumerAddress.ToEndPoint();
            }
            else
            {
                throw new Exception("客户端服务必须设置为Producer或者Consumer其中之一。");
            }
            var brokerAdminEndpoint = brokerInfo.AdminAddress.ToEndPoint();
            var remotingClient = new SocketRemotingClient(brokerEndPoint, _settings.SocketSetting);
            var adminRemotingClient = new SocketRemotingClient(brokerAdminEndpoint, _settings.SocketSetting);
            var brokerConnection = new BrokerConnection(brokerInfo, remotingClient, adminRemotingClient);
            if(_producer !=null && _producer.ResponseHandler != null)
            {
                remotingClient.RegisterResponseHandler((int)BrokerRequestCode.SendMessage, _producer.ResponseHandler);
                remotingClient.RegisterResponseHandler((int)BrokerRequestCode.BatchSendMessage, _producer.ResponseHandler);
            }
            brokerConnection.Start();
            return brokerConnection;
        }


        private string BuildClientId(string clientName)
        {
            var ip = SocketUtils.GetLocalIPV4().ToString();
            var processId = Process.GetCurrentProcess().Id;
            if (string.IsNullOrWhiteSpace(clientName))
            {
                clientName = "default";
            }
            return $"{ip}@{clientName}@{processId}@{_nameServerIndex}";
        }
    }
}
