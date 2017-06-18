using OceanChip.Common.Components;
using OceanChip.Common.Extensions;
using OceanChip.Common.Logging;
using OceanChip.Common.Remoting;
using OceanChip.Common.Scheduling;
using OceanChip.Common.Serializing;
using OceanChip.Common.Socketing;
using OceanChip.Common.Utilities;
using OceanChip.Queue.Broker.Client;
using OceanChip.Queue.Broker.LongPolling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.Diagnostics;
using System.Threading;
using System.IO;
using OceanChip.Queue.Protocols.Brokers;
using OceanChip.Queue.Protocols.Brokers.Requests;
using OceanChip.Queue.Protocols.Brokers;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Broker.RequestHandlers;
using OceanChip.Queue.Broker.RequestHandlers.Admin;
using OceanChip.Queue.Protocols.NameServers.Requests;

namespace OceanChip.Queue.Broker
{
    public class BrokerController
    {
        private static BrokerController _instance;
        private readonly ILogger _logger;
        private readonly IQueueStore _queueStore;
        private readonly IMessageStore _messageStore;
        private readonly IConsumeOffsetStore _consumeOffsetStore;
        private readonly IBinarySerializer _binarySerializer;
        private readonly IScheduleService _scheduleService;
        private readonly SuspendedPullRequestManager _suspendedPullRequestManager;
        private readonly ProducerManager _producerManager;
        private readonly ConsumerManager _consumerManager;
        private readonly GetConsumerListService _getConsumerListService;
        private readonly GetTopicConsumeInfoListService _getTopicConsumeInfoListService;
        private readonly SocketRemotingServer _producerSocketRemotingServer;
        private readonly SocketRemotingServer _consumerSocketRemotingServer;
        private readonly SocketRemotingServer _adminSocketRemotingServer;
        private readonly ConsoleEventHandlerService _service;
        private readonly IChunkStatisticService _chunkReadStatisticService;
        private readonly ITpsStatisticService _tpsStatisticService;
        private readonly IList<SocketRemotingClient> _nameServerRemotingClientList;
        private string[] _latestMessageIds;
        private long _messageIdSequece;
        private int _isShuttingdown = 0;
        private int _isCleaning = 0;

        public static BrokerController Instance => _instance;
        public BrokerSetting Setting { get; private set; }

        public ConsumerManager ConsumerManager => _consumerManager;
        public ProducerManager ProducerManager => _producerManager;

        public bool IsCleaning => _isCleaning == 1;

        private BrokerController(BrokerSetting setting)
        {
            Setting = setting ?? new BrokerSetting();
            this.Setting.BrokerInfo.Valid();
            if(this.Setting.NameServerList==null || Setting.NameServerList.Count() == 0)
            {
                throw new ArgumentException("NameServerList不能为空");
            }
            this._latestMessageIds = new string[this.Setting.LastestMessageShowCount];
            this._producerManager = ObjectContainer.Resolve<ProducerManager>();
            this._consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            this._messageStore = ObjectContainer.Resolve<IMessageStore>();
            this._consumeOffsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            this._queueStore = ObjectContainer.Resolve<IQueueStore>();
            this._getTopicConsumeInfoListService = ObjectContainer.Resolve<GetTopicConsumeInfoListService>();
            this._getConsumerListService = ObjectContainer.Resolve<GetConsumerListService>();
            this._scheduleService = ObjectContainer.Resolve<IScheduleService>();
            this._binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            this._suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            this._chunkReadStatisticService = ObjectContainer.Resolve<IChunkStatisticService>();
            this._tpsStatisticService = ObjectContainer.Resolve<ITpsStatisticService>();

            this._producerSocketRemotingServer = new SocketRemotingServer("OQueue.Broker.ProducerRemotingServer", Setting.BrokerInfo.ProducerAddress.ToEndPoint(), this.Setting.SocketSetting);
            this._consumerSocketRemotingServer = new SocketRemotingServer("OQueue.Broke.ConsumerRemotingServer", this.Setting.BrokerInfo.ConsumerAddress.ToEndPoint(), this.Setting.SocketSetting);
            this._adminSocketRemotingServer = new SocketRemotingServer("OQueue.Broker.AdminRemotingServer", this.Setting.BrokerInfo.AdminAddress.ToEndPoint(), this.Setting.SocketSetting);

            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _producerSocketRemotingServer.RegisterConnectionEventListener(new ProducerConnectionEventListener(this));
            _consumerSocketRemotingServer.RegisterConnectionEventListener(new ConsumerConnectionEventListener(this));

            _service = new ConsoleEventHandlerService();
            _service.RegisterClosingEventHandler(eventCode => { Shutdown(); });
            _nameServerRemotingClientList = this.Setting.NameServerList.ToRomotingClientList(this.Setting.SocketSetting).ToList();

        }

        private void RegisterRequestHandlers()
        {
            RegisterRequestHandler<ProducerHeartbeatRequestHandler>(BrokerRequestCode.ProductHeartbeat);
            RegisterRequestHandler<BatchSendMessageRequestHandler>(BrokerRequestCode.BatchSendMessage);
            RegisterRequestHandler<ConsumerHeartbeatRequestHandler>(BrokerRequestCode.ConsumerHeartbeat);
            RegisterRequestHandler<GetConsumerIdsForTopicRequestHandler>(BrokerRequestCode.GetConsumerIdsForTopic);
            RegisterRequestHandler<PullMessageRequestHandler>(BrokerRequestCode.PullMessage);
            RegisterRequestHandler<SendMessageRequestHandler>(BrokerRequestCode.SendMessage);
            RegisterRequestHandler<UpdateQueueConsumeOffsetRequestHandler>(BrokerRequestCode.UpdateQueueConsumeOffsetRequest);

            RegisterRequestHandler<AddQueueRequestHandler>(BrokerRequestCode.AddQueue);
            RegisterRequestHandler<CreateTopicRequestHandler>(BrokerRequestCode.CreateTopic);
            RegisterRequestHandler<DeleteConsumerGroupRequestHandler>(BrokerRequestCode.DeleteConsumerGroup);
            RegisterRequestHandler<DeleteQueueRequestHandler>(BrokerRequestCode.DeleteQueue);
            RegisterRequestHandler<DeleteTopicRequestHandler>(BrokerRequestCode.DeleteTopic);
            RegisterRequestHandler<GetBrokerStatisticInfoRequestHandler>(BrokerRequestCode.GetBrokerStatisticInfo);
            RegisterRequestHandler<GetBrokerLastestSendMessagesRequestHandler>(BrokerRequestCode.GetLastestMessages);
            RegisterRequestHandler<GetConsumerListRequestHandler>(BrokerRequestCode.GetConsumerList);
            RegisterRequestHandler<GetMessageDetailRequestHandler>(BrokerRequestCode.GetMessageDetail);
            RegisterRequestHandler<GetProducerListRequestHandler>(BrokerRequestCode.GetProducerList);
            RegisterRequestHandler<GetTopicConsumeInfoRequestHandler>(BrokerRequestCode.GetTopicConsumeInfo);
            RegisterRequestHandler<GetTopicQueueInfoRequestHandler>(BrokerRequestCode.GetTopicQueueInfo);
            RegisterRequestHandler<SetQueueConsumerVisibleRequestHandler>(BrokerRequestCode.SetQueueConsumerVisible);
            RegisterRequestHandler<SetQueueNextConsumeOffsetRequestHandler>(BrokerRequestCode.SetQueueNextConsumeOffset);
            RegisterRequestHandler<SetQueueProducerVisibleRequestHandler>(BrokerRequestCode.SetQueueProducerVisible);

        }
        private void RegisterRequestHandler<T>(BrokerRequestCode code) where T: class,IRequestHandler, new()
        {
            _producerSocketRemotingServer.RegisterRequestHandler((int)code, new T());
        }

        public static BrokerController Create(BrokerSetting setting = null)
        {
            _instance = new BrokerController(setting);
            _instance.RegisterRequestHandlers();
            return _instance;
        }
        public BrokerController Clean()
        {
            var watch = Stopwatch.StartNew();
            _logger.Info("Broker开始清理...");
            if(Interlocked.CompareExchange(ref _isCleaning, 1, 0) == 0)
            {
                try
                {
                    _queueStore.Shutdown();
                    _consumeOffsetStore.Shutdown();
                    _messageStore.Shutdown();
                    _suspendedPullRequestManager.Clean();

                    if (Directory.Exists(this.Setting.FileStoreRootPath))
                    {
                        Directory.Delete(this.Setting.FileStoreRootPath, true);
                    }
                    //重新加载和启动所有组件
                    _messageStore.Load();
                    _queueStore.Load();
                    _consumeOffsetStore.Start();
                    _messageStore.Start();
                    _queueStore.Start();
                    _logger.InfoFormat("Broker 清理成功，花费时间：{0}ms，producer:[{1}],consumer:[{2}],admin:[{3}]",
                        watch.ElapsedMilliseconds,
                        this.Setting.BrokerInfo.ProducerAddress,
                        this.Setting.BrokerInfo.ConsumerAddress,
                        this.Setting.BrokerInfo.AdminAddress
                        );
                    watch.Stop();

                }catch(Exception ex)
                {
                    _logger.Error("Broker清理失败", ex);
                }
                finally
                {
                    Interlocked.Exchange(ref _isCleaning, 0);
                }
            }
            return this;
        }

        public BrokerController Start()
        {
            Stopwatch watch = Stopwatch.StartNew();
            _logger.Info("Broker开始启动...");

            _messageStore.Load();
            _queueStore.Load();

            if(_messageStore.ChunkCount==0|| _queueStore.GetAllQueueCount() == 0)
            {
                _logger.InfoFormat("消息存在或队列存储为空，尝试清除所有Broker保存的文件");
                _messageStore.Shutdown();
                _queueStore.Shutdown();
                if (Directory.Exists(Setting.FileStoreRootPath))
                {
                    Directory.Delete(Setting.FileStoreRootPath, true);
                }
                _logger.InfoFormat("所有保存的Broker文件清除成功.");
                _messageStore.Load();
                _queueStore.Load();
            }

            _consumeOffsetStore.Start();
            _messageStore.Start();
            _queueStore.Start();
            _producerManager.Start();
            _consumerManager.Start();
            _suspendedPullRequestManager.Start();
            _consumerSocketRemotingServer.Start();
            _producerSocketRemotingServer.Start();
            _adminSocketRemotingServer.Start();
            _chunkReadStatisticService.Start();
            _tpsStatisticService.Start();

            RemoveNotExistQueueConsumeOffsets();
            StartAllNameServerClients();
            RegisterBrokerToAllNameServers();
            _scheduleService.StartTask("RegisterBrokerToAllNameServers", RegisterBrokerToAllNameServers, 100, Setting.RegisterBrokerToNameServerInterval);

            Interlocked.Exchange(ref _isShuttingdown, 0);
            _logger.InfoFormat("Broker启动成功，花费时间：{0}ms,producer:[{1}],consumer:[{2}],admin:[{3}]",
                watch.ElapsedMilliseconds, Setting.BrokerInfo.ProducerAddress,
                Setting.BrokerInfo.ConsumerAddress, Setting.BrokerInfo.AdminAddress
                );
            return this;
        }

       

        public BrokerController Shutdown()
        {
            if(Interlocked.CompareExchange(ref _isShuttingdown, 1, 0) == 0)
            {
                var watch = Stopwatch.StartNew();
                _logger.InfoFormat("Broker开始关闭服务,producer:[{0}],consumer:[{1}],admin:[{2}]",
                    this.Setting.BrokerInfo.ProducerAddress,
                    this.Setting.BrokerInfo.ConsumerAddress,
                    this.Setting.BrokerInfo.AdminAddress);
                _scheduleService.StopTask("RegisterBrokerToAllNameServers");
                UnRegisterBrokerToAllNameServers();
                StopAllNameServerClients();
                _producerSocketRemotingServer.Shutdown();
                _consumerSocketRemotingServer.Shutdown();
                _adminSocketRemotingServer.Shutdown();
                _producerManager.Shutdown();
                _consumerManager.Shutdown();
                _suspendedPullRequestManager.Shutdown();
                _messageStore.Shutdown();
                _consumeOffsetStore.Shutdown();
                _queueStore.Shutdown();
                _chunkReadStatisticService.Shutdown();
                _tpsStatisticService.Shutdown();
                _logger.InfoFormat("Broker关闭服务成功，花费时间:{0}ms", watch.ElapsedMilliseconds);
                watch.Stop();
            }
            return this;
        }
        public BrokerStatisticInfo GetBrokerStatisticInfo()
        {
            return new BrokerStatisticInfo()
            {
                BrokerInfo = Setting.BrokerInfo,
                TopicCount = _queueStore.GetAllTopics().Count(),
                QueueCount = _queueStore.GetAllQueueCount(),
                TotalUnconsumedMessageCount = _queueStore.GetTotalUnConsumeMessageCount(),
                ConsumerGroupCount = _consumeOffsetStore.GetConsumerGroupCount(),
                ProducerCount = _producerManager.GetProducerCount(),
                ConsumerCount = _consumerManager.GetAllConsumerCount(),
                MessageChunkCount = _messageStore.ChunkCount,
                MessageMaxChunkNum = _messageStore.MaxChunkNum,
                MessageMinChunkNum = _messageStore.MinChunkNum,
                TotalConsumeThroughput = _tpsStatisticService.GetTotalConsumeThroughput(),
                TotalSendThroughput = _tpsStatisticService.GetTotalSendThroughput(),
            };
        }
        public string GetLastestSendMessageIds()
        {
            return string.Join(",", _latestMessageIds.ToList());
        }
        public void AddLatestMessage(string messageId,DateTime createTime,DateTime storedTime)
        {
            var sequence = Interlocked.Increment(ref _messageIdSequece);
            var index = sequence % _latestMessageIds.Length;
            _latestMessageIds[index] = string.Format("{0}_{1}_{2}", messageId, createTime.Ticks, storedTime.Ticks);
        }

        private void StartAllNameServerClients()
        {
            foreach(var client in _nameServerRemotingClientList)
            {
                client.Start();
            }
        }
        private void StopAllNameServerClients()
        {
            foreach(var client in _nameServerRemotingClientList)
            {
                client.Shutdown();
            }
        }

        private void RegisterBrokerToAllNameServers()
        {
            var request = new BrokerRegistrationRequest
            {
                BrokerInfo=Setting.BrokerInfo,
                TotalSendThroughput=_tpsStatisticService.GetTotalSendThroughput(),
                TotalConsumeThroughput=_tpsStatisticService.GetTotalConsumeThroughput(),
                TotalUnConsumedMessageCount=_queueStore.GetTotalUnConsumeMessageCount(),
                TopicQueueInfoList=_queueStore.GetTopicQueueInfoList(),
                TopicConsumeInfoList=_getTopicConsumeInfoListService.GetAllTopicConsumeInfoList().ToList(),
                ProducerList=_producerManager.GetAllProducers().ToList(),
                ConsumerList=_getConsumerListService.GetAllConsumerList().ToList(),
            };
            foreach(var client in _nameServerRemotingClientList)
            {
                RegisterBrokerToAllNameServer(request, client);
            }
        }

        private void RegisterBrokerToAllNameServer(BrokerRegistrationRequest request, SocketRemotingClient client)
        {
            var nameServerAddress = client.ServerEndPoint.ToAddress();
            try
            {
                var data = _binarySerializer.Serialize(request);
                var remotingRequest = new RemotingRequest((int)NameServerRequestCode.RegisterBroker, data);
                var remotingResponse = client.InvokeSync(remotingRequest, 5000);
                if(remotingResponse.RequestCode != ResponseCode.Success)
                {
                    _logger.ErrorFormat("注册Broker服务失败,BrokerInfo:{0},nameServerAddress:{1},返回码：{2},错误信息：{3}", 
                        Setting.BrokerInfo, 
                        nameServerAddress,
                        remotingResponse.RequestCode,
                        Encoding.UTF8.GetString(remotingResponse.ResponseBody));
                }
            }catch(Exception ex)
            {
                _logger.Error($"注册Broker服务发生异常，BrokerInfo:{Setting.BrokerInfo},nameServerAddress:{nameServerAddress}", ex);
            }
        }

        private void UnRegisterBrokerToAllNameServers()
        {
            var request = new BrokerUnRegistrationRequest
            {
                BrokerInfo = Setting.BrokerInfo
            };
            foreach(var client in _nameServerRemotingClientList)
            {
                UnRegisterBrokerToAllNameServer(request, client);
            }
        }

        private void UnRegisterBrokerToAllNameServer(BrokerUnRegistrationRequest request, SocketRemotingClient client)
        {
            var nameServerAddress = client.ServerEndPoint.ToAddress();
            try
            {
                var data = _binarySerializer.Serialize(request);
                var remotingRequest = new RemotingRequest((int)NameServerRequestCode.UnregisterBroker, data);
                var remotingResponse = client.InvokeSync(remotingRequest, 5000);
                if (remotingResponse.RequestCode != ResponseCode.Success)
                {
                    _logger.ErrorFormat("注销Broker服务失败,BrokerInfo:{0},nameServerAddress:{1},返回码：{2},错误信息：{3}",
                        Setting.BrokerInfo,
                        nameServerAddress,
                        remotingResponse.RequestCode,
                        Encoding.UTF8.GetString(remotingResponse.ResponseBody));
                }
            }
            catch (Exception ex)
            {
                _logger.Error($"注销Broker服务发生异常，BrokerInfo:{Setting.BrokerInfo},nameServerAddress:{nameServerAddress}", ex);
            }
            throw new NotImplementedException();
        }

        private void RemoveNotExistQueueConsumeOffsets()
        {
            var consumeKeys = _consumeOffsetStore.GetConsumeKeys();
            foreach(var key in consumeKeys)
            {
                if (!_queueStore.IsQueueExists(key))
                {
                    _consumeOffsetStore.DeleteConsumeOffset(key);
                }
            }
        }

        class ProducerConnectionEventListener : IConnectionEventListener
        {
            private BrokerController _brokerController;
            public ProducerConnectionEventListener(BrokerController controller)
            {
                this._brokerController = controller;
            }
            public void OnConnectionAccepted(ITcpConnection connection)
            {
                var connectionId = connection.RemoteEndPoint.ToAddress();
                _brokerController._logger.InfoFormat("生产者请求链接，链接地址：{0}", connectionId);
            }

            public void OnConnectionClosed(ITcpConnection connection, SocketError socketError)
            {
                var connectionId = connection.RemoteEndPoint.ToAddress();
                _brokerController._logger.InfoFormat("生产者断开链接，链接地址：{0}", connectionId);
                _brokerController._producerManager.RemoveProducer(connectionId);
            }

            public void OnConnectionEstableished(ITcpConnection connection)
            {
            }

            public void OnConnectionFailed(SocketError socketError)
            {
            }
        }
        class ConsumerConnectionEventListener : IConnectionEventListener
        {
            private BrokerController _brokerController;
            public ConsumerConnectionEventListener(BrokerController controller)
            {
                this._brokerController = controller;
            }
            public void OnConnectionAccepted(ITcpConnection connection)
            {
                var connectionId = connection.RemoteEndPoint.ToAddress();
                _brokerController._logger.InfoFormat("消费者请求链接，链接地址：{0}", connectionId);
            }

            public void OnConnectionClosed(ITcpConnection connection, SocketError socketError)
            {
                var connectionId = connection.RemoteEndPoint.ToAddress();
                _brokerController._logger.InfoFormat("消费者断开链接，链接地址：{0}", connectionId);
                if(_brokerController.Setting.RemoveConsumerWhenDisconnect)
                    _brokerController._consumerManager.RemoveConsumer(connectionId);
            }

            public void OnConnectionEstableished(ITcpConnection connection)
            {
            }

            public void OnConnectionFailed(SocketError socketError)
            {
            }
        }
    }
}
