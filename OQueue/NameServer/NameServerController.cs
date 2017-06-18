using OceanChip.Common.Components;
using OceanChip.Common.Logging;
using OceanChip.Common.Remoting;
using OceanChip.Common.Socketing;
using OceanChip.Common.Utilities;
using OceanChip.Queue.NameServer.RequestHandlers;
using OceanChip.Queue.Protocols.Brokers;
using System.Diagnostics;
using System.Threading;
using System;
using System.Net.Sockets;
using OceanChip.Common.Extensions;

namespace OceanChip.Queue.NameServer
{
    public class NameServerController
    {
        private readonly ILogger _logger;
        private readonly SocketRemotingServer _socketRemotingServer;
        private readonly ConsoleEventHandlerService _service;
        private int _isShuttingdown = 1;
        public NameServerSetting Setting { get; private set; }

        public ClusterManager ClusterManager { get; private set; }

        public NameServerController(NameServerSetting setting = null)
        {
            this.Setting = setting ?? new NameServerSetting();
            this.ClusterManager = new ClusterManager(this);
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _service = new ConsoleEventHandlerService();
            _service.RegisterClosingEventHandler(eventCode => { Shutdown(); });
            _socketRemotingServer = new SocketRemotingServer("OceanChip.Queue.NameServer.RemotingServer", Setting.BindingAddress, Setting.SocketSetting);
            RegisterRequestHandlers();
        }
        public NameServerController Start()
        {
            Stopwatch watch = Stopwatch.StartNew();
            _logger.Info("NameServer服务开始启动...");
            ClusterManager.Start();
            _socketRemotingServer.Start();
            Interlocked.Exchange(ref _isShuttingdown, 0);
            _logger.Info($"NameServer服务启动成功，启动花费时间：{watch.ElapsedMilliseconds}ms,绑定地址：{Setting.BindingAddress}");
            watch.Stop();
            watch = null;
            return this;
        }
        public NameServerController Shutdown()
        {
            if(Interlocked.CompareExchange(ref _isShuttingdown, 1, 0) == 0)
            {
                Stopwatch watch = Stopwatch.StartNew();
                _logger.Info($"NameServer 服务准备关闭....，绑定地址：{Setting.BindingAddress}");
                _socketRemotingServer.Shutdown();
                ClusterManager.Shutdown();
                _logger.Info($"NameServer服务关闭成功，启动花费时间：{watch.ElapsedMilliseconds}ms");
                watch.Stop();
                watch = null;
            }
            return this;
        }

        private void RegisterRequestHandlers()
        {
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.RegisterBroker, new RegisterBrokerRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.UnregisterBroker, new UnRegisterBrokerRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetAllClusters, new GetAllClustersRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetClusterBrokers, new GetClusterBrokersRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetClusterBrokerStatusInfoList, new GetClusterBrokerStatusInfoListRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetTopicRouteInfo, new GetTopicRouteInfoRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetTopicQueueInfo, new GetTopicQueueInfoRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetTopicConsumeInfo, new GetTopicConsumeInfoRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetProducerList, new GetProducerListRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetConsumerList, new GetConsumerListRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.CreateTopic, new CreateTopicForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.DeleteTopic, new DeleteTopicForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.AddQueue, new AddQueueForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.DeleteQueue, new DeleteQueueForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.SetQueueProducerVisible, new SetQueueProducerVisibleForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.SetQueueConsumerVisible, new SetQueueConsumerVisibleForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.SetQueueNextConsumeOffset, new SetQueueNextConsumeOffsetForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.DeleteConsumerGroup, new DeleteConsumerGroupForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetTopicAccumulateInfoList, new GetTopicAccumulateInfoListRequestHandler(this));

        }

        class BrokerConnectionEventListener : IConnectionEventListener
        {
            private NameServerController _nameServerController;
            public BrokerConnectionEventListener(NameServerController nameServerController)
            {
                this._nameServerController = nameServerController;
            }
            public void OnConnectionAccepted(ITcpConnection connection)
            {
                var connectionId = connection.RemoteEndPoint.ToAddress();
                _nameServerController._logger.Info($"Broker接收请求，Connectionid：{connectionId}");
            }

            public void OnConnectionClosed(ITcpConnection connection, SocketError socketError)
            {
                var connectionId = connection.RemoteEndPoint.ToAddress();
                _nameServerController._logger.Info($"Broker断开链接，ConnectionId:{connectionId}");
                _nameServerController.ClusterManager.RemoveBroker(connection);
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