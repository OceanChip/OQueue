using OceanChip.Common.Components;
using OceanChip.Common.Extensions;
using OceanChip.Common.Logging;
using OceanChip.Common.Remoting;
using OceanChip.Common.Utilities;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Protocols.Brokers;
using OceanChip.Queue.Protocols.Brokers.Request;
using OceanChip.Queue.Protocols.Brokers.Requests;
using OceanChip.Queue.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Clients.Producers
{
    public class Producer
    {
        private readonly object _lockObj = new object();
        private readonly ClientService _clientService;
        private readonly IQueueSelector _queueSelector;
        private readonly ILogger _logger;
        private IResponseHandler _responseHandler;
        private bool _started;

        public string Name { get; private set; }
        public ProducerSetting Setting{get;private set;}
        public IResponseHandler ResponseHandler => _responseHandler;

        public Producer(string name = null) : this(null, name) { }
        public Producer(ProducerSetting setting=null,string name = null)
        {
            this.Name = name;
            this.Setting = setting ?? new ProducerSetting();
            if(Setting.NameServerList ==null || this.Setting.NameServerList.Count() == 0)
            {
                throw new Exception("地址列表不能为空");
            }
            _queueSelector = ObjectContainer.Resolve<IQueueSelector>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            var clientSetting = new ClientSetting
            {
                ClientName = Name,
                ClusterName = this.Setting.ClusterName,
                NameServerList = this.Setting.NameServerList,
                SocketSetting = this.Setting.SocketSetting,
                OnlyFindMasterBroker = true,
                SendHeartbeatInterval = this.Setting.HeartbeatBrokerInterval,
                RefreshBrokerAndTopicRouteInfoInterval = this.Setting.RefreshBrokerAndTopicRouteInfoInterval,
            };
            _clientService = new ClientService(clientSetting, this, null);
        }

        public Producer RegisterResponseHandler(IResponseHandler responseHandler)
        {
            _responseHandler = responseHandler;
            return this;
        }
        public Producer Start()
        {
            _clientService.Start();
            _started = true;
            _logger.Info("Producer已启动.");
            return this;
        }
        public Producer Shutdown()
        {
            _clientService.Stop();
            _logger.Info("Producer已关闭");
            _started = false;
            return this;
        }
        public SendResult Send(Message message,string routingKey,int timeoutMillisecond = 30 * 1000)
        {
            if (!_started)
                throw new Exception("Producer未启动，请先启动Producer");

            var sendResult = SendAsync(message, routingKey, timeoutMillisecond).WaitResult(timeoutMillisecond + 3000);
            if(sendResult == null)
            {
                sendResult = new SendResult(SendStatus.Timeout, null, string.Format("发生消息超时，message：{0},routingKey:{1},timeoutMilliseconds:{2}", message, routingKey, timeoutMillisecond));
            }
            return sendResult;
        }

        public BatchSendResult BatchSend(IEnumerable<Message> messages, string routingKey, int timeoutMillisecond = 30 * 1000)
        {
            if (!_started)
                throw new Exception("Producer未启动，请先启动Producer");

            var sendResult = BatchSendAsync(messages, routingKey, timeoutMillisecond).WaitResult(timeoutMillisecond + 3000);
            if (sendResult == null)
            {
                sendResult = new BatchSendResult(SendStatus.Timeout, null, string.Format("发生消息超时,routingKey:{0},timeoutMilliseconds:{1}", routingKey, timeoutMillisecond));
            }
            return sendResult;
        }

        public async Task<BatchSendResult> BatchSendAsync(IEnumerable<Message> messages, string routingKey, int timeoutMillisecond=1000*30)
        { 
            Check.NotNull(messages, nameof(messages));
            if (!_started)
                throw new Exception("Producer未启动，请先启动Producer");

            var message=messages.First();
            var sendResult = default(BatchSendResult);
            var retryCount = 0;
            while (retryCount <= Setting.SendMessageMaxRetryCount)
            {
                MessageQueue messageQueue;
                BrokerConnection brokerConnection;
                if (!TryGetAvailableMessageQueue(message, routingKey, out messageQueue, out brokerConnection))
                {
                    throw new Exception(string.Format("topic[{0}]的消息队列无效", messageQueue.Topic));
                }
                var remotingRequest = BuildBatchSendMessageRequest(messages, messageQueue.QueueId, brokerConnection);
                try
                {
                    var remotingResponse = await brokerConnection.RemotingClient.InvokeAsync(remotingRequest, timeoutMillisecond);
                    if (remotingResponse == null)
                    {
                        sendResult = new BatchSendResult(SendStatus.Timeout, null, string.Format("发生消息超时，queue:{0},routingKey:{1},timeoutMilliseconds:{2},brokerInfo:{3}", messageQueue, routingKey, timeoutMillisecond, brokerConnection.BrokerInfo));
                    }
                    return ParseBatchSendResult(remotingResponse);
                }
                catch (Exception ex)
                {
                    sendResult = new BatchSendResult(SendStatus.Failed, null, ex.ToString());
                }
                if (sendResult.SendStatus == SendStatus.Success)
                {
                    return sendResult;
                }
                if (retryCount > 0)
                {
                    _logger.ErrorFormat("批量发送消息失败，queue:{0},broker:{1},sendResult:{2},retryTimes:{3}", messageQueue, brokerConnection.BrokerInfo, sendResult, retryCount);
                }
                else
                {
                    _logger.ErrorFormat("批量发送消息失败,queue:{0},broker:{1},sendResult:{2}", messageQueue, brokerConnection.BrokerInfo, sendResult);
                }
                retryCount++;
            }
            return sendResult;
        }
        public void SendWithCallback(Message message,string routingKey)
        {
            Check.NotNull(message, nameof(message));
            if (!_started)
                throw new Exception("Producer未启动，请先启动Producer");

            MessageQueue messageQueue;
            BrokerConnection brokerConnection;
            if (!TryGetAvailableMessageQueue(message, routingKey, out messageQueue, out brokerConnection))
            {
                throw new Exception(string.Format("topic[{0}]的消息队列无效", messageQueue.Topic));
            }
            var remotingRequest = BuildSendMessageRequest(message, messageQueue.QueueId, brokerConnection);
            brokerConnection.RemotingClient.InvokeWithCallback(remotingRequest);
        }


        public void BatchSendWithCallback(IEnumerable<Message> messages,string routingKey)
        {
            Check.NotNull(messages, nameof(messages));
            if (!_started)
                throw new Exception("Producer未启动，请先启动Producer");
            var message = messages.First();
            MessageQueue messageQueue;
            BrokerConnection brokerConnection;
            if (!TryGetAvailableMessageQueue(message, routingKey, out messageQueue, out brokerConnection))
            {
                throw new Exception(string.Format("topic[{0}]的消息队列无效", messageQueue.Topic));
            }
            var remotingRequest = BuildBatchSendMessageRequest(messages, messageQueue.QueueId, brokerConnection);
            brokerConnection.RemotingClient.InvokeWithCallback(remotingRequest);
        }
        public void SendOneway(Message message,string routingKey)
        {
            Check.NotNull(message, nameof(message));
            if (!_started)
                throw new Exception("Producer未启动，请先启动Producer");

            MessageQueue messageQueue;
            BrokerConnection brokerConnection;
            if (!TryGetAvailableMessageQueue(message, routingKey, out messageQueue, out brokerConnection))
            {
                throw new Exception(string.Format("topic[{0}]的消息队列无效", messageQueue.Topic));
            }
            var remotingRequest = BuildSendMessageRequest(message, messageQueue.QueueId, brokerConnection);
            brokerConnection.RemotingClient.InvokeOnway(remotingRequest);
        }
        public void BatchSendOneway(IEnumerable<Message> messages, string routingKey)
        {
            Check.NotNull(messages, nameof(messages));
            if (!_started)
                throw new Exception("Producer未启动，请先启动Producer");
            var message = messages.First();
            MessageQueue messageQueue;
            BrokerConnection brokerConnection;
            if (!TryGetAvailableMessageQueue(message, routingKey, out messageQueue, out brokerConnection))
            {
                throw new Exception(string.Format("topic[{0}]的消息队列无效", messageQueue.Topic));
            }
            var remotingRequest = BuildBatchSendMessageRequest(messages, messageQueue.QueueId, brokerConnection);
            brokerConnection.RemotingClient.InvokeOnway(remotingRequest);
        }
        public static BatchSendResult ParseBatchSendResult(RemotingResponse remotingResponse)
        {
            Check.NotNull(remotingResponse, nameof(remotingResponse));
            if (remotingResponse.RequestCode == ResponseCode.Success)
            {
                var result = BatchMessageUtils.DecodeMessageStoreResult(remotingResponse.ResponseBody);
                return new BatchSendResult(SendStatus.Success, result, null);
            }
            else if (remotingResponse.RequestCode == 0)
            {
                return new BatchSendResult(SendStatus.Timeout, null, Encoding.UTF8.GetString(remotingResponse.ResponseBody));
            }
            else
            {
                return new BatchSendResult(SendStatus.Failed, null, Encoding.UTF8.GetString(remotingResponse.ResponseBody));
            }
        }

        public async Task<SendResult> SendAsync(Message message, string routingKey, int timeoutMillisecond=1000*30)
        {
            Check.NotNull(message, nameof(message));
            if (!_started)
                throw new Exception("Producer未启动，请先启动Producer");

            var sendResult = default(SendResult);
            var retryCount = 0;
            while (retryCount <= Setting.SendMessageMaxRetryCount)
            {
                MessageQueue messageQueue;
                BrokerConnection brokerConnection;
                if(!TryGetAvailableMessageQueue(message,routingKey,out messageQueue,out brokerConnection))
                {
                    throw new Exception(string.Format("topic[{0}]的消息队列无效",messageQueue.Topic));
                }
                var remotingRequest = BuildSendMessageRequest(message, messageQueue.QueueId, brokerConnection);
                try
                {
                    var remotingResponse = await brokerConnection.RemotingClient.InvokeAsync(remotingRequest, timeoutMillisecond);
                    if (remotingResponse == null)
                    {
                        sendResult = new SendResult(SendStatus.Timeout, null, string.Format("发生消息超时，message:{0},routingKey:{1},timeoutMilliseconds:{2},brokerInfo:{3}", message, routingKey, timeoutMillisecond, brokerConnection.BrokerInfo));
                    }
                    return ParseSendResult(remotingResponse);
                }catch(Exception ex)
                {
                    sendResult = new SendResult(SendStatus.Failed, null, ex.ToString());
                }
                if(sendResult.SendStatus == SendStatus.Success)
                {
                    return sendResult;
                }
                if (retryCount > 0)
                {
                    _logger.ErrorFormat("发送消息失败，queue:{0},broker:{1},sendResult:{2},retryTimes:{3}", messageQueue, brokerConnection.BrokerInfo, sendResult, retryCount);
                }
                else
                {
                    _logger.ErrorFormat("发送消息失败,queue:{0},broker:{1},sendResult:{2}", messageQueue, brokerConnection.BrokerInfo, sendResult);
                }
                retryCount++;
            }
            return sendResult;
        }

        public static SendResult ParseSendResult(RemotingResponse remotingResponse)
        {
            Check.NotNull(remotingResponse, nameof(remotingResponse));
            if(remotingResponse.RequestCode == ResponseCode.Success)
            {
                var result = MessageUtils.DecodeMessageStoreResult(remotingResponse.ResponseBody);
                return new SendResult(SendStatus.Success, result, null);
            }else if (remotingResponse.RequestCode == 0)
            {
                return new SendResult(SendStatus.Timeout, null, Encoding.UTF8.GetString(remotingResponse.ResponseBody));
            }
            else
            {
                return new SendResult(SendStatus.Failed, null, Encoding.UTF8.GetString(remotingResponse.ResponseBody));
            }
        }
        internal void SendHeartbeat()
        {
            var brokerConnections = _clientService.GetAllBrokerConnections();

            foreach(var brokerConn in brokerConnections)
            {
                var remotingClient = brokerConn.RemotingClient;
                var clientId = _clientService.GetClientId();
                try
                {
                    var data = Encoding.UTF32.GetBytes(clientId);
                    remotingClient.InvokeOnway(new RemotingRequest((int)BrokerRequestCode.ProductHeartbeat, data));
                }catch(Exception ex)
                {
                    if (remotingClient.IsConnected)
                    {
                        _logger.Error($"发生Producer心跳报文发生异常,brokerInfo:{brokerConn.BrokerInfo}", ex);
                    }
                }
            }
        }
        private RemotingRequest BuildSendMessageRequest(Message message, int queueId, BrokerConnection brokerConnection)
        {
            var request = new SendMessageRequest
            {
                Message = message,
                QueueId = queueId,
                ProducerAddress = brokerConnection.RemotingClient.LocalEndPoint.ToAddress()
            };
            var data = MessageUtils.EncodeSendMessageRequest(request);
            if (data.Length > Setting.MessageMaxSize)
            {
                throw new Exception($"消息长度({data.Length})不能大于最大值({Setting.MessageMaxSize})");
            }
            return new RemotingRequest((int)BrokerRequestCode.SendMessage, data);
        }
        private RemotingRequest BuildBatchSendMessageRequest(IEnumerable<Message> messages, int queueId, BrokerConnection brokerConnection)
        {
            var request = new BatchSendMessageRequest
            {
                Messages = messages,
                QueueId = queueId,
                ProducerAddress = brokerConnection.RemotingClient.LocalEndPoint.ToAddress()
            };
            var data = BatchMessageUtils.EncodeSendMessageRequest(request);
            if (data.Length > Setting.MessageMaxSize)
            {
                throw new Exception($"消息长度({data.Length})不能大于最大值({Setting.MessageMaxSize})");
            }
            return new RemotingRequest((int)BrokerRequestCode.BatchSendMessage, data);
        }

        private bool TryGetAvailableMessageQueue(Message message, string routingKey, out MessageQueue messageQueue, out BrokerConnection brokerConnection)
        {
            messageQueue = null;
            brokerConnection = null;
            var retryCount = 0;

            while (retryCount < Setting.SendMessageMaxRetryCount)
            {
                messageQueue = GetAvailableMessageQueue(message, routingKey);
                if (messageQueue == null)
                {
                    if (retryCount == 0)
                    {
                        _logger.ErrorFormat("topic[{0}]消息队列无效", message.Topic);
                    }
                    else
                    {
                        _logger.ErrorFormat("topic[{0}]消息队列无效，retryTimes:{1}", message.Topic, retryCount);
                    }
                }
                else
                {
                    brokerConnection = _clientService.GetBrokerConnection(messageQueue.BrokerName);
                    if(brokerConnection!=null && brokerConnection.RemotingClient.IsConnected)
                    {
                        return true;
                    }
                    if(retryCount == 0)
                    {
                        _logger.ErrorFormat("队列[{0}]的Broker无效", messageQueue);
                    }
                    else
                    {
                        _logger.ErrorFormat("队列[{0}]的Broker无效,retryTimes:{1}", messageQueue,retryCount);
                    }
                }
                retryCount++;
            }
            return false;
        }

        private MessageQueue GetAvailableMessageQueue(Message message, string routingKey)
        {
            var messageQueueList = _clientService.GetTopicMessageQueues(message.Topic);
            if(messageQueueList==null || messageQueueList.IsEmpty())
            {
                return null;
            }
            return _queueSelector.SelectMessageQueue(messageQueueList, message, routingKey);
        }
    }
}
