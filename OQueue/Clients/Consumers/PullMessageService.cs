using OceanChip.Common.Components;
using OceanChip.Common.Logging;
using OceanChip.Common.Scheduling;
using OceanChip.Common.Serializing;
using OceanChip.Queue.Protocols;
using System.Collections.Concurrent;
using System;
using OceanChip.Common.Utilities;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using OceanChip.Common.Extensions;
using OceanChip.Queue.Protocols.Brokers.Requests;
using OceanChip.Queue.Protocols.Brokers;
using OceanChip.Common.Remoting;
using System.Text;
using System.Linq;
using System.IO;

namespace OceanChip.Queue.Clients.Consumers
{
    internal class PullMessageService
    {
        private readonly object _lockObj = new object();
        private readonly string _clientId;
        private readonly Consumer _consumer;
        private readonly ClientService _clientService;
        private readonly IBinarySerializer _binarySerializer;
        private readonly BlockingCollection<ConsumingMessage> _consumingMessageQueue;
        private readonly BlockingCollection<ConsumingMessage> _messageRetryQueue;
        private readonly BlockingCollection<QueueMessage> _pulledMessageQueue;
        private readonly Worker _consumeMessageWorker;
        private readonly IScheduleService _scheduleService;
        private IMessageHandler _messageHandler;
        private readonly ILogger _logger;

        public PullMessageService(Consumer consumer, ClientService clientService)
        {
            this._consumer = consumer;
            _clientService = clientService;
            _clientId = clientService.GetClientId();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            if (consumer.Setting.AutoPull)
            {
                if(consumer.Setting.MessageHandleMode== MessageHandleMode.Sequential)
                {
                    _consumingMessageQueue = new BlockingCollection<ConsumingMessage>();
                    _consumeMessageWorker = new Worker("ConsumeMessage", () => HandleMessage(_consumingMessageQueue.Take()));
                }
                _messageRetryQueue = new BlockingCollection<ConsumingMessage>();
            }
            else
            {
                _pulledMessageQueue = new BlockingCollection<QueueMessage>();
            }
        }
        public void SetMessageHandler(IMessageHandler messageHandler)
        {
            Check.NotNull(messageHandler, nameof(messageHandler));
            _messageHandler = messageHandler;
        }
        public void SchedulePullRequest(PullRequest pullRequest)
        {
            if(_consumer.Setting.AutoPull && _messageHandler == null)
            {
                _logger.Error("任务计划由于消息处理函数未设置而被取消");
                return;
            }
            Task.Factory.StartNew(ExecutePullRequest, pullRequest);
        }
        public void Start()
        {
            if (_consumer.Setting.AutoPull)
            {
                if (_messageHandler == null)
                {
                    throw new Exception("由于消息处理函数未设置，如果启动自动拉取消息服务，请先调用SetMessageHandler方法");
                }
                _scheduleService.StartTask("RetryMessage", RetryMessage, 1000, _consumer.Setting.RetryMessageInterval);
            }
            _logger.InfoFormat("{0} startted.", GetType().Name);
        }

        public void Stop()
        {
            if (_consumer.Setting.AutoPull)
            {
                if(_consumer.Setting.MessageHandleMode== MessageHandleMode.Sequential)
                {
                    _consumeMessageWorker.Stop();
                }
                _scheduleService.StopTask("RetryMessage");
            }
            _logger.InfoFormat("{0} stopped.", GetType().Name);
        }
        public IEnumerable<QueueMessage> PullMessage(int maxCount,int timeoutMilliseconds,CancellationToken cancellation)
        {
            var totalMessages = new List<QueueMessage>();
            var timeoutCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellation, new CancellationTokenSource(timeoutMilliseconds).Token);

            lock (_lockObj)
            {
                while(!timeoutCancellationTokenSource.IsCancellationRequested
                    && totalMessages.Count<maxCount
                    && (_pulledMessageQueue.Count>0|| totalMessages.Count == 0))
                {
                    try
                    {
                        totalMessages.Add(_pulledMessageQueue.Take(timeoutCancellationTokenSource.Token));
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                }
            }
            return totalMessages;
        }
        private void ExecutePullRequest(object parameter)
        {
            if (_consumer.Stopped) return;

            var pullRequest = parameter as PullRequest;
            if (pullRequest == null) return;
            PullMessage(pullRequest);
        }
        private void PullMessage(PullRequest request)
        {
            var brokerConnection = _clientService.GetBrokerConnection(request.MessageQueue.BrokerName);
            if(brokerConnection == null)
            {
                Task.Factory.StartDelayedTask(5 * 1000, () => SchedulePullRequest(request));
                _logger.ErrorFormat("拉取消息失败,未找到Broker Connection，pullRequest:{0}", request);
                return;
            }
            var remotingClient = brokerConnection.RemotingClient;
            try
            {
                if (_consumer.Stopped) return;
                if (request.IsDropped) return;

                var messageCount = 0;
                var flowControlThreshold = 0;

                if (_consumer.Setting.AutoPull)
                {
                    messageCount = request.ProcessQueue.GetMessageCount();
                    flowControlThreshold = _consumer.Setting.PullMessageFlowControlThreshold;
                }
                else
                {
                    messageCount = _pulledMessageQueue.Count;
                    flowControlThreshold = _consumer.Setting.ManualPullLocalMessageQueueMaxSize;
                }

                if (messageCount > flowControlThreshold)
                {
                    var millseconds = FlowControlUtil.CalculateFlowControlTimeMilliseconds(
                        messageCount,
                        flowControlThreshold,
                        _consumer.Setting.PullMessageFlowControlStepPercent,
                        _consumer.Setting.PullMessageFlowControlStepWaitMilliseconds
                        );
                    Task.Factory.StartDelayedTask(millseconds, () => SchedulePullRequest(request));
                    return;
                }
                var pmRequest = new PullMessageRequest
                {
                    ConsumerId = _clientId,
                    ConsumerGroup = _consumer.GroupName,
                    MessageQueue = request.MessageQueue,
                    Tags = string.Join("|", request.Tags),
                    QueueOffset = request.NextConsumeOffset,
                    PullMessageBatchSize = _consumer.Setting.PullMessageBatchSize,
                    ConsumeFromWhere = _consumer.Setting.ConsumeFromWhere,
                    SuspendPullRequestMilliseconds = _consumer.Setting.SuspendPullRequestMilliseconds
                };
                var data = SerializePullMessageRequest(pmRequest);
                var remotingRequest = new RemotingRequest((int)BrokerRequestCode.PullMessage, data);

                request.PullStartTime = DateTime.Now;
                remotingClient.InvokeAsync(remotingRequest, _consumer.Setting.PullRequestTimeoutMilliseconds)
                    .ContinueWith(pullTask =>
                    {
                        try
                        {
                            if (_consumer.Stopped) return;
                            if (request.IsDropped) return;
                            if (pullTask.Exception != null)
                            {
                                _logger.Error($"拉取消息失败，pullRequest:{request}", pullTask.Exception);
                                SchedulePullRequest(request);
                            }

                            ProcessPullResponse(request, pullTask.Result, pulledMessages =>
                            {
                                var filterMessages = pulledMessages.Where(x => IsQueueMessageMatchTag(x, request.Tags));
                                var consumingMessages = filterMessages.Select(x => new ConsumingMessage(x, request)).ToList();

                                if (_consumer.Setting.AutoPull)
                                {
                                    request.ProcessQueue.AddMessages(consumingMessages);
                                    foreach (var consumingMessage in consumingMessages)
                                    {
                                        if (_consumer.Setting.MessageHandleMode == MessageHandleMode.Sequential)
                                        {
                                            _consumingMessageQueue.Add(consumingMessage);
                                        }
                                        else
                                        {
                                            Task.Factory.StartNew(HandleMessage, consumingMessage);
                                        }
                                    }
                                }
                                else
                                {
                                    foreach (var consumingMessage in consumingMessages)
                                    {
                                        _pulledMessageQueue.Add(consumingMessage.Message);
                                    }
                                }
                            });
                        }
                        catch (Exception ex)
                        {
                            if (_consumer.Stopped) return;
                            if (request.IsDropped) return;
                            if (remotingClient.IsConnected)
                            {
                                string remotingResponseBodyLength;
                                if (pullTask.Result != null)
                                {
                                    remotingResponseBodyLength = pullTask.Result.ResponseBody.Length.ToString();
                                }
                                else
                                {
                                    remotingResponseBodyLength = "pull message result is null.";
                                }
                                _logger.Error($"处理拉取的结果发生异常，pullRequest:{request},remotingResponseBodyLength:{remotingResponseBodyLength}.",ex);
                            }
                            SchedulePullRequest(request);
                        }
                    });
            }catch(Exception ex)
            {
                if (_consumer.Stopped) return;
                if (request.IsDropped) return;

                if (remotingClient.IsConnected)
                {
                    _logger.Error($"拉取消息发生异常，pullRequest:{request}", ex);
                }
                SchedulePullRequest(request);
            }
        }

        private bool IsQueueMessageMatchTag(QueueMessage x, HashSet<string> tags)
        {
            if (tags == null || tags.Count == 0)
                return true;

            foreach(var tag in tags)
            {
                if (tag == "*" || tag == x.Tag)
                    return true;
            }
            return false;
        }

        private void ProcessPullResponse(PullRequest request, RemotingResponse remotingResponse, Action<IEnumerable<QueueMessage>> handlePulledMessageAction)
        {
            if (remotingResponse != null)
            {
                _logger.Error($"拉取消费返回值为null,pullRequest:{request}");
                SchedulePullRequest(request);
                return;
            }
            if(remotingResponse.ResponseCode == -1)
            {
                _logger.ErrorFormat("拉取消息失败，pullRequest:{0},errorMessage:{1}", request, Encoding.UTF8.GetString(remotingResponse.ResponseBody));
                SchedulePullRequest(request);
                return;
            }
            if(remotingResponse.RequestCode ==(short) PullStatus.Found)
            {
                var messages = DecodeMessages(request, remotingResponse.ResponseBody);
                if (messages.Count() > 0)
                {
                    handlePulledMessageAction(messages);
                    request.NextConsumeOffset = messages.Last().QueueOffset + 1;
                }
            }else if (remotingResponse.ResponseCode == (short)PullStatus.NextOffsetReset)
            {
                var nextOffset = BitConverter.ToInt64(remotingResponse.ResponseBody, 0);
                ResetNextConsumeOffset(request, nextOffset);
            }else if(remotingResponse.ResponseCode == (short)PullStatus.NoNewMessage)
            {

            }else if (remotingResponse.ResponseCode == (short)PullStatus.Ignored)
            {
                _logger.InfoFormat("Pull Request已经取消，pullRequest:{0}", request);
                return;
            }else if(remotingResponse.ResponseCode == (short)PullStatus.BrokerIsCleaning)
            {
                Thread.Sleep(5000);
            }
            SchedulePullRequest(request);
        }
        private void RetryMessage()
        {
            ConsumingMessage message;
            if(_messageRetryQueue.TryTake(out message))
            {
                HandleMessage(message);
            }
        }


        private IEnumerable<QueueMessage> DecodeMessages(PullRequest request, byte[] buffer)
        {
            var messages = new List<QueueMessage>();
            if (buffer == null || buffer.Length <= 4)
                return messages;

            try
            {
                var nextOffset = 0;
                var messageLength = ByteUtil.DecodeInt(buffer, nextOffset, out nextOffset);
                while (messageLength > 0)
                {
                    var message = new QueueMessage();
                    var messageBytes = new byte[messageLength];
                    Buffer.BlockCopy(buffer, nextOffset, messageBytes, 0, messageLength);
                    nextOffset += messageLength;
                    message.ReadForm(messageBytes);
                    if (!message.IsValid())
                    {
                        _logger.ErrorFormat("无效的消息，pullRequest:{0}", request);
                        continue;
                    }
                    messages.Add(message);
                    if (nextOffset >= buffer.Length)
                    {
                        break;
                    }
                    messageLength = ByteUtil.DecodeInt(buffer, nextOffset, out nextOffset);
                }
            }catch(Exception ex)
            {
                _logger.Error($"解包消息发生异常，pullRequest:{request}", ex);
            }
            return messages;
        }
        
        private void ResetNextConsumeOffset(PullRequest request, long newOffset)
        {
            var brokerConnection = _clientService.GetBrokerConnection(request.MessageQueue.BrokerName);
            if (brokerConnection == null)
            {
                _logger.ErrorFormat("通过无效的Broker重置消费者位置失败，pullRequest:{0},nexOffset:{1}", request, newOffset);
                return;
            }
            var remotingClient = brokerConnection.RemotingClient;
            try
            {
                var oldOffset = request.NextConsumeOffset;
                request.NextConsumeOffset = newOffset;
                request.ProcessQueue.MarkAllConsumeingMessageIgnored();
                request.ProcessQueue.Reset();

                var updateRequest = new UpdateQueueOffsetRequest(_consumer.GroupName, request.MessageQueue, newOffset - 1);
                var remotingRequest = new RemotingRequest((int)BrokerRequestCode.UpdateQueueConsumeOffsetRequest, _binarySerializer.Serialize(updateRequest));
                remotingClient.InvokeOnway(remotingRequest);
                _logger.InfoFormat("Resetted nextConsumeOffset,[PullRequest:{0},oldOffset:{1},newOffset:{2}]", request, oldOffset, newOffset);
            }catch(Exception ex)
            {
                if (remotingClient.IsConnected)
                {
                    _logger.Error($"重置消费者位置失败，pullRequest:{request},newOffset:{newOffset}", ex);
                }
            }
        }

        private byte[] SerializePullMessageRequest(PullMessageRequest pmRequest)
        {
            using (var stream = new MemoryStream())
            {
                PullMessageRequest.WriteToStream(pmRequest, stream);
                return stream.ToArray();
            }
        }

        private void HandleMessage( object parameter)
        {
            var consumingMessage = parameter as ConsumingMessage;
            if (_consumer.Stopped) return;
            if (consumingMessage == null) return;
            if (consumingMessage.PullRequest.IsDropped) return;
            if (consumingMessage.IsIgnored)
            {
                RemoveHandleMessage(consumingMessage);
                return;
            }
            try
            {
                _messageHandler.Handle(consumingMessage.Message, new MessageContext(currentQueueMessage => RemoveHandleMessage(consumingMessage)));
            }catch(Exception ex)
            {
                //TODO，目前，对于消费失败（遇到异常）的消息，我们先记录错误日志，然后将该消息放入本地内存的重试队列；
                //放入重试队列后，会定期对该消息进行重试，重试队列中的消息会定时被取出一个来重试。
                //通过这样的设计，可以确保消费有异常的消息不会被认为消费已成功，也就是说不会从ProcessQueue中移除；
                //但不影响该消息的后续消息的消费，该消息的后续消息仍然能够被消费，但是ProcessQueue的消费位置，即滑动门不会向前移动了；
                //因为只要该消息一直消费遇到异常，那就意味着该消息所对应的queueOffset不能被认为已消费；
                //而我们发送到broker的是当前最小的已被成功消费的queueOffset，所以broker上记录的当前queue的消费位置（消费进度）不会往前移动，
                //直到当前失败的消息消费成功为止。所以，如果我们重启了消费者服务器，那下一次开始消费的消费位置还是从当前失败的位置开始，
                //即便当前失败的消息的后续消息之前已经被消费过了；所以应用需要对每个消息的消费都要支持幂等；
                //未来，我们会在broker上支持重试队列，然后我们可以将消费失败的消息发回到broker上的重试队列，发回到broker上的重试队列成功后，
                //就可以让当前queue的消费位置往前移动了。
                LogMessageHandlingException(consumingMessage, ex);
                _messageRetryQueue.Add(consumingMessage);
            }
        }

        private void LogMessageHandlingException(ConsumingMessage message, Exception ex)
        {
            _logger.Error($"消息处理发生异常，消息信息：[message:{message.Message.MessageId},topic:{message.Message.Topic}," +
                $"queueId:{message.Message.QueueId},queueOffset:{message.Message.QueueOffset}," +
                $"createdTime:{message.Message.CreatedTime},storedTime:{message.Message.StoredTime}," +
                $"brokerName:{message.PullRequest.MessageQueue.BrokerName},groupName:{_consumer.GroupName}]", ex);
        }

        private void RemoveHandleMessage(ConsumingMessage consumingMessage)
        {
            consumingMessage.PullRequest.ProcessQueue.RemoveMessage(consumingMessage);
        }
    }
}