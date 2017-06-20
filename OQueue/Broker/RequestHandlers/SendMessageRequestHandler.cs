using OceanChip.Common.Components;
using OceanChip.Common.Logging;
using OceanChip.Common.Remoting;
using OceanChip.Common.Utilities;
using OceanChip.Queue.Broker.Exceptions;
using OceanChip.Queue.Broker.LongPolling;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.RequestHandlers
{
    public class SendMessageRequestHandler : IRequestHandler
    {
        private readonly SuspendedPullRequestManager _suspendedPullRequestManager;
        private readonly IMessageStore _messageStore;
        private readonly IQueueStore _queueStore;
        private readonly ILogger _logger;
        private readonly ILogger _sendRTLogger;
        private readonly object _syncObj = new object();
        private readonly bool _notifyWhenMessageArrived;
        private readonly BufferQueue<StoreContext> _bufferQueue;
        private readonly ITpsStatisticService _tpsStatisticService;
        private const string SendMessageFailedText = "Send message failed.";

        public SendMessageRequestHandler()
        {
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _tpsStatisticService = ObjectContainer.Resolve<ITpsStatisticService>();
            _notifyWhenMessageArrived = BrokerController.Instance.Setting.NotifyWhenMessageArrived;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _sendRTLogger = ObjectContainer.Resolve<ILoggerFactory>().Create("SendRT");
            var messageWriteQueueThreshold = BrokerController.Instance.Setting.MessageWriteQueueThreshold;
            _bufferQueue = new BufferQueue<StoreContext>("QueueBufferQueue", messageWriteQueueThreshold, OnQueueMessageCompleted, _logger);
        }

        private void OnQueueMessageCompleted(StoreContext obj)
        {
            obj.OnComplete();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (remotingRequest.Body.Length > BrokerController.Instance.Setting.MessageMaxSize)
                throw new Exception($"消息长度({remotingRequest.Body.Length})超过最大({BrokerController.Instance.Setting.MessageMaxSize})限制");

            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }

            var request = MessageUtils.DecodeSendMessageRequest(remotingRequest.Body);
            var message = request.Message;
            var queueId = request.QueueId;
            var queue = _queueStore.GetQueue(message.Topic, queueId);
            if(queue == null)
            {
                throw new QueueNotExistException(message.Topic, queueId);
            }
            _messageStore.StoreMessageAsync(queue, message, (record, parameter) =>
            {
                var storeContext = parameter as StoreContext;
                if(record.LogPosition>=0 && !string.IsNullOrEmpty(record.MessageId))
                {
                    storeContext.Queue.AddMessage(record.LogPosition, record.Tag);
                    storeContext.MessageLogRecord = record;
                    storeContext.Success = true;
                }
                else
                {
                    storeContext.Success = false;
                }
                _bufferQueue.EnqueueMessage(storeContext);
            }, new StoreContext
            {
                RemotingRequest=remotingRequest,
                RequestHandlerContext=context,
                Queue=queue,
                SendMessageRequestHandler=this
            }, request.ProducerAddress);
            _tpsStatisticService.AddTopicSendCount(message.Topic, queueId);
            return null;
        }

        class StoreContext
        {
            public IRequestHandlerContext RequestHandlerContext;
            public RemotingRequest RemotingRequest;
            public Queue Queue;
            public MessageLogRecord MessageLogRecord;
            public SendMessageRequestHandler SendMessageRequestHandler;
            public bool Success;

            public void OnComplete()
            {
                if (Success)
                {
                    var result = new MessageStoreResult(
                        MessageLogRecord.MessageId,
                        MessageLogRecord.Code,
                        MessageLogRecord.Topic,
                        MessageLogRecord.QueueId,
                        MessageLogRecord.QueueOffset,
                        MessageLogRecord.Tag,
                        MessageLogRecord.CreatedTime,
                        MessageLogRecord.StoredTime
                        );
                    var data = MessageUtils.EncodeMessageStoreResult(result);
                    var response = RemotingResponseFactory.CreateResponse(RemotingRequest, data);

                    RequestHandlerContext.SendRemotingResponse(response);

                    if (SendMessageRequestHandler._notifyWhenMessageArrived)
                    {
                        SendMessageRequestHandler._suspendedPullRequestManager.NotifyNewMessage(MessageLogRecord.Topic, result.QueueId, result.QueueOffset);
                    }

                    BrokerController.Instance.AddLatestMessage(result.MessageId, result.CreatedTime, result.StoredTime);
                }
                else
                {
                    var response = RemotingResponseFactory.CreateResponse(RemotingRequest, ResponseCode.Failed, Encoding.UTF8.GetBytes(SendMessageFailedText));
                    RequestHandlerContext.SendRemotingResponse(response);
                }
            }
        }
    }
}
