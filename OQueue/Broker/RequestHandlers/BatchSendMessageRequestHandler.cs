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
    public class BatchSendMessageRequestHandler : IRequestHandler
    {
        private readonly SuspendedPullRequestManager _suspendedPullManager;
        private readonly IMessageStore _messageStore;
        private readonly IQueueStore _queueStore;
        private readonly ILogger _logger;
        private readonly ILogger _sendRTLogger;
        private readonly object _syncObj = new object();
        private readonly bool _notifyWhenMessageArrived;
        private readonly BufferQueue<StoreContext> _bufferQueue;
        private readonly ITpsStatisticService _tpsStatisticService;
        private const string SendMessageFailedText = "发生批量消息失败";

        public BatchSendMessageRequestHandler()
        {
            _suspendedPullManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _tpsStatisticService = ObjectContainer.Resolve<ITpsStatisticService>();
            _notifyWhenMessageArrived = BrokerController.Instance.Setting.NotifyWhenMessageArrived;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _sendRTLogger = ObjectContainer.Resolve<ILoggerFactory>().Create("BatchSendRT");
            var messageWriteQueueThreshold = BrokerController.Instance.Setting.BatchMessageWriteQueueThreshold;
            _bufferQueue = new BufferQueue<StoreContext>("QueueBufferQuquq", messageWriteQueueThreshold, OnQueueMessageCompleted, _logger);
        }

        private void OnQueueMessageCompleted(StoreContext storeContext)
        {
            storeContext.OnComplete();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (remotingRequest.Body.Length > BrokerController.Instance.Setting.MessageMaxSize)
                throw new Exception($"消息长度({remotingRequest.Body.Length})超过最大({BrokerController.Instance.Setting.MessageMaxSize})限制");

            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }

            var request = BatchMessageUtils.DecodeSendMessageRequest(remotingRequest.Body);
            var messages = request.Messages;
            if(messages.Count()==0)
            {
                throw new ArgumentException("无效的批量发生消息，消息列表不能为空");
            }

            var topic = messages.First().Topic;
            var queueId = request.QueueId;
            var queue = _queueStore.GetQueue(topic, queueId);
            if(queue ==null)
            {
                throw new QueueNotExistException(topic, queueId);
            }
            _messageStore.BatchStoreMessageAsync(queue, request.Messages, (batchRecord, parameter) =>
            {
                var storeContext = parameter as StoreContext;
                if(batchRecord.Records.Any(x=>x.LogPosition<0|| string.IsNullOrEmpty(x.MessageId)))
                {
                    storeContext.Success = false;
                }
                else
                {
                    foreach(var record in batchRecord.Records)
                    {
                        storeContext.Queue.AddMessage(record.LogPosition, record.Tag);
                    }
                    storeContext.BatchMessageLogRecord = batchRecord;
                    storeContext.Success = true;
                }
                _bufferQueue.EnqueueMessage(storeContext);
            },new StoreContext
            {
                RemotingRequest=remotingRequest,
                RequestHandlerContext=context,
                Queue=queue,
                BatchSendMessageRequestHandler=this
            },request.ProducerAddress);
            foreach(var message in request.Messages)
            {
                _tpsStatisticService.AddTopicSendCount(topic, queueId);
            }
            return null;
        }

        class StoreContext
        {
            public IRequestHandlerContext RequestHandlerContext;
            public RemotingRequest RemotingRequest;
            public Queue Queue;
            public BatchMessageLogRecord BatchMessageLogRecord;
            public BatchSendMessageRequestHandler BatchSendMessageRequestHandler;
            public bool Success;

            public void OnComplete()
            {
                if (Success)
                {
                    var recordCount = BatchMessageLogRecord.Records.Count();
                    var messageResult = new List<BatchMessageItemResult>();
                    foreach(var record in BatchMessageLogRecord.Records)
                    {
                        messageResult.Add(new BatchMessageItemResult(record.MessageId, record.Code, record.QueueOffset, record.Tag, record.CreatedTime, record.StoredTime));
                    }
                    var result = new BatchMessageStoreResult(Queue.Topic, Queue.QueueId, messageResult);
                    var data = BatchMessageUtils.EncodeMessageStoreResult(result);
                    var response = RemotingResponseFactory.CreateResponse(RemotingRequest, data);

                    RequestHandlerContext.SendRemotingResponse(response);

                    if(BatchSendMessageRequestHandler._notifyWhenMessageArrived && recordCount > 0)
                    {
                        BatchSendMessageRequestHandler
                            ._suspendedPullManager
                            .NotifyNewMessage(Queue.Topic, Queue.QueueId, BatchMessageLogRecord.Records.First().QueueOffset);
                    }
                    if (recordCount > 0)
                    {
                        var lastRecord = BatchMessageLogRecord.Records.Last();
                        BrokerController.Instance.AddLatestMessage(lastRecord.MessageId, lastRecord.CreatedTime, lastRecord.StoredTime);
                    }
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
