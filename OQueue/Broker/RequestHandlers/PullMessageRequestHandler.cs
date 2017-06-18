using OceanChip.Common.Components;
using OceanChip.Common.Logging;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Broker.Client;
using OceanChip.Queue.Broker.LongPolling;
using OceanChip.Queue.Protocols;
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Queue.Protocols.Brokers.Requests;
using OceanChip.Queue.Utils;
using OceanChip.Common.Storage.Exceptions;
using OceanChip.Common.Extensions;

namespace OceanChip.Queue.Broker.RequestHandlers
{
    public class PullMessageRequestHandler : IRequestHandler
    {
        private ConsumerManager _consumerManager;
        private SuspendedPullRequestManager _suspendedPullRequestManager;
        private IMessageStore _messageStore;
        private IQueueStore _queueStore;
        private IConsumeOffsetStore _offsetStore;
        private ILogger _logger;
        public PullMessageRequestHandler()
        {
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

        }
        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                return BuildBrokerIsCleaningResponse(remotingRequest);
            }

            var request = DeserializePullMessageRequest(remotingRequest.Body);
            var topic = request.MessageQueue.Topic;
            var queueId = request.MessageQueue.QueueId;
            var tags = request.Tags;
            var pullOffset = request.QueueOffset;

            //如果消费者第一次过来取消息，则计算下一个应该拉取的位置，并返回给消费者
            var nextConsumeOffset = 0L;
            if (pullOffset < 0)
            {
                nextConsumeOffset = GetNextConsumeOffset(topic, queueId, request.ConsumerGroup, request.ConsumeFromWhere);
                return BuildNextOffsetResetResponse(remotingRequest, nextConsumeOffset);
            }
            else if(_offsetStore.TryFetchNextConsumeOffset(topic,queueId,request.ConsumerGroup, out nextConsumeOffset)) 
            {
                    return BuildNextOffsetResetResponse(remotingRequest, nextConsumeOffset);
            }

            var result = PullMessages(topic, tags, queueId, pullOffset, request.PullMessageBatchSize);

            if (result.Status == PullStatus.Found)
                return BuildFoundResponse(remotingRequest, result.Messages);
            else if (result.Status == PullStatus.NextOffsetReset)
                return BuildNextOffsetResetResponse(remotingRequest, result.NextBeginOffset);
            else if (result.Status == PullStatus.QueueNotExist)
                return BuildQueueNotExistResponse(remotingRequest);
            else if(result.Status == PullStatus.NoNewMessage)
            {
                if (request.SuspendPullRequestMilliseconds > 0)
                {
                    var pullRequest = new PullRequest(
                        remotingRequest,
                        request,
                        context,
                        DateTime.Now,
                        request.SuspendPullRequestMilliseconds,
                        ExecutePullRequest,
                        ExecutePullRequest,
                        ExecutedNoNewMessagePullRequest,
                        ExecuteReplacePullRequest);
                    _suspendedPullRequestManager.SuspendPullRequest(pullRequest);
                    return null;
                }
                return BuildNoNewMessageResponse(remotingRequest);
            }
            else
            {
                throw new Exception("无效状态");
            }
        }
        

        private PullMessageResult PullMessages(string topic, string tags, int queueId, long pullOffset, int maxPullSize)
        {
            //找队列
            var queue = _queueStore.GetQueue(topic, queueId);
            if(queue == null)
            {
                return new PullMessageResult
                {
                    Status = PullStatus.QueueNotExist
                };
            }

            //尝试拉取消息
            var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topic, queueId);
            var messages = new List<byte[]>();
            var queueOffset = pullOffset;
            var messageChunkNotExistException = default(ChunkNotExistException);

            while(queueOffset<=queueCurrentOffset && messages.Count< maxPullSize)
            {
                long messagePosition = -1;
                int tagCode;
                //获取消息位置
                try
                {
                    messagePosition = queue.GetMessagePosition(queueOffset, out tagCode);
                }catch(ChunkNotExistException ex)
                {
                    _logger.Error($"Chunk队列不存在,topic:{topic},queueId:{queueId},queueOffset:{queueOffset}", ex);
                    break;
                }
                catch(ChunkReadException ex)
                {
                    _logger.Error($"Chunk队列读取错误,topic:{topic},queueId:{queueId},queueOffset:{queueOffset}", ex);
                    break;
                }

                if (messagePosition < 0)
                    break;

                try
                {
                    var message = _messageStore.GetMessageBuffer(messagePosition);
                    if (message == null)
                    {
                        break;
                    }
                    if (string.IsNullOrEmpty(tags) || tags == "*")
                    {
                        messages.Add(message);
                    }
                    else
                    {
                        var tagList = tags.Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
                        foreach(var tag in tagList)
                        {
                            if(tag=="*" || tag.GetStringHashcode() == tagCode)
                            {
                                messages.Add(message);
                                break;
                            }
                        }
                    }
                    queueOffset++;
                }catch(ChunkNotExistException ex)
                {
                    messageChunkNotExistException = ex;
                    break;
                }catch(ChunkReadException ex)
                {
                    //遇到这种异常，说明某个消息在队列（Queue chunk)中存在，但在message chunk中不存在；
                    //出现这种现象的原因是有与，默认情况下，message chunk,queue chunk都是异步持久化，默认定时100S刷盘一次；
                    //所以，当broker正好在被关闭的时刻，假如queue chunk刷盘成功了，而对应的消息在message chunk来不及，那就意味着这部分消息就丢失了；
                    //那当broker下次重启后，丢失的那些消息就找不到了，无法被消费；所以当consumer拉取这些消息时，就会抛这个异常；
                    //这种情况下，重试拉取已经没有意义，故我们能做的是记录错误日志，记录下来那个topic下的那个queue的那个位置找不到了；这样我们就知道哪些消息丢失了；
                    //然后我们继续拉取该队列的后续的消息。
                    //如果大家希望避免这种问题，如果你的业务场景消息量不大或者使用了SSD硬盘，则建议使用同步算盘的方式，这样确保消息不丢，
                    //大家通过配置ChunkManagerConfig.SyncFlush=true来实现；
                    _logger.Error(string.Format("消息读取失败,topic:{0},queueId:{1},queueOffset:{2}", topic, queueId, queueOffset), ex);
                    queueOffset++;
                }
            }
            if (messages.Count > 0)
            {
                return new PullMessageResult
                {
                    Status = PullStatus.Found,
                    Messages = messages
                };
            }

            var queueMinOffset = _queueStore.GetQueueMinOffset(topic, queueId);
            if(pullOffset < queueMinOffset)
            {
                _logger.InfoFormat("将下一个pulloffset重置为queueMinOffset,[pullOffset:{0},queueMinOffset:{1}",pullOffset,queueMinOffset);
                return new PullMessageResult
                {
                    Status = PullStatus.NextOffsetReset,
                    NextBeginOffset = queueMinOffset
                };
            }else if (pullOffset > queueCurrentOffset + 1)//太大
            {
                _logger.InfoFormat("将下一个pulloffset重置为queueCurrentOffset,[pullOffset:{0},queueCurrentOffset:{1}", pullOffset, queueCurrentOffset);
                return new PullMessageResult
                {
                    Status = PullStatus.NextOffsetReset,
                    NextBeginOffset = queueCurrentOffset+1
                };
            }else if(messageChunkNotExistException != null)
            {
                var nextPullOffset = CalculateNextPullOffset(queue, pullOffset, queueCurrentOffset);
                _logger.InfoFormat("将下一个pulloffset重置为CalculateNextPullOffset,[pullOffset:{0},CalculateNextPullOffset:{1}", pullOffset, nextPullOffset);
                return new PullMessageResult
                {
                    Status = PullStatus.NextOffsetReset,
                    NextBeginOffset = nextPullOffset
                };
            }
            else
            {

                //到这里，说明当前的pullOffset在队列的正确范围，即：>=queueMinOffset and <= queueCurrentOffset；
                //但如果还是没有拉取到消息，则可能的情况有：
                //1）要拉取的消息还未来得及刷盘；
                //2）要拉取的消息对应的Queue的Chunk文件被删除了；
                //不管是哪种情况，告诉Consumer没有新消息即可。如果是情况1，则也许下次PullRequest就会拉取到消息；如果是情况2，则下次PullRequest就会被判断出pullOffset过小
                return new PullMessageResult
                {
                    Status = PullStatus.NoNewMessage
                };
            }
        }
        private long CalculateNextPullOffset(Queue queue,long pullOffset,long queueCurrentOffset)
        {
            var queueOffset = pullOffset + 1;
            while (queueOffset <= queueCurrentOffset)
            {
                int tagCode;
                var messagePosition = queue.GetMessagePosition(queueOffset, out tagCode);
                if (_messageStore.IsMessagePositionExists(messagePosition))
                {
                    return queueOffset;
                }
                queueOffset++;
            }
            return queueOffset;
        }
        private void ExecutePullRequest(PullRequest request)
        {
            if (IsPullRequestValid(request))
                return;

            var pullMessageRequest = request.PullMessageRequest;
            var topic = pullMessageRequest.MessageQueue.Topic;
            var queueId = pullMessageRequest.MessageQueue.QueueId;
            var pullOffset = pullMessageRequest.QueueOffset;
            var pullResult = PullMessages(topic, pullMessageRequest.Tags, queueId, pullOffset, pullMessageRequest.PullMessageBatchSize);
            var remotingRequest = request.RemotingRequest;

            if(pullResult.Status == PullStatus.Found)
            {
                SendRemotingResponse(request, BuildFoundResponse(remotingRequest, pullResult.Messages));
            }else if(pullResult.Status == PullStatus.NextOffsetReset)
            {
                SendRemotingResponse(request, BuildNextOffsetResetResponse(remotingRequest, pullResult.NextBeginOffset));
            }else if(pullResult.Status == PullStatus.QueueNotExist)
            {
                SendRemotingResponse(request, BuildQueueNotExistResponse(remotingRequest));
            }else if(pullResult.Status == PullStatus.NoNewMessage)
            {
                SendRemotingResponse(request, BuildNoNewMessageResponse(remotingRequest));
            }
        }


        private void ExecuteReplacePullRequest(PullRequest request)
        {
            if (IsPullRequestValid(request))
                return;
            SendRemotingResponse(request, BuildIgnoredResponse(request.RemotingRequest));
        }
        private void ExecutedNoNewMessagePullRequest(PullRequest request)
        {
            if (IsPullRequestValid(request))
                return;
            SendRemotingResponse(request, BuildNoNewMessageResponse(request.RemotingRequest));
        }
        private bool IsPullRequestValid(PullRequest request)
        {
            try
            {
                return _consumerManager.IsConsumerActive(request.PullMessageRequest.ConsumerGroup, request.PullMessageRequest.ConsumerId);
            }catch(ObjectDisposedException)
            {
                return false;
            }
        }
        private RemotingResponse BuildNoNewMessageResponse(RemotingRequest remotingRequest)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.NextOffsetReset);
        }
        private RemotingResponse BuildIgnoredResponse(RemotingRequest remotingRequest)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.Ignored);
        }
        private RemotingResponse BuildNextOffsetResetResponse(RemotingRequest remotingRequest,long nextOffset)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.NextOffsetReset,BitConverter.GetBytes(nextOffset));
        }
        private RemotingResponse BuildQueueNotExistResponse(RemotingRequest remotingRequest)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.QueueNotExist);
        }
        private RemotingResponse BuildBrokerIsCleaningResponse(RemotingRequest remotingRequest)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.BrokerIsCleaning);
        }
        private RemotingResponse BuildFoundResponse(RemotingRequest remotingRequest,IEnumerable<byte[]> messages)
        {
            return RemotingResponseFactory.CreateResponse(remotingRequest, (short)PullStatus.Found, Combine(messages));
        }
        private void SendRemotingResponse(PullRequest pullRequest,RemotingResponse remotingResponse)
        {
            pullRequest.RequestHandlerContext.SendRemotingResponse(remotingResponse);
        }
        private long GetNextConsumeOffset(string topic,int queueId,string consumerGroup,ConsumeFromWhere consumeFromWhere)
        {
            var queueConsumeOffset = _offsetStore.GetConsumeOffset(topic, queueId, consumerGroup);
            if (queueConsumeOffset >= 0)
            {
                var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topic, queueId);
                return queueCurrentOffset < queueConsumeOffset ? queueCurrentOffset + 1 : queueConsumeOffset + 1;
            }
            if(consumeFromWhere == ConsumeFromWhere.FirstOffset)
            {
                var queueMinOffset = _queueStore.GetQueueMinOffset(topic, queueId);
                if (queueMinOffset < 0)
                    return 0;
                return queueMinOffset;
            }
            else
            {
                var queueCurrentOffset = _queueStore.GetQueueCurrentOffset(topic,queueId);
                if (queueCurrentOffset < 0)
                    return 0;
                return queueCurrentOffset + 1;
            }
        }
        private static PullMessageRequest DeserializePullMessageRequest(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                return PullMessageRequest.ReadFromStream(stream);
            }
        }
        private static byte[] Combine(IEnumerable<byte[]> arrays)
        {
            byte[] destination = new byte[arrays.Sum(x => x.Length)];
            int offset = 0;
            foreach(byte[] data in arrays)
            {
                Buffer.BlockCopy(data, 0, destination, offset, data.Length);
                offset += data.Length;
            }
            return destination;
        }
        class PullMessageResult
        {
            public PullStatus Status { get; set; }
            public long NextBeginOffset { get; set; }
            public IEnumerable<byte[]> Messages { get; set; }
        }
    }
}
