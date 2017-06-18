using OceanChip.Common.Components;
using OceanChip.Common.Extensions;
using OceanChip.Common.Logging;
using OceanChip.Common.Remoting;
using OceanChip.Common.Scheduling;
using OceanChip.Common.Serializing;
using OceanChip.Common.Utilities;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Protocols.Brokers;
using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Concurrent;

namespace OceanChip.Queue.Clients.Consumers
{
    public class CommitConsumeOffsetService
    {
        private readonly string _clientId;
        private readonly  Consumer _consumer;
        private readonly ClientService _clientService;
        private readonly IBinarySerializer _binarySerializer;
        private readonly IScheduleService _scheduleService;
        private readonly ConcurrentDictionary<string, ConsumeOffsetInfo> _consumeOffsetDict;
        private readonly ILogger _logger;

        public CommitConsumeOffsetService(Consumer consumer, ClientService clientService)
        {
            this._consumer = consumer;
            _clientService = clientService;
            _consumeOffsetDict = new ConcurrentDictionary<string, ConsumeOffsetInfo>();
            _clientId = clientService.GetClientId();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

        }

        public void Start()
        {
            if (_consumer.Setting.CommitConsumeOffsetAsync)
            {
                _scheduleService.StartTask("CommitOffsets", CommitOffsets, 1000, _consumer.Setting.CommitConsumerOffsetInterval);
            }
            _logger.InfoFormat("{0} startted.", GetType().Name);
        }


        public void Stop()
        {
            if (_consumer.Setting.CommitConsumeOffsetAsync)
            {
                _scheduleService.StopTask("CommitOffsets");
            }
            _logger.InfoFormat("{0} stopped.", GetType().Name);
        }

        public void CommitConsumeOffset(string brokerName, string topic, int queueId, long consumeOffset)
        {
            Check.NotNullOrEmpty(brokerName, nameof(brokerName));
            Check.NotNullOrEmpty(topic, nameof(topic));
            Check.Nonnegative(queueId, nameof(queueId));
            Check.Nonnegative(consumeOffset, nameof(consumeOffset));

            if (_consumer.Setting.CommitConsumeOffsetAsync)
            {
                var key = $"{brokerName}_{topic}_{queueId}";
                _consumeOffsetDict.AddOrUpdate(key, x =>
                {
                    return new ConsumeOffsetInfo { MessageQueue = new MessageQueue(brokerName, topic, queueId), ConsumeOffset = consumeOffset };
                }, (x, y) =>
                 {
                     y.ConsumeOffset = consumeOffset;
                     return y;
                 });
            }
            else
            {
                CommitConsumeOffset(new MessageQueue(brokerName, topic, queueId), consumeOffset,true);
            }
        }
        public void CommitConsumeOffset(PullRequest request)
        {
            var consumeOffset = request.ProcessQueue.GetConsumedQueueOffset();
            if (consumeOffset >= 0)
            {
                if (!request.ProcessQueue.TryUpdatePreviousConsumeQueueOffset(consumeOffset))
                {
                    return;
                }
                CommitConsumeOffset(request.MessageQueue, consumeOffset);
            }
        }
        public void CommitConsumeOffset(MessageQueue messageQueue,long consumeOffset,bool throwIfException = false)
        {
            Check.NotNull(messageQueue, nameof(messageQueue));
            Check.Nonnegative(consumeOffset, nameof(consumeOffset));

            var brokerConnection = _clientService.GetBrokerConnection(messageQueue.BrokerName);
            if (brokerConnection == null)
            {
                _logger.ErrorFormat("CommitConsumeOffset failed as the target broker connection not found,messageQueue:{0}", messageQueue);
                return;
            }
            var remotingClient = brokerConnection.AdminRemotingClient;

            var request = new UpdateQueueOffsetRequest(_consumer.GroupName, messageQueue, consumeOffset);
            var remotingRequest = new RemotingRequest((int)BrokerRequestCode.UpdateQueueConsumeOffsetRequest, _binarySerializer.Serialize(request));
            var brokerAddress = remotingClient.ServerEndPoint.ToAddress();

            try
            {
                remotingClient.InvokeOnway(remotingRequest);
                if (_logger.IsDebugEnabled)
                {
                    _logger.DebugFormat("CommitConsumeOffset success,consumerGroup:{0},consumerId:{1},messageQueue:{2},consumeOffset:{3},brokerAddress:{4}",
                        _consumer.GroupName,
                        _clientId,
                        messageQueue,
                        consumeOffset,
                        brokerAddress);
                }
            }catch(Exception ex)
            {
                if (remotingClient.IsConnected)
                {
                    _logger.Error(string.Format("CommitConsumeOffset success,consumerGroup:{0},consumerId:{1},messageQueue:{2},consumeOffset:{3},brokerAddress:{4}",
                        _consumer.GroupName,
                        _clientId,
                        messageQueue,
                        consumeOffset,
                        brokerAddress),ex);
                }
                if (throwIfException)
                    throw;
            }
        }
        private void CommitOffsets()
        {
            foreach(var offset in _consumeOffsetDict.Values)
            {
                CommitConsumeOffset(offset.MessageQueue, offset.ConsumeOffset);
            }
        }
        private class ConsumeOffsetInfo
        {
            public MessageQueue MessageQueue;
            public long ConsumeOffset;
        }
    }
}