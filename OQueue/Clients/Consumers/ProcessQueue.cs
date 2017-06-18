using OceanChip.Common.Extensions;
using System.Collections.Generic;
using System.Linq;

namespace OceanChip.Queue.Clients.Consumers
{
    public class ProcessQueue
    {
        private readonly object _lockObj = new object();
        private readonly SortedDictionary<long, ConsumingMessage> _messageDict = new SortedDictionary<long, ConsumingMessage>();
        private long _consumedQueueOffset = -1L;
        private long _previousConsumeQueueOffset = -1L;
        private long _maxQueueOffset = -1L;
        private int _messageCount = 0;

        public void Reset()
        {
            lock (_lockObj)
            {
                _messageDict.Clear();
                _consumedQueueOffset = -1;
                _previousConsumeQueueOffset = -1;
                _maxQueueOffset = -1;
                _messageCount = 0;
            }
        }
        public bool TryUpdatePreviousConsumeQueueOffset(long current)
        {
            if(current !=_previousConsumeQueueOffset)
            {
                _previousConsumeQueueOffset = current;
                return true;
            }
            return false;
        }
        public void MarkAllConsumeingMessageIgnored()
        {
            lock (_lockObj)
            {
                var existingConsumingMessages = _messageDict.Values.ToList();
                foreach(var consumingMessage in existingConsumingMessages)
                {
                    consumingMessage.IsIgnored = true;
                }
            }
        }
        public void AddMessages(IEnumerable<ConsumingMessage> messages)
        {
            lock (_lockObj)
            {
                foreach(var msg in messages)
                {
                    if (_messageDict.ContainsKey(msg.Message.QueueOffset))
                        continue;
                    _messageDict[msg.Message.QueueOffset] = msg;
                    if(_maxQueueOffset==-1 && msg.Message.QueueOffset >= 0)
                    {
                        _maxQueueOffset = msg.Message.QueueOffset;
                    }else if (msg.Message.QueueOffset > _maxQueueOffset)
                    {
                        _maxQueueOffset = msg.Message.QueueOffset;
                    }
                    _messageCount++;
                }
            }
        }
        public void RemoveMessage(ConsumingMessage message)
        {
            lock (_lockObj)
            {
                if (_messageDict.Remove(message.Message.QueueOffset))
                {
                    if (_messageDict.Keys.IsNotEmpty())
                    {
                        _consumedQueueOffset = _messageDict.Keys.First() - 1;
                    }
                    else
                    {
                        _consumedQueueOffset = _maxQueueOffset; ;
                    }
                    _messageCount--;
                }
            }
        }
        public int GetMessageCount()
        {
            return _messageCount;
        }
        public long GetConsumedQueueOffset()
        {
            return _consumedQueueOffset;
        }
    }
}