using OceanChip.Queue.Protocols;
using System;
using System.Collections.Generic;

namespace OceanChip.Queue.Broker
{
    public interface IMessageStore
    {
        int MaxChunkNum { get; }
        long MinMessagePosition { get; }
        long CurrentMessagePosition { get; }
        int ChunkCount { get; }
        int MinChunkNum { get; }

        void Load();
        void Start();
        void Shutdown();
        void StoreMessageAsync(IQueue queue, Message message, Action<MessageLogRecord, object> callback, object parameter,string producerAddress);
        void BatchStoreMessageAsync(IQueue queue, IEnumerable<Message> messages, Action<BatchMessageLogRecord, object> callback, object parameter, string producerAddress);
        byte[] GetMessageBuffer(long position);
        QueueMessage GetMessage(long position);
        bool IsMessagePositionExists(long position);
        void UpdateMinConsumedMessagePosition(long minConsumedMessagePosition);
    }
}