using OceanChip.Queue.Protocols.Brokers;
using System.Collections.Generic;

namespace OceanChip.Queue.Broker
{
    public interface IConsumeOffsetStore
    {
        void Start();
        void Shutdown();

        int GetConsumerGroupCount();
        IEnumerable<string> GetAllConsumeGroupNames();
        bool DeleteConsumerGroup(string group);
        long GetConsumeOffset(string topic, int queueId, string group);
        long GetMinConsumeOffset(string topic, int queueId);
        void UpdateConsumeOffset(string topic, int queueId, string group, long offset);
        void DeleteConsumeOffset(QueueKey key);
        void SetConsumeNextOffset(string topic, int queueId, string group, long nextOffset);
        bool TryFetchNextConsumeOffset(string topic, int queueId, string group, out long nextOffset);
        IEnumerable<QueueKey> GetConsumeKeys();
        IEnumerable<TopicConsumeInfo> GetAllTopicConsumeInfoList();
        IEnumerable<TopicConsumeInfo> GetTopicConsumeInfoList(string groupName, string topic);
    }
}