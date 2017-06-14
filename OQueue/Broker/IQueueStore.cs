using OceanChip.Queue.Protocols.Brokers;
using System.Collections.Generic;

namespace OceanChip.Queue.Broker
{
    public interface IQueueStore
    {
        void Load();
        void Start();
        void Shutdown();
        /// <summary>
        /// 获取所有主题
        /// </summary>
        /// <returns></returns>
        IEnumerable<string> GetAllTopics();
        /// <summary>
        /// 获取队列
        /// </summary>
        /// <param name="topic">主题名称</param>
        /// <param name="queueId">队列编号</param>
        /// <returns></returns>
        Queue GetQueue(string topic, int queueId);
        /// <summary>
        /// 获取队列个数
        /// </summary>
        /// <returns></returns>
        int GetAllQueueCount();
        IEnumerable<Queue> GetAllQueues();
        /// <summary>
        /// 获取队列主题列表
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        IList<TopicQueueInfo> GetTopicQueueInfoList(string topic = null);
        /// <summary>
        /// 获取最小消费消息位置
        /// </summary>
        /// <returns></returns>
        long GetMinConsumeMessagePosition();
        /// <summary>
        /// 获取未处理消费消息数量
        /// </summary>
        /// <returns></returns>
        long GetTotalUnConsumeMessageCount();
        /// <summary>
        /// 判断主题是否存在
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        bool IsTopicExists(string topic);
        /// <summary>
        /// 判断队列是否存在
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        bool IsQueueExists(QueueKey key);
        /// <summary>
        /// 判断队列是否存在
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="queueId"></param>
        /// <returns></returns>
        bool IsQueueExists(string topic, int queueId);
        /// <summary>
        /// 获取队列当前位置
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="queueId"></param>
        /// <returns></returns>
        long GetQueueCurrentOffset(string topic, int queueId);
        /// <summary>
        /// 获取队列最小位置
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="queueId"></param>
        /// <returns></returns>
        long GetQueueMinOffset(string topic, int queueId);
        /// <summary>
        /// 添加队列
        /// </summary>
        /// <param name="topic"></param>
        void AddQueue(string topic);
        /// <summary>
        /// 删除队列
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="queueId"></param>
        void DeleteQueue(string topic, int queueId);
        /// <summary>
        /// 设置生产者是否可见
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="queueId"></param>
        /// <param name="visible"></param>
        void SetProducerVisibile(string topic, int queueId, bool visible);
        /// <summary>
        /// 设置消费者是否可见
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="queueId"></param>
        /// <param name="visible"></param>
        void SetConsumerVisibile(string topic, int queueId, bool visible);
        /// <summary>
        /// 创建主题
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="initialQueueCount"></param>
        /// <returns></returns>
        IEnumerable<int> CreateTopic(string topic, int? initialQueueCount);
        /// <summary>
        /// 删除主题
        /// </summary>
        /// <param name="topic"></param>
        void DeleteTopic(string topic);
        /// <summary>
        /// 获取队列
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="autoCreate"></param>
        /// <returns></returns>
        IEnumerable<Queue> GetQueues(string topic, bool autoCreate = false);
    }
}