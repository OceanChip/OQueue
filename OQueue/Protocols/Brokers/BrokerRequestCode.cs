using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers
{
    public enum BrokerRequestCode
    {
        /// <summary>
        /// 发生消息
        /// </summary>
        SendMessage=10,
        /// <summary>
        /// 拉消息
        /// </summary>
        PullMessage=11,
        /// <summary>
        /// 批量发消息
        /// </summary>
        BatchSendMessage=12,
        /// <summary>
        /// Producer心跳
        /// </summary>
        ProductHeartbeat = 100,
        /// <summary>
        /// Consumer心跳
        /// </summary>
        ConsumerHeartbeat=101,
        /// <summary>
        /// 通过主题获取消费者Id集合
        /// </summary>
        GetConsumerIdsForTopic=102,
        /// <summary>
        /// 更新消费者队列偏移请求
        /// </summary>
        UpdateQueueConsumeOffsetRequest=103,
        GetBrokerStatisticInfo=1000,
        GetTopicQueueInfo=1001,
        GetTopicConsumeInfo=1002,
        GetProducerList=1003,
        GetConsumerList=1004,
        CreateTopic=1005,
        DeleteTopic=1006,
        AddQueue=1007,
        DeleteQueue=1008,
        SetQueueProducerVisible=1009,
        SetQueueConsumerVisible=1010,
        SetQueueNextConsumeOffset=1011,
        DeleteConsumerGroup=1012,
        GetMessageDetail=1013,
        GetLastestMessages=1014,
    }
}
