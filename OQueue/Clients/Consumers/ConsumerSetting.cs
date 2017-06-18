using OceanChip.Common.Socketing;
using OceanChip.Queue.Protocols;
using System.Collections.Generic;
using System.Net;

namespace OceanChip.Queue.Clients.Consumers
{
    public class ConsumerSetting
    {
        public string ClusterName { get; set; }
        public IEnumerable<IPEndPoint> NameServerList { get; set; }
        public SocketSetting SocketSetting { get; set; }
        public bool AutoPull { get; set; }
        public bool CommitConsumeOffsetAsync { get; set; }
        public int ManualPullLocalMessageQueueMaxSize { get; set; }
        /// <summary>
        /// 消费者负载均衡的间隔,默认1s
        /// </summary>
        public int RebalanceInterval { get; set; }
        public int RefreshBrokerAndTopicRouteInfoInterval { get; set; }
        public int HeartbeatBrokerInterval { get; set; }
        public int CommitConsumerOffsetInterval { get; set; }
        public int PullMessageFlowControlThreshold { get; set; }
        /// <summary>
        /// 当拉取消息开始流控时，需要逐渐增加流控时间的步长百分比，默认为1%；
        /// <remarks>
        /// 假设当前本地拉取且并未消费的消息数超过阀值时，需要逐渐增加流控时间；具体增加多少时间取决于
        /// PullMessageFlowControlStepPercent以及PullMessageFlowControlStepWaitMilliseconds属性的配置值；
        /// 举个例子，假设流控阀值为1000，步长百分比为1%，每个步长等待时间为1ms；
        /// 然后，假如当前拉取到本地未消费的消息数为1200，
        /// 则超出阀值的消息数是：1200 - 1000 = 200，
        /// 步长为：1000 * 1% = 10；
        /// 然后，200 / 10 = 20，即当前超出的消息数是步长的20倍；
        /// 所以，最后需要等待的时间为20 * 1ms = 20ms;
        /// </remarks>
        /// </summary>
        public int PullMessageFlowControlStepPercent { get; set; }
        public int PullMessageFlowControlStepWaitMilliseconds { get; set; }
        public int SuspendPullRequestMilliseconds { get; set; }
        public int PullRequestTimeoutMilliseconds { get; set; }
        public int RetryMessageInterval{get;set;}
        public int PullMessageBatchSize { get; set; }
        public ConsumeFromWhere ConsumeFromWhere { get; set; }
        public MessageHandleMode MessageHandleMode { get; set; }

        public ConsumerSetting()
        {
            ClusterName = "DefaultCluster";
            NameServerList = new List<IPEndPoint>()
            {
                new IPEndPoint(SocketUtils.GetLocalIPV4(),9593)
            };
            SocketSetting = new SocketSetting();
            RebalanceInterval = 1000;
            HeartbeatBrokerInterval = 1000;
            RefreshBrokerAndTopicRouteInfoInterval = 5000;
            CommitConsumerOffsetInterval = 1000;
            PullMessageFlowControlThreshold = 10000;
            PullMessageFlowControlStepPercent = 1;
            PullMessageFlowControlStepWaitMilliseconds = 1;
            SuspendPullRequestMilliseconds = 1000 * 60;
            PullRequestTimeoutMilliseconds = 1000 * 70;
            RetryMessageInterval = 1000;
            PullMessageBatchSize = 64;
            ConsumeFromWhere = ConsumeFromWhere.LastOffset;
            MessageHandleMode = MessageHandleMode.Parallel;
            AutoPull = true;
            CommitConsumeOffsetAsync = true;
            ManualPullLocalMessageQueueMaxSize = 10000 * 10;
        }

    }
}