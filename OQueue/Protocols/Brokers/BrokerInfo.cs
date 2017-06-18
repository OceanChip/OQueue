using OceanChip.Common.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers
{
    [Serializable]
    public class BrokerInfo
    {
        /// <summary>
        /// 名字，默认为DefaultBroker
        /// </summary>
        public string BrokerName { get; set; }
        /// <summary>
        /// 分组名，当实现主备时，MasterBroker和它的所有的SlaveBroker的分组名相同；
        /// 不同MasterBroker的分组名要求不同，默认为DefaultGroup
        /// </summary>
        public string GroupName { get; set; }
        /// <summary>
        /// 集群名，一个集群下可以有多个MasterBroker,每个MasterBroker可以有多个SlaverBroker
        /// 默认集群名为DefaultCluster
        /// </summary>
        public string ClusterName { get; set; }
        /// <summary>
        /// Broker的角色，目前有Master，Slave两种角色；默认为Master
        /// </summary>
        public int BrokerRole { get; set; }
        /// <summary>
        /// 供Producer连接的地址；默认IP为本地IP，默认端口5000，格式为IP：port
        /// </summary>
        public string ProducerAddress { get; set; }
        /// <summary>
        /// 供Consumer连接的地址；默认IP为本地IP，默认端口5001，格式为IP：port
        /// </summary>
        public string ConsumerAddress { get; set; }
        /// <summary>
        /// Producer,Consumer对Broker发送的发消息和拉消息除外的其他的内部请求，以及后台管理控制台发送的查询请求使用的地址；
        /// 默认Ip为本地IP，默认端口为5002，格式为IP：Port
        /// </summary>
        public string AdminAddress { get; set; }
        public BrokerInfo() { }
        public BrokerInfo(string name,string groupName,string clusterName,BrokerRole role,string producerAddress,string consumerAddress,string adminAddress)
        {
            this.BrokerName = name;
            this.GroupName = groupName;
            this.ClusterName = clusterName;
            this.BrokerRole = (int)role;
            this.ProducerAddress = producerAddress;
            this.ConsumerAddress = consumerAddress;
            this.AdminAddress = adminAddress;
        }
        public void Valid()
        {
            Check.NotNull(BrokerName, nameof(BrokerName));
            Check.NotNull(GroupName, nameof(GroupName));
            Check.NotNull(ClusterName, nameof(ClusterName));
            Check.NotNull(ProducerAddress, nameof(ProducerAddress));
            Check.NotNull(ConsumerAddress, nameof(ConsumerAddress));
            Check.NotNull(AdminAddress, nameof(AdminAddress));
            if(BrokerRole != (int)Brokers.BrokerRole.Master && BrokerRole != (int)Brokers.BrokerRole.Slave)
            {
                throw new ArgumentException("无效Broker权限：" + BrokerRole);
            }
        }
        public bool IsEqualsWith(BrokerInfo other)
        {
            if (other == null)
                return false;
            return BrokerName == other.BrokerName
                && GroupName == other.GroupName
                && ClusterName == other.ClusterName
                && BrokerRole == other.BrokerRole
                && ProducerAddress == other.ProducerAddress
                && ConsumerAddress == other.ConsumerAddress
                && AdminAddress == other.AdminAddress;
        }
        public override string ToString()
        {
            return $"[BrokerName={BrokerName},GroupName={GroupName},ClusterName={ClusterName},BrokerRole={BrokerRole}," +
                $"ProducerAddress={ProducerAddress},ConsumerAddress={ConsumerAddress},AdminAddress={AdminAddress}]"; ;
        }
    }
}
