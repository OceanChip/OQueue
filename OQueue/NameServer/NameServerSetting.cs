using OceanChip.Common.Socketing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.NameServer
{
    public class NameServerSetting
    {
        /// <summary>
        /// 服务地址，默认端口号9593
        /// </summary>
        public IPEndPoint BindingAddress { get; set; }
        /// <summary>
        /// Broker不活跃最大允许时间，如果一个Broker超过此时间未发送心跳，则认为此Broker挂掉了，默认超时时间为10s
        /// </summary>
        public int BrokerInactiveMaxMilliseconds { get; set; }
        /// <summary>
        /// 是否自动创建主题，默认为True，线上环境建议设置为false,Topic应该总是有后台管理控制台来创建
        /// </summary>
        public bool AutoCreateTopic { get; set; }
        /// <summary>
        /// TCP通信层设置
        /// </summary>
        public SocketSetting SocketSetting { get; set; }

        public NameServerSetting(int port = 9593)
        {
            BindingAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), port);
            BrokerInactiveMaxMilliseconds = 10 * 1000;
            AutoCreateTopic = true;
        }
    }
}
