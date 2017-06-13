using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols
{
    public enum PullStatus:short
    {
        /// <summary>
        /// 发现
        /// </summary>
        Found=1,
        /// <summary>
        /// 无新消息
        /// </summary>
        NoNewMessage=2,
        NextOffsetReset=3,
        Ignored=4,
        QueueNotExist=5,
        BrokerIsCleaning=6,
    }
}
