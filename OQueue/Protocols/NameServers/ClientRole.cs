using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers
{
    public enum ClientRole
    {
        /// <summary>
        /// 生产者
        /// </summary>
        Producer=0,
        /// <summary>
        /// 消费者
        /// </summary>
        Consumer=1
    }
}
