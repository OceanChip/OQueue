using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers
{
    public enum BrokerRole
    {
        /// <summary>
        /// 主
        /// </summary>
        Master=0,
        /// <summary>
        /// 备
        /// </summary>
        Slave=1
    }
}
