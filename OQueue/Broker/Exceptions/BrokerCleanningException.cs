using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.Exceptions
{
    public class BrokerCleanningException:Exception
    {
        public BrokerCleanningException() : base($"Broker正在清除") { }
    }
}
