using OceanChip.Common.Socketing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.Client
{
    public class ClientHeartbeatInfo
    {
        public ITcpConnection Connection { get; private set; }
        public DateTime LastHeartbeartTime { get; set; }
        public ClientHeartbeatInfo(ITcpConnection connection)
        {
            this.Connection = connection;
        }
        public bool IsTimeout(double timeoutMilliseconds)
        {
            return (DateTime.Now - LastHeartbeartTime).TotalMilliseconds>timeoutMilliseconds;
        }
    }
}
