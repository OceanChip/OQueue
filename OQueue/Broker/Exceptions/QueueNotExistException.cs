using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.Exceptions
{
   public class QueueNotExistException:Exception
    {
        public QueueNotExistException(string topic,int queueId) :
            base($"队列不存在。Topic:{topic},QueueId:{queueId}")
        { }
    }
}
