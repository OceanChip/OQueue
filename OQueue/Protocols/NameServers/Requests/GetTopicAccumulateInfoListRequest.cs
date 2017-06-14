using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.NameServers.Requests
{

    [Serializable]
    public class GetTopicAccumulateInfoListRequest
    {
        public long AccumulateThreshold { get; set; }
    }
}
