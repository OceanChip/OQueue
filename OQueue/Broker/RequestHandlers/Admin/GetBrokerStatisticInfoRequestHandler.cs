using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Common.Serializing;
using OceanChip.Common.Components;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class GetBrokerStatisticInfoRequestHandler : AbstractRequestHanlder
    {
        private IBinarySerializer _binarySerializer;

        public GetBrokerStatisticInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }
        protected override byte[] GetResponseData(IRequestHandlerContext context)
        {
            var statistic = BrokerController.Instance.GetBrokerStatisticInfo();
            return _binarySerializer.Serialize(statistic);
        }
    }
}
