using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Broker.Client;
using OceanChip.Common.Components;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class GetProducerListRequestHandler : AbstractRequestHanlder
    {
        private ProducerManager _producerManager;
        public GetProducerListRequestHandler()
        {
            _producerManager = ObjectContainer.Resolve<ProducerManager>();
        }
        protected override byte[] GetResponseData(IRequestHandlerContext context)
        {
            var productIdList = _producerManager.GetAllProducers();
            return Encoding.UTF8.GetBytes(string.Join(",", productIdList));
        }
    }
}
