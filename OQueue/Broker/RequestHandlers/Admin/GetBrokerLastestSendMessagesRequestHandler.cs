using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class GetBrokerLastestSendMessagesRequestHandler : AbstractRequestHanlder
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context)
        {
            var lastestSendMessageids = BrokerController.Instance.GetLastestSendMessageIds();
            return Encoding.UTF8.GetBytes(lastestSendMessageids);
        }
    }
}
