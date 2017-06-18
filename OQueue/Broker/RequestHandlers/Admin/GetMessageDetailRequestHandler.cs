using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Utils;
using OceanChip.Queue.Protocols;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class GetMessageDetailRequestHandler : AbstractRequestHanlder<GetMessageDetailRequest, IMessageStore>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, GetMessageDetailRequest request)
        {
            var msgIdInfo = MessageIdUtil.ParseMessageId(request.MessageId);
            var message = _service.GetMessage(msgIdInfo.MessagePosition);
            var messages = new List<QueueMessage>();
            if (message != null)
                messages.Add(message);
            return _binarySerializer.Serialize(messages);
        }
    }
}
