using OceanChip.Queue.Broker.Client;
using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public class DeleteConsumerGroupRequestHandler : AbstractRequestHanlder<DeleteConsumerGroupRequest, IConsumeOffsetStore, ConsumerManager>
    {
        protected override byte[] GetResponseData(IRequestHandlerContext context, DeleteConsumerGroupRequest request)
        {
            if (string.IsNullOrEmpty(request.GroupName))
            {
                throw new ArgumentException("DeleteConsumerGroupRequest.GroupName不能为空");
            }

            var group = _service2.GetConsumerGroup(request.GroupName);
            if(group !=null && group.GetConsumerCount() > 0)
            {
                throw new Exception("消费者组存在消费者，当不允许删除");
            }
            var success = _service1.DeleteConsumerGroup(request.GroupName);
            return Encoding.UTF32.GetBytes(success?"1":"0");
        }
    }
}
