using OceanChip.Common.Components;
using OceanChip.Common.Remoting;
using OceanChip.Common.Serializing;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Protocols.Brokers;
using OceanChip.Queue.Protocols.Brokers.Requests;
using OceanChip.Queue.Protocols.Brokers.Requests;
using OceanChip.Queue.Protocols.NameServers.Requests;
using OceanChip.Queue.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class AddQueueForClusterRequestHandler : AbstractRequestHandler<AddQueueForClusterRequest>
    {
        public AddQueueForClusterRequestHandler(NameServerController nameServerController)
            :base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context,AddQueueForClusterRequest request)
        {
            var requestService = new BrokerRequestService(this._nameServerController);
            requestService.ExecuteActionToAllClusterBrokers(request.ClusterName, remotingClient =>
            {
                var requestData = _binarySerializer.Serialize(new AddQueueRequest(request.Topic));
                var remotingResponse = remotingClient.InvokeSync(new RemotingRequest((int)BrokerRequestCode.AddQueue, requestData), 30000);
                if (remotingResponse.ResponseCode != ResponseCode.Success)
                {
                    throw new Exception($"AddQueue失败，错误信息：{Encoding.UTF8.GetString(remotingResponse.ResponseBody)}");
                }
            });
            return EmptyBytes;
        }
    }
}
