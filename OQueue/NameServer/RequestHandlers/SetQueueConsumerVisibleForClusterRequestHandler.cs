using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.Brokers.Requests;
using OceanChip.Queue.Protocols.Brokers;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Protocols.NameServers.Requests;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class SetQueueConsumerVisibleForClusterRequestHandler : AbstractRequestHandler<SetQueueConsumerVisibleForClusterRequest>
    {
        public SetQueueConsumerVisibleForClusterRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, SetQueueConsumerVisibleForClusterRequest request)
        {
            var requestService = new BrokerRequestService(_nameServerController);
            requestService.ExecuteActionToAllClusterBrokers(request.ClusterName, remotingClient =>
            {
                var requestData = _binarySerializer.Serialize(new SetQueueConsumerVisibleRequest(request.Topic, request.QueueId, request.Visible));
                var remotingResponse = remotingClient.InvokeSync(new RemotingRequest((int)BrokerRequestCode.SetQueueConsumerVisible, requestData), 30000);
                if (remotingResponse.ResponseCode != ResponseCode.Success)
                {
                    throw new Exception(string.Format("SetQueueConsumerVisible 失败, 失败原因: {0}", Encoding.UTF8.GetString(remotingResponse.ResponseBody)));
                }
            });
            return EmptyBytes;
        }
    }
}
