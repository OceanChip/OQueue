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
    public class DeleteQueueForClusterRequestHandler : AbstractRequestHandler<DeleteQueueForClusterRequest>
    {
        public DeleteQueueForClusterRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, DeleteQueueForClusterRequest request)
        {
            var requestService = new BrokerRequestService(_nameServerController);

            requestService.ExecuteActionToAllClusterBrokers(request.ClusterName, remotingClient =>
            {
                var requestData = _binarySerializer.Serialize(new DeleteQueueRequest(request.Topic, request.QueueId));
                var remotingResponse = remotingClient.InvokeSync(new RemotingRequest((int)BrokerRequestCode.DeleteQueue, requestData), 30000);
                if (remotingResponse.ResponseCode != ResponseCode.Success)
                {
                    throw new Exception(string.Format("DeleteQueue failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.ResponseBody)));
                }
            });
            return EmptyBytes;
        }
    }
}
