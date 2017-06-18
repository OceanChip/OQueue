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
    public class DeleteTopicForClusterRequestHandler : AbstractRequestHandler<DeleteTopicForClusterRequest>
    {
        public DeleteTopicForClusterRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {

        }

        protected override byte[] Process(IRequestHandlerContext context, DeleteTopicForClusterRequest request)
        {
            var requestService = new BrokerRequestService(_nameServerController);
            requestService.ExecuteActionToAllClusterBrokers(request.ClusterName, remotingClient =>
            {
                var requestData = _binarySerializer.Serialize(new DeleteTopicRequest(request.Topic));
                var remotingResponse = remotingClient.InvokeSync(new RemotingRequest((int)BrokerRequestCode.DeleteTopic, requestData), 30000);
                if (remotingResponse.ResponseCode != ResponseCode.Success)
                {
                    throw new Exception(string.Format("删除主题失败,errorMessage:{0}", Encoding.UTF8.GetString(remotingResponse.ResponseBody)));
                }
            });
            return EmptyBytes;
        }
    }
}
