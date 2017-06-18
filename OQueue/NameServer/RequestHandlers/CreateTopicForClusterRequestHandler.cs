using OceanChip.Queue.Protocols.NameServers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols.Brokers.Requests;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Protocols.Brokers;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class CreateTopicForClusterRequestHandler:AbstractRequestHandler<CreateTopicForClusterRequest>
    {
        public CreateTopicForClusterRequestHandler(NameServerController nameServerController)
            : base(nameServerController)
        {

        }

        protected override byte[] Process(IRequestHandlerContext context, CreateTopicForClusterRequest request)
        {
            var requestService = new BrokerRequestService(_nameServerController);

            requestService.ExecuteActionToAllClusterBrokers(request.ClusterName, remotingClient =>
            {
                var requestData = _binarySerializer.Serialize(new CreateTopicRequest(request.Topic, request.InitialQueueCount));
                var remotingResponse = remotingClient.InvokeSync(new RemotingRequest((int)BrokerRequestCode.CreateTopic, requestData), 30000);
                if (remotingResponse.ResponseCode != ResponseCode.Success)
                {
                    throw new Exception(string.Format("创建Topic 失败, 错误消息: {0}", Encoding.UTF8.GetString(remotingResponse.ResponseBody)));
                }
            });
            return EmptyBytes;
        }
    }
}
