using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public abstract class BrokerRequestHandler<T> : AbstractRequestHandler<T> where T:class
    {
        public BrokerRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        //protected override void Process(IRequestHandlerContext context, T request)
        //{
        //    var requestService = new BrokerRequestService(_nameServerController);

        //    requestService.ExecuteActionToAllClusterBrokers(request.ClusterName, remotingClient =>
        //    {
        //        var requestData = _binarySerializer.Serialize(new CreateTopicRequest(request.Topic, request.InitialQueueCount));
        //        var remotingResponse = remotingClient.InvokeSync(new RemotingRequest((int)BrokerRequestCode.CreateTopic, requestData), 30000);
        //        if (remotingResponse.ResponseCode != ResponseCode.Success)
        //        {
        //            throw new Exception(string.Format("CreateTopic failed, errorMessage: {0}", Encoding.UTF8.GetString(remotingResponse.ResponseBody)));
        //        }
        //    });
        //}
    }
}
