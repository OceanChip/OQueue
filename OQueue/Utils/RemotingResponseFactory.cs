using OceanChip.Common.Remoting;
using OceanChip.Queue.Protocols;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Utils
{
    public class RemotingResponseFactory
    {
        private static readonly byte[] EmptyData = new byte[0];
        public static RemotingResponse CreateResponse(RemotingRequest remotingRequest)
        {
            return CreateResponse(remotingRequest, ResponseCode.Success, EmptyData);
        }
        public static RemotingResponse CreateResponse(RemotingRequest remotingRequest,short responseCode)
        {
            return CreateResponse(remotingRequest, responseCode, EmptyData);
        }
        public static RemotingResponse CreateResponse(RemotingRequest remotingRequest, byte[] data)
        {
            return CreateResponse(remotingRequest, ResponseCode.Success, data);
        }

        public static RemotingResponse CreateResponse(RemotingRequest request,short responseCode,byte[] data)
        {
            return new RemotingResponse(
                request.Type,
                request.Code,
                request.Sequence,
                request.CreatedTime,
                responseCode,
                data,
                DateTime.Now,
                request.Header,
                null);
        }
    }
}
