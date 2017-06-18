using OceanChip.Common.Components;
using OceanChip.Common.Remoting;
using OceanChip.Common.Serializing;
using OceanChip.Queue.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public abstract class AbstractRequestHandler<T> : IRequestHandler where T:class
    {
        protected readonly static byte[] EmptyBytes = new byte[0];
        protected NameServerController _nameServerController;
        protected IBinarySerializer _binarySerializer;
        public AbstractRequestHandler(NameServerController nameServerController)
        {
            this._nameServerController = nameServerController;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }
        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            byte[] data;
            if (typeof(T) == typeof(NoneDataSerializer))
            {
                data = Process(context, null);
            }
            else
            {
                var request = _binarySerializer.Deserialize<T>(remotingRequest.Body);
                data = Process(context, request);
            }
            return RemotingResponseFactory.CreateResponse(remotingRequest,data);
        }

        protected abstract byte[] Process(IRequestHandlerContext context,T request);
    }
}
