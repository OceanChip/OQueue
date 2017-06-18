using OceanChip.Common.Components;
using OceanChip.Common.Remoting;
using OceanChip.Common.Serializing;
using OceanChip.Queue.Broker.Exceptions;
using OceanChip.Queue.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.RequestHandlers.Admin
{
    public abstract class AbstractRequestHanlder : IRequestHandler
    {
        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }
            return RemotingResponseFactory.CreateResponse(remotingRequest, GetResponseData(context));

        }
        protected abstract byte[] GetResponseData(IRequestHandlerContext context);
    }
    public abstract class AbstractRequestHanlder<T> : IRequestHandler where T:class
    {
        public readonly static byte[] EmptyBytes = new byte[0];
        protected readonly IBinarySerializer _binarySerializer;
        public AbstractRequestHanlder()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }
        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }
            byte[] data;
            if (typeof(T) == typeof(NoneDataSerializer))
            {
                data = GetResponseData(context, null);
            }
            else
            {
                var request = _binarySerializer.Deserialize<T>(remotingRequest.Body);
                data = GetResponseData(context, request);
            }
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
        protected abstract byte[] GetResponseData(IRequestHandlerContext context,T request);
    }
    public abstract class AbstractRequestHanlder<T,TService>: AbstractRequestHanlder<T> where T : class where TService : class
    {
        protected readonly TService _service;

        public AbstractRequestHanlder()
        {
            _service = ObjectContainer.Resolve<TService>();
        }
    }
    public abstract class AbstractRequestHanlder<T, TService1,TServer2> : AbstractRequestHanlder<T> where T : class where TService1 : class where TServer2:class
    {
        protected readonly TService1 _service1;
        protected readonly TServer2 _service2;

        public AbstractRequestHanlder()
        {
            _service1 = ObjectContainer.Resolve<TService1>();
            _service2 = ObjectContainer.Resolve<TServer2>();
        }
    }
}
