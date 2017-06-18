using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Remoting;
using OceanChip.Queue.Utils;

namespace OceanChip.Queue.NameServer.RequestHandlers
{
    public class GetAllClustersRequestHandler : AbstractRequestHandler<NoneDataSerializer>
    {
        public GetAllClustersRequestHandler(NameServerController nameServerController) : base(nameServerController)
        {
        }

        protected override byte[] Process(IRequestHandlerContext context, NoneDataSerializer request)
        {
            var clusterList = _nameServerController.ClusterManager.GetAllClusters();
            var data = _binarySerializer.Serialize(clusterList);
            return data;
        }
    }
}
