using OceanChip.Common.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.DeleteMessageStrategies
{
    public interface IDeleteMessageStrategy
    {
        IEnumerable<Chunk> GetAllowDeleteChunks(ChunkManager chunkManager, long maxMessagePosition);
    }
}
