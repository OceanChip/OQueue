using OceanChip.Common.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker
{
    public static class Extensions
    {
        public static BufferLogRecord TryReadRecrodBufferAt(this ChunkReader chunkReader, long position)
        {
            return chunkReader.TryReadAt(position, recordBuffer =>
            {
                var record = new BufferLogRecord();
                record.ReadFrom(recordBuffer);
                return record;
            });
        }
    }
}
