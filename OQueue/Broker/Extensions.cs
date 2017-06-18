using OceanChip.Common.Remoting;
using OceanChip.Common.Storage;
using OceanChip.Queue.Protocols.Brokers;
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
        public static SocketRemotingServer RegisterRequestHandler<T>(this SocketRemotingServer server, BrokerRequestCode code)where T: class,IRequestHandler, new()
        {
            server.RegisterRequestHandler((int)code, new T());
            return server;
        }
    }
}
