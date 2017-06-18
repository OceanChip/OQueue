using OceanChip.Common.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace OceanChip.Queue.Broker
{
    public class BufferLogRecord : ILogRecord
    {
        public byte[] RecordBuffer { get; set; }
        public void ReadFrom(byte[] recordBuffer)
        {
            RecordBuffer = new byte[4 + recordBuffer.Length];
            Buffer.BlockCopy(BitConverter.GetBytes(recordBuffer.Length), 0, recordBuffer, 0, 4);
            Buffer.BlockCopy(recordBuffer, 0, RecordBuffer, 4, recordBuffer.Length);
        }

        public void WriteTo(long logPosition, BinaryWriter writer)
        {
            writer.Write(RecordBuffer);
        }
    }
}
