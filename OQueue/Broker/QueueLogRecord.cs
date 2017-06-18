using OceanChip.Common.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace OceanChip.Queue.Broker
{
    public class QueueLogRecord : ILogRecord
    {
        public long MessageLogPosition { get; private set; }
        public int TagCode { get; private set; }

        public QueueLogRecord() { }
        public QueueLogRecord(long messageLogPosition,int tagCode)
        {
            this.MessageLogPosition = messageLogPosition;
            this.TagCode = tagCode;
        }
        public void ReadFrom(byte[] recordBuffer)
        {
            MessageLogPosition = BitConverter.ToInt64(recordBuffer, 0);
            TagCode = BitConverter.ToInt32(recordBuffer, 8);
        }

        public void WriteTo(long logPosition, BinaryWriter writer)
        {
            writer.Write(MessageLogPosition);
            writer.Write(TagCode);
        }
    }
}
