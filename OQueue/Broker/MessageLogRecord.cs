using System;
using System.IO;
using OceanChip.Common.Storage;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Utils;
using System.Text;

namespace OceanChip.Queue.Broker
{
    [Serializable]
    public class MessageLogRecord : QueueMessage, ILogRecord
    {
        private static readonly byte[] EmptyBytes = new byte[0];
        private readonly Action<MessageLogRecord, object> _callback;
        private readonly object _parameter;

        public MessageLogRecord() { }
        public MessageLogRecord(
            string topic,
            int code,
            byte[] body,
            int queueId,
            long queueOffset,
            DateTime createdTime,
            DateTime storedTime,
            string tag,
            string producerAddress,
            Action<MessageLogRecord,object> callback,
            object parameter):
            base(null, topic, code, body, queueId, queueOffset, createdTime, storedTime, tag, producerAddress)
        {
            _callback = callback;
            _parameter = parameter;
        }
        //public void ReadFrom(byte[] recordBuffer)
        //{
        //}

        public void WriteTo(long logPosition, BinaryWriter writer)
        {
            LogPosition = LogPosition;

            MessageId = MessageIdUtil.CreateMessageId(logPosition);

            writer.Write(LogPosition);

            Writer(MessageId, writer);

            Writer(Topic, writer);

            if (string.IsNullOrEmpty(Tag))
            {
                writer.Write(EmptyBytes.Length);
                writer.Write(EmptyBytes);
            }
            else
            {
                Writer(Tag, writer);
            }

            this.Writer(ProducerAddress, writer);

            writer.Write(Code);

            writer.Write(Body.Length);
            writer.Write(Body);

            writer.Write(QueueId);

            writer.Write(QueueOffset);
            writer.Write(CreatedTime.Ticks);
            writer.Write(StoredTime.Ticks);
        }
        public void OnPersisted()
        {
            _callback?.Invoke(this, _parameter);
        }
        private void Writer(string msg,BinaryWriter writer)
        {            
            var msgBytes = Encoding.UTF8.GetBytes(msg);
            writer.Write(msgBytes.Length);
            writer.Write(msgBytes);
        }
    }
}