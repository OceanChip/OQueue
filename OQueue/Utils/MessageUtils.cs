using OceanChip.Common.Utilities;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Protocols.Brokers.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Utils
{
    public class MessageUtils
    {
        private static readonly byte[] EmptyBytes = new byte[0];
        public static byte[] EncodeSendMessageRequest(SendMessageRequest request)
        {
            var queueIdBytes = BitConverter.GetBytes(request.QueueId);

            var messageCodeBytes = BitConverter.GetBytes(request.Message.Code);

            var messageCreatedTimeticksBytes = ByteUtil.EncodeDateTime(request.Message.CreatedTime);

            byte[] topicBytes;
            byte[] topicLengthBytes;
            ByteUtil.EncodeString(request.Message.Topic, out topicLengthBytes, out topicBytes);

            var tagBytes = EmptyBytes;
            if (!string.IsNullOrWhiteSpace(request.Message.Tag))
            {
                tagBytes = Encoding.UTF8.GetBytes(request.Message.Tag);
            }
            var tagLengthBytes = BitConverter.GetBytes(tagBytes.Length);

            byte[] producerAddressBytes;
            byte[] producerAddressLengthBytes;
            ByteUtil.EncodeString(request.ProducerAddress, out producerAddressLengthBytes, out producerAddressBytes);

            return ByteUtil.Combine(queueIdBytes, messageCodeBytes, messageCreatedTimeticksBytes, topicLengthBytes,
                topicBytes, tagBytes, producerAddressLengthBytes, producerAddressBytes, request.Message.Body);

        }
        public static SendMessageRequest DecodeSendMessageRequest(byte[] messageBuffer)
        {
            int srcOffset = 0;
            var queueId = ByteUtil.DecodeInt(messageBuffer, srcOffset, out srcOffset);
            var messageCode=ByteUtil.DecodeInt(messageBuffer, srcOffset, out srcOffset);
            var createdTime=ByteUtil.DecodeDateTime(messageBuffer, srcOffset, out srcOffset);
            var topic=ByteUtil.DecodeString(messageBuffer, srcOffset, out srcOffset);
            var tag=ByteUtil.DecodeString(messageBuffer, srcOffset, out srcOffset);
            var producerAddress=ByteUtil.DecodeString(messageBuffer, srcOffset, out srcOffset);
            return new SendMessageRequest { QueueId = queueId,
                Message = new Protocols.Message(topic, 
                messageCode,
                ByteUtil.DecodeBytes(messageBuffer, srcOffset,out srcOffset),
                createdTime,tag
                ),
                ProducerAddress=producerAddress
            };
        }
        public static byte[] EncodeMessageStoreResult(MessageStoreResult result)
        {
            var messageCodeBytes = BitConverter.GetBytes(result.Code);
            var queueIdBytes = BitConverter.GetBytes(result.QueueId);
            var queueOffsetBytes = BitConverter.GetBytes(result.QueueOffset);

            byte[] messageIdLengthBytes;
            byte[] messageIdBytes;
            ByteUtil.EncodeString(result.MessageId, out messageIdLengthBytes, out messageIdBytes);

            byte[] topBytes;
            byte[] topicLengthBytes;
            ByteUtil.EncodeString(result.Topic, out topicLengthBytes, out topBytes);

            byte[] tagBytes = EmptyBytes;
            if (!string.IsNullOrEmpty(result.Tag))
            {
                tagBytes = Encoding.UTF8.GetBytes(result.Tag);
            }
            byte[] tagLengthBytes = BitConverter.GetBytes(tagBytes.Length);
            byte[] createdTimeTicksBytes = BitConverter.GetBytes(result.CreatedTime.Ticks);
            byte[] storedTimeTicksBytes = BitConverter.GetBytes(result.StoredTime.Ticks);

            return ByteUtil.Combine(
                messageCodeBytes,queueIdBytes,queueOffsetBytes,
                messageIdLengthBytes,messageIdBytes,
                topicLengthBytes,topBytes,
                tagLengthBytes,tagBytes,
                createdTimeTicksBytes,storedTimeTicksBytes
                );
        }
        public static MessageStoreResult DecodeMessageStoreResult(byte[] buffer)
        {
            int srcOffset = 0;
            int messageCode = ByteUtil.DecodeInt(buffer, srcOffset, out srcOffset);
            int queueId=ByteUtil.DecodeInt(buffer, srcOffset, out srcOffset);
            long queueOffset=ByteUtil.DecodeLong(buffer, srcOffset, out srcOffset);
            string messageId=ByteUtil.DecodeString(buffer, srcOffset, out srcOffset);
            string topic=ByteUtil.DecodeString(buffer, srcOffset, out srcOffset);
            string tag=ByteUtil.DecodeString(buffer, srcOffset, out srcOffset);
            DateTime createdTime=ByteUtil.DecodeDateTime(buffer, srcOffset, out srcOffset);
            DateTime storedTime=ByteUtil.DecodeDateTime(buffer, srcOffset, out srcOffset);
            return new MessageStoreResult(messageId,messageCode,topic,queueId,queueOffset
                ,tag,createdTime,storedTime);
        }
    }
}
