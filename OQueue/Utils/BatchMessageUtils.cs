using OceanChip.Common.Utilities;
using OceanChip.Queue.Protocols;
using OceanChip.Queue.Protocols.Brokers.Request;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Utils
{
    public class BatchMessageUtils
    {
        private static readonly byte[] EmptyBytes = new byte[0];
        public static byte[] EncodeSendMessageRequest(BatchSendMessageRequest request)
        {
            var bytesList = new List<byte[]>();

            var queueIdBytes = BitConverter.GetBytes(request.QueueId);

            var producerAddressBytes = Encoding.UTF8.GetBytes(request.ProducerAddress);
            var producerAddressLenghtBytes = BitConverter.GetBytes(producerAddressBytes.Length);

            var messageCountBytes = BitConverter.GetBytes(request.Messages.Count());

            bytesList.AddRange(new byte[][] { queueIdBytes, producerAddressLenghtBytes, producerAddressBytes, messageCountBytes });

            //messages
            foreach(var message in request.Messages)
            {
                //topic
                var topicBytes = Encoding.UTF8.GetBytes(message.Topic);
                var topicLengthBytes = BitConverter.GetBytes(topicBytes.Length);
                //messageCode
                var messageCodeBytes = BitConverter.GetBytes(message.Code);
                //createTimeTicks
                var messageCreatedTimeTickBytes = BitConverter.GetBytes(message.CreatedTime.Ticks);

                //tag
                var tagBytes = EmptyBytes;
                if (!string.IsNullOrEmpty(message.Tag))
                {
                    tagBytes = Encoding.UTF8.GetBytes(message.Tag);
                }
                var tagLengthBytes = BitConverter.GetBytes(tagBytes.Length);

                //body Length
                var bodyLengthBytes = BitConverter.GetBytes(message.Body.Length);

                bytesList.AddRange(new byte[][] { topicLengthBytes,topicBytes,messageCodeBytes,messageCreatedTimeTickBytes
                    ,tagLengthBytes,tagBytes,bodyLengthBytes,message.Body});
            }
            return ByteUtil.Combine(bytesList.ToArray());
        }

        public static BatchSendMessageRequest DecodeSendMessageRequest(byte[] buffer)
        {
            var srcOffset = 0;

            var queueId = ByteUtil.DecodeInt(buffer, srcOffset, out srcOffset);
            var producerAddress = ByteUtil.DecodeString(buffer, srcOffset, out srcOffset);
            var messageCount = ByteUtil.DecodeInt(buffer, srcOffset, out srcOffset);
            var messages = new List<Message>();

            for(int i = 0; i < messageCount; i++)
            {
                var topic = ByteUtil.DecodeString(buffer, srcOffset, out srcOffset);
                var code = ByteUtil.DecodeInt(buffer, srcOffset, out srcOffset);
                var createdTime = ByteUtil.DecodeDateTime(buffer, srcOffset, out srcOffset);
                var tag=ByteUtil.DecodeString(buffer, srcOffset, out srcOffset);
                var bodyLength=ByteUtil.DecodeInt(buffer, srcOffset, out srcOffset);
                var body = new byte[bodyLength];
                Buffer.BlockCopy(buffer, srcOffset, body, 0, bodyLength);
                srcOffset += bodyLength;
                messages.Add(new Message(topic, code, body, createdTime, tag));
            }
            return new BatchSendMessageRequest
            {
                QueueId = queueId,
                Messages = messages,
                ProducerAddress = producerAddress,
            };
        }

        public static byte[] EncodeMessageStoreResult(BatchMessageStoreResult result)
        {
            var bytesList = new List<byte[]>();

            var queueIdBytes = BitConverter.GetBytes(result.QueueId);

            var topicBytes = Encoding.UTF8.GetBytes(result.Topic);
            var topicLengthBytes = BitConverter.GetBytes(topicBytes.Length);

            var messageCountBytes = BitConverter.GetBytes(result.MessageResults.Count());

            bytesList.AddRange(new byte[][] { queueIdBytes, topicLengthBytes, topicBytes, messageCountBytes });
            foreach(var message in result.MessageResults)
            {
                byte[] messageIdLengthBytes;
                byte[] messageIdBytes;
                ByteUtil.EncodeString(message.MessageId, out messageIdLengthBytes, out messageIdBytes);

                byte[] codeBytes = BitConverter.GetBytes(message.Code);

                byte[] queueOffsetBytes = BitConverter.GetBytes(message.QueueOffset);

                byte[] createdTimeTickBytes = BitConverter.GetBytes(message.CreatedTime.Ticks);

                byte[] storedTimeTickBytes = BitConverter.GetBytes(message.StoredTime.Ticks);

                byte[] tagLengthBytes = null;
                byte[] tagBytes = null;
                ByteUtil.EncodeString(message.Tag, out tagLengthBytes, out tagBytes);

                bytesList.AddRange(new byte[][] { messageIdLengthBytes,messageIdBytes,codeBytes,queueOffsetBytes,
                    createdTimeTickBytes,storedTimeTickBytes,tagLengthBytes,tagBytes});
            }
            return ByteUtil.Combine(bytesList.ToArray());
        }
        public static BatchMessageStoreResult DecodeMessageStoreResult(byte[] buffer)
        {
            int srcOffset = 0;

            var queueId = ByteUtil.DecodeInt(buffer, srcOffset, out srcOffset);
            var topic=ByteUtil.DecodeString(buffer, srcOffset, out srcOffset);
            var messageCount=ByteUtil.DecodeInt(buffer, srcOffset, out srcOffset);
            var itemList = new List<BatchMessageItemResult>();

            for(var i = 0; i < messageCount; i++)
            {
                var messageId=ByteUtil.DecodeString(buffer, srcOffset, out srcOffset);
                var code=ByteUtil.DecodeInt(buffer, srcOffset, out srcOffset);
                var queueOffset=ByteUtil.DecodeLong(buffer, srcOffset, out srcOffset);
                var createdTime=ByteUtil.DecodeDateTime(buffer, srcOffset, out srcOffset);
                var storedTime=ByteUtil.DecodeDateTime(buffer, srcOffset, out srcOffset);
                var tag=ByteUtil.DecodeString(buffer, srcOffset, out srcOffset);
                var item = new BatchMessageItemResult(messageId, code, queueOffset, tag, createdTime, storedTime);

                itemList.Add(item);
            }
            return new BatchMessageStoreResult(topic, queueId, itemList);
        }
    }
}
