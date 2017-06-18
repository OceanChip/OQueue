using OceanChip.Common.Extensions;
using OceanChip.Common.Utilities;
using OceanChip.Queue.Broker;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Utils
{
    public class MessageIdUtil
    {
        private static byte[] _ipBytes;
        private static byte[] _portBytes;

        public static string CreateMessageId(long messagePosition)
        {
            if (_ipBytes == null)
                _ipBytes = BrokerController.Instance.Setting.BrokerInfo.ProducerAddress.ToEndPoint().Address.GetAddressBytes();
            if (_portBytes == null)
                _portBytes = BitConverter.GetBytes(BrokerController.Instance.Setting.BrokerInfo.ProducerAddress.ToEndPoint().Port);
            byte[] posBytes=BitConverter.GetBytes(messagePosition);
            byte[] messageidBytes = ByteUtil.Combine(_ipBytes, _portBytes, posBytes);
            return ObjectId.ToHexString(messageidBytes);
        }
        public static MessageIdInfo ParseMessageId(string messageId)
        {
            var messageidBytes = ObjectId.ParseHexString(messageId);
            var ipBytes = new byte[4];
            var portBytes = new byte[4];
            var messagePositionBytes = new byte[8];
            Buffer.BlockCopy(messageidBytes, 0, ipBytes, 0, 4);
            Buffer.BlockCopy(messageidBytes, 4, portBytes, 0, 4);
            Buffer.BlockCopy(messageidBytes, 8, messagePositionBytes, 0, 8);

            var ip = BitConverter.ToInt32(ipBytes, 0);
            var port = BitConverter.ToInt32(portBytes, 0);
            var messagePosition = BitConverter.ToInt64(messagePositionBytes, 0);
            return new MessageIdInfo
            {
                IP = new IPAddress(ip),
                Port = port,
                MessagePosition = messagePosition
            };
        }
    }
    public struct MessageIdInfo
    {
        public IPAddress IP;
        public int Port;
        public long MessagePosition;
    }
}
