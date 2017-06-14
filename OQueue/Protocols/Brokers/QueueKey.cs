using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols.Brokers
{
    public class QueueKey : IComparable<QueueKey>, IComparable
    {
        /// <summary>
        /// 主题
        /// </summary>
        public string Topic { get; set; }
        /// <summary>
        /// 队列Id
        /// </summary>
        public int QueueId { get; set; }
        public QueueKey() { }
        public QueueKey(string topic,int queueId)
        {
            this.Topic = topic;
            this.QueueId = queueId;
        }
        public static bool operator ==(QueueKey left,QueueKey right)
        {
            return IsEqual(left, right);
        }
        public static bool operator !=(QueueKey left,QueueKey right)
        {
            return !IsEqual(left, right);
        }
        public override bool Equals(object obj)
        {
            if(obj == null || obj.GetType() != GetType())
            {
                return false;
            }
            var other = (QueueKey)obj;
            return Topic == other.Topic && QueueId == other.QueueId;
        }
        public override string ToString()
        {
            return $"{Topic}@{QueueId}";
        }
        public override int GetHashCode()
        {
            return (Topic + QueueId.ToString()).GetHashCode();
        }
        public static bool IsEqual(QueueKey left,QueueKey right)
        {
            if (ReferenceEquals(left, null) ^ (ReferenceEquals(right, null)))
                return false;

            return ReferenceEquals(left, null) || left.Equals(right);
        }
        public int CompareTo(QueueKey other)
        {
            return ToString().CompareTo(other.ToString());
        }

        public int CompareTo(object obj)
        {
            return ToString().CompareTo(obj.ToString());
        }
    }
}
