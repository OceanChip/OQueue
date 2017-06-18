using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Storage;
using OceanChip.Common.Utilities;
using System.IO;

namespace OceanChip.Queue.Broker.DeleteMessageStrategies
{
    /// <summary>
    /// 更加保存的时间来清除队列
    /// </summary>
    public class DeleteMessageByTimeStrategy : IDeleteMessageStrategy
    {
        public int MaxStorgeHours { get; private set; }
        public DeleteMessageByTimeStrategy(int maxStorgeHours = 24 * 30)
        {
            Check.Positive(maxStorgeHours, nameof(maxStorgeHours));
            this.MaxStorgeHours = maxStorgeHours;
        }
        /// <summary>
        /// 表示消息可以保存的最大时间
        /// </summary>
        /// <remarks>
        /// 比如：设置为7*24，则表示如果某个chunk里面的所有消息都消费过了，且该chunk里面的所有消息都是24*7小时之前储存的，
        /// 则该chunk可以被删除了，
        /// 默认值为24*30，即保存一个月；用户可以根据自己的服务器磁盘的大小决定消息可以保留的时间长度
        /// </remarks>
        /// <param name="chunkManager"></param>
        /// <param name="maxMessagePosition"></param>
        /// <returns></returns>
        public IEnumerable<Chunk> GetAllowDeleteChunks(ChunkManager chunkManager, long maxMessagePosition)
        {
            var chunks = new List<Chunk>();
            var allCompletedChunks = chunkManager
                .GetAllChunks()
                .Where(x => x.IsCompleted && CheckMessageConsumeOffset(x, maxMessagePosition))
                .OrderBy(x => x.ChunkHeader.ChunkNumber);

            foreach(var chunk in allCompletedChunks)
            {
                var lastWriteTime = new FileInfo(chunk.FileName).LastWriteTime;
                var storageHours = (DateTime.Now - lastWriteTime).TotalHours;
                if (storageHours >= MaxStorgeHours)
                    chunks.Add(chunk);
            }
            return chunks;
        }

        private bool CheckMessageConsumeOffset(Chunk chunk, long maxMessagePosition)
        {
            if (BrokerController.Instance.Setting.DeleteMessageIgnoreUnConsumed)
            {
                return true;
            }
            return chunk.ChunkHeader.ChunkDataEndPosition <= maxMessagePosition;
        }
    }
}
