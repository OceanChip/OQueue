using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OceanChip.Common.Storage;
using OceanChip.Common.Utilities;

namespace OceanChip.Queue.Broker.DeleteMessageStrategies
{
    public class DeleteMessageByCountStrategy : IDeleteMessageStrategy
    {
        public int MaxChunkCount { get; private set; }
        public DeleteMessageByCountStrategy(int maxChunkCount = 100)
        {
            Check.Positive(maxChunkCount, nameof(maxChunkCount));
            this.MaxChunkCount = maxChunkCount;
        }
        public IEnumerable<Chunk> GetAllowDeleteChunks(ChunkManager chunkManager, long maxMessagePosition)
        {
            var chunks = new List<Chunk>();
            var allCompletedChunks = chunkManager
                .GetAllChunks()
                .Where(x => x.IsCompleted && CheckMessageConsumeOffset(x, maxMessagePosition))
                .OrderBy(x => x.ChunkHeader.ChunkNumber).ToList();
            var exceedCount = allCompletedChunks.Count - MaxChunkCount;
            if (exceedCount <= 0)
                return chunks;
            for(var i = 0; i < exceedCount; i++)
            {
                chunks.Add(allCompletedChunks[i]);
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
