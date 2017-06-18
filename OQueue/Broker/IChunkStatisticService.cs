namespace OceanChip.Queue.Broker
{
    public interface IChunkStatisticService
    {
        void AddFileReadCount(int chunkNum);
        void AddUnManagedReadCount(int chunkNum);
        void AddCachedReadCount(int chunkNum);
        void AddWriteBytes(int chunkNum, int byteCount);
        void Start();
        void Shutdown();
    }
}