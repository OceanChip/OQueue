using OceanChip.Queue.Protocols;

namespace OceanChip.Queue.Clients.Consumers
{
    public interface IMessageContext
    {
        void OnMessageHandled(QueueMessage message);
    }
}