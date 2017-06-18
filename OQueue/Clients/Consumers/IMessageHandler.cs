using OceanChip.Queue.Protocols;

namespace OceanChip.Queue.Clients.Consumers
{
    public interface IMessageHandler
    {
        void Handle(QueueMessage message, IMessageContext context);
    }
}