using OceanChip.Common.Components;
using OceanChip.Common.Extensions;
using OceanChip.Common.Logging;
using OceanChip.Common.Scheduling;
using OceanChip.Common.Socketing;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker.Client
{
    public class ProducerManager
    {
        class ProducerInfo
        {
            public string ProducerId;
            public ClientHeartbeatInfo HeartbeatInfo;
        }
        private readonly ConcurrentDictionary<string, ProducerInfo> _producerDict = new ConcurrentDictionary<string, ProducerInfo>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;

        public ProducerManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Start()
        {
            _producerDict.Clear();
            _scheduleService.StartTask("ScanNotActiveProducer", ScanNotActiveProducer, 1000, 1000);
        }
        public void Shutdown()
        {
            _producerDict.Clear();
            _scheduleService.StopTask("ScanNotActiveProducer");
        }
        public void RegisterProducer(ITcpConnection connection,string producerId)
        {
            var connectionId = connection.RemoteEndPoint.ToAddress();
            _producerDict.AddOrUpdate(connectionId, key =>
            {
                var producer = new ProducerInfo
                {
                    ProducerId = producerId,
                    HeartbeatInfo = new ClientHeartbeatInfo(connection) { LastHeartbeartTime = DateTime.Now }
                };
                _logger.InfoFormat("注册生产者,Productid：{0},ConnectionId:{1}", producerId, connectionId);
                return producer;
            }, (key, existingProducerInfo) =>
            {
                existingProducerInfo.HeartbeatInfo.LastHeartbeartTime = DateTime.Now;
                return existingProducerInfo;
            });
        }
        public void RemoveProducer(string connectionId)
        {
            ProducerInfo producer;
            if(_producerDict.TryRemove(connectionId,out producer)){
                try
                {
                    producer.HeartbeatInfo.Connection.Close();
                }catch(Exception ex)
                {
                    _logger.Error($"关闭生产者连接失败，ProductId:{producer.ProducerId},connectionId:{connectionId}", ex);
                }
                _logger.InfoFormat("异常生产者成功，ProducerId:{0},ConnectionId:{1},LastHeartbeat:{2}",
                    producer.ProducerId, connectionId, producer.HeartbeatInfo.LastHeartbeartTime);
            }
        }
        public int GetProducerCount()
        {
            return _producerDict.Count;
        }
        public IEnumerable<string> GetAllProducers()
        {
            return _producerDict.Values.Select(x => x.ProducerId).ToList();
        }
        public bool IsProducerExist(string producerId)
        {
            return _producerDict.Values.Any(x => x.ProducerId == producerId);
        }
        private void ScanNotActiveProducer()
        {
            foreach(var entry in _producerDict)
            {
                if (entry.Value.HeartbeatInfo.IsTimeout(BrokerController.Instance.Setting.ProducerExpiredTimeout))
                {
                    RemoveProducer(entry.Key);
                }
            }
        }
    }
}
