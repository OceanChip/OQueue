using OceanChip.Common.Scheduling;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker
{
    public class DefaultTpsStatisticService : ITpsStatisticService
    {
        private const int ConsumeTpsStatInterval = 5;
        class CountInfo
        {
            public long PreviousCount;
            public long CurrentCount;
            public long Throughput;
            public void CalculateThroughput()
            {
                Throughput = CurrentCount - PreviousCount;
                PreviousCount = CurrentCount;
            }
        }

        private readonly IScheduleService _scheduleService;
        private ConcurrentDictionary<string, CountInfo> _sendTpsDict;
        private ConcurrentDictionary<string, CountInfo> _consumeTpsDict;

        public DefaultTpsStatisticService(IScheduleService scheduleService)
        {
            this._scheduleService = scheduleService;
            _sendTpsDict = new ConcurrentDictionary<string, CountInfo>();
            _consumeTpsDict = new ConcurrentDictionary<string, CountInfo>();
        }
        public void AddTopicSendCount(string topic, int queuId)
        {
            _sendTpsDict.AddOrUpdate($"{topic}_{queuId}", x =>
            {
                return new CountInfo { CurrentCount = 1 };
            }, (x, y) =>
             {
                 Interlocked.Increment(ref y.CurrentCount);
                 return y;
             });
        }

        public long GetTopicConsumeThroughput(string topic, int queueId, string consumeGroup)
        {
            var key = $"{topic}_{queueId}_{consumeGroup}";
            CountInfo count;
            if (_sendTpsDict.TryGetValue(key, out count))
            {
                return count.Throughput/ConsumeTpsStatInterval;
            }
            return 0L;
        }

        public long GetTopicSendThroughput(string topic, int queueId)
        {
            var key = $"{topic}_{queueId}";
            CountInfo count;
            if(_sendTpsDict.TryGetValue(key,out count))
            {
                return count.Throughput;
            }
            return 0L;
        }

        public long GetTotalConsumeThroughput()
        {
            return _consumeTpsDict.Values.Sum(x => x.Throughput) / ConsumeTpsStatInterval;
        }

        public long GetTotalSendThroughput()
        {
            return _sendTpsDict.Values.Sum(x => x.Throughput);
        }

        public void Shutdown()
        {
            _scheduleService.StopTask("CalculateThroughput");
            _scheduleService.StopTask("CalculateConsumeThroughput");
        }

        public void Start()
        {
            _scheduleService.StartTask("CalculateThroughput", () =>
            {
                foreach (var entry in _sendTpsDict)
                {
                    entry.Value.CalculateThroughput();
                }
            }, 1000, 1000);
            _scheduleService.StartTask("CalculateConsumeThroughput", () =>
            {
                foreach (var entry in _sendTpsDict)
                {
                    entry.Value.CalculateThroughput();
                }
            }, 1000,ConsumeTpsStatInterval*1000);
        }

        public void UpdateTopicConsumeOffset(string topic, int queueId, string consumeGroup, long consumeOffset)
        {
            _consumeTpsDict.AddOrUpdate($"{topic}_{queueId}_{consumeGroup}", x =>
            {
                return new CountInfo { CurrentCount = consumeOffset };
            }, (x, y) =>
            {
                y.CurrentCount = consumeOffset;
                return y;
            });
        }
    }
}
