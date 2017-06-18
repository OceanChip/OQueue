using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Broker
{
    public class BatchMessageLogRecord
    {
        private readonly IEnumerable<MessageLogRecord> _records;
        private readonly Action<BatchMessageLogRecord, object> _callback;
        private readonly object _parameter;

        public IEnumerable<MessageLogRecord> Records => _records;
        public Action<BatchMessageLogRecord, object> Callback => _callback;
        public BatchMessageLogRecord(IEnumerable<MessageLogRecord> records,Action<BatchMessageLogRecord,object> callback,object parameter)
        {
            this._records = records;
            this._callback = callback;
            this._parameter = parameter;
        }
        public void OnPersisted()
        {
            _callback?.Invoke(this, _parameter);
        }
    }
}
