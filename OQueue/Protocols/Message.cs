using OceanChip.Common.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OceanChip.Queue.Protocols
{
    [Serializable]
    public class Message
    {
        public string Topic { get; set; }
        public string Tag { get; set; }
        public int Code { get; set; }
        public byte[] Body { get; set; }
        public DateTime CreatedTime { get; set; }
        public Message() { }
        public Message(string topic,int code,byte[] body,string tag=null):this(topic,code,body,DateTime.Now,tag){}
            public Message(string topic,int code,byte[] body,DateTime createTime,string tag = null)
        {
            Check.NotNull(topic, nameof(topic));
            Check.Positive(code, nameof(code));
            Check.NotNull(body, nameof(body));

            this.Topic = topic;
            this.Tag = tag;
            this.Code = code;
            this.Body = body;
            this.CreatedTime = createTime;
        }
        public override string ToString()
        {
            return $"[Topic={Topic},Code={Code},BodyLength={Body.Length},Tag={Tag},CreatedTime:{CreatedTime}]";
        }
    }
}
