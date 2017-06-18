using OceanChip.Common.Configurations;
using OceanChip.Queue.Configurations;
using OceanChip.Queue.NameServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuickStart.NameServer
{
    class Program
    {

        static void Main(string[] args)
        {
            InitializeEQueue();
            new NameServerController().Start();
            Console.ReadLine();
        }

        static void InitializeEQueue()
        {
            var configuration = Configuration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler()
                .RegisterQueueComponents();
        }
    }
}
