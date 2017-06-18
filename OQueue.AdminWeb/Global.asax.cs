using Autofac;
using Autofac.Integration.Mvc;
using OceanChip.Common.Autoface;
using OceanChip.Common.Components;
using OceanChip.Common.Configurations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Web;
using System.Web.Mvc;
using System.Web.Optimization;
using System.Web.Routing;

namespace OQueue.AdminWeb
{
    public class MvcApplication : System.Web.HttpApplication
    {
        protected void Application_Start()
        {
            AreaRegistration.RegisterAllAreas();
            FilterConfig.RegisterGlobalFilters(GlobalFilters.Filters);
            RouteConfig.RegisterRoutes(RouteTable.Routes);

            var configuration = Configuration.Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4net()
                .UseJsonNet();

            configuration.SetDefault<SendEmailService, SendEmailService>();
            configuration.SetDefault<MessageService, MessageService>();
            ObjectContainer.Resolve<MessageService>().Start();
            RegisterControllers();
        }

        private static void RegisterControllers()
        {
            var webAssembly = Assembly.GetExecutingAssembly();
            var container = (ObjectContainer.Current as AutofacObjectContainer).Container;
            var builder = new ContainerBuilder();
            builder.RegisterControllers(webAssembly);
            builder.Update(container);
            DependencyResolver.SetResolver(new AutofacDependencyResolver(container));

        }
    }
}
