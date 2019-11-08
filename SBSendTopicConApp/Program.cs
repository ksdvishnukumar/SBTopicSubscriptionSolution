using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Configuration;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace SBSendTopicConApp
{
  class Program
  {
    static NamespaceManager _namespaceManager;
    static void Main(string[] args)
    {
      CollectSBDetails();
      SendMessage();
    }

    static void SendMessage()
    {
      TokenProvider tokenProvider = _namespaceManager.Settings.TokenProvider;

      if (!_namespaceManager.TopicExists("DataCollectionTopic"))
      {
        _namespaceManager.CreateTopic("DataCollectionTopic");

        if (!_namespaceManager.SubscriptionExists("DataCollectionTopic", "Inventory"))
        {
          _namespaceManager.CreateSubscription("DataCollectionTopic", "Inventory");
        }
        if (!_namespaceManager.SubscriptionExists("DataCollectionTopic", "Dashboard"))
        {
          _namespaceManager.CreateSubscription("DataCollectionTopic", "Dashboard");
        }

        MessagingFactory factory = MessagingFactory.Create(_namespaceManager.Address, tokenProvider);

        BrokeredMessage message = new BrokeredMessage();//can pass a user defined class too
        message.Label = "SalesReport";
        message.Properties["StoreName"] = "Nike";
        message.Properties["MachineID"] = "POS1";

        BrokeredMessage message1 = new BrokeredMessage();//can pass a user defined class too
        message1.Label = "SalesRep";
        message1.Properties["StoreName"] = "Adidas";
        message1.Properties["MachineID"] = "POS3";

        MessageSender sender = factory.CreateMessageSender("DataCollectionTopic");
        sender.Send(message);
        sender.Send(message1);
        Console.WriteLine("Message Sent Successfully");
      }

    }

    private static void CollectSBDetails()
    {
      _namespaceManager = NamespaceManager.CreateFromConnectionString(ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"].ToString());
    }
  }
}
