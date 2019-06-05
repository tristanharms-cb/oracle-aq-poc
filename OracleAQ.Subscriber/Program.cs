using Newtonsoft.Json;
using OracleQueueAdapter;
using System;
using System.Threading;

namespace OracleAQ.Subscriber
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var cts = new CancellationTokenSource();

            var q = new OracleQueueHelper(
                "Data Source =(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=testing-oracle-vanessa-sm.chj0idpp42cr.eu-west-1.rds.amazonaws.com)(PORT=2484))(CONNECT_DATA=(SID=ORCL)));User Id=devvanessa;Password=mordor;Pooling=True",
                "AA_TEST_QUE");

            q.OnMessageException += (sender, exceptionArgs) =>
            {
                Console.WriteLine(exceptionArgs.Exception.Message);
            };

            q.OnMessageReceived += (sender, msgArgs) =>
            {
                Console.WriteLine($"Msg received ID: {msgArgs.Message.Id} and Content: {msgArgs.Message.Content}");

                try
                {
                    var stock = JsonConvert.DeserializeObject<ProductStock>(msgArgs.Message.Content);                    

                    msgArgs.Message.Acknowledge();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Message will be abandoned! {e.Message}");
                    msgArgs.Message.Abandon();
                }
            };

            q.BeginListening(cts.Token).Wait(cts.Token);
        }
    }

    internal class ProductStock
    {
        [JsonProperty]
        internal int ProductId { get; set; }

        [JsonProperty]
        internal int StockQuantity { get; set; }
    }
}