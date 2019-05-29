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

            q.OnMsgRecvd += (sender, msgArgs) =>
            {
                Console.WriteLine(msgArgs.Message.Content);

                var stock = JsonConvert.DeserializeObject<ProductStock>(msgArgs.Message.Content);
            };

            q.BeginListening(cts.Token).Wait(cts.Token);
        }
    }

    internal class ProductStock
    {
        [JsonProperty]
        internal int ProductId { get; set; }

        [JsonProperty]
        internal int Stock { get; set; }
    }
}