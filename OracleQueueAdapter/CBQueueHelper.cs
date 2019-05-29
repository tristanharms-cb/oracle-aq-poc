using System;
using System.Threading;
using System.Threading.Tasks;

namespace OracleQueueAdapter
{
    public class CBQueueHelper
    {
        public event EventHandler<MessageEventArgs> OnMsgRecvd;

        public async Task Do(CancellationToken token)
        {
            //var client = AdvancedQueueClientFactory.Create<StockChanged>(
            //    new QueueSettings("AA_TEST_QUE_OBJ"),
            //    "Data Source =(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=testing-oracle-vanessa-sm.chj0idpp42cr.eu-west-1.rds.amazonaws.com)(PORT=2484))(CONNECT_DATA=(SID=ORCL)));User Id=devvanessa;Password=mordor;Pooling=True",
            //    "STOCK_MUTATION");

            //while (!token.IsCancellationRequested)
            //{
            //    using (var message = client.GetNextMessage())
            //    {
            //        // do some processing
            //        OnMsgRecvd?.Invoke(
            //            this,
            //            new MessageEventArgs
            //            {
            //                Message = new QMessage
            //                {
            //                    Content = JsonConvert.SerializeObject(
            //                        message.Content)
            //                }
            //            });

            //        message.Acknowledge();
            //    }
            //}
        }
    }

    public struct StockChanged //: INullable, IOracleCustomType
    {
        public int ProductId { get; set; }
        public int OldValue { get; set; }

        public int NewValue { get; set; }

        public bool IsNull => ProductId == 0 &&
                              OldValue == 0 &&
                              NewValue == 0;

        //public void FromCustomObject(Oracle.DataAccess.Client.OracleConnection con, IntPtr pUdt)
        //{
        //}

        //public void ToCustomObject(Oracle.DataAccess.Client.OracleConnection con, IntPtr pUdt)
        //{
        //}
    }
}