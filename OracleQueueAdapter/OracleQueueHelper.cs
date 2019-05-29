using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Types;
using OracleParameter = Oracle.ManagedDataAccess.Client.OracleParameter;

namespace OracleQueueAdapter
{
    public class OracleQueueHelper
    {
        private readonly string _connectionString;
        private readonly string _queueName;

        public OracleQueueHelper(string connectionString, string queueName)
        {
            _connectionString = connectionString;
            _queueName = queueName;
            var assemblyDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            OracleConfiguration.WalletLocation =
                $"(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY={assemblyDirectory})))";
        }

        public event EventHandler<MessageEventArgs> OnMsgRecvd;

        public async Task BeginListening(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                var (msg, msgId) = await GetMessage(_connectionString, _queueName, token);

                OnMsgRecvd?.Invoke(
                    this,
                    new MessageEventArgs
                    {
                        Message = new QMessage
                        {
                            Content = msg,
                            Id = msgId
                        }
                    });

                //await Task.Delay(2000, token);
            }
        }

        private async Task<(string,string)> GetMessage(
            string connectionString,
            string queueName,
            CancellationToken token)
        {
            var msgContent = string.Empty;
            var msgId = string.Empty;

            using (var connection = new OracleConnection(connectionString))
            {
                await connection.OpenAsync(token);
                var command = connection.CreateCommand();

                command.CommandText = @"declare
                                            dequeue_options DBMS_AQ.dequeue_options_t;
                                            message_options DBMS_AQ.message_properties_t;
                                            payload raw(4096);
                                            msgid raw(16);
                                        begin                                            
                                            DBMS_AQ.DEQUEUE(
                                                queue_name => :queue_name,
                                                dequeue_options => dequeue_options,
                                                message_properties => message_options,
                                                payload => :payload,
                                                msgid => :msgid
                                            );
                                        end;";

                // THIS MUST BE THE FIRST PARAM
                command.Parameters.Add("queue_name", queueName);

                command.Parameters.Add(
                    new OracleParameter(
                            "payload",
                            OracleDbType.Raw,
                            ParameterDirection.Output)
                    { Size = 4096 });

                command.Parameters.Add(
                    new OracleParameter(
                            "msgid",
                            OracleDbType.Raw,
                            ParameterDirection.Output)
                    { Size = 16 });

                await command.ExecuteNonQueryAsync(token);
                var orb = (OracleBinary)command.Parameters["payload"].Value;

                msgContent = Encoding.UTF8.GetString(orb.Value);

                msgId = command.Parameters["msgid"].Value.ToString();
            }

            return (msgContent, msgId);
        }
    }

    public class MessageEventArgs : EventArgs
    {
        public QMessage Message { get; set; }
    }

    public class QMessage
    {
        public string Content { get; set; }
        public string Id { get; set; }
    }
}