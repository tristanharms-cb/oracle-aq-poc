﻿using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using OracleBinary = Oracle.ManagedDataAccess.Types.OracleBinary;
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

        public event EventHandler<MessageEventArgs> OnMessageReceived;

        public event EventHandler<MessageExceptionArgs> OnMessageException;

        public async Task BeginListening(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var (msg, msgId, transaction) = 
                        await GetMessage(
                            _connectionString, 
                            _queueName, 
                            token);

                    OnMessageReceived?.Invoke(
                        this,
                        new MessageEventArgs
                        {
                            Message = new AcknowledgeableMessage(
                                msg,
                                msgId,
                                transaction)
                        });
                }
                catch (Exception e)
                {
                    OnMessageException?.Invoke(
                        this,
                        new MessageExceptionArgs
                        { Exception = e });

                    await Task.Delay(1000, token);
                }
            }
        }

        private async Task<(string, string, IDbTransaction)> GetMessage(
            string connectionString,
            string queueName,
            CancellationToken token)
        {
            var msgContent = string.Empty;
            var msgId = string.Empty;

            var connection = new OracleConnection(connectionString);

            await connection.OpenAsync(token);

            var transaction = connection.BeginTransaction();            

            var command = connection.CreateCommand();

            command.CommandText = @"declare
                                            dq_options DBMS_AQ.dequeue_options_t;
                                            message_options DBMS_AQ.message_properties_t;
                                            payload raw(4096);
                                            msgid raw(16);
                                        begin
                                            DBMS_AQ.DEQUEUE(
                                                queue_name => :queue_name,
                                                dequeue_options => dq_options,
                                                message_properties => message_options,
                                                payload => :payload,
                                                msgid => :msgid);
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

            try
            {
                await command.ExecuteNonQueryAsync(token);                
                //transaction.Commit();
            }
            catch (DbException dbException)
            {
                transaction.Rollback();
                throw;
            }

            var orb = (OracleBinary)command.Parameters["payload"].Value;

            msgContent = Encoding.UTF8.GetString(orb.Value);

            msgId = Encoding.UTF8.GetString(((OracleBinary)command.Parameters["msgid"].Value).Value);

            return (msgContent, msgId, transaction);
        }
    }

    public class MessageEventArgs : EventArgs
    {
        public AcknowledgeableMessage Message { get; set; }
    }

    public class MessageExceptionArgs : EventArgs
    {
        public Exception Exception { get; set; }
    }

    public class AcknowledgeableMessage
    {
        private readonly IDbTransaction _transaction;

        public AcknowledgeableMessage(
            string content,
            string id,
            IDbTransaction transaction)
        {
            Content = content;
            Id = id;
            _transaction = transaction;
        }

        public string Content { get; }
        public string Id { get; }
        

        public void Acknowledge()
        {
            _transaction?.Commit();
        }

        public void Abandon()
        {
            _transaction?.Rollback();
        }
    }
}