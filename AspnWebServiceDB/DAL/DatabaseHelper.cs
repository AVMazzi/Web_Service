using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Configuration;

/*

<add key="CONN_STR" value="server=.\SQLEXPRESS;initial catalog=BarreiraFiscal;user id=sa;password=sasa;" />
    <add key="DB_PROVIDER" value="System.Data.SqlClient" />

*/

namespace DAL
{
    public class DatabaseHelper
    {
        #region Transaction

        // Objeto de conexão utilizado para Transações
        private DbConnection connection = null;

        /// <summary>
        /// Propriedade de conexão para acesso a banco (Utilizada apenas em Transações)
        /// </summary>
        private DbConnection Connection
        {
            get
            {
                try
                {
                    if (connection == null)
                    {
                        connection = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["connectionString"].ProviderName).CreateConnection();
                    }
                    if ((connection != null) && (connection.State != ConnectionState.Open))
                    {
                        connection.ConnectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
                        connection.Open();
                    }
                }
                catch
                {
                    return null;
                }

                return connection;
            }
        }

        /// <summary>
        /// Fecha a conexão com o Banco de Dados (Utilizado apenas em Transações)
        /// </summary>
        public bool CloseConnection()
        {
            try
            {
                if ((Connection != null) && (Connection.State != ConnectionState.Closed))
                {
                    Connection.Close();
                    Connection.Dispose();
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

        private DbTransaction transaction = null;

        /// <summary>
        /// Abre uma Transação
        /// </summary>
        /// <returns>Retorna se conseguiu abrir a transação com sucesso</returns>
        public bool StartTransaction()
        {
            try
            {
                if ((transaction == null) && (Connection != null) && (Connection.State == ConnectionState.Open))
                {
                    transaction = Connection.BeginTransaction();
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Salva todos os commandos executados no Banco de Dados e fecha a Conexão com o banco de dados
        /// </summary>
        /// <returns>Retorna se conseguiu executar a transação com sucesso</returns>
        public bool Commit()
        {
            try
            {
                if ((transaction != null) && (Connection != null) && (Connection.State == ConnectionState.Open))
                {
                    transaction.Commit();
                    transaction = null;
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch
            {
                return false;
            }
            finally
            {
                CloseConnection();
            }
        }

        /// <summary>
        /// Cancela a operação de Transação aberta e fecha a Conexão com o banco de dados
        /// </summary>
        /// <returns>Retorna se conseguiu ou não cancelar a Transação</returns>
        public bool Rollback()
        {
            try
            {
                if ((transaction != null) && (Connection != null) && (Connection.State == ConnectionState.Open))
                {
                    transaction.Rollback();
                    transaction = null;
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch
            {
                return false;
            }
            finally
            {
                CloseConnection();
            }
        }

        #endregion

        #region Select

        #region DataReader

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>IDataReader</returns>
        /// <remarks>É NECESSÁRIO chamar os métodos "Close()" e "Dispose()" depois de utilizar o DataReader</remarks>
        public IDataReader GetDataReader(string query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                DbCommand cmd = conn.CreateCommand();
                cmd.Connection = conn;
                cmd.CommandText = query ?? string.Empty;
                cmd.CommandType = CommandType.Text;

                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>IDataReader</returns>
        /// <remarks>É NECESSÁRIO chamar os métodos "Close()" e "Dispose()" depois de utilizar o DataReader</remarks>
        public IDataReader GetDataReader(StringBuilder query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                DbCommand cmd = conn.CreateCommand();
                cmd.Connection = conn;
                if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                cmd.CommandType = CommandType.Text;

                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>IDataReader</returns>
        /// <remarks>É NECESSÁRIO chamar os métodos "Close()" e "Dispose()" depois de utilizar o DataReader</remarks>
        public IDataReader GetDataReader(string query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                DbCommand cmd = conn.CreateCommand();
                cmd.Connection = conn;
                cmd.CommandText = query ?? string.Empty;
                cmd.CommandType = CommandType.Text;

                if ((parameters != null) && (parameters.Count > 0))
                {
                    foreach (DictionaryEntry de in parameters)
                    {
                        DbParameter db = cmd.CreateParameter();
                        db.ParameterName = de.Key.ToString();
                        db.Value = de.Value;
                        cmd.Parameters.Add(db);
                    }
                }

                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>IDataReader</returns>
        /// <remarks>É NECESSÁRIO chamar os métodos "Close()" e "Dispose()" depois de utilizar o DataReader</remarks>
        public IDataReader GetDataReader(StringBuilder query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                DbCommand cmd = conn.CreateCommand();
                cmd.Connection = conn;
                if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                cmd.CommandType = CommandType.Text;

                if ((parameters != null) && (parameters.Count > 0))
                {
                    foreach (DictionaryEntry de in parameters)
                    {
                        DbParameter db = cmd.CreateParameter();
                        db.ParameterName = de.Key.ToString();
                        db.Value = de.Value;
                        cmd.Parameters.Add(db);
                    }
                }

                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>IDataReader</returns>
        /// <remarks>É NECESSÁRIO chamar os métodos "Close()" e "Dispose()" depois de utilizar o DataReader</remarks>
        public IDataReader GetDataReaderProc(string procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                DbCommand cmd = conn.CreateCommand();
                cmd.Connection = conn;
                cmd.CommandText = procedure ?? string.Empty;
                cmd.CommandType = CommandType.StoredProcedure;

                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>IDataReader</returns>
        /// <remarks>É NECESSÁRIO chamar os métodos "Close()" e "Dispose()" depois de utilizar o DataReader</remarks>
        public IDataReader GetDataReaderProc(StringBuilder procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                DbCommand cmd = conn.CreateCommand();
                cmd.Connection = conn;
                if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                cmd.CommandType = CommandType.StoredProcedure;

                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>IDataReader</returns>
        /// <remarks>É NECESSÁRIO chamar os métodos "Close()" e "Dispose()" depois de utilizar o DataReader</remarks>
        public IDataReader GetDataReaderProc(string procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                DbCommand cmd = conn.CreateCommand();
                cmd.Connection = conn;
                cmd.CommandText = procedure ?? string.Empty;
                cmd.CommandType = CommandType.StoredProcedure;

                if ((parameters != null) && (parameters.Count > 0))
                {
                    foreach (DictionaryEntry de in parameters)
                    {
                        DbParameter db = cmd.CreateParameter();
                        db.ParameterName = de.Key.ToString();
                        db.Value = de.Value;
                        cmd.Parameters.Add(db);
                    }
                }

                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>IDataReader</returns>
        /// <remarks>É NECESSÁRIO chamar os métodos "Close()" e "Dispose()" depois de utilizar o DataReader</remarks>
        public IDataReader GetDataReaderProc(StringBuilder procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                DbCommand cmd = conn.CreateCommand();
                cmd.Connection = conn;
                if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                cmd.CommandType = CommandType.StoredProcedure;

                if ((parameters != null) && (parameters.Count > 0))
                {
                    foreach (DictionaryEntry de in parameters)
                    {
                        DbParameter db = cmd.CreateParameter();
                        db.ParameterName = de.Key.ToString();
                        db.Value = de.Value;
                        cmd.Parameters.Add(db);
                    }
                }

                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>IDataReader</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação - Os métodos "Close()" e "Dispose"
        /// fecham apenas o DataReader</remarks>
        public IDataReader GetDataReaderTransaction(string query)
        {
            try
            {
                DbCommand cmd = Connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = query ?? string.Empty;
                cmd.CommandType = CommandType.Text;

                return cmd.ExecuteReader();
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>IDataReader</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação - Os métodos "Close()" e "Dispose"
        /// fecham apenas o DataReader</remarks>
        public IDataReader GetDataReaderTransaction(StringBuilder query)
        {
            try
            {
                DbCommand cmd = Connection.CreateCommand();
                cmd.Transaction = transaction;
                if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                cmd.CommandType = CommandType.Text;

                return cmd.ExecuteReader();
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>IDataReader</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação - Os métodos "Close()" e "Dispose"
        /// fecham apenas o DataReader</remarks>
        public IDataReader GetDataReaderTransaction(string query, HybridDictionary parameters)
        {
            try
            {
                DbCommand cmd = Connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = query ?? string.Empty;
                cmd.CommandType = CommandType.Text;

                if ((parameters != null) && (parameters.Count > 0))
                {
                    foreach (DictionaryEntry de in parameters)
                    {
                        DbParameter db = cmd.CreateParameter();
                        db.ParameterName = de.Key.ToString();
                        db.Value = de.Value;
                        cmd.Parameters.Add(db);
                    }
                }

                return cmd.ExecuteReader();
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>IDataReader</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação - Os métodos "Close()" e "Dispose"
        /// fecham apenas o DataReader</remarks>
        public IDataReader GetDataReaderTransaction(StringBuilder query, HybridDictionary parameters)
        {
            try
            {
                DbCommand cmd = Connection.CreateCommand();
                cmd.Transaction = transaction;
                if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                cmd.CommandType = CommandType.Text;

                if ((parameters != null) && (parameters.Count > 0))
                {
                    foreach (DictionaryEntry de in parameters)
                    {
                        DbParameter db = cmd.CreateParameter();
                        db.ParameterName = de.Key.ToString();
                        db.Value = de.Value;
                        cmd.Parameters.Add(db);
                    }
                }

                return cmd.ExecuteReader();
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>IDataReader</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação - Os métodos "Close()" e "Dispose"
        /// fecham apenas o DataReader</remarks>
        public IDataReader GetDataReaderTransactionProc(string procedure)
        {
            try
            {
                DbCommand cmd = Connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = procedure ?? string.Empty;
                cmd.CommandType = CommandType.StoredProcedure;

                return cmd.ExecuteReader();
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>IDataReader</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação - Os métodos "Close()" e "Dispose"
        /// fecham apenas o DataReader</remarks>
        public IDataReader GetDataReaderTransactionProc(StringBuilder procedure)
        {
            try
            {
                DbCommand cmd = Connection.CreateCommand();
                cmd.Transaction = transaction;
                if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                cmd.CommandType = CommandType.StoredProcedure;

                return cmd.ExecuteReader();
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>IDataReader</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação - Os métodos "Close()" e "Dispose"
        /// fecham apenas o DataReader</remarks>
        public IDataReader GetDataReaderTransactionProc(string procedure, HybridDictionary parameters)
        {
            try
            {
                DbCommand cmd = Connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandText = procedure ?? string.Empty;
                cmd.CommandType = CommandType.StoredProcedure;

                if ((parameters != null) && (parameters.Count > 0))
                {
                    foreach (DictionaryEntry de in parameters)
                    {
                        DbParameter db = cmd.CreateParameter();
                        db.ParameterName = de.Key.ToString();
                        db.Value = de.Value;
                        cmd.Parameters.Add(db);
                    }
                }

                return cmd.ExecuteReader();
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um IDataReader preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>IDataReader</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação - Os métodos "Close()" e "Dispose"
        /// fecham apenas o DataReader</remarks>
        public IDataReader GetDataReaderTransactionProc(StringBuilder procedure, HybridDictionary parameters)
        {
            try
            {
                DbCommand cmd = Connection.CreateCommand();
                cmd.Transaction = transaction;
                if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                cmd.CommandType = CommandType.StoredProcedure;

                if ((parameters != null) && (parameters.Count > 0))
                {
                    foreach (DictionaryEntry de in parameters)
                    {
                        DbParameter db = cmd.CreateParameter();
                        db.ParameterName = de.Key.ToString();
                        db.Value = de.Value;
                        cmd.Parameters.Add(db);
                    }
                }

                return cmd.ExecuteReader();
            }
            catch
            {
                throw;
            }
        }

        #endregion

        #region DataTable

        /// <summary>
        /// Retorna um DataTable preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>DataTable</returns>
        public DataTable GetDataTable(string query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["connectionString"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>DataTable</returns>
        public DataTable GetDataTable(StringBuilder query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch(Exception e)
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataTable</returns>
        public DataTable GetDataTable(string query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataTable</returns>
        public DataTable GetDataTable(StringBuilder query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>DataTable</returns>
        public DataTable GetDataTableProc(string procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>DataTable</returns>
        public DataTable GetDataTableProc(StringBuilder procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataTable</returns>
        public DataTable GetDataTableProc(string procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataTable</returns>
        public DataTable GetDataTableProc(StringBuilder procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// <param name="query">Comando SQL</param>
        /// </summary>
        /// <returns>DataTable</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataTable GetDataTableTransaction(string query)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// <param name="query">Comando SQL</param>
        /// </summary>
        /// <returns>DataTable</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataTable GetDataTableTransaction(StringBuilder query)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataTable</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataTable GetDataTableTransaction(string query, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataTable</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataTable GetDataTableTransaction(StringBuilder query, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// <param name="procedure">Nome da Procedure</param>
        /// </summary>
        /// <returns>DataTable</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataTable GetDataTableTransactionProc(string procedure)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// <param name="procedure">Nome da Procedure</param>
        /// </summary>
        /// <returns>DataTable</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataTable GetDataTableTransactionProc(StringBuilder procedure)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// <param name="procedure">Nome da Procedure</param>        
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataTable</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataTable GetDataTableTransactionProc(string procedure, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataTable preenchido
        /// <param name="procedure">Nome da Procedure</param>        
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataTable</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataTable GetDataTableTransactionProc(StringBuilder procedure, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        #endregion

        #region DataSet

        /// <summary>
        /// Retorna um DataSet preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSet(string query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSet(StringBuilder query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSet(string query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSet(StringBuilder query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSetProc(string procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSetProc(StringBuilder procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSetProc(string procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSetProc(StringBuilder procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// <param name="query">Comando SQL</param>
        /// </summary>
        /// <returns>DataSet</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataSet GetDataSetTransaction(string query)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// <param name="query">Comando SQL</param>
        /// </summary>
        /// <returns>DataSet</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataSet GetDataSetTransaction(StringBuilder query)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataSet</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataSet GetDataSetTransaction(string query, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataSet</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataSet GetDataSetTransaction(StringBuilder query, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// <param name="procedure">Nome da Procedure</param>
        /// </summary>
        /// <returns>DataSet</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataSet GetDataSetTransactionProc(string procedure)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// <param name="procedure">Nome da Procedure</param>
        /// </summary>
        /// <returns>DataSet</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataSet GetDataSetTransactionProc(StringBuilder procedure)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// <param name="procedure">Nome da Procedure</param>        
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataSet</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataSet GetDataSetTransactionProc(string procedure, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataSet preenchido
        /// <param name="procedure">Nome da Procedure</param>        
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataSet</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataSet GetDataSetTransactionProc(StringBuilder procedure, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());

                        using (DataSet ds = new DataSet())
                        {
                            ds.Tables.Add(dt);
                            return ds;
                        }
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        #endregion

        #region DataView

        /// <summary>
        /// Retorna um DataView preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>DataView</returns>
        public DataView GetDataView(string query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>DataView</returns>
        public DataView GetDataView(StringBuilder query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataView</returns>
        public DataView GetDataView(string query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataView</returns>
        public DataView GetDataView(StringBuilder query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>DataView</returns>
        public DataView GetDataViewProc(string procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>DataView</returns>
        public DataView GetDataViewProc(StringBuilder procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataView</returns>
        public DataView GetDataViewProc(string procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>DataView</returns>
        public DataView GetDataViewProc(StringBuilder procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// <param name="query">Comando SQL</param>
        /// </summary>
        /// <returns>DataView</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataView GetDataViewTransaction(string query)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// <param name="query">Comando SQL</param>
        /// </summary>
        /// <returns>DataView</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataView GetDataViewTransaction(StringBuilder query)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataView</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataView GetDataViewTransaction(string query, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataView</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataView GetDataViewTransaction(StringBuilder query, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// <param name="procedure">Nome da Procedure</param>
        /// </summary>
        /// <returns>DataView</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataView GetDataViewTransactionProc(string procedure)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// <param name="procedure">Nome da Procedure</param>
        /// </summary>
        /// <returns>DataView</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataView GetDataViewTransactionProc(StringBuilder procedure)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// <param name="procedure">Nome da Procedure</param>        
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataView</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataView GetDataViewTransactionProc(string procedure, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Retorna um DataView preenchido
        /// <param name="procedure">Nome da Procedure</param>        
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// </summary>
        /// <returns>DataView</returns>
        /// <remarks>O Fechamento de conexão deve ocorrer na Transação</remarks>
        public DataView GetDataViewTransactionProc(StringBuilder procedure, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    if ((parameters != null) && (parameters.Count > 0))
                    {
                        foreach (DictionaryEntry de in parameters)
                        {
                            DbParameter db = cmd.CreateParameter();
                            db.ParameterName = de.Key.ToString();
                            db.Value = de.Value;
                            cmd.Parameters.Add(db);
                        }
                    }

                    using (DataTable dt = new DataTable())
                    {
                        dt.Load(cmd.ExecuteReader());
                        return dt.DefaultView;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        #endregion

        #endregion

        #region Insert, Update e Delete (ExecuteNonQuery)


        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>true ou false</returns>
        
        public bool ExecuteNonQuery(string query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteNonQueryParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQuery(StringBuilder query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteNonQueryParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQuery(string query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteNonQueryParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQuery(StringBuilder query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteNonQueryParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryProc(string procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteNonQueryParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryProc(StringBuilder procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteNonQueryParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryProc(string procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteNonQueryParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryProc(StringBuilder procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteNonQueryParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION)
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryTransaction(string query)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteNonQueryParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION)
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryTransaction(StringBuilder query)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteNonQueryParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION)
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryTransaction(string query, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteNonQueryParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION)
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryTransaction(StringBuilder query, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteNonQueryParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION e PROCEDURE)
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryTransactionProc(string procedure)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteNonQueryParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION e PROCEDURE)
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryTransactionProc(StringBuilder procedure)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteNonQueryParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION e PROCEDURE)
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryTransactionProc(string procedure, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteNonQueryParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION e PROCEDURE)
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>true ou false</returns>
        public bool ExecuteNonQueryTransactionProc(StringBuilder procedure, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteNonQueryParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
        }

        #endregion

        #region Insert, Update e Delete (ExecuteScalar)

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>object</returns>
        public object ExecuteScalar(string query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteScalarParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>object</returns>
        public object ExecuteScalar(StringBuilder query)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteScalarParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>object</returns>
        public object ExecuteScalar(string query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteScalarParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>object</returns>
        public object ExecuteScalar(StringBuilder query, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteScalarParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>object</returns>
        public object ExecuteScalarProc(string procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteScalarParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>object</returns>
        public object ExecuteScalarProc(StringBuilder procedure)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteScalarParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>object</returns>
        public object ExecuteScalarProc(string procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteScalarParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>object</returns>
        public object ExecuteScalarProc(StringBuilder procedure, HybridDictionary parameters)
        {
            DbConnection conn = null;

            try
            {
                conn = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["ERPConnection"].ProviderName).CreateConnection();
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["ERPConnection"].ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.Connection = conn;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteScalarParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if ((conn != null) && (conn.State != ConnectionState.Closed))
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION)
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>object</returns>
        public object ExecuteScalarTransaction(string query)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteScalarParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION)
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <returns>object</returns>
        public object ExecuteScalarTransaction(StringBuilder query)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteScalarParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION)
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>object</returns>
        public object ExecuteScalarTransaction(string query, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query ?? string.Empty;
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteScalarParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION)
        /// </summary>
        /// <param name="query">Comando SQL</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>object</returns>
        public object ExecuteScalarTransaction(StringBuilder query, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (query != null) { cmd.CommandText = query.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.Text;

                    return AddAndExecuteScalarParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION e PROCEDURE)
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>object</returns>
        public object ExecuteScalarTransactionProc(string procedure)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteScalarParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION e PROCEDURE)
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <returns>object</returns>
        public object ExecuteScalarTransactionProc(StringBuilder procedure)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteScalarParameters(cmd, null);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION e PROCEDURE)
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>object</returns>
        public object ExecuteScalarTransactionProc(string procedure, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = procedure ?? string.Empty;
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteScalarParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Executa uma query de INSERT, UPDATE ou DELETE (TRANSACTION e PROCEDURE)
        /// </summary>
        /// <param name="procedure">Nome da Procedure</param>
        /// <param name="parameters">HybridDictionary de parâmetros (Chave / Valor)</param>
        /// <returns>object</returns>
        public object ExecuteScalarTransactionProc(StringBuilder procedure, HybridDictionary parameters)
        {
            try
            {
                using (DbCommand cmd = Connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (procedure != null) { cmd.CommandText = procedure.ToString(); } else { cmd.CommandText = string.Empty; }
                    cmd.CommandType = CommandType.StoredProcedure;

                    return AddAndExecuteScalarParameters(cmd, parameters);
                }
            }
            catch
            {
                throw;
            }
        }

        #endregion

        #region Internal Methods

        private bool AddAndExecuteNonQueryParameters(DbCommand cmd, IDictionary parameters)
        {
            // Setando parâmetros
            if ((parameters != null) && (parameters.Count > 0))
            {
                foreach (DictionaryEntry de in parameters)
                {
                    DbParameter db = cmd.CreateParameter();
                    db.ParameterName = de.Key.ToString();
                    db.Value = de.Value;
                    cmd.Parameters.Add(db);
                }
            }

            // Executa o comando o retorna o número de linhas afetadas
            return cmd.ExecuteNonQuery() > 0 ? true : false;
        }

        private object AddAndExecuteScalarParameters(DbCommand cmd, IDictionary parameters)
        {
            // Setando parâmetros
            if ((parameters != null) && (parameters.Count > 0))
            {
                foreach (DictionaryEntry de in parameters)
                {
                    DbParameter db = cmd.CreateParameter();
                    db.ParameterName = de.Key.ToString();
                    db.Value = de.Value;
                    cmd.Parameters.Add(db);
                }
            }

            // Executa o comando e retorna um objeto com a 1ª linha da 1ª coluna
            return cmd.ExecuteScalar();
        }

        #endregion
    }

    public static class MyDalExtensions
    {
        #region Create DataReader

        public static T DataReaderToEntity<T>(this IDataReader reader) where T : class
        {
            if ((reader != null) && (!reader.IsClosed))
            {
                try
                {
                    var properties = typeof(T).GetProperties();
                    T item = null;

                    while (reader.Read())
                    {
                        item = Activator.CreateInstance(typeof(T)) as T;
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var property = properties.Where(p => p.Name.Equals(reader.GetName(i))).FirstOrDefault();
                            if (property != null)
                            {
                                var tipo = reader[i].GetType();
                                if (tipo.Equals(typeof(DBNull)))
                                {
                                    property.SetValue(item, null, null);
                                }
                                else
                                {
                                    property.SetValue(item, reader[i], null);
                                }
                            }
                        }
                    }
                    return item;
                }
                catch
                {
                    throw;
                }
                finally
                {
                    reader.Dispose();
                }
            }
            else
            {
                return null;
            }
        }

        public static List<T> DataReaderToList<T>(this IDataReader reader) where T : class
        {
            if ((reader != null) && (!reader.IsClosed))
            {
                try
                {
                    List<T> list = null;
                    var properties = typeof(T).GetProperties();

                    while (reader.Read())
                    {
                        list = new List<T>();
                        T item = Activator.CreateInstance(typeof(T)) as T;

                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var property = properties.Where(p => p.Name.Equals(reader.GetName(i))).FirstOrDefault();
                            if (property != null)
                            {
                                var tipo = reader[i].GetType();
                                if (tipo.Equals(typeof(DBNull)))
                                {
                                    property.SetValue(item, null, null);
                                }
                                else
                                {
                                    property.SetValue(item, reader[i], null);
                                }
                            }
                        }
                        list.Add(item);
                    }
                    return list;
                }
                catch
                {
                    throw;
                }
                finally
                {
                    reader.Dispose();
                }
            }
            else
            {
                return null;
            }
        }

        #endregion
    }
}