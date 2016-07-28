#region "Comments"

//'***************************************************************************************************************
//'*  Company Name            :
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By              :    Santosh on 10-May-16
//'*  Module/Project Leader   :    Santosh Pisipati
//'*  Description             :
//'*  Module/Form/Class Name  :
//'*  Configuration ID        :
//'***************************************************************************************************************
//'*  Revision History
//'***************************************************************************************************************
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.ManagedDataAccess.Client;
using System;
using System.Configuration;
using System.Data;
using System.Diagnostics;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class WorkFlow
    {
        #region "LIst of Members"

        private static string UserName = ConfigurationManager.AppSettings["UsrStr"];

        /// <summary>
        /// The _ connection
        /// </summary>
        private OracleConnection _Connection;

        /// <summary>
        /// The _ data set
        /// </summary>
        private DataSet _DataSet;

        /// <summary>
        /// The _ data adapter
        /// </summary>
        private OracleDataAdapter _DataAdapter;

        /// <summary>
        /// The _ data reader
        /// </summary>
        private OracleDataReader _DataReader;

        /// <summary>
        /// The _ data view
        /// </summary>
        private DataView _DataView;

        /// <summary>
        /// The _ data table
        /// </summary>
        private DataTable _DataTable;

        /// <summary>
        /// The _ command
        /// </summary>
        private OracleCommand _Command;

        //Private Shared _UserName As String
        /// <summary>
        /// The QFLX connection string
        /// </summary>
        private string QflxConnectionString;

        #endregion "LIst of Members"

        #region "List of Properties"

        /// <summary>
        /// Gets or sets my connection.
        /// </summary>
        /// <value>
        /// My connection.
        /// </value>
        public OracleConnection MyConnection
        {
            get { return _Connection; }
            set { _Connection = value; }
        }

        /// <summary>
        /// Gets or sets my data set.
        /// </summary>
        /// <value>
        /// My data set.
        /// </value>
        public DataSet MyDataSet
        {
            get { return _DataSet; }
            set { _DataSet = value; }
        }

        /// <summary>
        /// Gets or sets my data adapter.
        /// </summary>
        /// <value>
        /// My data adapter.
        /// </value>
        public OracleDataAdapter MyDataAdapter
        {
            get { return _DataAdapter; }
            set { _DataAdapter = value; }
        }

        /// <summary>
        /// Gets or sets my data reader.
        /// </summary>
        /// <value>
        /// My data reader.
        /// </value>
        public OracleDataReader MyDataReader
        {
            get { return _DataReader; }
            set { _DataReader = value; }
        }

        /// <summary>
        /// Gets or sets my data view.
        /// </summary>
        /// <value>
        /// My data view.
        /// </value>
        public DataView MyDataView
        {
            get { return _DataView; }
            set { _DataView = value; }
        }

        /// <summary>
        /// Gets or sets my data table.
        /// </summary>
        /// <value>
        /// My data table.
        /// </value>
        public DataTable MyDataTable
        {
            get { return _DataTable; }
            set { _DataTable = value; }
        }

        /// <summary>
        /// Gets or sets my command.
        /// </summary>
        /// <value>
        /// My command.
        /// </value>
        public OracleCommand MyCommand
        {
            get { return _Command; }
            set { _Command = value; }
        }

        /// <summary>
        /// Gets the name of my user.
        /// </summary>
        /// <value>
        /// The name of my user.
        /// </value>
        public string MyUserName
        {
            get { return UserName; }
        }

        #endregion "List of Properties"

        #region "Constructors"

        /// <summary>
        /// Initializes a new instance of the <see cref="WorkFlow"/> class.
        /// </summary>
        public WorkFlow()
        {
            _Connection = new OracleConnection();
            QflxConnectionString = ConfigurationManager.AppSettings["ConStr"];
            _Connection.ConnectionString = QflxConnectionString;
            _Command = new OracleCommand();
            _DataSet = new DataSet();
            _DataTable = new DataTable();
            _DataAdapter = new OracleDataAdapter();
        }

        #endregion "Constructors"

        #region "Methods and Functions"

        /// <summary>
        /// Executes the commands.
        /// </summary>
        /// <param name="SQLQuery">The SQL query.</param>
        /// <returns></returns>
        public bool ExecuteCommands(string SQLQuery = "")
        {
            try
            {
                object ReturnValue = null;
                string connectionString = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString;
                _DataSet = new DataSet();
                //Code End
                _Command.Connection = new OracleConnection(connectionString);
                if (_Command.Connection.State == ConnectionState.Broken | _Command.Connection.State == ConnectionState.Closed | _Command.Connection == null)
                {
                    _Command.Connection.Open();
                }
                if (!string.IsNullOrEmpty(SQLQuery))
                {
                    _Command.CommandText = SQLQuery;
                    _Command.CommandType = CommandType.Text;
                }
                ReturnValue = _Command.ExecuteNonQuery();
                if (ReturnValue == null)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (OracleException sqlEXP)
            {
                throw sqlEXP;
            }
            catch (Exception EXP)
            {
                throw EXP;
            }
            finally
            {
                _Command.Cancel();
                _Connection.Close();
            }
        }

        /// <summary>
        /// Executes the scaler.
        /// </summary>
        /// <param name="SQLQuery">The SQL query.</param>
        /// <returns></returns>
        public string ExecuteScaler(string SQLQuery = "")
        {
            string ConStatus = null;
            object ReturnValue = null;
            try
            {
                string connectionString = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString;
                _DataSet = new DataSet();
                //Code End
                _Command.Connection = new OracleConnection(connectionString);
                if (_Command.Connection.State == ConnectionState.Broken | _Command.Connection.State == ConnectionState.Closed | _Command.Connection == null)
                {
                    _Command.Connection.Open();
                    ConStatus = "NEW";
                }
                else
                {
                    ConStatus = "OLD";
                }
                if (!string.IsNullOrEmpty(SQLQuery))
                {
                    _Command.CommandText = SQLQuery;
                    _Command.CommandType = CommandType.Text;
                    _Command.Parameters.Clear();
                }

                ReturnValue = _Command.ExecuteScalar();
                if (ReturnValue == null)
                {
                    return "";
                }
                else
                {
                    return Convert.ToString(ReturnValue);
                }
            }
            catch (OracleException sqlEXP)
            {
                throw sqlEXP;
            }
            catch (Exception EXP)
            {
                throw EXP;
            }
            finally
            {
                _Command.Cancel();
                _Connection.Close();
            }
        }

        /// <summary>
        /// Gets the data reader.
        /// </summary>
        /// <param name="SQLQuery">The SQL query.</param>
        /// <returns></returns>
        public OracleDataReader GetDataReader(string SQLQuery)
        {
            string ConStatus = null;
            try
            {
                string connectionString = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString;
                //Code End
                _Command.Connection = new OracleConnection(connectionString);
                if (_Command.Connection.State == ConnectionState.Broken | _Command.Connection.State == ConnectionState.Closed | _Command.Connection == null)
                {
                    _Command.Connection.Open();
                    ConStatus = "NEW";
                }
                else
                {
                    ConStatus = "OLD";
                }
                _Command = new OracleCommand(SQLQuery, _Command.Connection);
                _DataReader = _Command.ExecuteReader(CommandBehavior.CloseConnection);
                return _DataReader;
            }
            catch (OracleException eSQL)
            {
                throw eSQL;
            }
            catch (Exception eX)
            {
                throw eX;
            }
            finally
            {
                if (ConStatus == "NEW")
                {
                }
            }
        }

        /// <summary>
        /// Gets the data set.
        /// </summary>
        /// <param name="SQLQuery">The SQL query.</param>
        /// <returns></returns>
        public DataSet GetDataSet(string SQLQuery)
        {
            try
            {
                string connectionString = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString;
                _DataSet = new DataSet();
                //Code End
                _Command.Connection = new OracleConnection(connectionString);
                if (_Command.Connection.State != ConnectionState.Open)
                {
                    _Command.Connection.Open();
                }
                _Command.CommandType = CommandType.Text;
                _Command.CommandText = SQLQuery;
                _DataAdapter.SelectCommand = _Command;
                _DataAdapter.Fill(_DataSet);
                return _DataSet;
            }
            catch (OracleException sqlEXP)
            {
                throw sqlEXP;
            }
            catch (Exception EXP)
            {
                throw EXP;
            }
            finally
            {
                _Command.Cancel();
                _Connection.Close();
            }
        }

        /// <summary>
        /// Gets the data table.
        /// </summary>
        /// <param name="SQLQuery">The SQL query.</param>
        /// <returns></returns>
        public DataTable GetDataTable(string SQLQuery)
        {
            try
            {
                string connectionString = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString;
                _Command.Connection = new OracleConnection(connectionString);
                _DataTable = new DataTable();
                if (_Command.Connection.State != ConnectionState.Open)
                {
                    _Command.Connection.Open();
                }
                _Command.CommandType = CommandType.Text;
                _Command.CommandText = SQLQuery;
                _DataAdapter.SelectCommand = _Command;
                _DataAdapter.Fill(_DataTable);
                return _DataTable;
            }
            catch (OracleException sqlEXP)
            {
                throw sqlEXP;
            }
            catch (Exception EXP)
            {
                throw EXP;
            }
            finally
            {
                _Command.Cancel();
                _Connection.Close();
            }
        }

        /// <summary>
        /// Gets the data adapter.
        /// </summary>
        /// <param name="SQLQuery">The SQL query.</param>
        /// <returns></returns>
        public OracleDataAdapter GetDataAdapter(string SQLQuery)
        {
            try
            {
                string connectionString = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString;
                _Command.Connection = new OracleConnection(connectionString);
                if (_Command.Connection.State != ConnectionState.Open)
                {
                    _Command.Connection.Open();
                }
                OracleDataAdapter DA1 = new OracleDataAdapter(SQLQuery, _Command.Connection);
                return DA1;
            }
            catch (OracleException sqlEXP)
            {
                throw sqlEXP;
            }
            catch (Exception EXP)
            {
                throw EXP;
            }
            finally
            {
                _Command.Cancel();
                _Connection.Close();
            }
        }

        /// <summary>
        /// Executes the transaction commands.
        /// </summary>
        /// <param name="SQLQuery">The SQL query.</param>
        /// <returns></returns>
        public bool ExecuteTransactionCommands(string SQLQuery = "")
        {
            try
            {
                if (!string.IsNullOrEmpty(SQLQuery))
                {
                    _Command.CommandText = SQLQuery;
                    _Command.CommandType = CommandType.Text;
                }
                if (_Command.ExecuteNonQuery() > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (OracleException sqlEXP)
            {
                throw sqlEXP;
            }
            catch (Exception EXP)
            {
                throw EXP;
            }
            finally
            {
                _Command.Cancel();
            }
        }

        /// <summary>
        /// Opens the connection.
        /// </summary>
        public void OpenConnection()
        {
            try
            {
                string connectionString = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString;
                _Connection = new OracleConnection(connectionString);
                if (_Connection.State == ConnectionState.Broken | _Connection.State == ConnectionState.Closed | _Connection == null)
                {
                    _Connection.Open();
                    _Command.Connection = _Connection;
                }
            }
            catch (Exception ex)
            {
                Debug.Write(ex.Message);
                Debug.Write(ex.Source);
                Debug.Write(ex.StackTrace);
                Debug.Write(ex.HelpLink);
                Debug.Flush();
            }
        }

        /// <summary>
        /// Closes the connection.
        /// </summary>
        public void CloseConnection()
        {
            try
            {
                string connectionString = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString;
                _Connection = new OracleConnection(connectionString);
                _Command.Connection = _Connection;
                if (_Connection.State == ConnectionState.Broken | _Connection.State == ConnectionState.Executing | _Connection.State == ConnectionState.Open | _Connection == null)
                {
                    _Connection.Close();
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Gets the data table.
        /// </summary>
        /// <param name="PacakageName">Name of the pacakage.</param>
        /// <param name="ProcedureName">Name of the procedure.</param>
        /// <returns></returns>
        public DataTable GetDataTable(string PacakageName, string ProcedureName)
        {
            try
            {
                UserName = ConfigurationManager.AppSettings["UsrStr"];
                string connectionString = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString;
                _Connection = new OracleConnection(connectionString);
                _DataTable = new DataTable();
                _Command.Connection = _Connection;

                if (_Connection.State != ConnectionState.Open)
                {
                    _Connection.Open();
                }

                _Command.CommandType = CommandType.StoredProcedure;
                _Command.CommandText = UserName + "." + PacakageName + "." + ProcedureName;

                _DataAdapter.SelectCommand = _Command;
                _DataAdapter.Fill(_DataTable);
                return _DataTable;
            }
            catch (OracleException sqlEXP)
            {
                throw sqlEXP;
            }
            catch (Exception EXP)
            {
                throw EXP;
            }
            finally
            {
                _Command.Cancel();
                _Connection.Close();
            }
        }

        /// <summary>
        /// Gets the data set.
        /// </summary>
        /// <param name="PacakageName">Name of the pacakage.</param>
        /// <param name="ProcedureName">Name of the procedure.</param>
        /// <returns></returns>
        public DataSet GetDataSet(string PacakageName, string ProcedureName)
        {
            try
            {
                string connectionString = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString;
                UserName = ConfigurationManager.AppSettings["UsrStr"];
                _Connection = new OracleConnection(connectionString);
                _DataSet = new DataSet();
                _Command.Connection = _Connection;

                if (_Connection.State != ConnectionState.Open)
                {
                    _Connection.Open();
                }

                _Command.CommandType = CommandType.StoredProcedure;
                _Command.CommandText = UserName + "." + PacakageName + "." + ProcedureName;

                _DataAdapter.SelectCommand = _Command;
                _DataAdapter.Fill(_DataSet);
                return _DataSet;
            }
            catch (OracleException sqlEXP)
            {
                throw sqlEXP;
            }
            catch (Exception EXP)
            {
                throw EXP;
            }
            finally
            {
                _Command.Cancel();
                _Connection.Close();
            }
        }

        /// <summary>
        /// Gets the data reader.
        /// </summary>
        /// <param name="PacakageName">Name of the pacakage.</param>
        /// <param name="ProcedureName">Name of the procedure.</param>
        /// <returns></returns>
        public OracleDataReader GetDataReader(string PacakageName, string ProcedureName)
        {
            string ConStatus = null;
            try
            {
                UserName = ConfigurationManager.AppSettings["UsrStr"];
                string connectionString = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString;
                _Connection = new OracleConnection(connectionString);
                if (_Connection.State == ConnectionState.Broken | _Connection.State == ConnectionState.Closed | _Connection == null)
                {
                    _Connection.Open();
                    ConStatus = "NEW";
                }
                else
                {
                    ConStatus = "OLD";
                }

                _Command.Connection = _Connection;
                _Command.CommandType = CommandType.StoredProcedure;
                _Command.CommandText = UserName + "." + PacakageName + "." + ProcedureName;
                _DataReader = _Command.ExecuteReader(CommandBehavior.CloseConnection);

                return _DataReader;
            }
            catch (OracleException eSQL)
            {
                throw eSQL;
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }

        #endregion "Methods and Functions"

    }
}