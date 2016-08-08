#region "Comments"

//'***************************************************************************************************************
//'*  Company Name            :
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By              :    Santosh on 31-May-16
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

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsAirFreightSlabs : CommonFeatures
    {
        /// <summary>
        /// The m_ type data set
        /// </summary>
        private static DataSet M_TypeDataSet = new DataSet();

        #region "Property "

        /// <summary>
        /// Gets the type data set.
        /// </summary>
        /// <value>
        /// The type data set.
        /// </value>
        public static DataSet TypeDataSet
        {
            get { return M_TypeDataSet; }
        }

        #endregion "Property "

        #region "Constructor"

        /// <summary>
        /// Initializes a new instance of the <see cref="clsAirFreightSlabs"/> class.
        /// </summary>
        public clsAirFreightSlabs()
        {
            string strTypeSQL = null;
            strTypeSQL = "SELECT 0 AIRFREIGHT_SLABS_TBL_PK, ' ' BREAKPOINT_ID  FROM  DUAL UNION ";
            strTypeSQL += "SELECT A.AIRFREIGHT_SLABS_TBL_PK,A.BREAKPOINT_ID  FROM  AIRFREIGHT_SLABS_TBL A WHERE A.BREAKPOINT_TYPE = 2 AND A.ACTIVE_FLAG=1 ";
            strTypeSQL += "ORDER BY BREAKPOINT_ID";
            try
            {
                M_TypeDataSet = (new WorkFlow()).GetDataSet(strTypeSQL);
                //Manjunath  PTS ID:Sep-02   12/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Constructor"

        /// <summary>
        /// The m_ data set
        /// </summary>
        private DataSet M_DataSet;

        #region "List of Properties"

        /// <summary>
        /// Gets or sets my data set.
        /// </summary>
        /// <value>
        /// My data set.
        /// </value>
        public DataSet MyDataSet
        {
            get { return M_DataSet; }
            set { M_DataSet = value; }
        }

        #endregion "List of Properties"

        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="strBreakPointId">The string break point identifier.</param>
        /// <param name="strDescription">The string description.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="IsActive">The is active.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public string FetchAll(string strBreakPointId, string strDescription, string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, Int16 IsActive = 0, bool blnSortAscending = false, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (strBreakPointId.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And upper(BREAKPOINT_ID) like '%" + strBreakPointId.ToUpper().Replace("'", "''") + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And upper(BREAKPOINT_ID) like '" + strBreakPointId.ToUpper().Replace("'", "''") + "%' ";
                }
            }
            if (strDescription.Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And upper(BREAKPOINT_DESC) like '%" + strDescription.ToUpper().Replace("'", "''") + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And upper(BREAKPOINT_DESC) like '" + strDescription.ToUpper().Replace("'", "''") + "%' ";
                }
            }

            if (IsActive == 1)
            {
                strCondition += " and ACTIVE_FLAG = 1";
            }
            strSQL = "SELECT Count(*) from AIRFREIGHT_SLABS_TBL where 1=1";
            strSQL += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
            {
                CurrentPage = 1;
            }
            if (TotalRecords == 0)
            {
                CurrentPage = 0;
            }
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;
            strSQL = " select  * from (";
            strSQL += " SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL += " AIRFREIGHT_SLABS_TBL_PK,";
            strSQL += " ACTIVE_FLAG,";
            strSQL += " BREAKPOINT_ID,";
            strSQL += " BREAKPOINT_DESC,";
            strSQL += " BREAKPOINT_RANGE,";
            strSQL += " BREAKPOINT_RANGE_TO,";
            strSQL += " BASIS,";
            strSQL += " decode(BASIS,1,'Kgs',2,'Flat') BASIS_NAME, ";
            strSQL += " BREAKPOINT_TYPE,";
            strSQL += " decode(BREAKPOINT_TYPE,1,'Breakpoint',2,'ULD') BREAKPOINT_TYPE_NAME, ";
            strSQL += " SEQUENCE_NO,";
            strSQL += " Version_No, ";
            strSQL += " 0 flag ";
            strSQL += " FROM AIRFREIGHT_SLABS_TBL ";
            strSQL += " WHERE ( 1 = 1) ";

            strSQL += strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }
            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL += " )q) WHERE SR_NO  Between " + start + " and " + last;
            try
            {
                DataSet Ds = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(Ds, Formatting.Indented);
                //Modified by Manjunath  PTS ID:Sep-02   12/09/2011
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch Function"

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                DtTbl = M_DataSet.Tables[0];
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".AIRFREIGHT_SLABS_TBL_PKG.AIRFREIGHT_SLABS_TBL_INS";
                var _with2 = _with1.Parameters;
                insCommand.Parameters.Add("BREAKPOINT_ID_IN", OracleDbType.Varchar2, 20, "BREAKPOINT_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["BREAKPOINT_ID_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("BREAKPOINT_RANGE_IN", OracleDbType.Double, 10, "BREAKPOINT_RANGE").Direction = ParameterDirection.Input;
                insCommand.Parameters["BREAKPOINT_RANGE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("BREAKPOINT_RANGE_TO_IN", OracleDbType.Double, 10, "BREAKPOINT_RANGE_TO").Direction = ParameterDirection.Input;
                insCommand.Parameters["BREAKPOINT_RANGE_TO_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("BREAKPOINT_DESC_IN", OracleDbType.Varchar2, 50, "BREAKPOINT_DESC").Direction = ParameterDirection.Input;
                insCommand.Parameters["BREAKPOINT_DESC_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("BREAKPOINT_TYPE_IN", OracleDbType.Int32, 1, "BREAKPOINT_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["BREAKPOINT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("BASIS_IN", OracleDbType.Int32, 1, "BASIS").Direction = ParameterDirection.Input;
                insCommand.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("SEQUENCE_NO_IN", OracleDbType.Int32, 2, "SEQUENCE_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["SEQUENCE_NO_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".AIRFREIGHT_SLABS_TBL_PKG.AIRFREIGHT_SLABS_TBL_UPD";
                updCommand.Parameters.Add("AIRFREIGHT_SLABS_TBL_PK_IN", OracleDbType.Int32, 10, "AIRFREIGHT_SLABS_TBL_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["AIRFREIGHT_SLABS_TBL_PK_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("BREAKPOINT_ID_IN", OracleDbType.Varchar2, 20, "BREAKPOINT_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["BREAKPOINT_ID_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("BREAKPOINT_RANGE_IN", OracleDbType.Double, 10, "BREAKPOINT_RANGE").Direction = ParameterDirection.Input;
                updCommand.Parameters["BREAKPOINT_RANGE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("BREAKPOINT_RANGE_TO_IN", OracleDbType.Double, 10, "BREAKPOINT_RANGE_TO").Direction = ParameterDirection.Input;
                updCommand.Parameters["BREAKPOINT_RANGE_TO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("BREAKPOINT_DESC_IN", OracleDbType.Varchar2, 50, "BREAKPOINT_DESC").Direction = ParameterDirection.Input;
                updCommand.Parameters["BREAKPOINT_DESC_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("BREAKPOINT_TYPE_IN", OracleDbType.Int32, 1, "BREAKPOINT_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["BREAKPOINT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("BASIS_IN", OracleDbType.Int32, 1, "BASIS").Direction = ParameterDirection.Input;
                updCommand.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("SEQUENCE_NO_IN", OracleDbType.Int32, 2, "SEQUENCE_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["SEQUENCE_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                var _with4 = objWK.MyDataAdapter;
                _with4.InsertCommand = insCommand;
                _with4.InsertCommand.Transaction = TRAN;
                _with4.UpdateCommand = updCommand;
                _with4.UpdateCommand.Transaction = TRAN;
                RecAfct = _with4.Update(M_DataSet);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }

        #endregion "Save Function"
    }
}