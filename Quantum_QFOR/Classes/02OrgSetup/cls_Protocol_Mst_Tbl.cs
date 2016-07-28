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
    public class cls_Protocol_Mst_Tbl : CommonFeatures
    {
        #region "List of Members of the Class"

        /// <summary>
        /// The m_ protocol_ MST_ pk
        /// </summary>
        private Int64 M_Protocol_Mst_Pk;

        /// <summary>
        /// The m_ protocol_ name
        /// </summary>
        private string M_Protocol_NAME;

        #endregion "List of Members of the Class"

        /// <summary>
        /// The m_ protocol_ value
        /// </summary>
        private string M_Protocol_VALUE;

        #region "List of Properties"

        /// <summary>
        /// Gets or sets the protocol_ MST_ pk.
        /// </summary>
        /// <value>
        /// The protocol_ MST_ pk.
        /// </value>
        public Int64 Protocol_Mst_Pk
        {
            get { return M_Protocol_Mst_Pk; }
            set { M_Protocol_Mst_Pk = value; }
        }

        /// <summary>
        /// Gets or sets the protocol_ identifier.
        /// </summary>
        /// <value>
        /// The protocol_ identifier.
        /// </value>
        public string Protocol_Id
        {
            get { return M_Protocol_NAME; }
            set { M_Protocol_NAME = value; }
        }

        /// <summary>
        /// Gets or sets the name of the protocol_.
        /// </summary>
        /// <value>
        /// The name of the protocol_.
        /// </value>
        public string Protocol_Name
        {
            get { return M_Protocol_VALUE; }
            set { M_Protocol_VALUE = value; }
        }

        #endregion "List of Properties"

        #region "FetchAll Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="ProtocolPk">The protocol pk.</param>
        /// <param name="ProtocolName">Name of the protocol.</param>
        /// <param name="ProtocolValue">The protocol value.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strSortColumn">The string sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="intIntervalMonthly">The int interval monthly.</param>
        /// <param name="IsActive">The is active.</param>
        /// <param name="intBusType">Type of the int bus.</param>
        /// <param name="intUser">The int user.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int64 ProtocolPk, string ProtocolName, string ProtocolValue, string SearchType, string strSortColumn, Int32 CurrentPage, Int32 TotalPage, bool blnSortAscending = false, int intIntervalMonthly = 0, int IsActive = -1,
        int intBusType = 0, int intUser = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            if (SearchType == "C")
            {
                if (ProtocolName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(Protocol_NAME) LIKE '%" + ProtocolName.ToUpper().Replace("'", "''") + "%'";
                }

                if (ProtocolValue.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(Protocol_VALUE) LIKE '%" + ProtocolValue.ToUpper().Replace("'", "''") + "%'";
                }

                if (ProtocolPk != 0)
                {
                    strCondition = strCondition + " AND Protocol_Mst_Pk = " + ProtocolPk + " ";
                }

                if (IsActive != -1)
                {
                    strCondition = strCondition + " AND IS_ACTIVATED = " + IsActive;
                }
            }
            else
            {
                if (ProtocolName.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(Protocol_NAME) LIKE '" + ProtocolName.ToUpper().Replace("'", "''") + "%'";
                }

                if (ProtocolValue.Trim().Length > 0)
                {
                    strCondition = strCondition + " AND UPPER(Protocol_VALUE) LIKE '" + ProtocolValue.ToUpper().Replace("'", "''") + "%'";
                }

                if (ProtocolPk != 0)
                {
                    strCondition = strCondition + " AND Protocol_Mst_Pk = " + ProtocolPk + " ";
                }

                if (IsActive != -1)
                {
                    strCondition = strCondition + " AND IS_ACTIVATED = " + IsActive;
                }
            }
            if (intBusType == 3 & intUser == 3)
            {
                strCondition += " AND BUSINESS_TYPE IN (1,2,3) ";
            }
            else if (intBusType == 3 & intUser == 2)
            {
                strCondition += " AND BUSINESS_TYPE IN (2,3) ";
            }
            else if (intBusType == 3 & intUser == 1)
            {
                strCondition += " AND BUSINESS_TYPE IN (1,3) ";
            }
            else
            {
                strCondition += " AND BUSINESS_TYPE in (" + intBusType + " , 3) ";
            }

            strSQL = "SELECT Count(*) from Protocol_MST_TBL where 1=1";
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

            strSQL = "  Select * From (SELECT ROWNUM SR_NO, q.* From ( Select ";
            strSQL = "Protocol_MST_PK, ";
            strSQL = "DECODE(IS_ACTIVATED ,'0','false','1','true') IS_ACTIVATED , ";
            strSQL = "Protocol_NAME, ";
            strSQL = "Protocol_VALUE, ";
            strSQL = "0 RESET_INTERVAL, ";
            strSQL = " DECODE(BUSINESS_TYPE,'0','','1','Air','2','Sea','3','Both','4','Removals') BUSINESS_TYPE,VERSION_NO,";
            strSQL = "APPLY_BARCODE";
            strSQL = "FROM Protocol_MST_TBL ";
            strSQL = "WHERE 1=1 ";
            strSQL += strCondition;

            if (!strSortColumn.Equals("SR_NO"))
            {
                strSQL += "order by " + strSortColumn;
            }

            if (!blnSortAscending & !strSortColumn.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }

            strSQL += ") q  ) WHERE SR_NO  Between " + start + " and " + last;

            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
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

        #endregion "FetchAll Function"

        #region "Fetch Protocol Function"

        /// <summary>
        /// Fetches the protocol.
        /// </summary>
        /// <param name="ProtocolPK">The protocol pk.</param>
        /// <param name="ProtocolName">Name of the protocol.</param>
        /// <param name="ProtocolValue">The protocol value.</param>
        /// <returns></returns>
        public string FetchProtocol(Int16 ProtocolPK = 0, string ProtocolName = "", string ProtocolValue = "")
        {
            string strSQL = null;
            strSQL = " select ";
            strSQL += " ' ' Protocol_NAME,";
            strSQL += " ' ' Protocol_VALUE,";
            strSQL += " 0 Protocol_mst_PK ";
            strSQL += " from ";
            strSQL += " DUAL ";
            strSQL += " UNION";
            strSQL += " select ";
            strSQL += " Protocol_NAME,";
            strSQL += " Protocol_VALUE,";
            strSQL += " Protocol_mst_pk ";
            strSQL += " from Protocol_mst_tbl";
            strSQL += " order by Protocol_NAME ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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

        /// <summary>
        /// Fetches the protcol on pk.
        /// </summary>
        /// <param name="ProtocolPk">The protocol pk.</param>
        /// <returns></returns>
        public DataSet FetchProtcolOnPK(long ProtocolPk)
        {
            string StrSql = null;
            WorkFlow objWk = new WorkFlow();

            StrSql += "SELECT PROTOCOL_MST_PK,";
            StrSql += "PROTOCOL_NAME,";
            StrSql += "PROTOCOL_VALUE,PMT.VERSION_NO,IS_ACTIVATED,RESET_INTERVAL ";
            StrSql += "FROM PROTOCOL_MST_TBL PMT";
            StrSql += "WHERE PMT.PROTOCOL_MST_PK = " + ProtocolPk;
            try
            {
                return objWk.GetDataSet(StrSql);
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

        #endregion "Fetch Protocol Function"

        #region "Insert Function"

        /// <summary>
        /// Inserts this instance.
        /// </summary>
        /// <returns></returns>
        public int Insert()
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                Int32 intPkVal = default(Int32);
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with1 = objWS.MyCommand.Parameters;
                _with1.Add("PROTOCOL_NAME_IN", M_Protocol_NAME).Direction = ParameterDirection.Input;
                _with1.Add("PROTOCOL_VALUE_IN", M_Protocol_VALUE).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", intPkVal).Direction = ParameterDirection.Output;
                objWS.MyCommand.CommandText = "Protocol_MST_TBL_PKG.Protocol_Mst_Tbl_Ins";
                if (objWS.ExecuteCommands() == true)
                {
                    return intPkVal;
                }
                else
                {
                    return -1;
                }
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

        #endregion "Insert Function"

        #region "Update Function"

        /// <summary>
        /// Updates this instance.
        /// </summary>
        /// <returns></returns>
        public int Update()
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with2 = objWS.MyCommand.Parameters;
                _with2.Add("Protocol_Mst_Pk_IN", M_Protocol_Mst_Pk).Direction = ParameterDirection.Input;
                _with2.Add("Protocol_Id_IN", M_Protocol_NAME).Direction = ParameterDirection.Input;
                _with2.Add("Protocol_Name_IN", M_Protocol_VALUE).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = "Protocol_MST_TBL_PKG.Protocol_Mst_Tbl_UPD";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
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

        #endregion "Update Function"

        #region "Delete Function"

        /// <summary>
        /// Deletes this instance.
        /// </summary>
        /// <returns></returns>
        public int Delete()
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with3 = objWS.MyCommand.Parameters;
                _with3.Add("Protocol_Mst_Pk_IN", M_Protocol_Mst_Pk).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = "Protocol_MST_TBL_PKG.Protocol_Mst_Tbl_DEL";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
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

        #endregion "Delete Function"

        #region "Delete States Function"

        /// <summary>
        /// Deletes the state.
        /// </summary>
        /// <returns></returns>
        public int DeleteState()
        {
            try
            {
                WorkFlow objWS = new WorkFlow();
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                var _with4 = objWS.MyCommand.Parameters;
                _with4.Add("Protocol_Mst_Pk_IN", M_Protocol_Mst_Pk).Direction = ParameterDirection.Input;
                objWS.MyCommand.CommandText = "Protocol_MST_TBL_PKG.STATE_MST_TBL_DEL";
                if (objWS.ExecuteCommands() == true)
                {
                    return 1;
                }
                else
                {
                    return -1;
                }
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

        #endregion "Delete States Function"

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="intBusinessType">Type of the int business.</param>
        /// <param name="Barcode">The barcode.</param>
        /// <param name="StorageVal">The storage value.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet, int intBusinessType, Int16 Barcode = 0, Int16 StorageVal = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();

            try
            {
                DataTable DtTbl = new DataTable();
                DtTbl = M_DataSet.Tables[0];
                var _with5 = insCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".PROTOCOL_MST_TBL_PKG.PROTOCOL_MST_TBL_INS";
                var _with6 = _with5.Parameters;

                insCommand.Parameters.Add("PROTOCOL_NAME_IN", OracleDbType.Char, 50, "PROTOCOL_NAME").Direction = ParameterDirection.Input;
                insCommand.Parameters["PROTOCOL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PROTOCOL_VALUE_IN", OracleDbType.Char, 50, "PROTOCOL_VALUE").Direction = ParameterDirection.Input;
                insCommand.Parameters["PROTOCOL_VALUE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RESET_INTERVAL_IN", OracleDbType.Int32, 1, "RESET_INTERVAL").Direction = ParameterDirection.Input;
                insCommand.Parameters["RESET_INTERVAL_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("IS_ACTIVATED_IN", OracleDbType.Int32, 1, "IS_ACTIVATED").Direction = ParameterDirection.Input;
                insCommand.Parameters["IS_ACTIVATED_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("BUSINESS_TYPE_IN", intBusinessType).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("APPLY_BARCODE_IN", Barcode).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("APPLY_STORAGE_IN", StorageVal).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PROTOCOL_MST_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with7 = delCommand;
                _with7.Connection = objWK.MyConnection;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".PROTOCOL_MST_TBL_PKG.PROTOCOL_MST_TBL_DEL";
                var _with8 = _with7.Parameters;
                delCommand.Parameters.Add("PROTOCOL_MST_PK_IN", OracleDbType.Int32, 10, "PROTOCOL_MST_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["PROTOCOL_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Char, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with9 = updCommand;
                _with9.Connection = objWK.MyConnection;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWK.MyUserName + ".PROTOCOL_MST_TBL_PKG.PROTOCOL_MST_TBL_UPD";
                var _with10 = _with9.Parameters;

                updCommand.Parameters.Add("PROTOCOL_MST_PK_IN", OracleDbType.Int32, 10, "PROTOCOL_MST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PROTOCOL_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PROTOCOL_NAME_IN", OracleDbType.Char, 50, "PROTOCOL_NAME").Direction = ParameterDirection.Input;
                updCommand.Parameters["PROTOCOL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PROTOCOL_VALUE_IN", OracleDbType.Char, 50, "PROTOCOL_VALUE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PROTOCOL_VALUE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RESET_INTERVAL_IN", OracleDbType.Int32, 1, "RESET_INTERVAL").Direction = ParameterDirection.Input;
                updCommand.Parameters["RESET_INTERVAL_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("IS_ACTIVATED_IN", OracleDbType.Int32, 1, "IS_ACTIVATED").Direction = ParameterDirection.Input;
                updCommand.Parameters["IS_ACTIVATED_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                //updCommand.Parameters["LAST_MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BUSINESS_TYPE_IN", intBusinessType).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("APPLY_BARCODE_IN", Barcode).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("APPLY_STORAGE_IN", StorageVal).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                //updCommand.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Char, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with11 = objWK.MyDataAdapter;

                _with11.InsertCommand = insCommand;
                _with11.InsertCommand.Transaction = TRAN;
                _with11.UpdateCommand = updCommand;
                _with11.UpdateCommand.Transaction = TRAN;
                RecAfct = _with11.Update(M_DataSet);
                TRAN.Commit();

                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    arrMessage.Add(M_DataSet.Tables[0].Rows[0][1]);
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
                objWK.CloseConnection();
            }
        }

        #endregion "Save Function"

        /// <summary>
        /// Fetches the protocol all.
        /// </summary>
        /// <param name="biztype">The biztype.</param>
        /// <returns></returns>
        public string FetchProtocolAll(int biztype = 0)
        {
            WorkFlow objwf = new WorkFlow();
            string StrSql = null;
            StrSql += " select ppt.sr_no,";
            StrSql += "  ppt.attribute,";
            StrSql += "   ppt.description,";
            StrSql += "   ppt.example,";
            StrSql += "  Null Selected ";
            StrSql += " from PROTOCOL_PARAMETER_TBL ppt";
            StrSql += "   where ppt.biz_type = " + biztype;
            try
            {
                DataSet getDs = objwf.GetDataSet(StrSql);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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
    }
}