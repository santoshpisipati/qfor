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
//'*  Modified Date(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    public class cls_MOVECODE_SETTING_MST_TBL : CommonFeatures
    {
        //Private arrMessage As New ArrayList

        #region "List of Members of Class"

        //Private M_COMMODITY_GROUP_PK As Int16
        //Private M_COMMODITY_GROUP_CODE As String
        //Private M_COMMODITY_GROUP_DESC As String

        #endregion "List of Members of Class"

        private long M_MOVECODE_SETTING_PK;

        #region "List of Properties"

        public long MOVECODE_SETTING_PK
        {
            get { return M_MOVECODE_SETTING_PK; }
            set { M_MOVECODE_SETTING_PK = value; }
        }

        #endregion "List of Properties"

        #region "Fetch Function"

        public DataSet FetchAll(Int16 P_MOVECODE_SETTING_PK = 0, string P_SETTING_ID = "", string P_SETTING_DESC = "", string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 2)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (P_MOVECODE_SETTING_PK > 0)
            {
                strCondition = strCondition + "AND MOVECODE_SETTING_PK= " + P_MOVECODE_SETTING_PK;
            }

            if (P_SETTING_ID.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(SETTING_ID) LIKE '" + P_SETTING_ID.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(SETTING_ID) LIKE '%" + P_SETTING_ID.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(SETTING_ID) LIKE '%" + P_SETTING_ID.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (P_SETTING_DESC.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(SETTING_DESC) LIKE '" + P_SETTING_DESC.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(SETTING_DESC) LIKE '%" + P_SETTING_DESC.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(SETTING_DESC) LIKE '%" + P_SETTING_DESC.ToUpper().Replace("'", "''") + "%'";
                }
            }

            strSQL = "SELECT Count(*) from MOVECODE_SETTING_MST_TBL where 1=1";
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

            if (Convert.ToInt32(SortCol) > 0)
            {
                strCondition = strCondition + " order by " + Convert.ToInt32(SortCol);
            }

            strSQL = "select * from (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL = strSQL + " MOVECODE_SETTING_PK,";
            strSQL = strSQL + " SETTING_ID,";
            strSQL = strSQL + " SETTING_DESC,";
            strSQL = strSQL + " Version_No ";
            strSQL = strSQL + " FROM MOVECODE_SETTING_MST_TBL ";
            strSQL = strSQL + " WHERE  1 = 1 ";

            strSQL += strCondition;

            strSQL = strSQL + " )q ) WHERE SR_NO  Between " + start + " and " + last;
            strSQL += "ORDER BY SETTING_ID";

            try
            {
                return objWF.GetDataSet(strSQL);
                //Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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

        public DataSet FetchSettingDetails(Int64 PK, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow ObjWf = new WorkFlow();
            string strSQL = null;

            Int32 TotalRecords = default(Int32);
            Int32 start = default(Int32);
            Int32 last = default(Int32);

            strSQL = "SELECT count(*)";
            strSQL = strSQL + "FROM MOVECODE_SETTING_DTL_TRN MSD, MOVECODE_MST_TBL MMT";
            strSQL = strSQL + "WHERE MSD.MOVECODE_FK(+)=MMT.MOVECODE_PK ";
            strSQL = strSQL + "AND MSD.MOVECODE_SETTING_MST_FK(+)=" + PK;

            TotalRecords = Convert.ToInt32(ObjWf.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            strSQL = "select * from (";
            strSQL = strSQL + "SELECT";
            strSQL = strSQL + "ROWNUM SR_NO,";
            strSQL = strSQL + "(CASE WHEN NVL(MSD.MOVECODE_SETTING_DTL_PK,0)=0 THEN ROWNUM ELSE MSD.MOVECODE_SETTING_DTL_PK END)  MOVECODE_SETTING_DTL_PK,";
            strSQL = strSQL + "(CASE WHEN NVL(MSD.MOVECODE_SETTING_DTL_PK ,0)=0 THEN 0 ELSE 1 END ) SEL,";
            strSQL = strSQL + "MMT.MOVECODE_PK,";
            strSQL = strSQL + "MMT.MOVECODE_ID,";
            strSQL = strSQL + "MMT.MOVECODE_DESC,";
            strSQL = strSQL + "MSD.VERSION_NO";
            strSQL = strSQL + "FROM MOVECODE_SETTING_DTL_TRN MSD, MOVECODE_MST_TBL MMT";
            strSQL = strSQL + "WHERE MSD.MOVECODE_FK(+)=MMT.MOVECODE_PK ";
            strSQL = strSQL + "AND MSD.MOVECODE_SETTING_MST_FK(+)=" + PK;
            strSQL = strSQL + " )q  WHERE SR_NO  Between " + start + " and " + last;
            //strSQL = strSQL & vbCrLf & " )q  WHERE SR_NO  Between 1 and 14"
            strSQL = strSQL + "ORDER BY MOVECODE_ID";

            try
            {
                return ObjWf.GetDataSet(strSQL);
                // Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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

        public DataSet FetchSettingControlDetails(long PK)
        {
            WorkFlow ObjWf = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT  ";
            strSQL = strSQL + " MOVECODE_SETTING_PK,";
            strSQL = strSQL + " SETTING_ID,";
            strSQL = strSQL + " SETTING_DESC,";
            strSQL = strSQL + " Version_No ";
            strSQL = strSQL + " FROM MOVECODE_SETTING_MST_TBL ";
            strSQL = strSQL + " WHERE MOVECODE_SETTING_PK=" + PK;

            try
            {
                return ObjWf.GetDataSet(strSQL);
                //Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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

        public DataSet FetchWithoutJoin(Int64 PK)
        {
            WorkFlow ObjWf = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT";
            strSQL = strSQL + "ROWNUM SR_NO,";
            strSQL = strSQL + "MSD.MOVECODE_SETTING_DTL_PK,";
            strSQL = strSQL + "(CASE WHEN NVL(MSD.MOVECODE_SETTING_DTL_PK ,0)=0 THEN 0 ELSE 1 END ) SEL,";
            strSQL = strSQL + "MMT.MOVECODE_PK,";
            strSQL = strSQL + "MMT.MOVECODE_ID,";
            strSQL = strSQL + "MMT.MOVECODE_DESC,";
            strSQL = strSQL + "MSD.VERSION_NO";
            strSQL = strSQL + "FROM MOVECODE_SETTING_DTL_TRN MSD, MOVECODE_MST_TBL MMT";
            strSQL = strSQL + "WHERE MSD.MOVECODE_FK=MMT.MOVECODE_PK ";
            strSQL = strSQL + "AND MSD.MOVECODE_SETTING_MST_FK(+)=" + PK;
            //strSQL = strSQL & vbCrLf & " )q  WHERE SR_NO  Between " & start & " and " & last

            try
            {
                return ObjWf.GetDataSet(strSQL);
                //Catch sqlExp As OracleException
                //Modified by Manjunath  PTS ID:Sep-02  14/09/2011
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

        #region "Delete"

        public ArrayList Delete(ArrayList DeletedRow)
        {
            WorkFlow obJWk = new WorkFlow();
            OracleTransaction oraTran = null;
            OracleCommand delCommand = new OracleCommand();
            string strReturn = null;
            string[] arrRowDetail = null;
            Int32 i = default(Int32);
            try
            {
                obJWk.OpenConnection();
                for (i = 0; i <= DeletedRow.Count - 1; i++)
                {
                    oraTran = obJWk.MyConnection.BeginTransaction();
                    var _with1 = obJWk.MyCommand;
                    _with1.Transaction = oraTran;
                    _with1.Connection = obJWk.MyConnection;
                    _with1.CommandType = CommandType.StoredProcedure;
                    _with1.CommandText = obJWk.MyUserName + ".MOVECODE_SETTING_MST_TBL_PKG.MOVECODE_SETTING_MST_TBL_DEL";
                    arrRowDetail = Convert.ToString(DeletedRow[i]).Split(',');
                    _with1.Parameters.Clear();
                    var _with2 = _with1.Parameters;
                    _with2.Add("MOVECODE_SETTING_PK_IN", arrRowDetail[0]).Direction = ParameterDirection.Input;
                    _with2.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with2.Add("VERSION_NO_IN", arrRowDetail[1]).Direction = ParameterDirection.Input;
                    _with2.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with2.Add("RETURN_VALUE", strReturn).Direction = ParameterDirection.Output;
                    obJWk.MyCommand.Parameters["RETURN_VALUE"].OracleDbType = OracleDbType.Varchar2;
                    obJWk.MyCommand.Parameters["RETURN_VALUE"].Size = 50;
                    try
                    {
                        if (_with1.ExecuteNonQuery() > 0)
                        {
                            oraTran.Commit();
                        }
                        else
                        {
                            arrMessage.Add(arrRowDetail[0] + " cannot be deleted");
                            oraTran.Rollback();
                        }
                    }
                    catch (Exception e)
                    {
                        arrMessage.Add(arrRowDetail[0] + " cannot be deleted");
                        oraTran.Rollback();
                    }
                }
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("Success");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //Manjunath  PTS ID:Sep-02  14/09/2011
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                obJWk.MyConnection.Close();
            }
        }

        #endregion "Delete"

        #region "Save Function"

        public ArrayList SaveSettingsDTL(DataSet M_DataSet)
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

                var _with3 = insCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".MOVECODE_SETTING_DTL_TRN_PKG.MOVECODE_SETTING_DTL_TRN_INS";
                var _with4 = _with3.Parameters;
                //insCommand.Parameters.Add("MOVECODE_SETTING_MST_FK_IN", OracleClient.OracleDbType.Varchar2, 20, "MOVECODE_SETTING_MST_FK").Direction = ParameterDirection.Input
                //insCommand.Parameters["MOVECODE_SETTING_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("MOVECODE_SETTING_MST_FK_IN", M_MOVECODE_SETTING_PK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("MOVECODE_FK_IN", OracleDbType.Varchar2, 50, "MOVECODE_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["MOVECODE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MOVECODE_MST_TBL_PKG").Direction = ParameterDirection.Output;

                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with5 = delCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".MOVECODE_SETTING_DTL_TRN_PKG.MOVECODE_SETTING_DTL_TRN_DEL";
                var _with6 = _with5.Parameters;

                delCommand.Parameters.Add("MOVECODE_SETTING_DTL_PK_IN", OracleDbType.Int32, 10, "MOVECODE_SETTING_DTL_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["MOVECODE_SETTING_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with7 = updCommand;
                _with7.Connection = objWK.MyConnection;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".MOVECODE_SETTING_DTL_TRN_PKG.MOVECODE_SETTING_DTL_TRN_UPD";
                var _with8 = _with7.Parameters;

                updCommand.Parameters.Add("MOVECODE_SETTING_DTL_PK_IN", OracleDbType.Int32, 10, "MOVECODE_SETTING_DTL_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MOVECODE_SETTING_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MOVECODE_SETTING_MST_FK_IN", OracleDbType.Varchar2, 20, "MOVECODE_SETTING_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MOVECODE_SETTING_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MOVECODE_FK_IN", OracleDbType.Varchar2, 50, "MOVECODE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MOVECODE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with9 = objWK.MyDataAdapter;

                _with9.InsertCommand = insCommand;
                _with9.InsertCommand.Transaction = TRAN;
                _with9.UpdateCommand = updCommand;
                _with9.UpdateCommand.Transaction = TRAN;
                _with9.DeleteCommand = delCommand;
                _with9.DeleteCommand.Transaction = TRAN;
                RecAfct = _with9.Update(M_DataSet);
                TRAN.Commit();
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
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
                objWK.CloseConnection();
            }
        }

        public ArrayList SaveSettingsMST(DataSet M_DataSet)
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

                var _with10 = insCommand;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".MOVECODE_SETTING_DTL_TRN_PKG.MOVECODE_SETTING_DTL_TRN_INS";
                var _with11 = _with10.Parameters;
                //insCommand.Parameters.Add("MOVECODE_SETTING_MST_FK_IN", OracleClient.OracleDbType.Varchar2, 20, "MOVECODE_SETTING_MST_FK").Direction = ParameterDirection.Input
                //insCommand.Parameters["MOVECODE_SETTING_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                insCommand.Parameters.Add("MOVECODE_SETTING_MST_FK_IN", M_MOVECODE_SETTING_PK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("MOVECODE_FK_IN", OracleDbType.Varchar2, 50, "MOVECODE_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["MOVECODE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MOVECODE_MST_TBL_PKG").Direction = ParameterDirection.Output;

                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with12 = delCommand;
                _with12.Connection = objWK.MyConnection;
                _with12.CommandType = CommandType.StoredProcedure;
                _with12.CommandText = objWK.MyUserName + ".MOVECODE_SETTING_DTL_TRN_PKG.MOVECODE_SETTING_DTL_TRN_DEL";
                var _with13 = _with12.Parameters;

                delCommand.Parameters.Add("MOVECODE_SETTING_DTL_PK_IN", OracleDbType.Int32, 10, "MOVECODE_SETTING_DTL_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["MOVECODE_SETTING_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with14 = updCommand;
                _with14.Connection = objWK.MyConnection;
                _with14.CommandType = CommandType.StoredProcedure;
                _with14.CommandText = objWK.MyUserName + ".MOVECODE_SETTING_DTL_TRN_PKG.MOVECODE_SETTING_DTL_TRN_UPD";
                var _with15 = _with14.Parameters;

                updCommand.Parameters.Add("MOVECODE_SETTING_DTL_PK_IN", OracleDbType.Int32, 10, "MOVECODE_SETTING_DTL_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MOVECODE_SETTING_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MOVECODE_SETTING_MST_FK_IN", OracleDbType.Varchar2, 20, "MOVECODE_SETTING_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MOVECODE_SETTING_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MOVECODE_FK_IN", OracleDbType.Varchar2, 50, "MOVECODE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MOVECODE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with16 = objWK.MyDataAdapter;

                _with16.InsertCommand = insCommand;
                _with16.InsertCommand.Transaction = TRAN;
                _with16.UpdateCommand = updCommand;
                _with16.UpdateCommand.Transaction = TRAN;
                _with16.DeleteCommand = delCommand;
                _with16.DeleteCommand.Transaction = TRAN;
                RecAfct = _with16.Update(M_DataSet);
                TRAN.Commit();
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
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
                objWK.CloseConnection();
            }
        }

        public long InsertRecord(string SETTING_ID_IN, string SETTING_DESC_IN)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            try
            {
                strSQL = "INSERT INTO MOVECODE_SETTING_MST_TBL (";
                strSQL = strSQL + "MOVECODE_SETTING_PK,";
                strSQL = strSQL + "SETTING_ID,";
                strSQL = strSQL + "SETTING_DESC,";
                strSQL = strSQL + "CREATED_BY_FK,";
                strSQL = strSQL + "CREATED_DT) ";
                strSQL = strSQL + "VALUES (";
                strSQL = strSQL + "SEQ_MOVECODE_SETTING_DTL_TRN.NEXTVAL,";
                strSQL = strSQL + "UPPER('" + SETTING_ID_IN.Trim() + "'),";
                strSQL = strSQL + "'" + SETTING_DESC_IN.Trim() + "',";
                strSQL = strSQL + M_CREATED_BY_FK + ",";
                strSQL = strSQL + "SYSDATE)";

                objWK.ExecuteCommands(strSQL);
                strSQL = "SELECT MST.MOVECODE_SETTING_PK FROM  MOVECODE_SETTING_MST_TBL MST WHERE TRIM(MST.SETTING_ID)='" + SETTING_ID_IN.ToUpper() + "' AND MST.SETTING_DESC='" + SETTING_DESC_IN + "'";
                return Convert.ToInt64(objWK.ExecuteScaler(strSQL));
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        public bool UpdateRecord(string SETTING_ID_IN, string SETTING_DESC_IN, long MOVECODE_SETTING_PK_IN, Int16 VERSION_NO_IN)
        {
            bool functionReturnValue = false;
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            try
            {
                strSQL = "UPDATE MOVECODE_SETTING_MST_TBL SET ";
                strSQL = strSQL + "SETTING_ID                              = UPPER('" + SETTING_ID_IN.Trim() + "'),";
                strSQL = strSQL + "SETTING_DESC                            = '" + SETTING_DESC_IN.Trim() + "',";
                strSQL = strSQL + "LAST_MODIFIED_BY_FK                     = " + M_LAST_MODIFIED_BY_FK + ",";
                strSQL = strSQL + "LAST_MODIFIED_DT                        = SYSDATE";
                strSQL = strSQL + ",Version_No = Version_No + 1";
                strSQL = strSQL + "WHERE";
                strSQL = strSQL + "MOVECODE_SETTING_PK =" + MOVECODE_SETTING_PK_IN;
                strSQL = strSQL + "AND VERSION_NO                           =" + VERSION_NO_IN;

                functionReturnValue = objWK.ExecuteCommands(strSQL);
                //Manjunath  PTS ID:Sep-02  14/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }

        public string GetVersionNumber(long MOVECODE_SETTING_PK_IN)
        {
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
            try
            {
                strSQL = "SELECT  MSM.VERSION_NO FROM MOVECODE_SETTING_MST_TBL MSM WHERE MSM.MOVECODE_SETTING_PK=" + MOVECODE_SETTING_PK_IN;
                return objWK.ExecuteScaler(strSQL);
                //Manjunath  PTS ID:Sep-02  14/09/2011
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

        #endregion "Save Function"

        #region "Each Row Updation"

        //Sub OnRowUpdated(ByVal objsender As Object, ByVal e As OracleRowUpdatedEventArgs)
        //    Try
        //        If e.RecordsAffected < 1 Then
        //            If e.Errors.Message <> "" Then
        //                arrMessage.Add(CType(e.Row.Item(2), String) & "~" & e.Errors.Message)
        //            Else
        //                arrMessage.Add(CType(e.Command.Parameters[0].Value, String) & "~" & e.Errors.Message)
        //            End If
        //            e.Status = UpdateStatus.SkipCurrentRow
        //        End If
        //    Catch ex As Exception
        //        Throw ex
        //    End Try
        //End Sub

        #endregion "Each Row Updation"
    }
}