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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    public class clsMOVECODE_MST_TBL : CommonFeatures
    {
        //Private arrMessage As New ArrayList

        #region "List of Members of Class"

        private Int16 M_COMMODITY_GROUP_PK;
        private string M_COMMODITY_GROUP_CODE;

        #endregion "List of Members of Class"

        private string M_COMMODITY_GROUP_DESC;

        #region "List of Properties"

        public Int16 CommGrp_Pk
        {
            get { return M_COMMODITY_GROUP_PK; }
            set { M_COMMODITY_GROUP_PK = value; }
        }

        public string CommGrp_Code
        {
            get { return M_COMMODITY_GROUP_CODE; }
            set { M_COMMODITY_GROUP_CODE = value; }
        }

        public string CommGrp_Desc
        {
            get { return M_COMMODITY_GROUP_DESC; }
            set { M_COMMODITY_GROUP_DESC = value; }
        }

        #endregion "List of Properties"

        #region "Fetch Function"

        //Public Function FetchAll( _
        //Optional ByVal P_Commodity_Group_Pk As Int16 = 0, _
        //Optional ByVal P_Commodity_Group_Code As String = "", _
        //Optional ByVal P_Commodity_Group_Desc As String = "", _
        // Optional ByVal SearchType As String = "", _
        //            Optional ByVal SortExpression As String = "", _
        //            Optional ByRef CurrentPage As Int32 = 0, _
        //            Optional ByRef TotalPage As Int32 = 0, _
        //            Optional ByVal SortCol As Int16 = 2 _
        // ) As DataSet
        public DataSet FetchAll(Int16 P_Movecode_Pk = 0, string P_Movecode_Id = "", string P_Movecode_Desc = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 2, bool blnSortAscending = false, int intBusType = 0,
        int intUser = 0, int ActiveFlag = -1)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (P_Movecode_Pk > 0)
            {
                strCondition = strCondition + "AND Movecode_Pk= " + P_Movecode_Pk;
            }

            if (P_Movecode_Id.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(Movecode_Id) LIKE '" + P_Movecode_Id.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(Movecode_Id) LIKE '%" + P_Movecode_Id.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(Movecode_Id) LIKE '%" + P_Movecode_Id.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (P_Movecode_Desc.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(Movecode_Desc) LIKE '" + P_Movecode_Desc.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(Movecode_Desc) LIKE '%" + P_Movecode_Desc.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(Movecode_Desc) LIKE '%" + P_Movecode_Desc.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (ActiveFlag == 1)
            {
                strCondition += " AND ACTIVE_FLAG = 1 ";
            }
            else
            {
                strCondition += "";
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
                strCondition += " AND BUSINESS_TYPE = " + intBusType + " ";
            }

            strSQL = "SELECT Count(*) from movecode_mst_tbl where 1=1";
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

            strSQL = "select * from (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT  ";
            strSQL = strSQL + " MOVECODE_PK,";
            strSQL = strSQL + " ACTIVE_FLAG,";
            strSQL = strSQL + " MOVECODE_ID,";
            strSQL = strSQL + " MOVECODE_DESC,";
            strSQL = strSQL + " BUSINESS_TYPE,";
            strSQL = strSQL + " VERSION_NO ";
            strSQL = strSQL + " FROM MOVECODE_MST_TBL ";
            strSQL = strSQL + " WHERE  1 = 1 ";

            strSQL += strCondition;

            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }

            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }

            strSQL = strSQL + " )q ) WHERE SR_NO  BETWEEN " + start + " AND " + last;

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

        #endregion "Fetch Function"

        #region "Save Function"

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
                _with1.CommandText = objWK.MyUserName + ".MOVECODE_MST_TBL_PKG.MOVECODE_MST_TBL_INS";
                var _with2 = _with1.Parameters;
                insCommand.Parameters.Add("MOVECODE_ID_IN", OracleDbType.Varchar2, 20, "MOVECODE_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["MOVECODE_ID_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MOVECODE_DESC_IN", OracleDbType.Varchar2, 100, "MOVECODE_DESC").Direction = ParameterDirection.Input;
                insCommand.Parameters["MOVECODE_DESC_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MOVECODE_MST_TBL_PKG").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".MOVECODE_MST_TBL_PKG.MOVECODE_MST_TBL_UPD";
                var _with4 = _with3.Parameters;

                updCommand.Parameters.Add("MOVECODE_PK_IN", OracleDbType.Int32, 10, "MOVECODE_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MOVECODE_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MOVECODE_ID_IN", OracleDbType.Varchar2, 20, "MOVECODE_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["MOVECODE_ID_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MOVECODE_DESC_IN", OracleDbType.Varchar2, 100, "MOVECODE_DESC").Direction = ParameterDirection.Input;
                updCommand.Parameters["MOVECODE_DESC_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = objWK.MyDataAdapter;

                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;
                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;

                RecAfct = _with5.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    //This is if any error occurs then rollback..
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    //This part executes only when no error in the execution..
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
                objWK.CloseConnection();
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

        #region "Fetch Business type"

        public DataSet FetchBusinessType()
        {
            string strSQL = null;
            strSQL = "SELECT  B.BUSINESS_TYPE,B.BUSINESS_TYPE_DISPLAY FROM BUSINESS_TYPE_MST_TBL B";
            WorkFlow objWF = new WorkFlow();
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

        #endregion "Fetch Business type"
    }
}