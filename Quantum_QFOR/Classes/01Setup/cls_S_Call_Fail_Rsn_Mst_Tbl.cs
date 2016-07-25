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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    public class cls_S_Call_Fail_Rsn_Mst_Tbl : CommonFeatures
    {
        #region "Fetch Function"

        public DataSet FetchAll(Int64 P_S_CALL_FAIL_REASON_PK = -1, string P_S_CALL_FAIL_REASON_ID = "", string P_S_CALL_FAIL_REASON = "", Int32 ActiveFlag = 0, string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 2, Int32 flag = 0)
        {
            string strSQL = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (ActiveFlag == 1)
            {
                strCondition += " AND ACTIVE_FLAG =1 ";
                //Else
                //    strCondition &= vbCrLf & " AND ACTIVE_FLAG =0 "
            }
            //If P_S_CALL_FAIL_REASON_PK.ToString.Trim.Length > 0 Then
            //    If SearchType = "C" Then
            //        strCondition = strCondition & " And S_CALL_FAIL_REASON_PK like '%" & P_S_CALL_FAIL_REASON_PK & "%' "
            //    Else
            //        strCondition = strCondition & " And S_CALL_FAIL_REASON_PK like '" & P_S_CALL_FAIL_REASON_PK & "%' "
            //    End If
            //Else
            //End If
            if (P_S_CALL_FAIL_REASON_ID.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And upper(S_CALL_FAIL_REASON_ID) like '%" + P_S_CALL_FAIL_REASON_ID.ToUpper() + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And upper(S_CALL_FAIL_REASON_ID) like '" + P_S_CALL_FAIL_REASON_ID.ToUpper() + "%' ";
                }
            }
            else
            {
            }
            if (P_S_CALL_FAIL_REASON.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strCondition = strCondition + " And upper(S_CALL_FAIL_REASON) like '%" + P_S_CALL_FAIL_REASON.ToUpper() + "%' ";
                }
                else
                {
                    strCondition = strCondition + " And upper(S_CALL_FAIL_REASON) like '" + P_S_CALL_FAIL_REASON.ToUpper() + "%' ";
                }
            }
            else
            {
            }
            strSQL = "SELECT Count(*) from S_CALL_FAIL_REASON_MST_TBL where 1=1";
            strSQL += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

            if (Convert.ToInt32(SortCol) > 0)
            {
                strCondition = strCondition + " order by " + Convert.ToInt32(SortCol);
            }

            strSQL = " select * from (";
            strSQL += "select ROWNUM SR_NO, qry.* from ( Select";
            strSQL = strSQL + " S_CALL_FAIL_REASON_PK,";
            strSQL = strSQL + " NVL(ACTIVE_FLAG,0) ACTIVE_FLAG ,";
            strSQL = strSQL + " S_CALL_FAIL_REASON_ID,";
            strSQL = strSQL + " S_CALL_FAIL_REASON,";
            strSQL = strSQL + " Version_No ";
            strSQL = strSQL + " FROM S_CALL_FAIL_REASON_MST_TBL ";
            strSQL = strSQL + " WHERE ( 1 = 1) ";
            strSQL += strCondition;
            //If SortExpression.Trim.Length > 0 Then
            //    strSQL &= vbCrLf & " " & SortExpression
            //Else
            //    strSQL &= vbCrLf & " order by S_CALL_FAIL_REASON_ID"
            //End If
            strSQL += " )qry)  WHERE ROWNUM   Between " + start + " and " + last;
            //strSQL &= " order by S_CALL_FAIL_REASON_ID"

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

        public ArrayList Save(DataSet M_DataSet, bool Import = false)
        {
            //sivachandran 05Jun08 Imp-Exp-Wiz 16May08
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
                _with1.CommandText = objWK.MyUserName + ".S_CALL_FAIL_REASON_MST_TBL_PKG.S_CALL_FAIL_REASON_MST_TBL_INS";
                var _with2 = _with1.Parameters;

                insCommand.Parameters.Add("S_CALL_FAIL_REASON_ID_IN", OracleDbType.Varchar2, 0, "S_CALL_FAIL_REASON_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["S_CALL_FAIL_REASON_ID_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("S_CALL_FAIL_REASON_IN", OracleDbType.Varchar2, 0, "S_CALL_FAIL_REASON").Direction = ParameterDirection.Input;
                insCommand.Parameters["S_CALL_FAIL_REASON_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "S_CALL_FAIL_REASON_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = delCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".S_CALL_FAIL_REASON_MST_TBL_PKG.S_CALL_FAIL_REASON_MST_TBL_DEL";
                var _with4 = _with3.Parameters;
                delCommand.Parameters.Add("S_CALL_FAIL_REASON_PK_IN", OracleDbType.Int32, 10, "S_CALL_FAIL_REASON_PK").Direction = ParameterDirection.Input;
                delCommand.Parameters["S_CALL_FAIL_REASON_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = updCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".S_CALL_FAIL_REASON_MST_TBL_PKG.S_CALL_FAIL_REASON_MST_TBL_UPD";
                var _with6 = _with5.Parameters;

                updCommand.Parameters.Add("S_CALL_FAIL_REASON_PK_IN", OracleDbType.Int32, 10, "S_CALL_FAIL_REASON_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["S_CALL_FAIL_REASON_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("S_CALL_FAIL_REASON_ID_IN", OracleDbType.Varchar2, 0, "S_CALL_FAIL_REASON_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["S_CALL_FAIL_REASON_ID_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("S_CALL_FAIL_REASON_IN", OracleDbType.Varchar2, 0, "S_CALL_FAIL_REASON").Direction = ParameterDirection.Input;
                updCommand.Parameters["S_CALL_FAIL_REASON_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                //objWK.MyDataAdapter.ContinueUpdateOnError = True
                var _with7 = objWK.MyDataAdapter;

                _with7.InsertCommand = insCommand;
                _with7.InsertCommand.Transaction = TRAN;
                _with7.UpdateCommand = updCommand;
                _with7.UpdateCommand.Transaction = TRAN;
                _with7.DeleteCommand = delCommand;
                _with7.DeleteCommand.Transaction = TRAN;
                RecAfct = _with7.Update(M_DataSet);
                TRAN.Commit();
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    //arrMessage.Add("All Data Saved Successfully") 'sivachandran 05Jun08 Imp-Exp-Wiz 16May08
                    if (Import == false)
                    {
                        arrMessage.Add("All Data Saved Successfully");
                    }
                    else
                    {
                        arrMessage.Add("Data Imported Successfully");
                    }
                    //End
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
    }
}