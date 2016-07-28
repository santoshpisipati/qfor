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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;
namespace Quantum_QFOR
{
    public class clsSAL_CAL_REASON_MST_TBL : CommonFeatures
	{

		#region "Fetch Function"
		public DataSet FetchAll(Int64 P_Sal_Cal_Reason_Mst_Tbl_Pk = -1, string P_Sal_Cal_Id = "", string P_Sal_Cal_Reason = "", string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 2, Int32 flag = 0, Int32 Export = 0)
		{
			string strSQL = null;
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			if (flag == 0) {
				strCondition += " AND 1=2";
			}
			//If P_Sal_Cal_Reason_Mst_Tbl_Pk.ToString.Trim.Length > 0 Then
			//    If SearchType = "C" Then
			//        strCondition = strCondition & " And Sal_Cal_Reason_Mst_Tbl_Pk like '%" & P_Sal_Cal_Reason_Mst_Tbl_Pk & "%' "
			//    Else
			//        strCondition = strCondition & " And Sal_Cal_Reason_Mst_Tbl_Pk like '" & P_Sal_Cal_Reason_Mst_Tbl_Pk & "%' "
			//    End If
			//Else
			//End If
			if (P_Sal_Cal_Id.ToString().Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " And upper(Sal_Cal_Id) like '%" + P_Sal_Cal_Id.ToUpper() + "%' ";
				} else {
					strCondition = strCondition + " And upper(Sal_Cal_Id) like '" + P_Sal_Cal_Id.ToUpper() + "%' ";
				}
			} else {
			}
			if (P_Sal_Cal_Reason.ToString().Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " And upper(Sal_Cal_Reason) like '%" + P_Sal_Cal_Reason.ToUpper() + "%' ";
				} else {
					strCondition = strCondition + " And upper(Sal_Cal_Reason) like '" + P_Sal_Cal_Reason.ToUpper() + "%' ";
				}
			} else {
			}
			strSQL = "SELECT Count(*) from SAL_CAL_REASON_MST_TBL where 1=1";
			strSQL += strCondition;
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;

			if (Convert.ToInt32(SortCol) > 0) {
				strCondition = strCondition + " order by " + Convert.ToInt32(SortCol);
			}

			strSQL = " select * from (";
			strSQL += "select ROWNUM SR_NO, qry.* from ( Select";
			strSQL = strSQL + " Sal_Cal_Reason_Mst_Tbl_Pk,";
			strSQL = strSQL + " Sal_Cal_Id,";
			strSQL = strSQL + " Sal_Cal_Reason,";
			strSQL = strSQL + " Version_No ";
			strSQL = strSQL + " FROM SAL_CAL_REASON_MST_TBL ";
			strSQL = strSQL + " WHERE ( 1 = 1) ";
			strSQL += strCondition;
			//If SortExpression.Trim.Length > 0 Then
			//    strSQL &= vbCrLf & " " & SortExpression
			//Else
			//    strSQL &= vbCrLf & " order by Sal_Cal_Id"
			//End If
			if (Export == 0) {
				strSQL += " )qry)  WHERE SR_NO Between " + start + " and " + last;
			} else {
				strSQL += " )qry) ";
			}

			//strSQL &= " order by Sal_Cal_Id"

			try {
				return objWF.GetDataSet(strSQL);
				//Catch sqlExp As OracleException
			//Modified by Manjunath  PTS ID:Sep-02  14/09/2011
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion
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

			try {
				DataTable DtTbl = new DataTable();
				DataRow DtRw = null;
				int i = 0;
				DtTbl = M_DataSet.Tables[0];

				var _with1 = insCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".SAL_CAL_REASON_MST_TBL_PKG.SAL_CAL_REASON_MST_TBL_INS";
				var _with2 = _with1.Parameters;

				insCommand.Parameters.Add("SAL_CAL_ID_IN", OracleDbType.Varchar2, 0, "Sal_Cal_Id").Direction = ParameterDirection.Input;
				insCommand.Parameters["SAL_CAL_ID_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("SAL_CAL_REASON_IN", OracleDbType.Varchar2, 0, "Sal_Cal_Reason").Direction = ParameterDirection.Input;
				insCommand.Parameters["SAL_CAL_REASON_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "Sal_Cal_Reason_Mst_Tbl_Pk").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


				var _with3 = delCommand;
				_with3.Connection = objWK.MyConnection;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWK.MyUserName + ".SAL_CAL_REASON_MST_TBL_PKG.SAL_CAL_REASON_MST_TBL_DEL";
				var _with4 = _with3.Parameters;
				delCommand.Parameters.Add("SAL_CAL_REASON_MST_TBL_PK_IN", OracleDbType.Int32, 10, "Sal_Cal_Reason_Mst_Tbl_Pk").Direction = ParameterDirection.Input;
				delCommand.Parameters["SAL_CAL_REASON_MST_TBL_PK_IN"].SourceVersion = DataRowVersion.Current;

				delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

				delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
				delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


				var _with5 = updCommand;
				_with5.Connection = objWK.MyConnection;
				_with5.CommandType = CommandType.StoredProcedure;
				_with5.CommandText = objWK.MyUserName + ".SAL_CAL_REASON_MST_TBL_PKG.SAL_CAL_REASON_MST_TBL_UPD";
				var _with6 = _with5.Parameters;

				updCommand.Parameters.Add("SAL_CAL_REASON_MST_TBL_PK_IN", OracleDbType.Int32, 10, "Sal_Cal_Reason_Mst_Tbl_Pk").Direction = ParameterDirection.Input;
				updCommand.Parameters["SAL_CAL_REASON_MST_TBL_PK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("SAL_CAL_ID_IN", OracleDbType.Varchar2, 0, "Sal_Cal_Id").Direction = ParameterDirection.Input;
				updCommand.Parameters["SAL_CAL_ID_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("SAL_CAL_REASON_IN", OracleDbType.Varchar2, 0, "Sal_Cal_Reason").Direction = ParameterDirection.Input;
				updCommand.Parameters["SAL_CAL_REASON_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with7 = objWK.MyDataAdapter;

				_with7.InsertCommand = insCommand;
				_with7.InsertCommand.Transaction = TRAN;
				_with7.UpdateCommand = updCommand;
				_with7.UpdateCommand.Transaction = TRAN;
				_with7.DeleteCommand = delCommand;
				_with7.DeleteCommand.Transaction = TRAN;
				RecAfct = _with7.Update(M_DataSet);
				if (RecAfct > 0) {
					TRAN.Commit();
				}
				if (arrMessage.Count > 0) {
					return arrMessage;
					//Else  'sivachandran 05Jun08 Imp-Exp-Wiz 16May08
					//    arrMessage.Add("All Data Saved Successfully")
					//    Return arrMessage
				} else if (Import == false) {
					arrMessage.Add("All Data Saved Successfully");
					//End
				} else {
					arrMessage.Add("Data Imported Successfully");
					//sivachandran 05Jun08 Imp-Exp-Wiz 16May08
				}
				return arrMessage;
				//End
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion
	}
}
