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
    public class cls_Sales_Call_Type_Mst_Tbl : CommonFeatures
	{
		#region "Fetch Function"
		public DataSet FetchAll(Int64 P_SALES_CALL_TYPE_PK = -1, string P_SALES_CALL_TYPE_ID = "", string P_SALES_CALL_TYPE_DESC = "", Int32 iChecked = 0, string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 2, Int32 flag = 0)
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
			if (iChecked == 1) {
				strCondition += " AND ACTIVE_FLAG =1 ";
			}
			if (P_SALES_CALL_TYPE_ID.ToString().Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " And upper(SALES_CALL_TYPE_ID) like '%" + P_SALES_CALL_TYPE_ID.ToUpper() + "%' ";
				} else {
					strCondition = strCondition + " And upper(SALES_CALL_TYPE_ID) like '" + P_SALES_CALL_TYPE_ID.ToUpper() + "%' ";
				}
			} else {
			}
			if (P_SALES_CALL_TYPE_DESC.ToString().Trim().Length > 0) {

				if (SearchType == "C") {
					strCondition = strCondition + " And upper(SALES_CALL_TYPE_DESC) like '%" + P_SALES_CALL_TYPE_DESC.ToUpper() + "%' ";
				} else {
					strCondition = strCondition + " And upper(SALES_CALL_TYPE_DESC) like '" + P_SALES_CALL_TYPE_DESC.ToUpper() + "%' ";
				}
			} else {
			}
			strSQL = "SELECT Count(*) from S_CALL_TYPE_MST_TBL where 1=1";
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
			strSQL = strSQL + " SALES_CALL_TYPE_PK,";
			strSQL = strSQL + " NVL(ACTIVE_FLAG,0) ACTIVE_FLAG ,";
			strSQL = strSQL + " SALES_CALL_TYPE_ID,";
			strSQL = strSQL + " SALES_CALL_TYPE_DESC,";
			strSQL = strSQL + " Version_No ";
			strSQL = strSQL + " FROM S_CALL_TYPE_MST_TBL ";
			strSQL = strSQL + " WHERE ( 1 = 1) ";
			strSQL += strCondition;
			strSQL += " )qry)  WHERE SR_NO   Between " + start + " and " + last;
			try {
				return objWF.GetDataSet(strSQL);
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
				_with1.CommandText = objWK.MyUserName + ".S_CALL_TYPE_MST_TBL_PKG.S_CALL_TYPE_MST_TBL_INS";
				var _with2 = _with1.Parameters;
				insCommand.Parameters.Add("SALES_CALL_TYPE_ID_IN", OracleDbType.Varchar2, 0, "SALES_CALL_TYPE_ID").Direction = ParameterDirection.Input;
				insCommand.Parameters["SALES_CALL_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("SALES_CALL_TYPE_DESC_IN", OracleDbType.Varchar2, 0, "SALES_CALL_TYPE_DESC").Direction = ParameterDirection.Input;
				insCommand.Parameters["SALES_CALL_TYPE_DESC_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				insCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "SALES_CALL_TYPE_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with3 = delCommand;
				_with3.Connection = objWK.MyConnection;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWK.MyUserName + ".S_CALL_TYPE_MST_TBL_PKG.S_CALL_TYPE_MST_TBL_DEL";
				var _with4 = _with3.Parameters;
				delCommand.Parameters.Add("SALES_CALL_TYPE_PK_IN", OracleDbType.Int32, 10, "SALES_CALL_TYPE_PK").Direction = ParameterDirection.Input;
				delCommand.Parameters["SALES_CALL_TYPE_PK_IN"].SourceVersion = DataRowVersion.Current;
				delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
				delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with5 = updCommand;
				_with5.Connection = objWK.MyConnection;
				_with5.CommandType = CommandType.StoredProcedure;
				_with5.CommandText = objWK.MyUserName + ".S_CALL_TYPE_MST_TBL_PKG.S_CALL_TYPE_MST_TBL_UPD";
				var _with6 = _with5.Parameters;
				updCommand.Parameters.Add("SALES_CALL_TYPE_PK_IN", OracleDbType.Int32, 10, "SALES_CALL_TYPE_PK").Direction = ParameterDirection.Input;
				updCommand.Parameters["SALES_CALL_TYPE_PK_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("SALES_CALL_TYPE_ID_IN", OracleDbType.Varchar2, 0, "SALES_CALL_TYPE_ID").Direction = ParameterDirection.Input;
				updCommand.Parameters["SALES_CALL_TYPE_ID_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("SALES_CALL_TYPE_DESC_IN", OracleDbType.Varchar2, 0, "SALES_CALL_TYPE_DESC").Direction = ParameterDirection.Input;
				updCommand.Parameters["SALES_CALL_TYPE_DESC_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("ACTIVE_FLAG_IN", OracleDbType.Int32, 1, "ACTIVE_FLAG").Direction = ParameterDirection.Input;
				updCommand.Parameters["ACTIVE_FLAG_IN"].SourceVersion = DataRowVersion.Current;
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
				if (arrMessage.Count > 0) {
					return arrMessage;
				} else {
					if (Import == false) {
						arrMessage.Add("All Data Saved Successfully");
					} else {
						arrMessage.Add("Data Imported Successfully");
					}
					TRAN.Commit();
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion
		#region " Fetch for Import"
		public DataSet Fetch()
		{
			WorkFlow objWF = new WorkFlow();
			string SQL = null;
			SQL = "SELECT * FROM S_CALL_TYPE_MST_TBL";
			try {
				return objWF.GetDataSet(SQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion
	}
}
