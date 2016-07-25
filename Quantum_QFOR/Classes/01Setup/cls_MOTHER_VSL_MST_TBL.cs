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
    public class clsMOTHER_VSL_MST_TBL : CommonFeatures
	{

		#region "Fetch Function"
		public DataSet FetchAll(Int64 P_Mother_Vsl_Mst_Pk = 0, Int64 P_Active = 1, string P_Mother_Vsl_Id = "", string P_Mother_Vsl_Name = "", string P_Mother_Vsl_Optr = "", string SearchType = "", string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 3)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();

			if (P_Mother_Vsl_Mst_Pk > 0) {
				strCondition += " AND Mother_Vsl_Mst_Pk=" + P_Mother_Vsl_Mst_Pk;
			}

			if (P_Mother_Vsl_Id.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition += " AND UPPER(Mother_Vsl_Id) LIKE '" + P_Mother_Vsl_Id.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition += " AND UPPER(Mother_Vsl_Id) LIKE '%" + P_Mother_Vsl_Id.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition += " AND UPPER(Mother_Vsl_Id) LIKE '%" + P_Mother_Vsl_Id.ToUpper().Replace("'", "''") + "%'" ;
				}
			}


			if (P_Mother_Vsl_Name.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition += " AND UPPER(Mother_Vsl_Name) LIKE '" + P_Mother_Vsl_Name.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition += " AND UPPER(Mother_Vsl_Name) LIKE '%" + P_Mother_Vsl_Name.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition += " AND UPPER(Mother_Vsl_Name) LIKE '%" + P_Mother_Vsl_Name.ToUpper().Replace("'", "''") + "%'" ;
				}
			}


			if (P_Mother_Vsl_Optr.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition += " AND UPPER(Mother_Vsl_Optr) LIKE '" + P_Mother_Vsl_Optr.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition += " AND UPPER(Mother_Vsl_Optr) LIKE '%" + P_Mother_Vsl_Optr.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition += " AND UPPER(Mother_Vsl_Optr) LIKE '%" + P_Mother_Vsl_Optr.ToUpper().Replace("'", "''") + "%'" ;
				}
			}

			if (P_Active == 1) {
				strCondition += " AND Active = 1 ";
			} else {
				strCondition += "";
			}

			strSQL = "SELECT Count(*) from MOTHER_VSL_MST_TBL where 1=1";
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

			strSQL = " select * from (";
			strSQL = strSQL + "SELECT ROWNUM SR_NO, q.* from ";
			strSQL = strSQL + "(SELECT  ";
			strSQL = strSQL + " Mother_Vsl_Mst_Pk,";
			strSQL = strSQL + " Active,";
			strSQL = strSQL + " Mother_Vsl_Id,";
			strSQL = strSQL + " Mother_Vsl_Name,";
			strSQL = strSQL + " Mother_Vsl_Optr,";
			strSQL = strSQL + " Version_No ";
			strSQL = strSQL + " FROM MOTHER_VSL_MST_TBL ";
			strSQL = strSQL + " WHERE ( 1 = 1) ";
			strSQL += strCondition;
			strSQL += "order by Active,Mother_Vsl_Id) q  ) WHERE SR_NO  Between " + start + " and " + last;

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


			try {
				DataTable DtTbl = new DataTable();
				DataRow DtRw = null;
				int i = 0;
				DtTbl = M_DataSet.Tables[0];
				var _with1 = insCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".MOTHER_VSL_MST_TBL_PKG.MOTHER_VSL_MST_TBL_INS";
				var _with2 = _with1.Parameters;

				insCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 10, "ACTIVE").Direction = ParameterDirection.Input;
				insCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("MOTHER_VSL_ID_IN", OracleDbType.Varchar2, 50, "MOTHER_VSL_ID").Direction = ParameterDirection.Input;
				insCommand.Parameters["MOTHER_VSL_ID_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("MOTHER_VSL_NAME_IN", OracleDbType.Varchar2, 50, "MOTHER_VSL_NAME").Direction = ParameterDirection.Input;
				insCommand.Parameters["MOTHER_VSL_NAME_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("MOTHER_VSL_OPTR_IN", OracleDbType.Varchar2, 50, "MOTHER_VSL_OPTR").Direction = ParameterDirection.Input;
				insCommand.Parameters["MOTHER_VSL_OPTR_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				insCommand.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MOTHER_VSL_MST_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


				var _with3 = delCommand;
				_with3.Connection = objWK.MyConnection;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWK.MyUserName + ".MOTHER_VSL_MST_TBL_PKG.MOTHER_VSL_MST_TBL_DEL";
				var _with4 = _with3.Parameters;
				delCommand.Parameters.Add("MOTHER_VSL_MST_PK_IN", OracleDbType.Int32, 10, "MOTHER_VSL_MST_PK").Direction = ParameterDirection.Input;
				delCommand.Parameters["MOTHER_VSL_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

				delCommand.Parameters.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

				delCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "Version_No").Direction = ParameterDirection.Input;
				delCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				delCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				delCommand.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


				var _with5 = updCommand;
				_with5.Connection = objWK.MyConnection;
				_with5.CommandType = CommandType.StoredProcedure;
				_with5.CommandText = objWK.MyUserName + ".MOTHER_VSL_MST_TBL_PKG.MOTHER_VSL_MST_TBL_UPD";
				var _with6 = _with5.Parameters;

				updCommand.Parameters.Add("MOTHER_VSL_MST_PK_IN", OracleDbType.Int32, 10, "MOTHER_VSL_MST_PK").Direction = ParameterDirection.Input;
				updCommand.Parameters["MOTHER_VSL_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 10, "ACTIVE").Direction = ParameterDirection.Input;
				updCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("MOTHER_VSL_ID_IN", OracleDbType.Varchar2, 50, "MOTHER_VSL_ID").Direction = ParameterDirection.Input;
				updCommand.Parameters["MOTHER_VSL_ID_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("MOTHER_VSL_NAME_IN", OracleDbType.Varchar2, 50, "MOTHER_VSL_NAME").Direction = ParameterDirection.Input;
				updCommand.Parameters["MOTHER_VSL_NAME_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("MOTHER_VSL_OPTR_IN", OracleDbType.Varchar2, 50, "MOTHER_VSL_OPTR").Direction = ParameterDirection.Input;
				updCommand.Parameters["MOTHER_VSL_OPTR_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "Version_No").Direction = ParameterDirection.Input;
				updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				updCommand.Parameters["CONFIG_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

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
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully");
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
	}
}
