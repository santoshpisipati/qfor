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

//Option Strict On
//Option Explicit On 
using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class clsSALES_CALL_TRN : CommonFeatures
	{

		#region "Class Level variable"
		private enum Header
		{
			SLNO = 0,
			CALLPK = 1,
			CALLID = 2,
			CALLDT = 3,
			CUSTFK = 4,
			CUSTNAME = 5,
			CALLSTATUS = 6,
			VERSIONNO = 7,
			DELFLAG = 8,
			CHEFLAG = 9
		}
		#endregion

		#region "Fetch Function"
		public DataSet FetchAll(Int16 P_Sales_Call_Pk = 0, string P_Sales_Call_Id = "", string P_Sales_Call_Dt = "", Int32 P_Call_Status = 0, Int32 P_Customer_Mst_Fk = 0, string SearchType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 intFrst = 0, Int32 flag = 0)
		{

			string strSQL = null;
			string strSQL1 = null;
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strCondition = null;
			string strCondition1 = null;
			Int32 TotalRecords = default(Int32);
			DateTime EnteredDate = default(DateTime);
			WorkFlow objWF = new WorkFlow();
			System.Web.UI.Page objPage = new System.Web.UI.Page();
			long Location_PK = (Int64)HttpContext.Current.Session["LOGED_IN_LOC_FK"];

			if (flag == 0) {
				strCondition += " AND 1 = 2 ";
			}

			if (intFrst == 0) {
				strCondition += " And SCT.SALES_CALL_PK = 0 ";
			}

			if (P_Sales_Call_Pk > 0) {
				strCondition += " And SCT.SALES_CALL_PK =" + P_Sales_Call_Pk;
			}

			if (P_Sales_Call_Id.ToString().Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition += " And UPPER(SCT.SALES_CALL_ID) like '" + P_Sales_Call_Id.ToUpper().Replace("'", "''") + "%' ";
					} else {
						strCondition += " And UPPER(SCT.SALES_CALL_ID) like '%" + P_Sales_Call_Id.ToUpper().Replace("'", "''") + "%' ";
					}
				} else {
					strCondition += " And UPPER(SCT.SALES_CALL_ID) like '%" + P_Sales_Call_Id.ToUpper().Replace("'", "''") + "%' ";
				}
			}

			if (P_Sales_Call_Dt.ToString().Trim().Length > 0) {
               
				strCondition = strCondition + " AND SCT.SALES_CALL_DT = TO_DATE('" + Convert.ToDateTime(P_Sales_Call_Dt).ToString("{0:mm/dd/yyyy}") + "','dd/mm/yyyy')";
			}

			if (P_Call_Status > 0) {
				strCondition += " And SCT.CALL_STATUS =" + P_Call_Status;
			}

			if (P_Customer_Mst_Fk > 0) {
				strCondition += " And SCT.CUSTOMER_MST_FK =" + P_Customer_Mst_Fk;
			}

			strSQL = " SELECT ";
			strSQL += " SCT.SALES_CALL_PK, ";
			strSQL += " SCT.SALES_CALL_ID, ";
			strSQL += " TO_DATE(SCT.SALES_CALL_DT, 'DD/MM/YYYY') SALES_CALL_DT, ";
			strSQL += " SCT.CUSTOMER_MST_FK, ";
			strSQL += " CUST.CUSTOMER_ID, ";
			strSQL += " CUST.CUSTOMER_NAME, ";
			strSQL += " DECODE(SCT.CALL_STATUS, '1', 'Active', '2', 'Prospective', '3', 'Acquired', '4','Escalate', '5','Lost'), ";
			strSQL += " SCT.PLAN_STATUS, ";
			strSQL += " SCT.VERSION_NO, TO_CHAR(SCF.FOLLOWUP_DATE, 'DD/MM/YYYY') FOLLOWUP_DATE, '' DELFLAG ";
			strSQL += " FROM ";
			strSQL += " SALES_CALL_TRN SCT, ";
			strSQL += " CUSTOMER_MST_TBL CUST, LOCATION_MST_TBL LMT, CUSTOMER_CONTACT_DTLS CCD, SALES_CALL_FOLLOWUP SCF ";
			strSQL += " WHERE SCT.CUSTOMER_MST_FK=CUST.CUSTOMER_MST_PK ";
			strSQL += " AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK ";
			strSQL += " AND LMT.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"];
			strSQL += " AND CUST.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ";
			strSQL += " AND SCT.PLAN_STATUS in (2,5) AND SCF.SALES_CALL_FK(+) = SCT.SALES_CALL_PK ";

			if (IsAdministrator() == 1) {
				strSQL1 = strSQL + strCondition;
			} else {
				strSQL1 = strSQL + strCondition + " AND SCT.EMPLOYEE_MST_FK = " + HttpContext.Current.Session["EMP_PK"];
				strSQL1 += " Union ";
				strSQL1 += strSQL + " AND SCF.FOLLOWUP_BY = " + HttpContext.Current.Session["EMP_PK"];
			}

			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler("SELECT COUNT(*) FROM (" + strSQL1 + ")"));
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

			strSQL1 = " SELECT * FROM (SELECT ROWNUM SR_NO, Q.* FROM (" + strSQL1 + "  ORDER BY SALES_CALL_PK DESC ) Q) ";
			strSQL1 += " WHERE SR_NO BETWEEN " + start + " AND " + last;

			try {
				return objWF.GetDataSet(strSQL1);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Fetch SalesCall Details"
		public object FetchCallDetails(long CallPK, string CallStatus)
		{
			string strSQL = null;
			strSQL = string.Empty ;
			strSQL += " select " ;
			strSQL += "SCT.SALES_CALL_PK,";
			strSQL += "SCT.SALES_CALL_ID,";
			strSQL += "SCT.SALES_CALL_DT,";
			strSQL += "SCT.CUSTOMER_MST_FK,";
			strSQL += "CUST.CUSTOMER_ID,";
			strSQL += "CUST.CUSTOMER_NAME,";
			strSQL += "NVL(SCT.PERSON_SALESCALL_MET,SCT.PERSON_MET) AS PERSON_MET,";
			strSQL += "SCT.SALES_CALL_REASON,";
			strSQL += "SCT.SAL_CAL_REASON_MST_FK REASON_FK,";
			strSQL += "SCT.CALL_TYPE CALLTYPE,";
			strSQL += "nvl(SCT.Biztype,0) Biztype,";
			strSQL += "SCT.FR_TIME,";
			strSQL += "SCT.TO_TIME,";
			strSQL += "SCT.CALL_TREND,";
			strSQL += "DECODE(SCT.CALL_STATUS,'1','Active','2','Prospective','3','Acquired','4','Escalated','5','Lost','6' ,'Cancelled') STATUS,";

			if (CallStatus.ToUpper() == "ACTIVE" | CallStatus.ToUpper() == "PROSPECTIVE" | CallStatus.ToUpper() == "ACQUIRED" | CallStatus.ToUpper() == "APPROVED") {
				strSQL += "SCT.NEXT_ACTION_FK,";
				strSQL += "NX.NEXT_ACTIONS_DESC,";
				strSQL += "SCT.NEXT_ACTION_DT,";
				strSQL += "SCT.NEXT_ACTION,";
			} else {
				strSQL += "FAL.FAIL_ON_ACCOUNT,";
				strSQL += "FAL.FAIL_FACTOR,";
			}
			strSQL += "SCT.NOTES,";
			strSQL += "SCT.IF_LOST_REASON,";
			strSQL += "SCT.VERSION_NO ,EMP.EMPLOYEE_NAME,SCT.ASSIGN_TO_FK";
			strSQL += "FROM ";
			strSQL += "SALES_CALL_TRN SCT,";
			strSQL += " S_CALL_TYPE_MST_TBL STY, ";
			strSQL += " SAL_CAL_REASON_MST_TBL SCR,";


			strSQL += "CUSTOMER_MST_TBL CUST,EMPLOYEE_MST_TBL EMP";

			if (CallStatus.ToUpper() == "ACTIVE" | CallStatus.ToUpper() == "PROSPECTIVE" | CallStatus.ToUpper() == "ACQUIRED" | CallStatus.ToUpper() == "APPROVED") {
				strSQL += ",";
				strSQL += "NEXT_ACTIONS_TBL NX";
			} else {
				strSQL += ",";
				strSQL += "FAIL_ACCOUNT_MST_TBL FAL";
			}
			strSQL += "WHERE";
			strSQL += "SCT.CUSTOMER_MST_FK=CUST.CUSTOMER_MST_PK";
			strSQL += " AND SCT.SAL_CAL_REASON_MST_FK = SCR.SAL_CAL_REASON_MST_TBL_PK(+) ";
			strSQL += " AND SCT.CALL_TYPE = STY.SALES_CALL_TYPE_PK(+)";
			strSQL += " AND EMP.EMPLOYEE_MST_PK(+) = SCT.ASSIGN_TO_FK ";

			if (CallStatus.ToUpper() == "ACTIVE" | CallStatus.ToUpper() == "PROSPECTIVE" | CallStatus.ToUpper() == "ACQUIRED" | CallStatus.ToUpper() == "APPROVED") {
				strSQL += "AND SCT.NEXT_ACTION_FK=NX.NEXT_ACTIONS_TBL_PK (+)";
			}

			strSQL += "AND SCT.SALES_CALL_PK=" + CallPK;

			WorkFlow objWF = new WorkFlow();
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

		#region "Generate Protocol For Sales Call"
		public string GenProto(string Key, Int64 ILocationId, Int64 IEmployeeId, Int64 UserId)
		{
			string strGenerateProto = null;
			strGenerateProto = GenerateProtocolKey(Key, ILocationId, IEmployeeId, DateTime.Now, "","" , "", UserId);
			return strGenerateProto;
		}
		#endregion

		#region "Save Function"
		public ArrayList Save(DataSet M_DataSet, DataSet G_DataSet, long SalesCallPK)
		{

			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;
			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();

			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();


			try {
				var _with1 = insCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".SALES_CALL_TRN_PKG.SALES_CALL_TRN_INS";

				insCommand.Parameters.Add("SALES_CALL_ID_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SALES_CALL_ID"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SALES_CALL_ID"])).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("SALES_CALL_DT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SALES_CALL_DT"].ToString()) ? DateTime.MinValue : Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["SALES_CALL_DT"]))).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("FR_TIME_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["FR_TIME"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["FR_TIME"])).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("TO_TIME_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["TO_TIME"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["TO_TIME"])).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("BIZ_TYPE_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["BIZTYPE"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["BIZTYPE"])).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("SALES_CALL_TYPE_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SALES_CALL_TYPE_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SALES_CALL_TYPE_FK"])).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("EMPLOYEE_MST_PK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["EMPLOYEE_MST_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["EMPLOYEE_MST_PK"])).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CALL_RESULT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CALL_RESULT"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CALL_RESULT"])).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CALL_TREND_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CALL_TREND"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CALL_TREND"])).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CALL_STATUS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CALL_STATUS"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CALL_STATUS"])).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("SALES_CALL_REASON_PK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SAL_CAL_REASON_MST_TBL_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SAL_CAL_REASON_MST_TBL_PK"])).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("PERSON_TO_MEET_PK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["PERSON_TO_MEET_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["PERSON_TO_MEET_PK"])).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

				var _with2 = updCommand;
				_with2.Connection = objWK.MyConnection;
				_with2.CommandType = CommandType.StoredProcedure;
				_with2.CommandText = objWK.MyUserName + ".SALES_CALL_TRN_PKG.SALES_CALL_TRN_UPD";

				updCommand.Parameters.Add("SALES_CALL_PK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SALES_CALL_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SALES_CALL_PK"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("SALES_CALL_ID_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SALES_CALL_ID"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SALES_CALL_ID"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("SALES_CALL_DT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SALES_CALL_DT"].ToString()) ? DateTime.MinValue : Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["SALES_CALL_DT"]))).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("FR_TIME_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["FR_TIME"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["FR_TIME"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("TO_TIME_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["TO_TIME"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["TO_TIME"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("BIZ_TYPE_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["BIZTYPE"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["BIZTYPE"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("SALES_CALL_TYPE_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SALES_CALL_TYPE_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SALES_CALL_TYPE_FK"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("EMPLOYEE_MST_PK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["EMPLOYEE_MST_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["EMPLOYEE_MST_PK"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("CALL_RESULT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CALL_RESULT"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CALL_RESULT"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("CALL_TREND_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CALL_TREND"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CALL_TREND"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("CALL_STATUS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CALL_STATUS"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CALL_STATUS"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("SALES_CALL_REASON_PK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SAL_CAL_REASON_MST_TBL_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SAL_CAL_REASON_MST_TBL_PK"])).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("PERSON_TO_MEET_PK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["PERSON_TO_MEET_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["PERSON_TO_MEET_PK"])).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                

				var _with3 = objWK.MyDataAdapter;

				if (SalesCallPK <= 0) {
					_with3.InsertCommand = insCommand;
					_with3.InsertCommand.Transaction = TRAN;

					RecAfct = _with3.InsertCommand.ExecuteNonQuery();
					SalesCallPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
				} else {
					_with3.UpdateCommand = updCommand;
					_with3.UpdateCommand.Transaction = TRAN;
					RecAfct = _with3.UpdateCommand.ExecuteNonQuery();
					SalesCallPK = (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SALES_CALL_PK"].ToString()) ? 0 : Convert.ToInt64(M_DataSet.Tables[0].Rows[0]["SALES_CALL_PK"]));
				}

				if (RecAfct > 0) {
					if (arrMessage.Count == 0) {
						if (SaveComments(G_DataSet, SalesCallPK, TRAN) > 0) {
							TRAN.Commit();
							arrMessage.Add("All Data Saved Successfully");
							arrMessage.Add(SalesCallPK);
							return arrMessage;
						} else {
							arrMessage.Add("Error");
							TRAN.Rollback();
							return arrMessage;
						}
					} else {
						TRAN.Rollback();
						return arrMessage;
					}
				} else {
					TRAN.Rollback();
					arrMessage.Add("Error");
					return arrMessage;
				}

			} catch (OracleException oraEx) {
				TRAN.Rollback();
				arrMessage.Add(oraEx.Message);
				return arrMessage;
			} catch (Exception ex) {
				throw ex;
			} finally {
				if (objWK.MyConnection.State == ConnectionState.Open) {
					objWK.MyConnection.Close();
				}
			}

		}
		#endregion

		#region " Save Comments "
		private long SaveComments(DataSet G_DataSet, long SalesCallPK, OracleTransaction TRAN)
		{

			WorkFlow objWS = new WorkFlow();
			WorkFlow objWK = new WorkFlow();
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();

			Int32 SalesCommentsPK = default(Int32);
			Int32 RecAfct = default(Int32);
			int i = 0;
			string strLeadSrcPKs = "0";

			for (i = 0; i <= G_DataSet.Tables[0].Rows.Count - 1; i++) {
				if (!string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[i]["SALES_CALL_COMMENTS_PK"].ToString())) {
					if (Convert.ToInt32(G_DataSet.Tables[0].Rows[i]["SALES_CALL_COMMENTS_PK"]) > 0) {
						strLeadSrcPKs = strLeadSrcPKs + ", " + G_DataSet.Tables[0].Rows[i]["SALES_CALL_COMMENTS_PK"];
					}
				}
			}

			objWK.ExecuteCommands("DELETE FROM SALES_CALL_COMMENTS SCC WHERE SCC.SALES_CALL_COMMENTS_PK NOT IN (" + strLeadSrcPKs + ")");

			objWK.MyConnection = TRAN.Connection;



			try {
				objWS.MyCommand.CommandType = CommandType.StoredProcedure;

				for (i = 0; i <= G_DataSet.Tables[0].Rows.Count - 1; i++) {
					var _with4 = insCommand;
					_with4.Connection = objWK.MyConnection;
					_with4.CommandType = CommandType.StoredProcedure;
					_with4.CommandText = objWK.MyUserName + ".SALES_CALL_TRN_PKG.SALES_CALL_CMT_INS";
					_with4.Parameters.Clear();
					var _with5 = _with4.Parameters;

					insCommand.Parameters.Add("SALES_CALL_PK_IN", SalesCallPK).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("COMMENTS_DATE_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[i]["COMMENTS_DATE"].ToString()) ? DateTime.MinValue : Convert.ToDateTime(G_DataSet.Tables[0].Rows[i]["COMMENTS_DATE"]))).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("EXECUTIVE_MST_FK_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[i]["EXECUTIVE_MST_FK"].ToString()) ? "" : G_DataSet.Tables[0].Rows[i]["EXECUTIVE_MST_FK"])).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("COMMENTS_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[i]["COMMENTS"].ToString()) ? "" : G_DataSet.Tables[0].Rows[i]["COMMENTS"])).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;

					insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

					var _with6 = updCommand;
					_with6.Connection = objWK.MyConnection;
					_with6.CommandType = CommandType.StoredProcedure;
					_with6.CommandText = objWK.MyUserName + ".SALES_CALL_TRN_PKG.SALES_CALL_CMT_UPD";
					_with6.Parameters.Clear();
					var _with7 = _with6.Parameters;
					updCommand.Parameters.Add("SALES_CALL_COMMENTS_PK_IN", G_DataSet.Tables[0].Rows[i]["SALES_CALL_COMMENTS_PK"]).Direction = ParameterDirection.Input;
					updCommand.Parameters.Add("SALES_CALL_PK_IN", SalesCallPK).Direction = ParameterDirection.Input;
					updCommand.Parameters.Add("COMMENTS_DATE_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[i]["COMMENTS_DATE"].ToString()) ? DateTime.MinValue : Convert.ToDateTime(G_DataSet.Tables[0].Rows[i]["COMMENTS_DATE"]))).Direction = ParameterDirection.Input;
					updCommand.Parameters.Add("EXECUTIVE_MST_FK_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[i]["EXECUTIVE_MST_FK"].ToString()) ? "" : G_DataSet.Tables[0].Rows[i]["EXECUTIVE_MST_FK"])).Direction = ParameterDirection.Input;
					updCommand.Parameters.Add("COMMENTS_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[i]["COMMENTS"].ToString()) ? "" : G_DataSet.Tables[0].Rows[i]["COMMENTS"])).Direction = ParameterDirection.Input;
					updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;

					updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    

					var _with8 = objWK.MyDataAdapter;

					SalesCommentsPK = (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[i]["SALES_CALL_COMMENTS_PK"].ToString()) ? 0 : Convert.ToInt32(G_DataSet.Tables[0].Rows[i]["SALES_CALL_COMMENTS_PK"]));

					if (SalesCommentsPK <= 0) {
						_with8.InsertCommand = insCommand;
						_with8.InsertCommand.Transaction = TRAN;
						RecAfct = RecAfct + _with8.InsertCommand.ExecuteNonQuery();
					} else {
						_with8.UpdateCommand = updCommand;
						_with8.UpdateCommand.Transaction = TRAN;
						RecAfct = RecAfct + _with8.UpdateCommand.ExecuteNonQuery();
					}

				}

				return RecAfct;

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region " Save Followup Function "
		public ArrayList SaveFollowUp(long SalesCallFollowUpPK, long SalesCallPK, System.DateTime FollowUpDate, long FollowUpBy, string FollowUpTime)
		{

			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;
			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();

			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();


			try {
				var _with9 = insCommand;
				_with9.Connection = objWK.MyConnection;
				_with9.CommandType = CommandType.StoredProcedure;
				_with9.CommandText = objWK.MyUserName + ".SALES_CALL_FOLLOWUP_PKG.SALES_CALL_FOLLOWUP_INS";

				insCommand.Parameters.Add("SALES_CALL_FK_IN", SalesCallPK).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("FOLLOWUP_DATE_IN", FollowUpDate).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("FOLLOWUP_BY_IN", FollowUpBy).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("FOLLOWUP_TIME_IN", FollowUpTime).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

				var _with10 = updCommand;
				_with10.Connection = objWK.MyConnection;
				_with10.CommandType = CommandType.StoredProcedure;
				_with10.CommandText = objWK.MyUserName + ".SALES_CALL_FOLLOWUP_PKG.SALES_CALL_FOLLOWUP_INS";

				updCommand.Parameters.Add("SALES_CALL_FOLLOWUP_PK_IN", SalesCallFollowUpPK).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("SALES_CALL_FK_IN", SalesCallPK).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("FOLLOWUP_DATE_IN", FollowUpDate).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("FOLLOWUP_BY_IN", FollowUpBy).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("FOLLOWUP_TIME_IN", FollowUpTime).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                

				var _with11 = objWK.MyDataAdapter;

				if (SalesCallFollowUpPK <= 0) {
					_with11.InsertCommand = insCommand;
					_with11.InsertCommand.Transaction = TRAN;

					RecAfct = _with11.InsertCommand.ExecuteNonQuery();
					SalesCallFollowUpPK = Convert.ToInt64(insCommand.Parameters["RETURN_VALUE"].Value);
				} else {
					_with11.UpdateCommand = updCommand;
					_with11.UpdateCommand.Transaction = TRAN;
					RecAfct = _with11.UpdateCommand.ExecuteNonQuery();
				}

				if (RecAfct > 0) {
					if (arrMessage.Count == 0) {
						TRAN.Commit();
						arrMessage.Add("All Data Saved Successfully");
						arrMessage.Add(SalesCallPK);
						return arrMessage;
					} else {
						TRAN.Rollback();
						return arrMessage;
					}
				} else {
					TRAN.Rollback();
					arrMessage.Add("Error");
					return arrMessage;
				}

			} catch (OracleException oraEx) {
				TRAN.Rollback();
				arrMessage.Add(oraEx.Message);
				return arrMessage;
			} catch (Exception ex) {
				throw ex;
			} finally {
				if (objWK.MyConnection.State == ConnectionState.Open) {
					objWK.MyConnection.Close();
				}
			}

		}
		#endregion

		#region " Save Escalate To Function "
		public ArrayList SaveEscalateTo(long SalesCallPK, long EscalateToPK, string Reason)
		{

			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			int RecAft = 0;


			try {
				sb.Append(" UPDATE SALES_CALL_TRN SCT SET SCT.EMPLOYEE_MST_FK = " + EscalateToPK);
				sb.Append(" , SCT.ESCALATE_TO_REASON = '" + Reason);
				sb.Append("' WHERE SCT.SALES_CALL_PK = " + SalesCallPK);

				objWF.ExecuteCommands(sb.ToString());

				arrMessage.Add("All Data Saved Successfully");

				return arrMessage;

			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}

		}
		#endregion

		#region " Save Delegate To Function "
		public ArrayList SaveDelegateTo(long SalesCallPK, long DelegateToPK, string Reason)
		{

			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			int RecAft = 0;

			try {
				sb.Append(" UPDATE SALES_CALL_TRN SCT SET SCT.EMPLOYEE_MST_FK = " + DelegateToPK);
				sb.Append(" , SCT.DELEGATE_TO_REASON = '" + Reason);
				sb.Append("' WHERE SCT.SALES_CALL_PK = " + SalesCallPK);

				objWF.ExecuteCommands(sb.ToString());
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}

		}
        #endregion

        #region "Insert Function"
        public ArrayList InsertSalesCalls(string SalesCallID, string SalesCallDt, long CustomerFK, string PersonMet, string CallReason, long CallType, long CallTrend, long CallStatus, long NextActionFK, string NextActionDt,
        string NextAction, string Notes, string IfLostReason, long FailAcct, long FailFactor, Int64 ConfigPK, string FRTime, string TOTime, DataSet M_Dataset, string Reasonpk = "",
        Int32 Assign = 0, long BizType = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            try {
                WorkFlow objWS = new WorkFlow();
                Int32 intPkVal = default(Int32);
                System.DBNull StrNUll = null;
                DateTime SaleCallTime = default(DateTime);
                DateTime NextActTime = default(DateTime);


                objWS.MyCommand.CommandType = CommandType.StoredProcedure;

                var _with12 = objWS.MyCommand;
                _with12.Parameters.Add("SALES_CALL_ID_IN", SalesCallID).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(SalesCallDt))
                {
                    _with12.Parameters.Add("SALES_CALL_DT_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    SaleCallTime = Convert.ToDateTime("#" + SalesCallDt + "#");
                    _with12.Parameters.Add("SALES_CALL_DT_IN", Convert.ToDateTime(SalesCallDt)).Direction = ParameterDirection.Input;
                }
                _with12.Parameters.Add("CUSTOMER_MST_FK_IN", CustomerFK).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("PERSON_MET_IN", PersonMet).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(Reasonpk)) {
                    _with12.Parameters.Add("SALES_CALL_REASON_PK_IN", "").Direction = ParameterDirection.Input;
                } else {
                    _with12.Parameters.Add("SALES_CALL_REASON_PK_IN", Reasonpk).Direction = ParameterDirection.Input;
                }
                _with12.Parameters.Add("SALES_CALL_REASON_IN", CallReason).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("CALL_TYPE_IN", CallType).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("CALL_TREND_IN", CallTrend).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("CALL_STATUS_IN", CallStatus).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("NEXT_ACTION_FK_IN", NextActionFK).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(NextActionDt)) {
                    _with12.Parameters.Add("NEXT_ACTION_DT_IN", "").Direction = ParameterDirection.Input;
                } else {
                    NextActTime = Convert.ToDateTime("#" + NextActionDt + "#");
                    _with12.Parameters.Add("NEXT_ACTION_DT_IN", NextActTime).Direction = ParameterDirection.Input;
                }

                _with12.Parameters.Add("NEXT_ACTION_IN", NextAction).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("NOTES_IN", Notes).Direction = ParameterDirection.Input;
                if ((IfLostReason != null)) {
                    _with12.Parameters.Add("IF_LOST_REASON_IN", IfLostReason).Direction = ParameterDirection.Input;
                } else {
                    _with12.Parameters.Add("IF_LOST_REASON_IN", "").Direction = ParameterDirection.Input;
                }
                if (!(FailAcct == 0)) {
                    _with12.Parameters.Add("FAIL_ON_ACCOUNT_IN", FailAcct).Direction = ParameterDirection.Input;
                } else {
                    _with12.Parameters.Add("FAIL_ON_ACCOUNT_IN", "").Direction = ParameterDirection.Input;
                }

                if (!(FailFactor == 0)) {
                    _with12.Parameters.Add("FAIL_FACTOR_IN", FailFactor).Direction = ParameterDirection.Input;
                } else {
                    _with12.Parameters.Add("FAIL_FACTOR_IN", "").Direction = ParameterDirection.Input;
                }

                _with12.Parameters.Add("SALES_PLAN_FK_IN", "").Direction = ParameterDirection.Input;

                _with12.Parameters.Add("NEXT_SALES_PLAN_FK_IN", "").Direction = ParameterDirection.Input;
                if (ExecPk > 0) {
                    _with12.Parameters.Add("CREATED_BY_FK_IN", ExecPk).Direction = ParameterDirection.Input;
                } else {
                    _with12.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                }
                _with12.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("FR_TIME_IN", FRTime).Direction = ParameterDirection.Input;

                _with12.Parameters.Add("TO_TIME_IN", TOTime).Direction = ParameterDirection.Input;
                _with12.Parameters.Add("ASSIGN_TO", Assign).Direction = ParameterDirection.Input;

                _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "SALES_CALL_TRN_PK").Direction = ParameterDirection.Output;
                objWS.MyCommand.Parameters["IF_LOST_REASON_IN"].Size = 200;
                objWS.MyCommand.Parameters["NEXT_ACTION_IN"].Size = 200;
                objWS.MyCommand.Parameters["NOTES_IN"].Size = 200;
                long PKValue = 0;
                Int32 intPKValue = default(Int32);
                objWS.MyCommand.CommandText = objWS.MyUserName + ".SALES_CALL_TRN_PKG.SALES_CALL_TRN_INS";
                if (objWS.ExecuteCommands() == true) {
                    TRAN.Commit();
                    PKValue = Convert.ToInt64(objWS.MyCommand.Parameters["RETURN_VALUE"].Value);
                    arrMessage.Add("All Data Saved Successfully");
                    arrMessage.Add(PKValue);
                    return arrMessage;
                } else {
                    return arrMessage;
                }
            } catch (OracleException OraExp) {
                throw OraExp;
            } catch (Exception ex) {
                TRAN.Rollback();
                return arrMessage;
            } finally {
                objWK.MyCommand.Connection.Close();
            }
        }
		#endregion

		#region "Delete Function"
		public ArrayList DelSave(DataSet M_DataSet, bool DelFlg = false)
		{
			WorkFlow objWK = new WorkFlow();
			OracleTransaction oraTran = null;
			int intPKVal = 0;
			Int32 inti = default(Int32);
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand delCommand = new OracleCommand();
			Int16 i = default(Int16);
			try {
				objWK.OpenConnection();
				oraTran = objWK.MyConnection.BeginTransaction();
				objWK.MyCommand.Transaction = oraTran;
				var _with13 = delCommand;
				_with13.Connection = objWK.MyConnection;
				_with13.CommandType = CommandType.StoredProcedure;
				_with13.CommandText = objWK.MyUserName + ".SALES_CALL_TRN_PKG.SALES_CALL_TRN_DEL";
				var _with14 = _with13.Parameters;
				_with14.Add("SALES_CALL_PK_IN", OracleDbType.Int32, 10, "SALES_CALL_PK").Direction = ParameterDirection.Input;

				_with14.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

				_with14.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;

				_with14.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

				delCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with15 = objWK.MyDataAdapter;
				_with15.DeleteCommand = delCommand;
				_with15.DeleteCommand.Transaction = oraTran;
				RecAfct = _with15.Update(M_DataSet);

				if (arrMessage.Count > 0) {
					oraTran.Rollback();
					return arrMessage;
				} else {
					arrMessage.Add("All Data Saved Successfully");
					oraTran.Commit();
					return arrMessage;
				}
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				oraTran.Rollback();
				return arrMessage;
			} finally {
				objWK.MyConnection.Close();
			}
		}
		#endregion

		#region "Cancelled"
		public object Cancelled(string strPk)
		{
			string strSQL = null;
			string[] PK = strPk.Split(',');
			WorkFlow objWF = new WorkFlow();
			try {
				strSQL = " update sales_call_trn sct ";
				strSQL = strSQL + " set sct.PLAN_STATUS = 4 ";
				strSQL = strSQL + " where sct.sales_call_pk in (" + strPk + ") ";
				return objWF.ExecuteScaler(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region " Fetch Call Type "
		public DataSet FetchCallType()
		{
			try {
				string StrSql = null;
				WorkFlow objWF = new WorkFlow();
				StrSql = " SELECT 0 SALES_CALL_TYPE_PK, '' SALES_CALL_TYPE_DESC FROM DUAL UNION ";
				StrSql += " SELECT SCT.SALES_CALL_TYPE_PK, SCT.SALES_CALL_TYPE_DESC FROM S_CALL_TYPE_MST_TBL SCT ";
				return objWF.GetDataSet(StrSql);
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "FetchOutStand"
		public DataSet FetchOutStand(int CustPk)
		{
			WorkFlow objWF = new WorkFlow();
			DataSet ds = new DataSet();
			string strSQL = null;
			string strSQL1 = null;
			Int16 BizType = default(Int16);
			strSQL1 = "SELECT CMST.BUSINESS_TYPE FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK = " + CustPk;
			BizType = Convert.ToInt16(objWF.ExecuteScaler(strSQL1.ToString()));
			try {
				if (BizType == 2 | BizType == 0 | BizType == 1) {
					strSQL = "SELECT ROWNUM SR_NO , A.JOBCARD_REF_NO,A.JOBCARD_DATE,A.INVOICE_REF_NO,A.INVOICE_DATE,A.DUEDATE,A.CURRENCY_ID,A.INVOICE,A.INVOICE_AMT-A.OUT_AMOUNT,";
					strSQL = strSQL + " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else A.OUTSTD_DAYS end OUTSTD_DAYS from  (";
					strSQL = strSQL + " select JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,sum(OUT_AMOUNT) OUT_AMOUNT, CASE WHEN OUTSTD_DAYS <= 0 THEN 0 ELSE OUTSTD_DAYS END OUTSTD_DAYS from (";

					strSQL = strSQL + " SELECT JOB.JOBCARD_REF_NO,";
					strSQL = strSQL + " TO_CHAR(TO_DATE(JOB.JOBCARD_DATE),'DD/MM/YYYY') JOBCARD_DATE,";
					strSQL = strSQL + " CIT.INVOICE_REF_NO,";
					strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE),'DD/MM/YYYY') INVOICE_DATE,";
					strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE) + nvl(cust.credit_days, 0),'DD/MM/YYYY') DUEDATE,";
					strSQL = strSQL + " CURR.CURRENCY_ID,";
					strSQL = strSQL + " CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ",CIT.INVOICE_DATE) INVOICE,";
					strSQL = strSQL + " ((CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) - ";
					strSQL = strSQL + " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)))INVOICE_AMT,";
					strSQL = strSQL + " (nvl(COLT.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(Col.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + ",Col.Collections_Date)) OUT_AMOUNT,";


					strSQL = strSQL + " ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - (cit.invoice_date + nvl(cust.credit_days,0)),0)) OUTSTD_DAYS";
					strSQL = strSQL + "  FROM";
					strSQL = strSQL + " consol_invoice_tbl CIT,";
					strSQL = strSQL + " consol_invoice_trn_tbl CITT,";
					if (BizType == 2 | BizType == 0) {
						strSQL = strSQL + " job_card_sea_exp_tbl job,";
						strSQL = strSQL + " booking_sea_tbl book,";
					} else if (BizType == 1) {
						strSQL = strSQL + " job_card_AIR_exp_tbl job,";
						strSQL = strSQL + " booking_AIR_tbl book,";
					}
					strSQL = strSQL + " customer_mst_tbl cust,";
					strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CURR,";
					strSQL = strSQL + " collections_tbl        col,";
					strSQL = strSQL + " collections_trn_tbl    colt";
					strSQL = strSQL + " WHERE";
					strSQL = strSQL + " cit.consol_invoice_pk = citt.consol_invoice_fk";
					if (BizType == 2 | BizType == 0) {
						strSQL = strSQL + " AND citt.job_card_fk = job.job_card_sea_exp_pk";
						strSQL = strSQL + " AND job.booking_sea_fk = book.booking_sea_pk";
					} else if (BizType == 1) {
						strSQL = strSQL + " AND citt.job_card_fk = job.job_card_AIR_exp_pk";
						strSQL = strSQL + " AND job.booking_AIR_fk = book.booking_AIR_pk";
					}
					strSQL = strSQL + " AND book.cust_customer_mst_fk = cust.customer_mst_pk(+)";
					strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
					strSQL = strSQL + " AND book.cust_customer_mst_fk = " + CustPk;
					strSQL = strSQL + " AND cit.invoice_date <= SYSDATE";
					strSQL = strSQL + " AND COL.COLLECTIONS_TBL_PK = COLT.COLLECTIONS_TBL_FK";
					strSQL = strSQL + " AND COLT.INVOICE_REF_NR = CIT.INVOICE_REF_NO ";
					strSQL = strSQL + " GROUP BY";
					strSQL = strSQL + " JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,CIT.INVOICE_DATE,nvl(cust.credit_days, 0),CURRENCY_ID,CIT.CURRENCY_MST_FK,";
					strSQL = strSQL + " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)))";
					strSQL = strSQL + " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS)A";
					strSQL = strSQL + "   WHERE (A.INVOICE_AMT - A.OUT_AMOUNT) > 0 ";
				}

				if (BizType == 3 | BizType == 4) {
					strSQL = " SELECT ROWNUM SR_NO,Q.* FROM(";
					strSQL = strSQL + "(SELECT A.JOBCARD_REF_NO,A.JOBCARD_DATE,A.INVOICE_REF_NO,A.INVOICE_DATE,A.DUEDATE,A.CURRENCY_ID,A.INVOICE,A.INVOICE_AMT-A.OUT_AMOUNT,";
					strSQL = strSQL + " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else A.OUTSTD_DAYS end OUTSTD_DAYS from  (";
					strSQL = strSQL + " select JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,sum(OUT_AMOUNT) OUT_AMOUNT, CASE WHEN OUTSTD_DAYS <= 0 THEN 0 ELSE OUTSTD_DAYS END OUTSTD_DAYS from (";

					strSQL = strSQL + " SELECT JOB.JOBCARD_REF_NO,";
					strSQL = strSQL + " TO_CHAR(TO_DATE(JOB.JOBCARD_DATE),'DD/MM/YYYY') JOBCARD_DATE,";
					strSQL = strSQL + " CIT.INVOICE_REF_NO,";
					strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE),'DD/MM/YYYY')INVOICE_DATE,";
					strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE) + nvl(cust.credit_days, 0),'DD/MM/YYYY') DUEDATE,";
					strSQL = strSQL + " CURR.CURRENCY_ID,";
					strSQL = strSQL + " CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ",CIT.INVOICE_DATE) INVOICE,";
					strSQL = strSQL + " ((CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) - ";
					strSQL = strSQL + " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)))INVOICE_AMT,";
					strSQL = strSQL + " (nvl(COLT.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(Col.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + ",Col.Collections_Date)) OUT_AMOUNT,";


					strSQL = strSQL + " ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - (cit.invoice_date + nvl(cust.credit_days,0)),0)) OUTSTD_DAYS";
					strSQL = strSQL + "  FROM";
					strSQL = strSQL + " consol_invoice_tbl CIT,";
					strSQL = strSQL + " consol_invoice_trn_tbl CITT,";

					strSQL = strSQL + " job_card_sea_exp_tbl job,";
					strSQL = strSQL + " booking_sea_tbl book,";

					strSQL = strSQL + " customer_mst_tbl cust,";
					strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CURR,";
					strSQL = strSQL + " collections_tbl        col,";
					strSQL = strSQL + " collections_trn_tbl    colt";
					strSQL = strSQL + " WHERE";
					strSQL = strSQL + " cit.consol_invoice_pk = citt.consol_invoice_fk";

					strSQL = strSQL + " AND citt.job_card_fk = job.job_card_sea_exp_pk";
					strSQL = strSQL + " AND job.booking_sea_fk = book.booking_sea_pk";

					strSQL = strSQL + " AND book.cust_customer_mst_fk = cust.customer_mst_pk(+)";
					strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
					strSQL = strSQL + " AND book.cust_customer_mst_fk = " + CustPk;
					strSQL = strSQL + " AND cit.invoice_date <= SYSDATE";
					strSQL = strSQL + " AND COL.COLLECTIONS_TBL_PK = COLT.COLLECTIONS_TBL_FK";
					strSQL = strSQL + " AND COLT.INVOICE_REF_NR = CIT.INVOICE_REF_NO ";
					strSQL = strSQL + " GROUP BY";
					strSQL = strSQL + " JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,CIT.INVOICE_DATE,nvl(cust.credit_days, 0),CURRENCY_ID,CIT.CURRENCY_MST_FK,";
					strSQL = strSQL + " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)))";
					strSQL = strSQL + " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS)A WHERE (A.INVOICE_AMT - A.OUT_AMOUNT)> 0)";

					strSQL = strSQL + " UNION ";

					strSQL = strSQL + "(SELECT  A.JOBCARD_REF_NO,A.JOBCARD_DATE,A.INVOICE_REF_NO,A.INVOICE_DATE,A.DUEDATE,A.CURRENCY_ID,A.INVOICE,A.INVOICE_AMT-A.OUT_AMOUNT,";
					strSQL = strSQL + " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else A.OUTSTD_DAYS end OUTSTD_DAYS from  (";
					strSQL = strSQL + " select JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,sum(OUT_AMOUNT) OUT_AMOUNT, CASE WHEN OUTSTD_DAYS <= 0 THEN 0 ELSE OUTSTD_DAYS END OUTSTD_DAYS from (";

					strSQL = strSQL + " SELECT JOB.JOBCARD_REF_NO,";
					strSQL = strSQL + " TO_CHAR(TO_DATE(JOB.JOBCARD_DATE),'DD/MM/YYYY') JOBCARD_DATE,";
					strSQL = strSQL + " CIT.INVOICE_REF_NO,";
					strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE),'DD/MM/YYYY') INVOICE_DATE,";
					strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE) + nvl(cust.credit_days, 0),'DD/MM/YYYY') DUEDATE,";
					strSQL = strSQL + " CURR.CURRENCY_ID,";
					strSQL = strSQL + " CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ",CIT.INVOICE_DATE) INVOICE,";
					strSQL = strSQL + " ((CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) - ";
					strSQL = strSQL + " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)))INVOICE_AMT,";
					strSQL = strSQL + " (nvl(COLT.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(Col.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + ",Col.Collections_Date)) OUT_AMOUNT,";


					strSQL = strSQL + " ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - (cit.invoice_date + nvl(cust.credit_days,0)),0)) OUTSTD_DAYS";
					strSQL = strSQL + "  FROM";
					strSQL = strSQL + " consol_invoice_tbl CIT,";
					strSQL = strSQL + " consol_invoice_trn_tbl CITT,";

					strSQL = strSQL + " job_card_AIR_exp_tbl job,";
					strSQL = strSQL + " booking_AIR_tbl book,";

					strSQL = strSQL + " customer_mst_tbl cust,";
					strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CURR,";
					strSQL = strSQL + " collections_tbl        col,";
					strSQL = strSQL + " collections_trn_tbl    colt";
					strSQL = strSQL + " WHERE";
					strSQL = strSQL + " cit.consol_invoice_pk = citt.consol_invoice_fk";

					strSQL = strSQL + " AND citt.job_card_fk = job.job_card_AIR_exp_pk";
					strSQL = strSQL + " AND job.booking_AIR_fk = book.booking_AIR_pk";

					strSQL = strSQL + " AND book.cust_customer_mst_fk = cust.customer_mst_pk(+)";
					strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
					strSQL = strSQL + " AND book.cust_customer_mst_fk = " + CustPk;
					strSQL = strSQL + " AND cit.invoice_date <= SYSDATE";
					strSQL = strSQL + " AND COL.COLLECTIONS_TBL_PK = COLT.COLLECTIONS_TBL_FK";
					strSQL = strSQL + " AND COLT.INVOICE_REF_NR = CIT.INVOICE_REF_NO ";
					strSQL = strSQL + " GROUP BY";
					strSQL = strSQL + " JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,CIT.INVOICE_DATE,nvl(cust.credit_days, 0),CURRENCY_ID,CIT.CURRENCY_MST_FK,";
					strSQL = strSQL + " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)))";
					strSQL = strSQL + " WHERE INVOICE_AMT > 0";
					strSQL = strSQL + " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS)A))Q";
				}
				ds = objWF.GetDataSet(strSQL);
				return ds;
			} catch (Exception ex) {
				throw ex;
			} 
		}
		#endregion

		#region "FetchOutStand"
		public string SendMessage(long lngCreatedBy, long lngPkValue, string strDocId, string strCustomer, bool blnApp, string strSpecificURL, long lngLocFk, string strSRRRefNo, string strSrrDate, long SenderFk = 0,
		string SalesCallId = "")
		{
			string functionReturnValue = null;
			DataSet dsMsg = new DataSet();
			DataSet dsDoc = null;
			DataSet dsUser = new DataSet();
			DataSet dsSender = new DataSet();
			string strUsrName = null;
			string strLocName = null;
			string strMsgSub = null;
			string strMsgHeader = null;
			string strMsgFooter = null;
			string strMsgBody = null;
			DataRow DR = null;
			DataSet dsUserFks = null;
			long lngdocPk = 0;
			string strSenderLocName = null;
			string strSenderUsrName = null;
			string strPageURL = null;
			long lngUserFk = 0;
			int intSenderCnt = 0;
			cls_ApprovalRequests objSalesCallTRN = new cls_ApprovalRequests();

			dsDoc = objSalesCallTRN.FetchDocument(strDocId);
			if (dsDoc.Tables[0].Rows.Count > 0) {
				lngdocPk = Convert.ToInt64(dsDoc.Tables[0].Rows[0]["DOCUMENT_MST_PK"]);
				dsMsg = objSalesCallTRN.GetMessageInfo(lngdocPk, -1);
				if (dsMsg.Tables[0].Rows.Count < 1) {
					DR = dsMsg.Tables[0].NewRow();
					dsMsg.Tables[0].Rows.Add(DR);
				}

				dsUserFks = GetUserInfo(strSRRRefNo);
				if (dsUserFks.Tables[0].Rows.Count > 0) {
					for (intSenderCnt = 0; intSenderCnt <= dsUserFks.Tables[0].Columns.Count - 1; intSenderCnt++) {
						if (!(string.IsNullOrEmpty(dsUserFks.Tables[0].Rows[0][intSenderCnt].ToString()))) {
							if ((Convert.ToInt32(dsUserFks.Tables[0].Rows[0][intSenderCnt])) > 0) {
								dsUser = objSalesCallTRN.FetchUserDetails(Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]));
								strUsrName = Convert.ToString(dsUser.Tables[0].Rows[0]["USER_NAME"]);
								strLocName = Convert.ToString(dsUser.Tables[0].Rows[0]["LOCATION_NAME"]);

								if (dsMsg.Tables[0].Rows.Count > 0) {
									var _with16 = dsMsg.Tables[0].Rows[0];
									_with16["Sender_Fk"] = lngCreatedBy;
									dsSender = objSalesCallTRN.FetchUserDetails(lngCreatedBy);
									strSenderUsrName = Convert.ToString(dsSender.Tables[0].Rows[0]["USER_NAME"]);
									strSenderLocName = Convert.ToString(dsSender.Tables[0].Rows[0]["LOCATION_NAME"]);

									_with16["Receiver_Fk"] = Convert.ToInt64(dsUserFks.Tables[0].Rows[0][intSenderCnt]);
									_with16["Msg_Read"] = 0;
									_with16["Followup_Flag"] = 0;
									_with16["Have_Attachment"] = 1;

									strMsgBody = Convert.ToString(_with16["Msg_Body"]);
									strMsgBody = strMsgBody.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
									strMsgBody = strMsgBody.Replace("<RECEIVER>", strUsrName);
									strMsgBody = strMsgBody.Replace("<DateTime>", Convert.ToString(DateTime.Now));
									strMsgBody = strMsgBody.Replace("<number>", strSRRRefNo);
									strMsgBody = strMsgBody.Replace("<SRR DATE>", strSrrDate);
									strMsgBody = strMsgBody.Replace("<Customer>", strCustomer);
									_with16["Msg_Body"] = strMsgBody;

									strMsgSub = Convert.ToString(_with16["MSG_SUBJECT"]);
									strMsgSub = strMsgSub.Replace("<SENDER>", (strSenderUsrName + ", " + strSenderLocName));
									strMsgSub = strMsgSub.Replace("<RECEIVER>", strUsrName);
									strMsgSub = strMsgSub.Replace("<DateTime>", Convert.ToString(DateTime.Now));
									strMsgSub = strMsgSub.Replace("<number>", strSRRRefNo);
									strMsgSub = strMsgSub.Replace("<SRR DATE>", strSrrDate);
									strMsgSub = strMsgSub.Replace("<Customer>", strCustomer);
									_with16["Msg_Subject"] = strMsgSub;

									_with16["Read_Receipt"] = 0;
									_with16["Document_Mst_Fk"] = lngdocPk;
									_with16["User_Message_Folders_Fk"] = 1;
									_with16["Msg_Received_Dt"] = DateTime.Now;
								} else {
									return "Please define WorkFlow for the particular document";
								}
								if (dsMsg.Tables[1].Rows.Count < 1) {
									DR = dsMsg.Tables[1].NewRow();
									dsMsg.Tables[1].Rows.Add(DR);
								}
								if (dsMsg.Tables[1].Rows.Count > 0) {
									var _with17 = dsMsg.Tables[1].Rows[0];
									strPageURL = Convert.ToString(_with17["URL_PAGE"]);
									strPageURL = strPageURL.Replace("directory", strSpecificURL);
									strPageURL = strPageURL.Replace("pkvalue", Convert.ToString(lngPkValue));
									_with17["URL_PAGE"] = strPageURL;
									_with17["ATTACHMENT_DATA"] = strPageURL;
								}

								if ((objSalesCallTRN.SaveUnApprovedTDR(dsMsg, lngCreatedBy) == -1)) {
									return "<br>Due to some problem Mail has not been sent</br>";
									return functionReturnValue;
								}
							}
						}
					}
					return "<br>Mail has been sent</br>";
				} else {
					return "<br>Mail has not been sent.Document Id not created.</br>";
				}
			}
			return functionReturnValue;
		}

		public DataSet GetUserInfo(string SalesCallId)
		{
			WorkFlow objWF = new WorkFlow();
			StringBuilder strSqlBuilder = new StringBuilder();
			try {
				strSqlBuilder.Append(" SELECT UMT.USER_MST_PK,SAL.APPROVER_FK  FROM SALES_CALL_TRN SAL,USER_MST_TBL UMT WHERE UMT.EMPLOYEE_MST_FK = SAL.CREATED_BY_FK AND SAL.SALES_CALL_ID =  '" + SalesCallId + "'");
				return objWF.GetDataSet(strSqlBuilder.ToString());
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region " Fetch Comments "
		public DataSet FetchComments(long SalesCallPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			try {
				sb.Append("SELECT ROWNUM SLNR,");
				sb.Append("       SCC.SALES_CALL_COMMENTS_PK,");
				sb.Append("       SCC.COMMENTS_DATE,");
				sb.Append("       SCC.EXECUTIVE_MST_FK,");
				sb.Append("       EMP.EMPLOYEE_NAME,");
				sb.Append("       SCC.COMMENTS,");
				sb.Append("       '' DEL ");
				sb.Append("  FROM SALES_CALL_COMMENTS SCC, EMPLOYEE_MST_TBL EMP");
				sb.Append(" WHERE SCC.EXECUTIVE_MST_FK = EMP.EMPLOYEE_MST_PK");
				sb.Append("   AND SCC.SALES_CALL_PK = " + SalesCallPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region " Fetch History "

		public DataSet FetchHistory(long CustPK)
		{

			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);


			try {
				sb.Append("SELECT ROWNUM SLNR, SCT.SALES_CALL_PK,");
				sb.Append("       SCT.SALES_CALL_ID,");
				sb.Append("       TO_DATE(SCT.SALES_CALL_DT, 'dd/MM/yyyy') SALES_CALL_DT,");
				sb.Append("       SCT.FR_TIME,");
				sb.Append("       SCT.TO_TIME,");
				sb.Append("       EMP.EMPLOYEE_NAME,");
				sb.Append("       DMT.DESIGNATION_NAME,");
				sb.Append("       CASE");
				sb.Append("         WHEN SCT.CALL_RESULT = 1 THEN");
				sb.Append("          'Positive'");
				sb.Append("         ELSE");
				sb.Append("          'Negative'");
				sb.Append("       END CALL_RESULT,");
				sb.Append("       CASE");
				sb.Append("         WHEN SCT.CALL_TREND = 1 THEN");
				sb.Append("          'Positive'");
				sb.Append("         ELSE");
				sb.Append("          'Negative'");
				sb.Append("       END CALL_TREND,");
				sb.Append("       CASE");
				sb.Append("         WHEN SCT.CALL_STATUS = 1 THEN");
				sb.Append("          'Active'");
				sb.Append("         ELSE");
				sb.Append("          'Acquired'");
				sb.Append("       END CALL_STATUS,");
				sb.Append("       SRMT.SAL_CAL_REASON");
				sb.Append("  FROM SALES_CALL_TRN         SCT,");
				sb.Append("       EMPLOYEE_MST_TBL       EMP,");
				sb.Append("       DESIGNATION_MST_TBL    DMT,");
				sb.Append("       SAL_CAL_REASON_MST_TBL SRMT");
				sb.Append(" WHERE EMP.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK");
				sb.Append("   AND EMP.DESIGNATION_MST_FK = DMT.DESIGNATION_MST_PK");
				sb.Append("   AND SCT.SALES_CALL_REASON_FK = SRMT.SAL_CAL_REASON_MST_TBL_PK");
				sb.Append("   AND SCT.CUSTOMER_MST_FK = " + CustPK);
				sb.Append(" ORDER BY SCT.SALES_CALL_DT DESC");

				return objWF.GetDataSet(sb.ToString());

			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}

		#endregion

		#region " Fetch Header "

		public DataSet FetchHeader(long SalesCallPK = -1)
		{

			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);


			try {
				sb.Append("SELECT SCT.SALES_CALL_PK,");
				sb.Append("       SCT.SALES_CALL_ID,");
				sb.Append("       SCT.SALES_CALL_DT,");
				sb.Append("       SCT.CUSTOMER_MST_FK,");
				sb.Append("       CMT.CUSTOMER_ID,");
				sb.Append("       CMT.CUSTOMER_NAME,");
				sb.Append("       EMT.EMPLOYEE_MST_PK,");
				sb.Append("       EMT.EMPLOYEE_ID,");
				sb.Append("       EMT.EMPLOYEE_NAME,");
				sb.Append("       DMT.DESIGNATION_MST_PK,");
				sb.Append("       DMT.DESIGNATION_ID,");
				sb.Append("       DMT.DESIGNATION_NAME,");
				sb.Append("       SCT.FR_TIME,");
				sb.Append("       SCT.TO_TIME,");
				sb.Append("       SCT.BIZTYPE,");
				sb.Append("       SCT.SALES_CALL_TYPE_FK,");
				sb.Append("       SCT.CALL_RESULT,");
				sb.Append("       SCT.CALL_TREND,");
				sb.Append("       SCT.CALL_STATUS,");
				sb.Append("       SRMT.SAL_CAL_REASON_MST_TBL_PK,");
				sb.Append("       SRMT.SAL_CAL_ID,");
				sb.Append("       SRMT.SAL_CAL_REASON,");
				sb.Append("       LMT.LOCATION_MST_PK, ");
				sb.Append("       LMT.LOCATION_NAME, ");
				sb.Append("       SCT.PERSON_TO_MEET_PK, ");
				sb.Append("       CCT.NAME PERSON_TO_MEET_NAME, ");
				sb.Append("       CCT.DESIGNATION PERSON_TO_MEET_DESIG ");
				sb.Append("  FROM SALES_CALL_TRN         SCT,");
				sb.Append("       CUSTOMER_MST_TBL       CMT,");
				sb.Append("       EMPLOYEE_MST_TBL       EMT,");
				sb.Append("       DESIGNATION_MST_TBL    DMT,");
				sb.Append("       SAL_CAL_REASON_MST_TBL SRMT,");
				sb.Append("       LOCATION_MST_TBL       LMT, CUSTOMER_CONTACT_TRN CCT ");
				sb.Append(" WHERE CMT.CUSTOMER_MST_PK = SCT.CUSTOMER_MST_FK");
				sb.Append("   AND EMT.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK");
				sb.Append("   AND DMT.DESIGNATION_MST_PK = EMT.DESIGNATION_MST_FK");
				sb.Append("   AND CCT.CUST_CONTACT_PK = SCT.PERSON_TO_MEET_PK");
				sb.Append("   AND SRMT.SAL_CAL_REASON_MST_TBL_PK(+) = SCT.SALES_CALL_REASON_FK");
				sb.Append("   AND LMT.LOCATION_MST_PK = EMT.LOCATION_MST_FK");
				sb.Append("   AND SCT.SALES_CALL_PK = " + SalesCallPK);

				return objWF.GetDataSet(sb.ToString());

			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}

		#endregion

		#region " Fetch Executive Parameters "

		public DataSet FetchExecutivePrameter(long ExecutivePK)
		{

			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);


			try {
				sb.Append("SELECT LMT.LOCATION_MST_PK,");
				sb.Append("       LMT.LOCATION_NAME,");
				sb.Append("       DMT.DESIGNATION_MST_PK,");
				sb.Append("       DMT.DESIGNATION_NAME");
				sb.Append("  FROM EMPLOYEE_MST_TBL EMT, LOCATION_MST_TBL LMT, DESIGNATION_MST_TBL DMT");
				sb.Append(" WHERE LMT.LOCATION_MST_PK = EMT.LOCATION_MST_FK");
				sb.Append("   AND DMT.DESIGNATION_MST_PK = EMT.DESIGNATION_MST_FK");
				sb.Append("   AND EMT.EMPLOYEE_MST_PK = " + ExecutivePK);

				return objWF.GetDataSet(sb.ToString());

			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}

		}

		#endregion

		#region " Fetch Follow Up "

		public DataSet FetchFollowUp(long SalesCallPK)
		{

			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);


			try {
				sb.Append("SELECT SCT.SALES_CALL_ID,");
				sb.Append("       SCT.SALES_CALL_DT,");
				sb.Append("       EXC.EMPLOYEE_MST_PK EXECUTIVE_FK,");
				sb.Append("       EXC.EMPLOYEE_ID EXECUTIVE_ID,");
				sb.Append("       EXC.EMPLOYEE_NAME EXECUTIVE_NAME,");
				sb.Append("       SCF.FOLLOWUP_DATE,");
				sb.Append("       SCF.FOLLOWUP_TIME,");
				sb.Append("       FLU.EMPLOYEE_MST_PK FOLLOWUP_BY_FK,");
				sb.Append("       FLU.EMPLOYEE_ID FOLLOWUP_BY_ID,");
				sb.Append("       FLU.EMPLOYEE_NAME FOLLOWUP_BY_NAME");
				sb.Append("  FROM SALES_CALL_FOLLOWUP SCF,");
				sb.Append("       SALES_CALL_TRN      SCT,");
				sb.Append("       EMPLOYEE_MST_TBL    EXC,");
				sb.Append("       EMPLOYEE_MST_TBL    FLU");
				sb.Append(" WHERE SCF.SALES_CALL_FK(+) = SCT.SALES_CALL_PK");
				sb.Append("   AND EXC.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK");
				sb.Append("   AND FLU.EMPLOYEE_MST_PK(+) = SCF.FOLLOWUP_BY");
				sb.Append("   AND SCT.SALES_CALL_PK = " + SalesCallPK);

				return objWF.GetDataSet(sb.ToString());

			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}

		}

		#endregion

		#region " Fetch Escalate To "
		public DataSet FetchEscalateTo(long SalesCallPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			try {
				sb.Append("SELECT SCT.SALES_CALL_ID,");
				sb.Append("       SCT.SALES_CALL_DT,");
				sb.Append("       EXC.EMPLOYEE_MST_PK,");
				sb.Append("       EXC.EMPLOYEE_ID,");
				sb.Append("       EXC.EMPLOYEE_NAME,");
				sb.Append("       SCT.ESCALATE_TO_REASON");
				sb.Append("  FROM SALES_CALL_TRN SCT, EMPLOYEE_MST_TBL EXC");
				sb.Append(" WHERE EXC.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK");
				sb.Append("   AND SCT.SALES_CALL_PK = " + SalesCallPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region " Fetch Delegate To "
		public DataSet FetchDelegateTo(long SalesCallPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			try {
				sb.Append("SELECT SCT.SALES_CALL_ID,");
				sb.Append("       SCT.SALES_CALL_DT,");
				sb.Append("       EXC.EMPLOYEE_MST_PK,");
				sb.Append("       EXC.EMPLOYEE_ID,");
				sb.Append("       EXC.EMPLOYEE_NAME,");
				sb.Append("       SCT.DELEGATE_TO_REASON");
				sb.Append("  FROM SALES_CALL_TRN SCT, EMPLOYEE_MST_TBL EXC");
				sb.Append(" WHERE EXC.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK");
				sb.Append("   AND SCT.SALES_CALL_PK = " + SalesCallPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region " Fetch ReVisit "
		public DataSet FetchReVisit(long SalesCallPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			try {
				sb.Append("SELECT SPRT.FREQUENCY, SPRT.DAYS, SPRT.STARTDATE, SPRT.ENDDATE ");
				sb.Append("  FROM SALES_PLAN_RECURRING_TRN SPRT");
				sb.Append(" WHERE SPRT.SALES_CALL_FK = " + SalesCallPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region " Fetch Nomination "
		public DataSet FetchNomination(long SalesCallPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			DataSet ds = null;
			try {
				sb.Append("SELECT SCN.NOMINATION_PK,");
				sb.Append("       SCN.SALES_CALL_FK,");
				sb.Append("       SCT.SALES_CALL_ID,");
				sb.Append("       SCT.SALES_CALL_DT,");
				sb.Append("       EXE.EMPLOYEE_MST_PK        EXECUTIVE_FK,");
				sb.Append("       EXE.EMPLOYEE_ID            EXECUTIVE_ID,");
				sb.Append("       EXE.EMPLOYEE_NAME          EXECUTIVE_NAME,");
				sb.Append("       POL_E.EMPLOYEE_MST_PK      POL_EXECUTIVE_FK,");
				sb.Append("       POL_E.EMPLOYEE_ID          POL_EXECUTIVE_ID,");
				sb.Append("       POL_E.EMPLOYEE_NAME        POL_EXECUTIVE_NAME,");
				sb.Append("       POL_A.AGENT_MST_PK         POL_AGENT_FK,");
				sb.Append("       POL_A.AGENT_ID             POL_AGENT_ID,");
				sb.Append("       POL_A.AGENT_NAME           POL_AGENT_NAME,");
				sb.Append("       POD_A.AGENT_MST_PK         POD_AGENT_FK,");
				sb.Append("       POD_A.AGENT_ID             POD_AGENT_ID,");
				sb.Append("       POD_A.AGENT_NAME           POD_AGENT_NAME,");
				sb.Append("       SHIP.CUSTOMER_MST_PK       SHIPPER_FK,");
				sb.Append("       SHIP.CUSTOMER_ID           SHIPPER_ID,");
				sb.Append("       SHIP.CUSTOMER_NAME         SHIPPER_NAME,");
				sb.Append("       CONS.CUSTOMER_MST_PK       CONSIGNEE_FK,");
				sb.Append("       CONS.CUSTOMER_ID           CONSIGNEE_ID,");
				sb.Append("       CONS.CUSTOMER_NAME         CONSIGNEE_NAME,");
				sb.Append("       POL.PORT_MST_PK            POL_FK,");
				sb.Append("       POL.PORT_ID                POL_ID,");
				sb.Append("       POL.PORT_NAME              POL_NAME,");
				sb.Append("       POD.PORT_MST_PK            POD_FK,");
				sb.Append("       POD.PORT_ID                POD_ID,");
				sb.Append("       POD.PORT_NAME              POD_NAME,");
				sb.Append("       STMT.SHIPPING_TERMS_MST_PK,");
				sb.Append("       STMT.INCO_CODE,");
				sb.Append("       STMT.INCO_CODE_DESCRIPTION,");
				sb.Append("       SCN.CARGO_TYPE,");
				sb.Append("       CGMT.COMMODITY_GROUP_PK,");
				sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
				sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
				sb.Append("       CMT.COMMODITY_MST_PK,");
				sb.Append("       CMT.COMMODITY_ID,");
				sb.Append("       CMT.COMMODITY_NAME,");
				sb.Append("       PMT.PACK_TYPE_MST_PK,");
				sb.Append("       PMT.PACK_TYPE_ID,");
				sb.Append("       PMT.PACK_TYPE_DESC,");
				sb.Append("       SCN.QUANTITY,");
				sb.Append("       SCN.WEIGHT,");
				sb.Append("       SCN.VOLUME,");
				sb.Append("       SCN.REMARKS, POL_A.LOCATION_MST_FK POL_AGT_LOC_FK, POD_A.LOCATION_MST_FK POD_AGT_LOC_FK ");
				sb.Append("  FROM SALES_CALL_NOMINATION   SCN,");
				sb.Append("       SALES_CALL_TRN          SCT,");
				sb.Append("       EMPLOYEE_MST_TBL        EXE,");
				sb.Append("       EMPLOYEE_MST_TBL        POL_E,");
				sb.Append("       AGENT_MST_TBL           POL_A,");
				sb.Append("       AGENT_MST_TBL           POD_A,");
				sb.Append("       CUSTOMER_MST_TBL        SHIP,");
				sb.Append("       CUSTOMER_MST_TBL        CONS,");
				sb.Append("       PORT_MST_TBL            POL,");
				sb.Append("       PORT_MST_TBL            POD,");
				sb.Append("       SHIPPING_TERMS_MST_TBL  STMT,");
				sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
				sb.Append("       COMMODITY_MST_TBL       CMT,");
				sb.Append("       PACK_TYPE_MST_TBL       PMT");
				sb.Append(" WHERE SCT.SALES_CALL_PK = SCN.SALES_CALL_FK");
				sb.Append("   AND EXE.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK");
				sb.Append("   AND POL_E.EMPLOYEE_MST_PK = SCN.POL_EXECUTIVE_FK");
				sb.Append("   AND POL_A.AGENT_MST_PK = SCN.POL_AGENT_FK");
				sb.Append("   AND POD_A.AGENT_MST_PK = SCN.POD_AGENT_FK");
				sb.Append("   AND SHIP.CUSTOMER_MST_PK = SCN.SHIPPER_FK");
				sb.Append("   AND CONS.CUSTOMER_MST_PK = SCN.CONSIGNEE_FK");
				sb.Append("   AND POL.PORT_MST_PK = SCN.POL_FK");
				sb.Append("   AND POD.PORT_MST_PK = SCN.POD_FK");
				sb.Append("   AND STMT.SHIPPING_TERMS_MST_PK = SCN.SHIP_TERM_FK");
				sb.Append("   AND CGMT.COMMODITY_GROUP_PK = SCN.COMMODITY_GRP_FK");
				sb.Append("   AND CMT.COMMODITY_MST_PK = SCN.COMMODITY_FK");
				sb.Append("   AND PMT.PACK_TYPE_MST_PK = SCN.PACK_TYPE_FK");
				sb.Append("   AND SCT.SALES_CALL_PK = " + SalesCallPK);

				ds = objWF.GetDataSet(sb.ToString());

				if (ds.Tables[0].Rows.Count > 0) {
					return ds;
				}

				sb.Replace(sb.ToString(), "");
				sb.Append("SELECT NULL                NOMINATION_PK,");
				sb.Append("       SCT.SALES_CALL_PK   SALES_CALL_FK,");
				sb.Append("       SCT.SALES_CALL_ID,");
				sb.Append("       SCT.SALES_CALL_DT,");
				sb.Append("       EXE.EMPLOYEE_MST_PK EXECUTIVE_FK,");
				sb.Append("       EXE.EMPLOYEE_ID     EXECUTIVE_ID,");
				sb.Append("       EXE.EMPLOYEE_NAME   EXECUTIVE_NAME,");
				sb.Append("       NULL                POL_EXECUTIVE_FK,");
				sb.Append("       NULL                POL_EXECUTIVE_ID,");
				sb.Append("       NULL                POL_EXECUTIVE_NAME,");
				sb.Append("       NULL                POL_AGENT_FK,");
				sb.Append("       NULL                POL_AGENT_ID,");
				sb.Append("       NULL                POL_AGENT_NAME,");
				sb.Append("       NULL                POD_AGENT_FK,");
				sb.Append("       NULL                POD_AGENT_ID,");
				sb.Append("       NULL                POD_AGENT_NAME,");
				sb.Append("       NULL                SHIPPER_FK,");
				sb.Append("       NULL                SHIPPER_ID,");
				sb.Append("       NULL                SHIPPER_NAME,");
				sb.Append("       NULL                CONSIGNEE_FK,");
				sb.Append("       NULL                CONSIGNEE_ID,");
				sb.Append("       NULL                CONSIGNEE_NAME,");
				sb.Append("       NULL                POL_FK,");
				sb.Append("       NULL                POL_ID,");
				sb.Append("       NULL                POL_NAME,");
				sb.Append("       NULL                POD_FK,");
				sb.Append("       NULL                POD_ID,");
				sb.Append("       NULL                POD_NAME,");
				sb.Append("       NULL                SHIPPING_TERMS_MST_PK,");
				sb.Append("       NULL                INCO_CODE,");
				sb.Append("       NULL                INCO_CODE_DESCRIPTION,");
				sb.Append("       NULL                CARGO_TYPE,");
				sb.Append("       NULL                COMMODITY_GROUP_PK,");
				sb.Append("       NULL                COMMODITY_GROUP_CODE,");
				sb.Append("       NULL                COMMODITY_GROUP_DESC,");
				sb.Append("       NULL                COMMODITY_MST_PK,");
				sb.Append("       NULL                COMMODITY_ID,");
				sb.Append("       NULL                COMMODITY_NAME,");
				sb.Append("       NULL                PACK_TYPE_MST_PK,");
				sb.Append("       NULL                PACK_TYPE_ID,");
				sb.Append("       NULL                PACK_TYPE_DESC,");
				sb.Append("       NULL                QUANTITY,");
				sb.Append("       NULL                WEIGHT,");
				sb.Append("       NULL                VOLUME,");
				sb.Append("       NULL                REMARKS, NULL POL_AGT_LOC_FK, NULL POD_AGT_LOC_FK");
				sb.Append("  FROM SALES_CALL_TRN SCT, EMPLOYEE_MST_TBL EXE");
				sb.Append(" WHERE EXE.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK");
				sb.Append("   AND SCT.SALES_CALL_PK = " + SalesCallPK);

				return objWF.GetDataSet(sb.ToString());

			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region " Save Nomination "
		public ArrayList SaveNomination(DataSet M_DataSet, long NominationPK)
		{

			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;
			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();

			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();


			try {
				var _with18 = insCommand;
				_with18.Connection = objWK.MyConnection;
				_with18.CommandType = CommandType.StoredProcedure;
				_with18.CommandText = objWK.MyUserName + ".SALES_CALL_NOMINATION_PKG.SALES_CALL_NOMINATION_INS";

				_with18.Parameters.Add("SALES_CALL_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SALES_CALL_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SALES_CALL_FK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("POL_EXECUTIVE_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["POL_EXECUTIVE_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["POL_EXECUTIVE_FK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("POL_AGENT_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["POL_AGENT_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["POL_AGENT_FK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("POD_AGENT_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["POD_AGENT_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["POD_AGENT_FK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("SHIPPER_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SHIPPER_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SHIPPER_FK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("CONSIGNEE_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CONSIGNEE_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CONSIGNEE_FK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("POL_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["POL_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["POL_FK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("POD_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["POD_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["POD_FK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("SHIP_TERM_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SHIPPING_TERMS_MST_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SHIPPING_TERMS_MST_PK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("CARGO_TYPE_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CARGO_TYPE"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("COMMODITY_GRP_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["COMMODITY_GROUP_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["COMMODITY_GROUP_PK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("COMMODITY_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["COMMODITY_MST_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["COMMODITY_MST_PK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("PACK_TYPE_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["PACK_TYPE_MST_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["PACK_TYPE_MST_PK"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("QUANTITY_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["QUANTITY"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["QUANTITY"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["WEIGHT"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["WEIGHT"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("VOLUME_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["VOLUME"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["VOLUME"])).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("REMARKS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["REMARKS"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["REMARKS"])).Direction = ParameterDirection.Input;

				_with18.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				_with18.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

				var _with19 = updCommand;
				_with19.Connection = objWK.MyConnection;
				_with19.CommandType = CommandType.StoredProcedure;
				_with19.CommandText = objWK.MyUserName + ".SALES_CALL_NOMINATION_PKG.SALES_CALL_NOMINATION_UPD";

				_with19.Parameters.Add("NOMINATION_PK_IN", NominationPK).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("SALES_CALL_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SALES_CALL_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SALES_CALL_FK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("POL_EXECUTIVE_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["POL_EXECUTIVE_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["POL_EXECUTIVE_FK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("POL_AGENT_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["POL_AGENT_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["POL_AGENT_FK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("POD_AGENT_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["POD_AGENT_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["POD_AGENT_FK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("SHIPPER_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SHIPPER_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SHIPPER_FK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("CONSIGNEE_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CONSIGNEE_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CONSIGNEE_FK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("POL_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["POL_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["POL_FK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("POD_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["POD_FK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["POD_FK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("SHIP_TERM_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SHIPPING_TERMS_MST_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["SHIPPING_TERMS_MST_PK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("CARGO_TYPE_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CARGO_TYPE"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("COMMODITY_GRP_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["COMMODITY_GROUP_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["COMMODITY_GROUP_PK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("COMMODITY_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["COMMODITY_MST_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["COMMODITY_MST_PK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("PACK_TYPE_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["PACK_TYPE_MST_PK"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["PACK_TYPE_MST_PK"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("QUANTITY_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["QUANTITY"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["QUANTITY"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["WEIGHT"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["WEIGHT"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("VOLUME_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["VOLUME"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["VOLUME"])).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("REMARKS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["REMARKS"].ToString()) ? "" : M_DataSet.Tables[0].Rows[0]["REMARKS"])).Direction = ParameterDirection.Input;

				_with19.Parameters.Add("LAST_MODIFIED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
				_with19.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;


				var _with20 = objWK.MyDataAdapter;

				if (NominationPK <= 0) {
					_with20.InsertCommand = insCommand;
					_with20.InsertCommand.Transaction = TRAN;

					RecAfct = _with20.InsertCommand.ExecuteNonQuery();
					NominationPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
				} else {
					_with20.UpdateCommand = updCommand;
					_with20.UpdateCommand.Transaction = TRAN;
					RecAfct = _with20.UpdateCommand.ExecuteNonQuery();
					NominationPK = (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["NOMINATION_PK"].ToString()) ?0 : Convert.ToInt64( M_DataSet.Tables[0].Rows[0]["NOMINATION_PK"]));
				}

				if (RecAfct > 0) {
					if (arrMessage.Count == 0) {
						TRAN.Commit();
						arrMessage.Add("All Data Saved Successfully");
						arrMessage.Add(NominationPK);
						return arrMessage;
					} else {
						TRAN.Rollback();
						return arrMessage;
					}
				} else {
					TRAN.Rollback();
					arrMessage.Add("Error");
					return arrMessage;
				}

			} catch (OracleException oraEx) {
				TRAN.Rollback();
				arrMessage.Add(oraEx.Message);
				return arrMessage;
			} catch (Exception ex) {
				throw ex;
			} finally {
				if (objWK.MyConnection.State == ConnectionState.Open) {
					objWK.MyConnection.Close();
				}
			}

		}
		#endregion

		#region " Fetch Potential "
		public DataSet FetchPotential(long SalesCallPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region " Delete Sales Call "
		public void DeleteSalesCall(string SalesCallPK)
		{
			WorkFlow objWF = new WorkFlow();
			try {
				objWF.ExecuteCommands("DELETE FROM SALES_CALL_COMMENTS SCT WHERE SCT.SALES_CALL_PK = " + SalesCallPK);
				objWF.ExecuteCommands("DELETE FROM SALES_CALL_FOLLOWUP SCT WHERE SCT.SALES_CALL_FK = " + SalesCallPK);
				objWF.ExecuteCommands("DELETE FROM SALES_CALL_TRN SCT WHERE SCT.SALES_CALL_PK = " + SalesCallPK);
				objWF.ExecuteCommands("DELETE FROM SALES_LEAD_CONT_TRN  SCT WHERE SCT.SALES_LEAD_FK = " + SalesCallPK);

				objWF.ExecuteCommands("DELETE FROM SALES_PLAN_RECURRING_TRN SCT WHERE SCT.SALES_CALL_FK = " + SalesCallPK);
				objWF.ExecuteCommands("DELETE FROM SALES_LEAD_HDR  SCT WHERE SCT.SALES_CALL_FK = " + SalesCallPK);
				objWF.ExecuteCommands("DELETE FROM SALES_LEAD_SRC_TRN  SCT WHERE SCT.SALES_LEAD_HDR_FK = (SELECT SC.SALES_LEAD_HDR_PK FROM SALES_LEAD_HDR SC WHERE SC.SALES_CALL_FK = " + SalesCallPK + ")");
				objWF.ExecuteCommands("DELETE FROM SALES_CALL_NOMINATION SCT WHERE SCT.SALES_CALL_FK = " + SalesCallPK);
				objWF.ExecuteCommands("DELETE FROM SALES_CALL_TRN SCT WHERE SCT.SALES_CALL_PK = " + SalesCallPK);
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region " Update Upstream Sales Call "
		public void UpdateSalesCall(string SalesCallPK)
		{
			WorkFlow objWF = new WorkFlow();
			try {
				objWF.ExecuteCommands("UPDATE SALES_CALL_TRN SCT SET SCT.PLAN_STATUS = 5 WHERE SCT.SALES_CALL_PK = " + SalesCallPK);

			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region "Function to check whether a user is an administrator or not"
		public int IsAdministrator()
		{
			string strSQL = null;
			Int16 Admin = default(Int16);
			WorkFlow objWF = new WorkFlow();
			strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
			strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
			strSQL = strSQL + "AND U.USER_MST_PK = " + HttpContext.Current.Session["USER_PK"];
			try {
				Admin = Convert.ToInt16(objWF.ExecuteScaler(strSQL.ToString()));
				if (Admin == 1) {
					return 1;
				} else {
					return 0;
				}
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region " Display Executive "
		public DataSet DisplayExecutive(long ExePK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("SELECT EMT.EMPLOYEE_MST_PK,");
			sb.Append("       EMT.EMPLOYEE_ID,");
			sb.Append("       EMT.EMPLOYEE_NAME,");
			sb.Append("       LMT.LOCATION_MST_PK,");
			sb.Append("       LMT.LOCATION_NAME,");
			sb.Append("       DMT.DESIGNATION_MST_PK,");
			sb.Append("       DMT.DESIGNATION_NAME");
			sb.Append("  FROM EMPLOYEE_MST_TBL EMT, LOCATION_MST_TBL LMT, DESIGNATION_MST_TBL DMT");
			sb.Append(" WHERE LMT.LOCATION_MST_PK = EMT.LOCATION_MST_FK");
			sb.Append("   AND DMT.DESIGNATION_MST_PK = EMT.DESIGNATION_MST_FK");
			sb.Append("   AND EMT.EMPLOYEE_MST_PK = " + ExePK);
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

	}
}
