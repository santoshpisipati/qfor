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
using System.Web;

namespace Quantum_QFOR
{
    public class cls_user_task : CommonFeatures
	{

		#region "Fetch"
		public DataSet Fetch(Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 2)
		{

			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strCondition = null;
			string strSQL1 = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			var User = HttpContext.Current.Session["user_pk"];
			try {
				sb.AppendLine("SELECT ROWNUM SlNo, A.*");
				sb.AppendLine("  FROM (SELECT UST.WF_MGR_USER_TASK_ACTIVITY,");
				sb.AppendLine("               decode(UST.WF_MGR_USER_TASK_ACTIVITY_TYPE,");
				sb.AppendLine("                      1,");
				sb.AppendLine("                      'Internal',");
				sb.AppendLine("                      2,");
				sb.AppendLine("                      'Manual') ActitvityType,");
				sb.AppendLine("               UST.WF_MGR_USER_TASK_REF_NR,");
				sb.AppendLine("               WFLIST.WF_MGR_ADM_TASK_START_DT START_DT,");
				//sb.AppendLine("               TO_CHAR(WFLIST.WF_MGR_ADM_TASK_START_DT +")
				//sb.AppendLine("                       WC.WF_RULES_INT_DEADLINE +")
				//sb.AppendLine("                       numtodsinterval(WC.WF_RULES_INT_DEADLINE_HOURS,")
				//sb.AppendLine("                                       'hour') +")
				//sb.AppendLine("                       numtodsinterval(WC.WF_RULES_INT_DEADLINE_MINS,")
				//sb.AppendLine("                                       'minute'),")
				//sb.AppendLine("                       DATETIMEFORMAT24) DEADLINE,")

				//sb.AppendLine("   TO_CHAR(FN_EX_WEEKENDDATE(WC.WF_RULES_INT_CONFIG_PK,(WFLIST.WF_MGR_ADM_TASK_START_DT + ")
				//sb.AppendLine("           WC.WF_RULES_INT_DEADLINE + ")
				//sb.AppendLine("          NUMTODSINTERVAL(WC.WF_RULES_INT_DEADLINE_HOURS,'HOUR') + ")
				//sb.AppendLine("         NUMTODSINTERVAL(WC.WF_RULES_INT_DEADLINE_MINS,'MINUTE'))),DATETIMEFORMAT24 ) DEADLINE, ")

				sb.AppendLine("   FN_EX_WEEKENDDATE(WC.WF_RULES_INT_CONFIG_PK,(WFLIST.WF_MGR_ADM_TASK_START_DT + ");
				sb.AppendLine("           WC.WF_RULES_INT_DEADLINE + ");
				sb.AppendLine("          NUMTODSINTERVAL(WC.WF_RULES_INT_DEADLINE_HOURS,'HOUR') + ");
				sb.AppendLine("         NUMTODSINTERVAL(WC.WF_RULES_INT_DEADLINE_MINS,'MINUTE'))) DEADLINE, ");

				sb.AppendLine("               decode(UST.WF_MGR_USER_TASK_DLMODE,");
				sb.AppendLine("                      1,");
				sb.AppendLine("                      'Mins',");
				sb.AppendLine("                      2,");
				sb.AppendLine("                      'Hours',");
				sb.AppendLine("                      3,");
				sb.AppendLine("                      'Days') Dlmode,");
				sb.AppendLine("               decode(UST.WF_MGR_USER_TASK_PRIORITY,");
				sb.AppendLine("                      1,");
				sb.AppendLine("                      'Low',");
				sb.AppendLine("                      2,");
				sb.AppendLine("                      'High',");
				sb.AppendLine("                      3,");
				sb.AppendLine("                      'Critical') Priority,");
				sb.AppendLine("               decode(UST.WF_MGR_USER_TASK_STATUS,");
				sb.AppendLine("                      1,");
				sb.AppendLine("                      'Nottaken',");
				sb.AppendLine("                      2,");
				sb.AppendLine("                      'Taken',");
				sb.AppendLine("                      3,");
				sb.AppendLine("                      'Escalated',");
				sb.AppendLine("                      4,");
				sb.AppendLine("                      'Completed') Status,");
				sb.AppendLine("               '' Sel,");
				sb.AppendLine("               UST.WF_MGR_USER_TASK_REF_FK,");
				sb.AppendLine("               wf_mgr_user_task_pk");
				sb.AppendLine("          FROM WF_MGR_ADM_TASK_LIST_TBL      WFLIST,");
				sb.AppendLine("               WF_mgr_user_task_list_tbl     UST,");
				sb.AppendLine("               WORKFLOW_MGR_TASK_MSG_TBL     TASK,");
				sb.AppendLine("               WORKFLOW_RULES_INT_MST_TBL    WF,");
				sb.AppendLine("               WORKFLOW_RULES_INT_APPL_TBL   APP,");
				sb.AppendLine("               WORKFLOW_RULES_INT_CONFIG_TBL WC,");
				sb.AppendLine("               WF_ACTIVITY_MST_TBL           ACT,");
				sb.AppendLine("               WF_ACTIVITY_MST_TBL           NEXTACT,");
				sb.AppendLine("               USER_MST_TBL                  U");
				sb.AppendLine("         WHERE WF.WF_RULES_INT_ACTIVITY = ACT.WF_ACTIVITY_MST_TBL_PK");
				sb.AppendLine("           AND WF.WF_RULES_INT_NEXT_ACTIVITY =");
				sb.AppendLine("               NEXTACT.WF_ACTIVITY_MST_TBL_PK");
				sb.AppendLine("           AND WF.WF_RULES_INT_MST_TBL_PK = APP.WORKFLOW_RULES_INT_MST_FK");
				sb.AppendLine("           AND WF.WF_RULES_INT_MST_TBL_PK = WC.WF_RULES_INT_MST_FK");
				sb.AppendLine("           AND APP.WORKFLOW_RULES_INT_CONFIG_FK = WC.WF_RULES_INT_CONFIG_PK");
				sb.AppendLine("           AND UPPER(WFLIST.WF_MGR_ADM_TASK_REF_NR) =");
				sb.AppendLine("               UPPER(UST.WF_MGR_USER_TASK_REF_NR)");
				sb.AppendLine("           AND UPPER(WFLIST.WF_MGR_ADM_TASK_ACTIVITY) =");
				sb.AppendLine("               UPPER(ACT.WF_ACTIVITY_NAME)");
				sb.AppendLine("           AND WFLIST.WF_MGR_ADM_TASK_PK = TASK.ADM_TASK_FK");
				sb.AppendLine("           AND WFLIST.WF_MGR_ADM_TASK_REF_FK = TASK.DOC_REF_NO_PK");
				sb.AppendLine("           AND UPPER(TASK.NEXT_ACTIVITY) = UPPER(NEXTACT.WF_ACTIVITY_NAME)");
				sb.AppendLine("           AND U.USER_MST_PK = UST.wf_mgr_user_task_user_fk");
				sb.AppendLine("           AND U.DEFAULT_LOCATION_FK =");
				sb.AppendLine("               TO_NUMBER(APP.WORKFLOW_RULES_INT_LOC_FK)");
				sb.AppendLine("           AND UST.wf_mgr_user_task_user_fk = " + User);
				sb.AppendLine("         order by UST.WF_MGR_USER_TASK_START_DT DESC,");
				sb.AppendLine("                  UST.WF_MGR_USER_TASK_REF_NR   DESC) A");

				//sb.AppendLine("and UST.WF_MGR_USER_TASK_REF_FK=WMA.WF_MGR_ADM_TASK_REF_FK"
				strSQL1 = "SELECT Count(*) from (";
				strSQL1 += sb.ToString() + ")";
				TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
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
				strSQL1 = " select * from (";
				strSQL1 += sb.ToString();
				strSQL1 += " ) WHERE SlNo Between " + start + " and " + last;
				return objWF.GetDataSet(strSQL1.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
				//Catch ex As Exception

			}
		}

		#endregion
		#region "Class Members"
		private string WF_MGR_USER_TASK_REF_NR;
		private string WF_MGR_USER_TASK_STATUS;
		private string WF_MGR_USER_TASK_ACTIVITY;
		private string WF_MGR_USER_TASK_A_TYPE;
		private string WF_MGR_USER_TASK_START_DT;
		private string WF_MGR_USER_TASK_DEADLINE;
		private string WF_MGR_USER_TASK_DLMODE;
			#endregion
		private string WF_MGR_USER_TASK_PRIORITY;
		#region "properties"
		//Public Property WF_MGR_USER_TASK_REF_NR() As String
		//    Get
		//        Return WF_MGR_USER_TASK_REF_NR
		//    End Get
		//    Set(ByVal Value As String)
		//        WF_MGR_USER_TASK_REF_NR = Value
		//    End Set
		//End Property
		//Public Property WF_MGR_USER_TASK_STATUS() As String
		//    Get
		//        Return WF_MGR_USER_TASK_STATUS
		//    End Get
		//    Set(ByVal Value As String)
		//        WF_MGR_USER_TASK_STATUS = Value
		//    End Set
		//End Property
		#endregion
		#region "Insert"

		public ArrayList Insert(DataSet M_DataSet)
		{
			WorkFlow objWK = new WorkFlow();
			System.Web.UI.Page objPage = new System.Web.UI.Page();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			Int32 intLoc = default(Int32);
			intLoc = (Int32)HttpContext.Current.Session["LOGED_IN_LOC_FK"];
			//objWK.MyCommand.Transaction = TRAN
			int intPKVal = 0;
			Int32 i = default(Int32);
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			// Dim updCommand As New OracleClient.OracleCommand
			OracleCommand update = new OracleCommand();
			OracleCommand up = new OracleCommand();
			OracleCommand delete = new OracleCommand();
			arrMessage.Clear();
			Int32 nRowCnt = default(Int32);
			string sql = null;
			WorkFlow objWS = new WorkFlow();

			try {
				//With insCommand
				//    .Connection = objWK.MyConnection
				//    .CommandType = CommandType.StoredProcedure
				//    .CommandText = objWK.MyUserName & ".WORKFLOW_RULES_ENTRY_PKG.INS_TSK_ADMIN"
				//    With .Parameters
				//        insCommand.Parameters.Add("WF_MGR_USER_TASK_ACTIVITY_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_ACTIVITY")).Direction = ParameterDirection.Input
				//        insCommand.Parameters["WF_MGR_USER_TASK_ACTIVITY_IN"].SourceVersion = DataRowVersion.Current
				//        insCommand.Parameters.Add("WF_MGR_USER_TASK_A_TYPE_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_ACTIVITY_TYPE")).Direction = ParameterDirection.Input
				//        insCommand.Parameters["WF_MGR_USER_TASK_A_TYPE_IN"].SourceVersion = DataRowVersion.Current
				//        insCommand.Parameters.Add("WF_MGR_USER_TASK_REF_NR_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_REF_NR")).Direction = ParameterDirection.Input
				//        insCommand.Parameters["WF_MGR_USER_TASK_REF_NR_IN"].SourceVersion = DataRowVersion.Current
				//        insCommand.Parameters.Add("WF_MGR_USER_TASK_STATUS_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_STATUS")).Direction = ParameterDirection.Input
				//        insCommand.Parameters["WF_MGR_USER_TASK_STATUS_IN"].SourceVersion = DataRowVersion.Current
				//        insCommand.Parameters.Add("WF_MGR_USER_TASK_START_DT_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_START_DT")).Direction = ParameterDirection.Input
				//        insCommand.Parameters["WF_MGR_USER_TASK_START_DT_IN"].SourceVersion = DataRowVersion.Current
				//        insCommand.Parameters.Add("WF_MGR_USER_TASK_DEADLINE_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_DEADLINE")).Direction = ParameterDirection.Input
				//        insCommand.Parameters["WF_MGR_USER_TASK_DEADLINE_IN"].SourceVersion = DataRowVersion.Current
				//        insCommand.Parameters.Add("WF_MGR_USER_TASK_DLMODE_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_DLMODE")).Direction = ParameterDirection.Input
				//        insCommand.Parameters["WF_MGR_USER_TASK_DLMODE_IN"].SourceVersion = DataRowVersion.Current
				//        insCommand.Parameters.Add("WF_MGR_USER_TASK_PRIORITY_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_PRIORITY")).Direction = ParameterDirection.Input
				//        insCommand.Parameters["WF_MGR_USER_TASK_PRIORITY_IN"].SourceVersion = DataRowVersion.Current
				//        insCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
				//        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
				//    End With
				//End With

				//With delCommand
				//    .Connection = objWK.MyConnection
				//    .CommandType = CommandType.StoredProcedure
				//    .CommandText = objWK.MyUserName & ".WORKFLOW_RULES_ENTRY_PKG.DEL_TSK_USER"
				//    With .Parameters
				//        delCommand.Parameters.Add("WF_MGR_USER_TASK_ACTIVITY_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_ACTIVITY")).Direction = ParameterDirection.Input
				//        delCommand.Parameters["WF_MGR_USER_TASK_ACTIVITY_IN"].SourceVersion = DataRowVersion.Current
				//        delCommand.Parameters.Add("WF_MGR_USER_TASK_A_TYPE_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_ACTIVITY_TYPE")).Direction = ParameterDirection.Input
				//        delCommand.Parameters["WF_MGR_USER_TASK_A_TYPE_IN"].SourceVersion = DataRowVersion.Current
				//        delCommand.Parameters.Add("WF_MGR_USER_TASK_REF_NR_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_REF_NR")).Direction = ParameterDirection.Input
				//        delCommand.Parameters["WF_MGR_USER_TASK_REF_NR_IN"].SourceVersion = DataRowVersion.Current
				//        delCommand.Parameters.Add("WF_MGR_USER_TASK_STATUS_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_STATUS")).Direction = ParameterDirection.Input
				//        delCommand.Parameters["WF_MGR_USER_TASK_STATUS_IN"].SourceVersion = DataRowVersion.Current
				//        delCommand.Parameters.Add("WF_MGR_USER_TASK_START_DT_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_START_DT")).Direction = ParameterDirection.Input
				//        delCommand.Parameters["WF_MGR_USER_TASK_START_DT_IN"].SourceVersion = DataRowVersion.Current
				//        delCommand.Parameters.Add("WF_MGR_USER_TASK_DEADLINE_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_DEADLINE")).Direction = ParameterDirection.Input
				//        delCommand.Parameters["WF_MGR_USER_TASK_DEADLINE_IN"].SourceVersion = DataRowVersion.Current
				//        delCommand.Parameters.Add("WF_MGR_USER_TASK_DLMODE_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_DLMODE")).Direction = ParameterDirection.Input
				//        delCommand.Parameters["WF_MGR_USER_TASK_DLMODE_IN"].SourceVersion = DataRowVersion.Current
				//        delCommand.Parameters.Add("WF_MGR_USER_TASK_PRIORITY_IN", M_DataSet.Tables(0).Rows(0).Item("WF_MGR_USER_TASK_PRIORITY")).Direction = ParameterDirection.Input
				//        delCommand.Parameters["WF_MGR_USER_TASK_PRIORITY_IN"].SourceVersion = DataRowVersion.Current
				//        delCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
				//        delCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
				//    End With
				//End With


				//AddHandler objWK.MyDataAdapter.RowUpdated, New OracleRowUpdatedEventHandler(AddressOf OnRowUpdated)

				//With objWK.MyDataAdapter
				//.InsertCommand = insCommand
				//.InsertCommand.Transaction = TRAN
				//.DeleteCommand = delCommand
				//.DeleteCommand.Transaction = TRAN
				//RecAfct = .Update(M_DataSet)
				//    If arrMessage.Count > 0 Then
				//        TRAN.Rollback()
				//        Return arrMessage
				//    Else
				//        TRAN.Commit()
				//        arrMessage.Add("All Data Saved Successfully")
				//        Return arrMessage
				//    End If
				//End With
				for (nRowCnt = 0; nRowCnt <= M_DataSet.Tables[0].Rows.Count - 1; nRowCnt++) {
					sql = " UPDATE workflow_mgr_task_msg_tbl set user_fks= " + HttpContext.Current.Session["user_pk"] + " where  ";
					sql += "adm_task_fk='" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_pk"] + "'";

					var _with1 = up;
					_with1.Transaction = TRAN;
					_with1.Connection = objWK.MyConnection;
					_with1.CommandType = CommandType.StoredProcedure;
					//.CommandText = objWK.MyUserName & ".WORKFLOW_RULES_ENTRY_PKG.UPD_TSK_ALOTMENT_ADMIN"
					_with1.CommandText = sql;
					up.ExecuteNonQuery();
				}


				for (nRowCnt = 0; nRowCnt <= M_DataSet.Tables[0].Rows.Count - 1; nRowCnt++) {
					sql = " Update  Wf_Mgr_Adm_Task_List_Tbl set ";
					sql += "       Wf_Mgr_Adm_Task_Activity='" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_ACTIVITY"] + "',Wf_Mgr_Adm_Task_Activity_Type='" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_ACTIVITY_TYPE"] + "',";
					//sql &= "       Wf_Mgr_Adm_Task_Cuser_Fk='3',"
					sql += "       Wf_Mgr_Adm_Task_Priority='" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_PRIORITY"] + "',";
					sql += "       Wf_Mgr_Adm_Task_Status='3', WF_MGR_ADM_TASK_START_DT = TO_DATE('" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_START_DT"] + "', 'DD/MM/YYYY HH24:MI:SS'),";
					sql += "        WF_MGR_ADM_TASK_DEADLINE = TO_DATE('" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_DEADLINE"] + "','DD/MM/YYYY HH24:MI:SS') - TO_DATE(SYSDATE,'DD/MM/YYYY HH24:MI:SS'),Wf_Mgr_Adm_Task_Dlmode='" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_DLMODE"] + "',";
					if (intLoc != 3) {
						//sql &= "    Wf_Mgr_Adm_Task_Loc_Fk='3',"
						sql += "    Wf_Mgr_Adm_Task_Loc_Fk='" + intLoc + "',";
					} else {
						sql += "    Wf_Mgr_Adm_Task_Loc_Fk='" + intLoc + "',";
					}
					sql += "       Wf_Mgr_Adm_Task_Ruser_Fk= " + HttpContext.Current.Session["user_pk"] + ", ";
					sql += "        LAST_MODIFIED_BY_FK=" + HttpContext.Current.Session["USER_PK"];
					sql += "      where Wf_Mgr_Adm_Task_Pk='" + M_DataSet.Tables[0].Rows[0]["wf_mgr_user_task_pk"] + "'";

					var _with2 = insCommand;
					_with2.Transaction = TRAN;
					_with2.Connection = objWK.MyConnection;
					_with2.CommandType = CommandType.StoredProcedure;
					_with2.CommandText = sql;
				}

				for (nRowCnt = 0; nRowCnt <= M_DataSet.Tables[0].Rows.Count - 1; nRowCnt++) {
					sql = " delete from wf_mgr_user_task_list_tbl where  ";
					sql += "wf_mgr_user_task_pk='" + M_DataSet.Tables[0].Rows[0]["wf_mgr_user_task_pk"] + "'";

					var _with3 = update;
					_with3.Transaction = TRAN;
					_with3.Connection = objWK.MyConnection;
					_with3.CommandType = CommandType.StoredProcedure;
					//.CommandText = objWK.MyUserName & ".WORKFLOW_RULES_ENTRY_PKG.UPD_TSK_ALOTMENT_ADMIN"
					_with3.CommandText = sql;
				}
				for (nRowCnt = 0; nRowCnt <= M_DataSet.Tables[0].Rows.Count - 1; nRowCnt++) {
					sql = " delete from workflow_mgr_alert_msg_tbl workf where workf.alert_msg_pk = '" + M_DataSet.Tables[0].Rows[0]["wf_mgr_user_task_pk"] + "'";
					var _with4 = delete;
					_with4.Transaction = TRAN;
					_with4.Connection = objWK.MyConnection;
					_with4.CommandType = CommandType.StoredProcedure;
					//.CommandText = objWK.MyUserName & ".WORKFLOW_RULES_ENTRY_PKG.UPD_TSK_ALOTMENT_ADMIN"
					_with4.CommandText = sql;
				}
				delete.ExecuteNonQuery();

				if (insCommand.ExecuteNonQuery() > 0) {
					update.ExecuteNonQuery();
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				} else {
					TRAN.Rollback();
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

		public ArrayList Complete(DataSet M_DataSet)
		{
			WorkFlow objWK = new WorkFlow();
			System.Web.UI.Page objPage = new System.Web.UI.Page();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			Int32 i = default(Int32);
			Int32 intLoc = default(Int32);
			intLoc = (Int32)HttpContext.Current.Session["LOGED_IN_LOC_FK"];
			OracleCommand insCommand = new OracleCommand();
			arrMessage.Clear();
			string sql = null;

			try {
				if (M_DataSet.Tables.Count > 0) {
					if (M_DataSet.Tables[0].Rows.Count > 0) {
						sql = " Update  Wf_Mgr_Adm_Task_List_Tbl set ";
						sql += "       Wf_Mgr_Adm_Task_Activity='" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_ACTIVITY"] + "',Wf_Mgr_Adm_Task_Activity_Type='" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_ACTIVITY_TYPE"] + "',";
						//sql &= "       Wf_Mgr_Adm_Task_Cuser_Fk='3',"
						sql += "       Wf_Mgr_Adm_Task_Priority='" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_PRIORITY"] + "',";
						sql += "       Wf_Mgr_Adm_Task_Status='4', WF_MGR_ADM_TASK_START_DT = TO_DATE((TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS')),'DD/MM/YYYY HH24:MI:SS') ,";
						sql += "       WF_MGR_ADM_TASK_DEADLINE = TO_DATE('" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_DEADLINE"] + "','DD/MM/YYYY HH24:MI:SS') - TO_DATE(SYSDATE,'DD/MM/YYYY HH24:MI:SS'),Wf_Mgr_Adm_Task_Dlmode='" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_DLMODE"] + "',";
						if (intLoc != 3) {
							sql += "    Wf_Mgr_Adm_Task_Loc_Fk='3',";
						} else {
							sql += "    Wf_Mgr_Adm_Task_Loc_Fk='" + intLoc + "',";
						}
						sql += "       Wf_Mgr_Adm_Task_Ruser_Fk='3',";
						sql += "       WF_MGR_ADM_TASK_COMPLETED_ON = to_date((to_char(sysdate,'dd/mm/yyyy hh24:mi:ss')),'dd/mm/yyyy hh24:mi:ss'), ";
						sql += "       LAST_MODIFIED_BY_FK=" + HttpContext.Current.Session["USER_PK"];
						sql += "       where Wf_Mgr_Adm_Task_Pk='" + M_DataSet.Tables[0].Rows[0]["wf_mgr_user_task_pk"] + "'";

						var _with5 = insCommand;
						_with5.Transaction = TRAN;
						_with5.Connection = objWK.MyConnection;
						_with5.CommandType = CommandType.StoredProcedure;
						_with5.CommandText = sql;

						insCommand.ExecuteNonQuery();
						sql = " update wf_mgr_user_task_list_tbl set wf_mgr_user_task_user_fk=null where  ";
						sql += "wf_mgr_user_task_pk='" + M_DataSet.Tables[0].Rows[0]["wf_mgr_user_task_pk"] + "'";

						var _with6 = insCommand;
						_with6.Transaction = TRAN;
						_with6.Connection = objWK.MyConnection;
						_with6.CommandType = CommandType.StoredProcedure;
						//.CommandText = objWK.MyUserName & ".WORKFLOW_RULES_ENTRY_PKG.UPD_TSK_ALOTMENT_ADMIN"
						_with6.CommandText = sql;
						insCommand.ExecuteNonQuery();


						sql = " delete from workflow_mgr_task_msg_tbl where  ";
						sql += "adm_task_fk='" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_pk"] + "'";

						var _with7 = insCommand;
						_with7.Transaction = TRAN;
						_with7.Connection = objWK.MyConnection;
						_with7.CommandType = CommandType.StoredProcedure;
						//.CommandText = objWK.MyUserName & ".WORKFLOW_RULES_ENTRY_PKG.UPD_TSK_ALOTMENT_ADMIN"
						_with7.CommandText = sql;
						insCommand.ExecuteNonQuery();

						sql = " delete from workflow_mgr_alert_msg_tbl workf where workf.alert_msg_pk = '" + M_DataSet.Tables[0].Rows[0]["WF_MGR_USER_TASK_pk"] + "'";
						var _with8 = insCommand;
						_with8.Transaction = TRAN;
						_with8.Connection = objWK.MyConnection;
						_with8.CommandType = CommandType.StoredProcedure;
						//.CommandText = objWK.MyUserName & ".WORKFLOW_RULES_ENTRY_PKG.UPD_TSK_ALOTMENT_ADMIN"
						_with8.CommandText = sql;

						insCommand.ExecuteNonQuery();

						if (insCommand.ExecuteNonQuery() > 0) {
							TRAN.Commit();
							arrMessage.Add("All Data Saved Successfully");
							return arrMessage;
						} else {
							TRAN.Rollback();
							return arrMessage;
						}

					}
				}
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
            return new ArrayList();
		}


		#endregion
		#region "DELETE"
		public int Delete(DataSet M_DataSet)
		{
			WorkFlow objWS = new WorkFlow();
			Int32 intPkVal = default(Int32);
			try {
				objWS.MyCommand.CommandType = CommandType.StoredProcedure;
				var _with9 = objWS.MyCommand.Parameters;
				_with9.Add("WF_MGR_USER_TASK_REF_NR_IN", OracleDbType.Varchar2, 50, WF_MGR_USER_TASK_REF_NR).Direction = ParameterDirection.Input;
				_with9.Add("WF_MGR_USER_TASK_STATUS_IN", OracleDbType.Varchar2, 15, WF_MGR_USER_TASK_STATUS).Direction = ParameterDirection.Input;
				//.Add("RETURN_VALUE", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Output
				objWS.MyCommand.CommandText = objWS.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.DEL_TSK_USER";
				if (objWS.ExecuteCommands() == true) {
					return 1;
				} else {
					return -1;
				}
			//Manjunath  PTS ID:Sep-02  15/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
	}
}
