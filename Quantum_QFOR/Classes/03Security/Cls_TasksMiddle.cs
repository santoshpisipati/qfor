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
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    public class Cls_TasksMiddle : CommonFeatures
	{
		#region "Fetch Tasks"
		public int FetchRulesKey(string refno)
		{
			StringBuilder strsql = new StringBuilder();
			WorkFlow objwf = new WorkFlow();
			strsql.Append(" select distinct adm.wf_mgr_adm_task_ref_fk from wf_mgr_adm_task_list_tbl ");
			strsql.Append("  adm where adm.wf_mgr_adm_task_ref_nr='" + refno + "' ");
			try {
				return Convert.ToInt32(objwf.ExecuteScaler(strsql.ToString()));
			} catch (Exception ex) {
				throw ex;
			} 
		}
		public int FetchBKGpk(int jobpk)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			strSQL += "select job.booking_mst_fk from JOB_CARD_TRN job where job.job_card_trn_pk=" + jobpk;
			try {
				return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			//Manjunath  PTS ID:Sep-02  16/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet CreateQuery()
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			strSQL += "select AM.MESSAGE,AM.REF_NR from ALERT_MESSAGE AM";
			try {
				return objWF.GetDataSet(strSQL);
			//Manjunath  PTS ID:Sep-02  16/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchInvoice(Int32 invpk)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			strSQL += "select con.business_type,con.process_type,con.customer_mst_fk ";
			strSQL += " from consol_invoice_tbl con where con.consol_invoice_pk= " + invpk;
			try {
				return objWF.GetDataSet(strSQL);
			//Manjunath  PTS ID:Sep-02  16/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataTable FetchQueueList(int Userpk, Int16 SortFlag = 0)
		{
			StringBuilder sb = new StringBuilder(5000);
			string ch = ",";
			WorkFlow objWF = new WorkFlow();
			sb.AppendLine(" SELECT ACTIVITY_PK,ACTIVITY,DOC_REF_NO_PK,DOC_REF_NO,STATUS,MESSAGE,NEXTACTIVITY,ACCEPT,PRIORITY,REF_TO,PK,ADM_TASK_FK,REF_TO_TASK,DEADLINE");

			sb.AppendLine(" FROM (SELECT  Task.Activity_Pk, upper(task.activity) activity, task.doc_ref_no_pk, task.doc_ref_no,");
			sb.AppendLine(" decode(task.status,1,'Active',2,'Done')status, task.message, (select act.wf_activity_name from wf_activity_mst_tbl act where upper(act.wf_activity_name) = upper(task.next_activity)) nextactivity,");
			sb.AppendLine("(case when (task.accept is null or task.accept = 0) then '0' else '1' end )accept,");
			sb.AppendLine(" decode(task.priority,2,'High', 3, 'Critical', 1, 'Low') priority, Task.ref_to, '' pk, ");
			sb.AppendLine(" task.adm_task_fk, task.REF_TO_TASK, ");
			//sb.AppendLine(" (case")
			//sb.AppendLine(" when wflist.wf_mgr_adm_task_dlmode = 1 then")
			//sb.AppendLine(" to_char((wflist.wf_mgr_adm_task_start_dt + (wflist.wf_mgr_adm_task_deadline/(60*24))),'dd/MM/yyyy hh:mm:ss')")
			//sb.AppendLine(" when wflist.wf_mgr_adm_task_dlmode = 2 then")
			//sb.AppendLine("  to_char((wflist.wf_mgr_adm_task_start_dt + (wflist.wf_mgr_adm_task_deadline/24)),'dd/MM/yyyy hh:mm:ss')")
			//sb.AppendLine(" when wflist.wf_mgr_adm_task_dlmode = 3 then")
			//sb.AppendLine("  to_char((wflist.wf_mgr_adm_task_start_dt + wflist.wf_mgr_adm_task_deadline),'dd/MM/yyyy hh:mm:ss') ")
			//sb.AppendLine(" end) Deadline, ")
			sb.AppendLine("                TO_CHAR(WFLIST.WF_MGR_ADM_TASK_START_DT +");
			sb.AppendLine("                        WC.WF_RULES_INT_DEADLINE +");
			sb.AppendLine("                        numtodsinterval(WC.WF_RULES_INT_DEADLINE_HOURS,");
			sb.AppendLine("                                        'hour') +");
			sb.AppendLine("                        numtodsinterval(WC.WF_RULES_INT_DEADLINE_MINS,");
			sb.AppendLine("                                        'minute'),");
			sb.AppendLine("                        DATETIMEFORMAT24) DEADLINE2,");

			sb.AppendLine("   TO_CHAR(FN_EX_WEEKENDDATE(WC.WF_RULES_INT_CONFIG_PK,(WFLIST.WF_MGR_ADM_TASK_START_DT + ");
			sb.AppendLine("           WC.WF_RULES_INT_DEADLINE + ");
			sb.AppendLine("          NUMTODSINTERVAL(WC.WF_RULES_INT_DEADLINE_HOURS,'HOUR') + ");
			sb.AppendLine("         NUMTODSINTERVAL(WC.WF_RULES_INT_DEADLINE_MINS,'MINUTE'))),DATETIMEFORMAT24 ) DEADLINE, ");

			//sb.AppendLine(" (case")
			//sb.AppendLine(" when wflist.wf_mgr_adm_task_dlmode = 1 then")
			//sb.AppendLine(" (wflist.wf_mgr_adm_task_start_dt + (wflist.wf_mgr_adm_task_deadline/(60*24)))")
			//sb.AppendLine(" when wflist.wf_mgr_adm_task_dlmode = 2 then")
			//sb.AppendLine(" (wflist.wf_mgr_adm_task_start_dt + (wflist.wf_mgr_adm_task_deadline/24))")
			//sb.AppendLine(" when wflist.wf_mgr_adm_task_dlmode = 3 then")
			//sb.AppendLine(" (wflist.wf_mgr_adm_task_start_dt + wflist.wf_mgr_adm_task_deadline) ")
			//sb.AppendLine(" end) Deadline1")
			sb.AppendLine("                (WFLIST.WF_MGR_ADM_TASK_START_DT +");
			sb.AppendLine("                        WC.WF_RULES_INT_DEADLINE +");
			sb.AppendLine("                        numtodsinterval(WC.WF_RULES_INT_DEADLINE_HOURS,");
			sb.AppendLine("                                        'hour') +");
			sb.AppendLine("                        numtodsinterval(WC.WF_RULES_INT_DEADLINE_MINS,");
			sb.AppendLine("                                        'minute')) DEADLINE1 ");

			sb.AppendLine("  FROM Workflow_mgr_TASK_msg_tbl Task, ");
			sb.AppendLine("       WF_MGR_ADM_TASK_LIST_TBL  WFLIST,");
			//-----------------------------------------------------------
			//sb.AppendLine("SELECT DISTINCT WFLIST.WF_MGR_ADM_TASK_ACTIVITY,")
			//sb.AppendLine("                WFLIST.WF_MGR_ADM_TASK_REF_NR,")
			//sb.AppendLine("                to_char(WFLIST.WF_MGR_ADM_TASK_START_DT, DATETIMEFORMAT24) TASK_START_DT,")
			//sb.AppendLine("                TO_CHAR(WFLIST.WF_MGR_ADM_TASK_START_DT +")
			//sb.AppendLine("                        WC.WF_RULES_INT_DEADLINE +")
			//sb.AppendLine("                        numtodsinterval(WC.WF_RULES_INT_DEADLINE_HOURS,")
			//sb.AppendLine("                                        'hour') +")
			//sb.AppendLine("                        numtodsinterval(WC.WF_RULES_INT_DEADLINE_MINS,")
			//sb.AppendLine("                                        'minute'),")
			//sb.AppendLine("                        DATETIMEFORMAT24) DEADLINE,")
			//sb.AppendLine("                WC.WF_RULES_INT_DEADLINE,")
			//sb.AppendLine("                WC.WF_RULES_INT_DEADLINE_HOURS,")
			//sb.AppendLine("                WC.WF_RULES_INT_DEADLINE_MINS,")
			//sb.AppendLine("                WFLIST.WF_MGR_ADM_TASK_DEADLINE,")
			//sb.AppendLine("                WFLIST.WF_MGR_ADM_TASK_LOC_FK,")
			//sb.AppendLine("                APP.WORKFLOW_RULES_INT_LOC_FK,")
			//sb.AppendLine("                WFLIST.WF_MGR_ADM_TASK_CUSER_FK")
			//sb.AppendLine("  FROM WF_MGR_ADM_TASK_LIST_TBL      WFLIST,")
			//sb.AppendLine("       WORKFLOW_MGR_TASK_MSG_TBL     TASK,")
			sb.AppendLine("       WORKFLOW_RULES_INT_MST_TBL    WF,");
			sb.AppendLine("       WORKFLOW_RULES_INT_APPL_TBL   APP,");
			sb.AppendLine("       WORKFLOW_RULES_INT_CONFIG_TBL WC,");
			sb.AppendLine("       WF_ACTIVITY_MST_TBL           ACT,");
			sb.AppendLine("       WF_ACTIVITY_MST_TBL           NEXTACT,");
			sb.AppendLine("       USER_MST_TBL                  U");
			sb.AppendLine(" WHERE WF.WF_RULES_INT_ACTIVITY = ACT.WF_ACTIVITY_MST_TBL_PK");
			sb.AppendLine("   AND WF.WF_RULES_INT_NEXT_ACTIVITY = NEXTACT.WF_ACTIVITY_MST_TBL_PK");
			sb.AppendLine("   AND WF.WF_RULES_INT_MST_TBL_PK = APP.WORKFLOW_RULES_INT_MST_FK");
			sb.AppendLine("   AND WF.WF_RULES_INT_MST_TBL_PK = WC.WF_RULES_INT_MST_FK");
			sb.AppendLine("   AND APP.WORKFLOW_RULES_INT_CONFIG_FK = WC.WF_RULES_INT_CONFIG_PK");
			sb.AppendLine("   AND UPPER(WFLIST.WF_MGR_ADM_TASK_ACTIVITY) = UPPER(ACT.WF_ACTIVITY_NAME)");
			sb.AppendLine("   AND WFLIST.WF_MGR_ADM_TASK_PK = TASK.ADM_TASK_FK");
			sb.AppendLine("   AND WFLIST.WF_MGR_ADM_TASK_REF_FK = TASK.DOC_REF_NO_PK");
			sb.AppendLine("   AND UPPER(TASK.NEXT_ACTIVITY) = UPPER(NEXTACT.WF_ACTIVITY_NAME)");
			sb.AppendLine("   AND U.USER_MST_PK = " + Userpk);
			sb.AppendLine("   AND U.DEFAULT_LOCATION_FK = TO_NUMBER(APP.WORKFLOW_RULES_INT_LOC_FK)");

			//-----------------------------------------------------------
			sb.AppendLine("   AND INSTR('" + ch + "'||task.user_fks||'" + ch + "','," + Userpk + ",') > 0 and Task.accp_user_fk is null ");
			//sb.AppendLine("   AND wflist.wf_mgr_adm_task_pk = task.adm_task_fk  and  task.doc_ref_no_pk = wflist.wf_mgr_adm_task_ref_fk")
			sb.AppendLine("   AND task.doc_ref_no = wflist.wf_mgr_adm_task_ref_nr ");
			sb.AppendLine(" AND NVL(WFLIST.HISTORY,0)=0");

			if (SortFlag != 0) {
				sb.AppendLine("and Task.Priority = " + SortFlag + ")");
			} else {
				sb.AppendLine(" )");
			}
			sb.AppendLine(" order by Deadline1 DESC ");
			try {
				return objWF.GetDataTable(sb.ToString());
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataTable FetchAlerts(int Userpk)
		{
			string strSQL = null;
			string ch = ",";
			WorkFlow objWF = new WorkFlow();
			//strSQL &= vbCrLf & " select alert.activity_pk,(select act.wf_activity_name from wf_activity_mst_tbl act where act.wf_activity_mst_tbl_pk=task.activity_pk) activity,alert.doc_ref_no_pk,alert.doc_ref_no,"
			strSQL += " select alert.activity_pk, upper(task.activity) ,alert.doc_ref_no_pk,alert.doc_ref_no,";
			strSQL += " '' status,GET_WORKFLOW_MSEG(alert.doc_ref_no_pk,alert.activity_pk,task.ref_to),(select act.wf_activity_name from wf_activity_mst_tbl act where upper(act.wf_activity_name) = upper(task.next_activity))nextactivity,";
			//strSQL &= vbCrLf & " (case when alert.status=1 then '1' else '0' end)accept,"
			//strSQL &= vbCrLf & " (case when alert.status=2 then '1' else '0' end)cancl,"
			//strSQL &= vbCrLf & "  (case when alert.status=3 then '1' else '0' end)snooze,"
			strSQL += "  decode(alert.priority,2, 'High', 3, 'Critical', 1, 'Low')priority ,'' Accept,Alert.ref_to";
			strSQL += "  from workflow_mgr_alert_msg_tbl Alert,workflow_mgr_task_msg_tbl task ";
			strSQL += "  where alert.alert_msg_pk=task.adm_task_fk and task.accp_user_fk = " + Userpk;
			//strSQL &= vbCrLf & "  and instr(GET_WORKFLOW_MSEG(alert.doc_ref_no_pk,alert.activity_pk,task.ref_to),'OD') > 0 "
			strSQL += "  order by Alert.Priority ";
			try {
				return objWF.GetDataTable(strSQL);
			//Manjunath  PTS ID:Sep-02  16/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataTable FetchTasks(Int32 userpk)
		{
			string strSQL = null;
			string ch = ",";
			WorkFlow objWF = new WorkFlow();
			//strSQL &= vbCrLf & " SELECT  Task.Activity_Pk, (select act.wf_activity_name from wf_activity_mst_tbl act where act.wf_activity_mst_tbl_pk=task.activity_pk) activity,task.doc_ref_no_pk,task.doc_ref_no,"
			strSQL += " SELECT  Task.Activity_Pk,  upper(task.activity),task.doc_ref_no_pk,task.doc_ref_no,";
			strSQL += " decode(task.status,1,'Active',2,'Done')status,task.message,(select act.wf_activity_name from wf_activity_mst_tbl act where upper(act.wf_activity_name) = upper(task.next_activity))nextactivity,";
			strSQL += "(case when (task.accept is null or task.accept=0) then '0' else '1' end )accept,";
			strSQL += "decode(task.priority,2,'High', 3, 'Critical', 1, 'Low')priority,Task.ref_to, '' pk,task.ref_to_task ";
			strSQL += " FROM Workflow_mgr_TASK_msg_tbl Task where INSTR('" + ch + "'||task.accp_user_fk||'" + ch + "','," + userpk + ",') > 0 and task.user_fks is null order by Task.Priority ";
			try {
				return objWF.GetDataTable(strSQL);
			//Manjunath  PTS ID:Sep-02  16/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Rate/Contract Expiry List"
		public DataSet FetchRateExpiryForAlert(int UserMstPk, int CurrentPage = 0, int TotalPage = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataSet dsRateExpiry = new DataSet();
			objWF.MyCommand = new OracleCommand();
			objWF.MyCommand.Parameters.Clear();
			objWF.MyCommand.Parameters.Add("USER_MST_PK_IN", UserMstPk).Direction = ParameterDirection.Input;
			objWF.MyCommand.Parameters.Add("M_PAGESIZE_IN", 15).Direction = ParameterDirection.Input;
			//M_MasterPageSize
			objWF.MyCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
			objWF.MyCommand.Parameters.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
			objWF.MyCommand.Parameters.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
			try {
				dsRateExpiry = objWF.GetDataSet("ALERT_NOTIFICATION_PKG", "FETCH_RATE_EXPIRY");
				CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENT_PAGE_IN"].Value);
				TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTAL_PAGE_IN"].Value);
			} catch (Exception ex) {
				throw ex;
			} finally {
			}
			return dsRateExpiry;
		}
		#endregion
	}
}
