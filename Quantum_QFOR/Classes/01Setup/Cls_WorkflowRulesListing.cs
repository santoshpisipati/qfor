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
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_WorkflowRulesListing : CommonFeatures
    {
        #region "Location"

        /// <summary>
        /// Locations the specified user loc pk.
        /// </summary>
        /// <param name="userLocPK">The user loc pk.</param>
        /// <returns></returns>
        public DataSet location(string userLocPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            string strReturn = null;
            WorkFlow objWF = new WorkFlow();
            OracleDataReader dr = null;
            try
            {
                strQuery.Append("");
                strQuery.Append(" SELECT L.LOCATION_ID, ");
                strQuery.Append("       L.LOCATION_MST_PK, ");
                strQuery.Append("       L.REPORTING_TO_FK, ");
                strQuery.Append("       L.LOCATION_TYPE_FK ");
                strQuery.Append("  FROM LOCATION_MST_TBL L ");
                strQuery.Append("  WHERE L.LOCATION_MST_PK = " + userLocPK);
                strQuery.Append("UNION ");
                strQuery.Append(" SELECT L.LOCATION_ID, ");
                strQuery.Append("       L.LOCATION_MST_PK, ");
                strQuery.Append("       L.REPORTING_TO_FK, ");
                strQuery.Append("       L.LOCATION_TYPE_FK ");
                strQuery.Append("  FROM LOCATION_MST_TBL L ");
                strQuery.Append("  WHERE L.REPORTING_TO_FK = " + userLocPK);
                return objWF.GetDataSet(strQuery.ToString());
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

        #endregion "Location"

        #region "Activity"

        /// <summary>
        /// Activities the specified CHK.
        /// </summary>
        /// <param name="chk">The CHK.</param>
        /// <returns></returns>
        public DataSet activity(Int16 chk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            string strReturn = null;
            WorkFlow objWF = new WorkFlow();
            OracleDataReader dr = null;
            try
            {
                strQuery.Append("");
                strQuery.Append("   SELECT '<ALL>' WF_ACTIVITY_NAME, ");
                strQuery.Append("       0 WF_ACTIVITY_MST_TBL_PK ");
                strQuery.Append("  FROM DUAL ");
                strQuery.Append("UNION ");
                strQuery.Append(" select WAMT.WF_ACTIVITY_NAME, ");
                strQuery.Append("        WAMT.WF_ACTIVITY_MST_TBL_PK");
                strQuery.Append("       from WF_ACTIVITY_MST_TBL WAMT,WORKFLOW_RULES_INT_MST_TBL WRMT ");
                strQuery.Append("       where WAMT.wf_activity_mst_tbl_pk<>0  AND WRMT.WF_RULES_INT_ACTIVITY = WAMT.WF_ACTIVITY_MST_TBL_PK");
                if (chk == 1)
                {
                    strQuery.Append("  and wrmt.Active = " + chk + "");
                }

                if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 1)
                {
                    strQuery.Append("      and wamt.biz_type in (1,3)");
                }
                else if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 2)
                {
                    strQuery.Append("      and wamt.biz_type in (2,3)");
                }
                strQuery.Append("  order by WF_ACTIVITY_NAME ");
                return objWF.GetDataSet(strQuery.ToString());
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

        #endregion "Activity"

        #region "Get Workflow Rules - Interior & Exterior"

        /// <summary>
        /// Gets the int wr.
        /// </summary>
        /// <param name="Active">if set to <c>true</c> [active].</param>
        /// <returns></returns>
        public DataTable GetIntWR(bool Active = true)
        {
            WorkFlow objWF = new WorkFlow();
            string StrQuery = "";
            if (Active)
            {
                StrQuery = "SELECT * FROM WORKFLOW_RULES_INT_MST_TBL WRIMT WHERE WRIMT.ACTIVE=1 ORDER BY WRIMT.WF_RULES_INT_REF_NO";
            }
            else
            {
                StrQuery = "SELECT * FROM WORKFLOW_RULES_INT_MST_TBL WRIMT ORDER BY WRIMT.WF_RULES_INT_REF_NO";
            }
            return objWF.GetDataTable(StrQuery);
        }

        /// <summary>
        /// Gets the ext wr.
        /// </summary>
        /// <returns></returns>
        public DataTable GetExtWR()
        {
            WorkFlow objWF = new WorkFlow();
            return objWF.GetDataTable("SELECT WREMT.WF_RULES_EXT_MST_TBL_PK,WREMT.WF_RULES_EXT_REF_NO FROM WORKFLOW_RULES_EXT_MST_TBL WREMT ORDER BY WREMT.WF_RULES_EXT_REF_NO");
        }

        #endregion "Get Workflow Rules - Interior & Exterior"

        #region "Fetch Internal"

        /// <summary>
        /// Fetches the internal.
        /// </summary>
        /// <param name="Activity">The activity.</param>
        /// <param name="Rule">The rule.</param>
        /// <param name="fdate">The fdate.</param>
        /// <param name="tdate">The tdate.</param>
        /// <param name="chk">The CHK.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchInternal(Int32 Activity, string Rule, string fdate, string tdate, Int16 chk, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            string strSQL1 = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            strSQL += "select ";
            strSQL += "  wrmt.wf_rules_int_mst_tbl_pk \"PK\",";
            strSQL += "  wrmt.wf_rules_int_ref_no \"Rules RefNo\",";
            strSQL += "  wfam.wf_activity_name \"Activity\" , ";
            strSQL += "  (select WFAM.WF_ACTIVITY_NAME from wf_activity_mst_tbl wfam ";
            strSQL += "  where WRMT.WF_RULES_INT_NEXT_ACTIVITY=WFAM.WF_ACTIVITY_MST_TBL_PK";

            if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 1)
            {
                strSQL += "   and wfam.biz_type in (1,3))  \"Next Activity\",";
            }
            else if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 2)
            {
                strSQL += "   and wfam.biz_type in (2,3))  \"Next Activity\",";
            }
            else
            {
                strSQL += "   and wfam.biz_type in (1,2,3))  \"Next Activity\",";
            }

            strSQL += "  wrmt.created_dt \"Created Date\",";
            strSQL += "  wrmt.last_modified_dt \"Modified Date\",";
            strSQL += "  wrmt.wf_rules_int_expiry_dt \"Expiry Date\",";
            strSQL += "  NVL(wrmt.ACTIVE, 0) \"Active\" ";
            strSQL += "  from Workflow_rules_int_mst_tbl wrmt,";
            strSQL += "  wf_activity_mst_tbl wfam";
            strSQL += "  where wrmt.wf_rules_int_activity = wfam.wf_activity_mst_tbl_pk ";
            if (Activity != 0)
            {
                strSQL += "and wrmt.Wf_Rules_Int_Activity=" + Activity;
            }

            if (!string.IsNullOrEmpty(Rule))
            {
                strSQL += "and wrmt.Wf_Rules_Int_Ref_No = '" + Rule + "'";
            }
            if (chk == 1)
            {
                strSQL += "and wrmt.Active =" + chk;
            }

            if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 1)
            {
                strSQL += "   and wfam.biz_type in (1,3)";
            }
            else if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 2)
            {
                strSQL += "   and wfam.biz_type in (2,3)";
            }
            if (!((fdate == null | string.IsNullOrEmpty(fdate)) & (tdate == null | string.IsNullOrEmpty(tdate))))
            {
                strSQL += " AND to_date (wrmt.Created_Dt,'dd/mm/yyyy') BETWEEN TO_DATE('" + fdate + "','" + dateFormat + "')  AND TO_DATE('" + tdate + "','" + dateFormat + "') ";
            }
            else if (!(fdate == null | string.IsNullOrEmpty(fdate)))
            {
                strSQL += " AND wrmt.Created_Dt >= TO_DATE('" + fdate + "',dateformat) ";
            }
            else if (!(tdate == null | string.IsNullOrEmpty(tdate)))
            {
                strSQL += " AND wrmt.Created_Dt >= TO_DATE('" + tdate + "',dateformat) ";
            }
            strSQL1 = " select count(*) from (";
            strSQL1 += strSQL + ")";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
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
            strSQL += " order by wrmt.created_dt desc ";
            strSQL1 = " select a.* from ( select rownum slno,q.* from (";
            strSQL1 += strSQL;
            strSQL1 += "  )q )a  WHERE SlNo Between " + start + " and " + last;
            try
            {
                return objWF.GetDataSet(strSQL1);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch Internal"

        #region "Fetch Manual"

        /// <summary>
        /// Fetches the manual.
        /// </summary>
        /// <param name="Task">The task.</param>
        /// <param name="fdate">The fdate.</param>
        /// <param name="tdate">The tdate.</param>
        /// <param name="chk">The CHK.</param>
        /// <param name="man">The man.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchManual(string Task, string fdate, string tdate, Int16 chk, Int16 man, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            string strSQL1 = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            strSQL += "select rownum slno,q.* from (select";
            strSQL += "  WREM.WF_RULES_EXT_MST_TBL_PK ,WREM.WF_RULES_EXT_REF_NO ,";
            strSQL += "  WREM.WF_RULES_EXT_ACTIVITY,WFAM.WF_ACTIVITY_NAME, (select WFAM.WF_ACTIVITY_NAME from wf_activity_mst_tbl wfam";
            strSQL += "  where WREM.WF_RULES_EXT_NEXT_ACTIVITY=WFAM.WF_ACTIVITY_MST_TBL_PK) \"Next_Activity\",WREM.CREATED_DT,WFS.PRIORITY,";
            strSQL += "  NVL(WREM.wf_Active, 0) \"Active\" ,NVL(WREM.Wf_Rules_Ext_Mandatory, 0) \"Mandatory\"";
            strSQL += "  from Workflow_rules_ext_mst_tbl WREM,wf_activity_mst_tbl wfam,";
            strSQL += "  wf_priority_status_tbl WFS";
            strSQL += "  where WREM.WF_RULES_EXT_PREV_ACTIVITY=WFAM.WF_ACTIVITY_MST_TBL_PK";
            strSQL += "  and WREM.WF_RULES_EXT_PRIORITY=WFS.WF_PRIORITY_STATUS_TBL_PK";
            if (!string.IsNullOrEmpty(Task))
            {
                strSQL += "and WREM.Wf_Rules_Ext_Ref_No = '" + Task + "'";
            }
            if (chk == 1)
            {
                strSQL += "and WREM.wf_Active =" + chk;
            }
            if (man == 1)
            {
                strSQL += "and WREM.Wf_Rules_Ext_Mandatory =" + man;
            }
            if (!((fdate == null | string.IsNullOrEmpty(fdate)) & (tdate == null | string.IsNullOrEmpty(tdate))))
            {
                strSQL += " AND to_date (WREM.Created_Dt,'dd/mm/yyyy') BETWEEN TO_DATE('" + fdate + "','" + dateFormat + "')  AND TO_DATE('" + tdate + "','" + dateFormat + "') ";
            }
            else if (!(fdate == null | string.IsNullOrEmpty(fdate)))
            {
                strSQL += " AND WREM.Created_Dt >= TO_DATE('" + fdate + "',dateformat) ";
            }
            else if (!(tdate == null | string.IsNullOrEmpty(tdate)))
            {
                strSQL += " AND WREM.Created_Dt = TO_DATE('" + tdate + "',dateformat) ";
            }
            strSQL += "  order by WFAM.WF_ACTIVITY_MST_TBL_PK ,WREM.WF_RULES_EXT_REF_NO)q";

            strSQL1 = " select count(*) from (";
            strSQL1 += strSQL + ")";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
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

            strSQL1 = " select * from (";
            strSQL1 += strSQL;
            strSQL1 += " ) WHERE SlNo Between " + start + " and " + last;
            try
            {
                return objWF.GetDataSet(strSQL1);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch Manual"

        #region "Fetch"

        /// <summary>
        /// Fetches the specified flag.
        /// </summary>
        /// <param name="flag">if set to <c>true</c> [flag].</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet Fetch(bool flag = false, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            string strSQL1 = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            strSQL += " select rownum SlNo, wf.* from (select distinct w.* from ";
            strSQL += " (select umt.user_id \"UserId\",";
            strSQL += "  altb.doc_type \"Activity\",";
            strSQL += "  altb.doc_ref_nr \"Ref.Nr.\",";
            strSQL += "  wfs.wf_status \"status\",wfp.priority \"Priority\",";
            strSQL += " case wfs.wf_status_tbl_pk when 1 then almsg.overdue";
            strSQL += " else '' end \"OverDue\"";
            strSQL += "  from alert_table altb,alert_message almsg,user_mst_tbl umt,";
            strSQL += "  WF_INTERNAL_RULES_ACTIVITY_TBL WFIR,";
            strSQL += "  WF_STATUS_TBL WFS,";
            strSQL += "  WF_PRIORITY_STATUS_TBL wfp";
            strSQL += "  where altb.doc_ref_nr = almsg.ref_nr";
            strSQL += "  and umt.user_mst_pk = altb.doc_cr_by";
            strSQL += "  and wfir.wf_status = wfs.wf_status_tbl_pk";
            strSQL += "  AND WFIR.WF_STATUS = altb.wf_status";
            strSQL += "  and wfir.priority = wfp.wf_priority_status_tbl_pk";
            strSQL += "  and wfp.wf_priority_status_tbl_pk = altb.priority";
            if (flag)
            {
                strSQL += "  and 1 = 2 ";
            }
            strSQL += "  order by wfir.wf_status ,wfir.priority ) W)wf";

            strSQL1 = " select count(*) from (";
            strSQL1 += strSQL + ")";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL1));
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
            strSQL1 = " select * from (";
            strSQL1 += strSQL;
            strSQL1 += " ) WHERE SlNo Between " + start + " and " + last;
            try
            {
                return objWF.GetDataSet(strSQL1);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch"

        #region "GetUser"

        /// <summary>
        /// Gets the user.
        /// </summary>
        /// <returns></returns>
        public DataSet GetUser()
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            string strReturn = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("  SELECT umt.user_id,umt.user_mst_pk ");
                strQuery.Append("  FROM user_mst_tbl umt ");
                strQuery.Append(" where umt.is_activated= 1 ");
                strQuery.Append(" order by umt.user_id");
                return objWF.GetDataSet(strQuery.ToString());
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

        #endregion "GetUser"
    }
}