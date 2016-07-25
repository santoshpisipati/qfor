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
using System.Web;

namespace Quantum_QFOR
{
    public class Cls_Taskalotment : CommonFeatures
    {
        #region "Fetch Function"

        public DataSet Fetch(string ValidFrom = " ", string ValidTo = " ", Int32 CurrentPage = 0, Int32 TotalPage = 0, string User = " ", bool flag = false, Int32 Status = 0)
        {
            string StrSQL = null;
            string strSQL1 = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            sb.AppendLine("SELECT  distinct");
            sb.AppendLine(" wfadm.wf_mgr_adm_task_activity Activity,");
            sb.AppendLine(" decode(wfadm.wf_mgr_adm_task_activity_type,1,'Internal',2,'Manual') ActivityType,");
            sb.AppendLine(" wfadm.wf_mgr_adm_task_ref_nr TaskRefNo,");
            sb.AppendLine(" (select u.user_id from user_mst_tbl u where u.user_mst_pk = wfadm.wf_mgr_adm_task_cuser_fk) CurrentUser,");
            sb.AppendLine(" decode(wfadm.wf_mgr_adm_task_priority,1,'Low',2,'High',3,'Critical') priority,");
            sb.AppendLine(" decode(wfadm.wf_mgr_adm_task_status,1,'Not Taken',2,'Taken',3,'Escalated',4,'Completed') Status,");
            sb.AppendLine(" wfadm.wf_mgr_adm_task_start_dt startdate,");
            //sb.AppendLine(" wfadm.wf_mgr_adm_task_deadline deadline,")

            //sb.AppendLine("               TO_CHAR(wfadm.WF_MGR_ADM_TASK_START_DT +")
            //sb.AppendLine("                       WC.WF_RULES_INT_DEADLINE +")
            //sb.AppendLine("                       numtodsinterval(WC.WF_RULES_INT_DEADLINE_HOURS,")
            //sb.AppendLine("                                       'hour') +")
            //sb.AppendLine("                       numtodsinterval(WC.WF_RULES_INT_DEADLINE_MINS,")
            //sb.AppendLine("                                       'minute'),")
            //sb.AppendLine("                       DATETIMEFORMAT24) DEADLINE,")
            sb.AppendLine("   FN_EX_WEEKENDDATE(WC.WF_RULES_INT_CONFIG_PK,(wfadm.WF_MGR_ADM_TASK_START_DT + ");
            sb.AppendLine("           WC.WF_RULES_INT_DEADLINE + ");
            sb.AppendLine("          NUMTODSINTERVAL(WC.WF_RULES_INT_DEADLINE_HOURS,'HOUR') + ");
            sb.AppendLine("         NUMTODSINTERVAL(WC.WF_RULES_INT_DEADLINE_MINS,'MINUTE'))) DEADLINE, ");

            //sb.AppendLine(" decode(wfadm.wf_mgr_adm_task_dlmode ,1,'Mins',2,'Hours',3,'Days') deadlinemode,")
            sb.AppendLine(" 'NA' deadlinemode,");
            sb.AppendLine(" '' ReasignedTo,'....'  users,'' sel ,");
            sb.AppendLine(" wfadm.wf_mgr_adm_task_cuser_fk,");
            sb.AppendLine(" wfadm.wf_mgr_adm_task_ruser_fk,wfadm.wf_mgr_adm_task_ref_fk,wfadm.wf_mgr_adm_task_pk,wfadm.wf_mgr_adm_task_start_dt");
            sb.AppendLine(" from Wf_mgr_adm_task_list_tbl wfadm,  ");
            //------------------
            sb.AppendLine("               WORKFLOW_MGR_TASK_MSG_TBL     TASK,");
            sb.AppendLine("               WORKFLOW_RULES_INT_MST_TBL    WF,");
            sb.AppendLine("               WORKFLOW_RULES_INT_APPL_TBL   APP,");
            sb.AppendLine("               WORKFLOW_RULES_INT_CONFIG_TBL WC,");
            sb.AppendLine("               WF_ACTIVITY_MST_TBL           ACT,");
            sb.AppendLine("               WF_ACTIVITY_MST_TBL           NEXTACT,");
            sb.AppendLine("               USER_MST_TBL                  U ");
            if (!string.IsNullOrEmpty(User))
            {
                sb.AppendLine("  ,user_mst_tbl UMT ");
            }
            sb.AppendLine("         WHERE WF.WF_RULES_INT_ACTIVITY = ACT.WF_ACTIVITY_MST_TBL_PK");
            sb.AppendLine("           AND WF.WF_RULES_INT_NEXT_ACTIVITY =");
            sb.AppendLine("               NEXTACT.WF_ACTIVITY_MST_TBL_PK");

            string biz_type = null;
            if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 1)
            {
                sb.AppendLine("     and act.biz_type in (1,3)");
            }
            else if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 2)
            {
                sb.AppendLine("     and act.biz_type in (2,3)");
            }
            else
            {
                sb.AppendLine("     and act.biz_type in (1,2,3)");
            }

            sb.AppendLine("           AND WF.WF_RULES_INT_MST_TBL_PK = APP.WORKFLOW_RULES_INT_MST_FK");
            sb.AppendLine("           AND WF.WF_RULES_INT_MST_TBL_PK = WC.WF_RULES_INT_MST_FK");
            sb.AppendLine("           AND APP.WORKFLOW_RULES_INT_CONFIG_FK = WC.WF_RULES_INT_CONFIG_PK");
            sb.AppendLine("           AND UPPER(wfadm.WF_MGR_ADM_TASK_ACTIVITY) =");
            sb.AppendLine("               UPPER(ACT.WF_ACTIVITY_NAME)");
            sb.AppendLine("           AND wfadm.WF_MGR_ADM_TASK_PK = TASK.ADM_TASK_FK");
            sb.AppendLine("           AND wfadm.WF_MGR_ADM_TASK_REF_FK = TASK.DOC_REF_NO_PK");
            sb.AppendLine("           AND UPPER(TASK.NEXT_ACTIVITY) = UPPER(NEXTACT.WF_ACTIVITY_NAME)");
            //sb.AppendLine("           AND U.USER_MST_PK = wfadm.wf_mgr_adm_task_cuser_fk(+)")
            sb.AppendLine("          AND APP.WORKFLOW_RULES_INT_LOC_FK IN ('" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + "') ");
            if (!string.IsNullOrEmpty(User))
            {
                sb.AppendLine("           AND U.USER_MST_PK = " + User);
            }
            else
            {
                sb.AppendLine("           AND U.USER_MST_PK = " + HttpContext.Current.Session["user_pk"]);
            }

            //sb.AppendLine("           AND U.DEFAULT_LOCATION_FK =")
            //sb.AppendLine("               TO_NUMBER(APP.WORKFLOW_RULES_INT_LOC_FK)")

            //------------------
            //sb.AppendLine(" where 1=1")

            if (Status > 0)
            {
                sb.AppendLine(" and wfadm.wf_mgr_adm_task_status= " + Status);
            }
            //end

            //StrSQL &= " where  wfadm.Wf_Mgr_Adm_Task_Loc_Fk=loc.location_mst_pk"
            //sb.AppendLine("  where loc.location_mst_pk=wfadm.wf_mgr_adm_task_loc_fk "
            //sb.AppendLine(" AND to_date('" & System.String.Format("{0:dd-MMM-yyyy}", enteredFromDate) & "','dd-Mon-yyyy') between WMR.WFLW_ADM_TASK_START_DT AND "
            //sb.AppendLine(" nvl(WMR.WFLW_ADM_TASK_START_DT,to_date('" & System.String.Format("{0:dd-MMM-yyyy}", enteredFromDate) & "')) "
            //If lngUserLocFk <> "0" Then
            //    sb.AppendLine(" and loc.location_mst_pk = " & lngUserLocFk
            //End If
            if (!((ValidFrom == null | string.IsNullOrEmpty(ValidFrom)) & (ValidTo == null | string.IsNullOrEmpty(ValidTo))))
            {
                sb.AppendLine(" AND to_date (wfadm.WF_MGR_ADM_TASK_START_DT,'dd/mm/yyyy') BETWEEN TO_DATE('" + ValidFrom + "','" + dateFormat + "')  AND TO_DATE('" + ValidTo + "','" + dateFormat + "') ");
            }
            else if (!(ValidFrom == null | string.IsNullOrEmpty(ValidFrom)))
            {
                sb.AppendLine(" AND wfadm.WF_MGR_ADM_TASK_START_DT >= TO_DATE('" + ValidFrom + "',dateformat) ");
            }
            else if (!(ValidTo == null | string.IsNullOrEmpty(ValidTo)))
            {
                sb.AppendLine(" AND wfadm.WF_MGR_ADM_TASK_START_DT >= TO_DATE('" + ValidTo + "',dateformat) ");
            }
            //If ValidFrom <> "" Then
            //    sb.AppendLine("and wfadm.WF_MGR_ADM_TASK_START_DT >= '" & ValidFrom & "'"
            //    sb.AppendLine("and wfadm.WF_MGR_ADM_TASK_START_DT <= '" & ValidTo & "'"
            //End If
            if (!string.IsNullOrEmpty(User))
            {
                sb.AppendLine(" and umt.user_mst_pk=wfadm.wf_mgr_adm_task_cuser_fk");
                sb.AppendLine(" and UMT.USER_MST_PK = U.USER_MST_PK");
                sb.AppendLine(" and UMT.USER_MST_PK = " + User);
            }
            if (flag)
            {
                sb.AppendLine("  and 1 = 2 ");
            }
            //sb.AppendLine(" order by wfadm.wf_mgr_adm_task_ref_fk desc"    'commented by prasant
            //sb.AppendLine("  AND umt.user_mst_pk=WMR.wflw_adm_task_cuser_fk "

            strSQL1 = " select count(*) from (";
            strSQL1 += sb.ToString() + ")";
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
            //strSQL1 = " select * from ("
            strSQL1 = "select * from (select  rownum SlNr,q.* from (";
            strSQL1 += sb.ToString() + " order by wfadm.wf_mgr_adm_task_start_dt desc)q)";
            strSQL1 += "  WHERE SlNr Between " + start + " and " + last;
            try
            {
                return objWF.GetDataSet(strSQL1.ToString());
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

        #endregion "Fetch Function"

        #region "Save Function"

        public ArrayList Save(DataSet dsmain)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string sql = null;
            Int16 Adm_Task = default(Int16);
            Int16 Task_msg = default(Int16);
            objWK.OpenConnection();
            //Dim Update_Proc As String
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand UpdateCmd = new OracleCommand();
            OracleCommand update = new OracleCommand();
            string UserName = objWK.MyUserName;
            arrMessage.Clear();
            try
            {
                //Dim dttbl As New DataTable
                //Dim DtRw As DataRow
                //Dim i As Integer
                //dttbl = M_DataSet.Tables(0)
                for (nRowCnt = 0; nRowCnt <= dsmain.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    sql = " UPDATE Wf_mgr_adm_task_list_tbl SET  Wf_Mgr_Adm_Task_Cuser_Fk  = '" + dsmain.Tables[0].Rows[nRowCnt]["WFLW_ADM_TASK_RUSER"] + "' ,";
                    sql += " Wf_Mgr_Adm_Task_Status = '" + dsmain.Tables[0].Rows[nRowCnt]["WFLW_ADM_TASK_STATUS"] + "' ,";
                    sql += " CREATED_BY_FK = '" + HttpContext.Current.Session["USER_PK"] + "' ,";
                    sql += " LAST_MODIFIED_BY_FK = '" + HttpContext.Current.Session["USER_PK"] + "' ";
                    sql += " WHERE(Wf_Mgr_Adm_Task_Pk = '" + dsmain.Tables[0].Rows[nRowCnt]["WFLW_mgr_adm_task_pk"] + "' ";
                    sql += " and wf_mgr_adm_task_activity = '" + dsmain.Tables[0].Rows[nRowCnt]["WFLW_adm_task_activity"] + "')";

                    var _with1 = UpdateCmd;
                    _with1.Transaction = TRAN;
                    _with1.Connection = objWK.MyConnection;
                    _with1.CommandType = CommandType.StoredProcedure;
                    //.CommandText = objWK.MyUserName & ".WORKFLOW_RULES_ENTRY_PKG.UPD_TSK_ALOTMENT_ADMIN"
                    _with1.CommandText = sql;

                    //  & " SET  Wf_Mgr_Adm_Task_Cuser_Fk  =  WFLW_ADM_TASK_CUSER_IN ,"
                    //Wf_Mgr_Adm_Task_Status = WFLW_ADM_TASK_STATUS_IN

                    //WHERE(Wf_Mgr_Adm_Task_Ref_Fk = WFLW_ADM_TASK_REF_FK_IN)
                    //  and wf_mgr_adm_task_activity   = WFLW_adm_task_activity_IN "
                    //For nRowCnt = 0 To dsmain.Tables(0).Rows.Count - 1
                    //    With .Parameters
                    //        .Clear()
                    //        UpdateCmd.Parameters.Add("WFLW_ADM_TASK_CUSER_IN", dsmain.Tables(0).Rows(nRowCnt).Item("WFLW_ADM_TASK_RUSER")).Direction = ParameterDirection.Input
                    //        UpdateCmd.Parameters["WFLW_ADM_TASK_CUSER_IN"].SourceVersion = DataRowVersion.Current
                    //        UpdateCmd.Parameters.Add("WFLW_ADM_TASK_STATUS_IN", dsmain.Tables(0).Rows(nRowCnt).Item("WFLW_ADM_TASK_STATUS")).Direction = ParameterDirection.Input
                    //        UpdateCmd.Parameters["WFLW_ADM_TASK_STATUS_IN"].SourceVersion = DataRowVersion.Current
                    //        UpdateCmd.Parameters.Add("WFLW_ADM_TASK_REF_FK_IN ", dsmain.Tables(0).Rows(nRowCnt).Item("WFLW_ADM_TASK_REF_FK")).Direction = ParameterDirection.Input
                    //        UpdateCmd.Parameters["WFLW_ADM_TASK_REF_FK_IN "].SourceVersion = DataRowVersion.Current
                    //        UpdateCmd.Parameters.Add("WFLW_adm_task_activity_IN", dsmain.Tables(0).Rows(nRowCnt).Item("WFLW_adm_task_activity")).Direction = ParameterDirection.Input
                    //        UpdateCmd.Parameters["WFLW_adm_task_activity_IN"].SourceVersion = DataRowVersion.Current
                    //        'updCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
                    //        'updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
                    //    End With

                    //Next accp_user_fk
                    Adm_Task = Convert.ToInt16(UpdateCmd.ExecuteNonQuery());
                }
                for (nRowCnt = 0; nRowCnt <= dsmain.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    sql = " UPDATE workflow_mgr_task_msg_tbl set accept=0,accp_user_fk=null, user_fks= '" + dsmain.Tables[0].Rows[nRowCnt]["WFLW_ADM_TASK_RUSER"] + "'";
                    sql += " where adm_task_fk= '" + dsmain.Tables[0].Rows[nRowCnt]["WFLW_mgr_adm_task_pk"] + "' ";
                    var _with2 = update;
                    _with2.Transaction = TRAN;
                    _with2.Connection = objWK.MyConnection;
                    _with2.CommandType = CommandType.StoredProcedure;
                    _with2.CommandText = sql;
                    Task_msg = Convert.ToInt16(update.ExecuteNonQuery());
                }

                //For nRowCnt = 0 To dsmain.Tables(0).Rows.Count - 1
                //    sql = " UPDATE WF_mgr_user_task_list_tbl set Wf_Mgr_User_Task_User_Fk= '" & dsmain.Tables(0).Rows(nRowCnt).Item("WFLW_ADM_TASK_RUSER") & "'"
                //    sql &= " where Wf_Mgr_User_Task_Ref_Fk= '" & dsmain.Tables(0).Rows(nRowCnt).Item("WFLW_ADM_TASK_REF_FK") & "' "
                //    With update
                //        .Transaction = TRAN
                //        .Connection = objWK.MyConnection
                //        .CommandType = CommandType.StoredProcedure
                //        .CommandText = sql
                //    End With
                //    update.ExecuteNonQuery()
                //Next
                if (Adm_Task > 0 & Task_msg > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //End With
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