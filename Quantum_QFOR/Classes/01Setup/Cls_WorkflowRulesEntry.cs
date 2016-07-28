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
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_WorkflowRulesEntry : CommonFeatures
    {
        /// <summary>
        /// </summary>
        private long _PK;

        /// <summary>
        /// The m_ data set
        /// </summary>
        private static DataSet M_DataSet = new DataSet();

        #region " MyDataSet()"

        /// <summary>
        /// Gets my data set.
        /// </summary>
        /// <value>
        /// My data set.
        /// </value>
        public static DataSet MyDataSet
        {
            get { return M_DataSet; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Cls_WorkflowRulesEntry"/> class.
        /// </summary>
        /// <param name="SelectAll">if set to <c>true</c> [select all].</param>
        public Cls_WorkflowRulesEntry(bool SelectAll = false)
        {
            string Sql = null;
            Sql += " select 0 WF_ACTIVITY_MST_TBL_PK,";
            Sql += " ' ' WF_ACTIVITY_NAME from dual";
            Sql += " union";
            Sql += " select WAMT.WF_ACTIVITY_MST_TBL_PK, ";
            Sql += " WAMT.WF_ACTIVITY_NAME ";
            Sql += " from WF_ACTIVITY_MST_TBL WAMT ";
            Sql += " where WAMT.wf_activity_mst_tbl_pk<>0";
            Sql += " order by WF_ACTIVITY_NAME ";

            try
            {
                M_DataSet = (new WorkFlow()).GetDataSet(Sql);
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

        /// <summary>
        /// Ativities this instance.
        /// </summary>
        /// <returns></returns>
        public DataSet Ativity()
        {
            WorkFlow objWF = new WorkFlow();
            string Sql = null;
            Sql += " select 0 WF_ACTIVITY_MST_TBL_PK,";
            Sql += " ' ' WF_ACTIVITY_NAME from dual";
            Sql += " union";
            Sql += " select WAMT.WF_ACTIVITY_MST_TBL_PK, ";
            Sql += " WAMT.WF_ACTIVITY_NAME ";
            Sql += " from WF_ACTIVITY_MST_TBL WAMT ";
            Sql += " where WAMT.wf_activity_mst_tbl_pk<>0";
            Sql += " order by WF_ACTIVITY_NAME ";
            try
            {
                return objWF.GetDataSet(Sql);
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

        #endregion " MyDataSet()"

        #region "Fetch dsinternal"

        /// <summary>
        /// Dsinternals the specified internal pk.
        /// </summary>
        /// <param name="InternalPk">The internal pk.</param>
        /// <returns></returns>
        public DataSet dsinternal(string InternalPk = "")
        {
            string Sql = null;
            WorkFlow objWF = new WorkFlow();
            Sql += " select wfin.doc_status, ";
            Sql += " wfin.wf_status, ";
            Sql += " wfin.priority, wfin.deadline, decode(wfin.deadline_mode,1,'Mins',2,'Hrs',3,'Days'), ";
            Sql += " wfin.alert_freq, decode(wfin.alert_mode,1,'Min',2,'Hrs',3,'Days'), decode(wfin.alert_type,1,'Message',2,'E-Mail'), ";
            Sql += " wfin.overdue_alert, wfin.escalate_alert, wfin.wf_internal_rules_act_tbl_pk ";
            Sql += " from wf_INTERNAL_RULES_ACTIVITY_TBL wfin ";
            Sql += " where  ";
            Sql += " wfin.rules_activity_mst_tbl_fk =  " + InternalPk;
            Sql += " order by wfin.wf_internal_rules_act_tbl_pk ";
            try
            {
                return objWF.GetDataSet(Sql);
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

        #endregion "Fetch dsinternal"

        #region "Fetch "

        /// <summary>
        /// Fetches the specified activity pk.
        /// </summary>
        /// <param name="ActivityPk">The activity pk.</param>
        /// <returns></returns>
        public DataSet Fetch(string ActivityPk = "")
        {
            string Sql = null;
            WorkFlow objWF = new WorkFlow();
            Sql += " select WAMT.WF_ACTIVITY_MST_TBL_PK, ";
            Sql += " WAMT.WF_ACTIVITY_NAME ";
            Sql += " from WF_ACTIVITY_MST_TBL WAMT where 1=2";
            Sql += " order by WAMT.WF_ACTIVITY_MST_TBL_PK ";
            try
            {
                return objWF.GetDataSet(Sql);
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

        #endregion "Fetch "

        #region "FetchDocStat "

        /// <summary>
        /// Fetches the document stat.
        /// </summary>
        /// <param name="IRulesPk">The i rules pk.</param>
        /// <returns></returns>
        public DataTable FetchDocStat(string IRulesPk = "")
        {
            string Sql = null;
            WorkFlow objWF = new WorkFlow();
            Sql += "select WDST.WF_DOC_STATUS_TBL_PK, ";
            Sql += "  WDST.DOC_STATUS ";
            Sql += " from WF_DOC_STATUS_TBL WDST ";
            try
            {
                return objWF.GetDataTable(Sql);
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

        #endregion "FetchDocStat "

        #region "FetchPriority "

        /// <summary>
        /// Fetches the priority.
        /// </summary>
        /// <param name="PriorityPk">The priority pk.</param>
        /// <returns></returns>
        public DataTable FetchPriority(string PriorityPk = "")
        {
            string Sql = null;
            WorkFlow objWF = new WorkFlow();
            Sql += "select WPST.WF_PRIORITY_STATUS_TBL_PK, ";
            Sql += "  WPST.PRIORITY ";
            Sql += " from WF_PRIORITY_STATUS_TBL WPST ";
            try
            {
                return objWF.GetDataTable(Sql);
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

        #endregion "FetchPriority "

        #region "FetchWfStat "

        /// <summary>
        /// Fetches the wf stat.
        /// </summary>
        /// <param name="WfPk">The wf pk.</param>
        /// <returns></returns>
        public DataTable FetchWfStat(string WfPk = "")
        {
            string Sql = null;
            WorkFlow objWF = new WorkFlow();

            Sql += "select WST.WF_STATUS_TBL_PK, ";
            Sql += "WST.WF_STATUS ";
            Sql += " from WF_STATUS_TBL WST ";
            try
            {
                return objWF.GetDataTable(Sql);
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

        #endregion "FetchWfStat "

        #region "FetchfromListing "

        /// <summary>
        /// Fetchfroms the listing.
        /// </summary>
        /// <param name="WfPk">The wf pk.</param>
        /// <returns></returns>
        public DataTable FetchfromListing(string WfPk = "")
        {
            string Sql = null;
            WorkFlow objWF = new WorkFlow();
            Sql += " select wrmt.wf_rules_int_mst_tbl_pk, ";
            Sql += " wrmt.wf_rules_int_ref_no,wrmt.created_dt, wrmt.last_modified_dt, wrmt.wf_rules_int_expiry_dt, ";
            Sql += " wrmt.wf_rules_int_activity,wfam.wf_activity_name,wrmt.wf_rules_int_next_activity,wfam.wf_activity_name,wrmt.WF_RULES_INT_PRIORITY,wrmt.active";
            Sql += "from Workflow_rules_int_mst_tbl wrmt, wf_activity_mst_tbl wfam ";
            Sql += " where wrmt.wf_rules_int_activity = wfam.wf_activity_mst_tbl_pk and wrmt.wf_rules_int_mst_tbl_pk = " + WfPk;
            try
            {
                return objWF.GetDataTable(Sql);
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

        #endregion "FetchfromListing "

        #region "FetchManual "

        /// <summary>
        /// Fetches the manual.
        /// </summary>
        /// <param name="WfPk">The wf pk.</param>
        /// <param name="Task">The task.</param>
        /// <returns></returns>
        public DataTable FetchManual(string WfPk = "", string Task = "")
        {
            string Sql = null;
            WorkFlow objWF = new WorkFlow();

            Sql += " select WREM.WF_RULES_EXT_MST_TBL_PK,WREM.WF_RULES_EXT_REF_NO,WREM.CREATED_DT,WREM.WF_RULES_EXT_ACTIVITY, ";
            Sql += " WREM.WF_RULES_EXT_ACTIVITY_DESC,WREM.WF_RULES_EXT_PREV_ACTIVITY,WREM.WF_RULES_EXT_NEXT_ACTIVITY, ";
            Sql += " WREM.WF_RULES_EXT_PRIORITY,";
            Sql += " WREM.Wf_Active,WREM.Wf_Rules_Ext_Mandatory";
            Sql += " from Workflow_rules_ext_mst_tbl WREM,wf_activity_mst_tbl wfam";
            Sql += " where WREM.WF_RULES_EXT_PREV_ACTIVITY = wfam.wf_activity_mst_tbl_pk";
            if (!string.IsNullOrEmpty(WfPk))
            {
                Sql += " and WREM.WF_RULES_EXT_MST_TBL_PK= " + WfPk;
            }
            if (!string.IsNullOrEmpty(Task))
            {
                Sql += " and WREM.Wf_Rules_Ext_Ref_No='" + Task + "' ";
            }
            Sql += " order by WREM.WF_RULES_EXT_MST_TBL_PK";
            try
            {
                return objWF.GetDataTable(Sql);
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

        #endregion "FetchManual "

        #region "FetchNxtAxnGrid "

        /// <summary>
        /// Fetches the NXT axn grid.
        /// </summary>
        /// <param name="Pks">The PKS.</param>
        /// <returns></returns>
        public DataSet FetchNxtAxnGrid(string Pks = "")
        {
            string Sql = null;
            WorkFlow objWF = new WorkFlow();

            Sql += " select wfam.wf_activity_mst_tbl_pk, wfam.wf_activity_name ";
            Sql += " from wf_activity_mst_tbl wfam ";
            Sql += " where wfam.wf_activity_mst_tbl_pk in ( " + Pks + " )";

            try
            {
                return objWF.GetDataSet(Sql);
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

        #endregion "FetchNxtAxnGrid "

        #region "FetchNxtAxnGrid "

        /// <summary>
        /// Gets the location.
        /// </summary>
        /// <param name="userLocPK">The user loc pk.</param>
        /// <returns></returns>
        public DataSet GetLocation(string userLocPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            string strReturn = null;
            WorkFlow objWF = new WorkFlow();
            OracleDataReader dr = null;
            try
            {
                strQuery = new System.Text.StringBuilder();
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

        #endregion "FetchNxtAxnGrid "

        #region "priority "

        /// <summary>
        /// Priorities this instance.
        /// </summary>
        /// <returns></returns>
        public object priority()
        {
            string Sql = null;
            WorkFlow objWF = new WorkFlow();
            Sql += " select WFS.WF_PRIORITY_STATUS_TBL_PK,WFS.PRIORITY ";
            Sql += " from wf_priority_status_tbl WFS ";
            Sql += " order by WFS.WF_PRIORITY_STATUS_TBL_PK";
            try
            {
                return objWF.GetDataSet(Sql);
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

        #endregion "priority "

        #region "Insert Function"

        /// <summary>
        /// Saves the activity.
        /// </summary>
        /// <param name="ruleref">The ruleref.</param>
        /// <param name="frmDate">The FRM date.</param>
        /// <param name="activity">The activity.</param>
        /// <param name="key">The key.</param>
        /// <param name="ILocationId">The i location identifier.</param>
        /// <param name="IEmployeeId">The i employee identifier.</param>
        /// <param name="Userid">The userid.</param>
        /// <returns></returns>
        public string SaveActivity(string ruleref, System.DateTime frmDate, Int32 activity, string key, Int64 ILocationId, Int64 IEmployeeId, Int64 Userid)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction INSERT = null;
            objWK.OpenConnection();
            INSERT = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = INSERT;
            try
            {
                objWK.MyCommand.Parameters.Clear();
                var _with1 = objWK.MyCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.RULE_ACTIVITY";
                ruleref = GenerateProtocolKey(key, ILocationId, IEmployeeId, DateTime.Now, "", "", "", Userid);
                _with1.Parameters.Add("RULES_REF_NO_IN", ruleref).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("RULES_DATE_IN", frmDate).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("ACTIVITY_IN", activity).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                _with1.ExecuteNonQuery();

                INSERT.Commit();
                return Convert.ToString(_with1.Parameters["RETURN_VALUE"].Value);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "Insert Function"

        #region "GenerateProtocolKey"

        /// <summary>
        /// Generates the specified ruleref.
        /// </summary>
        /// <param name="ruleref">The ruleref.</param>
        /// <param name="key">The key.</param>
        /// <param name="ILocationId">The i location identifier.</param>
        /// <param name="IEmployeeId">The i employee identifier.</param>
        /// <param name="Userid">The userid.</param>
        /// <returns></returns>
        public string Generate(string ruleref, string key, Int64 ILocationId, Int64 IEmployeeId, Int64 Userid)
        {
            ruleref = GenerateProtocolKey(key, ILocationId, IEmployeeId, DateTime.Now, "", "", "", Userid);
            return ruleref;
        }

        #endregion "GenerateProtocolKey"

        #region "SaveNext"

        /// <summary>
        /// Saves the next.
        /// </summary>
        /// <param name="NextPk">The next pk.</param>
        /// <returns></returns>
        public string SaveNext(Int64 NextPk)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction INSERT = null;
            objWK.OpenConnection();
            INSERT = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = INSERT;
            try
            {
                objWK.MyCommand.Parameters.Clear();
                var _with2 = objWK.MyCommand;
                _with2.CommandType = CommandType.StoredProcedure;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.NEXT_ACTIVITY";
                _with2.Parameters.Add("NEXT_ACTIVITY_IN", NextPk).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10).Direction = ParameterDirection.Output;
                _with2.ExecuteNonQuery();
                INSERT.Commit();
                return Convert.ToString(_with2.Parameters["RETURN_VALUE"].Value);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "SaveNext"

        #region "SaveInternal"

        /// <summary>
        /// Saves the internal.
        /// </summary>
        /// <param name="rulefk">The rulefk.</param>
        /// <param name="dsInternalData">The ds internal data.</param>
        /// <returns></returns>
        public ArrayList SaveInternal(Int64 rulefk, DataSet dsInternalData)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction INSERT = null;
            OracleCommand insInternalDetails = new OracleCommand();
            Int32 RecAfct = default(Int32);
            objWK.OpenConnection();
            INSERT = objWK.MyConnection.BeginTransaction();

            try
            {
                var _with3 = insInternalDetails;
                _with3.Transaction = INSERT;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.UPD_INTERNAL_RULES";
                var _with4 = _with3.Parameters;
                _with4.Clear();
                insInternalDetails.Parameters.Add("Wf_Rules_Int_Mst_Tbl_Pk_IN", rulefk).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Int_Ref_No_IN", dsInternalData.Tables[0].Rows[0]["RULES_REF_NO"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Created_Dt_IN", dsInternalData.Tables[0].Rows[0]["CREATED_DATE"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Last_Modified_Dt_IN", dsInternalData.Tables[0].Rows[0]["MODIFIED_DATE"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("LAST_MODIFIED_BY_FK_IN", dsInternalData.Tables[0].Rows[0]["M_CREATED_BY_FK"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Int_Expiry_Dt_IN", dsInternalData.Tables[0].Rows[0]["EXPIRY_DATE"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Int_Priority_IN", dsInternalData.Tables[0].Rows[0]["PRIORITY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Active_IN", dsInternalData.Tables[0].Rows[0]["ACTIVE"]).Direction = ParameterDirection.Input;
                insInternalDetails.ExecuteNonQuery();
                if (arrMessage.Count > 0)
                {
                    INSERT.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    INSERT.Commit();
                    return arrMessage;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        /// <summary>
        /// Saves the workflow supervisors.
        /// </summary>
        /// <param name="WF_RULES_INT_MST_FK">The w f_ rule s_ in t_ ms t_ fk.</param>
        /// <param name="LOCATION_FK">The locatio n_ fk.</param>
        /// <param name="dsSupervisors">The ds supervisors.</param>
        /// <returns></returns>
        public ArrayList SaveWorkflowSupervisors(int WF_RULES_INT_MST_FK, int LOCATION_FK, DataSet dsSupervisors)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            OracleCommand insInternalDetails = new OracleCommand();
            Int32 RecAfct = default(Int32);
            string deleteUserFks = "";
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            try
            {
                objWK.MyCommand = new OracleCommand();
                var _with5 = objWK.MyCommand;
                _with5.Transaction = TRAN;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.SAVE_WF_SUPERVISORS";
                foreach (DataRow drSV in dsSupervisors.Tables[0].Rows)
                {
                    _with5.Parameters.Clear();
                    _with5.Parameters.Add("WF_RULES_INT_MST_PK_IN", WF_RULES_INT_MST_FK).Direction = ParameterDirection.Input;
                    _with5.Parameters.Add("LOCATION_MST_FK_IN", drSV["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with5.Parameters.Add("USER_MST_FK_IN", drSV["USER_MST_FK"]).Direction = ParameterDirection.Input;
                    _with5.Parameters.Add("TO_OR_CC_IN", drSV["TO_OR_CC"]).Direction = ParameterDirection.Input;
                    _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Output;
                    _with5.ExecuteNonQuery();
                    if (string.IsNullOrEmpty(deleteUserFks))
                    {
                        deleteUserFks = drSV["USER_MST_FK"].ToString();
                    }
                    else
                    {
                        deleteUserFks += "," + drSV["USER_MST_FK"];
                    }
                }
                if (!string.IsNullOrEmpty(deleteUserFks.Trim()) & deleteUserFks.Trim() != "0" & LOCATION_FK > 0)
                {
                    _with5.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.DELETE_WF_SUPERVISORS";
                    _with5.Parameters.Clear();
                    _with5.Parameters.Add("WF_RULES_INT_MST_PK_IN", WF_RULES_INT_MST_FK).Direction = ParameterDirection.Input;
                    _with5.Parameters.Add("LOCATION_MST_FK_IN", LOCATION_FK).Direction = ParameterDirection.Input;
                    _with5.Parameters.Add("USER_MST_FKS_IN", deleteUserFks.Trim()).Direction = ParameterDirection.Input;
                    _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Output;
                    _with5.ExecuteNonQuery();
                }

                arrMessage.Add("All Data Saved Successfully");
                TRAN.Commit();
                return arrMessage;
            }
            catch (OracleException OraExp)
            {
                TRAN.Rollback();
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }

        #endregion "SaveInternal"

        #region "InsertInternal"

        /// <summary>
        /// Inserts the internal.
        /// </summary>
        /// <param name="dsInternalData">The ds internal data.</param>
        /// <returns></returns>
        public ArrayList InsertInternal(DataSet dsInternalData)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction INSERT = null;
            OracleCommand insInternalDetails = new OracleCommand();
            Int32 RecAfct = default(Int32);
            objWK.OpenConnection();
            INSERT = objWK.MyConnection.BeginTransaction();
            try
            {
                var _with6 = insInternalDetails;
                _with6.Transaction = INSERT;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.INS_INTERNAL_RULES";
                var _with7 = _with6.Parameters;
                _with7.Clear();
                insInternalDetails.Parameters.Add("Wf_Rules_Int_Ref_No_IN", dsInternalData.Tables[0].Rows[0]["RULES_REF_NO"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Created_Dt_IN", dsInternalData.Tables[0].Rows[0]["CREATED_DATE"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Last_Modified_Dt_IN", dsInternalData.Tables[0].Rows[0]["MODIFIED_DATE"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Int_Expiry_Dt_IN", dsInternalData.Tables[0].Rows[0]["EXPIRY_DATE"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Int_Activity_IN", dsInternalData.Tables[0].Rows[0]["ACTIVITY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Int_Next_Activity_IN", dsInternalData.Tables[0].Rows[0]["NEXT_ACTIVITY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Int_Priority_IN", dsInternalData.Tables[0].Rows[0]["PRIORITY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Active_IN", dsInternalData.Tables[0].Rows[0]["ACTIVE"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                insInternalDetails.ExecuteNonQuery();
                RecAfct = Convert.ToInt32(insInternalDetails.Parameters["RETURN_VALUE"].Value);
                if (arrMessage.Count > 0)
                {
                    INSERT.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    arrMessage.Add(RecAfct);
                    INSERT.Commit();
                    return arrMessage;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "InsertInternal"

        #region "InsertManual"

        /// <summary>
        /// Inserts the manual.
        /// </summary>
        /// <param name="dsInternalData">The ds internal data.</param>
        /// <param name="ruleref">The ruleref.</param>
        /// <param name="key">The key.</param>
        /// <param name="ILocationId">The i location identifier.</param>
        /// <param name="IEmployeeId">The i employee identifier.</param>
        /// <param name="Userid">The userid.</param>
        /// <returns></returns>
        public string InsertManual(DataSet dsInternalData, string ruleref, string key, Int64 ILocationId, Int64 IEmployeeId, Int64 Userid)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction INSERT = null;
            OracleCommand update = new OracleCommand();
            OracleCommand insInternalDetails = new OracleCommand();
            Int32 RecAfct = default(Int32);
            string sql = null;
            objWK.OpenConnection();
            INSERT = objWK.MyConnection.BeginTransaction();
            try
            {
                var _with8 = insInternalDetails;
                _with8.Transaction = INSERT;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.INS_MANUAL_RULES";

                var _with9 = _with8.Parameters;
                _with9.Clear();
                ruleref = GenerateProtocolKey(key, ILocationId, IEmployeeId, DateTime.Now, "", "", "", Userid);
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Ref_No_IN", ruleref).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("CREATED_BY_FK_IN", dsInternalData.Tables[0].Rows[0]["M_CREATED_BY_FK"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Created_Dt_IN", dsInternalData.Tables[0].Rows[0]["CREATED_DATE"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Activity_IN", getDefault(dsInternalData.Tables[0].Rows[0]["ACTIVITY"], " ")).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Activity_Desc_IN", getDefault(dsInternalData.Tables[0].Rows[0]["DESCRIPTION"], " ")).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Prev_Activity_IN", dsInternalData.Tables[0].Rows[0]["PREVIOUS_ACTIVITY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Next_Activity_IN", dsInternalData.Tables[0].Rows[0]["NEXT_ACTIVITY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Priority_IN", dsInternalData.Tables[0].Rows[0]["PRIORITY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Active_IN", dsInternalData.Tables[0].Rows[0]["ACTIVE"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("MANDATORY_IN", dsInternalData.Tables[0].Rows[0]["MANDATORY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("VERSION_NO_IN", dsInternalData.Tables[0].Rows[0]["VERSION_NO"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insInternalDetails.ExecuteNonQuery();
                if (!string.IsNullOrEmpty(ruleref))
                {
                    INSERT.Commit();
                    return ruleref;
                }
                else
                {
                    INSERT.Rollback();
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return "";
        }

        #endregion "InsertManual"

        #region "SaveManual"

        /// <summary>
        /// Saves the manual.
        /// </summary>
        /// <param name="rulefk">The rulefk.</param>
        /// <param name="dsInternalData">The ds internal data.</param>
        /// <returns></returns>
        public ArrayList SaveManual(Int64 rulefk, DataSet dsInternalData)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction INSERT = null;
            OracleCommand insInternalDetails = new OracleCommand();
            Int32 RecAfct = default(Int32);
            objWK.OpenConnection();
            INSERT = objWK.MyConnection.BeginTransaction();
            try
            {
                var _with10 = insInternalDetails;
                _with10.Transaction = INSERT;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.UPD_MANUAL_RULES";
                var _with11 = _with10.Parameters;
                _with11.Clear();
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Mst_Tbl_Pk_IN", rulefk).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Ref_No_IN", dsInternalData.Tables[0].Rows[0]["TASKS_REF_NO"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Created_Dt_IN", dsInternalData.Tables[0].Rows[0]["CREATED_DATE"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("LAST_MODIFIED_BY_FK_IN", dsInternalData.Tables[0].Rows[0]["M_CREATED_BY_FK"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Activity_IN", dsInternalData.Tables[0].Rows[0]["ACTIVITY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Activity_Desc_IN", dsInternalData.Tables[0].Rows[0]["DESCRIPTION"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Prev_Activity_IN", dsInternalData.Tables[0].Rows[0]["PREVIOUS_ACTIVITY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Next_Activity_IN", dsInternalData.Tables[0].Rows[0]["NEXT_ACTIVITY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Wf_Rules_Ext_Priority_IN", dsInternalData.Tables[0].Rows[0]["PRIORITY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("Active_IN", dsInternalData.Tables[0].Rows[0]["ACTIVE"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("MANDATORY_IN", dsInternalData.Tables[0].Rows[0]["MANDATORY"]).Direction = ParameterDirection.Input;
                insInternalDetails.Parameters.Add("VERSION_NO_IN", dsInternalData.Tables[0].Rows[0]["VERSION_NO"]).Direction = ParameterDirection.Input;
                insInternalDetails.ExecuteNonQuery();
                if (arrMessage.Count > 0)
                {
                    INSERT.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    INSERT.Commit();
                    return arrMessage;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "SaveManual"

        #region "Fetch Grid"

        /// <summary>
        /// Fetches the GRD.
        /// </summary>
        /// <param name="ActivityPK">The activity pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchGrd(int ActivityPK, Int32 CurrentPage, Int32 TotalPage)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with12 = objWF.MyDataAdapter;
                _with12.SelectCommand = new OracleCommand();
                _with12.SelectCommand.Connection = objWF.MyConnection;
                _with12.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_WORKFLOW_RULES_PKG.FETCH_GIRD";
                _with12.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with12.SelectCommand.Parameters.Add("ACTIVITY_MST_IN", (ActivityPK == -1 ? 0 : ActivityPK)).Direction = ParameterDirection.Input;
                _with12.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with12.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = M_MasterPageSize;
                _with12.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with12.SelectCommand.Parameters.Add("GRD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with12.Fill(ds);
                TotalPage = Convert.ToInt32(_with12.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(_with12.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);

                return ds;
            }
            catch (Exception ex)
            {
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #endregion "Fetch Grid"

        #region "Fetch Supervisors"

        /// <summary>
        /// Fetches the wf supervisors.
        /// </summary>
        /// <param name="WorkFlowRuleIntMstPk">The work flow rule int MST pk.</param>
        /// <param name="LocationMstFk">The location MST fk.</param>
        /// <param name="UserFks">The user FKS.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchWFSupervisors(int WorkFlowRuleIntMstPk, int LocationMstFk, string UserFks = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            DataSet ds = new DataSet();
            WorkFlow objwf = new WorkFlow();
            try
            {
                objwf.OpenConnection();
                objwf.MyCommand = new OracleCommand();
                var _with13 = objwf.MyCommand;
                _with13.Connection = objwf.MyConnection;
                _with13.CommandType = CommandType.StoredProcedure;
                _with13.CommandText = objwf.MyUserName + ".FETCH_WORKFLOW_RULES_PKG.FETCH_WF_SUPERVISORS";
                _with13.Parameters.Clear();
                _with13.Parameters.Add("WF_RULES_INT_MST_FK_IN", WorkFlowRuleIntMstPk).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("LOCATION_MST_FK_IN", LocationMstFk).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("USER_MST_FKS_IN", (string.IsNullOrEmpty(UserFks.Trim()) | UserFks.Trim() == "0" ? "" : UserFks.Trim())).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("MASTER_PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.InputOutput;
                _with13.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with13.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with13.Parameters.Add("GRD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objwf.MyDataAdapter = new OracleDataAdapter(objwf.MyCommand);
                objwf.MyDataAdapter.Fill(ds);
                TotalPage = Convert.ToInt32(objwf.MyCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objwf.MyCommand.Parameters["CURRENT_PAGE_IN"].Value);
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objwf.CloseConnection();
            }
            return ds;
        }

        #endregion "Fetch Supervisors"

        #region "Delete WF Rules"

        /// <summary>
        /// Deletes the wf rules.
        /// </summary>
        /// <param name="DelPks">The delete PKS.</param>
        /// <returns></returns>
        public ArrayList DelWFRules(string DelPks)
        {
            string Sql = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                arrMessage.Clear();
                Sql = " DELETE FROM WORKFLOW_RULES_INT_APPL_TBL WF WHERE WF.WORKFLOW_RULES_INT_CONFIG_FK IN (" + DelPks + ")";

                if (objWF.ExecuteCommands(Sql) == true)
                {
                    Sql = " DELETE FROM WORKFLOW_RULES_INT_CONFIG_TBL WCF WHERE WCF.WF_RULES_INT_CONFIG_PK IN (" + DelPks + ")";
                    if (objWF.ExecuteCommands(Sql) == true)
                    {
                        arrMessage.Add("All Data Saved Successfully");
                    }
                    else
                    {
                        arrMessage.Add("Error while deleting the Records");
                    }
                }
                else
                {
                    arrMessage.Add("Error while deleting the Records");
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new ArrayList();
        }

        #endregion "Delete WF Rules"

        #region "Update Active Status"

        /// <summary>
        /// Updates the active status.
        /// </summary>
        /// <param name="ds">The ds.</param>
        /// <returns></returns>
        public ArrayList UpdateActiveStatus(DataSet ds)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            OracleCommand updCommand = new OracleCommand();
            Int32 RecAfct = default(Int32);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            int i = 0;
            try
            {
                if ((ds != null))
                {
                    for (i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                    {
                        var _with14 = updCommand;
                        _with14.Transaction = TRAN;
                        _with14.Connection = objWK.MyConnection;
                        _with14.CommandType = CommandType.StoredProcedure;
                        _with14.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.UPDATE_ACTIVE_STATUS";
                        var _with15 = _with14.Parameters;
                        _with15.Clear();
                        updCommand.Parameters.Add("CONFIG_MST_FK_IN", ds.Tables[0].Rows[i]["CONFIG_MST_FK"]).Direction = ParameterDirection.Input;
                        updCommand.Parameters.Add("ACTIVE_IN", ds.Tables[0].Rows[i]["ACTIVE"]).Direction = ParameterDirection.Input;

                        updCommand.ExecuteNonQuery();
                    }
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    TRAN.Commit();
                    return arrMessage;
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #endregion "Update Active Status"

        #region "Activity"

        /// <summary>
        /// Activities this instance.
        /// </summary>
        /// <returns></returns>
        public DataSet activity()
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            string strReturn = null;
            WorkFlow objWF = new WorkFlow();
            OracleDataReader dr = null;

            try
            {
                strQuery.Append(" select 0 WF_ACTIVITY_MST_TBL_PK,");
                strQuery.Append(" ' ' WF_ACTIVITY_NAME from dual");
                strQuery.Append(" union");
                strQuery.Append(" select WAMT.WF_ACTIVITY_MST_TBL_PK, ");
                strQuery.Append(" WAMT.WF_ACTIVITY_NAME ");
                strQuery.Append(" from WF_ACTIVITY_MST_TBL WAMT ");
                strQuery.Append(" where WAMT.wf_activity_mst_tbl_pk<>0");
                if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 1)
                {
                    strQuery.Append("      and wamt.biz_type in (1,3)");
                }
                else if ((Int16)HttpContext.Current.Session["BIZ_TYPE"] == 2)
                {
                    strQuery.Append("      and wamt.biz_type in (2,3)");
                }
                strQuery.Append(" order by WF_ACTIVITY_NAME ");

                //strQuery.Append("")
                //strQuery.Append("   SELECT '<ALL>' WF_ACTIVITY_NAME, ")
                //strQuery.Append("       0 WF_ACTIVITY_MST_TBL_PK ")
                //strQuery.Append("  FROM DUAL ")
                //strQuery.Append("UNION ")
                //strQuery.Append(" select WAMT.WF_ACTIVITY_NAME, ")
                //strQuery.Append("        WAMT.WF_ACTIVITY_MST_TBL_PK")
                //strQuery.Append("       from WF_ACTIVITY_MST_TBL WAMT,WORKFLOW_RULES_INT_MST_TBL WRMT ")
                //strQuery.Append("       where WAMT.wf_activity_mst_tbl_pk<>0  AND WRMT.WF_RULES_INT_ACTIVITY = WAMT.WF_ACTIVITY_MST_TBL_PK")

                //If CType(HttpContext.Current.Session("BIZ_TYPE"), Int16) = 1 Then
                //    strQuery.Append("      and wamt.biz_type in (1,3)")
                //ElseIf CType(HttpContext.Current.Session("BIZ_TYPE"), Int16) = 2 Then
                //    strQuery.Append("      and wamt.biz_type in (2,3)")
                //End If
                //strQuery.Append("  order by WF_ACTIVITY_NAME ")

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
    }
}