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
using System.Linq;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_wfmgrruleconfig : CommonFeatures
    {
        /// <summary>
        /// The _ pk configuration
        /// </summary>
        private long _PkConfig;

        #region "FetchApp"

        /// <summary>
        /// Fetches the application.
        /// </summary>
        /// <param name="IntPK">The int pk.</param>
        /// <param name="CntryPK">The cntry pk.</param>
        /// <param name="LocPK">The loc pk.</param>
        /// <returns></returns>
        public DataSet FetchApp(Int64 IntPK, int CntryPK = 0, int LocPK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL += " SELECT rownum SLNO, Q.* FROM(select";
            strSQL += "  0 WF_RULES_INT_APPL_PK,'1' SEL,";
            strSQL += "  CMT.COUNTRY_NAME,'..' USERID,";
            strSQL += "  CMT.COUNTRY_MST_PK,";
            if (LocPK == 0)
            {
                strSQL += "  (select rowtocol('select w.workflow_rules_int_loc_fk from Workflow_rules_int_appl_tbl w ";
                strSQL += "   where w.workflow_rules_int_mst_fk=" + IntPK;
                strSQL += "  and w.workflow_rules_int_country=' || wria.WORKFLOW_RULES_INT_COUNTRY) from dual) as WORKFLOW_RULES_INT_LOC_FK";
            }
            else
            {
                strSQL += "'" + Convert.ToString(LocPK) + "' as  WORKFLOW_RULES_INT_LOC_FK";
            }
            strSQL += "  from Workflow_rules_int_appl_tbl WRIA,";
            strSQL += "  location_mst_tbl LMT,country_mst_tbl CMT";
            strSQL += " where WRIA.WORKFLOW_RULES_INT_COUNTRY=CMT.COUNTRY_MST_PK";
            strSQL += " and LMT.COUNTRY_MST_FK= CMT.COUNTRY_MST_PK";
            strSQL += " and WRIA.WORKFLOW_RULES_INT_MST_FK=" + IntPK;
            strSQL += " and WRIA.WORKFLOW_RULES_INT_COUNTRY=" + CntryPK;
            strSQL += " UNION";
            strSQL += " select 0 WF_RULES_INT_APPL_PK, '0' sel,";
            strSQL += " CMT.COUNTRY_NAME,'..' USERID,CMT.COUNTRY_MST_PK,'0'WORKFLOW_RULES_INT_LOC_FK";
            strSQL += " FROM country_mst_tbl CMT";
            strSQL += " WHERE 1 = 1";
            strSQL += " and cmt.country_mst_pk not in (select WRIA.WORKFLOW_RULES_INT_COUNTRY from Workflow_rules_int_appl_tbl WRIA where WRIA.WORKFLOW_RULES_INT_MST_FK=" + IntPK;
            strSQL += " and WRIA.WORKFLOW_RULES_INT_COUNTRY=" + CntryPK;
            strSQL += " ) AND cmt.active_flag = 1";
            strSQL += " ORDER BY sel desc,COUNTRY_NAME asc ) q";
            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "FetchApp"

        #region "FetchApp"

        /// <summary>
        /// Fetches the application ext.
        /// </summary>
        /// <param name="IntPK">The int pk.</param>
        /// <returns></returns>
        public DataSet FetchAppExt(Int64 IntPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL += " SELECT rownum slno, Q.* FROM(select";
            strSQL += "  WRIA.WF_RULES_EXT_APPL_PK,'1' Sel,";
            strSQL += "  CMT.COUNTRY_NAME,'..' USERID,";
            strSQL += "  CMT.COUNTRY_MST_PK,WRIA.WORKFLOW_RULES_EXT_LOC_FK";
            strSQL += "  from Workflow_rules_Ext_appl_tbl WRIA,";
            strSQL += "  location_mst_tbl LMT,country_mst_tbl CMT";
            strSQL += " where WRIA.WORKFLOW_RULES_EXT_COUNTRY=CMT.COUNTRY_MST_PK";
            strSQL += "and LMT.COUNTRY_MST_FK= CMT.COUNTRY_MST_PK";
            strSQL += "and WRIA.WORKFLOW_RULES_EXT_MST_FK=" + IntPK;
            strSQL += "UNION";
            strSQL += "select 0 WF_RULES_EXT_APPL_PK, '0' sel,";
            strSQL += "CMT.COUNTRY_NAME,'..' USERID,CMT.COUNTRY_MST_PK,'0'WORKFLOW_RULES_EXT_LOC_FK";
            strSQL += "FROM country_mst_tbl CMT";
            strSQL += "WHERE 1 = 1";
            strSQL += "and cmt.country_mst_pk not in (select WRIA.WORKFLOW_RULES_EXT_COUNTRY from Workflow_rules_Ext_appl_tbl WRIA where WRIA.WORKFLOW_RULES_EXT_MST_FK=" + IntPK;
            strSQL += ") AND cmt.active_flag = 1";
            strSQL += "ORDER BY sel desc,COUNTRY_NAME asc ) q";
            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "FetchApp"

        #region "FetchAlert"

        /// <summary>
        /// Fetches the alert.
        /// </summary>
        /// <param name="IntPK">The int pk.</param>
        /// <param name="CntryPK">The cntry pk.</param>
        /// <param name="LocPK">The loc pk.</param>
        /// <returns></returns>
        public DataTable FetchAlert(Int64 IntPK, int CntryPK = 0, int LocPK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL += " select WRIA.wf_rules_int_config_pk,WRIA.WF_RULES_INT_MST_FK ,WRIA.WF_RULES_INT_ALERT_TYPE,WRIA.WF_RULES_INT_ALERT_FREQ,";
            strSQL += "  WRIA.WF_RULES_INT_ALERT_FMODE,WRIA.WF_RULES_INT_DEADLINE,WRIA.WF_RULES_INT_DEADLINE_MODE,";
            strSQL += "  WRIA.WORKFLOW_RULES_INT_OVERDUE,WRIA.WF_RULES_INT_OVERDUE_MODE,WRIA.WF_RULES_INT_AUTO_ESC,";
            strSQL += "  WRIA.WF_RULES_INT_AUTO_ESC_TO,WRIA.WF_RULES_INT_AUTO_ESC_AFTER,WRIA.WF_RULES_INT_AUTO_ESC_MODE,";
            strSQL += "  wf_rules_int_deadline_hours, wf_rules_int_deadline_mins, wf_rules_int_overdue_hours, wf_rules_int_overdue_mins, wf_rules_int_alert_fmode_hours, wf_rules_int_alert_fmode_mins, wf_rules_int_auto_esc_hours, wf_rules_int_auto_esc_mins,WRIA.weekends";
            strSQL += "  from WORKFLOW_RULES_INT_CONFIG_TBL WRIA";
            strSQL += "  where WRIA.WF_RULES_INT_MST_FK =" + IntPK;
            strSQL += "  and wria.wf_rules_int_config_pk in(Select WORKFLOW_RULES_INT_APPL_TBL.WORKFLOW_RULES_INT_CONFIG_FK";
            strSQL += "  FROM WORKFLOW_RULES_INT_APPL_TBL";
            strSQL += "  WHERE WORKFLOW_RULES_INT_MST_FK =" + IntPK;
            strSQL += "  AND WORKFLOW_RULES_INT_COUNTRY = " + CntryPK;
            strSQL += "  AND WORKFLOW_RULES_INT_LOC_FK ='" + LocPK + "'";
            strSQL += ")";
            try
            {
                return objWF.GetDataTable(strSQL);
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

        #endregion "FetchAlert"

        #region "FetchAlertExt"

        /// <summary>
        /// Fetches the alert ext.
        /// </summary>
        /// <param name="IntPK">The int pk.</param>
        /// <returns></returns>
        public DataTable FetchAlertExt(Int64 IntPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL += " select WRIA.wf_rules_ext_config_pk,WRIA.WF_RULES_EXT_MST_FK ,WRIA.WF_RULES_EXT_ALERT_TYPE,WRIA.WF_RULES_EXT_ALERT_FREQ,";
            strSQL += "  WRIA.WF_RULES_EXT_ALERT_FMODE,WRIA.WF_RULES_EXT_DEADLINE,WRIA.WF_RULES_EXT_DEADLINE_MODE,";
            strSQL += "  WRIA.WORKFLOW_RULES_EXT_OVERDUE,WRIA.WF_RULES_EXT_OVERDUE_MODE,WRIA.WF_RULES_EXT_AUTO_ESC,";
            strSQL += "  WRIA.WF_RULES_EXT_AUTO_ESC_TO,WRIA.WF_RULES_EXT_AUTO_ESC_AFTER,WRIA.WF_RULES_EXT_AUTO_ESC_MODE";
            strSQL += "  from WORKFLOW_RULES_EXT_CONFIG_TBL WRIA";
            strSQL += "  where WRIA.WF_RULES_EXT_MST_FK =" + IntPK;
            try
            {
                return objWF.GetDataTable(strSQL);
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

        #endregion "FetchAlertExt"

        #region "Save"

        /// <summary>
        /// Saves the configuration ext new.
        /// </summary>
        /// <param name="IntFK">The int fk.</param>
        /// <param name="dsInternalData">The ds internal data.</param>
        /// <param name="dsApp">The ds application.</param>
        /// <param name="WEEKENDS_IN">The weekend s_ in.</param>
        /// <returns></returns>
        public ArrayList SaveConfigExtNew(Int64 IntFK, DataSet dsInternalData, DataSet dsApp, string WEEKENDS_IN)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction INSERT = null;
            OracleCommand Insconfig = new OracleCommand();
            objWK.OpenConnection();
            INSERT = objWK.MyConnection.BeginTransaction();
            try
            {
                var _with1 = Insconfig;
                _with1.Transaction = INSERT;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.INS_CONFIG";

                var _with2 = _with1.Parameters;
                _with2.Clear();
                //Insconfig.Parameters.Add("WF_RULES_INT_CONFIG_PK_IN", dsInternalData.Tables(0).Rows(0).Item("CONFIG_PK")).Direction = ParameterDirection.Input
                Insconfig.Parameters.Add("WF_RULES_EXT_MST_FK_IN", IntFK).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WF_RULES_EXT_ALERT_TYPE_IN", dsInternalData.Tables[0].Rows[0]["ALERT_TYPE"]).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WF_RULES_EXT_ALERT_FREQ_IN", getDefault(dsInternalData.Tables[0].Rows[0]["ALERT_FREQUENCY"], 0)).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WF_RULES_EXT_ALERT_FMODE_IN", dsInternalData.Tables[0].Rows[0]["ALERT_FREQUENCY_MODE"]).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WF_RULES_EXT_DEADLINE_IN", getDefault(dsInternalData.Tables[0].Rows[0]["DEADLINE"], 0)).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WF_RULES_EXT_DEADLINE_MODE_IN", dsInternalData.Tables[0].Rows[0]["DEADLINE_IN"]).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WORKFLOW_RULES_EXT_OVERDUE_IN", getDefault(dsInternalData.Tables[0].Rows[0]["OVERDUE_PERIOD"], 0)).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WF_RULES_EXT_OVERDUE_MODE_IN", dsInternalData.Tables[0].Rows[0]["OVERDUE_IN"]).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WF_RULES_EXT_AUTO_ESC_IN", dsInternalData.Tables[0].Rows[0]["ALERT_ESCALATE"]).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WF_RULES_EXT_AUTO_ESC_TO_IN", dsInternalData.Tables[0].Rows[0]["ESCALATE"]).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WF_RULES_EXT_AUTO_ESC_AFTER_IN", dsInternalData.Tables[0].Rows[0]["ESCALATE_AFTER"]).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WF_RULES_EXT_AUTO_ESC_MODE_IN", dsInternalData.Tables[0].Rows[0]["ESCALATE_MODE"]).Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("WEEKENDS_IN", OracleDbType.Varchar2, 20, "WEEKENDS_IN").Direction = ParameterDirection.Input;
                Insconfig.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;

                arrMessage.Add(Convert.ToString(Insconfig.Parameters["RETURN_VALUE"].Value));

                Insconfig.ExecuteNonQuery();
                _PkConfig = Convert.ToInt64(_with1.Parameters["RETURN_VALUE"].Value);
                arrMessage = SaveAppExt(dsApp, _PkConfig, IntFK, Insconfig);
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
                objWK.CloseConnection();
            }
        }

        #endregion "Save"

        #region "SaveAppExt"

        /// <summary>
        /// Saves the application ext.
        /// </summary>
        /// <param name="dsApp">The ds application.</param>
        /// <param name="ConfigPk">The configuration pk.</param>
        /// <param name="IntPk">The int pk.</param>
        /// <param name="Insconfig">The insconfig.</param>
        /// <returns></returns>
        public ArrayList SaveAppExt(DataSet dsApp, long ConfigPk, long IntPk, OracleCommand Insconfig)
        {
            Int32 nRowCnt = default(Int32);

            WorkFlow objWK = new WorkFlow();

            string UserName = objWK.MyUserName;
            arrMessage.Clear();
            string[] LocPKs = null;
            int i = 0;
            try
            {
                var _with3 = Insconfig;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.INS_APPEXT";
                for (nRowCnt = 0; nRowCnt <= dsApp.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    LocPKs = dsApp.Tables[0].Rows[nRowCnt]["WF_RULES_INT_LOC_FK"].ToString().Split(',');
                    for (i = 0; i <= LocPKs.Count() - 1; i++)
                    {
                        _with3.Parameters.Clear();
                        Insconfig.Parameters.Add("WF_RULES_EXT_MST_FK_IN", IntPk).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_EXT_Config_Fk_IN", ConfigPk).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_EXT_Loc_Fk_IN", LocPKs[i]).Direction = ParameterDirection.Input;
                        Insconfig.Parameters["WF_RULES_EXT_Loc_Fk_IN"].SourceVersion = DataRowVersion.Current;
                        Insconfig.Parameters.Add("WF_RULES_EXT_COUNTRY_IN", getDefault(dsApp.Tables[0].Rows[nRowCnt]["WF_RULES_INT_COUNTRY"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters["WF_RULES_EXT_COUNTRY_IN"].SourceVersion = DataRowVersion.Current;
                        Insconfig.ExecuteNonQuery();
                    }
                }

                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        #endregion "SaveAppExt"

        #region "SaveConfigExt"

        /// <summary>
        /// Saves the configuration ext.
        /// </summary>
        /// <param name="IntFK">The int fk.</param>
        /// <param name="dsInternalData">The ds internal data.</param>
        /// <param name="dsApp">The ds application.</param>
        /// <param name="WEEKENDS_IN">The weekend s_ in.</param>
        /// <returns></returns>
        public ArrayList SaveConfigExt(Int64 IntFK, DataSet dsInternalData, DataSet dsApp, string WEEKENDS_IN)
        {
            string count = null;
            string sql = null;
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand update = new OracleCommand();
            OracleCommand delete = new OracleCommand();
            arrMessage.Clear();

            try
            {
                sql = " UPDATE WORKFLOW_RULES_EXT_CONFIG_TBL set ";
                sql += " WF_RULES_EXT_ALERT_TYPE='" + dsInternalData.Tables[0].Rows[0]["ALERT_TYPE"] + "',";
                sql += " WF_RULES_EXT_ALERT_FREQ='" + dsInternalData.Tables[0].Rows[0]["ALERT_FREQUENCY"] + "',";
                sql += " WF_RULES_EXT_ALERT_FMODE='" + dsInternalData.Tables[0].Rows[0]["ALERT_FREQUENCY_MODE"] + "',";
                sql += " WF_RULES_EXT_DEADLINE='" + dsInternalData.Tables[0].Rows[0]["DEADLINE"] + "',";
                sql += " WF_RULES_EXT_DEADLINE_MODE='" + dsInternalData.Tables[0].Rows[0]["DEADLINE_IN"] + "',";
                sql += " WORKFLOW_RULES_EXT_OVERDUE='" + dsInternalData.Tables[0].Rows[0]["OVERDUE_PERIOD"] + "',";
                sql += " WF_RULES_EXT_OVERDUE_MODE='" + dsInternalData.Tables[0].Rows[0]["OVERDUE_IN"] + "',";
                sql += " WF_RULES_EXT_AUTO_ESC='" + dsInternalData.Tables[0].Rows[0]["ALERT_ESCALATE"] + "',";
                sql += " WF_RULES_EXT_AUTO_ESC_TO='" + dsInternalData.Tables[0].Rows[0]["ESCALATE"] + "',";
                sql += " WF_RULES_EXT_AUTO_ESC_AFTER='" + dsInternalData.Tables[0].Rows[0]["ESCALATE_AFTER"] + "',";
                sql += " WF_RULES_EXT_AUTO_ESC_MODE='" + dsInternalData.Tables[0].Rows[0]["ESCALATE_MODE"] + "'";
                sql += " where wf_rules_ext_mst_fk='" + IntFK + "'";
                var _with4 = update;
                _with4.Transaction = TRAN;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = sql;
                count = Convert.ToString(update.ExecuteNonQuery());
                sql = " Delete from Workflow_rules_ext_appl_tbl where WORKFLOW_RULES_EXT_MST_FK='" + IntFK + "'";
                var _with5 = delete;
                _with5.Transaction = TRAN;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = sql;
                delete.ExecuteNonQuery();

                for (nRowCnt = 0; nRowCnt <= dsApp.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    sql = " INSERT into Workflow_rules_ext_appl_tbl(";
                    sql += " WF_RULES_EXT_APPL_PK,WORKFLOW_RULES_EXT_MST_FK,WORKFLOW_RULES_EXT_CONFIG_FK,";
                    sql += " WORKFLOW_RULES_EXT_LOC_FK,WORKFLOW_RULES_EXT_COUNTRY)";
                    sql += " values (";
                    sql += " SEQ_EXTERNAL_APP_TBL.NEXTVAL ,";
                    sql += " '" + IntFK + "',";
                    sql += " '" + dsInternalData.Tables[0].Rows[0]["CONFIG_PK"] + "',";
                    sql += " '" + dsApp.Tables[0].Rows[nRowCnt]["WF_RULES_INT_LOC_FK"] + "',";
                    sql += " '" + dsApp.Tables[0].Rows[nRowCnt]["WF_RULES_INT_COUNTRY"] + "')";
                    var _with6 = update;
                    _with6.Transaction = TRAN;
                    _with6.Connection = objWK.MyConnection;
                    _with6.CommandType = CommandType.StoredProcedure;
                    _with6.CommandText = sql;
                    count = Convert.ToString(update.ExecuteNonQuery());
                }
                if (Convert.ToInt32(count) > 0)
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

        #endregion "SaveConfigExt"

        #region "SaveConfig"

        /// <summary>
        /// Saves the configuration.
        /// </summary>
        /// <param name="IntFK">The int fk.</param>
        /// <param name="dsInternalData">The ds internal data.</param>
        /// <param name="dsApp">The ds application.</param>
        /// <param name="WEEKENDS_IN">The weekend s_ in.</param>
        /// <returns></returns>
        public ArrayList SaveConfig(Int64 IntFK, DataSet dsInternalData, DataSet dsApp, string WEEKENDS_IN)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction INSERT = null;
            OracleCommand Insconfig = new OracleCommand();
            objWK.OpenConnection();
            INSERT = objWK.MyConnection.BeginTransaction();
            string[] LocPKs = null;
            try
            {
                if ((dsInternalData.Tables[0].Rows[0]["CONFIG_PK"] == null))
                {
                    dsInternalData.Tables[0].Rows[0]["CONFIG_PK"] = 0;
                }
                for (int nRowCnt = 0; nRowCnt <= dsApp.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    LocPKs = dsApp.Tables[0].Rows[nRowCnt]["WF_RULES_INT_LOC_FK"].ToString().Split(',');
                    for (int i = 0; i <= LocPKs.Count() - 1; i++)
                    {
                        var _with7 = Insconfig;
                        _with7.Transaction = INSERT;
                        _with7.Connection = objWK.MyConnection;
                        _with7.CommandType = CommandType.StoredProcedure;
                        _with7.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.UPD_CONFIG";

                        var _with8 = _with7.Parameters;
                        _with8.Clear();
                        Insconfig.Parameters.Add("WF_RULES_INT_CONFIG_PK_IN", dsInternalData.Tables[0].Rows[0]["CONFIG_PK"]).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_MST_FK_IN", IntFK).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_ALERT_TYPE_IN", dsInternalData.Tables[0].Rows[0]["ALERT_TYPE"]).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_ALERT_FREQ_IN", getDefault(dsInternalData.Tables[0].Rows[0]["ALERT_FREQUENCY"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_ALERT_FMODE_IN", dsInternalData.Tables[0].Rows[0]["ALERT_FREQUENCY_MODE"]).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_DEADLINE_IN", getDefault(dsInternalData.Tables[0].Rows[0]["DEADLINE"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_DEADLINE_MODE_IN", dsInternalData.Tables[0].Rows[0]["DEADLINE_IN"]).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WORKFLOW_RULES_INT_OVERDUE_IN", getDefault(dsInternalData.Tables[0].Rows[0]["OVERDUE_PERIOD"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_OVERDUE_MODE_IN", dsInternalData.Tables[0].Rows[0]["OVERDUE_IN"]).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_AUTO_ESC_IN", dsInternalData.Tables[0].Rows[0]["ALERT_ESCALATE"]).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_AUTO_ESC_TO_IN", dsInternalData.Tables[0].Rows[0]["ESCALATE"]).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("ESCALATE_AFTER_IN", dsInternalData.Tables[0].Rows[0]["ESCALATE_AFTER"]).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("ESCALATE_MODE_IN", dsInternalData.Tables[0].Rows[0]["ESCALATE_MODE"]).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_LOC_FK_IN", LocPKs[i]).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_COUNTRY_IN", getDefault(dsApp.Tables[0].Rows[nRowCnt]["WF_RULES_INT_COUNTRY"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_DEADLINE_HRS_IN", getDefault(dsInternalData.Tables[0].Rows[0]["DEADLINE_HOURS"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_DEADLINE_MINS_IN", getDefault(dsInternalData.Tables[0].Rows[0]["DEADLINE_MINS"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_OVERDUE_HRS_IN", getDefault(dsInternalData.Tables[0].Rows[0]["OVERDUE_HOURS"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_OVERDUE_MINS_IN", getDefault(dsInternalData.Tables[0].Rows[0]["OVERDUE_MINS"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_FMODE_HRS_IN", getDefault(dsInternalData.Tables[0].Rows[0]["FMODE_HOURS"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_FMODE_MINS_IN", getDefault(dsInternalData.Tables[0].Rows[0]["FMODE_MINS"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_ESC_HOURS_IN", getDefault(dsInternalData.Tables[0].Rows[0]["ESC_HOURS"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WF_RULES_INT_ESC_MINS_IN", getDefault(dsInternalData.Tables[0].Rows[0]["ESC_MINS"], 0)).Direction = ParameterDirection.Input;
                        Insconfig.Parameters.Add("WEEKENDS_IN", WEEKENDS_IN).Direction = ParameterDirection.Input;
                        Insconfig.ExecuteNonQuery();
                    }
                }
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
                objWK.CloseConnection();
            }
        }

        #endregion "SaveConfig"

        #region "SaveApp"

        /// <summary>
        /// Saves the application.
        /// </summary>
        /// <param name="dsApp">The ds application.</param>
        /// <param name="ConfigPk">The configuration pk.</param>
        /// <param name="IntPk">The int pk.</param>
        /// <param name="Insconfig">The insconfig.</param>
        /// <returns></returns>
        public ArrayList SaveApp(DataSet dsApp, long ConfigPk, long IntPk, OracleCommand Insconfig)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string UserName = objWK.MyUserName;
            arrMessage.Clear();

            try
            {
                var _with9 = Insconfig;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_ENTRY_PKG.INS_APP";
                for (nRowCnt = 0; nRowCnt <= dsApp.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    _with9.Parameters.Clear();
                    Insconfig.Parameters.Add("WF_RULES_INT_MST_FK_IN", IntPk).Direction = ParameterDirection.Input;
                    Insconfig.Parameters.Add("WF_RULES_INT_Config_Fk_IN", ConfigPk).Direction = ParameterDirection.Input;
                    Insconfig.Parameters.Add("WF_RULES_INT_Loc_Fk_IN", dsApp.Tables[0].Rows[nRowCnt]["WF_RULES_INT_LOC_FK"]).Direction = ParameterDirection.Input;
                    Insconfig.Parameters["WF_RULES_INT_Loc_Fk_IN"].SourceVersion = DataRowVersion.Current;
                    Insconfig.Parameters.Add("WF_RULES_INT_COUNTRY_IN", getDefault(dsApp.Tables[0].Rows[nRowCnt]["WF_RULES_INT_COUNTRY"], 0)).Direction = ParameterDirection.Input;
                    Insconfig.Parameters["WF_RULES_INT_COUNTRY_IN"].SourceVersion = DataRowVersion.Current;
                    Insconfig.ExecuteNonQuery();
                }

                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        #endregion "SaveApp"

        #region "Fetch WeekDeatisl"

        /// <summary>
        /// Fetches the weekends.
        /// </summary>
        /// <param name="Pk_Value">The PK_ value.</param>
        /// <returns></returns>
        public DataSet FetchWeekends(string Pk_Value)
        {
            string StrSQL = null;
            try
            {
                StrSQL = string.Empty;
                StrSQL += " SELECT T.WEEKENDS FROM WORKFLOW_RULES_INT_CONFIG_TBL T where t.wf_rules_int_config_pk = " + Pk_Value;
                WorkFlow objWf = new WorkFlow();
                return objWf.GetDataSet(StrSQL);
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Fetch WeekDeatisl"
    }
}