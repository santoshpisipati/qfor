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
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Scheduler : CommonFeatures
    {
        #region "Drop Down Values"

        /// <summary>
        /// Fetches the drop down.
        /// </summary>
        /// <param name="ConfigId">The configuration identifier.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchDropDown(string ConfigId, string Flag = "")
        {
            string strSQL = null;
            strSQL = strSQL + " SELECT ";
            strSQL = strSQL + " DD_VALUE,";
            strSQL = strSQL + " DD_ID";
            strSQL = strSQL + " FROM QFOR_DROP_DOWN_TBL";
            strSQL = strSQL + " WHERE CONFIG_ID='" + ConfigId + "'";
            if (!string.IsNullOrEmpty(Flag))
            {
                strSQL = strSQL + " AND DD_FLAG='" + Flag + "'";
            }
            strSQL = strSQL + " ORDER BY DD_VALUE ";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Drop Down Values"

        #region "Service Scheduler Fetch"

        /// <summary>
        /// Fetches the schedule setup.
        /// </summary>
        /// <param name="APPLICATION_MST_PK">The applicatio n_ ms t_ pk.</param>
        /// <param name="APPLICATION_NAME">Name of the applicatio n_.</param>
        /// <param name="ACTIVE">The active.</param>
        /// <returns></returns>
        public DataSet FetchScheduleSetup(int APPLICATION_MST_PK = 0, string APPLICATION_NAME = "", short ACTIVE = -1)
        {
            WorkFlow objWF = new WorkFlow();
            var _with1 = objWF.MyCommand;
            _with1.Parameters.Clear();
            _with1.Parameters.Add("APPLICATION_MST_PK_IN", (APPLICATION_MST_PK == 0 ? 0 : APPLICATION_MST_PK)).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("APPLICATION_NAME_IN", (string.IsNullOrEmpty(APPLICATION_NAME) ? "" : APPLICATION_NAME)).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("ACTIVE_IN", (ACTIVE == 0 | ACTIVE == 1 ? ACTIVE : Int16.MinValue)).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("APP_SETTINGS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
            return objWF.GetDataSet("QFSI_APPLICATION_SETTINGS_PKG", "FETCH_APPLICATIONS");
        }

        #endregion "Service Scheduler Fetch"

        #region "Service Scheduler Event Vier"

        /// <summary>
        /// Fetches the SCH event viewer.
        /// </summary>
        /// <param name="EvtBy">The evt by.</param>
        /// <param name="EvtStatus">The evt status.</param>
        /// <param name="User_PK">The user_ pk.</param>
        /// <param name="dtValidFrom">The dt valid from.</param>
        /// <param name="dtValidTo">The dt valid to.</param>
        /// <param name="Flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="From_Flg">The from_ FLG.</param>
        /// <returns></returns>
        public DataSet FetchSchEventViewer(Int32 EvtBy, Int32 EvtStatus, Int32 User_PK, string dtValidFrom, string dtValidTo, Int32 Flag, Int32 CurrentPage, Int32 TotalPage, int From_Flg = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with2 = objWF.MyDataAdapter;
                _with2.SelectCommand = new OracleCommand();
                _with2.SelectCommand.Connection = objWF.MyConnection;
                _with2.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_EVENT_VIEWER_PKG.FETCH_EVENT_VIEWER";
                _with2.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with2.SelectCommand.Parameters.Add("EVTBY_IN", (EvtBy == 0 ? 0 : EvtBy)).Direction = ParameterDirection.Input;
                _with2.SelectCommand.Parameters.Add("EVTSTATUS_IN", (EvtStatus == 0 ? 0 : EvtStatus)).Direction = ParameterDirection.Input;
                _with2.SelectCommand.Parameters.Add("USER_PK_IN", (User_PK == 0 ? 0 : User_PK)).Direction = ParameterDirection.Input;
                _with2.SelectCommand.Parameters.Add("DTVALIDFROM_IN", (string.IsNullOrEmpty(dtValidFrom) ? "" : dtValidFrom)).Direction = ParameterDirection.Input;
                _with2.SelectCommand.Parameters.Add("DTVALIDTO_IN", (string.IsNullOrEmpty(dtValidTo) ? "" : dtValidTo)).Direction = ParameterDirection.Input;
                _with2.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with2.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = M_MasterPageSize;
                _with2.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with2.SelectCommand.Parameters.Add("FLAG_IN", Flag).Direction = ParameterDirection.Input;
                _with2.SelectCommand.Parameters.Add("FROM_FLAG_IN", From_Flg).Direction = ParameterDirection.Input;
                _with2.SelectCommand.Parameters.Add("CUR_OUT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Fill(ds);
            }
            catch (Exception ex)
            {
            }
            finally
            {
                objWF.CloseConnection();
            }
            return ds;
        }

        #endregion "Service Scheduler Event Vier"

        #region "save record"

        /// <summary>
        /// Savs the e_ fi n_ schedule.
        /// </summary>
        /// <param name="dsScheduleSetup">The ds schedule setup.</param>
        /// <returns></returns>
        public string SAVE_FIN_SCHEDULE(DataSet dsScheduleSetup)
        {
            string ErrorMsg = "";
            WorkFlow objWF = new WorkFlow();
            objWF.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWF.MyConnection.BeginTransaction();
            try
            {
                foreach (DataRow row in dsScheduleSetup.Tables[0].Rows)
                {
                    var _with3 = objWF.MyCommand;
                    _with3.Parameters.Clear();
                    _with3.Transaction = TRAN;
                    _with3.CommandType = CommandType.StoredProcedure;
                    if (Convert.ToInt32(row["APPLICATION_MST_PK"]) == 0)
                    {
                        _with3.CommandText = objWF.MyUserName + ".QFSI_APPLICATION_SETTINGS_PKG.INSERT_APP_SETTING";
                        _with3.Parameters.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with3.CommandText = objWF.MyUserName + ".QFSI_APPLICATION_SETTINGS_PKG.UPDATE_APP_SETTING";
                        _with3.Parameters.Add("APPLICATION_MST_PK_IN", row["APPLICATION_MST_PK"]).Direction = ParameterDirection.Input;
                        _with3.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    }
                    _with3.Parameters.Add("RECORD_UPDATE_IN", row["RECORD_UPDATE"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("SCHEDULE_TYPE_IN", row["SCHEDULE_TYPE"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("RUN_EVERY_IN", row["RUN_EVERY"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("CHK_REPEAT_IN", row["CHK_REPEAT"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("REPEAT_IN", row["REPEAT"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("REPEAT_TYPE_IN", row["REPEAT_TYPE"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("START_TIME_IN", row["START_TIME"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("NEXT_SCHEDULE_IN", row["NEXT_SCHEDULE"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("MAIL_TO_IN", row["MAIL_TO"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("MAIL_CC_IN", row["MAIL_CC"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("AGENT_AS_IN", row["AGENT_AS"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("COST_CATEGORY_IN", row["COST_CATEGORY"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("COST_CENTER_IN", row["COST_CENTER"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("USE_OPTIONAL_IN", row["USE_OPTIONAL"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("EVENT_LOG_PATH_IN", row["EVENT_LOG_PATH"]).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    _with3.ExecuteNonQuery();
                }
                TRAN.Commit();
                ErrorMsg = "All Data Saved Successfully";
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                ErrorMsg = ex.Message;
            }
            finally
            {
                objWF.CloseConnection();
            }
            return ErrorMsg;
        }

        #endregion "save record"

        #region "Fetch Activity Log"

        /// <summary>
        /// Fetches the activity log.
        /// </summary>
        /// <param name="ChkOnLoad">The CHK on load.</param>
        /// <param name="SearchFlag">The search flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="RecType">Type of the record.</param>
        /// <param name="Category">The category.</param>
        /// <param name="UpdateType">Type of the update.</param>
        /// <param name="frmDt">The FRM dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="trnType">Type of the TRN.</param>
        /// <param name="RefNr">The reference nr.</param>
        /// <param name="Desc">The desc.</param>
        /// <param name="ExportFlg">The export FLG.</param>
        /// <returns></returns>
        public DataSet FetchActivityLog(Int16 ChkOnLoad, Int16 SearchFlag, Int16 CurrentPage, Int16 TotalPage, Int16 RecType = 0, Int16 Category = 0, Int16 UpdateType = 0, string frmDt = "", string ToDt = "", string trnType = "",
        string RefNr = "", string Desc = "", int ExportFlg = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();

            try
            {
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("REC_TYPE_IN", RecType).Direction = ParameterDirection.Input;
                _with4.Add("CATEGORY_IN", Category).Direction = ParameterDirection.Input;
                _with4.Add("UPDATE_TYPE_IN", UpdateType).Direction = ParameterDirection.Input;
                _with4.Add("FROM_DT_IN", (string.IsNullOrEmpty(frmDt) ? null : frmDt)).Direction = ParameterDirection.Input;
                _with4.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? null : ToDt)).Direction = ParameterDirection.Input;
                _with4.Add("TRN_TYPE_IN", (string.IsNullOrEmpty(trnType) ? null : trnType)).Direction = ParameterDirection.Input;
                _with4.Add("REF_IN", (string.IsNullOrEmpty(RefNr) ? null : RefNr)).Direction = ParameterDirection.Input;
                _with4.Add("ERROR_DESC_IN", (string.IsNullOrEmpty(Desc) ? null : Desc)).Direction = ParameterDirection.Input;
                _with4.Add("ONLOAD_IN", ChkOnLoad).Direction = ParameterDirection.Input;
                _with4.Add("SEARCH_FLG_IN", SearchFlag).Direction = ParameterDirection.Input;
                _with4.Add("EXPORT_FLG_IN", ExportFlg).Direction = ParameterDirection.Input;
                _with4.Add("PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with4.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with4.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with4.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("ACTIVITY_LOG_PKG", "FETCH_ACTIVITY_LOG");
                TotalPage = Convert.ToInt16(objWF.MyCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt16(objWF.MyCommand.Parameters["CURRENT_PAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt16(objWF.MyCommand.Parameters["CURRENT_PAGE_IN"].Value);
                }
                return DS;
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

        #endregion "Fetch Activity Log"

        #region "Get Scheduler Push Type"

        /// <summary>
        /// Gets the type of the scheduler push.
        /// </summary>
        /// <returns></returns>
        public bool GetSchedulerPushType()
        {
            string strSql = null;
            Int16 RecCount = default(Int16);
            WorkFlow objWF = new WorkFlow();
            DataSet ds = null;
            strSql = " SELECT COUNT(*) FROM APPLICATION_MST_TBL AMT";
            try
            {
                RecCount = Convert.ToInt16(objWF.ExecuteScaler(strSql));
                if (RecCount > 0)
                {
                    strSql = " SELECT RECORD_UPDATE FROM APPLICATION_MST_TBL AMT";
                    ds = objWF.GetDataSet(strSql);
                    if (Convert.ToInt16(ds.Tables[0].Rows[0]["RECORD_UPDATE"]) == 0)
                    {
                        //SaveEventViewer(1, 1)
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                SaveEventViewer(1, 2);
                throw sqlExp;
                return false;
            }
            catch (Exception exp)
            {
                SaveEventViewer(1, 2);
                ErrorMessage = exp.Message;
                throw exp;
                return false;
            }
        }

        #endregion "Get Scheduler Push Type"

        #region "Save Event Viewer"

        /// <summary>
        /// Saves the event viewer.
        /// </summary>
        /// <param name="Events">The events.</param>
        /// <param name="EventStatus">The event status.</param>
        public void SaveEventViewer(Int32 Events, Int32 EventStatus)
        {
            WorkFlow objwf = new WorkFlow();
            objwf.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objwf.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            try
            {
                var _with5 = insCommand;
                _with5.Connection = objwf.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objwf.MyUserName + ".FETCH_EVENT_VIEWER_PKG.EVENT_VIEWER_INS";
                _with5.Parameters.Clear();
                _with5.Parameters.Add("E_EVENT_IN", Events).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("E_STATUS_IN", EventStatus).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("E_TYPE_IN", 1).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("E_USER_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "E_PK").Direction = ParameterDirection.Output;
                _with5.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with6 = objwf.MyDataAdapter;
                _with6.InsertCommand = insCommand;
                _with6.InsertCommand.Transaction = TRAN;
                RecAfct = _with6.InsertCommand.ExecuteNonQuery();

                if (RecAfct > 0)
                {
                    TRAN.Commit();
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (Exception OraExp)
            {
                throw OraExp;
            }
            finally
            {
                objwf.MyConnection.Close();
            }
        }

        #endregion "Save Event Viewer"
    }
}