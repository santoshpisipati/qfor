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

using Oracle.DataAccess.Client;
using System;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Net;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsApplicationSetup : CommonFeatures
    {
        #region "APPLICATIONS"

        /// <summary>
        /// Gets the application header settings.
        /// </summary>
        /// <param name="APPLICATION_MST_PK">The applicatio n_ ms t_ pk.</param>
        /// <param name="APPLICATION_NAME">Name of the applicatio n_.</param>
        /// <param name="ACTIVE">The active.</param>
        /// <returns></returns>
        public DataSet GetAppHeaderSettings(int APPLICATION_MST_PK = 0, string APPLICATION_NAME = "", short ACTIVE = -1)
        {
            WorkFlow objwf = new WorkFlow();
            //objwf.MyCommand.Parameters.Clear()
            var _with1 = objwf.MyCommand;
            _with1.Parameters.Clear();
            _with1.Parameters.Add("APPLICATION_MST_PK_IN", (APPLICATION_MST_PK == 0 ? 0 : APPLICATION_MST_PK)).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("APPLICATION_NAME_IN", (string.IsNullOrEmpty(APPLICATION_NAME) ? "" : APPLICATION_NAME)).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("ACTIVE_IN", (ACTIVE == 0 | ACTIVE == 1 ? ACTIVE : 0)).Direction = ParameterDirection.Input;
            _with1.Parameters.Add("APP_SETTINGS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
            return objwf.GetDataSet("QFSI_APPLICATION_SETTINGS_PKG", "FETCH_APPLICATIONS");
        }

        /// <summary>
        /// Savs the e_ ap p_ setting.
        /// </summary>
        /// <param name="APPLICATION_NAME">Name of the applicatio n_.</param>
        /// <param name="SERVER_NAME">Name of the serve r_.</param>
        /// <param name="DB_NAME">Name of the d b_.</param>
        /// <param name="DB_PASSWORD">The d b_ password.</param>
        /// <param name="ENCRYPTED">The encrypted.</param>
        /// <param name="CONNECTION_STRING">The connectio n_ string.</param>
        /// <param name="MAIL_SERVER">The mai l_ server.</param>
        /// <param name="MAIL_USERNAME">The mai l_ username.</param>
        /// <param name="MAIL_PASSWORD">The mai l_ password.</param>
        /// <param name="ENCRYPTED_MAIL_PWD">The encrypte d_ mai l_ password.</param>
        /// <param name="MAIL_TO">The mai l_ to.</param>
        /// <param name="MAIL_CC">The mai l_ cc.</param>
        /// <param name="MAIL_BCC">The mai l_ BCC.</param>
        /// <param name="MAIL_START_FROM">The mai l_ star t_ from.</param>
        /// <param name="MAIL_INTERVAL">The mai l_ interval.</param>
        /// <param name="MAIL_INTERVAL_TYPE">Type of the mai l_ interva l_.</param>
        /// <param name="AGENT_AS">The agen t_ as.</param>
        /// <param name="COST_CATEGORY">The cos t_ category.</param>
        /// <param name="COST_CENTER">The cos t_ center.</param>
        /// <param name="USE_OPTIONAL">The us e_ optional.</param>
        /// <param name="EVENT_LOG_PATH">The even t_ lo g_ path.</param>
        /// <param name="ACTIVE">The active.</param>
        /// <param name="DB_USERNAME">The d b_ username.</param>
        /// <param name="APPLICATION_MST_PK">The applicatio n_ ms t_ pk.</param>
        /// <returns></returns>
        public string SAVE_APP_SETTING(string APPLICATION_NAME, string SERVER_NAME, string DB_NAME, string DB_PASSWORD, string ENCRYPTED, string CONNECTION_STRING, string MAIL_SERVER, string MAIL_USERNAME, string MAIL_PASSWORD, string ENCRYPTED_MAIL_PWD,
        string MAIL_TO, string MAIL_CC, string MAIL_BCC, string MAIL_START_FROM, string MAIL_INTERVAL, string MAIL_INTERVAL_TYPE, string AGENT_AS, string COST_CATEGORY, string COST_CENTER, string USE_OPTIONAL,
        string EVENT_LOG_PATH, string ACTIVE, string DB_USERNAME, int APPLICATION_MST_PK = 0)
        {
            string ErrorMsg = "";
            WorkFlow objwf = new WorkFlow();
            objwf.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objwf.MyConnection.BeginTransaction();
            try
            {
                var _with2 = objwf.MyCommand;
                _with2.Parameters.Clear();
                _with2.Transaction = TRAN;
                _with2.CommandType = CommandType.StoredProcedure;
                if (APPLICATION_MST_PK == 0)
                {
                    _with2.CommandText = objwf.MyUserName + ".QFSI_APPLICATION_SETTINGS_PKG.INSERT_APP_SETTING";
                    _with2.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with2.CommandText = objwf.MyUserName + ".QFSI_APPLICATION_SETTINGS_PKG.UPDATE_APP_SETTING";
                    _with2.Parameters.Add("APPLICATION_MST_PK_IN", APPLICATION_MST_PK).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                }
                _with2.Parameters.Add("APPLICATION_NAME_IN", APPLICATION_NAME).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("SERVER_NAME_IN", SERVER_NAME).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("DB_NAME_IN", DB_NAME).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("DB_PASSWORD_IN", DB_PASSWORD).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("ENCRYPTED_IN", ENCRYPTED).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("CONNECTION_STRING_IN", CONNECTION_STRING).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("MAIL_SERVER_IN", MAIL_SERVER).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("MAIL_USERNAME_IN", MAIL_USERNAME).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("MAIL_PASSWORD_IN", MAIL_PASSWORD).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("ENCRYPTED_MAIL_PWD_IN", ENCRYPTED_MAIL_PWD).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("MAIL_TO_IN", MAIL_TO).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("MAIL_CC_IN", (string.IsNullOrEmpty(MAIL_CC.ToString().Trim()) ? "" : MAIL_CC)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("MAIL_BCC_IN", (string.IsNullOrEmpty(MAIL_BCC.ToString().Trim()) ? "" : MAIL_BCC)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("MAIL_START_FROM_IN", MAIL_START_FROM).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("MAIL_INTERVAL_IN", MAIL_INTERVAL).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("MAIL_INTERVAL_TYPE_IN", MAIL_INTERVAL_TYPE).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("AGENT_AS_IN", AGENT_AS).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("COST_CATEGORY_IN", COST_CATEGORY).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("COST_CENTER_IN", COST_CENTER).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("USE_OPTIONAL_IN", USE_OPTIONAL).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("EVENT_LOG_PATH_IN", EVENT_LOG_PATH).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("ACTIVE_IN", ACTIVE).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("DB_USERNAME_IN", DB_USERNAME).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("FINANCE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                _with2.ExecuteNonQuery();
                if (APPLICATION_MST_PK == 0)
                    APPLICATION_MST_PK = Convert.ToInt32(_with2.Parameters["RETURN_VALUE"].Value);
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
                objwf.CloseConnection();
            }
            return ErrorMsg;
        }

        #endregion "APPLICATIONS"

        #region "Finance"

        /// <summary>
        /// Gets the finance setup.
        /// </summary>
        /// <param name="FINANCE_MST_PK">The financ e_ ms t_ pk.</param>
        /// <param name="FINANCE_APP_NAME">Name of the financ e_ ap p_.</param>
        /// <param name="ACTIVE">The active.</param>
        /// <returns></returns>
        public DataSet GetFinanceSetup(int FINANCE_MST_PK = 0, string FINANCE_APP_NAME = "", short ACTIVE = -1)
        {
            WorkFlow objwf = new WorkFlow();
            var _with3 = objwf.MyCommand;
            _with3.Parameters.Clear();
            _with3.Parameters.Add("FINANCE_MST_PK_IN", (FINANCE_MST_PK == 0 ? 0 : FINANCE_MST_PK)).Direction = ParameterDirection.Input;
            _with3.Parameters.Add("FINANCE_APP_NAME_IN", (string.IsNullOrEmpty(FINANCE_APP_NAME) ? "" : FINANCE_APP_NAME)).Direction = ParameterDirection.Input;
            _with3.Parameters.Add("ACTIVE_IN", (ACTIVE == 0 | ACTIVE == 1 ? ACTIVE : 0)).Direction = ParameterDirection.Input;
            _with3.Parameters.Add("APP_SETTINGS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
            return objwf.GetDataSet("QFSI_APPLICATION_SETTINGS_PKG", "FETCH_FINANCE_SETUP");
        }

        /// <summary>
        /// Savs the e_ fi n_ setting.
        /// </summary>
        /// <param name="dsAppSetup">The ds application setup.</param>
        /// <returns></returns>
        public string SAVE_FIN_SETTING(DataSet dsAppSetup)
        {
            string ErrorMsg = "";
            WorkFlow objwf = new WorkFlow();
            objwf.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objwf.MyConnection.BeginTransaction();
            try
            {
                foreach (DataRow row in dsAppSetup.Tables[0].Rows)
                {
                    int FINANCE_MST_PK = 0;
                    try
                    {
                        FINANCE_MST_PK = Convert.ToInt32(row["FINANCE_MST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        FINANCE_MST_PK = 0;
                    }
                    var _with4 = objwf.MyCommand;
                    _with4.Parameters.Clear();
                    _with4.Transaction = TRAN;
                    _with4.CommandType = CommandType.StoredProcedure;
                    if (FINANCE_MST_PK == 0)
                    {
                        _with4.CommandText = objwf.MyUserName + ".QFSI_APPLICATION_SETTINGS_PKG.INSERT_FIN_SETTING";
                        _with4.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with4.CommandText = objwf.MyUserName + ".QFSI_APPLICATION_SETTINGS_PKG.UPDATE_FIN_SETTING";
                        _with4.Parameters.Add("FINANCE_MST_PK_IN", FINANCE_MST_PK).Direction = ParameterDirection.Input;
                        _with4.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    }
                    _with4.Parameters.Add("FINANCE_APP_NAME_IN", row["FINANCE_APP_NAME"]).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("SERVER_IP_IN", row["SERVER_IP"]).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("PORT_NR_IN", row["PORT_NR"]).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("ACTIVE_IN", (string.IsNullOrEmpty(row["ACTIVE"].ToString()) ? (FINANCE_MST_PK == 0 ? 1 : 0) : row["ACTIVE"])).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("REMARKS_IN", row["REMARKS"]).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    _with4.ExecuteNonQuery();
                    if (FINANCE_MST_PK == 0)
                    {
                        FINANCE_MST_PK = Convert.ToInt32(_with4.Parameters["RETURN_VALUE"].Value);
                        row["FINANCE_MST_PK"] = FINANCE_MST_PK;
                    }
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
                objwf.CloseConnection();
            }
            return ErrorMsg;
        }

        #endregion "Finance"

        #region "Mappings"

        /// <summary>
        /// Fetches the mappings.
        /// </summary>
        /// <param name="APPLICATION_MST_PK">The applicatio n_ ms t_ pk.</param>
        /// <param name="FINANCE_MST_PK">The financ e_ ms t_ pk.</param>
        /// <returns></returns>
        public DataSet FetchMappings(int APPLICATION_MST_PK = 0, int FINANCE_MST_PK = 0)
        {
            WorkFlow objwf = new WorkFlow();
            var _with5 = objwf.MyCommand;
            _with5.Parameters.Clear();
            _with5.Parameters.Add("APPLICATION_MST_PK_IN", (APPLICATION_MST_PK == 0 ? 0 : APPLICATION_MST_PK)).Direction = ParameterDirection.Input;
            _with5.Parameters.Add("FINANCE_MST_PK_IN", (FINANCE_MST_PK == 0 ? 0 : FINANCE_MST_PK)).Direction = ParameterDirection.Input;
            _with5.Parameters.Add("APP_SETTINGS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
            return objwf.GetDataSet("QFSI_APPLICATION_SETTINGS_PKG", "FETCH_MAPPINGS");
        }

        /// <summary>
        /// Savs the e_ fi n_ mappings.
        /// </summary>
        /// <param name="dsAppSetup">The ds application setup.</param>
        /// <returns></returns>
        public string SAVE_FIN_MAPPINGS(DataSet dsAppSetup)
        {
            string ErrorMsg = "";
            WorkFlow objwf = new WorkFlow();
            objwf.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objwf.MyConnection.BeginTransaction();
            try
            {
                foreach (DataRow row in dsAppSetup.Tables[0].Rows)
                {
                    int APPLICATION_MST_PK = 0;
                    APPLICATION_MST_PK = Convert.ToInt32(row["APPLICATION_MST_PK"]);
                    var _with6 = objwf.MyCommand;
                    _with6.Parameters.Clear();
                    _with6.Transaction = TRAN;
                    _with6.CommandType = CommandType.StoredProcedure;
                    _with6.CommandText = objwf.MyUserName + ".QFSI_APPLICATION_SETTINGS_PKG.SAVE_FIN_MAPPINGS";
                    _with6.Parameters.Add("APPLICATION_MST_PK_IN", APPLICATION_MST_PK).Direction = ParameterDirection.Input;
                    _with6.Parameters.Add("FINANCE_MST_PK_IN", row["FINANCE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with6.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    _with6.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    _with6.ExecuteNonQuery();
                    if (APPLICATION_MST_PK == 0)
                    {
                        APPLICATION_MST_PK = Convert.ToInt32(_with6.Parameters["RETURN_VALUE"].Value);
                        row["APPLICATION_MST_PK"] = APPLICATION_MST_PK;
                    }
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
                objwf.CloseConnection();
            }
            return ErrorMsg;
        }

        #endregion "Mappings"

        #region "SERVICE SCHEDULER"

        /// <summary>
        /// Fetches the schedule setup.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchScheduleSetup()
        {
            WorkFlow objwf = new WorkFlow();
            var _with7 = objwf.MyCommand;
            _with7.Parameters.Clear();
            _with7.Parameters.Add("APP_SETTINGS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
            return objwf.GetDataSet("QFSI_APPLICATION_SETTINGS_PKG", "FETCH_SERVICE_SCHEDULER");
        }

        /// <summary>
        /// Savs the e_ fi n_ schedule.
        /// </summary>
        /// <param name="dsScheduleSetup">The ds schedule setup.</param>
        /// <returns></returns>
        public string SAVE_FIN_SCHEDULE(DataSet dsScheduleSetup)
        {
            string ErrorMsg = "";
            WorkFlow objwf = new WorkFlow();
            objwf.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objwf.MyConnection.BeginTransaction();
            try
            {
                foreach (DataRow row in dsScheduleSetup.Tables[0].Rows)
                {
                    int FIN_SCHEDULER_PK = 0;
                    try
                    {
                        FIN_SCHEDULER_PK = Convert.ToInt32(row["FIN_SCHEDULER_SETUP_PK"]);
                    }
                    catch (Exception ex)
                    {
                        FIN_SCHEDULER_PK = 0;
                    }
                    var _with8 = objwf.MyCommand;
                    _with8.Parameters.Clear();
                    _with8.Transaction = TRAN;
                    _with8.CommandType = CommandType.StoredProcedure;
                    if (FIN_SCHEDULER_PK == 0)
                    {
                        _with8.CommandText = objwf.MyUserName + ".QFSI_APPLICATION_SETTINGS_PKG.INSERT_SERVICE_SCHEDULER";
                        _with8.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with8.CommandText = objwf.MyUserName + ".QFSI_APPLICATION_SETTINGS_PKG.UPDATE_SERVICE_SCHEDULER";
                        _with8.Parameters.Add("FIN_SCHEDULER_SETUP_PK_IN", FIN_SCHEDULER_PK).Direction = ParameterDirection.Input;
                        _with8.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                    }
                    _with8.Parameters.Add("SERVICE_INTERVAL_IN", row["SERVICE_INTERVAL"]).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("SERVICE_INTERVAL_TYPE_IN", row["SERVICE_INTERVAL_TYPE"]).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("ACTIVITY_LOG_PATH_IN", row["ACTIVITY_LOG_PATH"]).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("STORE_XML_IN", row["STORE_XML"]).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("XML_STORE_LOC_IN", row["XML_STORE_LOC"]).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("LAST_RUN_ON_IN", row["LAST_RUN_ON"]).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("NEXT_SCHEDULED_AT_IN", row["NEXT_SCHEDULED_AT"]).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    _with8.ExecuteNonQuery();
                    if (FIN_SCHEDULER_PK == 0)
                    {
                        FIN_SCHEDULER_PK = Convert.ToInt32(_with8.Parameters["RETURN_VALUE"].Value);
                        row["FIN_SCHEDULER_SETUP_PK"] = FIN_SCHEDULER_PK;
                    }
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
                objwf.CloseConnection();
            }
            return ErrorMsg;
        }

        #endregion "SERVICE SCHEDULER"

        #region "Cost Category Settings"

        /// <summary>
        /// Gets the cost category settings.
        /// </summary>
        /// <param name="APPLICATION_MST_PK">The applicatio n_ ms t_ pk.</param>
        /// <returns></returns>
        public DataSet GetCostCategorySettings(int APPLICATION_MST_PK = 0)
        {
            DataSet dsApp = new DataSet();
            dsApp = GetAppHeaderSettings(APPLICATION_MST_PK);
            WorkFlow objwf = new WorkFlow();
            string DB_NAME = Convert.ToString(dsApp.Tables[0].Rows[0]["DB_NAME"]);
            string PRODUCT_NAME = Convert.ToString(dsApp.Tables[0].Rows[0]["APPLICATION_NAME"]);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT CC.ACTIVE,");
            sb.Append("       0 FIN_APP_PK,");
            sb.Append("       CC.FIN_APP_LOC_FK FIN_APP_LOC_FK,");
            sb.Append("       QF.FIN_APP_NAME LOCATION_NAME,");
            sb.Append("       CC.FIN_APP_NAME,");
            sb.Append("       CC.FIN_APP_DESC,");
            sb.Append("       CC.REMARKS");
            sb.Append("  FROM " + DB_NAME + ".FIN_APP_COSTGROUP_MASTER CC, ");
            sb.Append("  " + DB_NAME + ".FIN_APP_LOC_MASTER QF ");
            sb.Append(" WHERE 1=1 ");
            sb.Append("   AND UPPER(CC.FIN_APP_PRODUCT) = '" + PRODUCT_NAME.ToUpper() + "'");
            sb.Append("   AND CC.FIN_APP_LOC_FK = QF.FIN_APP_FK ");
            sb.Append("   AND QF.FIN_APP_STATUS <> 1 ");

            return objwf.GetDataSet(sb.ToString());
            //With objwf.MyCommand
            //    .Parameters.Clear()
            //    .Parameters.Add("APPLICATION_MST_PK_IN", IIf(APPLICATION_MST_PK = 0, "", APPLICATION_MST_PK)).Direction = ParameterDirection.Input
            //    .Parameters.Add("APP_SETTINGS_CUR", OracleClient.OracleDbType.RefCursor).Direction = ParameterDirection.Output
            //End With
            //Return objwf.GetDataSet("QFSI_APPLICATION_SETTINGS_PKG", "FETCH_COST_CATEGORY")
        }

        /// <summary>
        /// Updats the e_ cos t_ ca t_ settings.
        /// </summary>
        /// <param name="APPLICATION_MST_PK">The applicatio n_ ms t_ pk.</param>
        /// <param name="DS_SETTINGS">The d s_ settings.</param>
        /// <returns></returns>
        public object UPDATE_COST_CAT_SETTINGS(int APPLICATION_MST_PK, DataSet DS_SETTINGS)
        {
            WorkFlow objwf = new WorkFlow();
            DataSet dsApp = new DataSet();
            dsApp = GetAppHeaderSettings(APPLICATION_MST_PK);
            string DB_NAME = Convert.ToString(dsApp.Tables[0].Rows[0]["DB_NAME"]);
            string PRODUCT_NAME = Convert.ToString(dsApp.Tables[0].Rows[0]["APPLICATION_NAME"]);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            foreach (DataRow ROW in DS_SETTINGS.Tables[0].Rows)
            {
                sb.Append("UPDATE " + DB_NAME + ".FIN_APP_COSTGROUP_MASTER CG");
                sb.Append("   SET CG.FIN_APP_NAME    = '" + ROW["FIN_APP_NAME"] + "',");
                sb.Append("       CG.FIN_APP_DESC    = '" + ROW["FIN_APP_DESC"] + "',");
                sb.Append("       CG.FIN_APP_PRODUCT = '" + PRODUCT_NAME + "',");
                sb.Append("       CG.REMARKS         = '" + ROW["REMARKS"] + "',");
                sb.Append("       CG.ACTIVE          = " + ROW["ACTIVE"]);
                sb.Append(" WHERE CG.FIN_APP_PK = " + ROW["FIN_APP_PK"]);
            }
            try
            {
                objwf.ExecuteCommands(sb.ToString());
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
            return "All Data Saved Successfully";
        }

        #endregion "Cost Category Settings"

        #region "Tally Response"

        /// <summary>
        /// Gets the list of companies.
        /// </summary>
        /// <param name="FinAppServerConStr">The fin application server con string.</param>
        /// <returns></returns>
        public DataSet GetListOfCompanies(string FinAppServerConStr)
        {
            string RequestXML = "<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER><BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>List of Companies</REPORTNAME></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>";
            return GetTallyResponse(RequestXML, FinAppServerConStr);
        }

        /// <summary>
        /// Gets the tally tables.
        /// </summary>
        /// <returns></returns>
        public DataSet GetTallyTables()
        {
            int i = 0;
            string query = "SELECT Company.`$_DBName`, Company.`$_ThisYearBeg`, Company.`$_ThisYearEnd`, Company.`$_PrevYearBeg`, Company.`$_PrevYearEnd`, Company.`$_ThisQuarterBeg`, Company.`$_ThisQuarterEnd`, Company.`$_PrevQuarterBeg`, Company.`$_PrevQuarterEnd`, Company.`$_Name`, Company.`$_Address1`, Company.`$_Address2`, Company.`$_Address3`, Company.`$_Address4`, Company.`$_Address5` FROM Techweb.TallyUser.Company Company ORDER BY Company.`$_DBName`";
            string source = "PORT=9000;DSN=TallyODBC64_9000;DRIVER=Tally ODBC Driver64;SERVER={(local)}";
            OdbcConnection con = new OdbcConnection(source);
            con.Open();
            OdbcCommand cmd = new OdbcCommand(query, con);
            OdbcDataAdapter DataAdp = new OdbcDataAdapter(cmd);
            var ds = new DataSet();
            DataSet DsTbls = new DataSet();
            DataTable dt = new DataTable();
            dt = con.GetSchema("Tables");
            DsTbls.Tables.Add(dt);
            //For i = 0 To dt.Rows.Count - 1
            //    fetchTallyTableContent(dt.Rows(i).Item(2).ToString())
            //Next

            DataAdp.Fill(ds);
            return DsTbls;
        }

        /// <summary>
        /// Fetches the content of the tally table.
        /// </summary>
        /// <param name="tblName">Name of the table.</param>
        /// <returns></returns>
        public DataSet fetchTallyTableContent(string tblName)
        {
            string query = "SELECT * FROM Techweb.TallyUser." + tblName + " " + tblName;
            string source = "PORT=9000;DSN=TallyODBC64_9000;DRIVER=Tally ODBC Driver64;SERVER={(local)}";
            OdbcConnection con = new OdbcConnection(source);
            con.Open();
            OdbcCommand cmd = new OdbcCommand(query, con);
            OdbcDataAdapter DataAdp = new OdbcDataAdapter(cmd);
            DataSet ds = new DataSet();
            DataAdp.Fill(ds);
            return ds;
        }

        /// <summary>
        /// Gets the tally response.
        /// </summary>
        /// <param name="RequestXML">The request XML.</param>
        /// <param name="URL">The URL.</param>
        /// <returns></returns>
        public DataSet GetTallyResponse(string RequestXML, string URL)
        {
            WebRequest TallyRequest = null;
            byte[] byteArray = null;
            Stream dataStream = null;
            WebResponse response = null;
            StreamReader reader = null;
            string responseFromTallyServer = null;
            DataSet TallyResponseDataSet = new DataSet();
            try
            {
                TallyRequest = WebRequest.Create(URL);
                ((HttpWebRequest)TallyRequest).UserAgent = ".NET Framework Example Client";
                TallyRequest.Method = "POST";
                byteArray = Encoding.UTF8.GetBytes(RequestXML);
                TallyRequest.ContentType = "application/x-www-form-urlencoded";
                TallyRequest.ContentLength = byteArray.Length;
                dataStream = TallyRequest.GetRequestStream();
                dataStream.Write(byteArray, 0, byteArray.Length);
                dataStream.Close();
                response = TallyRequest.GetResponse();
                dataStream = response.GetResponseStream();
                reader = new StreamReader(dataStream);
                responseFromTallyServer = reader.ReadToEnd().ToString();
                TallyResponseDataSet.ReadXml(new StringReader(responseFromTallyServer));
                reader.Close();
                dataStream.Close();
                response.Close();
                byteArray = null;
                response = null;
                responseFromTallyServer = null;
                dataStream = null;
                return TallyResponseDataSet;
            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        #endregion "Tally Response"

        #region "Schedular Log"

        /// <summary>
        /// Fetches the schedular log.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchSchedularLog()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT FSST.LAST_RUN_ON, FSST.NEXT_SCHEDULED_AT");
                sb.Append("  FROM FIN_SCHEDULER_SETUP_TBL FSST");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Schedular Log"
    }
}