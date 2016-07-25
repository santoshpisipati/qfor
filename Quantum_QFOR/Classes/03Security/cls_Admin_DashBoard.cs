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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Admin_DashBoard : CommonFeatures
    {
        //
        /// <summary>
        /// Gets the grid.
        /// </summary>
        /// <param name="Str">The string.</param>
        /// <param name="strDesc">The string desc.</param>
        /// <param name="dtpDate">The DTP date.</param>
        /// <param name="Modules">The modules.</param>
        /// <param name="Searchtype">The searchtype.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet GetGrid(Int32 Str, Int32 strDesc, string dtpDate, Int32 Modules, string Searchtype, string SortExpression = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string sql = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (Searchtype == "S")
            {
                if (Str > 0)
                {
                    strCondition = strCondition + "  and Login_Log.USER_MST_FK=" + Str;
                }
            }
            else
            {
                if (Str > 0)
                {
                    strCondition = strCondition + "  and Login_Log.USER_MST_FK=" + Str;
                }
            }

            if (Searchtype == "S")
            {
                if (strDesc > 0)
                {
                    strCondition = strCondition + " and Login_Log.LOCATION_MST_FK=" + strDesc;
                }
            }
            else
            {
                if (strDesc > 0)
                {
                    strCondition = strCondition + " and Login_Log.LOCATION_MST_FK=" + strDesc;
                }
            }

            if (dtpDate.Trim().Length > 0)
            {
                DateTime EnteredDate = default(DateTime);
                //EnteredDate = dtpDate;
                if (Searchtype == "S")
                {
                    strCondition = strCondition + " and TO_char(Login_Log.LOGGED_IN_TIME,'dd-Mon-yyyy') = '" + System.String.Format("{0:dd-MMM-yyyy}", EnteredDate) + "'";
                }
                else
                {
                    strCondition = strCondition + " and TO_char(Login_Log.LOGGED_IN_TIME,'dd-Mon-yyyy') = '" + System.String.Format("{0:dd-MMM-yyyy}", EnteredDate) + "'";
                }
            }
            else
            {
                DateTime EnteredDate = default(DateTime);
                EnteredDate = DateTime.Now;
                strCondition = strCondition + " and TO_char(Login_Log.LOGGED_IN_TIME,'dd-Mon-yyyy') = '" + System.String.Format("{0:dd-MMM-yyyy}", EnteredDate) + "'";
            }

            if (Searchtype == "S")
            {
                if (Modules > 0)
                {
                    strCondition = strCondition + " and Login_Log.MODULE_MENU_MST_FK=" + Modules;
                }
            }
            else
            {
                if (Modules > 0)
                {
                    strCondition = strCondition + " and Login_Log.MODULE_MENU_MST_FK=" + Modules;
                }
            }

            sql = "select Count(*) from Login_Log_Trn Login_Log where 1=1 ";
            sql += strCondition;

            //TotalRecords = (Int32)objWF.ExecuteScaler(sql);
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            sql = " select * from (";
            sql = sql + " SELECT ROWNUM SR_NO,q.* FROM ";
            sql = sql + " (select ";
            sql = sql + " Login_Log.LOGIN_LOG_TRN_PK,";
            sql = sql + " Login_Log.USER_MST_FK,";
            sql = sql + " USR.USER_ID,";
            sql = sql + " USR.USER_NAME,";
            sql = sql + " Login_Log.LOCATION_MST_FK,";
            sql = sql + " LOC.LOCATION_ID,";
            sql = sql + " Login_Log.CONFIG_MST_FK,";
            sql = sql + " Login_Log.MENU_MST_FK,";
            sql = sql + " MNU.MENU_ID,";
            sql = sql + " Login_Log.MODULE_MENU_MST_FK,";
            sql = sql + " Mod_menu.MENU_ID MODULES,";
            sql = sql + " Login_Log.LOGGED_IN_TIME,";
            sql = sql + " Login_Log.LOGGED_OUT_TIME";
            sql = sql + " from";
            sql = sql + " Login_Log_Trn Login_Log,";
            sql = sql + " User_Mst_Tbl USR,";
            sql = sql + " Location_Mst_Tbl LOC,";
            sql = sql + " Menu_Mst_Tbl MNU,";
            sql = sql + " menu_mst_tbl Mod_Menu";
            sql = sql + " where";
            sql = sql + " login_log.user_mst_fk = usr.user_mst_pk";
            sql = sql + " and login_log.location_mst_fk = loc.location_mst_pk";
            sql = sql + " and login_log.menu_mst_fk = mnu.menu_mst_pk(+)";
            sql = sql + " and login_log.module_menu_mst_fk = mod_menu.menu_mst_pk(+)";
            sql = sql + strCondition;
            sql = sql + " order by 3";
            sql = sql + " )q ) WHERE SR_NO  Between " + start + " and " + last;
            try
            {
                return objWF.GetDataSet(sql);
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

        /// <summary>
        /// Fetches the user.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchUser()
        {
            string StrSql = null;

            StrSql = "select 0  user_mst_pk,";
            StrSql = StrSql + " ' ' user_id from User_Mst_Tbl  UNION";
            StrSql = StrSql + " select  user_mst_pk,";
            StrSql = StrSql + " user_id from User_Mst_Tbl order by user_id ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(StrSql);
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

        /// <summary>
        /// Fetches the location.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchLocation()
        {
            string StrSql = null;

            StrSql = StrSql + " select 0  location_mst_pk,";
            StrSql = StrSql + " ' ' location_id from location_mst_tbl  UNION";
            StrSql = StrSql + " select  location_mst_pk,";
            StrSql = StrSql + " location_id from location_mst_tbl order by location_id ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(StrSql);
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

        /// <summary>
        /// Fetches the modules.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchModules()
        {
            string StrSql = null;

            StrSql = "select 0 MENU_MST_PK,";
            StrSql = StrSql + "' ' menu_id from Menu_Mst_Tbl where menu_level=1 UNION";
            StrSql = StrSql + "select MENU_MST_PK,";
            StrSql = StrSql + "menu_id from Menu_Mst_Tbl where menu_level=1 order by menu_id ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(StrSql);
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

        /// <summary>
        /// Fetches the report.
        /// </summary>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <returns></returns>
        public DataSet FetchReport(string FromDate = "", string ToDate = "")
        {
            DataSet MainDS1 = new DataSet();
            string Sql = null;

            DateTime FDate = default(DateTime);
            DateTime TDate = default(DateTime);
            //FDate = System.String.Format("{0:dd-MM-yyyy}", FromDate);
            //TDate = System.String.Format("{0:dd-MM-yyyy}", ToDate);

            Sql = "select STRN.SCHEDULER_TRN_PK,";
            Sql = Sql + "STRN.SCHEDULER_NO,";
            Sql = Sql + "STRN.SCHEDULE_DESCRIPTION,";
            Sql = Sql + "SDUR.SCHEDULE_DATE,";
            Sql = Sql + "SDUR.SCHEDULE_TIME,";
            Sql = Sql + "SDUR.SCHEDULE_NEXT_RUN,";
            Sql = Sql + "SDUR.SCHEDULE_NEXT_TIME,";
            Sql = Sql + "STRN.SCHEDULE_ACTIVE,";
            Sql = Sql + "STRN.SCHEDULE_FIRST_RUN,";
            Sql = Sql + "STRN.SCHEDULE_LAST_RUN";
            Sql = Sql + "from schedule_trn STRN,";
            Sql = Sql + "schedule_duration_trn SDUR";
            Sql = Sql + "where SDUR.SCHEDULE_TRN_FK = STRN.SCHEDULER_TRN_PK";
            //Sql = Sql & vbCrLf & "AND SDUR.SCHEDULE_DATE >= '" & System.String.Format("{0:dd-MM-yyyy}", FDate) & "'"
            //Sql = Sql & vbCrLf & "AND SDUR.SCHEDULE_DATE <= '" & System.String.Format("{0:dd-MM-yyyy}", TDate) & "'"
            Sql = Sql + "AND SDUR.SCHEDULE_DATE >= '" + FDate.ToString("dd-MMM-yyyy") + "'";
            //Sql = Sql & vbCrLf & "AND SDUR.SCHEDULE_DATE <= '" & Format(TDate, "dd-MMM-yyyy") & "'"

            try
            {
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(Sql);
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

        /// <summary>
        /// Fetches the login.
        /// </summary>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <returns></returns>
        public DataSet FetchLogin(string FromDate = "", string ToDate = "")
        {
            DataSet MainDS1 = new DataSet();
            string Sql = null;

            DateTime FDate = default(DateTime);
            DateTime TDate = default(DateTime);
            //FDate = System.String.Format("{0:dd-MM-yyyy}", FromDate);
            //TDate = System.String.Format("{0:dd-MM-yyyy}", ToDate);

            Sql = "select Login_Log.LOGIN_LOG_TRN_PK,";
            Sql = Sql + "Login_Log.USER_MST_FK,";
            Sql = Sql + "USR.USER_ID,";
            Sql = Sql + "USR.USER_NAME,";
            Sql = Sql + "LOC.LOCATION_ID,";
            Sql = Sql + "Login_Log.LOGGED_IN_TIME,";
            Sql = Sql + "Login_Log.LOGGED_OUT_TIME";
            Sql = Sql + "from Login_Log_Trn Login_Log,";
            Sql = Sql + "User_Mst_Tbl USR,";
            Sql = Sql + "Location_Mst_Tbl LOC,";
            Sql = Sql + "Menu_Mst_Tbl MNU,";
            Sql = Sql + "menu_mst_tbl Mod_Menu";
            Sql = Sql + "where login_log.user_mst_fk = usr.user_mst_pk";
            Sql = Sql + "and login_log.location_mst_fk = loc.location_mst_pk";
            Sql = Sql + "and login_log.menu_mst_fk = mnu.menu_mst_pk(+)";
            Sql = Sql + "and login_log.module_menu_mst_fk = mod_menu.menu_mst_pk(+)";
            Sql = Sql + "and Login_Log.LOGGED_IN_TIME >= '" + FDate.ToString("dd-MMM-yyyy") + "'";
            //Sql = Sql & vbCrLf & "and Login_Log.LOGGED_IN_TIME <= '" & Format(TDate, "dd-MMM-yyyy") & "'"

            try
            {
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(Sql);
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

        /// <summary>
        /// Fetches the program usage.
        /// </summary>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <returns></returns>
        public DataSet FetchProgramUsage(string FromDate = "", string ToDate = "")
        {
            DataSet MainDS1 = new DataSet();
            string Sql = null;

            DateTime FDate = default(DateTime);
            DateTime TDate = default(DateTime);
            //FDate = System.String.Format("{0:dd-MM-yyyy}", FromDate);
            //TDate = System.String.Format("{0:dd-MM-yyyy}", ToDate);

            Sql = "SELECT USER_ID,";
            Sql = Sql + "USER_NAME,";
            Sql = Sql + "MENU_ID,";
            Sql = Sql + "MENU_URL,";
            Sql = Sql + "No_Of_Runs ,";
            Sql = Sql + "max(ltrn.LOGGED_IN_TIME)LastRun ";
            Sql = Sql + "FROM";
            Sql = Sql + "(select";
            Sql = Sql + "USR.USER_ID,";
            Sql = Sql + "USR.USER_NAME,";
            Sql = Sql + "Login_Log.LOCATION_MST_FK,";
            Sql = Sql + "LOC.LOCATION_ID,";
            Sql = Sql + "MNU.MENU_ID,";
            Sql = Sql + "MNU.MENU_URL,";
            Sql = Sql + "Login_Log.MODULE_MENU_MST_FK,";
            Sql = Sql + "Mod_menu.MENU_ID MODULES,";
            Sql = Sql + "COUNT(*) No_Of_Runs ,";
            Sql = Sql + "login_log.menu_mst_fk";
            Sql = Sql + "from";
            Sql = Sql + "Login_Log_Trn Login_Log,";
            Sql = Sql + "User_Mst_Tbl USR,";
            Sql = Sql + "Location_Mst_Tbl LOC,";
            Sql = Sql + "Menu_Mst_Tbl MNU,";
            Sql = Sql + "menu_mst_tbl Mod_Menu";
            Sql = Sql + "where";
            Sql = Sql + "login_log.user_mst_fk = usr.user_mst_pk";
            Sql = Sql + "and login_log.location_mst_fk = loc.location_mst_pk";
            Sql = Sql + "and login_log.menu_mst_fk = mnu.menu_mst_pk(+)";
            Sql = Sql + "and login_log.module_menu_mst_fk = mod_menu.menu_mst_pk(+)";
            Sql = Sql + "and Login_Log.LOGGED_IN_TIME >='" + FDate.ToString("dd-MMM-yyyy") + "'";
            //Sql = Sql & vbCrLf & "and Login_Log.LOGGED_IN_TIME <='" & Format(TDate, "dd-MMM-yyyy") & "'"
            Sql = Sql + "GROUP BY";
            Sql = Sql + "USR.USER_ID,";
            Sql = Sql + "USR.USER_NAME,";
            Sql = Sql + "Login_Log.LOCATION_MST_FK,";
            Sql = Sql + "LOC.LOCATION_ID,";
            Sql = Sql + "MNU.MENU_ID,";
            Sql = Sql + "MNU.MENU_URL,";
            Sql = Sql + "Login_Log.MODULE_MENU_MST_FK,";
            Sql = Sql + "login_log.menu_mst_fk,";
            Sql = Sql + "Mod_menu.MENU_ID)q, ";
            Sql = Sql + "Login_Log_Trn ltrn";
            Sql = Sql + "WHERE";
            Sql = Sql + "q.menu_mst_fk =  ltrn.menu_mst_fk(+)";
            Sql = Sql + "GROUP BY";
            Sql = Sql + "USER_ID,";
            Sql = Sql + "USER_NAME,";
            Sql = Sql + "MENU_ID,";
            Sql = Sql + "MENU_URL,";
            Sql = Sql + "No_Of_Runs";

            try
            {
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(Sql);
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
    }
}