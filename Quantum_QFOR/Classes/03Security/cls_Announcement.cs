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

// ERROR: Not supported in C#: OptionDeclaration
using Oracle.ManagedDataAccess.Client;
using System;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Web;
using System.Xml;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Announcement : CommonFeatures
    {
        #region "Global Variable"

        /// <summary>
        /// The m_ SMT p_ server
        /// </summary>
        public string M_SMTP_SERVER = ConfigurationManager.AppSettings["MailServer"];

        /// <summary>
        /// The m_ sen d_ username
        /// </summary>
        public string M_SEND_USERNAME = ConfigurationManager.AppSettings["SEND_USERNAME"];

        /// <summary>
        /// The m_ sen d_ password
        /// </summary>
        public string M_SEND_PASSWORD = ConfigurationManager.AppSettings["SEND_PASSWORD"];

        /// <summary>
        /// The m_ mai l_ server
        /// </summary>
        public string M_MAIL_SERVER = ConfigurationManager.AppSettings["MailServer"];

        /// <summary>
        /// The m_ sen d_ name
        /// </summary>
        public string M_SEND_NAME = ConfigurationManager.AppSettings["CustSupportName"];

        /// <summary>
        /// The m_ ann identifier
        /// </summary>
        public static string M_AnnId = "";

        /// <summary>
        /// The create_ by
        /// </summary>
        public static string Create_By = "";

        /// <summary>
        /// The m_ name
        /// </summary>
        public static string M_Name = ConfigurationManager.AppSettings["SEND_USERNAME"];

        /// <summary>
        /// The m_ password
        /// </summary>
        public static string M_Password = ConfigurationManager.AppSettings["SEND_PASSWORD"];

        #endregion "Global Variable"

        /// <summary>
        /// The file name
        /// </summary>
        public static string FileName;

        #region "Check the length"

        /// <summary>
        /// Checks the length.
        /// </summary>
        /// <param name="Description">The description.</param>
        /// <returns></returns>
        public int checkLength(string Description)
        {
            WorkFlow objWK = new WorkFlow();
            string str = null;
            StringBuilder strsql = new StringBuilder();
            try
            {
                strsql.Append(" SELECT no_html('" + Description + "') FROM DUAL");
                Description = objWK.ExecuteScaler(strsql.ToString());
                strsql.Clear();
                strsql.Append(" SELECT GET_STRING_LENGTH('" + Description + "',3800) FROM DUAL");
                return Convert.ToInt32(objWK.ExecuteScaler(strsql.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
            }
            return 0;
        }

        #endregion "Check the length"

        #region "Fetch Information-Announcements"

        /// <summary>
        /// Accesses the rights.
        /// </summary>
        /// <param name="locpk">The locpk.</param>
        /// <param name="userpk">The userpk.</param>
        /// <returns></returns>
        public int AccessRights(int locpk, int userpk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            try
            {
                strsql.Append(" select users.allowed_operations_value from USER_ACCESS_TRN users where users.config_mst_fk in (4126) AND");
                strsql.Append(" users.user_mst_fk = '" + userpk + "' and users.location_mst_fk = '" + locpk + "' ");
                return Convert.ToInt32(objWK.ExecuteScaler(strsql.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
            }
            return 0;
        }

        /// <summary>
        /// Deletes the specified pk.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public int Del(int pk)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            try
            {
                strsql.Append(" select status from announcement_tbl ann where ann.announcement_pk= " + pk);
                return Convert.ToInt32(objWK.ExecuteScaler(strsql.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
            }
            return 0;
        }

        /// <summary>
        /// Fetches the announ.
        /// </summary>
        /// <param name="status">The status.</param>
        /// <param name="strsort">The strsort.</param>
        /// <param name="loca">The loca.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="isAdmin">if set to <c>true</c> [is admin].</param>
        /// <returns></returns>
        public DataTable FetchAnnoun(int status, string strsort, int loca, int flag = 1, bool isAdmin = false)
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;
            string strd1 = null;
            string strd3 = null;
            System.DateTime date1 = default(System.DateTime);
            System.DateTime date2 = default(System.DateTime);

            string strLogUser = Convert.ToString(HttpContext.Current.Session["USER_PK"]);
            switch ((flag))
            {
                case 1:
                    StrSql = "select ANNOUNCEMENT_PK,SUBJECT,locATION_FK,to_date(announcement_dt,dateformat)announcement_dt , PRIORITY, filenames ";
                    StrSql += "  from ANNOUNCEMENT_TBL where  ( to_date('" + DateTime.Today.Date + "','dd/mm/yyyy') between VALID_FROM and VALID_TO ) and status in (0,4) ";
                    if (isAdmin == false)
                    {
                        StrSql += "  AND  (INSTR(ANNOUNCEMENT_TBL.USERS_MST_FK,'" + strLogUser + "') > 0  OR  ANNOUNCEMENT_TBL.CREATED_BY_FK =  " + strLogUser + ")";
                    }
                    StrSql += "  order by announcement_dt desc";
                    break;

                case 2:
                    StrSql = "select SUBJECT,VALID_FROM,BODY,locATION_FK from ANNOUNCEMENT_TBL where STATUS IN (2)";
                    if (isAdmin == false)
                    {
                        StrSql += "  AND  (INSTR(ANNOUNCEMENT_TBL.USERS_MST_FK,'" + strLogUser + "') > 0  OR  ANNOUNCEMENT_TBL.CREATED_BY_FK =  " + strLogUser + ")";
                    }
                    StrSql += "   order by SUBJECT asc ";
                    break;

                case 4:
                    StrSql = "select ANNOUNCEMENT_PK,ANNOUNCEMENT_ID,SUBJECT,VALID_FROM,BODY,locATION_FK from ANNOUNCEMENT_TBL where STATUS IN ( " + status + ")";
                    if (isAdmin == false)
                    {
                        StrSql += "  AND  (INSTR(ANNOUNCEMENT_TBL.USERS_MST_FK,'" + strLogUser + "') > 0  OR  ANNOUNCEMENT_TBL.CREATED_BY_FK =  " + strLogUser + ")";
                    }
                    StrSql += "  order by " + strsort + " asc";
                    break;
            }
            try
            {
                return objWF.GetDataTable(StrSql);
            }
            catch (OracleException ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        /// <summary>
        /// Fetches the identifier.
        /// </summary>
        /// <param name="locid">The locid.</param>
        /// <param name="protocol">The protocol.</param>
        /// <returns></returns>
        public string FetchID(Int64 locid, string protocol)
        {
            string res1 = null;
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Parameters.Clear();
            var _with2 = objWK.MyCommand;
            _with2.CommandType = CommandType.StoredProcedure;
            _with2.CommandText = objWK.MyUserName + ".GENERATE_PROTOCOL_KEY_ANN";
            _with2.Parameters.Add("loc", locid).Direction = ParameterDirection.Input;
            _with2.Parameters.Add("p", protocol).Direction = ParameterDirection.Input;
            _with2.Parameters.Add("res", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
            try
            {
                _with2.ExecuteNonQuery();
                return Convert.ToString(_with2.Parameters["res"].Value);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the announ list.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <param name="loca">The loca.</param>
        /// <returns></returns>
        public DataTable FetchAnnounList(int pk, int loca)
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;
            StrSql = "select ANNOUNCEMENT_ID,ANNOUNCEMENT_DT,SUBJECT,BODY from ANNOUNCEMENT_TBL where ANNOUNCEMENT_PK=" + pk;
            try
            {
                return objWF.GetDataTable(StrSql);
            }
            catch (OracleException OraExp)
            {
                ErrorMessage = OraExp.Message;
                throw OraExp;
            }
        }

        /// <summary>
        /// Fetchlocations the type.
        /// </summary>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchlocationType(int loc)
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;

            StrSql = "select location_type_desc from location_type_mst_tbl where location_type_mst_pk=(";
            StrSql += "select location_type_fk from location_mst_tbl where location_mst_pk=" + loc + ")";

            try
            {
                return objWF.ExecuteScaler(StrSql);
                //
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Finds the location.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public int FindLocation(int pk)
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;
            StrSql = " select loc.location_mst_pk from location_mst_tbl loc where loc.location_mst_pk= ";
            StrSql += " (select u.default_location_fk from user_mst_tbl u where u.user_mst_pk = ";
            StrSql += "  (select ann.createdby_fk from announcement_tbl ann where ann.announcement_pk = " + pk + " ))";
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(StrSql));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Fetchlocations the name.
        /// </summary>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchlocationName(int loc)
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;
            StrSql = "select location_name from location_mst_tbl where location_mst_pk=" + loc;
            try
            {
                return objWF.ExecuteScaler(StrSql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        #region "Fetch"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="annpk">The annpk.</param>
        /// <param name="AnnType">Type of the ann.</param>
        /// <returns></returns>
        public DataTable FetchAll(int annpk, int AnnType)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (annpk != 0)
            {
                if (AnnType == 0)
                {
                    sb.Append("SELECT T.ANNOUNCEMENT_PK,");
                    sb.Append("       T.ANNOUNCEMENT_DT,");
                    sb.Append("       T.ANNOUNCEMENT_ID,");
                    sb.Append("       T.SUBJECT,");
                    sb.Append("       T.BODY,");
                    sb.Append("       T.LINK,");
                    sb.Append("       T.STATUS,");
                    sb.Append("       T.VALID_FROM,");
                    sb.Append("       T.VALID_TO,");
                    sb.Append("       T.TYPE_FLAG,");
                    sb.Append("       T.PRIORITY,");
                    sb.Append("       ROWTOCOL('SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L WHERE L.LOCATION_MST_PK IN (' ||");
                    sb.Append("                T.LOCATION_FK || ')') LOCATION_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT L.LOCATION_ID FROM LOCATION_MST_TBL L WHERE L.LOCATION_MST_PK IN (' ||");
                    sb.Append("                T.LOCATION_FK || ')') LOCATION_ID,");
                    sb.Append("       ROWTOCOL('SELECT L.LOCATION_NAME FROM LOCATION_MST_TBL L WHERE L.LOCATION_MST_PK IN (' ||");
                    sb.Append("                T.LOCATION_FK || ')') LOCATION_NAME,");

                    sb.Append("       ROWTOCOL('SELECT D.DEPARTMENT_MST_PK FROM DEPARTMENT_MST_TBL D WHERE D.DEPARTMENT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.DEPARTMENT_MST_FK, 0) || ')') DEPARTMENT_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT D.DEPARTMENT_ID FROM DEPARTMENT_MST_TBL D WHERE D.DEPARTMENT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.DEPARTMENT_MST_FK, 0) || ')') DEPARTMENT_ID,");
                    sb.Append("       ROWTOCOL('SELECT D.DEPARTMENT_NAME FROM DEPARTMENT_MST_TBL D WHERE D.DEPARTMENT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.DEPARTMENT_MST_FK, 0) || ')') DEPARTMENT_NAME,");

                    sb.Append("       ROWTOCOL('SELECT DM.DESIGNATION_MST_PK FROM DESIGNATION_MST_TBL DM WHERE DM.DESIGNATION_MST_PK IN (' ||");
                    sb.Append("                NVL(T.DESIGNATION_MST_FK, 0) || ')') DESIGNATION_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT DM.DESIGNATION_ID FROM DESIGNATION_MST_TBL DM WHERE DM.DESIGNATION_MST_PK IN (' ||");
                    sb.Append("                NVL(T.DESIGNATION_MST_FK, 0) || ')') DESIGNATION_ID,");
                    sb.Append("       ROWTOCOL('SELECT DM.DESIGNATION_NAME FROM DESIGNATION_MST_TBL DM WHERE DM.DESIGNATION_MST_PK IN (' ||");
                    sb.Append("                NVL(T.DESIGNATION_MST_FK, 0) || ')') DESIGNATION_NAME,");

                    sb.Append("       ROWTOCOL('SELECT  UM.USER_MST_PK FROM USER_MST_TBL UM WHERE UM.USER_MST_PK IN (' ||");
                    sb.Append("                NVL(T.USERS_MST_FK, 0) || ')') USER_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT  UM.USER_ID FROM USER_MST_TBL UM WHERE UM.USER_MST_PK IN (' ||");
                    sb.Append("                NVL(T.USERS_MST_FK, 0) || ')') USER_ID,");
                    sb.Append("       ROWTOCOL('SELECT  UM.USER_NAME FROM USER_MST_TBL UM WHERE UM.USER_MST_PK IN (' ||");
                    sb.Append("                NVL(T.USERS_MST_FK, 0) || ')') USER_NAME,");

                    sb.Append("       ROWTOCOL('SELECT  EM.EMPLOYEE_MST_PK FROM EMPLOYEE_MST_TBL EM WHERE EM.EMPLOYEE_MST_PK IN (' ||");
                    sb.Append("                NVL(T.MANAGEMENT_MST_FK, 0) || ')') EMPLOYEE_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT  EM.EMPLOYEE_ID FROM EMPLOYEE_MST_TBL EM WHERE EM.EMPLOYEE_MST_PK IN (' ||");
                    sb.Append("                NVL(T.MANAGEMENT_MST_FK, 0) || ')') EMPLOYEE_ID,");
                    sb.Append("       ROWTOCOL('SELECT  EM.EMPLOYEE_NAME FROM EMPLOYEE_MST_TBL EM WHERE EM.EMPLOYEE_MST_PK IN (' ||");
                    sb.Append("                NVL(T.MANAGEMENT_MST_FK, 0) || ')') EMPLOYEE_NAME");
                    sb.Append("  FROM ANNOUNCEMENT_TBL T");
                    sb.Append(" WHERE T.ANNOUNCEMENT_PK =" + annpk);
                    sb.Append("   AND T.TYPE_FLAG =" + AnnType);
                }
                else
                {
                    sb.Append("SELECT T.ANNOUNCEMENT_PK,");
                    sb.Append("       T.ANNOUNCEMENT_DT_EXT,");
                    sb.Append("       T.ANNOUNCEMENT_IDEXT,");
                    sb.Append("       T.SUBJECT,");
                    sb.Append("       T.BODY,");
                    sb.Append("       T.LINK,");
                    sb.Append("       T.STATUS_EXT,");
                    sb.Append("       T.VALID_FROM_EXT,");
                    sb.Append("       T.VALID_TO_EXT,");
                    sb.Append("       T.TYPE_FLAG,");
                    sb.Append("       T.PRIORITY_EXT,");
                    sb.Append("       T.SPECIFIC,");
                    sb.Append("       T.TO_MAIL,");
                    sb.Append("       T.CC_MAIL,");
                    sb.Append("       ROWTOCOL('SELECT AM.AREA_MST_PK FROM AREA_MST_TBL AM WHERE AM.AREA_MST_PK IN (' ||");
                    sb.Append("                NVL(T.AREA_MST_FK, 0) || ')') AREA_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT AM.AREA_ID FROM AREA_MST_TBL AM WHERE AM.AREA_MST_PK IN (' ||");
                    sb.Append("                NVL(T.AREA_MST_FK, 0) || ')') AREA_ID,");
                    sb.Append("       ROWTOCOL('SELECT AM.AREA_NAME FROM AREA_MST_TBL AM WHERE AM.AREA_MST_PK IN (' ||");
                    sb.Append("                NVL(T.AREA_MST_FK, 0) || ')') AREA_NAME,");

                    sb.Append("       ROWTOCOL('SELECT AG.AGENT_MST_PK FROM AGENT_MST_TBL AG WHERE AG.AGENT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.AGENT_MST_FK, 0) || ')') AGENT_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT AG.AGENT_ID FROM AGENT_MST_TBL AG WHERE AG.AGENT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.AGENT_MST_FK, 0) || ')') AGENT_ID,");
                    sb.Append("       ROWTOCOL('SELECT AG.AGENT_NAME FROM AGENT_MST_TBL AG WHERE AG.AGENT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.AGENT_MST_FK, 0) || ')') AGENT_NAME,");

                    sb.Append("       ROWTOCOL('SELECT V.VENDOR_MST_PK FROM VENDOR_MST_TBL V WHERE V.VENDOR_MST_PK IN (' ||");
                    sb.Append("                NVL(T.VENDOR_MST_FK, 0) || ')') VENDOR_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT V.VENDOR_ID FROM VENDOR_MST_TBL V WHERE V.VENDOR_MST_PK IN (' ||");
                    sb.Append("                NVL(T.VENDOR_MST_FK, 0) || ')') VENDOR_ID,");
                    sb.Append("       ROWTOCOL('SELECT V.VENDOR_NAME FROM VENDOR_MST_TBL V WHERE V.VENDOR_MST_PK IN (' ||");
                    sb.Append("                NVL(T.VENDOR_MST_FK, 0) || ')') VENDOR_NAME,");

                    sb.Append("       ROWTOCOL('SELECT RM.REGION_MST_PK FROM REGION_MST_TBL RM WHERE RM.REGION_MST_PK IN (' ||");
                    sb.Append("                NVL(T.REGION_MST_FK, 0) || ')') REGION_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT RM.REGION_CODE FROM REGION_MST_TBL RM WHERE RM.REGION_MST_PK IN (' ||");
                    sb.Append("                NVL(T.REGION_MST_FK, 0) || ')') REGION_ID,");
                    sb.Append("       ROWTOCOL('SELECT RM.REGION_NAME FROM REGION_MST_TBL RM WHERE RM.REGION_MST_PK IN (' ||");
                    sb.Append("                NVL(T.REGION_MST_FK, 0) || ')') REGION_NAME,");

                    sb.Append("       ROWTOCOL('SELECT SM.SECTOR_MST_PK FROM SECTOR_MST_TBL SM WHERE SM.SECTOR_MST_PK IN (' ||");
                    sb.Append("                NVL(T.SECTOR_MST_FK, 0) || ')') SECTOR_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT SM.SECTOR_ID FROM SECTOR_MST_TBL SM WHERE SM.SECTOR_MST_PK IN (' ||");
                    sb.Append("                NVL(T.SECTOR_MST_FK, 0) || ')') SECTOR_ID,");
                    sb.Append("       ROWTOCOL('SELECT SM.SECTOR_DESC FROM SECTOR_MST_TBL SM WHERE SM.SECTOR_MST_PK IN (' ||");
                    sb.Append("                NVL(T.SECTOR_MST_FK, 0) || ')') SECTOR_NAME,");

                    sb.Append("       ROWTOCOL('SELECT CMT.CUSTOMER_MST_PK FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK IN (' ||");
                    sb.Append("                NVL(T.CUSTOMER_MST_FK, 0) || ')') CUSTOMER_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT CMT.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK IN (' ||");
                    sb.Append("                NVL(T.CUSTOMER_MST_FK, 0) || ')') CUSTOMER_ID,");
                    sb.Append("       ROWTOCOL('SELECT CMT.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK IN (' ||");
                    sb.Append("                NVL(T.CUSTOMER_MST_FK, 0) || ')') CUSTOMER_NAME,");

                    sb.Append("       ROWTOCOL('SELECT TM.TRADE_MST_PK FROM TRADE_MST_TBL TM WHERE TM.TRADE_MST_PK IN (' ||");
                    sb.Append("                NVL(T.TRADE_MST_FK, 0) || ')') TRADE_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT TM.TRADE_CODE FROM TRADE_MST_TBL TM WHERE TM.TRADE_MST_PK IN (' ||");
                    sb.Append("                NVL(T.TRADE_MST_FK, 0) || ')') TRADE_ID,");
                    sb.Append("       ROWTOCOL('SELECT TM.TRADE_NAME FROM TRADE_MST_TBL TM WHERE TM.TRADE_MST_PK IN (' ||");
                    sb.Append("                NVL(T.TRADE_MST_FK, 0) || ')') TRADE_NAME,");

                    sb.Append("       ROWTOCOL('SELECT CNT.COUNTRY_MST_PK FROM COUNTRY_MST_TBL CNT WHERE CNT.COUNTRY_MST_PK IN (' ||");
                    sb.Append("                NVL(T.COUNTRY_MST_FK, 0) || ')') COUNTRY_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT CNT.COUNTRY_ID FROM COUNTRY_MST_TBL CNT WHERE CNT.COUNTRY_MST_PK IN (' ||");
                    sb.Append("                NVL(T.COUNTRY_MST_FK, 0) || ')') COUNTRY_ID,");
                    sb.Append("       ROWTOCOL('SELECT CNT.COUNTRY_NAME FROM COUNTRY_MST_TBL CNT WHERE CNT.COUNTRY_MST_PK IN (' ||");
                    sb.Append("                NVL(T.COUNTRY_MST_FK, 0) || ')') COUNTRY_NAME,");

                    sb.Append("       ROWTOCOL('SELECT PMT.PORT_GRP_MST_PK FROM PORT_GROUP_MST_TBL PMT WHERE PMT.PORT_GRP_MST_PK IN (' ||");
                    sb.Append("                NVL(T.PORTGROUP_MST_FK, 0) || ')') PORT_GRP_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT PMT.PORT_GRP_CODE FROM PORT_GROUP_MST_TBL PMT WHERE PMT.PORT_GRP_MST_PK IN (' ||");
                    sb.Append("                NVL(T.PORTGROUP_MST_FK, 0) || ')') PORT_GRP_ID,");
                    sb.Append("       ROWTOCOL('SELECT PMT.PORT_GRP_NAME FROM PORT_GROUP_MST_TBL PMT WHERE PMT.PORT_GRP_MST_PK IN (' ||");
                    sb.Append("                NVL(T.PORTGROUP_MST_FK, 0) || ')') PORT_GRP_NAME,");

                    sb.Append("       ROWTOCOL('SELECT POL.PORT_MST_PK FROM PORT_MST_TBL POL WHERE POL.PORT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.POL_MST_FK, 0) || ')') POL_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT POL.PORT_ID FROM PORT_MST_TBL POL WHERE POL.PORT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.POL_MST_FK, 0) || ')') POL_PORT_ID,");
                    sb.Append("       ROWTOCOL('SELECT POL.PORT_NAME FROM PORT_MST_TBL POL WHERE POL.PORT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.POL_MST_FK, 0) || ')') POL_PORT_NAME,");

                    sb.Append("       ROWTOCOL('SELECT POD.PORT_MST_PK FROM PORT_MST_TBL POD WHERE POD.PORT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.POD_MST_FK, 0) || ')') POD_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT POD.PORT_ID FROM PORT_MST_TBL POD WHERE POD.PORT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.POD_MST_FK, 0) || ')') POD_PORT_ID,");
                    sb.Append("       ROWTOCOL('SELECT POD.PORT_NAME FROM PORT_MST_TBL POD WHERE POD.PORT_MST_PK IN (' ||");
                    sb.Append("                NVL(T.POD_MST_FK, 0) || ')') POD_PORT_NAME,");

                    sb.Append("       ROWTOCOL('SELECT CDT.COMMODITY_MST_PK FROM COMMODITY_MST_TBL CDT WHERE CDT.COMMODITY_MST_PK IN (' ||");
                    sb.Append("                NVL(T.COMMODITY_MST_FK, 0) || ')') COMMODITY_MST_PK,");
                    sb.Append("       ROWTOCOL('SELECT CDT.COMMODITY_ID FROM COMMODITY_MST_TBL CDT WHERE CDT.COMMODITY_MST_PK IN (' ||");
                    sb.Append("                NVL(T.COMMODITY_MST_FK, 0) || ')') COMMODITY_ID,");
                    sb.Append("       ROWTOCOL('SELECT CDT.COMMODITY_NAME FROM COMMODITY_MST_TBL CDT WHERE CDT.COMMODITY_MST_PK IN (' ||");
                    sb.Append("              NVL(T.COMMODITY_MST_FK, 0) || ')') COMMODITY_NAME");
                    sb.Append("  FROM ANNOUNCEMENT_TBL T");
                    sb.Append(" WHERE T.ANNOUNCEMENT_PK =" + annpk);
                    sb.Append("  AND T.TYPE_FLAG = " + AnnType);
                }
            }
            else
            {
                if (AnnType == 0)
                {
                    sb.Append("SELECT * FROM ANNOUNCEMENT_TBL ANT ");
                    sb.Append("where ANT.TYPE_FLAG=" + AnnType);
                    sb.Append("ORDER BY ANT.ANNOUNCEMENT_ID");
                }
                else
                {
                    sb.Append("SELECT * FROM ANNOUNCEMENT_TBL ANT ");
                    sb.Append("where ANT.TYPE_FLAG=" + AnnType);
                    sb.Append("ORDER BY ANT.ANNOUNCEMENT_ID");
                }
            }
            try
            {
                return objWF.GetDataTable(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        #endregion "Fetch"

        /// <summary>
        /// Fetches the announcement.
        /// </summary>
        /// <param name="Ann_PK">The ann_ pk.</param>
        /// <returns></returns>
        public DataTable FetchAnnouncement(int Ann_PK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;
            if (Ann_PK != 0)
            {
                StrSql = "SELECT * FROM ANNOUNCEMENT_TBL ANT WHERE ANT.ANNOUNCEMENT_PK=" + Ann_PK;
            }
            else
            {
                StrSql = "SELECT * FROM ANNOUNCEMENT_TBL ANT ORDER BY ANT.ANNOUNCEMENT_ID";
            }
            try
            {
                return objWF.GetDataTable(StrSql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the email.
        /// </summary>
        /// <param name="PartyPK">The party pk.</param>
        /// <param name="Type">The type.</param>
        /// <returns></returns>
        public string FetchEmail(string PartyPK = "0", int Type = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;
            if (Type == 0)
            {
                StrSql = "SELECT ROWTOCOL('SELECT EMAIL FROM CUSTOMER_CONTACT_TRN WHERE CUSTOMER_MST_FK IN(" + PartyPK + ")','|') FROM DUAL";
            }
            else if (Type == 1)
            {
                StrSql = "SELECT ROWTOCOL('SELECT ADM_EMAIL_ID FROM VENDOR_CONTACT_DTLS WHERE NVL(ADM_EMAIL_ID,'' '')<>'' '' AND VENDOR_MST_FK IN(" + PartyPK + ")','|') FROM DUAL";
            }
            else if (Type == 2)
            {
                StrSql = "SELECT ROWTOCOL('SELECT ADM_EMAIL_ID FROM AGENT_CONTACT_DTLS WHERE NVL(ADM_EMAIL_ID,'' '')<>'' '' AND AGENT_MST_FK IN(" + PartyPK + ")','|') FROM DUAL";
            }
            try
            {
                return objWF.ExecuteScaler(StrSql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Fetchlocations all.
        /// </summary>
        /// <param name="locPk">The loc pk.</param>
        /// <returns></returns>
        public DataTable FetchlocationAll(long locPk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;
            if (locPk > 0)
            {
                StrSql = "select location_mst_pk,location_id,location_name from location_mst_tbl where location_mst_tbl.location_mst_pk=" + locPk;
            }
            else
            {
                StrSql = "select location_mst_pk,location_name from location_mst_tbl order by location_mst_pk";
            }

            try
            {
                return objWF.GetDataTable(StrSql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataTable();
        }

        /// <summary>
        /// Delands the modify rights.
        /// </summary>
        /// <param name="locpk">The locpk.</param>
        /// <param name="pk">The pk.</param>
        /// <param name="userpk">The userpk.</param>
        /// <returns></returns>
        public int DelandModifyRights(int locpk, int pk, int userpk)
        {
            WorkFlow objWF = new WorkFlow();
            string strsql = null;
            try
            {
                strsql = "select count(*) from location_mst_tbl loc where loc.location_mst_pk=" + locpk + " and loc.location_mst_pk = ";
                strsql += " (select u.default_location_fk from user_mst_tbl u where u.user_mst_pk= ";
                strsql += " (select ann.created_by_fk from announcement_tbl ann where ann.announcement_pk=" + pk + ") and u.user_mst_pk=" + userpk + ")";
                return Convert.ToInt32(objWF.ExecuteScaler(strsql));
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
        /// Deletes the announcement.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <param name="TYPE">The type.</param>
        /// <returns></returns>
        public string DelAnnouncement(int pk, string TYPE = "1")
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Parameters.Clear();

            try
            {
                var _with3 = objWK.MyCommand;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".ANN_INFO_TBL_PKG.ANN_INFO_TBL_UPSTATUS";
                _with3.Parameters.Add("pk", pk).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("TYPE_IN", TYPE).Direction = ParameterDirection.Input;
                _with3.ExecuteNonQuery();
                TRAN.Commit();
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                ErrorMessage = ex.Message;
                throw ex;
            }
            return "";
        }

        #region "Fetch"

        /// <summary>
        /// Fetches the specified status.
        /// </summary>
        /// <param name="status">The status.</param>
        /// <param name="status2">The status2.</param>
        /// <param name="check">The check.</param>
        /// <param name="AnnType">Type of the ann.</param>
        /// <param name="id">The identifier.</param>
        /// <param name="idExt">The identifier ext.</param>
        /// <param name="annfdate">The annfdate.</param>
        /// <param name="anntdate">The anntdate.</param>
        /// <param name="annsub">The annsub.</param>
        /// <param name="des">The DES.</param>
        /// <param name="annsub2">The annsub2.</param>
        /// <param name="des2">The des2.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="strsort">The strsort.</param>
        /// <param name="blnsortascending">if set to <c>true</c> [blnsortascending].</param>
        /// <param name="annfDtExt">The annf dt ext.</param>
        /// <param name="annToDtExt">The ann to dt ext.</param>
        /// <param name="LocPKs">The loc p ks.</param>
        /// <param name="DeptPKs">The dept p ks.</param>
        /// <param name="DesigPKs">The desig p ks.</param>
        /// <param name="UserPKs">The user p ks.</param>
        /// <param name="MangementPKs">The mangement p ks.</param>
        /// <param name="RegionPks">The region PKS.</param>
        /// <param name="TradePKs">The trade p ks.</param>
        /// <param name="POLPks">The pol PKS.</param>
        /// <param name="AreaPKs">The area p ks.</param>
        /// <param name="SectorPKs">The sector p ks.</param>
        /// <param name="CountryPKs">The country p ks.</param>
        /// <param name="PODPks">The pod PKS.</param>
        /// <param name="AgentPKs">The agent p ks.</param>
        /// <param name="CustomerPKS">The customer PKS.</param>
        /// <param name="PortGrp">The port GRP.</param>
        /// <param name="CommPKs">The comm p ks.</param>
        /// <returns></returns>
        public DataTable Fetch(int status, int status2, int check, int AnnType, string id = "", string idExt = "", string annfdate = "", string anntdate = "", string annsub = "", string des = "",
        string annsub2 = "", string des2 = "", Int32 CurrentPage = 1, Int32 TotalPage = 0, string strsort = "SUBJECT", bool blnsortascending = true, string annfDtExt = "", string annToDtExt = "", string LocPKs = "", string DeptPKs = "",
        string DesigPKs = "", string UserPKs = "", string MangementPKs = "", string RegionPks = "", string TradePKs = "", string POLPks = "", string AreaPKs = "", string SectorPKs = "", string CountryPKs = "", string PODPks = "",
        string AgentPKs = "", string CustomerPKS = "", string PortGrp = "", string CommPKs = "")
        {
            WorkFlow objWF = new WorkFlow();
            string day = null;
            string pday = null;
            string mon = null;
            string pmon = null;
            string strsql = null;
            string sql = null;
            string sql1 = null;
            string strd1 = null;
            string strd3 = null;
            string sorting = null;
            string val = null;
            int chdate = 0;
            int chdates = 0;
            int month = 0;
            Int32 TotalRecords = default(Int32);
            Int32 start = default(Int32);
            Int32 last = default(Int32);
            Int32 i = default(Int32);
            System.DateTime date1 = default(System.DateTime);
            System.DateTime date2 = default(System.DateTime);
            Array arrPKS = null;

            if (AnnType == 0)
            {
                sql1 = "select count(*) ";
                sql = "select ROWNUM SR_NO , ANNOUNCEMENT_PK,ANNOUNCEMENT_ID,SUBJECT,TO_DATE(VALID_FROM,DATEFORMAT) VALID_FROM,no_html(BODY) BODY,locATION_FK,STATUS ";
                strsql = " from ( select * from ANNOUNCEMENT_TBL ) where ";
            }
            else
            {
                sql1 = "select count(*) ";
                sql = "select ROWNUM SR_NO , ANNOUNCEMENT_PK,ANNOUNCEMENT_IDEXT,SUBJECT,TO_DATE(VALID_FROM_EXT,DATEFORMAT) VALID_FROM_EXT,no_html(BODY) BODY,locATION_FK,STATUS_EXT ";
                strsql = " from ( select * from ANNOUNCEMENT_TBL )where ";
            }

            if (check == 1)
            {
                if (!string.IsNullOrEmpty(id))
                {
                    strsql += "ANNOUNCEMENT_ID LIKE " + "'" + id.Replace("'", "''") + "%'" + "and ";
                }
                if (!string.IsNullOrEmpty(annfdate))
                {
                    strsql += " VALID_FROM >= to_date('" + Convert.ToDateTime(annfdate) + "','dd/mm/yyyy') and ";
                }

                if (!string.IsNullOrEmpty(anntdate))
                {
                    strsql += " VALID_TO <= to_date('" + Convert.ToDateTime(anntdate) + "','dd/mm/yyyy') and ";
                }

                if (!string.IsNullOrEmpty(annsub))
                {
                    strsql += "SUBJECT like " + "'" + annsub.Replace("'", "''") + "%'" + " and ";
                }

                if (!string.IsNullOrEmpty(des))
                {
                    strsql += "BODY like " + "'" + des.Replace("'", "''") + "%'" + " and ";
                }

                if (!string.IsNullOrEmpty(idExt))
                {
                    strsql += "ANNOUNCEMENT_IDEXT LIKE " + "'" + id.Replace("'", "''") + "%'" + "and ";
                }

                if (status2 != 5)
                {
                    if (!string.IsNullOrEmpty(annfDtExt))
                    {
                        strsql += " VALID_FROM_EXT >= to_date('" + Convert.ToDateTime(annfDtExt) + "','dd/mm/yyyy') and ";
                    }
                }

                if (!string.IsNullOrEmpty(annToDtExt))
                {
                    strsql += " VALID_TO_EXT <= to_date('" + Convert.ToDateTime(annToDtExt) + "','dd/mm/yyyy') and ";
                }
                if (!string.IsNullOrEmpty(annsub2))
                {
                    strsql += "SUBJECT like " + "'" + annsub2.Replace("'", "''") + "%'" + " and ";
                }

                if (!string.IsNullOrEmpty(des2))
                {
                    strsql += "BODY like " + "'" + des2.Replace("'", "''") + "%'" + " and ";
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(id))
                {
                    strsql += "ANNOUNCEMENT_ID LIKE " + "'%" + id.Replace("'", "''") + "%'" + "and ";
                }
                if (!string.IsNullOrEmpty(annfdate))
                {
                    strsql += " VALID_FROM  >= to_date('" + Convert.ToDateTime(annfdate) + "','dd/mm/yyyy') and ";
                }

                if (!string.IsNullOrEmpty(anntdate))
                {
                    strsql += " VALID_TO <= to_date('" + Convert.ToDateTime(anntdate) + "','dd/mm/yyyy') and ";
                }

                if (!string.IsNullOrEmpty(annsub))
                {
                    strsql += "SUBJECT like " + "'%" + annsub.Replace("'", "''") + "%'" + " and ";
                }

                if (!string.IsNullOrEmpty(des))
                {
                    strsql += "BODY like " + "'%" + des.Replace("'", "''") + "%'" + " and ";
                }

                if (!string.IsNullOrEmpty(idExt))
                {
                    strsql += "ANNOUNCEMENT_IDEXT LIKE " + "'%" + idExt.Replace("'", "''") + "%'" + "and ";
                }
                if (status2 != 5)
                {
                    if (!string.IsNullOrEmpty(annfDtExt))
                    {
                        strsql += " VALID_FROM_EXT  >= to_date('" + Convert.ToDateTime(annfDtExt) + "','dd/mm/yyyy') and ";
                    }
                }

                if (!string.IsNullOrEmpty(annToDtExt))
                {
                    strsql += " VALID_TO_EXT <= to_date('" + Convert.ToDateTime(annToDtExt) + "','dd/mm/yyyy') and ";
                }
                if (!string.IsNullOrEmpty(annsub2))
                {
                    strsql += "SUBJECT like " + "'%" + annsub2.Replace("'", "''") + "%'" + " and ";
                }
                if (!string.IsNullOrEmpty(des2))
                {
                    strsql += "BODY like " + "'%" + des2.Replace("'", "''") + "%'" + " and ";
                }
            }
            if (LocPKs != "0" & !string.IsNullOrEmpty(LocPKs))
            {
                arrPKS = LocPKs.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(location_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (DeptPKs != "0" & !string.IsNullOrEmpty(DeptPKs))
            {
                arrPKS = DeptPKs.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(department_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (DesigPKs != "0" & !string.IsNullOrEmpty(DesigPKs))
            {
                arrPKS = DesigPKs.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(designation_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (UserPKs != "0" & !string.IsNullOrEmpty(UserPKs))
            {
                arrPKS = UserPKs.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(users_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (MangementPKs != "0" & !string.IsNullOrEmpty(MangementPKs))
            {
                arrPKS = MangementPKs.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(management_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }

            if (RegionPks != "0" & !string.IsNullOrEmpty(RegionPks))
            {
                arrPKS = RegionPks.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(region_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (TradePKs != "0" & !string.IsNullOrEmpty(TradePKs))
            {
                arrPKS = TradePKs.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(trade_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (POLPks != "0" & !string.IsNullOrEmpty(POLPks))
            {
                arrPKS = POLPks.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(pol_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (AreaPKs != "0" & !string.IsNullOrEmpty(AreaPKs))
            {
                arrPKS = AreaPKs.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(area_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (SectorPKs != "0" & !string.IsNullOrEmpty(SectorPKs))
            {
                arrPKS = SectorPKs.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(sector_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (CountryPKs != "0" & !string.IsNullOrEmpty(CountryPKs))
            {
                arrPKS = CountryPKs.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(country_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (PODPks != "0" & !string.IsNullOrEmpty(PODPks))
            {
                arrPKS = PODPks.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(pod_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (AgentPKs != "0" & !string.IsNullOrEmpty(AgentPKs))
            {
                arrPKS = AgentPKs.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(agent_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (CustomerPKS != "0" & !string.IsNullOrEmpty(CustomerPKS))
            {
                arrPKS = CustomerPKS.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(customer_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (PortGrp != "0" & !string.IsNullOrEmpty(PortGrp))
            {
                arrPKS = PortGrp.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(portgroup_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }
            if (CommPKs != "0" & !string.IsNullOrEmpty(CommPKs))
            {
                arrPKS = CommPKs.Split(',');
                strsql += "(1=2";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += " or instr(commodity_mst_fk || ','," + arrPKS.GetValue(i) + "||',')>0";
                }
                strsql += ") and ";
            }

            date1 = DateTime.Today.AddMonths(-1);
            date1 = date1.AddDays(-1);
            date2 = DateTime.Today.AddDays(-1);
            if (AnnType == 0)
            {
                switch ((status))
                {
                    case 0:
                        strsql += " (( to_date('" + DateTime.Today.Date + "','dd/mm/yyyy') between VALID_FROM and VALID_TO ) and ( STATUS   = 0 or STATUS  = 4 )) ";
                        break;

                    case 1:
                        strsql += " ( VALID_TO between to_date('" + date1 + "','dd/mm/yyyy') and to_date('" + date2 + "','dd/mm/yyyy')) and   STATUS  in (0,1,4)";
                        break;

                    case 2:
                        strsql += "   VALID_TO < to_date('" + date1 + "','dd/mm/yyyy') and status <>3";
                        break;

                    case 3:
                        strsql += " STATUS = " + status;
                        break;

                    case 4:
                        strsql += " VALID_FROM > to_date('" + DateTime.Today.Date + "','dd/mm/yyyy') and STATUS = 4 ";
                        break;

                    case 5:
                        strsql += " STATUS >= 0 AND STATUS<>3 ";
                        break;
                }
                strsql += " AND TYPE_FLAG= " + AnnType;

                switch (strsort)
                {
                    case "Announcement ID":
                        val = " Announcement_ID ";
                        break;

                    case "DateTime":
                        val = " VALID_FROM ";
                        break;

                    case "Subject":
                        val = " SUBJECT ";
                        break;

                    case "Description":
                        val = " BODY ";
                        break;
                }

                if (blnsortascending == true)
                {
                    sorting = " ORDER BY ANNOUNCEMENT_PK desc ";
                }
                else
                {
                    sorting = " ORDER BY ANNOUNCEMENT_PK desc ";
                }
            }
            else
            {
                switch ((status2))
                {
                    case 0:
                        strsql += " (( to_date('" + DateTime.Today.Date + "','dd/mm/yyyy') between TO_DATE(VALID_FROM_EXT,'DD/MM/YYYY') and TO_DATE(VALID_TO_EXT,'DD/MM/YYYY')) and ( STATUS_EXT   = 0 or STATUS_EXT  = 4 )) ";
                        break;

                    case 1:
                        strsql += " ( VALID_TO_EXT between to_date('" + date1 + "','dd/mm/yyyy') and to_date('" + date2 + "','dd/mm/yyyy')) and   STATUS_EXT  in (0,1,4)";
                        break;

                    case 2:
                        strsql += "   VALID_TO_EXT < to_date('" + date1 + "','dd/mm/yyyy') and STATUS_EXT <>3";
                        break;

                    case 3:
                        strsql += " STATUS_EXT = " + status;
                        break;

                    case 4:
                        strsql += " VALID_FROM_EXT > to_date('" + DateTime.Today.Date + "','dd/mm/yyyy') and STATUS_EXT = 4 ";
                        break;

                    case 5:
                        strsql += " STATUS_EXT >= 0 and status_ext <>3";
                        break;
                }
                strsql += " AND TYPE_FLAG= " + AnnType;

                switch (strsort)
                {
                    case "Announcement ID":
                        val = " ANNOUNCEMENT_IDEXT ";
                        break;

                    case "DateTime":
                        val = " VALID_FROM_EXT ";
                        break;

                    case "Subject":
                        val = " SUBJECT ";
                        break;

                    case "Description":
                        val = " BODY ";
                        break;
                }

                if (blnsortascending == true)
                {
                    sorting = " ORDER BY ANNOUNCEMENT_PK desc ";
                }
                else
                {
                    sorting = " ORDER BY ANNOUNCEMENT_PK desc ";
                }
            }
            try
            {
                sql1 = sql1 + strsql + sorting;
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

                if (strsort != "Sl.Nr")
                {
                    strsql = sql + strsql + sorting;
                }
                else
                {
                    strsql = sql + strsql;
                }

                return objWF.GetDataTable(strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        #endregion "Fetch"

        #region "Attachment"

        /// <summary>
        /// Checks for attachment.
        /// </summary>
        /// <param name="filename">The filename.</param>
        /// <returns></returns>
        public bool CheckForAttachment(string filename)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            try
            {
                strsql.Append(" select COUNT(*) from ANNOUNCEMENT_TBL ANN WHERE ANN.FILENAMES LIKE '%" + filename + "%' ");
                if (Convert.ToInt32(objWK.ExecuteScaler(strsql.ToString())) > 0)
                {
                    return false;
                }
                else
                {
                    return true;
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
        }

        /// <summary>
        /// Attaches the files.
        /// </summary>
        /// <param name="AnnPk">The ann pk.</param>
        /// <returns></returns>
        public DataSet AttachFiles(string AnnPk)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strsql.Append(" SELECT a.file_name, a.file_path FROM attach_file_dtl_tbl a WHERE a.announcement_fk= " + AnnPk);
                return objWF.GetDataSet(strsql.ToString());
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
        /// Saves the attachment.
        /// </summary>
        /// <param name="Refno">The refno.</param>
        /// <param name="filename">The filename.</param>
        /// <param name="flags">The flags.</param>
        /// <returns></returns>
        public string SaveAttachment(string Refno, string filename, Int32 flags)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            Int32 i = default(Int32);
            Int32 Pk = default(Int32);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Parameters.Clear();
            try
            {
                Pk = FetchPK(Refno);
                var _with4 = objWK.MyCommand;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".ANN_INFO_TBL_PKG.ANN_INFO_TBL_ATTACHMENT";
                _with4.Transaction = TRAN;
                _with4.Parameters.Add("Pk_IN", Pk).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("FILENAME_IN", filename).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("flag", flags).Direction = ParameterDirection.Input;
                _with4.ExecuteNonQuery();
                TRAN.Commit();
                return "All Data Saved Successfully";
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Attachment"

        /// <summary>
        /// Fetches the pk.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <param name="Type">The type.</param>
        /// <returns></returns>
        public int FetchPK(string id, string Type = "0")
        {
            string strsql = null;
            try
            {
                WorkFlow objWK = new WorkFlow();
                if (Type == "0")
                {
                    strsql = " select ANNOUNCEMENT_PK from announcement_tbl where ANNOUNCEMENT_ID = '" + id + "'";
                }
                else
                {
                    strsql = " select ANNOUNCEMENT_PK from announcement_tbl where ANNOUNCEMENT_IDEXT = '" + id + "'";
                }
                return Convert.ToInt32(objWK.ExecuteScaler(strsql));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the files.
        /// </summary>
        /// <param name="refno">The refno.</param>
        /// <returns></returns>
        public DataSet FetchFiles(string refno)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            try
            {
                strsql.Append(" select ANN.FILENAMES from ANNOUNCEMENT_TBL ANN WHERE ANN.ANNOUNCEMENT_ID LIKE '%" + refno + "%' ");
                return objWK.GetDataSet(strsql.ToString());
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

        #endregion "Fetch Information-Announcements"

        #region "Send Mail"

        /// <summary>
        /// Sends the completed callback.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The <see cref="AsyncCompletedEventArgs"/> instance containing the event data.</param>
        private static void SendCompletedCallback(object sender, AsyncCompletedEventArgs e)
        {
            // Get the unique identifier for this asynchronous operation.

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            int RetVal = 0;
            string MailDesc = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Parameters.Clear();
            try
            {
                MailMessage mail = (MailMessage)e.UserState;
                string str = null;
                int MailStatus = 0;
                if (e.Error != null)
                {
                    str = e.Error.ToString();
                    MailStatus = 2;
                }
                else
                {
                    str = "Mail Send Successfully";
                    MailStatus = 1;
                }
                if (string.Compare(M_Name, "gmail.com") > 0)
                {
                    System.Net.WebClient objClient = new System.Net.WebClient();
                    XmlNodeList nodelist = null;
                    XmlNode node = null;
                    string response = null;
                    XmlDocument xmlDoc = new XmlDocument();
                    int mailCount = 0;
                    int tempCounter = 0;
                    string emailFrom = null;
                    string emailMessages = null;
                    objClient.Credentials = new System.Net.NetworkCredential(M_Name, M_Password);
                    response = Encoding.UTF8.GetString(objClient.DownloadData("https://mail.google.com/mail/feed/atom"));
                    response = response.Replace("<feed version=\"0.3\" xmlns=\"http://purl.org/atom/ns#\">", "<feed>");

                    xmlDoc.LoadXml(response);
                    node = xmlDoc.SelectSingleNode("/feed/fullcount");
                    mailCount = Convert.ToInt32(node.InnerText);
                    //Get the number of unread emails

                    if (mailCount > 0)
                    {
                        nodelist = xmlDoc.SelectNodes("/feed/entry");
                        node = xmlDoc.SelectSingleNode("title");
                        tempCounter = 0;
                        foreach (XmlNode node_loopVariable in nodelist)
                        {
                            node = node_loopVariable;
                            emailMessages = node.ChildNodes.Item(1).InnerText;
                            emailFrom = node.ChildNodes.Item(6).ChildNodes[0].InnerText;
                            tempCounter = tempCounter + 1;
                            if (tempCounter <= 10)
                            {
                                if (string.Compare(emailMessages.ToUpper(), mail.To[0].Address.ToUpper()) > 0)
                                {
                                    str = emailMessages;
                                    MailStatus = 2;
                                    break; // TODO: might not be correct. Was : Exit For
                                }
                            }
                        }
                    }
                }

                if (str == "Mail Send Successfully")
                {
                    MailDesc = str;
                }
                else if (string.Compare(str.ToUpper(), "MAILBOX NOT FOUND") > 0 | string.Compare(str.ToUpper(), "INVALID MAILBOX") > 0 | string.Compare(str.ToUpper(), "USER UNKNOWN") > 0 | string.Compare(str.ToUpper(), "NOT OUR CUSTOMER") > 0 | string.Compare(str.ToUpper(), "MAILBOX UNAVAILABLE") > 0)
                {
                    MailDesc = "Invalid Email ID";
                }
                else if (string.Compare(str.ToUpper(), "MAILBOX FULL") > 0 | string.Compare(str.ToUpper(), "QUOTA EXCEEDED") > 0)
                {
                    MailDesc = "Mail Box Full";
                }
                else if (string.Compare(str.ToUpper(), "BLACKLIST FILTERS") > 0)
                {
                    MailDesc = "Email is Blocked by receiver";
                }
                else if (string.Compare(str.ToUpper(), "CONTENT FILTERS") > 0)
                {
                    MailDesc = "Email looks like a spam and is blocked by receiver";
                }
                else if (string.Compare(str.ToUpper(), "5.5.1 AUTHENTICATION REQUIRED") > 0)
                {
                    MailDesc = "Unable to Connect to SMTP Server";
                }
                else
                {
                    MailDesc = "Invalid Email ID";
                }

                var _with5 = objWK.MyCommand;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".MAIL_SEND_STATUS_TBL_PKG.MAIL_SEND_STATUS_INS";
                _with5.Parameters.Add("MAIL_TYPE_IN", 1).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("DOC_TYPE_IN", "").Direction = ParameterDirection.Input;
                _with5.Parameters.Add("DOC_REF_NR_IN", M_AnnId).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("MAIL_SUBJECT_IN", mail.Subject).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("SEND_FROM_IN", mail.From.Address).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("SEND_TO_IN", mail.To[0].Address).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("MAIL_STATUS_IN", MailStatus).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("MAIL_DESC_IN", MailDesc).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("MAIL_DESC_DTL_IN", str).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("CREATED_BY_FK_IN", Create_By).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("MAIL_BODY_IN", mail.Body).Direction = ParameterDirection.Input;
                if ((FileName != null))
                {
                    FileName = FileName;
                }
                else
                {
                    FileName = "";
                }
                _with5.Parameters.Add("MAIL_FILE_IN", FileName).Direction = ParameterDirection.Input;
                // .Parameters.Add("MAIL_CC_IN", mail.CC(0).Address).Direction = ParameterDirection.Input
                // .Parameters.Add("MAIL_BCC_IN", mail.Bcc(0).Address).Direction = ParameterDirection.Input
                _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                RetVal = _with5.ExecuteNonQuery();
                TRAN.Commit();
                FileName = "";
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }

        /// <summary>
        /// Sends the mail.
        /// </summary>
        /// <param name="MailId">The mail identifier.</param>
        /// <param name="Subject">The subject.</param>
        /// <param name="Body">The body.</param>
        /// <param name="CustomerName">Name of the customer.</param>
        /// <param name="File_Paths">The file_ paths.</param>
        /// <param name="Annpk">The annpk.</param>
        /// <param name="ChkSpecific">The CHK specific.</param>
        /// <returns></returns>
        public string SendMail(string MailId, string Subject, string Body, string CustomerName = "", string File_Paths = "", string Annpk = "", int ChkSpecific = 0)
        {
            string functionReturnValue = null;

            try
            {
                SmtpClient smtpserver = new SmtpClient();
                MailMessage objMail = new MailMessage();
                WorkFlow objWF = new WorkFlow();
                string EAttach = null;
                string dsMail = null;
                Int32 intCnt = default(Int32);
                string strbody = null;
                string M_MAIL_ATTACHMENTS_PATH = null;
                bool Result = false;
                Array EmailId = null;
                Int16 i = default(Int16);
                Int16 j = default(Int16);
                Array CustName = null;
                if (ChkSpecific == 0)
                {
                    if (!string.IsNullOrEmpty(CustomerName))
                    {
                        CustName = CustomerName.Split(',');
                    }
                }

                if (!string.IsNullOrEmpty(MailId))
                {
                    EmailId = MailId.Split('|');
                    for (j = 0; j <= EmailId.Length - 1; j++)
                    {
                        if (!string.IsNullOrEmpty(EmailId.GetValue(j).ToString()))
                        {
                            objMail = new MailMessage();
                            objMail.From = new MailAddress(M_SEND_USERNAME, M_SEND_NAME);
                            objMail.To.Add(EmailId.GetValue(j).ToString());
                            objMail.Subject = Subject;
                            objMail.IsBodyHtml = true;
                            if (ChkSpecific == 0)
                            {
                                strbody = " Dear  " + CustName.GetValue(j);
                            }
                            strbody = " ";
                            strbody += "<P>";
                            strbody += " " + Body + "</p>";
                            //strbody &= " This is an auto generated e-Mail. Please do not reply to this e-Mail-ID." & vbCrLf
                            strbody += " ";
                            objMail.Body = strbody;
                            objMail.BodyEncoding = Encoding.UTF8;
                            if (!string.IsNullOrEmpty(File_Paths))
                            {
                                M_MAIL_ATTACHMENTS_PATH = File_Paths;
                                for (i = 0; i <= M_MAIL_ATTACHMENTS_PATH.Split(Convert.ToChar(",")).Length - 1; i++)
                                {
                                    EAttach = M_MAIL_ATTACHMENTS_PATH.Split(Convert.ToChar(","))[i];
                                    objMail.Attachments.Add(new Attachment(EAttach));
                                }
                            }
                            smtpserver = new SmtpClient(M_MAIL_SERVER, 587);
                            NetworkCredential basicCredentials = new NetworkCredential(M_SEND_USERNAME, M_SEND_PASSWORD);
                            if (string.Compare(M_SEND_USERNAME, "gmail.com") > 0)
                            {
                                smtpserver.EnableSsl = true;
                            }
                            smtpserver.UseDefaultCredentials = true;
                            smtpserver.Credentials = basicCredentials;
                            object userState = objMail;
                            smtpserver.SendCompleted += SendCompletedCallback;
                            smtpserver.SendAsync(objMail, userState);
                        }
                    }
                }
                objMail = null;
                Result = objWF.ExecuteCommands("update announcement_tbl set POST_FLAG=1 where announcement_pk=' " + Annpk + "'");
                if (!string.IsNullOrEmpty(MailId))
                {
                    return "All Data Saved Successfully and Mail has been sent.";
                }
                else
                {
                    return "Posted Succesfully.";
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return "All Data Saved Successfully.Due to some problem Mail has not been sent";
                return functionReturnValue;
            }
            return functionReturnValue;
        }

        #endregion "Send Mail"

        #region "Fetch Names"

        /// <summary>
        /// Fetches the names.
        /// </summary>
        /// <param name="PartyPK">The party pk.</param>
        /// <param name="Type">The type.</param>
        /// <returns></returns>
        public string FetchNames(string PartyPK = "0", int Type = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string StrSql = null;
            if (Type == 0)
            {
                StrSql = "SELECT ROWTOCOL('SELECT CUSTOMER_NAME FROM CUSTOMER_MST_TBL WHERE CUSTOMER_MST_PK IN(" + PartyPK + ")') FROM DUAL";
            }
            else if (Type == 1)
            {
                StrSql = "SELECT ROWTOCOL('SELECT VENDOR_NAME FROM VENDOR_MST_TBL WHERE VENDOR_MST_PK IN(" + PartyPK + ")') FROM DUAL";
            }
            else if (Type == 2)
            {
                StrSql = "SELECT ROWTOCOL('SELECT AGENT_NAME FROM AGENT_MST_TBL WHERE AGENT_MST_PK IN(" + PartyPK + ")') FROM DUAL";
            }
            try
            {
                return objWF.ExecuteScaler(StrSql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        #endregion "Fetch Names"

        #region "save Announcement"
        public string SaveAnnouncement(DataSet dsmain, int opt, string key, Int64 ILocationId, Int64 IEmployeeId, Int64 Userid, int TypeFlag, int RestrictionPK = 0)
        {
            int find = 0;
            int count = 0;
            int store = 0;
            string id = null;
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Parameters.Clear();
            string AnnouncePk = null;
            try
            {
                var _with1 = objWK.MyCommand;
                _with1.CommandType = CommandType.StoredProcedure;


                if (opt == 0)
                {
                    _with1.CommandText = objWK.MyUserName + ".ANN_INFO_TBL_PKG.ANN_INFO_TBL_INS";
                }
                else
                {
                    _with1.CommandText = objWK.MyUserName + ".ANN_INFO_TBL_PKG.ANN_INFO_TBL_UP";
                }
                if (opt == 0)
                {
                    id = GenerateProtocolKey(key, ILocationId, IEmployeeId, DateTime.Now, "", "","" , Userid);
                    if (TypeFlag == 0)
                    {
                        _with1.Parameters.Add("ANN_ID", id).Direction = ParameterDirection.Input;
                        _with1.Parameters.Add("ANN_IDEXT", "0").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("ANN_ID", "0").Direction = ParameterDirection.Input;
                        _with1.Parameters.Add("ANN_IDEXT", id).Direction = ParameterDirection.Input;
                    }
                    _with1.Parameters.Add("RESTRICT_PK_IN", RestrictionPK).Direction = ParameterDirection.Input;
                }
                else
                {
                    if (TypeFlag == 0)
                    {
                        _with1.Parameters.Add("ANNOUNCEMENT_PK_IN", dsmain.Tables["tblmaster"].Rows[0]["ANNOUNCEMENT_PK"]).Direction = ParameterDirection.Input;
                        _with1.Parameters.Add("ANN_ID", dsmain.Tables["tblmaster"].Rows[0]["ANNOUNCEMENT_ID"]).Direction = ParameterDirection.Input;
                        _with1.Parameters.Add("ANN_IDEXT", "0").Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Parameters.Add("ANNOUNCEMENT_PK_IN", dsmain.Tables["tblmaster"].Rows[0]["ANNOUNCEMENT_PK"]).Direction = ParameterDirection.Input;
                        _with1.Parameters.Add("ANN_ID", "0").Direction = ParameterDirection.Input;
                        _with1.Parameters.Add("ANN_IDEXT", dsmain.Tables["tblmaster"].Rows[0]["ANNOUNCEMENT_IDEXT"]).Direction = ParameterDirection.Input;
                    }
                }

                _with1.Parameters.Add("ANN_DT", dsmain.Tables["tblmaster"].Rows[0]["ANNOUNCEMENT_DT"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("ANN_SUBJECT", dsmain.Tables["tblmaster"].Rows[0]["SUBJECT"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("ANN_BODY", dsmain.Tables["tblmaster"].Rows[0]["BODY"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("STATUS_INT", dsmain.Tables["tblmaster"].Rows[0]["STATUS"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("VALIDFROM_INT", dsmain.Tables["tblmaster"].Rows[0]["VALID_FROM"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("VALIDTO_INT", dsmain.Tables["tblmaster"].Rows[0]["VALID_TO"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("LOC_FK", dsmain.Tables["tblmaster"].Rows[0]["locATION_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CRETD_FK", dsmain.Tables["tblmaster"].Rows[0]["CREATED_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("PRIORITY_IN", dsmain.Tables["tblmaster"].Rows[0]["PRIORITY"]).Direction = ParameterDirection.Input;
                ///'' -------------INTERNAL----------------------------------------
                _with1.Parameters.Add("DEPARTMENT_FK", dsmain.Tables["tblmaster"].Rows[0]["DEPARTMENT_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("USER_FK", dsmain.Tables["tblmaster"].Rows[0]["USERS_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("DESIGNATION_FK", dsmain.Tables["tblmaster"].Rows[0]["DESIGNATION_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("MANAGEMENT_FK", dsmain.Tables["tblmaster"].Rows[0]["MANAGEMENT_MST_FK"]).Direction = ParameterDirection.Input;

                ///''-----------EXTERNAL---------------------------------------------------''
                _with1.Parameters.Add("AREA_FK", dsmain.Tables["tblmaster"].Rows[0]["AREA_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("AGENT_FK", dsmain.Tables["tblmaster"].Rows[0]["AGENT_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("VENDOR_FK", dsmain.Tables["tblmaster"].Rows[0]["VENDOR_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("REGION_FK", dsmain.Tables["tblmaster"].Rows[0]["REGION_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("SECTOR_FK", dsmain.Tables["tblmaster"].Rows[0]["SECTOR_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CUSTOMER_FK", dsmain.Tables["tblmaster"].Rows[0]["CUSTOMER_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("TRADE_FK", dsmain.Tables["tblmaster"].Rows[0]["TRADE_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("COUNTRY_FK", dsmain.Tables["tblmaster"].Rows[0]["COUNTRY_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("PORTGROUP_FK", dsmain.Tables["tblmaster"].Rows[0]["PORTGROUP_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("POL_FK", dsmain.Tables["tblmaster"].Rows[0]["POL_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("POD_FK", dsmain.Tables["tblmaster"].Rows[0]["POD_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("COMMODITY_FK", dsmain.Tables["tblmaster"].Rows[0]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("ANNOUNCE_TYPE", Convert.ToInt32(dsmain.Tables["tblmaster"].Rows[0]["TYPE_FLAG"])).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("ANNOUNCE_DTEXT", dsmain.Tables["tblmaster"].Rows[0]["ANNOUNCEMENT_DT_EXT"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("STATUS_EXT", Convert.ToInt32((string.IsNullOrEmpty(dsmain.Tables["tblmaster"].Rows[0]["STATUS_EXT"].ToString()) ? "0" : dsmain.Tables["tblmaster"].Rows[0]["STATUS_EXT"]))).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("VALIDFROM_EXT", dsmain.Tables["tblmaster"].Rows[0]["VALID_FROM_EXT"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("VALIDTO_EXT", dsmain.Tables["tblmaster"].Rows[0]["VALID_TO_EXT"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("PRIORITY_EXT_IN", dsmain.Tables["tblmaster"].Rows[0]["PRIORITY_EXT"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("SPECIFIC_IN", dsmain.Tables["tblmaster"].Rows[0]["SPECIFIC"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("TO_MAIL_IN", dsmain.Tables["tblmaster"].Rows[0]["TO_MAIL"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CC_MAIL_IN", dsmain.Tables["tblmaster"].Rows[0]["CC_MAIL"]).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100).Direction = ParameterDirection.Output;
                _with1.ExecuteNonQuery();
                TRAN.Commit();
                if (opt == 0)
                {
                    AnnouncePk = Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                    //' Return id
                }
                else
                {
                    AnnouncePk = Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                return AnnouncePk;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }
        #endregion
    }
}