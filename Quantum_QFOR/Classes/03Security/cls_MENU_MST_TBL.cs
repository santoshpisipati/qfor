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
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_MENU_MST_TBL : CommonFeatures
    {
        #region "List of Members of the Class"

        private Int32 M_MENU_MST_PK;
        private Int16 M_MENU_LEVEL;
        private Int32 M_MENU_MST_FK;
        private string M_MENU_ID;
        private Int32 M_MENU_TYPE_FK;
        private Int32 M_CONFIG_MST_FK;
        private string M_MENU_URL;
        //THis property is only used by IsUserValid Function it has no concern with User Class at all.

        #endregion "List of Members of the Class"

        #region "List of Properties"

        public Int32 Menu_Mst_PK
        {
            get { return M_MENU_MST_PK; }
            set { M_MENU_MST_PK = value; }
        }

        public Int16 Menu_Level
        {
            get { return M_MENU_LEVEL; }
            set { M_MENU_LEVEL = value; }
        }

        public Int32 Menu_Mst_FK
        {
            get { return M_MENU_MST_FK; }
            set { M_MENU_MST_FK = value; }
        }

        public Int32 Menu_Type_FK
        {
            get { return M_MENU_TYPE_FK; }
            set { M_MENU_TYPE_FK = value; }
        }

        public Int32 Config_Mst_FK
        {
            get { return M_CONFIG_MST_FK; }
            set { M_CONFIG_MST_FK = value; }
        }

        public string Menu_ID
        {
            get { return M_MENU_ID; }
            set { M_MENU_ID = value; }
        }

        public string Menu_URL
        {
            get { return M_MENU_URL; }
            set { M_MENU_URL = value; }
        }

        #endregion "List of Properties"

        #region "List of Functions"

        public DataSet FetchMenu(Int32 ROLE, Int32 USER)
        {
            string strSQL = null;

            strSQL = "SELECT ROWNUM SLNO,QRY.* FROM (";
            strSQL = strSQL + " SELECT NVL(USRROLE.ROLE_ACCESS_TRN_PK,0) PK, ";
            strSQL = strSQL + " MST.MENU_MST_PK MENU_PK,   ";
            strSQL = strSQL + " NVL(MTXT.MENU_TEXT,'')  MENU_TEXT ,   ";
            strSQL = strSQL + " NVL(MST1.MENU_ID,'')  MENU_PARENT , ";
            strSQL = strSQL + " MST.DISPLAY_ORDER, MST.MENU_LEVEL , ";
            strSQL = strSQL + " NVL(MST.CONFIG_ID_FK,0) CONFIG_ID_FK,    ";
            strSQL = strSQL + " NVL(CONFMST.APPLICABLE_OPERATIONS_VALUE,0) ACCESSRIGHT, ";
            strSQL = strSQL + " (CASE WHEN NVL(USR.ALLOWED_OPERATIONS_VALUE,0) >0 THEN ";
            strSQL = strSQL + " NVL(USR.ALLOWED_OPERATIONS_VALUE, 0)";
            strSQL = strSQL + " ELSE NVL(USRROLE.ALLOWED_OPERATIONS_VALUE,0) END) ALLOWEDRIGHT, ";
            strSQL = strSQL + " nvl(USR.VERSION_NO,0) VERSION_NO , ";
            strSQL = strSQL + " (CASE WHEN NVL(USRROLE.ALLOWED_OPERATIONS_VALUE,0) =0 THEN 0 ELSE 1 END )SELECTED ,";
            strSQL = strSQL + " NVL(USRROLE.ALLOWED_OPERATIONS_VALUE,0)  MODFLAG ,";
            strSQL = strSQL + " NVL(USR.USER_ACCESS_PK,0)  USER_ACCESS_PK ";
            strSQL = strSQL + " FROM MENU_MST_TBL MST, ";
            strSQL = strSQL + " MENU_MST_TBL MST1,   ";
            strSQL = strSQL + " MENU_TEXT_MST_TBL MTXT,    ";
            strSQL = strSQL + " CONFIG_MST_TBL CONFMST , ";
            strSQL = strSQL + " ROLE_ACCESS_TRN USRROLE,";
            strSQL = strSQL + " USER_ACCESS_TRN USR ";
            strSQL = strSQL + " WHERE ";
            strSQL = strSQL + " CONFMST.CONFIG_MST_PK= USRROLE.CONFIG_MST_FK(+)";
            strSQL = strSQL + " AND MTXT.MENU_MST_FK(+) = MST.MENU_MST_PK  ";
            strSQL = strSQL + " AND MST.MENU_MST_FK=MST1.MENU_MST_PK  ";
            strSQL = strSQL + " AND MTXT.MENU_TEXT IS NOT NULL   ";
            strSQL = strSQL + " AND USRROLE.ROLE_MST_FK(+)=" + ROLE + "";
            strSQL = strSQL + " AND MTXT.ENVIRONMENT_MST_FK(+) = 1   ";
            strSQL = strSQL + " AND CONFMST.CONFIG_MST_PK(+) =  MST.CONFIG_ID_FK   ";
            strSQL = strSQL + " AND CONFMST.CONFIG_MST_PK=USR.CONFIG_MST_FK(+)";
            strSQL = strSQL + " AND USR.USER_MST_FK(+)=" + USER + "";
            strSQL = strSQL + " ORDER BY MST.DISPLAY_ORDER) QRY";

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

        public string GetMenuData(Int64 lngEnvPk, Int64 lngLocPk, Int64 lngUserPK)
        {
            Int64 lngROLEPK = 463;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            OracleDataReader dR = null;
            dR = objWF.GetDataReader("SELECT NVL(ROLE_MST_FK,0) Role FROM USER_MST_TBL WHERE USER_MST_PK=" + lngUserPK);
            if (dR.Read())
            {
                Int32 value = Convert.ToInt32( dR["Role"]);
                lngROLEPK = Convert.ToInt64(value);
            }
            dR.Close();
            ///strSQL = "SELECT * FROM ("
            strSQL += "SELECT QRY.MENU_MST_PK,QRY.DISPLAY_ORDER,QRY.MENU_LEVEL,QRY.MENU_URL,";
            strSQL += " QRY.MENU_ID,QRY.MENU_TEXT,QRY.MENU_TOOL_TIP,QRY.MENU_MST_FK,";

            //commenting & adding by thiyagarajan on 30/11/08 to display menu items according to users accesss rights
            //strSQL &= vbCrLf & " (CASE  WHEN QRY.TOTVAL>-1 THEN QRY.TOTVAL ELSE QRY.ROLEVAL END ) TOTVAL,"
            strSQL += " (CASE  WHEN QRY.TOTVAL>-1 THEN QRY.TOTVAL ELSE -1 END ) TOTVAL,";

            //strSQL &= vbCrLf & "(CASE  WHEN QRY.ALLVAL>-1 THEN QRY.ALLVAL ELSE QRY.ALLROLEVAL END ) ALLVAL"
            strSQL += "(CASE  WHEN QRY.ALLVAL>-1 THEN QRY.ALLVAL ELSE -1 END ) ALLVAL";
            //end

            strSQL += " FROM ";
            strSQL += " (SELECT MENUMST.MENU_MST_PK,";
            strSQL += " MENUMST.DISPLAY_ORDER,";
            strSQL += " MENUMST.MENU_LEVEL,";
            strSQL += " NVL(MENUMST.MENU_URL, '') MENU_URL,";
            strSQL += " MENUMST.MENU_ID,";
            strSQL += " MENUTXT.MENU_TEXT,";
            strSQL += " MENUTXT.MENU_TOOL_TIP,";
            strSQL += " MENUMST.MENU_MST_FK,";

            //commenting & adding by thiyagarajan on 30/11/08 to display menu items according to users accesss rights
            //strSQL &= vbCrLf & " NVL(USRACC.ALLOWED_OPERATIONS_VALUE, -1) TOTVAL,"
            strSQL += "  nvl((case when (instr(','||USRACC.menu_mst_fk||',' , ','||MENUMST.MENU_MST_PK||',')) > 0 then ";
            strSQL += "   USRACC.ALLOWED_OPERATIONS_VALUE else 0 end),0) TOTVAL, ";
            //End

            strSQL += " NVL((SELECT RA.ALLOWED_OPERATIONS_VALUE ";
            strSQL += " FROM ROLE_ACCESS_TRN RA ";
            strSQL += " WHERE RA.ROLE_MST_FK=" + lngROLEPK + " AND ";
            strSQL += " RA.CONFIG_MST_FK=MENUMST.CONFIG_ID_FK),0) ROLEVAL,";

            strSQL += " NVL((SELECT SUM(NVL(UACC.ALLOWED_OPERATIONS_VALUE, 0))";
            strSQL += " FROM MENU_MST_TBL MMST, USER_ACCESS_TRN UACC";
            strSQL += " WHERE UACC.CONFIG_MST_FK = MMST.CONFIG_ID_FK AND";
            strSQL += " SUBSTR(MMST.DISPLAY_ORDER, 1, 2) =";
            strSQL += " RTRIM(MENUMST.DISPLAY_ORDER) AND UACC.USER_MST_FK = " + lngUserPK + " AND ";
            //strSQL &= vbCrLf & " UACC.LOCATION_MST_FK = 1 ),-1) ALLVAL,"
            //adding by thiyagarajan on 30/11/08 to display menu items according to users accesss rights
            strSQL += "  (instr(','||UACC.menu_mst_fk||',' , ','||MMST.MENU_MST_PK||',')) > 0 AND ";

            strSQL += " UACC.LOCATION_MST_FK =" + lngLocPk + " ),0) ALLVAL,";
            //end

            strSQL += " NVL((SELECT SUM(NVL(RACC.ALLOWED_OPERATIONS_VALUE, 0))";
            strSQL += " FROM MENU_MST_TBL MMST, ROLE_ACCESS_TRN RACC";
            strSQL += " WHERE RACC.CONFIG_MST_FK = MMST.CONFIG_ID_FK AND";
            strSQL += " SUBSTR(MMST.DISPLAY_ORDER, 1, 2) =";
            strSQL += " RTRIM(MENUMST.DISPLAY_ORDER) AND RACC.ROLE_MST_FK=" + lngROLEPK + "  ),0) ALLROLEVAL";
            strSQL += " FROM MENU_MST_TBL      MENUMST,";
            strSQL += " MENU_TEXT_MST_TBL MENUTXT,";
            strSQL += " USER_ACCESS_TRN   USRACC";
            strSQL += " WHERE MENUTXT.MENU_MST_FK = MENUMST.MENU_MST_PK AND";
            strSQL += " NVL(MENUMST.ACTIVE_FLAG, 1) = 1 AND";
            strSQL += " USRACC.CONFIG_MST_FK(+) = MENUMST.CONFIG_ID_FK AND";
            strSQL += " MENUTXT.ENVIRONMENT_MST_FK = " + lngEnvPk + " AND USRACC.LOCATION_MST_FK(+) = " + lngLocPk + " AND ";

            strSQL += " USRACC.USER_MST_FK(+)=" + lngUserPK + "";
            //if ((short)HttpContext.Current.Session["BIZ_TYPE"] == 1)
            //{
                strSQL = strSQL + "  AND MENUMST.BIZ_TYPE IN (1,3)";
            //}
            //else if ((short)HttpContext.Current.Session["BIZ_TYPE"] == 2)
            //{
            //    strSQL = strSQL + "  AND MENUMST.BIZ_TYPE IN (2,3)";
            //}
            strSQL += " ORDER BY MENUMST.DISPLAY_ORDER) QRY";

            try
            {
                DataSet getDs = objWF.GetDataSet(strSQL);
                return JsonConvert.SerializeObject(getDs, Formatting.Indented);
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

        public DataSet GetMenuDataNew(Int64 lngEnvPk, Int64 lngLocPk, Int64 lngUserPK)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT QRY.MENU_MST_PK,");
            sb.Append("       QRY.DISPLAY_ORDER,");
            sb.Append("       QRY.MENU_LEVEL,");
            sb.Append("       QRY.MENU_URL,");
            sb.Append("       QRY.MENU_ID,");
            sb.Append("       QRY.MENU_TEXT,");
            sb.Append("       QRY.MENU_TOOL_TIP,");
            sb.Append("       QRY.MENU_MST_FK");
            sb.Append("  FROM (SELECT MENUMST.MENU_MST_PK,");
            sb.Append("               MENUMST.DISPLAY_ORDER,");
            sb.Append("               MENUMST.MENU_LEVEL,");
            sb.Append("               NVL(MENUMST.MENU_URL, '') MENU_URL,");
            //sb.Append("               MENUMST.MENU_ID,")
            sb.Append("               MENUTXT.MENU_TEXT MENU_ID,");
            sb.Append("               MENUTXT.MENU_TEXT,");
            sb.Append("               MENUTXT.MENU_TOOL_TIP,");
            sb.Append("               MENUMST.MENU_MST_FK");
            sb.Append("          FROM MENU_MST_TBL      MENUMST,");
            sb.Append("               MENU_TEXT_MST_TBL MENUTXT,");
            sb.Append("               USER_ACCESS_TRN   USRACC");
            sb.Append("         WHERE MENUTXT.MENU_MST_FK = MENUMST.MENU_MST_PK");
            sb.Append("           AND NVL(MENUMST.ACTIVE_FLAG, 1) = 1");
            sb.Append("           AND USRACC.CONFIG_MST_FK(+) = MENUMST.CONFIG_ID_FK");
            sb.Append("           AND MENUTXT.ENVIRONMENT_MST_FK =" + lngEnvPk);
            sb.Append("           AND USRACC.LOCATION_MST_FK(+) =" + lngLocPk);
            sb.Append("           AND USRACC.USER_MST_FK(+) =" + lngUserPK);
            sb.Append("         ORDER BY MENUMST.DISPLAY_ORDER) QRY");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        public DataSet GetMenuData1(Int64 lngEnvPk, Int64 lngLocPk, Int64 lngUserPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            strSQL = strSQL + " SELECT ";
            strSQL = strSQL + " MENUMST.MENU_MST_PK,";
            strSQL = strSQL + " MENUMST.display_order,   ";
            strSQL = strSQL + " MENUMST.MENU_LEVEL,";
            strSQL = strSQL + " NVL(MENUMST.MENU_URL,'') MENU_URL,";
            strSQL = strSQL + " MENUMST.MENU_ID,";
            strSQL = strSQL + " MENUTXT.MENU_TEXT,";
            strSQL = strSQL + " MENUTXT.MENU_TOOL_TIP, ";
            strSQL = strSQL + " MENUMST.MENU_MST_FK ,";
            strSQL = strSQL + " nvl(USRACC.ALLOWED_OPERATIONS_VALUE,0) TOTVAL,";
            strSQL += "NVL(( ";
            strSQL += " SELECT ";
            strSQL += " SUM(NVL(UACC.ALLOWED_OPERATIONS_VALUE,0)) ";
            strSQL += " FROM ";
            strSQL += " Menu_Mst_Tbl mmst, ";
            strSQL += " USER_ACCESS_TRN UACC ";
            strSQL += " WHERE ";
            strSQL += " UACC.CONFIG_MST_FK = MMST.CONFIG_ID_FK ";
            strSQL += "AND  SUBSTR(MMST.DISPLAY_ORDER,1,2) = RTRIM(MENUMST.DISPLAY_ORDER) ";
            strSQL += "AND uacc.user_mst_fk = " + lngUserPK + " ";
            strSQL += " AND uacc.location_mst_fk = " + lngLocPk + " ";
            strSQL += "  ),0) ALLVAL ";
            strSQL = strSQL + " FROM ";
            strSQL = strSQL + " MENU_MST_TBL MENUMST,";
            strSQL = strSQL + " MENU_TEXT_MST_TBL MENUTXT,";
            strSQL = strSQL + " USER_ACCESS_TRN USRACC";
            strSQL = strSQL + " WHERE";
            strSQL = strSQL + " MENUTXT.MENU_MST_FK = MENUMST.MENU_MST_PK";
            strSQL = strSQL + " and NVL(MENUMST.ACTIVE_FLAG,1) = 1";
            strSQL = strSQL + " AND USRACC.CONFIG_MST_FK(+) = MENUMST.CONFIG_ID_FK";
            strSQL = strSQL + " AND MENUTXT.ENVIRONMENT_MST_FK =  " + lngEnvPk;
            strSQL = strSQL + " AND USRACC.LOCATION_MST_FK(+) = " + lngLocPk;
            strSQL = strSQL + " AND USRACC.USER_MST_FK(+) = " + lngUserPK;
            //strSQL = strSQL & vbCrLf & " AND ( nvl(USRACC.ALLOWED_OPERATIONS_VALUE,0) > 0 OR MENUMST.MENU_LEVEL = 1)"
            strSQL = strSQL + " ORDER BY MENUMST.DISPLAY_ORDER";

            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
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

        #region "Fetch Function"

        public DataSet Fetch(Int16 P_Environment, Int64 P_User_PK = 1, bool SearchWith = false)
        {
            string strSQL = null;
            strSQL = string.Empty;
            strSQL += "Select MST.MENU_MST_PK, ";
            strSQL += " MST.MENU_MST_FK, ";
            strSQL += " NVL(Mtxt.MENU_Text,'')  MENU_Text , ";
            strSQL += " nvl(MST.CONFIG_ID_FK,0) CONFIG_ID_FK, ";
            strSQL += " NVL(CONFMST.APPLICABLE_OPERATIONS_VALUE,0) APPLICABLE_OPERATIONS_VALUE, ";
            strSQL += " NVL(UAT.ALLOWED_OPERATIONS_VALUE,15) ALLOWED_OPERATIONS_VALUE, ";
            strSQL += " NVL(UAT.USER_ACCESS_PK,0) USER_ACCESS_PK, ";
            strSQL += " NVL(UAT.VERSION_NO,0) VERSION_NO,  ";
            strSQL += " MST.Menu_Level  ";
            strSQL += " FROM Menu_MST_TBL MST,  ";
            strSQL += " Menu_Text_Mst_Tbl MTxt,  ";
            strSQL += " CONFIG_MST_TBL CONFMST , ";
            strSQL += " USER_ACCESS_TRN UAT ";
            strSQL = strSQL + " WHERE MTxt.Menu_Mst_FK(+) = Mst.Menu_Mst_Pk ";
            strSQL += "AND MTXT.MENU_TEXT IS NOT NULL ";
            strSQL = strSQL + " And MTxt.Environment_Mst_FK(+) = " + P_Environment + " ";
            strSQL = strSQL + " And CONFMST.CONFIG_MST_PK(+) =  Mst.CONFIG_ID_Fk ";
            strSQL = strSQL + " And CONFMST.CONFIG_MST_PK = UAT.Config_Mst_FK(+) ";
            strSQL = strSQL + " And UAT.User_Mst_FK(+) = " + P_User_PK + "";
            if (SearchWith)
            {
                strSQL = strSQL + " And Mst.CONFIG_ID_Fk is not null";
            }
            strSQL = strSQL + "ORDER BY Mst.DISPLAY_ORDER";
            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
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

        #endregion "Fetch Function"

        #endregion "List of Functions"
    }
}