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
using System.Linq;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_SALESPLAN : CommonFeatures
    {
        #region "Executive Enhance Search"
        public string FetchExecutive_All(string strcond, string loc = "0")
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strReq = null;
            string strSERACH_IN = null;
            dynamic strNull = DBNull.Value;
            arr = strcond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EXECUTIVE_PKG.GET_EXECUTIVE_ALL";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        public string FetchExecutive_Other(string strcond, string loc = "0")
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strReq = null;
            string strSERACH_IN = null;
            dynamic strNull = DBNull.Value;
            arr = strcond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EXECUTIVE_PKG.GET_EXECUTIVE_OTHER";

                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }
        #endregion

        #region "To Know Executive Type"
        // This Will give that the User is Admin or Manager

        public DataSet GetExecutiveType(string EmpPK)
        {

            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //strQuery.Append(" SELECT DESIG.DESIGNATION_MST_PK " & vbCrLf)
                //strQuery.Append(" FROM USER_MST_TBL UMT, EMPLOYEE_MST_TBL EMT, DESIGNATION_MST_TBL DESIG, PARAMETERS_TBL PARAM " & vbCrLf)
                //strQuery.Append(" WHERE UMT.EMPLOYEE_MST_FK = EMT.EMPLOYEE_MST_PK AND EMT.DESIGNATION_MST_FK = DESIG.DESIGNATION_MST_PK " & vbCrLf)
                //strQuery.Append(" AND UMT.USER_MST_PK = '" & UserPK & "'")
                //strQuery.Append("" & vbCrLf)

                strQuery.Append(" SELECT DESIG.DESIGNATION_MST_PK ");
                strQuery.Append(" FROM EMPLOYEE_MST_TBL EMT, DESIGNATION_MST_TBL DESIG ");
                strQuery.Append(" WHERE EMT.DESIGNATION_MST_FK = DESIG.DESIGNATION_MST_PK ");
                strQuery.Append(" AND EMT.EMPLOYEE_MST_PK = " + EmpPK);

                return objWF.GetDataSet(strQuery.ToString());

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

        public DataSet ExecutiveID(string EmpPK)
        {

            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strQuery.Append(" SELECT EMP.EMPLOYEE_MST_PK, EMP.EMPLOYEE_ID, EMP.EMPLOYEE_NAME ");
                strQuery.Append(" FROM EMPLOYEE_MST_TBL EMP ");
                strQuery.Append(" WHERE EMP.EMPLOYEE_MST_PK = " + EmpPK);

                return objWF.GetDataSet(strQuery.ToString());

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

        public string GetUserPK(string EmpPK)
        {
            WorkFlow objWF = new WorkFlow();


            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

                sb.Append("SELECT UMT.USER_MST_PK");
                sb.Append("  FROM EMPLOYEE_MST_TBL EMT, USER_MST_TBL UMT");
                sb.Append(" WHERE UMT.EMPLOYEE_MST_FK = EMT.EMPLOYEE_MST_PK");
                sb.Append("   AND EMT.EMPLOYEE_MST_PK = " + EmpPK);


                return objWF.ExecuteScaler(sb.ToString());

            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }

        }

        public DataSet GetExecutiveID(string SalesPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append(" SELECT EMP.EMPLOYEE_MST_PK, EMP.EMPLOYEE_ID, EMP.EMPLOYEE_NAME, TO_CHAR(SALES.SALES_CALL_DT, 'DD/MM/YYYY') SALES_CALL_DT");
                strQuery.Append(" FROM SALES_CALL_TRN SALES, EMPLOYEE_MST_TBL EMP");
                strQuery.Append(" WHERE SALES.EMPLOYEE_MST_FK = EMP.EMPLOYEE_MST_PK ");
                strQuery.Append(" AND SALES.SALES_CALL_PK = '" + SalesPK + "'");
                strQuery.Append("");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        public DataSet Parameter()
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append(" SELECT PARAM.SALES_MANAGER SM, PARAM.SALES_EXECUTIVE SE FROM PARAMETERS_TBL PARAM");
                strQuery.Append("");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        #endregion

        #region "Function to check whether a user is an administrator or not"

        public int IsAdministrator()
        {

            string strSQL = null;
            Int16 Admin = default(Int16);
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
            strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
            strSQL = strSQL + "AND U.USER_MST_PK = " + HttpContext.Current.Session["USER_PK"];
            try
            {
                Admin = Convert.ToInt16(objWF.ExecuteScaler(strSQL.ToString()));
                if (Admin == 1)
                {
                    return 1;
                }
                else
                {
                    return 0;
                }

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
        #endregion

        #region "Fetch All"

        public DataSet FetchAll(string ExecPK, System.DateTime SelectedDt, int isAdmin, string From = "")
        {

            string strSQL = "";
            WorkFlow objWF = new WorkFlow();


            try
            {
                strSQL += " SELECT SPT.SPT_PK, ";
                strSQL += " SPT.HOURS || ' ' || SPT.MERIDIEM AS HOURS, ";
                strSQL += " SPT.TIME_SLOT, ";
                strSQL += " QSCT.SALES_CALL_PK, ";
                if (isAdmin>0)
                {
                    strSQL += " CASE WHEN QSCT.SALES_CALL_PK IS NULL OR QSCT.SALES_CALL_PK = '' THEN '' ELSE QSCT.SUBJECT || ' ( ' || QSCT.EMPLOYEE_NAME || ', ' || QSCT.CUSTOMER_NAME || ' ) ( ' || QSCT.FR_TIME || ' To ' || QSCT.TO_TIME || ' )' END AS SUBJECT, ";
                }
                else
                {
                    strSQL += " CASE WHEN QSCT.SALES_CALL_PK IS NULL OR QSCT.SALES_CALL_PK = '' THEN '' ELSE QSCT.SUBJECT || ' ( ' || QSCT.EMPLOYEE_NAME || ', ' || QSCT.CUSTOMER_NAME || ' ) ( ' || QSCT.FR_TIME || ' To ' || QSCT.TO_TIME || ' )' END AS SUBJECT, ";
                }

                strSQL += " QSCT.PLAN_STATUS, QSCT.CUSTOMER_MST_FK ";
                strSQL += " FROM (SELECT ";

                strSQL += " FR_TIME, TO_TIME, PLAN_STATUS,SUBJECT,SALES_CALL_PK, SUBSTR(FR_TIME, 1, 3) || CASE ";
                strSQL += " WHEN SUBSTR(FR_TIME, 4, 2) >= 30 THEN ";
                strSQL += " '30' ";
                strSQL += " ELSE ";
                strSQL += " '00' ";
                strSQL += " END AS ROUND_FROMTIME, ";
                strSQL += "  ";
                strSQL += " CASE ";
                strSQL += " WHEN TO_NUMBER(SUBSTR(TO_TIME, 4, 2)) = 0 THEN ";
                strSQL += " TO_TIME ";
                strSQL += " WHEN TO_NUMBER(SUBSTR(TO_TIME, 4, 2)) > 0 AND TO_NUMBER(SUBSTR(TO_TIME, 4, 2)) <= 30 THEN ";
                strSQL += " SUBSTR(TO_TIME, 1, 3) || '30' ";
                strSQL += " WHEN TO_NUMBER(SUBSTR(TO_TIME, 4, 2)) > 30 AND TO_NUMBER(SUBSTR(TO_TIME, 4, 2)) <= 59 THEN ";
                strSQL += " LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(TO_TIME, 1, 2)) + 1, '00')) || ':00' ";
                strSQL += " END AS ROUND_TOTIME, EMT.EMPLOYEE_NAME, SCT.CUSTOMER_MST_FK, CMT.CUSTOMER_NAME ";

                strSQL += " FROM SALES_CALL_TRN SCT, EMPLOYEE_MST_TBL EMT, USER_MST_TBL     UMT, CUSTOMER_MST_TBL CMT ";
                strSQL += "    WHERE 1 = 1 ";
                if (SelectedDt != DateTime.MinValue)
                {
                    strSQL += "  AND SALES_CALL_DT = TO_date('" + SelectedDt + "','" + dateFormat + "') ";
                }
                strSQL += " AND EMT.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK AND CMT.CUSTOMER_MST_PK = SCT.CUSTOMER_MST_FK";

                strSQL += " AND SCT.CREATED_BY_FK = UMT.USER_MST_PK AND UMT.DEFAULT_LOCATION_FK IN";
                strSQL += " (SELECT L.LOCATION_MST_PK";
                strSQL += " FROM LOCATION_MST_TBL L";
                strSQL += " START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"];
                strSQL += " CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)";

                if (isAdmin == 1)
                {
                    if (ExecPK != HttpContext.Current.Session["EMP_PK"])
                    {
                        strSQL += " AND SCT.EMPLOYEE_MST_FK = " + ExecPK;
                    }
                }
                else
                {
                    strSQL += " AND SCT.EMPLOYEE_MST_FK = " + ExecPK;
                }

                strSQL += " UNION SELECT SUBSTR(SCF.FOLLOWUP_TIME, 1, 3) || CASE";
                strSQL += "         WHEN SUBSTR(SCF.FOLLOWUP_TIME, 4, 2) >= 30 THEN";
                strSQL += "          '30'";
                strSQL += "         ELSE";
                strSQL += "          '00'";
                strSQL += "       END AS FR_TIME,";
                strSQL += "       CASE";
                strSQL += "         WHEN TO_NUMBER(SUBSTR(SCF.FOLLOWUP_TIME, 4, 2)) >= 0 AND";
                strSQL += "              TO_NUMBER(SUBSTR(SCF.FOLLOWUP_TIME, 4, 2)) <= 30 THEN";
                strSQL += "          SUBSTR(SCF.FOLLOWUP_TIME, 1, 3) || '30'";
                strSQL += "         WHEN TO_NUMBER(SUBSTR(SCF.FOLLOWUP_TIME, 4, 2)) > 30 AND";
                strSQL += "              TO_NUMBER(SUBSTR(SCF.FOLLOWUP_TIME, 4, 2)) <= 59 THEN";
                strSQL += "          LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(SCF.FOLLOWUP_TIME, 1, 2)) + 1, '00')) ||";
                strSQL += "          ':00'";
                strSQL += "       END AS TO_TIME,";
                strSQL += "       PLAN_STATUS,";
                strSQL += "       SUBJECT,";
                strSQL += "       SALES_CALL_PK,";
                strSQL += "       SUBSTR(SCF.FOLLOWUP_TIME, 1, 3) || CASE";
                strSQL += "         WHEN SUBSTR(SCF.FOLLOWUP_TIME, 4, 2) >= 30 THEN";
                strSQL += "          '30'";
                strSQL += "         ELSE";
                strSQL += "          '00'";
                strSQL += "       END AS ROUND_FROMTIME,";
                strSQL += "       CASE";
                strSQL += "         WHEN TO_NUMBER(SUBSTR(SCF.FOLLOWUP_TIME, 4, 2)) >= 0 AND";
                strSQL += "              TO_NUMBER(SUBSTR(SCF.FOLLOWUP_TIME, 4, 2)) <= 30 THEN";
                strSQL += "          SUBSTR(SCF.FOLLOWUP_TIME, 1, 3) || '30'";
                strSQL += "         WHEN TO_NUMBER(SUBSTR(SCF.FOLLOWUP_TIME, 4, 2)) > 30 AND";
                strSQL += "              TO_NUMBER(SUBSTR(SCF.FOLLOWUP_TIME, 4, 2)) <= 59 THEN";
                strSQL += "          LTRIM(TO_CHAR(TO_NUMBER(SUBSTR(SCF.FOLLOWUP_TIME, 1, 2)) + 1, '00')) ||";
                strSQL += "          ':00'";
                strSQL += "       END AS ROUND_TOTIME,";
                strSQL += "       EMT.EMPLOYEE_NAME,";
                strSQL += "       SCT.CUSTOMER_MST_FK,";
                strSQL += "       CMT.CUSTOMER_NAME";
                strSQL += "  FROM SALES_CALL_TRN      SCT,";
                strSQL += "       SALES_CALL_FOLLOWUP SCF,";
                strSQL += "       EMPLOYEE_MST_TBL    EMT,";
                strSQL += "       CUSTOMER_MST_TBL    CMT";
                strSQL += " WHERE SCT.SALES_CALL_PK = SCF.SALES_CALL_FK";
                strSQL += "   AND EMT.EMPLOYEE_MST_PK = SCF.FOLLOWUP_BY";
                strSQL += "   AND CMT.CUSTOMER_MST_PK = SCT.CUSTOMER_MST_FK";
                if (SelectedDt != DateTime.MinValue)
                {
                    strSQL += "   AND SCF.FOLLOWUP_DATE = TO_date('" + SelectedDt + "','" + dateFormat + "')";
                }


                if (isAdmin == 1)
                {
                    if (ExecPK != HttpContext.Current.Session["EMP_PK"])
                    {
                        strSQL += " AND SCF.FOLLOWUP_BY = " + ExecPK;
                    }
                }
                else
                {
                    strSQL += " AND SCF.FOLLOWUP_BY = " + ExecPK;
                }

                strSQL += ") QSCT, SALES_PLAN_TIMESLOT SPT ";

                strSQL += " WHERE SPT.TIME_STARTS >= QSCT.ROUND_FROMTIME(+) ";
                strSQL += " AND SPT.TIME_ENDS <= QSCT.ROUND_TOTIME(+) ";

                strSQL += " ORDER BY SPT.SPT_PK ";

                return objWF.GetDataSet(strSQL);

            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }

        }
        #endregion

        #region "Fetch Month View"
        public DataSet FetchMonthWeekView(string ExecPK, System.DateTime fromDate, System.DateTime toDate, bool isAdmin)
        {
            string strSQL = "";
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += " SELECT ";
                strSQL += " SCT.SALES_CALL_ID, ";
                strSQL += " SCT.SALES_CALL_PK, ";
                strSQL += " SCT.FR_TIME, ";
                strSQL += " SCT.TO_TIME, ";
                strSQL += " CMT.CUSTOMER_NAME , ";
                strSQL += " DECODE(SCT.PLAN_STATUS, '1','Pending','2','Approved','3','Rejected','4','Cancelled','5','Closed','','Pending') PLAN_STATUS, ";
                strSQL += " TO_CHAR(SCT.SALES_CALL_DT,dateformat) SALES_CALL_DT ";
                strSQL += " FROM SALES_CALL_TRN SCT, USER_MST_TBL UMT, CUSTOMER_MST_TBL CMT ";
                strSQL += " WHERE SCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) ";

                if (!isAdmin)
                {
                    strSQL += " AND SCT.EMPLOYEE_MST_FK = '" + ExecPK + "'";
                }
                strSQL += " AND SCT.CREATED_BY_FK = UMT.USER_MST_PK ";
                strSQL += " AND UMT.DEFAULT_LOCATION_FK IN (SELECT L.LOCATION_MST_PK";
                strSQL += " FROM LOCATION_MST_TBL L";
                strSQL += " START WITH L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"];
                strSQL += " CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)";
                if (fromDate != DateTime.MinValue & toDate != DateTime.MinValue)
                {
                    strSQL += " AND SCT.SALES_CALL_DT >= TO_DATE('" + fromDate + "', dateformat) AND";
                    strSQL += " SCT.SALES_CALL_DT <= TO_DATE('" + toDate + "', dateformat)";
                }
                strSQL += " ORDER BY SCT.FR_TIME";
                return objWF.GetDataSet(strSQL);
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        #endregion

        #region "Fetch My Customer"
        public DataSet FetchMyCustomer(string ExecPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //strQuery.Append(" SELECT CMT.CUSTOMER_MST_PK ""HIDDEN"", CMT.CUSTOMER_NAME" & vbCrLf)
                //strQuery.Append(" FROM CUSTOMER_MST_TBL CMT," & vbCrLf)
                //strQuery.Append(" USER_MST_TBL     UMT," & vbCrLf)
                //strQuery.Append(" EMPLOYEE_MST_TBL EMT," & vbCrLf)
                //strQuery.Append(" LOCATION_MST_TBL LMT" & vbCrLf)
                //strQuery.Append(" WHERE CMT.REP_EMP_MST_FK = EMT.EMPLOYEE_MST_PK" & vbCrLf)
                //strQuery.Append(" AND EMT.EMPLOYEE_MST_PK = UMT.EMPLOYEE_MST_FK" & vbCrLf)
                //strQuery.Append(" AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK" & vbCrLf)
                //strQuery.Append(" AND UMT.USER_MST_PK = '" & UserPK & "'")

                strQuery.Append("SELECT ROWNUM SR_NO,q.* FROM ");
                strQuery.Append(" (SELECT CMT.CUSTOMER_MST_PK \"HIDDEN\", CMT.CUSTOMER_NAME, CCD.ADM_CONTACT_PERSON PIC");
                strQuery.Append(" FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD, EMPLOYEE_MST_TBL EMT");
                strQuery.Append(" WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+)");
                strQuery.Append(" AND CMT.ACTIVE_FLAG=1 ");
                //adding by thiyagarajan on 22/12/08 to avoid display inactive cust.
                strQuery.Append(" AND CMT.REP_EMP_MST_FK = EMT.EMPLOYEE_MST_PK");
                strQuery.Append(" AND EMT.EMPLOYEE_MST_PK = '" + ExecPK + "'");
                strQuery.Append(" ORDER BY CMT.CUSTOMER_NAME) q");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        #endregion

        #region " Fetch Customer "

        public DataSet FetchCustomer(string LocPK, string ExePK, int CustCatagory, long CustPk = 0, bool ShowAll = false)
        {

            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                if (ShowAll == true)
                {
                    //' New Customer
                    if (CustCatagory == 1)
                    {
                        strQuery.Append(" SELECT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME ");
                        strQuery.Append(" FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD ");
                        strQuery.Append(" WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) ");
                        strQuery.Append(" AND CMT.ACTIVE_FLAG = 1 AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                        strQuery.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk);
                        strQuery.Append(" ORDER BY CMT.CUSTOMER_NAME ");
                        //' My Customer
                    }
                    else if (CustCatagory == 2)
                    {
                        strQuery.Append(" SELECT DISTINCT * ");
                        strQuery.Append("   FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME ");
                        strQuery.Append("           FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD ");
                        strQuery.Append("          WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND CMT.ACTIVE_FLAG = 1 AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " AND CMT.REP_EMP_MST_FK = " + ExePK);
                        strQuery.Append("         UNION ");
                        strQuery.Append("         SELECT DISTINCT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME ");
                        strQuery.Append("           FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD ");
                        strQuery.Append("          WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND CMT.ACTIVE_FLAG = 1 AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " AND CMT.REP_EMP_MST_FK = " + ExePK + ") ");
                        //' Missing Customer
                    }
                    else if (CustCatagory == 3)
                    {
                        strQuery.Append(" SELECT DISTINCT QRY.* ");
                        strQuery.Append("   FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME ");
                        strQuery.Append("           FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD, BOOKING_MST_TBL BST ");
                        strQuery.Append("          WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND BST.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK  AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                        strQuery.Append("            AND BST.BOOKING_DATE < SYSDATE - 30 AND CMT.ACTIVE_FLAG = 1)QRY ");
                        strQuery.Append("  WHERE QRY.CUSTOMER_MST_PK NOT IN ");
                        strQuery.Append("        (SELECT DISTINCT * ");
                        strQuery.Append("           FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK ");
                        strQuery.Append("                   FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD, BOOKING_MST_TBL BST ");
                        strQuery.Append("                  WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND BST.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK  AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                        strQuery.Append("                    AND BST.BOOKING_DATE >= SYSDATE - 30 AND CMT.ACTIVE_FLAG = 1)) ");
                        //' Top Customer
                    }
                    else if (CustCatagory == 4)
                    {
                        strQuery.Append(" SELECT CUSTOMER_MST_PK, CUSTOMER_NAME FROM (SELECT CUSTOMER_MST_PK, CUSTOMER_NAME, REVENUE FROM (SELECT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME, SUM(FETCH_JOB_CARD_SEA_EXP_ACTREV(JOB.JOB_CARD_TRN_PK, " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ")) REVENUE ");
                        strQuery.Append(" FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD, JOB_CARD_TRN JOB, BOOKING_MST_TBL BKG ");
                        strQuery.Append(" WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK AND BKG.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK AND CMT.ACTIVE_FLAG = 1 AND BKG.STATUS = 2 GROUP BY CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME) ORDER BY REVENUE DESC) WHERE ROWNUM <= 30 ");
                        //' Negative Customer
                    }
                    else if (CustCatagory == 5)
                    {
                        strQuery.Append(" SELECT CUSTOMER_MST_PK, CUSTOMER_NAME FROM (SELECT CUSTOMER_MST_PK, CUSTOMER_NAME, REVENUE FROM (SELECT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME, SUM(FETCH_JOB_CARD_SEA_EXP_ACTREV(JOB.JOB_CARD_TRN_PK, " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " )) REVENUE ");
                        strQuery.Append(" FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD, JOB_CARD_TRN JOB, BOOKING_MST_TBL BKG ");
                        strQuery.Append(" WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK AND BKG.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK AND CMT.ACTIVE_FLAG = 1 AND BKG.STATUS = 2 GROUP BY CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME) ORDER BY REVENUE) WHERE ROWNUM <= 30 ");
                    }
                    else
                    {
                        strQuery.Append(" SELECT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME ");
                        strQuery.Append(" FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD ");
                        strQuery.Append(" WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) ");
                        strQuery.Append(" AND CMT.ACTIVE_FLAG = 1 AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                        strQuery.Append(" ORDER BY CMT.CUSTOMER_NAME ");
                    }
                }
                else
                {
                    //' New Customer
                    if (CustCatagory == 0)
                    {
                        strQuery.Append(" SELECT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME ");
                        strQuery.Append(" FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD ");
                        strQuery.Append(" WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) ");
                        strQuery.Append(" AND CMT.ACTIVE_FLAG = 1 AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " and cmt.rep_emp_mst_fk=" + ExePK);
                        strQuery.Append(" AND CMT.CUSTOMER_MST_PK = " + CustPk);
                        strQuery.Append(" ORDER BY CMT.CUSTOMER_NAME ");
                        //' My Customer
                    }
                    else if (CustCatagory == 1)
                    {
                        strQuery.Append(" SELECT DISTINCT * ");
                        strQuery.Append("   FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME ");
                        strQuery.Append("           FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD ");
                        strQuery.Append("          WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND CMT.ACTIVE_FLAG = 1 AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " AND CMT.REP_EMP_MST_FK = " + ExePK);
                        strQuery.Append("         UNION ");
                        strQuery.Append("         SELECT DISTINCT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME ");
                        strQuery.Append("           FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD ");
                        strQuery.Append("          WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND CMT.ACTIVE_FLAG = 1 AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " AND CMT.REP_EMP_MST_FK = " + ExePK + ") ");
                        //' Missing Customer
                    }
                    else if (CustCatagory == 2)
                    {
                        strQuery.Append(" SELECT DISTINCT QRY.* ");
                        strQuery.Append("   FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME ");
                        strQuery.Append("           FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD, BOOKING_MST_TBL BST ");
                        strQuery.Append("          WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND BST.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK  AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " and cmt.rep_emp_mst_fk=" + ExePK);
                        strQuery.Append("            AND BST.BOOKING_DATE < SYSDATE - 30 AND CMT.ACTIVE_FLAG = 1) QRY ");
                        strQuery.Append("  WHERE QRY.CUSTOMER_MST_PK NOT IN ");
                        strQuery.Append("        (SELECT DISTINCT * ");
                        strQuery.Append("           FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK ");
                        strQuery.Append("                   FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD, BOOKING_MST_TBL BST ");
                        strQuery.Append("                  WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND BST.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK  AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " and cmt.rep_emp_mst_fk=" + ExePK);
                        strQuery.Append("                    AND BST.BOOKING_DATE >= SYSDATE - 30 AND CMT.ACTIVE_FLAG = 1)) ");
                        //' Top Customer
                    }
                    else if (CustCatagory == 3)
                    {
                        strQuery.Append(" SELECT CUSTOMER_MST_PK, CUSTOMER_NAME FROM (SELECT CUSTOMER_MST_PK, CUSTOMER_NAME, REVENUE FROM (SELECT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME, SUM(FETCH_JOB_CARD_SEA_EXP_ACTREV(JOB.JOB_CARD_TRN_PK, " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ")) REVENUE ");
                        strQuery.Append(" FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD, JOB_CARD_TRN JOB, BOOKING_MST_TBL BKG ");
                        strQuery.Append(" WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " and cmt.rep_emp_mst_fk=" + ExePK + "  AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK AND BKG.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK AND CMT.ACTIVE_FLAG = 1 AND BKG.STATUS = 2 GROUP BY CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME) ORDER BY REVENUE DESC) WHERE ROWNUM <= 30 ");
                        // Negative Customer
                    }
                    else if (CustCatagory == 4)
                    {
                        strQuery.Append(" SELECT CUSTOMER_MST_PK, CUSTOMER_NAME FROM (SELECT CUSTOMER_MST_PK, CUSTOMER_NAME, REVENUE FROM (SELECT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME, SUM(FETCH_JOB_CARD_SEA_EXP_ACTREV(JOB.JOB_CARD_TRN_PK, " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ")) REVENUE ");
                        strQuery.Append(" FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD, JOB_CARD_TRN JOB, BOOKING_MST_TBL BKG ");
                        strQuery.Append(" WHERE CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND CCD.ADM_LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " and cmt.rep_emp_mst_fk=" + ExePK + " AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK AND BKG.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK AND CMT.ACTIVE_FLAG = 1 AND BKG.STATUS = 2 GROUP BY CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_NAME) ORDER BY REVENUE) WHERE ROWNUM <= 30 ");
                    }
                }
                return objWF.GetDataSet(strQuery.ToString());

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

        public DataSet FetchCustomerDetail(long CustPK)
        {

            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();


            try
            {
                strQuery.Append(" SELECT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_ID, CMT.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK = " + CustPK);

                return objWF.GetDataSet(strQuery.ToString());

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

        #endregion

        #region "Save"
        public ArrayList Save(DataSet G_DataSet, System.DateTime SalesCallDt, string ExecutivePK, ref string Location, Int64 ConfigPK, bool blnRecurring, int Frequency, int Days, string StartDate, string EndDate,
        ref int strPK)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();

            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            bool chkflag = false;
            //Dim SALES_CALL_ID As String

            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            try
            {
                WorkFlow objWS = new WorkFlow();
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                DtTbl = G_DataSet.Tables[0];

                objWS.MyCommand.CommandType = CommandType.StoredProcedure;

                var _with3 = insCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".EN_EXECUTIVE_PKG.SAL_PLAN_INS";
                _with3.Parameters.Clear();
                var _with4 = _with3.Parameters;
                insCommand.Parameters.Add("SALES_CALL_ID_IN", OracleDbType.Varchar2, 20, "SALES_CALL_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["SALES_CALL_ID_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("FR_TIME_IN", OracleDbType.Varchar2, 10, "FR_TIME").Direction = ParameterDirection.Input;
                insCommand.Parameters["FR_TIME_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("TO_TIME_IN", OracleDbType.Varchar2, 10, "TO_TIME").Direction = ParameterDirection.Input;
                insCommand.Parameters["TO_TIME_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "CUSTOMER_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("PERSON_TO_MEET_PK_IN", OracleDbType.Int32, 10, "PERSON_TO_MEET_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["PERSON_TO_MEET_PK_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("PLAN_STATUS_IN", OracleDbType.Int32, 1, "PLAN_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["PLAN_STATUS_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("SALES_CALL_DT_IN", getDefault(Convert.ToDateTime(SalesCallDt), DBNull.Value)).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 200, "NOTES").Direction = ParameterDirection.Input;
                insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("SUBJECT_IN", OracleDbType.Varchar2, 200, "SUBJECT").Direction = ParameterDirection.Input;
                insCommand.Parameters["SUBJECT_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("REM_IN", OracleDbType.Varchar2, 200, "REMARKS").Direction = ParameterDirection.Input;
                insCommand.Parameters["REM_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("EXECUTIVE_FK_IN", ExecutivePK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "SALES_CALL_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with5 = updCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".EN_EXECUTIVE_PKG.SAL_PLAN_UPD";
                _with5.Parameters.Clear();
                var _with6 = _with5.Parameters;
                updCommand.Parameters.Add("SALES_CALL_PK_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["SALES_CALL_PK"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["SALES_CALL_PK"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("SALES_CALL_ID_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["SALES_CALL_ID"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["SALES_CALL_ID"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("FR_TIME_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["FR_TIME"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["FR_TIME"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("TO_TIME_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["TO_TIME"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["TO_TIME"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("PERSON_TO_MEET_PK_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["PERSON_TO_MEET_PK"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["PERSON_TO_MEET_PK"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("PLAN_STATUS_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["PLAN_STATUS"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["PLAN_STATUS"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("SALES_CALL_DT_IN", getDefault(Convert.ToDateTime(SalesCallDt), DBNull.Value)).Direction = ParameterDirection.Input;
                if (!string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["NOTES"].ToString()))
                {
                    updCommand.Parameters.Add("REMARKS_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["NOTES"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["NOTES"])).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("REMARKS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                // updCommand.Parameters.Add("SUBJECT_IN", IIf(IsDBNull(G_DataSet.Tables(0).Rows(0).Item("SUBJECT")), DBNull.Value, G_DataSet.Tables(0).Rows(0).Item("SUBJECT"))).Direction = ParameterDirection.Input
                updCommand.Parameters.Add("SUBJECT_IN", getDefault(G_DataSet.Tables[0].Rows[0]["SUBJECT"], "")).Direction = ParameterDirection.Input;
                //updCommand.Parameters["SUBJECT_IN"].SourceVersion = DataRowVersion.Current

                if (!string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["REMARKS"].ToString()))
                {
                    updCommand.Parameters.Add("REM_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["REMARKS"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["REMARKS"])).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("REM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters.Add("EXECUTIVE_FK_IN", ExecutivePK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                if (!string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["APPROVER"].ToString()))
                {
                    updCommand.Parameters.Add("REMARKS_APPROVER_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["APPROVER"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["APPROVER"])).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("REMARKS_APPROVER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                

                var _with7 = objWK.MyDataAdapter;
                Int32 pkd = default(Int32);
                pkd = Convert.ToInt32(string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["SALES_CALL_PK"].ToString()) ? 0 : G_DataSet.Tables[0].Rows[0]["SALES_CALL_PK"]);
                if (pkd <= 0)
                {
                    _with7.InsertCommand = insCommand;
                    _with7.InsertCommand.Transaction = TRAN;
                    //RecAfct = .InsertCommand.ExecuteNonQuery()
                    RecAfct = _with7.Update(G_DataSet);
                    strPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                }
                else
                {
                    _with7.UpdateCommand = updCommand;
                    _with7.UpdateCommand.Transaction = TRAN;
                    RecAfct = _with7.UpdateCommand.ExecuteNonQuery();
                    strPK = Convert.ToInt32(string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["SALES_CALL_PK"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["SALES_CALL_PK"]);
                }

                if (RecAfct > 0)
                {
                    if (blnRecurring == true & Convert.ToInt32(G_DataSet.Tables[0].Rows[0]["PLAN_STATUS"]) == 1)
                    {
                        //If G_DataSet.Tables(0).Rows(0).Item("PLAN_STATUS") = 1 Then
                        saveRecurring(G_DataSet, Convert.ToString(strPK), ExecutivePK, Location, ConfigPK, blnRecurring, Frequency, Days, StartDate, EndDate,TRAN);
                    }
                    else if (blnRecurring == false & Convert.ToInt32(G_DataSet.Tables[0].Rows[0]["PLAN_STATUS"]) == 1)
                    {
                        removeRecurring(Convert.ToString(strPK));
                    }
                    if (arrMessage.Count == 0)
                    {
                        TRAN.Commit();
                        arrMessage.Add("All Data Saved Successfully");
                        arrMessage.Add(strPK);
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                else
                {
                    TRAN.Rollback();
                }

            }
            catch (OracleException oraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(oraEx.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }

            //objWK.MyDataAdapter.ContinueUpdateOnError = True

            //With objWK.MyDataAdapter
            //    Dim pkd As Int32
            //    pkd = IIf(IsDBNull(G_DataSet.Tables(0).Rows(0).Item("SALES_CALL_PK")), 0, G_DataSet.Tables(0).Rows(0).Item("SALES_CALL_PK"))
            //    If pkd <= 0 Then
            //        .InsertCommand = insCommand
            //        .InsertCommand.Transaction = TRAN
            //        'RecAfct = .InsertCommand.ExecuteNonQuery()
            //        RecAfct = .Update(G_DataSet)
            //        strPK = insCommand.Parameters["RETURN_VALUE"].Value
            //    Else
            //        .UpdateCommand = updCommand
            //        .UpdateCommand.Transaction = TRAN
            //        RecAfct = .UpdateCommand.ExecuteNonQuery()
            //        strPK = IIf(IsDBNull(G_DataSet.Tables(0).Rows(0).Item("SALES_CALL_PK")), DBNull.Value, G_DataSet.Tables(0).Rows(0).Item("SALES_CALL_PK"))
            //    End If

            //    '''.InsertCommand = insCommand
            //    '''.InsertCommand.Transaction = TRAN
            //    '''.UpdateCommand = updCommand
            //    '''.UpdateCommand.Transaction = TRAN
            //    ''''Added by minakshi on 16-feb-09 for protocal rollbacking
            //    '''If Not IsNothing(G_DataSet.GetChanges(DataRowState.Added)) Then
            //    '''    chkflag = True
            //    '''Else
            //    '''    chkflag = False
            //    '''End If
            //    ''''Ended by minakshi

            //    '''RecAfct = .Update(G_DataSet)
            //    ''''modified by latha for commiting the tran only on sucessful save.

            //    If RecAfct > 0 Then
            //        If blnRecurring = True And G_DataSet.Tables(0).Rows(0).Item("PLAN_STATUS") = 1 Then
            //            saveRecurring(G_DataSet, strPK, ExecutivePK, Location, EmpPk, ConfigPK, _
            //                          blnRecurring, Frequency, Days, StartDate, EndDate, UserPk, TRAN)
            //        End If

            //        TRAN.Commit()
            //    Else
            //        TRAN.Rollback()
            //        'Added by minakshi on 16-feb-09 for protocal rollbacking
            //        'If chkflag Then
            //        '    RollbackProtocolKey("SALES CALL", HttpContext.Current.Session["LOGED_IN_LOC_FK"], HttpContext.Current.Session["EMP_PK"], G_DataSet.Tables(0).Rows(0).Item("SALES_CALL_ID"), Date.Now)
            //        'End If
            //        'Ended by minakshi
            //    End If
            //    If arrMessage.Count > 0 Then
            //        Return arrMessage
            //    Else
            //        arrMessage.Add("All Data Saved Successfully")
            //        Return arrMessage
            //    End If

            //End With
            //Catch oraexp As OracleException
            //    Throw oraexp
            //Catch ex As Exception
            //    Throw ex
            //Finally
            //    objWK.MyConnection.Close()
            //End Try
            return new ArrayList();
        }
        #endregion

        #region " Save Recurring Sales Plan "

        private long removeRecurring(string SC_PK)
        {

            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;

            DS = objWK.GetDataSet("SELECT SRT.RECURRING_PK FROM SALES_PLAN_RECURRING_TRN SRT WHERE SRT.SALES_CALL_FK = " + SC_PK);

            if (DS.Tables[0].Rows.Count > 0)
            {
                objWK.ExecuteCommands("DELETE FROM SALES_CALL_TRN SCT WHERE SCT.PLAN_STATUS = 1 AND SCT.RECURRING_FK = " + DS.Tables[0].Rows[0]["RECURRING_PK"]);
                objWK.ExecuteCommands("DELETE FROM SALES_PLAN_RECURRING_TRN SCT WHERE SCT.RECURRING_PK = " + DS.Tables[0].Rows[0]["RECURRING_PK"]);
            }
            return 0;
        }

        private long saveRecurring(DataSet G_DataSet, string SC_PK, string ExecutivePK, string Location, Int64 ConfigPK, bool blnRecurring, int Frequency, int Days, string StartDate, string EndDate,
      OracleTransaction TRAN)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.MyConnection = TRAN.Connection;

            Int32 RecAfct = default(Int32);

            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            try
            {
                WorkFlow objWS = new WorkFlow();
                int i = 0;

                objWS.MyCommand.CommandType = CommandType.StoredProcedure;

                var _with8 = insCommand;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".EN_EXECUTIVE_PKG.RECURRING_DETAILS_INS";
                _with8.Parameters.Clear();
                var _with9 = _with8.Parameters;
                insCommand.Parameters.Add("SALES_CALL_FK_IN", SC_PK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("FREQUENCY_IN", Frequency).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("DAYS_IN", Days).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("STARTDATE_IN", Convert.ToDateTime(StartDate)).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("ENDDATE_IN", Convert.ToDateTime(EndDate)).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CHK_RECURRING_IN", (blnRecurring == true ? 1 : 0)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("FR_TIME_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["FR_TIME"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["FR_TIME"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("TO_TIME_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["TO_TIME"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["TO_TIME"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("PERSON_TO_MEET_PK_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["PERSON_TO_MEET_PK"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["PERSON_TO_MEET_PK"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("PLAN_STATUS_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["PLAN_STATUS"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["PLAN_STATUS"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("REMARKS_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["NOTES"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["NOTES"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("SUBJECT_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["SUBJECT"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["SUBJECT"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("LOCATION_MST_FK_IN", Location).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("EMPLOYEE_MST_FK_IN", ExecutivePK).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("USER_MST_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;

                //insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with10 = updCommand;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".EN_EXECUTIVE_PKG.RECURRING_DETAILS_UPD";
                _with10.Parameters.Clear();
                var _with11 = _with10.Parameters;
                updCommand.Parameters.Add("RECURRING_PK_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["RECURRING_PK"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["RECURRING_PK"])).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("SALES_CALL_FK_IN", SC_PK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("FREQUENCY_IN", Frequency).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("DAYS_IN", Days).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("STARTDATE_IN", Convert.ToDateTime(StartDate)).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("ENDDATE_IN", Convert.ToDateTime(EndDate)).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CHK_RECURRING_IN", (blnRecurring == true ? 1 : 0)).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("FR_TIME_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["FR_TIME"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["FR_TIME"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("TO_TIME_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["TO_TIME"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["TO_TIME"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("PERSON_TO_MEET_PK_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["PERSON_TO_MEET_PK"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["PERSON_TO_MEET_PK"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("PLAN_STATUS_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["PLAN_STATUS"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["PLAN_STATUS"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("REMARKS_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["NOTES"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["NOTES"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("SUBJECT_IN", (string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["SUBJECT"].ToString()) ? DBNull.Value : G_DataSet.Tables[0].Rows[0]["SUBJECT"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("LOCATION_MST_FK_IN", Location).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("EMPLOYEE_MST_FK_IN", ExecutivePK).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("USER_MST_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;

                //updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                

                var _with12 = objWK.MyDataAdapter;
                Int32 pkd = default(Int32);
                pkd = Convert.ToInt32(string.IsNullOrEmpty(G_DataSet.Tables[0].Rows[0]["RECURRING_PK"].ToString()) ? 0 : G_DataSet.Tables[0].Rows[0]["RECURRING_PK"]);

                if (pkd <= 0)
                {
                    _with12.InsertCommand = insCommand;
                    _with12.InsertCommand.Transaction = TRAN;
                    RecAfct = _with12.InsertCommand.ExecuteNonQuery();

                    return Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);

                }
                else
                {
                    _with12.UpdateCommand = updCommand;
                    _with12.UpdateCommand.Transaction = TRAN;
                    RecAfct = _with12.UpdateCommand.ExecuteNonQuery();

                    return pkd;
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
        }
        #endregion

        #region " Save ReVisit "

        public ArrayList SaveReVisit(long SalesCallPK, long Frequency, long Days, string StartDate, string EndDate)
        {

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            DataSet DS = null;
            long ExecutivePK = 0;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);


            try
            {
                sb.Append("SELECT SCT.FR_TIME,");
                sb.Append("       SCT.TO_TIME,");
                sb.Append("       SCT.CUSTOMER_MST_FK,");
                sb.Append("       SCT.PERSON_TO_MEET_PK,");
                sb.Append("       SCT.PLAN_STATUS,");
                sb.Append("       SCT.NOTES,");
                sb.Append("       SCT.SUBJECT,");
                sb.Append("       SCT.EMPLOYEE_MST_FK,");
                sb.Append("       SPRT.RECURRING_PK ");
                sb.Append("  FROM SALES_CALL_TRN SCT, SALES_PLAN_RECURRING_TRN SPRT ");
                sb.Append(" WHERE SPRT.SALES_CALL_FK(+) = SCT.SALES_CALL_PK and SCT.SALES_CALL_PK = " + SalesCallPK);

                DS = objWK.GetDataSet(sb.ToString());

                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();

                saveRecurring(DS, Convert.ToString(SalesCallPK), Convert.ToString(DS.Tables[0].Rows[0]["EMPLOYEE_MST_FK"]), Convert.ToString(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), ConfigurationPK, true, Convert.ToInt32(Frequency), Convert.ToInt32(Days), StartDate, EndDate, TRAN);

                if (arrMessage.Count == 0)
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

        }

        #endregion

        #region "Delete Function"
        public object DeleteRow(string strDelTrn)
        {
            string strSQL = null;
            string[] PK = strDelTrn.Split(',');
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL = " DELETE FROM SALES_CALL_TRN ";
                strSQL = strSQL + " WHERE SALES_CALL_PK IN (" + strDelTrn + ") ";
                return objWF.ExecuteScaler(strSQL.ToString());
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
        #endregion

        #region "Approval"
        public object Approve(string strPk, string approver, Int64 UserPk = 0)
        {
            string strSQL = null;
            string[] PK = strPk.Split(',');
            WorkFlow objWF = new WorkFlow();


            try
            {
                strSQL = " update sales_call_trn sct ";
                strSQL = strSQL + " set sct.PLAN_STATUS = 2, sct.CALL_STATUS = 1, sct.APPROVER_FK = '" + UserPk + "', sct.call_trend = 1 , SCT.REMARKS_APPROVER = '" + approver + "' ";
                strSQL = strSQL + " where sct.sales_call_pk in (" + strPk + ") ";

                return objWF.ExecuteScaler(strSQL.ToString());
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("Approved");
                    return arrMessage;
                }

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
        #endregion

        #region "Cancel"

        public object Cancel(string strPk)
        {

            string strSQL = null;
            string[] PK = strPk.Split(',');
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSQL = " update sales_call_trn sct ";
                strSQL = strSQL + " set sct.PLAN_STATUS = 4 ";
                strSQL = strSQL + " where sct.sales_call_pk in (" + strPk + ") ";

                return objWF.ExecuteScaler(strSQL.ToString());

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
        #endregion

        #region "Rejection"

        public object Reject(string strPk, string approver)
        {

            string strSQL = null;
            string[] PK = strPk.Split(',');
            WorkFlow objWF = new WorkFlow();


            try
            {
                strSQL = " update sales_call_trn sct ";
                strSQL = strSQL + " set sct.PLAN_STATUS = 3, SCT.REMARKS_APPROVER = '" + approver + "'  ";
                strSQL = strSQL + " where sct.sales_call_pk in (" + strPk + ") ";

                return objWF.ExecuteScaler(strSQL.ToString());

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
        #endregion

        #region "FetchPIC"
        public DataSet FetchPIC(int CustomerPK = 0)
        {
            string strSQL = "";
            DataSet Ds = new DataSet();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //strSQL &= vbCrLf & "SELECT CUST.NAME FROM customer_contact_trn CUST ORDER BY CUST.NAME"
                strSQL += "SELECT CCON.NAME FROM CUSTOMER_CONTACT_TRN CCON WHERE CCON.CUSTOMER_MST_FK = " + CustomerPK;
                strSQL += " ORDER BY CCON.NAME asc ";
                Ds = objWF.GetDataSet(strSQL);
                return Ds;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }
        #endregion

        #region " Protocol "

        public string GenerateServiceNo(long nLocationId, long nEmployeeId)
        {
            string functionReturnValue = null;

            functionReturnValue = GenerateProtocolKey("SALES CALL", nLocationId, nEmployeeId, DateTime.Now,"" ,"" ,"" , Convert.ToInt32(HttpContext.Current.Session["USER_PK"]));

            return functionReturnValue;

        }

        #endregion

        #region "FetchPreference"
        public DataSet fetchpreference(long pk)
        {
            string strSQL = null;
            strSQL = "SELECT COUNT(*)";
            strSQL += " From DOCUMENT_PREF_LOC_MST_TBL D, DOCUMENT_PREFERENCE_MST_TBL DP ";
            strSQL += "WHERE ";
            strSQL += " D.LOCATION_MST_FK = " + pk + " ";
            strSQL += "AND D.DOC_PREFERENCE_FK = DP.DOCUMENT_PREFERENCE_MST_PK";
            strSQL += "AND  DP.DOCUMENT_PREFERENCE_NAME='Sales Plan'";

            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
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

        #endregion


        #region " Sales Plan Duplication Query "

        public string CheckDuplicateSalePlan(string EmpPK, System.DateTime SelectedDt, string FromTime, string ToTime, string SP_PK = "-1")
        {

            string strSQL = "";

            WorkFlow objWF = new WorkFlow();


            try
            {
                strSQL += " SELECT COUNT(Q.SALES_CALL_PK) ";
                strSQL += " FROM SALES_CALL_TRN Q ";
                strSQL += " WHERE Q.SALES_CALL_PK <> " + SP_PK;
                strSQL += " AND Q.EMPLOYEE_MST_FK = " + EmpPK;
                strSQL += " AND Q.SALES_CALL_DT = TO_date('" + SelectedDt + "', '" + dateFormat + "') ";
                strSQL += " AND ((Q.FR_TIME >= '" + FromTime + "' AND Q.FR_TIME < '" + ToTime + "') OR ";
                strSQL += " (Q.TO_TIME > '" + FromTime + "' AND Q.TO_TIME <= '" + ToTime + "')) ";

                return objWF.ExecuteScaler(strSQL);

            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }

        }

        #endregion

        public string FetchCustomerID(string CustPK)
        {

            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strQuery.Append(" SELECT CMT.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK = " + CustPK);

                return objWF.ExecuteScaler(strQuery.ToString());

            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #region " Delete "
        public void DeleteSalesPlan(string strPk = "-1")
        {

            string strSQL = null;
            WorkFlow objWF = new WorkFlow();


            try
            {
                strSQL = " DELETE FROM SALES_CALL_TRN QSCT WHERE QSCT.PLAN_STATUS <> 2 AND ";
                strSQL = strSQL + " QSCT.RECURRING_FK IN (SELECT QSPRT.RECURRING_PK FROM ";
                strSQL = strSQL + " SALES_PLAN_RECURRING_TRN QSPRT ";
                strSQL = strSQL + " WHERE QSPRT.SALES_CALL_FK = " + strPk + ")";

                objWF.ExecuteCommands(strSQL.ToString());

                strSQL = " DELETE FROM SALES_CALL_TRN SCT WHERE SCT.SALES_CALL_PK = " + strPk;

                objWF.ExecuteCommands(strSQL.ToString());

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
        #endregion

        #region " Fetch All "

        public DataSet FetchSalesPlan(int SC_PK = -1)
        {

            string strSQL = "";
            WorkFlow objWF = new WorkFlow();


            try
            {
                strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
                strSQL += "(SELECT ";
                strSQL += "SCT.SALES_CALL_ID, ";
                strSQL += "SCT.SALES_CALL_PK, ";
                strSQL += "SCT.SALES_CALL_DT, ";
                strSQL += "SCT.FR_TIME, ";
                strSQL += "SCT.TO_TIME, ";
                strSQL += "SCT.CUSTOMER_MST_FK, ";
                strSQL += "CMT.CUSTOMER_NAME , ";
                strSQL += " CCT.CUST_CONTACT_PK PERSON_TO_MEET_PK ,";
                strSQL += " CCT.NAME PERSON_TO_MEET_NAME ,";
                strSQL += " CCT.DESIGNATION PERSON_TO_MEET_DESIGNATION,";
                strSQL += " SCT.PLAN_STATUS, ";
                strSQL += " SRM.SAL_CAL_REASON_MST_TBL_PK SAL_CAL_REASON_MST_FK,";
                strSQL += " SRM.SAL_CAL_REASON SAL_CAL_REASON , ";
                strSQL += " STM.SALES_CALL_TYPE_PK SALES_CALL_TYPE_FK,";
                strSQL += " STM.SALES_CALL_TYPE_ID, ";
                strSQL += " SCT.NOTES, ";
                strSQL += " '' remark, ";
                strSQL += " SCT.REMARKS_APPROVER approver , SCT.SUBJECT, ";
                strSQL += " '' Sel, ";
                strSQL += " QSPRT.RECURRING_PK, ";
                strSQL += " QSPRT.FREQUENCY, ";
                strSQL += " QSPRT.DAYS, ";
                strSQL += " QSPRT.STARTDATE, ";
                strSQL += " QSPRT.ENDDATE, ";
                strSQL += " QSPRT.CHK_RECURRING,";
                strSQL += " SCT.EMPLOYEE_MST_FK, ";
                strSQL += " EMT.EMPLOYEE_ID, ";
                strSQL += " EMT.EMPLOYEE_NAME, SCT.REMARKS ";
                strSQL += " FROM SALES_CALL_TRN SCT, CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_TRN CCT, SALES_PLAN_RECURRING_TRN QSPRT, EMPLOYEE_MST_TBL EMT, S_CALL_TYPE_MST_TBL STM, SAL_CAL_REASON_MST_TBL   SRM ";
                strSQL += " WHERE SCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) AND CCT.CUST_CONTACT_PK(+) = SCT.PERSON_TO_MEET_PK";
                strSQL += " AND QSPRT.SALES_CALL_FK(+) = SCT.SALES_CALL_PK AND SRM.SAL_CAL_REASON_MST_TBL_PK(+) = SCT.SALES_CALL_REASON_FK ";
                strSQL += " AND EMT.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK AND STM.SALES_CALL_TYPE_PK(+) = SCT.SALES_CALL_TYPE_FK ";
                strSQL += " AND SCT.SALES_CALL_PK = " + SC_PK;
                strSQL += ") q ";

                return objWF.GetDataSet(strSQL);
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        #endregion
    }
}