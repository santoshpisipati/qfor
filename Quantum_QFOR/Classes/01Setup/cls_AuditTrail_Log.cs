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
    internal class cls_AuditTrail_Log : CommonFeatures
    {
        #region "Fetch Audit LogDetails"

        /// <summary>
        /// FN_s the fetch_ audit log details.
        /// </summary>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="locPk">The loc pk.</param>
        /// <param name="UserPk">The user pk.</param>
        /// <param name="modulefk">The modulefk.</param>
        /// <param name="menu_fk">The menu_fk.</param>
        /// <param name="fromDt">From dt.</param>
        /// <param name="toDt">To dt.</param>
        /// <param name="PostBackFlag">The post back flag.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataTable fn_Fetch_AuditLogDetails(Int32 CurrentPage, Int32 TotalPage, Int64 locPk = 0, Int64 UserPk = 0, Int64 modulefk = 0, Int64 menu_fk = 0, string fromDt = "", string toDt = "", Int16 PostBackFlag = 0, Int32 flag = 0)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                int EnvPk = 0;
                try
                {
                    if ((HttpContext.Current.Session["ENVIRONMENT_PK"] != null))
                    {
                        EnvPk = Convert.ToInt32(HttpContext.Current.Session["ENVIRONMENT_PK"]);
                        if (!(EnvPk > 0))
                            EnvPk = 1;
                    }
                    else
                    {
                        EnvPk = 1;
                    }
                }
                catch (Exception ex)
                {
                    EnvPk = 1;
                }
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append(" SELECT ROWNUM SLNR, QRY.* FROM ( ");
                sb.Append("SELECT L.USER_MST_FK,");
                sb.Append("       UMT.USER_ID,");
                sb.Append("       L.LOCATION_MST_FK,");
                sb.Append("       LOC.LOCATION_ID,");
                sb.Append("       L.AUDIT_DATE_TIME AUDIT_DATE_TIME,");
                sb.Append("       L.MODULEFK,");
                sb.Append("       MODL_TEXT.MENU_TEXT MODULENAME,");
                sb.Append("       L.MENU_FK,");
                sb.Append("       MENU.MENU_TEXT MENUNAME,");
                sb.Append("       L.OPERATION_FLAG,");
                sb.Append("       '' BTN_DTL,");
                sb.Append("       L.AUDIT_LOG_PK, L.TRAN_ID, L.LEVELCOUNT ");
                sb.Append("  FROM VIEW_QCOR_AUDIT_LOG L,");
                sb.Append("       USER_MST_TBL        UMT,");
                sb.Append("       LOCATION_MST_TBL    LOC,");
                sb.Append("       MENU_TEXT_MST_TBL   MENU,");
                sb.Append("       MENU_TEXT_MST_TBL   MODL_TEXT,");
                sb.Append("       MENU_MST_TBL        MODL");
                sb.Append(" WHERE L.USER_MST_FK = UMT.USER_MST_PK");
                sb.Append("   AND L.LOCATION_MST_FK = LOC.LOCATION_MST_PK");
                sb.Append("   AND L.MODULEFK = MODL.MENU_MST_PK");
                sb.Append("   AND MODL.MENU_MST_PK = MODL_TEXT.MENU_MST_FK");
                sb.Append("   AND L.MENU_FK = MENU.MENU_MST_FK ");
                sb.Append("   AND MENU.ENVIRONMENT_MST_FK= " + (EnvPk > 0 ? EnvPk : 1));
                sb.Append("   AND MODL_TEXT.ENVIRONMENT_MST_FK= " + (EnvPk > 0 ? EnvPk : 1));
                if (!string.IsNullOrEmpty(fromDt))
                {
                    sb.Append(" and l.audit_date_time >= TO_DATE('" + fromDt + "','" + dateFormat + "')");
                }
                if (!string.IsNullOrEmpty(toDt))
                {
                    sb.Append(" and l.audit_date_time <= TO_DATE('" + toDt + "','" + dateFormat + "')");
                }
                if (flag == 0)
                {
                    sb.Append(" AND 1=2");
                }
                if (locPk != 0)
                {
                    sb.Append(" and L.location_mst_fk =" + locPk);
                }
                if (UserPk != 0)
                {
                    sb.Append(" and L.user_mst_fk=" + UserPk);
                }
                if (modulefk != 0)
                {
                    sb.Append(" and L.modulefk=" + modulefk);
                }
                if (menu_fk != 0)
                {
                    sb.Append("  and L.menu_fk= " + menu_fk);
                }
                sb.Append("  ORDER BY L.AUDIT_DATE_TIME desc ) QRY ");

                ///'''''''''''''''common''''''''''''''''''''
                //Get the Total Pages
                Int32 last = default(Int32);
                Int32 start = default(Int32);
                Int32 TotalRecords = default(Int32);
                string StrSqlCount = null;

                StrSqlCount = "SELECT COUNT(*) FROM ( ";
                StrSqlCount = StrSqlCount + sb.ToString();
                StrSqlCount = StrSqlCount + " ) ";

                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrSqlCount.ToString()));
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

                ///'''''''''''''''''''''''''''''''''''''''''''''''''''
                ///'''''''''''''''''''''''''''common''''''''''''''''''''''''''''''''''
                string StrSqlRecords = null;
                StrSqlRecords = "SELECT * FROM ( ";
                StrSqlRecords = StrSqlRecords + sb.ToString();
                StrSqlRecords = StrSqlRecords + " ) WHERE SLNR BETWEEN " + start + " AND " + last;
                return objWF.GetDataTable(StrSqlRecords);
                //Manjunath  PTS ID:Sep-02  13/09/2011
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

        #endregion "Fetch Audit LogDetails"

        /// <summary>
        /// FN_s the audit_ log information header.
        /// </summary>
        /// <param name="auditLogPk">The audit log pk.</param>
        /// <returns></returns>
        /// ''AUDIT LOG INFORMATION DETAILS
        public DataTable fn_Audit_LogInfoHeader(long auditLogPk)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                int EnvPk = 0;
                try
                {
                    if ((HttpContext.Current.Session["ENVIRONMENT_PK"] != null))
                    {
                        EnvPk = Convert.ToInt32(HttpContext.Current.Session["ENVIRONMENT_PK"]);
                        if (!(EnvPk > 0))
                            EnvPk = 1;
                    }
                    else
                    {
                        EnvPk = 1;
                    }
                }
                catch (Exception ex)
                {
                    EnvPk = 1;
                }
                sb.Append("SELECT UMT.USER_ID,");
                sb.Append("       LOC.LOCATION_ID,");
                sb.Append("       MODL.MENU_TEXT MODULENAME,");
                sb.Append("       MENU.MENU_TEXT MENUNAME,");
                sb.Append("       L.AUDIT_DATE_TIME,");
                sb.Append("       L.RECORD_REF_NR");
                sb.Append("  FROM qcor_sec_audit_trail_log L,");
                sb.Append("       USER_MST_TBL             UMT,");
                sb.Append("       LOCATION_MST_TBL         LOC,");
                sb.Append("       MENU_TEXT_MST_TBL        MENU,");
                sb.Append("       MENU_TEXT_MST_TBL        MODL,");
                sb.Append("       MENU_MST_TBL             MENU_MST ");
                sb.Append(" WHERE L.USER_MST_FK = UMT.USER_MST_PK ");
                sb.Append("   AND L.LOCATION_MST_FK = LOC.LOCATION_MST_PK");
                sb.Append("   AND L.MENU_FK = MENU_MST.MENU_MST_PK");
                sb.Append("   AND MENU_MST.MENU_MST_PK = MENU.MENU_MST_FK(+)");
                sb.Append("   AND MENU_MST.MENU_MST_FK = MODL.MENU_MST_FK(+)");

                sb.Append("   AND MENU.ENVIRONMENT_MST_FK = " + EnvPk);
                sb.Append("   AND MODL.ENVIRONMENT_MST_FK = " + EnvPk);
                sb.Append("   AND L.AUDIT_LOG_PK=" + auditLogPk);
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataTable(sb.ToString());
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
        /// Fn_audit_logs the information details.
        /// </summary>
        /// <param name="TranId">The tran identifier.</param>
        /// <param name="ModuleFk">The module fk.</param>
        /// <returns></returns>
        public DataSet fn_audit_logInfoDetails(long TranId, int ModuleFk = 0)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT ROWNUM SLNR, Q.* FROM (SELECT ");
                sb.Append("       LT.FIELD_DESC,");
                sb.Append("       LT.FIELD_VALUE_OLD,");
                sb.Append("       LT.FIELD_VALUE_NEW,");

                sb.Append("       LT.RECORD_REF_NR");
                sb.Append("  FROM QCOR_SEC_AUDIT_TRAIL_LOG_TRN LT");
                sb.Append(" WHERE LT.AUDIT_LOG_FK IN (SELECT L.AUDIT_LOG_PK");
                sb.Append("                             FROM QCOR_SEC_AUDIT_TRAIL_LOG L, MENU_MST_TBL M ");
                sb.Append("                            WHERE L.TRAN_ID = " + TranId + " AND L.MENU_FK = M.MENU_MST_PK");
                if (ModuleFk > 0)
                {
                    sb.Append("                             AND M.MENU_MST_FK = " + ModuleFk + ")");
                }
                sb.Append(" ORDER BY LT.FIELD_DESC)Q");

                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(sb.ToString());
                //Manjunath  PTS ID:Sep-02  13/09/2011
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
    }
}