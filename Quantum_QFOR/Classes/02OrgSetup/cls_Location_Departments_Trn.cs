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

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Location_Departments_Trn : CommonFeatures
    {
        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="P_Location_Mst_Fk">The p_ location_ MST_ fk.</param>
        /// <param name="P_Location_Departments_Pk">The p_ location_ departments_ pk.</param>
        /// <param name="P_Department_Mst_Fk">The p_ department_ MST_ fk.</param>
        /// <param name="P_Active">The p_ active.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <returns></returns>
        public string FetchAll(Int64 P_Location_Mst_Fk = 0, Int64 P_Location_Departments_Pk = 0, Int64 P_Department_Mst_Fk = 0, Int64 P_Active = 0, string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;
            strSQL = "SELECT ";
            strSQL = strSQL + " Location_Mst_Fk,";
            strSQL = strSQL + " Department_Mst_Fk,";
            strSQL = strSQL + " Active, ";
            strSQL = strSQL + " reporting_loc_fk";
            strSQL = strSQL + " FROM LOCATION_DEPARTMENTS_TRN loc ";
            strSQL = strSQL + " where (1=1) ";
            if (P_Location_Mst_Fk != 0)
            {
                strSQL = strSQL + " And Location_Mst_Fk =" + P_Location_Mst_Fk;
            }
            WorkFlow objWF = new WorkFlow();
            try
            {
                return JsonConvert.SerializeObject(objWF.GetDataSet(strSQL), Formatting.Indented);
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
        /// Gets the working dept.
        /// </summary>
        /// <param name="P_Location_Mst_Fk">The p_ location_ MST_ fk.</param>
        /// <returns></returns>
        public string GetWorkingDept(long P_Location_Mst_Fk)
        {
            string Strsql = null;
            Strsql = string.Empty;
            Strsql += "(SELECT ";
            Strsql += "LOCDEPT.LOCATION_DEPARTMENTS_PK LOCDEPTPK, ";
            Strsql += "DEPT.DEPARTMENT_MST_PK, ";
            Strsql += "DEPT.DEPARTMENT_ID, ";
            Strsql += "DEPT.DEPARTMENT_NAME, ";
            Strsql += "LOCDEPT.REPORTING_LOC_FK,";
            Strsql += "LMT.location_id, ";
            Strsql += "1 StatFlg,  ";
            Strsql += "LOCDEPT.Active,  ";
            Strsql += "LOCDEPT.version_no  ";
            Strsql += "FROM ";
            Strsql += "DEPARTMENT_MST_TBL DEPT, ";
            Strsql += "LOCATION_DEPARTMENTS_TRN LOCDEPT, ";
            Strsql += "LOCATION_MST_TBL LMT ";
            Strsql += "WHERE ";
            Strsql += "LOCDEPT.DEPARTMENT_MST_FK = DEPT.DEPARTMENT_MST_PK ";
            Strsql += "AND LOCDEPT.LOCATION_MST_FK = " + P_Location_Mst_Fk + " ";
            Strsql += "AND LMT.location_mst_pk(+) = LOCDEPT.reporting_loc_fk";
            Strsql += "  ";
            Strsql += "UNION  ";
            Strsql += "  ";
            Strsql += "SELECT  ";
            Strsql += "0 LOCDEPTPK,   ";
            Strsql += "DEPT.DEPARTMENT_MST_PK,  ";
            Strsql += "DEPT.DEPARTMENT_ID,  ";
            Strsql += "DEPT.DEPARTMENT_NAME DEPARTMENT_NAME,  ";
            Strsql += "0 REPORTING_LOC_FK,  ";
            Strsql += "'' location_id,";
            Strsql += "0 StatFlg,  ";
            Strsql += "0 Active,  ";
            Strsql += "0 Version_no  ";
            Strsql += "FROM   ";
            Strsql += "DEPARTMENT_MST_TBL DEPT ";
            Strsql += "WHERE  ";
            Strsql += "DEPT.DEPARTMENT_MST_PK ";
            Strsql += "NOT IN (  ";
            Strsql += "SELECT   ";
            Strsql += "LocDEPT.Department_Mst_Fk ";
            Strsql += "FROM  ";
            Strsql += "LOCATION_DEPARTMENTS_TRN LOCDEPT ";
            Strsql += "WHERE  ";
            Strsql += "LOCDEPT.Location_Mst_Fk = " + P_Location_Mst_Fk + ")";
            Strsql += " and DEPT.Active_Flag =1)";
            Strsql += "ORDER BY DEPARTMENT_ID  ";
            Strsql += " ";
            WorkFlow objWF = new WorkFlow();
            try
            {
                return JsonConvert.SerializeObject(objWF.GetDataSet(Strsql), Formatting.Indented);
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