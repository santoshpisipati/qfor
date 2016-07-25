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
    public class clsROLE_MST_TBL : CommonFeatures
    {
        /// <summary>
        /// Roles the listing search.
        /// </summary>
        /// <param name="Str">The string.</param>
        /// <param name="strDesc">The string desc.</param>
        /// <param name="Searchtype">The searchtype.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="ActFlag">The act flag.</param>
        /// <returns></returns>
        public DataSet RoleListingSearch(string Str, string strDesc, string Searchtype, string strColumnName, bool blnSortAscending,  Int32 CurrentPage,  Int32 TotalPage, Int32 flag, Int32 ActFlag)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (Searchtype == "S")
            {
                if (Str.Length > 0)
                {
                    strCondition = strCondition + "  and upper(role_mst_tbl.role_id) like '" + Str.ToUpper().Replace("'", "''") + "%'";
                }
            }
            else
            {
                if (Str.Length > 0)
                {
                    strCondition = strCondition + " and upper(role_mst_tbl.role_id) like '%" + Str.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (Searchtype == "S")
            {
                if (strDesc.Length > 0)
                {
                    strCondition = strCondition + " and upper(role_mst_tbl.role_description) like '" + strDesc.ToUpper().Replace("'", "''") + "%'";
                }
            }
            else
            {
                if (strDesc.Length > 0)
                {
                    strCondition = strCondition + " and upper(role_mst_tbl.role_description) like '%" + strDesc.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (ActFlag == 1)
            {
                strCondition = strCondition + " AND ROLE_MST_TBL.ACTIVE_FLAG = " + ActFlag;
            }
            strSQL = "select Count(*) from role_mst_tbl where 1=1 ";
            strSQL += strCondition;
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;
            strSQL = "SELECT * from (";
            strSQL = strSQL + " SELECT ROWNUM SR_NO, q.* FROM ";
            strSQL = strSQL + " (select role_mst_tbl.role_mst_tbl_pk,";
            strSQL = strSQL + " role_mst_tbl.active_flag,";
            strSQL = strSQL + " role_mst_tbl.role_id,";
            strSQL = strSQL + " role_mst_tbl.role_description";
            strSQL = strSQL + " from role_mst_tbl where 1=1  ";
            strSQL = strSQL + strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }
            if (!blnSortAscending & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL = strSQL + " )q) WHERE SR_NO  Between " + start + " and " + last;
            strSQL += " ORDER BY  SR_NO";
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
    }
}