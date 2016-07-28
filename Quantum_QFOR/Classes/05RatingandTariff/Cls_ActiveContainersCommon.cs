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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class Cls_ActiveContainersCommon
    {
        /// <summary>
        /// Actives the containers.
        /// </summary>
        /// <param name="strContainer">The string container.</param>
        /// <returns></returns>
        public DataTable ActiveContainers(string strContainer = "")
        {
            string Str = null;
            WorkFlow objWF = new WorkFlow();
            string strCondition = "";
            Array arrContainer = null;
            Int16 i = default(Int16);
            arrContainer = strContainer.Split(',');
            for (i = 0; i <= arrContainer.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    if (arrContainer.GetValue(i).ToString() == "0")
                    {
                        strCondition = " ( CMT.CONTAINER_TYPE_MST_ID ='" + arrContainer.GetValue(i).ToString() + "')";
                    }
                    else
                    {
                        strCondition = " ( CMT.CONTAINER_TYPE_MST_ID =" + arrContainer.GetValue(i).ToString() + ")";
                    }
                }
                else
                {
                    strCondition += " OR ( CMT.CONTAINER_TYPE_MST_ID =" + arrContainer.GetValue(i).ToString() + ")";
                }
            }
            Str = "SELECT QRY.CONTAINER_TYPE_MST_PK,QRY.CONTAINER_TYPE_MST_ID,QRY.CHK FROM (";
            Str = Str + "SELECT " + " CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, " + " '1' CHK,CMT.PREFERENCES  " + " FROM CONTAINER_TYPE_MST_TBL CMT " + " WHERE CMT.ACTIVE_FLAG = 1  " + "AND ( " + strCondition + " ) " + "  UNION " + " SELECT " + " CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, " + "(CASE WHEN (" + strCondition + ") " + "THEN '1' ELSE '0' END ) CHK,CMT.PREFERENCES  " + " FROM CONTAINER_TYPE_MST_TBL CMT " + " WHERE CMT.ACTIVE_FLAG=1)Qry  " + " ORDER BY CHK DESC,PREFERENCES";
            try
            {
                DataTable dt = objWF.GetDataTable(Str);
                return dt;
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