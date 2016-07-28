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
    public class cls_ActiveContainers
    {
        /// <summary>
        /// Actives the containers.
        /// </summary>
        /// <param name="intEdiPk">The int edi pk.</param>
        /// <param name="intBkgPk">The int BKG pk.</param>
        /// <returns></returns>
        public DataTable ActiveContainers(int intEdiPk = 0, int intBkgPk = 0)
        {
            string Str = null;
            WorkFlow objWF = new WorkFlow();
            string DefaultContainers = null;

            //'Modified by ThiruMoorthy for the PTS Id : Nov-016b (EDI Upload)
            if (intEdiPk != 0 & intBkgPk != 0)
            {
                Str = "SELECT " + "T1.CONTAINER_TYPE_MST_PK,T1.CONTAINER_TYPE_MST_ID,  " + "CASE WHEN T1.CONTAINER_TYPE_MST_ID=T2.CONTAINER_TYPE_BASIS THEN 'true' ELSE 'false' END AS CHK FROM " + "(SELECT CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID FROM CONTAINER_TYPE_MST_TBL CMT " + "WHERE CMT.ACTIVE_FLAG=1 ORDER BY CMT.PREFERENCES) T1, (SELECT EBCB.CONTAINER_TYPE_BASIS " + "FROM EDI_BOOKING_CNTR_BASIS_TBL EBCB WHERE EBCB.EDI_MST_FK=" + intEdiPk + " AND EBCB.BOOKING_SEA_FK=" + intBkgPk + ") T2 " + "WHERE T1.CONTAINER_TYPE_MST_ID=T2.CONTAINER_TYPE_BASIS(+) ";
            }
            else
            {
                Str = "SELECT " + "CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, 'false' CHK  " + "FROM CONTAINER_TYPE_MST_TBL CMT " + "WHERE CMT.ACTIVE_FLAG=1  " + "ORDER BY CMT.PREFERENCES ";
            }

            try
            {
                DataTable dt = objWF.GetDataTable(Str);
                //Dim i As Int16
                //For i = 0 To dt.Rows.Count - 1
                //    If i > 9 Then
                //        Exit For
                //    End If
                //    dt.Rows(i).Item("CHK") = "true"
                //Next
                return dt;
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
        }
    }
}