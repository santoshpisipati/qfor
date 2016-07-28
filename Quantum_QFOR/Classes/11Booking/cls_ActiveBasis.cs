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
    public class cls_ActiveBasis
    {
        //This function returns all the active Dimentions.
        /// <summary>
        /// Acitves the dimentions.
        /// </summary>
        /// <param name="intEdiPk">The int edi pk.</param>
        /// <param name="intBkgPk">The int BKG pk.</param>
        /// <returns></returns>
        public DataTable AcitveDimentions(int intEdiPk = 0, int intBkgPk = 0)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            //'Modified by ThiruMoorthy for the PTS Id : Nov-016b (EDI Upload)
            if (intEdiPk != 0 & intBkgPk != 0)
            {
                strSQL = "SELECT " + "T1.DIMENTION_UNIT_MST_PK,T1.DIMENTION_ID,  " + "CASE WHEN T1.DIMENTION_ID=T2.CONTAINER_TYPE_BASIS THEN '1' ELSE '0' END AS CHK FROM " + "(SELECT UOM.DIMENTION_UNIT_MST_PK, UOM.DIMENTION_ID FROM DIMENTION_UNIT_MST_TBL UOM " + "WHERE UOM.ACTIVE = 1 ORDER BY UOM.DIMENTION_ID) T1, (SELECT EBCB.CONTAINER_TYPE_BASIS " + "FROM EDI_BOOKING_CNTR_BASIS_TBL EBCB WHERE EBCB.EDI_MST_FK=" + intEdiPk + " AND EBCB.BOOKING_SEA_FK=" + intBkgPk + ") T2 " + "WHERE T1.DIMENTION_ID=T2.CONTAINER_TYPE_BASIS(+) ";
            }
            else
            {
                strSQL = "SELECT " + "  UOM.DIMENTION_UNIT_MST_PK, " + "  UOM.DIMENTION_ID, " + "  '0' CHK " + "FROM " + "  DIMENTION_UNIT_MST_TBL UOM " + "WHERE " + "  UOM.ACTIVE = 1 " + "ORDER BY " + "  UOM.DIMENTION_ID ";
            }

            try
            {
                return objWF.GetDataTable(strSQL);
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