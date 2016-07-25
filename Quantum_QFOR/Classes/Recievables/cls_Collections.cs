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

using System;
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCollections : CommonFeatures
    {
        /// <summary>
        /// Fetchlists the specified biztype.
        /// </summary>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Collectiondate">The collectiondate.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="Collectionrefno">The collectionrefno.</param>
        /// <param name="Invrefno">The invrefno.</param>
        /// <param name="Custpk">The custpk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public object Fetchlist(long Biztype, long Process, string Collectiondate, string fromDate = "", string ToDate = "", string Collectionrefno = "", string Invrefno = "", string Custpk = "", Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strSQLBuilder = new StringBuilder();
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder strCount = new StringBuilder();

            strSQLBuilder.Append(" SELECT CMT.CUSTOMER_NAME Party , ");
            strSQLBuilder.Append(" CINV.INVOICE_REF_NO InvoiceNr,");
            strSQLBuilder.Append(" COL.COLLECTIONS_REF_NO RecdrefNr,");
            strSQLBuilder.Append(" COL.COLLECTIONS_DATE CollectionDate,");
            strSQLBuilder.Append(" CLTRN.RECD_AMOUNT_HDR_CURR  RecdAmount,");
            strSQLBuilder.Append(" CMT.CURRENCY_ID Currency");
            strSQLBuilder.Append(" FROM ");
            strSQLBuilder.Append(" COLLECTIONS_TBL COL,");
            strSQLBuilder.Append(" CUSTOMER_MST_TBL CMT,");
            strSQLBuilder.Append(" COLLECTIONS_TRN_TBL   CLTRN,");
            strSQLBuilder.Append(" CONSOL_INVOICE_TBL    CINV,");
            strSQLBuilder.Append(" CURRENCY_TYPE_MST_TBL CMT");
            strSQLBuilder.Append(" WHERE  ");
            strSQLBuilder.Append(" COL.BUSINESS_TYPE = " + Biztype + " ");
            strSQLBuilder.Append(" AND COL.PROCESS_TYPE =" + Process + " ");
            strSQLBuilder.Append(" AND COL.COLLECTIONS_TBL_PK = CLTRN.COLLECTIONS_TBL_FK");
            strSQLBuilder.Append(" AND CMT.CUSTOMER_MST_PK = COL.CUSTOMER_MST_FK");
            strSQLBuilder.Append(" AND CINV.INVOICE_REF_NO = CLTRN.INVOICE_REF_NR");
            strSQLBuilder.Append("  AND CMT.CURRENCY_MST_PK = COL.CURRENCY_MST_FK");
            strSQLBuilder.Append(" AND COL.COLLECTIONS_DATE = TO_DATE('" + Collectiondate + "', '" + dateFormat + "')");
            strSQLBuilder.Append(" AND COL.COLLECTIONS_REF_NO = '" + Collectionrefno + "'");
            strSQLBuilder.Append(" AND COL.CUSTOMER_MST_FK = '" + Custpk + " '");
            strSQLBuilder.Append(" AND CLTRN.INVOICE_REF_NR IN");

            //Air import
            if (Process == 1 & Biztype == 1)
            {
                strSQLBuilder.Append(" (SELECT BG.INVOICE_REF_NO");
                strSQLBuilder.Append(" FROM INV_CUST_SEA_IMP_TBL BG, COLLECTIONS_TRN_TBL DG");
                strSQLBuilder.Append(" WHERE BG.INVOICE_REF_NO = DG.INVOICE_REF_NR");
                strSQLBuilder.Append(" AND BG.INVOICE_REF_NO IN ('" + Invrefno + "')");
                strSQLBuilder.Append(" AND BG.INVOICE_DATE BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
                strSQLBuilder.Append(" UNION");
                strSQLBuilder.Append(" SELECT C.INVOICE_REF_NR");
                strSQLBuilder.Append(" FROM COLLECTIONS_TRN_TBL C, CONSOL_INVOICE_TBL F");
                strSQLBuilder.Append(" WHERE(C.INVOICE_REF_NR = F.INVOICE_REF_NO)");
                strSQLBuilder.Append(" AND C.INVOICE_REF_NR IN ('" + Invrefno + "')");
                strSQLBuilder.Append(" AND F.INVOICE_DATE BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
                strSQLBuilder.Append(")");
                //Air Import
            }
            else if (Process == 1 & Biztype == 2)
            {
                strSQLBuilder.Append(" (SELECT BG.INVOICE_REF_NO");
                strSQLBuilder.Append(" FROM INV_CUST_AIR_IMP_TBL BG, COLLECTIONS_TRN_TBL DG");
                strSQLBuilder.Append(" WHERE BG.INVOICE_REF_NO = DG.INVOICE_REF_NR");
                strSQLBuilder.Append(" AND BG.INVOICE_REF_NO IN ('" + Invrefno + "')");
                strSQLBuilder.Append(" AND BG.INVOICE_DATE BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
                strSQLBuilder.Append(" UNION");
                strSQLBuilder.Append(" SELECT C.INVOICE_REF_NR");
                strSQLBuilder.Append(" FROM COLLECTIONS_TRN_TBL C, CONSOL_INVOICE_TBL F");
                strSQLBuilder.Append(" WHERE(C.INVOICE_REF_NR = F.INVOICE_REF_NO)");
                strSQLBuilder.Append(" AND C.INVOICE_REF_NR IN ('" + Invrefno + "')");
                strSQLBuilder.Append(" AND F.INVOICE_DATE BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
                strSQLBuilder.Append(")");
                //Air Export
            }
            else if (Process == 2 & Biztype == 1)
            {
                strSQLBuilder.Append(" (SELECT BG.INVOICE_REF_NO");
                strSQLBuilder.Append(" FROM INV_CUST_AIR_EXP_TBL BG, COLLECTIONS_TRN_TBL DG");
                strSQLBuilder.Append(" WHERE BG.INVOICE_REF_NO = DG.INVOICE_REF_NR");
                strSQLBuilder.Append(" AND BG.INVOICE_REF_NO IN ('" + Invrefno + "')");
                strSQLBuilder.Append(" AND BG.INVOICE_DATE BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
                strSQLBuilder.Append(" UNION");
                strSQLBuilder.Append(" SELECT C.INVOICE_REF_NR");
                strSQLBuilder.Append(" FROM COLLECTIONS_TRN_TBL C, CONSOL_INVOICE_TBL F");
                strSQLBuilder.Append(" WHERE(C.INVOICE_REF_NR = F.INVOICE_REF_NO)");
                strSQLBuilder.Append(" AND C.INVOICE_REF_NR IN ('" + Invrefno + "')");
                strSQLBuilder.Append(" AND F.INVOICE_DATE BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
                strSQLBuilder.Append(")");
                //Sea Export
            }
            else if (Process == 2 & Biztype == 2)
            {
                strSQLBuilder.Append(" (SELECT BG.INVOICE_REF_NO");
                strSQLBuilder.Append(" FROM INV_CUST_SEA_EXP_TBL BG, COLLECTIONS_TRN_TBL DG");
                strSQLBuilder.Append(" WHERE BG.INVOICE_REF_NO = DG.INVOICE_REF_NR");
                strSQLBuilder.Append(" AND BG.INVOICE_REF_NO IN ('" + Invrefno + "')");
                strSQLBuilder.Append(" AND BG.INVOICE_DATE BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
                strSQLBuilder.Append(" UNION");
                strSQLBuilder.Append(" SELECT C.INVOICE_REF_NR");
                strSQLBuilder.Append(" FROM COLLECTIONS_TRN_TBL C, CONSOL_INVOICE_TBL F");
                strSQLBuilder.Append(" WHERE(C.INVOICE_REF_NR = F.INVOICE_REF_NO)");
                strSQLBuilder.Append(" AND C.INVOICE_REF_NR IN ('" + Invrefno + "')");
                strSQLBuilder.Append(" AND F.INVOICE_DATE BETWEEN TO_DATE('" + fromDate + "', '" + dateFormat + "') AND TO_DATE('" + ToDate + "', '" + dateFormat + "')");
                strSQLBuilder.Append(")");
            }

            strCount.Append(" SELECT COUNT(*)");
            strCount.Append(" FROM ");
            strCount.Append("(" + strSQLBuilder.ToString() + ")");

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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

            strSQL.Append(" SELECT  qry.* FROM ");
            strSQL.Append(" (SELECT ROWNUM Sr_No,T.* FROM ");
            strSQL.Append((" (" + strSQLBuilder.ToString() + ")"));
            strSQL.Append(" T) qry  WHERE SR_NO  Between " + start + " and " + last);
            strSQL.Append(" ORDER BY PARTY  ");
            try
            {
                return (new WorkFlow()).GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}