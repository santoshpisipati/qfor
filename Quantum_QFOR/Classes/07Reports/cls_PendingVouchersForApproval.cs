#region "Comments"

//'***************************************************************************************************************
//'*  Company Name:
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By  :    Santosh on 31-May-16
//'*  Module/Project Leader   :    Santosh Pisipati
//'*  Description :
//'*  Module/Form/Class Name  :
//'*  Configuration ID        :
//'***************************************************************************************************************
//'*  Revision History
//'***************************************************************************************************************
//'*  Modified DateTime(DD-MON-YYYY)  Modified By     Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using System;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_PendingVouchersForApproval : CommonFeatures
    {
        #region "Fetch VoucherListing"

        /// <summary>
        /// Fetches the voucher listing.
        /// </summary>
        /// <param name="fromDt">From dt.</param>
        /// <param name="toDt">To dt.</param>
        /// <param name="supplier">The supplier.</param>
        /// <param name="bizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <param name="cur">The current.</param>
        /// <param name="intLoc">The int loc.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <returns></returns>
        public DataSet fetchVoucherListing(string fromDt, string toDt, int supplier, int bizType, int process, int cur, Int32 intLoc = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0,
        Int32 ExportExcel = 0)
        {
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = "";
            DataSet ds = new DataSet();
            DataSet ds1 = new DataSet();
            try
            {
                if (bizType == 2 & process == 1)
                {
                    sb.Append(" SELECT INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("               INVTBL.INVOICE_REF_NO VOUCHERNO,");
                    sb.Append("               TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                    sb.Append("               VMST.VENDOR_NAME VENDOR,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS ESTIMATED_COST,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS AMOUNT,");
                    sb.Append("               TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT");
                    sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                    sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                    sb.Append("               VENDOR_MST_TBL VMST,");
                    sb.Append("               JOB_CARD_TRN JOB_EXP,");
                    sb.Append("               BOOKING_MST_TBL BST,");
                    sb.Append("               CURRENCY_TYPE_MST_TBL CURR");
                    sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                    sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                    sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = JOB_EXP.JOBCARD_REF_NO");
                    sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                    sb.Append("           AND BST.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                    sb.Append("           AND INVTBL.BUSINESS_TYPE = 2");
                    sb.Append("           AND INVTBL.PROCESS_TYPE = 1");
                    sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                    if (flag == 0)
                    {
                        sb.Append(" AND 1=2 ");
                    }
                    if (!string.IsNullOrEmpty(fromDt))
                    {
                        sb.Append(" And TO_DATE(JOB_EXP.JOBCARD_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(toDt))
                    {
                        sb.Append("  And TO_DATE(JOB_EXP.JOBCARD_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                    }
                    if (supplier > 0)
                    {
                        sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                    }
                    sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                    sb.Append("                  INVTBL.INVOICE_DATE,");
                    sb.Append("                  VMST.VENDOR_NAME,");
                    sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                    sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                  CURR.CURRENCY_ID,");
                    sb.Append("                  JOB_EXP.JOB_CARD_TRN_PK,");
                    sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                    sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                    sb.Append("                  INVTBL.CURRENCY_MST_FK");
                    sb.Append("        UNION");
                    sb.Append("        SELECT DISTINCT INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                        INVTBL.INVOICE_REF_NO VOUCHERNO,");
                    sb.Append("                        TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                    sb.Append("                        VMST.VENDOR_NAME VENDOR,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS ESTIMATED_COST,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS AMOUNT,");
                    sb.Append("                        TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT");
                    sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                    sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                    sb.Append("               VENDOR_MST_TBL VMST,");
                    sb.Append("               CURRENCY_TYPE_MST_TBL CURR,");
                    sb.Append("               MASTER_JC_SEA_EXP_TBL MJOB_EXP,");
                    sb.Append("               JOB_CARD_TRN JOB_EXP");
                    sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                    sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                    sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = MJOB_EXP.MASTER_JC_REF_NO");
                    sb.Append("           AND MJOB_EXP.MASTER_JC_SEA_EXP_PK = JOB_EXP.MASTER_JC_FK");
                    sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                    sb.Append("           AND INVTBL.BUSINESS_TYPE = 2");
                    sb.Append("           AND INVTBL.PROCESS_TYPE = 1");
                    sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                    if (flag == 0)
                    {
                        sb.Append(" AND 1=2 ");
                    }
                    if (!string.IsNullOrEmpty(fromDt))
                    {
                        sb.Append(" And TO_DATE(MJOB_EXP.MASTER_JC_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(toDt))
                    {
                        sb.Append("  And TO_DATE(MJOB_EXP.MASTER_JC_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                    }
                    if (supplier > 0)
                    {
                        sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                    }
                    sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                    sb.Append("                  INVTBL.INVOICE_DATE,");
                    sb.Append("                  VMST.VENDOR_NAME,");
                    sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                    sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                  CURR.CURRENCY_ID,");
                    sb.Append("                  MJOB_EXP.MASTER_JC_SEA_EXP_PK,");
                    sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                    sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                    sb.Append("                  INVTBL.CURRENCY_MST_FK");
                }
                else if (bizType == 1 & process == 1)
                {
                    sb.Append(" SELECT INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("               INVTBL.INVOICE_REF_NO VOUCHERNO,");
                    sb.Append("               TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                    sb.Append("               VMST.VENDOR_NAME VENDOR,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS ESTIMATED_COST,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS AMOUNT,");
                    sb.Append("               TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT");
                    sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                    sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                    sb.Append("               VENDOR_MST_TBL VMST,");
                    sb.Append("               PORT_MST_TBL POD,");
                    sb.Append("               JOB_CARD_TRN JOB_EXP,");
                    sb.Append("               BOOKING_MST_TBL BST,");
                    sb.Append("               CURRENCY_TYPE_MST_TBL CURR");
                    sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                    sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                    sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = JOB_EXP.JOBCARD_REF_NO");
                    sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                    sb.Append("           AND BST.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                    sb.Append("           AND INVTBL.BUSINESS_TYPE = 1");
                    sb.Append("           AND INVTBL.PROCESS_TYPE = 1");
                    sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                    if (flag == 0)
                    {
                        sb.Append(" AND 1=2 ");
                    }
                    if (!string.IsNullOrEmpty(fromDt))
                    {
                        sb.Append(" And TO_DATE(JOB_EXP.JOBCARD_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(toDt))
                    {
                        sb.Append("  And TO_DATE(JOB_EXP.JOBCARD_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                    }
                    if (supplier > 0)
                    {
                        sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                    }
                    sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                    sb.Append("                  INVTBL.INVOICE_DATE,");
                    sb.Append("                  VMST.VENDOR_NAME,");
                    sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                    sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                  CURR.CURRENCY_ID,");
                    sb.Append("                  JOB_EXP.JOB_CARD_TRN_PK,");
                    sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                    sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                    sb.Append("                  INVTBL.CURRENCY_MST_FK");
                    sb.Append("        UNION");
                    sb.Append("        SELECT DISTINCT INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                        INVTBL.INVOICE_REF_NO VOUCHERNO,");
                    sb.Append("                        TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                    sb.Append("                        VMST.VENDOR_NAME VENDOR,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS ESTIMATED_COST,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS AMOUNT,");
                    sb.Append("                        TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT");
                    sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                    sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                    sb.Append("               VENDOR_MST_TBL VMST,");
                    sb.Append("               CURRENCY_TYPE_MST_TBL CURR,");
                    sb.Append("               MASTER_JC_AIR_EXP_TBL MJOB_EXP,");
                    sb.Append("               JOB_CARD_TRN  JOB_EXP");
                    sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                    sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                    sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = MJOB_EXP.MASTER_JC_REF_NO");
                    sb.Append("           AND MJOB_EXP.MASTER_JC_AIR_EXP_PK = JOB_EXP.MASTER_JC_FK");
                    sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                    sb.Append("           AND INVTBL.BUSINESS_TYPE = 1");
                    sb.Append("           AND INVTBL.PROCESS_TYPE = 1");
                    sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                    if (flag == 0)
                    {
                        sb.Append(" AND 1=2 ");
                    }
                    if (!string.IsNullOrEmpty(fromDt))
                    {
                        sb.Append(" And TO_DATE(MJOB_EXP.MASTER_JC_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(toDt))
                    {
                        sb.Append("  And TO_DATE(MJOB_EXP.MASTER_JC_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                    }
                    if (supplier > 0)
                    {
                        sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                    }
                    sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                    sb.Append("                  INVTBL.INVOICE_DATE,");
                    sb.Append("                  VMST.VENDOR_NAME,");
                    sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                    sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                  CURR.CURRENCY_ID,");
                    sb.Append("                  MJOB_EXP.MASTER_JC_AIR_EXP_PK,");
                    sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                    sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                    sb.Append("                  INVTBL.CURRENCY_MST_FK");
                }
                else if (bizType == 2 & process == 2)
                {
                    sb.Append(" SELECT INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("               INVTBL.INVOICE_REF_NO VOUCHERNO,");
                    sb.Append("               TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                    sb.Append("               VMST.VENDOR_NAME VENDOR,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS ESTIMATED_COST,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS AMOUNT,");
                    sb.Append("               TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT");
                    sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                    sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                    sb.Append("               VENDOR_MST_TBL VMST,");
                    sb.Append("               JOB_CARD_TRN JOB_IMP,");
                    sb.Append("               CURRENCY_TYPE_MST_TBL CURR");
                    sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                    sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                    sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = JOB_IMP.JOBCARD_REF_NO");
                    sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                    sb.Append("           AND INVTBL.BUSINESS_TYPE = 2");
                    sb.Append("           AND INVTBL.PROCESS_TYPE = 2");
                    sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                    if (flag == 0)
                    {
                        sb.Append(" AND 1=2 ");
                    }
                    if (!string.IsNullOrEmpty(fromDt))
                    {
                        sb.Append(" And TO_DATE(JOB_IMP.JOBCARD_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(toDt))
                    {
                        sb.Append("  And TO_DATE(JOB_IMP.JOBCARD_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                    }
                    if (supplier > 0)
                    {
                        sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                    }
                    sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                    sb.Append("                  INVTBL.INVOICE_DATE,");
                    sb.Append("                  VMST.VENDOR_NAME,");
                    sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                    sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                  CURR.CURRENCY_ID,");
                    sb.Append("                  JOB_IMP.JOB_CARD_TRN_PK,");
                    sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                    sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                    //sb.Append("                  INVTRNTBL.ESTIMATED_AMT,")
                    sb.Append("                  INVTBL.CURRENCY_MST_FK");
                    //sb.Append("                  INVTRNTBL.ACTUAL_AMT")
                    sb.Append("        UNION");
                    sb.Append("        SELECT DISTINCT INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                        INVTBL.INVOICE_REF_NO VOUCHERNO,");
                    sb.Append("                        TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                    sb.Append("                        VMST.VENDOR_NAME VENDOR,");
                    //sb.Append("                        (SELECT SUM(NVL(MJCST.TOTAL_COST, 0))")
                    //sb.Append("                           FROM MJC_TRN_SEA_IMP_COST MJCST")
                    //sb.Append("                          WHERE MJCST.MASTER_JC_FK =")
                    //sb.Append("                                MJOB_IMP.MASTER_JC_SEA_IMP_PK) ESTIMATED_COST,")
                    sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS ESTIMATED_COST,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS AMOUNT,");
                    sb.Append("                        TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT");
                    sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                    sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                    sb.Append("               VENDOR_MST_TBL VMST,");
                    sb.Append("               CURRENCY_TYPE_MST_TBL CURR,");
                    sb.Append("               MASTER_JC_SEA_IMP_TBL MJOB_IMP,");
                    sb.Append("               JOB_CARD_TRN JOB_IMP");
                    sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                    sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                    sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = MJOB_IMP.MASTER_JC_REF_NO");
                    sb.Append("           AND MJOB_IMP.MASTER_JC_SEA_IMP_PK = JOB_IMP.MASTER_JC_FK");
                    sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                    sb.Append("           AND INVTBL.BUSINESS_TYPE = 2");
                    sb.Append("           AND INVTBL.PROCESS_TYPE = 2");
                    sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                    if (flag == 0)
                    {
                        sb.Append(" AND 1=2 ");
                    }
                    if (!string.IsNullOrEmpty(fromDt))
                    {
                        sb.Append(" And TO_DATE(MJOB_IMP.MASTER_JC_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(toDt))
                    {
                        sb.Append("  And TO_DATE(MJOB_IMP.MASTER_JC_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                    }
                    if (supplier > 0)
                    {
                        sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                    }
                    //If cur > 0 Then
                    //    sb.Append(" And INVTBL.CURRENCY_MST_FK = " & cur & "")
                    //End If
                    sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                    sb.Append("                  INVTBL.INVOICE_DATE,");
                    sb.Append("                  VMST.VENDOR_NAME,");
                    sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                    sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                  CURR.CURRENCY_ID,");
                    sb.Append("                  MJOB_IMP.MASTER_JC_SEA_IMP_PK,");
                    sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                    sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                    //sb.Append("                  INVTRNTBL.ESTIMATED_AMT,")
                    sb.Append("                  INVTBL.CURRENCY_MST_FK");
                    //sb.Append("                  INVTRNTBL.ACTUAL_AMT")
                }
                else if (bizType == 1 & process == 2)
                {
                    //----------------AIR IMPORT  -----------
                    sb.Append(" SELECT INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("               INVTBL.INVOICE_REF_NO VOUCHERNO,");
                    sb.Append("               TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                    sb.Append("               VMST.VENDOR_NAME VENDOR,");
                    //sb.Append("               (SELECT SUM(NVL(JCST.TOTAL_COST, 0))")
                    //sb.Append("                  FROM JOB_TRN_AIR_IMP_COST JCST")
                    //sb.Append("                 WHERE JCST.JOB_CARD_TRN_FK = JOB_IMP.JOB_CARD_TRN_PK) ESTIMATED_COST,")
                    sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS ESTIMATED_COST,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS AMOUNT,");
                    sb.Append("               TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT");
                    sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                    sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                    sb.Append("               VENDOR_MST_TBL VMST,");
                    sb.Append("               JOB_CARD_TRN JOB_IMP,");
                    sb.Append("               CURRENCY_TYPE_MST_TBL CURR");
                    sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                    sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                    sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = JOB_IMP.JOBCARD_REF_NO");
                    sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                    sb.Append("           AND INVTBL.BUSINESS_TYPE = 1");
                    sb.Append("           AND INVTBL.PROCESS_TYPE = 2");
                    sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                    if (flag == 0)
                    {
                        sb.Append(" AND 1=2 ");
                    }
                    if (!string.IsNullOrEmpty(fromDt))
                    {
                        sb.Append(" And TO_DATE(JOB_IMP.JOBCARD_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(toDt))
                    {
                        sb.Append("  And TO_DATE(JOB_IMP.JOBCARD_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                    }
                    if (supplier > 0)
                    {
                        sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                    }
                    //If cur > 0 Then
                    //    sb.Append(" And INVTBL.CURRENCY_MST_FK = " & cur & "")
                    //End If
                    sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                    sb.Append("                  INVTBL.INVOICE_DATE,");
                    sb.Append("                  VMST.VENDOR_NAME,");
                    sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                    sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                  CURR.CURRENCY_ID,");
                    sb.Append("                  JOB_IMP.JOB_CARD_TRN_PK,");
                    sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                    sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                    //sb.Append("                  INVTRNTBL.ESTIMATED_AMT,")
                    sb.Append("                  INVTBL.CURRENCY_MST_FK");
                    //sb.Append("                  INVTRNTBL.ACTUAL_AMT")
                    sb.Append("        UNION");
                    sb.Append("        SELECT DISTINCT INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                        INVTBL.INVOICE_REF_NO VOUCHERNO,");
                    sb.Append("                        TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                    sb.Append("                        VMST.VENDOR_NAME VENDOR,");
                    //sb.Append("                        (SELECT SUM(NVL(MJCST.TOTAL_COST, 0))")
                    //sb.Append("                           FROM MJC_TRN_AIR_IMP_COST MJCST")
                    //sb.Append("                          WHERE MJCST.MASTER_JC_FK =")
                    //sb.Append("                                MJOB_IMP.MASTER_JC_AIR_IMP_PK) ESTIMATED_COST,")
                    sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS ESTIMATED_COST,");
                    sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                    sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                    sb.Append("               2)) AS AMOUNT,");
                    sb.Append("                        TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT");
                    sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                    sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                    sb.Append("               VENDOR_MST_TBL VMST,");
                    sb.Append("               CURRENCY_TYPE_MST_TBL CURR,");
                    sb.Append("               MASTER_JC_AIR_IMP_TBL MJOB_IMP,");
                    sb.Append("               JOB_CARD_TRN JOB_IMP");
                    sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                    sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                    sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = MJOB_IMP.MASTER_JC_REF_NO");
                    sb.Append("           AND MJOB_IMP.MASTER_JC_AIR_IMP_PK = JOB_IMP.MASTER_JC_FK");
                    sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                    sb.Append("           AND INVTBL.BUSINESS_TYPE = 1");
                    sb.Append("           AND INVTBL.PROCESS_TYPE = 2");
                    sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                    if (flag == 0)
                    {
                        sb.Append(" AND 1=2 ");
                    }
                    if (!string.IsNullOrEmpty(fromDt))
                    {
                        sb.Append(" And TO_DATE(MJOB_IMP.MASTER_JC_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(toDt))
                    {
                        sb.Append("  And TO_DATE(MJOB_IMP.MASTER_JC_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                    }
                    if (supplier > 0)
                    {
                        sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                    }
                    //If cur > 0 Then
                    //    sb.Append(" And INVTBL.CURRENCY_MST_FK = " & cur & "")
                    //End If
                    sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                    sb.Append("                  INVTBL.INVOICE_DATE,");
                    sb.Append("                  VMST.VENDOR_NAME,");
                    sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                    sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                    sb.Append("                  CURR.CURRENCY_ID,");
                    sb.Append("                  MJOB_IMP.MASTER_JC_AIR_IMP_PK,");
                    sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                    sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                    //sb.Append("                  INVTRNTBL.ESTIMATED_AMT,")
                    sb.Append("                  INVTBL.CURRENCY_MST_FK");
                    //sb.Append("                  INVTRNTBL.ACTUAL_AMT")
                }

                strSQL = " select * from (";
                strSQL += sb.ToString() + ")";
                ds1 = objWF.GetDataSet(strSQL);
                if (ds1.Tables[0].Rows.Count > 0)
                {
                    TotalRecords = ds1.Tables[0].Rows.Count;
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
                }

                strSQL = " SELECT * FROM (SELECT ROWNUM SlNr, X.* FROM(";
                strSQL += sb.ToString();
                strSQL += " ORDER BY VOUCHERDATE DESC, VOUCHERNO DESC";
                if (ExportExcel == 0)
                {
                    strSQL += " ) X ) WHERE SlNr Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " ) X ) ";
                }
                ds = objWF.GetDataSet(strSQL);
                return ds;
            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the voucher listing new.
        /// </summary>
        /// <param name="fromDt">From dt.</param>
        /// <param name="toDt">To dt.</param>
        /// <param name="supplier">The supplier.</param>
        /// <param name="bizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <param name="cur">The current.</param>
        /// <param name="intLoc">The int loc.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <returns></returns>
        public DataSet fetchVoucherListingNew(string fromDt, string toDt, int supplier, int bizType, int process, int cur, Int32 intLoc = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0,
        Int32 ExportExcel = 0)
        {
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = "";
            DataSet ds = new DataSet();
            DataSet ds1 = new DataSet();
            string strCondition = null;

            try
            {
                sb.Append("SELECT INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                       INVTBL.INVOICE_REF_NO VOUCHERNO,");
                sb.Append("                       TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                sb.Append("                       VMST.VENDOR_NAME VENDOR,");
                sb.Append("                       SUM(ROUND(INVTRNTBL.ESTIMATED_AMT *");
                sb.Append("                                 GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK,");
                sb.Append("                                                 " + cur + ",");
                sb.Append("                                                 INVTBL.INVOICE_DATE),");
                sb.Append("                                 2)) AS ESTIMATED_COST,");
                sb.Append("                       SUM(ROUND(INVTRNTBL.ACTUAL_AMT *");
                sb.Append("                                 GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK,");
                sb.Append("                                                 " + cur + ",");
                sb.Append("                                                 INVTBL.INVOICE_DATE),");
                sb.Append("                                 2)) AS AMOUNT,");
                sb.Append("                       TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT,");
                sb.Append("                       DECODE(INVTBL.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("                       DECODE(INVTBL.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE");
                sb.Append("                  FROM INV_SUPPLIER_TBL      INVTBL,");
                sb.Append("                       INV_SUPPLIER_TRN_TBL  INVTRNTBL,");
                sb.Append("                       VENDOR_MST_TBL        VMST,");
                sb.Append("                       CURRENCY_TYPE_MST_TBL CURR");
                sb.Append("                 WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                sb.Append("                   AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                sb.Append("                   AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                sb.Append("                   AND INVTRNTBL.ELEMENT_APPROVED = 0");
                if (flag == 0)
                {
                    sb.Append(" AND 1=2 ");
                }
                if (bizType != 0)
                {
                    sb.Append("                   AND INVTBL.BUSINESS_TYPE = " + bizType);
                }
                if (process != 0)
                {
                    sb.Append("                   AND INVTBL.PROCESS_TYPE = " + process);
                }
                if (!string.IsNullOrEmpty(fromDt))
                {
                    sb.Append(" And TO_DATE(INVTBL.INVOICE_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(toDt))
                {
                    sb.Append("  And TO_DATE(INVTBL.INVOICE_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                }
                if (supplier > 0)
                {
                    sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                }
                sb.Append("                 GROUP BY INVTBL.INVOICE_REF_NO,");
                sb.Append("                          INVTBL.INVOICE_DATE,");
                sb.Append("                          VMST.VENDOR_NAME,");
                sb.Append("                          INVTRNTBL.ELEMENT_APPROVED,");
                sb.Append("                          INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                          CURR.CURRENCY_ID,");
                sb.Append("                          INVTBL.SUPPLIER_DUE_DT,");
                sb.Append("                          INVTBL.SUPPLIER_INV_NO,");
                sb.Append("                          INVTBL.CURRENCY_MST_FK,");
                sb.Append("                          INVTBL.BUSINESS_TYPE,");
                sb.Append("                          INVTBL.PROCESS_TYPE");

                strSQL = " select * from (";
                strSQL += sb.ToString() + ")";
                ds1 = objWF.GetDataSet(strSQL);
                if (ds1.Tables[0].Rows.Count > 0)
                {
                    TotalRecords = ds1.Tables[0].Rows.Count;
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
                }

                strSQL = " SELECT * FROM (SELECT ROWNUM SlNr, X.* FROM(";
                strSQL += sb.ToString();
                strSQL += " ORDER BY VOUCHERDATE DESC, VOUCHERNO DESC";
                if (ExportExcel == 0)
                {
                    strSQL += " ) X ) WHERE SlNr Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " ) X ) ";
                }
                ds = objWF.GetDataSet(strSQL);

                return ds;
            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the voucher listing new1.
        /// </summary>
        /// <param name="fromDt">From dt.</param>
        /// <param name="toDt">To dt.</param>
        /// <param name="supplier">The supplier.</param>
        /// <param name="bizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <param name="cur">The current.</param>
        /// <param name="intLoc">The int loc.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <returns></returns>
        public DataSet fetchVoucherListingNew1(string fromDt, string toDt, int supplier, int bizType, int process, int cur, Int32 intLoc = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0,
        Int32 ExportExcel = 0)
        {
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = "";
            DataSet ds = new DataSet();
            DataSet ds1 = new DataSet();
            string strCondition = null;

            try
            {
                //-------SEA EXPORT----------
                sb.Append(" SELECT INVTBL.INV_SUPPLIER_PK,");
                sb.Append("               INVTBL.INVOICE_REF_NO VOUCHERNO,");
                sb.Append("               TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                sb.Append("               VMST.VENDOR_NAME VENDOR,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS ESTIMATED_COST,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS AMOUNT,");
                sb.Append("               TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT,");
                sb.Append("     DECODE(INVTBL.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INVTBL.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE");
                sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                sb.Append("               VENDOR_MST_TBL VMST,");
                sb.Append("               JOB_CARD_TRN JOB_EXP,");
                sb.Append("               BOOKING_MST_TBL BST,");
                sb.Append("\t              PORT_MST_TBL POL,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CURR");
                sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = JOB_EXP.JOBCARD_REF_NO");
                sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                sb.Append("           AND BST.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("\t          AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("\t  AND POL.LOCATION_MST_FK IN (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L START WITH L.LOCATION_MST_PK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK)");
                if (bizType != 0)
                {
                    sb.Append("                   AND INVTBL.BUSINESS_TYPE = " + bizType);
                }
                if (process != 0)
                {
                    sb.Append("                   AND INVTBL.PROCESS_TYPE = " + process);
                }
                sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                if (flag == 0)
                {
                    sb.Append(" AND 1=2 ");
                }
                if (!string.IsNullOrEmpty(fromDt))
                {
                    sb.Append(" And TO_DATE(INVTBL.INVOICE_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(toDt))
                {
                    sb.Append("  And TO_DATE(INVTBL.INVOICE_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                }
                if (supplier > 0)
                {
                    sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                }
                //If cur > 0 Then
                //    sb.Append(" And INVTBL.CURRENCY_MST_FK = " & cur & "")
                //End If
                sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                sb.Append("                  INVTBL.INVOICE_DATE,");
                sb.Append("                  VMST.VENDOR_NAME,");
                sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                  CURR.CURRENCY_ID,");
                sb.Append("                  JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                sb.Append("                  INVTBL.BUSINESS_TYPE,");
                sb.Append("                  INVTBL.CURRENCY_MST_FK,");
                sb.Append("                  INVTBL.PROCESS_TYPE");
                sb.Append("        UNION");
                sb.Append("        SELECT DISTINCT INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                        INVTBL.INVOICE_REF_NO VOUCHERNO,");
                sb.Append("                        TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                sb.Append("                        VMST.VENDOR_NAME VENDOR,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS ESTIMATED_COST,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS AMOUNT,");
                sb.Append("                        TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT,");
                sb.Append("     DECODE(INVTBL.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INVTBL.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE");
                sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                sb.Append("               VENDOR_MST_TBL VMST,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CURR,");
                sb.Append("               MASTER_JC_SEA_EXP_TBL MJOB_EXP,");
                sb.Append("\tBOOKING_MST_TBL BST,");
                sb.Append("\tPORT_MST_TBL POL,");
                sb.Append("               JOB_CARD_TRN JOB_EXP");
                sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = MJOB_EXP.MASTER_JC_REF_NO");
                sb.Append("           AND MJOB_EXP.MASTER_JC_SEA_EXP_PK = JOB_EXP.MASTER_JC_FK");
                sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                if (bizType != 0)
                {
                    sb.Append("                   AND INVTBL.BUSINESS_TYPE = " + bizType);
                }
                if (process != 0)
                {
                    sb.Append("                   AND INVTBL.PROCESS_TYPE = " + process);
                }
                sb.Append("\tAND BST.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("\tAND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("\tAND POL.LOCATION_MST_FK IN (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L START WITH L.LOCATION_MST_PK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK)");
                sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                if (flag == 0)
                {
                    sb.Append(" AND 1=2 ");
                }
                if (!string.IsNullOrEmpty(fromDt))
                {
                    sb.Append(" And TO_DATE(INVTBL.INVOICE_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(toDt))
                {
                    sb.Append("  And TO_DATE(INVTBL.INVOICE_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                }
                if (supplier > 0)
                {
                    sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                }
                //If cur > 0 Then
                //    sb.Append(" And INVTBL.CURRENCY_MST_FK = " & cur & "")
                //End If
                sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                sb.Append("                  INVTBL.INVOICE_DATE,");
                sb.Append("                  VMST.VENDOR_NAME,");
                sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                  CURR.CURRENCY_ID,");
                sb.Append("                  MJOB_EXP.MASTER_JC_SEA_EXP_PK,");
                sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                sb.Append("                  INVTBL.CURRENCY_MST_FK,");
                sb.Append("                  INVTBL.BUSINESS_TYPE,");
                sb.Append("                  INVTBL.PROCESS_TYPE");

                sb.Append("                  UNION");
                //-------SEA IMPORT----------
                sb.Append(" SELECT INVTBL.INV_SUPPLIER_PK,");
                sb.Append("               INVTBL.INVOICE_REF_NO VOUCHERNO,");
                sb.Append("               TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                sb.Append("               VMST.VENDOR_NAME VENDOR,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS ESTIMATED_COST,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS AMOUNT,");
                sb.Append("               TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT,");
                sb.Append("     DECODE(INVTBL.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INVTBL.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE");
                sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                sb.Append("               VENDOR_MST_TBL VMST,");
                sb.Append("               JOB_CARD_TRN JOB_IMP,");
                sb.Append("\tPORT_MST_TBL POD,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CURR");
                sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = JOB_IMP.JOBCARD_REF_NO");
                sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                if (bizType != 0)
                {
                    sb.Append("                   AND INVTBL.BUSINESS_TYPE = " + bizType);
                }
                if (process != 0)
                {
                    sb.Append("                   AND INVTBL.PROCESS_TYPE = " + process);
                }
                sb.Append(" AND POD.PORT_MST_PK = JOB_IMP.PORT_MST_POD_FK");
                sb.Append("\tAND POD.LOCATION_MST_FK IN(SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L START WITH L.LOCATION_MST_PK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK)");
                sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                if (flag == 0)
                {
                    sb.Append(" AND 1=2 ");
                }
                if (!string.IsNullOrEmpty(fromDt))
                {
                    sb.Append(" And TO_DATE(INVTBL.INVOICE_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(toDt))
                {
                    sb.Append("  And TO_DATE(INVTBL.INVOICE_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                }
                if (supplier > 0)
                {
                    sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                }
                //If cur > 0 Then
                //    sb.Append(" And INVTBL.CURRENCY_MST_FK = " & cur & "")
                //End If
                sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                sb.Append("                  INVTBL.INVOICE_DATE,");
                sb.Append("                  VMST.VENDOR_NAME,");
                sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                  CURR.CURRENCY_ID,");
                sb.Append("                  JOB_IMP.JOB_CARD_TRN_PK,");
                sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                sb.Append("                  INVTBL.CURRENCY_MST_FK,");
                sb.Append("                  INVTBL.BUSINESS_TYPE,");
                sb.Append("                  INVTBL.PROCESS_TYPE");

                sb.Append("        UNION");
                sb.Append("        SELECT DISTINCT INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                        INVTBL.INVOICE_REF_NO VOUCHERNO,");
                sb.Append("                        TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                sb.Append("                        VMST.VENDOR_NAME VENDOR,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS ESTIMATED_COST,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS AMOUNT,");
                sb.Append("                        TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT,");
                sb.Append("     DECODE(INVTBL.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INVTBL.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE");
                sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                sb.Append("               VENDOR_MST_TBL VMST,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CURR,");
                sb.Append("               MASTER_JC_SEA_IMP_TBL MJOB_IMP,");
                sb.Append("               JOB_CARD_TRN JOB_IMP,");
                sb.Append("\t              PORT_MST_TBL POD");
                sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = MJOB_IMP.MASTER_JC_REF_NO");
                sb.Append("           AND MJOB_IMP.MASTER_JC_SEA_IMP_PK = JOB_IMP.MASTER_JC_FK");
                sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                if (bizType != 0)
                {
                    sb.Append("                   AND INVTBL.BUSINESS_TYPE = " + bizType);
                }
                if (process != 0)
                {
                    sb.Append("                   AND INVTBL.PROCESS_TYPE = " + process);
                }
                sb.Append("           AND POD.PORT_MST_PK = JOB_IMP.PORT_MST_POD_FK");
                sb.Append("           AND POD.LOCATION_MST_FK IN(SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L START WITH L.LOCATION_MST_PK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK)");
                sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                if (flag == 0)
                {
                    sb.Append(" AND 1=2 ");
                }
                if (!string.IsNullOrEmpty(fromDt))
                {
                    sb.Append(" And TO_DATE(INVTBL.INVOICE_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(toDt))
                {
                    sb.Append("  And TO_DATE(INVTBL.INVOICE_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                }
                if (supplier > 0)
                {
                    sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                }
                //If cur > 0 Then
                //    sb.Append(" And INVTBL.CURRENCY_MST_FK = " & cur & "")
                //End If
                sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                sb.Append("                  INVTBL.INVOICE_DATE,");
                sb.Append("                  VMST.VENDOR_NAME,");
                sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                  CURR.CURRENCY_ID,");
                sb.Append("                  MJOB_IMP.MASTER_JC_SEA_IMP_PK,");
                sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                sb.Append("                  INVTBL.CURRENCY_MST_FK,");
                sb.Append("                  INVTBL.BUSINESS_TYPE,");
                sb.Append("                  INVTBL.PROCESS_TYPE");

                sb.Append("                  UNION");

                //-------AIR EXPORT----------
                sb.Append(" SELECT INVTBL.INV_SUPPLIER_PK,");
                sb.Append("               INVTBL.INVOICE_REF_NO VOUCHERNO,");
                sb.Append("               TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                sb.Append("               VMST.VENDOR_NAME VENDOR,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS ESTIMATED_COST,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS AMOUNT,");
                sb.Append("               TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT,");
                sb.Append("     DECODE(INVTBL.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INVTBL.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE");
                sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                sb.Append("               VENDOR_MST_TBL VMST,");
                sb.Append("               PORT_MST_TBL POL,");
                sb.Append("               JOB_CARD_TRN JOB_EXP,");
                sb.Append("               BOOKING_MST_TBL BST,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CURR");
                sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = JOB_EXP.JOBCARD_REF_NO");
                sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                sb.Append("           AND BST.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                if (bizType != 0)
                {
                    sb.Append("                   AND INVTBL.BUSINESS_TYPE = " + bizType);
                }
                if (process != 0)
                {
                    sb.Append("                   AND INVTBL.PROCESS_TYPE = " + process);
                }
                sb.Append("\tAND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("\tAND POL.LOCATION_MST_FK IN (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L START WITH L.LOCATION_MST_PK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK)");
                sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                if (flag == 0)
                {
                    sb.Append(" AND 1=2 ");
                }
                if (!string.IsNullOrEmpty(fromDt))
                {
                    sb.Append(" And TO_DATE(INVTBL.INVOICE_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(toDt))
                {
                    sb.Append("  And TO_DATE(INVTBL.INVOICE_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                }
                if (supplier > 0)
                {
                    sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                }
                //If cur > 0 Then
                //    sb.Append(" And INVTBL.CURRENCY_MST_FK = " & cur & "")
                //End If
                sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                sb.Append("                  INVTBL.INVOICE_DATE,");
                sb.Append("                  VMST.VENDOR_NAME,");
                sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                  CURR.CURRENCY_ID,");
                sb.Append("                  JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                sb.Append("                  INVTBL.CURRENCY_MST_FK,");
                sb.Append("                  INVTBL.BUSINESS_TYPE,");
                sb.Append("                  INVTBL.PROCESS_TYPE");

                sb.Append("        UNION");
                sb.Append("        SELECT DISTINCT INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                        INVTBL.INVOICE_REF_NO VOUCHERNO,");
                sb.Append("                        TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                sb.Append("                        VMST.VENDOR_NAME VENDOR,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS ESTIMATED_COST,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS AMOUNT,");
                sb.Append("                        TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT,");
                sb.Append("     DECODE(INVTBL.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INVTBL.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE");
                sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                sb.Append("               VENDOR_MST_TBL VMST,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CURR,");
                sb.Append("               MASTER_JC_AIR_EXP_TBL MJOB_EXP,");
                sb.Append("               JOB_CARD_TRN  JOB_EXP,");
                sb.Append("\tBOOKING_MST_TBL BST,");
                sb.Append("\tPORT_MST_TBL POL");
                sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = MJOB_EXP.MASTER_JC_REF_NO");
                sb.Append("           AND MJOB_EXP.MASTER_JC_AIR_EXP_PK = JOB_EXP.MASTER_JC_FK");
                sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                if (bizType != 0)
                {
                    sb.Append("                   AND INVTBL.BUSINESS_TYPE = " + bizType);
                }
                if (process != 0)
                {
                    sb.Append("                   AND INVTBL.PROCESS_TYPE = " + process);
                }
                sb.Append("\tAND BST.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("\tAND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("\tAND POL.LOCATION_MST_FK IN (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L START WITH L.LOCATION_MST_PK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK)");
                sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                if (flag == 0)
                {
                    sb.Append(" AND 1=2 ");
                }
                if (!string.IsNullOrEmpty(fromDt))
                {
                    sb.Append(" And TO_DATE(INVTBL.INVOICE_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(toDt))
                {
                    sb.Append("  And TO_DATE(INVTBL.INVOICE_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                }
                if (supplier > 0)
                {
                    sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                }
                //If cur > 0 Then
                //    sb.Append(" And INVTBL.CURRENCY_MST_FK = " & cur & "")
                //End If
                sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                sb.Append("                  INVTBL.INVOICE_DATE,");
                sb.Append("                  VMST.VENDOR_NAME,");
                sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                  CURR.CURRENCY_ID,");
                sb.Append("                  MJOB_EXP.MASTER_JC_AIR_EXP_PK,");
                sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                sb.Append("                  INVTBL.CURRENCY_MST_FK,");
                sb.Append("                  INVTBL.BUSINESS_TYPE,");
                sb.Append("                  INVTBL.PROCESS_TYPE");

                sb.Append("                  UNION");

                //----------------AIR IMPORT  -----------
                sb.Append(" SELECT INVTBL.INV_SUPPLIER_PK,");
                sb.Append("               INVTBL.INVOICE_REF_NO VOUCHERNO,");
                sb.Append("               TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                sb.Append("               VMST.VENDOR_NAME VENDOR,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS ESTIMATED_COST,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS AMOUNT,");
                sb.Append("               TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT,");
                sb.Append("     DECODE(INVTBL.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INVTBL.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE");
                sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                sb.Append("               VENDOR_MST_TBL VMST,");
                sb.Append("               JOB_CARD_TRN JOB_IMP,");
                sb.Append("\tPORT_MST_TBL POD,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CURR");
                sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = JOB_IMP.JOBCARD_REF_NO");
                sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                if (bizType != 0)
                {
                    sb.Append("                   AND INVTBL.BUSINESS_TYPE = " + bizType);
                }
                if (process != 0)
                {
                    sb.Append("                   AND INVTBL.PROCESS_TYPE = " + process);
                }
                sb.Append("           AND POD.PORT_MST_PK = JOB_IMP.PORT_MST_POD_FK");
                sb.Append("           AND POD.LOCATION_MST_FK IN(SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L START WITH L.LOCATION_MST_PK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK)");
                sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                if (flag == 0)
                {
                    sb.Append(" AND 1=2 ");
                }
                if (!string.IsNullOrEmpty(fromDt))
                {
                    sb.Append(" And TO_DATE(INVTBL.INVOICE_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(toDt))
                {
                    sb.Append("  And TO_DATE(INVTBL.INVOICE_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                }
                if (supplier > 0)
                {
                    sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                }
                //If cur > 0 Then
                //    sb.Append(" And INVTBL.CURRENCY_MST_FK = " & cur & "")
                //End If
                sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                sb.Append("                  INVTBL.INVOICE_DATE,");
                sb.Append("                  VMST.VENDOR_NAME,");
                sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                  CURR.CURRENCY_ID,");
                sb.Append("                  JOB_IMP.JOB_CARD_TRN_PK,");
                sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                sb.Append("                  INVTBL.CURRENCY_MST_FK,");
                sb.Append("                  INVTBL.BUSINESS_TYPE,");
                sb.Append("                  INVTBL.PROCESS_TYPE");
                sb.Append("        UNION");
                sb.Append("        SELECT DISTINCT INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                        INVTBL.INVOICE_REF_NO VOUCHERNO,");
                sb.Append("                        TO_DATE(INVTBL.INVOICE_DATE, DATEFORMAT) VOUCHERDATE,");
                sb.Append("                        VMST.VENDOR_NAME VENDOR,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ESTIMATED_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS ESTIMATED_COST,");
                sb.Append("               SUM(ROUND(INVTRNTBL.ACTUAL_AMT * ");
                sb.Append("               GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + cur + ",INVTBL.INVOICE_DATE),");
                sb.Append("               2)) AS AMOUNT,");
                sb.Append("                        TO_DATE(INVTBL.SUPPLIER_DUE_DT, DATEFORMAT) SUPPLIER_DUE_DT,");
                sb.Append("     DECODE(INVTBL.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INVTBL.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE");
                sb.Append("          FROM INV_SUPPLIER_TBL INVTBL,");
                sb.Append("               INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                sb.Append("               VENDOR_MST_TBL VMST,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CURR,");
                sb.Append("               MASTER_JC_AIR_IMP_TBL MJOB_IMP,");
                sb.Append("               JOB_CARD_TRN JOB_IMP,");
                sb.Append("\tPORT_MST_TBL POD");
                sb.Append("         WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
                sb.Append("           AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                sb.Append("           AND INVTRNTBL.JOBCARD_REF_NO = MJOB_IMP.MASTER_JC_REF_NO");
                sb.Append("           AND MJOB_IMP.MASTER_JC_AIR_IMP_PK = JOB_IMP.MASTER_JC_FK");
                sb.Append("           AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                if (bizType != 0)
                {
                    sb.Append("                   AND INVTBL.BUSINESS_TYPE = " + bizType);
                }
                if (process != 0)
                {
                    sb.Append("                   AND INVTBL.PROCESS_TYPE = " + process);
                }
                sb.Append("           AND POD.PORT_MST_PK = JOB_IMP.PORT_MST_POD_FK");
                sb.Append("           AND POD.LOCATION_MST_FK IN(SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L START WITH L.LOCATION_MST_PK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK)");
                sb.Append("           AND INVTRNTBL.ELEMENT_APPROVED = 0");
                if (flag == 0)
                {
                    sb.Append(" AND 1=2 ");
                }
                if (!string.IsNullOrEmpty(fromDt))
                {
                    sb.Append(" And TO_DATE(INVTBL.INVOICE_DATE,dateformat) >= TO_DATE('" + fromDt.PadLeft(10) + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(toDt))
                {
                    sb.Append("  And TO_DATE(INVTBL.INVOICE_DATE,dateformat) <= TO_DATE('" + toDt.PadLeft(10) + "',dateformat)");
                }
                if (supplier > 0)
                {
                    sb.Append(" And VMST.VENDOR_MST_PK = " + supplier + " ");
                }
                //If cur > 0 Then
                //    sb.Append(" And INVTBL.CURRENCY_MST_FK = " & cur & "")
                //End If
                sb.Append("         GROUP BY INVTBL.INVOICE_REF_NO,");
                sb.Append("                  INVTBL.INVOICE_DATE,");
                sb.Append("                  VMST.VENDOR_NAME,");
                sb.Append("                  INVTRNTBL.ELEMENT_APPROVED,");
                sb.Append("                  INVTBL.INV_SUPPLIER_PK,");
                sb.Append("                  CURR.CURRENCY_ID,");
                sb.Append("                  MJOB_IMP.MASTER_JC_AIR_IMP_PK,");
                sb.Append("                  INVTBL.SUPPLIER_DUE_DT,");
                sb.Append("                  INVTBL.SUPPLIER_INV_NO,");
                sb.Append("                  INVTBL.CURRENCY_MST_FK,");
                sb.Append("                  INVTBL.BUSINESS_TYPE,");
                sb.Append("                  INVTBL.PROCESS_TYPE");

                strSQL = " select * from (";
                strSQL += sb.ToString() + ")";
                ds1 = objWF.GetDataSet(strSQL);
                if (ds1.Tables[0].Rows.Count > 0)
                {
                    TotalRecords = ds1.Tables[0].Rows.Count;
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
                }

                strSQL = " SELECT * FROM (SELECT ROWNUM SlNr, X.* FROM(";
                strSQL += sb.ToString();
                strSQL += " ORDER BY VOUCHERDATE DESC, VOUCHERNO DESC";
                if (ExportExcel == 0)
                {
                    strSQL += " ) X ) WHERE SlNr Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " ) X ) ";
                }
                ds = objWF.GetDataSet(strSQL);

                return ds;
            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        #endregion "Fetch VoucherListing"
    }
}