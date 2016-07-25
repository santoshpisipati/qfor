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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_SupplierSalesReport : CommonFeatures
    {
        #region "Get Supplier Type"

        /// <summary>
        /// Gets the type of the supplier.
        /// </summary>
        /// <returns></returns>
        public DataSet GetSupplierType()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT VTMT.VENDOR_TYPE_PK, VTMT.VENDOR_TYPE_NAME");
                sb.Append("  FROM VENDOR_TYPE_MST_TBL VTMT");
                sb.Append(" WHERE VTMT.ACTIVE_FLAG = 1");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Get Supplier Type"

        #region "Fetch Grid Data"

        /// <summary>
        /// Fetches the grid data.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="SupplierPK">The supplier pk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="ServiceType">Type of the service.</param>
        /// <param name="VendorTypePK">The vendor type pk.</param>
        /// <param name="VslId">The VSL identifier.</param>
        /// <param name="VoyId">The voy identifier.</param>
        /// <returns></returns>
        public DataSet FetchGridData(int BizType = 0, int ProcessType = 0, string POLPK = "", string PODPK = "", string SupplierPK = "", string FromDt = "", string ToDt = "", string CargoType = "", string ServiceType = "", string VendorTypePK = "",
        string VslId = "", string VoyId = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sb2 = new System.Text.StringBuilder(5000);
            DataSet MainDS = new DataSet();
            OracleDataAdapter DA = new OracleDataAdapter();
            WorkFlow objWF = new WorkFlow();
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);

            string JobCostQuery = " (SELECT SUM(NVL(JCOST.TOTAL_COST,0)*GET_EX_RATE_BUY(JTSEC.CURRENCY_MST_FK," + BaseCurrFk + ",JCSET.JOBCARD_DATE)) ";
            JobCostQuery += "  FROM JOB_TRN_COST JCOST WHERE JCOST.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK) ";

            string JobCostQuery1 = " (SELECT SUM(NVL(JCOST.TOTAL_COST,0)*GET_EX_RATE_BUY(JTSEC.CURRENCY_MST_FK," + BaseCurrFk + ",JCSET.JOBCARD_DATE)) ";
            JobCostQuery1 += "  FROM JOB_TRN_COST JCOST WHERE JCOST.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK) ";

            try
            {
                sb.Append(" SELECT DISTINCT  VENDOR_MST_PK, VENDOR_NAME, SUM(TOTALCOST) TOTALCOST");
                sb.Append(" FROM (");
                sb.Append("SELECT DISTINCT VMT.VENDOR_MST_PK, VMT.VENDOR_NAME,");
                sb.Append("       CASE");
                sb.Append("         WHEN PAYMENT.INV_SUPPLIER_TBL_FK IS NOT NULL THEN");
                sb.Append("          SUM(DISTINCT PAYMENT.PAID_AMT)");
                sb.Append("         WHEN SUPP_INV.INV_SUPPLIER_PK IS NOT NULL THEN");
                sb.Append("          SUM(DISTINCT SUPP_INV.INV_AMT)");
                sb.Append("         ELSE");
                sb.Append("           " + JobCostQuery + " ");
                sb.Append("       END TOTALCOST");
                sb.Append("  FROM ");
                if (ProcessType == 1)
                {
                    sb.Append("   BOOKING_MST_TBL BST,");
                }
                sb.Append("       JOB_CARD_TRN JCSET,");
                sb.Append("       PORT_MST_TBL POL,");
                sb.Append("       PORT_MST_TBL POD,");
                sb.Append("       JOB_TRN_COST JTSEC,");
                if (BizType == 1)
                {
                    sb.Append("       AIRLINE_MST_TBL AMT,");
                }
                sb.Append("       VENDOR_MST_TBL VMT,");
                sb.Append("       VENDOR_SERVICES_TRN VS,");

                sb.Append("       (SELECT IST.INV_SUPPLIER_PK,");
                sb.Append("               ISTT.JOBCARD_REF_NO,");
                sb.Append("               CASE");
                sb.Append("                 WHEN SUM(NVL(ISTT.PAYABLE_AMT, 0)) > 0 THEN");
                sb.Append("                  ROUND(SUM(NVL(ISTT.PAYABLE_AMT, 0)) *");
                sb.Append("                  GET_EX_RATE_BUY(IST.CURRENCY_MST_FK, " + BaseCurrFk + ", IST.INVOICE_DATE),2)");
                sb.Append("                 ELSE");
                sb.Append("                  ROUND(SUM(NVL(ISTT.TOTAL_COST, 0)) *");
                sb.Append("                  GET_EX_RATE_BUY(IST.CURRENCY_MST_FK, " + BaseCurrFk + ", IST.INVOICE_DATE),2)");
                sb.Append("               END INV_AMT");
                sb.Append("          FROM INV_SUPPLIER_TBL IST, INV_SUPPLIER_TRN_TBL ISTT");
                sb.Append("         WHERE IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                sb.Append("         AND IST.APPROVED <> 3 ");
                if (BizType != 3)
                {
                    sb.Append("           AND IST.BUSINESS_TYPE = " + BizType);
                }
                sb.Append("           AND IST.PROCESS_TYPE = " + ProcessType);
                sb.Append("         GROUP BY IST.INV_SUPPLIER_PK, ISTT.JOBCARD_REF_NO,IST.CURRENCY_MST_FK,IST.INVOICE_DATE) SUPP_INV,");
                sb.Append("       (SELECT PTT.INV_SUPPLIER_TBL_FK,");
                sb.Append("         ROUND(SUM(NVL(PTT.PAID_AMOUNT_HDR_CURR, 0)) *");
                sb.Append("             GET_EX_RATE_BUY(PT.CURRENCY_MST_FK, " + BaseCurrFk + ", PT.PAYMENT_DATE),");
                sb.Append("             2) PAID_AMT");
                sb.Append("          FROM PAYMENTS_TBL PT, PAYMENT_TRN_TBL PTT");
                sb.Append("         WHERE PT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("         AND PT.APPROVED <> 3 ");
                sb.Append("         GROUP BY PTT.INV_SUPPLIER_TBL_FK,PT.CURRENCY_MST_FK, PT.PAYMENT_DATE) PAYMENT");
                sb.Append("  WHERE");
                if (ProcessType == 1)
                {
                    sb.Append("  BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK");
                    sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                    sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                    sb.Append("   AND BST.STATUS <> 3");
                }
                else
                {
                    sb.Append("   POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                    sb.Append("   AND POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK");
                }
                if (BizType == 1)
                {
                    if (ProcessType == 1)
                    {
                        sb.Append("   AND BST.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+)");
                    }
                    else
                    {
                        sb.Append("   AND JCSET.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+)");
                    }
                }
                sb.Append("   AND JTSEC.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("   AND JTSEC.VENDOR_MST_FK = VMT.VENDOR_MST_PK(+)");
                sb.Append("   AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("   AND JTSEC.INV_SUPPLIER_FK = SUPP_INV.INV_SUPPLIER_PK(+)");
                sb.Append("   AND PAYMENT.INV_SUPPLIER_TBL_FK(+) = SUPP_INV.INV_SUPPLIER_PK");
                sb.Append("  AND VMT.VENDOR_MST_PK IS NOT NULL ");
                if (!string.IsNullOrEmpty(POLPK))
                {
                    sb.Append("   AND POL.PORT_MST_PK =" + POLPK);
                }
                if (!string.IsNullOrEmpty(PODPK))
                {
                    sb.Append("   AND POD.PORT_MST_PK=" + PODPK);
                }
                if (!string.IsNullOrEmpty(SupplierPK))
                {
                    sb.Append(" AND  VMT.VENDOR_MST_PK IN( " + SupplierPK + ")");
                }
                if (BizType == 1)
                {
                    if (!string.IsNullOrEmpty(VslId))
                    {
                        sb.Append(" AND  AMT.AIRLINE_NAME ='" + VslId.Trim() + "'");
                    }
                    if (!string.IsNullOrEmpty(VoyId))
                    {
                        sb.Append(" AND  JCSET.VOYAGE_FLIGHT_NO ='" + VoyId.Trim() + "'");
                    }
                }
                else
                {
                    if (!string.IsNullOrEmpty(VslId))
                    {
                        sb.Append(" AND  JCSET.VESSEL_NAME ='" + VslId.Trim() + "' ");
                    }
                    if (!string.IsNullOrEmpty(VoyId))
                    {
                        sb.Append(" AND  JCSET.VOYAGE_FLIGHT_NO ='" + VoyId.Trim() + "'");
                    }
                }
                if (!string.IsNullOrEmpty(VendorTypePK) & Convert.ToInt32(VendorTypePK) != -1)
                {
                    sb.Append("  AND VS.VENDOR_TYPE_FK = " + VendorTypePK);
                }
                if (ProcessType == 1)
                {
                    if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                    {
                        sb.Append("    AND BST.BOOKING_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                    }
                    else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                    {
                        sb.Append("   AND BST.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                    }
                    else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                    {
                        sb.Append("   AND BST.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                    }
                }
                else
                {
                    if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                    {
                        sb.Append("    AND JCSET.JOBCARD_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                    }
                    else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                    {
                        sb.Append("   AND JCSET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                    }
                    else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                    {
                        sb.Append("   AND JCSET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                    }
                }
                if (CargoType != "0")
                {
                    if (ProcessType == 1 & BizType == 2)
                    {
                        sb.Append("  AND BST.CARGO_TYPE=" + CargoType);
                    }
                    else if (ProcessType == 2 & BizType == 2)
                    {
                        sb.Append("  AND JCSET.CARGO_TYPE=" + CargoType);
                    }
                }
                if (ServiceType != "1")
                {
                    //'Ordered Service
                    if (ServiceType == "2")
                    {
                        sb.Append("  AND SUPP_INV.INV_SUPPLIER_PK IS  NULL");
                        sb.Append("  AND PAYMENT.INV_SUPPLIER_TBL_FK IS NULL");
                        //'Invoice Raised
                    }
                    else if (ServiceType == "3")
                    {
                        sb.Append("  AND SUPP_INV.INV_SUPPLIER_PK IS NOT NULL");
                        sb.Append("  AND PAYMENT.INV_SUPPLIER_TBL_FK IS NULL");
                        //' Payment Settled
                    }
                    else if (ServiceType == "4")
                    {
                        sb.Append("  AND PAYMENT.INV_SUPPLIER_TBL_FK IS NOT  NULL");
                    }
                }
                if (BizType != 3)
                {
                    sb.Append("  AND JCSET.BUSINESS_TYPE=" + BizType);
                }
                if (ProcessType > 0)
                {
                    sb.Append("  AND JCSET.PROCESS_TYPE=" + ProcessType);
                }
                sb.Append("   GROUP BY VMT.VENDOR_MST_PK,");
                sb.Append("                  VMT.VENDOR_NAME,");
                sb.Append("                  PAYMENT.INV_SUPPLIER_TBL_FK,");
                sb.Append("                  SUPP_INV.INV_SUPPLIER_PK,");
                sb.Append("                  JCSET.JOBCARD_DATE,");
                sb.Append("                  JTSEC.CURRENCY_MST_FK,");
                sb.Append("                  JCSET.JOB_CARD_TRN_PK) ");
                sb.Append("   GROUP BY VENDOR_MST_PK, VENDOR_NAME ");

                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "GROUP");

                sb.Remove(0, sb.Length);
                sb1.Remove(0, sb1.Length);

                sb.Append("SELECT DISTINCT VMT.VENDOR_MST_PK,");
                if (ProcessType == 1)
                {
                    sb.Append("       BST.BOOKING_MST_PK BOOKINGPK,");
                    sb.Append("       BST.BOOKING_REF_NO,");
                    sb.Append("       TO_CHAR(BST.BOOKING_DATE,dateFormat)BOOKING_DATE,");
                }
                else
                {
                    sb.Append("       ''BOOKINGPK,");
                    sb.Append("       ''BOOKING_REF_NO,");
                    sb.Append("       ''BOOKING_DATE,");
                }
                sb.Append("       JCSET.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       JCSET.JOBCARD_REF_NO,");
                sb.Append("       TO_CHAR(JCSET.JOBCARD_DATE,dateFormat)JOBCARD_DATE,");
                if (ProcessType == 1)
                {
                    sb.Append("     MBL.MBL_EXP_TBL_PK MPK, ");
                    sb.Append("     MBL.MBL_REF_NO MNO, ");
                }
                else
                {
                    sb.Append("     ''MPK, ");
                    sb.Append("     JCSET.MBL_MAWB_REF_NO MNO, ");
                }

                if (BizType == 2 | BizType == 3)
                {
                    sb.Append("       (CASE");
                    sb.Append("         WHEN (NVL(JCSET.VESSEL_NAME, '') || '/' || NVL(JCSET.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                    sb.Append("          ''");
                    sb.Append("         ELSE");
                    sb.Append("          NVL(JCSET.VESSEL_NAME, '') || '/' || NVL(JCSET.VOYAGE_FLIGHT_NO, '')");
                    sb.Append("       END) AS VESVOYAGE,");
                    sb.Append("      ''FLIGHTNR,");
                    sb.Append("       (CASE");
                    sb.Append("         WHEN (NVL(JCSET.VESSEL_NAME, '') || '/' || NVL(JCSET.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                    sb.Append("          ''");
                    sb.Append("         ELSE");
                    sb.Append("          NVL(JCSET.VESSEL_NAME, '') || '/' || NVL(JCSET.VOYAGE_FLIGHT_NO, '')");
                    sb.Append("       END) AS VSLFLIGHT,");
                }
                else if (BizType == 1)
                {
                    sb.Append("   ''VESVOYAGE,");
                    sb.Append("   JCSET.VOYAGE_FLIGHT_NO AS FLIGHTNR,");
                    sb.Append("   JCSET.VOYAGE_FLIGHT_NO AS VSLFLIGHT,");
                }
                sb.Append("       POL.PORT_ID POL,");
                sb.Append("       POL.PORT_ID AOO,");
                sb.Append("       POL.PORT_ID AOOPOL,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       POD.PORT_ID AOD,");
                sb.Append("       POD.PORT_ID AODPOD,");
                sb.Append("       TO_CHAR(JCSET.ETD_DATE,dateFormat)ETD_DATE,");
                sb.Append("       TO_CHAR(JCSET.ETA_DATE,dateFormat)ETA_DATE,");
                if (ProcessType == 1)
                {
                    sb.Append("       DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                }
                else if (ProcessType == 2 & BizType == 2)
                {
                    sb.Append("       DECODE(JCSET.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                }
                else
                {
                    sb.Append("      ''CARGO_TYPE,");
                }
                if (BizType == 1)
                {
                    sb.Append(" 0 TEU,");
                    sb.Append("  (SELECT SUM(NVL(JTSE.CHARGEABLE_WEIGHT, 0))");
                    sb.Append("   FROM JOB_TRN_CONT JTSE");
                    sb.Append("  WHERE JTSE.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK) CHWT,");
                }
                else
                {
                    sb.Append("       (SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))");
                    sb.Append("          FROM JOB_TRN_CONT JTSE, CONTAINER_TYPE_MST_TBL CTMT");
                    sb.Append("         WHERE JTSE.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                    sb.Append("           AND JTSE.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)) TEU,");
                    sb.Append(" 0 CHWT,");
                }
                sb.Append("       CASE");
                sb.Append("         WHEN SUPP_INV.INV_SUPPLIER_PK IS NOT NULL AND PAYMENT.INV_SUPPLIER_TBL_FK IS NULL  THEN");
                sb.Append("          'Invoice Received'");
                sb.Append("         WHEN PAYMENT.INV_SUPPLIER_TBL_FK IS NOT NULL THEN");
                sb.Append("          'Payment Raised'");
                sb.Append("         ELSE");
                sb.Append("          'Ordered Service'");
                sb.Append("       END STATUS,");
                sb.Append("       CASE");
                sb.Append("         WHEN PAYMENT.INV_SUPPLIER_TBL_FK IS NOT NULL THEN");
                sb.Append("          SUM(DISTINCT PAYMENT.PAID_AMT)");
                sb.Append("         WHEN SUPP_INV.INV_SUPPLIER_PK IS NOT NULL THEN");
                sb.Append("          SUM(DISTINCT SUPP_INV.INV_AMT)");
                sb.Append("         ELSE");
                sb.Append("           " + JobCostQuery + " ");
                sb.Append("       END EACOST, 'SEA' BizType ");
                sb.Append("  FROM ");
                if (ProcessType == 1)
                {
                    sb.Append("   BOOKING_MST_TBL BST,");
                    sb.Append("       MBL_EXP_TBL MBL,");
                }
                if (BizType == 1)
                {
                    sb.Append("       AIRLINE_MST_TBL AMT,");
                }
                sb.Append("       JOB_CARD_TRN JCSET,");
                sb.Append("       PORT_MST_TBL POL,");
                sb.Append("       PORT_MST_TBL POD,");
                sb.Append("       JOB_TRN_COST JTSEC,");
                sb.Append("       VENDOR_MST_TBL VMT,");
                sb.Append("       VENDOR_SERVICES_TRN VS,");
                sb.Append("       (SELECT IST.INV_SUPPLIER_PK,");
                sb.Append("               ISTT.JOBCARD_REF_NO,");
                sb.Append("               CASE");
                sb.Append("                 WHEN SUM(NVL(ISTT.PAYABLE_AMT, 0)) > 0 THEN");
                sb.Append("                  ROUND(SUM(NVL(ISTT.PAYABLE_AMT, 0)) *");
                sb.Append("                  GET_EX_RATE_BUY(IST.CURRENCY_MST_FK, " + BaseCurrFk + ", IST.INVOICE_DATE),2)");
                sb.Append("                 ELSE");
                sb.Append("                  ROUND(SUM(NVL(ISTT.TOTAL_COST, 0)) *");
                sb.Append("                  GET_EX_RATE_BUY(IST.CURRENCY_MST_FK, " + BaseCurrFk + ", IST.INVOICE_DATE),2)");
                sb.Append("               END INV_AMT");
                sb.Append("          FROM INV_SUPPLIER_TBL IST, INV_SUPPLIER_TRN_TBL ISTT");
                sb.Append("         WHERE IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
                if (BizType != 3)
                {
                    sb.Append("           AND IST.BUSINESS_TYPE = " + BizType);
                }
                sb.Append("           AND IST.PROCESS_TYPE = " + ProcessType);
                sb.Append("         GROUP BY IST.INV_SUPPLIER_PK, ISTT.JOBCARD_REF_NO,IST.CURRENCY_MST_FK,IST.INVOICE_DATE) SUPP_INV,");
                sb.Append("       (SELECT PTT.INV_SUPPLIER_TBL_FK,");
                sb.Append("         ROUND(SUM(NVL(PTT.PAID_AMOUNT_HDR_CURR, 0)) *");
                sb.Append("             GET_EX_RATE_BUY(PT.CURRENCY_MST_FK, " + BaseCurrFk + ", PT.PAYMENT_DATE),");
                sb.Append("             2) PAID_AMT");
                sb.Append("          FROM PAYMENTS_TBL PT, PAYMENT_TRN_TBL PTT");
                sb.Append("         WHERE PT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
                sb.Append("         GROUP BY PTT.INV_SUPPLIER_TBL_FK,PT.CURRENCY_MST_FK, PT.PAYMENT_DATE) PAYMENT");
                sb.Append(" WHERE ");
                if (ProcessType == 1)
                {
                    sb.Append("  BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK");
                    sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                    sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                    sb.Append("   AND BST.STATUS <> 3");
                    sb.Append("   AND MBL.MBL_EXP_TBL_PK(+) = JCSET.MBL_MAWB_FK");
                }
                else
                {
                    sb.Append("   POL.PORT_MST_PK = JCSET.PORT_MST_POL_FK");
                    sb.Append("   AND POD.PORT_MST_PK = JCSET.PORT_MST_POD_FK");
                }
                if (BizType == 1)
                {
                    if (ProcessType == 1)
                    {
                        sb.Append("   AND BST.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+)");
                    }
                    else
                    {
                        sb.Append("   AND JCSET.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+)");
                    }
                }
                sb.Append("   AND JTSEC.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("   AND JTSEC.VENDOR_MST_FK = VMT.VENDOR_MST_PK(+)");
                sb.Append("   AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("   AND JTSEC.INV_SUPPLIER_FK = SUPP_INV.INV_SUPPLIER_PK(+)");
                sb.Append("   AND PAYMENT.INV_SUPPLIER_TBL_FK(+) = SUPP_INV.INV_SUPPLIER_PK");
                sb.Append("  AND VMT.VENDOR_MST_PK IS NOT NULL ");
                if (!string.IsNullOrEmpty(POLPK))
                {
                    sb.Append("   AND POL.PORT_MST_PK =" + POLPK);
                }
                if (!string.IsNullOrEmpty(PODPK))
                {
                    sb.Append("   AND POD.PORT_MST_PK=" + PODPK);
                }
                if (!string.IsNullOrEmpty(SupplierPK))
                {
                    sb.Append("  AND VMT.VENDOR_MST_PK IN( " + SupplierPK + ")");
                }
                if (BizType == 1)
                {
                    if (!string.IsNullOrEmpty(VslId))
                    {
                        sb.Append(" AND  AMT.AIRLINE_NAME ='" + VslId.Trim() + "'");
                    }
                    if (!string.IsNullOrEmpty(VoyId))
                    {
                        sb.Append(" AND  JCSET.VOYAGE_FLIGHT_NO ='" + VoyId.Trim() + "'");
                    }
                }
                else
                {
                    if (!string.IsNullOrEmpty(VslId))
                    {
                        sb.Append(" AND  JCSET.VESSEL_NAME ='" + VslId.Trim() + "' ");
                    }
                    if (!string.IsNullOrEmpty(VoyId))
                    {
                        sb.Append(" AND  JCSET.VOYAGE_FLIGHT_NO ='" + VoyId.Trim() + "'");
                    }
                }
                if (!string.IsNullOrEmpty(VendorTypePK) & Convert.ToInt32(VendorTypePK) != -1)
                {
                    sb.Append("  AND VS.VENDOR_TYPE_FK = " + VendorTypePK);
                }
                if (ProcessType == 1)
                {
                    if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                    {
                        sb.Append("    AND BST.BOOKING_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                    }
                    else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                    {
                        sb.Append("   AND BST.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                    }
                    else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                    {
                        sb.Append("   AND BST.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                    }
                }
                else
                {
                    if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                    {
                        sb.Append("    AND JCSET.JOBCARD_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                    }
                    else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                    {
                        sb.Append("   AND JCSET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                    }
                    else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                    {
                        sb.Append("   AND JCSET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                    }
                }
                if (CargoType != "0")
                {
                    if (ProcessType == 1 & BizType == 2)
                    {
                        sb.Append("  AND BST.CARGO_TYPE=" + CargoType);
                    }
                    else if (ProcessType == 2 & BizType == 2)
                    {
                        sb.Append("  AND JCSET.CARGO_TYPE=" + CargoType);
                    }
                }
                if (ServiceType != "1")
                {
                    //'Ordered Service
                    if (ServiceType == "2")
                    {
                        sb.Append("  AND SUPP_INV.INV_SUPPLIER_PK IS  NULL");
                        sb.Append("  AND PAYMENT.INV_SUPPLIER_TBL_FK IS NULL");
                        //'Invoice Raised
                    }
                    else if (ServiceType == "3")
                    {
                        sb.Append("  AND SUPP_INV.INV_SUPPLIER_PK IS NOT NULL");
                        sb.Append("  AND PAYMENT.INV_SUPPLIER_TBL_FK IS NULL");
                        //' Payment Settled
                    }
                    else if (ServiceType == "4")
                    {
                        sb.Append("  AND PAYMENT.INV_SUPPLIER_TBL_FK IS NOT  NULL");
                    }
                }
                if (BizType != 3)
                {
                    sb.Append("  AND JCSET.BUSINESS_TYPE=" + BizType);
                }
                if (ProcessType > 0)
                {
                    sb.Append("  AND JCSET.PROCESS_TYPE=" + ProcessType);
                }
                sb.Append(" GROUP BY VMT.VENDOR_MST_PK,");
                if (ProcessType == 1)
                {
                    sb.Append("          BST.BOOKING_MST_PK,");
                    sb.Append("          BST.BOOKING_REF_NO,");
                    sb.Append("          BST.BOOKING_DATE,");
                    sb.Append("          BST.CARGO_TYPE,");
                    sb.Append("         MBL.MBL_EXP_TBL_PK, ");
                    sb.Append("         MBL.MBL_REF_NO, ");
                }
                else
                {
                    sb.Append("         JCSET.MBL_MAWB_REF_NO, ");
                }
                if (BizType == 2 | BizType == 3)
                {
                    sb.Append("          JCSET.VESSEL_NAME,");
                    sb.Append("          JCSET.VOYAGE_FLIGHT_NO,");
                    if (ProcessType == 2)
                    {
                        sb.Append("         JCSET.CARGO_TYPE,");
                    }
                }
                else if (BizType == 1)
                {
                    sb.Append("  JCSET.VOYAGE_FLIGHT_NO,");
                }
                sb.Append("          JCSET.JOB_CARD_TRN_PK,");
                sb.Append("          JCSET.JOBCARD_REF_NO,");
                sb.Append("          JCSET.JOBCARD_DATE,");

                sb.Append("          POL.PORT_ID,");
                sb.Append("          POD.PORT_ID,");
                sb.Append("          JCSET.ETD_DATE,");
                sb.Append("          JCSET.ETA_DATE,");
                sb.Append("          PAYMENT.INV_SUPPLIER_TBL_FK,");
                sb.Append("          SUPP_INV.INV_SUPPLIER_PK,JTSEC.CURRENCY_MST_FK");

                if (BizType == 3)
                {
                    if (ProcessType == 1)
                    {
                        sb.Append("  ORDER BY BOOKING_DATE DESC ");
                    }
                    else
                    {
                        sb.Append("  ORDER BY JOBCARD_DATE DESC ");
                    }
                }
                else
                {
                    if (ProcessType == 1)
                    {
                        sb.Append("  ORDER BY TO_DATE(BOOKING_DATE) DESC ");
                    }
                    else
                    {
                        sb.Append("  ORDER BY TO_DATE(JOBCARD_DATE) DESC ");
                    }
                }

                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "LOCATION");

                DataRelation relLocGroup_Details = new DataRelation("LOCGROUP", new DataColumn[] { MainDS.Tables[0].Columns["VENDOR_MST_PK"] }, new DataColumn[] { MainDS.Tables[1].Columns["VENDOR_MST_PK"] });
                relLocGroup_Details.Nested = true;
                MainDS.Relations.Add(relLocGroup_Details);
                return MainDS;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw;
            }
        }

        #endregion "Fetch Grid Data"
    }
}