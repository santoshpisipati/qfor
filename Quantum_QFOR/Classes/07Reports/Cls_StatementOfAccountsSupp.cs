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
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_StatementOfAccountsSupp : CommonFeatures
    {
        #region "StatmentofAccounts"

        /// <summary>
        /// Gets the stat acc supp.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="Loc">The loc.</param>
        /// <returns></returns>
        public DataSet GetStatAccSupp(int BizType, int Process, string fromDate, string toDate = "", int Customer = 0, string Loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSQL = new StringBuilder();
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            StringBuilder strCondition = new StringBuilder();
            string condition = null;
            Int32 intLoc = default(Int32);
            intLoc = (int)HttpContext.Current.Session["LOGED_IN_LOC_FK"];
            if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate))))
            {
                strCondition.Append(" AND I.invoice_date BETWEEN TO_DATE('" + fromDate + "','" + dateFormat + "')  AND TO_DATE('" + toDate + "','" + dateFormat + "') ");
            }
            else if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
            {
                strCondition.Append(" AND I.invoice_date >= TO_DATE('" + fromDate + "',dateformat) ");
            }
            else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
            {
                strCondition.Append(" AND I.invoice_date >= TO_DATE('" + toDate + "',dateformat) ");
            }
            condition = strCondition.ToString();
            if (BizType == 2 & Process == 1)
            {
                strSQL.Append("SELECT QRY.*");
                strSQL.Append("  FROM (SELECT ROWNUM \"SL.NR\", T.*");
                strSQL.Append("          FROM (SELECT IST.jobcard_ref_no JOBCARD,POL.PORT_ID POL,POD.PORT_ID POD,I.INVOICE_REF_NO SupplierRefNO,");
                strSQL.Append("                       i.INVOICE_DATE Inv_date,");
                strSQL.Append("                       I.INVOICE_AMT,");
                strSQL.Append("                       LMT.LOCATION_NAME,");
                strSQL.Append("                       V.VENDOR_ID,");
                strSQL.Append("                       P.PAYMENT_REF_NO,");
                strSQL.Append("                       P.PAYMENT_DATE,");
                strSQL.Append("                       p.payment_amt,");
                strSQL.Append("                       (i.invoice_amt - NVL(p.payment_amt, 0.00))balance,");
                strSQL.Append("                       CUR.CURRENCY_ID");
                strSQL.Append("                       ");
                strSQL.Append("                  FROM PAYMENTS_TBL          P,BOOKING_MST_TBL b,");
                strSQL.Append("                       PAYMENT_TRN_TBL       PTRN,");
                strSQL.Append("                       CURRENCY_TYPE_MST_TBL CUR,");
                strSQL.Append("                       USER_MST_TBL          UMT,");
                strSQL.Append("                       INV_SUPPLIER_TBL      I,");
                strSQL.Append("                       VENDOR_MST_TBL        V,");
                strSQL.Append("                       INV_SUPPLIER_TRN_TBL  IST,");
                strSQL.Append("                       JOB_CARD_TRN   JOB,");
                strSQL.Append("                       location_mst_tbl       LMT,PORT_MST_TBL          POL,PORT_MST_TBL          POD, ");
                strSQL.Append("                       VESSEL_VOYAGE_TRN      VTRN,");
                strSQL.Append("                       JOB_TRN_PIA     PIA ");
                strSQL.Append("                 WHERE JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK");
                strSQL.Append("                   and I.INV_SUPPLIER_PK =Ist.INV_SUPPLIER_TBL_FK");
                strSQL.Append("                   and job.BOOKING_MST_FK=b.BOOKING_MST_PK");
                strSQL.Append("                   AND P.PAYMENT_TBL_PK = PTRN.PAYMENTS_TBL_FK");
                strSQL.Append("                   AND CUR.CURRENCY_MST_PK = P.CURRENCY_MST_FK");
                strSQL.Append("                   AND P.CREATED_BY_FK = UMT.USER_MST_PK");
                strSQL.Append("                   AND PTRN.INV_SUPPLIER_TBL_FK = I.INV_SUPPLIER_PK");
                strSQL.Append("                   AND V.VENDOR_MST_PK = P.VENDOR_MST_FK");
                strSQL.Append("                   AND lmt.location_mst_pk=umt.default_location_fk");
                strSQL.Append("                   AND V.VENDOR_MST_PK = " + Customer);
                strSQL.Append("                   AND P.PROCESS_TYPE = " + Process);
                strSQL.Append("                   AND P.BUSINESS_TYPE =" + BizType);
                if (Loc != "0")
                {
                    strSQL.Append("                   AND UMT.DEFAULT_LOCATION_FK =" + Loc);
                }
                strSQL.Append("                    and pia.JOB_TRN_PIA_PK = IST.JOB_CARD_PIA_FK");
                strSQL.Append("                    and pia.JOB_CARD_TRN_FK = job.JOB_CARD_TRN_PK");
                strSQL.Append("                   and p.approved='1' AND B.PORT_MST_POD_FK=POD.PORT_MST_PK AND B.PORT_MST_POL_FK=POL.PORT_MST_PK ");
                strSQL.Append("                     " + condition);
                strSQL.Append("                 GROUP BY IST.jobcard_ref_no,p.PAYMENT_TBL_PK,");
                strSQL.Append("                          P.PAYMENT_TBL_PK,");
                strSQL.Append("                          V.VENDOR_ID,");
                strSQL.Append("                          P.PAYMENT_REF_NO,");
                strSQL.Append("                          P.PAYMENT_DATE,");
                strSQL.Append("                          CUR.CURRENCY_ID,");
                strSQL.Append("                          P.APPROVED,i.invoice_ref_no,i.INVOICE_DATE,I.INVOICE_AMT,p.payment_amt,");
                strSQL.Append("                          LMT.LOCATION_NAME, POD.PORT_ID, POL.PORT_ID");
                strSQL.Append("                 ORDER BY PAYMENT_DATE DESC, PAYMENT_REF_NO DESC) T) QRY");
            }
            else if (BizType == 2 & Process == 2)
            {
                strSQL.Append("SELECT QRY.*");
                strSQL.Append("  FROM (SELECT ROWNUM \"SL.NR\", T.*");
                strSQL.Append("          FROM (SELECT IST.jobcard_ref_no JOBCARD,POL.PORT_ID POL,POD.PORT_ID POD,I.INVOICE_REF_NO SupplierRefNO,");
                strSQL.Append("                       i.INVOICE_DATE Inv_date,");
                strSQL.Append("                       I.INVOICE_AMT,");
                strSQL.Append("                       LMT.LOCATION_NAME,");
                strSQL.Append("                       V.VENDOR_ID,");
                strSQL.Append("                       P.PAYMENT_REF_NO,");
                strSQL.Append("                       P.PAYMENT_DATE,");
                strSQL.Append("                       p.payment_amt,");
                strSQL.Append("                       (i.invoice_amt - NVL(p.payment_amt, 0.00))balance,");
                strSQL.Append("                       CUR.CURRENCY_ID");
                strSQL.Append("                       ");
                strSQL.Append("                  FROM PAYMENTS_TBL          P, BOOKING_MST_TBL b,");
                strSQL.Append("                       PAYMENT_TRN_TBL       PTRN,");
                strSQL.Append("                       CURRENCY_TYPE_MST_TBL CUR,");
                strSQL.Append("                       USER_MST_TBL          UMT,");
                strSQL.Append("                       INV_SUPPLIER_TBL      I,");
                strSQL.Append("                       VENDOR_MST_TBL        V,");
                strSQL.Append("                       INV_SUPPLIER_TRN_TBL  IST,");
                strSQL.Append("                       JOB_CARD_TRN   JOB,");
                strSQL.Append("                       location_mst_tbl       LMT,PORT_MST_TBL          POL,PORT_MST_TBL          POD,");
                strSQL.Append("                       VESSEL_VOYAGE_TRN      VTRN ");
                strSQL.Append("                 WHERE JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK");
                strSQL.Append("                   and I.INV_SUPPLIER_PK =Ist.INV_SUPPLIER_TBL_FK");
                strSQL.Append("                   and job.BOOKING_MST_FK=b.BOOKING_MST_PK");
                strSQL.Append("                   AND P.PAYMENT_TBL_PK = PTRN.PAYMENTS_TBL_FK");
                strSQL.Append("                   AND CUR.CURRENCY_MST_PK = P.CURRENCY_MST_FK");
                strSQL.Append("                   AND P.CREATED_BY_FK = UMT.USER_MST_PK");
                strSQL.Append("                   AND PTRN.INV_SUPPLIER_TBL_FK = I.INV_SUPPLIER_PK");
                strSQL.Append("                   AND V.VENDOR_MST_PK = P.VENDOR_MST_FK");
                strSQL.Append("                   AND lmt.location_mst_pk=umt.default_location_fk");
                strSQL.Append("                   AND V.VENDOR_MST_PK = " + Customer);
                strSQL.Append("                   AND P.PROCESS_TYPE = " + Process);
                strSQL.Append("                   AND P.BUSINESS_TYPE =" + BizType);
                if (Loc != "0")
                {
                    strSQL.Append("                   AND UMT.DEFAULT_LOCATION_FK =" + Loc);
                }
                strSQL.Append("                   and p.approved='1' AND B.PORT_MST_POD_FK=POD.PORT_MST_PK AND B.PORT_MST_POL_FK=POL.PORT_MST_PK ");
                strSQL.Append("                     " + condition);
                strSQL.Append("                 GROUP BY IST.jobcard_ref_no,p.PAYMENT_TBL_PK,");
                strSQL.Append("                          P.PAYMENT_TBL_PK,");
                strSQL.Append("                          V.VENDOR_ID,");
                strSQL.Append("                          P.PAYMENT_REF_NO,");
                strSQL.Append("                          P.PAYMENT_DATE,");
                strSQL.Append("                          CUR.CURRENCY_ID,");
                strSQL.Append("                          P.APPROVED,i.invoice_ref_no,i.INVOICE_DATE,I.INVOICE_AMT,p.payment_amt,");
                strSQL.Append("                          LMT.LOCATION_NAME ,POD.PORT_ID,POL.PORT_ID ");
                strSQL.Append("                 ORDER BY PAYMENT_DATE DESC, PAYMENT_REF_NO DESC) T) QRY");
            }
            else if (BizType == 1 & Process == 1)
            {
                strSQL.Append("SELECT QRY.*");
                strSQL.Append("  FROM (SELECT ROWNUM \"SL.NR\", T.*");
                strSQL.Append("          FROM (SELECT IST.jobcard_ref_no JOBCARD,POL.PORT_ID POL,POD.PORT_ID POD,I.INVOICE_REF_NO SupplierRefNO,");
                strSQL.Append("                       i.INVOICE_DATE Inv_date,");
                strSQL.Append("                       I.INVOICE_AMT,");
                strSQL.Append("                       LMT.LOCATION_NAME,");
                strSQL.Append("                       V.VENDOR_ID,");
                strSQL.Append("                       P.PAYMENT_REF_NO,");
                strSQL.Append("                       P.PAYMENT_DATE,");
                strSQL.Append("                       p.payment_amt,");
                strSQL.Append("                       (i.invoice_amt - NVL(p.payment_amt, 0.00))balance,");
                strSQL.Append("                       CUR.CURRENCY_ID");
                strSQL.Append("                       ");
                strSQL.Append("                  FROM PAYMENTS_TBL          P,BOOKING_MST_TBL b,");
                strSQL.Append("                       PAYMENT_TRN_TBL       PTRN,");
                strSQL.Append("                       CURRENCY_TYPE_MST_TBL CUR,");
                strSQL.Append("                       USER_MST_TBL          UMT,");
                strSQL.Append("                       INV_SUPPLIER_TBL      I,");
                strSQL.Append("                       VENDOR_MST_TBL        V,");
                strSQL.Append("                       INV_SUPPLIER_TRN_TBL  IST,");
                strSQL.Append("                       JOB_CARD_TRN   JOB,PORT_MST_TBL          POL,PORT_MST_TBL          POD,");
                strSQL.Append("                       location_mst_tbl       LMT,");
                strSQL.Append("                       JOB_TRN_PIA   PIA");
                strSQL.Append("                   WHERE I.INV_SUPPLIER_PK =Ist.INV_SUPPLIER_TBL_FK");
                strSQL.Append("                   and job.BOOKING_MST_FK=b.BOOKING_MST_PK");
                strSQL.Append("                   AND P.PAYMENT_TBL_PK = PTRN.PAYMENTS_TBL_FK");
                strSQL.Append("                   AND CUR.CURRENCY_MST_PK = P.CURRENCY_MST_FK");
                strSQL.Append("                   AND P.CREATED_BY_FK = UMT.USER_MST_PK");
                strSQL.Append("                   AND PTRN.INV_SUPPLIER_TBL_FK = I.INV_SUPPLIER_PK");
                strSQL.Append("                   AND V.VENDOR_MST_PK = P.VENDOR_MST_FK");
                strSQL.Append("                   AND lmt.location_mst_pk=umt.default_location_fk");
                strSQL.Append("                   AND V.VENDOR_MST_PK = " + Customer);
                strSQL.Append("                   AND P.PROCESS_TYPE = " + Process);
                strSQL.Append("                   AND P.BUSINESS_TYPE =" + BizType);
                if (Loc != "0")
                {
                    strSQL.Append("                   AND UMT.DEFAULT_LOCATION_FK =" + Loc);
                }
                strSQL.Append("                   AND PIA.JOB_TRN_PIA_PK = IST.JOB_CARD_PIA_FK");
                strSQL.Append("                   AND PIA.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
                //end
                strSQL.Append("                   and p.approved='1' AND B.PORT_MST_POD_FK=POD.PORT_MST_PK AND B.PORT_MST_POL_FK=POL.PORT_MST_PK");
                strSQL.Append("                     " + condition);
                strSQL.Append("                 GROUP BY IST.jobcard_ref_no,p.PAYMENT_TBL_PK,");
                strSQL.Append("                          P.PAYMENT_TBL_PK,");
                strSQL.Append("                          V.VENDOR_ID,");
                strSQL.Append("                          P.PAYMENT_REF_NO,");
                strSQL.Append("                          P.PAYMENT_DATE,");
                strSQL.Append("                          CUR.CURRENCY_ID,");
                strSQL.Append("                          P.APPROVED,i.invoice_ref_no,i.INVOICE_DATE,I.INVOICE_AMT,p.payment_amt,");
                strSQL.Append("                          LMT.LOCATION_NAME,POL.PORT_ID,POD.PORT_ID");
                strSQL.Append("                 ORDER BY PAYMENT_DATE DESC, PAYMENT_REF_NO DESC) T) QRY");
            }
            else if (BizType == 1 & Process == 2)
            {
                strSQL.Append("SELECT QRY.*");
                strSQL.Append("  FROM (SELECT ROWNUM \"SL.NR\", T.*");
                strSQL.Append("          FROM (SELECT IST.jobcard_ref_no JOBCARD,POL.PORT_ID POL,POD.PORT_ID POD,I.INVOICE_REF_NO SupplierRefNO,");
                strSQL.Append("                       i.INVOICE_DATE Inv_date,");
                strSQL.Append("                       I.INVOICE_AMT,");
                strSQL.Append("                       LMT.LOCATION_NAME,");
                strSQL.Append("                       V.VENDOR_ID,");
                strSQL.Append("                       P.PAYMENT_REF_NO,");
                strSQL.Append("                       P.PAYMENT_DATE,");
                strSQL.Append("                       p.payment_amt,");
                strSQL.Append("                       (i.invoice_amt - NVL(p.payment_amt, 0.00))balance,");
                strSQL.Append("                       CUR.CURRENCY_ID");
                strSQL.Append("                       ");
                strSQL.Append("                  FROM PAYMENTS_TBL          P,BOOKING_MST_TBL b,");
                strSQL.Append("                       PAYMENT_TRN_TBL       PTRN,");
                strSQL.Append("                       CURRENCY_TYPE_MST_TBL CUR,");
                strSQL.Append("                       USER_MST_TBL          UMT,");
                strSQL.Append("                       INV_SUPPLIER_TBL      I,");
                strSQL.Append("                       VENDOR_MST_TBL        V,");
                strSQL.Append("                       INV_SUPPLIER_TRN_TBL  IST,");
                strSQL.Append("                       JOB_CARD_TRN   JOB,PORT_MST_TBL          POL,PORT_MST_TBL          POD,");
                strSQL.Append("                       location_mst_tbl       LMT");
                strSQL.Append("                   WHERE I.INV_SUPPLIER_PK =Ist.INV_SUPPLIER_TBL_FK");
                strSQL.Append("                   and job.BOOKING_MST_FK=b.BOOKING_MST_PK");
                strSQL.Append("                   AND P.PAYMENT_TBL_PK = PTRN.PAYMENTS_TBL_FK");
                strSQL.Append("                   AND CUR.CURRENCY_MST_PK = P.CURRENCY_MST_FK");
                strSQL.Append("                   AND P.CREATED_BY_FK = UMT.USER_MST_PK");
                strSQL.Append("                   AND PTRN.INV_SUPPLIER_TBL_FK = I.INV_SUPPLIER_PK");
                strSQL.Append("                   AND V.VENDOR_MST_PK = P.VENDOR_MST_FK");
                strSQL.Append("                   AND lmt.location_mst_pk=umt.default_location_fk");
                strSQL.Append("                   AND V.VENDOR_MST_PK = " + Customer);
                strSQL.Append("                   AND P.PROCESS_TYPE = " + Process);
                strSQL.Append("                   AND P.BUSINESS_TYPE =" + BizType);
                if (Loc != "0")
                {
                    strSQL.Append("                   AND UMT.DEFAULT_LOCATION_FK =" + Loc);
                }
                strSQL.Append("                   and p.approved='1' AND B.PORT_MST_POD_FK=POD.PORT_MST_PK AND B.PORT_MST_POL_FK=POL.PORT_MST_PK");
                strSQL.Append("                     " + condition);
                strSQL.Append("                 GROUP BY IST.jobcard_ref_no,p.PAYMENT_TBL_PK,");
                strSQL.Append("                          P.PAYMENT_TBL_PK,");
                strSQL.Append("                          V.VENDOR_ID,");
                strSQL.Append("                          P.PAYMENT_REF_NO,");
                strSQL.Append("                          P.PAYMENT_DATE,");
                strSQL.Append("                          CUR.CURRENCY_ID,");
                strSQL.Append("                          P.APPROVED,i.invoice_ref_no,i.INVOICE_DATE,I.INVOICE_AMT,p.payment_amt,");
                strSQL.Append("                          LMT.LOCATION_NAME ,POL.PORT_ID,POD.PORT_ID");
                strSQL.Append("                 ORDER BY PAYMENT_DATE DESC, PAYMENT_REF_NO DESC) T) QRY");
            }
            try
            {
                return objWF.GetDataSet(strSQL.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "StatmentofAccounts"
    }
}