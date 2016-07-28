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
    public class Cls_GenerateListofPayment : CommonFeatures
    {
        #region "Container Allocation Grid Function"

        /// <summary>
        /// Fetches the payment details sea_ air.
        /// </summary>
        /// <param name="Curr">The curr.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="SupplierName">Name of the supplier.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcType">Type of the proc.</param>
        /// <param name="expExcel">The exp excel.</param>
        /// <returns></returns>
        public DataSet FetchPaymentDetailsSea_Air(string Curr = "", Int32 LocFk = 0, string SupplierName = "", string FromDt = "", string ToDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, string BizType = "", string ProcType = "",
        Int32 expExcel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            try
            {
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And TO_DATE(p.payment_date, dateformat) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                else
                {
                    strCondition = strCondition + " And TO_DATE(p.payment_date, dateformat) >= TO_DATE('15/08/1947',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And TO_DATE(p.payment_date, dateformat) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                else
                {
                    strCondition = strCondition + " And TO_DATE(p.payment_date, dateformat) <= TO_DATE('" + DateTime.Now.ToString("dd/MM/yyyy") + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(SupplierName))
                {
                    strCondition = strCondition + " And v.vendor_mst_pk = '" + SupplierName + "'";
                }
                if (BizType != "0")
                {
                    strCondition = strCondition + " And P.BUSINESS_TYPE = " + BizType + "";
                }
                if (ProcType != "0")
                {
                    strCondition = strCondition + " And P.PROCESS_TYPE = " + ProcType + "";
                }
                if (!string.IsNullOrEmpty(Curr))
                {
                    strCondition = strCondition;
                }

                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT QRY.*");
                sb.Append("  FROM (SELECT T.*");
                sb.Append("          FROM (SELECT P.PAYMENT_TBL_PK PK,");
                if (expExcel == 0)
                {
                    sb.Append("P.PAYMENT_REF_NO,");
                }
                else
                {
                    sb.Append("P.PAYMENT_REF_NO,");
                }
                sb.Append("                       TO_DATE(P.PAYMENT_DATE, DATEFORMAT)PAYMENT_DATE,");
                sb.Append("                       V.VENDOR_NAME AS Supplier_Name,");
                sb.Append("                       SUM(NVL(PTRN.PAID_AMOUNT_HDR_CURR,0)* ");
                sb.Append("                          GET_EX_RATE_BUY(I.CURRENCY_MST_FK," + BaseCurrFk + " ,I.SUPPLIER_INV_DT)) PAYMENTAMT, ");

                sb.Append("                       (SELECT SUM(NVL(ISTT.Payable_Amt,NVL(ISTT.ACTUAL_AMT,0)+NVL(ISTT.TAX_AMOUNT,0))* ");
                sb.Append("                          GET_EX_RATE_BUY(I.CURRENCY_MST_FK," + BaseCurrFk + " ,I.SUPPLIER_INV_DT) ) ");
                sb.Append("                       FROM INV_SUPPLIER_TRN_TBL ISTT  WHERE ISTT.INV_SUPPLIER_TBL_FK = I.INV_SUPPLIER_PK) VOUCHERAMT,");

                sb.Append("                       TO_DATE(I.APPROVED_DATE , DATEFORMAT)VOUCHEAPPDATE,");
                sb.Append("                       TO_DATE(I.SUPPLIER_DUE_DT, DATEFORMAT) SUPPDEUDATE, '' CHK,");
                sb.Append("                     DECODE(P.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("                     DECODE(P.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE");

                sb.Append("                  FROM PAYMENTS_TBL          P, ");
                sb.Append("                       PAYMENT_TRN_TBL       PTRN,");
                sb.Append("                       CURRENCY_TYPE_MST_TBL CUR,");
                sb.Append("                       USER_MST_TBL          UMT,");
                sb.Append("                       INV_SUPPLIER_TBL      I,");
                sb.Append("                       VENDOR_MST_TBL        V");
                sb.Append("                 WHERE P.PAYMENT_TBL_PK = PTRN.PAYMENTS_TBL_FK");
                sb.Append("                   AND CUR.CURRENCY_MST_PK = P.CURRENCY_MST_FK");
                sb.Append("                   AND P.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append("                   AND PTRN.INV_SUPPLIER_TBL_FK = I.INV_SUPPLIER_PK");
                sb.Append("                   AND V.VENDOR_MST_PK = P.VENDOR_MST_FK");
                sb.Append("                   AND I.APPROVED=1");
                sb.Append("                   AND P.APPROVED = 0");
                sb.Append("                  ");
                sb.Append(strCondition);
                sb.Append("                   AND UMT.DEFAULT_LOCATION_FK =" + LocFk + " ");
                //"& LocFk & "") '1301
                sb.Append("                 GROUP BY P.PAYMENT_TBL_PK,");
                sb.Append("                          V.VENDOR_NAME,");
                sb.Append("                          P.PAYMENT_REF_NO,");
                sb.Append("                          P.PAYMENT_DATE,");
                sb.Append("                          CUR.CURRENCY_ID,");
                sb.Append("                          P.APPROVED,");
                sb.Append("                          I.INV_SUPPLIER_PK,");
                sb.Append("                          I.APPROVED_DATE,");
                sb.Append("                          I.SUPPLIER_DUE_DT,");
                sb.Append("                          I.CURRENCY_MST_FK,");
                sb.Append("                          I.SUPPLIER_INV_DT,");
                sb.Append("                          P.BUSINESS_TYPE,");
                sb.Append("                          P.PROCESS_TYPE");

                sb.Append("                 ORDER BY P.PAYMENT_DATE DESC, PAYMENT_REF_NO DESC) T) QRY");
                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";

                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

                strSQL = " SELECT * FROM (SELECT ROWNUM SLNO, q.* FROM(";
                strSQL += sb.ToString();
                if (expExcel == 0)
                {
                    strSQL += " )q ) WHERE SLNO Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " )q )";
                }
                return objWF.GetDataSet(strSQL);
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

        #endregion "Container Allocation Grid Function"
    }
}