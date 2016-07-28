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

namespace Quantum_QFOR
{
    public class Cls_ListInvoicePendingCollectionReport : CommonFeatures
    {
        #region "Fetch Invoice Pending for Collection "

        /// <summary>
        /// Fetches the invoice pending for collection.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="IsReportData">if set to <c>true</c> [is report data].</param>
        /// <param name="BaseCurrencyPK">The base currency pk.</param>
        /// <returns></returns>
        public DataSet FetchInvoicePendingForCollection(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", Int32 TotalPage = 0, Int32 CurrentPage = 0, short BizType = 2, short ProcessType = 1, bool IsReportData = false, Int32 BaseCurrencyPK = 173)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            sb.Append("SELECT INV.CONSOL_INVOICE_PK INVOICEPK,");
            sb.Append(" INV.INVOICE_REF_NO INVOICEREFNR, ");
            sb.Append(" to_char(INV.INVOICE_DATE,dateformat)  INVOICEDATE,");
            sb.Append(" CMT.CUSTOMER_NAME FREIGHTPAYER,");
            sb.Append(" ROUND(NVL(INV.NET_RECEIVABLE/DECODE(NVL(get_ex_rate(" + BaseCurrencyPK + ", inv.currency_mst_fk, INV.INVOICE_DATE),");
            sb.Append(" 1),0,1,get_ex_rate(" + BaseCurrencyPK + ", inv.currency_mst_fk, INV.INVOICE_DATE)),");
            sb.Append(" 0),");
            sb.Append("  2) INVOICEAMOUNT,");
            sb.Append(" ROUND(NVL((select sum(ctrn.recd_amount_hdr_curr)");
            sb.Append(" /DECODE(NVL(get_ex_rate(" + BaseCurrencyPK + ", inv.currency_mst_fk, INV.INVOICE_DATE),");
            sb.Append(" 1),0,1,get_ex_rate(" + BaseCurrencyPK + ", inv.currency_mst_fk, INV.INVOICE_DATE))");
            sb.Append(" from collections_trn_tbl ctrn");
            sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
            sb.Append(" 0),2) RECEIVED,");
            sb.Append(" ROUND(NVL((INV.NET_RECEIVABLE -");
            sb.Append(" NVL((select sum(WMT.WRITEOFF_AMOUNT)");
            sb.Append(" from Writeoff_Manual_Tbl WMT");
            sb.Append(" where WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
            sb.Append(" 0.00) -");
            sb.Append(" NVL((select sum(ctrn.recd_amount_hdr_curr)");
            sb.Append(" from collections_trn_tbl ctrn");
            sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
            sb.Append(" 0.00)),");
            sb.Append(" 0) / DECODE(NVL(get_ex_rate(" + BaseCurrencyPK + ", inv.currency_mst_fk, INV.INVOICE_DATE),");
            sb.Append(" 1),0,1,get_ex_rate(" + BaseCurrencyPK + ", inv.currency_mst_fk, INV.INVOICE_DATE)),2)  BALANCE, ");
            sb.Append(" round(sysdate-inv.invoice_date,0)  OSDays,");
            sb.Append(" CMT.CUSTOMER_MST_PK CUST_PK");
            sb.Append(" FROM CONSOL_INVOICE_TBL                 INV,");
            sb.Append(" CONSOL_INVOICE_TRN_TBL                  INVTRN,  ");
            sb.Append(" CUSTOMER_MST_TBL                        CMT,   ");
            sb.Append(" PORT_MST_TBL                            POL,");
            sb.Append(" PORT_MST_TBL                            POD, ");
            if (BizType == 2 & ProcessType == 1)
            {
                sb.Append(" BOOKING_SEA_TBL                     BKS,");
                sb.Append(" HBL_EXP_TBL                         HBL, ");
                sb.Append(" VESSEL_VOYAGE_TRN                   VTRN, ");
                sb.Append(" JOB_CARD_SEA_EXP_TBL                JOB");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_SEA_EXP_PK(+)");
                sb.Append(" AND BKS.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND BKS.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND JOB.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append(" AND JOB.BOOKING_SEA_FK = BKS.BOOKING_SEA_PK");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+) ");
                sb.Append(" AND POL.LOCATION_MST_FK =");
            }
            else if (BizType == 1 & ProcessType == 1)
            {
                sb.Append(" BOOKING_AIR_TBL                     BKA,");
                sb.Append(" HAWB_EXP_TBL                        HAWB,");
                sb.Append(" JOB_CARD_AIR_EXP_TBL                JOB");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_AIR_EXP_PK(+)");
                sb.Append(" AND BKA.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND BKA.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND BKA.BOOKING_AIR_PK = JOB.BOOKING_AIR_FK");
                sb.Append(" AND JOB.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND POL.LOCATION_MST_FK =");
            }
            else if (BizType == 2 & ProcessType == 2)
            {
                sb.Append(" JOB_CARD_SEA_IMP_TBL                 JOB,");
                sb.Append(" VESSEL_VOYAGE_TRN                    VTRN");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_SEA_IMP_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK=POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK=POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+)");
                sb.Append(" AND POD.LOCATION_MST_FK = ");
            }
            else if (BizType == 1 & ProcessType == 2)
            {
                sb.Append(" JOB_CARD_AIR_IMP_TBL                 JOB");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_AIR_IMP_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK=POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK=POD.PORT_MST_PK(+) ");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND POD.LOCATION_MST_FK = ");
            }
            sb.Append(" (SELECT L.LOCATION_MST_PK");
            sb.Append(" FROM LOCATION_MST_TBL L");
            sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
            sb.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            sb.Append(" AND INV.PROCESS_TYPE = '" + ProcessType + "'");
            sb.Append(" AND INV.BUSINESS_TYPE = '" + BizType + "'");
            sb.Append(" AND INV.CHK_INVOICE = 1");
            sb.Append(" AND (INV.NET_RECEIVABLE -");
            sb.Append("  NVL((select sum(ctrn.recd_amount_hdr_curr)");
            sb.Append(" from collections_trn_tbl ctrn");
            sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
            sb.Append("  0)) > 0");
            if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
            {
                strCondition = strCondition + " And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)";
            }
            if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
            {
                strCondition = strCondition + " And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)";
            }
            if (!string.IsNullOrEmpty(CustName))
            {
                strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
            }
            sb.Append(strCondition);
            sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
            sb.Append(" INV.INVOICE_REF_NO,");
            sb.Append("  INV.INVOICE_DATE,");
            sb.Append(" CMT.CUSTOMER_NAME,");
            sb.Append(" CMT.CUSTOMER_MST_PK,");
            sb.Append(" INV.NET_RECEIVABLE,");
            sb.Append(" inv.currency_mst_fk");
            sb.Append(" ORDER BY INV.INVOICE_DATE DESC");
            if (IsReportData)
            {
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q )";
            }
            else
            {
                strSQL = "select count(*) from (";
                strSQL += sb.ToString() + ")";
                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
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
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
            }
            return objWF.GetDataSet(strSQL);
        }

        #endregion "Fetch Invoice Pending for Collection "

        #region "FETCH INVOICE DATA"

        /// <summary>
        /// Fetches the inv pending for col.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="IsReportData">if set to <c>true</c> [is report data].</param>
        /// <param name="BaseCurrencyPK">The base currency pk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchInvPendingForCol(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustPK = "", Int32 TotalPage = 0, Int32 CurrentPage = 0, string BizType = "", string ProcessType = "", bool IsReportData = false, Int32 BaseCurrencyPK = 0,
                Int32 flag = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            int ReportData = 0;
            try
            {
                if (IsReportData == true)
                {
                    ReportData = 1;
                }
                else
                {
                    ReportData = 0;
                }
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(CustPK) ? "" : CustPK)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_PK_IN", LocFk).Direction = ParameterDirection.Input;
                _with1.Add("CURRENCY_FK_IN", BaseCurrencyPK).Direction = ParameterDirection.Input;
                _with1.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
                _with1.Add("TODATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with1.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with1.Add("ISREPORT_DATA_IN", ReportData).Direction = ParameterDirection.Input;
                _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_INVOICE_COLLECTION_PKG", "FETCH_INVOICE_COLLECTION");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FETCH INVOICE DATA"
    }
}