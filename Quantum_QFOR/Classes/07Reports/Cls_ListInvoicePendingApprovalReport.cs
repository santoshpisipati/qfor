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
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_ListInvoicePendingApprovalReport : CommonFeatures
    {
        #region "Fetch Job Card for Sea-Export"

        //This fetch Job Cards for Sea-Export
        /// <summary>
        /// Fetches the invoice sea export.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="ExpExcel">The exp excel.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceSeaExport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0, short BizType = 2, short ProcessType = 1,
        short ExpExcel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK                 INVOICEPK,");
                sb.Append("INV.INVOICE_REF_NO                           INVOICEREFNR,");
                sb.Append("INV.INVOICE_DATE                             INVOICEDATE,");
                sb.Append("SHP.CUSTOMER_NAME                            SHIPPER,");
                sb.Append("CONS.CUSTOMER_NAME                           CONSIGNEE,");
                sb.Append("POD.PORT_ID                                  POD,");
                sb.Append("    '' ETD_DATE,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append("CMT.CUSTOMER_NAME                            CUST_NAME,");
                sb.Append("     DECODE(INV.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INV.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE,");
                sb.Append("SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR,0))       GrossAMT");
                sb.Append(" FROM CONSOL_INVOICE_TBL                       INV,");
                sb.Append("CONSOL_INVOICE_TRN_TBL                        INVTRN,");
                sb.Append("JOB_CARD_TRN                          JOB,");
                sb.Append("BOOKING_MST_TBL                               BKS,");
                sb.Append("HBL_EXP_TBL                                   HBL,");
                sb.Append("CUSTOMER_MST_TBL                              CMT,");
                sb.Append("CUSTOMER_MST_TBL                              SHP,");
                sb.Append("CUSTOMER_MST_TBL                             CONS,");
                sb.Append("CURRENCY_TYPE_MST_TBL                        CUMT,");
                sb.Append("VESSEL_VOYAGE_TRN                            VTRN,");
                sb.Append("PORT_MST_TBL                                 POL,");
                sb.Append("PORT_MST_TBL                                 POD,");
                sb.Append("MBL_EXP_TBL                                  MBL");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append(" AND JOB.BOOKING_MST_FK = BKS.BOOKING_MST_PK(+)");
                sb.Append(" AND JOB.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+)");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                sb.Append(" AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                sb.Append(" AND INV.CHK_INVOICE = 0");
                sb.Append(" AND INV.IS_FAC_INV <> 1");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append(" NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append(" from collections_trn_tbl ctrn");
                sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append("0)) > 0");
                sb.Append(strCondition);
                sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append(" INV.INVOICE_REF_NO,");
                sb.Append("INV.INVOICE_DATE,");
                sb.Append("SHP.CUSTOMER_NAME,");
                sb.Append("CONS.CUSTOMER_NAME,");
                sb.Append("POD.PORT_ID ,");
                //sb.Append("JOB.ETD_DATE,")
                sb.Append("CMT.CUSTOMER_MST_PK,");
                sb.Append("CMT.CUSTOMER_NAME,");
                sb.Append("INV.BUSINESS_TYPE,");
                sb.Append("INV.PROCESS_TYPE");
                sb.Append(" ORDER BY INV.INVOICE_DATE DESC");

                strSQL = "select count(*) from (";
                strSQL += sb.ToString() + ")";
                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
                //TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
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
                if (ExpExcel == 1)
                {
                    strSQL += " )q ) ";
                }
                else
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch Job Card for Sea-Export"

        #region "Fetch Job Card for Air-Export"

        //This fetch Job Cards for Air-Export
        /// <summary>
        /// Fetches the invoice air export.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="ExpExcel">The exp excel.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceAirExport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0, short BizType = 2, short ProcessType = 1,
        short ExpExcel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)";
                }

                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }

                sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK INVOICEPK,");
                sb.Append(" INV.INVOICE_REF_NO    INVOICEREFNR,");
                sb.Append(" INV.INVOICE_DATE      INVOICEDATE,");
                sb.Append(" SHP.CUSTOMER_NAME  SHIPPER,");
                sb.Append(" CONS.CUSTOMER_NAME CONSIGNEE,");
                sb.Append(" POD.PORT_ID        POD,");
                sb.Append(" '' ETD_DATE,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME   CUST_NAME,");
                sb.Append("     DECODE(INV.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INV.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE,");
                sb.Append(" SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR, 0)) GrossAMT");
                sb.Append(" FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");
                sb.Append(" JOB_CARD_TRN   JOB,");
                sb.Append(" BOOKING_MST_TBL        BKS,");
                sb.Append(" CUSTOMER_MST_TBL CMT,");
                sb.Append(" CUSTOMER_MST_TBL SHP,");
                sb.Append(" CUSTOMER_MST_TBL      CONS,");
                sb.Append(" CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append(" HAWB_EXP_TBL HAWB,");
                sb.Append(" PORT_MST_TBL POL,");
                sb.Append(" PORT_MST_TBL POD,");
                sb.Append(" MAWB_EXP_TBL MBL");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.BOOKING_MST_FK = BKS.BOOKING_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                sb.Append(" AND JOB.MBL_MAWB_FK = MBL.MAWB_EXP_TBL_PK(+)");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                sb.Append(" AND POL.LOCATION_MST_FK = ");
                sb.Append("(SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                sb.Append(" AND INV.CHK_INVOICE = 0");
                sb.Append(" AND INV.IS_FAC_INV <> 1");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append(" NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append(" from collections_trn_tbl ctrn");
                sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append(" 0)) > 0");
                sb.Append(strCondition);
                sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append(" INV.INVOICE_REF_NO,");
                sb.Append(" INV.INVOICE_DATE,");
                sb.Append(" SHP.CUSTOMER_NAME,");
                sb.Append(" CONS.CUSTOMER_NAME,");
                sb.Append("  POD.PORT_ID,");
                //sb.Append(" JOB.ETD_DATE,")
                sb.Append("CMT.CUSTOMER_MST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME,");
                sb.Append("INV.BUSINESS_TYPE,");
                sb.Append("INV.PROCESS_TYPE");
                sb.Append(" ORDER BY INV.INVOICE_DATE DESC");
                strSQL = "select count(*) from (";
                strSQL += sb.ToString() + ")";
                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
                //TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
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
                if (ExpExcel == 1)
                {
                    strSQL += " )q ) ";
                }
                else
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch Job Card for Air-Export"

        #region "Fetch Job Card for Sea-Import"

        //This fetch Job Cards for Sea-Import
        /// <summary>
        /// Fetches the invoice sea import.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="ExpExcel">The exp excel.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceSeaImport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0, short BizType = 2, short ProcessType = 1,
        short ExpExcel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }

                sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK     INVOICEPK,");
                sb.Append(" INV.INVOICE_REF_NO              INVOICEREFNR,");
                sb.Append(" INV.INVOICE_DATE                INVOICEDATE,");
                sb.Append(" SHP.CUSTOMER_NAME               SHIPPER,");
                sb.Append(" CONS.CUSTOMER_NAME              CONSIGNEE,");
                sb.Append(" POD.PORT_ID                     POD,");
                sb.Append(" '' ETD_DATE,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME               CUST_NAME,");
                sb.Append("     DECODE(INV.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INV.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE,");
                sb.Append(" SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR,0)) GrossAMT");
                sb.Append(" FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append(" CONSOL_INVOICE_TRN_TBL      INVTRN,");
                sb.Append(" JOB_CARD_TRN        JOB,");
                sb.Append(" CUSTOMER_MST_TBL            CMT,");
                sb.Append(" CUSTOMER_MST_TBL            SHP,");
                sb.Append(" CUSTOMER_MST_TBL            CONS,");
                sb.Append(" CURRENCY_TYPE_MST_TBL       CUMT,");
                sb.Append(" VESSEL_VOYAGE_TRN           VTRN,");
                sb.Append(" PORT_MST_TBL                POL,");
                sb.Append(" PORT_MST_TBL                POD");

                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+)");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                sb.Append(" AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                sb.Append(" and INV.CHK_INVOICE = 0");
                sb.Append(" AND INV.IS_FAC_INV <> 1");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append(" NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append(" from collections_trn_tbl ctrn");
                sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append(" 0)) > 0");
                sb.Append(strCondition);
                sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append(" INV.INVOICE_REF_NO,");
                sb.Append(" INV.INVOICE_DATE,");
                sb.Append(" SHP.CUSTOMER_NAME,");
                sb.Append(" CONS.CUSTOMER_NAME,");
                sb.Append(" POD.PORT_ID ,");
                //sb.Append(" JOB.ETD_DATE,")
                sb.Append("CMT.CUSTOMER_MST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME,");
                sb.Append("INV.BUSINESS_TYPE,");
                sb.Append("INV.PROCESS_TYPE");
                sb.Append(" ORDER BY INV.INVOICE_DATE DESC");

                strSQL = "select count(*) from (";
                strSQL += sb.ToString() + ")";
                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
                //TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
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
                if (ExpExcel == 1)
                {
                    strSQL += " )q ) ";
                }
                else
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch Job Card for Sea-Import"

        #region "Fetch Job Card for Air-Import"

        //This fetch Job Cards for Air-Import
        /// <summary>
        /// Fetches the invoice air import.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="ExpExcel">The exp excel.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceAirImport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0, short BizType = 2, short ProcessType = 1,
        short ExpExcel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }

                sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK                     INVOICEPK,");
                sb.Append(" INV.INVOICE_REF_NO                       INVOICEREFNR,");
                sb.Append(" INV.INVOICE_DATE                         INVOICEDATE,");
                sb.Append(" SHP.CUSTOMER_NAME                        SHIPPER,");
                sb.Append(" CONS.CUSTOMER_NAME                       CONSIGNEE,");
                sb.Append("  POD.PORT_ID                              POD,");
                sb.Append(" '' ETD_DATE,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME               CUST_NAME,");
                sb.Append("     DECODE(INV.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INV.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE,");
                sb.Append("  SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR,0))   GrossAMT");
                sb.Append(" FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");
                sb.Append(" JOB_CARD_TRN   JOB,");
                sb.Append(" CUSTOMER_MST_TBL       CMT,");
                sb.Append(" CUSTOMER_MST_TBL       SHP,  ");
                sb.Append(" CUSTOMER_MST_TBL       CONS,");
                sb.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                sb.Append(" PORT_MST_TBL           POL,");
                sb.Append(" PORT_MST_TBL    POD");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) ");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+) ");
                sb.Append(" AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append("  AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                sb.Append(" and INV.CHK_INVOICE = 0");
                sb.Append(" AND INV.IS_FAC_INV <> 1");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append("   NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append("  from collections_trn_tbl ctrn");
                sb.Append("  where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append("   0)) > 0");
                sb.Append(strCondition);
                sb.Append("  GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append("  INV.INVOICE_REF_NO,");
                sb.Append("  INV.INVOICE_DATE,");
                sb.Append("  SHP.CUSTOMER_NAME,");
                sb.Append("  CONS.CUSTOMER_NAME,");
                sb.Append("   POD.PORT_ID ,");
                //sb.Append("  JOB.ETD_DATE,")
                sb.Append("CMT.CUSTOMER_MST_PK,");
                sb.Append("  CMT.CUSTOMER_NAME,");
                sb.Append("INV.BUSINESS_TYPE,");
                sb.Append("INV.PROCESS_TYPE");
                sb.Append("  ORDER BY INV.INVOICE_DATE DESC");

                strSQL = "select count(*) from (";
                strSQL += sb.ToString() + ")";
                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
                //TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
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
                if (ExpExcel == 1)
                {
                    strSQL += " )q ) ";
                }
                else
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch Job Card for Air-Import"

        #region "Fetch All"

        /// <summary>
        /// Fetches the invoice all.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="ExpExcel">The exp excel.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceAll(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0, int BizType = 0, int ProcessType = 0,
        int ExpExcel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);

            try
            {
                sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK                 INVOICEPK,");
                sb.Append("INV.INVOICE_REF_NO                           INVOICEREFNR,");
                sb.Append("INV.INVOICE_DATE                             INVOICEDATE,");
                sb.Append("SHP.CUSTOMER_NAME                            SHIPPER,");
                sb.Append("CONS.CUSTOMER_NAME                           CONSIGNEE,");
                sb.Append("POD.PORT_ID                                  POD,");
                sb.Append("    '' ETD_DATE,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME                            CUST_NAME,");
                sb.Append("     DECODE(INV.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INV.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE,");
                sb.Append("SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR,0))       GrossAMT");
                sb.Append(" FROM CONSOL_INVOICE_TBL                       INV,");
                sb.Append("CONSOL_INVOICE_TRN_TBL                        INVTRN,");
                sb.Append("JOB_CARD_TRN                       JOB,");
                sb.Append("BOOKING_MST_TBL                               BKS,");
                sb.Append("HBL_EXP_TBL                                   HBL,");
                sb.Append("CUSTOMER_MST_TBL                              CMT,");
                sb.Append("CUSTOMER_MST_TBL                              SHP,");
                sb.Append("CUSTOMER_MST_TBL                             CONS,");
                sb.Append("CURRENCY_TYPE_MST_TBL                        CUMT,");
                sb.Append("VESSEL_VOYAGE_TRN                            VTRN,");
                sb.Append("PORT_MST_TBL                                 POL,");
                sb.Append("PORT_MST_TBL                                 POD,");
                sb.Append("MBL_EXP_TBL                                  MBL");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append(" AND JOB.BOOKING_MST_FK = BKS.BOOKING_MST_PK(+)");
                sb.Append(" AND JOB.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+)");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                sb.Append(" AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" AND INV.CHK_INVOICE = 0");
                sb.Append(" AND INV.IS_FAC_INV <> 1");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append(" NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append(" from collections_trn_tbl ctrn");
                sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append("0)) > 0");
                if (ProcessType != 0)
                {
                    sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                }
                if (BizType != 0)
                {
                    sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                }
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append("  And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append(" INV.INVOICE_REF_NO,");
                sb.Append("INV.INVOICE_DATE,");
                sb.Append("SHP.CUSTOMER_NAME,");
                sb.Append("CONS.CUSTOMER_NAME,");
                sb.Append("POD.PORT_ID ,");
                //sb.Append("JOB.ETD_DATE,")
                sb.Append("CMT.CUSTOMER_MST_PK,");
                sb.Append("CMT.CUSTOMER_NAME,");
                sb.Append("INV.BUSINESS_TYPE,");
                sb.Append("INV.PROCESS_TYPE");

                sb.Append(" UNION");

                sb.Append(" SELECT INV.CONSOL_INVOICE_PK INVOICEPK,");
                sb.Append(" INV.INVOICE_REF_NO    INVOICEREFNR,");
                sb.Append(" INV.INVOICE_DATE      INVOICEDATE,");
                sb.Append(" SHP.CUSTOMER_NAME  SHIPPER,");
                sb.Append(" CONS.CUSTOMER_NAME CONSIGNEE,");
                sb.Append(" POD.PORT_ID        POD,");
                sb.Append(" '' ETD_DATE,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME   CUST_NAME,");
                sb.Append("     DECODE(INV.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INV.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE,");
                sb.Append(" SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR, 0)) GrossAMT");
                sb.Append(" FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");
                sb.Append(" JOB_CARD_TRN  JOB,");
                sb.Append(" BOOKING_MST_TBL        BKS,");
                sb.Append(" CUSTOMER_MST_TBL CMT,");
                sb.Append(" CUSTOMER_MST_TBL SHP,");
                sb.Append(" CUSTOMER_MST_TBL      CONS,");
                sb.Append(" CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append(" HAWB_EXP_TBL HAWB,");
                sb.Append(" PORT_MST_TBL POL,");
                sb.Append(" PORT_MST_TBL POD,");
                sb.Append(" MAWB_EXP_TBL MBL");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.BOOKING_MST_FK = BKS.BOOKING_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                sb.Append(" AND JOB.MBL_MAWB_FK = MBL.MAWB_EXP_TBL_PK(+)");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                sb.Append(" AND POL.LOCATION_MST_FK = ");
                sb.Append("(SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" AND INV.CHK_INVOICE = 0");
                sb.Append(" AND INV.IS_FAC_INV <> 1");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append(" NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append(" from collections_trn_tbl ctrn");
                sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append(" 0)) > 0");
                if (ProcessType != 0)
                {
                    sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                }
                if (BizType != 0)
                {
                    sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                }
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append(" INV.INVOICE_REF_NO,");
                sb.Append(" INV.INVOICE_DATE,");
                sb.Append(" SHP.CUSTOMER_NAME,");
                sb.Append(" CONS.CUSTOMER_NAME,");
                sb.Append(" POD.PORT_ID,");
                //sb.Append(" JOB.ETD_DATE,")
                sb.Append(" CMT.CUSTOMER_MST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME,");
                sb.Append(" INV.BUSINESS_TYPE,");
                sb.Append(" INV.PROCESS_TYPE");

                sb.Append(" UNION");

                sb.Append(" SELECT INV.CONSOL_INVOICE_PK     INVOICEPK,");
                sb.Append(" INV.INVOICE_REF_NO              INVOICEREFNR,");
                sb.Append(" INV.INVOICE_DATE                INVOICEDATE,");
                sb.Append(" SHP.CUSTOMER_NAME               SHIPPER,");
                sb.Append(" CONS.CUSTOMER_NAME              CONSIGNEE,");
                sb.Append(" POD.PORT_ID                     POD,");
                sb.Append(" '' ETD_DATE,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME               CUST_NAME,");
                sb.Append("     DECODE(INV.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INV.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE,");
                sb.Append(" SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR,0)) GrossAMT");
                sb.Append(" FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append(" CONSOL_INVOICE_TRN_TBL      INVTRN,");
                sb.Append(" JOB_CARD_TRN        JOB,");
                sb.Append(" CUSTOMER_MST_TBL            CMT,");
                sb.Append(" CUSTOMER_MST_TBL            SHP,");
                sb.Append(" CUSTOMER_MST_TBL            CONS,");
                sb.Append(" CURRENCY_TYPE_MST_TBL       CUMT,");
                sb.Append(" VESSEL_VOYAGE_TRN           VTRN,");
                sb.Append(" PORT_MST_TBL                POL,");
                sb.Append(" PORT_MST_TBL                POD");

                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+)");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                sb.Append(" AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" and INV.CHK_INVOICE = 0");
                sb.Append(" AND INV.IS_FAC_INV <> 1");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append(" NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append(" from collections_trn_tbl ctrn");
                sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append(" 0)) > 0");
                if (ProcessType != 0)
                {
                    sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                }
                if (BizType != 0)
                {
                    sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                }
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append(" INV.INVOICE_REF_NO,");
                sb.Append(" INV.INVOICE_DATE,");
                sb.Append(" SHP.CUSTOMER_NAME,");
                sb.Append(" CONS.CUSTOMER_NAME,");
                sb.Append(" POD.PORT_ID ,");
                //sb.Append(" JOB.ETD_DATE,")
                sb.Append(" CMT.CUSTOMER_MST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME,");
                sb.Append("INV.BUSINESS_TYPE,");
                sb.Append("INV.PROCESS_TYPE");

                sb.Append(" UNION");

                sb.Append(" SELECT INV.CONSOL_INVOICE_PK                     INVOICEPK,");
                sb.Append(" INV.INVOICE_REF_NO                       INVOICEREFNR,");
                sb.Append(" INV.INVOICE_DATE                         INVOICEDATE,");
                sb.Append(" SHP.CUSTOMER_NAME                        SHIPPER,");
                sb.Append(" CONS.CUSTOMER_NAME                       CONSIGNEE,");
                sb.Append("  POD.PORT_ID                              POD,");
                sb.Append(" '' ETD_DATE,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME               CUST_NAME,");
                sb.Append("     DECODE(INV.BUSINESS_TYPE,'1','AIR','2','SEA') BUSINESS_TYPE,");
                sb.Append("\t    DECODE(INV.PROCESS_TYPE,'1','EXPORT','2','IMPORT') PROCESS_TYPE,");
                sb.Append("  SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR,0))   GrossAMT");
                sb.Append(" FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");
                sb.Append(" JOB_CARD_TRN   JOB,");
                sb.Append(" CUSTOMER_MST_TBL       CMT,");
                sb.Append(" CUSTOMER_MST_TBL       SHP,  ");
                sb.Append(" CUSTOMER_MST_TBL       CONS,");
                sb.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                sb.Append(" PORT_MST_TBL           POL,");
                sb.Append(" PORT_MST_TBL    POD");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) ");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+) ");
                sb.Append(" AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append("  AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" and INV.CHK_INVOICE = 0");
                sb.Append(" AND INV.IS_FAC_INV <> 1");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append("   NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append("  from collections_trn_tbl ctrn");
                sb.Append("  where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append("   0)) > 0");
                if (ProcessType != 0)
                {
                    sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                }
                if (BizType != 0)
                {
                    sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                }
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                sb.Append("  GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append("  INV.INVOICE_REF_NO,");
                sb.Append("  INV.INVOICE_DATE,");
                sb.Append("  SHP.CUSTOMER_NAME,");
                sb.Append("  CONS.CUSTOMER_NAME,");
                sb.Append("   POD.PORT_ID ,");
                //sb.Append("  JOB.ETD_DATE,")
                sb.Append("  CMT.CUSTOMER_MST_PK,");
                sb.Append("  CMT.CUSTOMER_NAME,");
                sb.Append("  INV.BUSINESS_TYPE,");
                sb.Append("  INV.PROCESS_TYPE");

                sb.Append(" ORDER BY INVOICEPK DESC");
                strSQL = "select count(*) from (";
                strSQL += sb.ToString() + ")";
                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
                //TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
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
                if (ExpExcel == 1)
                {
                    strSQL += " )q ) ";
                }
                else
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch All"

        #region "Fetch Job Card for Sea-Export Report"

        //This fetch Job Cards for Sea-Export Report
        /// <summary>
        /// Fetches the invoice sea export report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceSeaExportReport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", short BizType = 2, short ProcessType = 1)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                sb.Append("SELECT INV.CONSOL_INVOICE_PK                 INVOICEPK,");
                sb.Append("INV.INVOICE_REF_NO                           INVOICEREFNR,");
                sb.Append("  TO_CHAR(INV.INVOICE_DATE,dateformat)         INVOICEDATE,");
                sb.Append("SHP.CUSTOMER_NAME                            SHIPPER,");
                sb.Append("CONS.CUSTOMER_NAME                           CONSIGNEE,");
                sb.Append("POD.PORT_ID                                  POD,");
                sb.Append("    TO_CHAR(JOB.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                sb.Append("SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR,0))       GrossAMT,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append("CMT.CUSTOMER_NAME                            CUST_NAME");

                sb.Append(" FROM CONSOL_INVOICE_TBL                       INV,");
                sb.Append("CONSOL_INVOICE_TRN_TBL                        INVTRN,");
                sb.Append("JOB_CARD_SEA_EXP_TBL                          JOB,");
                sb.Append("BOOKING_SEA_TBL                               BKS,");
                sb.Append("HBL_EXP_TBL                                   HBL,");
                sb.Append("CUSTOMER_MST_TBL                              CMT,");
                sb.Append("CUSTOMER_MST_TBL                              SHP,");
                sb.Append("CUSTOMER_MST_TBL                             CONS,");
                sb.Append("CURRENCY_TYPE_MST_TBL                        CUMT,");
                sb.Append("VESSEL_VOYAGE_TRN                            VTRN,");
                sb.Append("PORT_MST_TBL                                 POL,");
                sb.Append("PORT_MST_TBL                                 POD,");
                sb.Append("MBL_EXP_TBL                                  MBL");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_SEA_EXP_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK(+)");
                sb.Append(" AND JOB.BOOKING_SEA_FK = BKS.BOOKING_SEA_PK(+)");
                sb.Append(" AND JOB.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK(+)");
                sb.Append(" AND BKS.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND BKS.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+)");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                sb.Append(" AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                sb.Append(" AND INV.CHK_INVOICE = 0");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append(" NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append(" from collections_trn_tbl ctrn");
                sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append("0)) > 0");

                sb.Append(strCondition);

                sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append(" INV.INVOICE_REF_NO,");
                sb.Append("INV.INVOICE_DATE,");
                sb.Append("SHP.CUSTOMER_NAME,");
                sb.Append("CONS.CUSTOMER_NAME,");
                sb.Append("POD.PORT_ID ,");
                sb.Append("JOB.ETD_DATE,");
                sb.Append("CMT.CUSTOMER_MST_PK,");
                sb.Append("CMT.CUSTOMER_NAME");
                sb.Append(" ORDER BY INV.INVOICE_DATE DESC");

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q )";
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

        #endregion "Fetch Job Card for Sea-Export Report"

        #region "Fetch Job Card for Air-Export Report"

        //This fetch Job Cards for Air-Export
        /// <summary>
        /// Fetches the invoice air export report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceAirExportReport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", short BizType = 2, short ProcessType = 1)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }

                sb.Append("SELECT INV.CONSOL_INVOICE_PK INVOICEPK,");
                sb.Append(" INV.INVOICE_REF_NO    INVOICEREFNR,");
                sb.Append("  TO_CHAR(INV.INVOICE_DATE,dateformat)         INVOICEDATE,");
                sb.Append(" SHP.CUSTOMER_NAME  SHIPPER,");
                sb.Append(" CONS.CUSTOMER_NAME CONSIGNEE,");
                sb.Append(" POD.PORT_ID        POD,");
                sb.Append(" TO_CHAR(JOB.ETD_DATE, DATETIMEFORMAT24)       ETD_DATE,");
                sb.Append(" SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR, 0)) GrossAMT,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME   CUST_NAME");

                sb.Append(" FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");
                sb.Append(" JOB_CARD_AIR_EXP_TBL   JOB,");
                sb.Append(" BOOKING_AIR_TBL        BKS,");
                sb.Append(" CUSTOMER_MST_TBL CMT,");
                sb.Append(" CUSTOMER_MST_TBL SHP,");
                sb.Append(" CUSTOMER_MST_TBL      CONS,");
                sb.Append(" CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append(" HAWB_EXP_TBL HAWB,");
                sb.Append(" PORT_MST_TBL POL,");
                sb.Append(" PORT_MST_TBL POD,");
                sb.Append(" MBL_EXP_TBL MBL");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_AIR_EXP_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.BOOKING_AIR_FK = BKS.BOOKING_AIR_PK(+)");
                sb.Append(" AND BKS.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND BKS.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                sb.Append(" AND POL.LOCATION_MST_FK = ");
                sb.Append("(SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                sb.Append(" AND INV.CHK_INVOICE = 0");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append(" NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append(" from collections_trn_tbl ctrn");
                sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append(" 0)) > 0");
                sb.Append(strCondition);
                sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append(" INV.INVOICE_REF_NO,");
                sb.Append(" INV.INVOICE_DATE,");
                sb.Append(" SHP.CUSTOMER_NAME,");
                sb.Append(" CONS.CUSTOMER_NAME,");
                sb.Append("  POD.PORT_ID,");
                sb.Append(" JOB.ETD_DATE,");
                sb.Append("CMT.CUSTOMER_MST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME");
                sb.Append(" ORDER BY INV.INVOICE_DATE DESC");

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q )";
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

        #endregion "Fetch Job Card for Air-Export Report"

        #region "Fetch Job Card for Sea-Import Report"

        //This fetch Job Cards for Sea-Import
        /// <summary>
        /// Fetches the invoice sea import report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceSeaImportReport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", short BizType = 2, short ProcessType = 1)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }

                sb.Append("SELECT INV.CONSOL_INVOICE_PK     INVOICEPK,");
                sb.Append(" INV.INVOICE_REF_NO              INVOICEREFNR,");
                sb.Append("  TO_CHAR(INV.INVOICE_DATE,dateformat)         INVOICEDATE,");
                sb.Append(" SHP.CUSTOMER_NAME               SHIPPER,");
                sb.Append(" CONS.CUSTOMER_NAME              CONSIGNEE,");
                sb.Append(" POD.PORT_ID                     POD,");
                sb.Append(" TO_CHAR(JOB.ETD_DATE, DATETIMEFORMAT24)       ETD_DATE,");
                sb.Append(" SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR,0)) GrossAMT,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME               CUST_NAME");

                sb.Append(" FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append(" CONSOL_INVOICE_TRN_TBL      INVTRN,");
                sb.Append(" JOB_CARD_SEA_IMP_TBL        JOB,");
                sb.Append(" CUSTOMER_MST_TBL            CMT,");
                sb.Append(" CUSTOMER_MST_TBL            SHP,");
                sb.Append(" CUSTOMER_MST_TBL            CONS,");
                sb.Append(" CURRENCY_TYPE_MST_TBL       CUMT,");
                sb.Append(" VESSEL_VOYAGE_TRN           VTRN,");
                sb.Append(" PORT_MST_TBL                POL,");
                sb.Append(" PORT_MST_TBL                POD");

                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_SEA_IMP_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.VOYAGE_TRN_FK = VTRN.VOYAGE_TRN_PK(+)");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                sb.Append(" AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                sb.Append(" and INV.CHK_INVOICE = 0");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append(" NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append(" from collections_trn_tbl ctrn");
                sb.Append(" where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append(" 0)) > 0");
                sb.Append(strCondition);
                sb.Append(" GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append(" INV.INVOICE_REF_NO,");
                sb.Append(" INV.INVOICE_DATE,");
                sb.Append(" SHP.CUSTOMER_NAME,");
                sb.Append(" CONS.CUSTOMER_NAME,");
                sb.Append(" POD.PORT_ID ,");
                sb.Append(" JOB.ETD_DATE,");
                sb.Append("CMT.CUSTOMER_MST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME");
                sb.Append(" ORDER BY INV.INVOICE_DATE DESC");

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q )";
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

        #endregion "Fetch Job Card for Sea-Import Report"

        #region "Fetch Job Card for Air-Import Report"

        //This fetch Job Cards for Air-Import
        /// <summary>
        /// Fetches the invoice air import report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceAirImportReport(Int32 LocFk = 0, string FromDt = "", string ToDt = "", string CustName = "", string ETDDt = "", short BizType = 2, short ProcessType = 1)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!(FromDt == null | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!(ToDt == null | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " And INV.INVOICE_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JOB.ETD_DATE,dateformat) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }

                sb.Append("SELECT INV.CONSOL_INVOICE_PK                     INVOICEPK,");
                sb.Append(" INV.INVOICE_REF_NO                       INVOICEREFNR,");
                sb.Append("  TO_CHAR(INV.INVOICE_DATE,dateformat)         INVOICEDATE,");
                sb.Append(" SHP.CUSTOMER_NAME                        SHIPPER,");
                sb.Append(" CONS.CUSTOMER_NAME                       CONSIGNEE,");
                sb.Append("  POD.PORT_ID                              POD,");
                sb.Append(" TO_CHAR(JOB.ETD_DATE, DATETIMEFORMAT24)       ETD_DATE,");
                sb.Append("  SUM(NVL(INVTRN.TOT_AMT_IN_LOC_CURR,0))   GrossAMT,");
                sb.Append("  CMT.CUSTOMER_MST_PK  CUST_PK,");
                sb.Append(" CMT.CUSTOMER_NAME               CUST_NAME");

                sb.Append(" FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");
                sb.Append(" JOB_CARD_AIR_IMP_TBL   JOB,");
                sb.Append(" CUSTOMER_MST_TBL       CMT,");
                sb.Append(" CUSTOMER_MST_TBL       SHP,  ");
                sb.Append(" CUSTOMER_MST_TBL       CONS,");
                sb.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                sb.Append(" PORT_MST_TBL           POL,");
                sb.Append(" PORT_MST_TBL    POD");
                sb.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_AIR_IMP_PK(+)");
                sb.Append(" AND JOB.SHIPPER_CUST_MST_FK = SHP.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append(" AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) ");
                sb.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+) ");
                sb.Append(" AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append(" FROM LOCATION_MST_TBL L");
                sb.Append(" WHERE L.LOCATION_MST_PK =" + LocFk + ")");
                sb.Append("  AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                sb.Append(" AND INV.PROCESS_TYPE ='" + ProcessType + "'");
                sb.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "'");
                sb.Append(" and INV.CHK_INVOICE = 0");
                sb.Append(" AND (INV.NET_RECEIVABLE -");
                sb.Append("   NVL((select sum(ctrn.recd_amount_hdr_curr)");
                sb.Append("  from collections_trn_tbl ctrn");
                sb.Append("  where ctrn.invoice_ref_nr like inv.invoice_ref_no),");
                sb.Append("   0)) > 0");
                sb.Append(strCondition);
                sb.Append("  GROUP BY INV.CONSOL_INVOICE_PK,");
                sb.Append("  INV.INVOICE_REF_NO,");
                sb.Append("  INV.INVOICE_DATE,");
                sb.Append("  SHP.CUSTOMER_NAME,");
                sb.Append("  CONS.CUSTOMER_NAME,");
                sb.Append("   POD.PORT_ID ,");
                sb.Append("  JOB.ETD_DATE,");
                sb.Append(" CMT.CUSTOMER_MST_PK,");
                sb.Append("  CMT.CUSTOMER_NAME");
                sb.Append("  ORDER BY INV.INVOICE_DATE DESC");

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                strSQL += " )q )";
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

        #endregion "Fetch Job Card for Air-Import Report"
    }
}