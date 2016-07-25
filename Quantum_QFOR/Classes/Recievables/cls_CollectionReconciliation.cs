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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCollectionReconciliation : CommonFeatures
    {
        #region "Fetch Grid Listing Details"

        /// <summary>
        /// Fetches the listing details.
        /// </summary>
        /// <param name="P_CUSTOMER_FK">The p_ custome r_ fk.</param>
        /// <param name="P_LOC_PK">The p_ lo c_ pk.</param>
        /// <param name="P_INV_NO">The p_ in v_ no.</param>
        /// <param name="P_FROM_DT">The p_ fro m_ dt.</param>
        /// <param name="P_TO_DT">The p_ t o_ dt.</param>
        /// <param name="P_FRT_COLL_NO">The p_ fr t_ col l_ no.</param>
        /// <param name="P_FRT_COLL_DT">The p_ fr t_ col l_ dt.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Flag">The flag.</param>
        /// <param name="biztype">The biztype.</param>
        /// <param name="processtype">The processtype.</param>
        /// <returns></returns>
        public DataSet FetchListingDetails(Int32 P_CUSTOMER_FK = 0, Int32 P_LOC_PK = 0, string P_INV_NO = "", string P_FROM_DT = "", string P_TO_DT = "", string P_FRT_COLL_NO = "", string P_FRT_COLL_DT = "", string SearchType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
        long Flag = 0, Int16 biztype = 2, Int16 processtype = 1)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            System.Text.StringBuilder sbcount = new System.Text.StringBuilder();
            System.Text.StringBuilder sbrecords = new System.Text.StringBuilder();
            string strCondition = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            string DoInvoice = null;
            string InvoiceNo = null;

            if (P_FROM_DT.ToString().Trim().Length > 0)
            {
                strCondition += "  AND to_date(FRT_HDR.COLLECTIONS_DATE,'" + dateFormat + "') >= to_date('" + P_FROM_DT + "' ,'" + dateFormat + "')";
            }

            if (P_TO_DT.ToString().Trim().Length > 0)
            {
                strCondition += "  AND to_date(FRT_HDR.COLLECTIONS_DATE,'" + dateFormat + "') <= to_date('" + P_TO_DT + "' ,'" + dateFormat + "')";
            }

            if (P_FRT_COLL_DT.ToString().Trim().Length > 0)
            {
                strCondition += "  AND to_date(FRT_HDR.COLLECTIONS_DATE,'" + dateFormat + "') = to_date('" + P_FRT_COLL_DT + "' ,'" + dateFormat + "')";
            }

            if (P_INV_NO.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(INV.INVOICE_REF_NO) LIKE '" + P_INV_NO.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(INV.INVOICE_REF_NO) LIKE '%" + P_INV_NO.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(INV.INVOICE_REF_NO) LIKE '%" + P_INV_NO.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (P_FRT_COLL_NO.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(FRT_HDR.COLLECTIONS_REF_NO) LIKE '" + P_FRT_COLL_NO.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(FRT_HDR.COLLECTIONS_REF_NO) LIKE '%" + P_FRT_COLL_NO.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(FRT_HDR.COLLECTIONS_REF_NO) LIKE '%" + P_FRT_COLL_NO.ToUpper().Replace("'", "''") + "%'";
                }
            }

            if (P_CUSTOMER_FK != 0)
            {
                strCondition = strCondition + " AND FRT_HDR.CUSTOMER_MST_FK = " + P_CUSTOMER_FK + " ";
            }

            if (Flag == 0)
            {
                strCondition += " AND 1=2 ";
            }

            if (P_LOC_PK != 0)
            {
                strCondition = strCondition + " AND inv.created_by_fk in (select u. user_mst_pk from user_mst_tbl u where u.default_location_fk in ";
                strCondition = strCondition + " (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L    START WITH L.LOCATION_MST_PK =  " + P_LOC_PK + "   CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK ))";
            }

            strCondition = strCondition + " AND FRT_HDR.Business_Type = " + biztype + " ";
            strCondition = strCondition + " AND FRT_HDR.Process_Type = " + processtype + " ";

            sb.Append("select ROWNUM AS SLNO, QRY.*");
            sb.Append("  FROM ( ");
            sb.Append(" SELECT DISTINCT CUST.CUSTOMER_MST_PK CUSTOMER_MST_FK,");
            sb.Append("                                CUST.CUSTOMER_ID CUSTOMER_ID,");
            sb.Append("                                CUST.CUSTOMER_NAME CUSTOMER_NAME,");
            sb.Append("                                FRT_HDR.COLLECTIONS_REF_NO FREIGHT_COLL_NR,");
            sb.Append("                                TO_DATE(FRT_HDR.COLLECTIONS_DATE,");
            sb.Append("                                        'dd/MM/yyyy') FREIGHT_COLL_DT,");
            sb.Append("                                CURR.CURRENCY_ID CURRENCY,");
            sb.Append("                                NVL(SUM(FRT_DTL.RECD_AMOUNT_HDR_CURR), 0) AMOUNT,");
            sb.Append("                                SUM((SELECT NVL(CIT.NET_RECEIVABLE *");
            sb.Append("                                       (SELECT NVL(GET_EX_RATE(CIT.CURRENCY_MST_FK,");
            sb.Append("                                                               CURR.CURRENCY_MST_PK,");
            sb.Append("                                                               CIT.INVOICE_DATE),");
            sb.Append("                                                   0)");
            sb.Append("                                          FROM DUAL),");
            sb.Append("                                       0)");
            sb.Append("                              FROM CONSOL_INVOICE_TBL CIT");
            sb.Append("                             WHERE CIT.INVOICE_REF_NO =");
            sb.Append("                                   FRT_DTL.INVOICE_REF_NR)) NET_RECEIVABLE,");
            sb.Append("                                NVL(FRT_DTL.EXCESS_AMOUNT, 0) OUTSTANDING,");
            sb.Append("                                '...' BTN_INV_DTLS,");
            sb.Append("                                FRT_HDR.COLLECTIONS_TBL_PK FRT_COLL_HDR_PK,");
            sb.Append("                                '0' CHGFLAG");
            sb.Append("                  FROM COLLECTIONS_TBL          FRT_HDR,");
            sb.Append("                        CONSOL_INVOICE_TBL        INV,");
            sb.Append("                       COLLECTIONS_TRN_TBL      FRT_DTL,");
            sb.Append("                       COLLECTIONS_MODE_TRN_TBL CMODE,");
            sb.Append("                       CUSTOMER_MST_TBL         CUST,");
            sb.Append("                       CURRENCY_TYPE_MST_TBL    CURR");
            sb.Append("                 WHERE FRT_HDR.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
            sb.Append("                   AND FRT_HDR.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
            sb.Append("                   and FRT_DTL.Invoice_Ref_Nr = INV.invoice_ref_no");
            sb.Append("                   AND FRT_HDR.COLLECTIONS_TBL_PK =");
            sb.Append("                       FRT_DTL.COLLECTIONS_TBL_FK");
            sb.Append("                   AND CMODE.COLLECTIONS_TBL_FK = FRT_HDR.COLLECTIONS_TBL_PK");
            sb.Append("                   AND FRT_DTL.EXCESS_AMOUNT > 0");
            sb.Append(strCondition);
            sb.Append("                 GROUP BY CUST.CUSTOMER_MST_PK,");
            sb.Append("                          CUST.CUSTOMER_ID,");
            sb.Append("                          CUST.CUSTOMER_NAME,");
            sb.Append("                          FRT_HDR.COLLECTIONS_REF_NO,");
            sb.Append("                          FRT_HDR.COLLECTIONS_DATE,");
            sb.Append("                          FRT_HDR.COLLECTIONS_TBL_PK,");
            sb.Append("                          CURR.CURRENCY_ID,");
            sb.Append("                          FRT_DTL.EXCESS_AMOUNT");
            sb.Append(" ) qry WHERE QRY.AMOUNT IS NOT NULL ");

            sbcount.Append(" select count(*) from ( ");
            sbcount.Append(sb.ToString());
            sbcount.Append(" ) ");

            //TotalRecords = (Int32)objWF.ExecuteScaler(sbcount.ToString());
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

            sbrecords.Append(" select * from ( ");
            sbrecords.Append(sb.ToString());
            sbrecords.Append(" ) WHERE SLNO  Between " + start + " and " + last);

            try
            {
                return objWF.GetDataSet(sbrecords.ToString());
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch Grid Listing Details"

        #region "Fetch Header Data"

        /// <summary>
        /// Fetches the header data.
        /// </summary>
        /// <param name="FRT_HDR_PK">The fr t_ hd r_ pk.</param>
        /// <returns></returns>
        public DataSet FetchHeaderData(Int32 FRT_HDR_PK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT distinct        CUST.CUSTOMER_ID CUSTOMER_ID,");
            sb.Append("                       CUST.CUSTOMER_NAME CUSTOMER_NAME,");
            sb.Append("                       FRT_HDR.COLLECTIONS_REF_NO FREIGHT_COLL_NR,");
            sb.Append("                       to_date(FRT_HDR.COLLECTIONS_DATE, 'dd/MM/yyyy') FREIGHT_COLL_DT,");
            sb.Append("                       CURR.CURRENCY_ID CURRENCY,");
            sb.Append(" /*   (select (frtchq.cheque_amount - frtchq.reconciled_amount) ");
            sb.Append("                     from freight_collect_cheque_trn frtchq");
            sb.Append("                   where frtchq.freight_collection_mst_fk =");
            sb.Append("                     FRT_HDR.FREIGHT_COLLECT_HDR_PK)*/ ");
            sb.Append("      NVL(SUM(FRT_DTL.RECD_AMOUNT_HDR_CURR), 0) AMOUNT, FRT_HDR.CURRENCY_MST_FK, ");
            sb.Append("      FRT_DTL.EXCESS_AMOUNT EXCESS_AMT, ");
            sb.Append("    FRT_HDR.COLLECTIONS_TBL_PK,FRT_HDR.BUSINESS_TYPE, FRT_HDR.PROCESS_TYPE");
            sb.Append("                       from COLLECTIONS_TBL      FRT_HDR,");
            sb.Append("                       COLLECTIONS_TRN_TBL       FRT_DTL,");
            sb.Append("                       COLLECTIONS_MODE_TRN_TBL  CMODE,");
            sb.Append("                       CUSTOMER_MST_TBL          CUST,");
            sb.Append("                       CURRENCY_TYPE_MST_TBL     CURR");
            sb.Append("                 WHERE FRT_HDR.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
            sb.Append("                   AND FRT_HDR.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
            sb.Append("                   AND FRT_HDR.COLLECTIONS_TBL_PK = FRT_DTL.COLLECTIONS_TBL_FK");
            sb.Append("                   AND CMODE.COLLECTIONS_TBL_FK = FRT_HDR.COLLECTIONS_TBL_PK");
            sb.Append("                   AND FRT_HDR.COLLECTIONS_TBL_PK = " + FRT_HDR_PK + " ");
            sb.Append("                   AND FRT_DTL.EXCESS_AMOUNT>0");
            sb.Append("                   GROUP BY CUST.CUSTOMER_MST_PK,");
            sb.Append("                          CUST.CUSTOMER_ID,");
            sb.Append("                          CUST.CUSTOMER_NAME,");
            sb.Append("                          FRT_HDR.COLLECTIONS_REF_NO,");
            sb.Append("                          FRT_HDR.COLLECTIONS_DATE,");
            sb.Append("                          FRT_HDR.COLLECTIONS_TBL_PK,");
            sb.Append("                          CURR.CURRENCY_ID, CMODE.RECD_AMOUNT,FRT_DTL.EXCESS_AMOUNT, FRT_HDR.CURRENCY_MST_FK,FRT_HDR.BUSINESS_TYPE, FRT_HDR.PROCESS_TYPE");
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch Header Data"

        #region "Fetch Grid Data"

        /// <summary>
        /// Fetches the grid data.
        /// </summary>
        /// <param name="FRT_HDR_PK">The fr t_ hd r_ pk.</param>
        /// <param name="CUST_PK">The cus t_ pk.</param>
        /// <param name="DtFromDate">The dt from date.</param>
        /// <param name="DtToDate">The dt to date.</param>
        /// <param name="FrmFlag">The FRM flag.</param>
        /// <param name="CURR_PK">The current r_ pk.</param>
        /// <param name="ColDate">The col date.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Pro">The pro.</param>
        /// <returns></returns>
        public DataSet FetchGridData(Int32 FRT_HDR_PK, Int32 CUST_PK, string DtFromDate, string DtToDate, string FrmFlag = "", Int32 CURR_PK = 0, string ColDate = "", Int32 Biz = 0, Int32 Pro = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (FrmFlag == "OTC")
            {
                sb.Append(" select rownum as slno ,qry.* from ( ");
                sb.Append("select FrtDTl.Collections_Trn_Pk FRT_RECONCIL_DTL_PK,");
                sb.Append("       inv.consol_invoice_pk INVOICE_FK,");
                sb.Append("       inv.invoice_ref_no INVOICE_NR,");
                sb.Append("       inv.invoice_date INVOICE_DATE,");
                sb.Append("       curr.currency_id CURRENCY,");
                sb.Append("       inv.invoice_amt INVOICE_AMT,");
                sb.Append("       nvl((select sum(ctrn.Recd_Amount_Hdr_Curr) from COLLECTIONS_TRN_TBL ctrn");
                sb.Append("       where ctrn.invoice_ref_nr = inv.invoice_ref_no),0) RECEIVED_AMT,");
                sb.Append("       FRTDTL.RECD_AMOUNT_HDR_CURR CURRENT_RECPT_AMT,");
                sb.Append("       NVL(GET_EX_RATE(inv.currency_mst_fk, FrtHdr.Currency_Mst_Fk, sysdate), 0) ROE,");
                sb.Append("       FRTDTL.RECD_AMOUNT_HDR_CURR AMT_IN_COLL_CURR,");
                sb.Append("       (INV.INVOICE_AMT - nvl((select sum(ctrn.Recd_Amount_Hdr_Curr) from COLLECTIONS_TRN_TBL ctrn");
                sb.Append("       where ctrn.invoice_ref_nr = inv.invoice_ref_no),0)) OUTSTANDING,");
                sb.Append("       case");
                sb.Append("         when nvl((select sum(ctrn.Recd_Amount_Hdr_Curr) from COLLECTIONS_TRN_TBL ctrn");
                sb.Append("         where ctrn.invoice_ref_nr = inv.invoice_ref_no),0) = inv.invoice_amt then");
                sb.Append("          '1'");
                sb.Append("         else");
                sb.Append("          '0'");
                sb.Append("       end SELFLAG");
                sb.Append("  from CONSOL_INVOICE_TBL        inv,");
                sb.Append("       COLLECTIONS_TBL           FrtHdr,");
                sb.Append("       COLLECTIONS_TRN_TBL       FrtDTl,");
                sb.Append("       currency_type_mst_tbl   curr");
                sb.Append(" where FrtHdr.Collections_Tbl_Pk = FrtDTl.Collections_Tbl_Fk");
                sb.Append("   and FrtDTl.Invoice_Ref_Nr = inv.invoice_ref_no");
                sb.Append("   and FrtHdr.Currency_Mst_Fk = curr.currency_mst_pk");
                sb.Append("   and FrtHdr.Collections_Tbl_Pk in (" + FRT_HDR_PK + ")");
                sb.Append("   AND FrtHdr.Customer_Mst_Fk in (" + CUST_PK + " )");
                sb.Append("   ) QRY ");
            }
            else
            {
                sb.Append("SELECT ROWNUM AS SLNO, QRY.*");
                sb.Append("  FROM (SELECT DISTINCT INV.CONSOL_INVOICE_PK FRT_RECONCIL_DTL_PK,");
                sb.Append("                        INV.CONSOL_INVOICE_PK INVOICE_FK,");
                sb.Append("                        INV.INVOICE_REF_NO INVOICE_NR,");
                sb.Append("                        INV.INVOICE_DATE INVOICE_DATE,");
                sb.Append("                               '' JOB_TYPE,");
                sb.Append("                        CURR.CURRENCY_ID CURRENCY,");
                sb.Append("                        INV.NET_RECEIVABLE INVOICE_AMT,");
                sb.Append("                        NVL(GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                        " + CURR_PK + ", ");
                sb.Append("                                        '" + ColDate + "'), ");
                sb.Append("                            0) ROE,");
                sb.Append("                        (INV.NET_RECEIVABLE * NVL(GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                              " + CURR_PK + ", ");
                sb.Append("                                                              '" + ColDate + "'),");
                sb.Append("                                                  0)) AMT_IN_COLL_CURR,");
                sb.Append("                        (NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                              FROM COLLECTIONS_TRN_TBL CTRN");
                sb.Append("                             WHERE CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                            0)* NVL(GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                              " + CURR_PK + ", ");
                sb.Append("                                                              '" + ColDate + "'),");
                sb.Append("                                                  0)) RECEIVED_AMT,");
                sb.Append("                        0 CURRENT_RECPT_AMT,");
                sb.Append("                        (((INV.NET_RECEIVABLE * NVL(GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                              " + CURR_PK + ", ");
                sb.Append("                                                              '" + ColDate + "'),");
                sb.Append("                                                  0))) -");
                sb.Append("                        (NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                               FROM COLLECTIONS_TRN_TBL CTRN");
                sb.Append("                              WHERE CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                             0)* NVL(GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                              " + CURR_PK + ", ");
                sb.Append("                                                              '" + ColDate + "'),");
                sb.Append("                                                  0))) OUTSTANDING,");
                sb.Append("                        '' SELFLAG");
                sb.Append("          FROM CONSOL_INVOICE_TBL INV,");
                sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
                sb.Append("               (SELECT FRTDTL.INVOICE_REF_NR,");
                sb.Append("                       FRTHDR.CUSTOMER_MST_FK,");
                sb.Append("                       FRTDTL.RECD_AMOUNT_HDR_CURR,");
                sb.Append("                       FRTDTL.COLLECTIONS_TRN_PK");
                sb.Append("                  FROM COLLECTIONS_TBL FRTHDR, COLLECTIONS_TRN_TBL FRTDTL");
                sb.Append("                 WHERE FRTHDR.COLLECTIONS_TBL_PK = FRTDTL.COLLECTIONS_TBL_FK) COL,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CURR");
                sb.Append("         WHERE COL.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
                sb.Append("           AND INV.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                sb.Append("           AND INV.CHK_INVOICE = 1");
                sb.Append("                      AND (INV.NET_RECEIVABLE * NVL(GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                              " + CURR_PK + ", ");
                sb.Append("                                                              '" + ColDate + "'),");
                sb.Append("                                                  0)) >");
                sb.Append("                        (NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                               FROM COLLECTIONS_TRN_TBL CTRN");
                sb.Append("                              WHERE CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                             0)* NVL(GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                              " + CURR_PK + ", ");
                sb.Append("                                                              '" + ColDate + "'),");
                sb.Append("                                                  0))");
                sb.Append("           AND INV.CUSTOMER_MST_FK = " + CUST_PK);
                sb.Append("           AND INV.BUSINESS_TYPE = " + Biz);
                sb.Append("           AND INV.PROCESS_TYPE = " + Pro);
                sb.Append("           ORDER BY TO_DATE(INVOICE_DATE) DESC, INVOICE_NR DESC");
                sb.Append("   ) QRY ");
            }

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch Grid Data"

        #region "Update Functions"

        /// <summary>
        /// Updates the invoice received amt.
        /// </summary>
        /// <param name="DS_FrtColl">The d s_ FRT coll.</param>
        /// <returns></returns>
        public bool UpdateInvoiceReceivedAmt(DataSet DS_FrtColl)
        {
            string UpdQuery = null;
            int i = 0;
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            try
            {
                for (i = 0; i <= DS_FrtColl.Tables[0].Rows.Count - 1; i++)
                {
                    UpdQuery = string.Empty;
                    UpdQuery += "UPDATE invoice_trn inv  ";
                    UpdQuery += " SET inv.recived_amount  = inv.recived_amount  + " + DS_FrtColl.Tables[0].Rows[i]["CURRENT_RECPT_AMT"] + " ";
                    UpdQuery += " WHERE inv.invoice_trn_pk  =  " + DS_FrtColl.Tables[0].Rows[i]["INVOICE_FK"] + " ";
                    objWK.ExecuteCommands(UpdQuery);
                }
                return true;
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

        /// <summary>
        /// Updates the freight HDR.
        /// </summary>
        /// <param name="FrtHDRPK">The FRT HDRPK.</param>
        /// <returns></returns>
        public bool UpdateFreightHDR(Int32 FrtHDRPK)
        {
            string UpdQuery = null;
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            try
            {
                UpdQuery = string.Empty;
                UpdQuery += "update freight_collect_hdr_trn fhdr set fhdr.fully_recociled = 1 where fhdr.freight_collect_hdr_pk = " + FrtHDRPK + "  ";
                objWK.ExecuteCommands(UpdQuery);
                return true;
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

        /// <summary>
        /// Updates the FRT cheque amt.
        /// </summary>
        /// <param name="FrtHDRPK">The FRT HDRPK.</param>
        /// <param name="ReconcilAmt">The reconcil amt.</param>
        /// <returns></returns>
        public bool UpdateFrtChequeAmt(Int32 FrtHDRPK, double ReconcilAmt)
        {
            string UpdQuery = null;
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            try
            {
                UpdQuery = string.Empty;
                UpdQuery += "update freight_collect_cheque_trn fc set fc.reconciled_amount = fc.reconciled_amount + " + ReconcilAmt + "  where fc.freight_collection_mst_fk =  " + FrtHDRPK + "  ";
                objWK.ExecuteCommands(UpdQuery);
                return true;
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

        /// <summary>
        /// Save_s the coll_ reconcile.
        /// </summary>
        /// <param name="collPk">The coll pk.</param>
        /// <param name="InvRefNr">The inv reference nr.</param>
        /// <param name="Amt">The amt.</param>
        public void Save_Coll_Reconcile(long collPk, string InvRefNr, double Amt)
        {
            //
            WorkFlow objWK = new WorkFlow();
            OracleCommand cmd = new OracleCommand();
            objWK.OpenConnection();
            try
            {
                var _with1 = cmd;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".COLLECTIONS_TBL_PKG.Save_Reconcile_Collection";
                var _with2 = _with1.Parameters;
                _with2.Add("Coll_Pk_IN", collPk).Direction = ParameterDirection.Input;
                _with2.Add("Inv_RefNr_IN", InvRefNr).Direction = ParameterDirection.Input;
                _with2.Add("Coll_Amt_IN", Amt).Direction = ParameterDirection.Input;
                cmd.ExecuteNonQuery();
                objWK.CloseConnection();
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

        /// <summary>
        /// Fetches the col pk.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchColPk(string JobPk, int BizType)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT DISTINCT C.COLLECTIONS_TBL_PK,inv.customer_mst_fk");
            sb.Append("  FROM COLLECTIONS_TBL        C,");
            sb.Append("       COLLECTIONS_TRN_TBL    CTRN,");
            sb.Append("       CONSOL_INVOICE_TBL     INV,");
            sb.Append("       CONSOL_INVOICE_TRN_TBL INVTRN");
            sb.Append(" WHERE C.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
            sb.Append("   AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
            sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
            sb.Append("   AND INV.PROCESS_TYPE=1 AND INV.BUSINESS_TYPE=" + BizType);
            sb.Append("   AND INVTRN.JOB_CARD_FK IN (" + JobPk + ")");

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Update Functions"
    }
}