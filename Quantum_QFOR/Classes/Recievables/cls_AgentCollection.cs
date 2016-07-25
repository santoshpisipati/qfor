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
using System.Collections;
using System.Configuration;
using System.Data;

//Snigdharani - 05/12/2008 - Crated this class file for Collection of agent invoices
// ERROR: Not supported in C#: OptionDeclaration
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsAgentCollection : CommonFeatures
    {
        /// <summary>
        /// The pk value
        /// </summary>
        public int PkVal;

        /// <summary>
        /// The coll reference nr
        /// </summary>
        public string CollRefNr;

        #region " Fetch"

        /// <summary>
        /// Fetches the data.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CollType">Type of the coll.</param>
        /// <param name="intAgentPk">The int agent pk.</param>
        /// <param name="InvRefNo">The inv reference no.</param>
        /// <param name="intJobPk">The int job pk.</param>
        /// <param name="intBaseCurrPk">The int base curr pk.</param>
        /// <param name="lngLocPk">The LNG loc pk.</param>
        /// <param name="strFromDt">The string from dt.</param>
        /// <param name="strToDt">The string to dt.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <returns></returns>
        public DataSet FetchData(short BizType, short Process, short CollType, int intAgentPk, string InvRefNo, int intJobPk, int intBaseCurrPk, long lngLocPk, string strFromDt = "", string strToDt = "",
        int ExType = 1)
        {
            WorkFlow objWF = new WorkFlow();

            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder strExRate = new System.Text.StringBuilder(1000);
            System.Text.StringBuilder strExRate1 = new System.Text.StringBuilder(1000);
            System.Text.StringBuilder strCond = new System.Text.StringBuilder(2000);
            if (CollType == 0)
            {
                if (!(intAgentPk > 0) & !(InvRefNo.Length > 0) & !(intJobPk > 0))
                {
                    strCond.Append(" AND 1=2");
                }
                if (!(InvRefNo.Length > 0) & !(intJobPk > 0))
                {
                    if (strFromDt.Length > 0 & !(strToDt.Length > 0))
                    {
                        strCond.Append("           AND JOB.JOBCARD_DATE >= TO_DATE('" + strFromDt + "', '" + dateFormat + "')");
                    }
                    else if (!(strFromDt.Length > 0) & strToDt.Length > 0)
                    {
                        strCond.Append("AND JOB.JOBCARD_DATE <= TO_DATE('" + strToDt + "', '" + dateFormat + "')");
                    }
                    else if (strFromDt.Length > 0 & strToDt.Length > 0)
                    {
                        strCond.Append("           AND JOB.JOBCARD_DATE BETWEEN TO_DATE('" + strFromDt + "', '" + dateFormat + "') AND");
                        strCond.Append("               TO_DATE('" + strToDt + "', '" + dateFormat + "')");
                    }
                }
                if (Process == 2 & ExType == 3)
                {
                    strExRate.Append(" (select get_ex_rate1( " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.CURRENCY_MST_FK,INV.INVOICE_DATE," + ExType + ") from dual) ROE,");
                    strExRate.Append(" round((INV.net_inv_amt)*(select get_ex_rate1(" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.CURRENCY_MST_FK,INV.INVOICE_DATE," + ExType + ") from dual),2) AmtinLoc,");
                }
                else if ((Process == 2 & ExType == 1) | (Process == 1 & ExType == 1))
                {
                    strExRate.Append(" (select get_ex_rate( INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.INVOICE_DATE) from dual) ROE,");
                    strExRate.Append(" round((INV.net_inv_amt)*(select get_ex_rate(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.INVOICE_DATE) from dual),2) AmtinLoc,");
                }
                else if (Process == 1 & ExType == 2)
                {
                    strExRate.Append(" (select get_roe_sell( INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",job.voyage_trn_fk) from dual) ROE,");
                    strExRate.Append(" round((INV.net_inv_amt)*(select get_roe_sell(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",job.voyage_trn_fk) from dual),2) AmtinLoc,");
                }
                if (Process == 2 & ExType == 3)
                {
                    strExRate1.Append(" round((inv.net_inv_amt * (select get_ex_rate1(" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", ");
                    strExRate1.Append("  INV.CURRENCY_MST_FK,");
                    strExRate1.Append("  INV.INVOICE_DATE," + ExType + ")");
                    strExRate1.Append("  from dual) - ");
                }
                else if ((Process == 2 & ExType == 1) | (Process == 1 & ExType == 1))
                {
                    strExRate1.Append(" round((inv.net_inv_amt * (select get_ex_rate(INV.CURRENCY_MST_FK, ");
                    strExRate1.Append("  " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                    strExRate1.Append("  INV.INVOICE_DATE)");
                    strExRate1.Append("  from dual) - ");
                }
                else if (Process == 1 & ExType == 2)
                {
                    strExRate1.Append(" round((inv.net_inv_amt * (select get_roe_sell(INV.CURRENCY_MST_FK, ");
                    strExRate1.Append("  " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                    strExRate1.Append("  job.voyage_trn_fk)");
                    strExRate1.Append("  from dual) - ");
                }
                if (BizType == 2 & Process == 2)
                {
                    sb.Append("SELECT ROWNUM SNo, INVPK, INVOICE_REF_NO, INVOICE_DATE, CURRENCY_MST_FK,");
                    sb.Append("       CURRENCY_ID, QRY.net_inv_amt NET_PAYABLE, ROE, AMTINLOC, recieved,");
                    sb.Append("       receivable, CurrReceipt, SEL, CUST");
                    sb.Append("  FROM (SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                    sb.Append("                        INV.INVOICE_REF_NO,");
                    sb.Append("                        INV.INVOICE_DATE,");
                    sb.Append("                        INV.CURRENCY_MST_FK,");
                    sb.Append("                        CUMT.CURRENCY_ID,");
                    sb.Append("                        INV.net_inv_amt,");
                    sb.Append(strExRate);
                    sb.Append("                        nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR)");
                    sb.Append("                              from collections_tbl     clt,");
                    sb.Append("                                   collections_trn_tbl clttrn");
                    sb.Append("                             where clt.collections_tbl_pk =");
                    sb.Append("                                   clttrn.collections_tbl_fk and clttrn.from_inv_or_consol_inv = 3");
                    sb.Append("                               and Clttrn.Invoice_Ref_Nr =");
                    sb.Append("                                   inv.invoice_ref_no),");
                    sb.Append("                            0) recieved,");
                    sb.Append(strExRate1);
                    sb.Append("                         nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR)");
                    sb.Append("                                                      from collections_tbl     clt,");
                    sb.Append("                                                           collections_trn_tbl clttrn");
                    sb.Append("                                                     where clt.collections_tbl_pk =");
                    sb.Append("                                                           clttrn.collections_tbl_fk and clttrn.from_inv_or_consol_inv = 3");
                    sb.Append("                                                       and Clttrn.Invoice_Ref_Nr =");
                    sb.Append("                                                           inv.invoice_ref_no),");
                    sb.Append("                                                    0)),");
                    sb.Append("                              2) receivable,");
                    sb.Append("                        0 CurrReceipt,");
                    sb.Append("                        '0' Sel,");
                    sb.Append("                        (SELECT AMT.AGENT_MST_PK FROM AGENT_MST_TBL AMT WHERE AMT.AGENT_MST_PK = " + intAgentPk + ") CUST");
                    sb.Append("          FROM INV_AGENT_TBL INV,");
                    sb.Append("               JOB_CARD_TRN  JOB,");
                    sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                    sb.Append("               USER_MST_TBL          UMT");
                    sb.Append("         WHERE INV.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
                    sb.Append("           AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
                    sb.Append("           AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + "");
                    sb.Append("           AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                    if (intAgentPk > 0)
                    {
                        sb.Append("           AND (JOB.POL_AGENT_MST_FK = " + intAgentPk + " OR JOB.CB_AGENT_MST_FK = " + intAgentPk + " )");
                        sb.Append("           AND INV.AGENT_MST_FK = " + intAgentPk + " ");
                    }
                    if (InvRefNo.Length > 0)
                    {
                        sb.Append(" AND INV.INVOICE_REF_NO = '" + InvRefNo + "'");
                    }
                    if (intJobPk > 0)
                    {
                        sb.Append("           AND JOB.JOB_CARD_TRN_PK = " + intJobPk + "");
                    }
                    sb.Append(strCond);
                    sb.Append(") QRY");
                }
                else if (BizType == 1 & Process == 2)
                {
                    sb.Append("SELECT ROWNUM SNo, INVPK, INVOICE_REF_NO, INVOICE_DATE, CURRENCY_MST_FK,");
                    sb.Append("       CURRENCY_ID, QRY.net_inv_amt NET_PAYABLE, ROE, AMTINLOC, recieved,");
                    sb.Append("       receivable, CurrReceipt, SEL, CUST");
                    sb.Append(" FROM (SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                    sb.Append("                INV.INVOICE_REF_NO,");
                    sb.Append("                INV.INVOICE_DATE,");
                    sb.Append("                INV.CURRENCY_MST_FK,");
                    sb.Append("                CUMT.CURRENCY_ID,");
                    sb.Append("                INV.net_inv_amt,");
                    sb.Append(strExRate);
                    sb.Append("                nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR)");
                    sb.Append("                      from collections_tbl clt, collections_trn_tbl clttrn");
                    sb.Append("                     where clt.collections_tbl_pk =");
                    sb.Append("                           clttrn.collections_tbl_fk and clttrn.from_inv_or_consol_inv = 3");
                    sb.Append("                       and Clttrn.Invoice_Ref_Nr = inv.invoice_ref_no),");
                    sb.Append("                    0) recieved,");
                    sb.Append(strExRate1);
                    sb.Append("                      nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR)");
                    sb.Append("                             from collections_tbl     clt,");
                    sb.Append("                                  collections_trn_tbl clttrn");
                    sb.Append("                            where clt.collections_tbl_pk =");
                    sb.Append("                                  clttrn.collections_tbl_fk and clttrn.from_inv_or_consol_inv = 3");
                    sb.Append("                              and Clttrn.Invoice_Ref_Nr =");
                    sb.Append("                                  inv.invoice_ref_no),");
                    sb.Append("                           0)),");
                    sb.Append("                      2) receivable,");
                    sb.Append("                0 CurrReceipt,");
                    sb.Append("                '0' Sel,");
                    sb.Append("                        (SELECT AMT.AGENT_MST_PK FROM AGENT_MST_TBL AMT WHERE AMT.AGENT_MST_PK = " + intAgentPk + ") CUST");
                    sb.Append("  FROM INV_AGENT_TBL INV,");
                    sb.Append("       JOB_CARD_TRN  JOB,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
                    sb.Append("       USER_MST_TBL          UMT");
                    sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                    sb.Append("   AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
                    sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + "");
                    sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                    if (intAgentPk > 0)
                    {
                        sb.Append("   AND (JOB.POL_AGENT_MST_FK = " + intAgentPk + " OR JOB.CB_AGENT_MST_FK = " + intAgentPk + " )");
                        sb.Append("   AND INV.AGENT_MST_FK = " + intAgentPk + " ");
                    }
                    if (InvRefNo.Length > 0)
                    {
                        sb.Append(" AND INV.INVOICE_REF_NO = '" + InvRefNo + "'");
                    }
                    if (intJobPk > 0)
                    {
                        sb.Append("           AND JOB.JOB_CARD_TRN_PK = " + intJobPk + "");
                    }
                    sb.Append(strCond);
                    sb.Append(") QRY");
                }
                else if (BizType == 1 & Process == 1)
                {
                    sb.Append("SELECT ROWNUM SNo, INVPK, INVOICE_REF_NO, INVOICE_DATE, CURRENCY_MST_FK,");
                    sb.Append("       CURRENCY_ID, QRY.net_inv_amt NET_PAYABLE, ROE, AMTINLOC, recieved,");
                    sb.Append("       receivable, CurrReceipt, SEL, CUST");
                    sb.Append("  FROM (SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                    sb.Append("                INV.INVOICE_REF_NO,");
                    sb.Append("                INV.INVOICE_DATE,");
                    sb.Append("                INV.CURRENCY_MST_FK,");
                    sb.Append("                CUMT.CURRENCY_ID,");
                    sb.Append("                INV.net_inv_amt,");
                    sb.Append(strExRate);
                    sb.Append("                nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR)");
                    sb.Append("                      from collections_tbl clt, collections_trn_tbl clttrn");
                    sb.Append("                     where clt.collections_tbl_pk =");
                    sb.Append("                           clttrn.collections_tbl_fk and clttrn.from_inv_or_consol_inv = 3");
                    sb.Append("                       and Clttrn.Invoice_Ref_Nr = inv.invoice_ref_no),");
                    sb.Append("                    0) recieved,");
                    sb.Append(strExRate1);
                    sb.Append("                      nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR)");
                    sb.Append("                             from collections_tbl     clt,");
                    sb.Append("                                  collections_trn_tbl clttrn");
                    sb.Append("                            where clt.collections_tbl_pk =");
                    sb.Append("                                  clttrn.collections_tbl_fk and clttrn.from_inv_or_consol_inv = 3");
                    sb.Append("                              and Clttrn.Invoice_Ref_Nr =");
                    sb.Append("                                  inv.invoice_ref_no),");
                    sb.Append("                           0)),");
                    sb.Append("                      2) receivable,");
                    sb.Append("                0 CurrReceipt,");
                    sb.Append("                '0' Sel,");
                    sb.Append("                        (SELECT AMT.AGENT_MST_PK FROM AGENT_MST_TBL AMT WHERE AMT.AGENT_MST_PK = " + intAgentPk + ") CUST");
                    sb.Append("  FROM INV_AGENT_TBL INV,");
                    sb.Append("       JOB_CARD_TRN  JOB,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
                    sb.Append("       USER_MST_TBL          UMT");
                    sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                    sb.Append("   AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
                    sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + "");
                    sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                    if (intAgentPk > 0)
                    {
                        sb.Append("   AND (JOB.DP_AGENT_MST_FK = " + intAgentPk + "  OR JOB.CB_AGENT_MST_FK = " + intAgentPk + " )");
                        sb.Append("   AND INV.AGENT_MST_FK = " + intAgentPk + " ");
                    }
                    if (InvRefNo.Length > 0)
                    {
                        sb.Append(" AND INV.INVOICE_REF_NO = '" + InvRefNo + "'");
                    }
                    if (intJobPk > 0)
                    {
                        sb.Append("           AND JOB.JOB_CARD_TRN_PK = " + intJobPk + "");
                    }
                    sb.Append(strCond);
                    sb.Append(") QRY");
                }
                else if (BizType == 2 & Process == 1)
                {
                    sb.Append("SELECT ROWNUM SNo, INVPK, INVOICE_REF_NO, INVOICE_DATE, CURRENCY_MST_FK,");
                    sb.Append("       CURRENCY_ID, QRY.net_inv_amt NET_PAYABLE, ROE, AMTINLOC, recieved,");
                    sb.Append("       receivable, CurrReceipt, SEL, CUST");
                    sb.Append("  FROM (SELECT DISTINCT INV.INV_AGENT_PK INVPK,");
                    sb.Append("                INV.INVOICE_REF_NO,");
                    sb.Append("                INV.INVOICE_DATE,");
                    sb.Append("                INV.CURRENCY_MST_FK,");
                    sb.Append("                CUMT.CURRENCY_ID,");
                    sb.Append("                INV.net_inv_amt,");
                    sb.Append(strExRate);
                    sb.Append("                nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR)");
                    sb.Append("                      from collections_tbl clt, collections_trn_tbl clttrn");
                    sb.Append("                     where clt.collections_tbl_pk =");
                    sb.Append("                           clttrn.collections_tbl_fk and clttrn.from_inv_or_consol_inv = 3");
                    sb.Append("                       and Clttrn.Invoice_Ref_Nr = inv.invoice_ref_no),");
                    sb.Append("                    0) recieved,");
                    sb.Append(strExRate1);
                    sb.Append("                      nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR)");
                    sb.Append("                             from collections_tbl     clt,");
                    sb.Append("                                  collections_trn_tbl clttrn");
                    sb.Append("                            where clt.collections_tbl_pk =");
                    sb.Append("                                  clttrn.collections_tbl_fk and clttrn.from_inv_or_consol_inv = 3");
                    sb.Append("                              and Clttrn.Invoice_Ref_Nr =");
                    sb.Append("                                  inv.invoice_ref_no),");
                    sb.Append("                           0)),");
                    sb.Append("                      2) receivable,");
                    sb.Append("                0 CurrReceipt,");
                    sb.Append("                '0' Sel,");
                    sb.Append("                        (SELECT AMT.AGENT_MST_PK FROM AGENT_MST_TBL AMT WHERE AMT.AGENT_MST_PK = " + intAgentPk + ") CUST");
                    sb.Append("  FROM INV_AGENT_TBL INV,");
                    sb.Append("       JOB_CARD_TRN  JOB,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
                    sb.Append("       USER_MST_TBL          UMT");
                    sb.Append(" WHERE INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                    sb.Append("   AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
                    sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + "");
                    sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                    if (intAgentPk > 0)
                    {
                        sb.Append("   AND (JOB.DP_AGENT_MST_FK = " + intAgentPk + "  OR JOB.CB_AGENT_MST_FK = " + intAgentPk + " )");
                        sb.Append("   AND INV.AGENT_MST_FK = " + intAgentPk + " ");
                    }
                    if (InvRefNo.Length > 0)
                    {
                        sb.Append(" AND INV.INVOICE_REF_NO = '" + InvRefNo + "'");
                    }
                    if (intJobPk > 0)
                    {
                        sb.Append("           AND JOB.JOB_CARD_TRN_PK = " + intJobPk + "");
                    }
                    sb.Append(strCond);
                    sb.Append(") QRY");
                }
                sb.Append(" WHERE QRY.receivable > 0");
            }
            else
            {
                sb.Append("SELECT ROWNUM          SNO,");
                sb.Append("       INVPK,");
                sb.Append("       INVOICE_REF_NO,");
                sb.Append("       INVOICE_DATE,");
                sb.Append("       CURRENCY_MST_FK,");
                sb.Append("       CURRENCY_ID,");
                sb.Append("       QRY.NET_RECEIVABLE NET_PAYABLE,");
                sb.Append("       ROE,");
                sb.Append("       AMTINLOC,");
                sb.Append("       RECIEVED,");
                sb.Append("       RECEIVABLE,");
                sb.Append("       CURRRECEIPT,");
                sb.Append("       SEL,");
                sb.Append("       CUST");
                sb.Append("  FROM (SELECT DISTINCT INV.CONSOL_INVOICE_PK INVPK,");
                sb.Append("                        INV.INVOICE_REF_NO,");
                sb.Append("                        INV.INVOICE_DATE,");
                sb.Append("                        INV.CURRENCY_MST_FK,");
                sb.Append("                        CUMT.CURRENCY_ID,");
                sb.Append("                        INV.NET_RECEIVABLE,");
                sb.Append("                        (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                            INV.INVOICE_DATE)");
                sb.Append("                           FROM DUAL) ROE,");
                sb.Append("                        ROUND((INV.NET_RECEIVABLE) *");
                sb.Append("                              (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                  " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                                  INV.INVOICE_DATE)");
                sb.Append("                                 FROM DUAL),");
                sb.Append("                              2) AMTINLOC,");
                sb.Append("                        NVL((SELECT SUM(CLTTRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                              FROM COLLECTIONS_TBL     CLT,");
                sb.Append("                                   COLLECTIONS_TRN_TBL CLTTRN");
                sb.Append("                             WHERE CLT.COLLECTIONS_TBL_PK =");
                sb.Append("                                   CLTTRN.COLLECTIONS_TBL_FK");
                sb.Append("                               AND CLTTRN.FROM_INV_OR_CONSOL_INV = 3");
                sb.Append("                               AND CLTTRN.INVOICE_REF_NR =");
                sb.Append("                                   INV.INVOICE_REF_NO),");
                sb.Append("                            0) RECIEVED,");
                sb.Append("                        ROUND((INV.NET_RECEIVABLE *");
                sb.Append("                              (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                   " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                sb.Append("                                                   INV.INVOICE_DATE)");
                sb.Append("                                  FROM DUAL) - NVL((SELECT SUM(CLTTRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                                                      FROM COLLECTIONS_TBL     CLT,");
                sb.Append("                                                           COLLECTIONS_TRN_TBL CLTTRN");
                sb.Append("                                                     WHERE CLT.COLLECTIONS_TBL_PK =");
                sb.Append("                                                           CLTTRN.COLLECTIONS_TBL_FK");
                sb.Append("                                                       AND CLTTRN.FROM_INV_OR_CONSOL_INV = 3");
                sb.Append("                                                       AND CLTTRN.INVOICE_REF_NR =");
                sb.Append("                                                           INV.INVOICE_REF_NO),");
                sb.Append("                                                    0)),");
                sb.Append("                              2) RECEIVABLE,");
                sb.Append("                        0 CURRRECEIPT,");
                sb.Append("                        '0' SEL,");
                if (BizType == 2)
                {
                    sb.Append("                        (SELECT AMT.OPERATOR_MST_PK");
                    sb.Append("                           FROM OPERATOR_MST_TBL AMT");
                    sb.Append("                          WHERE AMT.OPERATOR_MST_PK = " + intAgentPk + ") CUST");
                }
                else
                {
                    sb.Append("                        (SELECT AMT.AIRLINE_MST_PK");
                    sb.Append("                           FROM AIRLINE_MST_TBL AMT");
                    sb.Append("                          WHERE AMT.AIRLINE_MST_PK = " + intAgentPk + ") CUST");
                }

                sb.Append("          FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append("               CONSOL_INVOICE_TRN_TBL INVFRT,");
                if (BizType == 2 & Process == 1)
                {
                    sb.Append(" JOB_CARD_TRN   JOB,");
                }
                else if (BizType == 2 & Process == 2)
                {
                    sb.Append(" JOB_CARD_TRN   JOB,");
                }
                else if (BizType == 1 & Process == 1)
                {
                    sb.Append(" JOB_CARD_TRN   JOB,");
                }
                else if (BizType == 1 & Process == 2)
                {
                    sb.Append(" JOB_CARD_TRN   JOB,");
                }
                sb.Append("               CURRENCY_TYPE_MST_TBL  CUMT,");
                sb.Append("               USER_MST_TBL           UMT");
                sb.Append("         WHERE INV.CONSOL_INVOICE_PK = INVFRT.CONSOL_INVOICE_FK");

                if (BizType == 2 & Process == 1)
                {
                    sb.Append("           AND INVFRT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                }
                else if (BizType == 2 & Process == 2)
                {
                    sb.Append("           AND INVFRT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                }
                else if (BizType == 1 & Process == 1)
                {
                    sb.Append("           AND INVFRT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                }
                else if (BizType == 1 & Process == 2)
                {
                    sb.Append("           AND INVFRT.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                }

                sb.Append("           AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk);
                sb.Append("           AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                if (intAgentPk > 0)
                {
                    sb.Append("           AND INV.SUPPLIER_MST_FK = " + intAgentPk);
                }
                sb.Append("           AND INV.BUSINESS_TYPE = " + BizType);
                sb.Append("           AND INV.PROCESS_TYPE = " + Process);
                sb.Append("           AND INV.CHK_INVOICE = 1");
                if (InvRefNo.Length > 0)
                {
                    sb.Append(" AND UPPER(INV.INVOICE_REF_NO) = UPPER('" + InvRefNo + "')");
                }
                if (intJobPk > 0)
                {
                    sb.Append("           AND INVFRT.JOB_CARD_FK = " + intJobPk + "");
                }
                sb.Append("        ) QRY");
                sb.Append(" WHERE QRY.RECEIVABLE > 0");
            }
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the saved data.
        /// </summary>
        /// <param name="intColPk">The int col pk.</param>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcess">The int process.</param>
        /// <param name="intColType">Type of the int col.</param>
        /// <param name="intBaseCurr">The int base curr.</param>
        /// <param name="intCollCurr">The int coll curr.</param>
        /// <returns></returns>
        public DataSet FetchSavedData(int intColPk, short intBizType, short intProcess, short intColType, int intBaseCurr, int intCollCurr)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("COLPK_IN", intColPk).Direction = ParameterDirection.Input;
                _with1.Add("BIZTYPE_IN", intBizType).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_IN", intProcess).Direction = ParameterDirection.Input;
                _with1.Add("COLTYPE_IN", intColType).Direction = ParameterDirection.Input;
                _with1.Add("CUR_IN", intBaseCurr).Direction = ParameterDirection.Input;
                _with1.Add("COLLCUR_IN", intCollCurr).Direction = ParameterDirection.Input;
                _with1.Add("HDR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("INV_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("MODE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_COLLECTION_PKG", "FETCH_AFTER_SAVE_AGT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the payment details.
        /// </summary>
        /// <param name="Mode">The mode.</param>
        /// <returns></returns>
        public DataSet FetchPaymentDetails(short Mode)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWf = new WorkFlow();

            strBuilder.Append(" select rownum,cm.collections_mode_trn_pk,");
            strBuilder.Append(" cm.collections_tbl_fk,");
            strBuilder.Append(" cm.receipt_mode,");
            strBuilder.Append(" decode(receipt_mode,1,'cheque',2,'Cash',3,'Bank Transfer') \"Mode\",");
            strBuilder.Append(" cm.cheque_number,");
            strBuilder.Append(" to_char(cm.cheque_date,'" + dateFormat + "') cheque_date, ");
            strBuilder.Append(" cm.bank_mst_fk,");
            strBuilder.Append(" CM.BANK_NAME,");
            strBuilder.Append(" CM.DEPOSITED_IN,");
            strBuilder.Append(" cm.currency_mst_fk,");
            strBuilder.Append(" cmt.currency_id,");
            if (Mode == 2)
            {
                strBuilder.Append(" '' Amount,");
            }
            else
            {
                strBuilder.Append("  cm.recd_amount Amount,");
            }
            strBuilder.Append(" cm.exchange_rate,");
            strBuilder.Append(" cm.recd_amount");
            strBuilder.Append("   from collections_mode_trn_tbl cm,currency_type_mst_tbl cmt where");
            if (Mode == 2)
            {
                strBuilder.Append(" 1=2 and");
            }
            strBuilder.Append(" cm.currency_mst_fk = cmt.currency_mst_pk");
            try
            {
                return objWf.GetDataSet(strBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Fetch"

        #region "To Fetch JobcardStatus"

        /// <summary>
        /// Fetches the collection.
        /// </summary>
        /// <param name="intJobPk">The int job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public object FetchCollection(int intJobPk = 0, short BizType = 0, short Process = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("select JOB_CARD_TRN_PK,");
                sb.Append("       JOBCARD_REF_NO,");
                if (Process == 1)
                {
                    sb.Append(" CB_DP_LOAD_AGENT AGENTTYPE");
                }
                else
                {
                    sb.Append(" CB_DP_LOAD_AGENT AGENTTYPE");
                }
                sb.Append("  From (select distinct JOB.JOB_CARD_TRN_PK,");
                sb.Append("                        JOB.JOBCARD_REF_NO,");
                sb.Append("                        NVL(INV.NET_INV_AMT,0)NET_INV_AMT,");
                sb.Append("                        NVL(clttrn.recd_amount_hdr_curr,0)recd_amount_hdr_curr,");
                sb.Append("                        (INV.NET_INV_AMT - NVL(SUM(DISTINCT clttrn.recd_amount_hdr_curr), 0))COLLECTION,");
                if (Process == 1)
                {
                    sb.Append(" INV.CB_DP_LOAD_AGENT");
                }
                else
                {
                    sb.Append(" INV.CB_DP_LOAD_AGENT");
                }
                sb.Append("          from INV_AGENT_TBL     INV,");
                sb.Append("               INV_AGENT_TRN_TBL INVFRT,");
                sb.Append("               JOB_CARD_TRN      JOB,");
                sb.Append("               JOB_TRN_FD        JOBFRT,");
                sb.Append("               collections_tbl           clt,");
                sb.Append("               collections_trn_tbl       clttrn");
                sb.Append("         where INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
                sb.Append("           AND clt.collections_tbl_pk = clttrn.collections_tbl_fk");
                sb.Append("           AND INV.INV_AGENT_PK = invfrt.inv_AGENT_fk");
                sb.Append("           AND clttrn.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("           AND JOB.JOB_CARD_TRN_PK = JOBFRT.JOB_CARD_TRN_FK");
                //sb.Append("           AND INVFRT.COST_FRT_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK")
                sb.Append("           AND clt.business_type ='" + BizType + "'");
                sb.Append("           AND clt.process_type = '" + Process + "'");
                sb.Append("           AND JOB.JOB_CARD_TRN_PK =" + intJobPk + "");
                sb.Append("         group By JOB.JOB_CARD_TRN_PK,");
                sb.Append("                  JOB.JOBCARD_REF_NO,");
                sb.Append("                  INV.NET_INV_AMT,");
                sb.Append("                  clttrn.recd_amount_hdr_curr,");
                if (Process == 1)
                {
                    sb.Append(" INV.CB_DP_LOAD_AGENT)");
                }
                else
                {
                    sb.Append(" INV.CB_DP_LOAD_AGENT)");
                }
                sb.Append(" where COLLECTION = 0");
                //If BizType = 1 Then
                //    sb.Replace("SEA", "AIR")
                //End If
                //If Process = 2 Then
                //    sb.Replace("EXP", "IMP")
                //End If
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the agtcollection.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public object fetchAgtcollection(string JOBPK = "", short BizType = 0, short Process = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT JOB.JOB_CARD_TRN_PK,");
                sb.Append("       INVAGT.INV_AGENT_PK,");
                if (Process == 1)
                {
                    sb.Append("       INVAGT.CB_DP_LOAD_AGENT AGENTTYPE");
                }
                else
                {
                    sb.Append("       INVAGT.CB_DP_LOAD_AGENT AGENTTYPE");
                }
                sb.Append("  FROM JOB_CARD_TRN JOB, INV_AGENT_TBL INVAGT");
                sb.Append("  WHERE JOB.JOB_CARD_TRN_PK = " + JOBPK);
                sb.Append("   AND INVAGT.JOB_CARD_FK(+) = JOB.JOB_CARD_TRN_PK");
                if (BizType == 1)
                {
                    sb.Replace("SEA", "AIR");
                }
                if (Process == 2)
                {
                    sb.Replace("EXP", "IMP");
                }
                return objWF.GetDataSet(sb.ToString());
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
        /// Checkstatuses the specified ds agt collection.
        /// </summary>
        /// <param name="dsAgtCollection">The ds agt collection.</param>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public object checkstatus(DataSet dsAgtCollection, int JobPk = 0, short BizType = 0, short Process = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 i = default(Int32);
            bool IsDPAgent = false;
            bool IsCBAgent = false;
            try
            {
                if (dsAgtCollection.Tables.Count > 0)
                {
                    if (dsAgtCollection.Tables[0].Rows.Count > 0)
                    {
                        for (i = 0; i <= dsAgtCollection.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty((dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"]).ToString()))
                            {
                                if (Convert.ToInt32(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"]) == 2)
                                {
                                    IsDPAgent = true;
                                }
                                else if (Convert.ToInt32(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"]) == 1)
                                {
                                    IsCBAgent = true;
                                }
                            }
                        }
                    }
                }

                sb.Append("select * FROM  JOB_CARD_TRN JOB");
                sb.Append("  where JOB.JOB_CARD_TRN_PK=" + JobPk);
                sb.Append("  AND JOB.COLLECTION_STATUS=1");
                sb.Append("  AND JOB.PAYEMENT_STATUS=1");
                if (Process == 1)
                {
                    sb.Append("  AND JOB.HBL_RELEASED_STATUS=1");
                }
                else
                {
                    sb.Append("  AND JOB.DO_STATUS=1");
                }
                if (IsDPAgent == true & IsCBAgent == true)
                {
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                    sb.Append("  AND JOB.DPAGENT_STATUS=1");
                }
                else if (IsDPAgent == true)
                {
                    sb.Append("  AND JOB.DPAGENT_STATUS=1");
                }
                else if (IsCBAgent == true)
                {
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                }

                if (BizType == 1)
                {
                    sb.Replace("SEA", "AIR");
                }
                if (Process == 2)
                {
                    sb.Replace("EXP", "IMP");
                    sb.Replace("DPAGENT_STATUS", "LOADAGENT_STATUS");
                }
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Updatejobcarddates the specified job pk.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public ArrayList updatejobcarddate(int JobPk = 0, short BizType = 0, short Process = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            try
            {
                updCmdUser.Transaction = TRAN;
                if (BizType == 2 & Process == 1)
                {
                    str = "UPDATE JOB_CARD_TRN  j SET ";
                    str += "   j.JOB_CARD_STATUS = 2, j.JOB_CARD_CLOSED_ON = SYSDATE";
                    str += " WHERE j.JOB_CARD_TRN_PK=" + JobPk;
                }
                else if (BizType == 2 & Process == 2)
                {
                    str = "UPDATE JOB_CARD_TRN  JA SET ";
                    str += "   JA.JOB_CARD_STATUS = 2, JA.JOB_CARD_CLOSED_ON =SYSDATE ";
                    str += " WHERE JA.JOB_CARD_TRN_PK=" + JobPk;
                }
                else if (BizType == 1 & Process == 1)
                {
                    str = "UPDATE JOB_CARD_TRN JAE SET ";
                    str += "   JAE.JOB_CARD_STATUS = 2, JAE.JOB_CARD_CLOSED_ON =SYSDATE";
                    str += " WHERE JAE.JOB_CARD_TRN_PK=" + JobPk;
                }
                else if (BizType == 1 & Process == 2)
                {
                    str = "UPDATE JOB_CARD_TRN JAI SET ";
                    str += "   JAI.JOB_CARD_STATUS = 2, JAI.JOB_CARD_CLOSED_ON =SYSDATE";
                    str += " WHERE  JAI.JOB_CARD_TRN_PK=" + JobPk;
                }

                var _with3 = updCmdUser;
                _with3.Connection = objWK.MyConnection;
                _with3.Transaction = TRAN;
                _with3.CommandType = CommandType.Text;
                _with3.CommandText = str;
                intIns = Convert.ToInt16(_with3.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("Protocol Generated Succesfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "To Fetch JobcardStatus"

        #region " Save"

        /// <summary>
        /// Saves the data.
        /// </summary>
        /// <param name="dsSave">The ds save.</param>
        /// <param name="CollectionNo">The collection no.</param>
        /// <param name="nLocationPk">The n location pk.</param>
        /// <param name="nEmpId">The n emp identifier.</param>
        /// <param name="NetAmt">The net amt.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="CrLimit">The cr limit.</param>
        /// <param name="CrLimitUsed">The cr limit used.</param>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <param name="BIZ">The biz.</param>
        /// <param name="PROCESS">The process.</param>
        /// <param name="CollType">Type of the coll.</param>
        /// <returns></returns>
        public int SaveData(DataSet dsSave, string CollectionNo, long nLocationPk, long nEmpId, double NetAmt, string Customer, double CrLimit, double CrLimitUsed, int JobPk, int ExType = 1,
        int BIZ = 1, int PROCESS = 1, int CollType = 1)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            int intSaveSucceeded = 0;
            OracleTransaction TRAN = null;
            int intPkValue = 0;
            int intChldCnt = 0;
            try
            {
                if (string.IsNullOrEmpty(Convert.ToString(CollectionNo)))
                {
                    CollectionNo = GenerateCollectionNo(nLocationPk, nEmpId, Convert.ToInt64(dsSave.Tables[0].Rows[0]["CREATED_BY_FK_IN"]), objWK);
                    if (Convert.ToString(CollectionNo) == "Protocol Not Defined.")
                    {
                        CollectionNo = "";
                        return -1;
                    }
                }
                CollRefNr = CollectionNo;
                var _with4 = dsSave.Tables[0].Rows[0];
                objWK.MyCommand.Connection = objWK.MyConnection;
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".COLLECTIONS_TBL_PKG.COLLECTIONS_TBL_INS";
                objWK.MyCommand.Parameters.Clear();
                objWK.MyCommand.Parameters.Add("PROCESS_TYPE_IN", _with4["PROCESS_TYPE_IN"]);
                objWK.MyCommand.Parameters.Add("BUSINESS_TYPE_IN", _with4["BUSINESS_TYPE_IN"]);
                objWK.MyCommand.Parameters.Add("COLLECTION_TYPE_IN", _with4["COLLECTION_TYPE_IN"]);
                objWK.MyCommand.Parameters.Add("COLLECTIONS_REF_NO_IN", Convert.ToString(CollectionNo));
                objWK.MyCommand.Parameters.Add("COLLECTIONS_DATE_IN", Convert.ToDateTime(_with4["COLLECTIONS_DATE_IN"]));
                objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with4["CURRENCY_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("REMARKS_IN", _with4["REMARKS_IN"]);
                objWK.MyCommand.Parameters.Add("CUSTOMER_MST_FK_IN", _with4["CUSTOMER_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("CREATED_BY_FK_IN", _with4["CREATED_BY_FK_IN"]);
                objWK.MyCommand.Parameters.Add("EXCH_RATE_TYPE_FK_IN", ExType);
                objWK.MyCommand.Parameters.Add("CONFIG_MST_PK_IN", _with4["CONFIG_MST_PK_IN"]);
                objWK.MyCommand.Parameters.Add("AGENT_MST_FK_IN", _with4["AGENT_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("RETURN_VALUE", _with4["CREATED_BY_FK_IN"]).Direction = ParameterDirection.Output;

                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;
                objWK.MyCommand.ExecuteNonQuery();
                intPkValue = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                PkVal = intPkValue;
                for (intChldCnt = 0; intChldCnt <= dsSave.Tables[1].Rows.Count - 1; intChldCnt++)
                {
                    objWK.MyCommand.Parameters.Clear();
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".COLLECTIONS_TBL_PKG.COLLECTIONS_TRN_TBL_INS";
                    var _with5 = dsSave.Tables[1].Rows[intChldCnt];
                    objWK.MyCommand.Parameters.Add("COLLECTIONS_TBL_FK_IN", intPkValue);
                    objWK.MyCommand.Parameters.Add("FROM_INV_OR_CONSOL_INV_IN", _with5["FROM_INV_OR_CONSOL_INV_IN"]);
                    objWK.MyCommand.Parameters.Add("INVOICE_REF_NR_IN", _with5["INVOICE_REF_NR_IN"]);
                    objWK.MyCommand.Parameters.Add("RECD_AMOUNT_HDR_CURR_IN", _with5["RECD_AMOUNT_HDR_CURR_IN"]);
                    objWK.MyCommand.Parameters.Add("EXCH_RATE_FLUCTUATION_IN", _with5["EXCH_RATE_FLUCTUATION_IN"]);
                    objWK.MyCommand.Parameters.Add("CUSTOMER_MST_FK_IN", dsSave.Tables[0].Rows[0]["CUSTOMER_MST_FK_IN"]);
                    //Added By PrakashChandra on 03/06/2008
                    intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
                }

                intChldCnt = 0;
                for (intChldCnt = 0; intChldCnt <= dsSave.Tables[2].Rows.Count - 1; intChldCnt++)
                {
                    objWK.MyCommand.Parameters.Clear();
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".COLLECTIONS_TBL_PKG.COLLECTIONS_MODE_TRN_TBL_INS";
                    var _with6 = dsSave.Tables[2].Rows[intChldCnt];
                    objWK.MyCommand.Parameters.Add("COLLECTIONS_TBL_FK_IN", intPkValue);
                    objWK.MyCommand.Parameters.Add("RECEIPT_MODE_IN", _with6["RECEIPT_MODE_IN"]);
                    objWK.MyCommand.Parameters.Add("CHEQUE_NUMBER_IN", _with6["CHEQUE_NUMBER_IN"]);
                    objWK.MyCommand.Parameters.Add("CHEQUE_DATE_IN", _with6["CHEQUE_DATE_IN"]);
                    objWK.MyCommand.Parameters.Add("BANK_MST_FK_IN", _with6["BANK_PK_IN"]);
                    objWK.MyCommand.Parameters.Add("BANK_NAME_IN", _with6["BANK_NAME_IN"]);
                    objWK.MyCommand.Parameters.Add("DEPOSITED_IN", _with6["DEPOSITED_IN"]);
                    objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with6["CURRENCY_MST_FK_IN"]);
                    objWK.MyCommand.Parameters.Add("RECD_AMOUNT_IN", _with6["RECD_AMOUNT_IN"]);
                    objWK.MyCommand.Parameters.Add("EXCHANGE_RATE_IN", _with6["EXCHANGE_RATE_IN"]);
                    objWK.MyCommand.Parameters.Add("RETURN_VALUE", _with6["CURRENCY_MST_FK_IN"]);
                    intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
                }
                if (intSaveSucceeded > 0)
                {
                    if (CrLimit > 0)
                    {
                        SaveCreditLimit(NetAmt, Customer, CrLimitUsed, TRAN);
                    }
                    TRAN.Commit();

                    if (intPkValue > 0)
                    {
                        if (Convert.ToBoolean(ConfigurationSettings.AppSettings["QFINGeneral"]) == true)
                        {
                            try
                            {
                                TRAN = objWK.MyConnection.BeginTransaction();
                                objWK.MyCommand.Transaction = TRAN;
                                objWK.MyCommand.Parameters.Clear();
                                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                                objWK.MyCommand.CommandText = objWK.MyUserName + ".ACCOUNTING_INTEGREATION_PKG.DATA_PUSH_FRT_COLLECTION_INS";
                                objWK.MyCommand.Parameters.Add("FRT_COLLECT_PK_IN", intPkValue).Direction = ParameterDirection.Input;
                                objWK.MyCommand.Parameters.Add("LOCAL_CUR_FK_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                                objWK.MyCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                                objWK.MyCommand.ExecuteNonQuery();
                                TRAN.Commit();
                            }
                            catch (Exception ex)
                            {
                            }
                        }
                    }

                    DataSet dsData = new DataSet();
                    Int32 NetRec = default(Int32);
                    Int32 Rec = default(Int32);
                    string Col_Status = null;
                    string ColRef = null;
                    ColRef = Fetch_Col_Ref_No(intPkValue);
                    objWK.MyCommand.Parameters.Clear();
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".COLLECTIONS_TBL_PKG.CHECK_AGENTCOL_DATA";
                    var _with7 = objWK.MyCommand.Parameters;
                    _with7.Add("COL_REF_IN", ColRef).Direction = ParameterDirection.Input;
                    _with7.Add("BIZ_TYPE_IN", BIZ).Direction = ParameterDirection.Input;
                    _with7.Add("PROCESS_IN", PROCESS).Direction = ParameterDirection.Input;
                    _with7.Add("COL_TYPE_IN", CollType).Direction = ParameterDirection.Input;
                    _with7.Add("COL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                    objWK.MyDataAdapter.Fill(dsData);
                    if ((dsData != null))
                    {
                        NetRec = Convert.ToInt32(Convert.ToString(dsData.Tables[0].Rows[0][0]) != null ? 0 : dsData.Tables[0].Rows[0][0]);
                        Rec = Convert.ToInt32(Convert.ToString(dsData.Tables[0].Rows[0][1]) != null ? 0 : dsData.Tables[0].Rows[0][1]);
                    }
                    if (NetRec == Rec)
                    {
                        Col_Status = "Agent Collection Done Against Invoice Int32 '" + ColRef + "'";
                    }
                    else
                    {
                        Col_Status = "Part Agent Collection Done Against Invoice Int32 '" + ColRef + "'";
                    }
                    objWK.MyCommand.Parameters.Clear();
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
                    var _with8 = objWK.MyCommand;
                    _with8.Parameters.Add("Key_fk_in", JobPk).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("BIZ_TYPE_IN", dsSave.Tables[0].Rows[0]["BUSINESS_TYPE_IN"]).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("PROCESS_IN", dsSave.Tables[0].Rows[0]["PROCESS_TYPE_IN"]).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("status_in", Col_Status).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("locationfk_in", nLocationPk).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("OnStatus_in", "COL-INS").Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("pkStatus_in", "O").Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("flagInsUpd_in", Convert.ToString(CollectionNo)).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("CreatedUser_in", dsSave.Tables[0].Rows[0]["CREATED_BY_FK_IN"]).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with8.ExecuteNonQuery();
                }
                else
                {
                    TRAN.Rollback();
                }
                return intSaveSucceeded;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        /// <summary>
        /// Fetch_s the col_ ref_ no.
        /// </summary>
        /// <param name="ColPk">The col pk.</param>
        /// <returns></returns>
        public string Fetch_Col_Ref_No(Int32 ColPk)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                string sqlstr = null;
                string ColRefNo = null;
                sqlstr = "select ctrn.invoice_ref_nr from collections_trn_tbl ctrn where ctrn.collections_tbl_fk='" + ColPk + "'";
                ColRefNo = objWF.ExecuteScaler(sqlstr);
                return ColRefNo;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Save"

        #region " Enhance Search Function for Job Card "

        /// <summary>
        /// Fetch_s the job_ for_ collection.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Job_For_Collection(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand cmd = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSearchIn = "";
            short intBizType = 0;
            short intProcess = 0;
            int intParty = 0;
            long intLocPk = 0;
            string strReq = null;
            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSearchIn = arr[1];
            if (arr.Length > 2)
                intBizType = Convert.ToInt16(arr[2]);
            if (arr.Length > 3)
                intProcess = Convert.ToInt16(arr[3]);
            if (arr.Length > 4)
            {
                if (!string.IsNullOrEmpty(Convert.ToString(arr[4])))
                {
                    intParty = Convert.ToInt32(arr[4]);
                }
            }

            if (arr.Length > 5)
                intLocPk = Convert.ToInt64(arr[5]);

            try
            {
                objWF.OpenConnection();
                cmd.Connection = objWF.MyConnection;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = objWF.MyUserName + ".EN_JOB_FOR_COLLECTION.GET_JOB_COLL";
                var _with9 = cmd.Parameters;
                _with9.Add("SEARCH_IN", getDefault(strSearchIn, "")).Direction = ParameterDirection.Input;
                _with9.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with9.Add("BUSINESS_TYPE_IN", intBizType).Direction = ParameterDirection.Input;
                _with9.Add("PROCESS_IN", intProcess).Direction = ParameterDirection.Input;
                _with9.Add("PARTY_IN", intParty).Direction = ParameterDirection.Input;
                _with9.Add("LOC_IN", intLocPk).Direction = ParameterDirection.Input;
                _with9.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                cmd.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                cmd.ExecuteNonQuery();
                strReturn = Convert.ToString(cmd.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                cmd.Connection.Close();
            }
        }

        #endregion " Enhance Search Function for Job Card "

        #region " Protocol Reference Int32"

        /// <summary>
        /// Generates the collection no.
        /// </summary>
        /// <param name="nLocationId">The n location identifier.</param>
        /// <param name="nEmployeeId">The n employee identifier.</param>
        /// <param name="nCreatedBy">The n created by.</param>
        /// <param name="ObjWK">The object wk.</param>
        /// <returns></returns>
        public string GenerateCollectionNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null)
        {
            return GenerateProtocolKey("COLLECTIONS", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, ObjWK);
        }

        #endregion " Protocol Reference Int32"

        #region "Get DataSet for Report"

        /// <summary>
        /// Gets the rep head ds.
        /// </summary>
        /// <param name="colpk">The colpk.</param>
        /// <returns></returns>
        public DataSet getRepHeadDS(int colpk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder strSql = new StringBuilder();

            strSql.Append(" SELECT COL.PROCESS_TYPE PROTYPE, COL.BUSINESS_TYPE BIZTYPE, COL.COLLECTIONS_TBL_PK COLPK,");
            strSql.Append(" COL.COLLECTIONS_REF_NO COLREFNO, TO_CHAR(COL.COLLECTIONS_DATE,'" + dateFormat + "') COLDATE,");
            strSql.Append(" LOC.OFFICE_NAME CMPNM, LOC.ADDRESS_LINE1 CMPADD1, LOC.ADDRESS_LINE2 CMPADD2,");
            strSql.Append(" LOC.ADDRESS_LINE3 CMPADD3, LOC.CITY CMPCITY, LOC.ZIP CMPZIP, CNT.COUNTRY_NAME CMPCNT,");

            //by thiyagarajan for display address in  report on 27/2/08
            strSql.Append(" ('PHONE :'||LOC.TELE_PHONE_NO||' '||'FAX :'||LOC.FAX_NO) PHONE,");
            strSql.Append("  (select corp.home_page from corporate_mst_tbl corp where corp.corporate_mst_pk=loc.corporate_mst_fk) URL,");
            //end

            strSql.Append(" AMT.AGENT_NAME CUSTNM, CURR.CURRENCY_ID CURRNM, SUM(COLTRN.RECD_AMOUNT_HDR_CURR) COLAMT");
            strSql.Append(" FROM COLLECTIONS_TBL COL, COLLECTIONS_TRN_TBL COLTRN, AGENT_MST_TBL         AMT, CURRENCY_TYPE_MST_TBL CURR,");
            strSql.Append(" COUNTRY_MST_TBL CNT, USER_MST_TBL USMST, LOCATION_MST_TBL LOC");
            strSql.Append(" WHERE COL.COLLECTIONS_TBL_PK = COLTRN.COLLECTIONS_TBL_FK(+) AND COL.AGENT_MST_FK = AMT.AGENT_MST_PK(+)");
            strSql.Append(" AND LOC.COUNTRY_MST_FK = CNT.COUNTRY_MST_PK(+) AND COL.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)");
            strSql.Append(" AND COL.CREATED_BY_FK = USMST.USER_MST_PK(+) AND USMST.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
            strSql.Append(" AND COL.COLLECTIONS_TBL_PK = '" + colpk + "'");
            strSql.Append(" GROUP BY loc.corporate_mst_fk,COL.PROCESS_TYPE, COL.BUSINESS_TYPE, COL.COLLECTIONS_TBL_PK, COL.COLLECTIONS_REF_NO,");
            strSql.Append(" COL.COLLECTIONS_DATE, LOC.OFFICE_NAME, LOC.ADDRESS_LINE1, LOC.ADDRESS_LINE2, LOC.ADDRESS_LINE3,");
            strSql.Append(" LOC.CITY, LOC.ZIP, CNT.COUNTRY_NAME, AMT.AGENT_NAME, CURR.CURRENCY_ID,loc.tele_phone_no,loc.fax_no");
            try
            {
                return ObjWk.GetDataSet(strSql.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Gets the mod det ds.
        /// </summary>
        /// <param name="colpk">The colpk.</param>
        /// <returns></returns>
        public DataSet getModDetDs(int colpk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder strSql = new StringBuilder();

            strSql.Append("SELECT DECODE(COLMOD.RECEIPT_MODE,1,'CHEQUE',2,'CASH',3,'BANK TRANSFER',4,'DD',5,'PO') \"Mode\", COLMOD.COLLECTIONS_TBL_FK COLMODFK, COLMOD.CHEQUE_NUMBER CHQNO,");
            strSql.Append(" TO_CHAR(COLMOD.CHEQUE_DATE,'" + dateFormat + "') CHQDT, CURR.CURRENCY_ID CURRNM,");
            strSql.Append(" round((COLMOD.RECD_AMOUNT / COLMOD.EXCHANGE_RATE),2) RECAMT, COLMOD.EXCHANGE_RATE ROE, COLMOD.RECD_AMOUNT LOCAMT,");
            strSql.Append(" COLMOD.Bank_Name");
            strSql.Append(" FROM COLLECTIONS_MODE_TRN_TBL COLMOD, CURRENCY_TYPE_MST_TBL CURR");
            strSql.Append(" WHERE COLMOD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)");
            strSql.Append(" AND COLMOD.COLLECTIONS_TBL_FK = " + colpk);
            try
            {
                return ObjWk.GetDataSet(strSql.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Gets the trans det ds.
        /// </summary>
        /// <param name="COLPK_IN">The colp k_ in.</param>
        /// <param name="CUR_IN">The cu r_ in.</param>
        /// <param name="ColType">Type of the col.</param>
        /// <returns></returns>
        public DataSet getTransDetDs(int COLPK_IN, int CUR_IN, int ColType)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (ColType == 1)
            {
                sb.Append("SELECT DISTINCT '1' SNO,");
                sb.Append("                        INV.CONSOL_INVOICE_PK INV_AGENT_SEA_EXP_PK,");
                sb.Append("                        INV.INVOICE_REF_NO,");
                sb.Append("                        TO_CHAR(INV.INVOICE_DATE,DATEFORMAT) INVOICE_DATE,");
                sb.Append("                        INV.CURRENCY_MST_FK,");
                sb.Append("                        CUMT.CURRENCY_ID,");
                sb.Append("                        INV.NET_RECEIVABLE RECAMT,");
                sb.Append("                        (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                            " + CUR_IN + ",");
                sb.Append("                                            CLN.COLLECTIONS_DATE)");
                sb.Append("                           FROM DUAL) ROE,");
                sb.Append("                        ROUND((INV.NET_RECEIVABLE) *");
                sb.Append("                              (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                  " + CUR_IN + ",");
                sb.Append("                                                  CLN.COLLECTIONS_DATE)");
                sb.Append("                                 FROM DUAL),");
                sb.Append("                              2) AMTINLOC,");
                sb.Append("                        (SELECT SUM(NVL(CRN.RECD_AMOUNT_HDR_CURR, 0))");
                sb.Append("                           FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("                          WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("                            AND CRN.FROM_INV_OR_CONSOL_INV = 3) RECIEVED,");
                sb.Append("                        ");
                sb.Append("                        ROUND(NVL(INV.NET_RECEIVABLE *");
                sb.Append("                                  (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                      " + CUR_IN + ",");
                sb.Append("                                                      CLN.COLLECTIONS_DATE)");
                sb.Append("                                     FROM DUAL) -");
                sb.Append("                                  (SELECT SUM(NVL(CRN.RECD_AMOUNT_HDR_CURR,");
                sb.Append("                                                  0))");
                sb.Append("                                     FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("                                    WHERE CRN.INVOICE_REF_NR =");
                sb.Append("                                          INV.INVOICE_REF_NO");
                sb.Append("                                      AND CRN.FROM_INV_OR_CONSOL_INV = 3),");
                sb.Append("                                  0),");
                sb.Append("                              2) RECEIVABLE,");
                sb.Append("                        ");
                sb.Append("                        NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) CURRRECEIPT,");
                sb.Append("                        '1' SEL");
                sb.Append("          FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append("               CONSOL_INVOICE_TRN_TBL INVFRT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL     CUMT,");
                sb.Append("               COLLECTIONS_TBL           CLN,");
                sb.Append("               COLLECTIONS_TRN_TBL       CTRN");
                sb.Append("         WHERE INV.CONSOL_INVOICE_PK = INVFRT.CONSOL_INVOICE_FK");
                sb.Append("           AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("           AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN);
                sb.Append("           AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
            }
            else
            {
                sb.Append("SELECT CTRN.COLLECTIONS_TBL_FK COLFK,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE,'" + dateFormat + "') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID, INV.NET_INV_AMT RECAMT,");
                //sb.Append("       round((CTRN.RECD_AMOUNT_HDR_CURR *")
                //sb.Append("             (select get_ex_rate(inv.currency_mst_fk,")
                //sb.Append("                                  " & CUR_IN & ",")
                //sb.Append("                                  inv.invoice_date)")
                //sb.Append("                 from dual)),")
                //sb.Append("             2) RECAMT,")
                sb.Append("       (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("          from dual) ROE,");
                sb.Append("       round((INV.NET_INV_AMT) * (case");
                sb.Append("               when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                       from dual) = 0 then");
                sb.Append("                1");
                sb.Append("               else");
                sb.Append("                (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                   from dual)");
                sb.Append("             end),");
                sb.Append("             2) AmtinLoc,");
                sb.Append("       (select sum(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          from COLLECTIONS_TRN_TBL CRN");
                sb.Append("         where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
                sb.Append("       round(nvl(inv.net_inv_amt * (select get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                                       " + CUR_IN + ",");
                sb.Append("                                                       CLN.COLLECTIONS_DATE)");
                sb.Append("                                      from dual) -");
                sb.Append("                 (select sum(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    from COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) receivable,");
                sb.Append("       nvl(CTRN.RECD_AMOUNT_HDR_CURR, 0) CurrReceipt");
                sb.Append("  FROM INV_AGENT_TBL     INV,");
                sb.Append("       Inv_AGENT_Trn_Tbl INVFRT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL    CUMT,");
                sb.Append("       COLLECTIONS_TBL          CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL      CTRN");
                sb.Append(" WHERE INV.INV_AGENT_PK = INVFRT.INV_AGENT_FK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + "");
                sb.Append("   AND CLN.PROCESS_TYPE = 1");
                sb.Append("   AND CLN.BUSINESS_TYPE = 2");
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append(" UNION ");
                sb.Append("SELECT CTRN.COLLECTIONS_TBL_FK COLFK,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE,'" + dateFormat + "') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID, INV.NET_INV_AMT RECAMT,");
                //sb.Append("       round((CTRN.RECD_AMOUNT_HDR_CURR *")
                //sb.Append("             (select get_ex_rate(inv.currency_mst_fk,")
                //sb.Append("                                  " & CUR_IN & ",")
                //sb.Append("                                  inv.invoice_date)")
                //sb.Append("                 from dual)),")
                //sb.Append("             2) RECAMT,")
                sb.Append("       (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("          from dual) ROE,");
                sb.Append("       round((INV.NET_INV_AMT) * (case");
                sb.Append("               when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                       from dual) = 0 then");
                sb.Append("                1");
                sb.Append("               else");
                sb.Append("                (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                   from dual)");
                sb.Append("             end),");
                sb.Append("             2) AmtinLoc,");
                sb.Append("       (select sum(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          from COLLECTIONS_TRN_TBL CRN");
                sb.Append("         where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
                sb.Append("       round(nvl(inv.net_inv_amt * (select get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                                       " + CUR_IN + ",");
                sb.Append("                                                       CLN.COLLECTIONS_DATE)");
                sb.Append("                                      from dual) -");
                sb.Append("                 (select sum(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    from COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) receivable,");
                sb.Append("       nvl(CTRN.RECD_AMOUNT_HDR_CURR, 0) CurrReceipt");
                sb.Append("  FROM INV_AGENT_TBL     INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVFRT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL    CUMT,");
                sb.Append("       COLLECTIONS_TBL          CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL      CTRN");
                sb.Append(" WHERE INV.INV_AGENT_PK = INVFRT.INV_AGENT_FK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND CLN.PROCESS_TYPE = 1");
                sb.Append("   AND CLN.BUSINESS_TYPE = 1");
                sb.Append("   AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + "");
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append(" UNION ");
                sb.Append("SELECT CTRN.COLLECTIONS_TBL_FK COLFK,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE,'" + dateFormat + "') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID, INV.NET_INV_AMT RECAMT,");
                //sb.Append("       round((CTRN.RECD_AMOUNT_HDR_CURR *")
                //sb.Append("             (select get_ex_rate(inv.currency_mst_fk,")
                //sb.Append("                                  " & CUR_IN & ",")
                //sb.Append("                                  CLN.COLLECTIONS_DATE)")
                //sb.Append("                 from dual)),")
                //sb.Append("             2) RECAMT,")
                sb.Append("       (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("          from dual) ROE,");
                sb.Append("       round((INV.NET_INV_AMT) * (case");
                sb.Append("               when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                       from dual) = 0 then");
                sb.Append("                1");
                sb.Append("               else");
                sb.Append("                (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                   from dual)");
                sb.Append("             end),");
                sb.Append("             2) AmtinLoc,");
                sb.Append("       (select sum(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          from COLLECTIONS_TRN_TBL CRN");
                sb.Append("         where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
                sb.Append("       round(nvl(inv.net_inv_amt * (select get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                                       " + CUR_IN + ",");
                sb.Append("                                                       CLN.COLLECTIONS_DATE)");
                sb.Append("                                      from dual) -");
                sb.Append("                 (select sum(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    from COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) receivable,");
                sb.Append("       nvl(CTRN.RECD_AMOUNT_HDR_CURR, 0) CurrReceipt");
                sb.Append("  FROM INV_AGENT_TBL     INV,");
                sb.Append("       Inv_AGENT_Trn_Tbl INVFRT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL    CUMT,");
                sb.Append("       COLLECTIONS_TBL          CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL      CTRN");
                sb.Append(" WHERE INV.INV_AGENT_PK = invfrt.inv_AGENT_fk");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.PROCESS_TYPE = 2");
                sb.Append("   AND CLN.BUSINESS_TYPE = 2");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + "");
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append(" UNION ");
                sb.Append("SELECT CTRN.COLLECTIONS_TBL_FK COLFK,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE,'" + dateFormat + "') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID, INV.NET_INV_AMT RECAMT,");
                //sb.Append("       round((CTRN.RECD_AMOUNT_HDR_CURR *")
                //sb.Append("             (select get_ex_rate(inv.currency_mst_fk,")
                //sb.Append("                                  " & CUR_IN & ",")
                //sb.Append("                                  inv.invoice_date)")
                //sb.Append("                 from dual)),")
                //sb.Append("             2) RECAMT,")
                sb.Append("       (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("          from dual) ROE,");
                sb.Append("       round((INV.NET_INV_AMT) * (case");
                sb.Append("               when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                       from dual) = 0 then");
                sb.Append("                1");
                sb.Append("               else");
                sb.Append("                (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                   from dual)");
                sb.Append("             end),");
                sb.Append("             2) AmtinLoc,");
                sb.Append("       (select sum(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          from COLLECTIONS_TRN_TBL CRN");
                sb.Append("         where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
                sb.Append("       round(nvl(inv.net_inv_amt * (select get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                                       " + CUR_IN + ",");
                sb.Append("                                                       CLN.COLLECTIONS_DATE)");
                sb.Append("                                      from dual) -");
                sb.Append("                 (select sum(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    from COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) receivable,");
                sb.Append("       nvl(CTRN.RECD_AMOUNT_HDR_CURR, 0) CurrReceipt");
                sb.Append("  FROM INV_AGENT_TBL     INV,");
                sb.Append("       Inv_AGENT_Trn_Tbl INVFRT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL    CUMT,");
                sb.Append("       COLLECTIONS_TBL          CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL      CTRN");
                sb.Append(" WHERE INV.INV_AGENT_PK = invfrt.inv_AGENT_fk");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + "");
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("   AND CLN.PROCESS_TYPE = 2");
                sb.Append("   AND CLN.BUSINESS_TYPE = 1");
            }

            try
            {
                return ObjWk.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Get DataSet for Report"

        #region "Credit Limit"

        /// <summary>
        /// Fetches the customer credit amt.
        /// </summary>
        /// <param name="CustName">Name of the customer.</param>
        /// <returns></returns>
        public double FetchCustCreditAmt(string CustName)
        {
            string Strsql = null;
            double CreditAmt = 0;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = "select c.credit_limit from Customer_Mst_Tbl c where c.customer_name in('" + CustName + "')";
                CreditAmt = Convert.ToDouble(getDefault(ObjWF.ExecuteScaler(Strsql), 0));
                return CreditAmt;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Added By prakash Chandra on 2/12/2008 for pts: credit limit
        /// <summary>
        /// Fetchcols the customer credit amt.
        /// </summary>
        /// <param name="CustName">Name of the customer.</param>
        /// <returns></returns>
        public double FetchcolCustCreditAmt(string CustName)
        {
            string Strsql = null;
            double CreditAmt = 0;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = "select c.credit_limit from Customer_Mst_Tbl c where c.customer_id in('" + CustName + "')";
                CreditAmt = Convert.ToDouble(getDefault(ObjWF.ExecuteScaler(Strsql), 0));
                return CreditAmt;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Saves the credit limit.
        /// </summary>
        /// <param name="NetAmt">The net amt.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="CrLimitUsed">The cr limit used.</param>
        /// <param name="TRAN">The tran.</param>
        public void SaveCreditLimit(double NetAmt, string Customer, double CrLimitUsed, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand cmd = new OracleCommand();
            string strSQL = null;
            double temp = 0;
            //temp = CrLimitUsed - NetAmt
            temp = NetAmt + CrLimitUsed;
            try
            {
                cmd.CommandType = CommandType.Text;
                cmd.Connection = TRAN.Connection;
                cmd.Transaction = TRAN;

                cmd.Parameters.Clear();
                strSQL = "update customer_mst_tbl a set a.credit_limit_used = " + temp;
                strSQL = strSQL + " where a.customer_name in ('" + Customer + "')";
                cmd.CommandText = strSQL;
                cmd.ExecuteNonQuery();
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Converts to base currency.
        /// </summary>
        /// <param name="intBaseCurrPk">The int base curr pk.</param>
        /// <param name="intBaseDate">The int base date.</param>
        /// <param name="BaseCurrency">The base currency.</param>
        /// <param name="extype">The extype.</param>
        /// <returns></returns>
        public double ConvertToBaseCurrency(int intBaseCurrPk, System.DateTime intBaseDate, Int32 BaseCurrency = 0, Int32 extype = 1)
        {
            StringBuilder strSql = new StringBuilder();
            double RateOfExchange = 0;
            WorkFlow ObjWF = new WorkFlow();
            //" & intBaseCurrPk & "
            try
            {
                if (extype == 1)
                {
                    strSql.Append("select  GET_EX_RATE(" + intBaseCurrPk + "," + BaseCurrency + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL");
                }
                else
                {
                    strSql.Append("select  GET_EX_RATE1(" + intBaseCurrPk + "," + BaseCurrency + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )," + extype + ") FROM DUAL");
                }
                RateOfExchange = Convert.ToDouble(ObjWF.ExecuteScaler(strSql.ToString()));
                return RateOfExchange;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            ///*
            //            #Region " Fetch base currency Exchange rate export"
            //        Public Overloads Function FetchROE(ByVal baseCurrency As Int64, ByVal todate As String) As DataSet
            //            Dim strSQL As StringBuilder = New StringBuilder
            //            Dim objWF As New WorkFlow
            //            Try

            //                strSQL.Append(vbCrLf & "SELECT")
            //                strSQL.Append(vbCrLf & "    CURR.CURRENCY_MST_PK,")
            //                strSQL.Append(vbCrLf & "    CURR.CURRENCY_ID,")
            //                'strSQL.Append(vbCrLf & "    ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK,(select c.currency_mst_fk from corporate_mst_tbl c),round(sysdate - .5)),6) AS ROE")
            //                strSQL.Append(vbCrLf & "    ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK,(select c.currency_mst_fk from corporate_mst_tbl c),to_date('" & todate & "',dateformat)),6) as ROE ") ')),6) AS ROE")
            //                strSQL.Append(vbCrLf & "FROM")
            //                strSQL.Append(vbCrLf & "    CURRENCY_TYPE_MST_TBL CURR")
            //                strSQL.Append(vbCrLf & "WHERE")
            //                strSQL.Append(vbCrLf & "    CURR.ACTIVE_FLAG = 1")

            //                Return objWF.GetDataSet(strSQL.ToString)
            //            Catch sqlExp As OracleException
            //                ErrorMessage = sqlExp.Message
            //                Throw sqlExp
            //            Catch exp As Exception
            //                ErrorMessage = exp.Message
            //                Throw exp
            //            End Try

            //        End Function
            //#End Region
            //*/
        }

        #endregion "Credit Limit"

        #region "Fetch Customer"

        /// <summary>
        /// Fetch_s the customer.
        /// </summary>
        /// <param name="InvRefNo">The inv reference no.</param>
        /// <returns></returns>
        public DataSet Fetch_Cust(string InvRefNo)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("select cmt.customer_id from ");
                strQuery.Append("consol_invoice_tbl cit ,");
                strQuery.Append("customer_mst_tbl cmt ");
                strQuery.Append("where ");
                strQuery.Append("cit.customer_mst_fk=cmt.customer_mst_pk(+)");
                strQuery.Append("and cit.invoice_ref_no= '" + InvRefNo + "'");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Customer"

        #region "Fetch Currency"

        /// <summary>
        /// Gets the curr pk.
        /// </summary>
        /// <param name="InvRefno">The inv refno.</param>
        /// <returns></returns>
        public DataSet GetCurrPK(string InvRefno)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append(" SELECT CURR.CURRENCY_MST_PK CURRPK ,CURR.CURRENCY_ID CURRID FROM CONSOL_INVOICE_TBL CON,CURRENCY_TYPE_MST_TBL CURR WHERE ");
                strQuery.Append(" CURR.CURRENCY_MST_PK=CON.CURRENCY_MST_FK AND CON.INVOICE_REF_NO LIKE '" + InvRefno + "' ");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the currency.
        /// </summary>
        /// <param name="CurrencyPK">The currency pk.</param>
        /// <param name="CurrencyID">The currency identifier.</param>
        /// <param name="CurrencyName">Name of the currency.</param>
        /// <param name="ActiveOnly">if set to <c>true</c> [active only].</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <param name="COLDate">The col date.</param>
        /// <param name="DDLCurrency">The DDL currency.</param>
        /// <returns></returns>
        public DataSet FetchCurrency(Int16 CurrencyPK = 0, string CurrencyID = "", string CurrencyName = "", bool ActiveOnly = true, Int16 ExType = 1, string COLDate = "", string DDLCurrency = "")
        {
            string strSQL = null;
            strSQL = " select ' ' CURRENCY_ID,";
            strSQL = strSQL + "' ' CURRENCY_NAME, ";
            strSQL = strSQL + "0 CURRENCY_MST_PK ";
            strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL ";
            strSQL = strSQL + " UNION ";
            strSQL = strSQL + " SELECT C.CURRENCY_ID, C.CURRENCY_NAME, C.CURRENCY_MST_PK ";
            strSQL = strSQL + " FROM CURRENCY_TYPE_MST_TBL C";
            strSQL = strSQL + " WHERE C.CURRENCY_MST_PK=" + DDLCurrency;
            strSQL = strSQL + " UNION ";
            strSQL = strSQL + "Select CMT.CURRENCY_ID, ";
            strSQL = strSQL + "CMT.CURRENCY_NAME,";
            strSQL = strSQL + "CMT.CURRENCY_MST_PK ";
            strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL CMT , EXCHANGE_RATE_TRN EXC  Where 1=1 ";
            if (CurrencyPK > 0)
            {
                strSQL = strSQL + " And CMT.CURRENCY_MST_PK =" + Convert.ToString(CurrencyPK);
            }
            strSQL = strSQL + " AND EXC.CURRENCY_MST_BASE_FK = " + DDLCurrency;
            strSQL = strSQL + " AND EXC.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK ";
            strSQL = strSQL + " AND " + DDLCurrency + " <> CMT.CURRENCY_MST_PK";
            strSQL = strSQL + " AND EXC.EXCHANGE_RATE IS NOT NULL ";
            strSQL = strSQL + " AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK ";
            strSQL = strSQL + " AND EXC.VOYAGE_TRN_FK IS NULL ";
            if (string.IsNullOrEmpty(COLDate))
            {
                strSQL = strSQL + " AND ROUND(SYSDATE-0.5) Between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
                //nvl(TO_DATE,NULL_DATE_FORMAT) "
            }
            else
            {
                strSQL = strSQL + " AND TO_DATE(' " + Convert.ToDateTime(COLDate).ToString("dd/MM/yyyy") + "','" + dateFormat + "') between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
                //nvl(TO_DATE,NULL_DATE_FORMAT) "
            }
            if (ActiveOnly)
            {
                strSQL = strSQL + " And CMT.Active_Flag = 1  ";
            }
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch Currency"

        #region " Fetch Exchange Rate Bassed On Exchange Rate Type -Hidden"

        /// <summary>
        /// Fetches the exch type roe.
        /// </summary>
        /// <param name="baseCurrency">The base currency.</param>
        /// <param name="ExType">Type of the ex.</param>
        /// <param name="Coldate">The coldate.</param>
        /// <returns></returns>
        public DataSet FetchExchTypeROE(Int64 baseCurrency, Int64 ExType = 1, string Coldate = "")
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                Coldate = Coldate.Split(' ')[0];
            }
            catch (Exception ex)
            {
            }
            try
            {
                strBuilder.Append(" SELECT DISTINCT");
                strBuilder.Append(" CMT.CURRENCY_MST_PK,");
                strBuilder.Append(" CMT.CURRENCY_ID,");
                if (ExType == 3)
                {
                    strBuilder.Append(" ROUND(GET_EX_RATE1( ");
                    strBuilder.Append("CMT.CURRENCY_MST_PK, " + baseCurrency + ",ROUND(TO_DATE(' " + Coldate + "',dateFormat))," + ExType + "),6) AS ROE");
                }
                else
                {
                    strBuilder.Append(" ROUND(GET_EX_RATE( ");
                    strBuilder.Append("CMT.CURRENCY_MST_PK, " + baseCurrency + ",ROUND(TO_DATE(' " + Coldate + "',dateFormat))),6) AS ROE");
                }
                strBuilder.Append(" FROM");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CMT , EXCHANGE_RATE_TRN EXC");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" CMT.ACTIVE_FLAG = 1");
                strBuilder.Append(" AND EXC.CURRENCY_MST_FK=CMT.CURRENCY_MST_PK ");
                strBuilder.Append(" AND EXC.CURRENCY_MST_BASE_FK =" + baseCurrency);
                //HttpContext.Current.Session("CURRENCY_MST_PK"))
                strBuilder.Append(" AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK");
                strBuilder.Append(" AND EXC.VOYAGE_TRN_FK IS NULL");
                return objWF.GetDataSet(strBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion " Fetch Exchange Rate Bassed On Exchange Rate Type -Hidden"

        #region "FetchPK"

        /// <summary>
        /// Fetches the curr pk saved.
        /// </summary>
        /// <param name="ColPk">The col pk.</param>
        /// <returns></returns>
        public int FetchCurrPkSaved(int ColPk)
        {
            string Strsql = null;
            int CurrPk = 0;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = "select c.currency_mst_fk from collections_tbl c where c.collections_tbl_pk in ('" + ColPk + "')";
                CurrPk = Convert.ToInt32(ObjWF.ExecuteScaler(Strsql));
                return CurrPk;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchPK"

        #region "Fetch MBL PKs"

        /// <summary>
        /// Fetches the MBL nr.
        /// </summary>
        /// <param name="InvPK">The inv pk.</param>
        /// <param name="Biztype">The biztype.</param>
        /// <returns></returns>
        public DataSet FetchMBLNr(int InvPK, int Biztype)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with10 = objWF.MyCommand.Parameters;
                _with10.Add("INVPK_IN", InvPK).Direction = ParameterDirection.Input;
                _with10.Add("BIZTYPE_IN", Biztype).Direction = ParameterDirection.Input;
                _with10.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                _with10.Add("MBL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_COLLECTION_PKG", "FETCH_MBL_PK");
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

        #endregion "Fetch MBL PKs"
    }
}