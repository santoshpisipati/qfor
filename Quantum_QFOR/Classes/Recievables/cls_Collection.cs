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

//Option Strict On
// ERROR: Not supported in C#: OptionDeclaration

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
   	public class clsCollection : CommonFeatures
    {
        public int PkVal;
        //Snigdharani - 05/12/2008
        public string CollRefNr;

        #region " Fetch"
        public DataSet FetchData(short BizType, short Process, int intCustomerFk, string InvRefNo, int intJobPk, int intBaseCurrPk, long lngLocPk, string strFromDt = "", string strToDt = "", int ExType = 1,
        string ConInvPK = "")
        {

            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            // intBaseCurrPk replaced by SESSION("CURRENCY_MST_PK") 
            //modifying by thiyagarajan on 5/3/09 after discussing with Domain

            //Added by rabbani on 22/11/06 To Exclude zero receivable from the  query
            strBuilder.Append(" SELECT  ");
            strBuilder.Append(" SNO, ");
            strBuilder.Append(" INVPK, INVOICE_REF_NO, ");
            strBuilder.Append(" INVOICE_DATE, CURRENCY_MST_FK, ");
            strBuilder.Append(" CURRENCY_ID,  ");
            strBuilder.Append(" NET_PAYABLE, ROE, AMTINLOC, ");
            strBuilder.Append(" 0 TDS_PERCENTAGE, ");
            strBuilder.Append(" 0 TDS_AMOUNT, ");
            strBuilder.Append(" AMTINLOC RECEIVABLE, ");
            strBuilder.Append(" RECIEVED, ");
            strBuilder.Append(" CURRRECEIPT, ");
            strBuilder.Append(" AMTINLOC - RECIEVED OUTSTANDING, ");
            strBuilder.Append(" SEL, ");
            strBuilder.Append(" CUST ");
            strBuilder.Append(" FROM ");
            strBuilder.Append(" (  ");
            //End by rabbani on 22/11/06
            strBuilder.Append(" SELECT  ");
            strBuilder.Append(" DISTINCT  '1' sno,");
            if (BizType == 2 & Process == 1)
            {
                strBuilder.Append(" INV.INV_CUST_SEA_EXP_PK INVPK, ");
            }
            else if (BizType == 1 & Process == 1)
            {
                strBuilder.Append(" INV.INV_CUST_AIR_EXP_PK INVPK, ");
            }
            else if (BizType == 2 & Process == 2)
            {
                strBuilder.Append(" INV.INV_CUST_SEA_IMP_PK INVPK, ");
            }
            else
            {
                strBuilder.Append(" INV.INV_CUST_AIR_IMP_PK INVPK, ");
            }

            strBuilder.Append(" INV.INVOICE_REF_NO, ");
            strBuilder.Append(" INV.INVOICE_DATE,");
            strBuilder.Append(" INV.CURRENCY_MST_FK,");
            strBuilder.Append(" CUMT.CURRENCY_ID,");
            strBuilder.Append(" INV.NET_PAYABLE,");
            if (Process == 2 & ExType == 3)
            {
                strBuilder.Append(" (select get_ex_rate1( " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.CURRENCY_MST_FK,INV.INVOICE_DATE," + ExType + ") from dual) ROE,");
                strBuilder.Append(" round((INV.NET_PAYABLE)*(select get_ex_rate1(" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.CURRENCY_MST_FK,INV.INVOICE_DATE," + ExType + ") from dual),2) AmtinLoc,");
                //This Line Added by Sivachandran for PTS CR-12052008-001
            }
            else if ((Process == 2 & ExType == 1) | (Process == 1 & ExType == 1))
            {
                strBuilder.Append(" (select get_ex_rate( INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.INVOICE_DATE) from dual) ROE,");
                strBuilder.Append(" round((INV.NET_PAYABLE)*(select get_ex_rate(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.INVOICE_DATE) from dual),2) AmtinLoc,");
                //Added By sivachandran for PTS CR-12052008-001
            }
            else if (Process == 1 & ExType == 2)
            {
                strBuilder.Append(" (select get_roe_sell( INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",job.voyage_trn_fk) from dual) ROE,");
                strBuilder.Append(" round((INV.NET_PAYABLE)*(select get_roe_sell(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",job.voyage_trn_fk) from dual),2) AmtinLoc,");
            }
            //End
            strBuilder.Append(" nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR) from ");
            strBuilder.Append(" collections_tbl clt, ");
            strBuilder.Append(" collections_trn_tbl clttrn ");
            strBuilder.Append(" where");
            strBuilder.Append(" clt.collections_tbl_pk = clttrn.collections_tbl_fk");
            strBuilder.Append(" and Clttrn.Invoice_Ref_Nr   = inv.invoice_ref_no");
            strBuilder.Append("  ),0) recieved,");
            if (Process == 2 & ExType == 3)
            {
                strBuilder.Append(" round((inv.NET_PAYABLE * (select get_ex_rate1(" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", ");
                strBuilder.Append("  INV.CURRENCY_MST_FK,");
                strBuilder.Append("  INV.INVOICE_DATE," + ExType + ")");
                strBuilder.Append("  from dual) - ");
            }
            else if ((Process == 2 & ExType == 1) | (Process == 1 & ExType == 1))
            {
                strBuilder.Append(" round((inv.NET_PAYABLE * (select get_ex_rate(INV.CURRENCY_MST_FK, ");
                strBuilder.Append("  " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                strBuilder.Append("  INV.INVOICE_DATE)");
                strBuilder.Append("  from dual) - ");
                //Added by sivachandran for PTS CR-12052008-001
            }
            else if (Process == 1 & ExType == 2)
            {
                strBuilder.Append(" round((inv.NET_PAYABLE * (select get_roe_sell(INV.CURRENCY_MST_FK, ");
                strBuilder.Append("  " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                strBuilder.Append("  job.voyage_trn_fk)");
                strBuilder.Append("  from dual) - ");
            }
            //End

            strBuilder.Append(" nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR) ");
            strBuilder.Append("  from collections_tbl     clt,");
            strBuilder.Append("  collections_trn_tbl clttrn");
            strBuilder.Append("  where clt.collections_tbl_pk =");
            strBuilder.Append("  clttrn.collections_tbl_fk");
            strBuilder.Append("  and Clttrn.Invoice_Ref_Nr = inv.invoice_ref_no),0) ");
            strBuilder.Append(" ),2) receivable, ");

            strBuilder.Append("  0 CurrReceipt,");
            strBuilder.Append(" '1' Sel ,");


            //Sea Export
            if (BizType == 2 & Process == 1)
            {
                strBuilder.Append(" JOB.SHIPPER_CUST_MST_FK CUST");
                strBuilder.Append(" FROM INV_CUST_SEA_EXP_TBL INV,");
                strBuilder.Append(" JOB_CARD_TRN JOB, ");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,USER_MST_TBL UMT ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append("  INV.JOB_CARD_SEA_EXP_FK = JOB.Job_Card_Trn_Pk ");
                strBuilder.Append(" AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK ");
                strBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + "");
                strBuilder.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK ");

                if (!(intCustomerFk > 0) & !(InvRefNo.Length > 0) & !(intJobPk > 0))
                {
                    strBuilder.Append(" AND 1=2");
                }
                if (intCustomerFk > 0)
                {
                    strBuilder.Append(" AND JOB.SHIPPER_CUST_MST_FK   = " + intCustomerFk + "");
                }

                if (InvRefNo.Length > 0)
                {
                    strBuilder.Append(" AND INV.invoice_ref_no = '" + InvRefNo + "'");
                }

                if (intJobPk > 0)
                {
                    strBuilder.Append(" AND JOB.Job_Card_Trn_Pk = " + intJobPk + "");
                }
                if (!(InvRefNo.Length > 0) & !(intJobPk > 0))
                {
                    if (strFromDt.Length > 0)
                    {
                        //strBuilder.Append(" AND INV.INVOICE_DATE >= TO_DATE('" & strFromDt & "','" & dateFormat & "') ")
                    }
                    if (strToDt.Length > 0)
                    {
                        strBuilder.Append(" AND INV.INVOICE_DATE <= TO_DATE('" + strToDt + "','" + dateFormat + "' )");
                    }
                }
                //Air Export
            }
            else if (BizType == 1 & Process == 1)
            {
                strBuilder.Append(" JOB.SHIPPER_CUST_MST_FK CUST");
                strBuilder.Append(" FROM INV_CUST_AIR_EXP_TBL INV,");
                strBuilder.Append("  JOB_CARD_TRN JOB, ");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,USER_MST_TBL UMT ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append(" INV.JOB_CARD_AIR_EXP_FK = JOB.JOB_CARD_TRN_PK ");
                strBuilder.Append(" AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK ");
                strBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + "");
                strBuilder.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK ");

                if (!(intCustomerFk > 0) & !(InvRefNo.Length > 0) & !(intJobPk > 0))
                {
                    strBuilder.Append(" AND 1=2");
                }

                if (intCustomerFk > 0)
                {
                    strBuilder.Append(" AND JOB.SHIPPER_CUST_MST_FK   = " + intCustomerFk + "");
                }

                if (InvRefNo.Length > 0)
                {
                    strBuilder.Append(" AND INV.invoice_ref_no = '" + InvRefNo + "'");
                }

                if (intJobPk > 0)
                {
                    strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK = " + intJobPk + "");
                }

                if (!(InvRefNo.Length > 0) & !(intJobPk > 0))
                {
                    if (strFromDt.Length > 0)
                    {
                        //strBuilder.Append(" AND INV.INVOICE_DATE > TO_DATE('" & strFromDt & "','" & dateFormat & "') ")
                    }
                    if (strToDt.Length > 0)
                    {
                        strBuilder.Append(" AND INV.INVOICE_DATE < TO_DATE('" + strToDt + "','" + dateFormat + "') ");
                    }
                }
                //Sea Import
            }
            else if (BizType == 2 & Process == 2)
            {
                strBuilder.Append(" JOB.CONSIGNEE_CUST_MST_FK CUST");
                strBuilder.Append("  FROM INV_CUST_SEA_IMP_TBL     INV,");
                strBuilder.Append(" JOB_CARD_TRN JOB, ");
                strBuilder.Append("  CURRENCY_TYPE_MST_TBL  CUMT,USER_MST_TBL UMT ");

                strBuilder.Append(" WHERE ");
                strBuilder.Append(" INV.JOB_CARD_SEA_IMP_FK = job.JOB_CARD_TRN_PK ");
                strBuilder.Append(" AND INV.Currency_Mst_Fk = cumt.currency_mst_pk ");
                strBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + "");
                strBuilder.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK ");

                if (!(intCustomerFk > 0) & !(InvRefNo.Length > 0) & !(intJobPk > 0))
                {
                    strBuilder.Append(" AND 1=2");
                }
                if (intCustomerFk > 0)
                {
                    strBuilder.Append(" AND JOB.consignee_cust_mst_fk =  " + intCustomerFk + "");
                }

                if (InvRefNo.Length > 0)
                {
                    strBuilder.Append(" AND INV.invoice_ref_no = '" + InvRefNo + "'");
                }

                if (intJobPk > 0)
                {
                    strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK = " + intJobPk + "");
                }
                if (!(InvRefNo.Length > 0) & !(intJobPk > 0))
                {
                    if (strFromDt.Length > 0)
                    {
                        //strBuilder.Append(" AND INV.INVOICE_DATE >= TO_DATE('" & strFromDt & "','" & dateFormat & "') ")
                    }
                    if (strToDt.Length > 0)
                    {
                        strBuilder.Append(" AND INV.INVOICE_DATE <= TO_DATE('" + strToDt + "','" + dateFormat + "') ");
                    }
                }
                //Air Import
            }
            else if (BizType == 1 & Process == 2)
            {
                strBuilder.Append(" JOB.CONSIGNEE_CUST_MST_FK CUST");
                strBuilder.Append("  FROM INV_CUST_air_IMP_TBL     INV,");
                strBuilder.Append(" JOB_CARD_TRN JOB, ");
                strBuilder.Append("  CURRENCY_TYPE_MST_TBL  CUMT,USER_MST_TBL UMT ");

                strBuilder.Append(" WHERE ");
                strBuilder.Append(" INV.JOB_CARD_AIR_IMP_FK = job.JOB_CARD_TRN_PK ");
                strBuilder.Append(" AND INV.Currency_Mst_Fk = cumt.currency_mst_pk ");
                strBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + "");
                strBuilder.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK ");
                if (!(intCustomerFk > 0) & !(InvRefNo.Length > 0) & !(intJobPk > 0))
                {
                    strBuilder.Append(" AND 1=2");
                }
                if (intCustomerFk > 0)
                {
                    strBuilder.Append(" AND JOB.consignee_cust_mst_fk =  " + intCustomerFk + "");
                }
                if (InvRefNo.Length > 0)
                {
                    strBuilder.Append(" AND INV.invoice_ref_no = '" + InvRefNo + "'");
                }

                if (intJobPk > 0)
                {
                    strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK = " + intJobPk + "");
                }
                if (!(InvRefNo.Length > 0) & !(intJobPk > 0))
                {
                    if (strFromDt.Length > 0)
                    {
                        //strBuilder.Append(" AND INV.INVOICE_DATE >= TO_DATE('" & strFromDt & "','" & dateFormat & "') ")
                    }
                    if (strToDt.Length > 0)
                    {
                        strBuilder.Append(" AND INV.INVOICE_DATE <= TO_DATE('" + strToDt + "','" + dateFormat + "') ");
                    }
                }
            }

            strBuilder.Append(" union");
            strBuilder.Append(" SELECT  DISTINCT '1' sno,INV.CONSOL_INVOICE_PK INVPK, ");
            strBuilder.Append(" INV.INVOICE_REF_NO, ");
            strBuilder.Append(" INV.INVOICE_DATE,");
            strBuilder.Append(" INV.CURRENCY_MST_FK,");
            strBuilder.Append(" CUMT.CURRENCY_ID, ");
            strBuilder.Append(" INV.NET_RECEIVABLE, ");
            if (Process == 2 & ExType == 3)
            {
                strBuilder.Append(" (select get_ex_rate1(" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.CURRENCY_MST_FK,INV.INVOICE_DATE," + ExType + ") from dual) ROE, ");
                strBuilder.Append(" round((INV.NET_RECEIVABLE)*(select get_ex_rate1(" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.CURRENCY_MST_FK,INV.INVOICE_DATE," + ExType + ") from dual),2) AmtinLoc, ");
                //Added by sivachandran for PTS CR-12052008-001
            }
            else if ((Process == 2 & ExType == 1) | (Process == 1 & ExType == 1))
            {
                strBuilder.Append(" (select get_ex_rate(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.INVOICE_DATE) from dual) ROE, ");
                strBuilder.Append(" round((INV.NET_RECEIVABLE)*(select get_ex_rate(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.INVOICE_DATE) from dual),2) AmtinLoc, ");
                //'Added by sivachandran for PTS CR-12052008-001 On 19MAy2008
            }
            else if (Process == 1 & ExType == 2)
            {
                strBuilder.Append(" (select GET_ROE_SELL1(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.CONSOL_INVOICE_PK) from dual) ROE, ");
                strBuilder.Append(" round((INV.NET_RECEIVABLE)*(select GET_ROE_SELL1(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.CONSOL_INVOICE_PK) from dual),2) AmtinLoc, ");
            }
            //End
            strBuilder.Append("   nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR) from ");
            strBuilder.Append("   collections_tbl clt, ");
            strBuilder.Append("  collections_trn_tbl clttrn ");
            strBuilder.Append("  where ");
            strBuilder.Append("  clt.collections_tbl_pk = clttrn.collections_tbl_fk ");
            strBuilder.Append("   and Clttrn.Invoice_Ref_Nr   = inv.invoice_ref_no ");
            strBuilder.Append("    ),0) recieved, ");
            //Modified by Snigdharani - 04/12/2008 - Because Write Off Amount has to be Deducted from Net Recievables
            if (Process == 2 & ExType == 3)
            {
                strBuilder.Append("   round(nvl(inv.NET_RECEIVABLE* (select get_ex_rate1(" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.CURRENCY_MST_FK,INV.INVOICE_DATE," + ExType + ") from dual) - nvl(nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR) from  ");
            }
            else if ((Process == 2 & ExType == 1) | (Process == 1 & ExType == 1))
            {
                strBuilder.Append("   round(nvl(inv.NET_RECEIVABLE* (select get_ex_rate(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.INVOICE_DATE) from dual) - nvl(nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR) from  ");
                //Added by sivachandran for PTS CR-12052008-001
            }
            else if (Process == 1 & ExType == 2)
            {
                strBuilder.Append("   round(nvl(inv.NET_RECEIVABLE* (select GET_ROE_SELL1(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.CONSOL_INVOICE_PK) from dual) - nvl(nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR) from  ");
            }
            //End
            strBuilder.Append("   collections_tbl clt, ");
            strBuilder.Append("   collections_trn_tbl clttrn ");
            strBuilder.Append("   where ");
            strBuilder.Append("  clt.collections_tbl_pk = clttrn.collections_tbl_fk ");
            strBuilder.Append("   and Clttrn.Invoice_Ref_Nr   = inv.invoice_ref_no ");
            strBuilder.Append("   ), 0) + nvl((select sum(WRM.WRITEOFF_AMOUNT) ");
            strBuilder.Append("   from writeoff_manual_tbl WRM ");
            strBuilder.Append("   where wrm.consol_invoice_fk = ");
            strBuilder.Append("   inv.consol_invoice_pk), ");
            //MODIFIED BY LATHA BASED ON SURYA DISCUSSION
            strBuilder.Append("   0),0),0),2) receivable, ");
            //strBuilder.Append("   ),0),0),2) receivable, ")
            strBuilder.Append("  0 CurrReceipt, ");
            strBuilder.Append("  '1' Sel, ");
            strBuilder.Append(" INV.CUSTOMER_MST_FK CUST");

            strBuilder.Append("   FROM CONSOL_INVOICE_TBL INV, ");
            strBuilder.Append("   CONSOL_INVOICE_TRN_TBL INVFRT, ");
            strBuilder.Append("   CURRENCY_TYPE_MST_TBL CUMT,USER_MST_TBL UMT ");
            strBuilder.Append("   WHERE  ");
            strBuilder.Append("   INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK ");
            strBuilder.Append("   AND INV.CONSOL_INVOICE_PK = INVFRT.CONSOL_INVOICE_FK ");
            //COMMENTED BY LATHA
            // strBuilder.Append("   AND INVFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK ")
            strBuilder.Append("   AND  INV.process_type = " + Process + "");
            strBuilder.Append("   AND  INV.business_type = " + BizType + "");
            strBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngLocPk + "");
            strBuilder.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK ");
            strBuilder.Append(" AND INV.CHK_INVOICE = 1 ");

            if (!(intCustomerFk > 0) & !(InvRefNo.Length > 0) & !(intJobPk > 0))
            {
                strBuilder.Append(" AND 1=2");
            }
            if (intCustomerFk > 0)
            {
                strBuilder.Append("   AND INV.CUSTOMER_MST_FK = " + intCustomerFk + " ");
            }
            if (InvRefNo.Length > 0)
            {
                strBuilder.Append(" AND INV.INVOICE_REF_NO = '" + InvRefNo + "'");
            }
            //added by Manivannan
            if (intJobPk > 0)
            {
                strBuilder.Append(" AND INVFRT.JOB_CARD_FK = " + intJobPk + "");
            }
            //end
            if (!string.IsNullOrEmpty(ConInvPK))
            {
                strBuilder.Append(" AND INV.CONSOL_INVOICE_PK in ( " + ConInvPK + ")");
            }
            if (!(InvRefNo.Length > 0) & !(intJobPk > 0))
            {
                if (strFromDt.Length > 0)
                {
                    //strBuilder.Append(" AND INV.INVOICE_DATE >= TO_DATE('" & strFromDt & "','" & dateFormat & "') ")
                }
                if (strToDt.Length > 0)
                {
                    strBuilder.Append(" AND INV.INVOICE_DATE <= TO_DATE('" + strToDt + "','" + dateFormat + "') ");
                }
            }
            //Added by rabbani on 22/11/06 To Exclude zero receivable from the  query
            strBuilder.Append(" ) ");
            //strBuilder.Append(" WHERE receivable > 0 ")
            //End by rabbani on 22/11/06
            try
            {
                return objWF.GetDataSet(strBuilder.ToString());
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
        public DataSet FetchSavedData(int intColPk, short intBizType, short intProcess, int intBaseCurr, int intCollCurr)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("COLPK_IN", intColPk).Direction = ParameterDirection.Input;
                _with1.Add("BIZTYPE_IN", intBizType).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_IN", intProcess).Direction = ParameterDirection.Input;
                _with1.Add("CUR_IN", intBaseCurr).Direction = ParameterDirection.Input;
                _with1.Add("COLLCUR_IN", intCollCurr).Direction = ParameterDirection.Input;
                _with1.Add("HDR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("INV_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("MODE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_COLLECTION_PKG", "FETCH_AFTER_SAVE");
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


        public DataSet FetchPaymentDetails(short Mode)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWf = new WorkFlow();

            strBuilder.Append(" select rownum,cm.collections_mode_trn_pk,");
            strBuilder.Append(" cm.collections_tbl_fk,");
            strBuilder.Append(" cm.receipt_mode,");
            strBuilder.Append(" decode(receipt_mode,1,'cheque',2,'Cash',3,'Bank Transfer',4,'DD',5,'PO') \"Mode\",");
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
        #endregion

        #region " Save"
        public int SaveData(ref DataSet dsSave, ref string CollectionNo, long nLocationPk, long nEmpId, double NetAmt, string Customer, double CrLimit, double CrLimitUsed, Array JobPk, int ExType = 1)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            int intSaveSucceeded = 0;
            OracleTransaction TRAN = null;
            int intPkValue = 0;
            int intChldCnt = 0;
            int i = 0;
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
                //Snigdharani - 05/12/2008
                var _with2 = dsSave.Tables[0].Rows[0];
                objWK.MyCommand.Connection = objWK.MyConnection;
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".COLLECTIONS_TBL_PKG.COLLECTIONS_TBL_INS";
                objWK.MyCommand.Parameters.Clear();
                objWK.MyCommand.Parameters.Add("PROCESS_TYPE_IN", _with2["PROCESS_TYPE_IN"]);
                objWK.MyCommand.Parameters.Add("BUSINESS_TYPE_IN", _with2["BUSINESS_TYPE_IN"]);
                objWK.MyCommand.Parameters.Add("COLLECTIONS_REF_NO_IN", Convert.ToString(CollectionNo));
                objWK.MyCommand.Parameters.Add("COLLECTIONS_DATE_IN", Convert.ToDateTime(_with2["COLLECTIONS_DATE_IN"]));
                objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with2["CURRENCY_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("REMARKS_IN", _with2["REMARKS_IN"]);
                objWK.MyCommand.Parameters.Add("CUSTOMER_MST_FK_IN", _with2["CUSTOMER_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("CREATED_BY_FK_IN", _with2["CREATED_BY_FK_IN"]);
                //add by latha to save the exchange rate type
                objWK.MyCommand.Parameters.Add("EXCH_RATE_TYPE_FK_IN", ExType);
                objWK.MyCommand.Parameters.Add("CONFIG_MST_PK_IN", _with2["CONFIG_MST_PK_IN"]);
                objWK.MyCommand.Parameters.Add("AGENT_MST_FK_IN", _with2["AGENT_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("RETURN_VALUE", _with2["CREATED_BY_FK_IN"]).Direction = ParameterDirection.Output;

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
                    var _with3 = dsSave.Tables[1].Rows[intChldCnt];
                    objWK.MyCommand.Parameters.Add("COLLECTIONS_TBL_FK_IN", intPkValue);
                    objWK.MyCommand.Parameters.Add("FROM_INV_OR_CONSOL_INV_IN", _with3["FROM_INV_OR_CONSOL_INV_IN"]);
                    objWK.MyCommand.Parameters.Add("INVOICE_REF_NR_IN", _with3["INVOICE_REF_NR_IN"]);
                    objWK.MyCommand.Parameters.Add("RECD_AMOUNT_HDR_CURR_IN", _with3["RECD_AMOUNT_HDR_CURR_IN"]);
                    objWK.MyCommand.Parameters.Add("EXCH_RATE_FLUCTUATION_IN", _with3["EXCH_RATE_FLUCTUATION_IN"]);
                    objWK.MyCommand.Parameters.Add("CUSTOMER_MST_FK_IN", dsSave.Tables[0].Rows[0]["CUSTOMER_MST_FK_IN"]);
                    //Added By PrakashChandra on 03/06/2008
                    objWK.MyCommand.Parameters.Add("TDS_PERCENTAGE_IN", _with3["TDS_PERCENTAGE_IN"]);
                    objWK.MyCommand.Parameters.Add("TDS_AMOUNT_IN", _with3["TDS_AMOUNT_IN"]);
                    intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
                }

                intChldCnt = 0;
                for (intChldCnt = 0; intChldCnt <= dsSave.Tables[2].Rows.Count - 1; intChldCnt++)
                {
                    objWK.MyCommand.Parameters.Clear();
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".COLLECTIONS_TBL_PKG.COLLECTIONS_MODE_TRN_TBL_INS";
                    var _with4 = dsSave.Tables[2].Rows[intChldCnt];
                    objWK.MyCommand.Parameters.Add("COLLECTIONS_TBL_FK_IN", intPkValue);
                    objWK.MyCommand.Parameters.Add("RECEIPT_MODE_IN", _with4["RECEIPT_MODE_IN"]);
                    objWK.MyCommand.Parameters.Add("CHEQUE_NUMBER_IN", _with4["CHEQUE_NUMBER_IN"]);
                    objWK.MyCommand.Parameters.Add("CHEQUE_DATE_IN", _with4["CHEQUE_DATE_IN"]);
                    objWK.MyCommand.Parameters.Add("BANK_MST_FK_IN", _with4["BANK_PK_IN"]);
                    objWK.MyCommand.Parameters.Add("BANK_NAME_IN", _with4["BANK_NAME_IN"]);
                    objWK.MyCommand.Parameters.Add("DEPOSITED_IN", _with4["DEPOSITED_IN"]);
                    objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with4["CURRENCY_MST_FK_IN"]);
                    objWK.MyCommand.Parameters.Add("RECD_AMOUNT_IN", _with4["RECD_AMOUNT_IN"]);
                    objWK.MyCommand.Parameters.Add("EXCHANGE_RATE_IN", _with4["EXCHANGE_RATE_IN"]);
                    objWK.MyCommand.Parameters.Add("RETURN_VALUE", _with4["CURRENCY_MST_FK_IN"]);
                    intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
                }


                //end
                if (intSaveSucceeded > 0)
                {
                    //'Added by Venkata on 03-Mar-07 To store CreditLimitUsed in Customer master table
                    //'on discussion with Kiran
                    if (CrLimit > 0)
                    {
                        SaveCreditLimit(NetAmt, Customer, CrLimitUsed, TRAN);
                    }
                    TRAN.Commit();
                    //Push to financial system if realtime is selected
                    if (intPkValue > 0)
                    {
                        if (Convert.ToBoolean(ConfigurationManager.AppSettings["QFINGeneral"]) == true)
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
                        cls_Scheduler objSch = new cls_Scheduler();
                        ArrayList schDtls = null;
                        bool errGen = false;
                        if (objSch.GetSchedulerPushType() == true)
                        {
                            //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                            //try
                            //{
                            //    schDtls = objSch.FetchSchDtls();
                            //    //'Used to Fetch the Sch Dtls
                            //    objPush.UpdateTaxElement(schDtls[10], schDtls[2], schDtls[6], schDtls[4], ref errGen);
                            //    objPush.UpdatebankAcc(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //    objPush.UpdateCurrency(schDtls[10], schDtls[2], schDtls[6], schDtls[4], ref errGen);
                            //    objPush.UpdateTransaction(schDtls[10], schDtls[2], schDtls[6], schDtls[4], ref errGen, intPkValue);
                            //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                            //    {
                            //        objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                            //    }
                            //}
                            //catch (Exception ex)
                            //{
                            //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                            //    {
                            //        objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                            //    }
                            //}
                        }

                    }
                    //*****************************************************************
                    //-----Manjunath for Excess amount---
                    objWK.MyCommand.Parameters.Clear();
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".COLLECTIONS_TBL_PKG.SAVE_EXCESS_AMOUNT";

                    var _with5 = objWK.MyCommand.Parameters;
                    _with5.Add("COLLECTIONS_TBL_FK_IN", intPkValue).Direction = ParameterDirection.Input;
                    objWK.MyCommand.ExecuteNonQuery();

                    //-----Added By Anand 11/11/2008: To Get the Invoice Amount and to set the status-----------------------
                    DataSet dsData = new DataSet();
                    Int32 NetRec = default(Int32);
                    Int32 Rec = default(Int32);
                    string Col_Status = null;
                    string ColRef = null;

                    ColRef = Fetch_Col_Ref_No(intPkValue);

                    objWK.MyCommand.Parameters.Clear();
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".COLLECTIONS_TBL_PKG.CHECK_COL_DATA";

                    var _with6 = objWK.MyCommand.Parameters;
                    _with6.Add("COL_REF_IN", ColRef).Direction = ParameterDirection.Input;
                    _with6.Add("COL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;


                    objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                    objWK.MyDataAdapter.Fill(dsData);

                    //If Not IsNothing(dsData) Then
                    if (dsData.Tables[0].Rows.Count > 0)
                    {
                        NetRec = (string.IsNullOrEmpty(dsData.Tables[0].Rows[0][0].ToString()) ? 0 : Convert.ToInt32(dsData.Tables[0].Rows[0][0]));
                        Rec = (string.IsNullOrEmpty(dsData.Tables[0].Rows[0][1].ToString()) ? 0 : Convert.ToInt32(dsData.Tables[0].Rows[0][1]));
                    }

                    if (NetRec == Rec)
                    {
                        Col_Status = "Collection Done Against Invoice Number '" + ColRef + "'";
                    }
                    else
                    {
                        Col_Status = "Part Collection Done Against Invoice Number '" + ColRef + "'";
                    }

                    //--------------------------------------------Completed----------------------------------------------

                    //created by Thiyagarajan on 5/5/08 for storing Collections information into TRACK-N-TRACE table
                    for (i = 0; i <= JobPk.Length - 1; i++)
                    {
                        objWK.MyCommand.Parameters.Clear();
                        objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                        objWK.MyCommand.CommandText = objWK.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
                        var _with7 = objWK.MyCommand;
                        _with7.Parameters.Add("Key_fk_in", JobPk.GetValue(i)).Direction = ParameterDirection.Input;
                        _with7.Parameters.Add("BIZ_TYPE_IN", dsSave.Tables[0].Rows[0]["BUSINESS_TYPE_IN"]).Direction = ParameterDirection.Input;
                        _with7.Parameters.Add("PROCESS_IN", dsSave.Tables[0].Rows[0]["PROCESS_TYPE_IN"]).Direction = ParameterDirection.Input;
                        _with7.Parameters.Add("status_in", Col_Status).Direction = ParameterDirection.Input;
                        // Modified By Anand to set the collection status
                        _with7.Parameters.Add("locationfk_in", nLocationPk).Direction = ParameterDirection.Input;
                        _with7.Parameters.Add("OnStatus_in", "COL-INS").Direction = ParameterDirection.Input;
                        _with7.Parameters.Add("pkStatus_in", "O").Direction = ParameterDirection.Input;
                        _with7.Parameters.Add("flagInsUpd_in", Convert.ToString(CollectionNo)).Direction = ParameterDirection.Input;
                        _with7.Parameters.Add("Container_Data_in", DBNull.Value).Direction = ParameterDirection.Input;
                        _with7.Parameters.Add("CreatedUser_in", dsSave.Tables[0].Rows[0]["CREATED_BY_FK_IN"]).Direction = ParameterDirection.Input;
                        _with7.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with7.ExecuteNonQuery();
                    }

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
        public object fetchcollection(string JOBPK = "", short BizType = 0, short Process = 0)
        {

            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT JOB.Job_Card_Trn_Pk,");
                sb.Append("                JOB.JOBCARD_REF_NO,");
                sb.Append("                JOBFRT.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                CASE");
                sb.Append("                  WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL THEN");
                sb.Append("                   JOBFRT.INV_AGENT_TRN_FK");
                sb.Append("                  ELSE");
                sb.Append("                   JOBFRT.CONSOL_INVOICE_TRN_FK");
                sb.Append("                END CONSOL_INVOICE_TRN_FK,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL THEN");
                sb.Append("                   (SELECT SUM(I.NET_INV_AMT)");
                sb.Append("                      FROM INV_AGENT_TBL I");
                sb.Append("                     WHERE I.JOB_CARD_FK = JOB.Job_Card_Trn_Pk)");
                sb.Append("                  ELSE");
                sb.Append("                   INV.INVOICE_AMT");
                sb.Append("                END) INVOICE_AMT,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL THEN");
                sb.Append("                   (SELECT NVL(SUM(DISTINCT CI.RECD_AMOUNT_HDR_CURR), 0)");
                sb.Append("                      FROM INV_AGENT_TBL I, COLLECTIONS_TRN_TBL CI");
                sb.Append("                     WHERE I.INVOICE_REF_NO = CI.INVOICE_REF_NR");
                sb.Append("                       AND CI.COLLECTIONS_TRN_PK = CIT.COLLECTIONS_TRN_PK)");
                sb.Append("                  ELSE");
                sb.Append("                   NVL(SUM(DISTINCT CIT.RECD_AMOUNT_HDR_CURR), 0)");
                sb.Append("                END) RECD_AMOUNT_HDR_CURR,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL THEN");
                sb.Append("                   (SELECT SUM(I.NET_INV_AMT)");
                sb.Append("                      FROM INV_AGENT_TBL I");
                sb.Append("                     WHERE I.JOB_CARD_FK = JOB.Job_Card_Trn_Pk) -");
                sb.Append("                   (SELECT NVL(SUM(DISTINCT CI.RECD_AMOUNT_HDR_CURR), 0)");
                sb.Append("                      FROM INV_AGENT_TBL I, COLLECTIONS_TRN_TBL CI");
                sb.Append("                     WHERE I.INVOICE_REF_NO = CI.INVOICE_REF_NR");
                sb.Append("                       AND CI.COLLECTIONS_TRN_PK = CIT.COLLECTIONS_TRN_PK)");
                sb.Append("                  ELSE");
                sb.Append("                   INV.INVOICE_AMT -");
                sb.Append("                   NVL(SUM(DISTINCT CIT.RECD_AMOUNT_HDR_CURR), 0)");
                sb.Append("                END) COLLECTION");
                sb.Append("  FROM COLLECTIONS_TBL        C,");
                sb.Append("       COLLECTIONS_TRN_TBL    CIT,");
                sb.Append("       JOB_CARD_TRN   JOB,");
                sb.Append("       JOB_TRN_FD     JOBFRT,");
                sb.Append("       CONSOL_INVOICE_TBL     INV,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL INVTR");
                sb.Append(" WHERE C.COLLECTIONS_TBL_PK = CIT.COLLECTIONS_TBL_FK");
                sb.Append("   AND INV.CONSOL_INVOICE_PK = INVTR.CONSOL_INVOICE_FK");
                sb.Append("   AND CIT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("   AND INVTR.JOB_CARD_FK = JOB.Job_Card_Trn_Pk");
                sb.Append("   AND JOB.Job_Card_Trn_Pk = JOBFRT.JOB_CARD_TRN_FK");
                sb.Append("   AND INV.BUSINESS_TYPE =" + BizType);
                sb.Append("   AND INV.PROCESS_TYPE = " + Process);
                if (!string.IsNullOrEmpty(JOBPK))
                {
                    sb.Append("   AND JOB.Job_Card_Trn_Pk =" + JOBPK);
                }

                sb.Append("GROUP BY Job_Card_Trn_Pk,");
                sb.Append("          JOBCARD_REF_NO,");
                sb.Append("          FREIGHT_ELEMENT_MST_FK,");
                sb.Append("          CONSOL_INVOICE_TRN_FK,");
                sb.Append("          JOBFRT.INV_AGENT_TRN_FK,");
                sb.Append("          CIT.COLLECTIONS_TRN_PK,");
                sb.Append("          INVOICE_AMT");

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
        public object fetchAgtcollection(string JOBPK = "", short BizType = 0, short Process = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT JOB.JOB_CARD_TRN_PK,");
                sb.Append("       INVAGT.INV_AGENT_PK,");
                sb.Append("       INVAGT.CB_DP_LOAD_AGENT AGENTTYPE ");
                sb.Append("  FROM JOB_CARD_TRN JOB, INV_AGENT_TBL INVAGT");
                sb.Append("  WHERE JOB.JOB_CARD_TRN_PK = " + JOBPK);
                sb.Append("   AND INVAGT.JOB_CARD_FK(+) = JOB.JOB_CARD_TRN_PK");
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
        public DataSet GetJOBPK(string ColRefNr = "", short BizType = 0, short Process = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT CONTRN.JOB_CARD_FK,");
                sb.Append("       CON.INVOICE_REF_NO,");
                sb.Append("       COL.COLLECTIONS_REF_NO");
                sb.Append("  FROM ");
                sb.Append("       CONSOL_INVOICE_TBL     CON,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL CONTRN,");
                sb.Append("       COLLECTIONS_TBL        COL,");
                sb.Append("       COLLECTIONS_TRN_TBL    COLTRN");
                sb.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
                sb.Append("   AND CON.INVOICE_REF_NO = COLTRN.INVOICE_REF_NR");
                sb.Append("   AND COL.COLLECTIONS_TBL_PK = COLTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND CON.BUSINESS_TYPE =" + BizType);
                sb.Append("   AND CON.PROCESS_TYPE = " + Process);
                if (!string.IsNullOrEmpty(ColRefNr))
                {
                    sb.Append("   AND COL.COLLECTIONS_REF_NO = '" + ColRefNr + "'");
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
        public string Fetch_Col_Ref_No(Int32 ColPk)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlstr = null;
            string ColRefNo = null;
            sqlstr = "select ctrn.invoice_ref_nr from collections_trn_tbl ctrn where ctrn.collections_tbl_fk='" + ColPk + "'";
            ColRefNo = objWF.ExecuteScaler(sqlstr);
            return ColRefNo;
        }
        #endregion

        #region " Enhance Search Function for Job Card "
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
            Int32 intAgentOrParty = 0;
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
            if (arr.Length > 6)
                intAgentOrParty = Convert.ToInt32(arr[6]);
            //Snigdharani - 01/06/2009

            try
            {
                objWF.OpenConnection();
                cmd.Connection = objWF.MyConnection;
                cmd.CommandType = CommandType.StoredProcedure;
                //If this value is one then job card is for agent collection
                if (intAgentOrParty == 1)
                {
                    cmd.CommandText = objWF.MyUserName + ".EN_JOB_FOR_COLLECTION.GET_JOB_AGT_COLL";
                    //Job Card is for normal Collection
                }
                else
                {
                    cmd.CommandText = objWF.MyUserName + ".EN_JOB_FOR_COLLECTION.GET_JOB_COLL";
                }
                //cmd.CommandText = objWF.MyUserName & ".EN_JOB_FOR_COLLECTION.GET_JOB_COLL"
                var _with8 = cmd.Parameters;
                _with8.Add("SEARCH_IN", getDefault(strSearchIn, DBNull.Value)).Direction = ParameterDirection.Input;
                _with8.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with8.Add("BUSINESS_TYPE_IN", intBizType).Direction = ParameterDirection.Input;
                _with8.Add("PROCESS_IN", intProcess).Direction = ParameterDirection.Input;
                _with8.Add("PARTY_IN", intParty).Direction = ParameterDirection.Input;
                _with8.Add("LOC_IN", intLocPk).Direction = ParameterDirection.Input;
                _with8.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        #endregion

        #region " Protocol Reference Number"
        public string GenerateCollectionNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null)
        {
            return GenerateProtocolKey("COLLECTIONS", nLocationId, nEmployeeId, DateTime.Now,"" ,"" ,"" , nCreatedBy, ObjWK);
        }
        #endregion

        #region "Get DataSet for Report"
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

            strSql.Append(" CUST.CUSTOMER_NAME CUSTNM, CURR.CURRENCY_ID CURRNM, SUM(COLTRN.RECD_AMOUNT_HDR_CURR) COLAMT");
            strSql.Append(" FROM COLLECTIONS_TBL COL, COLLECTIONS_TRN_TBL COLTRN, CUSTOMER_MST_TBL CUST, CURRENCY_TYPE_MST_TBL CURR,");
            strSql.Append(" COUNTRY_MST_TBL CNT, USER_MST_TBL USMST, LOCATION_MST_TBL LOC");
            strSql.Append(" WHERE COL.COLLECTIONS_TBL_PK = COLTRN.COLLECTIONS_TBL_FK(+) AND COL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK(+)");
            strSql.Append(" AND LOC.COUNTRY_MST_FK = CNT.COUNTRY_MST_PK(+) AND COL.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)");
            strSql.Append(" AND COL.CREATED_BY_FK = USMST.USER_MST_PK(+) AND USMST.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
            strSql.Append(" AND COL.COLLECTIONS_TBL_PK = '" + colpk + "'");
            strSql.Append(" GROUP BY loc.corporate_mst_fk,COL.PROCESS_TYPE, COL.BUSINESS_TYPE, COL.COLLECTIONS_TBL_PK, COL.COLLECTIONS_REF_NO,");
            strSql.Append(" COL.COLLECTIONS_DATE, LOC.OFFICE_NAME, LOC.ADDRESS_LINE1, LOC.ADDRESS_LINE2, LOC.ADDRESS_LINE3,");
            strSql.Append(" LOC.CITY, LOC.ZIP, CNT.COUNTRY_NAME, CUST.CUSTOMER_NAME, CURR.CURRENCY_ID,loc.tele_phone_no,loc.fax_no");
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
        public DataSet getModDetDs(int colpk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder strSql = new StringBuilder();

            strSql.Append("SELECT DECODE(COLMOD.RECEIPT_MODE,1,'Cheque',2,'Cash',3,'Bank Transfer',4,'DD',5,'PO') \"Mode\", COLMOD.COLLECTIONS_TBL_FK COLMODFK, decode(COLMOD.RECEIPT_MODE,2, 'Cash',to_char(COLMOD.CHEQUE_NUMBER)) CHQNO,");
            strSql.Append(" TO_CHAR(COLMOD.CHEQUE_DATE,'" + dateFormat + "') CHQDT, CURR.CURRENCY_ID CURRNM,");
            strSql.Append(" round((COLMOD.RECD_AMOUNT / COLMOD.EXCHANGE_RATE),2) RECAMT, COLMOD.EXCHANGE_RATE ROE,  ROUND((COLMOD.RECD_AMOUNT), 2) LOCAMT,");
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
        public DataSet getTransDetDs(int COLPK_IN, int CUR_IN)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder strSql = new StringBuilder();

            //Biz Type = Sea Process  = Export 
            // strSql.Append(" IF BIZTYPE_IN =2 and PROCESS_IN = 1  then")

            strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT,");
            // DatFormat Added By Anand to display dd/MM/yyyy format
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE,");
            strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1  else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
            //Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
            strSql.Append(" round(nvl(inv.net_payable * ");
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk," + CUR_IN + ",inv.invoice_date) from dual) - ");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
            strSql.Append(" 0),2)  receivable , ");
            strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt ");

            strSql.Append(" FROM INV_CUST_SEA_EXP_TBL INV, Inv_Cust_Trn_Sea_Exp_Tbl INVFRT,");
            strSql.Append(" CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN,  COLLECTIONS_TRN_TBL CTRN");
            strSql.Append(" WHERE INV.INV_CUST_SEA_EXP_PK = invfrt.inv_cust_sea_exp_fk AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
            strSql.Append(" AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN);
            strSql.Append(" AND CLN.PROCESS_TYPE = 1 AND CLN.BUSINESS_TYPE = 2");
            strSql.Append(" AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");

            strSql.Append(" UNION ");
            strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID,INV.NET_RECEIVABLE RECAMT, ");
            // round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " & CUR_IN & ", inv.invoice_date) from dual)),2) RECAMT,
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) ROE, ");
            strSql.Append(" round((INV.NET_RECEIVABLE) * (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) end),2) AmtinLoc,");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
            //Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
            strSql.Append(" round(nvl(inv.net_receivable * ");
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",CLN.COLLECTIONS_DATE) from dual) - ");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
            strSql.Append(" 0),2)  receivable ,");
            strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt");

            strSql.Append(" FROM CONSOL_INVOICE_TBL  INV, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN ");
            strSql.Append(" WHERE CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
            strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
            strSql.Append(" AND CLN.PROCESS_TYPE = INV.PROCESS_TYPE AND CLN.BUSINESS_TYPE = INV.BUSINESS_TYPE");
            strSql.Append(" AND CLN.PROCESS_TYPE = 1 AND CLN.BUSINESS_TYPE = 2");
            //strSql.Append(" AND CLN.Business_Type = 2 AND CLN.Process_Type =1;")

            // Biz Type = Air -- Process  = Export  
            //strSql.Append(" else IF BIZTYPE_IN =1 and PROCESS_IN = 1   then ")
            strSql.Append(" UNION ");
            strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT,");
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE,");
            strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
            strSql.Append("(select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
            //Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
            strSql.Append(" round(nvl(inv.net_payable *");
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",inv.invoice_date) from dual) - ");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
            strSql.Append(" 0),2)  receivable ,");
            strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt ");
            strSql.Append(" FROM INV_CUST_AIR_EXP_TBL INV, Inv_Cust_Trn_AIR_Exp_Tbl INVFRT, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN");
            strSql.Append(" WHERE INV.INV_CUST_AIR_EXP_PK = invfrt.inv_cust_AIR_exp_fk AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK ");
            strSql.Append(" AND CLN.PROCESS_TYPE = 1 AND CLN.BUSINESS_TYPE = 1");
            strSql.Append(" AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK  AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");

            strSql.Append(" UNION ");
            strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, INV.NET_RECEIVABLE RECAMT,");
            //round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " & CUR_IN & ", inv.invoice_date) from dual)),2) RECAMT,
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) ROE,");
            strSql.Append(" round((INV.NET_RECEIVABLE) * (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) end),2) AmtinLoc,");
            strSql.Append("(select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
            //Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
            strSql.Append(" round(nvl(inv.net_receivable * ");
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",CLN.COLLECTIONS_DATE) from dual) - ");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
            strSql.Append(" 0),2)  receivable ,");
            strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt");
            strSql.Append(" FROM CONSOL_INVOICE_TBL INV, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN");
            strSql.Append(" WHERE CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
            strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO ");
            strSql.Append(" AND CLN.PROCESS_TYPE = INV.PROCESS_TYPE AND CLN.BUSINESS_TYPE = INV.BUSINESS_TYPE");
            strSql.Append(" AND CLN.PROCESS_TYPE = 1 AND CLN.BUSINESS_TYPE = 1");
            // strSql.Append(" AND CLN.Business_Type = 1 AND CLN.Process_Type =1;")

            // Biz Type = Sea -- Process  = Import 
            //strSql.Append(" ELSE IF BIZTYPE_IN =2 and PROCESS_IN = 2  then")
            strSql.Append(" UNION ");
            strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT,");
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE,");
            strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
            //Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
            strSql.Append(" round(nvl(inv.net_payable * ");
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",inv.invoice_date) from dual) - ");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
            strSql.Append(" 0),2)  receivable , ");
            strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt ");
            strSql.Append(" FROM INV_CUST_SEA_IMP_TBL INV, Inv_Cust_Trn_Sea_IMP_Tbl INVFRT, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN,");
            strSql.Append(" COLLECTIONS_TRN_TBL CTRN WHERE INV.INV_CUST_SEA_IMP_PK = invfrt.inv_cust_sea_IMP_fk");
            strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
            strSql.Append(" AND CLN.PROCESS_TYPE = 2 AND CLN.BUSINESS_TYPE = 2");
            strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");

            strSql.Append(" UNION ");
            strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID,INV.NET_RECEIVABLE RECAMT,");
            // round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " & CUR_IN & ", inv.invoice_date) from dual)),2) RECAMT,
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) ROE,");
            strSql.Append(" round((INV.NET_RECEIVABLE) * (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) end),2) AmtinLoc,");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved, ");
            //Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
            strSql.Append(" round(nvl(inv.net_receivable * ");
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",CLN.COLLECTIONS_DATE) from dual) - ");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
            strSql.Append(" 0),2)  receivable ,");
            strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt");
            strSql.Append(" FROM CONSOL_INVOICE_TBL INV, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN");
            strSql.Append(" WHERE  CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK  AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
            strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
            strSql.Append(" AND CLN.PROCESS_TYPE = INV.PROCESS_TYPE AND CLN.BUSINESS_TYPE = INV.BUSINESS_TYPE");
            strSql.Append(" AND CLN.PROCESS_TYPE = 2 AND CLN.BUSINESS_TYPE = 2");
            //strSql.Append(" AND CLN.Business_Type = 2 AND CLN.Process_Type =2;")

            // Biz Type = AIR -- Process  = IMPORT
            // strSql.Append(" ELSE IF BIZTYPE_IN =1 and PROCESS_IN = 2   then")
            strSql.Append(" UNION ");
            strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT,");
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE, ");
            strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
            //Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
            strSql.Append(" round(nvl(inv.net_payable * ");
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",inv.invoice_date) from dual) - ");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
            strSql.Append(" 0),2)  receivable ,");
            strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt ");
            strSql.Append(" FROM INV_CUST_AIR_IMP_TBL INV, Inv_Cust_Trn_AIR_IMP_Tbl INVFRT, CURRENCY_TYPE_MST_TBL CUMT,");
            strSql.Append(" COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN");
            strSql.Append(" WHERE INV.INV_CUST_AIR_IMP_PK = invfrt.inv_cust_AIR_IMP_fk AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
            strSql.Append(" AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
            strSql.Append(" AND CLN.PROCESS_TYPE = 2 AND CLN.BUSINESS_TYPE = 1");

            strSql.Append(" UNION ");
            strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, INV.NET_RECEIVABLE RECAMT, ");
            //round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " & CUR_IN & ", inv.invoice_date) from dual)),2) RECAMT,
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) ROE,");
            strSql.Append(" round((INV.NET_RECEIVABLE) * (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", CLN.COLLECTIONS_DATE) from dual) end),2) AmtinLoc,");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved, ");
            //Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
            strSql.Append(" round(nvl(inv.net_receivable* ");
            strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",CLN.COLLECTIONS_DATE) from dual) - ");
            strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
            strSql.Append(" 0),2)  receivable ,");
            strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt");
            strSql.Append(" FROM CONSOL_INVOICE_TBL INV, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN");
            strSql.Append(" WHERE CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
            strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
            strSql.Append(" AND CLN.PROCESS_TYPE = INV.PROCESS_TYPE AND CLN.BUSINESS_TYPE = INV.BUSINESS_TYPE");
            strSql.Append(" AND CLN.PROCESS_TYPE = 2 AND CLN.BUSINESS_TYPE = 1");
            //strSql.Append(" AND CLN.Business_Type = 1 AND CLN.Process_Type =2;")

            ///strSql.Append(" SELECT COLTRN.COLLECTIONS_TBL_FK COLTRNFK, COLTRN.INVOICE_REF_NR INVREFNO,")
            ///strSql.Append(" TO_CHAR(INV.INVOICE_DATE,'" & dateFormat & "') INVDT, COLTRN.RECD_AMOUNT_HDR_CURR RECAMT")
            ///strSql.Append(" , curr.CURRENCY_ID CURRNM, COLMOD.EXCHANGE_RATE ROE, (COLTRN.RECD_AMOUNT_HDR_CURR * COLMOD.EXCHANGE_RATE) LOCAMT") 'new
            ///strSql.Append(" FROM COLLECTIONS_TRN_TBL COLTRN, COLLECTIONS_TBL COL, CONSOL_INVOICE_TBL INV")
            ///strSql.Append(" , CURRENCY_TYPE_MST_TBL curr, COLLECTIONS_MODE_TRN_TBL COLMOD") 'new
            ///strSql.Append(" WHERE COLTRN.COLLECTIONS_TBL_FK = COL.COLLECTIONS_TBL_PK AND COLTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO")
            ///strSql.Append(" AND COL.PROCESS_TYPE = INV.PROCESS_TYPE AND COL.BUSINESS_TYPE = INV.BUSINESS_TYPE")
            ///strSql.Append(" and colmod.collections_tbl_fk = COL.COLLECTIONS_TBL_PK(+)") 'new
            ///strSql.Append(" and col.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)") 'new
            ///strSql.Append(" AND COLTRN.FROM_INV_OR_CONSOL_INV = 2 AND COL.COLLECTIONS_TBL_PK =" & colpk)

            ///strSql.Append(" UNION")
            ///strSql.Append(" SELECT COLTRN.COLLECTIONS_TBL_FK COLTRNFK, COLTRN.INVOICE_REF_NR INVREFNO,")
            ///strSql.Append(" TO_CHAR(INV.INVOICE_DATE,'" & dateFormat & "') INVDT, COLTRN.RECD_AMOUNT_HDR_CURR RECAMT")
            ///strSql.Append(" , curr.CURRENCY_ID CURRNM, COLMOD.EXCHANGE_RATE ROE, (COLTRN.RECD_AMOUNT_HDR_CURR * COLMOD.EXCHANGE_RATE) LOCAMT") 'new
            ///strSql.Append(" FROM COLLECTIONS_TRN_TBL COLTRN, COLLECTIONS_TBL COL, INV_CUST_SEA_EXP_TBL INV")
            ///strSql.Append(" , CURRENCY_TYPE_MST_TBL curr, COLLECTIONS_MODE_TRN_TBL COLMOD") 'new
            ///strSql.Append(" WHERE COLTRN.COLLECTIONS_TBL_FK = COL.COLLECTIONS_TBL_PK AND COLTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO")
            ///strSql.Append(" AND COL.PROCESS_TYPE = 1 AND COL.BUSINESS_TYPE = 2 AND COLTRN.FROM_INV_OR_CONSOL_INV = 1")
            ///strSql.Append(" and colmod.collections_tbl_fk = COL.COLLECTIONS_TBL_PK(+)") 'new
            ///strSql.Append(" and col.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)") 'new
            ///strSql.Append(" and col.collections_tbl_pk =" & colpk)

            ///strSql.Append(" UNION")
            ///strSql.Append(" SELECT COLTRN.COLLECTIONS_TBL_FK COLTRNFK, COLTRN.INVOICE_REF_NR INVREFNO,")
            ///strSql.Append(" TO_CHAR(INV.INVOICE_DATE,'" & dateFormat & "') INVDT, COLTRN.RECD_AMOUNT_HDR_CURR RECAMT")
            ///strSql.Append(" , curr.CURRENCY_ID CURRNM, COLMOD.EXCHANGE_RATE ROE, (COLTRN.RECD_AMOUNT_HDR_CURR * COLMOD.EXCHANGE_RATE) LOCAMT") 'new
            ///strSql.Append(" FROM COLLECTIONS_TRN_TBL COLTRN, COLLECTIONS_TBL COL, INV_CUST_AIR_EXP_TBL INV")
            ///strSql.Append(" , CURRENCY_TYPE_MST_TBL curr, COLLECTIONS_MODE_TRN_TBL COLMOD") 'new
            ///strSql.Append(" WHERE COLTRN.COLLECTIONS_TBL_FK = COL.COLLECTIONS_TBL_PK AND COLTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO")
            ///strSql.Append(" AND COL.PROCESS_TYPE = 1 AND COL.BUSINESS_TYPE = 1 AND COLTRN.FROM_INV_OR_CONSOL_INV = 1")
            ///strSql.Append(" and colmod.collections_tbl_fk = COL.COLLECTIONS_TBL_PK(+)") 'new
            ///strSql.Append(" and col.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)") 'new
            ///strSql.Append(" and col.collections_tbl_pk =" & colpk)

            ///strSql.Append(" UNION")
            ///strSql.Append(" SELECT COLTRN.COLLECTIONS_TBL_FK COLTRNFK, COLTRN.INVOICE_REF_NR INVREFNO,")
            ///strSql.Append(" TO_CHAR(INV.INVOICE_DATE,'" & dateFormat & "') INVDT, COLTRN.RECD_AMOUNT_HDR_CURR RECAMT")
            ///strSql.Append(" , curr.CURRENCY_ID CURRNM, COLMOD.EXCHANGE_RATE ROE, (COLTRN.RECD_AMOUNT_HDR_CURR * COLMOD.EXCHANGE_RATE) LOCAMT") 'new
            ///strSql.Append(" FROM COLLECTIONS_TRN_TBL COLTRN, COLLECTIONS_TBL COL, INV_CUST_SEA_IMP_TBL INV")
            ///strSql.Append(" , CURRENCY_TYPE_MST_TBL curr, COLLECTIONS_MODE_TRN_TBL COLMOD") 'new
            ///strSql.Append(" WHERE COLTRN.COLLECTIONS_TBL_FK = COL.COLLECTIONS_TBL_PK AND COLTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO")
            ///strSql.Append(" AND COL.PROCESS_TYPE = 2 AND COL.BUSINESS_TYPE = 2 AND COLTRN.FROM_INV_OR_CONSOL_INV = 1")
            ///strSql.Append(" and colmod.collections_tbl_fk = COL.COLLECTIONS_TBL_PK(+)") 'new
            ///strSql.Append(" and col.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)") 'new
            ///strSql.Append(" and col.collections_tbl_pk =" & colpk)

            ///strSql.Append(" UNION")
            ///strSql.Append(" SELECT COLTRN.COLLECTIONS_TBL_FK COLTRNFK, COLTRN.INVOICE_REF_NR INVREFNO,")
            ///strSql.Append(" TO_CHAR(INV.INVOICE_DATE,'" & dateFormat & "') INVDT, COLTRN.RECD_AMOUNT_HDR_CURR RECAMT")
            ///strSql.Append(" , curr.CURRENCY_ID CURRNM, COLMOD.EXCHANGE_RATE ROE, (COLTRN.RECD_AMOUNT_HDR_CURR * COLMOD.EXCHANGE_RATE) LOCAMT") 'new
            ///strSql.Append(" FROM COLLECTIONS_TRN_TBL COLTRN, COLLECTIONS_TBL COL, INV_CUST_AIR_IMP_TBL INV")
            ///strSql.Append(" , CURRENCY_TYPE_MST_TBL curr, COLLECTIONS_MODE_TRN_TBL COLMOD") 'new
            ///strSql.Append(" WHERE COLTRN.COLLECTIONS_TBL_FK = COL.COLLECTIONS_TBL_PK AND COLTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO")
            ///strSql.Append(" AND COL.PROCESS_TYPE = 2 AND COL.BUSINESS_TYPE = 1 AND COLTRN.FROM_INV_OR_CONSOL_INV = 1")
            ///strSql.Append(" and colmod.collections_tbl_fk = COL.COLLECTIONS_TBL_PK(+)") 'new
            ///strSql.Append(" and col.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)") 'new
            ///strSql.Append(" and col.collections_tbl_pk =" & colpk)
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
        #endregion

        #region "Credit Limit"
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
        public double FetchcolCustCreditAmt(string CustName)
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
                strSQL = strSQL  + " where a.customer_name in ('" + Customer + "')";
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
        public object checkstatus(ref DataSet dsAgtCollection, int JobPk = 0, short BizType = 0, short Process = 0)
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
                            if (!string.IsNullOrEmpty(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"].ToString()))
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

                if (IsDPAgent == true & IsCBAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.Job_Card_Trn_Pk=" + JobPk);
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
                    sb.Append("  AND JOB.DPAGENT_STATUS=1");
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                }
                else if (IsDPAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.Job_Card_Trn_Pk=" + JobPk);
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
                    sb.Append("  AND JOB.DPAGENT_STATUS=1");
                }
                else if (IsCBAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.Job_Card_Trn_Pk=" + JobPk);
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
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                }
                else
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append(" where JOB.Job_Card_Trn_Pk=" + JobPk);
                    sb.Append(" AND JOB.COLLECTION_STATUS=1");
                    sb.Append(" AND JOB.PAYEMENT_STATUS=1");
                    if (Process == 1)
                    {
                        sb.Append(" AND JOB.HBL_RELEASED_STATUS=1");
                    }
                    else
                    {
                        sb.Append("  AND JOB.DO_STATUS=1");
                    }
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
                    str += " WHERE j.Job_Card_Trn_Pk=" + JobPk;
                }
                else if (BizType == 2 & Process == 2)
                {
                    str = "UPDATE JOB_CARD_TRN  JA SET ";
                    str += "   JA.JOB_CARD_STATUS = 2, JA.JOB_CARD_CLOSED_ON = SYSDATE ";
                    str += " WHERE JA.JOB_CARD_TRN_PK=" + JobPk;
                }
                else if (BizType == 1 & Process == 1)
                {
                    str = "UPDATE JOB_CARD_TRN JAE SET ";
                    str += "   JAE.JOB_CARD_STATUS = 2, JAE.JOB_CARD_CLOSED_ON = SYSDATE";
                    str += " WHERE JAE.JOB_CARD_TRN_PK=" + JobPk;
                }
                else if (BizType == 1 & Process == 2)
                {
                    str = "UPDATE JOB_CARD_TRN JAI SET ";
                    str += "   JAI.JOB_CARD_STATUS = 2, JAI.JOB_CARD_CLOSED_ON = SYSDATE";
                    str += " WHERE  JAI.JOB_CARD_TRN_PK=" + JobPk;
                }

                var _with9 = updCmdUser;
                _with9.Connection = objWK.MyConnection;
                _with9.Transaction = TRAN;
                _with9.CommandType = CommandType.Text;
                _with9.CommandText = str;
                intIns = Convert.ToInt16(_with9.ExecuteNonQuery());
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

        public ArrayList UpdateCollectionJobcard(int JobPk = 0, short BizType = 0, short Process = 0)
        {


            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            arrMessage.Clear();
            try
            {
                updCmdUser.Transaction = TRAN;
                if (BizType == 2 & Process == 1)
                {
                    str = "UPDATE JOB_CARD_TRN  j SET ";
                    str += "   j.collection_status = 1, j.collection_date = SYSDATE";
                    str += " WHERE j.Job_Card_Trn_Pk=" + JobPk;
                }
                else if (BizType == 2 & Process == 2)
                {
                    str = "UPDATE JOB_CARD_TRN  JA SET ";
                    str += "   JA.collection_status = 1, JA.collection_date=SYSDATE";
                    str += " WHERE JA.JOB_CARD_TRN_PK=" + JobPk;
                }
                else if (BizType == 1 & Process == 1)
                {
                    str = "UPDATE JOB_CARD_TRN JAE SET ";
                    str += "   JAE.Collection_Status = 1, JAE.collection_date=SYSDATE";
                    str += " WHERE JAE.JOB_CARD_TRN_PK=" + JobPk;
                }
                else if (BizType == 1 & Process == 2)
                {
                    str = "UPDATE JOB_CARD_TRN JAI SET ";
                    str += "   JAI.Collection_Status = 1, JAI.collection_date=SYSDATE";
                    str += " WHERE  JAI.JOB_CARD_TRN_PK=" + JobPk;
                }

                var _with10 = updCmdUser;
                _with10.Connection = objWK.MyConnection;
                _with10.Transaction = TRAN;
                _with10.CommandType = CommandType.Text;
                _with10.CommandText = str;
                intIns = Convert.ToInt16(_with10.ExecuteNonQuery());
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
                    strSql.Append("select  GET_EX_RATE(" + intBaseCurrPk + "," + BaseCurrency + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL" );
                }
                else
                {
                    strSql.Append("select  GET_EX_RATE1(" + intBaseCurrPk + "," + BaseCurrency + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )," + extype + ") FROM DUAL" );
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
        #endregion

        //Added by jitendra 
        public DataSet Fetch_Cust(string InvRefNo)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strQuery.Append("select cmt.customer_name from " );
                strQuery.Append("consol_invoice_tbl cit ," );
                strQuery.Append("customer_mst_tbl cmt " );
                strQuery.Append("where " );
                strQuery.Append("cit.customer_mst_fk=cmt.customer_mst_pk(+)" );
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
        public DataSet Fetch_JobCardNr(string InvRefNo, short BizType, short Process)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //'Sea
                if (BizType == 2 & Process == 1)
                {
                    strQuery.Append("  SELECT DISTINCT JOB_EXP.JOBCARD_REF_NO FROM");
                    strQuery.Append("  CONSOL_INVOICE_TRN_TBL INVTRN,CONSOL_INVOICE_TBL INV,");
                    strQuery.Append("  JOB_CARD_TRN JOB_EXP");
                    strQuery.Append("  WHERE JOB_EXP.Job_Card_Trn_Pk=INVTRN.JOB_CARD_FK");
                    strQuery.Append("  AND INV.CONSOL_INVOICE_PK=INVTRN.CONSOL_INVOICE_FK");
                    strQuery.Append("  AND INV.INVOICE_REF_NO='" + InvRefNo + "'");
                }
                else if (BizType == 2 & Process == 2)
                {
                    strQuery.Append("  SELECT DISTINCT JOB_IMP.JOBCARD_REF_NO FROM");
                    strQuery.Append("  CONSOL_INVOICE_TRN_TBL INVTRN,CONSOL_INVOICE_TBL INV,");
                    strQuery.Append("  JOB_CARD_TRN JOB_IMP");
                    strQuery.Append("  WHERE JOB_IMP.JOB_CARD_TRN_PK=INVTRN.JOB_CARD_FK");
                    strQuery.Append("  AND INV.CONSOL_INVOICE_PK=INVTRN.CONSOL_INVOICE_FK");
                    strQuery.Append("  AND INV.INVOICE_REF_NO='" + InvRefNo + "'");
                    //'Air
                }
                else if (BizType == 1 & Process == 1)
                {
                    strQuery.Append("  SELECT DISTINCT JOB_EXP.JOBCARD_REF_NO FROM");
                    strQuery.Append("  CONSOL_INVOICE_TRN_TBL INVTRN,CONSOL_INVOICE_TBL INV,");
                    strQuery.Append("  JOB_CARD_TRN JOB_EXP");
                    strQuery.Append("  WHERE JOB_EXP.JOB_CARD_TRN_PK=INVTRN.JOB_CARD_FK");
                    strQuery.Append("  AND INV.CONSOL_INVOICE_PK=INVTRN.CONSOL_INVOICE_FK");
                    strQuery.Append("  AND INV.INVOICE_REF_NO='" + InvRefNo + "'");
                }
                else if (BizType == 1 & Process == 2)
                {
                    strQuery.Append("  SELECT DISTINCT JOB_IMP.JOBCARD_REF_NO FROM");
                    strQuery.Append("  CONSOL_INVOICE_TRN_TBL INVTRN,CONSOL_INVOICE_TBL INV,");
                    strQuery.Append("  JOB_CARD_TRN JOB_IMP ");
                    strQuery.Append("  WHERE JOB_IMP.JOB_CARD_TRN_PK=INVTRN.JOB_CARD_FK");
                    strQuery.Append("  AND INV.CONSOL_INVOICE_PK=INVTRN.CONSOL_INVOICE_FK");
                    strQuery.Append("  AND INV.INVOICE_REF_NO='" + InvRefNo + "'");
                }
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }
        #region "Fetch Currency"
        //adding by thiyagarajan on 3/12/08 for location based currency task
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
        //as per discussion with balaji added by latha to fetch only those currencies having exchange rates in the grid
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
            //strSQL = strSQL & " WHERE C.CURRENCY_MST_PK=" & HttpContext.Current.Session("CURRENCY_MST_PK")
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
            //strSQL = strSQL & " AND " & HttpContext.Current.Session("CURRENCY_MST_PK") & " <> CMT.CURRENCY_MST_PK"
            strSQL = strSQL + " AND " + DDLCurrency + " <> CMT.CURRENCY_MST_PK";
            //strSQL = strSQL & " AND EXC.EXCH_RATE_TYPE_FK = " & ExType
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
            //strSQL = strSQL & "order by CURRENCY_ID"
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
        #endregion

        #region " Fetch Exchange Rate Bassed On Exchange Rate Type -Hidden"
        public DataSet FetchExchTypeROE(Int64 baseCurrency, Int64 ExType = 1, string Coldate = "", int VslVoyFK = 0)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strBuilder.Append(" SELECT DISTINCT");
                strBuilder.Append(" CMT.CURRENCY_MST_PK,");
                strBuilder.Append(" CMT.CURRENCY_ID,");
                if (ExType == 3 | ExType == 2)
                {
                    strBuilder.Append(" ROUND(GET_EX_RATE1( ");
                    strBuilder.Append("CMT.CURRENCY_MST_PK, " + baseCurrency + ",ROUND(TO_DATE(' " + Coldate + "',dateFormat))," + ExType + "," + VslVoyFK + "),6) AS ROE");
                }
                else
                {
                    strBuilder.Append(" ROUND(GET_EX_RATE( ");
                    strBuilder.Append("CMT.CURRENCY_MST_PK, " + baseCurrency + ",ROUND(TO_DATE(' " + Coldate + "',dateFormat))),6) AS ROE");
                }
                //AND TO_DATE(' " & RFQDate & "',dateFormat) between EXC.FROM_DATE and nvl(TO_DATE,NULL_DATE_FORMAT)
                strBuilder.Append(" FROM");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CMT , EXCHANGE_RATE_TRN EXC");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" CMT.ACTIVE_FLAG = 1");
                strBuilder.Append(" AND EXC.CURRENCY_MST_FK=CMT.CURRENCY_MST_PK ");
                strBuilder.Append(" AND EXC.CURRENCY_MST_BASE_FK =" + baseCurrency);
                strBuilder.Append(" AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK");
                strBuilder.Append(" AND EXC.VOYAGE_TRN_FK IS NULL");
                //strBuilder.Append(" AND EXC.EXCH_RATE_TYPE_FK = " & ExType & "")
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
        #endregion

        #region "FetchPK"
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
        #endregion

        #region "Fetch Collection Report Details"
        public DataSet FetchReportHeader(int COLPK_IN, int CUR_IN)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            try
            {
                sb.Append("SELECT CTRN.TDS_PERCENTAGE PROTYPE,");
                sb.Append("       CTRN.TDS_AMOUNT BIZTYPE,");
                sb.Append("       CLN.COLLECTIONS_TBL_PK COLPK,");
                sb.Append("       CLN.COLLECTIONS_REF_NO COLREFNO,");
                sb.Append("       TO_CHAR(CLN.COLLECTIONS_DATE, 'dd/MM/yyyy') COLDATE,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID,");
                sb.Append("       ROUND((CTRN.RECD_AMOUNT_HDR_CURR *");
                sb.Append("             (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                 FROM DUAL)),");
                sb.Append("             2) RECAMT,");
                sb.Append("       (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("          FROM DUAL) ROE,");
                sb.Append("       ROUND((CTRN.RECD_AMOUNT_HDR_CURR) / (CASE");
                sb.Append("               WHEN (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                       FROM DUAL) = 0 THEN");
                sb.Append("                1");
                sb.Append("               ELSE");
                sb.Append("                (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                   FROM DUAL)");
                sb.Append("             END),");
                sb.Append("             2) AMTINLOC,");
                sb.Append("       (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("         WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) RECIEVED,");
                sb.Append("       ROUND(NVL(INV.NET_PAYABLE * (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                       " + CUR_IN + ",");
                sb.Append("                                                       INV.INVOICE_DATE)");
                sb.Append("                                      FROM DUAL) -");
                sb.Append("                 (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) RECEIVABLE,");
                sb.Append("       NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) CURRRECEIPT,");
                sb.Append("       0 LOCRECEIPT");
                sb.Append("  FROM INV_CUST_SEA_EXP_TBL     INV,");
                sb.Append("       INV_CUST_TRN_SEA_EXP_TBL INVFRT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL    CUMT,");
                sb.Append("       COLLECTIONS_TBL          CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL      CTRN");
                sb.Append(" WHERE INV.INV_CUST_SEA_EXP_PK = INVFRT.INV_CUST_SEA_EXP_FK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN);
                sb.Append("   AND CLN.PROCESS_TYPE = 1");
                sb.Append("   AND CLN.BUSINESS_TYPE = 2");
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append(" UNION ");
                sb.Append("SELECT CTRN.TDS_PERCENTAGE PROTYPE,");
                sb.Append("       CTRN.TDS_AMOUNT BIZTYPE,");
                sb.Append("       CLN.COLLECTIONS_TBL_PK COLPK,");
                sb.Append("       CLN.COLLECTIONS_REF_NO COLREFNO,");
                sb.Append("       TO_CHAR(CLN.COLLECTIONS_DATE, 'dd/MM/yyyy') COLDATE,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID,");
                sb.Append("       INV.NET_RECEIVABLE RECAMT,");
                sb.Append("       (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("          FROM DUAL) ROE,");
                sb.Append("       ROUND((INV.NET_RECEIVABLE) * (CASE");
                sb.Append("               WHEN (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                        " + CUR_IN + ",");
                sb.Append("                                        CLN.COLLECTIONS_DATE)");
                sb.Append("                       FROM DUAL) = 0 THEN");
                sb.Append("                1");
                sb.Append("               ELSE");
                sb.Append("                (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                   FROM DUAL)");
                sb.Append("             END),");
                sb.Append("             2) AMTINLOC,");
                sb.Append("       (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("         WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) RECIEVED,");
                sb.Append("       ROUND(NVL(INV.NET_RECEIVABLE * (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                          " + CUR_IN + ",");
                sb.Append("                                                          CLN.COLLECTIONS_DATE)");
                sb.Append("                                         FROM DUAL) -");
                sb.Append("                 (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) RECEIVABLE,");
                sb.Append("       NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) CURRRECEIPT,");
                sb.Append("       0 LOCRECEIPT");
                sb.Append("  FROM CONSOL_INVOICE_TBL    INV,");
                sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("       COLLECTIONS_TBL       CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL   CTRN");
                sb.Append(" WHERE CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN);
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("   AND CLN.PROCESS_TYPE = INV.PROCESS_TYPE");
                sb.Append("   AND CLN.BUSINESS_TYPE = INV.BUSINESS_TYPE");
                sb.Append("   AND CLN.PROCESS_TYPE = 1");
                sb.Append("   AND CLN.BUSINESS_TYPE = 2");
                sb.Append(" UNION ");
                sb.Append("SELECT CTRN.TDS_PERCENTAGE PROTYPE,");
                sb.Append("       CTRN.TDS_AMOUNT BIZTYPE,");
                sb.Append("       CLN.COLLECTIONS_TBL_PK COLPK,");
                sb.Append("       CLN.COLLECTIONS_REF_NO COLREFNO,");
                sb.Append("       TO_CHAR(CLN.COLLECTIONS_DATE, 'dd/MM/yyyy') COLDATE,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID,");
                sb.Append("       ROUND((CTRN.RECD_AMOUNT_HDR_CURR *");
                sb.Append("             (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                 FROM DUAL)),");
                sb.Append("             2) RECAMT,");
                sb.Append("       (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("          FROM DUAL) ROE,");
                sb.Append("       ROUND((CTRN.RECD_AMOUNT_HDR_CURR) / (CASE");
                sb.Append("               WHEN (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                       FROM DUAL) = 0 THEN");
                sb.Append("                1");
                sb.Append("               ELSE");
                sb.Append("                (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                   FROM DUAL)");
                sb.Append("             END),");
                sb.Append("             2) AMTINLOC,");
                sb.Append("       (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("         WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) RECIEVED,");
                sb.Append("       ROUND(NVL(INV.NET_PAYABLE * (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                       " + CUR_IN + ",");
                sb.Append("                                                       INV.INVOICE_DATE)");
                sb.Append("                                      FROM DUAL) -");
                sb.Append("                 (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) RECEIVABLE,");
                sb.Append("       NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) CURRRECEIPT,");
                sb.Append("       0 LOCRECEIPT");
                sb.Append("  FROM INV_CUST_AIR_EXP_TBL     INV,");
                sb.Append("       INV_CUST_TRN_AIR_EXP_TBL INVFRT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL    CUMT,");
                sb.Append("       COLLECTIONS_TBL          CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL      CTRN");
                sb.Append(" WHERE INV.INV_CUST_AIR_EXP_PK = INVFRT.INV_CUST_AIR_EXP_FK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND CLN.PROCESS_TYPE = 1");
                sb.Append("   AND CLN.BUSINESS_TYPE = 1");
                sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN);
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append(" UNION ");
                sb.Append("SELECT CTRN.TDS_PERCENTAGE PROTYPE,");
                sb.Append("       CTRN.TDS_AMOUNT BIZTYPE,");
                sb.Append("       CLN.COLLECTIONS_TBL_PK COLPK,");
                sb.Append("       CLN.COLLECTIONS_REF_NO COLREFNO,");
                sb.Append("       TO_CHAR(CLN.COLLECTIONS_DATE, 'dd/MM/yyyy') COLDATE,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID,");
                sb.Append("       INV.NET_RECEIVABLE RECAMT,");
                sb.Append("       (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("          FROM DUAL) ROE,");
                sb.Append("       ROUND((INV.NET_RECEIVABLE) * (CASE");
                sb.Append("               WHEN (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                        " + CUR_IN + ",");
                sb.Append("                                        CLN.COLLECTIONS_DATE)");
                sb.Append("                       FROM DUAL) = 0 THEN");
                sb.Append("                1");
                sb.Append("               ELSE");
                sb.Append("                (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                   FROM DUAL)");
                sb.Append("             END),");
                sb.Append("             2) AMTINLOC,");
                sb.Append("       (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("         WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) RECIEVED,");
                sb.Append("       ROUND(NVL(INV.NET_RECEIVABLE * (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                          " + CUR_IN + ",");
                sb.Append("                                                          CLN.COLLECTIONS_DATE)");
                sb.Append("                                         FROM DUAL) -");
                sb.Append("                 (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) RECEIVABLE,");
                sb.Append("       NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) CURRRECEIPT,");
                sb.Append("       0 LOCRECEIPT");
                sb.Append("  FROM CONSOL_INVOICE_TBL    INV,");
                sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("       COLLECTIONS_TBL       CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL   CTRN");
                sb.Append(" WHERE CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN);
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("   AND CLN.PROCESS_TYPE = INV.PROCESS_TYPE");
                sb.Append("   AND CLN.BUSINESS_TYPE = INV.BUSINESS_TYPE");
                sb.Append("   AND CLN.PROCESS_TYPE = 1");
                sb.Append("   AND CLN.BUSINESS_TYPE = 1");
                sb.Append(" UNION ");
                sb.Append("SELECT CTRN.TDS_PERCENTAGE PROTYPE,");
                sb.Append("       CTRN.TDS_AMOUNT BIZTYPE,");
                sb.Append("       CLN.COLLECTIONS_TBL_PK COLPK,");
                sb.Append("       CLN.COLLECTIONS_REF_NO COLREFNO,");
                sb.Append("       TO_CHAR(CLN.COLLECTIONS_DATE, 'dd/MM/yyyy') COLDATE,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID,");
                sb.Append("       ROUND((CTRN.RECD_AMOUNT_HDR_CURR *");
                sb.Append("             (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                 FROM DUAL)),");
                sb.Append("             2) RECAMT,");
                sb.Append("       (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("          FROM DUAL) ROE,");
                sb.Append("       ROUND((CTRN.RECD_AMOUNT_HDR_CURR) / (CASE");
                sb.Append("               WHEN (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                       FROM DUAL) = 0 THEN");
                sb.Append("                1");
                sb.Append("               ELSE");
                sb.Append("                (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                   FROM DUAL)");
                sb.Append("             END),");
                sb.Append("             2) AMTINLOC,");
                sb.Append("       (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("         WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) RECIEVED,");
                sb.Append("       ROUND(NVL(INV.NET_PAYABLE * (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                       " + CUR_IN + ",");
                sb.Append("                                                       INV.INVOICE_DATE)");
                sb.Append("                                      FROM DUAL) -");
                sb.Append("                 (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) RECEIVABLE,");
                sb.Append("       NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) CURRRECEIPT,");
                sb.Append("       0 LOCRECEIPT");
                sb.Append("  FROM INV_CUST_SEA_IMP_TBL     INV,");
                sb.Append("       INV_CUST_TRN_SEA_IMP_TBL INVFRT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL    CUMT,");
                sb.Append("       COLLECTIONS_TBL          CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL      CTRN");
                sb.Append(" WHERE INV.INV_CUST_SEA_IMP_PK = INVFRT.INV_CUST_SEA_IMP_FK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.PROCESS_TYPE = 2");
                sb.Append("   AND CLN.BUSINESS_TYPE = 2");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN);
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append(" UNION ");
                sb.Append("SELECT CTRN.TDS_PERCENTAGE PROTYPE,");
                sb.Append("       CTRN.TDS_AMOUNT BIZTYPE,");
                sb.Append("       CLN.COLLECTIONS_TBL_PK COLPK,");
                sb.Append("       CLN.COLLECTIONS_REF_NO COLREFNO,");
                sb.Append("       TO_CHAR(CLN.COLLECTIONS_DATE, 'dd/MM/yyyy') COLDATE,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID,");
                sb.Append("       INV.NET_RECEIVABLE RECAMT,");
                sb.Append("       (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("          FROM DUAL) ROE,");
                sb.Append("       ROUND((INV.NET_RECEIVABLE) * (CASE");
                sb.Append("               WHEN (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                        " + CUR_IN + ",");
                sb.Append("                                        CLN.COLLECTIONS_DATE)");
                sb.Append("                       FROM DUAL) = 0 THEN");
                sb.Append("                1");
                sb.Append("               ELSE");
                sb.Append("                (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                   FROM DUAL)");
                sb.Append("             END),");
                sb.Append("             2) AMTINLOC,");
                sb.Append("       (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("         WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) RECIEVED,");
                sb.Append("       ROUND(NVL(INV.NET_RECEIVABLE * (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                          " + CUR_IN + ",");
                sb.Append("                                                          CLN.COLLECTIONS_DATE)");
                sb.Append("                                         FROM DUAL) -");
                sb.Append("                 (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) RECEIVABLE,");
                sb.Append("       NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) CURRRECEIPT,");
                sb.Append("       0 LOCRECEIPT");
                sb.Append("  FROM CONSOL_INVOICE_TBL    INV,");
                sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("       COLLECTIONS_TBL       CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL   CTRN");
                sb.Append(" WHERE CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN);
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("   AND CLN.PROCESS_TYPE = INV.PROCESS_TYPE");
                sb.Append("   AND CLN.BUSINESS_TYPE = INV.BUSINESS_TYPE");
                sb.Append("   AND CLN.PROCESS_TYPE = 2");
                sb.Append("   AND CLN.BUSINESS_TYPE = 2");
                sb.Append(" UNION ");
                sb.Append("SELECT CTRN.TDS_PERCENTAGE PROTYPE,");
                sb.Append("       CTRN.TDS_AMOUNT BIZTYPE,");
                sb.Append("       CLN.COLLECTIONS_TBL_PK COLPK,");
                sb.Append("       CLN.COLLECTIONS_REF_NO COLREFNO,");
                sb.Append("       TO_CHAR(CLN.COLLECTIONS_DATE, 'dd/MM/yyyy') COLDATE,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID,");
                sb.Append("       ROUND((CTRN.RECD_AMOUNT_HDR_CURR *");
                sb.Append("             (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                 FROM DUAL)),");
                sb.Append("             2) RECAMT,");
                sb.Append("       (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("          FROM DUAL) ROE,");
                sb.Append("       ROUND((CTRN.RECD_AMOUNT_HDR_CURR) / (CASE");
                sb.Append("               WHEN (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                       FROM DUAL) = 0 THEN");
                sb.Append("                1");
                sb.Append("               ELSE");
                sb.Append("                (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", INV.INVOICE_DATE)");
                sb.Append("                   FROM DUAL)");
                sb.Append("             END),");
                sb.Append("             2) AMTINLOC,");
                sb.Append("       (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("         WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) RECIEVED,");
                sb.Append("       ROUND(NVL(INV.NET_PAYABLE * (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                       " + CUR_IN + ",");
                sb.Append("                                                       INV.INVOICE_DATE)");
                sb.Append("                                      FROM DUAL) -");
                sb.Append("                 (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) RECEIVABLE,");
                sb.Append("       NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) CURRRECEIPT,");
                sb.Append("       0 LOCRECEIPT");
                sb.Append("  FROM INV_CUST_AIR_IMP_TBL     INV,");
                sb.Append("       INV_CUST_TRN_AIR_IMP_TBL INVFRT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL    CUMT,");
                sb.Append("       COLLECTIONS_TBL          CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL      CTRN");
                sb.Append(" WHERE INV.INV_CUST_AIR_IMP_PK = INVFRT.INV_CUST_AIR_IMP_FK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN);
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("   AND CLN.PROCESS_TYPE = 2");
                sb.Append("   AND CLN.BUSINESS_TYPE = 1");
                sb.Append(" UNION ");
                sb.Append("SELECT CTRN.TDS_PERCENTAGE PROTYPE,");
                sb.Append("       CTRN.TDS_AMOUNT BIZTYPE,");
                sb.Append("       CLN.COLLECTIONS_TBL_PK COLPK,");
                sb.Append("       CLN.COLLECTIONS_REF_NO COLREFNO,");
                sb.Append("       TO_CHAR(CLN.COLLECTIONS_DATE, 'dd/MM/yyyy') COLDATE,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       TO_CHAR(INV.INVOICE_DATE, 'dd/MM/yyyy') INVOICE_DATE,");
                sb.Append("       CUMT.CURRENCY_ID,");
                sb.Append("       INV.NET_RECEIVABLE RECAMT,");
                sb.Append("       (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("          FROM DUAL) ROE,");
                sb.Append("       ROUND((INV.NET_RECEIVABLE) * (CASE");
                sb.Append("               WHEN (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                        " + CUR_IN + ",");
                sb.Append("                                        CLN.COLLECTIONS_DATE)");
                sb.Append("                       FROM DUAL) = 0 THEN");
                sb.Append("                1");
                sb.Append("               ELSE");
                sb.Append("                (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK, " + CUR_IN + ", CLN.COLLECTIONS_DATE)");
                sb.Append("                   FROM DUAL)");
                sb.Append("             END),");
                sb.Append("             2) AMTINLOC,");
                sb.Append("       (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("         WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) RECIEVED,");
                sb.Append("       ROUND(NVL(INV.NET_RECEIVABLE * (SELECT GET_EX_RATE(INV.CURRENCY_MST_FK,");
                sb.Append("                                                          " + CUR_IN + ",");
                sb.Append("                                                          CLN.COLLECTIONS_DATE)");
                sb.Append("                                         FROM DUAL) -");
                sb.Append("                 (SELECT SUM(CRN.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                    FROM COLLECTIONS_TRN_TBL CRN");
                sb.Append("                   WHERE CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                 0),");
                sb.Append("             2) RECEIVABLE,");
                sb.Append("       NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0) CURRRECEIPT,");
                sb.Append("       0 LOCRECEIPT");
                sb.Append("  FROM CONSOL_INVOICE_TBL    INV,");
                sb.Append("       CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("       COLLECTIONS_TBL       CLN,");
                sb.Append("       COLLECTIONS_TRN_TBL   CTRN");
                sb.Append(" WHERE CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
                sb.Append("   AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN);
                sb.Append("   AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
                sb.Append("   AND CLN.PROCESS_TYPE = INV.PROCESS_TYPE");
                sb.Append("   AND CLN.BUSINESS_TYPE = INV.BUSINESS_TYPE");
                sb.Append("   AND CLN.PROCESS_TYPE = 2");
                sb.Append("   AND CLN.BUSINESS_TYPE = 1");

                return ObjWk.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }
        #endregion

        #region "Fetch JobType"
        public string FetchJobType(int ColPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append(" SELECT ROWTOCOL('SELECT DECODE(CITT.JOB_TYPE,");
                sb.Append(" ''1'',");
                sb.Append(" ''JobCard'',");
                sb.Append(" ''2'',");
                sb.Append(" ''CustomsBrokerage'',");
                sb.Append(" ''3'',");
                sb.Append(" ''TansportNote'')from COLLECTIONS_TRN_TBL CTT,CONSOL_INVOICE_TBL CIT ,CONSOL_INVOICE_TRN_TBL CITT");
                sb.Append(" WHERE CTT.INVOICE_REF_NR=CIT.INVOICE_REF_NO AND CIT.CONSOL_INVOICE_PK=CITT.CONSOL_INVOICE_FK AND CTT.COLLECTIONS_TBL_FK= " + ColPK + "') FROM DUAL");

                return ObjWk.ExecuteScaler(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }
        public DataSet FetchJobCount(string ConInvPK = "", string JobType = "JobCard")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                if ((ConInvPK == null))
                {
                    ConInvPK = "0";
                }
                if (string.IsNullOrEmpty(JobType))
                {
                    JobType = "JobCard";
                }
                if (JobType == "JobCard")
                {
                    sb.Append("  SELECT DISTINCT NVL(JC.VOYAGE_TRN_FK,0)VOYAGE_TRN_FK ");
                    sb.Append("  FROM CONSOL_INVOICE_TBL     CT,CONSOL_INVOICE_TRN_TBL CTRN,JOB_CARD_TRN           JC");
                    sb.Append("   WHERE CT.CONSOL_INVOICE_PK = CTRN.CONSOL_INVOICE_FK");
                    sb.Append(" AND CTRN.JOB_CARD_FK = JC.JOB_CARD_TRN_PK  AND CTRN.JOB_TYPE = 1 AND CT.CONSOL_INVOICE_PK IN( " + ConInvPK + ")");
                }
                else if (JobType == "CustomsBrokerage")
                {
                    sb.Append("  SELECT DISTINCT nvl(JC.VOYAGE_TRN_FK,0)VOYAGE_TRN_FK ");
                    sb.Append("  FROM CONSOL_INVOICE_TBL     CT,CONSOL_INVOICE_TRN_TBL CTRN,CBJC_TBL     JC");
                    sb.Append("   WHERE CT.CONSOL_INVOICE_PK = CTRN.CONSOL_INVOICE_FK");
                    sb.Append(" AND CTRN.JOB_CARD_FK = JC.CBJC_PK   AND CTRN.JOB_TYPE = 2 AND CT.CONSOL_INVOICE_PK IN( " + ConInvPK + ")");
                }
                else
                {
                    sb.Append("  SELECT DISTINCT NVL(JC.VSL_VOY_FK,0)VOYAGE_TRN_FK ");
                    sb.Append("  FROM CONSOL_INVOICE_TBL     CT,CONSOL_INVOICE_TRN_TBL CTRN,TRANSPORT_INST_SEA_TBL   JC");
                    sb.Append("   WHERE CT.CONSOL_INVOICE_PK = CTRN.CONSOL_INVOICE_FK");
                    sb.Append(" AND CTRN.JOB_CARD_FK = JC.TRANSPORT_INST_SEA_PK   AND CTRN.JOB_TYPE = 3 AND CT.CONSOL_INVOICE_PK IN( " + ConInvPK + ")");
                }

                return ObjWF.GetDataSet(sb.ToString());
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
        #endregion
    }
}