#region "Comments"

//'***************************************************************************************************************
//'*  Company Name            :
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By              :    Santosh on 10-May-16
//'*  Module/Project Leader   :    Santosh Pisipati
//'*  Description             :
//'*  Module/Form/Class Name  :
//'*  Configuration ID        :
//'***************************************************************************************************************
//'*  Revision History
//'***************************************************************************************************************
//'*  Modified Date(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    public class clsInvoiceAgentEntryAir : CommonFeatures
    {

        #region " Fetch"
        //This function is called to fetch all the job card cost elements
        //of a selected job card.
        //Used to populate the job card cost elements grid in the invoice page
        public DataSet FetchCostDetails(long nJobCardPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;

            strSQL = " SELECT ";
            strSQL += " JOBCOST.JOB_TRN_PIA_PK,";
            strSQL += " JOBCOST.JOB_CARD_TRN_FK,";
            strSQL += " JOBCOST.COST_ELEMENT_MST_FK,";
            strSQL += " JOBCOST.CURRENCY_MST_FK,";
            strSQL += " CEMT.COST_ELEMENT_NAME,";
            strSQL += " CUMT.CURRENCY_ID,";
            strSQL += " JOBCOST.ESTIMATED_AMT,";
            strSQL += " JOBCOST.INVOICE_AMT,";
            //strSQL &= vbCrLf & " (CASE WHEN JOBCOST.INV_AGENT_TRN_FK IS NULL THEN NULL "
            //strSQL &= vbCrLf & "   ELSE"
            //strSQL &= vbCrLf & "     (SELECT TRN.TOT_AMT"
            //strSQL &= vbCrLf & "             FROM INV_AGENT_TRN_TBL TRN"
            //strSQL &= vbCrLf & "             WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK"
            //strSQL &= vbCrLf & "             AND TRN.COST_FRT_ELEMENT=1"
            //strSQL &= vbCrLf & "             AND TRN.INV_AGENT_FK=JOBCOST.INVOICE_SEA_TBL_FK) END) INV_AMT,"
            //strSQL &= vbCrLf & " (CASE WHEN JOBCOST.INV_AGENT_TRN_FK IS NULL THEN 'False'"
            //strSQL &= vbCrLf & "       ELSE 'True' END) CHK"
            strSQL += "ROUND((CASE WHEN (JOBCOST.INVOICE_SEA_TBL_FK IS NULL AND JOBCOST.INV_AGENT_TRN_FK IS NULL ) THEN NULL WHEN JOBCOST.INVOICE_SEA_TBL_FK IS NOT NULL THEN (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_EXP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_CUST_TRN_AIR_EXP_PK=JOBCOST.INVOICE_SEA_TBL_FK) ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_AGENT_TRN_PK=JOBCOST.INV_AGENT_TRN_FK) END),2) INV_AMT,";
            strSQL += "(CASE WHEN (JOBCOST.INVOICE_SEA_TBL_FK IS NULL AND JOBCOST.INV_AGENT_TRN_FK IS NULL) THEN 'False' ELSE 'True' END) CHK";

            strSQL += " FROM JOB_TRN_PIA JOBCOST,";
            strSQL += " COST_ELEMENT_MST_TBL CEMT,";
            strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
            strSQL += " WHERE ";
            strSQL += " JOBCOST.COST_ELEMENT_MST_FK = CEMT.COST_ELEMENT_MST_PK";
            strSQL += " AND JOBCOST.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            strSQL += " AND JOBCOST.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
            strSQL += " ORDER BY CEMT.COST_ELEMENT_NAME,CUMT.CURRENCY_ID";
            try
            {
                DS = objWK.GetDataSet(strSQL);
                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
           
        }
        //This function is called to fetch all the job card freight elements and other charges elements
        //of a selected job card
        //Used to populate the job card freight elements grid in the invoice page
        public DataSet FetchFreightDetails(long nJobCardPK, short AgentType)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;

            strSQL = " SELECT";
            strSQL += " JOBFRT.JOB_TRN_FD_PK,";
            strSQL += " JOBFRT.JOB_CARD_TRN_FK,";
            strSQL += " JOBFRT.FREIGHT_ELEMENT_MST_FK,";
            strSQL += " JOBFRT.CURRENCY_MST_FK,";
            strSQL += " FMT.FREIGHT_ELEMENT_NAME,";
            strSQL += " DECODE(JOBFRT.FREIGHT_TYPE,1,'P',2,'C') AS PC,";
            strSQL += " CUMT.CURRENCY_ID,";
            strSQL += " JOBFRT.FREIGHT_AMT,";
            //surya18NOv06 If AgentType = 1 Then
            //add by latha for fetching the invoiceamt by converting into its currency on january 31
            strSQL += " ROUND((CASE WHEN (JOBFRT.INV_AGENT_TRN_FK IS NULL AND JOBFRT.INV_CUST_TRN_AIR_EXP_FK IS NULL and (jobfrt.CONSOL_INVOICE_TRN_FK is null  OR (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=jobfrt.CONSOL_INVOICE_TRN_FK)=2)) THEN NULL";
            strSQL += "       WHEN JOBFRT.INV_AGENT_TRN_FK IS not NULL then (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT IN (1,2) AND TRN.INV_AGENT_TRN_PK = JOBFRT.INV_AGENT_TRN_FK) WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN (SELECT SUM(CTRN.TOT_AMT / DECODE(Ctrn.exchange_rate,NULL,1,0,1,Ctrn.exchange_rate)) TOT_AMT FROM CONSOL_INVOICE_TRN_TBL CTRN WHERE CTRN.FRT_OTH_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK AND CTRN.FRT_OTH_ELEMENT = 1 AND (CTRN.JOB_CARD_FK = JOBFRT.JOB_CARD_TRN_FK))";
            strSQL += "        else (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_CUST_TRN_AIR_EXP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT IN (1,2) AND TRN.INV_CUST_TRN_AIR_EXP_PK = JOBFRT.INV_CUST_TRN_AIR_EXP_FK) END),2) INV_AMT,";
            //Else
            //    strSQL &= vbCrLf & " ROUND((CASE WHEN (JOBFRT.INV_AGENT_TRN_FK IS NULL AND JOBFRT.INV_CUST_TRN_AIR_EXP_FK IS NULL and jobfrt.CONSOL_INVOICE_TRN_FK is null) THEN NULL "
            //    strSQL &= vbCrLf & "       WHEN JOBFRT.INV_AGENT_TRN_FK IS NOT NULL THEN (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT = 2 AND TRN.INV_AGENT_TRN_PK=JOBFRT.INV_AGENT_TRN_FK) WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN (SELECT (TRN.TOT_AMT / DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM CONSOL_INVOICE_TRN_TBL TRN WHERE TRN.FRT_OTH_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.FRT_OTH_ELEMENT = 1 AND TRN.CONSOL_INVOICE_FK = JOBFRT.CONSOL_INVOICE_TRN_FK)"
            //    strSQL &= vbCrLf & "       ELSE (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_CUST_TRN_AIR_EXP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=2 AND TRN.INV_CUST_TRN_AIR_EXP_PK= JOBFRT.INV_CUST_TRN_AIR_EXP_FK) END),2) INV_AMT,"
            //End If
            strSQL += " (CASE WHEN JOBFRT.INV_AGENT_TRN_FK IS NULL AND JOBFRT.INV_CUST_TRN_AIR_EXP_FK IS NULL and (jobfrt.CONSOL_INVOICE_TRN_FK is null  OR (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=jobfrt.CONSOL_INVOICE_TRN_FK)=2)  THEN 'False' ELSE 'True' END) CHK";
            strSQL += " FROM ";
            strSQL += " JOB_TRN_FD JOBFRT,";
            strSQL += " FREIGHT_ELEMENT_MST_TBL FMT,";
            strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
            strSQL += " WHERE";
            strSQL += " JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
            strSQL += " AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            //Surya18Nov06 If AgentType = 1 Then
            //    strSQL &= vbCrLf & " and jobfrt.freight_type in (1,2) "
            //Else
            //    strSQL &= vbCrLf & " and jobfrt.freight_type =2 "
            //End If
            strSQL += " AND JOBFRT.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);

            //surya18Nov06 If AgentType = 1 Then
            strSQL += " ";
            strSQL += " union";
            strSQL += " ";

            strSQL += " SELECT ";
            strSQL += " JOBOTH.JOB_TRN_OTH_PK,";
            strSQL += " JOBOTH.JOB_CARD_TRN_FK,";
            strSQL += " JOBOTH.FREIGHT_ELEMENT_MST_FK,";
            strSQL += " JOBOTH.CURRENCY_MST_FK,";
            strSQL += " FMT.FREIGHT_ELEMENT_NAME,";
            //strSQL &= vbCrLf & " 'P' AS PC,"
            if (AgentType == 1)
            {
                strSQL += " 'P' AS PC,";
            }
            else
            {
                strSQL += " 'C' AS PC,";
            }
            strSQL += " CUMT.CURRENCY_ID,";
            strSQL += " JOBOTH.AMOUNT,";
            //add by latha for fetching the invoiceamt by converting into its currency on january 31
            strSQL += " ROUND((CASE WHEN (JOBOTH.INV_AGENT_TRN_FK IS NULL AND JOBOTH.INV_CUST_TRN_AIR_EXP_FK IS NULL and (JOBOTH.CONSOL_INVOICE_TRN_FK is null OR (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=JOBOTH.CONSOL_INVOICE_TRN_FK)=2)) THEN NULL WHEN JOBOTH.INV_AGENT_TRN_FK IS not NULL then (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=3 AND TRN.INV_AGENT_TRN_PK=JOBOTH.INV_AGENT_TRN_FK) WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN (SELECT SUM(CTRN.TOT_AMT / DECODE(Ctrn.exchange_rate,NULL,1,0,1,Ctrn.exchange_rate)) FROM CONSOL_INVOICE_TRN_TBL CTRN WHERE (CTRN.FRT_OTH_ELEMENT_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK) AND CTRN.FRT_OTH_ELEMENT = 2 AND (CTRN.JOB_CARD_FK = JOBOTH.JOB_CARD_TRN_FK) ) else (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_CUST_TRN_AIR_EXP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=3 AND TRN.INV_CUST_TRN_AIR_EXP_PK=JOBOTH.INV_CUST_TRN_AIR_EXP_FK) END),2)INV_AMT,";
            strSQL += " (CASE WHEN (JOBOTH.Inv_Cust_Trn_AIR_Exp_Fk is null and joboth.INV_AGENT_TRN_FK is null and (JOBOTH.CONSOL_INVOICE_TRN_FK is null  OR (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=JOBOTH.CONSOL_INVOICE_TRN_FK)=2)) THEN 'False' ELSE 'True' END) CHK";
            strSQL += " FROM";
            strSQL += " JOB_TRN_OTH_CHRG JOBOTH,";
            strSQL += " FREIGHT_ELEMENT_MST_TBL FMT,";
            strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
            strSQL += " WHERE";
            strSQL += " JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK";
            strSQL += " AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            strSQL += " AND JOBOTH.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
            // added by jitendra
            if (AgentType == 2)
            {
                strSQL += "AND JOBOTH.Freight_Type=2";
            }

            //End If
            try
            {
                DS = objWK.GetDataSet(strSQL);
                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //This function is called to fetch all the job card cost & freight elements
        //of a selected job card, which have not yet been invoiced if it is a new invoice
        //and the previously selected invoice elements if any, if it is an existing invoice
        //Union of 2 queries:
        // For new invoice:
        //       1.Select all job card cost elements of the selected job card which have not yet been invoiced
        //       2.Select all job card freight elements of the selected job card which have not yet been invoiced and payment type is Prepaid
        //     
        // For existing invoice:
        //       1.Select all invoice elements from the invoice transaction table

        //Used to populate the invoice elements grid in the invoice page
        public DataSet FetchInvoiceDetails(long nJobCardPK, long nBaseCurrPK, short AgentType, long nInvoicePK = 0, string UserPk = "0")
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;


            if (nInvoicePK == 0)
            {
                // adding by thiyagarajan on 28/2/09:VAT Implementation task 
                string vatcode = null;
                string custpk = null;
                strSQL = " select FETCH_EU(" + nJobCardPK + ",1,1) from dual";
                vatcode = objWK.ExecuteScaler(strSQL);
                strSQL = " SELECT J.SHIPPER_CUST_MST_FK FROM JOB_CARD_TRN J WHERE J.JOB_CARD_TRN_PK=" + nJobCardPK;
                custpk = objWK.ExecuteScaler(strSQL);
                if (string.IsNullOrEmpty(getDefault(custpk, "").ToString()))
                {
                    custpk = "0";
                }
                strSQL = "";
                //end by thiyagarajan 

                ///'''This Part is to fetch all Cost Elements which are not invoiced
                //strSQL = " SELECT 'COST' AS TYPE,"
                //strSQL &= vbCrLf & " JOBCOST.JOB_TRN_PIA_PK AS PK,"
                //strSQL &= vbCrLf & " JOBCOST.JOB_CARD_TRN_FK AS JOBCARD_FK,  "
                //strSQL &= vbCrLf & " JOBCOST.COST_ELEMENT_MST_FK AS ELEMENT_FK,"
                //strSQL &= vbCrLf & " JOBCOST.CURRENCY_MST_FK,"
                //strSQL &= vbCrLf & " CEMT.COST_ELEMENT_NAME AS ELEMENT_NAME,"
                //strSQL &= vbCrLf & " '' AS ELEMENT_SEARCH,"
                //strSQL &= vbCrLf & " CUMT.CURRENCY_ID,"
                //strSQL &= vbCrLf & " '' AS CURR_SEARCH,"
                //strSQL &= vbCrLf & "ROUND(GET_EX_RATE(JOBCOST.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS EXCHANGE_RATE,"
                //strSQL &= vbCrLf & " JOBCOST.ESTIMATED_AMT AS AMOUNT,"
                //strSQL &= vbCrLf & "ROUND(JOBCOST.ESTIMATED_AMT * GET_EX_RATE(JOBCOST.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS INV_AMOUNT,"
                //'strSQL &= vbCrLf & " '' AS TAX_PERCENT,"
                //'strSQL &= vbCrLf & " '' AS TAX_AMOUNT,"
                //'strSQL &= vbCrLf & "ROUND(JOBCOST.ESTIMATED_AMT * GET_EX_RATE(JOBCOST.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS TOTAL_AMOUNT,"
                //strSQL &= vbCrLf & " NVL(CORP.VAT_PERCENTAGE,0) AS TAX_PERCENT, "
                //strSQL &= vbCrLf & " (NVL(CORP.VAT_PERCENTAGE,0) * ROUND(JOBCOST.ESTIMATED_AMT * GET_EX_RATE(JOBCOST.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4)/100) AS TAX_AMOUNT,"
                //strSQL &= vbCrLf & " (ROUND(JOBCOST.ESTIMATED_AMT * GET_EX_RATE(JOBCOST.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) + (ROUND(GET_EX_RATE(" & CStr(nBaseCurrPK) & ",JOBCOST.CURRENCY_MST_FK,sysdate),6) * NVL(CORP.VAT_PERCENTAGE,0)/100) ) AS TOTAL_AMOUNT,"
                //strSQL &= vbCrLf & " '' AS REMARKS,"
                //strSQL &= vbCrLf & " 'New' AS ""MODE"","
                //strSQL &= vbCrLf & " 'False' AS CHK"
                //strSQL &= vbCrLf & " FROM"
                //strSQL &= vbCrLf & " JOB_TRN_PIA JOBCOST,"
                //strSQL &= vbCrLf & " COST_ELEMENT_MST_TBL CEMT,"
                //strSQL &= vbCrLf & " CURRENCY_TYPE_MST_TBL CUMT, "
                //strSQL &= vbCrLf & " CORPORATE_MST_TBL CORP"
                //strSQL &= vbCrLf & " WHERE"
                //strSQL &= vbCrLf & " JOBCOST.COST_ELEMENT_MST_FK = CEMT.COST_ELEMENT_MST_PK"
                //strSQL &= vbCrLf & " AND JOBCOST.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK"
                //strSQL &= vbCrLf & " AND JOBCOST.JOB_CARD_TRN_FK=" & CStr(nJobCardPK)
                //strSQL &= vbCrLf & " AND JOBCOST.INV_AGENT_TRN_FK  IS NULL"
                //strSQL &= vbCrLf & " AND JOBCOST.INVOICE_SEA_TBL_FK IS NULL"

                //strSQL &= vbCrLf & " "
                //strSQL &= vbCrLf & "UNION"
                //strSQL &= vbCrLf & " "

                ///'''This Part is to fetch all Freight Elements which are not invoiced and is of type Prepaid
                strSQL += " SELECT 'FREIGHT' AS TYPE,";
                strSQL += " JOBFRT.JOB_TRN_FD_PK AS PK,";
                strSQL += " JOBFRT.JOB_CARD_TRN_FK  AS JOBCARD_FK,";
                strSQL += " JOBFRT.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,";
                strSQL += " JOBFRT.CURRENCY_MST_FK,";
                strSQL += " FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,";
                strSQL += " '' AS ELEMENT_SEARCH,";
                strSQL += " CUMT.CURRENCY_ID,";
                strSQL += " '' AS CURR_SEARCH,";
                //'strSQL &= vbCrLf & " ROUND(GET_EX_RATE(JOBFRT.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS EXCHANGE_RATE,"
                strSQL += " ROUND(JOBFRT.FREIGHT_AMT,2) AS AMOUNT,";
                strSQL += " JOBFRT.EXCHANGE_RATE AS EXCHANGE_RATE,";
                //'strSQL &= vbCrLf & " ROUND(JOBFRT.FREIGHT_AMT * GET_EX_RATE(JOBFRT.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),2) AS INV_AMOUNT,"
                strSQL += " ROUND(JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE,2) AS INV_AMOUNT,";
                //strSQL &= vbCrLf & " '' AS TAX_PERCENT,"
                //strSQL &= vbCrLf & " '' AS TAX_AMOUNT,"
                //strSQL &= vbCrLf & " ROUND(JOBFRT.FREIGHT_AMT * GET_EX_RATE(JOBFRT.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS TOTAL_AMOUNT,"
                //strSQL &= vbCrLf & " NVL(CORP.VAT_PERCENTAGE,0) AS TAX_PERCENT,"
                //'strSQL &= vbCrLf & " ROUND((NVL(CORP.VAT_PERCENTAGE,0) * ROUND(JOBFRT.FREIGHT_AMT * GET_EX_RATE(JOBFRT.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4)/100),2) AS TAX_AMOUNT,"
                //strSQL &= vbCrLf & " ROUND((NVL(CORP.VAT_PERCENTAGE,0) * JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100,2) AS TAX_AMOUNT,"
                //'strSQL &= vbCrLf & " ((ROUND(JOBFRT.FREIGHT_AMT * GET_EX_RATE(" & CStr(nBaseCurrPK) & ",JOBFRT.CURRENCY_MST_FK,sysdate),2) * NVL(CORP.VAT_PERCENTAGE,0)/100) + ROUND(JOBFRT.FREIGHT_AMT * GET_EX_RATE(" & CStr(nBaseCurrPK) & ",JOBFRT.CURRENCY_MST_FK,sysdate),2) ) AS TOTAL_AMOUNT,"
                //strSQL &= vbCrLf & " (((JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE * NVL(CORP.VAT_PERCENTAGE,0))/100) + ROUND(JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE,2) ) AS TOTAL_AMOUNT,"

                //Added by Venkata 
                //strSQL &= vbCrLf & "NVL((select Distinct(frtv.vat_code) from frt_vat_country_tbl frtv,"
                //strSQL &= vbCrLf & "      user_mst_tbl umt,location_mst_tbl loc"
                //strSQL &= vbCrLf & "where umt.default_location_fk = loc.location_mst_pk"
                //strSQL &= vbCrLf & " and loc.country_mst_fk = frtv.country_mst_fk"
                //strSQL &= vbCrLf & "and umt.user_mst_pk =" & UserPk
                //strSQL &= vbCrLf & "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)"
                //strSQL &= vbCrLf & "),'') AS VAT_CODE,"

                //strSQL &= vbCrLf & "NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,"
                //strSQL &= vbCrLf & "      user_mst_tbl umt,location_mst_tbl loc"
                //strSQL &= vbCrLf & "where umt.default_location_fk = loc.location_mst_pk"
                //strSQL &= vbCrLf & " and loc.country_mst_fk = frtv.country_mst_fk"
                //strSQL &= vbCrLf & "and umt.user_mst_pk =" & UserPk
                //strSQL &= vbCrLf & "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)"
                //strSQL &= vbCrLf & "),null) AS VAT_PERCENT,"



                //VAT CODE
                strSQL += " (select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,1) from dual) VAT_CODE, ";
                //VAT PERCENT

                strSQL += " (select FETCH_VAT ( " + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2) from dual) VAT_PERCENT,";


                //VAT_PERCENT belongs to country instead of corporate 
                //by thiyagarajan on 3/12/08 for location based currency task

                //strSQL &= vbCrLf & "(NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,"
                //strSQL &= vbCrLf & "      user_mst_tbl umt,location_mst_tbl loc"
                //strSQL &= vbCrLf & "where umt.default_location_fk = loc.location_mst_pk"
                //strSQL &= vbCrLf & " and loc.country_mst_fk = frtv.country_mst_fk"
                //strSQL &= vbCrLf & "and umt.user_mst_pk =" & UserPk
                //strSQL &= vbCrLf & "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)"
                //strSQL &= vbCrLf & "),0)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100)  AS TAX_AMOUNT,"

                strSQL += " ((select FETCH_VAT ( " + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) TAX_AMOUNT,";


                //VAT_PERCENT belongs to country instead of corporate 
                //by thiyagarajan on 3/12/08 for location based currency task

                //strSQL &= vbCrLf & "((NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,"
                //strSQL &= vbCrLf & "      user_mst_tbl umt,location_mst_tbl loc"
                //strSQL &= vbCrLf & "where umt.default_location_fk = loc.location_mst_pk"
                //strSQL &= vbCrLf & " and loc.country_mst_fk = frtv.country_mst_fk"
                //strSQL &= vbCrLf & "and umt.user_mst_pk =" & UserPk
                //strSQL &= vbCrLf & "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)"
                //strSQL &= vbCrLf & "),0)*(JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) + JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) AS TOTAL_AMOUNT,"

                strSQL += "  (((select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) + JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) TOTAL_AMOUNT,";

                //VAT_PERCENT belongs to country instead of corporate 
                //by thiyagarajan on 3/12/08 for location based currency task

                strSQL += " '' AS REMARKS,";
                strSQL += " 'New' AS \"MODE\",";
                strSQL += " 'False' AS CHK";
                strSQL += " FROM";
                strSQL += " JOB_TRN_FD JOBFRT,";
                strSQL += " FREIGHT_ELEMENT_MST_TBL FMT,";
                strSQL += " CURRENCY_TYPE_MST_TBL CUMT,";
                strSQL += " CORPORATE_MST_TBL CORP";
                strSQL += " WHERE";
                strSQL += " JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK";
                strSQL += " AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
                strSQL += " AND JOBFRT.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
                strSQL += " AND JOBFRT.INV_AGENT_TRN_FK IS NULL";
                //strSQL &= vbCrLf & " AND JOBFRT.FREIGHT_TYPE=2"
                ///'''''''''
                if (AgentType == 1)
                {
                    strSQL += " AND JOBFRT.FREIGHT_TYPE IN (1,2)";
                }
                else
                {
                    strSQL += " AND JOBFRT.FREIGHT_TYPE = 2 ";
                }
                strSQL += " AND JOBFRT.INV_CUST_TRN_AIR_EXP_FK IS NULL";
                strSQL += " AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL";
                ///'''''''''

                strSQL += " ";
                strSQL += "UNION";
                strSQL += " ";

                ///'''This Part is to fetch Other Elements which are not invoiced from JOB_TRN_OTH_CHRG Table
                strSQL += " SELECT 'OTHER' AS TYPE,";
                strSQL += " JOBOTH.JOB_TRN_OTH_PK AS PK,";
                strSQL += " JOBOTH.JOB_CARD_TRN_FK AS JOBCARD_FK,";
                strSQL += " JOBOTH.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,";
                strSQL += " JOBOTH.CURRENCY_MST_FK,";
                strSQL += " FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,";
                strSQL += " '' AS ELEMENT_SEARCH,";
                strSQL += " CUMT.CURRENCY_ID,";
                strSQL += " '' AS CURR_SEARCH,";
                //'strSQL &= vbCrLf & "ROUND(GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS EXCHANGE_RATE,"
                strSQL += " ROUND(JOBOTH.AMOUNT,2) AS AMOUNT,";
                strSQL += " JOBOTH.EXCHANGE_RATE AS EXCHANGE_RATE,";
                //'strSQL &= vbCrLf & "ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),2) AS INV_AMOUNT,"
                strSQL += "ROUND(JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE,2) AS INV_AMOUNT,";
                //strSQL &= vbCrLf & " '' AS TAX_PERCENT,"
                //strSQL &= vbCrLf & " '' AS TAX_AMOUNT,"
                //strSQL &= vbCrLf & "ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS TOTAL_AMOUNT,"
                //strSQL &= vbCrLf & " NVL(CORP.VAT_PERCENTAGE,0) AS TAX_PERCENT,"
                //'strSQL &= vbCrLf & " ROUND((NVL(CORP.VAT_PERCENTAGE,0) * ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4)/100),2) AS TAX_AMOUNT,"
                //strSQL &= vbCrLf & " ROUND((NVL(CORP.VAT_PERCENTAGE,0) * JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100,2) AS TAX_AMOUNT,"
                //'strSQL &= vbCrLf & " ROUND(( ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),6) + (ROUND(GET_EX_RATE(" & CStr(nBaseCurrPK) & ",JOBOTH.CURRENCY_MST_FK,sysdate),6) * NVL(CORP.VAT_PERCENTAGE,0)/100)),2) AS TOTAL_AMOUNT,"
                //strSQL &= vbCrLf & " ROUND((JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE) + ((NVL(CORP.VAT_PERCENTAGE,0) * JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100),2) AS TOTAL_AMOUNT,"
                //Added by Venkata on 12/01/07

                //Vat Code
                //strSQL &= vbCrLf & "NVL((select Distinct(frtv.vat_code) from frt_vat_country_tbl frtv,"
                //strSQL &= vbCrLf & "      user_mst_tbl umt,location_mst_tbl loc"
                //strSQL &= vbCrLf & "where umt.default_location_fk = loc.location_mst_pk"
                //strSQL &= vbCrLf & " and loc.country_mst_fk = frtv.country_mst_fk"
                //strSQL &= vbCrLf & "and umt.user_mst_pk =" & UserPk
                //strSQL &= vbCrLf & "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)"
                //strSQL &= vbCrLf & "),'') AS VAT_CODE,"

                //Vat Percentage
                //strSQL &= vbCrLf & "NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,"
                //strSQL &= vbCrLf & "      user_mst_tbl umt,location_mst_tbl loc"
                //strSQL &= vbCrLf & "where umt.default_location_fk = loc.location_mst_pk"
                //strSQL &= vbCrLf & " and loc.country_mst_fk = frtv.country_mst_fk"
                //strSQL &= vbCrLf & "and umt.user_mst_pk =" & UserPk
                //strSQL &= vbCrLf & "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)"
                //strSQL &= vbCrLf & "),CORP.VAT_PERCENTAGE) AS VAT_PERCENT,"

                //strSQL &= vbCrLf & "(NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,"
                //strSQL &= vbCrLf & "      user_mst_tbl umt,location_mst_tbl loc"
                //strSQL &= vbCrLf & "where umt.default_location_fk = loc.location_mst_pk"
                //strSQL &= vbCrLf & " and loc.country_mst_fk = frtv.country_mst_fk"
                //strSQL &= vbCrLf & "and umt.user_mst_pk =" & UserPk
                //strSQL &= vbCrLf & "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)"
                //strSQL &= vbCrLf & "),CORP.VAT_PERCENTAGE)* ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4)/100) AS TAX_AMOUNT,"

                //strSQL &= vbCrLf & "(NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,"
                //strSQL &= vbCrLf & "      user_mst_tbl umt,location_mst_tbl loc"
                //strSQL &= vbCrLf & "where umt.default_location_fk = loc.location_mst_pk"
                //strSQL &= vbCrLf & " and loc.country_mst_fk = frtv.country_mst_fk"
                //strSQL &= vbCrLf & "and umt.user_mst_pk =" & UserPk
                //strSQL &= vbCrLf & "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)"
                //strSQL &= vbCrLf & "),CORP.VAT_PERCENTAGE)* ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4)/100) + ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS TOTAL_AMOUNT,"
                //End by Venkata

                //adding by thiyagarajan on 28/2/09:VAT Implementation task 
                strSQL += " (select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,1) from dual) VAT_CODE,";

                strSQL += "(select FETCH_VAT ( " + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2) from dual) VAT_PERCENT,";

                strSQL += "((select FETCH_VAT(" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100) TAX_AMOUNT,";

                strSQL += "(((select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100) + JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE) TOTAL_AMOUNT,";
                //end by thiyagarajan on 28/2/09

                strSQL += " '' AS REMARKS,";
                strSQL += " 'New' AS \"MODE\",";
                strSQL += " 'False' AS CHK";
                strSQL += " FROM";
                strSQL += " JOB_TRN_OTH_CHRG JOBOTH, ";
                strSQL += " FREIGHT_ELEMENT_MST_TBL FMT,";
                strSQL += " CURRENCY_TYPE_MST_TBL CUMT,";
                strSQL += " CORPORATE_MST_TBL CORP";
                strSQL += " WHERE";
                strSQL += " JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK";
                strSQL += " AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
                strSQL += " AND JOBOTH.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
                strSQL += " AND JOBOTH.INV_AGENT_TRN_FK IS NULL";
                strSQL += " AND JOBOTH.INV_CUST_TRN_AIR_EXP_FK  IS NULL";
                strSQL += " AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL";
                // added by jitendra
                if (AgentType == 2)
                {
                    strSQL += "AND JOBOTH.Freight_Type=2";
                }
            }
            else
            {
                //Modified by Mani
                //strSQL = " SELECT DECODE(TRN.COST_FRT_ELEMENT,1,'COST',2,'FREIGHT',3,'OTHER') AS TYPE,"
                strSQL = " SELECT DECODE(TRN.COST_FRT_ELEMENT,1,'FREIGHT',2,'FREIGHT',3,'OTHER') AS TYPE,";
                strSQL += " TRN.INV_AGENT_TRN_PK AS PK,";
                strSQL += " HDR.JOB_CARD_TRN_FK AS JOBCARD_FK,";
                strSQL += " TRN.COST_FRT_ELEMENT_FK AS ELEMENT_FK,";
                strSQL += " TRN.CURRENCY_MST_FK,";
                //Modified By Mani
                //strSQL &= vbCrLf & " (CASE TRN.COST_FRT_ELEMENT WHEN 1 THEN (SELECT COST_ELEMENT_NAME FROM COST_ELEMENT_MST_TBL C WHERE C.COST_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 2 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 3 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) END) AS ELEMENT_NAME,"
                strSQL += " (CASE TRN.COST_FRT_ELEMENT WHEN 1 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 2 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 3 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) END) AS ELEMENT_NAME,";
                strSQL += " '' AS ELEMENT_SEARCH,";
                strSQL += " CUMT.CURRENCY_ID,";
                strSQL += " '' AS CURR_SEARCH,";
                //strSQL &= vbCrLf & " ROUND(GET_EX_RATE(TRN.CURRENCY_MST_FK,HDR.CURRENCY_MST_FK,HDR.INVOICE_DATE),4) EXCHANGE_RATE,"
                strSQL += " ROUND(TRN.ELEMENT_AMT,2) AS AMOUNT,";
                strSQL += " TRN.exchange_rate AS EXCHANGE_RATE, ";
                strSQL += " ROUND(TRN.AMT_IN_INV_CURR,2) AS INV_AMOUNT,";
                strSQL += " TRN.VAT_CODE AS VAT_CODE,";
                //Added by Venkata on 12/01/07
                strSQL += " to_number('' || TRN.TAX_PCNT || '') AS VAT_PERCENT,";
                strSQL += " to_number('' || TRN.TAX_AMT || '')  AS TAX_AMOUNT,";
                strSQL += " ROUND(TRN.TOT_AMT,2) AS TOTAL_AMOUNT,";
                strSQL += " TRN.REMARKS,";
                strSQL += " 'Edit' AS \"MODE\",";
                strSQL += " 'True' AS CHK";
                strSQL += " FROM";
                strSQL += " INV_AGENT_TRN_TBL TRN,";
                strSQL += " INV_AGENT_TBL HDR,";
                strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
                strSQL += " WHERE";
                strSQL += " TRN.INV_AGENT_FK = HDR.INV_AGENT_PK";
                strSQL += " AND TRN.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
                strSQL += " AND HDR.INV_AGENT_PK=" + Convert.ToString(nInvoicePK);
            }
            try
            {
                DS = objWK.GetDataSet(strSQL);
                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //This function is called to fetch the job card reference no., date, booking no. etc
        //of a selected job card
        //Used to display the job card details, when an existing invoice is opened in edit mode
        //Modified by  Mani On 5/07/06
        public DataSet FetchJCDetails(double nJobCardPK, short AgentType = 0)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;

            strSQL = " SELECT ";
            strSQL += " JOB.JOBCARD_REF_NO,";
            strSQL += " TO_CHAR(JOB.JOBCARD_DATE,'" + dateFormat + "') AS JOBCARD_DATE, ";
            strSQL += " BKG.BOOKING_REF_NO,";
            strSQL += " HAWB.HAWB_REF_NO,";
            strSQL += " AMT.AGENT_MST_PK,";
            strSQL += " AMT.AGENT_NAME,";
            strSQL += " JOB.FLIGHT_NO,";
            strSQL += " POL.PORT_MST_PK POLPK,";
            strSQL += " POL.port_name POL,";
            strSQL += " POD.PORT_MST_PK PODPK,";
            strSQL += " POD.port_name POD";
            strSQL += " FROM";
            strSQL += "  JOB_CARD_TRN JOB,";
            strSQL += " BOOKING_MST_TBL BKG,";
            strSQL += " HAWB_EXP_TBL HAWB,";
            strSQL += " AGENT_MST_TBL AMT,";
            strSQL += " PORT_MST_TBL POL,";
            strSQL += " PORT_MST_TBL POD";
            strSQL += " WHERE";
            strSQL += " JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK";
            strSQL += " AND JOB.JOB_CARD_TRN_PK=HAWB.JOB_CARD_TRN_FK(+)";
            if (AgentType == 1)
            {
                strSQL += " AND JOB.CB_AGENT_MST_FK = AMT.AGENT_MST_PK";
            }
            else
            {
                strSQL += " AND JOB.DP_AGENT_MST_FK = AMT.AGENT_MST_PK";
            }
            strSQL += " AND BKG.PORT_MST_POL_FK=POL.PORT_MST_PK";
            strSQL += " AND BKG.PORT_MST_POD_FK=POD.PORT_MST_PK";
            strSQL += " AND JOB.JOB_CARD_TRN_PK=" + Convert.ToString(nJobCardPK);
            try
            {
                DS = objWK.GetDataSet(strSQL);
                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //This function is called to fetch all the fields of invoice header
        //Used for passing the details to save function
        public DataSet FetchHDR(long nInvPK)
        {
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_PK,");
            sb.Append("       INV.JOB_CARD_TRN_FK,");
            sb.Append("       INV.CB_AGENT_MST_FK,");
            sb.Append("       INV.INVOICE_REF_NO,");
            sb.Append("       TO_DATE(INV.INVOICE_DATE, DATEFORMAT) INVOICE_DATE,");
            sb.Append("       TO_DATE(INV.INVOICE_DUE_DATE, DATEFORMAT) INVOICE_DUE_DATE,");
            sb.Append("       INV.CURRENCY_MST_FK,");
            sb.Append("       INV.GROSS_INV_AMT,");
            sb.Append("       INV.VAT_PCNT,");
            sb.Append("       INV.VAT_AMT,");
            sb.Append("       INV.DISCOUNT_AMT,");
            sb.Append("       INV.NET_INV_AMT,");
            sb.Append("       INV.CREATED_BY_FK,");
            sb.Append("       INV.CREATED_DT,");
            sb.Append("       INV.LAST_MODIFIED_BY_FK,");
            sb.Append("       INV.BATCH_MST_FK,");
            sb.Append("       INV.CHK_INVOICE,");
            sb.Append("       INV.REMARKS,");
            sb.Append("       INV.CB_OR_DP_AGENT,");
            sb.Append("       INV.VERSION_NO,");
            sb.Append("       INV.INV_UNIQUE_REF_NR,");
            sb.Append("   UMTCRT.USER_NAME    AS CREATED_BY, ");
            sb.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
            sb.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
            sb.Append("   TO_DATE(INV.CREATED_DT) CREATED_BY_DT, ");
            sb.Append("   TO_DATE(INV.LAST_MODIFIED_DT) LAST_MODIFIEDBY_DT, ");
            sb.Append("   TO_DATE(INV.LAST_MODIFIED_DT) APPROVED_DT ");
            sb.Append("  FROM INV_AGENT_TBL INV,");
            sb.Append("  USER_MST_TBL UMTCRT, ");
            sb.Append("  USER_MST_TBL UMTUPD, ");
            sb.Append("  USER_MST_TBL UMTAPP ");
            sb.Append(" WHERE INV.INV_AGENT_PK =" + nInvPK);
            sb.Append(" AND UMTCRT.USER_MST_PK(+) = INV.CREATED_BY_FK ");
            sb.Append(" AND UMTUPD.USER_MST_PK(+) = INV.LAST_MODIFIED_BY_FK  ");
            sb.Append(" AND UMTAPP.USER_MST_PK(+) = INV.LAST_MODIFIED_BY_FK  ");
            //strSQL = "SELECT * FROM INV_AGENT_TBL i  WHERE i.INV_AGENT_PK=" & CStr(nInvPK)

            try
            {
                DS = objWK.GetDataSet(sb.ToString());
                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //This function is called to fetch Max Credit Note Amount for particular Invoice
        public double FetchCreditAmt(string JOBPK)
        {
            string Strsql = null;
            double CreditAmt = 0;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = " select ";
                Strsql += " nvl(sum(c.credit_note_amt), 0)";
                Strsql += " from cr_agent_tbl c";
                Strsql += " where c.inv_agent_fk = " + JOBPK;
                CreditAmt = Convert.ToDouble(ObjWF.ExecuteScaler(Strsql));
                return CreditAmt;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Int64 GetAgntpk(string JobCard)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            Int64 pk = default(Int64);
            strSQL = " SELECT ";
            strSQL += " AG.AGENT_MST_PK ";
            strSQL += " FROM JOB_CARD_TRN JA, ";
            strSQL += " BOOKING_MST_TBL BA, HAWB_EXP_TBL HA, ";
            strSQL += " CUSTOMER_MST_TBL,PORT_MST_TBL POL,PORT_MST_TBL POD,AGENT_MST_TBL AG  ";
            strSQL += " WHERE(JA.BOOKING_MST_FK = BA.BOOKING_MST_PK)";
            strSQL += " AND JA.JOB_CARD_TRN_PK=HA.JOB_CARD_TRN_FK (+)";
            strSQL += " AND CUST_CUSTOMER_MST_FK = CUSTOMER_MST_PK (+)";
            strSQL += " AND JA.CB_AGENT_MST_FK = AG.AGENT_MST_PK (+)";
            strSQL += " AND PORT_MST_POL_FK = POL.PORT_MST_PK";
            strSQL += " AND PORT_MST_POD_FK = POD.PORT_MST_PK";
            strSQL += " AND JOB_CARD_STATUS = 1";
            strSQL += " AND UPPER(JA.JOBCARD_REF_NO) LIKE '" + JobCard + "'";
            strSQL += " ORDER BY JA.JOBCARD_REF_NO";
            try
            {
                pk = Convert.ToInt32(objWK.ExecuteScaler(strSQL));
                return pk;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //This function is called to fetch all the fields of invoice transaction table
        // 'Used for passing the details to save function
        public DataSet FetchTRN(long nInvPK)
        {
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_TRN_PK,");
            sb.Append("       INV.INV_AGENT_FK,");
            sb.Append("       INV.COST_FRT_ELEMENT,");
            sb.Append("       INV.COST_FRT_ELEMENT_FK,");
            sb.Append("       INV.CURRENCY_MST_FK,");
            sb.Append("       INV.EXCHANGE_RATE,");
            sb.Append("       INV.ELEMENT_AMT,");
            sb.Append("       INV.TAX_PCNT,");
            sb.Append("       INV.TAX_AMT,");
            sb.Append("       INV.TOT_AMT,");
            sb.Append("       INV.AMT_IN_INV_CURR          ,");
            sb.Append("       INV.REMARKS,");
            sb.Append("       INV.VAT_CODE,");
            sb.Append("       0 JOB_TRN_PK");
            sb.Append("  FROM INV_AGENT_TRN_TBL INV");
            sb.Append(" WHERE INV.INV_AGENT_FK =" + nInvPK);
            //strSQL = "SELECT * FROM INV_AGENT_TRN_TBL I WHERE I.INV_AGENT_FK=" & CStr(nInvPK)

            try
            {
                DS = objWK.GetDataSet(sb.ToString());
                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //This function is called to fetch the exchange rates for all currencies
        //for the selected invoice currency
        //Public Function FetchExchangeRate(ByVal strBaseCurr As String, Optional ByVal strDate As String = "") As DataSet
        //    Try
        //        Dim strSQL As String
        //        Dim strCondition As String
        //        If strDate = "" Then
        //            strCondition = " AND EXC.FROM_DATE <= SYSDATE" & vbCrLf & _
        //                           " AND TO_DATE  >= SYSDATE"
        //        Else
        //            strCondition = " AND EXC.FROM_DATE <= TO_DATE(' " & strDate & "','" & dateFormat & "')  " & vbCrLf & _
        //                           " AND TO_DATE  >= TO_DATE(' " & strDate & "','" & dateFormat & "')"
        //        End If
        //        strSQL = "SELECT C.CURRENCY_MST_PK, " & vbCrLf & _
        //                 "' ' || C.CURRENCY_ID CURRENCY_ID, " & vbCrLf & _
        //                 "C.CURRENCY_NAME, " & vbCrLf & _
        //                 "1 EXCHANGE_RATE " & vbCrLf & _
        //                 "FROM CURRENCY_TYPE_MST_TBL C " & vbCrLf & _
        //                 "WHERE C.CURRENCY_MST_PK =  " & vbCrLf & _
        //                 strBaseCurr & vbCrLf & _
        //                 "AND C.ACTIVE_FLAG =1 " & vbCrLf & _
        //                 "UNION " & vbCrLf & _
        //                 "SELECT " & vbCrLf & _
        //                 "CURR.CURRENCY_MST_PK, " & vbCrLf & _
        //                 "CURR.CURRENCY_ID, " & vbCrLf & _
        //                 "CURR.CURRENCY_NAME, " & vbCrLf & _
        //                 "EXC.EXCHANGE_RATE " & vbCrLf & _
        //                 "FROM CURRENCY_TYPE_MST_TBL CURR, " & vbCrLf & _
        //                 "EXCHANGE_RATE_TRN EXC  " & vbCrLf & _
        //                 "WHERE  " & vbCrLf & _
        //                 "EXC.CURRENCY_MST_FK =" & strBaseCurr & vbCrLf & _
        //                 "AND CURR.ACTIVE_FLAG =1  " & vbCrLf & _
        //                 "AND EXC.CURRENCY_MST_BASE_FK = " & vbCrLf & _
        //                 " CURR.CURRENCY_MST_PK " & vbCrLf & _
        //                 strCondition & " ORDER BY CURRENCY_ID "

        //        Dim DS As DataSet
        //        DS = (New WorkFlow).GetDataSet(strSQL)
        //        Dim RowCnt As Int16
        //        Dim CurrId As String = CStr(DS.Tables(0).Rows(0).Item("CURRENCY_ID")).Trim
        //        DS.Tables(0).Rows(0).Item("CURRENCY_ID") = CurrId
        //        For RowCnt = 1 To DS.Tables(0).Rows.Count - 1
        //            If DS.Tables(0).Rows(RowCnt).Item("CURRENCY_ID") = CurrId Then

        //                DS.Tables(0).Rows.RemoveAt(RowCnt)
        //                Exit For
        //            End If
        //        Next

        //        Return DS
        //    Catch ex As Exception
        //        Throw ex
        //    End Try
        //End Function
        public DataSet GetCorpCurrency()
        {
            string strSQL = null;
            strSQL = "SELECT CMT.CURRENCY_MST_FK,CUMT.CURRENCY_ID FROM CORPORATE_MST_TBL CMT,CURRENCY_TYPE_MST_TBL CUMT";
            strSQL += "WHERE CMT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            try
            {
                DataSet DS = null;
                DS = (new WorkFlow()).GetDataSet(strSQL);
                return DS;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region "TO Update cbagentstatus"
        public ArrayList UpdateCbagentstatus(int JobPk = 0)
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

                str = "UPDATE JOB_CARD_TRN  j SET ";
                str += "   j.CBAGENT_STATUS = 0 ";
                str += " WHERE j.JOB_CARD_TRN_PK=" + JobPk;

                var _with1 = updCmdUser;
                _with1.Connection = objWK.MyConnection;
                _with1.Transaction = TRAN;
                _with1.CommandType = CommandType.Text;
                _with1.CommandText = str;
                intIns = Convert.ToInt16(_with1.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    //arrMessage.Add("Protocol Generated Succesfully")
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
        #endregion

        #region "To update DPagent Status"
        public ArrayList UpdateDPagentstatus(int JobPk = 0)
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

                str = "UPDATE JOB_CARD_TRN  j SET ";
                str += "   j.DPAGENT_STATUS = 0 ";
                str += " WHERE j.JOB_CARD_TRN_PK=" + JobPk;

                var _with2 = updCmdUser;
                _with2.Connection = objWK.MyConnection;
                _with2.Transaction = TRAN;
                _with2.CommandType = CommandType.Text;
                _with2.CommandText = str;
                intIns = Convert.ToInt16(_with2.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    //arrMessage.Add("Protocol Generated Succesfully")
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
        #endregion

        #region " Save"
        //This function is called to save the invoice header details
        public ArrayList Save(ref DataSet hdrDS, ref DataSet trnDS, long invPK, long nConfigPK, long lngLocPk, System.DateTime invDate, System.DateTime invdDate, int CheckApp, string AgentType)
        {
            WorkFlow objWK = new WorkFlow();
            string uniqueRefNr = "";
            if (invPK <= 0)
            {
                System.DateTime dt = default(System.DateTime);
                dt = System.DateTime.Now;
                string st = null;
                short refExist = 1;
                while (refExist > 0)
                {
                    st = Convert.ToString(dt.Day + dt.Month + dt.Year + dt.Hour + dt.Minute + dt.Second + dt.Millisecond);
                    uniqueRefNr = GetVEKInvoiceRef(0, 0, st);
                    refExist = Convert.ToInt16(objWK.ExecuteScaler("SELECT COUNT(*) FROM INV_AGENT_TBL INV WHERE UPPER(INV.INV_UNIQUE_REF_NR)=UPPER('" + uniqueRefNr + "')"));
                }
            }
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
            bool chkflag = false;
            Int32 RecAfct = default(Int32);
            long nJobCardPK = 0;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            try
            {
                nJobCardPK = Convert.ToInt32(hdrDS.Tables[0].Rows[0]["JOB_CARD_TRN_FK"]);
                var _with3 = insCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".INV_AGENT_TBL_PKG.INV_AGENT_TBL_INS";

                _with3.Parameters.Add("JOB_CARD_TRN_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_TRN_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["JOB_CARD_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CB_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "CB_AGENT_MST_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("INVOICE_REF_NO_IN", OracleDbType.Varchar2, 50, "INVOICE_REF_NO").Direction = ParameterDirection.Input;
                _with3.Parameters["INVOICE_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("INVOICE_DATE_IN", invDate).Direction = ParameterDirection.Input;
                _with3.Parameters["INVOICE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("INVOICE_DUE_DATE_IN", invdDate).Direction = ParameterDirection.Input;
                _with3.Parameters["INVOICE_DUE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CHK_INVOICE_IN", CheckApp).Direction = ParameterDirection.Input;
                _with3.Parameters["CHK_INVOICE_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("GROSS_INV_AMT_IN", OracleDbType.Int32, 15, "GROSS_INV_AMT").Direction = ParameterDirection.Input;
                _with3.Parameters["GROSS_INV_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("VAT_PCNT_IN", OracleDbType.Int32, 10, "VAT_PCNT").Direction = ParameterDirection.Input;
                _with3.Parameters["VAT_PCNT_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("VAT_AMT_IN", OracleDbType.Int32, 10, "VAT_AMT").Direction = ParameterDirection.Input;
                _with3.Parameters["VAT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("DISCOUNT_AMT_IN", OracleDbType.Int32, 10, "DISCOUNT_AMT").Direction = ParameterDirection.Input;
                _with3.Parameters["DISCOUNT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("NET_INV_AMT_IN", OracleDbType.Int32, 15, "NET_INV_AMT").Direction = ParameterDirection.Input;
                _with3.Parameters["NET_INV_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "REMARKS").Direction = ParameterDirection.Input;
                _with3.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CREATED_BY_FK_IN", OracleDbType.Int32, 10, "CREATED_BY_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("UNIQUE_REF_NO_IN", uniqueRefNr).Direction = ParameterDirection.Input;

                _with3.Parameters.Add("CB_OR_DP_AGENT_IN", OracleDbType.Int32, 1, "CB_OR_DP_AGENT").Direction = ParameterDirection.Input;
                _with3.Parameters["CB_OR_DP_AGENT_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "INV_AGENT_PK").Direction = ParameterDirection.Output;
                _with3.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with4 = updCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".INV_AGENT_TBL_PKG.INV_AGENT_TBL_UPD";

                _with4.Parameters.Add("INV_AGENT_PK_IN", OracleDbType.Int32, 10, "INV_AGENT_PK").Direction = ParameterDirection.Input;
                _with4.Parameters["INV_AGENT_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("JOB_CARD_TRN_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_TRN_FK").Direction = ParameterDirection.Input;
                _with4.Parameters["JOB_CARD_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("CB_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "CB_AGENT_MST_FK").Direction = ParameterDirection.Input;
                _with4.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("INVOICE_REF_NO_IN", OracleDbType.Varchar2, 50, "INVOICE_REF_NO").Direction = ParameterDirection.Input;
                _with4.Parameters["INVOICE_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("INVOICE_DATE_IN", invDate).Direction = ParameterDirection.Input;
                _with4.Parameters["INVOICE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("INVOICE_DUE_DATE_IN", invdDate).Direction = ParameterDirection.Input;
                _with4.Parameters["INVOICE_DUE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("CHK_INVOICE_IN", CheckApp).Direction = ParameterDirection.Input;
                _with4.Parameters["CHK_INVOICE_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                _with4.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("GROSS_INV_AMT_IN", OracleDbType.Int32, 15, "GROSS_INV_AMT").Direction = ParameterDirection.Input;
                _with4.Parameters["GROSS_INV_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("VAT_PCNT_IN", OracleDbType.Int32, 10, "VAT_PCNT").Direction = ParameterDirection.Input;
                _with4.Parameters["VAT_PCNT_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("VAT_AMT_IN", OracleDbType.Int32, 10, "VAT_AMT").Direction = ParameterDirection.Input;
                _with4.Parameters["VAT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("DISCOUNT_AMT_IN", OracleDbType.Int32, 10, "DISCOUNT_AMT").Direction = ParameterDirection.Input;
                _with4.Parameters["DISCOUNT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("NET_INV_AMT_IN", OracleDbType.Int32, 15, "NET_INV_AMT").Direction = ParameterDirection.Input;
                _with4.Parameters["NET_INV_AMT_IN"].SourceVersion = DataRowVersion.Current;

                //ADDED BY SUMI ON 28.03.06
                _with4.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "REMARKS").Direction = ParameterDirection.Input;
                _with4.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;
                //END REMARKS

                _with4.Parameters.Add("LAST_MODIFIED_BY_FK_IN", OracleDbType.Int32, 10, "LAST_MODIFIED_BY_FK").Direction = ParameterDirection.Input;
                _with4.Parameters["LAST_MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input;
                _with4.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;

                _with4.Parameters.Add("CB_OR_DP_AGENT_IN", OracleDbType.Int32, 1, "CB_OR_DP_AGENT").Direction = ParameterDirection.Input;
                _with4.Parameters["CB_OR_DP_AGENT_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                _with4.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                
                arrMessage.Clear();
                TRAN = objWK.MyConnection.BeginTransaction();

                var _with5 = objWK.MyDataAdapter;

                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;
                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;
                //Added by minakshi on 18-feb-09 for protocal rollbacking
                if ((hdrDS.GetChanges(DataRowState.Added) != null))
                {
                    chkflag = true;
                }
                else
                {
                    chkflag = false;
                }
                //Ended by minakshi
                RecAfct = _with5.Update(hdrDS.Tables[0]);
                if (RecAfct > 0)
                {

                    if (invPK > 0)
                    {
                    }
                    else
                    {
                        SaveTrn(trnDS, TRAN, Convert.ToInt32(hdrDS.Tables[0].Rows[0][0]), nJobCardPK, Convert.ToInt16(CheckApp));
                        // Amit ''For New Record only it Should update into Trak and Trace
                        if (AgentType == "CB")
                        {
                            objTrackNTrace.SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(hdrDS.Tables[0].Rows[0][0]), 1, 1, "Invoice to Agent", "INVOICE-CB-AGT-AIR-EXP", Convert.ToInt32(lngLocPk), objWK, "INS", Convert.ToInt64(hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]),"O");
                        }
                        else
                        {
                            objTrackNTrace.SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(hdrDS.Tables[0].Rows[0][0]), 1, 1, "Invoice to Agent", "INVOICE-DP-AGT-AIR-EXP", Convert.ToInt32(lngLocPk), objWK, "INS", Convert.ToInt64(hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]), "O");
                          
                        }
                        // End
                    }
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    //Added by minakshi on 18-feb-09 for protocal rollbacking
                    if (chkflag)
                    {
                        RollbackProtocolKey("AGENT INVOICE AIR EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(hdrDS.Tables[0].Rows[0]["INVOICE_REF_NO"]), System.DateTime.Now);
                    }
                    //Ended by minakshi  
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    //Push to financial system if realtime is selected
                    if (CheckApp == 1)
                    {
                        if (invPK > 0)
                        {
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
                                //    objPush.UpdateTransaction(schDtls[10], schDtls[2], schDtls[6], schDtls[4], ref errGen, invPK);
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                                //    }
                                //}
                                //catch (Exception ex)
                                //{
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 2, Session["USER_PK"]);
                                //    }
                                //}
                            }
                        }
                    }
                    //*****************************************************************
                    return arrMessage;
                }

            }
            catch (OracleException oraexp)
            {
                if ((TRAN != null))
                {
                    if (TRAN.Connection.State == ConnectionState.Open)
                    {
                        TRAN.Rollback();
                        //Added by minakshi on 18-feb-09 for protocal rollbacking
                        if (chkflag)
                        {
                            RollbackProtocolKey("AGENT INVOICE AIR EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(hdrDS.Tables[0].Rows[0]["INVOICE_REF_NO"]), System.DateTime.Now);
                        }
                        //Ended by minakshi  
                        TRAN = null;
                    }
                }
                throw oraexp;
            }
            catch (Exception ex)
            {
                if ((TRAN != null))
                {
                    if (TRAN.Connection.State == ConnectionState.Open)
                    {
                        TRAN.Rollback();
                        //Added by minakshi on 18-feb-09 for protocal rollbacking
                        if (chkflag)
                        {
                            RollbackProtocolKey("AGENT INVOICE AIR EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(hdrDS.Tables[0].Rows[0]["INVOICE_REF_NO"]), System.DateTime.Now);
                        }
                        //Ended by minakshi  
                        TRAN = null;
                    }
                }
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
        //This function is called to save the invoice transaction details
        private void SaveTrn(DataSet trnDS, OracleTransaction TRAN, long nInvPK, long nJobCardPK, Int16 InvStatus)
        {
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();


            try
            {
                objWK.MyConnection = TRAN.Connection;
                var _with6 = insCommand;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".INV_AGENT_TBL_PKG.INV_AGENT_TRN_TBL_INS";

                _with6.Parameters.Add("INV_AGENT_FK_IN", nInvPK).Direction = ParameterDirection.Input;

                _with6.Parameters.Add("COST_FRT_ELEMENT_IN", OracleDbType.Int32, 1, "COST_FRT_ELEMENT").Direction = ParameterDirection.Input;
                _with6.Parameters["COST_FRT_ELEMENT_IN"].SourceVersion = DataRowVersion.Current;

                _with6.Parameters.Add("COST_FRT_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_FRT_ELEMENT_FK").Direction = ParameterDirection.Input;
                _with6.Parameters["COST_FRT_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with6.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                _with6.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with6.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "EXCHANGE_RATE").Direction = ParameterDirection.Input;
                _with6.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                _with6.Parameters.Add("ELEMENT_AMT_IN", OracleDbType.Int32, 15, "ELEMENT_AMT").Direction = ParameterDirection.Input;
                _with6.Parameters["ELEMENT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with6.Parameters.Add("TAX_PCNT_IN", OracleDbType.Int32, 6, "TAX_PCNT").Direction = ParameterDirection.Input;
                _with6.Parameters["TAX_PCNT_IN"].SourceVersion = DataRowVersion.Current;

                _with6.Parameters.Add("TAX_AMT_IN", OracleDbType.Int32, 10, "TAX_AMT").Direction = ParameterDirection.Input;
                _with6.Parameters["TAX_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with6.Parameters.Add("TOT_AMT_IN", OracleDbType.Int32, 15, "TOT_AMT").Direction = ParameterDirection.Input;
                _with6.Parameters["TOT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with6.Parameters.Add("AMT_IN_INV_CURR_IN", OracleDbType.Int32, 15, "AMT_IN_INV_CURR").Direction = ParameterDirection.Input;
                _with6.Parameters["AMT_IN_INV_CURR_IN"].SourceVersion = DataRowVersion.Current;

                _with6.Parameters.Add("JOB_CARD_TRN_FK_IN", OracleDbType.Int32, 10, "JOB_TRN_PK").Direction = ParameterDirection.Input;
                _with6.Parameters["JOB_CARD_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with6.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "REMARKS").Direction = ParameterDirection.Input;
                _with6.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                //Added by Venkata on 12/10/07
                _with6.Parameters.Add("VAT_CODE_IN", OracleDbType.Varchar2, 20, Convert.ToString(getDefault("VAT_CODE", ""))).Direction = ParameterDirection.Input;
                _with6.Parameters["VAT_CODE_IN"].SourceVersion = DataRowVersion.Current;
                //End
                _with6.Parameters.Add("INV_STATUS_IN", InvStatus).Direction = ParameterDirection.Input;
                _with6.Parameters["INV_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                _with6.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "INV_AGENT_TRN_PK").Direction = ParameterDirection.Output;
                _with6.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with7 = updCommand;
                _with7.Connection = TRAN.Connection;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".INV_AGENT_TBL_PKG.INV_AGENT_TRN_TBL_UPD";

                _with7.Parameters.Add("INV_AGENT_TRN_PK_IN", OracleDbType.Int32, 10, "INV_AGENT_TRN_PK").Direction = ParameterDirection.Input;
                _with7.Parameters["INV_AGENT_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with7.Parameters.Add("INV_AGENT_FK_IN", nInvPK).Direction = ParameterDirection.Input;

                _with7.Parameters.Add("COST_FRT_ELEMENT_IN", OracleDbType.Int32, 1, "COST_FRT_ELEMENT").Direction = ParameterDirection.Input;
                _with7.Parameters["COST_FRT_ELEMENT_IN"].SourceVersion = DataRowVersion.Current;

                _with7.Parameters.Add("COST_FRT_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_FRT_ELEMENT_FK").Direction = ParameterDirection.Input;
                _with7.Parameters["COST_FRT_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with7.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                _with7.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with7.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "EXCHANGE_RATE").Direction = ParameterDirection.Input;
                _with7.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                _with7.Parameters.Add("ELEMENT_AMT_IN", OracleDbType.Int32, 15, "ELEMENT_AMT").Direction = ParameterDirection.Input;
                _with7.Parameters["ELEMENT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with7.Parameters.Add("TAX_PCNT_IN", OracleDbType.Int32, 6, "TAX_PCNT").Direction = ParameterDirection.Input;
                _with7.Parameters["TAX_PCNT_IN"].SourceVersion = DataRowVersion.Current;

                _with7.Parameters.Add("TAX_AMT_IN", OracleDbType.Int32, 10, "TAX_AMT").Direction = ParameterDirection.Input;
                _with7.Parameters["TAX_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with7.Parameters.Add("TOT_AMT_IN", OracleDbType.Int32, 15, "TOT_AMT").Direction = ParameterDirection.Input;
                _with7.Parameters["TOT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with7.Parameters.Add("AMT_IN_INV_CURR_IN", OracleDbType.Int32, 15, "AMT_IN_INV_CURR").Direction = ParameterDirection.Input;
                _with7.Parameters["AMT_IN_INV_CURR_IN"].SourceVersion = DataRowVersion.Current;

                _with7.Parameters.Add("JOB_CARD_TRN_FK_IN", OracleDbType.Int32, 10, "JOB_TRN_PK").Direction = ParameterDirection.Input;
                _with7.Parameters["JOB_CARD_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with7.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "REMARKS").Direction = ParameterDirection.Input;
                _with7.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                //Added by Venkata on 12/10/07
                _with7.Parameters.Add("VAT_CODE_IN", OracleDbType.Varchar2, 20, Convert.ToString(getDefault("VAT_CODE", DBNull.Value))).Direction = ParameterDirection.Input;
                _with7.Parameters["VAT_CODE_IN"].SourceVersion = DataRowVersion.Current;
                //End

                _with7.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100).Direction = ParameterDirection.Output;
                _with7.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                //With delCommand
                //    .Connection = TRAN.Connection
                //    .CommandType = CommandType.StoredProcedure
                //    .CommandText = objWK.MyUserName & ".INV_AGENT_TBL_PKG.INV_CUST_TRN_SEA_EXP_TBL_DEL"
                //    .Parameters.Add("INV_CUST_TRN_SEA_EXP_PK_IN", OracleDbType.Int32, 10, "INV_CUST_TRN_SEA_EXP_PK").Direction = ParameterDirection.Input
                //    .Parameters["INV_CUST_TRN_SEA_EXP_PK_IN"].SourceVersion = DataRowVersion.Current
                //    .Parameters.Add("RETURN_VALUE", OracleDbType.VarChar, 100).Direction = ParameterDirection.Output
                //    .Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
                //End With
                

                var _with8 = objWK.MyDataAdapter;
                _with8.InsertCommand = insCommand;
                _with8.InsertCommand.Transaction = TRAN;
                _with8.UpdateCommand = updCommand;
                _with8.UpdateCommand.Transaction = TRAN;
                //.DeleteCommand = delCommand
                //.DeleteCommand.Transaction = TRAN
                RecAfct = _with8.Update(trnDS.Tables[0]);
                if (RecAfct == 0)
                {
                    arrMessage.Add("Save not successful");
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
            }
        }
        //This function is called to generate the invoice ref. no.
        public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
        {
            return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
        }
        #endregion

        #region " Enhance Search"
        public string FetchInvoiceAgentJCNo(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strAgentType = null;
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            strAgentType = Convert.ToString(arr.GetValue(3));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_ACT_JOB_REF_FOR_EXP_INV";

                var _with9 = selectCommand.Parameters;
                _with9.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with9.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with9.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with9.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with9.Add("AGENTTYPE_IN", strAgentType).Direction = ParameterDirection.Input;
                _with9.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }
        public string FetchCustomerForJobCard(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strJobPK = null;
            string strReq = null;
            string strLoc = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strJobPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLoc = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_JOBCARD";

                var _with10 = selectCommand.Parameters;
                _with10.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with10.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with10.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                _with10.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with10.Add("JOB_CARD_PK_IN", strJobPK).Direction = ParameterDirection.Input;
                _with10.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }
        public string FetchVoyageForJobCard(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = null;
            string strBizType = null;
            string strJobPK = null;
            string strReq = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strVES = Convert.ToString(arr.GetValue(1));
            strVOY = Convert.ToString(arr.GetValue(2));
            strBizType = Convert.ToString(arr.GetValue(3));
            strJobPK = Convert.ToString(arr.GetValue(4));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.GET_VES_VOY_JOBCARD";

                var _with11 = selectCommand.Parameters;
                _with11.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with11.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY :"")).Direction = ParameterDirection.Input;
                _with11.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with11.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with11.Add("JOB_CARD_PK_IN", strJobPK).Direction = ParameterDirection.Input;
                _with11.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }
        public string FetchJobCardForInvoice(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_ACTIVE_JOB_REF_FOR_INV";

                var _with12 = selectCommand.Parameters;
                _with12.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with12.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with12.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with12.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion

        #region " Invoice Agent Report"
        public DataSet FetchInvAgentReport(Int64 nInvPK, short AgentFlag)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "SELECT  BAT.customer_ref_no     CUST_REF_NO,INVAGTEXP.INV_AGENT_PK INVPK,";
            strSQL += "INVAGTEXP.INVOICE_REF_NO INVREFNO,";
            strSQL += "INVAGTEXP.GROSS_INV_AMT INVAMT,";
            strSQL += "NVL(INVAGTEXP.DISCOUNT_AMT,0) DICSOUNT,";
            //strSQL &= vbCrLf & "INVAGTEXP.VAT_PCNT VATPCT,"
            strSQL += "INVTAGTEXP.VAT_CODE VATPCT,";
            strSQL += "INVAGTEXP.VAT_AMT VATAMT,";
            strSQL += "JAE.JOB_CARD_TRN_PK JOBPK,";
            strSQL += "JAE.JOBCARD_REF_NO JOBREFNO,";
            strSQL += "'' CLEARANCEPOINT,";
            strSQL += "AMST.AGENT_NAME         AGENTNAME,";
            strSQL += "AMST.ACCOUNT_NO        AGENTREFNO,";
            strSQL += "ADTLS.ADM_ADDRESS_1     AGENTADD1,";
            strSQL += "ADTLS.ADM_ADDRESS_2     AGENTADD2,";
            strSQL += "ADTLS.ADM_ADDRESS_3     AGENTADD3,";
            strSQL += "ADTLS.ADM_CITY          AGENTCITY,";
            strSQL += "ADTLS.ADM_ZIP_CODE      AGENTZIP,";
            strSQL += "ADTLS.ADM_PHONE_NO_1    AGENTPHONE,";
            strSQL += "ADTLS.ADM_FAX_NO        AGENTFAX,";
            strSQL += "ADTLS.ADM_EMAIL_ID      AGENTEMAIL,";
            strSQL += "AGTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,";

            strSQL += "SHIPMST.CUSTOMER_NAME    SHIPPER,";
            strSQL += "SHIPDTLS.ADM_ADDRESS_1   SHIPPERADD1,";
            strSQL += "SHIPDTLS.ADM_ADDRESS_2   SHIPPERADD2,";
            strSQL += "SHIPDTLS.ADM_ADDRESS_3   SHIPPERADD3,";
            strSQL += "SHIPDTLS.ADM_CITY        SHIPPERCITY,";
            strSQL += "SHIPDTLS.ADM_ZIP_CODE    SHIPPERZIP,";
            strSQL += "SHIPDTLS.ADM_PHONE_NO_1  SHIPPERPHONE,";
            strSQL += "SHIPDTLS.ADM_FAX_NO      SHIPPERFAX,";
            strSQL += "SHIPDTLS.ADM_EMAIL_ID    SHIPPEREMAIL,";
            strSQL += "SHIPCOUNTRY.COUNTRY_NAME SHIPPERCOUNTRY,";

            strSQL += "FEMST.FREIGHT_ELEMENT_NAME FREIGHTNAME,";
            strSQL += "NVL(INVTAGTEXP.AMT_IN_INV_CURR,0) FREIGHTAMT,";
            strSQL += "INVTAGTEXP.TAX_PCNT FRETAXPCNT,";
            strSQL += "NVL(INVTAGTEXP.TAX_AMT,0) FRETAXANT,";
            strSQL += "JAE.ETD_DATE ETD,";
            strSQL += "JAE.ETA_DATE ETA ,";
            strSQL += "1 CARGO_TYPE,";
            strSQL += "CURRMST.CURRENCY_ID CURRID,";
            strSQL += "CURRMST.CURRENCY_NAME CURRNAME,";
            strSQL += "JAE.FLIGHT_NO VES_FLIGHT,";
            strSQL += "JAE.PYMT_TYPE PYMT,";
            strSQL += "JAE.GOODS_DESCRIPTION GOODS,";
            strSQL += "JAE.MARKS_NUMBERS MARKS,";
            strSQL += "NVL(JAE.INSURANCE_AMT, 0) INSURANCE,";
            strSQL += "STMST.INCO_CODE TERMS,";
            strSQL += "AMST.VAT_NO AGTVATNO,";
            strSQL += "COLMST.PLACE_NAME COLPLACE,";
            strSQL += "DELMST.PLACE_NAME DELPLACE,";
            strSQL += "POLMST.PORT_NAME POL,";
            strSQL += "PODMST.PORT_NAME POD,";
            strSQL += " HAWB.HAWB_REF_NO HAWBREFNO,";
            strSQL += " MAWB.MAWB_REF_NO MAWBREFNO,";
            strSQL += " CGMST.COMMODITY_GROUP_DESC COMMODITY,";
            strSQL += "SUM(JAEC.VOLUME_IN_CBM) VOLUME,";
            strSQL += "SUM(JAEC.GROSS_WEIGHT) GROSS,";
            strSQL += " 0 NETWT,";
            strSQL += "SUM(JAEC.CHARGEABLE_WEIGHT) CHARWT";
            strSQL += " FROM INV_AGENT_TBL INVAGTEXP,";
            strSQL += " CURRENCY_TYPE_MST_TBL CURRMST,";

            strSQL += " INV_AGENT_TRN_TBL INVTAGTEXP,";
            strSQL += " FREIGHT_ELEMENT_MST_TBL   FEMST,";

            strSQL += " JOB_CARD_TRN    JAE,";
            strSQL += " JOB_TRN_AIR_EXP_CONT    JAEC,";
            strSQL += " SHIPPING_TERMS_MST_TBL  STMST,";
            strSQL += "  BOOKING_MST_TBL         BAT,";
            strSQL += "  PLACE_MST_TBL           COLMST,";
            strSQL += " PLACE_MST_TBL           DELMST,";
            strSQL += " PORT_MST_TBL            POLMST,";
            strSQL += " PORT_MST_TBL            PODMST,";
            strSQL += "  HAWB_EXP_TBL            HAWB,";
            strSQL += "  MAWB_EXP_TBL            MAWB,";
            strSQL += "  COMMODITY_GROUP_MST_TBL CGMST,";

            strSQL += " AGENT_MST_TBL      AMST,";
            strSQL += " AGENT_CONTACT_DTLS ADTLS,";
            strSQL += "  COUNTRY_MST_TBL    AGTCOUNTRY,";

            strSQL += " CUSTOMER_MST_TBL      SHIPMST,";
            strSQL += " CUSTOMER_CONTACT_DTLS SHIPDTLS,";
            strSQL += "  COUNTRY_MST_TBL SHIPCOUNTRY";

            strSQL += " WHERE(INVAGTEXP.JOB_CARD_TRN_FK = JAE.JOB_CARD_TRN_PK)";
            strSQL += " AND CURRMST.CURRENCY_MST_PK(+) = INVAGTEXP.CURRENCY_MST_FK";
            strSQL += " AND INVTAGTEXP.INV_AGENT_FK(+) = INVAGTEXP.INV_AGENT_PK";
            strSQL += " AND FEMST.FREIGHT_ELEMENT_MST_PK(+) = INVTAGTEXP.COST_FRT_ELEMENT_FK";
            strSQL += " AND JAE.JOB_CARD_TRN_PK = JAEC.JOB_CARD_TRN_FK(+)";
            strSQL += " AND STMST.SHIPPING_TERMS_MST_PK(+) = JAE.SHIPPING_TERMS_MST_FK";
            strSQL += " AND BAT.BOOKING_MST_PK(+) = JAE.BOOKING_MST_FK";
            strSQL += " AND COLMST.PLACE_PK(+) = BAT.COL_PLACE_MST_FK";
            strSQL += " AND DELMST.PLACE_PK(+) = BAT.DEL_PLACE_MST_FK";
            strSQL += " AND POLMST.PORT_MST_PK = BAT.PORT_MST_POL_FK";
            strSQL += " AND PODMST.PORT_MST_PK = BAT.PORT_MST_POD_FK";
            strSQL += " AND HAWB.HAWB_EXP_TBL_PK(+) = JAE.HAWB_EXP_TBL_FK";
            strSQL += " AND MAWB.MAWB_EXP_TBL_PK(+) = JAE.MAWB_EXP_TBL_FK";
            strSQL += " AND CGMST.COMMODITY_GROUP_PK(+) = JAE.COMMODITY_GROUP_FK";
            strSQL += " AND AMST.AGENT_MST_PK(+) = INVAGTEXP.CB_AGENT_MST_FK";
            strSQL += " AND ADTLS.AGENT_MST_FK(+) = AMST.AGENT_MST_PK";
            strSQL += " AND AGTCOUNTRY.COUNTRY_MST_PK(+) = ADTLS.ADM_COUNTRY_MST_FK";

            if ((AgentFlag == 1))
            {
                strSQL += " AND SHIPMST.CUSTOMER_MST_PK(+) = JAE.Shipper_Cust_Mst_Fk";
            }
            else
            {
                strSQL += " AND SHIPMST.CUSTOMER_MST_PK(+) = JAE.CONSIGNEE_CUST_MST_FK";
            }

            strSQL += " AND SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK";
            strSQL += " AND SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)";
            strSQL += "AND INVAGTEXP.INV_AGENT_PK=" + nInvPK;

            strSQL += " GROUP BY INVAGTEXP.INV_AGENT_PK,";
            strSQL += " INVAGTEXP.INVOICE_REF_NO,";
            strSQL += " INVAGTEXP.GROSS_INV_AMT,";
            strSQL += " INVAGTEXP.DISCOUNT_AMT,";
            strSQL += " INVAGTEXP.VAT_PCNT,";
            strSQL += " INVAGTEXP.VAT_AMT,";
            strSQL += "  JAE.JOB_CARD_TRN_PK,";
            strSQL += "  JAE.JOBCARD_REF_NO,";
            strSQL += "  AMST.AGENT_NAME,";
            strSQL += "AMST.ACCOUNT_NO ,";
            strSQL += "  ADTLS.ADM_ADDRESS_1,";
            strSQL += "  ADTLS.ADM_ADDRESS_2,";
            strSQL += "  ADTLS.ADM_ADDRESS_3,";
            strSQL += "  ADTLS.ADM_CITY,";
            strSQL += "  ADTLS.ADM_ZIP_CODE,";
            strSQL += "  ADTLS.ADM_PHONE_NO_1,";
            strSQL += " ADTLS.ADM_FAX_NO,";
            strSQL += "  ADTLS.ADM_EMAIL_ID,";
            strSQL += "  AGTCOUNTRY.COUNTRY_NAME,";
            strSQL += "  SHIPMST.CUSTOMER_NAME,";
            strSQL += "  SHIPDTLS.ADM_ADDRESS_1,";
            strSQL += "  SHIPDTLS.ADM_ADDRESS_2,";
            strSQL += " SHIPDTLS.ADM_ADDRESS_3,";
            strSQL += " SHIPDTLS.ADM_CITY,";
            strSQL += " SHIPDTLS.ADM_ZIP_CODE,";
            strSQL += "  SHIPDTLS.ADM_PHONE_NO_1,";
            strSQL += " SHIPDTLS.ADM_FAX_NO,";
            strSQL += " SHIPDTLS.ADM_EMAIL_ID,";
            strSQL += "  SHIPCOUNTRY.COUNTRY_NAME,";
            strSQL += "  FEMST.FREIGHT_ELEMENT_NAME,";
            strSQL += "  INVTAGTEXP.AMT_IN_INV_CURR,";
            strSQL += "  INVTAGTEXP.TAX_PCNT,";
            strSQL += "  INVTAGTEXP.TAX_AMT,";
            strSQL += " JAE.ETD_DATE ,";
            strSQL += "JAE.ETA_DATE,";
            strSQL += " CURRMST.CURRENCY_ID,";
            strSQL += "  CURRMST.CURRENCY_NAME,";
            strSQL += "  JAE.FLIGHT_NO,";
            strSQL += "  JAE.PYMT_TYPE,";
            strSQL += "  JAE.GOODS_DESCRIPTION,";
            strSQL += "  JAE.MARKS_NUMBERS,";
            strSQL += "  JAE.INSURANCE_AMT,";
            strSQL += "  STMST.INCO_CODE,";
            strSQL += "AMST.VAT_NO,";
            strSQL += " COLMST.PLACE_NAME,";
            strSQL += " DELMST.PLACE_NAME,";
            strSQL += " POLMST.PORT_NAME,";
            strSQL += "  PODMST.PORT_NAME,";
            strSQL += "  HAWB.HAWB_REF_NO,";
            strSQL += "  MAWB.MAWB_REF_NO,";
            strSQL += " CGMST.COMMODITY_GROUP_DESC,";
            strSQL += " BAT.customer_ref_no,INVTAGTEXP.VAT_CODE";
            try
            {
                return (objWK.GetDataSet(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchContainerDetails(Int64 nInvPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "SELECT JTSIC.PALETTE_SIZE  CONTAINER";
            strSQL += "FROM INV_AGENT_TBL IASI,";
            strSQL += "JOB_CARD_TRN  JAE,";
            strSQL += "JOB_TRN_AIR_EXP_CONT JTSIC";
            strSQL += "WHERE IASI.JOB_CARD_TRN_FK = JAE.JOB_CARD_TRN_PK";
            strSQL += "AND JTSIC.JOB_CARD_TRN_FK = JAE.JOB_CARD_TRN_PK";
            strSQL += "AND IASI.INV_AGENT_PK = " + nInvPK;
            try
            {
                return (objWK.GetDataSet(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Invoice Detail Report"
        public object INV_DETAIL_PRINT(Int64 nInvPK)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INVAGTEXP.INV_AGENT_PK,");
            sb.Append("       INVAGTEXP.INVOICE_REF_NO,");
            sb.Append("       INVAGTEXP.INVOICE_DATE,");
            sb.Append("       INVAGTEXP.INVOICE_DUE_DATE,");
            sb.Append("       CMST.CURRENCY_ID,");
            sb.Append("       sum(INVTAGTEXP.ELEMENT_AMT * INVTAGTEXP.EXCHANGE_RATE) TOTAMT,");
            sb.Append("       sum(NVL(INVTAGTEXP.TAX_AMT,0)) TAX_AMT,");
            sb.Append("       SUM(DISTINCT(NVL(INVAGTEXP.DISCOUNT_AMT, 0))) DICSOUNT,");
            sb.Append("       SUM(DISTINCT(NVL(INVAGTEXP.NET_INV_AMT, 0))) NET_INV_AMT,");
            sb.Append("       INVTAGTEXP.REMARKS");
            sb.Append("  FROM INV_AGENT_TBL     INVAGTEXP,");
            sb.Append("       CURRENCY_TYPE_MST_TBL     CMST,");
            sb.Append("       INV_AGENT_TRN_TBL INVTAGTEXP");
            sb.Append(" WHERE CMST.CURRENCY_MST_PK = INVAGTEXP.CURRENCY_MST_FK");
            sb.Append("   AND INVTAGTEXP.INV_AGENT_FK = INVAGTEXP.INV_AGENT_PK");
            sb.Append("   AND INVAGTEXP.INV_AGENT_PK =" + nInvPK + "");
            sb.Append(" GROUP BY INVAGTEXP.INV_AGENT_PK,");
            sb.Append("          INVAGTEXP.INVOICE_REF_NO,");
            sb.Append("          CMST.CURRENCY_ID,");
            sb.Append("          INVTAGTEXP.REMARKS,");
            sb.Append("          INVAGTEXP.INVOICE_DATE,");
            sb.Append("          INVAGTEXP.INVOICE_DUE_DATE");
            try
            {
                return (objWK.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region " Invoice CONSOL_INV_DETAIL_MAIN_PRINT Report"
        public DataSet CONSOL_INV_DETAIL_MAIN_PRINT(Int64 nInvPK, string UserName)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("select DISTINCT INVET.INV_AGENT_PK CONSOL_INVOICE_PK,");
            sb.Append("                JSET.JOBCARD_REF_NO,");
            sb.Append("                AMT.AGENT_NAME  CUSTOMER_NAME,");
            sb.Append("                PMTL.PORT_NAME POL,");
            sb.Append("                PMTD.PORT_NAME POD,");

            sb.Append("                HET.HAWB_REF_NO HBL_REF_NO,");
            sb.Append("                ' ' MBL_REF_NO,");

            //'AIR IMP
            sb.Append("                JSET.HBL_HAWB_REF_NO HBL_REF_NO,");
            sb.Append("                JSET.MBL_MAWB_REF_NO MBL_REF_NO,");
            ///'''''

            sb.Append("                SUM(INTVET.AMT_IN_INV_CURR) TOT,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN JSET.ARRIVAL_DATE IS NULL THEN");
            sb.Append("                   JSET.ETA_DATE");
            sb.Append("                  ELSE");
            sb.Append("                   JSET.ARRIVAL_DATE");
            sb.Append("                END) ARR_DEP_DATE,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN JSET.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
            sb.Append("                   JSET.VOYAGE_FLIGHT_NO");
            sb.Append("                  ELSE");
            sb.Append("                   ' '");
            sb.Append("                END) VSL_VOY,");
            sb.Append("                OMT.AIRLINE_NAME OPERATOR_NAME,");
            sb.Append("                PMTL.PORT_NAME PLR,");
            sb.Append("                PMTD.PORT_NAME PFD,");
            sb.Append("          '" + UserName + "' USER_NAME");
            sb.Append("  FROM INV_AGENT_TBL     INVET,");
            sb.Append("       JOB_CARD_TRN      JSET,");
            sb.Append("       INV_AGENT_TRN_TBL INTVET,");
            sb.Append("       BOOKING_MST_TBL           BST,");
            sb.Append("       AIRLINE_MST_TBL           OMT,");
            sb.Append("       AGENT_MST_TBL             AMT,");
            sb.Append("       PORT_MST_TBL              PMTL,");
            sb.Append("       PORT_MST_TBL              PMTD,");
            sb.Append("       HAWB_EXP_TBL              HET");

            sb.Append("  WHERE INVET.JOB_CARD_FK = JSET.JOB_CARD_TRN_PK");
            sb.Append("   AND INTVET.INV_AGENT_FK = INVET.INV_AGENT_PK");
            sb.Append("   AND OMT.AIRLINE_MST_PK = BST.CARRIER_MST_FK");
            sb.Append("   AND INVET.AGENT_MST_FK =  AMT.AGENT_MST_PK ");
            sb.Append("   AND BST.PORT_MST_POL_FK = PMTL.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PMTD.PORT_MST_PK(+)");

            //air exp
            sb.Append("   AND JSET.HBL_HAWB_FK = HET.JOB_CARD_AIR_EXP_FK(+)");
            sb.Append("   AND JSET.BOOKING_MST_FK = BST.BOOKING_MST_PK");
            ///''''''''''''''''''''

            sb.Append("   AND INVET.INV_AGENT_PK =" + nInvPK + "");
            sb.Append(" GROUP BY INV_AGENT_PK,");
            sb.Append("          JOBCARD_REF_NO,");
            sb.Append("          AMT.AGENT_NAME, ");
            sb.Append("          PMTL.PORT_NAME,");
            sb.Append("          PMTD.PORT_NAME,");
            sb.Append("          HET.HAWB_REF_NO,");
            sb.Append("          JSET.ARRIVAL_DATE,");
            sb.Append("          JSET.ETA_DATE,");
            sb.Append("          JSET.VOYAGE_FLIGHT_NO,");
            sb.Append("          OMT.AIRLINE_NAME,JSET.HBL_HAWB_REF_NO,JSET.MBL_MAWB_REF_NO");
            try
            {
                return (objWK.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region " Invoice INV_DETAIL_MAIN_PRINT Report"
        public DataSet CONSOL_INV_DETAIL_SUB_MAIN_PRINT(Int64 nInvPK)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INVET.INV_AGENT_PK CONSOL_INVOICE_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_NAME FRT_DESC,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               SUM(DISTINCT INTVET.ELEMENT_AMT) ELEMENT_AMT,");
            sb.Append("               INTVET.EXCHANGE_RATE,");
            sb.Append("               SUM(DISTINCT(INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE)) TOTAMT,");
            sb.Append("               JSET.JOBCARD_REF_NO FREIGHT_ELEMENT_ID,");
            sb.Append("               INTVET.TAX_AMT,");
            sb.Append("                INTVET.TAX_PCNT,");
            sb.Append("              (SUM(DISTINCT(INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE))+INTVET.TAX_AMT) INVOICE");
            sb.Append("          FROM INV_AGENT_TRN_TBL  INTVET,");
            sb.Append("               INV_AGENT_TBL      INVET,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL   CTMT,");
            sb.Append("               JOB_TRN_FD      JCFD,");
            sb.Append("                JOB_CARD_TRN   JSET");
            sb.Append("         WHERE INTVET.INV_AGENT_FK = INVET.INV_AGENT_PK");
            sb.Append("           AND INTVET.COST_FRT_ELEMENT_FK= FEMT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("           AND INTVET.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+)");
            sb.Append("           AND INVET.JOB_CARD_FK = JSET.JOB_CARD_TRN_PK(+)");
            sb.Append("           AND JSET.JOB_CARD_TRN_PK = JCFD.JOB_CARD_TRN_FK");
            sb.Append("           AND INTVET.COST_FRT_ELEMENT IN (1,2)");
            sb.Append("           AND INVET.INV_AGENT_PK = " + nInvPK + "");
            sb.Append("           GROUP BY INVET.INV_AGENT_PK,");
            sb.Append("                 CTMT.CURRENCY_ID,");
            sb.Append("                  INTVET.EXCHANGE_RATE,");
            sb.Append("                 FEMT.FREIGHT_ELEMENT_NAME,");
            sb.Append("                  FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("                   INTVET.TAX_PCNT,");
            sb.Append("                   INTVET.TAX_AMT,");
            sb.Append("                   JSET.JOBCARD_REF_NO");
            sb.Append("          UNION  ");
            sb.Append("                    SELECT INVET.INV_AGENT_PK CONSOL_INVOICE_FK,");
            sb.Append("                FEMT.FREIGHT_ELEMENT_NAME FRT_DESC,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               SUM(DISTINCT INTVET.ELEMENT_AMT) ELEMENT_AMT,");
            sb.Append("               INTVET.EXCHANGE_RATE,");
            sb.Append("               SUM((INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE)) TOTAMT,");
            sb.Append("                JSET.JOBCARD_REF_NO FREIGHT_ELEMENT_ID,");
            sb.Append("               INTVET.TAX_AMT,");
            sb.Append("                INTVET.TAX_PCNT,");
            sb.Append("              (SUM(DISTINCT(INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE))+INTVET.TAX_AMT) INVOICE");
            sb.Append("            FROM INV_AGENT_TRN_TBL  INTVET,");
            sb.Append("               INV_AGENT_TBL      INVET,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT,");
            sb.Append("               JOB_TRN_OTH_CHRG JCOTH,");
            sb.Append("               JOB_CARD_TRN     JSET");
            sb.Append("         WHERE INTVET.INV_AGENT_FK = INVET.INV_AGENT_PK");
            sb.Append("           AND INTVET.COST_FRT_ELEMENT_FK= FEMT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("           AND INTVET.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+)");
            sb.Append("           AND INVET.JOB_CARD_FK = JSET.JOB_CARD_TRN_PK(+)");
            sb.Append("           AND JSET.JOB_CARD_TRN_PK = JCOTH.JOB_CARD_TRN_FK(+)");
            sb.Append("           AND INTVET.COST_FRT_ELEMENT = 3");
            sb.Append("           AND INVET.INV_AGENT_PK = " + nInvPK + "");
            sb.Append("        GROUP BY INVET.INV_AGENT_PK,");
            sb.Append("                  CTMT.CURRENCY_ID,");
            sb.Append("                  INTVET.EXCHANGE_RATE,");
            sb.Append("                  FEMT.FREIGHT_ELEMENT_NAME,");
            sb.Append("                  FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("                   INTVET.TAX_AMT,");
            sb.Append("                    INTVET.TAX_PCNT,");
            sb.Append("                   JSET.JOBCARD_REF_NO");
            try
            {
                return (objWK.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region " Invoice INV_CUST_PRINT Report"
        public DataSet CONSOL_INV_CUST_PRINT(Int64 nInvPK)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT INVET.INV_AGENT_PK CONSOL_INVOICE_PK,");
            sb.Append("                        AMT.AGENT_NAME CUSTOMER_NAME,");
            sb.Append("                        ACD.ADM_ADDRESS_1,");
            sb.Append("                        ACD.ADM_ADDRESS_2,");
            sb.Append("                        ACD.ADM_ADDRESS_3,");
            sb.Append("                        ACD.ADM_ZIP_CODE,");
            sb.Append("                        ACD.ADM_CITY,");
            sb.Append("                        CMT.COUNTRY_NAME");
            sb.Append("          FROM INV_AGENT_TRN_TBL  INTVET,");
            sb.Append("               INV_AGENT_TBL      INVET,");
            sb.Append("               JOB_CARD_TRN   JCSE,");
            sb.Append("               AGENT_MST_TBL          AMT,");
            sb.Append("               AGENT_CONTACT_DTLS     ACD,");
            sb.Append("               COUNTRY_MST_TBL        CMT");
            sb.Append("         WHERE INTVET.INV_AGENT_FK = INVET.INV_AGENT_PK");
            sb.Append("           AND INVET.JOB_CARD_FK = JCSE.JOB_CARD_TRN_PK(+)");
            sb.Append("           AND INVET.AGENT_MST_FK = AMT.AGENT_MST_PK");
            sb.Append("           AND AMT.AGENT_MST_PK = ACD.AGENT_MST_FK");
            sb.Append("           AND ACD.ADM_COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
            sb.Append("           AND INVET.INV_AGENT_PK =" + nInvPK + "");
            try
            {
                return (objWK.GetDataSet(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fill Jobcard Other Details"

        public DataSet FillJobCardOtherCharges(string pk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("         SELECT");
            strSQL.Append("         oth_chrg.JOB_TRN_OTH_PK,");
            strSQL.Append("         frt.freight_element_mst_pk,");
            strSQL.Append("         frt.freight_element_id,");
            strSQL.Append("         frt.freight_element_name,");
            //strSQL.Append(vbCrLf & "         DECODE(oth_chrg.freight_type,1,'Prepaid',2,'Collect') Payment_Type, ")
            strSQL.Append("         oth_chrg.freight_type Payment_Type, ");
            strSQL.Append("         oth_chrg.location_mst_fk,");
            strSQL.Append("         lmt.location_id ,");
            strSQL.Append("         oth_chrg.frtpayer_cust_mst_fk,");
            strSQL.Append("         cmt.customer_id,");
            strSQL.Append("         curr.currency_mst_pk, oth_chrg.EXCHANGE_RATE \"ROE\",");
            strSQL.Append("         oth_chrg.amount amount,oth_chrg.INV_AGENT_TRN_FK,");
            strSQL.Append("         'false' \"Delete\", oth_chrg.PRINT_ON_MAWB \"Print\"");
            strSQL.Append("  FROM");
            strSQL.Append("         JOB_TRN_OTH_CHRG oth_chrg,");
            strSQL.Append("         JOB_CARD_TRN jobcard_mst,");
            strSQL.Append("         freight_element_mst_tbl frt,");
            strSQL.Append("         currency_type_mst_tbl curr,");
            strSQL.Append("         location_mst_tbl lmt,");
            strSQL.Append("         customer_mst_tbl cmt");
            strSQL.Append("  WHERE");
            strSQL.Append("         oth_chrg.JOB_CARD_TRN_FK = jobcard_mst.JOB_CARD_TRN_PK");
            strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.JOB_CARD_TRN_FK    = " + pk);
            strSQL.Append("         AND oth_chrg.location_mst_fk = lmt.location_mst_pk (+)");
            strSQL.Append("         AND oth_chrg.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
            strSQL.Append(" ORDER BY freight_element_id ");
            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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
        #endregion

        #region "Get Consignee PK"
        public int GetConsigneePK(string JOBPK)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append("   SELECT");
            strSQL.Append("   jobcard_mst.Consignee_Cust_Mst_Fk");
            strSQL.Append("  FROM");
            strSQL.Append("   JOB_CARD_TRN jobcard_mst");
            strSQL.Append("   WHERE jobcard_mst.JOB_CARD_TRN_PK = " + JOBPK);
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
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
        #endregion

        #region "Get Agent Freight Details"
        public DataSet GetAgtFrtDetails(string JOBPK)
        {
            string strSQL = null;
            strSQL = " SELECT DECODE(TRN.COST_FRT_ELEMENT,1,'FREIGHT',2,'FREIGHT',3,'OTHER') AS TYPE,";
            strSQL += " TRN.INV_AGENT_TRN_PK AS PK,";
            strSQL += " HDR.JOB_CARD_TRN_FK AS JOBCARD_FK,";
            strSQL += " TRN.COST_FRT_ELEMENT_FK AS ELEMENT_FK,";
            strSQL += " TRN.CURRENCY_MST_FK,";
            strSQL += " (CASE TRN.COST_FRT_ELEMENT WHEN 1 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 2 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 3 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) END) AS ELEMENT_NAME,";
            strSQL += " '' AS ELEMENT_SEARCH,";
            strSQL += " CUMT.CURRENCY_ID,";
            strSQL += " '' AS CURR_SEARCH,";
            strSQL += " TRN.ELEMENT_AMT AS AMOUNT,";
            strSQL += " TRN.EXCHANGE_RATE AS EXCHANGE_RATE,";
            strSQL += " TRN.AMT_IN_INV_CURR AS AMOUNT_LOCAL_CURR,";
            strSQL += "  (CASE";
            strSQL += " WHEN TRN.VAT_CODE = '0' THEN ";
            strSQL += "   '' ";
            strSQL += "  ELSE";
            strSQL += " TRN.VAT_CODE ";
            strSQL += " END) VAT_CODE,";
            strSQL += " to_number('' || TRN.TAX_PCNT || '') AS VAT_PERCENT,";
            strSQL += " to_number('' || TRN.TAX_AMT || '')  AS TAX_AMOUNT,";
            strSQL += " TRN.TOT_AMT AS TOTAL_AMOUNT,";
            strSQL += " TRN.REMARKS,";
            strSQL += " 'Edit' AS \"MODE\",";
            strSQL += " 'True' AS CHK, HDR.CB_OR_DP_AGENT";
            strSQL += " FROM";
            strSQL += " INV_AGENT_TRN_TBL TRN,";
            strSQL += " INV_AGENT_TBL HDR,";
            strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
            strSQL += " WHERE";
            strSQL += " TRN.INV_AGENT_FK = HDR.INV_AGENT_PK";
            strSQL += " AND TRN.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            strSQL += " AND HDR.JOB_CARD_TRN_FK = " + JOBPK;
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL);
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
        #endregion

        #region "Save Other Charge Details"
        public ArrayList SaveOthDetails(ref DataSet dsOtherCharges, long JOBPK)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand insOtherChargesDetails = new OracleCommand();
            Int32 RecAfct = default(Int32);
            try
            {
                var _with13 = insOtherChargesDetails;
                _with13.Connection = objWK.MyConnection;
                _with13.CommandType = CommandType.StoredProcedure;
                _with13.CommandText = objWK.MyUserName + ".INV_AGENT_SEA_EXP_TBL_PKG.JOB_TRN_OTH_CHRG_INS";
                var _with14 = _with13.Parameters;

                insOtherChargesDetails.Parameters.Add("JOB_CARD_TRN_FK_IN", JOBPK).Direction = ParameterDirection.Input;

                insOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("INV_AGENT_TRN_FK_IN", OracleDbType.Int32, 10, "INV_AGENT_TRN_FK").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["INV_AGENT_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_OTH_PK").Direction = ParameterDirection.Output;
                insOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with15 = objWK.MyDataAdapter;

                _with15.InsertCommand = insOtherChargesDetails;
                _with15.InsertCommand.Transaction = TRAN;

                RecAfct = _with15.Update(dsOtherCharges);

                if (RecAfct > 0)
                {
                    TRAN.Commit();
                    arrMessage.Clear();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }
        #endregion

    }
}