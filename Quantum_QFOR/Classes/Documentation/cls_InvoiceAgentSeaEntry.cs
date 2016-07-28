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
    public class clsInvoiceAgentSeaEntry : CommonFeatures
    {
        #region "Fetch"

        //This function is called to fetch all the job card cost elements
        //of a selected job card.
        //Used to populate the job card cost elements grid in the invoice page
        /// <summary>
        /// Fetches the cost details.
        /// </summary>
        /// <param name="nJobCardPK">The n job card pk.</param>
        /// <returns></returns>
        public DataSet FetchCostDetails(long nJobCardPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;

            strSQL = "SELECT ";
            strSQL += " JOBCOST.JOB_TRN_COST_PK JOB_TRN_SEA_EXP_PIA_PK,";
            strSQL += " JOBCOST.JOB_CARD_TRN_FK,";
            strSQL += " JOBCOST.COST_ELEMENT_MST_FK,";
            strSQL += " JOBCOST.CURRENCY_MST_FK,";
            strSQL += " CEMT.COST_ELEMENT_NAME,";
            strSQL += " CUMT.CURRENCY_ID,";
            strSQL += " JOBCOST.ESTIMATED_COST ESTIMATED_AMT,";
            strSQL += " JOBCOST.TOTAL_COST INVOICE_AMT,";
            strSQL += " ROUND((CASE WHEN (JOBCOST.INV_SUPPLIER_FK IS NULL AND JOBCOST.INV_AGENT_TRN_FK IS NULL) THEN NULL WHEN JOBCOST.INV_SUPPLIER_FK IS NOT NULL THEN (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_SEA_EXP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_CUST_TRN_SEA_EXP_PK=JOBCOST.INV_SUPPLIER_FK) ELSE (SELECT TRN1.TOT_AMT FROM INV_AGENT_TRN_TBL TRN1 WHERE TRN1.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN1.COST_FRT_ELEMENT=1 AND TRN1.INV_AGENT_TRN_PK=JOBCOST.INV_AGENT_TRN_FK) END),2) INV_AMT,";
            strSQL += " (CASE WHEN (JOBCOST.INV_SUPPLIER_FK IS NULL AND JOBCOST.INV_AGENT_TRN_FK IS NULL) THEN 'False' ELSE 'True' END) CHK";

            //strSQL &= vbCrLf & " FROM JOB_TRN_SEA_EXP_PIA JOBCOST,"
            strSQL += " FROM JOB_TRN_COST JOBCOST,";
            strSQL += " COST_ELEMENT_MST_TBL CEMT,";
            strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
            strSQL += " WHERE ";
            strSQL += " JOBCOST.COST_ELEMENT_MST_FK = CEMT.COST_ELEMENT_MST_PK";
            strSQL += " AND JOBCOST.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            strSQL += " AND JOBCOST.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);

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

        //This function is called to fetch all the job card freight elements
        //of a selected job card
        //Used to populate the job card freight elements grid in the invoice page
        /// <summary>
        /// Fetches the freight details.
        /// </summary>
        /// <param name="nJobCardPK">The n job card pk.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <returns></returns>
        public DataSet FetchFreightDetails(long nJobCardPK, short AgentType)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;

            strSQL = " SELECT Q.JOB_TRN_FD_PK, ";
            strSQL += " Q.JOB_CARD_TRN_FK,Q.FREIGHT_ELEMENT_MST_FK,";
            strSQL += " Q.CURRENCY_MST_FK,Q.FREIGHT_ELEMENT_NAME,";
            strSQL += " Q.PC,Q.CURRENCY_ID,";
            strSQL += " Q.FREIGHT_AMT,Q.INV_AMT,Q.CHK";
            strSQL += " FROM (SELECT P.* ";
            strSQL += " FROM (SELECT ";
            strSQL += " JOBFRT.JOB_TRN_FD_PK,";
            strSQL += " JOBFRT.JOB_CARD_TRN_FK,";
            strSQL += " JOBFRT.FREIGHT_ELEMENT_MST_FK,";
            strSQL += " JOBFRT.CURRENCY_MST_FK,";
            strSQL += " FMT.FREIGHT_ELEMENT_NAME,";
            strSQL += " FMT.PREFERENCE,";
            strSQL += " DECODE(JOBFRT.FREIGHT_TYPE,1,'P',2,'C',3,'F') AS PC,";
            //strSQL &= vbCrLf & " DECODE(exchange_rate,'null',1,0,1),"
            strSQL += " CUMT.CURRENCY_ID,";
            strSQL += " JOBFRT.FREIGHT_AMT,";

            strSQL += "ROUND((CASE WHEN (JOBFRT.INV_AGENT_TRN_FK IS NULL AND JOBFRT.INVOICE_TBL_FK IS NULL and (jobfrt.CONSOL_INVOICE_TRN_FK is null or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=jobfrt.CONSOL_INVOICE_TRN_FK)=2) ) THEN NULL WHEN JOBFRT.INV_AGENT_TRN_FK IS NOT NULL THEN (SELECT (TRN.Element_Amt+trn.tax_amt) TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT in (1,2) AND TRN.Inv_Agent_Trn_Pk=JOBFRT.INV_AGENT_TRN_FK)  WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN (SELECT SUM(CTRN.TOT_AMT / DECODE(Ctrn.exchange_rate,NULL,1,0,1,Ctrn.exchange_rate)) TOT_AMT FROM CONSOL_INVOICE_TRN_TBL CTRN WHERE CTRN.FRT_OTH_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK AND CTRN.FRT_OTH_ELEMENT = 1 AND (CTRN.JOB_CARD_FK = JOBFRT.JOB_CARD_TRN_FK) ) ELSE (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_CUST_TRN_SEA_EXP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT in (1,2) AND TRN.INV_CUST_TRN_SEA_EXP_PK= JOBFRT.INVOICE_TBL_FK)END),2) INV_AMT,";

            strSQL += " (CASE WHEN JOBFRT.INV_AGENT_TRN_FK IS NULL AND JOBFRT.INVOICE_TBL_FK IS NULL and (jobfrt.CONSOL_INVOICE_TRN_FK is null or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=jobfrt.CONSOL_INVOICE_TRN_FK)=2) THEN 'False' ELSE 'True' END) CHK";
            strSQL += " FROM ";
            strSQL += " JOB_TRN_FD JOBFRT,";
            strSQL += " FREIGHT_ELEMENT_MST_TBL FMT,";
            strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
            strSQL += " WHERE";
            strSQL += " JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
            //surya18Nov06 If AgentType = 2 Then
            //    strSQL &= vbCrLf & " AND JOBFRT.FREIGHT_TYPE=2"
            //End If
            strSQL += " AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            strSQL += " AND JOBFRT.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);

            //surya18Nov06 If AgentType = 1 Then
            strSQL += " ";
            strSQL += " UNION";
            strSQL += " ";
            strSQL += " SELECT ";
            strSQL += " JOBOTH.JOB_TRN_OTH_PK,";
            strSQL += " JOBOTH.JOB_CARD_TRN_FK,";
            strSQL += " JOBOTH.FREIGHT_ELEMENT_MST_FK,";
            strSQL += " JOBOTH.CURRENCY_MST_FK,";
            strSQL += " FMT.FREIGHT_ELEMENT_NAME,";
            strSQL += " FMT.PREFERENCE,";
            //strSQL &= vbCrLf & " 'P' AS PC,"
            if (AgentType == 1)
            {
                //strSQL &= vbCrLf & " 'P' AS PC,"
                strSQL += " DECODE(JOBOTH.FREIGHT_TYPE,1,'P',2,'C',3,'F') AS PC,";
            }
            else
            {
                //strSQL &= vbCrLf & " 'C' AS PC,"
                strSQL += " DECODE(JOBOTH.FREIGHT_TYPE,1,'P',2,'C',3,'F') AS PC,";
            }
            strSQL += " CUMT.CURRENCY_ID,";
            strSQL += " JOBOTH.AMOUNT,";
            //add by latha for fetching the invoiceamt by converting into its currency on january 31
            strSQL += " ROUND((CASE WHEN (JOBOTH.Inv_Agent_Trn_Fk IS NULL AND JOBOTH.INV_CUST_TRN_FK IS NULL and (JOBOTH.CONSOL_INVOICE_TRN_FK is null or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=jobOTH.CONSOL_INVOICE_TRN_FK)=2)) THEN NULL WHEN JOBOTH.Inv_Agent_Trn_Fk IS not NULL then (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_agent_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=3 AND TRN.INV_agent_TRN_PK=JOBOTH.INV_agent_TRN_FK) WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN (SELECT SUM(CTRN.TOT_AMT / DECODE(Ctrn.exchange_rate,NULL,1,0,1,Ctrn.exchange_rate)) TOT_AMT FROM CONSOL_INVOICE_TRN_TBL CTRN WHERE CTRN.FRT_OTH_ELEMENT_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK AND CTRN.FRT_OTH_ELEMENT = 2 AND (CTRN.JOB_CARD_FK = JOBOTH.JOB_CARD_TRN_FK)) else (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_CUST_TRN_SEA_EXP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=3 AND TRN.INV_CUST_TRN_SEA_EXP_PK=JOBOTH.INV_CUST_TRN_FK) END),2 )INV_AMT,";
            strSQL += " (CASE WHEN (JOBOTH.Inv_Cust_Trn_Fk is null and joboth.inv_agent_trn_fk is null and (jobOTH.CONSOL_INVOICE_TRN_FK is null or  (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=jobOTH.CONSOL_INVOICE_TRN_FK)=2)) THEN 'False' ELSE 'True' END) CHK";
            strSQL += " FROM ";
            strSQL += " JOB_TRN_OTH_CHRG JOBOTH,";
            strSQL += " FREIGHT_ELEMENT_MST_TBL FMT,";
            strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
            strSQL += " WHERE";
            strSQL += " JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
            strSQL += " AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";

            strSQL += " AND JOBOTH.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
            // added by jitendra
            if (AgentType == 2)
            {
                strSQL += "AND JOBOTH.Freight_Type=2";
            }
            strSQL += " ) P ORDER BY P.PREFERENCE) Q";
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
        //       1.Select all invoice elements if it is an existing invoice where the currency is not equal to the the invoice currency
        //       1.Select all invoice elements if it is an existing invoice where the currency is equal to the the invoice currency
        //Used to populate the invoice elements grid in the invoice page
        /// <summary>
        /// Fetches the invoice details.
        /// </summary>
        /// <param name="nJobCardPK">The n job card pk.</param>
        /// <param name="nBaseCurrPK">The n base curr pk.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <param name="nInvoicePK">The n invoice pk.</param>
        /// <param name="UserPk">The user pk.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceDetails(long nJobCardPK, long nBaseCurrPK, short AgentType, long nInvoicePK = 0, string UserPk = "0")
        {
            string strSQL = null;
            string str = null;
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;
            if (nInvoicePK == 0)
            {
                string vatcode = null;
                string custpk = null;
                strSQL = " select FETCH_EU(" + nJobCardPK + ",2,1) from dual";
                vatcode = objWK.ExecuteScaler(strSQL);
                if (AgentType == 1)
                {
                    strSQL = " SELECT J.CB_AGENT_MST_FK FROM JOB_CARD_TRN J WHERE J.JOB_CARD_TRN_PK=" + nJobCardPK;
                }
                else if (AgentType == 4)
                {
                    strSQL = " SELECT J.TARIFF_AGENT_MST_FK FROM JOB_CARD_TRN J WHERE J.JOB_CARD_TRN_PK=" + nJobCardPK;
                }
                else
                {
                    strSQL = " SELECT J.DP_AGENT_MST_FK FROM JOB_CARD_TRN J WHERE J.JOB_CARD_TRN_PK=" + nJobCardPK;
                }
                custpk = objWK.ExecuteScaler(strSQL);
                if (string.IsNullOrEmpty(getDefault(custpk, "").ToString()))
                {
                    custpk = "0";
                }
                strSQL = "";
                strSQL += " SELECT 'FREIGHT' AS TYPE,";
                strSQL += " JOBFRT.JOB_TRN_FD_PK AS PK,";
                strSQL += " JOBFRT.JOB_CARD_TRN_FK AS JOBCARD_FK,";
                strSQL += " JOBFRT.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,";
                strSQL += " JOBFRT.CURRENCY_MST_FK,";
                strSQL += " FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,";
                strSQL += " '' AS ELEMENT_SEARCH,";
                strSQL += " CUMT.CURRENCY_ID,";
                strSQL += " '' AS CURR_SEARCH,";
                strSQL += " ROUND(JOBFRT.FREIGHT_AMT,2) AS AMOUNT,";
                strSQL += " JOBFRT.EXCHANGE_RATE, ";
                strSQL += " ROUND(JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE,2)  AMOUNT_LOCAL_CURR,";

                //VAT CODE
                strSQL += " (select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,1,2) from dual) VAT_CODE, ";
                strSQL += " (select FETCH_VAT ( " + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2,2) from dual) VAT_PERCENT,";
                strSQL += " ((select FETCH_VAT ( " + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2,2) from dual)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) TAX_AMOUNT,";
                strSQL += "  (((select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2,2) from dual)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) + JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) TOTAL_AMOUNT,";
                strSQL += " '' AS REMARKS,";
                strSQL += " 'New' AS \"MODE\",";
                strSQL += " 'False' AS CHK";
                strSQL += " FROM ";
                strSQL += " JOB_TRN_FD JOBFRT,";
                strSQL += " FREIGHT_ELEMENT_MST_TBL FMT,";
                strSQL += " CURRENCY_TYPE_MST_TBL CUMT,";
                strSQL += " CORPORATE_MST_TBL CORP";
                strSQL += " WHERE";
                strSQL += " JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
                strSQL += " AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
                strSQL += " AND JOBFRT.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
                //strSQL &= vbCrLf & " AND JOBFRT.INV_AGENT_TRN_FK IS NULL"
                ///'''''''''
                if (AgentType == 1)
                {
                    strSQL += " AND JOBFRT.FREIGHT_TYPE IN (1,2)";
                }
                else if (AgentType == 4)
                {
                    strSQL += " AND JOBFRT.FREIGHT_TYPE IN (3)";
                }
                else
                {
                    strSQL += " AND JOBFRT.FREIGHT_TYPE = 2 ";
                }
                strSQL += " AND JOBFRT.INVOICE_TBL_FK IS NULL";
                //strSQL &= vbCrLf & " AND (JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=JOBFRT.CONSOL_INVOICE_TRN_FK)=2) "
                strSQL += " AND (JOBFRT.INV_AGENT_TRN_FK IS NULL or (SELECT cit.chk_invoice  FROM INV_AGENT_TBL CIT, INV_AGENT_TRN_TBL CTRN ";
                strSQL += "  WHERE CIT.INV_AGENT_PK = CTRN.INV_AGENT_FK   AND CTRN.INV_AGENT_TRN_PK = JOBFRT.INV_AGENT_TRN_FK) = 2) ";
                ///'''''''''
                strSQL += " ";
                strSQL += " UNION";
                strSQL += " ";
                strSQL += " SELECT 'OTHER' AS TYPE,";
                strSQL += " JOBOTH.JOB_TRN_OTH_PK AS PK,";
                strSQL += " JOBOTH.JOB_CARD_TRN_FK AS JOBCARD_FK,";
                strSQL += " JOBOTH.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,";
                strSQL += " JOBOTH.CURRENCY_MST_FK,";
                strSQL += " FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,";
                strSQL += " '' AS ELEMENT_SEARCH,";
                strSQL += " CUMT.CURRENCY_ID,";
                strSQL += " '' AS CURR_SEARCH,";
                strSQL += " JOBOTH.AMOUNT AS AMOUNT,";
                strSQL += " JOBOTH.EXCHANGE_RATE AS EXCHANGE_RATE,";
                strSQL += " ROUND(JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE,2) AS AMOUNT_LOCAL_CURR,";
                strSQL += " (select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,1,2) from dual) VAT_CODE,";

                strSQL += "(select FETCH_VAT ( " + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2,2) from dual) VAT_PERCENT,";

                strSQL += "((select FETCH_VAT(" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2,2) from dual)* (JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100) TAX_AMOUNT,";

                strSQL += "(((select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2,2) from dual)* (JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100) + JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE) TOTAL_AMOUNT,";
                strSQL += " '' AS REMARKS,";
                strSQL += " 'New' AS \"MODE\",";
                strSQL += " 'False' AS CHK";
                strSQL += " FROM ";
                strSQL += " JOB_TRN_OTH_CHRG JOBOTH,";
                strSQL += " FREIGHT_ELEMENT_MST_TBL FMT,";
                strSQL += " CURRENCY_TYPE_MST_TBL CUMT,";
                strSQL += " CORPORATE_MST_TBL CORP";
                strSQL += " WHERE";
                strSQL += " JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
                strSQL += " AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
                strSQL += " AND JOBOTH.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
                //strSQL &= vbCrLf & " AND JOBOTH.INV_AGENT_TRN_FK IS NULL"
                ///'''''''
                strSQL += " AND JOBOTH.INV_CUST_TRN_FK IS NULL";
                //strSQL &= vbCrLf & " AND (JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL or (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=JOBOTH.CONSOL_INVOICE_TRN_FK)=2) "
                strSQL += " AND (JOBOTH.INV_AGENT_TRN_FK IS NULL or (SELECT cit.chk_invoice  FROM INV_AGENT_TBL CIT, INV_AGENT_TRN_TBL CTRN ";
                strSQL += "  WHERE CIT.INV_AGENT_PK = CTRN.INV_AGENT_FK   AND CTRN.INV_AGENT_TRN_PK = JOBOTH.INV_AGENT_TRN_FK) = 2) ";
                if (AgentType == 2)
                {
                    strSQL += "AND JOBOTH.Freight_Type=2";
                }
                else if (AgentType == 4)
                {
                    strSQL += " AND JOBOTH.Freight_Type IN (3)";
                }

                ///'''''''
            }
            else
            {
                strSQL = " SELECT DECODE(TRN.COST_FRT_ELEMENT,1,'FREIGHT',2,'FREIGHT',3,'OTHER') AS TYPE,";
                strSQL += " TRN.INV_AGENT_TRN_PK AS PK,";
                strSQL += " HDR.JOB_CARD_FK AS JOBCARD_FK,";
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
                //'
                strSQL += " to_number('' || TRN.TAX_PCNT || '') AS VAT_PERCENT,";
                strSQL += " to_number('' || TRN.TAX_AMT || '')  AS TAX_AMOUNT,";
                strSQL += " TRN.TOT_AMT AS TOTAL_AMOUNT,";
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
        /// <summary>
        /// Fetches the jc details.
        /// </summary>
        /// <param name="nJobCardPK">The n job card pk.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <returns></returns>
        public DataSet FetchJCDetails(double nJobCardPK, short AgentType = 0)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;

            strSQL = "SELECT ";
            strSQL += "JOB.JOBCARD_REF_NO,";
            strSQL += "TO_CHAR(JOB.JOBCARD_DATE,'" + dateFormat + "') AS JOBCARD_DATE, ";
            strSQL += "BKG.BOOKING_REF_NO,";
            strSQL += "HBL.HBL_REF_NO,";
            strSQL += "CMT.AGENT_MST_PK,";
            strSQL += "CMT.AGENT_NAME,";
            strSQL += "JOB.VESSEL_NAME,";
            strSQL += "JOB.VOYAGE_FLIGHT_NO,";
            strSQL += "POL.PORT_MST_PK POLPK,";
            strSQL += "POL.PORT_NAME POL,";
            strSQL += "POD.PORT_MST_PK PODPK,";
            strSQL += "POD.PORT_NAME POD";
            strSQL += "FROM";
            strSQL += "JOB_CARD_TRN JOB,";
            strSQL += "BOOKING_MST_TBL BKG,";
            strSQL += "HBL_EXP_TBL HBL,";
            strSQL += "AGENT_MST_TBL CMT,";
            strSQL += "PORT_MST_TBL POL,";
            strSQL += "PORT_MST_TBL POD";
            strSQL += "WHERE";
            strSQL += "JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK(+) ";
            strSQL += "AND JOB.JOB_CARD_TRN_PK=HBL.JOB_CARD_SEA_EXP_FK(+)";
            //Check agent type
            if (AgentType == 1)
            {
                strSQL += "AND JOB.CB_AGENT_MST_FK=CMT.AGENT_MST_PK(+)";
                //Forign Agent
            }
            else if (AgentType == 4)
            {
                strSQL += "AND JOB.TARIFF_AGENT_MST_FK=CMT.AGENT_MST_PK(+)";
            }
            else
            {
                strSQL += "AND JOB.DP_AGENT_MST_FK=CMT.AGENT_MST_PK(+)";
            }
            strSQL += "AND JOB.PORT_MST_POL_FK=POL.PORT_MST_PK";
            strSQL += "AND JOB.PORT_MST_POD_FK=POD.PORT_MST_PK";
            strSQL += "AND JOB.JOB_CARD_TRN_PK=" + Convert.ToString(nJobCardPK);

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
        /// <summary>
        /// Fetches the HDR.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
        public DataSet FetchHDR(long nInvPK)
        {
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;

            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_PK,");
            sb.Append("       INV.JOB_CARD_FK,");
            sb.Append("       INV.AGENT_MST_FK,");
            sb.Append("       INV.INVOICE_REF_NO,");
            sb.Append("       TO_DATE(INV.INVOICE_DATE,DATEFORMAT) INVOICE_DATE,");
            sb.Append("       TO_DATE(INV.INVOICE_DUE_DATE,DATEFORMAT) INVOICE_DUE_DATE,");
            sb.Append("       INV.CURRENCY_MST_FK,");
            sb.Append("       INV.GROSS_INV_AMT INVOICE_AMT,");
            sb.Append("       INV.VAT_PCNT,");
            sb.Append("       INV.VAT_AMT,");
            sb.Append("       INV.DISCOUNT_AMT,");
            sb.Append("       INV.NET_INV_AMT NET_PAYABLE,");
            sb.Append("       INV.CREATED_BY_FK,");
            sb.Append("       INV.CREATED_DT,");
            sb.Append("       INV.LAST_MODIFIED_BY_FK,");
            sb.Append("       INV.BATCH_MST_FK,");
            //sb.Append("      INV.INVOICE_DUE_DATE,")
            sb.Append("       INV.CHK_INVOICE,");
            sb.Append("       INV.REMARKS,");
            sb.Append("       INV.VERSION_NO, ");
            sb.Append("       INV.CB_DP_LOAD_AGENT,");
            sb.Append("       INV.BUSINESS_TYPE,");
            sb.Append("       INV.PROCESS_TYPE,");
            sb.Append("       INV.INV_UNIQUE_REF_NR, ");
            sb.Append("       INV.PROFIT_SHARING_FLAG, ");
            sb.Append("       INV.PROFIT_SHARING_AMT, ");
            sb.Append("   UMTCRT.USER_NAME    AS CREATED_BY, ");
            sb.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
            sb.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
            sb.Append("   TO_DATE(INV.CREATED_DT) CREATED_BY_DT, ");
            sb.Append("   TO_DATE(INV.LAST_MODIFIED_DT) LAST_MODIFIEDBY_DT, ");
            sb.Append("   TO_DATE(INV.LAST_MODIFIED_DT) APPROVED_DT ");
            sb.Append("  FROM INV_AGENT_TBL INV, ");
            sb.Append("  USER_MST_TBL UMTCRT, ");
            sb.Append("  USER_MST_TBL UMTUPD, ");
            sb.Append("  USER_MST_TBL UMTAPP ");
            sb.Append(" WHERE INV.INV_AGENT_PK =" + nInvPK);
            sb.Append(" AND UMTCRT.USER_MST_PK(+) = INV.CREATED_BY_FK ");
            sb.Append(" AND UMTUPD.USER_MST_PK(+) = INV.LAST_MODIFIED_BY_FK  ");
            sb.Append(" AND UMTAPP.USER_MST_PK(+) = INV.LAST_MODIFIED_BY_FK  ");
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

        //Fetch profit sharing amount by jobcard
        /// <summary>
        /// Fetches the profit sharing amt.
        /// </summary>
        /// <param name="JOBCARD_PK">The jobcar d_ pk.</param>
        /// <param name="FREIGHT_MST_FK">The freigh t_ ms t_ fk.</param>
        /// <param name="BASE_CURRENCY_FK">The bas e_ currenc y_ fk.</param>
        /// <returns></returns>
        public double FetchProfitSharingAmt(long JOBCARD_PK, int FREIGHT_MST_FK = 0, int BASE_CURRENCY_FK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            double PS_AMT = 0;
            int _baseCurrency = BASE_CURRENCY_FK;
            if (BASE_CURRENCY_FK == 0)
            {
                _baseCurrency = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            }
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT ROUND(FN_GET_AGENT_PROF_COMM(" + JOBCARD_PK + "," + FREIGHT_MST_FK + "," + _baseCurrency + "),2) FROM DUAL ");
            try
            {
                PS_AMT = Convert.ToDouble(objWK.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return PS_AMT;
        }

        //This function is called to fetch Max Credit Note Amount for particular Invoice
        /// <summary>
        /// Fetches the credit amt.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
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

        /// <summary>
        /// Fetches the vat PCNT.
        /// </summary>
        /// <returns></returns>
        public double FetchVatPcnt()
        {
            string Strsql = null;
            double VatPcnt = 0;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = " select ";
                Strsql += " NVL(CORP.VAT_PERCENTAGE,0) AS VAT_PCNT";
                Strsql += " from CORPORATE_MST_TBL CORP";
                VatPcnt = Convert.ToDouble(ObjWF.ExecuteScaler(Strsql));
                return VatPcnt;
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
        /// <summary>
        /// Fetches the TRN.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
        public DataSet FetchTRN(long nInvPK)
        {
            //Dim strSQL As String
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;

            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INV.INV_AGENT_TRN_PK INV_AGENT_TRN_PK,");
            sb.Append("       INV.INV_AGENT_FK INV_AGENT_FK,");
            sb.Append("       INV.COST_FRT_ELEMENT,");
            sb.Append("       INV.COST_FRT_ELEMENT_FK,");
            sb.Append("       INV.CURRENCY_MST_FK,");
            sb.Append("       INV.EXCHANGE_RATE,");
            sb.Append("       INV.ELEMENT_AMT,");
            sb.Append("       INV.TAX_PCNT,");
            sb.Append("       INV.TAX_AMT,");
            sb.Append("       INV.TOT_AMT,");
            sb.Append("       INV.AMT_IN_INV_CURR          TOT_AMT_IN_LOC_CURR,");
            sb.Append("       INV.REMARKS,");
            sb.Append("       INV.VAT_CODE,");
            sb.Append("       INV.SURCHARGE");
            sb.Append("  FROM INV_AGENT_TRN_TBL INV");
            sb.Append(" WHERE INV.INV_AGENT_FK =" + nInvPK);

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

        /// <summary>
        /// Gets the corp currency.
        /// </summary>
        /// <returns></returns>
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

        #endregion "Fetch"

        #region "TO Update cbagentstatus"

        /// <summary>
        /// Updates the cbagentstatus.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <returns></returns>
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

                str = "UPDATE job_card_trn  j SET ";
                str += "   j.CBAGENT_STATUS = 0 ";
                str += " WHERE j.job_card_trn_pk=" + JobPk;

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

        #endregion "TO Update cbagentstatus"

        #region "TO Update DpAgentStatus"

        /// <summary>
        /// Updates the d pagentstatus.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <returns></returns>
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

                str = "UPDATE job_card_trn  j SET ";
                str += "   j.dpagent_status = 0 ";
                str += " WHERE j.job_card_trn_pk=" + JobPk;

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

        #endregion "TO Update DpAgentStatus"

        #region "Save"

        //This function is called to save the invoice header details
        /// <summary>
        /// Saves the specified HDR ds.
        /// </summary>
        /// <param name="hdrDS">The HDR ds.</param>
        /// <param name="trnDS">The TRN ds.</param>
        /// <param name="nConfigPK">The n configuration pk.</param>
        /// <param name="invPk">The inv pk.</param>
        /// <param name="InvoiceNo">The invoice no.</param>
        /// <param name="nLocationId">The n location identifier.</param>
        /// <param name="nEmpId">The n emp identifier.</param>
        /// <param name="lngLocPk">The LNG loc pk.</param>
        /// <param name="invDate">The inv date.</param>
        /// <param name="invdDate">The invd date.</param>
        /// <param name="CheckApp">The check application.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="polid">The polid.</param>
        /// <param name="podid">The podid.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet hdrDS, DataSet trnDS, long nConfigPK, long invPk, object InvoiceNo, long nLocationId, long nEmpId, long lngLocPk, System.DateTime invDate, System.DateTime invdDate,
        int CheckApp, string AgentType, int BizType, int ProcessType, string polid = "", string podid = "")
        {
            WorkFlow objWK = new WorkFlow();
            cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
            string uniqueRefNr = "";
            if (string.IsNullOrEmpty(Convert.ToString(InvoiceNo)))
            {
                if (string.IsNullOrEmpty(uniqueRefNr))
                {
                    System.DateTime dt = default(System.DateTime);
                    dt = System.DateTime.Now;
                    string st = null;
                    short refExist = 1;
                    while (refExist == 1)
                    {
                        st = Convert.ToString(dt.Day + dt.Month + dt.Year + dt.Hour + dt.Minute + dt.Second + dt.Millisecond);
                        uniqueRefNr = GetVEKInvoiceRef(0, 0, st);
                        refExist = Convert.ToInt16(objWK.ExecuteScaler("SELECT COUNT(*) FROM INV_AGENT_TBL INV WHERE UPPER(INV.INV_UNIQUE_REF_NR)=UPPER('" + uniqueRefNr + "')"));
                    }
                }
            }
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            long nJobCardPK = 0;
            bool chkflag = false;
            string strPk = null;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            string invoice = null;
            bool ChkFirstSave = false;

            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Connection = objWK.MyConnection;
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                if (string.IsNullOrEmpty(Convert.ToString(InvoiceNo)))
                {
                    invoice = GenerateProtocolKey("AGENT INVOICE SEA EXPORT", nLocationId, nEmpId, DateTime.Today, "", "", polid, CREATED_BY, objWK, "",
                    podid);
                    if (invoice == "Protocol Not Defined.")
                    {
                        if ((nEmpId == 0))
                        {
                            arrMessage.Add("This login does not have the permission to create Invoice.");
                            return arrMessage;
                        }
                        else
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                    }
                    ChkFirstSave = true;
                }
                else
                {
                    invoice = Convert.ToString(InvoiceNo);
                }

                objWK.MyCommand.Parameters.Clear();

                nJobCardPK = Convert.ToInt32(hdrDS.Tables[0].Rows[0]["JOB_CARD_FK"]);
                hdrDS.Tables[0].Rows[0]["INVOICE_REF_NO"] = invoice;
                var _with3 = insCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".INV_AGENT_TBL_PKG.INV_AGENT_TBL_INS";

                _with3.Parameters.Add("JOB_CARD_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["JOB_CARD_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("AGENT_MST_FK_IN", OracleDbType.Int32, 10, "AGENT_MST_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
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
                _with3.Parameters.Add("GROSS_INV_AMT_IN", OracleDbType.Int32, 15, "INVOICE_AMT").Direction = ParameterDirection.Input;
                _with3.Parameters["GROSS_INV_AMT_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("VAT_PCNT_IN", OracleDbType.Int32, 10, "VAT_PCNT").Direction = ParameterDirection.Input;
                _with3.Parameters["VAT_PCNT_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("VAT_AMT_IN", OracleDbType.Int32, 10, "VAT_AMT").Direction = ParameterDirection.Input;
                _with3.Parameters["VAT_AMT_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("DISCOUNT_AMT_IN", OracleDbType.Int32, 10, "DISCOUNT_AMT").Direction = ParameterDirection.Input;
                _with3.Parameters["DISCOUNT_AMT_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("NET_INV_AMT_IN", OracleDbType.Int32, 15, "NET_PAYABLE").Direction = ParameterDirection.Input;
                _with3.Parameters["NET_INV_AMT_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "REMARKS").Direction = ParameterDirection.Input;
                _with3.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("CREATED_BY_FK_IN", OracleDbType.Int32, 10, "CREATED_BY_FK").Direction = ParameterDirection.Input;
                _with3.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with3.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("UNIQUE_REF_NO_IN", uniqueRefNr).Direction = ParameterDirection.Input;

                _with3.Parameters.Add("CB_DP_LOAD_AGENT_IN", OracleDbType.Int32, 1, "CB_DP_LOAD_AGENT").Direction = ParameterDirection.Input;
                _with3.Parameters["CB_DP_LOAD_AGENT_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("BUSINESS_TYPE_IN", OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input;
                _with3.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                _with3.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("PROFIT_SHARING_FLAG_IN", OracleDbType.Int32, 1, "PROFIT_SHARING_FLAG").Direction = ParameterDirection.Input;
                _with3.Parameters["PROFIT_SHARING_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("PROFIT_SHARING_AMT_IN", OracleDbType.Double, 21, "PROFIT_SHARING_AMT").Direction = ParameterDirection.Input;
                _with3.Parameters["PROFIT_SHARING_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "INV_AGENT_PK").Direction = ParameterDirection.Output;
                _with3.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with4 = updCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".INV_AGENT_TBL_PKG.INV_AGENT_TBL_UPD";
                _with4.Parameters.Add("INV_AGENT_PK_IN", OracleDbType.Int32, 10, "INV_AGENT_PK").Direction = ParameterDirection.Input;
                _with4.Parameters["INV_AGENT_PK_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("JOB_CARD_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_FK").Direction = ParameterDirection.Input;
                _with4.Parameters["JOB_CARD_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("AGENT_MST_FK_IN", OracleDbType.Int32, 10, "AGENT_MST_FK").Direction = ParameterDirection.Input;
                _with4.Parameters["AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
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
                _with4.Parameters.Add("INVOICE_AMT_IN", OracleDbType.Int32, 15, "INVOICE_AMT").Direction = ParameterDirection.Input;
                _with4.Parameters["INVOICE_AMT_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("VAT_PCNT_IN", OracleDbType.Int32, 6, "VAT_PCNT").Direction = ParameterDirection.Input;
                _with4.Parameters["VAT_PCNT_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("VAT_AMT_IN", OracleDbType.Int32, 10, "VAT_AMT").Direction = ParameterDirection.Input;
                _with4.Parameters["VAT_AMT_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("DISCOUNT_AMT_IN", OracleDbType.Int32, 10, "DISCOUNT_AMT").Direction = ParameterDirection.Input;
                _with4.Parameters["DISCOUNT_AMT_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("NET_PAYABLE_IN", OracleDbType.Int32, 15, "NET_PAYABLE").Direction = ParameterDirection.Input;
                _with4.Parameters["NET_PAYABLE_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "REMARKS").Direction = ParameterDirection.Input;
                _with4.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("LAST_MODIFIED_BY_FK_IN", OracleDbType.Int32, 10, "LAST_MODIFIED_BY_FK").Direction = ParameterDirection.Input;
                _with4.Parameters["LAST_MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input;
                _with4.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;
                _with4.Parameters.Add("CONFIG_PK_IN", nConfigPK).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("CB_DP_LOAD_AGENT_IN", OracleDbType.Int32, 1, "CB_DP_LOAD_AGENT").Direction = ParameterDirection.Input;
                _with4.Parameters["CB_DP_LOAD_AGENT_IN"].SourceVersion = DataRowVersion.Current;

                //.Parameters.Add("BUSINESS_TYPE_IN", OracleClient.OracleDbType.Int32, 1, "BUSINESS_TYPE").Direction = ParameterDirection.Input
                //.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current

                //.Parameters.Add("PROCESS_TYPE_IN", OracleClient.OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input
                //.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current

                _with4.Parameters.Add("PROFIT_SHARING_FLAG_IN", OracleDbType.Int32, 1, "PROFIT_SHARING_FLAG").Direction = ParameterDirection.Input;
                _with4.Parameters["PROFIT_SHARING_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("PROFIT_SHARING_AMT_IN", OracleDbType.Double, 21, "PROFIT_SHARING_AMT").Direction = ParameterDirection.Input;
                _with4.Parameters["PROFIT_SHARING_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                _with4.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                arrMessage.Clear();
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
                    if (invPk > 0)
                    {
                        invPk = Convert.ToInt64(updCommand.Parameters["RETURN_VALUE"].Value);
                        SaveTrn(trnDS, TRAN, invPk, nJobCardPK, Convert.ToInt16(CheckApp), ChkFirstSave);
                    }
                    else
                    {
                        invPk = Convert.ToInt64(insCommand.Parameters["RETURN_VALUE"].Value);
                        SaveTrn(trnDS, TRAN, invPk, nJobCardPK, Convert.ToInt16(CheckApp), ChkFirstSave);
                    }

                    // Amit
                    ///''''''''''''''''''''''''''''
                    if (AgentType == "CB" & BizType == 2 & ProcessType == 1)
                    {
                        objTrackNTrace.SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(invPk), 2, 1, "Invoice to Agent", "INVOICE-CB-AGT-SEA-EXP", Convert.ToInt32(lngLocPk), objWK, "INS", Convert.ToInt32(hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]),
                        "O");
                    }
                    else if (AgentType == "DP" & BizType == 2 & ProcessType == 1)
                    {
                        objTrackNTrace.SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(invPk), 2, 1, "Invoice to Agent", "INVOICE-DP-AGT-SEA-EXP", Convert.ToInt32(lngLocPk), objWK, "INS", Convert.ToInt32(hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]),
                        "O");
                        ///''''
                    }
                    else if (AgentType == "CB" & BizType == 2 & ProcessType == 2)
                    {
                        objTrackNTrace.SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(invPk), 2, 2, "Invoice to Agent", "INVOICE-AGT-SEA-IMP", Convert.ToInt32(lngLocPk), objWK, "INS", Convert.ToInt32(hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]),
                        "O");
                        ///'''''''
                    }
                    else if (AgentType == "LA" & BizType == 2 & ProcessType == 2)
                    {
                        objTrackNTrace.SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(invPk), 2, 2, "Invoice to Agent", "INVOICE-AGT-SEA-IMP", Convert.ToInt32(lngLocPk), objWK, "INS", Convert.ToInt32(hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]),
                        "O");
                    }
                    else if (AgentType == "CB" & BizType == 1 & ProcessType == 1)
                    {
                        objTrackNTrace.SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(invPk), 1, 1, "Invoice to Agent", "INVOICE-CB-AGT-AIR-EXP", Convert.ToInt32(lngLocPk), objWK, "INS", Convert.ToInt32(hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]),
                        "O");
                    }
                    else if (AgentType == "DP" & BizType == 1 & ProcessType == 1)
                    {
                        objTrackNTrace.SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(invPk), 1, 1, "Invoice to Agent", "INVOICE-DP-AGT-AIR-EXP", Convert.ToInt32(lngLocPk), objWK, "INS", Convert.ToInt32(hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]),
                        "O");
                        ///''
                    }
                    else if (AgentType == "CB" & BizType == 1 & ProcessType == 2)
                    {
                        objTrackNTrace.SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(invPk), 1, 2, "Invoice to Agent", "INVOICE-AGT-AIR-IMP", Convert.ToInt32(lngLocPk), objWK, "INS", Convert.ToInt32(hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]),
                        "O");
                        ///''
                    }
                    else if (AgentType == "LA" & BizType == 1 & ProcessType == 2)
                    {
                        objTrackNTrace.SaveTrackAndTraceForInv(TRAN, Convert.ToInt32(invPk), 1, 2, "Invoice to Agent", "INVOICE-AGT-AIR-IMP", Convert.ToInt32(lngLocPk), objWK, "INS", Convert.ToInt32(hdrDS.Tables[0].Rows[0]["CREATED_BY_FK"]),
                        "O");
                    }
                    ///''''''''''''''''''''''''''''
                    //If AgentType = "CB" Then
                    //    objTrackNTrace.SaveTrackAndTraceForInv(TRAN, invPk, 2, 1, _
                    //                                                               "Invoice to Agent", "INVOICE-CB-AGT-SEA-EXP", _
                    //                                                               lngLocPk, objWK, "INS", _
                    //                                                               hdrDS.Tables(0).Rows(0)("CREATED_BY_FK"), "O")
                    //Else
                    //    objTrackNTrace.SaveTrackAndTraceForInv(TRAN, invPk, 2, 1, _
                    //                                           "Invoice to Agent", "INVOICE-DP-AGT-SEA-EXP", _
                    //                                           lngLocPk, objWK, "INS", _
                    //                                           hdrDS.Tables(0).Rows(0)("CREATED_BY_FK"), "O")
                    //End If
                    // End
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    //Added by minakshi on 18-feb-09 for protocal rollbacking
                    if (chkflag)
                    {
                        RollbackProtocolKey("AGENT INVOICE SEA EXPORT", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), invoice, System.DateTime.Now);
                    }
                    //Ended by minakshi
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    arrMessage.Add(invPk);
                    InvoiceNo = invoice;
                    //Push to financial system if realtime is selected
                    if (CheckApp == 1)
                    {
                        if (invPk > 0)
                        {
                            if (Convert.ToBoolean(ConfigurationManager.AppSettings["QFINGeneral"]) == true)
                            {
                                try
                                {
                                    TRAN = objWK.MyConnection.BeginTransaction();
                                    objWK.MyCommand.Transaction = TRAN;
                                    objWK.MyCommand.Parameters.Clear();
                                    objWK.MyCommand.CommandText = objWK.MyUserName + ".ACCOUNTING_INTEGREATION_PKG.INVOICE_AGENT_APPROVE_CANCEL";
                                    objWK.MyCommand.Parameters.Add("INVOICE_TRN_FK_IN", invPk).Direction = ParameterDirection.Input;
                                    objWK.MyCommand.Parameters.Add("LOCAL_CUR_FK", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                                    objWK.MyCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                    objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                                    objWK.MyCommand.ExecuteNonQuery();
                                    TRAN.Commit();
                                }
                                catch (Exception ex)
                                {
                                }
                            }

                            Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
                            ArrayList schDtls = null;
                            bool errGen = false;
                            if (objSch.GetSchedulerPushType() == true)
                            {
                                //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                                //try {
                                //	schDtls = objSch.FetchSchDtls();
                                //	//'Used to Fetch the Sch Dtls
                                //	objPush.UpdateTaxElement(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                                //	objPush.UpdatebankAcc(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                                //	objPush.UpdateCurrency(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                                //	objPush.UpdateTransaction(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, invPk);
                                //	if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
                                //		objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                                //	}
                                //} catch (Exception ex) {
                                //	if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
                                //		objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                                //	}
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
                            RollbackProtocolKey("AGENT INVOICE SEA EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), invoice, System.DateTime.Now);
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
                            RollbackProtocolKey("AGENT INVOICE SEA EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), invoice, System.DateTime.Now);
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
        /// <summary>
        /// Saves the TRN.
        /// </summary>
        /// <param name="trnDS">The TRN ds.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <param name="nJobCardPK">The n job card pk.</param>
        /// <param name="invStatus">The inv status.</param>
        /// <param name="ChkFirstSave">if set to <c>true</c> [CHK first save].</param>
        private void SaveTrn(DataSet trnDS, OracleTransaction TRAN, long nInvPK, long nJobCardPK, Int16 invStatus, bool ChkFirstSave = true)
        {
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            int i = 0;
            try
            {
                objWK.MyConnection = TRAN.Connection;
                if (ChkFirstSave)
                {
                    var _with6 = insCommand;
                    _with6.Connection = objWK.MyConnection;
                    _with6.CommandType = CommandType.StoredProcedure;
                    _with6.CommandText = objWK.MyUserName + ".INV_AGENT_TBL_PKG.INV_AGENT_TRN_TBL_INS";
                    _with6.Parameters.Add("INV_AGENT_FK_IN", nInvPK).Direction = ParameterDirection.Input;
                    _with6.Parameters["INV_AGENT_FK_IN"].SourceVersion = DataRowVersion.Current;
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
                    _with6.Parameters.Add("AMT_IN_INV_CURR_IN", OracleDbType.Int32, 15, "TOT_AMT_IN_LOC_CURR").Direction = ParameterDirection.Input;
                    _with6.Parameters["AMT_IN_INV_CURR_IN"].SourceVersion = DataRowVersion.Current;
                    _with6.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "REMARKS").Direction = ParameterDirection.Input;
                    _with6.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;
                    //Added by Venkata on 11/10/07
                    _with6.Parameters.Add("VAT_CODE_IN", OracleDbType.Varchar2, 20, getDefault("VAT_CODE", "").ToString()).Direction = ParameterDirection.Input;
                    _with6.Parameters["VAT_CODE_IN"].SourceVersion = DataRowVersion.Current;
                    //End
                    _with6.Parameters.Add("JOB_CARD_FK_IN", nJobCardPK).Direction = ParameterDirection.Input;
                    _with6.Parameters.Add("INV_STATUS_IN", invStatus).Direction = ParameterDirection.Input;
                    _with6.Parameters["INV_STATUS_IN"].SourceVersion = DataRowVersion.Current;
                    _with6.Parameters.Add("LOGED_IN_LOC_FK_IN", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                    _with6.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "INV_AGENT_TRN_PK").Direction = ParameterDirection.Output;
                    _with6.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    var _with7 = objWK.MyDataAdapter;
                    _with7.InsertCommand = insCommand;
                    _with7.InsertCommand.Transaction = TRAN;
                    RecAfct = _with7.Update(trnDS.Tables[0]);
                }
                else
                {
                    try
                    {
                        objWK.MyCommand.Connection = objWK.MyConnection;
                        objWK.MyCommand.Transaction = TRAN;
                        objWK.MyCommand.Parameters.Clear();
                        objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                        objWK.MyCommand.CommandText = objWK.MyUserName + ".INV_AGENT_TBL_PKG.INV_AGENT_TRN_TBL_DEL";
                        objWK.MyCommand.Parameters.Add("INV_AGENT_FK_IN", nInvPK);
                        objWK.MyCommand.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                    }
                    var _with8 = insCommand;
                    _with8.Connection = objWK.MyConnection;
                    _with8.CommandType = CommandType.StoredProcedure;
                    _with8.CommandText = objWK.MyUserName + ".INV_AGENT_TBL_PKG.INV_AGENT_TRN_TBL_INS";
                    _with8.Parameters.Add("INV_AGENT_FK_IN", nInvPK).Direction = ParameterDirection.Input;
                    _with8.Parameters["INV_AGENT_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with8.Parameters.Add("COST_FRT_ELEMENT_IN", OracleDbType.Int32, 1, "COST_FRT_ELEMENT").Direction = ParameterDirection.Input;
                    _with8.Parameters["COST_FRT_ELEMENT_IN"].SourceVersion = DataRowVersion.Current;
                    _with8.Parameters.Add("COST_FRT_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_FRT_ELEMENT_FK").Direction = ParameterDirection.Input;
                    _with8.Parameters["COST_FRT_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with8.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    _with8.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with8.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "EXCHANGE_RATE").Direction = ParameterDirection.Input;
                    _with8.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;
                    _with8.Parameters.Add("ELEMENT_AMT_IN", OracleDbType.Int32, 15, "ELEMENT_AMT").Direction = ParameterDirection.Input;
                    _with8.Parameters["ELEMENT_AMT_IN"].SourceVersion = DataRowVersion.Current;
                    _with8.Parameters.Add("TAX_PCNT_IN", OracleDbType.Int32, 6, "TAX_PCNT").Direction = ParameterDirection.Input;
                    _with8.Parameters["TAX_PCNT_IN"].SourceVersion = DataRowVersion.Current;
                    _with8.Parameters.Add("TAX_AMT_IN", OracleDbType.Int32, 10, "TAX_AMT").Direction = ParameterDirection.Input;
                    _with8.Parameters["TAX_AMT_IN"].SourceVersion = DataRowVersion.Current;
                    _with8.Parameters.Add("TOT_AMT_IN", OracleDbType.Int32, 15, "TOT_AMT").Direction = ParameterDirection.Input;
                    _with8.Parameters["TOT_AMT_IN"].SourceVersion = DataRowVersion.Current;
                    _with8.Parameters.Add("AMT_IN_INV_CURR_IN", OracleDbType.Int32, 15, "TOT_AMT_IN_LOC_CURR").Direction = ParameterDirection.Input;
                    _with8.Parameters["AMT_IN_INV_CURR_IN"].SourceVersion = DataRowVersion.Current;
                    _with8.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "REMARKS").Direction = ParameterDirection.Input;
                    _with8.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;
                    //Added by Venkata on 11/10/07
                    _with8.Parameters.Add("VAT_CODE_IN", OracleDbType.Varchar2, 20, getDefault("VAT_CODE", "").ToString()).Direction = ParameterDirection.Input;
                    _with8.Parameters["VAT_CODE_IN"].SourceVersion = DataRowVersion.Current;
                    //End
                    _with8.Parameters.Add("JOB_CARD_FK_IN", nJobCardPK).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("INV_STATUS_IN", invStatus).Direction = ParameterDirection.Input;
                    _with8.Parameters["INV_STATUS_IN"].SourceVersion = DataRowVersion.Current;
                    _with8.Parameters.Add("LOGED_IN_LOC_FK_IN", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                    _with8.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "INV_AGENT_TRN_PK").Direction = ParameterDirection.Output;
                    _with8.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with9 = objWK.MyDataAdapter;
                    _with9.InsertCommand = insCommand;
                    _with9.InsertCommand.Transaction = TRAN;
                    RecAfct = _with9.Update(trnDS.Tables[0]);
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
        /// <summary>
        /// Generates the key.
        /// </summary>
        /// <param name="strName">Name of the string.</param>
        /// <param name="nLocPK">The n loc pk.</param>
        /// <param name="nEmpPK">The n emp pk.</param>
        /// <param name="dtDate">The dt date.</param>
        /// <param name="nUserID">The n user identifier.</param>
        /// <returns></returns>
        public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
        {
            return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
        }

        #endregion "Save"

        #region "Invoice Agent Report"

        /// <summary>
        /// Fetches the inv agent report.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <param name="AgentFlag">The agent flag.</param>
        /// <returns></returns>
        public DataSet FetchInvAgentReport(Int64 nInvPK, short AgentFlag = 1)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            // AgentFlag = 1 = CB Agent
            // AgentFlag = 2 = DP Agent
            strSQL.Append(" SELECT ");
            strSQL.Append("     BAT.customer_ref_no     CUST_REF_NO ,");
            strSQL.Append("     INVAGTEXP.INV_AGENT_PK INVPK, ");
            strSQL.Append("     INVAGTEXP.INVOICE_REF_NO       INVREFNO,");
            strSQL.Append("     INVAGTEXP.GROSS_INV_AMT        INVAMT,");
            strSQL.Append("     NVL(INVAGTEXP.DISCOUNT_AMT,0)  DICSOUNT,");
            //strSQL.Append("     INVAGTEXP.VAT_PCNT             VATPCT,")
            strSQL.Append("      INVTAGTEXP.VAT_CODE            VATPCT,");
            strSQL.Append("     INVAGTEXP.VAT_AMT              VATAMT,");
            strSQL.Append("     JSE.job_card_trn_pk        JOBPK,");
            strSQL.Append("     JSE.JOBCARD_REF_NO             JOBREFNO,");
            strSQL.Append("     '' CLEARANCEPOINT,");
            strSQL.Append("     AMST.AGENT_NAME         AGENTNAME,");
            strSQL.Append("     AMST.ACCOUNT_NO        AGENTREFNO,");
            strSQL.Append("     ADTLS.ADM_ADDRESS_1     AGENTADD1,");
            strSQL.Append("     ADTLS.ADM_ADDRESS_2     AGENTADD2,");
            strSQL.Append("     ADTLS.ADM_ADDRESS_3     AGENTADD3,");
            strSQL.Append("     ADTLS.ADM_CITY          AGENTCITY,");
            strSQL.Append("     ADTLS.ADM_ZIP_CODE      AGENTZIP,");
            strSQL.Append("     ADTLS.ADM_PHONE_NO_1    AGENTPHONE,");
            strSQL.Append("     ADTLS.ADM_FAX_NO        AGENTFAX,");
            strSQL.Append("     ADTLS.ADM_EMAIL_ID      AGENTEMAIL,");
            strSQL.Append("     AGTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,");

            strSQL.Append("     SHIPMST.CUSTOMER_NAME    SHIPPER,");
            strSQL.Append("     SHIPDTLS.ADM_ADDRESS_1   SHIPPERADD1,");
            strSQL.Append("     SHIPDTLS.ADM_ADDRESS_2   SHIPPERADD2,");
            strSQL.Append("     SHIPDTLS.ADM_ADDRESS_3   SHIPPERADD3,");
            strSQL.Append("     SHIPDTLS.ADM_CITY        SHIPPERCITY,");
            strSQL.Append("     SHIPDTLS.ADM_ZIP_CODE    SHIPPERZIP,");
            strSQL.Append("     SHIPDTLS.ADM_PHONE_NO_1  SHIPPERPHONE,");
            strSQL.Append("     SHIPDTLS.ADM_FAX_NO      SHIPPERFAX,");
            strSQL.Append("     SHIPDTLS.ADM_EMAIL_ID    SHIPPEREMAIL,");
            strSQL.Append("     SHIPCOUNTRY.COUNTRY_NAME SHIPPERCOUNTRY,");

            //'changed for the surcharge
            strSQL.Append("     FEMST.FREIGHT_ELEMENT_NAME FREIGHTNAME,");
            //strSQL.Append("    CASE WHEN TRNFD.SURCHARGE IS NULL THEN")
            //strSQL.Append("    FEMST.FREIGHT_ELEMENT_NAME")
            //strSQL.Append("    ELSE FEMST.FREIGHT_ELEMENT_NAME || '( ' || TRNFD.SURCHARGE || '  ) ' || ''")
            //strSQL.Append("    END FREIGHTNAME,")

            strSQL.Append("     NVL(INVTAGTEXP.AMT_IN_INV_CURR,0) FREIGHTAMT,");
            strSQL.Append("     INVTAGTEXP.TAX_PCNT        FRETAXPCNT,");
            strSQL.Append("     NVL(INVTAGTEXP.TAX_AMT,0) FRETAXANT,");
            strSQL.Append("     JSE.ETD_DATE ETD,");
            strSQL.Append("     JSE.ETA_DATE ETA ,");
            strSQL.Append("     BAT.CARGO_TYPE CARGO_TYPE,");
            strSQL.Append("     CURRMST.CURRENCY_ID CURRID,");
            strSQL.Append("     CURRMST.CURRENCY_NAME CURRNAME,");
            strSQL.Append("     (CASE WHEN JSE.VOYAGE IS NOT NULL THEN");
            strSQL.Append("     JSE.VESSEL_NAME || '-' || JSE.VOYAGE ELSE");
            strSQL.Append("     JSE.VOYAGE END) VES_FLIGHT,");
            strSQL.Append("     JSE.PYMT_TYPE PYMT,");
            strSQL.Append("     JSE.GOODS_DESCRIPTION GOODS,");
            strSQL.Append("     JSE.MARKS_NUMBERS MARKS,");
            strSQL.Append("     NVL(JSE.INSURANCE_AMT, 0) INSURANCE,");
            strSQL.Append("     STMST.INCO_CODE TERMS,");
            strSQL.Append("     AMST.VAT_NO AGTVATNO,");
            strSQL.Append("     COLMST.PLACE_NAME COLPLACE,");
            strSQL.Append("     DELMST.PLACE_NAME DELPLACE,");
            strSQL.Append("     POLMST.PORT_NAME POL,");
            strSQL.Append("     PODMST.PORT_NAME POD,");
            strSQL.Append("     HBL.HBL_REF_NO HAWBREFNO,");
            strSQL.Append("     MBL.MBL_REF_NO MAWBREFNO,");
            strSQL.Append("     CGMST.COMMODITY_GROUP_DESC COMMODITY,");
            strSQL.Append("     SUM(JSEC.VOLUME_IN_CBM) VOLUME,");
            strSQL.Append("     SUM(JSEC.GROSS_WEIGHT) GROSS,");
            strSQL.Append("     SUM(JSEC.net_weight) NETWT,");
            strSQL.Append("     SUM(JSEC.CHARGEABLE_WEIGHT) CHARWT");

            strSQL.Append("     FROM  ");

            strSQL.Append("    INV_AGENT_TBL INVAGTEXP,");
            strSQL.Append("    CURRENCY_TYPE_MST_TBL CURRMST,");
            strSQL.Append("    INV_AGENT_TRN_TBL INVTAGTEXP,");
            strSQL.Append("    FREIGHT_ELEMENT_MST_TBL   FEMST,");
            strSQL.Append("    job_card_trn    JSE,");
            strSQL.Append("    JOB_TRN_CONT    JSEC,");
            strSQL.Append("    SHIPPING_TERMS_MST_TBL  STMST,");
            strSQL.Append("    BOOKING_MST_TBL         BAT,");
            strSQL.Append("    PLACE_MST_TBL           COLMST,");
            strSQL.Append("    PLACE_MST_TBL           DELMST,");
            strSQL.Append("    PORT_MST_TBL            POLMST,");
            strSQL.Append("    PORT_MST_TBL            PODMST,");
            strSQL.Append("    HBL_EXP_TBL            HBL,");
            strSQL.Append("    MBL_EXP_TBL            MBL,");
            strSQL.Append("    COMMODITY_GROUP_MST_TBL CGMST,");
            strSQL.Append("    AGENT_MST_TBL      AMST,");
            strSQL.Append("    AGENT_CONTACT_DTLS ADTLS,");
            strSQL.Append("    COUNTRY_MST_TBL    AGTCOUNTRY,");
            strSQL.Append("    CUSTOMER_MST_TBL      SHIPMST,");
            strSQL.Append("    CUSTOMER_CONTACT_DTLS SHIPDTLS,");
            strSQL.Append("    COUNTRY_MST_TBL SHIPCOUNTRY");
            //'added for surcharge
            //strSQL.Append("     JOB_TRN_SEA_EXP_FD        TRNFD,")
            //strSQL.Append("     JOB_TRN_OTH_CHRG  TRNOTH")

            strSQL.Append(" WHERE ");

            strSQL.Append(" INVAGTEXP.JOB_CARD_FK = JSE.job_card_trn_pk ");
            strSQL.Append(" AND CURRMST.CURRENCY_MST_PK(+) = INVAGTEXP.CURRENCY_MST_FK");
            strSQL.Append(" AND INVTAGTEXP.INV_AGENT_FK(+) = INVAGTEXP.INV_AGENT_PK");
            strSQL.Append(" AND FEMST.FREIGHT_ELEMENT_MST_PK(+) = INVTAGTEXP.COST_FRT_ELEMENT_FK");
            strSQL.Append(" AND JSE.job_card_trn_pk = JSEC.job_card_trn_fk(+)");
            strSQL.Append(" AND STMST.SHIPPING_TERMS_MST_PK(+) = JSE.SHIPPING_TERMS_MST_FK");
            strSQL.Append(" AND BAT.BOOKING_MST_PK(+) = JSE.BOOKING_MST_FK");
            strSQL.Append(" AND COLMST.PLACE_PK(+) = BAT.COL_PLACE_MST_FK");
            strSQL.Append(" AND DELMST.PLACE_PK(+) = BAT.DEL_PLACE_MST_FK");
            strSQL.Append(" AND POLMST.PORT_MST_PK = BAT.PORT_MST_POL_FK");
            strSQL.Append(" AND PODMST.PORT_MST_PK = BAT.PORT_MST_POD_FK");
            strSQL.Append(" AND HBL.HBL_EXP_TBL_PK(+) = JSE.HBL_EXP_TBL_FK");
            strSQL.Append(" AND MBL.MBL_EXP_TBL_PK(+) = JSE.MBL_EXP_TBL_FK");
            strSQL.Append(" AND CGMST.COMMODITY_GROUP_PK(+) = JSE.COMMODITY_GROUP_FK");

            strSQL.Append(" AND AMST.AGENT_MST_PK = INVAGTEXP.AGENT_MST_FK");
            strSQL.Append(" AND ADTLS.AGENT_MST_FK(+) = AMST.AGENT_MST_PK");
            strSQL.Append(" AND AGTCOUNTRY.COUNTRY_MST_PK(+) = ADTLS.ADM_COUNTRY_MST_FK");
            //'added for surcharge
            //strSQL.Append("  AND TRNFD.job_card_trn_fk = JSE.job_card_trn_pk")
            //strSQL.Append("  AND FEMST.FREIGHT_ELEMENT_MST_PK=TRNFD.Freight_Element_Mst_Fk")
            //strSQL.Append(" AND (TRNFD.job_card_trn_fk = JSE.job_card_trn_pk OR TRNOTH.job_card_trn_fk=JSE.job_card_trn_pk) ")
            //strSQL.Append(" AND (FEMST.FREIGHT_ELEMENT_MST_PK = TRNFD.Freight_Element_Mst_Fk OR FEMST.FREIGHT_ELEMENT_MST_PK=TRNOTH.FREIGHT_ELEMENT_MST_FK) ")

            if ((AgentFlag == 1))
            {
                strSQL.Append(" AND SHIPMST.CUSTOMER_MST_PK(+) = JSE.Shipper_Cust_Mst_Fk");
            }
            else
            {
                strSQL.Append(" AND SHIPMST.CUSTOMER_MST_PK(+) = JSE.consignee_cust_mst_fk");
            }

            strSQL.Append(" AND SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK");
            strSQL.Append(" AND SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)");
            strSQL.Append("  AND INVAGTEXP.INV_AGENT_PK=" + nInvPK + "");

            strSQL.Append(" GROUP BY BAT.customer_ref_no,INVAGTEXP.INV_AGENT_PK,");
            strSQL.Append(" INVAGTEXP.INVOICE_REF_NO,");
            strSQL.Append(" INVAGTEXP.GROSS_INV_AMT,");
            strSQL.Append(" INVAGTEXP.DISCOUNT_AMT,");
            strSQL.Append(" INVAGTEXP.VAT_PCNT,");
            strSQL.Append(" INVAGTEXP.VAT_AMT,");
            strSQL.Append(" JSE.job_card_trn_pk,");
            strSQL.Append(" JSE.JOBCARD_REF_NO,");
            strSQL.Append(" AMST.AGENT_NAME,");
            strSQL.Append(" AMST.ACCOUNT_NO ,");
            strSQL.Append(" ADTLS.ADM_ADDRESS_1,");
            strSQL.Append(" ADTLS.ADM_ADDRESS_2,");
            strSQL.Append(" ADTLS.ADM_ADDRESS_3,");
            strSQL.Append(" ADTLS.ADM_CITY,");
            strSQL.Append(" ADTLS.ADM_ZIP_CODE,");
            strSQL.Append(" ADTLS.ADM_PHONE_NO_1,");
            strSQL.Append(" ADTLS.ADM_FAX_NO,");
            strSQL.Append(" ADTLS.ADM_EMAIL_ID,");
            strSQL.Append(" AGTCOUNTRY.COUNTRY_NAME,");
            strSQL.Append(" SHIPMST.CUSTOMER_NAME,");
            strSQL.Append(" SHIPDTLS.ADM_ADDRESS_1,");
            strSQL.Append(" SHIPDTLS.ADM_ADDRESS_2,");
            strSQL.Append(" SHIPDTLS.ADM_ADDRESS_3,");
            strSQL.Append("  SHIPDTLS.ADM_CITY,");
            strSQL.Append("  SHIPDTLS.ADM_ZIP_CODE,");
            strSQL.Append(" SHIPDTLS.ADM_PHONE_NO_1,");
            strSQL.Append(" SHIPDTLS.ADM_FAX_NO,");
            strSQL.Append(" SHIPDTLS.ADM_EMAIL_ID,");
            strSQL.Append(" SHIPCOUNTRY.COUNTRY_NAME,");
            strSQL.Append(" FEMST.FREIGHT_ELEMENT_NAME,");
            strSQL.Append(" INVTAGTEXP.AMT_IN_INV_CURR,");
            strSQL.Append(" INVTAGTEXP.TAX_PCNT,");
            strSQL.Append(" INVTAGTEXP.TAX_AMT,");
            strSQL.Append("  JSE.ETD_DATE ,");
            strSQL.Append(" JSE.ETA_DATE,");
            strSQL.Append("  BAT.CARGO_TYPE,");
            strSQL.Append(" CURRMST.CURRENCY_ID,");
            strSQL.Append(" CURRMST.CURRENCY_NAME,");
            strSQL.Append(" (CASE WHEN JSE.VOYAGE IS NOT NULL THEN");
            strSQL.Append(" JSE.VESSEL_NAME || '-' || JSE.VOYAGE ELSE");
            strSQL.Append(" JSE.VOYAGE END),");
            strSQL.Append("  JSE.PYMT_TYPE,");
            strSQL.Append(" JSE.GOODS_DESCRIPTION,");
            strSQL.Append(" JSE.MARKS_NUMBERS,");
            strSQL.Append(" JSE.INSURANCE_AMT,");
            strSQL.Append(" STMST.INCO_CODE,");
            strSQL.Append(" AMST.VAT_NO,");
            strSQL.Append(" COLMST.PLACE_NAME,");
            strSQL.Append(" DELMST.PLACE_NAME,");
            strSQL.Append("  POLMST.PORT_NAME,");
            strSQL.Append(" PODMST.PORT_NAME,");
            strSQL.Append(" HBL.HBL_REF_NO,");
            strSQL.Append("  MBL.MBL_REF_NO,");
            strSQL.Append(" CGMST.COMMODITY_GROUP_DESC, INVTAGTEXP.VAT_CODE");
            //'added for surcharge
            //strSQL.Append(" TRNFD.SURCHARGE")
            try
            {
                return (objWK.GetDataSet(strSQL.ToString()));
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

        #endregion "Invoice Agent Report"

        #region "ContainerDetails"

        /// <summary>
        /// Fetches the container details.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetails(Int64 nInvPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "SELECT JTSIC.CONTAINER_NUMBER  CONTAINER";
            strSQL += "FROM INV_AGENT_TBL IASI,";
            strSQL += "job_card_trn  JSE,";
            strSQL += "JOB_TRN_CONT JTSIC";
            strSQL += "WHERE IASI.job_card_trn_fk = JSE.job_card_trn_pk";
            strSQL += "AND JTSIC.job_card_trn_fk = JSE.job_card_trn_pk";
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

        #endregion "ContainerDetails"

        #region "Invoice Detail Report"

        /// <summary>
        /// Ins the v_ detai l_ print.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
        public object INV_DETAIL_PRINT(Int64 nInvPK)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT INVAGTEXP.INV_AGENT_PK,");
            sb.Append("       INVAGTEXP.INVOICE_REF_NO,");
            sb.Append("       TO_char(INVAGTEXP.INVOICE_DATE, 'DD/MM/YYYY') INVOICE_DATE,");
            sb.Append("       TO_char(INVAGTEXP.INVOICE_DUE_DATE, 'DD/MM/YYYY') INVOICE_DUE_DATE,");
            sb.Append("       CMST.CURRENCY_ID,");
            sb.Append("       sum(INVTAGTEXP.ELEMENT_AMT * INVTAGTEXP.EXCHANGE_RATE) TOTAMT,");
            sb.Append("       sum(NVL(INVTAGTEXP.TAX_AMT,0)) TAX_AMT,");
            sb.Append("       SUM(DISTINCT(NVL(INVAGTEXP.DISCOUNT_AMT, 0))) DICSOUNT,");
            sb.Append("       SUM(DISTINCT(NVL(INVAGTEXP.NET_INV_AMT, 0))) NET_INV_AMT,");
            sb.Append("       INVTAGTEXP.REMARKS");
            sb.Append("  FROM INV_AGENT_TBL     INVAGTEXP,");
            sb.Append("       CURRENCY_TYPE_MST_TBL     CMST,");
            sb.Append("       INV_AGENT_TRN_TBL INVTAGTEXP");
            sb.Append("  WHERE CMST.CURRENCY_MST_PK = INVAGTEXP.CURRENCY_MST_FK");
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

        #endregion "Invoice Detail Report"

        #region "Invoice Detail Main Report"

        /// <summary>
        /// Consoes the l_ in v_ detai l_ mai n_ print.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public object CONSOL_INV_DETAIL_MAIN_PRINT(Int64 nInvPK, string UserName, int BizType, int ProcessType)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("select DISTINCT INVET.INV_AGENT_PK CONSOL_INVOICE_PK,");
            sb.Append("                JSET.JOBCARD_REF_NO,");
            sb.Append("                'JobCard'  CUSTOMER_NAME,");
            sb.Append("                PMTL.PORT_NAME POL,");
            sb.Append("                PMTD.PORT_NAME POD,");
            //If BizType = 2 And ProcessType = 2 Then 'SEA IMP
            if (ProcessType == 2)
            {
                sb.Append("                JSET.HBL_HAWB_REF_NO,");
                sb.Append("                JSET.MBL_MAWB_REF_NO,");
            }
            else if (BizType == 2)
            {
                sb.Append("                HET.HBL_REF_NO,");
                sb.Append("                MET.MBL_REF_NO,");
            }
            else if (BizType == 1)
            {
                sb.Append("                HET.HAWB_REF_NO HBL_REF_NO,");
                sb.Append("                MET.MAWB_REF_NO MBL_REF_NO,");
            }
            sb.Append("                SUM(INTVET.AMT_IN_INV_CURR) TOT,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN JSET.ARRIVAL_DATE IS NULL THEN");
            sb.Append("                   JSET.ETA_DATE");
            sb.Append("                  ELSE");
            sb.Append("                   JSET.ARRIVAL_DATE");
            sb.Append("                END) ARR_DEP_DATE,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN JSET.BUSINESS_TYPE=2 AND JSET.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
            sb.Append("                   JSET.VESSEL_NAME || '/' || JSET.VOYAGE_FLIGHT_NO");
            sb.Append("                  ELSE");
            sb.Append("                   JSET.VOYAGE_FLIGHT_NO");
            sb.Append("                   END) VSL_VOY,");
            if (BizType == 2)
            {
                sb.Append("                OMT.OPERATOR_NAME,");
            }
            else
            {
                sb.Append("                OMT.AIRLINE_NAME OPERATOR_NAME,");
            }
            sb.Append("                PMTL.PORT_NAME PLR,");
            sb.Append("                 PMTD.PORT_NAME PFD,");
            sb.Append("         '" + UserName + "' USER_NAME");
            sb.Append("  from INV_AGENT_TBL     INVET,");
            sb.Append("       JOB_CARD_TRN      JSET,");
            sb.Append("       INV_AGENT_TRN_TBL INTVET,");
            if (BizType == 2)
            {
                sb.Append("       OPERATOR_MST_TBL          OMT,");
            }
            else
            {
                sb.Append("       AIRLINE_MST_TBL          OMT,");
            }
            if (ProcessType == 1)
            {
                if (BizType == 2)
                {
                    sb.Append("       HBL_EXP_TBL               HET, ");
                    sb.Append("       MBL_EXP_TBL               MET, ");
                }
                else
                {
                    sb.Append("       HAWB_EXP_TBL               HET, ");
                    sb.Append("       MAWB_EXP_TBL               MET, ");
                }
            }
            sb.Append("       AGENT_MST_TBL             AMT,");
            sb.Append("       PORT_MST_TBL              PMTL,");
            sb.Append("       PORT_MST_TBL              PMTD,");
            sb.Append("       BOOKING_MST_TBL           BST ");

            sb.Append("    WHERE INVET.JOB_CARD_FK = JSET.JOB_CARD_TRN_PK");
            sb.Append("    AND INTVET.INV_AGENT_FK = INVET.INV_AGENT_PK");
            //sb.Append("    AND OMT.OPERATOR_MST_PK = JSET.CARRIER_MST_FK ") 'AND JSET.CARRIER_MST_FK=OMT.OPERATOR_MST_PK(+)
            if (BizType == 2)
            {
                sb.Append("    AND JSET.CARRIER_MST_FK = OMT.OPERATOR_MST_PK(+)");
            }
            else
            {
                sb.Append("    AND JSET.CARRIER_MST_FK = OMT.AIRLINE_MST_PK(+)");
            }
            sb.Append("    AND INVET.AGENT_MST_FK =  AMT.AGENT_MST_PK ");
            sb.Append("    AND JSET.PORT_MST_POL_FK = PMTL.PORT_MST_PK(+)");
            sb.Append("    AND JSET.PORT_MST_POD_FK = PMTD.PORT_MST_PK(+)");
            //SEA EXP
            if (ProcessType == 1)
            {
                sb.Append("   AND JSET.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                if (BizType == 2)
                {
                    sb.Append("   AND JSET.HBL_HAWB_FK = HET.HBL_EXP_TBL_PK(+) ");
                    sb.Append("   AND JSET.JOB_CARD_TRN_PK=HET.JOB_CARD_SEA_EXP_FK(+) ");
                    sb.Append("   AND JSET.MBL_MAWB_FK = MET.MBL_EXP_TBL_PK(+)");
                }
                else
                {
                    sb.Append("   AND JSET.HBL_HAWB_FK = HET.HAWB_EXP_TBL_PK(+) ");
                    sb.Append("   AND JSET.JOB_CARD_TRN_PK=HET.JOB_CARD_AIR_EXP_FK(+) ");
                    sb.Append("   AND JSET.MBL_MAWB_FK = MET.MAWB_EXP_TBL_PK(+)");
                }
            }
            sb.Append("   AND INVET.INV_AGENT_PK =" + nInvPK + "");
            sb.Append(" GROUP BY INVET.INV_AGENT_PK,");
            sb.Append("          JSET.JOBCARD_REF_NO,");
            sb.Append("          AMT.AGENT_NAME, ");
            sb.Append("          PMTL.PORT_NAME,");
            sb.Append("          PMTD.PORT_NAME,");
            sb.Append("          JSET.HBL_HAWB_REF_NO,");
            sb.Append("          JSET.MBL_MAWB_REF_NO,");
            sb.Append("          JSET.ARRIVAL_DATE,");
            sb.Append("          JSET.ETA_DATE,");
            sb.Append("          JSET.VOYAGE_FLIGHT_NO,");
            sb.Append("          JSET.VESSEL_NAME,");
            sb.Append("          JSET.BUSINESS_TYPE,");

            if (ProcessType == 1)
            {
                if (BizType == 2)
                {
                    sb.Append("          OMT.OPERATOR_NAME, HET.HBL_REF_NO,");
                    sb.Append("          MET.MBL_REF_NO");
                }
                else
                {
                    sb.Append("          OMT.AIRLINE_NAME, HET.HAWB_REF_NO,");
                    sb.Append("          MET.MAWB_REF_NO");
                }
            }
            else
            {
                if (BizType == 2)
                {
                    sb.Append("          OMT.OPERATOR_NAME ");
                }
                else
                {
                    sb.Append("          OMT.AIRLINE_NAME ");
                }
            }
            try
            {
                return (objWK.GetDataSet(sb.ToString()));
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

        #endregion "Invoice Detail Main Report"

        #region "Invoice Detail Charge Report"

        /// <summary>
        /// Consoes the l_ in v_ detai l_ su b_ mai n_ print.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public object CONSOL_INV_DETAIL_SUB_MAIN_PRINT(Int64 nInvPK, int BizType, int ProcessType)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("  SELECT CONSOL_INVOICE_FK,");
            sb.Append("       FRT_DESC,");
            sb.Append("       CONTAINER_TYPE_MST_ID,");
            sb.Append("       UNIT,");
            sb.Append("       CURRENCY_ID,");
            sb.Append("       ELEMENT_AMT,");
            sb.Append("       EXCHANGE_RATE,");
            sb.Append("       TOTAMT,");
            sb.Append("       FREIGHT_ELEMENT_ID,");
            sb.Append("       TAX_AMT,");
            sb.Append("       TAX_PCNT,");
            sb.Append("       INVOICE,");
            sb.Append("       PREFERENCES,");
            sb.Append("       FPREFERENCE");
            sb.Append("   FROM (SELECT CONSOL_INVOICE_FK,");
            sb.Append("               FRT_DESC,");
            sb.Append("               CONTAINER_TYPE_MST_ID,");
            sb.Append("               CASE");
            sb.Append("                 WHEN BUSINESS_TYPE = 1 THEN");
            sb.Append("                  QUANTITY");
            sb.Append("                 WHEN CARGO_TYPE IN (2, 4) AND CONTAINER_TYPE_MST_ID = 'CBM' THEN");
            sb.Append("                  VOLUME_IN_CBM");
            sb.Append("                 WHEN CARGO_TYPE IN (2, 4) AND CONTAINER_TYPE_MST_ID = 'MT' THEN");
            sb.Append("                  GROSS_WEIGHT");
            sb.Append("                 WHEN CARGO_TYPE IN (2, 4) AND CONTAINER_TYPE_MST_ID = 'W/M' THEN");
            sb.Append("                  CASE");
            sb.Append("                    WHEN VOLUME_IN_CBM > GROSS_WEIGHT THEN");
            sb.Append("                     VOLUME_IN_CBM");
            sb.Append("                    ELSE");
            sb.Append("                     GROSS_WEIGHT");
            sb.Append("                  END");
            sb.Append("                 WHEN CARGO_TYPE IN (2, 4) AND CONTAINER_TYPE_MST_ID = 'UNIT' THEN");
            sb.Append("                  1");
            sb.Append("               ");
            sb.Append("                 ELSE");
            sb.Append("                  COUNT(CONTAINER_TYPE_MST_ID)");
            sb.Append("               END UNIT,");
            sb.Append("               ");
            sb.Append("               CURRENCY_ID,");
            sb.Append("               SUM(ELEMENT_AMT) ELEMENT_AMT,");
            sb.Append("               EXCHANGE_RATE,");
            sb.Append("               SUM(TOTAMT) TOTAMT,");
            sb.Append("               FREIGHT_ELEMENT_ID,");
            sb.Append("               SUM(TAX_AMT) TAX_AMT,");
            sb.Append("               SUM(TAX_PCNT) TAX_PCNT,");
            sb.Append("               SUM(INVOICE) INVOICE,");
            sb.Append("               PREFERENCES,");
            sb.Append("               FPREFERENCE");
            sb.Append("          FROM (SELECT DISTINCT INVET.INV_AGENT_PK CONSOL_INVOICE_FK,");
            sb.Append("                                FEMT.FREIGHT_ELEMENT_NAME FRT_DESC,");
            sb.Append("                                CASE");
            sb.Append("                                  WHEN JSET.BUSINESS_TYPE = 1 THEN");
            sb.Append("                                   QDDT.DD_ID");
            sb.Append("                                  WHEN JSET.CARGO_TYPE IN (2, 4) THEN");
            sb.Append("                                   DUMT.DIMENTION_ID");
            sb.Append("                                  ELSE");
            sb.Append("                                   CTMT1.CONTAINER_TYPE_MST_ID");
            sb.Append("                                END CONTAINER_TYPE_MST_ID,");
            sb.Append("                                JSET.CARGO_TYPE,");
            sb.Append("                                JSET.BUSINESS_TYPE,");
            sb.Append("                                JCTC.VOLUME_IN_CBM,");
            sb.Append("                                JCTC.GROSS_WEIGHT,");
            sb.Append("                                JCFD.QUANTITY,");
            sb.Append("                                CTMT.CURRENCY_ID,");
            //  sb.Append("                                --   SUM(DISTINCT INTVET.ELEMENT_AMT) ELEMENT_AMT,")
            sb.Append("                                INTVET.ELEMENT_AMT   ELEMENT_AMT,");
            sb.Append("                                INTVET.EXCHANGE_RATE,");
            //    sb.Append("                                --      SUM(DISTINCT(INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE)) TOTAMT,")
            sb.Append("                                (INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE) TOTAMT,");
            sb.Append("                                JSET.JOBCARD_REF_NO FREIGHT_ELEMENT_ID,");
            sb.Append("                                INTVET.TAX_AMT,");
            sb.Append("                                INTVET.TAX_PCNT,");
            sb.Append("                                ((INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE) +");
            sb.Append("                                INTVET.TAX_AMT) INVOICE,");
            sb.Append("                                CTMT1.PREFERENCES,");
            sb.Append("                                INTVET.INV_AGENT_TRN_PK CONSOL_INVOICE_TRN_PK,");
            sb.Append("                                FEMT.PREFERENCE FPREFERENCE");
            sb.Append("                  FROM INV_AGENT_TRN_TBL       INTVET,");
            sb.Append("                       INV_AGENT_TBL           INVET,");
            sb.Append("                       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("                       CURRENCY_TYPE_MST_TBL   CTMT,");
            sb.Append("                       JOB_TRN_FD              JCFD,");
            sb.Append("                       JOB_CARD_TRN            JSET,");
            sb.Append("                       JOB_TRN_CONT            JCTC,");
            sb.Append("                       JOB_TRN_OTH_CHRG        JCOTH,");
            sb.Append("                       CONTAINER_TYPE_MST_TBL  CTMT1,");
            sb.Append("                       DIMENTION_UNIT_MST_TBL  DUMT,");
            sb.Append("                       QFOR_DROP_DOWN_TBL      QDDT");
            sb.Append("                 WHERE INTVET.INV_AGENT_FK = INVET.INV_AGENT_PK");
            sb.Append("                   AND INTVET.COST_FRT_ELEMENT_FK =");
            sb.Append("                       FEMT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("                   AND INTVET.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+)");
            sb.Append("                   AND INVET.JOB_CARD_FK = JSET.JOB_CARD_TRN_PK(+)");
            sb.Append("                   AND JSET.JOB_CARD_TRN_PK = JCFD.JOB_CARD_TRN_FK");
            sb.Append("                   AND INTVET.COST_FRT_ELEMENT = 3");
            sb.Append("   AND INVET.INV_AGENT_PK = " + nInvPK + "");
            sb.Append("                   AND JCTC.CONTAINER_TYPE_MST_FK =");
            sb.Append("                       CTMT1.CONTAINER_TYPE_MST_PK(+) ");
            sb.Append("                   AND JSET.JOB_CARD_TRN_PK = JCTC.JOB_CARD_TRN_FK(+)");
            // sb.Append("                      --  AND JCFD.INV_AGENT_TRN_FK = INTVET.INV_AGENT_TRN_PK")
            sb.Append("                   AND FEMT.CHARGE_BASIS = DUMT.DIMENTION_UNIT_MST_PK(+)");
            sb.Append("                   AND FEMT.CHARGE_BASIS = QDDT.DD_VALUE(+) ");
            sb.Append("                   AND QDDT.DD_FLAG(+) = 'valBasis'");
            sb.Append("                   AND JSET.JOB_CARD_TRN_PK = JCOTH.JOB_CARD_TRN_FK(+)");
            sb.Append("                   AND JCFD.JOB_TRN_CONT_FK = JCTC.JOB_TRN_CONT_PK)");
            // sb.Append("        --  AND jcfd.container_type_mst_fk=ctmt1.container_type_mst_pk)")
            sb.Append("         GROUP BY CONSOL_INVOICE_FK,");
            sb.Append("                  FRT_DESC,");
            sb.Append("                  CURRENCY_ID,");
            sb.Append("                  EXCHANGE_RATE,");
            sb.Append("                  FREIGHT_ELEMENT_ID,");
            sb.Append("                  PREFERENCES,");
            sb.Append("                  CONTAINER_TYPE_MST_ID,");
            sb.Append("                  CARGO_TYPE,");
            sb.Append("                  VOLUME_IN_CBM,");
            sb.Append("                  GROSS_WEIGHT,");
            sb.Append("                  BUSINESS_TYPE,");
            sb.Append("                  QUANTITY,");
            sb.Append("                  FPREFERENCE)");
            // sb.Append("--INVET.INV_AGENT_PK)")
            sb.Append("  UNION");
            sb.Append("  SELECT CONSOL_INVOICE_FK,");
            sb.Append("       FRT_DESC,");
            sb.Append("       CONTAINER_TYPE_MST_ID,");
            sb.Append("       UNIT,");
            sb.Append("       CURRENCY_ID,");
            sb.Append("       ELEMENT_AMT,");
            sb.Append("       EXCHANGE_RATE,");
            sb.Append("       TOTAMT,");
            sb.Append("       FREIGHT_ELEMENT_ID,");
            sb.Append("       TAX_AMT,");
            sb.Append("       TAX_PCNT,");
            sb.Append("       INVOICE,");
            sb.Append("       PREFERENCES,");
            sb.Append("       FPREFERENCE");
            sb.Append("  FROM (SELECT CONSOL_INVOICE_FK,");
            sb.Append("               FRT_DESC,");
            sb.Append("               CONTAINER_TYPE_MST_ID,");
            sb.Append("               CASE");
            sb.Append("                 WHEN BUSINESS_TYPE = 1 THEN");
            sb.Append("                  QUANTITY");
            sb.Append("                 WHEN CARGO_TYPE IN (2, 4) AND CONTAINER_TYPE_MST_ID = 'CBM' THEN");
            sb.Append("                  VOLUME_IN_CBM");
            sb.Append("                 WHEN CARGO_TYPE IN (2, 4) AND CONTAINER_TYPE_MST_ID = 'MT' THEN");
            sb.Append("                  GROSS_WEIGHT");
            sb.Append("                 WHEN CARGO_TYPE IN (2, 4) AND CONTAINER_TYPE_MST_ID = 'W/M' THEN");
            sb.Append("                  CASE");
            sb.Append("                    WHEN VOLUME_IN_CBM > GROSS_WEIGHT THEN");
            sb.Append("                     VOLUME_IN_CBM");
            sb.Append("                    ELSE");
            sb.Append("                     GROSS_WEIGHT");
            sb.Append("                  END");
            sb.Append("                 WHEN CARGO_TYPE IN (2, 4) AND CONTAINER_TYPE_MST_ID = 'UNIT' THEN");
            sb.Append("                  1");
            sb.Append("               ");
            sb.Append("                 ELSE");
            sb.Append("                  COUNT(CONTAINER_TYPE_MST_ID)");
            sb.Append("               END UNIT,");
            sb.Append("               ");
            sb.Append("               CURRENCY_ID,");
            sb.Append("               SUM(ELEMENT_AMT) ELEMENT_AMT,");
            sb.Append("               EXCHANGE_RATE,");
            sb.Append("               SUM(TOTAMT) TOTAMT,");
            sb.Append("               FREIGHT_ELEMENT_ID,");
            sb.Append("               SUM(TAX_AMT) TAX_AMT,");
            sb.Append("               SUM(TAX_PCNT) TAX_PCNT,");
            sb.Append("               SUM(INVOICE) INVOICE,");
            sb.Append("               PREFERENCES,");
            sb.Append("               FPREFERENCE");
            sb.Append("          FROM (SELECT DISTINCT INVET.INV_AGENT_PK CONSOL_INVOICE_FK,");
            sb.Append("                                FEMT.FREIGHT_ELEMENT_NAME FRT_DESC,");
            sb.Append("                                CASE");
            sb.Append("                                  WHEN JSET.BUSINESS_TYPE = 1 THEN");
            sb.Append("                                   QDDT.DD_ID");
            sb.Append("                                  WHEN JSET.CARGO_TYPE IN (2, 4) THEN");
            sb.Append("                                   DUMT.DIMENTION_ID");
            sb.Append("                                  ELSE");
            sb.Append("                                   CTMT1.CONTAINER_TYPE_MST_ID");
            sb.Append("                                END CONTAINER_TYPE_MST_ID,");
            sb.Append("                                JSET.CARGO_TYPE, ");
            sb.Append("                                JSET.BUSINESS_TYPE,");
            sb.Append("                                JCTC.VOLUME_IN_CBM,");
            sb.Append("                                JCTC.GROSS_WEIGHT,");
            sb.Append("                                JCFD.QUANTITY,");
            sb.Append("                                CTMT.CURRENCY_ID,");
            //sb.Append("                                --   SUM(DISTINCT INTVET.ELEMENT_AMT) ELEMENT_AMT,")
            sb.Append("                                INTVET.ELEMENT_AMT   ELEMENT_AMT,");
            sb.Append("                                INTVET.EXCHANGE_RATE,");
            //  sb.Append("                                --      SUM(DISTINCT(INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE)) TOTAMT,")
            sb.Append("                                (INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE) TOTAMT,");
            sb.Append("                                JSET.JOBCARD_REF_NO FREIGHT_ELEMENT_ID,");
            sb.Append("                                INTVET.TAX_AMT,");
            sb.Append("                                INTVET.TAX_PCNT,");
            sb.Append("                                ((INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE) +");
            sb.Append("                                INTVET.TAX_AMT) INVOICE,");
            sb.Append("                                CTMT1.PREFERENCES,");
            sb.Append("                                INTVET.INV_AGENT_TRN_PK CONSOL_INVOICE_TRN_PK,");
            sb.Append("                                FEMT.PREFERENCE FPREFERENCE");
            sb.Append("                  FROM INV_AGENT_TRN_TBL       INTVET,");
            sb.Append("                       INV_AGENT_TBL           INVET,");
            sb.Append("                       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("                       CURRENCY_TYPE_MST_TBL   CTMT,");
            sb.Append("                       JOB_TRN_FD              JCFD,");
            sb.Append("                       JOB_CARD_TRN            JSET,");
            sb.Append("                       JOB_TRN_CONT            JCTC,");
            sb.Append("                       CONTAINER_TYPE_MST_TBL  CTMT1,");
            sb.Append("                       DIMENTION_UNIT_MST_TBL  DUMT,");
            sb.Append("                       QFOR_DROP_DOWN_TBL      QDDT");
            sb.Append("                 WHERE INTVET.INV_AGENT_FK = INVET.INV_AGENT_PK");
            sb.Append("                   AND INTVET.COST_FRT_ELEMENT_FK =");
            sb.Append("                       FEMT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("                   AND INTVET.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+)");
            sb.Append("                   AND INVET.JOB_CARD_FK = JSET.JOB_CARD_TRN_PK");
            sb.Append("                   AND JSET.JOB_CARD_TRN_PK = JCFD.JOB_CARD_TRN_FK");
            sb.Append("                   AND INTVET.COST_FRT_ELEMENT IN (1, 2)");
            sb.Append("   AND INVET.INV_AGENT_PK = " + nInvPK + "");
            sb.Append("                   AND JCTC.CONTAINER_TYPE_MST_FK =");
            sb.Append("                       CTMT1.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("                   AND JSET.JOB_CARD_TRN_PK = JCTC.JOB_CARD_TRN_FK(+)");
            sb.Append("                   AND JCFD.INV_AGENT_TRN_FK = INTVET.INV_AGENT_TRN_PK");
            sb.Append("                   AND FEMT.CHARGE_BASIS = DUMT.DIMENTION_UNIT_MST_PK(+)");
            sb.Append("                   AND FEMT.CHARGE_BASIS = QDDT.DD_VALUE(+)");
            sb.Append("                   AND QDDT.DD_FLAG(+) = 'valBasis')");
            // sb.Append("                   AND JCFD.JOB_TRN_CONT_FK = JCTC.JOB_TRN_CONT_PK)")
            //sb.Append("                   --  AND jcfd.container_type_mst_fk=ctmt1.container_type_mst_pk)")
            sb.Append("         GROUP BY CONSOL_INVOICE_FK,");
            sb.Append("                  FRT_DESC,");
            sb.Append("                  CURRENCY_ID,");
            sb.Append("                  EXCHANGE_RATE,");
            sb.Append("                  FREIGHT_ELEMENT_ID,");
            sb.Append("                  PREFERENCES,");
            sb.Append("                  CONTAINER_TYPE_MST_ID,");
            sb.Append("                  CARGO_TYPE,");
            sb.Append("                  VOLUME_IN_CBM,");
            sb.Append("                  GROSS_WEIGHT,");
            sb.Append("                  BUSINESS_TYPE,");
            sb.Append("                  QUANTITY,");
            sb.Append("                  FPREFERENCE)");
            sb.Append("");
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

        #endregion "Invoice Detail Charge Report"

        #region "Invoice Agent Details  Report"

        /// <summary>
        /// Consoes the l_ in v_ cus t_ print.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
        public object CONSOL_INV_CUST_PRINT(Int64 nInvPK)
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
            sb.Append("           AND INVET.INV_AGENT_PK = " + nInvPK + "");
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

        #endregion "Invoice Agent Details  Report"

        #region "Fill Jobcard Other Details"

        /// <summary>
        /// Fills the job card other charges.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FillJobCardOtherCharges(string JOBPK)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append("         SELECT");
            strSQL.Append("         oth_chrg.JOB_TRN_OTH_PK,");
            strSQL.Append("         frt.freight_element_mst_pk,");
            strSQL.Append("         frt.freight_element_id,");
            strSQL.Append("         frt.freight_element_name,");
            strSQL.Append("         DECODE(oth_chrg.freight_type,1,'Prepaid',2,'Collect',3,'Foreign') Payment_Type, ");
            // By Amit on 26-April-2007
            strSQL.Append("         oth_chrg.location_mst_fk,");
            strSQL.Append("         lmt.location_id ,");
            strSQL.Append("         oth_chrg.frtpayer_cust_mst_fk,");
            strSQL.Append("         cmt.customer_id,");
            strSQL.Append("         curr.currency_mst_pk, ");
            strSQL.Append("         oth_chrg.exchange_rate ROE, ");
            strSQL.Append("         oth_chrg.amount amount,");
            strSQL.Append("         jobcard_mst.Consignee_Cust_Mst_Fk,oth_chrg.INV_AGENT_TRN_FK,");
            strSQL.Append("         'false' \"Delete\", oth_chrg.PRINT_ON_MBL \"Print\" ");
            strSQL.Append("FROM");
            strSQL.Append("         JOB_TRN_OTH_CHRG oth_chrg,");
            strSQL.Append("         job_card_trn jobcard_mst,");
            strSQL.Append("         freight_element_mst_tbl frt,");
            strSQL.Append("         currency_type_mst_tbl curr,");
            strSQL.Append("         location_mst_tbl lmt,");
            strSQL.Append("         customer_mst_tbl cmt");
            strSQL.Append("WHERE");
            strSQL.Append("         oth_chrg.job_card_trn_fk = jobcard_mst.job_card_trn_pk");
            strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.location_mst_fk = lmt.location_mst_pk (+)");
            strSQL.Append("         AND oth_chrg.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.job_card_trn_fk    = " + JOBPK);
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

        #endregion "Fill Jobcard Other Details"

        #region "Get Agent Freight Details"

        /// <summary>
        /// Gets the agt FRT details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet GetAgtFrtDetails(string JOBPK)
        {
            string strSQL = null;
            strSQL = " SELECT DECODE(TRN.COST_FRT_ELEMENT,1,'FREIGHT',2,'FREIGHT',3,'OTHER') AS TYPE,";
            strSQL += " TRN.INV_AGENT_TRN_PK AS PK,";
            strSQL += " HDR.JOB_CARD_FK AS JOBCARD_FK,";
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
            strSQL += " 'True' AS CHK, HDR.CB_DP_LOAD_AGENT";
            strSQL += " FROM";
            strSQL += " INV_AGENT_TRN_TBL TRN,";
            strSQL += " INV_AGENT_TBL HDR,";
            strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
            strSQL += " WHERE";
            strSQL += " TRN.INV_AGENT_FK = HDR.INV_AGENT_PK";
            strSQL += " AND TRN.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            strSQL += " AND HDR.JOB_CARD_FK = " + JOBPK;
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

        #endregion "Get Agent Freight Details"

        #region "Get Consignee PK"

        /// <summary>
        /// Gets the consignee pk.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public int GetConsigneePK(string JOBPK)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append("   SELECT");
            strSQL.Append("   nvl(jobcard_mst.Consignee_Cust_Mst_Fk,0)");
            strSQL.Append("  FROM");
            strSQL.Append("   job_card_trn jobcard_mst");
            strSQL.Append("   WHERE jobcard_mst.job_card_trn_pk = " + JOBPK);
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

        #endregion "Get Consignee PK"

        #region "DRAFT INV DETAILS"

        /// <summary>
        /// Drafs the t_ in v_ detai l_ mai n_ print.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="FrtElePKs">The FRT ele p ks.</param>
        /// <param name="Biz_Type">Type of the biz_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <returns></returns>
        public DataSet DRAFT_INV_DETAIL_MAIN_PRINT(int JobPk, string FrtElePKs, int Biz_Type, int Process_Type, int AgentType)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with13 = objWK.MyCommand.Parameters;
                _with13.Add("JOB_CARD_PK_IN", JobPk).Direction = ParameterDirection.Input;
                _with13.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with13.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with13.Add("USER_NAME_IN", HttpContext.Current.Session["USER_NAME"]).Direction = ParameterDirection.Input;
                _with13.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with13.Add("AGENT_TYPE_IN", AgentType).Direction = ParameterDirection.Input;
                _with13.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "AGENT_DETAILS_MAIN_RPT_PRINT");
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
        /// Drafs the t_ in v_ detai l_ su b_ mai n_ print.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="FrtElePKs">The FRT ele p ks.</param>
        /// <param name="Biz_Type">Type of the biz_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <param name="Loc_fk">The loc_fk.</param>
        /// <param name="CurrFK">The curr fk.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <param name="Log_Curr_fk">The log_ curr_fk.</param>
        /// <returns></returns>
        public DataSet DRAFT_INV_DETAIL_SUB_MAIN_PRINT(int JobPk, string FrtElePKs, int Biz_Type, int Process_Type, int Loc_fk = 0, int CurrFK = 0, int AgentType = 1, int Log_Curr_fk = 0)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with14 = objWK.MyCommand.Parameters;
                _with14.Add("JOB_CARD_PK_IN", JobPk).Direction = ParameterDirection.Input;
                _with14.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with14.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with14.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with14.Add("LOG_IN_LOC_FK_IN", Loc_fk).Direction = ParameterDirection.Input;
                _with14.Add("CURRENCY_FK_IN", CurrFK).Direction = ParameterDirection.Input;
                _with14.Add("AGENT_TYPE_IN", AgentType).Direction = ParameterDirection.Input;
                _with14.Add("LOGIN_CUR_FK_IN", Log_Curr_fk).Direction = ParameterDirection.Input;
                _with14.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "AGENT_SUB_MAIN_RPT_PRINT");
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
        /// Drafs the t_ in v_ cus t_ print.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="Biz_Type">Type of the biz_.</param>
        /// <param name="Process_Type">Type of the process_.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <returns></returns>
        public DataSet DRAFT_INV_CUST_PRINT(int JobPk, int Biz_Type, int Process_Type, int AgentType)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with15 = objWK.MyCommand.Parameters;
                _with15.Add("JOB_CARD_PK_IN", JobPk).Direction = ParameterDirection.Input;
                _with15.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with15.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with15.Add("AGENT_TYPE_IN", AgentType).Direction = ParameterDirection.Input;
                _with15.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "AGENT_INV_CUST_RPT_PRINT");
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

        #endregion "DRAFT INV DETAILS"

        #region "Invoice Detail Main Report SEA IMP"

        /// <summary>
        /// Consoes the l_ in v_ detai l_ mai n_ printseaimp.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public object CONSOL_INV_DETAIL_MAIN_PRINTSEAIMP(Int64 nInvPK, string UserName, int BizType, int ProcessType)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT INVET.INV_AGENT_PK CONSOL_INVOICE_PK,");
            sb.Append("                JSET.JOBCARD_REF_NO,");
            sb.Append("                'JobCard' CUSTOMER_NAME,");
            sb.Append("                PMTL.PORT_NAME POL,");
            sb.Append("                PMTD.PORT_NAME POD,");
            sb.Append("                JSET.HBL_HAWB_REF_NO,");
            sb.Append("                JSET.MBL_MAWB_REF_NO,");
            sb.Append("                SUM(INTVET.AMT_IN_INV_CURR) TOT,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN JSET.ARRIVAL_DATE IS NULL THEN");
            sb.Append("                   JSET.ETA_DATE");
            sb.Append("                  ELSE");
            sb.Append("                   JSET.ARRIVAL_DATE");
            sb.Append("                END) ARR_DEP_DATE,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN JSET.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
            sb.Append("                   JSET.VESSEL_NAME || '/' || JSET.VOYAGE_FLIGHT_NO");
            sb.Append("                  ELSE");
            sb.Append("                   JSET.VOYAGE_FLIGHT_NO");
            sb.Append("                END) VSL_VOY,");
            sb.Append("                PMTL.PORT_NAME PLR,");
            sb.Append("                PMTD.PORT_NAME PFD,");
            sb.Append("                'JAN MAAS' USER_NAME");
            sb.Append("  FROM INV_AGENT_TBL     INVET,");
            sb.Append("       JOB_CARD_TRN      JSET,");
            sb.Append("       INV_AGENT_TRN_TBL INTVET,");
            sb.Append("       AGENT_MST_TBL     AMT,");
            sb.Append("       PORT_MST_TBL      PMTL,");
            sb.Append("       PORT_MST_TBL      PMTD,");
            sb.Append("       BOOKING_MST_TBL   BST");
            sb.Append(" WHERE INVET.JOB_CARD_FK = JSET.JOB_CARD_TRN_PK");
            sb.Append("   AND INTVET.INV_AGENT_FK = INVET.INV_AGENT_PK");
            sb.Append("   AND INVET.Agent_Mst_Fk = AMT.AGENT_MST_PK");
            sb.Append("   AND JSET.PORT_MST_POL_FK = PMTL.PORT_MST_PK(+)");
            sb.Append("   AND JSET.PORT_MST_POD_FK = PMTD.PORT_MST_PK(+)");
            sb.Append("   AND INVET.INV_AGENT_PK = " + nInvPK);
            sb.Append("GROUP BY INVET.INV_AGENT_PK,");
            sb.Append("          JSET.JOBCARD_REF_NO,");
            sb.Append("          AMT.AGENT_NAME,");
            sb.Append("          PMTL.PORT_NAME,");
            sb.Append("          PMTD.PORT_NAME,");
            sb.Append("          JSET.HBL_HAWB_REF_NO,");
            sb.Append("          JSET.MBL_MAWB_REF_NO,");
            sb.Append("          JSET.ARRIVAL_DATE,");
            sb.Append("          JSET.ETA_DATE,");
            sb.Append("          JSET.VOYAGE_FLIGHT_NO,");
            sb.Append("          JSET.VESSEL_NAME");
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

        #endregion "Invoice Detail Main Report SEA IMP"
    }
}