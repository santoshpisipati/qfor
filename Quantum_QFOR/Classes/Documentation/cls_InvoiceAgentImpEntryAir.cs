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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsInvoiceAgentImpEntryAir : CommonFeatures
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

			strSQL = " SELECT ";
			strSQL += " JOBCOST.JOB_TRN_PIA_PK,";
			strSQL += " JOBCOST.JOB_CARD_TRN_FK,";
			strSQL += " JOBCOST.COST_ELEMENT_MST_FK,";
			strSQL += " JOBCOST.CURRENCY_MST_FK,";
			strSQL += " CEMT.COST_ELEMENT_NAME,";
			strSQL += " CUMT.CURRENCY_ID,";
			strSQL += " JOBCOST.ESTIMATED_AMT,";
			strSQL += " JOBCOST.INVOICE_AMT,";
			//strSQL &= vbCrLf & "  (CASE WHEN (JOBCOST.INV_AGENT_TRN_AIR_IMP_FK IS NULL AND JOBCOST.INV_CUST_TRN_AIR_IMP_FK IS NULL) THEN NULL WHEN JOBCOST.INV_AGENT_TRN_AIR_IMP_FK IS NOT NULL THEN (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_AGENT_AIR_IMP_FK=JOBCOST.INV_AGENT_TRN_AIR_IMP_FK) ELSE (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_IMP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_CUST_TRN_AIR_IMP_PK=JOBCOST.INV_CUST_TRN_AIR_IMP_FK) END) INV_AMT,"
			//strSQL &= vbCrLf & "  (CASE WHEN (JOBCOST.INV_AGENT_TRN_AIR_IMP_FK IS NULL AND JOBCOST.INV_CUST_TRN_AIR_IMP_FK IS NULL) THEN 'False' ELSE 'True' END) CHK"
			strSQL += "ROUND((CASE WHEN (JOBCOST.INVOICE_SEA_TBL_FK IS NULL AND JOBCOST.INV_AGENT_TRN_AIR_IMP_FK IS NULL) THEN NULL WHEN JOBCOST.INVOICE_SEA_TBL_FK IS NOT NULL THEN (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_IMP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_CUST_TRN_AIR_IMP_PK=JOBCOST.INVOICE_SEA_TBL_FK) ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_AGENT_TRN_PK=JOBCOST.INV_AGENT_TRN_AIR_IMP_FK) END),2) INV_AMT,";
			strSQL += "(CASE WHEN (JOBCOST.INVOICE_SEA_TBL_FK IS NULL AND JOBCOST.INV_AGENT_TRN_AIR_IMP_FK IS NULL) THEN 'False' ELSE 'True' END) CHK";

			strSQL += " FROM JOB_TRN_PIA JOBCOST,";
			strSQL += " COST_ELEMENT_MST_TBL CEMT,";
			strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
			strSQL += " WHERE ";
			strSQL += " JOBCOST.COST_ELEMENT_MST_FK = CEMT.COST_ELEMENT_MST_PK";
			strSQL += " AND JOBCOST.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
			strSQL += " AND JOBCOST.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
			strSQL += " ORDER BY CEMT.COST_ELEMENT_NAME,CUMT.CURRENCY_ID";
			try {
				DS = objWK.GetDataSet(strSQL);
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        //This function is called to fetch all the job card freight elements and other charges elements
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

			strSQL = " SELECT";
			strSQL += " JOBFRT.JOB_TRN_FD_PK,";
			strSQL += " JOBFRT.JOB_CARD_TRN_FK,";
			strSQL += " JOBFRT.FREIGHT_ELEMENT_MST_FK,";
			strSQL += " JOBFRT.CURRENCY_MST_FK,";
			strSQL += " FMT.FREIGHT_ELEMENT_NAME,";
			strSQL += " DECODE(JOBFRT.FREIGHT_TYPE,1,'P',2,'C') AS PC,";
			strSQL += " CUMT.CURRENCY_ID,";
			strSQL += " JOBFRT.FREIGHT_AMT,";
			//strSQL &= vbCrLf & " CUMT.CURRENCY_ID,"
			//strSQL &= vbCrLf & " (CASE WHEN JOBFRT.INV_AGENT_TRN_AIR_IMP_FK IS NULL THEN NULL"
			//strSQL &= vbCrLf & "      ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN"
			//strSQL &= vbCrLf & "         WHERE(TRN.COST_FRT_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK)"
			//strSQL &= vbCrLf & "         AND TRN.COST_FRT_ELEMENT=2 "
			//strSQL &= vbCrLf & "         AND TRN.INV_AGENT_AIR_IMP_FK = JOBFRT.INV_AGENT_TRN_AIR_IMP_FK) END) INV_AMT,"
			//strSQL &= vbCrLf & " (CASE WHEN JOBFRT.INV_AGENT_TRN_AIR_IMP_FK IS NULL THEN 'False' ELSE 'True' END) CHK"
			//surya18Nov06 If AgentType = 1 Then
			//add by latha for fetching the invoiceamt by converting into its currency on january 31
			strSQL += " ROUND((CASE WHEN (JOBFRT.INV_AGENT_TRN_AIR_IMP_FK IS NULL AND JOBFRT.INV_CUST_TRN_AIR_IMP_FK IS NULL and jobfrt.CONSOL_INVOICE_TRN_FK is null) THEN NULL ";
			strSQL += "       WHEN JOBFRT.INV_AGENT_TRN_AIR_IMP_FK IS NOT NULL THEN (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT in(1,2) AND TRN.INV_AGENT_TRN_PK=JOBFRT.INV_AGENT_TRN_AIR_IMP_FK) WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN (SELECT SUM(CTRN.TOT_AMT / DECODE(Ctrn.exchange_rate,NULL,1,0,1,Ctrn.exchange_rate)) TOT_AMT FROM CONSOL_INVOICE_TRN_TBL CTRN WHERE CTRN.FRT_OTH_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK AND CTRN.FRT_OTH_ELEMENT = 1 AND (CTRN.JOB_CARD_FK = JOBFRT.JOB_CARD_TRN_FK))";
			strSQL += "       ELSE (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_CUST_TRN_AIR_IMP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT in(1,2) AND TRN.INV_CUST_TRN_AIR_IMP_PK= JOBFRT.INV_CUST_TRN_AIR_IMP_FK) END),2) INV_AMT,";
			//Else
			//    strSQL &= vbCrLf & " ROUND((CASE WHEN (JOBFRT.INV_AGENT_TRN_AIR_IMP_FK IS NULL AND JOBFRT.INV_CUST_TRN_AIR_IMP_FK IS NULL and jobfrt.CONSOL_INVOICE_TRN_FK is null) THEN NULL "
			//    strSQL &= vbCrLf & "       WHEN JOBFRT.INV_AGENT_TRN_AIR_IMP_FK IS NOT NULL THEN (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT = 2 AND TRN.INV_AGENT_TRN_AIR_IMP_pK=JOBFRT.INV_AGENT_TRN_AIR_IMP_FK)"
			//    strSQL &= vbCrLf & "        WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN (SELECT (TRN.TOT_AMT / DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM CONSOL_INVOICE_TRN_TBL TRN WHERE TRN.FRT_OTH_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.FRT_OTH_ELEMENT = 1 AND TRN.CONSOL_INVOICE_FK = JOBFRT.CONSOL_INVOICE_TRN_FK) ELSE (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_CUST_TRN_AIR_IMP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_CUST_TRN_AIR_IMP_PK= JOBFRT.INV_CUST_TRN_AIR_IMP_FK) END),2) INV_AMT,"
			//End If
			strSQL += " (CASE WHEN JOBFRT.Inv_Agent_Trn_Air_IMP_Fk IS NULL AND JOBFRT.INV_CUST_TRN_AIR_IMP_FK IS NULL and (jobfrt.CONSOL_INVOICE_TRN_FK is null  OR (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=jobfrt.CONSOL_INVOICE_TRN_FK)=2)  THEN 'False' ELSE 'True' END) CHK";
			strSQL += " FROM";
			strSQL += " JOB_TRN_FD JOBFRT,";
			strSQL += " FREIGHT_ELEMENT_MST_TBL FMT,";
			strSQL += " CURRENCY_TYPE_MST_TBL CUMT ";
			strSQL += " WHERE";
			strSQL += " JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK";
			strSQL += " AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
			//If AgentType = 2 Then
			// strSQL &= vbCrLf & " and jobfrt.freight_type in (1,2) "
			// Else
			//surya18Nov06 strSQL &= vbCrLf & " and jobfrt.freight_type =1 "
			// End If
			strSQL += " AND JOBFRT.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);

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
			//'CB Agent
			if (AgentType == 1) {
				strSQL += " 'C' AS PC,";
			//'Load Agent
			} else {
				strSQL += " 'P' AS PC,";
			}
			strSQL += " CUMT.CURRENCY_ID,";
			strSQL += " JOBOTH.AMOUNT,";
			//strSQL &= vbCrLf & " CUMT.CURRENCY_ID,"
			//add by latha for fetching the invoiceamt by converting into its currency on january 31
			strSQL += " ROUND((CASE WHEN (JOBOTH.Inv_Agent_Trn_AIR_IMP_Fk IS NULL AND JOBOTH.INV_CUST_TRN_AIR_IMP_FK IS NULL and JOBOTH.CONSOL_INVOICE_TRN_FK is null) THEN NULL WHEN JOBOTH.Inv_Agent_Trn_AIR_IMP_Fk IS not NULL then (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=3 AND TRN.INV_agent_TRN_AIR_IMP_PK=JOBOTH.INV_agent_TRN_AIR_IMP_FK)  WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN (SELECT SUM(CTRN.TOT_AMT / DECODE(Ctrn.exchange_rate,NULL,1,0,1,Ctrn.exchange_rate)) TOT_AMT FROM CONSOL_INVOICE_TRN_TBL CTRN WHERE CTRN.FRT_OTH_ELEMENT_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK AND CTRN.FRT_OTH_ELEMENT = 2 AND (CTRN.JOB_CARD_FK = JOBOTH.JOB_CARD_TRN_FK)) else (SELECT (TRN.TOT_AMT/ DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) TOT_AMT FROM INV_CUST_TRN_AIR_IMP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=3 AND TRN.INV_CUST_TRN_AIR_IMP_PK=JOBOTH.INV_CUST_TRN_AIR_IMP_FK) END),2)INV_AMT,";
			strSQL += " (CASE WHEN (JOBOTH.Inv_Cust_Trn_AIR_IMP_Fk is null and joboth.inv_agent_trn_AIR_IMP_fk is null and JOBOTH.CONSOL_INVOICE_TRN_FK is null) THEN 'False' ELSE 'True' END) CHK";
			strSQL += " FROM";
			strSQL += " JOB_TRN_OTH_CHRG JOBOTH,";
			strSQL += " FREIGHT_ELEMENT_MST_TBL FMT,";
			strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
			strSQL += " WHERE";
			strSQL += " JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK";
			strSQL += " AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
			strSQL += " AND JOBOTH.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
			// added by jitendra
			if (AgentType == 2) {
				strSQL += "AND JOBOTH.Freight_Type=1";
			}
			try {
				DS = objWK.GetDataSet(strSQL);
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
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
			WorkFlow objWK = new WorkFlow();
			DataSet DS = null;


			if (nInvoicePK == 0) {
				// adding by thiyagarajan on 28/2/09:VAT Implementation task 
				string vatcode = null;
				string custpk = null;
				strSQL = " select FETCH_EU(" + nJobCardPK + ",1,2) from dual";
				vatcode = objWK.ExecuteScaler(strSQL);
				if (AgentType == 1) {
					strSQL = " SELECT J.CB_AGENT_MST_FK FROM JOB_CARD_TRN J WHERE J.JOB_CARD_TRN_PK=" + nJobCardPK;
				} else {
					strSQL = " SELECT J.CL_AGENT_MST_FK FROM JOB_CARD_TRN J WHERE J.JOB_CARD_TRN_PK=" + nJobCardPK;
				}
				custpk = objWK.ExecuteScaler(strSQL);
				if (string.IsNullOrEmpty(getDefault(custpk, "").ToString())) {
					custpk = "0";
				}
				strSQL = "";
				//end by thiyagarajan 

				///''''This Part is to fetch all Cost Elements which are not invoiced
				//strSQL = " SELECT 'COST' AS TYPE,"
				//strSQL &= vbCrLf & " JOBCOST.JOB_TRN_PIA_PK AS PK,"
				//strSQL &= vbCrLf & " JOBCOST.JOB_CARD_TRN_FK AS JOBCARD_FK,"
				//strSQL &= vbCrLf & " JOBCOST.COST_ELEMENT_MST_FK AS ELEMENT_FK,"
				//strSQL &= vbCrLf & " JOBCOST.CURRENCY_MST_FK,"
				//strSQL &= vbCrLf & " CEMT.COST_ELEMENT_NAME AS ELEMENT_NAME,"
				//strSQL &= vbCrLf & " '' AS ELEMENT_SEARCH,"
				//strSQL &= vbCrLf & " CUMT.CURRENCY_ID,"
				//strSQL &= vbCrLf & " '' AS CURR_SEARCH,"
				//strSQL &= vbCrLf & " ROUND(GET_EX_RATE(JOBCOST.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS EXCHANGE_RATE,"
				//strSQL &= vbCrLf & " JOBCOST.ESTIMATED_AMT AS AMOUNT,"
				//strSQL &= vbCrLf & " ROUND(JOBCOST.ESTIMATED_AMT * GET_EX_RATE(JOBCOST.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS INV_AMOUNT,"
				//'strSQL &= vbCrLf & " '' AS TAX_PERCENT,"
				//'strSQL &= vbCrLf & " '' AS TAX_AMOUNT,"
				//'strSQL &= vbCrLf & " ROUND(JOBCOST.ESTIMATED_AMT * GET_EX_RATE(JOBCOST.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS TOTAL_AMOUNT,"
				//strSQL &= vbCrLf & " NVL(CORP.VAT_PERCENTAGE,0) AS TAX_PERCENT,"
				//strSQL &= vbCrLf & " (NVL(CORP.VAT_PERCENTAGE,0) * ROUND(JOBCOST.ESTIMATED_AMT * GET_EX_RATE(JOBCOST.CURRENCY_MST_FK,466,SYSDATE),4)/100) AS TAX_AMOUNT,"
				//strSQL &= vbCrLf & " ((NVL(CORP.VAT_PERCENTAGE,0) * ROUND(JOBCOST.ESTIMATED_AMT * GET_EX_RATE(JOBCOST.CURRENCY_MST_FK,466,SYSDATE),4)/100) +  ROUND(JOBCOST.ESTIMATED_AMT * GET_EX_RATE(JOBCOST.CURRENCY_MST_FK,466,SYSDATE),4))AS TOTAL_AMOUNT,"
				//strSQL &= vbCrLf & " '' AS REMARKS,"
				//strSQL &= vbCrLf & " 'New' AS ""MODE"","
				//strSQL &= vbCrLf & " 'False' AS CHK"
				//strSQL &= vbCrLf & " FROM"
				//strSQL &= vbCrLf & " JOB_TRN_PIA JOBCOST,"
				//strSQL &= vbCrLf & " COST_ELEMENT_MST_TBL CEMT,"
				//strSQL &= vbCrLf & " CURRENCY_TYPE_MST_TBL CUMT,"
				//strSQL &= vbCrLf & " CORPORATE_MST_TBL CORP"
				//strSQL &= vbCrLf & " WHERE"
				//strSQL &= vbCrLf & " JOBCOST.COST_ELEMENT_MST_FK = CEMT.COST_ELEMENT_MST_PK"
				//strSQL &= vbCrLf & " AND JOBCOST.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK"
				//strSQL &= vbCrLf & " AND JOBCOST.JOB_CARD_TRN_FK=" & CStr(nJobCardPK)
				//strSQL &= vbCrLf & " AND JOBCOST.INV_AGENT_TRN_AIR_IMP_FK  IS NULL"
				//strSQL &= vbCrLf & " AND JOBcost.INV_CUST_TRN_AIR_IMP_FK IS NULL"

				//strSQL &= vbCrLf & " "
				//strSQL &= vbCrLf & "UNION"
				//strSQL &= vbCrLf & " "

				///''''This Part is to fetch all Freight Elements which are not invoiced and is of type Prepaid
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
				//'strSQL &= vbCrLf & " ROUND((NVL(CORP.VAT_PERCENTAGE,0) * ROUND(JOBFRT.FREIGHT_AMT * GET_EX_RATE(JOBFRT.CURRENCY_MST_FK,466,SYSDATE),4)/100),2) AS TAX_AMOUNT,"
				//strSQL &= vbCrLf & " ROUND((NVL(CORP.VAT_PERCENTAGE,0) * JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100,2) AS TAX_AMOUNT,"
				//'strSQL &= vbCrLf & " ROUND(((NVL(CORP.VAT_PERCENTAGE,0) * ROUND(JOBFRT.FREIGHT_AMT * GET_EX_RATE(JOBFRT.CURRENCY_MST_FK,466,SYSDATE),4)/100) + ROUND(JOBFRT.FREIGHT_AMT * GET_EX_RATE(JOBFRT.CURRENCY_MST_FK,466,SYSDATE),2)),2) AS TOTAL_AMOUNT,"
				//strSQL &= vbCrLf & " ROUND(((NVL(CORP.VAT_PERCENTAGE,0) * JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) + (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE),2) AS TOTAL_AMOUNT,"
				//Added by Venkata 

				//commented & adding by thiyagarajan on 28/2/09:VAT Implementation task 

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
				//VAT_PERCENT belongs to country instead of corporate 
				//by thiyagarajan on 3/12/08 for location based currency task

				//strSQL &= vbCrLf & "(NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,"
				//strSQL &= vbCrLf & "      user_mst_tbl umt,location_mst_tbl loc"
				//strSQL &= vbCrLf & "where umt.default_location_fk = loc.location_mst_pk"
				//strSQL &= vbCrLf & " and loc.country_mst_fk = frtv.country_mst_fk"
				//strSQL &= vbCrLf & "and umt.user_mst_pk =" & UserPk
				//strSQL &= vbCrLf & "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)"
				//strSQL &= vbCrLf & "),0)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100)  AS TAX_AMOUNT,"


				//VAT_PERCENT belongs to country instead of corporate 
				//by thiyagarajan on 3/12/08 for location based currency task

				//strSQL &= vbCrLf & "((NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,"
				//strSQL &= vbCrLf & "      user_mst_tbl umt,location_mst_tbl loc"
				//strSQL &= vbCrLf & "where umt.default_location_fk = loc.location_mst_pk"
				//strSQL &= vbCrLf & " and loc.country_mst_fk = frtv.country_mst_fk"
				//strSQL &= vbCrLf & "and umt.user_mst_pk =" & UserPk
				//strSQL &= vbCrLf & "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)"
				//strSQL &= vbCrLf & "),0)*(JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) + JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) AS TOTAL_AMOUNT,"

				//VAT_PERCENT belongs to country instead of corporate 
				//by thiyagarajan on 3/12/08 for location based currency task

				//VAT CODE
				strSQL += " (select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,1,2) from dual) VAT_CODE, ";
				//VAT PERCENT
				strSQL += " (select FETCH_VAT ( " + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2,2) from dual) VAT_PERCENT,";
				//TAX PERCENT
				strSQL += " ((select FETCH_VAT ( " + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2,2) from dual)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) TAX_AMOUNT,";
				//total amount
				strSQL += "  (((select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2,2) from dual)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) + JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) TOTAL_AMOUNT,";

				//end by thiyagarajan 

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
				strSQL += " AND JOBFRT.INV_AGENT_TRN_AIR_IMP_FK IS NULL";
				//strSQL &= vbCrLf & " AND JOBFRT.FREIGHT_TYPE=1"
				///'''''''''
				if (AgentType == 1) {
					strSQL += " AND JOBFRT.FREIGHT_TYPE IN (1,2)";
				} else {
					strSQL += " AND JOBFRT.FREIGHT_TYPE = 1 ";
				}
				strSQL += " AND JOBFRT.INV_CUST_TRN_AIR_IMP_FK IS NULL";
				strSQL += " AND (JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL  OR (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=JOBFRT.CONSOL_INVOICE_TRN_FK)=2) ";
				///'''''''''

				strSQL += " ";
				strSQL += "UNION";
				strSQL += " ";

				///''''This Part is to fetch Other Elements which are not invoiced from JOB_TRN_OTH_CHRG Table
				strSQL += " SELECT 'OTHER' AS TYPE,";
				strSQL += " JOBOTH.JOB_TRN_OTH_PK AS PK,";
				strSQL += " JOBOTH.JOB_CARD_TRN_FK AS JOBCARD_FK,";
				strSQL += " JOBOTH.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,";
				strSQL += " JOBOTH.CURRENCY_MST_FK,";
				strSQL += " FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,";
				strSQL += " '' AS ELEMENT_SEARCH,";
				strSQL += " CUMT.CURRENCY_ID,";
				strSQL += " '' AS CURR_SEARCH,";
				//'strSQL &= vbCrLf & " ROUND(GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS EXCHANGE_RATE,"
				strSQL += " ROUND(JOBOTH.AMOUNT,2) AS AMOUNT,";
				strSQL += " JOBOTH.EXCHANGE_RATE AS EXCHANGE_RATE,";
				//'strSQL &= vbCrLf & " ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),2) AS INV_AMOUNT,"
				strSQL += " ROUND(JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE,2) AS INV_AMOUNT,";
				//strSQL &= vbCrLf & " '' AS TAX_PERCENT,"
				//strSQL &= vbCrLf & " '' AS TAX_AMOUNT,"
				//strSQL &= vbCrLf & " ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," & CStr(nBaseCurrPK) & ",SYSDATE),4) AS TOTAL_AMOUNT,"
				//strSQL &= vbCrLf & " NVL(CORP.VAT_PERCENTAGE,0) AS TAX_PERCENT,"
				//'strSQL &= vbCrLf & " ROUND((NVL(CORP.VAT_PERCENTAGE,0) * ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK,466,SYSDATE),4)/100),2) AS TAX_AMOUNT,"
				//strSQL &= vbCrLf & " ROUND((NVL(CORP.VAT_PERCENTAGE,0) * JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100,2) AS TAX_AMOUNT,"
				//'strSQL &= vbCrLf & " ROUND(((NVL(CORP.VAT_PERCENTAGE,0) * ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK,466,SYSDATE),4)/100) + ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK,466,SYSDATE),2)),2) AS TOTAL_AMOUNT,"
				//strSQL &= vbCrLf & " ROUND(((NVL(CORP.VAT_PERCENTAGE,0) * JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100) + (JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE),2) AS TOTAL_AMOUNT,"
				//Added by Venkata on 12/01/07

				//commenting by thiyagarajan on 28/2/09:VAT Implementation task 

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
				strSQL += " (select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,1,2) from dual) VAT_CODE,";

				strSQL += "(select FETCH_VAT ( " + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2,2) from dual) VAT_PERCENT,";

				strSQL += "((select FETCH_VAT(" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2,2) from dual)* (JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100) TAX_AMOUNT,";

				strSQL += "(((select FETCH_VAT (" + vatcode + "," + custpk + "," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2,2) from dual)* (JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100) + JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE) TOTAL_AMOUNT,";
				//end by thiyagarajan on 28/2/09


				strSQL += " '' AS REMARKS,";
				strSQL += " 'New' AS \"MODE\",";
				strSQL += " 'False' AS CHK";
				strSQL += " FROM";
				strSQL += " JOB_TRN_OTH_CHRG JOBOTH, ";
				strSQL += " FREIGHT_ELEMENT_MST_TBL FMT,";
				strSQL += " CORPORATE_MST_TBL CORP,";
				strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
				strSQL += " WHERE";
				strSQL += " JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK";
				strSQL += " AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
				strSQL += " AND JOBOTH.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
				strSQL += " AND JOBOTH.INV_AGENT_TRN_AIR_IMP_FK IS NULL";
				strSQL += " AND JOBOTH.INV_CUST_TRN_AIR_IMP_FK IS NULL";
				strSQL += " AND (JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL  OR (SELECT cit.chk_invoice FROM consol_invoice_tbl CIT,CONSOL_INVOICE_TRN_TBL CTRN WHERE CIT.CONSOL_INVOICE_PK= CTRN.CONSOL_INVOICE_FK AND CTRN.CONSOL_INVOICE_TRN_PK=JOBOTH.CONSOL_INVOICE_TRN_FK)=2) ";
				// added by jitendra
				if (AgentType == 2) {
					strSQL += "AND JOBOTH.Freight_Type=1";
				}
			} else {
				//Modified by mani
				//strSQL = " SELECT DECODE(TRN.COST_FRT_ELEMENT,1,'COST',2,'FREIGHT') AS TYPE,"
				strSQL = " SELECT DECODE(TRN.COST_FRT_ELEMENT,1,'FREIGHT',2,'FREIGHT') AS TYPE,";
				strSQL += " TRN.INV_AGENT_TRN_PK AS PK,";
				strSQL += " HDR.JOB_CARD_FK AS JOBCARD_FK,";
				strSQL += " TRN.COST_FRT_ELEMENT_FK AS ELEMENT_FK,";
				strSQL += " TRN.CURRENCY_MST_FK,";
				//Modified by Mani
				//strSQL &= vbCrLf & " (CASE TRN.COST_FRT_ELEMENT WHEN 1 THEN (SELECT COST_ELEMENT_NAME FROM COST_ELEMENT_MST_TBL C WHERE C.COST_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 2 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 3 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) END) AS ELEMENT_NAME,"
				strSQL += " (CASE TRN.COST_FRT_ELEMENT WHEN 1 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 2 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 3 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) END) AS ELEMENT_NAME,";
				strSQL += " '' AS ELEMENT_SEARCH,";
				strSQL += " CUMT.CURRENCY_ID,";
				strSQL += " '' AS CURR_SEARCH,";
				//strSQL &= vbCrLf & " ROUND(GET_EX_RATE(TRN.CURRENCY_MST_FK,HDR.CURRENCY_MST_FK,HDR.INVOICE_DATE),4) EXCHANGE_RATE,"
				strSQL += " ROUND(TRN.ELEMENT_AMT,2) AS AMOUNT,";
				strSQL += " TRN.exchange_rate AS EXCHANGE_RATE, ";
				strSQL += " ROUND(TRN.AMT_IN_INV_CURR,2) AS INV_AMOUNT,";

				strSQL += "TRN.VAT_CODE AS VAT_CODE,";
				//Venkata on 12/01/07

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
				strSQL += " HDR.INV_AGENT_PK = TRN.INV_AGENT_FK";
				strSQL += " AND TRN.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
				strSQL += " AND HDR.INV_AGENT_PK=" + Convert.ToString(nInvoicePK);
			}

			try {
				DS = objWK.GetDataSet(strSQL);
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
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

			strSQL = " SELECT ";
			strSQL += " JOB.JOBCARD_REF_NO,";
			strSQL += " JOB.JOBCARD_DATE,";
			strSQL += " AMT.AGENT_NAME,";
			strSQL += " JOB.FLIGHT_NO,";
			strSQL += " POL.PORT_MST_PK POLPK,";
			strSQL += " POL.PORT_NAME POL,";
			strSQL += " POD.PORT_MST_PK PODPK,";
			strSQL += " POD.PORT_NAME POD,";
			strSQL += " JOB.HAWB_REF_NO ";
			strSQL += " FROM ";
			strSQL += " JOB_CARD_TRN JOB,";
			strSQL += " AGENT_MST_TBL AMT,";
			strSQL += " PORT_MST_TBL POL,";
			strSQL += " PORT_MST_TBL POD ";
			strSQL += " WHERE";
			//add by mani
			if (AgentType == 1) {
				strSQL += " JOB.CB_AGENT_MST_FK = AMT.AGENT_MST_PK";
			} else {
				strSQL += " JOB.POL_AGENT_MST_FK=AMT.AGENT_MST_PK";
			}

			strSQL += " AND job.PORT_MST_POL_FK=POL.PORT_MST_PK";
			strSQL += " AND job.PORT_MST_POD_FK=POD.PORT_MST_PK ";
			strSQL += " AND JOB.JOB_CARD_TRN_PK=" + Convert.ToString(nJobCardPK);

			try {
				DS = objWK.GetDataSet(strSQL);
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
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
			sb.Append("SELECT INV.INV_AGENT_PK ,");
			sb.Append("       INV.JOB_CARD_TRN_FK,");
			sb.Append("       INV.CB_AGENT_MST_FK,");
			sb.Append("       INV.INVOICE_REF_NO,");
			sb.Append("       TO_DATE(INV.INVOICE_DATE, DATEFORMAT) INVOICE_DATE,");
			sb.Append("       TO_DATE(INV.INVOICE_DUE_DATE, DATEFORMAT) INVOICE_DUE_DATE,");
			sb.Append("       INV.CURRENCY_MST_FK,");
			sb.Append("       INV.GROSS_INV_AMT ,");
			sb.Append("       INV.VAT_PCNT,");
			sb.Append("       INV.VAT_AMT,");
			sb.Append("       INV.DISCOUNT_AMT,");
			sb.Append("       INV.NET_INV_AMT ,");
			sb.Append("       INV.CREATED_BY_FK,");
			sb.Append("       INV.CREATED_DT,");
			sb.Append("       INV.LAST_MODIFIED_BY_FK,");
			sb.Append("       INV.BATCH_MST_FK,");
			sb.Append("       INV.CHK_INVOICE,");
			sb.Append("       INV.REMARKS,");
			sb.Append("       INV.CB_OR_LOAD_AGENT,");
			sb.Append("       INV.VERSION_NO,");
			sb.Append("       INV.INV_UNIQUE_REF_NR,");
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
			//strSQL = "SELECT * FROM INV_AGENT_TBL I WHERE I.INV_AGENT_PK=" & CStr(nInvPK)

			try {
				DS = objWK.GetDataSet(sb.ToString());
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
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
			try {
				Strsql = " select ";
				Strsql += " nvl(sum(c.credit_note_amt), 0)";
				Strsql += " from cr_agent_air_imp_tbl c";
				Strsql += " where c.inv_agent_air_imp_fk = " + JOBPK;
				CreditAmt = Convert.ToDouble(ObjWF.ExecuteScaler(Strsql));
				return CreditAmt;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}

        /// <summary>
        /// Gets the agntpk.
        /// </summary>
        /// <param name="JobCard">The job card.</param>
        /// <returns></returns>
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
			strSQL += " AND JA.JOB_CARD_TRN_PK=HA.JOB_CARD_AIR_EXP_FK (+)";
			strSQL += " AND CUST_CUSTOMER_MST_FK = CUSTOMER_MST_PK (+)";
			strSQL += " AND JA.CB_AGENT_MST_FK = AG.AGENT_MST_PK (+)";
			strSQL += " AND PORT_MST_POL_FK = POL.PORT_MST_PK";
			strSQL += " AND PORT_MST_POD_FK = POD.PORT_MST_PK";
			strSQL += " AND JOB_CARD_STATUS = 1";
			strSQL += " AND UPPER(JA.JOBCARD_REF_NO) LIKE '" + JobCard + "'";
			strSQL += " ORDER BY JA.JOBCARD_REF_NO";
			try {
				pk = Convert.ToInt64(objWK.ExecuteScaler(strSQL));
				return pk;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
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
			WorkFlow objWK = new WorkFlow();
			DataSet DS = null;
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT INV.INV_AGENT_TRN_PK ,");
			sb.Append("       INV.INV_AGENT_FK ,");
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
			sb.Append("       INV.VAT_CODE");
			sb.Append("  FROM INV_AGENT_TRN_TBL INV");
			sb.Append(" WHERE INV.INV_AGENT_FK =" + nInvPK);
			//strSQL = "SELECT * FROM INV_AGENT_TRN_TBL I WHERE I.INV_AGENT_AIR_IMP_FK=" & CStr(nInvPK)

			try {
				DS = objWK.GetDataSet(sb.ToString());
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
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
        /// <summary>
        /// Gets the corp currency.
        /// </summary>
        /// <returns></returns>
        public DataSet GetCorpCurrency()
		{
			string strSQL = null;
			strSQL = "SELECT CMT.CURRENCY_MST_FK,CUMT.CURRENCY_ID FROM CORPORATE_MST_TBL CMT,CURRENCY_TYPE_MST_TBL CUMT";
			strSQL += "WHERE CMT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
			try {
				DataSet DS = null;
				DS = (new WorkFlow()).GetDataSet(strSQL);
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}

        #endregion
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
			try {
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
				if (intIns > 0) {
					TRAN.Commit();
					//arrMessage.Add("Protocol Generated Succesfully")
					return arrMessage;
				}
			} catch (OracleException OraEx) {
				TRAN.Rollback();
				arrMessage.Add(OraEx.Message);
				return arrMessage;
			} finally {
				objWK.CloseConnection();
			}
            return new ArrayList();
		}
        #endregion
        #region "To Update LoadAgent Status"
        /// <summary>
        /// Updates the loadagentstatus.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <returns></returns>
        public ArrayList UpdateLoadagentstatus(int JobPk = 0)
		{


			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			OracleCommand updCmdUser = new OracleCommand();
			string str = null;
			Int16 intIns = default(Int16);
			try {
				updCmdUser.Transaction = TRAN;

				str = "UPDATE JOB_CARD_TRN  j SET ";
				str += "   j.LOADAGENT_STATUS = 0 ";
				str += " WHERE j.JOB_CARD_TRN_PK=" + JobPk;

				var _with2 = updCmdUser;
				_with2.Connection = objWK.MyConnection;
				_with2.Transaction = TRAN;
				_with2.CommandType = CommandType.Text;
				_with2.CommandText = str;
				intIns = Convert.ToInt16(_with2.ExecuteNonQuery());
				if (intIns > 0) {
					TRAN.Commit();
					//arrMessage.Add("Protocol Generated Succesfully")
					return arrMessage;
				}
			} catch (OracleException OraEx) {
				TRAN.Rollback();
				arrMessage.Add(OraEx.Message);
				return arrMessage;
			} finally {
				objWK.CloseConnection();
			}
            return new ArrayList();
        }
        #endregion

        #region "Enhance Search"
        /// <summary>
        /// Fetches the invoice agent jc no.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchInvoiceAgentJCNo(string strCond)
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


            try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_ACTIVE_JOB_REF_FOR_INV";

				var _with9 = selectCommand.Parameters;
				_with9.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
				_with9.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with9.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
				_with9.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}
        /// <summary>
        /// Fetches the customer for job card.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
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
			//Dim strLoc As String = ""
			arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
				strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
				strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
				strJobPK = Convert.ToString(arr.GetValue(3));
            //If arr.Length > 4 Then strLoc = arr(4)

            try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_JOBCARD_IMP";

				var _with10 = selectCommand.Parameters;
				_with10.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
				_with10.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				//.Add("LOCATION_IN", IIf(strLoc <> "", strLoc, "")).Direction = ParameterDirection.Input
				_with10.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
				_with10.Add("JOB_CARD_PK_IN", strJobPK).Direction = ParameterDirection.Input;
				_with10.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}
        /// <summary>
        /// Fetches the voyage for job card.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
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

            try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.GET_VES_VOY_JOBCARD";

				var _with11 = selectCommand.Parameters;
				_with11.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
				_with11.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
				_with11.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with11.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
				_with11.Add("JOB_CARD_PK_IN", strJobPK).Direction = ParameterDirection.Input;
				_with11.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}
        /// <summary>
        /// Fetches the job card for invoice.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchJobCardForInvoice(string strCond, string loc = "")
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSERACH_IN = null;
			string strBizType = null;
			string strReq = null;
			string strLoc = null;
			strLoc = loc;
			arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));

            try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_ACTIVE_JOB_REF_FOR_INV";

				var _with12 = selectCommand.Parameters;
				_with12.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
				_with12.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with12.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
				_with12.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
				_with12.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}

        #endregion

        #region "Invoice Agent Report"
        /// <summary>
        /// Fetches the inv agent report.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <param name="AgentFlag">The agent flag.</param>
        /// <returns></returns>
        public DataSet FetchInvAgentReport(Int64 nInvPK, short AgentFlag)
		{
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			strSQL = " SELECT INVAGTIMP.INV_AGENT_PK INVPK,";
			strSQL += "INVAGTIMP.INVOICE_REF_NO       INVREFNO,";
			strSQL += "INVAGTIMP.GROSS_INV_AMT        INVAMT,";
			strSQL += "NVL(INVAGTIMP.DISCOUNT_AMT,0) DICSOUNT,";
			strSQL += "INVAGTIMP.VAT_PCNT             VATPCT,";
			strSQL += "INVAGTIMP.VAT_AMT              VATAMT,";
			strSQL += "JAI.JOB_CARD_TRN_PK        JOBPK,";
			strSQL += "JAI.JOBCARD_REF_NO             JOBREFNO,";
			strSQL += "JAI.CLEARANCE_ADDRESS CLEARANCEPOINT,";
			strSQL += "AMST.AGENT_NAME         AGENTNAME,";
			strSQL += "AMST.ACCOUNT_NO        AGENTREFNO,";
			strSQL += "ADTLS.ADM_ADDRESS_1     AGENTADD1,";
			strSQL += "ADTLS.ADM_ADDRESS_2     AGENTADD2,";
			strSQL += " ADTLS.ADM_ADDRESS_3     AGENTADD3,";
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
			strSQL += "NVL(INVTAGTIMP.AMT_IN_INV_CURR,0) FREIGHTAMT,";
			strSQL += "INVTAGTIMP.TAX_PCNT        FRETAXPCNT,";
			strSQL += "NVL(INVTAGTIMP.TAX_AMT,0) FRETAXANT,";
			strSQL += "JAI.ETD_DATE ETD,";
			strSQL += "JAI.ETA_DATE ETA ,";
			strSQL += "1 CARGO_TYPE,";
			strSQL += "CURRMST.CURRENCY_ID CURRID,";
			strSQL += "CURRMST.CURRENCY_NAME CURRNAME,";
			strSQL += "JAI.FLIGHT_NO VES_FLIGHT,";
			strSQL += "JAI.PYMT_TYPE PYMT,";
			strSQL += "JAI.GOODS_DESCRIPTION GOODS,";
			strSQL += "JAI.MARKS_NUMBERS MARKS,";
			strSQL += "NVL(JAI.INSURANCE_AMT, 0) INSURANCE,";
			strSQL += "STMST.INCO_CODE TERMS,";
			strSQL += "AMST.VAT_NO AGTVATNO,";
			strSQL += "'' COLPLACE,";
			strSQL += "DELMST.PLACE_NAME DELPLACE,";
			strSQL += "POLMST.PORT_NAME POL,";
			strSQL += "PODMST.PORT_NAME POD,";
			strSQL += "JAI.HAWB_REF_NO HAWBREFNO,";
			strSQL += "JAI.MAWB_REF_NO MAWBREFNO,";
			strSQL += "CGMST.COMMODITY_GROUP_DESC COMMODITY,";
			strSQL += "SUM(JAIC.VOLUME_IN_CBM) VOLUME,";
			strSQL += "SUM(JAIC.GROSS_WEIGHT) GROSS,";
			strSQL += " sum(jaic.net_weight)  NETWT,";
			strSQL += "SUM(JAIC.CHARGEABLE_WEIGHT) CHARWT";
			strSQL += "FROM INV_AGENT_TBL INVAGTIMP,";
			strSQL += "CURRENCY_TYPE_MST_TBL CURRMST,";
			strSQL += "INV_AGENT_TRN_TBL INVTAGTIMP,";
			strSQL += "FREIGHT_ELEMENT_MST_TBL   FEMST,";
			strSQL += "JOB_CARD_TRN    JAI,";
			strSQL += "JOB_TRN_CONT    JAIC,";
			strSQL += "SHIPPING_TERMS_MST_TBL  STMST,";
			strSQL += " PLACE_MST_TBL           DELMST,";
			strSQL += " PORT_MST_TBL            POLMST,";
			strSQL += "PORT_MST_TBL            PODMST,";
			strSQL += "COMMODITY_GROUP_MST_TBL CGMST,";
			strSQL += "AGENT_MST_TBL      AMST,";
			strSQL += "AGENT_CONTACT_DTLS ADTLS,";
			strSQL += "COUNTRY_MST_TBL    AGTCOUNTRY,";
			strSQL += "CUSTOMER_MST_TBL      SHIPMST,";
			strSQL += "CUSTOMER_CONTACT_DTLS SHIPDTLS,";
			strSQL += "COUNTRY_MST_TBL SHIPCOUNTRY";

			strSQL += "WHERE(INVAGTIMP.JOB_CARD_TRN_FK = JAI.JOB_CARD_TRN_PK)";
			strSQL += "AND CURRMST.CURRENCY_MST_PK(+) = INVAGTIMP.CURRENCY_MST_FK";
			strSQL += "AND INVTAGTIMP.INV_AGENT_FK(+) = INVAGTIMP.INV_AGENT_PK";
			strSQL += "AND FEMST.FREIGHT_ELEMENT_MST_PK(+) = INVTAGTIMP.COST_FRT_ELEMENT_FK";
			strSQL += "AND JAI.JOB_CARD_TRN_PK = JAIC.JOB_CARD_TRN_FK(+)";
			strSQL += "AND STMST.SHIPPING_TERMS_MST_PK(+) = JAI.SHIPPING_TERMS_MST_FK";
			strSQL += "AND DELMST.PLACE_PK(+) = JAI.DEL_PLACE_MST_FK";
			strSQL += "AND POLMST.PORT_MST_PK = JAI.PORT_MST_POL_FK";
			strSQL += "AND PODMST.PORT_MST_PK =JAI.PORT_MST_POD_FK";
			strSQL += "AND CGMST.COMMODITY_GROUP_PK(+) = JAI.COMMODITY_GROUP_FK";
			if (AgentFlag == 1) {
				strSQL += "AND AMST.AGENT_MST_PK = JAI.cb_agent_mst_fk";
				strSQL += "AND SHIPMST.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK";
			} else {
				strSQL += "AND AMST.AGENT_MST_PK = JAI.pol_agent_mst_fk";
				strSQL += "AND SHIPMST.CUSTOMER_MST_PK(+) = JAI.SHIPPER_CUST_MST_FK";
			}
			strSQL += "AND ADTLS.AGENT_MST_FK(+) = AMST.AGENT_MST_PK";
			strSQL += "AND AGTCOUNTRY.COUNTRY_MST_PK(+) = ADTLS.ADM_COUNTRY_MST_FK";
			strSQL += "AND SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK";
			strSQL += "AND SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)";
			strSQL += "AND INVAGTIMP.INV_AGENT_PK=" + nInvPK;

			strSQL += "GROUP BY INVAGTIMP.INV_AGENT_PK,";
			strSQL += "INVAGTIMP.INVOICE_REF_NO,";
			strSQL += "INVAGTIMP.GROSS_INV_AMT,";
			strSQL += "INVAGTIMP.DISCOUNT_AMT,";
			strSQL += "INVAGTIMP.VAT_PCNT,";
			strSQL += "INVAGTIMP.VAT_AMT,";
			strSQL += "JAI.JOB_CARD_TRN_PK,";
			strSQL += "JAI.JOBCARD_REF_NO,";
			strSQL += "JAI.CLEARANCE_ADDRESS,";
			strSQL += "AMST.AGENT_NAME,";
			strSQL += "AMST.ACCOUNT_NO ,";
			strSQL += "ADTLS.ADM_ADDRESS_1,";
			strSQL += "ADTLS.ADM_ADDRESS_2,";
			strSQL += "ADTLS.ADM_ADDRESS_3,";
			strSQL += "ADTLS.ADM_CITY,";
			strSQL += "ADTLS.ADM_ZIP_CODE,";
			strSQL += " ADTLS.ADM_PHONE_NO_1,";
			strSQL += "ADTLS.ADM_FAX_NO,";
			strSQL += " ADTLS.ADM_EMAIL_ID,";
			strSQL += " AGTCOUNTRY.COUNTRY_NAME,";
			strSQL += "SHIPMST.CUSTOMER_NAME,";
			strSQL += "SHIPDTLS.ADM_ADDRESS_1,";
			strSQL += "SHIPDTLS.ADM_ADDRESS_2,";
			strSQL += "SHIPDTLS.ADM_ADDRESS_3,";
			strSQL += "SHIPDTLS.ADM_CITY,";
			strSQL += "SHIPDTLS.ADM_ZIP_CODE,";
			strSQL += "SHIPDTLS.ADM_PHONE_NO_1,";
			strSQL += "SHIPDTLS.ADM_FAX_NO,";
			strSQL += "SHIPDTLS.ADM_EMAIL_ID,";
			strSQL += "SHIPCOUNTRY.COUNTRY_NAME,";
			strSQL += "FEMST.FREIGHT_ELEMENT_NAME,";
			strSQL += "INVTAGTIMP.AMT_IN_INV_CURR,";
			strSQL += "INVTAGTIMP.TAX_PCNT,";
			strSQL += "INVTAGTIMP.TAX_AMT,";
			strSQL += " JAI.ETD_DATE ,";
			strSQL += "JAI.ETA_DATE,";
			strSQL += "CURRMST.CURRENCY_ID,";
			strSQL += "CURRMST.CURRENCY_NAME,";
			strSQL += "JAI.FLIGHT_NO,";
			strSQL += " JAI.PYMT_TYPE,";
			strSQL += "JAI.GOODS_DESCRIPTION,";
			strSQL += "JAI.MARKS_NUMBERS,";
			strSQL += "JAI.INSURANCE_AMT,";
			strSQL += "STMST.INCO_CODE,";
			strSQL += "AMST.VAT_NO,";
			strSQL += "DELMST.PLACE_NAME,";
			strSQL += "POLMST.PORT_NAME,";
			strSQL += "PODMST.PORT_NAME,";
			strSQL += "JAI.HAWB_REF_NO,";
			strSQL += " JAI.MAWB_REF_NO,";
			strSQL += "CGMST.COMMODITY_GROUP_DESC";


			try {
				return (objWK.GetDataSet(strSQL));

			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}

		}
        /// <summary>
        /// Fetches the container details.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetails(Int64 nInvPK)
		{
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			strSQL = "SELECT JTSIC.PALETTE_SIZE  CONTAINER";
			strSQL += "FROM INV_AGENT_TBL IASI,";
			strSQL += "JOB_CARD_TRN  JAE,";
			strSQL += "JOB_TRN_CONT JTSIC";
			strSQL += "WHERE IASI.JOB_CARD_TRN_FK = JAE.JOB_CARD_TRN_PK";
			strSQL += "AND JTSIC.JOB_CARD_TRN_FK = JAE.JOB_CARD_TRN_PK";
			strSQL += "AND IASI.INV_AGENT_PK = " + nInvPK;
			try {
				return (objWK.GetDataSet(strSQL));
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        /// <summary>
        /// Ins the v_ detai l_ print.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
        /// Added  by gangadhar for Detail Report Pts Id: Sep-012

        #region "Invoice Detail Report"
        public object INV_DETAIL_PRINT(Int64 nInvPK)
		{
			WorkFlow objWK = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT INVAGTIMP.INV_AGENT_PK,");
			sb.Append("       INVAGTIMP.INVOICE_REF_NO,");
			sb.Append("       INVAGTIMP.INVOICE_DATE,");
			sb.Append("       INVAGTIMP.INVOICE_DUE_DATE,");
			sb.Append("       CMST.CURRENCY_ID,");
			sb.Append("       sum(INVTAGTIMP.ELEMENT_AMT * INVTAGTIMP.EXCHANGE_RATE) TOTAMT,");
			sb.Append("       sum(NVL(INVTAGTIMP.TAX_AMT,0)) TAX_AMT,");
			sb.Append("       SUM(DISTINCT(NVL(INVAGTIMP.DISCOUNT_AMT, 0))) DICSOUNT,");
			sb.Append("       SUM(DISTINCT(NVL(INVAGTIMP.NET_INV_AMT, 0))) NET_INV_AMT,");
			sb.Append("       INVTAGTIMP.REMARKS");
			sb.Append("  FROM INV_AGENT_TBL     INVAGTIMP,");
			sb.Append("       CURRENCY_TYPE_MST_TBL     CMST,");
			sb.Append("       INV_AGENT_TRN_TBL INVTAGTIMP");
			sb.Append(" WHERE CMST.CURRENCY_MST_PK = INVAGTIMP.CURRENCY_MST_FK");
			sb.Append("   AND INVTAGTIMP.INV_AGENT_FK = INVAGTIMP.INV_AGENT_PK");
			sb.Append("   AND INVAGTIMP.INV_AGENT_PK =" + nInvPK + "");
			sb.Append(" GROUP BY INVAGTIMP.INV_AGENT_PK,");
			sb.Append("          INVAGTIMP.INVOICE_REF_NO,");
			sb.Append("          CMST.CURRENCY_ID,");
			sb.Append("          INVTAGTIMP.REMARKS,");
			sb.Append("          INVAGTIMP.INVOICE_DATE,");
			sb.Append("          INVAGTIMP.INVOICE_DUE_DATE");
			try {
				return (objWK.GetDataSet(sb.ToString()));
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion
        #region " Invoice CONSOL_INV_DETAIL_MAIN_PRINT Report"
        /// <summary>
        /// Consoes the l_ in v_ detai l_ mai n_ print.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <returns></returns>
        public DataSet CONSOL_INV_DETAIL_MAIN_PRINT(Int64 nInvPK, string UserName)
		{
			WorkFlow objWK = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("select DISTINCT INVET.INV_AGENT_PK CONSOL_INVOICE_PK,");
			sb.Append("                JSET.JOBCARD_REF_NO,");
			sb.Append("                AMT.AGENT_NAME  CUSTOMER_NAME,");
			sb.Append("                PMTL.PORT_NAME POL,");
			sb.Append("                PMTD.PORT_NAME POD,");
			sb.Append("                JSET.HBL_HAWB_REF_NO HBL_REF_NO,");
			sb.Append("                JSET.MBL_MAWB_REF_NO MBL_REF_NO,");
			sb.Append("              SUM(INTVET.AMT_IN_INV_CURR) TOT,");
			sb.Append("                (CASE");
			sb.Append("                  WHEN JSET.ARRIVAL_DATE IS NULL THEN");
			sb.Append("                   JSET.ETA_DATE");
			sb.Append("                  ELSE");
			sb.Append("                   JSET.ARRIVAL_DATE");
			sb.Append("                END) ARR_DEP_DATE,");
			sb.Append("                (CASE");
			sb.Append("                  WHEN JSET.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
			sb.Append("                 JSET.VOYAGE_FLIGHT_NO");
			sb.Append("                  ELSE");
			sb.Append("                  ' '");
			sb.Append("                END) VSL_VOY,");
			sb.Append("                OMT.AIRLINE_NAME OPERATOR_NAME,");
			sb.Append("                PMTL.PORT_NAME PLR,");
			sb.Append("                PMTD.PORT_NAME PFD,");
			sb.Append("           '" + UserName + "' USER_NAME");
			sb.Append("  from INV_AGENT_TBL     INVET,");
			sb.Append("       JOB_CARD_TRN      JSET,");
			sb.Append("       AIRLINE_MST_TBL           OMT,");
			sb.Append("       INV_AGENT_TRN_TBL INTVET,");
			sb.Append("       AGENT_MST_TBL               AMT,");
			sb.Append("       PORT_MST_TBL              PMTL,");
			sb.Append("       PORT_MST_TBL              PMTD  ");
			sb.Append(" WHERE INVET.JOB_CARD_FK = JSET.JOB_CARD_TRN_PK");
			sb.Append("   AND INTVET.INV_AGENT_FK = INVET.INV_AGENT_PK");
			sb.Append("    AND OMT.AIRLINE_MST_PK = JSET.CARRIER_MST_FK");
			sb.Append("     AND INVET.AGENT_MST_FK =  AMT.AGENT_MST_PK ");
			sb.Append("   AND JSET.PORT_MST_POL_FK = PMTL.PORT_MST_PK(+)");
			sb.Append("   AND JSET.PORT_MST_POD_FK = PMTD.PORT_MST_PK(+)");
			sb.Append("   AND INVET.INV_AGENT_PK =" + nInvPK + "");
			sb.Append(" GROUP BY INV_AGENT_PK,");
			sb.Append("          JOBCARD_REF_NO,");
			sb.Append("          AMT.AGENT_NAME, ");
			sb.Append("          PMTL.PORT_NAME,");
			sb.Append("          PMTD.PORT_NAME,");
			sb.Append("          JSET.HBL_HAWB_REF_NO,");
			sb.Append("          JSET.MBL_MAWB_REF_NO,");
			sb.Append("          JSET.ARRIVAL_DATE,");
			sb.Append("          JSET.ETA_DATE,");
			sb.Append("          JSET.VOYAGE_FLIGHT_NO,");
			sb.Append("          OMT.AIRLINE_NAME");
			try {
				return (objWK.GetDataSet(sb.ToString()));
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region " Invoice CONSOL_INV_DETAIL_SUB_MAIN_PRINT Report"
        /// <summary>
        /// Consoes the l_ in v_ detai l_ su b_ mai n_ print.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
        public DataSet CONSOL_INV_DETAIL_SUB_MAIN_PRINT(Int64 nInvPK)
		{
			WorkFlow objWK = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT INVET.INV_AGENT_PK CONSOL_INVOICE_FK,");
			sb.Append("              FEMT.FREIGHT_ELEMENT_NAME FRT_DESC,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               SUM(DISTINCT INTVET.ELEMENT_AMT) ELEMENT_AMT,");
			sb.Append("               INTVET.EXCHANGE_RATE,");
			sb.Append("               SUM(DISTINCT(INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE)) TOTAMT,");
			sb.Append("               JSET.JOBCARD_REF_NO FREIGHT_ELEMENT_ID,");
			sb.Append("               INTVET.TAX_AMT,");
			sb.Append("                INTVET.TAX_PCNT,");
			sb.Append("              (SUM(DISTINCT(INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE))+INTVET.TAX_AMT) INVOICE");
			sb.Append("         FROM INV_AGENT_TRN_TBL  INTVET,");
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
			//sb.Append("           AND INTVET.COST_FRT_ELEMENT=2")
			sb.Append("           AND INVET.INV_AGENT_PK =" + nInvPK + "");
			sb.Append("           GROUP BY INVET.INV_AGENT_PK,");
			sb.Append("                 CTMT.CURRENCY_ID,");
			sb.Append("                  INTVET.EXCHANGE_RATE,");
			sb.Append("                 FEMT.FREIGHT_ELEMENT_NAME,");
			sb.Append("                 JSET.JOBCARD_REF_NO,");
			sb.Append("                   INTVET.TAX_PCNT,");
			sb.Append("                   INTVET.TAX_AMT");
			sb.Append("          UNION  ");
			sb.Append("                    SELECT INVET.INV_AGENT_PK CONSOL_INVOICE_FK,");
			sb.Append("                FEMT.FREIGHT_ELEMENT_NAME FRT_DESC,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               SUM(DISTINCT INTVET.ELEMENT_AMT) ELEMENT_AMT,");
			sb.Append("               INTVET.EXCHANGE_RATE,");
			sb.Append("               SUM((INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE)) TOTAMT,");
			sb.Append("               JSET.JOBCARD_REF_NO FREIGHT_ELEMENT_ID,");
			sb.Append("               INTVET.TAX_AMT,");
			sb.Append("                INTVET.TAX_PCNT,");
			sb.Append("              (SUM((INTVET.ELEMENT_AMT * INTVET.EXCHANGE_RATE))+INTVET.TAX_AMT) INVOICE");
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
			sb.Append("           AND JSET.JOB_CARD_TRN_PK = JCOTH.JOB_CARD_TRN_FK");
			//sb.Append("           AND INTVET.COST_FRT_ELEMENT = 3")
			sb.Append("           AND INVET.INV_AGENT_PK =" + nInvPK + "");
			sb.Append("          GROUP BY INVET.INV_AGENT_PK,");
			sb.Append("                  CTMT.CURRENCY_ID,");
			sb.Append("                  INTVET.EXCHANGE_RATE,");
			sb.Append("                  FEMT.FREIGHT_ELEMENT_NAME,");
			sb.Append("                  JSET.JOBCARD_REF_NO,");
			sb.Append("                   INTVET.TAX_AMT,");
			sb.Append("                    INTVET.TAX_PCNT");
			try {
				return (objWK.GetDataSet(sb.ToString()));
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region " Invoice CONSOL_INV_CUST_PRINT Report"
        /// <summary>
        /// Consoes the l_ in v_ cus t_ print.
        /// </summary>
        /// <param name="nInvPK">The n inv pk.</param>
        /// <returns></returns>
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
			try {
				return (objWK.GetDataSet(sb.ToString()));
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "Get Other Charge Details"
        /// <summary>
        /// Fills the job card other charges.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet FillJobCardOtherCharges(string pk = "0")
		{
			System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			strSQL.Append("         SELECT");
			strSQL.Append("         oth_chrg.JOB_TRN_OTH_PK,");
			strSQL.Append("         frt.freight_element_mst_pk,");
			strSQL.Append("         frt.freight_element_id,");
			strSQL.Append("         frt.freight_element_name,");
			strSQL.Append("         curr.currency_mst_pk,");
			//strSQL.Append(vbCrLf & "         DECODE(oth_chrg.freight_type,1,'Prepaid',2,'Collect') PaymentType, ")
			strSQL.Append("          oth_chrg.freight_type PaymentType, ");
			strSQL.Append("   oth_chrg.location_mst_fk  \"location_fk\" ,");
			strSQL.Append("   loc.location_id \"location_id\" ,");
			strSQL.Append("   oth_chrg.frtpayer_cust_mst_fk \"frtpayer_mst_fk\" ,");
			strSQL.Append("   cus.customer_id \"frtpayer\",");
			strSQL.Append("         oth_chrg.EXCHANGE_RATE \"ROE\",");
			strSQL.Append("         oth_chrg.amount amount,oth_chrg.inv_agent_trn_air_imp_fk,");
			strSQL.Append("         'false' \"Delete\"");
			strSQL.Append("FROM");
			strSQL.Append("         JOB_TRN_OTH_CHRG oth_chrg,");
			strSQL.Append("         JOB_CARD_TRN jobcard_mst,");
			strSQL.Append("         freight_element_mst_tbl frt,");
			strSQL.Append("         currency_type_mst_tbl curr,");
			strSQL.Append("   location_mst_tbl loc,");
			strSQL.Append("   customer_mst_tbl cus");
			strSQL.Append("WHERE");
			strSQL.Append("         oth_chrg.JOB_CARD_TRN_FK = jobcard_mst.JOB_CARD_TRN_PK");
			strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
			strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
			strSQL.Append("         AND oth_chrg.JOB_CARD_TRN_FK    = " + pk);
			strSQL.Append("   AND oth_chrg.frtpayer_cust_mst_fk = cus.customer_mst_pk(+) ");
			strSQL.Append("   AND oth_chrg.location_mst_fk = loc.location_mst_pk(+) ");
			strSQL.Append("ORDER BY freight_element_id ");
			try {
				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Consignee PK"
        /// <summary>
        /// Gets the shipper pk.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public int GetShipperPK(string JOBPK)
		{
			System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			strSQL.Append("   SELECT");
			strSQL.Append("   jobcard_mst.shipper_cust_mst_fk");
			strSQL.Append("  FROM");
			strSQL.Append("   JOB_CARD_TRN jobcard_mst");
			strSQL.Append("   WHERE jobcard_mst.JOB_CARD_TRN_PK = " + JOBPK);
			try {
				return Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

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
			strSQL += " 'True' AS CHK, HDR.CB_OR_LOAD_AGENT";
			strSQL += " FROM";
			strSQL += " INV_AGENT_TRN_TBL TRN,";
			strSQL += " INV_AGENT_TBL HDR,";
			strSQL += " CURRENCY_TYPE_MST_TBL CUMT";
			strSQL += " WHERE";
			strSQL += " TRN.INV_AGENT_FK = HDR.INV_AGENT_PK";
			strSQL += " AND TRN.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
			strSQL += " AND HDR.JOB_CARD_TRN_FK = " + JOBPK;
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion
	}
}
