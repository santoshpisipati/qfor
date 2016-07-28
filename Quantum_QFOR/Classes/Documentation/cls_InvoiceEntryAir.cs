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
using System.Data;
namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsInvoiceEntryAir : CommonFeatures
	{

        #region " Fetch"
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
			strSQL += "JOBCOST.JOB_TRN_PIA_PK,";
			strSQL += "JOBCOST.JOB_CARD_TRN_FK,";
			strSQL += "JOBCOST.COST_ELEMENT_MST_FK,";
			strSQL += "JOBCOST.CURRENCY_MST_FK,";
			strSQL += "CEMT.COST_ELEMENT_NAME,";
			strSQL += "CUMT.CURRENCY_ID,";
			strSQL += "JOBCOST.ESTIMATED_AMT,";
			strSQL += "JOBCOST.INVOICE_AMT,";
			strSQL += "(CASE WHEN (JOBCOST.INV_CUST_TRN_FK IS NULL AND JOBCOST.INV_AGENT_TRN_FK IS NULL) THEN NULL WHEN JOBCOST.INV_CUST_TRN_FK IS NOT NULL THEN (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_EXP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_CUST_TRN_AIR_EXP_PK=JOBCOST.INV_CUST_TRN_FK) ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_AGENT_TRN_PK=JOBCOST.INV_AGENT_TRN_FK) END) INV_AMT,";
			strSQL += "(CASE WHEN (JOBCOST.INV_CUST_TRN_FK IS NULL AND JOBCOST.INV_AGENT_TRN_FK IS NULL) THEN 'False' ELSE 'True' END) CHK";
			strSQL += "FROM JOB_TRN_PIA JOBCOST,";
			strSQL += "COST_ELEMENT_MST_TBL CEMT,";
			strSQL += "CURRENCY_TYPE_MST_TBL CUMT";
			strSQL += "WHERE ";
			strSQL += "JOBCOST.COST_ELEMENT_MST_FK = CEMT.COST_ELEMENT_MST_PK";
			strSQL += "AND JOBCOST.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
			strSQL += "AND JOBCOST.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);

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
        /// <returns></returns>
        public DataSet FetchFreightDetails(long nJobCardPK)
		{
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			DataSet DS = null;

			strSQL = " SELECT ";
			strSQL += "JOBFRT.JOB_TRN_FD_PK,";
			strSQL += "JOBFRT.JOB_CARD_TRN_FK,";
			strSQL += "JOBFRT.FREIGHT_ELEMENT_MST_FK,";
			strSQL += "JOBFRT.CURRENCY_MST_FK,";
			strSQL += "FMT.FREIGHT_ELEMENT_NAME,";
			strSQL += "DECODE(JOBFRT.FREIGHT_TYPE,1,'P',2,'C') AS PC,";
			strSQL += "JOBFRT.FREIGHT_AMT,";
			strSQL += "CUMT.CURRENCY_ID,";
			strSQL += "ROUND((CASE WHEN (JOBFRT.INV_CUST_TRN_FK IS NULL AND JOBFRT.INV_AGENT_TRN_FK IS NULL AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL) THEN NULL WHEN JOBFRT.INV_CUST_TRN_FK IS NOT NULL THEN (SELECT (TRNCUST.TOT_AMT / DECODE(trnCUST.exchange_rate,NULL,1,0,1,trnCUST.exchange_rate)) FROM INV_CUST_TRN_AIR_EXP_TBL TRNCUST WHERE (TRNCUST.COST_FRT_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK) AND TRNCUST.COST_FRT_ELEMENT = 2 AND TRNCUST.INV_CUST_TRN_AIR_EXP_PK = JOBFRT.INV_CUST_TRN_FK) WHEN JOBFRT.INV_AGENT_TRN_FK IS NOT NULL THEN (SELECT (TRN.TOT_AMT / DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) FROM INV_AGENT_TRN_TBL TRN WHERE (TRN.COST_FRT_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK) AND TRN.COST_FRT_ELEMENT = 2 AND TRN.INV_AGENT_TRN_AIR_EXP_PK = JOBFRT.INV_AGENT_TRN_FK) WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN(SELECT SUM(CTRN.TOT_AMT / DECODE(Ctrn.exchange_rate,NULL,1,0,1,Ctrn.exchange_rate)) FROM CONSOL_INVOICE_TRN_TBL CTRN WHERE (CTRN.FRT_OTH_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK) AND CTRN.FRT_OTH_ELEMENT = 1  AND (CTRN.JOB_CARD_FK = JOBFRT.JOB_CARD_TRN_FK)  ) END),2) AS INV_AMT,";
			strSQL += "(CASE WHEN (JOBFRT.INV_CUST_TRN_FK IS NULL AND JOBFRT.INV_AGENT_TRN_FK IS NULL AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL) THEN 'False' ELSE 'True' END) CHK";

			strSQL += "FROM ";
			strSQL += "JOB_CARD_TRN JOBFRT,";
			strSQL += "FREIGHT_ELEMENT_MST_TBL FMT,";
			strSQL += "CURRENCY_TYPE_MST_TBL CUMT";
			strSQL += "WHERE";
			strSQL += "JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
			strSQL += "AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
			strSQL += "AND JOBFRT.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
			strSQL += "UNION";
			strSQL += " SELECT ";
			strSQL += "JOBOTH.JOB_TRN_OTH_PK,";
			strSQL += "JOBOTH.JOB_CARD_TRN_FK,";
			strSQL += "JOBOTH.FREIGHT_ELEMENT_MST_FK,";
			strSQL += "JOBOTH.CURRENCY_MST_FK,";
			strSQL += "FMT.FREIGHT_ELEMENT_NAME,";
			strSQL += "'P' AS PC,";
			strSQL += "JOBOTH.AMOUNT,";
			strSQL += "CUMT.CURRENCY_ID,";
			strSQL += "ROUND((CASE WHEN (JOBOTH.INV_CUST_TRN_FK IS NULL AND JOBOTH.INV_AGENT_TRN_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL) THEN NULL WHEN JOBOTH.INV_CUST_TRN_FK IS NOT NULL THEN (SELECT (TRN.TOT_AMT / DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) FROM INV_CUST_TRN_AIR_EXP_TBL TRN WHERE (TRN.COST_FRT_ELEMENT_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK) And TRN.COST_FRT_ELEMENT = 3 And (TRN.INV_CUST_TRN_AIR_EXP_PK = JOBOTH.INV_CUST_TRN_FK)) WHEN JOBOTH.INV_AGENT_TRN_FK IS NOT NULL THEN (SELECT (TRN.TOT_AMT / DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) FROM INV_AGENT_TRN_TBL TRN WHERE (TRN.COST_FRT_ELEMENT_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK) AND TRN.COST_FRT_ELEMENT = 3 AND (TRN.INV_AGENT_TRN_PK = JOBOTH.INV_AGENT_TRN_FK)) WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN (SELECT SUM(CTRN.TOT_AMT / DECODE(Ctrn.exchange_rate,NULL,1,0,1,Ctrn.exchange_rate)) FROM CONSOL_INVOICE_TRN_TBL CTRN WHERE (CTRN.FRT_OTH_ELEMENT_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK) AND CTRN.FRT_OTH_ELEMENT = 2 AND (CTRN.JOB_CARD_FK = JOBOTH.JOB_CARD_TRN_FK) )  END),2) AS INV_AMT,";
			strSQL += "(CASE WHEN (JOBOTH.INV_CUST_TRN_FK IS NULL AND JOBOTH.INV_AGENT_TRN_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL) THEN 'False' ELSE 'True' END) CHK";

			strSQL += "FROM ";
			strSQL += "JOB_TRN_OTH_CHRG JOBOTH,";
			strSQL += "FREIGHT_ELEMENT_MST_TBL FMT,";
			strSQL += "CURRENCY_TYPE_MST_TBL CUMT";
			strSQL += "WHERE";
			strSQL += "JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
			strSQL += "AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
			strSQL += "AND JOBOTH.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);


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
        //Union of 3 queries:
        // For new invoice:
        //       1.Select all job card cost elements of the selected job card which have not yet been invoiced
        //       2.Select all job card freight elements of the selected job card which have not yet been invoiced and payment type is Prepaid
        //       3.Select all job card - other charges elements of the selected job card which have not yet been invoiced
        // For existing invoice:
        //       1.Select all invoice elements from the invoice transaction table
        //       
        //Used to populate the invoice elements grid in the invoice page
        /// <summary>
        /// Fetches the invoice details.
        /// </summary>
        /// <param name="nJobCardPK">The n job card pk.</param>
        /// <param name="nBaseCurrPK">The n base curr pk.</param>
        /// <param name="nInvoicePK">The n invoice pk.</param>
        /// <param name="UserPk">The user pk.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceDetails(long nJobCardPK, long nBaseCurrPK, long nInvoicePK = 0, string UserPk = "0")
		{
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			DataSet DS = null;

			if (nInvoicePK == 0) {
				strSQL += " SELECT 'FREIGHT' AS TYPE,";
				strSQL += "JOBFRT.JOB_TRN_FD_PK AS PK,";
				strSQL += "JOBFRT.JOB_CARD_TRN_FK AS JOBCARD_FK,";
				strSQL += "JOBFRT.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,";
				strSQL += "JOBFRT.CURRENCY_MST_FK,";
				strSQL += "FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,";
				strSQL += "'' AS ELEMENT_SEARCH,";
				strSQL += "CUMT.CURRENCY_ID,";
				strSQL += "'' AS CURR_SEARCH,";
				strSQL += "JOBFRT.FREIGHT_AMT AS AMOUNT,";
				strSQL += "JOBFRT.EXCHANGE_RATE,";
				strSQL += "JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE AS INV_AMOUNT,";
				strSQL += "NVL((select Distinct(frtv.vat_code) from frt_vat_country_tbl frtv,";
				strSQL += "      user_mst_tbl umt,location_mst_tbl loc";
				strSQL += "where umt.default_location_fk = loc.location_mst_pk";
				strSQL += " and loc.country_mst_fk = frtv.country_mst_fk";
				strSQL += "and umt.user_mst_pk =" + UserPk;
				strSQL += "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)";
				strSQL += "),'') AS VAT_CODE,";

				strSQL += "NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,";
				strSQL += "      user_mst_tbl umt,location_mst_tbl loc";
				strSQL += "where umt.default_location_fk = loc.location_mst_pk";
				strSQL += " and loc.country_mst_fk = frtv.country_mst_fk";
				strSQL += "and umt.user_mst_pk =" + UserPk;
				strSQL += "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)";
				strSQL += "),CORP.VAT_PERCENTAGE) AS VAT_PERCENT,";

				strSQL += "(NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,";
				strSQL += "      user_mst_tbl umt,location_mst_tbl loc";
				strSQL += "where umt.default_location_fk = loc.location_mst_pk";
				strSQL += " and loc.country_mst_fk = frtv.country_mst_fk";
				strSQL += "and umt.user_mst_pk =" + UserPk;
				strSQL += "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)";
				strSQL += "),CORP.VAT_PERCENTAGE)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100)  AS TAX_AMOUNT,";


				strSQL += "((NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,";
				strSQL += "      user_mst_tbl umt,location_mst_tbl loc";
				strSQL += "where umt.default_location_fk = loc.location_mst_pk";
				strSQL += " and loc.country_mst_fk = frtv.country_mst_fk";
				strSQL += "and umt.user_mst_pk =" + UserPk;
				strSQL += "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)";
				strSQL += "),CORP.VAT_PERCENTAGE)*(JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) + JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) AS TOTAL_AMOUNT,";
				//end
				strSQL += "'' AS REMARKS,";
				strSQL += "'New' AS \"MODE\",";
				strSQL += "'False' AS CHK";
				strSQL += "FROM ";
				strSQL += "JOB_TRN_FD JOBFRT,";
				strSQL += "FREIGHT_ELEMENT_MST_TBL FMT,";
				strSQL += "CURRENCY_TYPE_MST_TBL CUMT,";
				strSQL += "CORPORATE_MST_TBL CORP";
				strSQL += "WHERE";
				strSQL += "JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
				strSQL += "AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
				strSQL += "AND JOBFRT.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
				strSQL += "AND JOBFRT.INV_CUST_TRN_FK IS NULL";
				strSQL += "AND JOBFRT.FREIGHT_TYPE=1";
				strSQL += "AND JOBFRT.INV_AGENT_TRN_FK IS NULL";
				strSQL += "AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL";

				strSQL += "UNION";

				strSQL += " SELECT 'OTHER' AS TYPE,";
				strSQL += "JOBOTH.JOB_TRN_OTH_PK AS PK,";
				strSQL += "JOBOTH.JOB_CARD_TRN_FK AS JOBCARD_FK,";
				strSQL += "JOBOTH.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,";
				strSQL += "JOBOTH.CURRENCY_MST_FK,";
				strSQL += "FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,";
				strSQL += "'' AS ELEMENT_SEARCH,";
				strSQL += "CUMT.CURRENCY_ID,";
				strSQL += "'' AS CURR_SEARCH,";
				strSQL += "JOBOTH.AMOUNT AS AMOUNT,";
				strSQL += "ROUND(GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," + Convert.ToString(nBaseCurrPK) + ",SYSDATE),2) AS EXCHANGE_RATE,";
				strSQL += "ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," + Convert.ToString(nBaseCurrPK) + ",SYSDATE),2) AS INV_AMOUNT,";
				strSQL += "NVL((select Distinct(frtv.vat_code) from frt_vat_country_tbl frtv,";
				strSQL += "      user_mst_tbl umt,location_mst_tbl loc";
				strSQL += "where umt.default_location_fk = loc.location_mst_pk";
				strSQL += " and loc.country_mst_fk = frtv.country_mst_fk";
				strSQL += "and umt.user_mst_pk =" + UserPk;
				strSQL += "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)";
				strSQL += "),'') AS VAT_CODE,";

				strSQL += "NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,";
				strSQL += "      user_mst_tbl umt,location_mst_tbl loc";
				strSQL += "where umt.default_location_fk = loc.location_mst_pk";
				strSQL += " and loc.country_mst_fk = frtv.country_mst_fk";
				strSQL += "and umt.user_mst_pk =" + UserPk;
				strSQL += "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)";
				strSQL += "),CORP.VAT_PERCENTAGE) AS VAT_PERCENT,";

				strSQL += "(NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,";
				strSQL += "      user_mst_tbl umt,location_mst_tbl loc";
				strSQL += "where umt.default_location_fk = loc.location_mst_pk";
				strSQL += " and loc.country_mst_fk = frtv.country_mst_fk";
				strSQL += "and umt.user_mst_pk =" + UserPk;
				strSQL += "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)";
				strSQL += "),CORP.VAT_PERCENTAGE)* ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," + Convert.ToString(nBaseCurrPK) + ",SYSDATE),4)/100) AS TAX_AMOUNT,";

				strSQL += "(NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,";
				strSQL += "      user_mst_tbl umt,location_mst_tbl loc";
				strSQL += "where umt.default_location_fk = loc.location_mst_pk";
				strSQL += " and loc.country_mst_fk = frtv.country_mst_fk";
				strSQL += "and umt.user_mst_pk =" + UserPk;
				strSQL += "and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)";
				strSQL += "),CORP.VAT_PERCENTAGE)* ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," + Convert.ToString(nBaseCurrPK) + ",SYSDATE),4)/100) + ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK," + Convert.ToString(nBaseCurrPK) + ",SYSDATE),4) AS TOTAL_AMOUNT,";
				//End by Venkata


				strSQL += "'' AS REMARKS,";
				strSQL += "'New' AS \"MODE\",";
				strSQL += "'False' AS CHK";
				strSQL += "FROM ";
				strSQL += "JOB_TRN_OTH_CHRG JOBOTH,";
				strSQL += "FREIGHT_ELEMENT_MST_TBL FMT,";
				strSQL += "CURRENCY_TYPE_MST_TBL CUMT,";
				strSQL += "CORPORATE_MST_TBL CORP";
				strSQL += "WHERE";
				strSQL += "JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK";
				strSQL += "AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
				strSQL += "AND JOBOTH.JOB_CARD_TRN_FK=" + Convert.ToString(nJobCardPK);
				strSQL += "AND JOBOTH.INV_CUST_TRN_FK IS NULL";
				strSQL += "AND JOBOTH.INV_AGENT_TRN_FK IS NULL";

				//Modified by manoharan
				strSQL += "AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL";

			} else {

				strSQL = "SELECT DECODE(TRN.COST_FRT_ELEMENT,1,'FREIGHT',2,'FREIGHT',3,'OTHER') AS TYPE,";
				strSQL += "TRN.INV_CUST_TRN_AIR_EXP_PK AS PK,";
				strSQL += "HDR.JOB_CARD_AIR_EXP_FK AS JOBCARD_FK,";
				strSQL += "TRN.COST_FRT_ELEMENT_FK AS ELEMENT_FK,";
				strSQL += "TRN.CURRENCY_MST_FK,";
				strSQL += "(CASE TRN.COST_FRT_ELEMENT WHEN 1 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 2 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 3 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) END) AS ELEMENT_NAME,";
				strSQL += "'' AS ELEMENT_SEARCH,";
				strSQL += "CUMT.CURRENCY_ID,";
				strSQL += "'' AS CURR_SEARCH,";
				strSQL += "TRN.ELEMENT_AMT AS AMOUNT,";
				strSQL += "ROUND((CASE TRN.ELEMENT_AMT WHEN 0 THEN 1 ELSE TRN.AMT_IN_INV_CURR/TRN.ELEMENT_AMT END),6) AS EXCHANGE_RATE,";
				strSQL += "TRN.EXCHANGE_RATE AS EXCHANGE_RATE,";
				strSQL += "TRN.AMT_IN_INV_CURR AS INV_AMOUNT,";
				strSQL += "TRN.VAT_CODE AS VAT_CODE,";
				strSQL += "TRN.TAX_PCNT AS VAT_PERCENT,";
				strSQL += "TRN.TAX_AMT AS TAX_AMOUNT,";
				strSQL += "TRN.TOT_AMT AS TOTAL_AMOUNT,";
				strSQL += "TRN.REMARKS,";
				strSQL += "'Edit' AS \"MODE\",";
				strSQL += "'True' AS CHK";
				strSQL += "FROM";
				strSQL += "INV_CUST_TRN_AIR_EXP_TBL TRN,";
				strSQL += "INV_CUST_AIR_EXP_TBL HDR,";
				strSQL += "CURRENCY_TYPE_MST_TBL CUMT";
				strSQL += "WHERE";
				strSQL += "TRN.INV_CUST_AIR_EXP_FK = HDR.INV_CUST_AIR_EXP_PK";
				strSQL += "AND TRN.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
				strSQL += "AND HDR.INV_CUST_AIR_EXP_PK=" + Convert.ToString(nInvoicePK);

			}

			try {
				DS = objWK.GetDataSet(strSQL);
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
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
        /// <returns></returns>
        public DataSet FetchJCDetails(long nJobCardPK)
		{
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			DataSet DS = null;

			strSQL = "SELECT ";
			strSQL += "JOB.JOBCARD_REF_NO,";
			strSQL += "JOB.JOBCARD_DATE,";
			strSQL += "BKG.BOOKING_REF_NO,";
			strSQL += "HAWB.HAWB_REF_NO,";
			strSQL += "CMT.CUSTOMER_NAME,";
			strSQL += "JOB.FLIGHT_NO,";
			strSQL += "POL.PORT_ID POL,";
			strSQL += "POD.PORT_ID POD";
			strSQL += "FROM";
			strSQL += "JOB_CARD_TRN JOB,";
			strSQL += "BOOKING_MST_TBL BKG,";
			strSQL += "HAWB_EXP_TBL HAWB,";
			strSQL += "CUSTOMER_MST_TBL CMT,";
			strSQL += "PORT_MST_TBL POL,";
			strSQL += "PORT_MST_TBL POD";
			strSQL += "WHERE";
			strSQL += "JOB.BOOKING_MST_PK = BKG.BOOKING_MST_FK";
			strSQL += "AND JOB.JOB_CARD_TRN_PK=HAWB.JOB_CARD_AIR_EXP_FK(+)";
			strSQL += "AND JOB.SHIPPER_CUST_MST_FK=CMT.CUSTOMER_MST_PK (+)";
			strSQL += "AND BKG.PORT_MST_POL_FK=POL.PORT_MST_PK";
			strSQL += "AND BKG.PORT_MST_POD_FK=POD.PORT_MST_PK";
			strSQL += "AND JOB.JOB_CARD_TRN_PK=" + Convert.ToString(nJobCardPK);

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


        /// <summary>
        /// Feches the on hire listing.
        /// </summary>
        /// <returns></returns>
        public DataSet FechOnHireListing()
		{
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			DataSet DS = null;
			strSQL = "SELECT ";
			strSQL += "'' SLNO,";
			strSQL += "'' HIRE_TRN_PK,";
			strSQL += "'' HIRE_REF_NO,";
			strSQL += "'' HIRE_DATE_REF,";
			strSQL += "'' NAME,";
			strSQL += "'' PORT_NAME,";
			strSQL += "'' DEPOT";
			strSQL += "FROM";
			strSQL += "DUAL";

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
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			DataSet DS = null;

			strSQL = "SELECT * FROM INV_CUST_AIR_EXP_TBL WHERE INV_CUST_AIR_EXP_PK=" + Convert.ToString(nInvPK);

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
				Strsql += " from CR_CUST_AIR_EXP_TBL c";
				Strsql += " where c.INV_CUST_AIR_EXP_FK = " + JOBPK;
				CreditAmt = Convert.ToDouble(ObjWF.ExecuteScaler(Strsql));
				return CreditAmt;
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
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			DataSet DS = null;

			strSQL = "SELECT * FROM INV_CUST_TRN_AIR_EXP_TBL WHERE INV_CUST_AIR_EXP_FK=" + Convert.ToString(nInvPK);

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
			try {
				Strsql = "select c.credit_limit from Customer_Mst_Tbl c where c.customer_name in('" + CustName + "')";
				CreditAmt = Convert.ToDouble(getDefault(ObjWF.ExecuteScaler(Strsql), 0));
				return CreditAmt;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Fetches the customer credit amt used.
        /// </summary>
        /// <param name="CustName">Name of the customer.</param>
        /// <returns></returns>
        public double FetchCustCreditAmtUsed(string CustName)
		{
			string Strsql = null;
			double CreditAmt = 0;
			WorkFlow ObjWF = new WorkFlow();
			try {
				Strsql = "select c.credit_limit_used from Customer_Mst_Tbl c where c.customer_name in('" + CustName + "')";
                CreditAmt = Convert.ToDouble(getDefault(ObjWF.ExecuteScaler(Strsql), 0));

                return CreditAmt;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        //Added by prakash chandra on 2/12/2008
        /// <summary>
        /// Fetchcols the customer credit amt used.
        /// </summary>
        /// <param name="CustName">Name of the customer.</param>
        /// <returns></returns>
        public double FetchcolCustCreditAmtUsed(string CustName)
		{
			string Strsql = null;
			double CreditAmt = 0;
			WorkFlow ObjWF = new WorkFlow();
			try {
				Strsql = "select c.credit_limit_used from Customer_Mst_Tbl c where c.customer_name in('" + CustName + "')";
                CreditAmt = Convert.ToDouble(getDefault(ObjWF.ExecuteScaler(Strsql), 0));
                return CreditAmt;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
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
			Int16 exe = default(Int16);
			double temp = 0;
			OracleCommand cmd = new OracleCommand();
			string strSQL = null;
			temp = CrLimitUsed + NetAmt;
			try {
				cmd.CommandType = CommandType.Text;
				cmd.Connection = TRAN.Connection;
				cmd.Transaction = TRAN;

				cmd.Parameters.Clear();
				strSQL = "update customer_mst_tbl a set a.credit_limit_used = " + temp;
				strSQL = strSQL + " where a.customer_name in ('" + Customer + "')";
				cmd.CommandText = strSQL;
				cmd.ExecuteNonQuery();

			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Fetches the curr pk.
        /// </summary>
        /// <param name="Currency">The currency.</param>
        /// <returns></returns>
        public int FetchCurrPk(string Currency)
		{
			string Strsql = null;
			int CurrPk = 0;
			WorkFlow ObjWF = new WorkFlow();
			try {
				Strsql = "select c.currency_mst_pk from currency_type_mst_tbl c where c.currency_id in ('" + Currency + "')";
				CurrPk = Convert.ToInt32(ObjWF.ExecuteScaler(Strsql));
				return CurrPk;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion


        /// <summary>
        /// Fills the container type data set.
        /// </summary>
        /// <returns></returns>
        public DataSet FillContainerTypeDataSet()
		{
			System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();

			strSQL.Append(" SELECT distinct cont.container_type_mst_pk,  ");
			strSQL.Append(" cont.container_type_mst_id ");
			//strSQL.Append(" cont.container_type_name ")
			strSQL.Append(" FROM container_type_mst_tbl cont ");
			strSQL.Append(" WHERE cont.ACTIVE_FLAG =1 ");
			strSQL.Append(" ORDER BY Cont.Preferences, cont.container_type_mst_id ");

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

        /// <summary>
        /// Fills the currency.
        /// </summary>
        /// <returns></returns>
        public DataSet FillCurrency()
		{
			System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();


			strSQL.Append(" SELECT Currency_Mst_Pk, ");
			strSQL.Append(" Currency_Id, ");
			strSQL.Append(" Currency_Name, ");
			strSQL.Append(" CREATED_BY_FK, ");
			strSQL.Append(" CREATED_DT, ");
			strSQL.Append(" LAST_MODIFIED_BY_FK, ");
			strSQL.Append(" LAST_MODIFIED_DT, ");
			strSQL.Append(" VERSION_NO ");
			strSQL.Append(" FROM CURRENCY_TYPE_MST_TBL ");
			strSQL.Append(" WHERE ACTIVE_FLAG =1 ");
			strSQL.Append(" ORDER BY Currency_Id ");

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
	}
}
