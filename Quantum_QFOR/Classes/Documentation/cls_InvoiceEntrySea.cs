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
    public class clsInvoiceEntrySea : CommonFeatures
	{

        #region " Fetch"

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
			strSQL += "(CASE WHEN (JOBCOST.INV_CUST_TRN_FK IS NULL AND JOBCOST.INV_AGENT_TRN_FK IS NULL) THEN NULL WHEN JOBCOST.INV_CUST_TRN_FK IS NOT NULL THEN (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_SEA_EXP_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_CUST_TRN_SEA_EXP_PK=JOBCOST.INV_CUST_TRN_FK) ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN WHERE TRN.COST_FRT_ELEMENT_FK=JOBCOST.COST_ELEMENT_MST_FK AND TRN.COST_FRT_ELEMENT=1 AND TRN.INV_AGENT_TRN_PK=JOBCOST.INV_AGENT_TRN_FK) END) INV_AMT,";
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
			} catch (Exception ex) {
				throw ex;
			}
		}

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
			strSQL += "ROUND((CASE WHEN (JOBFRT.INVOICE_TBL_FK IS NULL AND JOBFRT.INV_AGENT_TRN_FK IS NULL AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL) THEN NULL WHEN JOBFRT.INVOICE_TBL_FK IS NOT NULL THEN (SELECT (TRNCUST.TOT_AMT / DECODE(trnCUST.exchange_rate,NULL,1,0,1,trnCUST.exchange_rate)) FROM INV_CUST_TRN_SEA_EXP_TBL TRNCUST WHERE (TRNCUST.COST_FRT_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK) AND TRNCUST.COST_FRT_ELEMENT = 2 AND TRNCUST.INV_CUST_TRN_SEA_EXP_PK = JOBFRT.INVOICE_TBL_FK) WHEN JOBFRT.INV_AGENT_TRN_FK IS NOT NULL THEN (SELECT (TRN.TOT_AMT / DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) FROM INV_AGENT_TRN_TBL TRN WHERE (TRN.COST_FRT_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK) AND TRN.COST_FRT_ELEMENT = 2 AND TRN.INV_AGENT_TRN_PK = JOBFRT.INV_AGENT_TRN_FK) WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN (SELECT SUM(CTRN.TOT_AMT / DECODE(Ctrn.exchange_rate,NULL,1,0,1,Ctrn.exchange_rate)) FROM CONSOL_INVOICE_TRN_TBL CTRN WHERE (CTRN.FRT_OTH_ELEMENT_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK) AND CTRN.FRT_OTH_ELEMENT = 1 AND (CTRN.JOB_CARD_FK = JOBFRT.JOB_CARD_TRN_FK)) END),2) AS INV_AMT,";
			strSQL += "(CASE WHEN (JOBFRT.INVOICE_TBL_FK IS NULL AND JOBFRT.INV_AGENT_TRN_FK IS NULL AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL) THEN 'False' ELSE 'True' END) CHK";

			strSQL += "FROM ";
			strSQL += "JOB_TRN_FD JOBFRT,";
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
			strSQL += "ROUND((CASE WHEN (JOBOTH.INV_CUST_TRN_FK IS NULL AND JOBOTH.INV_AGENT_TRN_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL) THEN NULL WHEN JOBOTH.INV_CUST_TRN_FK IS NOT NULL THEN (SELECT (TRN.TOT_AMT / DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) FROM INV_CUST_TRN_SEA_EXP_TBL TRN WHERE (TRN.COST_FRT_ELEMENT_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK) And TRN.COST_FRT_ELEMENT = 3 And (TRN.INV_CUST_TRN_SEA_EXP_PK = JOBOTH.INV_CUST_TRN_FK)) WHEN JOBOTH.INV_AGENT_TRN_FK IS NOT NULL THEN (SELECT (TRN.TOT_AMT / DECODE(trn.exchange_rate,NULL,1,0,1,trn.exchange_rate)) FROM INV_AGENT_TRN_TBL TRN WHERE (TRN.COST_FRT_ELEMENT_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK) AND TRN.COST_FRT_ELEMENT = 3 AND (TRN.INV_AGENT_TRN_PK = JOBOTH.INV_AGENT_TRN_FK)) WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN (SELECT SUM(CTRN.TOT_AMT / DECODE(Ctrn.exchange_rate,NULL,1,0,1,Ctrn.exchange_rate)) FROM CONSOL_INVOICE_TRN_TBL CTRN WHERE (CTRN.FRT_OTH_ELEMENT_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK) AND CTRN.FRT_OTH_ELEMENT = 2 AND (CTRN.JOB_CARD_FK = JOBOTH.JOB_CARD_TRN_FK)) END),2) AS INV_AMT,";
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
			} catch (Exception ex) {
				throw ex;
			}
		}
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
				strSQL += "AND JOBFRT.INVOICE_TBL_FK IS NULL";
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
				strSQL += "JOBOTH.EXCHANGE_RATE,";
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
				strSQL += "AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL";

			} else {
				strSQL = "SELECT DECODE(TRN.COST_FRT_ELEMENT,1,'FREIGHT',2,'FREIGHT',3,'OTHER') AS TYPE,";
				strSQL += "TRN.INV_CUST_TRN_SEA_EXP_PK AS PK,";
				strSQL += "HDR.JOB_CARD_TRN_FK AS JOBCARD_FK,";
				strSQL += "TRN.COST_FRT_ELEMENT_FK AS ELEMENT_FK,";
				strSQL += "TRN.CURRENCY_MST_FK,";
				strSQL += "(CASE TRN.COST_FRT_ELEMENT WHEN 1 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 2 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) WHEN 3 THEN (SELECT FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL F WHERE F.FREIGHT_ELEMENT_MST_PK=TRN.COST_FRT_ELEMENT_FK) END) AS ELEMENT_NAME,";
				strSQL += "'' AS ELEMENT_SEARCH,";
				strSQL += "CUMT.CURRENCY_ID,";
				strSQL += "'' AS CURR_SEARCH,";
				strSQL += "TRN.ELEMENT_AMT AS AMOUNT,";
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
				strSQL += "INV_CUST_TRN_SEA_EXP_TBL TRN,";
				strSQL += "INV_CUST_SEA_EXP_TBL HDR,";
				strSQL += "CURRENCY_TYPE_MST_TBL CUMT";
				strSQL += "WHERE";
				strSQL += "TRN.INV_CUST_SEA_EXP_FK = HDR.INV_CUST_SEA_EXP_PK";
				strSQL += "AND TRN.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
				strSQL += "AND HDR.INV_CUST_SEA_EXP_PK=" + Convert.ToString(nInvoicePK);

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
			strSQL += "HBL.HBL_REF_NO,";
			strSQL += "CMT.CUSTOMER_NAME,";
			strSQL += "JOB.VESSEL_NAME,";
			strSQL += "JOB.VOYAGE,";
			strSQL += "JOB.VOYAGE_TRN_FK,";
			strSQL += "POL.PORT_NAME POL,";
			strSQL += "POD.PORT_NAME POD";
			strSQL += "FROM";
			strSQL += "JOB_CARD_TRN JOB,";
			strSQL += "BOOKING_MST_TBL BKG,";
			strSQL += "HBL_EXP_TBL HBL,";
			strSQL += "CUSTOMER_MST_TBL CMT,";
			strSQL += "PORT_MST_TBL POL,";
			strSQL += "PORT_MST_TBL POD";
			strSQL += "WHERE";
			strSQL += "JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK";
			strSQL += "AND JOB.JOB_CARD_TRN_PK=HBL.JOB_CARD_TRN_FK(+)";
			strSQL += "AND JOB.SHIPPER_CUST_MST_FK=CMT.CUSTOMER_MST_PK";
			strSQL += "AND BKG.PORT_MST_POL_FK=POL.PORT_MST_PK";
			strSQL += "AND BKG.PORT_MST_POD_FK=POD.PORT_MST_PK";
			strSQL += "AND JOB.JOB_CARD_TRN_PK=" + Convert.ToString(nJobCardPK);

			try {
				DS = objWK.GetDataSet(strSQL);
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
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

			strSQL = "SELECT * FROM INV_CUST_SEA_EXP_TBL WHERE INV_CUST_SEA_EXP_PK=" + Convert.ToString(nInvPK);

			try {
				DS = objWK.GetDataSet(strSQL);
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

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
				Strsql += " from CR_CUST_SEA_EXP_TBL c";
				Strsql += " where c.INV_CUST_SEA_EXP_FK = " + JOBPK;
				CreditAmt = Convert.ToDouble(ObjWF.ExecuteScaler(Strsql));
				return CreditAmt;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

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

			strSQL = "SELECT * FROM INV_CUST_TRN_SEA_EXP_TBL WHERE INV_CUST_SEA_EXP_FK=" + Convert.ToString(nInvPK);

			try {
				DS = objWK.GetDataSet(strSQL);
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
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
			try {
				DataSet DS = null;
				DS = (new WorkFlow()).GetDataSet(strSQL);
				return DS;
			} catch (OracleException Oraexp) {
				throw Oraexp;
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
			OracleCommand cmd = new OracleCommand();
			double temp = 0;
			string strSQL = null;
			temp = CrLimitUsed + NetAmt;
			try {
				cmd.CommandType = CommandType.Text;
				cmd.Connection = TRAN.Connection;
				cmd.Transaction = TRAN;

				cmd.Parameters.Clear();

				strSQL = "update customer_mst_tbl a set a.credit_limit_used =" + temp;
				strSQL = strSQL + " where a.customer_name in ('" + Customer + "')";
				cmd.CommandText = strSQL;
				cmd.ExecuteNonQuery();
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Fetches the curr pk.
        /// </summary>
        /// <param name="Currency">The currency.</param>
        /// <returns></returns>
        public double FetchCurrPk(string Currency)
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
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

	}
}
