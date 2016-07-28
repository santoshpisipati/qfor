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
    public class ClsImportFreightControl_Sea_Air : CommonFeatures
	{

        #region "Fetch On Search for Import Freight SEA"
        /// <summary>
        /// Fetches all imp sea freight.
        /// </summary>
        /// <param name="VesselPk">The vessel pk.</param>
        /// <param name="txtJobRef">The text job reference.</param>
        /// <param name="PolPK">The pol pk.</param>
        /// <param name="PodPK">The pod pk.</param>
        /// <param name="txtHbl">The text HBL.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefNr">The reference nr.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="BCurrencyPK">The b currency pk.</param>
        /// <param name="loc">The loc.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchAllImpSeaFreight(string VesselPk = "", string txtJobRef = "", int PolPK = 0, int PodPK = 0, string txtHbl = "", string RefType = "", string RefNr = "", Int32 Export = 0, int CurrentPage = 0, int TotalPage = 0,
		int BCurrencyPK = 0, int loc = 0, int BizType = 0)
		{
			Int32 Last = default(Int32);
			Int32 Start = default(Int32);
			Int32 TotalRecords = default(Int32);
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("SELECT *");
			sb.Append("  FROM (SELECT ROWNUM \"SNO\", T.*");
			sb.Append("          FROM ((SELECT QRY.JCIPK,");
			sb.Append("                        QRY.JCRNR,");
			sb.Append("                        QRY.JCDT,");
			sb.Append("                        QRY.BIZ_TYPE,");
			sb.Append("                        QRY.HRNR,");
			sb.Append("                        QRY.HDATE,");
			sb.Append("                        QRY.POL,");
			sb.Append("                        QRY.POD,");
			sb.Append("                        QRY.COLLECT,");
			sb.Append("                        QRY.CAN,");
			sb.Append("                        QRY.CANREFNR,");
			sb.Append("                        QRY.CDT,");
			sb.Append("                        QRY.INV,");
			sb.Append("                        QRY.CIPK,");
			sb.Append("                        QRY.INRNR,");
			sb.Append("                        QRY.INVOICE_DATE,");
			sb.Append("                        QRY.INV_CUR,");
			sb.Append("                        QRY.INVOICE_AMOUNT,");
			sb.Append("                        QRY.SUP_INV,");
			sb.Append("                        QRY.INVSPK,");
			sb.Append("                        QRY.INVSNR,");
			sb.Append("                        QRY.SUPPLIER_INV_DT,");
			sb.Append("                        QRY.SUP_INV_CUR,");
			sb.Append("                        QRY.SUP_INV_AMT,");
			sb.Append("                        QRY.COLLECTION,");
			sb.Append("                        QRY.COLREF,");
			sb.Append("                        QRY.COLREFDT,");
			sb.Append("                        QRY.COLCUR,");
			sb.Append("                        QRY.COLAMT,");
			sb.Append("                        QRY.OSCUR,");
			sb.Append("                        QRY.OS,");
			sb.Append("                        QRY.AGE,");
			sb.Append("                        QRY.CARGO_TYPE,");
			sb.Append("                        QRY.JCTYPE");
			sb.Append("       FROM (SELECT JCSIT.JOB_CARD_TRN_PK JCIPK,");
			sb.Append("       JCSIT.JOBCARD_REF_NO JCRNR,");
			sb.Append("       TO_DATE(JCSIT.JOBCARD_DATE, DATEFORMAT) JCDT,");
			sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFOR4320' AND DD.DD_VALUE = JCSIT.BUSINESS_TYPE) BIZ_TYPE,");
			sb.Append("       JCSIT.HBL_HAWB_REF_NO HRNR,");
			sb.Append("       TO_DATE(JCSIT.HBL_HAWB_DATE, DATEFORMAT) HDATE,");
			sb.Append("       POL.PORT_ID POL,");
			sb.Append("       POD.PORT_ID POD,");
			sb.Append("       NVL(ROUND(SUM(CASE WHEN JTSIF.FREIGHT_TYPE = 2 THEN");
			sb.Append("                  NVL(JTSIF.FREIGHT_AMT,0)*");
			sb.Append("                  GET_EX_RATE(JTSIF.CURRENCY_MST_FK," + BCurrencyPK + ",JCSIT.JOBCARD_DATE) ");
			sb.Append("       END),2),0) COLLECT,");
			sb.Append("       CASE");
			sb.Append("         WHEN CMT.BL_REF_NO IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END CAN,");
			sb.Append("       CMT.CAN_REF_NO CANREFNR,");
			sb.Append("       CMT.CAN_DATE CDT,");
			sb.Append("       CASE");
			sb.Append("         WHEN INVOICE.JOB_CARD_FK IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END INV,");
			sb.Append("       INVOICE.CONSOL_INVOICE_PK CIPK,");
			sb.Append("       INVOICE.INVOICE_REF_NO INRNR,");
			sb.Append("       TO_DATE(INVOICE.INVOICE_DATE, DATEFORMAT) INVOICE_DATE,");
			sb.Append("       INVOICE.CURRENCY_ID INV_CUR,");
			sb.Append("       NVL(INVOICE.TOT_AMT, 0) INVOICE_AMOUNT,");
			sb.Append("       CASE");
			sb.Append("         WHEN SUP_INV.JOB_CARD_TRN_FK IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END SUP_INV,");
			sb.Append("       SUP_INV.INV_SUPPLIER_PK INVSPK,");
			sb.Append("       SUP_INV.SUPPLIER_INV_NO INVSNR,");
			sb.Append("       TO_DATE(SUP_INV.SUPPLIER_INV_DT, DATEFORMAT) SUPPLIER_INV_DT,");
			sb.Append("       SUP_INV.CURRENCY_ID SUP_INV_CUR,");
			//sb.Append("       SUM(NVL(SUP_INV.INVOICE_AMT,0)) SUP_INV_AMT,")
			sb.Append("       NVL(SUP_INV.INVOICE_AMT,0) SUP_INV_AMT,");
			sb.Append("       CASE");
			sb.Append("         WHEN COLLECTION.INVOICE_REF_NO IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END COLLECTION,");
			sb.Append("       COLLECTION.COLLECTIONS_REF_NO COLREF,");
			sb.Append("       TO_DATE(COLLECTION.COLLECTIONS_DATE, DATEFORMAT) COLREFDT,");
			sb.Append("       COLLECTION.CURRENCY_ID COLCUR,");
			sb.Append("       NVL(COLLECTION.COLL_AMT, 0) COLAMT,");
			sb.Append("       (SELECT CMT11.CURRENCY_ID");
			sb.Append("          FROM CURRENCY_TYPE_MST_TBL CMT11");
			sb.Append("         WHERE CMT11.CURRENCY_MST_PK = " + BCurrencyPK + ") OSCUR,");
			sb.Append("       NVL(");
			sb.Append("           (NVL(INVOICE.TOT_AMT, 0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE) +");
			sb.Append("           NVL(SUP_INV.INVOICE_AMT, 0)*GET_EX_RATE(SUP_INV.CURRENCY_MST_FK," + BCurrencyPK + ",SUP_INV.SUPPLIER_INV_DT)) -");
			sb.Append("           NVL(COLLECTION.COLL_AMT, 0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("           0) OS,");
			sb.Append("       CASE WHEN ");
			sb.Append("           NVL((NVL(INVOICE.TOT_AMT, 0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE) +");
			sb.Append("           NVL(SUP_INV.INVOICE_AMT, 0)*GET_EX_RATE(SUP_INV.CURRENCY_MST_FK," + BCurrencyPK + ",SUP_INV.SUPPLIER_INV_DT)) -");
			sb.Append("           NVL(COLLECTION.COLL_AMT, 0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("           0) >0 THEN ");
			sb.Append("          ROUND(SYSDATE - TO_DATE(INVOICE.INVOICE_DATE))");
			sb.Append("         ELSE");
			sb.Append("          0");
			sb.Append("       END AGE,");
			sb.Append("       JCSIT.CARGO_TYPE CARGO_TYPE,");
			sb.Append("       JCSIT.JC_AUTO_MANUAL JCTYPE");
			sb.Append("  FROM JOB_CARD_TRN    JCSIT,");
			sb.Append("       PORT_MST_TBL            POL,");
			sb.Append("       PORT_MST_TBL            POD,");
			sb.Append("       JOB_TRN_FD      JTSIF,");
			sb.Append("       CAN_MST_TBL             CMT,");
			sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
			sb.Append("       VESSEL_VOYAGE_TRN       VVTRN,");
			sb.Append("       CUSTOMER_MST_TBL        CONS,");
			sb.Append("       CUSTOMER_MST_TBL        SHIP,");
			sb.Append("       COMMODITY_GROUP_MST_TBL COMG,");
			sb.Append("       (SELECT CITT.JOB_CARD_FK,");
			sb.Append("               CIT.CONSOL_INVOICE_PK,");
			sb.Append("               CIT.INVOICE_REF_NO,");
			sb.Append("               CIT.INVOICE_DATE,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               CIT.CURRENCY_MST_FK,");
			sb.Append("               ROUND(SUM(NVL(CITT.TOT_AMT,0)*");
			sb.Append("               GET_EX_RATE(CITT.CURRENCY_MST_FK,CIT.CURRENCY_MST_FK,");
			sb.Append("                            CIT.INVOICE_DATE)),2) TOT_AMT");
			sb.Append("          FROM CONSOL_INVOICE_TBL     CIT,");
			sb.Append("               CONSOL_INVOICE_TRN_TBL CITT,");
			sb.Append("               CURRENCY_TYPE_MST_TBL  CTMT");
			sb.Append("         WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
			sb.Append("           AND CTMT.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK");
			sb.Append("         GROUP BY CITT.JOB_CARD_FK,");
			sb.Append("                  CIT.CONSOL_INVOICE_PK,");
			sb.Append("                  CIT.INVOICE_REF_NO,");
			sb.Append("                  CIT.INVOICE_DATE,");
			sb.Append("                  CIT.CURRENCY_MST_FK,");
			sb.Append("                  CTMT.CURRENCY_ID) INVOICE,");
			//----------------------------------------------------------Invoice
			sb.Append("       (SELECT CIT.INVOICE_REF_NO,");
			sb.Append("               CT.COLLECTIONS_REF_NO,");
			sb.Append("               CT.COLLECTIONS_DATE,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               CT.CURRENCY_MST_FK,");
			sb.Append("               ROUND(SUM(NVL(CTT.RECD_AMOUNT_HDR_CURR, 0)), 2) COLL_AMT");
			sb.Append("          FROM COLLECTIONS_TBL       CT,");
			sb.Append("               COLLECTIONS_TRN_TBL   CTT,");
			sb.Append("               CONSOL_INVOICE_TBL    CIT,");
			sb.Append("               CURRENCY_TYPE_MST_TBL CTMT");
			sb.Append("         WHERE CT.COLLECTIONS_TBL_PK = CTT.COLLECTIONS_TBL_FK");
			sb.Append("           AND CIT.INVOICE_REF_NO = CTT.INVOICE_REF_NR");
			sb.Append("           AND CTMT.CURRENCY_MST_PK = CT.CURRENCY_MST_FK");
			sb.Append("         GROUP BY CIT.INVOICE_REF_NO,");
			sb.Append("                  CT.COLLECTIONS_REF_NO,");
			sb.Append("                  CT.COLLECTIONS_DATE,");
			sb.Append("                  CT.CURRENCY_MST_FK,");
			sb.Append("                  CTMT.CURRENCY_ID) COLLECTION,");
			//----------------------------------------------------------Collection
			if (RefType == "5") {
				if (!string.IsNullOrEmpty(RefNr)) {
					sb.Append("       (SELECT JCONT.JOB_CARD_TRN_FK");
					sb.Append("          FROM JOB_TRN_CONT JCONT");
					sb.Append("         WHERE JCONT.COMMODITY_MST_FKS IN");
					sb.Append("               (SELECT ROWTOCOL('SELECT C.COMMODITY_MST_PK FROM COMMODITY_MST_TBL C WHERE C.COMMODITY_ID IN (''" + RefNr + "'') ')");
					sb.Append("                  FROM DUAL)) JOB_CONT,");
				}
			}
			sb.Append("       (SELECT JTSIC.JOB_CARD_TRN_FK,");
			sb.Append("               IST.INV_SUPPLIER_PK,");
			sb.Append("               IST.SUPPLIER_INV_NO,");
			sb.Append("               IST.SUPPLIER_INV_DT,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               IST.CURRENCY_MST_FK,");
			sb.Append("               NVL(SUM(ROUND(ISTT.TOTAL_COST, 2)), 0) INVOICE_AMT");
			sb.Append("          FROM INV_SUPPLIER_TBL      IST,");
			sb.Append("               INV_SUPPLIER_TRN_TBL  ISTT,");
			sb.Append("               CURRENCY_TYPE_MST_TBL CTMT,");
			sb.Append("               JOB_TRN_COST  JTSIC");
			sb.Append("         WHERE IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
			sb.Append("           AND CTMT.CURRENCY_MST_PK = IST.CURRENCY_MST_FK");
			sb.Append("           AND JTSIC.JOB_TRN_COST_PK(+) = ISTT.JOB_TRN_EST_FK");
			sb.Append("         GROUP BY JTSIC.JOB_CARD_TRN_FK,");
			sb.Append("                  IST.INV_SUPPLIER_PK,");
			sb.Append("                  IST.SUPPLIER_INV_NO,");
			sb.Append("                  IST.SUPPLIER_INV_DT,");
			sb.Append("                  IST.CURRENCY_MST_FK,");
			sb.Append("                  CTMT.CURRENCY_ID) SUP_INV ");
			//----------------------------------------------------------Supplier Invoice
			sb.Append(" WHERE POL.PORT_MST_PK = JCSIT.PORT_MST_POL_FK");
			sb.Append("   AND POD.PORT_MST_PK = JCSIT.PORT_MST_POD_FK");
			sb.Append("   AND INVOICE.JOB_CARD_FK(+) = JCSIT.JOB_CARD_TRN_PK");
			sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VVTRN.VESSEL_VOYAGE_TBL_FK");
			sb.Append("   AND VVTRN.VOYAGE_TRN_PK(+) = JCSIT.VOYAGE_TRN_FK");
			sb.Append("   AND COLLECTION.INVOICE_REF_NO(+) = INVOICE.INVOICE_REF_NO");
			sb.Append("   AND SUP_INV.JOB_CARD_TRN_FK(+) = JCSIT.JOB_CARD_TRN_PK");
			sb.Append("   AND JCSIT.JOB_CARD_TRN_PK = JTSIF.JOB_CARD_TRN_FK(+)");
			sb.Append("   AND JCSIT.JOB_CARD_TRN_PK = CMT.JOB_CARD_FK(+)");
			sb.Append("   AND JCSIT.SHIPPER_CUST_MST_FK = SHIP.CUSTOMER_MST_PK");
			sb.Append("   AND JCSIT.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK");
			sb.Append("   AND JCSIT.COMMODITY_GROUP_FK = COMG.COMMODITY_GROUP_PK(+)");
			sb.Append("   AND JCSIT.VOYAGE_TRN_FK IS NOT NULL");
			sb.Append("  AND JCSIT.PROCESS_TYPE = 2 ");
			if (BizType > 0 & BizType != 3) {
				sb.Append(" AND  JCSIT.BUSINESS_TYPE=" + BizType);
			}

			sb.Append("   AND  POD.LOCATION_MST_FK=" + loc);

			if (Convert.ToInt32(RefType) > 0) {
				if (!string.IsNullOrEmpty(RefNr)) {
					if (RefType == "1") {
						sb.Append(" AND UPPER(SHIP.CUSTOMER_ID) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "2") {
						sb.Append(" AND UPPER(CONS.CUSTOMER_ID) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "3") {
						if (RefNr == "FCL" | RefNr == "fcl") {
							sb.Append(" AND  JCSIT.CARGO_TYPE = 1 ");
						} else if (RefNr == "LCL" | RefNr == "lcl") {
							sb.Append(" AND  JCSIT.CARGO_TYPE = 2");
						} else if (RefNr == "BBC" | RefNr == "Break Bulk") {
							sb.Append(" AND  JCSIT.CARGO_TYPE = 4");
						}
					} else if (RefType == "4") {
						sb.Append(" AND UPPER(COMG.COMMODITY_GROUP_DESC) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else {
						sb.Append("   AND JOB_CONT.JOB_CARD_TRN_FK(+) = JCSIT.JOB_CARD_TRN_PK");
					}
				}
			}

			if (Convert.ToInt32(VesselPk) > 0) {
				if (BizType == 2) {
					sb.Append(" AND VVTRN.VOYAGE_TRN_PK = " + VesselPk + "");
				} else if (BizType == 1) {
					sb.Append("  and JCSIT.CARRIER_MST_FK =  " + VesselPk + " ");
				} else {
					sb.Append(" AND (VVTRN.VOYAGE_TRN_PK = " + VesselPk + " OR  JCSIT.CARRIER_MST_FK =  " + VesselPk + ") ");
				}
			}

			if (!string.IsNullOrEmpty(txtJobRef.Trim()))
            {
				sb.Append(" AND UPPER(JCSIT.Jobcard_Ref_No) LIKE '%" + txtJobRef.Trim().ToUpper() + "%'");
			}

			if (PolPK > 0) {
				sb.Append("  AND POL.PORT_MST_PK = " + PolPK + "");
			}

			if (PodPK > 0) {
				sb.Append("  AND POD.PORT_MST_PK = " + PodPK + "");
			}

			if (!string.IsNullOrEmpty(txtHbl.Trim())) {
				sb.Append(" AND UPPER(JCSIT.HBL_HAWB_REF_NO) LIKE '%" + txtHbl.Trim().ToUpper() + "%'");
			}
			sb.Append("                          GROUP BY JCSIT.JOB_CARD_TRN_PK,");
			sb.Append("                                   JCSIT.JOBCARD_REF_NO,");
			sb.Append("                                   JOBCARD_DATE,");
			sb.Append("                                   JCSIT.HBL_HAWB_REF_NO,");
			sb.Append("                                   JCSIT.HBL_HAWB_DATE,");
			sb.Append("                                   JCSIT.DEPARTURE_DATE,");
			sb.Append("                                   POL.PORT_ID,");
			sb.Append("                                   POD.PORT_ID,");
			sb.Append("                                   CMT.BL_REF_NO,");
			sb.Append("                                   CMT.CAN_REF_NO,");
			sb.Append("                                   CMT.CAN_DATE,");
			sb.Append("                                   JCSIT.JOB_CARD_TRN_PK,");
			sb.Append("                                   INVOICE.CONSOL_INVOICE_PK,");
			sb.Append("                                   INVOICE.JOB_CARD_FK,");
			sb.Append("                                   INVOICE.INVOICE_REF_NO,");
			sb.Append("                                   INVOICE.INVOICE_DATE,");
			sb.Append("                                   INVOICE.CURRENCY_ID,");
			sb.Append("                                   INVOICE.CURRENCY_MST_FK,");
			sb.Append("                                   INVOICE.TOT_AMT,");
			sb.Append("                                   SUP_INV.JOB_CARD_TRN_FK,");
			sb.Append("                                   SUP_INV.INV_SUPPLIER_PK,");
			sb.Append("                                   SUP_INV.SUPPLIER_INV_NO,");
			sb.Append("                                   SUP_INV.SUPPLIER_INV_DT,");
			sb.Append("                                   SUP_INV.CURRENCY_ID,");
			sb.Append("                                   SUP_INV.CURRENCY_MST_FK,");
			sb.Append("                                   SUP_INV.INVOICE_AMT,");
			sb.Append("                                   COLLECTION.INVOICE_REF_NO,");
			sb.Append("                                   COLLECTION.COLLECTIONS_REF_NO,");
			sb.Append("                                   COLLECTION.COLLECTIONS_DATE,");
			sb.Append("                                   COLLECTION.CURRENCY_ID,");
			sb.Append("                                   COLLECTION.CURRENCY_MST_FK,");
			sb.Append("                                   JCSIT.BUSINESS_TYPE,");
			sb.Append("                                   JCSIT.CARGO_TYPE,");
			sb.Append("                                   JCSIT.JC_AUTO_MANUAL,");
			sb.Append("                                   COLLECTION.COLL_AMT ");
			sb.Append("                                   ORDER BY JCSIT.JOBCARD_DATE DESC) QRY) T))");

			try {
				DataTable tbl = new DataTable();
				tbl = objWF.GetDataTable(sb.ToString());
				TotalRecords = (Int32)tbl.Rows.Count;
				TotalPage = TotalRecords / RecordsPerPage;
				if (TotalRecords % RecordsPerPage != 0) {
					TotalPage += 1;
				}
				if (CurrentPage > TotalPage)
					CurrentPage = 1;
				if (TotalRecords == 0)
					CurrentPage = 0;
				Last = CurrentPage * RecordsPerPage;
				Start = (CurrentPage - 1) * RecordsPerPage + 1;
				if (Export == 0) {
					sb.Append("  WHERE SNO  Between " + Start + " and " + Last + "");
				}

				return objWF.GetDataSet(sb.ToString());
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch On Search for Import Freight Air"
        /// <summary>
        /// Fetches all imp air freight.
        /// </summary>
        /// <param name="AirLinePK">The air line pk.</param>
        /// <param name="txtJCRef">The text jc reference.</param>
        /// <param name="AOOPK">The aoopk.</param>
        /// <param name="AODPK">The aodpk.</param>
        /// <param name="txtHawbNr">The text hawb nr.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefNr">The reference nr.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="BCurrencyPK">The b currency pk.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public DataSet FetchAllImpAirFreight(int AirLinePK = 0, string txtJCRef = "", int AOOPK = 0, int AODPK = 0, string txtHawbNr = "", string RefType = "", string RefNr = "", Int32 Export = 0, int CurrentPage = 0, int TotalPage = 0,
		int BCurrencyPK = 0, int loc = 0)
		{

			Int32 Last = default(Int32);
			Int32 Start = default(Int32);
			Int32 TotalRecords = default(Int32);
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("SELECT *");
			sb.Append("  FROM (SELECT ROWNUM \"SNO\", T.*");
			sb.Append("          FROM ((SELECT QRY.JCIPK,");
			sb.Append("                        QRY.JCRNR,");
			sb.Append("                        QRY.JCDT,");
			sb.Append("                        QRY.BIZ_TYPE,");
			sb.Append("                        QRY.HRNR,");
			sb.Append("                        QRY.HDATE,");
			sb.Append("                        QRY.POL,");
			sb.Append("                        QRY.POD,");
			sb.Append("                        QRY.COLLECT,");
			sb.Append("                        QRY.CAN,");
			sb.Append("                        QRY.CANREFNR,");
			sb.Append("                        QRY.CDT,");
			sb.Append("                        QRY.INV,");
			sb.Append("                        QRY.CIPK,");
			sb.Append("                        QRY.INRNR,");
			sb.Append("                        QRY.INVOICE_DATE,");
			sb.Append("                        QRY.INV_CUR,");
			sb.Append("                        QRY.INVOICE_AMOUNT,");
			sb.Append("                        QRY.SUP_INV,");
			sb.Append("                        QRY.INVSPK,");
			sb.Append("                        QRY.INVSNR,");
			sb.Append("                        QRY.SUPPLIER_INV_DT,");
			sb.Append("                        QRY.SUP_INV_CUR,");
			sb.Append("                        QRY.SUP_INV_AMT,");
			sb.Append("                        QRY.COLLECTION,");
			sb.Append("                        QRY.COLREF,");
			sb.Append("                        QRY.COLREFDT,");
			sb.Append("                        QRY.COLCUR,");
			sb.Append("                        QRY.COLAMT,");
			sb.Append("                        QRY.OSCUR,");
			sb.Append("                        QRY.OS,");
			sb.Append("                        QRY.AGE,");
			sb.Append("                        '' CARGO_TYPE,");
			sb.Append("                        QRY.JCTYPE");
			sb.Append("                   FROM (SELECT JCAIT.JOB_CARD_TRN_PK JCIPK,");
			sb.Append("                                JCAIT.JOBCARD_REF_NO JCRNR,");
			sb.Append("                                TO_CHAR(JCAIT.JOBCARD_DATE, DATEFORMAT) JCDT,");
			sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFOR4320' AND DD.DD_VALUE = JCAIT.BUSINESS_TYPE) BIZ_TYPE,");
			sb.Append("                                JCAIT.HAWB_REF_NO HRNR,");
			sb.Append("                                TO_CHAR(JCAIT.HAWB_DATE, DATEFORMAT) HDATE,");
			sb.Append("                                POL.PORT_ID POL,");
			sb.Append("                                POD.PORT_ID POD,");
			sb.Append("                                NVL(ROUND(SUM(CASE");
			sb.Append("                                     WHEN JTAIF.FREIGHT_TYPE = 2 THEN");
			sb.Append("                                         NVL(JTAIF.FREIGHT_AMT,0)*");
			sb.Append("                                         GET_EX_RATE(JTAIF.CURRENCY_MST_FK," + BCurrencyPK + ",JCAIT.JOBCARD_DATE) ");
			sb.Append("                                      END),2),0) COLLECT,");
			sb.Append("                                CASE");
			sb.Append("                                  WHEN CMT.BL_REF_NO IS NOT NULL THEN");
			sb.Append("                                   'a'");
			sb.Append("                                  ELSE");
			sb.Append("                                   'r'");
			sb.Append("                                END CAN,");
			sb.Append("                                CMT.CAN_REF_NO CANREFNR,");
			sb.Append("                                CMT.CAN_DATE CDT,");
			sb.Append("                                CASE");
			sb.Append("                                  WHEN INVOICE.JOB_CARD_FK IS NOT NULL THEN");
			sb.Append("                                   'a'");
			sb.Append("                                  ELSE");
			sb.Append("                                   'r'");
			sb.Append("                                END INV,");
			sb.Append("                                INVOICE.CONSOL_INVOICE_PK CIPK,");
			sb.Append("                                INVOICE.INVOICE_REF_NO INRNR,");
			sb.Append("                                TO_CHAR(INVOICE.INVOICE_DATE, DATEFORMAT) INVOICE_DATE,");
			sb.Append("                                INVOICE.CURRENCY_ID INV_CUR,");
			sb.Append("                                Nvl(INVOICE.TOT_AMT,0) INVOICE_AMOUNT,");
			sb.Append("                                CASE");
			sb.Append("                                  WHEN SUP_INV.JOB_CARD_AIR_IMP_FK IS NOT NULL THEN");
			sb.Append("                                   'a'");
			sb.Append("                                  ELSE");
			sb.Append("                                   'r'");
			sb.Append("                                END SUP_INV,");
			sb.Append("                                SUP_INV.INV_SUPPLIER_PK INVSPK,");
			sb.Append("                                SUP_INV.SUPPLIER_INV_NO INVSNR,");
			sb.Append("                                TO_CHAR(SUP_INV.SUPPLIER_INV_DT, DATEFORMAT) SUPPLIER_INV_DT,");
			sb.Append("                                SUP_INV.CURRENCY_ID SUP_INV_CUR,");
			sb.Append("                                Nvl(SUP_INV.INVOICE_AMT,0) SUP_INV_AMT,");
			sb.Append("                                CASE");
			sb.Append("                                  WHEN COLLECTION.INVOICE_REF_NO IS NOT NULL THEN");
			sb.Append("                                   'a'");
			sb.Append("                                  ELSE");
			sb.Append("                                   'r'");
			sb.Append("                                END COLLECTION,");
			sb.Append("                                COLLECTION.COLLECTIONS_REF_NO COLREF,");
			sb.Append("                                TO_CHAR(COLLECTION.COLLECTIONS_DATE,");
			sb.Append("                                        DATEFORMAT) COLREFDT,");
			sb.Append("                                COLLECTION.CURRENCY_ID COLCUR,");
			sb.Append("                                Nvl(COLLECTION.COLL_AMT,0) COLAMT,");
			sb.Append("                                (SELECT CMT11.CURRENCY_ID");
			sb.Append("                                   FROM CURRENCY_TYPE_MST_TBL CMT11");
			sb.Append("                                  WHERE CMT11.CURRENCY_MST_PK = " + BCurrencyPK + ") OSCUR,");
			sb.Append("       NVL(");
			sb.Append("           (NVL(INVOICE.TOT_AMT, 0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE) +");
			sb.Append("           NVL(SUP_INV.INVOICE_AMT, 0)*GET_EX_RATE(SUP_INV.CURRENCY_MST_FK," + BCurrencyPK + ",SUP_INV.SUPPLIER_INV_DT)) -");
			sb.Append("           NVL(COLLECTION.COLL_AMT, 0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("           0) OS,");
			sb.Append("       CASE WHEN NVL(");
			sb.Append("           (NVL(INVOICE.TOT_AMT, 0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE) +");
			sb.Append("           NVL(SUP_INV.INVOICE_AMT, 0)*GET_EX_RATE(SUP_INV.CURRENCY_MST_FK," + BCurrencyPK + ",SUP_INV.SUPPLIER_INV_DT)) -");
			sb.Append("           NVL(COLLECTION.COLL_AMT, 0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("           0)>0 THEN ");
			sb.Append("                                   ROUND(SYSDATE -");
			sb.Append("                                         TO_DATE(INVOICE.INVOICE_DATE))");
			sb.Append("                                  ELSE");
			sb.Append("                                   0");
			sb.Append("                                END AGE,");
			sb.Append("                                JCAIT.JC_AUTO_MANUAL JCTYPE");
			sb.Append("                           FROM JOB_CARD_TRN JCAIT,");
			sb.Append("                                PORT_MST_TBL         POL,");
			sb.Append("                                PORT_MST_TBL         POD,");
			sb.Append("                                JOB_TRN_FD   JTAIF,");
			sb.Append("                                CAN_MST_TBL          CMT,");
			sb.Append("                                CUSTOMER_MST_TBL       CONS,");
			sb.Append("                                CUSTOMER_MST_TBL       SHIP,");
			sb.Append("                                COMMODITY_GROUP_MST_TBL COMG,");
			sb.Append("                                (SELECT CITT.JOB_CARD_FK,");
			sb.Append("                                        CIT.CONSOL_INVOICE_PK,");
			sb.Append("                                        CIT.INVOICE_REF_NO,");
			sb.Append("                                        CIT.INVOICE_DATE,");
			sb.Append("                                        CTMT.CURRENCY_ID,");
			sb.Append("                                        CIT.CURRENCY_MST_FK,");
			sb.Append("                                        ROUND(SUM(NVL(CITT.TOT_AMT,0)*");
			sb.Append("                                         GET_EX_RATE(CITT.CURRENCY_MST_FK,CIT.CURRENCY_MST_FK,");
			sb.Append("                                         CIT.INVOICE_DATE)),2) TOT_AMT");
			sb.Append("                                   FROM CONSOL_INVOICE_TBL     CIT,");
			sb.Append("                                        CONSOL_INVOICE_TRN_TBL CITT,");
			sb.Append("                                        CURRENCY_TYPE_MST_TBL  CTMT");
			sb.Append("                                  WHERE CIT.CONSOL_INVOICE_PK =");
			sb.Append("                                        CITT.CONSOL_INVOICE_FK");
			sb.Append("                                    AND CTMT.CURRENCY_MST_PK =");
			sb.Append("                                        CIT.CURRENCY_MST_FK");
			sb.Append("                                  GROUP BY CITT.JOB_CARD_FK,");
			sb.Append("                                           CIT.CONSOL_INVOICE_PK,");
			sb.Append("                                           CIT.INVOICE_REF_NO,");
			sb.Append("                                           CIT.INVOICE_DATE,");
			sb.Append("                                           CIT.CURRENCY_MST_FK,");
			sb.Append("                                           CTMT.CURRENCY_ID) INVOICE,");
			//---------------------------------------------------------------------------------INVOICE
			if (RefType == "4") {
				if (!string.IsNullOrEmpty(RefNr)) {
					sb.Append("                                (SELECT JCONT.JOB_CARD_TRN_FK");
					sb.Append("                                         FROM JOB_TRN_CONT JCONT");
					sb.Append("                                       WHERE JCONT.COMMODITY_MST_FK IN");
					sb.Append("                                       (SELECT ROWTOCOL('SELECT C.COMMODITY_MST_PK FROM COMMODITY_MST_TBL C WHERE C.COMMODITY_ID IN (''" + RefNr + "'')')");
					sb.Append("                                        FROM DUAL))JOB_CONT,");
				}
			}
			sb.Append("                                (SELECT CIT.INVOICE_REF_NO,");
			sb.Append("                                        CT.COLLECTIONS_REF_NO,");
			sb.Append("                                        CT.COLLECTIONS_DATE,");
			sb.Append("                                        CTMT.CURRENCY_ID,");
			sb.Append("                                        CT.CURRENCY_MST_FK,");
			sb.Append("                                        ROUND(SUM(NVL(CTT.RECD_AMOUNT_HDR_CURR ,0)),");
			sb.Append("                                            2) COLL_AMT");
			sb.Append("                                   FROM COLLECTIONS_TBL       CT,");
			sb.Append("                                        COLLECTIONS_TRN_TBL   CTT,");
			sb.Append("                                        CONSOL_INVOICE_TBL    CIT,");
			sb.Append("                                        CURRENCY_TYPE_MST_TBL CTMT");
			sb.Append("                                  WHERE CT.COLLECTIONS_TBL_PK =");
			sb.Append("                                        CTT.COLLECTIONS_TBL_FK");
			sb.Append("                                    AND CIT.INVOICE_REF_NO = CTT.INVOICE_REF_NR");
			sb.Append("                                    AND CTMT.CURRENCY_MST_PK =");
			sb.Append("                                        CT.CURRENCY_MST_FK");
			sb.Append("                                  GROUP BY CIT.INVOICE_REF_NO,");
			sb.Append("                                           CT.COLLECTIONS_REF_NO,");
			sb.Append("                                           CT.COLLECTIONS_DATE,");
			sb.Append("                                           CT.CURRENCY_MST_FK,");
			sb.Append("                                           CTMT.CURRENCY_ID) COLLECTION,");
			//---------------------------------------------------------------------------------COLLECTION
			sb.Append("                                (SELECT JTAIC.JOB_CARD_TRN_FK,");
			sb.Append("                                        IST.INV_SUPPLIER_PK,");
			sb.Append("                                        IST.SUPPLIER_INV_NO,");
			sb.Append("                                        IST.SUPPLIER_INV_DT,");
			sb.Append("                                        CTMT.CURRENCY_ID,");
			sb.Append("                                        IST.CURRENCY_MST_FK,");
			sb.Append("                                        ROUND(SUM(NVL(ISTT.TOTAL_COST ,0)),");
			sb.Append("                                            2) INVOICE_AMT");
			sb.Append("                                   FROM INV_SUPPLIER_TBL      IST,");
			sb.Append("                                        INV_SUPPLIER_TRN_TBL  ISTT,");
			sb.Append("                                        CURRENCY_TYPE_MST_TBL CTMT,");
			sb.Append("                                        JOB_TRN_COST  JTAIC");
			sb.Append("                                  WHERE IST.INV_SUPPLIER_PK =");
			sb.Append("                                        ISTT.INV_SUPPLIER_TBL_FK");
			sb.Append("                                    AND CTMT.CURRENCY_MST_PK =");
			sb.Append("                                        IST.CURRENCY_MST_FK");
			sb.Append("                                    AND JTAIC.JOB_TRN_COST_PK(+) =");
			sb.Append("                                        ISTT.JOB_TRN_EST_FK");
			sb.Append("                                  GROUP BY JTAIC.JOB_CARD_TRN_FK,");
			sb.Append("                                           IST.INV_SUPPLIER_PK,");
			sb.Append("                                           IST.SUPPLIER_INV_NO,");
			sb.Append("                                           IST.SUPPLIER_INV_DT,");
			sb.Append("                                           IST.CURRENCY_MST_FK,");
			sb.Append("                                           CTMT.CURRENCY_ID) SUP_INV");
			sb.Append("                          WHERE POL.PORT_MST_PK = JCAIT.PORT_MST_POL_FK");
			sb.Append("                            AND POD.PORT_MST_PK = JCAIT.PORT_MST_POD_FK");
			sb.Append("                            AND INVOICE.JOB_CARD_FK(+) =");
			sb.Append("                                JCAIT.JOB_CARD_TRN_PK");
			sb.Append("                            AND COLLECTION.INVOICE_REF_NO(+) =");
			sb.Append("                                INVOICE.INVOICE_REF_NO");
			sb.Append("                            AND SUP_INV.JOB_CARD_AIR_IMP_FK(+) =");
			sb.Append("                                JCAIT.JOB_CARD_TRN_PK");
			sb.Append("                            AND JCAIT.JOB_CARD_TRN_PK =");
			sb.Append("                                JTAIF.JOB_CARD_TRN_FK");
			sb.Append("                            AND JCAIT.JOB_CARD_TRN_PK = CMT.JOB_CARD_FK(+)");
			sb.Append("                             AND JCAIT.SHIPPER_CUST_MST_FK=SHIP.CUSTOMER_MST_PK");
			sb.Append("                             AND JCAIT.CONSIGNEE_CUST_MST_FK=CONS.CUSTOMER_MST_PK");
			sb.Append("                             AND JCAIT.COMMODITY_GROUP_FK=COMG.COMMODITY_GROUP_PK(+)");
			sb.Append("   AND  POD.LOCATION_MST_FK=" + loc);
			if (Convert.ToInt32(RefType) > 0) {
				if (!string.IsNullOrEmpty(RefNr)) {
					if (RefType == "1") {
						sb.Append(" AND UPPER(SHIP.CUSTOMER_ID) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "2") {
						sb.Append(" AND UPPER(CONS.CUSTOMER_ID) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "3") {
						sb.Append(" AND UPPER(COMG.COMMODITY_GROUP_DESC) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else {
						sb.Append("  AND JOB_CONT.JOB_CARD_TRN_FK(+) = JCAIT.JOB_CARD_TRN_PK");
					}
				}
			}

			if (AirLinePK != 0) {
				sb.Append("  and JCAIT.airline_mst_fk =  " + AirLinePK + " ");
			}

			if (!string.IsNullOrEmpty(txtJCRef.Trim()))
            {
				sb.Append(" AND UPPER(JCAIT.Jobcard_Ref_No) LIKE '%" + txtJCRef.Trim().ToUpper() + "%'");
			}

			if (AOOPK != 0) {
				sb.Append("  AND POL.PORT_MST_PK = " + AOOPK + "");
			}

			if (AODPK != 0) {
				sb.Append("  AND POD.PORT_MST_PK = " + AODPK + "");
			}

			if (!string.IsNullOrEmpty(txtHawbNr.Trim()))
            {
				sb.Append(" AND UPPER(JCAIT.HAWB_REF_NO) LIKE '%" + txtHawbNr.Trim().ToUpper() + "%'");
			}

			sb.Append("                          GROUP BY JCAIT.JOB_CARD_TRN_PK,");
			sb.Append("                                   JCAIT.JOBCARD_REF_NO,");
			sb.Append("                                   JOBCARD_DATE,");
			sb.Append("                                   JCAIT.BUSINESS_TYPE,");
			sb.Append("                                   JCAIT.HAWB_REF_NO,");
			sb.Append("                                   JCAIT.HAWB_DATE,");
			sb.Append("                                   JCAIT.DEPARTURE_DATE,");
			sb.Append("                                   POL.PORT_ID,");
			sb.Append("                                   POD.PORT_ID,");
			sb.Append("                                   CMT.BL_REF_NO,");
			sb.Append("                                   CMT.CAN_REF_NO,");
			sb.Append("                                   CMT.CAN_DATE,");
			sb.Append("                                   JCAIT.JOB_CARD_TRN_PK,");
			sb.Append("                                   INVOICE.CONSOL_INVOICE_PK,");
			sb.Append("                                   INVOICE.JOB_CARD_FK,");
			sb.Append("                                   INVOICE.INVOICE_REF_NO,");
			sb.Append("                                   INVOICE.INVOICE_DATE,");
			sb.Append("                                   INVOICE.CURRENCY_ID,");
			sb.Append("                                   INVOICE.CURRENCY_MST_FK,");
			sb.Append("                                   INVOICE.TOT_AMT,");
			sb.Append("                                   SUP_INV.JOB_CARD_AIR_IMP_FK,");
			sb.Append("                                   SUP_INV.INV_SUPPLIER_PK,");
			sb.Append("                                   SUP_INV.SUPPLIER_INV_NO,");
			sb.Append("                                   SUP_INV.SUPPLIER_INV_DT,");
			sb.Append("                                   SUP_INV.CURRENCY_ID,");
			sb.Append("                                   SUP_INV.CURRENCY_MST_FK,");
			sb.Append("                                   SUP_INV.INVOICE_AMT,");
			sb.Append("                                   COLLECTION.INVOICE_REF_NO,");
			sb.Append("                                   COLLECTION.COLLECTIONS_REF_NO,");
			sb.Append("                                   COLLECTION.COLLECTIONS_DATE,");
			sb.Append("                                   COLLECTION.CURRENCY_ID,");
			sb.Append("                                   COLLECTION.CURRENCY_MST_FK,");
			sb.Append("                                    JCAIT.JC_AUTO_MANUAL,");
			sb.Append("                                   COLLECTION.COLL_AMT ");
			sb.Append("                                   ORDER BY JCAIT.JOBCARD_DATE DESC) QRY) T))");

			try {
				DataTable tbl = new DataTable();
				tbl = objWF.GetDataTable(sb.ToString());
				TotalRecords = (Int32)tbl.Rows.Count;
				TotalPage = TotalRecords / RecordsPerPage;
				if (TotalRecords % RecordsPerPage != 0) {
					TotalPage += 1;
				}
				if (CurrentPage > TotalPage)
					CurrentPage = 1;
				if (TotalRecords == 0)
					CurrentPage = 0;
				Last = CurrentPage * RecordsPerPage;
				Start = (CurrentPage - 1) * RecordsPerPage + 1;

				if (Export == 0) {
					sb.Append("  WHERE SNO  Between " + Start + " and " + Last + "");
				}
				return objWF.GetDataSet(sb.ToString());

			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch All"
        /// <summary>
        /// Fetches all imp freight.
        /// </summary>
        /// <param name="VesselPk">The vessel pk.</param>
        /// <param name="txtJobRef">The text job reference.</param>
        /// <param name="PolPK">The pol pk.</param>
        /// <param name="PodPK">The pod pk.</param>
        /// <param name="txtHbl">The text HBL.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefNr">The reference nr.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="BCurrencyPK">The b currency pk.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public DataSet FetchAllImpFreight(string VesselPk = "", string txtJobRef = "", int PolPK = 0, int PodPK = 0, string txtHbl = "", string RefType = "", string RefNr = "", Int32 Export = 0, int CurrentPage = 0, int TotalPage = 0,
		int BCurrencyPK = 0, int loc = 0)
		{
			Int32 Last = default(Int32);
			Int32 Start = default(Int32);
			Int32 TotalRecords = default(Int32);
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("SELECT *");
			sb.Append("  FROM (SELECT ROWNUM \"SNO\", T.*");
			sb.Append("          FROM ((SELECT QRY.JCIPK,");
			sb.Append("                        QRY.JCRNR,");
			sb.Append("                        QRY.JCDT,");
			sb.Append("                        QRY.BIZ_TYPE,");
			sb.Append("                        QRY.HRNR,");
			sb.Append("                        QRY.HDATE,");
			sb.Append("                        QRY.POL,");
			sb.Append("                        QRY.POD,");
			sb.Append("                        QRY.COLLECT,");
			sb.Append("                        QRY.CAN,");
			sb.Append("                        QRY.CANREFNR,");
			sb.Append("                        QRY.CDT,");
			sb.Append("                        QRY.INV,");
			sb.Append("                        QRY.CIPK,");
			sb.Append("                        QRY.INRNR,");
			sb.Append("                        QRY.INVOICE_DATE,");
			sb.Append("                        QRY.INV_CUR,");
			sb.Append("                        QRY.INVOICE_AMOUNT,");
			sb.Append("                        QRY.SUP_INV,");
			sb.Append("                        QRY.INVSPK,");
			sb.Append("                        QRY.INVSNR,");
			sb.Append("                        QRY.SUPPLIER_INV_DT,");
			sb.Append("                        QRY.SUP_INV_CUR,");
			sb.Append("                        QRY.SUP_INV_AMT,");
			sb.Append("                        QRY.COLLECTION,");
			sb.Append("                        QRY.COLREF,");
			sb.Append("                        QRY.COLREFDT,");
			sb.Append("                        QRY.COLCUR,");
			sb.Append("                        QRY.COLAMT,");
			sb.Append("                        QRY.OSCUR,");
			sb.Append("                        QRY.OS,");
			sb.Append("                        QRY.AGE,");
			sb.Append("                        QRY.CARGO_TYPE,");
			sb.Append("                        QRY.JCTYPE");
			sb.Append("       FROM (SELECT JCSIT.JOB_CARD_TRN_PK JCIPK,");
			sb.Append("       JCSIT.JOBCARD_REF_NO JCRNR,");
			sb.Append("       TO_CHAR(JCSIT.JOBCARD_DATE, DATEFORMAT) JCDT,");
			sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFOR4320' AND DD.DD_VALUE = 2) BIZ_TYPE,");
			sb.Append("       JCSIT.HBL_REF_NO HRNR,");
			sb.Append("       TO_CHAR(JCSIT.HBL_DATE, DATEFORMAT) HDATE,");
			sb.Append("       POL.PORT_ID POL,");
			sb.Append("       POD.PORT_ID POD,");
			sb.Append("       NVL(ROUND(SUM(CASE WHEN JTSIF.FREIGHT_TYPE = 2 THEN");
			sb.Append("                  NVL(JTSIF.FREIGHT_AMT,0)*");
			sb.Append("                  GET_EX_RATE(JTSIF.CURRENCY_MST_FK," + BCurrencyPK + ",JCSIT.JOBCARD_DATE) ");
			sb.Append("       END),2),0) COLLECT,");
			sb.Append("       CASE");
			sb.Append("         WHEN CMT.BL_REF_NO IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END CAN,");
			sb.Append("       CMT.CAN_REF_NO CANREFNR,");
			sb.Append("       CMT.CAN_DATE CDT,");
			sb.Append("       CASE");
			sb.Append("         WHEN INVOICE.JOB_CARD_FK IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END INV,");
			sb.Append("       INVOICE.CONSOL_INVOICE_PK CIPK,");
			sb.Append("       INVOICE.INVOICE_REF_NO INRNR,");
			sb.Append("       TO_CHAR(INVOICE.INVOICE_DATE, DATEFORMAT) INVOICE_DATE,");
			sb.Append("       INVOICE.CURRENCY_ID INV_CUR,");
			sb.Append("       NVL(INVOICE.TOT_AMT, 0) INVOICE_AMOUNT,");
			sb.Append("       CASE");
			sb.Append("         WHEN SUP_INV.JOB_CARD_SEA_IMP_FK IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END SUP_INV,");
			sb.Append("       SUP_INV.INV_SUPPLIER_PK INVSPK,");
			sb.Append("       SUP_INV.SUPPLIER_INV_NO INVSNR,");
			sb.Append("       TO_CHAR(SUP_INV.SUPPLIER_INV_DT, DATEFORMAT) SUPPLIER_INV_DT,");
			sb.Append("       SUP_INV.CURRENCY_ID SUP_INV_CUR,");
			//sb.Append("       SUM(NVL(SUP_INV.INVOICE_AMT,0)) SUP_INV_AMT,")
			sb.Append("       NVL(SUP_INV.INVOICE_AMT,0) SUP_INV_AMT,");
			sb.Append("       CASE");
			sb.Append("         WHEN COLLECTION.INVOICE_REF_NO IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END COLLECTION,");
			sb.Append("       COLLECTION.COLLECTIONS_REF_NO COLREF,");
			sb.Append("       TO_CHAR(COLLECTION.COLLECTIONS_DATE, DATEFORMAT) COLREFDT,");
			sb.Append("       COLLECTION.CURRENCY_ID COLCUR,");
			sb.Append("       NVL(COLLECTION.COLL_AMT, 0) COLAMT,");
			sb.Append("       (SELECT CMT11.CURRENCY_ID");
			sb.Append("          FROM CURRENCY_TYPE_MST_TBL CMT11");
			sb.Append("         WHERE CMT11.CURRENCY_MST_PK = " + BCurrencyPK + ") OSCUR,");
			sb.Append("       NVL(");
			sb.Append("           (NVL(INVOICE.TOT_AMT, 0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE) +");
			sb.Append("           NVL(SUP_INV.INVOICE_AMT, 0)*GET_EX_RATE(SUP_INV.CURRENCY_MST_FK," + BCurrencyPK + ",SUP_INV.SUPPLIER_INV_DT)) -");
			sb.Append("           NVL(COLLECTION.COLL_AMT, 0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("           0) OS,");
			sb.Append("       CASE WHEN ");
			sb.Append("           NVL((NVL(INVOICE.TOT_AMT, 0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE) +");
			sb.Append("           NVL(SUP_INV.INVOICE_AMT, 0)*GET_EX_RATE(SUP_INV.CURRENCY_MST_FK," + BCurrencyPK + ",SUP_INV.SUPPLIER_INV_DT)) -");
			sb.Append("           NVL(COLLECTION.COLL_AMT, 0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("           0) >0 THEN ");
			sb.Append("          ROUND(SYSDATE - TO_DATE(INVOICE.INVOICE_DATE))");
			sb.Append("         ELSE");
			sb.Append("          0");
			sb.Append("       END AGE,");
			sb.Append("       JCSIT.CARGO_TYPE CARGO_TYPE,");
			sb.Append("       JCSIT.JC_AUTO_MANUAL JCTYPE");
			sb.Append("  FROM JOB_CARD_TRN    JCSIT,");
			sb.Append("       PORT_MST_TBL            POL,");
			sb.Append("       PORT_MST_TBL            POD,");
			sb.Append("       JOB_TRN_FD      JTSIF,");
			sb.Append("       CAN_MST_TBL             CMT,");
			sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
			sb.Append("       VESSEL_VOYAGE_TRN       VVTRN,");
			sb.Append("       CUSTOMER_MST_TBL        CONS,");
			sb.Append("       CUSTOMER_MST_TBL        SHIP,");
			sb.Append("       COMMODITY_GROUP_MST_TBL COMG,");
			sb.Append("       (SELECT CITT.JOB_CARD_FK,");
			sb.Append("               CIT.CONSOL_INVOICE_PK,");
			sb.Append("               CIT.INVOICE_REF_NO,");
			sb.Append("               CIT.INVOICE_DATE,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               CIT.CURRENCY_MST_FK,");
			sb.Append("               ROUND(SUM(NVL(CITT.TOT_AMT,0)*");
			sb.Append("               GET_EX_RATE(CITT.CURRENCY_MST_FK,CIT.CURRENCY_MST_FK,");
			sb.Append("                            CIT.INVOICE_DATE)),2) TOT_AMT");
			sb.Append("          FROM CONSOL_INVOICE_TBL     CIT,");
			sb.Append("               CONSOL_INVOICE_TRN_TBL CITT,");
			sb.Append("               CURRENCY_TYPE_MST_TBL  CTMT");
			sb.Append("         WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
			sb.Append("           AND CTMT.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK");
			sb.Append("         GROUP BY CITT.JOB_CARD_FK,");
			sb.Append("                  CIT.CONSOL_INVOICE_PK,");
			sb.Append("                  CIT.INVOICE_REF_NO,");
			sb.Append("                  CIT.INVOICE_DATE,");
			sb.Append("                  CIT.CURRENCY_MST_FK,");
			sb.Append("                  CTMT.CURRENCY_ID) INVOICE,");
			//----------------------------------------------------------Invoice
			sb.Append("       (SELECT CIT.INVOICE_REF_NO,");
			sb.Append("               CT.COLLECTIONS_REF_NO,");
			sb.Append("               CT.COLLECTIONS_DATE,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               CT.CURRENCY_MST_FK,");
			sb.Append("               ROUND(SUM(NVL(CTT.RECD_AMOUNT_HDR_CURR, 0)), 2) COLL_AMT");
			sb.Append("          FROM COLLECTIONS_TBL       CT,");
			sb.Append("               COLLECTIONS_TRN_TBL   CTT,");
			sb.Append("               CONSOL_INVOICE_TBL    CIT,");
			sb.Append("               CURRENCY_TYPE_MST_TBL CTMT");
			sb.Append("         WHERE CT.COLLECTIONS_TBL_PK = CTT.COLLECTIONS_TBL_FK");
			sb.Append("           AND CIT.INVOICE_REF_NO = CTT.INVOICE_REF_NR");
			sb.Append("           AND CTMT.CURRENCY_MST_PK = CT.CURRENCY_MST_FK");
			sb.Append("         GROUP BY CIT.INVOICE_REF_NO,");
			sb.Append("                  CT.COLLECTIONS_REF_NO,");
			sb.Append("                  CT.COLLECTIONS_DATE,");
			sb.Append("                  CT.CURRENCY_MST_FK,");
			sb.Append("                  CTMT.CURRENCY_ID) COLLECTION,");
			//----------------------------------------------------------Collection
			if (RefType == "5") {
				if (!string.IsNullOrEmpty(RefNr)) {
					sb.Append("       (SELECT JCONT.JOB_CARD_SEA_IMP_FK");
					sb.Append("          FROM JOB_TRN_CONT JCONT");
					sb.Append("         WHERE JCONT.COMMODITY_MST_FKS IN");
					sb.Append("               (SELECT ROWTOCOL('SELECT C.COMMODITY_MST_PK FROM COMMODITY_MST_TBL C WHERE C.COMMODITY_ID IN (''" + RefNr + "'') ')");
					sb.Append("                  FROM DUAL)) JOB_CONT,");
				}
			}
			sb.Append("       (SELECT JTSIC.JOB_CARD_SEA_IMP_FK,");
			sb.Append("               IST.INV_SUPPLIER_PK,");
			sb.Append("               IST.SUPPLIER_INV_NO,");
			sb.Append("               IST.SUPPLIER_INV_DT,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               IST.CURRENCY_MST_FK,");
			sb.Append("               NVL(SUM(ROUND(ISTT.TOTAL_COST, 2)), 0) INVOICE_AMT");
			sb.Append("          FROM INV_SUPPLIER_TBL      IST,");
			sb.Append("               INV_SUPPLIER_TRN_TBL  ISTT,");
			sb.Append("               CURRENCY_TYPE_MST_TBL CTMT,");
			sb.Append("               JOB_TRN_COST  JTSIC");
			sb.Append("         WHERE IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
			sb.Append("           AND CTMT.CURRENCY_MST_PK = IST.CURRENCY_MST_FK");
			sb.Append("           AND JTSIC.JOB_TRN_COST_PK(+) = ISTT.JOB_TRN_EST_FK");
			sb.Append("         GROUP BY JTSIC.JOB_CARD_SEA_IMP_FK,");
			sb.Append("                  IST.INV_SUPPLIER_PK,");
			sb.Append("                  IST.SUPPLIER_INV_NO,");
			sb.Append("                  IST.SUPPLIER_INV_DT,");
			sb.Append("                  IST.CURRENCY_MST_FK,");
			sb.Append("                  CTMT.CURRENCY_ID) SUP_INV ");
			//----------------------------------------------------------Supplier Invoice
			sb.Append(" WHERE POL.PORT_MST_PK = JCSIT.PORT_MST_POL_FK");
			sb.Append("   AND POD.PORT_MST_PK = JCSIT.PORT_MST_POD_FK");
			sb.Append("   AND INVOICE.JOB_CARD_FK(+) = JCSIT.JOB_CARD_TRN_PK");
			sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK = VVTRN.VESSEL_VOYAGE_TBL_FK");
			sb.Append("   AND VVTRN.VOYAGE_TRN_PK = JCSIT.VOYAGE_TRN_FK");
			sb.Append("   AND COLLECTION.INVOICE_REF_NO(+) = INVOICE.INVOICE_REF_NO");
			sb.Append("   AND SUP_INV.JOB_CARD_SEA_IMP_FK(+) = JCSIT.JOB_CARD_TRN_PK");
			sb.Append("   AND JCSIT.JOB_CARD_TRN_PK = JTSIF.JOB_CARD_SEA_IMP_FK(+)");
			sb.Append("   AND JCSIT.JOB_CARD_TRN_PK = CMT.JOB_CARD_FK(+)");
			sb.Append("   AND JCSIT.SHIPPER_CUST_MST_FK = SHIP.CUSTOMER_MST_PK");
			sb.Append("   AND JCSIT.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK");
			sb.Append("   AND JCSIT.COMMODITY_GROUP_FK = COMG.COMMODITY_GROUP_PK(+)");
			sb.Append("   AND  POD.LOCATION_MST_FK=" + loc);

			if (Convert.ToInt32(RefType) > 0) {
				if (!string.IsNullOrEmpty(RefNr)) {
					if (RefType == "1") {
						sb.Append(" AND UPPER(SHIP.CUSTOMER_ID) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "2") {
						sb.Append(" AND UPPER(CONS.CUSTOMER_ID) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "3") {
						if (RefNr == "FCL" | RefNr == "fcl") {
							sb.Append(" AND  JCSIT.CARGO_TYPE = 1");
						} else if (RefNr == "LCL" | RefNr == "lcl") {
							sb.Append(" AND  JCSIT.CARGO_TYPE = 2");
						} else if (RefNr == "BBC" | RefNr == "Break Bulk") {
							sb.Append(" AND  JCSIT.CARGO_TYPE = 4");
						}
					} else if (RefType == "4") {
						sb.Append(" AND UPPER(COMG.COMMODITY_GROUP_DESC) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else {
						sb.Append("   AND JOB_CONT.JOB_CARD_SEA_IMP_FK(+) = JCSIT.JOB_CARD_TRN_PK");
					}
				}
			}

			if (Convert.ToInt32(VesselPk) != 0) {
				sb.Append(" AND VVTRN.VOYAGE_TRN_PK = " + VesselPk + "");
			}

			if (!string.IsNullOrEmpty(txtJobRef.Trim()))
            {
				sb.Append(" AND UPPER(JCSIT.Jobcard_Ref_No) LIKE '%" + txtJobRef.Trim().ToUpper() + "%'");
			}

			if (PolPK != 0) {
				sb.Append("  AND POL.PORT_MST_PK = " + PolPK + "");
			}

			if (PodPK != 0) {
				sb.Append("  AND POD.PORT_MST_PK = " + PodPK + "");
			}

			if (!string.IsNullOrEmpty(txtHbl.Trim()))
            {
				sb.Append(" AND UPPER(JCSIT.HBL_REF_NO) LIKE '%" + txtHbl.Trim().ToUpper() + "%'");
			}
			sb.Append("                          GROUP BY JCSIT.JOB_CARD_TRN_PK,");
			sb.Append("                                   JCSIT.JOBCARD_REF_NO,");
			sb.Append("                                   JOBCARD_DATE,");
			sb.Append("                                   JCSIT.HBL_REF_NO,");
			sb.Append("                                   HBL_DATE,");
			sb.Append("                                   JCSIT.DEPARTURE_DATE,");
			sb.Append("                                   POL.PORT_ID,");
			sb.Append("                                   POD.PORT_ID,");
			sb.Append("                                   CMT.BL_REF_NO,");
			sb.Append("                                   CMT.CAN_REF_NO,");
			sb.Append("                                   CMT.CAN_DATE,");
			sb.Append("                                   JCSIT.JOB_CARD_TRN_PK,");
			sb.Append("                                   INVOICE.CONSOL_INVOICE_PK,");
			sb.Append("                                   INVOICE.JOB_CARD_FK,");
			sb.Append("                                   INVOICE.INVOICE_REF_NO,");
			sb.Append("                                   INVOICE.INVOICE_DATE,");
			sb.Append("                                   INVOICE.CURRENCY_ID,");
			sb.Append("                                   INVOICE.CURRENCY_MST_FK,");
			sb.Append("                                   INVOICE.TOT_AMT,");
			sb.Append("                                   SUP_INV.JOB_CARD_SEA_IMP_FK,");
			sb.Append("                                   SUP_INV.INV_SUPPLIER_PK,");
			sb.Append("                                   SUP_INV.SUPPLIER_INV_NO,");
			sb.Append("                                   SUP_INV.SUPPLIER_INV_DT,");
			sb.Append("                                   SUP_INV.CURRENCY_ID,");
			sb.Append("                                   SUP_INV.CURRENCY_MST_FK,");
			sb.Append("                                   SUP_INV.INVOICE_AMT,");
			sb.Append("                                   COLLECTION.INVOICE_REF_NO,");
			sb.Append("                                   COLLECTION.COLLECTIONS_REF_NO,");
			sb.Append("                                   COLLECTION.COLLECTIONS_DATE,");
			sb.Append("                                   COLLECTION.CURRENCY_ID,");
			sb.Append("                                   COLLECTION.CURRENCY_MST_FK,");
			sb.Append("                                   JCSIT.CARGO_TYPE,");
			sb.Append("                                   JCSIT.JC_AUTO_MANUAL,");
			sb.Append("                                   COLLECTION.COLL_AMT ");

			sb.Append(" UNION");

			sb.Append("                   SELECT JCAIT.JOB_CARD_TRN_PK JCIPK,");
			sb.Append("                                JCAIT.JOBCARD_REF_NO JCRNR,");
			sb.Append("                                TO_CHAR(JCAIT.JOBCARD_DATE, DATEFORMAT) JCDT,");
			sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFOR4320' AND DD.DD_VALUE = 1) BIZ_TYPE,");
			sb.Append("                                JCAIT.HAWB_REF_NO HRNR,");
			sb.Append("                                TO_CHAR(JCAIT.HAWB_DATE, DATEFORMAT) HDATE,");
			sb.Append("                                POL.PORT_ID POL,");
			sb.Append("                                POD.PORT_ID POD,");
			sb.Append("                                NVL(ROUND(SUM(CASE");
			sb.Append("                                     WHEN JTAIF.FREIGHT_TYPE = 2 THEN");
			sb.Append("                                         NVL(JTAIF.FREIGHT_AMT,0)*");
			sb.Append("                                         GET_EX_RATE(JTAIF.CURRENCY_MST_FK," + BCurrencyPK + ",JCAIT.JOBCARD_DATE) ");
			sb.Append("                                      END),2),0) COLLECT,");
			sb.Append("                                CASE");
			sb.Append("                                  WHEN CMT.BL_REF_NO IS NOT NULL THEN");
			sb.Append("                                   'a'");
			sb.Append("                                  ELSE");
			sb.Append("                                   'r'");
			sb.Append("                                END CAN,");
			sb.Append("                                CMT.CAN_REF_NO CANREFNR,");
			sb.Append("                                CMT.CAN_DATE CDT,");
			sb.Append("                                CASE");
			sb.Append("                                  WHEN INVOICE.JOB_CARD_FK IS NOT NULL THEN");
			sb.Append("                                   'a'");
			sb.Append("                                  ELSE");
			sb.Append("                                   'r'");
			sb.Append("                                END INV,");
			sb.Append("                                INVOICE.CONSOL_INVOICE_PK CIPK,");
			sb.Append("                                INVOICE.INVOICE_REF_NO INRNR,");
			sb.Append("                                TO_CHAR(INVOICE.INVOICE_DATE, DATEFORMAT) INVOICE_DATE,");
			sb.Append("                                INVOICE.CURRENCY_ID INV_CUR,");
			sb.Append("                                Nvl(INVOICE.TOT_AMT,0) INVOICE_AMOUNT,");
			sb.Append("                                CASE");
			sb.Append("                                  WHEN SUP_INV.JOB_CARD_AIR_IMP_FK IS NOT NULL THEN");
			sb.Append("                                   'a'");
			sb.Append("                                  ELSE");
			sb.Append("                                   'r'");
			sb.Append("                                END SUP_INV,");
			sb.Append("                                SUP_INV.INV_SUPPLIER_PK INVSPK,");
			sb.Append("                                SUP_INV.SUPPLIER_INV_NO INVSNR,");
			sb.Append("                                TO_CHAR(SUP_INV.SUPPLIER_INV_DT, DATEFORMAT) SUPPLIER_INV_DT,");
			sb.Append("                                SUP_INV.CURRENCY_ID SUP_INV_CUR,");
			sb.Append("                                Nvl(SUP_INV.INVOICE_AMT,0) SUP_INV_AMT,");
			sb.Append("                                CASE");
			sb.Append("                                  WHEN COLLECTION.INVOICE_REF_NO IS NOT NULL THEN");
			sb.Append("                                   'a'");
			sb.Append("                                  ELSE");
			sb.Append("                                   'r'");
			sb.Append("                                END COLLECTION,");
			sb.Append("                                COLLECTION.COLLECTIONS_REF_NO COLREF,");
			sb.Append("                                TO_CHAR(COLLECTION.COLLECTIONS_DATE,");
			sb.Append("                                        DATEFORMAT) COLREFDT,");
			sb.Append("                                COLLECTION.CURRENCY_ID COLCUR,");
			sb.Append("                                Nvl(COLLECTION.COLL_AMT,0) COLAMT,");
			sb.Append("                                (SELECT CMT11.CURRENCY_ID");
			sb.Append("                                   FROM CURRENCY_TYPE_MST_TBL CMT11");
			sb.Append("                                  WHERE CMT11.CURRENCY_MST_PK = " + BCurrencyPK + ") OSCUR,");
			//sb.Append("                                NVL((NVL(INVOICE.TOT_AMT, 0) +")
			//sb.Append("                                    NVL(SUP_INV.INVOICE_AMT, 0)) -")
			//sb.Append("                                    NVL(COLLECTION.COLL_AMT, 0),")
			//sb.Append("                                    0) OS,")
			sb.Append("       NVL(");
			sb.Append("           (NVL(INVOICE.TOT_AMT, 0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE) +");
			sb.Append("           NVL(SUP_INV.INVOICE_AMT, 0)*GET_EX_RATE(SUP_INV.CURRENCY_MST_FK," + BCurrencyPK + ",SUP_INV.SUPPLIER_INV_DT)) -");
			sb.Append("           NVL(COLLECTION.COLL_AMT, 0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("           0) OS,");
			sb.Append("       CASE WHEN NVL(");
			sb.Append("           (NVL(INVOICE.TOT_AMT, 0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE) +");
			sb.Append("           NVL(SUP_INV.INVOICE_AMT, 0)*GET_EX_RATE(SUP_INV.CURRENCY_MST_FK," + BCurrencyPK + ",SUP_INV.SUPPLIER_INV_DT)) -");
			sb.Append("           NVL(COLLECTION.COLL_AMT, 0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("           0)>0 THEN ");
			sb.Append("                                   ROUND(SYSDATE -");
			sb.Append("                                         TO_DATE(INVOICE.INVOICE_DATE))");
			sb.Append("                                  ELSE");
			sb.Append("                                   0");
			sb.Append("                                END AGE,");
			sb.Append("                                0 CARGO_TYPE,");
			sb.Append("                                JCAIT.JC_AUTO_MANUAL JCTYPE");
			sb.Append("                           FROM JOB_CARD_TRN JCAIT,");
			sb.Append("                                PORT_MST_TBL         POL,");
			sb.Append("                                PORT_MST_TBL         POD,");
			sb.Append("                                JOB_TRN_FD   JTAIF,");
			sb.Append("                                CAN_MST_TBL          CMT,");
			sb.Append("                                CUSTOMER_MST_TBL       CONS,");
			sb.Append("                                CUSTOMER_MST_TBL       SHIP,");
			sb.Append("                                COMMODITY_GROUP_MST_TBL COMG,");
			sb.Append("                                (SELECT CITT.JOB_CARD_FK,");
			sb.Append("                                        CIT.CONSOL_INVOICE_PK,");
			sb.Append("                                        CIT.INVOICE_REF_NO,");
			sb.Append("                                        CIT.INVOICE_DATE,");
			sb.Append("                                        CTMT.CURRENCY_ID,");
			sb.Append("                                        CIT.CURRENCY_MST_FK,");
			sb.Append("                                        ROUND(SUM(NVL(CITT.TOT_AMT,0)*");
			sb.Append("                                         GET_EX_RATE(CITT.CURRENCY_MST_FK,CIT.CURRENCY_MST_FK,");
			sb.Append("                                         CIT.INVOICE_DATE)),2) TOT_AMT");
			sb.Append("                                   FROM CONSOL_INVOICE_TBL     CIT,");
			sb.Append("                                        CONSOL_INVOICE_TRN_TBL CITT,");
			sb.Append("                                        CURRENCY_TYPE_MST_TBL  CTMT");
			sb.Append("                                  WHERE CIT.CONSOL_INVOICE_PK =");
			sb.Append("                                        CITT.CONSOL_INVOICE_FK");
			sb.Append("                                    AND CTMT.CURRENCY_MST_PK =");
			sb.Append("                                        CIT.CURRENCY_MST_FK");
			sb.Append("                                  GROUP BY CITT.JOB_CARD_FK,");
			sb.Append("                                           CIT.CONSOL_INVOICE_PK,");
			sb.Append("                                           CIT.INVOICE_REF_NO,");
			sb.Append("                                           CIT.INVOICE_DATE,");
			sb.Append("                                           CIT.CURRENCY_MST_FK,");
			sb.Append("                                           CTMT.CURRENCY_ID) INVOICE,");
			//---------------------------------------------------------------------------------INVOICE
			if (RefType == "4") {
				if (!string.IsNullOrEmpty(RefNr)) {
					sb.Append("                                (SELECT JCONT.JOB_CARD_AIR_IMP_FK");
					sb.Append("                                         FROM JOB_TRN_CONT JCONT");
					sb.Append("                                       WHERE JCONT.COMMODITY_MST_FK IN");
					sb.Append("                                       (SELECT ROWTOCOL('SELECT C.COMMODITY_MST_PK FROM COMMODITY_MST_TBL C WHERE C.COMMODITY_ID IN (''" + RefNr + "'')')");
					sb.Append("                                        FROM DUAL))JOB_CONT,");
				}
			}
			sb.Append("                                (SELECT CIT.INVOICE_REF_NO,");
			sb.Append("                                        CT.COLLECTIONS_REF_NO,");
			sb.Append("                                        CT.COLLECTIONS_DATE,");
			sb.Append("                                        CTMT.CURRENCY_ID,");
			sb.Append("                                        CT.CURRENCY_MST_FK,");
			sb.Append("                                        ROUND(SUM(NVL(CTT.RECD_AMOUNT_HDR_CURR ,0)),");
			sb.Append("                                            2) COLL_AMT");
			sb.Append("                                   FROM COLLECTIONS_TBL       CT,");
			sb.Append("                                        COLLECTIONS_TRN_TBL   CTT,");
			sb.Append("                                        CONSOL_INVOICE_TBL    CIT,");
			sb.Append("                                        CURRENCY_TYPE_MST_TBL CTMT");
			sb.Append("                                  WHERE CT.COLLECTIONS_TBL_PK =");
			sb.Append("                                        CTT.COLLECTIONS_TBL_FK");
			sb.Append("                                    AND CIT.INVOICE_REF_NO = CTT.INVOICE_REF_NR");
			sb.Append("                                    AND CTMT.CURRENCY_MST_PK =");
			sb.Append("                                        CT.CURRENCY_MST_FK");
			sb.Append("                                  GROUP BY CIT.INVOICE_REF_NO,");
			sb.Append("                                           CT.COLLECTIONS_REF_NO,");
			sb.Append("                                           CT.COLLECTIONS_DATE,");
			sb.Append("                                           CT.CURRENCY_MST_FK,");
			sb.Append("                                           CTMT.CURRENCY_ID) COLLECTION,");
			//---------------------------------------------------------------------------------COLLECTION
			sb.Append("                                (SELECT JTAIC.JOB_CARD_AIR_IMP_FK,");
			sb.Append("                                        IST.INV_SUPPLIER_PK,");
			sb.Append("                                        IST.SUPPLIER_INV_NO,");
			sb.Append("                                        IST.SUPPLIER_INV_DT,");
			sb.Append("                                        CTMT.CURRENCY_ID,");
			sb.Append("                                        IST.CURRENCY_MST_FK,");
			sb.Append("                                        ROUND(SUM(NVL(ISTT.TOTAL_COST ,0)),");
			sb.Append("                                            2) INVOICE_AMT");
			sb.Append("                                   FROM INV_SUPPLIER_TBL      IST,");
			sb.Append("                                        INV_SUPPLIER_TRN_TBL  ISTT,");
			sb.Append("                                        CURRENCY_TYPE_MST_TBL CTMT,");
			sb.Append("                                        JOB_TRN_COST  JTAIC");
			sb.Append("                                  WHERE IST.INV_SUPPLIER_PK =");
			sb.Append("                                        ISTT.INV_SUPPLIER_TBL_FK");
			sb.Append("                                    AND CTMT.CURRENCY_MST_PK =");
			sb.Append("                                        IST.CURRENCY_MST_FK");
			sb.Append("                                    AND JTAIC.JOB_TRN_COST_PK(+) =");
			sb.Append("                                        ISTT.JOB_TRN_EST_FK");
			sb.Append("                                  GROUP BY JTAIC.JOB_CARD_AIR_IMP_FK,");
			sb.Append("                                           IST.INV_SUPPLIER_PK,");
			sb.Append("                                           IST.SUPPLIER_INV_NO,");
			sb.Append("                                           IST.SUPPLIER_INV_DT,");
			sb.Append("                                           IST.CURRENCY_MST_FK,");
			sb.Append("                                           CTMT.CURRENCY_ID) SUP_INV");
			sb.Append("                          WHERE POL.PORT_MST_PK = JCAIT.PORT_MST_POL_FK");
			sb.Append("                            AND POD.PORT_MST_PK = JCAIT.PORT_MST_POD_FK");
			sb.Append("                            AND INVOICE.JOB_CARD_FK(+) =");
			sb.Append("                                JCAIT.JOB_CARD_TRN_PK");
			sb.Append("                            AND COLLECTION.INVOICE_REF_NO(+) =");
			sb.Append("                                INVOICE.INVOICE_REF_NO");
			sb.Append("                            AND SUP_INV.JOB_CARD_AIR_IMP_FK(+) =");
			sb.Append("                                JCAIT.JOB_CARD_TRN_PK");
			sb.Append("                            AND JCAIT.JOB_CARD_TRN_PK =");
			sb.Append("                                JTAIF.JOB_CARD_AIR_IMP_FK");
			sb.Append("                            AND JCAIT.JOB_CARD_TRN_PK = CMT.JOB_CARD_FK(+)");
			sb.Append("                             AND JCAIT.SHIPPER_CUST_MST_FK=SHIP.CUSTOMER_MST_PK");
			sb.Append("                             AND JCAIT.CONSIGNEE_CUST_MST_FK=CONS.CUSTOMER_MST_PK");
			sb.Append("                             AND JCAIT.COMMODITY_GROUP_FK=COMG.COMMODITY_GROUP_PK(+)");
			sb.Append("   AND  POD.LOCATION_MST_FK=" + loc);
			if (Convert.ToInt32(RefType) > 0) {
				if (!string.IsNullOrEmpty(RefNr)) {
					if (RefType == "1") {
						sb.Append(" AND UPPER(SHIP.CUSTOMER_ID) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "2") {
						sb.Append(" AND UPPER(CONS.CUSTOMER_ID) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "3") {
						sb.Append(" AND UPPER(COMG.COMMODITY_GROUP_DESC) LIKE '" + RefNr.ToUpper().Replace("'", "''") + "%'");
					} else {
						sb.Append("  AND JOB_CONT.JOB_CARD_AIR_IMP_FK(+) = JCAIT.JOB_CARD_TRN_PK");
					}
				}
			}

			if (Convert.ToInt32(VesselPk) != 0) {
				sb.Append("  and JCAIT.airline_mst_fk =  " + VesselPk + " ");
			}

			if (!string.IsNullOrEmpty(txtJobRef.Trim()))
            {
				sb.Append(" AND UPPER(JCAIT.Jobcard_Ref_No) LIKE '%" + txtJobRef.Trim().ToUpper() + "%'");
			}

			if (PolPK != 0) {
				sb.Append("  AND POL.PORT_MST_PK = " + PolPK + "");
			}

			if (PodPK != 0) {
				sb.Append("  AND POD.PORT_MST_PK = " + PodPK + "");
			}

			if (!string.IsNullOrEmpty(txtHbl.Trim()))
            {
				sb.Append(" AND UPPER(JCAIT.HAWB_REF_NO) LIKE '%" + txtHbl.Trim().ToUpper() + "%'");
			}

			sb.Append("                          GROUP BY JCAIT.JOB_CARD_TRN_PK,");
			sb.Append("                                   JCAIT.JOBCARD_REF_NO,");
			sb.Append("                                   JOBCARD_DATE,");
			sb.Append("                                   JCAIT.HAWB_REF_NO,");
			sb.Append("                                   JCAIT.HAWB_DATE,");
			sb.Append("                                   JCAIT.DEPARTURE_DATE,");
			sb.Append("                                   POL.PORT_ID,");
			sb.Append("                                   POD.PORT_ID,");
			sb.Append("                                   CMT.BL_REF_NO,");
			sb.Append("                                   CMT.CAN_REF_NO,");
			sb.Append("                                   CMT.CAN_DATE,");
			sb.Append("                                   JCAIT.JOB_CARD_TRN_PK,");
			sb.Append("                                   INVOICE.CONSOL_INVOICE_PK,");
			sb.Append("                                   INVOICE.JOB_CARD_FK,");
			sb.Append("                                   INVOICE.INVOICE_REF_NO,");
			sb.Append("                                   INVOICE.INVOICE_DATE,");
			sb.Append("                                   INVOICE.CURRENCY_ID,");
			sb.Append("                                   INVOICE.CURRENCY_MST_FK,");
			sb.Append("                                   INVOICE.TOT_AMT,");
			sb.Append("                                   SUP_INV.JOB_CARD_AIR_IMP_FK,");
			sb.Append("                                   SUP_INV.INV_SUPPLIER_PK,");
			sb.Append("                                   SUP_INV.SUPPLIER_INV_NO,");
			sb.Append("                                   SUP_INV.SUPPLIER_INV_DT,");
			sb.Append("                                   SUP_INV.CURRENCY_ID,");
			sb.Append("                                   SUP_INV.CURRENCY_MST_FK,");
			sb.Append("                                   SUP_INV.INVOICE_AMT,");
			sb.Append("                                   COLLECTION.INVOICE_REF_NO,");
			sb.Append("                                   COLLECTION.COLLECTIONS_REF_NO,");
			sb.Append("                                   COLLECTION.COLLECTIONS_DATE,");
			sb.Append("                                   COLLECTION.CURRENCY_ID,");
			sb.Append("                                   COLLECTION.CURRENCY_MST_FK,");
			sb.Append("                                    JCAIT.JC_AUTO_MANUAL,");
			sb.Append("                                   COLLECTION.COLL_AMT ");
			sb.Append("                                   ) QRY ORDER BY TO_DATE(QRY.JCDT,DATEFORMAT) DESC,QRY.JCRNR DESC) T))");

			try {
				DataTable tbl = new DataTable();
				tbl = objWF.GetDataTable(sb.ToString());
				TotalRecords = (Int32)tbl.Rows.Count;
				TotalPage = TotalRecords / RecordsPerPage;
				if (TotalRecords % RecordsPerPage != 0) {
					TotalPage += 1;
				}
				if (CurrentPage > TotalPage)
					CurrentPage = 1;
				if (TotalRecords == 0)
					CurrentPage = 0;
				Last = CurrentPage * RecordsPerPage;
				Start = (CurrentPage - 1) * RecordsPerPage + 1;

				if (Export == 0) {
					sb.Append("  WHERE SNO  Between " + Start + " and " + Last + "");
				}
				return objWF.GetDataSet(sb.ToString());

			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch Cargo Tracking"
        /// <summary>
        /// Fetches the cargo track.
        /// </summary>
        /// <param name="CustomerPk">The customer pk.</param>
        /// <param name="TransporterPK">The transporter pk.</param>
        /// <param name="TPNNrPK">The TPN nr pk.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <param name="PendingFor">The pending for.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefNr">The reference nr.</param>
        /// <param name="History">The history.</param>
        /// <param name="Fromdt">The fromdt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="loc">The loc.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchCargoTrack(int CustomerPk = 0, int TransporterPK = 0, int TPNNrPK = 0, string CommGrp = "0", string PendingFor = "0", string RefType = "0", string RefNr = "", int History = 0, string Fromdt = "", string ToDt = "",
		Int32 Export = 0, int CurrentPage = 0, int TotalPage = 0, int loc = 0, int Flag = 0)
		{
			WorkFlow objWK = new WorkFlow();
			OracleCommand objCommand = new OracleCommand();
			DataSet dsData = new DataSet();

			try {
				objWK.OpenConnection();
				objWK.MyCommand.Connection = objWK.MyConnection;

				var _with1 = objWK.MyCommand;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".CARGO_TRACKING_PKG.CARGO_TRACKING_FETCH";

				objWK.MyCommand.Parameters.Clear();
				var _with2 = objWK.MyCommand.Parameters;

				_with2.Add("CUSTOMER_PK_IN", CustomerPk).Direction = ParameterDirection.Input;
				_with2.Add("TRANSPORTER_PK_IN", TransporterPK).Direction = ParameterDirection.Input;
				_with2.Add("TPNNR_PK_IN", TPNNrPK).Direction = ParameterDirection.Input;
				_with2.Add("COMM_GRP_IN", Convert.ToInt32(CommGrp)).Direction = ParameterDirection.Input;
				_with2.Add("PENDING_FOR_IN", Convert.ToInt32(PendingFor)).Direction = ParameterDirection.Input;
				_with2.Add("REF_TYPE_IN", Convert.ToInt32(RefType)).Direction = ParameterDirection.Input;
				_with2.Add("REF_NR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
				_with2.Add("FROM_DATE_IN", (string.IsNullOrEmpty(Fromdt) ? "" : Fromdt)).Direction = ParameterDirection.Input;
				_with2.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
				_with2.Add("LOCATION_PK_IN", loc).Direction = ParameterDirection.Input;
				_with2.Add("HISTORY_IN", History).Direction = ParameterDirection.Input;
				_with2.Add("POST_BACK_IN", Flag).Direction = ParameterDirection.Input;
				_with2.Add("EXPORT_EXCEL_IN", Export).Direction = ParameterDirection.Input;
				_with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
				_with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with2.Add("CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
				objWK.MyDataAdapter.Fill(dsData);
				TotalPage = Convert.ToInt32(objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value);
				CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				if (TotalPage == 0) {
					CurrentPage = 0;
				} else {
					CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				}
				return dsData;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "Save_CargoTracking"
        /// <summary>
        /// Save_s the cargo tracking.
        /// </summary>
        /// <param name="objDs">The object ds.</param>
        public void Save_CargoTracking(DataSet objDs)
		{
			WorkFlow objWK = new WorkFlow();
			OracleCommand cmd = new OracleCommand();
			int i = 0;
			System.DateTime ONWARDETAB1 = default(System.DateTime);
			System.DateTime ONWARDATAB1 = default(System.DateTime);
			System.DateTime ONWARDETDB1 = default(System.DateTime);
			System.DateTime ONWARDATDB1 = default(System.DateTime);
			System.DateTime ONWARDETAB2 = default(System.DateTime);
			System.DateTime ONWARDATAB2 = default(System.DateTime);
			System.DateTime ONWARDETDB2 = default(System.DateTime);
			System.DateTime ONWARDATDB2 = default(System.DateTime);
			System.DateTime ONWARDETAB3 = default(System.DateTime);
			System.DateTime ONWARDATAB3 = default(System.DateTime);
			System.DateTime ONWARDETDB3 = default(System.DateTime);
			System.DateTime ONWARDATDB3 = default(System.DateTime);
			System.DateTime ONWARDETAB4 = default(System.DateTime);
			System.DateTime ONWARDATAB4 = default(System.DateTime);
			System.DateTime ONWARDETDB4 = default(System.DateTime);
			System.DateTime ONWARDATDB4 = default(System.DateTime);
			System.DateTime ONWARDETAB5 = default(System.DateTime);
			System.DateTime ONWARDATAB5 = default(System.DateTime);
			System.DateTime ONWARDETDB5 = default(System.DateTime);
			System.DateTime ONWARDATDB5 = default(System.DateTime);
			System.DateTime RETURNETAB1 = default(System.DateTime);
			System.DateTime RETURNATAB1 = default(System.DateTime);
			System.DateTime RETURNETDB1 = default(System.DateTime);
			System.DateTime RETURNATDB1 = default(System.DateTime);
			System.DateTime RETURNETAB2 = default(System.DateTime);
			System.DateTime RETURNATAB2 = default(System.DateTime);
			System.DateTime RETURNETDB2 = default(System.DateTime);
			System.DateTime RETURNATDB2 = default(System.DateTime);
			System.DateTime RETURNETAB3 = default(System.DateTime);
			System.DateTime RETURNATAB3 = default(System.DateTime);
			System.DateTime RETURNETDB3 = default(System.DateTime);
			System.DateTime RETURNATDB3 = default(System.DateTime);
			System.DateTime RETURNETAB4 = default(System.DateTime);
			System.DateTime RETURNATAB4 = default(System.DateTime);
			System.DateTime RETURNETDB4 = default(System.DateTime);
			System.DateTime RETURNATDB4 = default(System.DateTime);
			System.DateTime RETURNETAB5 = default(System.DateTime);
			System.DateTime RETURNATAB5 = default(System.DateTime);
			System.DateTime RETURNETDB5 = default(System.DateTime);
			System.DateTime RETURNATDB5 = default(System.DateTime);
			System.DateTime EST_DEL = default(System.DateTime);
			System.DateTime ACT_DEL = default(System.DateTime);
			System.DateTime EST_MTDEL = default(System.DateTime);
			System.DateTime ACT_MTDEL = default(System.DateTime);
			System.DateTime DATE_BYCUST = default(System.DateTime);
			objWK.OpenConnection();
			try {
				for (i = 0; i <= objDs.Tables[0].Rows.Count - 1; i++) {
					var _with3 = objDs.Tables[0].Rows[i];
					//'ONWARD BORDER1
					if (!string.IsNullOrEmpty(_with3["ONWARD_ETAB1"].ToString())) {
						ONWARDETAB1 = Convert.ToDateTime(_with3["ONWARD_ETAB1"].ToString());
					} else {
						ONWARDETAB1 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ATAB1"].ToString())) {
						ONWARDATAB1 = Convert.ToDateTime(_with3["ONWARD_ATAB1"].ToString());
					} else {
						ONWARDATAB1 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ETDB1"].ToString())) {
						ONWARDETDB1 = Convert.ToDateTime(_with3["ONWARD_ETDB1"].ToString());
					} else {
						ONWARDETDB1 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ATDB1"].ToString())) {
						ONWARDATDB1 =  Convert.ToDateTime(_with3["ONWARD_ATDB1"].ToString());
					} else {
						ONWARDATDB1 = DateTime.MinValue;
					}
					//'ONWARD BORDER2
					if (!string.IsNullOrEmpty(_with3["ONWARD_ETAB2"].ToString())) {
						ONWARDETAB2 =  Convert.ToDateTime(_with3["ONWARD_ETAB2"].ToString());
					} else {
						ONWARDETAB2 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ATAB2"].ToString())) {
						ONWARDATAB2 =  Convert.ToDateTime(_with3["ONWARD_ATAB2"].ToString());
					} else {
						ONWARDATAB2 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ETDB2"].ToString())) {
						ONWARDETDB2 =  Convert.ToDateTime(_with3["ONWARD_ETDB2"].ToString());
					} else {
						ONWARDETDB2 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ATDB2"].ToString())) {
						ONWARDATDB2 =  Convert.ToDateTime(_with3["ONWARD_ATDB2"].ToString());
					} else {
						ONWARDATDB2 = DateTime.MinValue;
					}
					//'ONWARD BORDER3
					if (!string.IsNullOrEmpty(_with3["ONWARD_ETAB3"].ToString())) {
						ONWARDETAB3 =  Convert.ToDateTime(_with3["ONWARD_ETAB3"].ToString());
					} else {
						ONWARDETAB3 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ATAB3"].ToString())) {
						ONWARDATAB3 =  Convert.ToDateTime(_with3["ONWARD_ATAB3"].ToString());
					} else {
						ONWARDATAB3 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ETDB3"].ToString())) {
						ONWARDETDB3 =  Convert.ToDateTime(_with3["ONWARD_ETDB3"].ToString());
					} else {
						ONWARDETDB3 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ATDB3"].ToString())) {
						ONWARDATDB3 =  Convert.ToDateTime(_with3["ONWARD_ATDB3"].ToString());
					} else {
						ONWARDATDB3 = DateTime.MinValue;
					}
					//'ONWARD BORDER4
					if (!string.IsNullOrEmpty(_with3["ONWARD_ETAB4"].ToString())) {
						ONWARDETAB4 =  Convert.ToDateTime(_with3["ONWARD_ETAB4"].ToString());
					} else {
						ONWARDETAB4 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ATAB4"].ToString())) {
						ONWARDATAB4 =  Convert.ToDateTime(_with3["ONWARD_ATAB4"].ToString());
					} else {
						ONWARDATAB4 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ETDB4"].ToString())) {
						ONWARDETDB4 =  Convert.ToDateTime(_with3["ONWARD_ETDB4"].ToString());
					} else {
						ONWARDETDB4 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ATDB4"].ToString())) {
						ONWARDATDB4 =  Convert.ToDateTime(_with3["ONWARD_ATDB4"].ToString());
					} else {
						ONWARDATDB4 = DateTime.MinValue;
					}
					//'ONWARD BORDER5
					if (!string.IsNullOrEmpty(_with3["ONWARD_ETAB5"].ToString())) {
						ONWARDETAB5 =  Convert.ToDateTime(_with3["ONWARD_ETAB5"].ToString());
					} else {
						ONWARDETAB5 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ATAB5"].ToString())) {
						ONWARDATAB5 =  Convert.ToDateTime(_with3["ONWARD_ATAB5"].ToString());
					} else {
						ONWARDATAB5 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ETDB5"].ToString())) {
						ONWARDETDB5 =  Convert.ToDateTime(_with3["ONWARD_ETDB5"].ToString());
					} else {
						ONWARDETDB5 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ONWARD_ATDB5"].ToString())) {
						ONWARDATDB5 =  Convert.ToDateTime(_with3["ONWARD_ATDB5"].ToString());
					} else {
						ONWARDATDB5 = DateTime.MinValue;
					}
					//'RETURN BORDER1
					if (!string.IsNullOrEmpty(_with3["RETURN_ETAB1"].ToString())) {
						RETURNETAB1 =  Convert.ToDateTime(_with3["RETURN_ETAB1"].ToString());
					} else {
						RETURNETAB1 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ATAB1"].ToString())) {
						RETURNATAB1 =  Convert.ToDateTime(_with3["RETURN_ATAB1"].ToString());
					} else {
						RETURNATAB1 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ETDB1"].ToString())) {
						RETURNETDB1 =  Convert.ToDateTime(_with3["RETURN_ETDB1"].ToString());
					} else {
						RETURNETDB1 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ATDB1"].ToString())) {
						RETURNATDB1 =  Convert.ToDateTime(_with3["RETURN_ATDB1"].ToString());
					} else {
						RETURNATDB1 = DateTime.MinValue;
					}
					//'RETURN BORDER2
					if (!string.IsNullOrEmpty(_with3["RETURN_ETAB2"].ToString())) {
						RETURNETAB2 =  Convert.ToDateTime(_with3["RETURN_ETAB2"].ToString());
					} else {
						RETURNETAB2 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ATAB2"].ToString())) {
						RETURNATAB2 =  Convert.ToDateTime(_with3["RETURN_ATAB2"].ToString());
					} else {
						RETURNATAB2 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ETDB2"].ToString())) {
						RETURNETDB2 =  Convert.ToDateTime(_with3["RETURN_ETDB2"].ToString());
					} else {
						RETURNETDB2 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ATDB2"].ToString())) {
						RETURNATDB2 =  Convert.ToDateTime(_with3["RETURN_ATDB2"].ToString());
					} else {
						RETURNATDB2 = DateTime.MinValue;
					}
					//'RETURN BORDER3
					if (!string.IsNullOrEmpty(_with3["RETURN_ETAB3"].ToString())) {
						RETURNETAB3 =  Convert.ToDateTime(_with3["RETURN_ETAB3"].ToString());
					} else {
						RETURNETAB3 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ATAB3"].ToString())) {
						RETURNATAB3 =  Convert.ToDateTime(_with3["RETURN_ATAB3"].ToString());
					} else {
						RETURNATAB3 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ETDB3"].ToString())) {
						RETURNETDB3 =  Convert.ToDateTime(_with3["RETURN_ETDB3"].ToString());
					} else {
						RETURNETDB3 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ATDB3"].ToString())) {
						RETURNATDB3 =  Convert.ToDateTime(_with3["RETURN_ATDB3"].ToString());
					} else {
						RETURNATDB3 = DateTime.MinValue;
					}
					//'RETURN BORDER4
					if (!string.IsNullOrEmpty(_with3["RETURN_ETAB4"].ToString())) {
						RETURNETAB4 =  Convert.ToDateTime(_with3["RETURN_ETAB4"].ToString());
					} else {
						RETURNETAB4 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ATAB4"].ToString())) {
						RETURNATAB4 =  Convert.ToDateTime(_with3["RETURN_ATAB4"].ToString());
					} else {
						RETURNATAB4 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ETDB4"].ToString())) {
						RETURNETDB4 =  Convert.ToDateTime(_with3["RETURN_ETDB4"].ToString());
					} else {
						RETURNETDB4 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ATDB4"].ToString())) {
						RETURNATDB4 =  Convert.ToDateTime(_with3["RETURN_ATDB4"].ToString());
					} else {
						RETURNATDB4 = DateTime.MinValue;
					}
					//'RETURN BORDER5
					if (!string.IsNullOrEmpty(_with3["RETURN_ETAB5"].ToString())) {
						RETURNETAB5 =  Convert.ToDateTime(_with3["RETURN_ETAB5"].ToString());
					} else {
						RETURNETAB5 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ATAB5"].ToString())) {
						RETURNATAB5 =  Convert.ToDateTime(_with3["RETURN_ATAB5"].ToString());
					} else {
						RETURNATAB5 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ETDB5"].ToString())) {
						RETURNETDB5 =  Convert.ToDateTime(_with3["RETURN_ETDB5"].ToString());
					} else {
						RETURNETDB5 = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["RETURN_ATDB5"].ToString())) {
						RETURNATDB5 =  Convert.ToDateTime(_with3["RETURN_ATDB5"].ToString());
					} else {
						RETURNATDB5 = DateTime.MinValue;
					}
					//'
					if (!string.IsNullOrEmpty(_with3["EST_DATE_DEL"].ToString())) {
						EST_DEL =  Convert.ToDateTime(_with3["EST_DATE_DEL"].ToString());
					} else {
						EST_DEL = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ACT_DATE_DEL"].ToString())) {
						ACT_DEL =  Convert.ToDateTime(_with3["ACT_DATE_DEL"].ToString());
					} else {
						ACT_DEL = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["EST_DATE_MTRET"].ToString())) {
						EST_MTDEL =  Convert.ToDateTime(_with3["EST_DATE_MTRET"].ToString());
					} else {
						EST_MTDEL = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["ACT_DATE_MTRET"].ToString())) {
						ACT_MTDEL =  Convert.ToDateTime(_with3["ACT_DATE_MTRET"]);
					} else {
						ACT_MTDEL = DateTime.MinValue;
					}
					if (!string.IsNullOrEmpty(_with3["CTR_RETURN_DT_BYCUST"].ToString())) {
						DATE_BYCUST =  Convert.ToDateTime(_with3["CTR_RETURN_DT_BYCUST"].ToString());
					} else {
						DATE_BYCUST = DateTime.MinValue;
					}

					var _with4 = cmd;
					_with4.Connection = objWK.MyConnection;
					_with4.CommandType = CommandType.StoredProcedure;
					_with4.CommandText = objWK.MyUserName + ".CARGO_TRACKING_PKG.CARGO_TRUCK_UPD";

					var _with5 = _with4.Parameters;
					_with5.Clear();
					_with5.Add("TPN_PK_IN", objDs.Tables[0].Rows[i]["TRANSPORT_INST_SEA_PK"]).Direction = ParameterDirection.Input;
					_with5.Add("TRUCK_PK_IN", objDs.Tables[0].Rows[i]["TRANSPORT_TRN_TRUCK_PK"]).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ETAB1_IN", getDefault((ONWARDETAB1 == DateTime.MinValue ? DateTime.MinValue : ONWARDETAB1), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ATAB1_IN", getDefault((ONWARDATAB1 == DateTime.MinValue ? DateTime.MinValue : ONWARDATAB1), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ETDB1_IN", getDefault((ONWARDETDB1 == DateTime.MinValue ? DateTime.MinValue : ONWARDETDB1), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ATDB1_IN", getDefault((ONWARDATDB1 == DateTime.MinValue ? DateTime.MinValue : ONWARDATDB1), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ETAB2_IN", getDefault((ONWARDETAB2 == DateTime.MinValue ? DateTime.MinValue : ONWARDETAB2), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ATAB2_IN", getDefault((ONWARDATAB2 == DateTime.MinValue ? DateTime.MinValue : ONWARDATAB2), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ETDB2_IN", getDefault((ONWARDETDB2 == DateTime.MinValue ? DateTime.MinValue : ONWARDETDB2), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ATDB2_IN", getDefault((ONWARDATDB2 == DateTime.MinValue ? DateTime.MinValue : ONWARDATDB2), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ETAB3_IN", getDefault((ONWARDETAB3 == DateTime.MinValue ? DateTime.MinValue : ONWARDETAB3), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ATAB3_IN", getDefault((ONWARDATAB3 == DateTime.MinValue ? DateTime.MinValue : ONWARDATAB3), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ETDB3_IN", getDefault((ONWARDETDB3 == DateTime.MinValue ? DateTime.MinValue : ONWARDETDB3), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ATDB3_IN", getDefault((ONWARDATDB3 == DateTime.MinValue ? DateTime.MinValue : ONWARDATDB3), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ETAB4_IN", getDefault((ONWARDETAB4 == DateTime.MinValue ? DateTime.MinValue : ONWARDETAB4), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ATAB4_IN", getDefault((ONWARDATAB4 == DateTime.MinValue ? DateTime.MinValue : ONWARDATAB4), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ETDB4_IN", getDefault((ONWARDETDB4 == DateTime.MinValue ? DateTime.MinValue : ONWARDETDB4), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ATDB4_IN", getDefault((ONWARDATDB4 == DateTime.MinValue ? DateTime.MinValue : ONWARDATDB4), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ETAB5_IN", getDefault((ONWARDETAB5 == DateTime.MinValue ? DateTime.MinValue : ONWARDETAB5), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ATAB5_IN", getDefault((ONWARDATAB5 == DateTime.MinValue ? DateTime.MinValue : ONWARDATAB5), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ETDB5_IN", getDefault((ONWARDETDB5 == DateTime.MinValue ? DateTime.MinValue : ONWARDETDB5), "")).Direction = ParameterDirection.Input;
					_with5.Add("ONWARD_ATDB5_IN", getDefault((ONWARDATDB5 == DateTime.MinValue ? DateTime.MinValue : ONWARDATDB5), "")).Direction = ParameterDirection.Input;
					//'
					_with5.Add("RETURN_ETAB1_IN", getDefault((RETURNETAB1 == DateTime.MinValue ? DateTime.MinValue : RETURNETAB1), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ATAB1_IN", getDefault((RETURNATAB1 == DateTime.MinValue ? DateTime.MinValue : RETURNATAB1), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ETDB1_IN", getDefault((RETURNETDB1 == DateTime.MinValue ? DateTime.MinValue : RETURNETDB1), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ATDB1_IN", getDefault((RETURNATDB1 == DateTime.MinValue ? DateTime.MinValue : RETURNATDB1), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ETAB2_IN", getDefault((RETURNETAB2 == DateTime.MinValue ? DateTime.MinValue : RETURNETAB2), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ATAB2_IN", getDefault((RETURNATAB2 == DateTime.MinValue ? DateTime.MinValue : RETURNATAB2), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ETDB2_IN", getDefault((RETURNETDB2 == DateTime.MinValue ? DateTime.MinValue : RETURNETDB2), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ATDB2_IN", getDefault((RETURNATDB2 == DateTime.MinValue ? DateTime.MinValue : RETURNATDB2), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ETAB3_IN", getDefault((RETURNETAB3 == DateTime.MinValue ? DateTime.MinValue : RETURNETAB3), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ATAB3_IN", getDefault((RETURNATAB3 == DateTime.MinValue ? DateTime.MinValue : RETURNATAB3), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ETDB3_IN", getDefault((RETURNETDB3 == DateTime.MinValue ? DateTime.MinValue : RETURNETDB3), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ATDB3_IN", getDefault((RETURNATDB3 == DateTime.MinValue ? DateTime.MinValue : RETURNATDB3), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ETAB4_IN", getDefault((RETURNETAB4 == DateTime.MinValue ? DateTime.MinValue : RETURNETAB4), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ATAB4_IN", getDefault((RETURNATAB4 == DateTime.MinValue ? DateTime.MinValue : RETURNATAB4), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ETDB4_IN", getDefault((RETURNETDB4 == DateTime.MinValue ? DateTime.MinValue : RETURNETDB4), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ATDB4_IN", getDefault((RETURNATDB4 == DateTime.MinValue ? DateTime.MinValue : RETURNATDB4), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ETAB5_IN", getDefault((RETURNETAB5 == DateTime.MinValue ? DateTime.MinValue : RETURNETAB5), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ATAB5_IN", getDefault((RETURNATAB5 == DateTime.MinValue ? DateTime.MinValue : RETURNATAB5), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ETDB5_IN", getDefault((RETURNETDB5 == DateTime.MinValue ? DateTime.MinValue : RETURNETDB5), "")).Direction = ParameterDirection.Input;
					_with5.Add("RETURN_ATDB5_IN", getDefault((RETURNATDB5 == DateTime.MinValue ? DateTime.MinValue : RETURNATDB5), "")).Direction = ParameterDirection.Input;

					_with5.Add("EST_DATE_DEL_IN", getDefault((EST_DEL == DateTime.MinValue ? DateTime.MinValue : EST_DEL), "")).Direction = ParameterDirection.Input;
					_with5.Add("ACT_DATE_DEL_IN", getDefault((ACT_DEL == DateTime.MinValue ? DateTime.MinValue : ACT_DEL), "")).Direction = ParameterDirection.Input;
					_with5.Add("CTR_RETURN_DT_BYCUST_IN", getDefault((DATE_BYCUST == DateTime.MinValue ? DateTime.MinValue : DATE_BYCUST), "")).Direction = ParameterDirection.Input;
					_with5.Add("EST_DATE_MTRET_IN", getDefault((EST_MTDEL == DateTime.MinValue ? DateTime.MinValue : EST_MTDEL), "")).Direction = ParameterDirection.Input;
					_with5.Add("ACT_DATE_MTRET_IN", getDefault((ACT_MTDEL == DateTime.MinValue ? DateTime.MinValue : ACT_MTDEL), "")).Direction = ParameterDirection.Input;

					_with5.Add("TRACK_PK1_IN", objDs.Tables[0].Rows[i]["TRACKPKOB1"]).Direction = ParameterDirection.Input;
					_with5.Add("TRACK_PK2_IN", objDs.Tables[0].Rows[i]["TRACKPKOB2"]).Direction = ParameterDirection.Input;
					_with5.Add("TRACK_PK3_IN", objDs.Tables[0].Rows[i]["TRACKPKOB3"]).Direction = ParameterDirection.Input;
					_with5.Add("TRACK_PK4_IN", objDs.Tables[0].Rows[i]["TRACKPKOB4"]).Direction = ParameterDirection.Input;
					_with5.Add("TRACK_PK5_IN", objDs.Tables[0].Rows[i]["TRACKPKOB5"]).Direction = ParameterDirection.Input;
					_with5.Add("TRACK_PK6_IN", objDs.Tables[0].Rows[i]["TRACKPKRB1"]).Direction = ParameterDirection.Input;
					_with5.Add("TRACK_PK7_IN", objDs.Tables[0].Rows[i]["TRACKPKRB2"]).Direction = ParameterDirection.Input;
					_with5.Add("TRACK_PK8_IN", objDs.Tables[0].Rows[i]["TRACKPKRB3"]).Direction = ParameterDirection.Input;
					_with5.Add("TRACK_PK9_IN", objDs.Tables[0].Rows[i]["TRACKPKRB4"]).Direction = ParameterDirection.Input;
					_with5.Add("TRACK_PK10_IN", objDs.Tables[0].Rows[i]["TRACKPKRB5"]).Direction = ParameterDirection.Input;
					_with5.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
					cmd.ExecuteNonQuery();

				}

				objWK.CloseConnection();
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "Fetch ExCeptionRpt"
        /// <summary>
        /// Fetches the ex ception RPT.
        /// </summary>
        /// <param name="LocationPk">The location pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="CustomerPk">The customer pk.</param>
        /// <param name="LinePk">The line pk.</param>
        /// <param name="VoyagePk">The voyage pk.</param>
        /// <param name="VoyageNr">The voyage nr.</param>
        /// <param name="PolPk">The pol pk.</param>
        /// <param name="PodPk">The pod pk.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefNr">The reference nr.</param>
        /// <param name="Fromdt">The fromdt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ddlExceptionBase">The DDL exception base.</param>
        /// <param name="ddlVersusTo">The DDL versus to.</param>
        /// <param name="ChkLoad">The CHK load.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchExCeptionRpt(string LocationPk = "0", int BizType = 0, int ProcessType = 0, string CustomerPk = "0", string LinePk = "0", string VoyagePk = "0", string VoyageNr = "", string PolPk = "0", string PodPk = "0", string RefType = "0",
		string RefNr = "", string Fromdt = "", string ToDt = "", string ddlExceptionBase = "", string ddlVersusTo = "", Int32 ChkLoad = 0, Int32 Export = 0, int CurrentPage = 0, int TotalPage = 0)
		{

			WorkFlow objWK = new WorkFlow();
			OracleCommand objCommand = new OracleCommand();
			DataSet dsData = new DataSet();

			try {
				objWK.OpenConnection();
				objWK.MyCommand.Connection = objWK.MyConnection;

				var _with6 = objWK.MyCommand;
				_with6.CommandType = CommandType.StoredProcedure;
				_with6.CommandText = objWK.MyUserName + ".CARGO_TRACKING_PKG.EXCEPTION_REPORT_FETCH";

				objWK.MyCommand.Parameters.Clear();
				var _with7 = objWK.MyCommand.Parameters;
				_with7.Add("LOCATION_PK_IN", (LocationPk == "0" ? "" : LocationPk)).Direction = ParameterDirection.Input;
				_with7.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with7.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
				_with7.Add("CUSTOMER_PK_IN", (CustomerPk == "0" ? "" : CustomerPk)).Direction = ParameterDirection.Input;
				_with7.Add("LINE_PK_IN", (LinePk == "0" ? "" : LinePk)).Direction = ParameterDirection.Input;
				_with7.Add("VESSEL_PK_IN", (VoyagePk == "0" ? "" : VoyagePk)).Direction = ParameterDirection.Input;
				_with7.Add("VOYAGE_NR_IN", (string.IsNullOrEmpty(VoyageNr) ? "" : VoyageNr)).Direction = ParameterDirection.Input;
				_with7.Add("POL_PK_IN", (PolPk == "0" ? "" : PolPk)).Direction = ParameterDirection.Input;
				_with7.Add("POD_PK_IN", (PodPk == "0" ? "" : PodPk)).Direction = ParameterDirection.Input;
				_with7.Add("REF_TYPE_IN", Convert.ToInt32(RefType)).Direction = ParameterDirection.Input;
				_with7.Add("REF_NR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
				_with7.Add("FROM_DATE_IN", (string.IsNullOrEmpty(Fromdt) ? "" : Fromdt)).Direction = ParameterDirection.Input;
				_with7.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
				_with7.Add("EXCEP_BASE_IN", ddlExceptionBase).Direction = ParameterDirection.Input;
				_with7.Add("VERSUS_TO_IN", ddlVersusTo).Direction = ParameterDirection.Input;
				_with7.Add("POST_BACK_IN", ChkLoad).Direction = ParameterDirection.Input;
				_with7.Add("EXPORT_EXCEL_IN", Export).Direction = ParameterDirection.Input;
				_with7.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
				_with7.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with7.Add("BASE_VERSUS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with7.Add("EXCEPTION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
				objWK.MyDataAdapter.Fill(dsData);
				TotalPage = Convert.ToInt32(objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value);
				CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				if (TotalPage == 0) {
					CurrentPage = 0;
				} else {
					CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				}
				CreateRelation(dsData);
				return dsData;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Creates the relation.
        /// </summary>
        /// <param name="dsData">The ds data.</param>
        private void CreateRelation(DataSet dsData)
		{
			DataRelation drCOUNT = null;
			try {
				drCOUNT = new DataRelation("COUNTRY", dsData.Tables[0].Columns["BASE_VERSUS"], dsData.Tables[1].Columns["BASE_VERSUS"]);
				drCOUNT.Nested = true;
				dsData.Relations.Add(drCOUNT);
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion


	}
}
