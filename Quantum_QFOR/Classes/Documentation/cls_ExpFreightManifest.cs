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
using System.Data;
namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_ExpFreightManifest : CommonFeatures
	{
        #region "Fetch On Search for Export Freight SEA"
        /// <summary>
        /// Fetches all exp FRT.
        /// </summary>
        /// <param name="VesselPk">The vessel pk.</param>
        /// <param name="txtJobRefPk">The text job reference pk.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefId">The reference identifier.</param>
        /// <param name="txtHblPk">The text HBL pk.</param>
        /// <param name="txtBookingPk">The text booking pk.</param>
        /// <param name="Excel">The excel.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="BCurrencyPK">The b currency pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <returns></returns>
        public DataSet FetchAllExpFrt(string VesselPk = "", string txtJobRefPk = "", int POLPK = 0, int PODPK = 0, string RefType = "", string RefId = "", string txtHblPk = "", string txtBookingPk = "", Int32 Excel = 0, int CurrentPage = 0,
		int TotalPage = 0, int BCurrencyPK = 0, Int32 LocFk = 0)
		{
			Int32 Last = default(Int32);
			Int32 Start = default(Int32);
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT JCSET.JOB_CARD_TRN_PK,");
			sb.Append("       JCSET.JOBCARD_REF_NO,");
			sb.Append("       TO_DATE(JCSET.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
			sb.Append("       nvl(HET.HBL_EXP_TBL_PK,0) HBL_EXP_TBL_PK,");
			sb.Append("       NVL(HET.HBL_REF_NO,' ') HBL_REF_NO,");
			sb.Append("       NVL(HET.HBL_STATUS,0) HBL_STATUS,");
			sb.Append("       TO_DATE(HET.HBL_DATE, DATEFORMAT) HBL_DATE,");
			sb.Append("       NVL(MET.MBL_EXP_TBL_PK,0) MBL_EXP_TBL_PK,");
			sb.Append("       NVL(MET.MBL_REF_NO,' ') MBL_REF_NO,");
			sb.Append("       TO_DATE(MET.MBL_DATE, DATEFORMAT) MBL_DATE,");
			sb.Append("       Nvl(SUM(JTSEF.FREIGHT_AMT*get_ex_rate(jtsef.currency_mst_fk," + BCurrencyPK + ",JCSET.JOBCARD_DATE)),0) AIF,");
			sb.Append("       Nvl(SUM(CASE");
			sb.Append("             WHEN JTSEF.FREIGHT_TYPE = 1 THEN");
			sb.Append("              JTSEF.FREIGHT_AMT*get_ex_rate(jtsef.currency_mst_fk," + BCurrencyPK + ",JCSET.JOBCARD_DATE)");
			sb.Append("           END),0) PREPAID,");
			sb.Append("       Nvl(SUM(CASE");
			sb.Append("             WHEN JTSEF.FREIGHT_TYPE = 2 THEN");
			sb.Append("              JTSEF.FREIGHT_AMT*get_ex_rate(jtsef.currency_mst_fk," + BCurrencyPK + ",JCSET.JOBCARD_DATE)");
			sb.Append("           END),0) COLLECT,");
			sb.Append("       CASE");
			sb.Append("         WHEN JCSET.DEPARTURE_DATE IS NOT NULL THEN");
			sb.Append("          'SAILED'");
			sb.Append("         ELSE");
			sb.Append("          'NOT SAILED'");
			sb.Append("       END VESSEL_STATUS,");
			sb.Append("       CASE");
			sb.Append("         WHEN (SELECT DISTINCT J.LOAD_DATE");
			sb.Append("                 FROM JOB_TRN_CONT J");
			sb.Append("                WHERE (J.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK)");
			sb.Append("                  AND ROWNUM = 1) IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END LOAD_CONFIRM,");
			sb.Append("       POL.PORT_ID POL,");
			sb.Append("       POD.PORT_ID POD,");
			sb.Append("       BST.BOOKING_MST_PK,");
			sb.Append("       BST.BOOKING_REF_NO,");
			sb.Append("       TO_DATE(BST.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
			sb.Append("       BST.CARGO_TYPE,");
			sb.Append("       NVL(SUM(JTSEF.FREIGHT_AMT*GET_EX_RATE(jtsef.currency_mst_fk," + BCurrencyPK + ",JCSET.JOBCARD_DATE)), 0) +");
			sb.Append("       (SELECT NVL(SUM(JTOTH.AMOUNT*GET_EX_RATE(JTOTH.CURRENCY_MST_FK," + BCurrencyPK + ",JCSET.JOBCARD_DATE)), 0)");
			sb.Append("          FROM JOB_TRN_OTH_CHRG JTOTH");
			sb.Append("         WHERE JTOTH.JOB_CARD_TRN_FK(+) = JCSET.JOB_CARD_TRN_PK) RECEIVABLE,");
			sb.Append("       CASE");
			sb.Append("         WHEN INVOICE.JOB_CARD_FK IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END INV,");
			sb.Append("       NVL(INVOICE.CONSOL_INVOICE_PK,0) CONSOL_INVOICE_PK,");
			sb.Append("       NVL(INVOICE.INVOICE_REF_NO,' ') INVOICE_REF_NO,");
			sb.Append("       TO_DATE(INVOICE.INVOICE_DATE, DATEFORMAT) INVOICE_DATE,");
			sb.Append("       NVL(INVOICE.CURRENCY_ID,' ') INV_CUR,");
			sb.Append("       Nvl(INVOICE.TOT_AMT,0) INVOICE_AMOUNT,");
			sb.Append("       CASE");
			sb.Append("         WHEN SUP_INV.JOB_CARD_TRN_FK IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END SUP_INV,");
			sb.Append("       NVL(SUP_INV.INV_SUPPLIER_PK,0) INV_SUPPLIER_PK,");
			sb.Append("       NVL(SUP_INV.SUPPLIER_INV_NO,' ') SUPPLIER_INV_NO,");
			sb.Append("       TO_DATE(SUP_INV.SUPPLIER_INV_DT, DATEFORMAT) SUPPLIER_INV_DT,");
			sb.Append("       NVL(SUP_INV.CURRENCY_ID,' ') SUP_INV_CUR,");
			sb.Append("       Nvl(SUP_INV.INVOICE_AMT,0) SUP_INV_AMT,");
			sb.Append("       CASE");
			sb.Append("         WHEN COLLECTION.INVOICE_REF_NO IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         ELSE");
			sb.Append("          'r'");
			sb.Append("       END COLLECTION,");
			sb.Append("       NVL(COLLECTION.COLLECTIONS_REF_NO,' ') COLREF,");
			sb.Append("       TO_DATE(COLLECTION.COLLECTIONS_DATE, DATEFORMAT) COLREFDT,");
			sb.Append("       NVL(COLLECTION.CURRENCY_ID,' ') COLCUR,");
			sb.Append("       Nvl(COLLECTION.COLL_AMT,0) COLAMT,");
			sb.Append("       (SELECT CMT11.CURRENCY_ID");
			sb.Append("          FROM CURRENCY_TYPE_MST_TBL CMT11");
			sb.Append("         WHERE CMT11.CURRENCY_MST_PK = " + BCurrencyPK + ") OSCUR,");
			sb.Append("       NVL((NVL(INVOICE.TOT_AMT,0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE)+ ");
			sb.Append("       NVL(SUP_INV.INVOICE_AMT,0)*GET_EX_RATE(SUP_INV.CURRENCY_MST_FK," + BCurrencyPK + ",SUP_INV.SUPPLIER_INV_DT)) -");
			sb.Append("       NVL(COLLECTION.COLL_AMT,0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("       0) OS,");
			sb.Append("      CASE WHEN ");
			sb.Append("       NVL((NVL(INVOICE.TOT_AMT,0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE)+ ");
			sb.Append("       NVL(SUP_INV.INVOICE_AMT,0)*GET_EX_RATE(SUP_INV.CURRENCY_MST_FK," + BCurrencyPK + ",SUP_INV.SUPPLIER_INV_DT)) -");
			sb.Append("       NVL(COLLECTION.COLL_AMT,0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("       0) >0 THEN ");
			sb.Append("      ROUND(SYSDATE - TO_DATE(INVOICE.INVOICE_DATE)) ");
			sb.Append("      ELSE 0 END AGE ");
			sb.Append("  FROM JOB_CARD_TRN JCSET,");
			sb.Append("       HBL_EXP_TBL HET,");
			sb.Append("       MBL_EXP_TBL MET,");
			sb.Append("       BOOKING_MST_TBL BST,");
			sb.Append("       PORT_MST_TBL POL,");
			sb.Append("       PORT_MST_TBL POD,");
			sb.Append("       JOB_TRN_FD JTSEF,");
			sb.Append("       VESSEL_VOYAGE_TBL VVT,");
			sb.Append("       VESSEL_VOYAGE_TRN VVTRN,");
			sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
			sb.Append("       CUSTOMER_MST_TBL SHIPPER,");
			sb.Append("       CUSTOMER_MST_TBL CONSIGNEE,");
			sb.Append("       (SELECT CITT.JOB_CARD_FK,");
			sb.Append("               CIT.CONSOL_INVOICE_PK,");
			sb.Append("               CIT.INVOICE_REF_NO,");
			sb.Append("               CIT.INVOICE_DATE,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               CIT.CURRENCY_MST_FK,");
			sb.Append("               ROUND(NVL(SUM(CITT.TOT_AMT*");
			sb.Append("                             GET_EX_RATE(CITT.CURRENCY_MST_FK,");
			sb.Append("                                         CIT.CURRENCY_MST_FK,");
			sb.Append("                                         CIT.INVOICE_DATE)),0),");
			sb.Append("                   0) TOT_AMT");
			sb.Append("          FROM CONSOL_INVOICE_TBL     CIT,");
			sb.Append("               CONSOL_INVOICE_TRN_TBL CITT,");
			sb.Append("               CURRENCY_TYPE_MST_TBL  CTMT");
			sb.Append("         WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
			sb.Append("           AND CIT.CHK_INVOICE <> 2");
			sb.Append("           AND CTMT.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK");
			sb.Append("         GROUP BY CITT.JOB_CARD_FK,");
			sb.Append("                  CIT.CONSOL_INVOICE_PK,");
			sb.Append("                  CIT.INVOICE_REF_NO,");
			sb.Append("                  CIT.INVOICE_DATE,");
			sb.Append("                  CTMT.CURRENCY_ID,");
			sb.Append("                  CIT.CURRENCY_MST_FK) INVOICE,");
			sb.Append("       (SELECT CIT.INVOICE_REF_NO,");
			sb.Append("               CT.COLLECTIONS_REF_NO,");
			sb.Append("               CT.COLLECTIONS_DATE,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               CT.CURRENCY_MST_FK,");
			sb.Append("               ROUND(SUM(NVL(CTT.RECD_AMOUNT_HDR_CURR,0) ");
			sb.Append("                   ),2) COLL_AMT");
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
			sb.Append("                  CTMT.CURRENCY_ID,");
			sb.Append("                  CT.CURRENCY_MST_FK) COLLECTION,");
			if (RefType == "5") {
				if (!string.IsNullOrEmpty(RefId)) {
					sb.Append("       (SELECT JCONT.JOB_CARD_TRN_FK");
					sb.Append("          FROM JOB_TRN_CONT JCONT");
					sb.Append("         WHERE JCONT.COMMODITY_MST_FKS IN");
					sb.Append("               (SELECT ROWTOCOL('SELECT C.COMMODITY_MST_PK FROM COMMODITY_MST_TBL C WHERE C.COMMODITY_ID IN (''" + RefId + "'') ')");
					sb.Append("                  FROM DUAL)) JOB_CONT,");
				}
			}
			sb.Append("       (SELECT JTSEC.JOB_CARD_TRN_FK,");
			sb.Append("               IST.INV_SUPPLIER_PK,");
			sb.Append("               IST.SUPPLIER_INV_NO,");
			sb.Append("               IST.SUPPLIER_INV_DT,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               IST.CURRENCY_MST_FK,");
			sb.Append("               ROUND(SUM(NVL(ISTT.TOTAL_COST,0)");
			sb.Append("                   ),2) INVOICE_AMT");
			sb.Append("          FROM INV_SUPPLIER_TBL      IST,");
			sb.Append("               INV_SUPPLIER_TRN_TBL  ISTT,");
			sb.Append("               CURRENCY_TYPE_MST_TBL CTMT,");
			sb.Append("               JOB_TRN_COST  JTSEC");
			sb.Append("         WHERE IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
			sb.Append("           AND CTMT.CURRENCY_MST_PK = IST.CURRENCY_MST_FK");
			sb.Append("           AND IST.APPROVED <> 3 ");
			sb.Append("           AND JTSEC.JOB_TRN_COST_PK(+) = ISTT.JOB_TRN_EST_FK");
			sb.Append("         GROUP BY JTSEC.JOB_CARD_TRN_FK,");
			sb.Append("                  IST.INV_SUPPLIER_PK,");
			sb.Append("                  IST.SUPPLIER_INV_NO,");
			sb.Append("                  IST.SUPPLIER_INV_DT,");
			sb.Append("                  CTMT.CURRENCY_ID,");
			sb.Append("                  IST.CURRENCY_MST_FK) SUP_INV ");
			sb.Append(" WHERE HET.HBL_EXP_TBL_PK(+) = JCSET.HBL_HAWB_FK");
			sb.Append("   AND MET.MBL_EXP_TBL_PK(+) = JCSET.MBL_MAWB_FK");
			sb.Append("   AND BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK");
			sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
			sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
			sb.Append("   AND INVOICE.JOB_CARD_FK(+) = JCSET.JOB_CARD_TRN_PK");
			sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK = VVTRN.VESSEL_VOYAGE_TBL_FK");
			sb.Append("   AND VVTRN.VOYAGE_TRN_PK = JCSET.VOYAGE_TRN_FK");
			sb.Append("   AND COLLECTION.INVOICE_REF_NO(+) = INVOICE.INVOICE_REF_NO");
			sb.Append("   AND SUP_INV.JOB_CARD_TRN_FK(+) = JCSET.JOB_CARD_TRN_PK");
			sb.Append("   AND JCSET.JOB_CARD_TRN_PK = JTSEF.JOB_CARD_TRN_FK");
			sb.Append("     AND SHIPPER.CUSTOMER_MST_PK(+) = JCSET.SHIPPER_CUST_MST_FK");
			sb.Append("     AND CONSIGNEE.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
			sb.Append("     AND JCSET.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
			sb.Append("     AND BST.STATUS <>3");
			sb.Append("     AND JCSET.BUSINESS_TYPE = 2 ");
			sb.Append("     AND JCSET.PROCESS_TYPE = 1 ");
			sb.Append("     AND POL.PORT_MST_PK IN (SELECT LWPT.PORT_MST_FK FROM LOCATION_WORKING_PORTS_TRN LWPT");
			sb.Append("     WHERE LWPT.ACTIVE = 1");
			sb.Append("     AND LWPT.LOCATION_MST_FK = " + LocFk + ")");
			if (Convert.ToInt32(txtJobRefPk) != 0) {
				sb.Append("  AND JCSET.JOB_CARD_TRN_PK = " + txtJobRefPk + "");
			}
			if (Convert.ToInt32(txtHblPk) != 0) {
				sb.Append("  AND HET.HBL_EXP_TBL_PK = " + txtHblPk + "");
			}
			if (Convert.ToInt32(txtBookingPk) != 0) {
				sb.Append("  AND BST.BOOKING_MST_PK = " + txtBookingPk + "");
			}
			if (POLPK != 0) {
				sb.Append("  AND POL.PORT_MST_PK  = " + POLPK + "");
			}
			if (PODPK != 0) {
				sb.Append("  AND POD.PORT_MST_PK = " + PODPK + "");
			}
			if (Convert.ToInt32(VesselPk) != 0) {
				sb.Append(" AND VVTRN.VOYAGE_TRN_PK = " + VesselPk + "");
			}
			if (Convert.ToInt32(RefType) > 0) {
				if (!string.IsNullOrEmpty(RefId)) {
					if (RefType == "1") {
						sb.Append(" AND UPPER(SHIPPER.CUSTOMER_ID) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "2") {
						sb.Append(" AND  UPPER(CONSIGNEE.CUSTOMER_ID) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "3") {
						if (RefId == "FCL" | RefId == "fcl") {
							sb.Append(" AND  BST.CARGO_TYPE = 1");
						} else if (RefId == "LCL" | RefId == "lcl") {
							sb.Append(" AND  BST.CARGO_TYPE = 2");
						} else if (RefId == "BBC" | RefId == "Break Bulk") {
							sb.Append(" AND  BST.CARGO_TYPE = 4");
						}
					} else if (RefType == "4") {
						sb.Append(" AND UPPER(CGMT.COMMODITY_GROUP_DESC) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else {
						sb.Append("   AND JOB_CONT.JOB_CARD_TRN_FK(+) = JCSET.JOB_CARD_TRN_PK");
					}
				}
			}
			sb.Append(" GROUP BY JCSET.JOB_CARD_TRN_PK,");
			sb.Append("          JCSET.JOBCARD_REF_NO,");
			sb.Append("          JOBCARD_DATE,");
			sb.Append("          HET.HBL_EXP_TBL_PK,");
			sb.Append("          HET.HBL_REF_NO,");
			sb.Append("          HET.HBL_STATUS,");
			sb.Append("          HET.HBL_DATE,");
			sb.Append("          JCSET.DEPARTURE_DATE,");
			sb.Append("          POL.PORT_ID,");
			sb.Append("          POD.PORT_ID,");
			sb.Append("          BST.BOOKING_REF_NO,");
			sb.Append("          BST.BOOKING_DATE,");
			sb.Append("          BST.CARGO_TYPE,");
			sb.Append("          JCSET.JOB_CARD_TRN_PK,");
			sb.Append("          BST.BOOKING_MST_PK,");
			sb.Append("          INVOICE.CONSOL_INVOICE_PK,");
			sb.Append("          INVOICE.JOB_CARD_FK,");
			sb.Append("          INVOICE.INVOICE_REF_NO,");
			sb.Append("          INVOICE.INVOICE_DATE,");
			sb.Append("          INVOICE.CURRENCY_ID,");
			sb.Append("          INVOICE.CURRENCY_MST_FK,");
			sb.Append("          INVOICE.TOT_AMT,");
			sb.Append("          SUP_INV.JOB_CARD_TRN_FK,");
			sb.Append("          SUP_INV.INV_SUPPLIER_PK,");
			sb.Append("          SUP_INV.SUPPLIER_INV_NO,");
			sb.Append("          SUP_INV.SUPPLIER_INV_DT,");
			sb.Append("          SUP_INV.CURRENCY_ID,");
			sb.Append("          SUP_INV.CURRENCY_MST_FK,");
			sb.Append("          SUP_INV.INVOICE_AMT,");
			sb.Append("          COLLECTION.INVOICE_REF_NO,");
			sb.Append("          COLLECTION.COLLECTIONS_REF_NO,");
			sb.Append("          COLLECTION.COLLECTIONS_DATE,");
			sb.Append("          COLLECTION.CURRENCY_ID,");
			sb.Append("          COLLECTION.CURRENCY_MST_FK,");
			sb.Append("          COLLECTION.COLL_AMT,");
			sb.Append("          MET.MBL_EXP_TBL_PK,");
			sb.Append("          MET.MBL_REF_NO,");
			sb.Append("          MET.MBL_DATE");

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
			System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
			sqlstr.Append(" SELECT Q.* FROM (SELECT ROWNUM AS \"SNO\", QRY.* FROM ");
			sqlstr.Append("  (" + sb.ToString() + " ");
			sqlstr.Append("   ORDER BY TO_DATE(JOBCARD_DATE) DESC) QRY )Q ");

			if (Excel == 0) {
				sqlstr.Append("  WHERE Q.SNO  BETWEEN " + Start + " AND " + Last + "");
			}

			try {
				return objWF.GetDataSet(sqlstr.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
        #endregion

        #region "Fetch On Search for Export Manifest SEA"
        /// <summary>
        /// Fetches all exp manifest.
        /// </summary>
        /// <param name="VesselPk">The vessel pk.</param>
        /// <param name="txtJobRefPk">The text job reference pk.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefId">The reference identifier.</param>
        /// <param name="txtHblPk">The text HBL pk.</param>
        /// <param name="txtBookingPk">The text booking pk.</param>
        /// <param name="Excel">The excel.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="BCurrencyPK">The b currency pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="txtShipper">The text shipper.</param>
        /// <param name="txtConsignee">The text consignee.</param>
        /// <param name="txtMblPK">The text MBL pk.</param>
        /// <returns></returns>
        public DataSet FetchAllExpManifest(string VesselPk = "", string txtJobRefPk = "", int POLPK = 0, int PODPK = 0, string RefType = "", string RefId = "", string txtHblPk = "", string txtBookingPk = "", Int32 Excel = 0, int CurrentPage = 0,
		int TotalPage = 0, int BCurrencyPK = 0, Int32 LocFk = 0, string txtShipper = "", string txtConsignee = "", Int32 txtMblPK = 0)
		{
			Int32 Last = default(Int32);
			Int32 Start = default(Int32);
			Int32 TotalRecords = default(Int32);
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("SELECT DISTINCT BST.BOOKING_MST_PK,");
			sb.Append("                BST.BOOKING_REF_NO,");
			sb.Append("                TO_DATE(BST.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
			sb.Append("                CASE");
			sb.Append("                  WHEN (SELECT COUNT(*)");
			sb.Append("                          FROM BOOKING_MST_TBL B");
			sb.Append("                         WHERE B.BOOKING_MST_PK = JCSET.BOOKING_MST_FK");
			sb.Append("                           AND ROWNUM = 1");
			sb.Append("                           AND B.STATUS IN (2, 6)) > 0 THEN");
			sb.Append("                   'a'");
			sb.Append("                  ELSE");
			sb.Append("                   'r'");
			sb.Append("                END BKG_CONFIRM,");
			sb.Append("                CASE");
			sb.Append("                  WHEN JCSET.SHIPPING_INST_FLAG = 1 THEN");
			sb.Append("                   'a'");
			sb.Append("                  ELSE");
			sb.Append("                   'r'");
			sb.Append("                END SHIPPING_INST_FLAG,");
			sb.Append("                TO_DATE(JCSET.SHIPPING_INST_DT, DATEFORMAT) SHIPPING_INST_DT,");
			sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPER,");
			sb.Append("                CONSIGNEE.CUSTOMER_NAME CONSIGNEE,");
			sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
			sb.Append("                JCSET.JOB_CARD_TRN_PK,");
			sb.Append("                JCSET.JOBCARD_REF_NO,");
			sb.Append("                TO_DATE(JCSET.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
			sb.Append("                HET.HBL_EXP_TBL_PK,");
			sb.Append("                HET.HBL_REF_NO,");
			sb.Append("                TO_DATE(HET.HBL_DATE, DATEFORMAT) HBL_DATE,");
			sb.Append("                DECODE(HET.HBL_STATUS,");
			sb.Append("                       '0',");
			sb.Append("                       'Draft',");
			sb.Append("                       '1',");
			sb.Append("                       'Confirmed',");
			sb.Append("                       '2',");
			sb.Append("                       'Released',");
			sb.Append("                       '3',");
			sb.Append("                       'Cancelled',");
			sb.Append("                       '4',");
			sb.Append("                       'All') HBL_STATUS,");
			sb.Append("                MET.MBL_EXP_TBL_PK,");
			sb.Append("                MET.MBL_REF_NO,");
			sb.Append("                TO_DATE(MET.MBL_DATE, DATEFORMAT) MBL_DATE,");
			sb.Append("                POL.PORT_ID POL,");
			sb.Append("                POD.PORT_ID POD,");
			sb.Append("       CASE WHEN VVTRN.VOYAGE IS NULL THEN");
			sb.Append("         VVT.VESSEL_NAME");
			sb.Append("        ELSE");
			sb.Append("          (VVT.VESSEL_NAME || '/' || VVTRN.VOYAGE)");
			sb.Append("        END VESSEL_VOYAGE,");
			sb.Append("                CASE");
			sb.Append("                  WHEN (SELECT DISTINCT J.LOAD_DATE");
			sb.Append("                          FROM JOB_TRN_CONT J");
			sb.Append("                         WHERE J.JOB_CARD_TRN_FK =");
			sb.Append("                               JCSET.JOB_CARD_TRN_PK");
			sb.Append("                           AND ROWNUM = 1) IS NOT NULL THEN");
			sb.Append("                   'a'");
			sb.Append("                  ELSE");
			sb.Append("                   'r'");
			sb.Append("                END LOAD_CONFIRM,");
			sb.Append("                ");
			sb.Append("                CASE");
			sb.Append("                  WHEN JCSET.CARGO_MANIFEST = 1 THEN");
			sb.Append("                   'a'");
			sb.Append("                  ELSE");
			sb.Append("                   'r'");
			sb.Append("                END MANIFEST,");
			sb.Append("                DECODE(JCSET.CARGO_MANIFEST, '0', 'OPEN', '1', 'CLOSED') MANI_STATUS,");
			sb.Append("                CASE");
			sb.Append("                  WHEN JCSET.DEPARTURE_DATE IS NOT NULL THEN");
			sb.Append("                   'SAILED'");
			sb.Append("                  ELSE");
			sb.Append("                   'NOT SAILED'");
			sb.Append("                END VESSEL_STATUS,");
			sb.Append("                BST.CARGO_TYPE");
			sb.Append("          FROM JOB_CARD_TRN JCSET,");
			sb.Append("               HBL_EXP_TBL          HET,");
			sb.Append("               MBL_EXP_TBL          MET,");
			sb.Append("               BOOKING_MST_TBL      BST,");
			sb.Append("               PORT_MST_TBL         POL,");
			sb.Append("               PORT_MST_TBL         POD,");
			sb.Append("               CUSTOMER_MST_TBL     SHIPPER,");
			sb.Append("               CUSTOMER_MST_TBL     CONSIGNEE,");
			sb.Append("               COMMODITY_GROUP_MST_TBL CGMT,");
			sb.Append("               VESSEL_VOYAGE_TBL VVT,");
			sb.Append("               VESSEL_VOYAGE_TRN VVTRN");
			sb.Append("         WHERE HET.HBL_EXP_TBL_PK(+) = JCSET.HBL_HAWB_FK");
			sb.Append("           AND MET.MBL_EXP_TBL_PK(+) = JCSET.MBL_MAWB_FK");
			sb.Append("           AND BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK");
			sb.Append("           AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
			sb.Append("           AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
			sb.Append("           AND SHIPPER.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
			sb.Append("           AND CONSIGNEE.CUSTOMER_MST_PK = JCSET.CONSIGNEE_CUST_MST_FK");
			sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK = VVTRN.VESSEL_VOYAGE_TBL_FK");
			sb.Append("   AND VVTRN.VOYAGE_TRN_PK = JCSET.VOYAGE_TRN_FK");
			sb.Append("     AND SHIPPER.CUSTOMER_MST_PK(+) = JCSET.SHIPPER_CUST_MST_FK");
			sb.Append("     AND CONSIGNEE.CUSTOMER_MST_PK(+) = JCSET.CONSIGNEE_CUST_MST_FK");
			sb.Append("      AND JCSET.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
			sb.Append("     AND POL.PORT_MST_PK IN (SELECT LWPT.PORT_MST_FK FROM LOCATION_WORKING_PORTS_TRN LWPT");
			sb.Append("     WHERE LWPT.ACTIVE = 1");
			sb.Append("     AND JCSET.BUSINESS_TYPE = 2 ");
			sb.Append("     AND JCSET.PROCESS_TYPE = 1 ");
			sb.Append("     AND LWPT.LOCATION_MST_FK = " + LocFk + ")");
			sb.Append("    AND BST.STATUS IN (1, 2, 6)");
			if (Convert.ToInt32(txtJobRefPk) != 0) {
				sb.Append("  AND JCSET.JOB_CARD_TRN_PK = " + txtJobRefPk + "");
			}
			if (Convert.ToInt32(txtHblPk) != 0) {
				sb.Append("  AND HET.HBL_EXP_TBL_PK = " + txtHblPk + "");
			}
			if (Convert.ToInt32(txtBookingPk) != 0) {
				sb.Append("  AND BST.BOOKING_MST_PK = " + txtBookingPk + "");
			}
			if (POLPK != 0) {
				sb.Append("  AND POL.PORT_MST_PK  = " + POLPK + "");
			}
			if (PODPK != 0) {
				sb.Append("  AND POD.PORT_MST_PK = " + PODPK + "");
			}
			if (Convert.ToInt32(VesselPk) != 0) {
				sb.Append(" AND VVTRN.VOYAGE_TRN_PK = " + VesselPk + "");
			}
			if (txtMblPK != 0) {
				sb.Append(" AND MET.MBL_EXP_TBL_PK = " + txtMblPK + "");
			}
			if (!string.IsNullOrEmpty(txtShipper)) {
				sb.Append(" AND UPPER(SHIPPER.CUSTOMER_NAME) LIKE '" + txtShipper.ToUpper().Replace("'", "''") + "'");
			}
			if (!string.IsNullOrEmpty(txtConsignee)) {
				sb.Append(" AND  UPPER(CONSIGNEE.CUSTOMER_NAME) LIKE '" + txtConsignee.ToUpper().Replace("'", "''") + "'");
			}
			if (Convert.ToInt32(RefType) > 0) {
				if (!string.IsNullOrEmpty(RefId)) {
					if (RefType == "1") {
						sb.Append(" AND UPPER(SHIPPER.CUSTOMER_NAME) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "2") {
						sb.Append(" AND  UPPER(CONSIGNEE.CUSTOMER_NAME) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "3") {
						if (RefId == "FCL" | RefId == "fcl") {
							sb.Append(" AND  BST.CARGO_TYPE = 1");
						} else if (RefId == "LCL" | RefId == "lcl") {
							sb.Append(" AND  BST.CARGO_TYPE = 2");
						} else if (RefId == "BBC" | RefId == "Break Bulk") {
							sb.Append(" AND  BST.CARGO_TYPE = 4");
						}
					} else if (RefType == "4") {
						sb.Append(" AND UPPER(CGMT.COMMODITY_GROUP_DESC) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else {
						sb.Append("AND JCSET.JOB_CARD_TRN_PK IN ");
						sb.Append("   (SELECT JCONT.JOB_CARD_TRN_FK");
						sb.Append("          FROM JOB_TRN_CONT JCONT");
						sb.Append("         WHERE JCONT.COMMODITY_MST_FK IN");
						sb.Append("               (SELECT C.COMMODITY_MST_PK FROM COMMODITY_MST_TBL C WHERE UPPER(C.COMMODITY_ID) like UPPER('%" + RefId + "%') ");
						sb.Append("OR UPPER(C.Commodity_Name) like UPPER('%" + RefId + "%')))");
					}
				}
			}

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
			System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
			sqlstr.Append(" SELECT Q.* FROM (SELECT ROWNUM AS \"SNO\", QRY.* FROM ");
			sqlstr.Append("  (" + sb.ToString() + " ");
			sqlstr.Append("   ORDER BY TO_DATE(JOBCARD_DATE) DESC) QRY )Q ");
			if (Excel == 0) {
				sqlstr.Append("  WHERE Q.SNO  BETWEEN " + Start + " AND " + Last + "");
			}
			try {
				return objWF.GetDataSet(sqlstr.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch On Search for Export Freight AIR"
        /// <summary>
        /// Fetches all exp FRT air.
        /// </summary>
        /// <param name="txtAirlinePK">The text airline pk.</param>
        /// <param name="lblAOOPK">The label aoopk.</param>
        /// <param name="lblAODPK">The label aodpk.</param>
        /// <param name="txtRefPK">The text reference pk.</param>
        /// <param name="txtJobPK">The text job pk.</param>
        /// <param name="txtHAWBPk">The text hawb pk.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefId">The reference identifier.</param>
        /// <param name="Excel">The excel.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="BCurrencyPK">The b currency pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <returns></returns>
        public DataSet FetchAllExpFrtAir(int txtAirlinePK = 0, int lblAOOPK = 0, int lblAODPK = 0, int txtRefPK = 0, int txtJobPK = 0, int txtHAWBPk = 0, string RefType = "", string RefId = "", Int32 Excel = 0, int CurrentPage = 0,
		int TotalPage = 0, int BCurrencyPK = 0, Int32 LocFk = 0)
		{
			Int32 Last = default(Int32);
			Int32 Start = default(Int32);
			Int32 TotalRecords = default(Int32);
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("SELECT JCAET.JOB_CARD_TRN_PK,");
			sb.Append("       JCAET.JOBCARD_REF_NO,");
			sb.Append("       TO_DATE(JCAET.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
			sb.Append("       Nvl(SUM(JTAEF.FREIGHT_AMT*GET_EX_RATE(JTAEF.CURRENCY_MST_FK," + BCurrencyPK + ",JCAET.JOBCARD_DATE)),0) AIF,");
			sb.Append("       Nvl(SUM(CASE");
			sb.Append("             WHEN JTAEF.FREIGHT_TYPE = 1 THEN");
			sb.Append("              JTAEF.FREIGHT_AMT*GET_EX_RATE(JTAEF.CURRENCY_MST_FK," + BCurrencyPK + ",JCAET.JOBCARD_DATE)");
			sb.Append("           END),0) PREPAID,");
			sb.Append("       Nvl(SUM(CASE");
			sb.Append("             WHEN JTAEF.FREIGHT_TYPE = 2 THEN");
			sb.Append("              JTAEF.FREIGHT_AMT*GET_EX_RATE(JTAEF.CURRENCY_MST_FK," + BCurrencyPK + ",JCAET.JOBCARD_DATE)");
			sb.Append("           END),0) COLLECT,");
			sb.Append("       HET.HAWB_EXP_TBL_PK,");
			sb.Append("       HET.HAWB_REF_NO,");
			sb.Append("       TO_DATE(HET.HAWB_DATE, DATEFORMAT) HAWB_DATE,");
			sb.Append("       HET.HAWB_STATUS,");
			sb.Append("       MET.MAWB_EXP_TBL_PK,");
			sb.Append("       MET.MAWB_REF_NO,");
			sb.Append("       TO_DATE(MET.MAWB_DATE, DATEFORMAT) MAWB_DATE,");
			sb.Append("       AMT.AIRLINE_NAME,");
			sb.Append("       JCAET.VOYAGE_FLIGHT_NO,");
			sb.Append("       AOO.PORT_ID AOO,");
			sb.Append("       AOD.PORT_ID AOD,");
			sb.Append("       BAT.BOOKING_MST_PK,");
			sb.Append("       BAT.BOOKING_REF_NO,");
			sb.Append("       TO_DATE(BAT.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
			sb.Append("       SUM(NVL(JTAEF.FREIGHT_AMT,0)*GET_EX_RATE(JTAEF.CURRENCY_MST_FK," + BCurrencyPK + ",JCAET.JOBCARD_DATE)) +");
			sb.Append("       (SELECT SUM(NVL(JTOTH.AMOUNT,0)*GET_EX_RATE(JTOTH.CURRENCY_MST_FK," + BCurrencyPK + ",JCAET.JOBCARD_DATE))");
			sb.Append("          FROM JOB_TRN_OTH_CHRG JTOTH");
			sb.Append("         WHERE JTOTH.JOB_CARD_TRN_FK(+) = JCAET.JOB_CARD_TRN_PK) RECEIVABLE,");
			sb.Append("       CASE");
			sb.Append("         WHEN INVOICE.JOB_CARD_FK IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         else");
			sb.Append("          'r'");
			sb.Append("       END INVOICE,");
			sb.Append("       INVOICE.CONSOL_INVOICE_PK,");
			sb.Append("       INVOICE.INVOICE_REF_NO,");
			sb.Append("       TO_DATE(INVOICE.INVOICE_DATE, DATEFORMAT) INVOICE_DATE,");
			sb.Append("       INVOICE.CURRENCY_ID INV_CUR,");
			sb.Append("       NVL(INVOICE.TOT_AMT,0) INV_AMT,");
			sb.Append("       CASE");
			sb.Append("         WHEN SUP_INV.JOB_CARD_TRN_FK IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         else");
			sb.Append("          'r'");
			sb.Append("       END SUP_INV,");
			sb.Append("       SUP_INV.INV_SUPPLIER_PK,");
			sb.Append("       SUP_INV.SUPPLIER_INV_NO,");
			sb.Append("       TO_DATE(SUP_INV.SUPPLIER_INV_DT, DATEFORMAT) SUPPLIER_INV_DT,");
			sb.Append("       SUP_INV.CURRENCY_ID SUP_INV_CUR,");
			sb.Append("       Nvl(SUP_INV.INVOICE_AMT,0) SUP_INV_AMT,");
			sb.Append("       CASE");
			sb.Append("         WHEN COLLECTION.INVOICE_REF_NO IS NOT NULL THEN");
			sb.Append("          'a'");
			sb.Append("         else");
			sb.Append("          'r'");
			sb.Append("       END COLLECTION,");
			sb.Append("       COLLECTION.COLLECTIONS_REF_NO,");
			sb.Append("       TO_DATE(COLLECTION.COLLECTIONS_DATE, DATEFORMAT) COLLECTIONS_DATE,");
			sb.Append("       COLLECTION.CURRENCY_ID COL_CUR,");
			sb.Append("       Nvl(COLLECTION.COLL_AMT,0),");
			sb.Append("     (SELECT CMT11.CURRENCY_ID FROM CURRENCY_TYPE_MST_TBL CMT11 WHERE CMT11.CURRENCY_MST_PK='" + BCurrencyPK + "') OSCUR,");
			sb.Append("      NVL(NVL(INVOICE.TOT_AMT, 0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE) -");
			sb.Append("      NVL(COLLECTION.COLL_AMT, 0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("      0) OS,");
			sb.Append("      CASE WHEN ");
			sb.Append("      NVL(NVL(INVOICE.TOT_AMT, 0)*GET_EX_RATE(INVOICE.CURRENCY_MST_FK," + BCurrencyPK + ",INVOICE.INVOICE_DATE) -");
			sb.Append("      NVL(COLLECTION.COLL_AMT, 0)*GET_EX_RATE(COLLECTION.CURRENCY_MST_FK," + BCurrencyPK + ",COLLECTION.COLLECTIONS_DATE),");
			sb.Append("         0) >0 THEN ");
			sb.Append("             ROUND(SYSDATE - TO_DATE(INVOICE.INVOICE_DATE))");
			sb.Append("         ELSE");
			sb.Append("             0");
			sb.Append("         END AGE");
			sb.Append("");
			sb.Append("  FROM JOB_CARD_TRN JCAET,");
			sb.Append("       JOB_TRN_FD JTAEF,");
			sb.Append("       HAWB_EXP_TBL HET,");
			sb.Append("       MAWB_EXP_TBL MET,");
			sb.Append("       BOOKING_MST_TBL BAT,");
			sb.Append("       AIRLINE_MST_TBL AMT,");
			sb.Append("       PORT_MST_TBL AOO,");
			sb.Append("       PORT_MST_TBL AOD,");
			sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
			sb.Append("       CUSTOMER_MST_TBL SHIPPER,");
			sb.Append("       CUSTOMER_MST_TBL CONSIGNEE,");
			sb.Append("       (SELECT CITT.JOB_CARD_FK,");
			sb.Append("               CIT.CONSOL_INVOICE_PK,");
			sb.Append("               CIT.INVOICE_REF_NO,");
			sb.Append("               CIT.INVOICE_DATE,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               CIT.CURRENCY_MST_FK,");
			sb.Append("               ROUND(SUM(NVL(CITT.TOT_AMT,0)*");
			sb.Append("                             GET_EX_RATE(CITT.CURRENCY_MST_FK,");
			sb.Append("                                         CIT.CURRENCY_MST_FK,");
			sb.Append("                                         CIT.INVOICE_DATE)),");
			sb.Append("                   2) TOT_AMT");
			sb.Append("          FROM CONSOL_INVOICE_TBL     CIT,");
			sb.Append("               CONSOL_INVOICE_TRN_TBL CITT,");
			sb.Append("               CURRENCY_TYPE_MST_TBL  CTMT");
			sb.Append("         WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
			sb.Append("           AND CIT.CHK_INVOICE <> 2 ");
			sb.Append("           AND CTMT.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK");
			sb.Append("         GROUP BY CITT.JOB_CARD_FK,");
			sb.Append("               CIT.CONSOL_INVOICE_PK,");
			sb.Append("                  CIT.INVOICE_REF_NO,");
			sb.Append("                  CIT.INVOICE_DATE,");
			sb.Append("                  CIT.CURRENCY_MST_FK,");
			sb.Append("                  CTMT.CURRENCY_ID) INVOICE,");
			sb.Append("       (SELECT JTAEC.JOB_CARD_TRN_FK,");
			sb.Append("                  IST.INV_SUPPLIER_PK,");
			sb.Append("               IST.SUPPLIER_INV_NO,");
			sb.Append("               IST.SUPPLIER_INV_DT,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               IST.CURRENCY_MST_FK,");
			sb.Append("               ROUND(SUM(NVL(ISTT.TOTAL_COST,0)");
			sb.Append("                   ),2) INVOICE_AMT");
			sb.Append("          FROM INV_SUPPLIER_TBL      IST,");
			sb.Append("               INV_SUPPLIER_TRN_TBL  ISTT,");
			sb.Append("               CURRENCY_TYPE_MST_TBL CTMT,");
			sb.Append("               JOB_TRN_COST  JTAEC");
			sb.Append("         WHERE IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
			sb.Append("           AND CTMT.CURRENCY_MST_PK = IST.CURRENCY_MST_FK");
			sb.Append("           AND IST.APPROVED <> 3 ");
			sb.Append("           AND JTAEC.JOB_TRN_COST_PK(+) = ISTT.JOB_TRN_EST_FK");
			sb.Append("         GROUP BY JTAEC.JOB_CARD_TRN_FK,");
			sb.Append("                  IST.INV_SUPPLIER_PK,");
			sb.Append("                  IST.SUPPLIER_INV_NO,");
			sb.Append("                  IST.SUPPLIER_INV_DT,");
			sb.Append("                  IST.CURRENCY_MST_FK,");
			sb.Append("                  CTMT.CURRENCY_ID) SUP_INV,");

			sb.Append("       (SELECT CIT.INVOICE_REF_NO,");
			sb.Append("               CT.COLLECTIONS_REF_NO,");
			sb.Append("               CT.COLLECTIONS_DATE,");
			sb.Append("               CTMT.CURRENCY_ID,");
			sb.Append("               CT.CURRENCY_MST_FK,");
			sb.Append("               ROUND(SUM(NVL(CTT.RECD_AMOUNT_HDR_CURR,0)");
			sb.Append("                   ),2) COLL_AMT");
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
			sb.Append("                  CTMT.CURRENCY_ID) COLLECTION");
			sb.Append("");
			sb.Append(" WHERE JCAET.JOB_CARD_TRN_PK = JTAEF.JOB_CARD_TRN_FK(+)");
			sb.Append("   AND HET.HAWB_EXP_TBL_PK(+) = JCAET.HBL_HAWB_FK");
			sb.Append("   AND MET.MAWB_EXP_TBL_PK(+) = JCAET.MBL_MAWB_FK");
			sb.Append("   AND BAT.BOOKING_MST_PK = JCAET.BOOKING_MST_FK");
			sb.Append("   AND AMT.AIRLINE_MST_PK = BAT.CARRIER_MST_FK");
			sb.Append("   AND AOO.PORT_MST_PK = BAT.PORT_MST_POL_FK");
			sb.Append("   AND AOD.PORT_MST_PK = BAT.PORT_MST_POD_FK");
			sb.Append("   AND INVOICE.JOB_CARD_FK(+) = JCAET.JOB_CARD_TRN_PK");
			sb.Append("   AND SUP_INV.JOB_CARD_TRN_FK(+) = JCAET.JOB_CARD_TRN_PK");
			sb.Append("   AND COLLECTION.INVOICE_REF_NO(+) = INVOICE.INVOICE_REF_NO");
			sb.Append("      AND JCAET.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
			sb.Append("     AND SHIPPER.CUSTOMER_MST_PK(+) = JCAET.SHIPPER_CUST_MST_FK");
			sb.Append("     AND CONSIGNEE.CUSTOMER_MST_PK(+) = JCAET.CONSIGNEE_CUST_MST_FK");
			sb.Append("     AND AOO.PORT_MST_PK IN (SELECT LWPT.PORT_MST_FK FROM LOCATION_WORKING_PORTS_TRN LWPT");
			sb.Append("     WHERE LWPT.ACTIVE = 1");
			sb.Append("     AND JCAET.BUSINESS_TYPE = 1 ");
			sb.Append("     AND JCAET.PROCESS_TYPE = 1 ");
			sb.Append("     AND LWPT.LOCATION_MST_FK = " + LocFk + ")");
			if (txtJobPK != 0) {
				sb.Append("  AND JCAET.JOB_CARD_TRN_PK = " + txtJobPK + "");
			}
			if (txtHAWBPk != 0) {
				sb.Append("  AND HET.HAWB_EXP_TBL_PK = " + txtHAWBPk + "");
			}
			if (txtRefPK != 0) {
				sb.Append("  AND BAT.BOOKING_MST_PK = " + txtRefPK + "");
			}
			if (lblAOOPK != 0) {
				sb.Append("  AND AOO.PORT_MST_PK  = " + lblAOOPK + "");
			}
			if (lblAODPK != 0) {
				sb.Append("  AND AOD.PORT_MST_PK = " + lblAODPK + "");
			}
			if (txtAirlinePK != 0) {
				sb.Append("  AND AMT.AIRLINE_MST_PK= " + txtAirlinePK + "");
			}
			if (Convert.ToInt32(RefType) > 0) {
				if (!string.IsNullOrEmpty(RefId)) {
					if (RefType == "1") {
						sb.Append(" AND UPPER(SHIPPER.CUSTOMER_ID) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "2") {
						sb.Append(" AND  UPPER(CONSIGNEE.CUSTOMER_ID) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "3") {
						sb.Append(" AND UPPER(CGMT.COMMODITY_GROUP_DESC) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else {
						sb.Append("AND JCAET.JOB_CARD_TRN_PK IN ");
						sb.Append("   (SELECT JCONT.JOB_CARD_TRN_FK");
						sb.Append("          FROM JOB_TRN_CONT JCONT");
						sb.Append("         WHERE JCONT.COMMODITY_MST_FKS IN");
						sb.Append("               (SELECT ROWTOCOL('SELECT C.COMMODITY_MST_PK FROM COMMODITY_MST_TBL C WHERE UPPER(C.COMMODITY_ID) like UPPER(''%" + RefId + "%'') ");
						sb.Append("OR UPPER(C.Commodity_Name) like UPPER(''%" + RefId + "%'')')FROM DUAL))");
					}
				}
			}
			sb.Append(" GROUP BY JCAET.JOB_CARD_TRN_PK,");
			sb.Append("          JCAET.JOBCARD_REF_NO,");
			sb.Append("          JCAET.JOBCARD_DATE,");
			sb.Append("          HET.HAWB_EXP_TBL_PK,");
			sb.Append("          HET.HAWB_REF_NO,");
			sb.Append("          HET.HAWB_DATE,");
			sb.Append("          HET.HAWB_STATUS,");
			sb.Append("          MET.MAWB_EXP_TBL_PK,");
			sb.Append("          MET.MAWB_REF_NO,");
			sb.Append("          MET.MAWB_DATE,");
			sb.Append("          AMT.AIRLINE_NAME,");
			sb.Append("          JCAET.VOYAGE_FLIGHT_NO,");
			sb.Append("          AOO.PORT_ID,");
			sb.Append("          AOD.PORT_ID,");
			sb.Append("          BAT.BOOKING_MST_PK,");
			sb.Append("          BAT.BOOKING_REF_NO,");
			sb.Append("          BAT.BOOKING_DATE,");
			sb.Append("          JCAET.JOB_CARD_TRN_PK,");
			sb.Append("          INVOICE.JOB_CARD_FK,");
			//
			sb.Append("          INVOICE.CONSOL_INVOICE_PK,");
			sb.Append("          INVOICE.INVOICE_REF_NO,");
			sb.Append("          INVOICE.INVOICE_DATE,");
			sb.Append("          INVOICE.CURRENCY_ID,");
			sb.Append("          INVOICE.CURRENCY_MST_FK,");
			sb.Append("          INVOICE.TOT_AMT,");
			sb.Append("          SUP_INV.JOB_CARD_TRN_FK,");
			sb.Append("          SUP_INV.INV_SUPPLIER_PK,");
			sb.Append("          SUP_INV.SUPPLIER_INV_NO,");
			sb.Append("          SUP_INV.SUPPLIER_INV_DT,");
			sb.Append("          SUP_INV.CURRENCY_ID,");
			sb.Append("          SUP_INV.CURRENCY_MST_FK,");
			sb.Append("          SUP_INV.INVOICE_AMT,");
			sb.Append("          COLLECTION.INVOICE_REF_NO,");
			sb.Append("          COLLECTION.COLLECTIONS_REF_NO,");
			sb.Append("          COLLECTION.COLLECTIONS_DATE,");
			sb.Append("          COLLECTION.CURRENCY_ID,");
			sb.Append("          COLLECTION.CURRENCY_MST_FK,");
			sb.Append("          COLLECTION.COLL_AMT  ");
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
				System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
				sqlstr.Append(" SELECT Q.* FROM (SELECT ROWNUM AS \"SNO\", QRY.* FROM ");
				sqlstr.Append("  (" + sb.ToString() + " ");
				sqlstr.Append("   ORDER BY TO_DATE(JOBCARD_DATE) DESC) QRY )Q ");

				if (Excel == 0) {
					sqlstr.Append("  WHERE Q.SNO  BETWEEN " + Start + " AND " + Last + "");
				}

				return objWF.GetDataSet(sqlstr.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch On Search for Export Manifest AIR"
        /// <summary>
        /// Fetches all exp manifest air.
        /// </summary>
        /// <param name="txtAirlinePK">The text airline pk.</param>
        /// <param name="lblAOOPK">The label aoopk.</param>
        /// <param name="lblAODPK">The label aodpk.</param>
        /// <param name="txtRefPK">The text reference pk.</param>
        /// <param name="txtJobPK">The text job pk.</param>
        /// <param name="txtHAWBPk">The text hawb pk.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefId">The reference identifier.</param>
        /// <param name="Excel">The excel.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="BCurrencyPK">The b currency pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="txtShipper">The text shipper.</param>
        /// <param name="txtFlightNo">The text flight no.</param>
        /// <param name="txtConsignee">The text consignee.</param>
        /// <param name="txtMblPK">The text MBL pk.</param>
        /// <returns></returns>
        public DataSet FetchAllExpManifestAir(int txtAirlinePK = 0, int lblAOOPK = 0, int lblAODPK = 0, int txtRefPK = 0, int txtJobPK = 0, int txtHAWBPk = 0, string RefType = "", string RefId = "", Int32 Excel = 0, int CurrentPage = 0,
		int TotalPage = 0, int BCurrencyPK = 0, Int32 LocFk = 0, string txtShipper = "", string txtFlightNo = "", string txtConsignee = "", Int32 txtMblPK = 0)
		{
			Int32 Last = default(Int32);
			Int32 Start = default(Int32);
			Int32 TotalRecords = default(Int32);
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("SELECT DISTINCT BAT.BOOKING_MST_PK,");
			sb.Append("                BAT.BOOKING_REF_NO,");
			sb.Append("                TO_DATE(BAT.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
			sb.Append("                CASE");
			sb.Append("                  WHEN (SELECT COUNT(*)");
			sb.Append("                          FROM BOOKING_MST_TBL B");
			sb.Append("                         WHERE B.BOOKING_MST_PK = JCAET.BOOKING_MST_FK");
			sb.Append("                           AND ROWNUM = 1");
			sb.Append("                           AND B.STATUS IN (2, 6)) > 0 THEN");
			sb.Append("                   'a'");
			sb.Append("                  ELSE");
			sb.Append("                   'r'");
			sb.Append("                END BKG_CONFIRM,");
			sb.Append("                CASE");
			sb.Append("                  WHEN JCAET.SHIPPING_INST_FLAG = 1 THEN");
			sb.Append("                   'a'");
			sb.Append("                  ELSE");
			sb.Append("                   'r'");
			sb.Append("                END SHIPPING_INST_FLAG,");
			sb.Append("                TO_DATE(JCAET.SHIPPING_INST_DT, DATEFORMAT) SHIPPING_INST_DT,");
			sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPER,");
			sb.Append("                CONSIGNEE.CUSTOMER_NAME CONSIGNEE,");
			sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
			sb.Append("                JCAET.JOB_CARD_TRN_PK,");
			sb.Append("                JCAET.JOBCARD_REF_NO,");
			sb.Append("                TO_DATE(JCAET.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
			sb.Append("                HET.HAWB_EXP_TBL_PK,");
			sb.Append("                HET.HAWB_REF_NO,");
			sb.Append("                TO_DATE(HET.HAWB_DATE, DATEFORMAT) HAWB_DATE,");
			sb.Append("                DECODE(HET.HAWB_STATUS,");
			sb.Append("                       '0',");
			sb.Append("                       'Draft',");
			sb.Append("                       '1',");
			sb.Append("                       'Released',");
			sb.Append("                       '2',");
			sb.Append("                       'Confirmed',");
			sb.Append("                       '3',");
			sb.Append("                       'Cancelled',");
			sb.Append("                       '4',");
			sb.Append("                       'All') HAWB_STATUS,");
			sb.Append("                MET.MAWB_EXP_TBL_PK,");
			sb.Append("                MET.MAWB_REF_NO,");
			sb.Append("                TO_DATE(MET.MAWB_DATE, DATEFORMAT) MAWB_DATE,");
			sb.Append("                AOO.PORT_ID AOO,");
			sb.Append("                AOD.PORT_ID AOD,");
			sb.Append("                AMT.AIRLINE_NAME,");
			sb.Append("                CASE");
			sb.Append("                  WHEN JCAET.VOYAGE_FLIGHT_NO IS NULL THEN");
			sb.Append("                   AMT.AIRLINE_ID");
			sb.Append("                  ELSE");
			sb.Append("                   (AMT.AIRLINE_ID || '/' || JCAET.VOYAGE_FLIGHT_NO)");
			sb.Append("                END FLIGHT_NO,");
			sb.Append("                CASE");
			sb.Append("                  WHEN JCAET.CARGO_MANIFEST = 1 THEN");
			sb.Append("                   'a'");
			sb.Append("                  ELSE");
			sb.Append("                   'r'");
			sb.Append("                END MANIFEST,");
			sb.Append("                DECODE(JCAET.CARGO_MANIFEST, '0', 'OPEN', '1', 'CLOSED') MANIF_STATUS");
			sb.Append("  FROM JOB_CARD_TRN JCAET,");
			sb.Append("       JOB_TRN_FD   JTAEF,");
			sb.Append("       HAWB_EXP_TBL         HET,");
			sb.Append("       MAWB_EXP_TBL         MET,");
			sb.Append("       BOOKING_MST_TBL      BAT,");
			sb.Append("       AIRLINE_MST_TBL      AMT,");
			sb.Append("       PORT_MST_TBL         AOO,");
			sb.Append("       PORT_MST_TBL         AOD,");
			sb.Append("       CUSTOMER_MST_TBL     SHIPPER,");
			sb.Append("       CUSTOMER_MST_TBL     CONSIGNEE,");
			sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
			sb.Append(" WHERE JCAET.JOB_CARD_TRN_PK = JTAEF.JOB_CARD_TRN_FK");
			sb.Append("   AND HET.HAWB_EXP_TBL_PK(+) = JCAET.HBL_HAWB_FK");
			sb.Append("   AND MET.MAWB_EXP_TBL_PK(+) = JCAET.MBL_MAWB_FK");
			sb.Append("   AND BAT.BOOKING_MST_PK = JCAET.BOOKING_MST_FK");
			sb.Append("   AND AMT.AIRLINE_MST_PK = BAT.CARRIER_MST_FK");
			sb.Append("   AND AOO.PORT_MST_PK = BAT.PORT_MST_POL_FK");
			sb.Append("   AND AOD.PORT_MST_PK = BAT.PORT_MST_POD_FK");
			sb.Append("   AND SHIPPER.CUSTOMER_MST_PK = JCAET.SHIPPER_CUST_MST_FK");

			sb.Append("   AND CONSIGNEE.CUSTOMER_MST_PK = JCAET.CONSIGNEE_CUST_MST_FK ");

			sb.Append("     AND JCAET.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
			sb.Append("     AND AOO.PORT_MST_PK IN (SELECT LWPT.PORT_MST_FK FROM LOCATION_WORKING_PORTS_TRN LWPT");
			sb.Append("     WHERE LWPT.ACTIVE = 1");
			sb.Append("     AND JCAET.BUSINESS_TYPE = 1 ");
			sb.Append("     AND JCAET.PROCESS_TYPE = 1 ");
			sb.Append("     AND LWPT.LOCATION_MST_FK = " + LocFk + ")");
			sb.Append("    AND BAT.STATUS IN (1, 2, 6)");
			if (txtJobPK != 0) {
				sb.Append("  AND JCAET.JOB_CARD_TRN_PK = " + txtJobPK + "");
			}
			if (txtHAWBPk != 0) {
				sb.Append("  AND HET.HAWB_EXP_TBL_PK = " + txtHAWBPk + "");
			}
			if (txtRefPK != 0) {
				sb.Append("  AND BAT.BOOKING_MST_PK = " + txtRefPK + "");
			}
			if (lblAOOPK != 0) {
				sb.Append("  AND AOO.PORT_MST_PK  = " + lblAOOPK + "");
			}
			if (lblAODPK != 0) {
				sb.Append("  AND AOD.PORT_MST_PK = " + lblAODPK + "");
			}
			if (txtAirlinePK != 0) {
				sb.Append("  AND AMT.AIRLINE_MST_PK= " + txtAirlinePK + "");
			}
			if (txtMblPK != 0) {
				sb.Append(" AND MET.MAWB_EXP_TBL_PK = " + txtMblPK + "");
			}
			if (!string.IsNullOrEmpty(txtShipper)) {
				sb.Append(" AND UPPER(SHIPPER.CUSTOMER_ID) LIKE '" + txtShipper.ToUpper().Replace("'", "''") + "'");
			}
			if (!string.IsNullOrEmpty(txtFlightNo)) {
				sb.Append(" AND UPPER(JCAET.Flight_No) LIKE '" + txtFlightNo.ToUpper().Replace("'", "''") + "'");
			}
			if (!string.IsNullOrEmpty(txtConsignee)) {
				sb.Append(" AND  UPPER(CONSIGNEE.CUSTOMER_ID) LIKE '" + txtConsignee.ToUpper().Replace("'", "''") + "'");
			}
			if (Convert.ToInt32(RefType) > 0) {
				if (!string.IsNullOrEmpty(RefId)) {
					if (RefType == "1") {
						sb.Append(" AND UPPER(SHIPPER.CUSTOMER_ID) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "2") {
						sb.Append(" AND  UPPER(CONSIGNEE.CUSTOMER_ID) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else if (RefType == "3") {
						sb.Append(" AND UPPER(CGMT.COMMODITY_GROUP_DESC) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
					} else {
						sb.Append("AND JCAET.JOB_CARD_TRN_PK IN ");
						sb.Append("   (SELECT JCONT.JOB_CARD_TRN_FK");
						sb.Append("          FROM JOB_TRN_CONT JCONT");
						sb.Append("         WHERE JCONT.COMMODITY_MST_FKS IN");
						sb.Append("               (SELECT C.COMMODITY_MST_PK FROM COMMODITY_MST_TBL C WHERE UPPER(C.COMMODITY_ID) like UPPER('%" + RefId + "%') ");
						sb.Append("OR UPPER(C.Commodity_Name) like UPPER('%" + RefId + "%')))");
					}
				}
			}
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
			System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
			sqlstr.Append(" SELECT Q.* FROM (SELECT ROWNUM AS \"SNO\", QRY.* FROM ");
			sqlstr.Append("  (" + sb.ToString() + " ");
			sqlstr.Append("   ORDER BY TO_DATE(JOBCARD_DATE) DESC) QRY )Q ");
			if (Excel == 0) {
				sqlstr.Append(" WHERE Q.SNO  BETWEEN " + Start + " AND " + Last + "");
			}
			try {
				return objWF.GetDataSet(sqlstr.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "CRM Fetch Details"
        /// <summary>
        /// Fetches the CRM report.
        /// </summary>
        /// <param name="VesselPk">The vessel pk.</param>
        /// <param name="txtExe">The text executable.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="RefId">The reference identifier.</param>
        /// <param name="txtCust">The text customer.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Excel">The excel.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ddlActivity">The DDL activity.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="Todate">The todate.</param>
        /// <param name="PendAct">The pend act.</param>
        /// <returns></returns>
        public DataSet FetchCRMReport(string VesselPk = "", string txtExe = "", int POLPK = 0, int PODPK = 0, string RefType = "", string RefId = "", string txtCust = "", string BizType = "", string CargoType = "", Int32 Excel = 0,
		int CurrentPage = 0, int TotalPage = 0, int ddlActivity = 0, string FromDate = "", string Todate = "", string PendAct = "")
		{
			Int32 Last = default(Int32);
			Int32 Start = default(Int32);
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append(" SELECT  ");
			sb.Append("        '' ENQPK, ");
			sb.Append("       CRM.ENQUIRY_REF_NO ENQ_REF_NO, ");
			sb.Append("       CRM.ENQUIRY_DATE ENQDATE, ");
			sb.Append("       '' QTNPK, ");
			sb.Append("       CRM.QUOTATION_REF_NO QTN_REF_NO, ");
			sb.Append("       CRM.QUOTATION_DATE QTNDATE, ");
			sb.Append("       '' BKGPK, ");
			sb.Append("       CRM.BOOKING_REF_NO BKG_REF_NO, ");
			sb.Append("       CRM.BOOKING_DATE BKGDATE, ");
			sb.Append("       CRM.SRR_REF_NO SRR, ");
			sb.Append("       CRM.SRR_DATE SRRDATE, ");
			sb.Append("       CRM.SRR_CONF SRR_CONFIRM, ");
			sb.Append("       CRM.POL POL, ");
			sb.Append("       CRM.POD POD, ");
			sb.Append("       CRM.CUSTOMER_NAME CUSTOMER, ");
			sb.Append("       CRM.VSLVOY VSLVOY, ");
			sb.Append("       CRM.EMPLOYEE_NAME EXECUTIVE, ");
			sb.Append("       CRM.SALES_PLAN, ");
			sb.Append("       CRM.SALES_CALL, ");
			sb.Append("       CRM.ENQ, ");
			sb.Append("       CRM.QTN, ");
			sb.Append("       CRM.QTN_CONF, ");
			sb.Append("       CRM.BKG PROV_BKG, ");
			sb.Append("       CRM.BKG_CONF CONF_BKG, ");
			sb.Append("       CRM.EBKG EBKG, ");
			sb.Append("       CRM.SHIPPED SHIPPED ");
			sb.Append("  FROM CRM_REPORT CRM ");
			sb.Append(" WHERE 1 = 1  ");

			if (POLPK != 0) {
				sb.Append(" AND CRM.PORT_MST_POL_FK  = " + POLPK + "");
			}
			if (PODPK != 0) {
				sb.Append("  AND CRM.PORT_MST_POD_FK = " + PODPK + "");
			}
			if (Convert.ToInt32(VesselPk) != 0) {
				sb.Append(" AND CRM.VESSEL_VOYAGE_FK = " + VesselPk + "");
			}
			if (Convert.ToInt32(txtExe) != 0) {
				sb.Append(" AND CRM.EXECUTIVE_MST_FK = " + txtExe + "");
			}
			if (Convert.ToInt32(txtCust) != 0) {
				sb.Append(" AND CRM.CUST_CUSTOMER_MST_FK = " + txtCust + "");
			}
			if (Convert.ToInt32(BizType) != 0) {
				sb.Append(" AND CRM.BUSINESS_TYPE = " + BizType + "");
			}

			if (Convert.ToInt32(CargoType) != 0) {
				sb.Append(" AND CRM.CARGO_TYPE = " + CargoType + "");
			}



			if (Convert.ToInt32(ddlActivity) > 0) {
				if (Convert.ToInt32(ddlActivity) == 1) {
					if (!((FromDate == null | string.IsNullOrEmpty(FromDate)) & (Todate == null | string.IsNullOrEmpty(Todate)))) {
						sb.Append("            AND To_CHAR(CRM.ENQUIRY_DATE) BETWEEN TO_DATE('" + FromDate + "',dateformat)  AND TO_DATE('" + Todate + "',dateformat) ");
					} else if (!(FromDate == null | string.IsNullOrEmpty(FromDate))) {
						sb.Append("            AND To_CHAR(CRM.ENQUIRY_DATE) >= TO_DATE('" + FromDate + "',dateformat) ");
					} else if (!(Todate == null | string.IsNullOrEmpty(Todate))) {
						sb.Append("            AND To_CHAR(CRM.ENQUIRY_DATE) <= TO_DATE('" + Todate + "',dateformat) ");
					}
				} else if (Convert.ToInt32(ddlActivity) == 2) {
					if (!((FromDate == null | string.IsNullOrEmpty(FromDate)) & (Todate == null | string.IsNullOrEmpty(Todate)))) {
						sb.Append("            AND To_CHAR(CRM.QUOTATION_DATE) BETWEEN TO_DATE('" + FromDate + "',dateformat)  AND TO_DATE('" + Todate + "',dateformat) ");
					} else if (!(FromDate == null | string.IsNullOrEmpty(FromDate))) {
						sb.Append("            AND To_CHAR(CRM.QUOTATION_DATE) >= TO_DATE('" + FromDate + "',dateformat) ");
					} else if (!(Todate == null | string.IsNullOrEmpty(Todate))) {
						sb.Append("            AND To_CHAR(CRM.QUOTATION_DATE) <= TO_DATE('" + Todate + "',dateformat) ");
					}
				} else if (Convert.ToInt32(ddlActivity) == 3) {
					if (!((FromDate == null | string.IsNullOrEmpty(FromDate)) & (Todate == null | string.IsNullOrEmpty(Todate)))) {
						sb.Append("            AND To_CHAR(CRM.SRR_DATE) BETWEEN TO_DATE('" + FromDate + "',dateformat)  AND TO_DATE('" + Todate + "',dateformat) ");
					} else if (!(FromDate == null | string.IsNullOrEmpty(FromDate))) {
						sb.Append("            AND To_CHAR(CRM.SRR_DATE) >= TO_DATE('" + FromDate + "',dateformat) ");
					} else if (!(Todate == null | string.IsNullOrEmpty(Todate))) {
						sb.Append("            AND To_CHAR(CRM.SRR_DATE) <= TO_DATE('" + Todate + "',dateformat) ");
					}
				//4
				} else {
					if (!((FromDate == null | string.IsNullOrEmpty(FromDate)) & (Todate == null | string.IsNullOrEmpty(Todate)))) {
						sb.Append("            AND To_CHAR(CRM.BOOKING_DATE) BETWEEN TO_DATE('" + FromDate + "',dateformat)  AND TO_DATE('" + Todate + "',dateformat) ");
					} else if (!(FromDate == null | string.IsNullOrEmpty(FromDate))) {
						sb.Append("            AND To_CHAR(CRM.BOOKING_DATE ) >= TO_DATE('" + FromDate + "',dateformat) ");
					} else if (!(Todate == null | string.IsNullOrEmpty(Todate))) {
						sb.Append("            AND To_CHAR(CRM.BOOKING_DATE) <= TO_DATE('" + Todate + "',dateformat) ");
					}
				}

			}



			if (Convert.ToInt32(RefType) > 0) {
				if (RefType == "1") {
					sb.Append(" AND UPPER(CRM.ENQUIRY_REF_NO) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
				} else if (RefType == "2") {
					sb.Append(" AND  UPPER(CRM.BOOKING_REF_NO) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
				} else if (RefType == "3") {
					sb.Append(" AND  UPPER(CRM.QUOTATION_REF_NO) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
				} else if (RefType == "4") {
					sb.Append(" AND  UPPER(CRM.SRR_REF_NO) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
				} else if (RefType == "5") {
					sb.Append(" AND  UPPER(CRM.SALES_PLAN) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
				} else if (RefType == "6") {
					sb.Append(" AND  UPPER(CRM.POL) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
				} else {
					sb.Append(" AND  UPPER(CRM.POD) LIKE '" + RefId.ToUpper().Replace("'", "''") + "%'");
				}

			}



			if (Convert.ToInt32(PendAct) > 0) {
				if (PendAct == "1") {
					sb.Append(" AND CRM.ENQ IS NULL ");
				} else if (PendAct == "2") {
					sb.Append(" AND  CRM.QTN IS NULL ");
				} else if (PendAct == "3") {
					sb.Append(" AND  CRM.SRR_CONF IS NULL ");
				} else if (PendAct == "4") {
					sb.Append(" AND  CRM.QTN_CONF IS NULL ");
				} else if (PendAct == "5") {
					sb.Append(" AND  CRM.BKG_CONF IS NULL ");
				} else if (PendAct == "6") {
					sb.Append(" AND  CRM.EBKG IS NULL ");
				} else {
					sb.Append(" AND  CRM.SHIPPED IS NULL  ");
				}

			}

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
			System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
			sqlstr.Append(" SELECT Q.* FROM (SELECT ROWNUM AS \"SNO\", QRY.* FROM ");
			sqlstr.Append("  (" + sb.ToString() + " ");
			sqlstr.Append("   ) QRY )Q ");
			if (Excel == 0) {
				sqlstr.Append("  WHERE Q.SNO  BETWEEN " + Start + " AND " + Last + "");
			}
			try {
				return objWF.GetDataSet(sqlstr.ToString());
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
