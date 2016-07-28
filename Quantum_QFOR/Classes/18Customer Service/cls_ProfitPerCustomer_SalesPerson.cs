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

using System;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using System.Web;

namespace Quantum_QFOR
{
    public class clsProfitPerCustomer_SalesPerson : CommonFeatures
	{

		#region "FetchListing"
		public DataSet FetchSeaGridDetail(string ddBasis = "0", string ddSymbols = "0", string Commodityfk = "0", double txtSymbol = 0.0, string BkgPK = "", string CustPK = "", string CommPK = "", string ExecPK = "", Int32 LocFk = 0, string fromDate = "",
		string toDate = "", Int32 flag = 0)
		{

			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			OracleDataAdapter DA = new OracleDataAdapter();
			DataSet MainDS = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			string strCondition = " ";
			int Count = 0;
			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
			string BookingPKs = null;

			try {
				sb.Append("SELECT *");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        LOCATION_NAME,");
				sb.Append("                        EMPLOYEE_ID,");
				sb.Append("                        EMPLOYEE_NAME,");
				sb.Append("                        CUSTOMER_ID,");
				sb.Append("                        CUSTOMER_NAME,");
				sb.Append("                        BOOKING_REF_NO1,");
				sb.Append("                        BOOKING_REF_NO,");
				sb.Append("                        VESSEL_NAME,");
				sb.Append("                        VOYAGE,");
				sb.Append("                        POL_ID,");
				sb.Append("                        POD_ID,");
				sb.Append("                        NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                        NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                         NVL(SUM(DISTINCT ALLOCTEU), 0) ALLOCTEU,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                        END NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(INCOME), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(INCOME), 0)");
				sb.Append("                        END INCOME,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(EXPENSES), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(EXPENSES), 0)");
				sb.Append("                        END EXPENSES,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(INCOME -");
				sb.Append("                                   EXPENSES ),");
				sb.Append("                               0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(INCOME -");
				sb.Append("                                   EXPENSES),");
				sb.Append("                               0)");
				sb.Append("                        END PROFIT_MARGIN");
				sb.Append("          FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                                CONTAINER_TYPE_MST_ID,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                                LOCATION_NAME,");
				sb.Append("                                EMPLOYEE_ID,");
				sb.Append("                                EMPLOYEE_NAME,");
				sb.Append("                                CUSTOMER_ID,");
				sb.Append("                                CUSTOMER_NAME,");
				sb.Append("                                BOOKING_REF_NO1,");
				sb.Append("                                BOOKING_REF_NO,");
				sb.Append("                                VESSEL_NAME,");
				sb.Append("                                VOYAGE,");
				sb.Append("                                POL_ID,");
				sb.Append("                                POD_ID,");
				sb.Append("                                NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                                NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                               NVL(SUM(DISTINCT ALLOCTEU * (NO_OF_BOXES)), 0) ALLOCTEU,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                                END NET_WEIGHT,");
				sb.Append("                                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(SUM(INCOME * (NO_OF_BOXES)), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(SUM(INCOME * (VOLUME_IN_CBM)), 0)");
				sb.Append("                                END INCOME,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(MAX(EXPENSES * (NO_OF_BOXES)), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(MAX(EXPENSES * (VOLUME_IN_CBM)), 0)");
				sb.Append("                                END EXPENSES,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(MAX(INCOME * (NO_OF_BOXES) -");
				sb.Append("                                           EXPENSES * (ALLOCTEU)),");
				sb.Append("                                       0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(MAX(INCOME * (VOLUME_IN_CBM) -");
				sb.Append("                                           EXPENSES * (VOLUME_IN_CBM)),");
				sb.Append("                                       0)");
				sb.Append("                                END PROFIT_MARGIN,");
				sb.Append("                                NO_OF_BOXES,");
				sb.Append("                                CARGO_TYPE,");
				sb.Append("                                GROSS_WEIGHT");
				sb.Append("                  FROM (SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BST.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BST.BOOKING_REF_NO,");
				sb.Append("                                        VST.VESSEL_NAME,");
				sb.Append("                                        VVT.VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '20' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC20DV,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '40' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC40DV,");
				sb.Append("                                        CTMT.CONTAINER_KIND,");
				sb.Append("                                        CTMT.CONTAINER_TYPE_MST_ID,");
				sb.Append("                                        BTSFL.NO_OF_BOXES,");
				sb.Append("                                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                                        BST.NET_WEIGHT,");
				sb.Append("                                        BST.VOLUME_IN_CBM,");
				sb.Append("                                        NVL(BTSFD.TARIFF_RATE,0)*");
				sb.Append("                                        GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) INCOME,");
				sb.Append("                                        BST.GROSS_WEIGHT,");
				sb.Append("                                        BST.CARGO_TYPE,");
				sb.Append("                                        (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),");
				sb.Append("                                                    0)");
				sb.Append("                                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                                            AND CST.COST_ELEMENT_ID =");
				sb.Append("                                                FREIGHT_ELEMENT_ID) EXPENSES");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BST,");
				sb.Append("                               BOOKING_TRN  BTSFL,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BTSFD,");
				sb.Append("                               CONTAINER_TYPE_MST_TBL   CTMT,");
				sb.Append("                               VESSEL_VOYAGE_TBL        VST,");
				sb.Append("                               VESSEL_VOYAGE_TRN        VVT,");
				sb.Append("                               PORT_MST_TBL             POL,");
				sb.Append("                               PORT_MST_TBL             POD,");
				sb.Append("                               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL    CUMT ");
				//sb.Append("                               ,COMMODITY_GROUP_MST_TBL  CGMT,")
				//sb.Append("                               COMMODITY_MST_TBL        CGT")
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND FMT.FREIGHT_ELEMENT_MST_PK =");
				sb.Append("                               BTSFD.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK = BTSFD.CURRENCY_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BST.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("                           AND BTSFD.BOOKING_TRN_FK = BTSFL.BOOKING_TRN_PK");
				sb.Append("                           AND VST.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("                           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				//sb.Append("                           AND CGMT.COMMODITY_GROUP_PK(+) =")
				//sb.Append("                               BST.COMMODITY_GROUP_FK")
				//sb.Append("                           AND CGT.COMMODITY_GROUP_FK =")
				//sb.Append("                               CGMT.COMMODITY_GROUP_PK")
				sb.Append("                           AND CTMT.CONTAINER_TYPE_MST_PK(+) =");
				sb.Append("                               BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("                           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BST.IS_EBOOKING = 0");
				sb.Append("                           AND BST.CARGO_TYPE <> 4");
				sb.Append("                           AND BST.STATUS IN (2, 6) ");
				sb.Append("                           AND BTSFD.TARIFF_RATE > 0");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BST.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND BTSFL.COMMODITY_MST_FK IN(" + CommPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BST.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				//If CommPK.Trim.Length > 0 Then
				//    sb.Append(" AND CGT.COMMODITY_MST_PK IN(" & CommPK & ")")
				//End If




				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BST.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                         UNION");

				sb.Append("                        SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                                        CST.COST_ELEMENT_ID FREIGHT_ELEMENT_ID,");
				sb.Append("                                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BST.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BST.BOOKING_REF_NO,");
				sb.Append("                                        VST.VESSEL_NAME,");
				sb.Append("                                        VVT.VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '20' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC20DV,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '40' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC40DV,");
				sb.Append("                                        CTMT.CONTAINER_KIND,");
				sb.Append("                                        CTMT.CONTAINER_TYPE_MST_ID,");
				sb.Append("                                        BTSFL.NO_OF_BOXES,");
				sb.Append("                                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                                        BST.NET_WEIGHT,");
				sb.Append("                                        BST.VOLUME_IN_CBM,");
				sb.Append("                                        0 INCOME,");
				sb.Append("                                        BST.GROSS_WEIGHT,");
				sb.Append("                                        BST.CARGO_TYPE,");
				sb.Append("                                        NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES ");
				sb.Append("                          FROM LOCATION_MST_TBL            LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL            EMT,");
				sb.Append("                               CUSTOMER_MST_TBL            CMT,");
				sb.Append("                               BOOKING_MST_TBL             BST,");
				sb.Append("                               BOOKING_TRN     BTSFL,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS    BTSFD,");
				sb.Append("                               CONTAINER_TYPE_MST_TBL      CTMT,");
				sb.Append("                               VESSEL_VOYAGE_TBL           VST,");
				sb.Append("                               VESSEL_VOYAGE_TRN           VVT,");
				sb.Append("                               PORT_MST_TBL                POL,");
				sb.Append("                               PORT_MST_TBL                POD,");
				sb.Append("                               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                               QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL       CUMT ");
				//sb.Append("                              ,COMMODITY_GROUP_MST_TBL     CGMT,")
				//sb.Append("                               COMMODITY_MST_TBL           CGT")
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                           AND Q.QUOTATION_FK = BST.BOOKING_MST_PK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK =  Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BST.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("                           AND BTSFD.BOOKING_TRN_FK = BTSFL.BOOKING_TRN_PK");
				sb.Append("                           AND VST.VESSEL_VOYAGE_TBL_PK(+) =");
				sb.Append("                               VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("                           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				//sb.Append("                           AND CGMT.COMMODITY_GROUP_PK(+) =")
				//sb.Append("                               BST.COMMODITY_GROUP_FK")
				//sb.Append("                           AND CGT.COMMODITY_GROUP_FK =")
				//sb.Append("                               CGMT.COMMODITY_GROUP_PK")
				sb.Append("                           AND CTMT.CONTAINER_TYPE_MST_PK(+) =");
				sb.Append("                               BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("                           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = 1521)");
				sb.Append("                           AND BST.IS_EBOOKING = 0");
				sb.Append("                           AND BST.CARGO_TYPE <> 4");
				sb.Append("                           AND BST.STATUS IN (2, 6) ");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BST.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND BTSFL.COMMODITY_MST_FK IN(" + CommPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BST.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				//If CommPK.Trim.Length > 0 Then
				//    sb.Append(" AND CGT.COMMODITY_MST_PK IN(" & CommPK & ")")
				//End If

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BST.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                        )");
				sb.Append("                 GROUP BY BOOKING_MST_PK,");
				sb.Append("                          CONTAINER_TYPE_MST_ID,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                          LOCATION_NAME,");
				sb.Append("                          EMPLOYEE_ID,");
				sb.Append("                          EMPLOYEE_NAME,");
				sb.Append("                          CUSTOMER_ID,");
				sb.Append("                          CUSTOMER_NAME,");
				sb.Append("                          BOOKING_REF_NO,");
				sb.Append("                          BOOKING_REF_NO1,");
				sb.Append("                          VESSEL_NAME,");
				sb.Append("                          VOYAGE,");
				sb.Append("                          POL_ID,");
				sb.Append("                          CARGO_TYPE,");
				sb.Append("                          NO_OF_BOXES,");
				sb.Append("                          GROSS_WEIGHT,");
				sb.Append("                          POD_ID)");

				sb.Append(" GROUP BY BOOKING_MST_PK,");
				sb.Append("          LOCATION_NAME,");
				sb.Append("          EMPLOYEE_ID,");
				sb.Append("          EMPLOYEE_NAME,");
				sb.Append("          CUSTOMER_ID,");
				sb.Append("          CUSTOMER_NAME,");
				sb.Append("          BOOKING_REF_NO,");
				sb.Append("          BOOKING_REF_NO1,");
				sb.Append("          VESSEL_NAME,");
				sb.Append("          VOYAGE,");
				sb.Append("          POL_ID,");
				sb.Append("          CARGO_TYPE,");
				sb.Append("          POD_ID)T");

				sb.Append(" WHERE  T.INCOME > 0 ");
				if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.VOLUME_IN_CBM <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.VOLUME_IN_CBM >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.VOLUME_IN_CBM =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.NET_WEIGHT <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.NET_WEIGHT >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.NET_WEIGHT =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.ALLOCTEU <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.ALLOCTEU >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.ALLOCTEU =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.INCOME <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.INCOME>" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.INCOME =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.EXPENSES <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.EXPENSES  >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.EXPENSES  =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.PROFIT_MARGIN <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.PROFIT_MARGIN >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.PROFIT_MARGIN  =" + txtSymbol);
				}

				DA = objWK.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "LOCATION");

				strBuilder.Append("-1,");
				if (MainDS.Tables[0].Rows.Count > 0) {
					for (Count = 0; Count <= MainDS.Tables[Count].Rows.Count - 1; Count++) {
						strBuilder.Append(Convert.ToString(MainDS.Tables[0].Rows[Count]["BOOKING_MST_PK"]).Trim() + ",");
					}
				}
				strBuilder.Remove(strBuilder.Length - 1, 1);
				BookingPKs = strBuilder.ToString();

				sb.Remove(0, sb.Length);
				sb.Append(" SELECT Q.* FROM(");
				sb.Append(" SELECT * FROM(");
				sb.Append("SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                FREIGHT_ELEMENT_ID,");
				sb.Append("                FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as CURRENCY_ID,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(SUM(INCOME * (NO_OF_BOXES)), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(SUM(INCOME * (VOLUME_IN_CBM)), 0)");
				sb.Append("                END INCOME,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(MAX(EXPENSES * (NO_OF_BOXES)), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(MAX(EXPENSES * (VOLUME_IN_CBM)), 0)");
				sb.Append("                END EXPENSES,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(SUM(INCOME * NO_OF_BOXES),0) - NVL(MAX(EXPENSES * NO_OF_BOXES),");
				sb.Append("                       0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(SUM(INCOME * VOLUME_IN_CBM),0) - ");
				sb.Append("                           NVL(MAX(EXPENSES * VOLUME_IN_CBM),");
				sb.Append("                       0)");
				sb.Append("                END PROFIT_MARGIN,");
				sb.Append("                NVL(MAX(ALLOCTEU * (NO_OF_BOXES)), 0) ALLOCTEU,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                END NET_WEIGHT,");
				sb.Append("                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM");
				sb.Append("  FROM (SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + "  as       CURRENCY_ID,");
				sb.Append("                        NVL(BTSFD.TARIFF_RATE,0)*");
				sb.Append("                        GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) INCOME,");
				sb.Append("                        (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)), 0)");
				sb.Append("                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                            AND CST.COST_ELEMENT_ID = FREIGHT_ELEMENT_ID) EXPENSES,");
				sb.Append("                        NVL(TARIFF_RATE*GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) -");
				sb.Append("                            (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append("                             GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)), 0)");
				sb.Append("                               FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                    COST_ELEMENT_MST_TBL        CST");
				sb.Append("                              WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                    Q.COST_ELEMENT_MST_FK");
				sb.Append("                                AND CST.COST_ELEMENT_ID = FREIGHT_ELEMENT_ID),");
				sb.Append("                            0) PROFIT_MARGIN,");
				sb.Append("                        BTSFL.NO_OF_BOXES,");
				sb.Append("                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                        BST.NET_WEIGHT,");
				sb.Append("                        BST.VOLUME_IN_CBM,");
				sb.Append("                        BST.GROSS_WEIGHT,");
				sb.Append("                        BST.CARGO_TYPE");
				sb.Append("          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("               CUSTOMER_MST_TBL         CMT,");
				sb.Append("               BOOKING_MST_TBL          BST,");
				sb.Append("               BOOKING_TRN  BTSFL,");
				sb.Append("               BOOKING_TRN_FRT_DTLS BTSFD,");
				sb.Append("               CONTAINER_TYPE_MST_TBL   CTMT,");
				sb.Append("               VESSEL_VOYAGE_TBL        VST,");
				sb.Append("               VESSEL_VOYAGE_TRN        VVT,");
				sb.Append("               PORT_MST_TBL             POL,");
				sb.Append("               PORT_MST_TBL             POD,");
				sb.Append("               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("               CURRENCY_TYPE_MST_TBL    CUMT ");
				//sb.Append("               ,COMMODITY_GROUP_MST_TBL  CGMT,")
				//sb.Append("               COMMODITY_MST_TBL        CGT")
				sb.Append("         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("           AND FMT.FREIGHT_ELEMENT_MST_PK = BTSFD.FREIGHT_ELEMENT_MST_FK");
				sb.Append("           AND CUMT.CURRENCY_MST_PK = BTSFD.CURRENCY_MST_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK(+) = BST.CUSTOMER_MST_FK");
				//sb.Append("           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("           AND BTSFD.BOOKING_TRN_FK = BTSFL.BOOKING_TRN_PK");
				sb.Append("           AND VST.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				//sb.Append("           AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK")
				//sb.Append("           AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("           AND CTMT.CONTAINER_TYPE_MST_PK(+) = BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
				sb.Append("           AND POL.LOCATION_MST_FK IN");
				sb.Append("               (SELECT L.LOCATION_MST_PK");
				sb.Append("                  FROM LOCATION_MST_TBL L");
				sb.Append("                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("           AND BST.IS_EBOOKING = 0");
				sb.Append("           AND BST.CARGO_TYPE <> 4");
				sb.Append("           AND BST.STATUS IN (2, 6) ");
				sb.Append("           AND BTSFD.TARIFF_RATE > 0");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				sb.Append(" AND BST.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("        UNION");

				sb.Append("        SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                        CST.COST_ELEMENT_ID   FREIGHT_ELEMENT_ID,");
				sb.Append("                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as        CURRENCY_ID,");
				sb.Append("                        0                     INCOME,");
				sb.Append("                        NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES,");
				sb.Append("                        (0 - NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)) PROFIT_MARGIN,");
				sb.Append("                        BTSFL.NO_OF_BOXES,");
				sb.Append("                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                        BST.NET_WEIGHT,");
				sb.Append("                        BST.VOLUME_IN_CBM,");
				sb.Append("                        BST.GROSS_WEIGHT,");
				sb.Append("                        BST.CARGO_TYPE");
				sb.Append("          FROM LOCATION_MST_TBL            LMT,");
				sb.Append("               EMPLOYEE_MST_TBL            EMT,");
				sb.Append("               CUSTOMER_MST_TBL            CMT,");
				sb.Append("               BOOKING_MST_TBL             BST,");
				sb.Append("               BOOKING_TRN     BTSFL,");
				sb.Append("               BOOKING_TRN_FRT_DTLS    BTSFD,");
				sb.Append("               CONTAINER_TYPE_MST_TBL      CTMT,");
				sb.Append("               VESSEL_VOYAGE_TBL           VST,");
				sb.Append("               VESSEL_VOYAGE_TRN           VVT,");
				sb.Append("               PORT_MST_TBL                POL,");
				sb.Append("               PORT_MST_TBL                POD,");
				sb.Append("               CURRENCY_TYPE_MST_TBL       CUMT,");
				//sb.Append("               COMMODITY_GROUP_MST_TBL     CGMT,")
				//sb.Append("               COMMODITY_MST_TBL           CGT,")
				sb.Append("               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("               QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("           AND Q.QUOTATION_FK = BST.BOOKING_MST_PK");
				sb.Append("           AND CUMT.CURRENCY_MST_PK = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK(+) = BST.CUSTOMER_MST_FK");
				//sb.Append("           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("           AND BTSFD.BOOKING_TRN_FK = BTSFL.BOOKING_TRN_PK");
				sb.Append("           AND VST.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				//sb.Append("           AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK")
				//sb.Append("           AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("           AND CTMT.CONTAINER_TYPE_MST_PK(+) = BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
				sb.Append("           AND POL.LOCATION_MST_FK IN");
				sb.Append("               (SELECT L.LOCATION_MST_PK");
				sb.Append("                  FROM LOCATION_MST_TBL L");
				sb.Append("                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("           AND BST.IS_EBOOKING = 0");
				sb.Append("           AND BST.CARGO_TYPE <> 4");
				sb.Append("           AND BST.STATUS IN (2, 6) ");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				sb.Append(" AND BST.BOOKING_MST_PK IN(" + BookingPKs + ")");
				sb.Append("                   )");

				sb.Append(" GROUP BY BOOKING_MST_PK, FREIGHT_ELEMENT_ID,FREIGHT_ELEMENT_NAME, CARGO_TYPE)T)Q ,freight_element_mst_tbl femt WHERE");
				sb.Append(" q.freight_element_id=femt.freight_element_id ORDER BY femt.preference");


				DA = objWK.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "FREIGHT");

				DataRelation rel = new DataRelation("LOCATION", new DataColumn[] { MainDS.Tables[0].Columns["BOOKING_MST_PK"] }, new DataColumn[] { MainDS.Tables[1].Columns["BOOKING_MST_PK"] });

				rel.Nested = true;
				MainDS.Relations.Add(rel);

				return MainDS;

			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public DataSet FetchAirGridDetail(string ddBasis = "0", string ddSymbols = "0", string Commodityfk = "0", double txtSymbol = 0.0, string BkgPK = "", string CustPK = "", string CommPK = "", string ExecPK = "", Int32 LocFk = 0, string fromDate = "",
		string toDate = "", Int32 flag = 0)
		{

			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			OracleDataAdapter DA = new OracleDataAdapter();
			DataSet MainDS = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			string strCondition = " ";
			int Count = 0;
			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			string BookingPKs = null;
			int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);

			try {
				sb.Append("SELECT *");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        LOCATION_NAME,");
				sb.Append("                        EMPLOYEE_ID,");
				sb.Append("                        EMPLOYEE_NAME,");
				sb.Append("                        CUSTOMER_ID,");
				sb.Append("                        CUSTOMER_NAME,");
				sb.Append("                        BOOKING_REF_NO1,");
				sb.Append("                        BOOKING_REF_NO,");
				sb.Append("                        VESSEL_NAME,");
				sb.Append("                        VOYAGE,");
				sb.Append("                        POL_ID,");
				sb.Append("                        POD_ID,");
				sb.Append("                        NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                        NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                        NVL(MAX(ALLOCTEU), 0) ALLOCTEU,");
				sb.Append("                        NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                        NVL(SUM(INCOME), 0) INCOME,");
				sb.Append("                        NVL(SUM(EXPENSES), 0) EXPENSES,");
				sb.Append("                        NVL(SUM(INCOME - EXPENSES), 0) PROFIT_MARGIN");
				sb.Append("          FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                                '' CONTAINER_TYPE_MST_ID,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                                LOCATION_NAME,");
				sb.Append("                                EMPLOYEE_ID,");
				sb.Append("                                EMPLOYEE_NAME,");
				sb.Append("                                CUSTOMER_ID,");
				sb.Append("                                CUSTOMER_NAME,");
				sb.Append("                                BOOKING_REF_NO1,");
				sb.Append("                                BOOKING_REF_NO,");
				sb.Append("                                VESSEL_NAME,");
				sb.Append("                                VOYAGE,");
				sb.Append("                                POL_ID,");
				sb.Append("                                POD_ID,");
				sb.Append("                                NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                                NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                                NVL(MAX(ALLOCTEU), 0) ALLOCTEU,");
				sb.Append("                                NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                                NVL(SUM(INCOME * NET_WEIGHT), 0) INCOME,");
				sb.Append("                                NVL(MAX(EXPENSES * NET_WEIGHT), 0) EXPENSES,");
				sb.Append("                                NVL(SUM(INCOME * NET_WEIGHT), 0) - ");
				sb.Append("                                        NVL(MAX(EXPENSES * NET_WEIGHT),");
				sb.Append("                                    0)");
				sb.Append("                  FROM (SELECT DISTINCT BMT.BOOKING_MST_PK BOOKING_MST_PK,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BMT.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BMT.BOOKING_REF_NO,");
				sb.Append("                                        AMT.AIRLINE_NAME VESSEL_NAME,");
				sb.Append("                                        JOB.VOYAGE_FLIGHT_NO VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        0 ALLOC20DV,");
				sb.Append("                                        0 ALLOC40DV,");
				sb.Append("                                        0 ALLOCTEU,");
				sb.Append("                                        BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                        BMT.VOLUME_IN_CBM VOLUME_IN_CBM,");
				sb.Append("                                        NVL(BFT.MIN_BASIS_RATE,0)*");
				sb.Append("                                        GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) INCOME,");
				sb.Append("                                        (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),");
				sb.Append("                                                    0)");
				sb.Append("                                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                                            AND CST.COST_ELEMENT_ID =");
				sb.Append("                                                FREIGHT_ELEMENT_ID) EXPENSES");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BMT,");
				sb.Append("                               BOOKING_TRN          BTT,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL    CUMT,");
				sb.Append("                               PORT_MST_TBL             POL,");
				sb.Append("                               PORT_MST_TBL             POD,");
				sb.Append("                               AIRLINE_MST_TBL          AMT,");
				sb.Append("                               JOB_CARD_TRN     JOB ");
				//sb.Append("                               ,COMMODITY_GROUP_MST_TBL  CGMT,")
				//sb.Append("                               COMMODITY_MST_TBL        CGT")
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND FMT.FREIGHT_ELEMENT_MST_PK(+) =");
				sb.Append("                               BFT.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK(+) = BFT.CURRENCY_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BMT.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                           AND BTT.BOOKING_TRN_PK = BFT.BOOKING_TRN_FK(+)");
				sb.Append("                           AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                           AND BMT.BOOKING_MST_PK = BTT.BOOKING_MST_FK AND BMT.BUSINESS_TYPE = 1");
				//sb.Append("                           AND CGMT.COMMODITY_GROUP_PK(+) =")
				//sb.Append("                               BMT.COMMODITY_GROUP_FK")
				//sb.Append("                           AND CGT.COMMODITY_GROUP_FK =")
				//sb.Append("                               CGMT.COMMODITY_GROUP_PK")
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BMT.IS_EBOOKING = 0");
				sb.Append("                           AND BMT.STATUS IN (2, 6) ");
				sb.Append("                           AND BFT.MIN_BASIS_RATE > 0");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BMT.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND BTT.COMMODITY_MST_FK IN(" + CommPK + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				//If CommPK.Trim.Length > 0 Then
				//    sb.Append(" AND CGT.COMMODITY_MST_PK IN(" & CommPK & ")")
				//End If

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BMT.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                        UNION");

				sb.Append("                        SELECT DISTINCT BMT.BOOKING_MST_PK,");
				sb.Append("                                        CST.COST_ELEMENT_ID   FREIGHT_ELEMENT_ID,");
				sb.Append("                                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BMT.BOOKING_REF_NO    BOOKING_REF_NO1,");
				sb.Append("                                        BMT.BOOKING_REF_NO,");
				sb.Append("                                        AMT.AIRLINE_NAME      VESSEL_NAME,");
				sb.Append("                                        JOB.VOYAGE_FLIGHT_NO         VOYAGE,");
				sb.Append("                                        POL.PORT_NAME         POL_ID,");
				sb.Append("                                        POD.PORT_NAME         POD_ID,");
				sb.Append("                                        0                     ALLOC20DV,");
				sb.Append("                                        0                     ALLOC40DV,");
				sb.Append("                                        0                     ALLOCTEU,");
				sb.Append("                                        BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                        BMT.VOLUME_IN_CBM     VOLUME_IN_CBM,");
				sb.Append("                                        0                     INCOME,");
				sb.Append("                                        NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES ");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BMT,");
				sb.Append("                               BOOKING_TRN          BTT,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL       CUMT,");
				sb.Append("                               PORT_MST_TBL                POL,");
				sb.Append("                               PORT_MST_TBL                POD,");
				sb.Append("                               AIRLINE_MST_TBL             AMT,");
				sb.Append("                               JOB_CARD_TRN        JOB,");
				//sb.Append("                               COMMODITY_GROUP_MST_TBL     CGMT,")
				//sb.Append("                               COMMODITY_MST_TBL           CGT,")
				sb.Append("                               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                               QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                           AND Q.QUOTATION_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK(+) = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BMT.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                           AND BTT.BOOKING_TRN_PK = BFT.BOOKING_TRN_FK");
				sb.Append("                           AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                           AND BMT.BOOKING_MST_PK = BTT.BOOKING_MST_FK");
				//sb.Append("                           AND CGMT.COMMODITY_GROUP_PK(+) =")
				//sb.Append("                               BMT.COMMODITY_GROUP_FK")
				//sb.Append("                           AND CGT.COMMODITY_GROUP_FK =")
				//sb.Append("                               CGMT.COMMODITY_GROUP_PK")
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BMT.IS_EBOOKING = 0");
				sb.Append("                           AND BMT.STATUS IN (2, 6) ");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND BTT.COMMODITY_MST_FK IN(" + CommPK + ")");
				}

				if (Convert.ToInt32(Commodityfk )> 0) {
					sb.Append(" AND BMT.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				//If CommPK.Trim.Length > 0 Then
				//    sb.Append(" AND CGT.COMMODITY_MST_PK IN(" & CommPK & ")")
				//End If

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BMT.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                 )");

				sb.Append("                 GROUP BY BOOKING_MST_PK,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                          LOCATION_NAME,");
				sb.Append("                          EMPLOYEE_ID,");
				sb.Append("                          EMPLOYEE_NAME,");
				sb.Append("                          CUSTOMER_ID,");
				sb.Append("                          CUSTOMER_NAME,");
				sb.Append("                          BOOKING_REF_NO,");
				sb.Append("                          BOOKING_REF_NO1,");
				sb.Append("                          VESSEL_NAME,");
				sb.Append("                          VOYAGE,");
				sb.Append("                          POL_ID,");
				sb.Append("                          POD_ID");
				sb.Append("                         )");

				sb.Append("         GROUP BY BOOKING_MST_PK,");
				sb.Append("                  LOCATION_NAME,");
				sb.Append("                  EMPLOYEE_ID,");
				sb.Append("                  EMPLOYEE_NAME,");
				sb.Append("                  CUSTOMER_ID,");
				sb.Append("                  CUSTOMER_NAME,");
				sb.Append("                  BOOKING_REF_NO,");
				sb.Append("                  BOOKING_REF_NO1,");
				sb.Append("                  VESSEL_NAME,");
				sb.Append("                  VOYAGE,");
				sb.Append("                  POL_ID,");
				sb.Append("                  POD_ID) T");

				sb.Append(" WHERE  T.INCOME >0 ");
				if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.VOLUME_IN_CBM <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.VOLUME_IN_CBM >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.VOLUME_IN_CBM =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.NET_WEIGHT<" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.NET_WEIGHT >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.NET_WEIGHT=" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.INCOME <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.INCOME>" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.INCOME =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.EXPENSES <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.EXPENSES  >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.EXPENSES  =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.PROFIT_MARGIN <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.PROFIT_MARGIN >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.PROFIT_MARGIN  =" + txtSymbol);
				}

				DA = objWK.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "LOCATION");

				strBuilder.Append("-1,");
				if (MainDS.Tables[0].Rows.Count > 0) {
					for (Count = 0; Count <= MainDS.Tables[Count].Rows.Count - 1; Count++) {
						strBuilder.Append(Convert.ToString(MainDS.Tables[0].Rows[Count]["BOOKING_MST_PK"]).Trim() + ",");
					}
				}
				strBuilder.Remove(strBuilder.Length - 1, 1);
				BookingPKs = strBuilder.ToString();

				sb.Remove(0, sb.Length);
				sb.Append("SELECT Q.* from (");
				sb.Append("SELECT * ");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        FREIGHT_ELEMENT_ID,");
				sb.Append("                        FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as        CURRENCY_ID,");
				sb.Append("                        NVL(SUM(INCOME * NET_WEIGHT), 0) INCOME,");
				sb.Append("                        NVL(MAX(EXPENSES * NET_WEIGHT), 0) EXPENSES,");
				sb.Append("                        NVL(SUM(INCOME * NET_WEIGHT),0) - ");
				sb.Append("                                NVL(MAX(EXPENSES * NET_WEIGHT),");
				sb.Append("                            0) PROFIT_MARGIN,");
				sb.Append("                        NVL(SUM(ALLOCTEU), 0) ALLOCTEU,");
				sb.Append("                        NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM");
				sb.Append("          FROM (SELECT DISTINCT BMT.BOOKING_MST_PK BOOKING_MST_PK,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_MST_PK,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as                CURRENCY_ID,");
				sb.Append("                                0 ALLOCTEU,");
				sb.Append("                                BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                BMT.VOLUME_IN_CBM,");
				sb.Append("                                NVL(BFT.MIN_BASIS_RATE,0)*");
				sb.Append("                                GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) INCOME,");
				sb.Append("                                (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append("                                 GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),");
				sb.Append("                                            0)");
				sb.Append("                                   FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                        COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                  WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                    AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                        Q.COST_ELEMENT_MST_FK");
				sb.Append("                                    AND CST.COST_ELEMENT_ID =");
				sb.Append("                                        FREIGHT_ELEMENT_ID) EXPENSES,");
				sb.Append("                                NVL(TARIFF_RATE*GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) -");
				sb.Append("                                    (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                    GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),");
				sb.Append("                                                0)");
				sb.Append("                                       FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                            COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                      WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                        AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                            Q.COST_ELEMENT_MST_FK");
				sb.Append("                                        AND CST.COST_ELEMENT_ID =");
				sb.Append("                                            FREIGHT_ELEMENT_ID),");
				sb.Append("                                    0) PROFIT_MARGIN");
				sb.Append("                  FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                       EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                       CUSTOMER_MST_TBL         CMT,");
				sb.Append("                       BOOKING_MST_TBL          BMT,");
				sb.Append("                       BOOKING_TRN          BTT,");
				sb.Append("                       BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                       FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                       CURRENCY_TYPE_MST_TBL    CUMT,");
				sb.Append("                       PORT_MST_TBL             POL,");
				sb.Append("                       PORT_MST_TBL             POD,");
				sb.Append("                       AIRLINE_MST_TBL          AMT,");
				sb.Append("                       JOB_CARD_TRN     JOB ");
				//sb.Append("                       ,COMMODITY_GROUP_MST_TBL  CGMT,")
				//sb.Append("                       COMMODITY_MST_TBL        CGT")
				sb.Append("                 WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                   AND FMT.FREIGHT_ELEMENT_MST_PK(+) =");
				sb.Append("                       BFT.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                   AND CUMT.CURRENCY_MST_PK(+) = BFT.CURRENCY_MST_FK");
				sb.Append("                   AND CMT.CUSTOMER_MST_PK(+) = BMT.CUSTOMER_MST_FK");
				//sb.Append("                   AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                   AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                   AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                   AND BTT.BOOKING_TRN_PK = BFT.BOOKING_TRN_FK(+)");
				sb.Append("                   AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                   AND BMT.BOOKING_MST_PK = BTT.BOOKING_MST_FK");
				//sb.Append("                   AND CGMT.COMMODITY_GROUP_PK(+) = BMT.COMMODITY_GROUP_FK")
				//sb.Append("                   AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("                   AND POL.LOCATION_MST_FK IN");
				sb.Append("                       (SELECT L.LOCATION_MST_PK");
				sb.Append("                          FROM LOCATION_MST_TBL L");
				sb.Append("                         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                   AND BMT.IS_EBOOKING = 0");
				sb.Append("                   AND BMT.STATUS IN (2, 6) ");
				sb.Append("                   AND BFT.MIN_BASIS_RATE > 0");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				sb.Append(" AND BMT.BOOKING_MST_PK  IN(" + BookingPKs + ")");

				sb.Append("               UNION");

				sb.Append("                SELECT DISTINCT BMT.BOOKING_MST_PK      BOOKING_MST_PK,");
				sb.Append("                                CST.COST_ELEMENT_MST_PK FREIGHT_ELEMENT_MST_PK,");
				sb.Append("                                CST.COST_ELEMENT_ID     FREIGHT_ELEMENT_ID,");
				sb.Append("                                CST.COST_ELEMENT_NAME   FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as                CURRENCY_ID,");
				sb.Append("                                0                       ALLOCTEU,");
				sb.Append("                                BMT.CHARGEABLE_WEIGHT   NET_WEIGHT,");
				sb.Append("                                BMT.VOLUME_IN_CBM,");
				sb.Append("                                0 INCOME,");
				sb.Append("                                NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES,");
				sb.Append("                                (0 - NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)) PROFIT_MARGIN ");
				sb.Append("                  FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                       EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                       CUSTOMER_MST_TBL         CMT,");
				sb.Append("                       BOOKING_MST_TBL          BMT,");
				sb.Append("                       BOOKING_TRN          BTT,");
				sb.Append("                       BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                       CURRENCY_TYPE_MST_TBL       CUMT,");
				sb.Append("                       PORT_MST_TBL                POL,");
				sb.Append("                       PORT_MST_TBL                POD,");
				sb.Append("                       AIRLINE_MST_TBL             AMT,");
				sb.Append("                       JOB_CARD_TRN        JOB,");
				//sb.Append("                       COMMODITY_GROUP_MST_TBL     CGMT,")
				//sb.Append("                       COMMODITY_MST_TBL           CGT,")
				sb.Append("                       COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                       QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("                 WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                   AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                   AND Q.QUOTATION_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND CUMT.CURRENCY_MST_PK(+) = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                   AND CMT.CUSTOMER_MST_PK(+) = BMT.CUSTOMER_MST_FK");
				//sb.Append("                   AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                   AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                   AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                   AND BTT.BOOKING_TRN_PK = BFT.BOOKING_TRN_FK(+)");
				sb.Append("                   AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                   AND BMT.BOOKING_MST_PK = BTT.BOOKING_MST_FK");
				//sb.Append("                   AND CGMT.COMMODITY_GROUP_PK(+) = BMT.COMMODITY_GROUP_FK")
				//sb.Append("                   AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("                   AND POL.LOCATION_MST_FK IN");
				sb.Append("                       (SELECT L.LOCATION_MST_PK");
				sb.Append("                          FROM LOCATION_MST_TBL L");
				sb.Append("                         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                   AND BMT.IS_EBOOKING = 0");
				sb.Append("                   AND BMT.STATUS IN (2, 6) ");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("                   )");

				sb.Append(" GROUP BY BOOKING_MST_PK,");
				sb.Append("          FREIGHT_ELEMENT_ID,");
				sb.Append("          FREIGHT_ELEMENT_NAME)T)Q ,freight_element_mst_tbl femt WHERE");
				sb.Append(" q.freight_element_id=femt.freight_element_id ORDER BY femt.preference");




				DA = objWK.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "FREIGHT");

				DataRelation rel = new DataRelation("LOCATION", new DataColumn[] { MainDS.Tables[0].Columns["BOOKING_MST_PK"] }, new DataColumn[] { MainDS.Tables[1].Columns["BOOKING_MST_PK"] });

				rel.Nested = true;
				MainDS.Relations.Add(rel);

				return MainDS;

			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public DataSet FetchBothGridDetail(string ddBasis = "0", string ddSymbols = "0", string Commodityfk = "0", double txtSymbol = 0.0, string BkgPK = "", string CustPK = "", string CommPK = "", string ExecPK = "", Int32 LocFk = 0, string fromDate = "",
		string toDate = "", Int32 flag = 0)
		{

			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			WorkFlow objWK = new WorkFlow();
			OracleDataAdapter DA = new OracleDataAdapter();
			DataSet MainDS = new DataSet();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			string strCondition = " ";
			int Count = 0;
			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			string BookingPKs = null;
			int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);

			try {
				sb.Append(" SELECT * ");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        LOCATION_NAME,");
				sb.Append("                        EMPLOYEE_ID,");
				sb.Append("                        EMPLOYEE_NAME,");
				sb.Append("                        CUSTOMER_ID,");
				sb.Append("                        CUSTOMER_NAME,");
				sb.Append("                        BOOKING_REF_NO1,");
				sb.Append("                        BOOKING_REF_NO,");
				sb.Append("                        VESSEL_NAME,");
				sb.Append("                        VOYAGE,");
				sb.Append("                        POL_ID,");
				sb.Append("                        POD_ID,");
				sb.Append("                        NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                        NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                         NVL(SUM(DISTINCT ALLOCTEU), 0) ALLOCTEU,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                        END NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(INCOME), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(INCOME), 0)");
				sb.Append("                        END INCOME,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(EXPENSES), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(EXPENSES), 0)");
				sb.Append("                        END EXPENSES,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(INCOME -");
				sb.Append("                                   EXPENSES),");
				sb.Append("                               0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(INCOME -");
				sb.Append("                                   EXPENSES),");
				sb.Append("                               0)");
				sb.Append("                        END PROFIT_MARGIN");
				sb.Append("          FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                                CONTAINER_TYPE_MST_ID,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                                LOCATION_NAME,");
				sb.Append("                                EMPLOYEE_ID,");
				sb.Append("                                EMPLOYEE_NAME,");
				sb.Append("                                CUSTOMER_ID,");
				sb.Append("                                CUSTOMER_NAME,");
				sb.Append("                                BOOKING_REF_NO1,");
				sb.Append("                                BOOKING_REF_NO,");
				sb.Append("                                VESSEL_NAME,");
				sb.Append("                                VOYAGE,");
				sb.Append("                                POL_ID,");
				sb.Append("                                POD_ID,");
				sb.Append("                                NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                                NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                               NVL(SUM(DISTINCT ALLOCTEU * (NO_OF_BOXES)), 0) ALLOCTEU,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                                END NET_WEIGHT,");
				sb.Append("                                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(SUM(INCOME * (NO_OF_BOXES)), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(SUM(INCOME * (VOLUME_IN_CBM)), 0)");
				sb.Append("                                END INCOME,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(MAX(EXPENSES * (NO_OF_BOXES)), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(MAX(EXPENSES * (VOLUME_IN_CBM)), 0)");
				sb.Append("                                END EXPENSES,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(SUM(INCOME * NO_OF_BOXES),0) - ");
				sb.Append("                                           NVL(MAX(EXPENSES * NO_OF_BOXES),");
				sb.Append("                                       0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(SUM(INCOME * VOLUME_IN_CBM),0) - ");
				sb.Append("                                           NVL(MAX(EXPENSES * VOLUME_IN_CBM),");
				sb.Append("                                       0)");
				sb.Append("                                END PROFIT_MARGIN,");
				sb.Append("                                NO_OF_BOXES,");
				sb.Append("                                CARGO_TYPE,");
				sb.Append("                                GROSS_WEIGHT");
				sb.Append("                  FROM (SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BST.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BST.BOOKING_REF_NO,");
				sb.Append("                                        VST.VESSEL_NAME,");
				sb.Append("                                        VVT.VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '20' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC20DV,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '40' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC40DV,");
				sb.Append("                                        CTMT.CONTAINER_KIND,");
				sb.Append("                                        CTMT.CONTAINER_TYPE_MST_ID,");
				sb.Append("                                        BTSFL.NO_OF_BOXES,");
				sb.Append("                                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                                        BST.NET_WEIGHT,");
				sb.Append("                                        BST.VOLUME_IN_CBM,");
				sb.Append("                                        NVL(BTSFD.TARIFF_RATE,0)*");
				sb.Append("                                        GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) INCOME,");
				sb.Append("                                        BST.GROSS_WEIGHT,");
				sb.Append("                                        BST.CARGO_TYPE,");
				sb.Append("                                        (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),");
				sb.Append("                                                    0)");
				sb.Append("                                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                                            AND CST.COST_ELEMENT_ID =");
				sb.Append("                                                FREIGHT_ELEMENT_ID) EXPENSES");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BST,");
				sb.Append("                               BOOKING_TRN  BTSFL,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BTSFD,");
				sb.Append("                               CONTAINER_TYPE_MST_TBL   CTMT,");
				sb.Append("                               VESSEL_VOYAGE_TBL        VST,");
				sb.Append("                               VESSEL_VOYAGE_TRN        VVT,");
				sb.Append("                               PORT_MST_TBL             POL,");
				sb.Append("                               PORT_MST_TBL             POD,");
				sb.Append("                               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL    CUMT ");
				//sb.Append("                               ,COMMODITY_GROUP_MST_TBL  CGMT,")
				//sb.Append("                               COMMODITY_MST_TBL        CGT")
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND FMT.FREIGHT_ELEMENT_MST_PK =");
				sb.Append("                               BTSFD.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK = BTSFD.CURRENCY_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BST.CUSTOMER_MST_FK");
				// sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("                           AND BTSFD.BOOKING_TRN_FK =");
				sb.Append("                               BTSFL.BOOKING_TRN_PK");
				sb.Append("                           AND VST.VESSEL_VOYAGE_TBL_PK(+) =");
				sb.Append("                               VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("                           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				//sb.Append("                           AND CGMT.COMMODITY_GROUP_PK(+) =")
				//sb.Append("                               BST.COMMODITY_GROUP_FK")
				//sb.Append("                           AND CGT.COMMODITY_GROUP_FK =")
				//sb.Append("                               CGMT.COMMODITY_GROUP_PK")
				sb.Append("                           AND CTMT.CONTAINER_TYPE_MST_PK(+) =");
				sb.Append("                               BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("                           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BST.IS_EBOOKING = 0");
				sb.Append("                           AND BST.CARGO_TYPE <> 4");
				sb.Append("                           AND BST.STATUS IN (2, 6)");
				sb.Append("                           AND BTSFD.TARIFF_RATE > 0");
				sb.Append("                           AND BST.BUSINESS_TYPE = 2 ");
				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BST.BOOKING_MST_PK IN(" + BkgPK + ")");
				}


				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND BTSFL.COMMODITY_MST_FK IN(" + CommPK + ")");
				}


				try {
					if (Convert.ToInt32(Commodityfk) > 0) {
						sb.Append(" AND BST.Commodity_Group_Fk IN(" + Commodityfk + ")");
					}
				} catch (Exception ex) {
				}
				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				//If CommPK.Trim.Length > 0 Then
				//    sb.Append(" AND CGT.COMMODITY_MST_PK IN(" & CommPK & ")")
				//End If

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BST.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                         UNION ");

				sb.Append("                        SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                                        CST.COST_ELEMENT_ID FREIGHT_ELEMENT_ID,");
				sb.Append("                                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BST.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BST.BOOKING_REF_NO,");
				sb.Append("                                        VST.VESSEL_NAME,");
				sb.Append("                                        VVT.VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '20' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC20DV,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '40' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC40DV,");
				sb.Append("                                        CTMT.CONTAINER_KIND,");
				sb.Append("                                        CTMT.CONTAINER_TYPE_MST_ID,");
				sb.Append("                                        BTSFL.NO_OF_BOXES,");
				sb.Append("                                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                                        BST.NET_WEIGHT,");
				sb.Append("                                        BST.VOLUME_IN_CBM,");
				sb.Append("                                        0 INCOME,");
				sb.Append("                                        BST.GROSS_WEIGHT,");
				sb.Append("                                        BST.CARGO_TYPE,");
				sb.Append("                                        NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES");
				sb.Append("                          FROM LOCATION_MST_TBL            LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL            EMT,");
				sb.Append("                               CUSTOMER_MST_TBL            CMT,");
				sb.Append("                               BOOKING_MST_TBL             BST,");
				sb.Append("                               BOOKING_TRN     BTSFL,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS    BTSFD,");
				sb.Append("                               CONTAINER_TYPE_MST_TBL      CTMT,");
				sb.Append("                               VESSEL_VOYAGE_TBL           VST,");
				sb.Append("                               VESSEL_VOYAGE_TRN           VVT,");
				sb.Append("                               PORT_MST_TBL                POL,");
				sb.Append("                               PORT_MST_TBL                POD,");
				sb.Append("                               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                               QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL       CUMT ");
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                           AND Q.QUOTATION_FK = BST.BOOKING_MST_PK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BST.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("                           AND BTSFD.BOOKING_TRN_FK =");
				sb.Append("                               BTSFL.BOOKING_TRN_PK");
				sb.Append("                           AND VST.VESSEL_VOYAGE_TBL_PK(+) =");
				sb.Append("                               VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("                           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				sb.Append("                           AND CTMT.CONTAINER_TYPE_MST_PK(+) =");
				sb.Append("                               BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("                           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = 1521)");
				sb.Append("                           AND BST.IS_EBOOKING = 0");
				sb.Append("                           AND BST.CARGO_TYPE <> 4");
				sb.Append("                           AND BST.STATUS IN (2, 6)");
				sb.Append("                           AND BST.BUSINESS_TYPE = 2 ");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BST.BOOKING_MST_PK IN(" + BkgPK + ")");
				}
				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND BTSFL.COMMODITY_MST_FK IN(" + CommPK + ")");
				}
				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BST.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BST.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                        )");
				sb.Append("                 GROUP BY BOOKING_MST_PK,");
				sb.Append("                                CONTAINER_TYPE_MST_ID,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                          LOCATION_NAME,");
				sb.Append("                          EMPLOYEE_ID,");
				sb.Append("                          EMPLOYEE_NAME,");
				sb.Append("                          CUSTOMER_ID,");
				sb.Append("                          CUSTOMER_NAME,");
				sb.Append("                          BOOKING_REF_NO,");
				sb.Append("                          BOOKING_REF_NO1,");
				sb.Append("                          VESSEL_NAME,");
				sb.Append("                          VOYAGE,");
				sb.Append("                          POL_ID,");
				sb.Append("                          CARGO_TYPE,");
				sb.Append("                          NO_OF_BOXES,");
				sb.Append("                          GROSS_WEIGHT,");
				sb.Append("                          POD_ID)");

				sb.Append(" GROUP BY BOOKING_MST_PK,");
				sb.Append("          LOCATION_NAME,");
				sb.Append("          EMPLOYEE_ID,");
				sb.Append("          EMPLOYEE_NAME,");
				sb.Append("          CUSTOMER_ID,");
				sb.Append("          CUSTOMER_NAME,");
				sb.Append("          BOOKING_REF_NO,");
				sb.Append("          BOOKING_REF_NO1,");
				sb.Append("          VESSEL_NAME,");
				sb.Append("          VOYAGE,");
				sb.Append("          POL_ID,");
				sb.Append("          CARGO_TYPE,");
				sb.Append("          POD_ID)T");


				sb.Append(" WHERE  T.INCOME > 0 ");
				if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.VOLUME_IN_CBM <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.VOLUME_IN_CBM >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.VOLUME_IN_CBM =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.NET_WEIGHT <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.NET_WEIGHT >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.NET_WEIGHT =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.ALLOCTEU <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.ALLOCTEU >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.ALLOCTEU =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.INCOME <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.INCOME>" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.INCOME =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.EXPENSES <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.EXPENSES  >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.EXPENSES  =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.PROFIT_MARGIN <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.PROFIT_MARGIN >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.PROFIT_MARGIN  =" + txtSymbol);
				}

				sb.Append("  UNION ");

				sb.Append("  SELECT *");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        LOCATION_NAME,");
				sb.Append("                        EMPLOYEE_ID,");
				sb.Append("                        EMPLOYEE_NAME,");
				sb.Append("                        CUSTOMER_ID,");
				sb.Append("                        CUSTOMER_NAME,");
				sb.Append("                        BOOKING_REF_NO1,");
				sb.Append("                        BOOKING_REF_NO,");
				sb.Append("                        VESSEL_NAME,");
				sb.Append("                        VOYAGE,");
				sb.Append("                        POL_ID,");
				sb.Append("                        POD_ID,");
				sb.Append("                        NVL(MAX(ALLOC20DV), '') ALLOC20DV,");
				sb.Append("                        NVL(MAX(ALLOC40DV), '') ALLOC40DV,");
				sb.Append("                        NVL(MAX(ALLOCTEU), '') ALLOCTEU,");
				sb.Append("                        NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                        NVL(SUM(INCOME), 0) INCOME,");
				sb.Append("                        NVL(SUM(EXPENSES), 0) EXPENSES,");
				sb.Append("                        NVL(SUM(INCOME - EXPENSES), 0) PROFIT_MARGIN");
				sb.Append("          FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                                '' CONTAINER_TYPE_MST_ID,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                                LOCATION_NAME,");
				sb.Append("                                EMPLOYEE_ID,");
				sb.Append("                                EMPLOYEE_NAME,");
				sb.Append("                                CUSTOMER_ID,");
				sb.Append("                                CUSTOMER_NAME,");
				sb.Append("                                BOOKING_REF_NO1,");
				sb.Append("                                BOOKING_REF_NO,");
				sb.Append("                                VESSEL_NAME,");
				sb.Append("                                VOYAGE,");
				sb.Append("                                POL_ID,");
				sb.Append("                                POD_ID,");
				sb.Append("                                NVL(SUM(ALLOC20DV), '') ALLOC20DV,");
				sb.Append("                                NVL(SUM(ALLOC40DV), '') ALLOC40DV,");
				sb.Append("                                NVL(SUM(ALLOCTEU), '') ALLOCTEU,");
				sb.Append("                                NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                                NVL(SUM(INCOME * NET_WEIGHT), 0) INCOME,");
				sb.Append("                                NVL(MAX(EXPENSES * NET_WEIGHT), 0) EXPENSES,");
				sb.Append("                                NVL(SUM(INCOME * NET_WEIGHT),0) - ");
				sb.Append("                                NVL(MAX(EXPENSES * NET_WEIGHT),0) ");
				sb.Append("                  FROM (SELECT DISTINCT BMT.BOOKING_MST_PK BOOKING_MST_PK,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BMT.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BMT.BOOKING_REF_NO,");
				sb.Append("                                        AMT.AIRLINE_NAME VESSEL_NAME,");
				sb.Append("                                        JOB.VOYAGE_FLIGHT_NO VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        '' ALLOC20DV,");
				sb.Append("                                        '' ALLOC40DV,");
				sb.Append("                                        '' ALLOCTEU,");
				sb.Append("                                        BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                        BMT.VOLUME_IN_CBM VOLUME_IN_CBM,");
				sb.Append("                                        NVL(BFT.MIN_BASIS_RATE,0)*");
				sb.Append("                                        GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) INCOME,");
				sb.Append("                                        (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),");
				sb.Append("                                                    0)");
				sb.Append("                                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                                            AND CST.COST_ELEMENT_ID =");
				sb.Append("                                                FREIGHT_ELEMENT_ID) EXPENSES");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BMT,");
				sb.Append("                               BOOKING_TRN          BTT,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL    CUMT,");
				sb.Append("                               PORT_MST_TBL             POL,");
				sb.Append("                               PORT_MST_TBL             POD,");
				sb.Append("                               AIRLINE_MST_TBL          AMT,");
				sb.Append("                               JOB_CARD_TRN     JOB ");
				//sb.Append("                               ,COMMODITY_GROUP_MST_TBL  CGMT,")
				//sb.Append("                               COMMODITY_MST_TBL        CGT")
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND FMT.FREIGHT_ELEMENT_MST_PK(+) = BFT.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK(+) = BFT.CURRENCY_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BMT.CUSTOMER_MST_FK");
				// sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                           AND BTT.BOOKING_TRN_PK = BFT.BOOKING_TRN_FK(+)");
				sb.Append("                           AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                           AND BMT.BOOKING_MST_PK = BTT.BOOKING_MST_FK");
				//sb.Append("                           AND CGMT.COMMODITY_GROUP_PK(+) =")
				//sb.Append("                               BMT.COMMODITY_GROUP_FK")
				//sb.Append("                           AND CGT.COMMODITY_GROUP_FK =")
				//sb.Append("                               CGMT.COMMODITY_GROUP_PK")
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BMT.IS_EBOOKING = 0");
				sb.Append("                           AND BMT.STATUS IN (2, 6) ");
				sb.Append("                           AND BFT.MIN_BASIS_RATE > 0");
				sb.Append("                           AND BMT.BUSINESS_TYPE = 1 ");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND BTT.COMMODITY_MST_FK IN(" + CommPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BMT.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				//If CommPK.Trim.Length > 0 Then
				//    sb.Append(" AND CGT.COMMODITY_MST_PK IN(" & CommPK & ")")
				//End If

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BMT.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                        UNION");

				sb.Append("                        SELECT DISTINCT BMT.BOOKING_MST_PK,");
				sb.Append("                                        CST.COST_ELEMENT_ID   FREIGHT_ELEMENT_ID,");
				sb.Append("                                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BMT.BOOKING_REF_NO    BOOKING_REF_NO1,");
				sb.Append("                                        BMT.BOOKING_REF_NO,");
				sb.Append("                                        AMT.AIRLINE_NAME      VESSEL_NAME,");
				sb.Append("                                        JOB.VOYAGE_FLIGHT_NO         VOYAGE,");
				sb.Append("                                        POL.PORT_NAME         POL_ID,");
				sb.Append("                                        POD.PORT_NAME         POD_ID,");
				sb.Append("                                        ''                     ALLOC20DV,");
				sb.Append("                                        ''                     ALLOC40DV,");
				sb.Append("                                       ''                     ALLOCTEU,");
				sb.Append("                                        BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                        BMT.VOLUME_IN_CBM     VOLUME_IN_CBM,");
				sb.Append("                                        0                     INCOME,");
				sb.Append("                                        NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES ");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BMT,");
				sb.Append("                               BOOKING_TRN          BTT,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL       CUMT,");
				sb.Append("                               PORT_MST_TBL                POL,");
				sb.Append("                               PORT_MST_TBL                POD,");
				sb.Append("                               AIRLINE_MST_TBL             AMT,");
				sb.Append("                               JOB_CARD_TRN        JOB,");
				//sb.Append("                               COMMODITY_GROUP_MST_TBL     CGMT,")
				//sb.Append("                               COMMODITY_MST_TBL           CGT,")
				sb.Append("                               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                               QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                           AND Q.QUOTATION_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK(+) = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BMT.CUSTOMER_MST_FK");
				// sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                           AND BTT.BOOKING_TRN_PK = BFT.BOOKING_TRN_FK(+)");
				sb.Append("                           AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                           AND BMT.BOOKING_MST_PK = BTT.BOOKING_MST_FK");
				//sb.Append("                           AND CGMT.COMMODITY_GROUP_PK(+) =")
				//sb.Append("                               BMT.COMMODITY_GROUP_FK")
				//sb.Append("                           AND CGT.COMMODITY_GROUP_FK =")
				//sb.Append("                               CGMT.COMMODITY_GROUP_PK")
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BMT.IS_EBOOKING = 0");
				sb.Append("                           AND BMT.STATUS IN (2, 6) ");
				sb.Append("                           AND BMT.BUSINESS_TYPE = 1 ");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND BTT.COMMODITY_MST_FK IN(" + CommPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BMT.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				//If CommPK.Trim.Length > 0 Then
				//    sb.Append(" AND CGT.COMMODITY_MST_PK IN(" & CommPK & ")")
				//End If

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BMT.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                 )");

				sb.Append("                 GROUP BY BOOKING_MST_PK,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                          LOCATION_NAME,");
				sb.Append("                          EMPLOYEE_ID,");
				sb.Append("                          EMPLOYEE_NAME,");
				sb.Append("                          CUSTOMER_ID,");
				sb.Append("                          CUSTOMER_NAME,");
				sb.Append("                          BOOKING_REF_NO,");
				sb.Append("                          BOOKING_REF_NO1,");
				sb.Append("                          VESSEL_NAME,");
				sb.Append("                          VOYAGE,");
				sb.Append("                          POL_ID,");
				sb.Append("                          POD_ID");
				sb.Append("                         )");

				sb.Append("         GROUP BY BOOKING_MST_PK,");
				sb.Append("                  LOCATION_NAME,");
				sb.Append("                  EMPLOYEE_ID,");
				sb.Append("                  EMPLOYEE_NAME,");
				sb.Append("                  CUSTOMER_ID,");
				sb.Append("                  CUSTOMER_NAME,");
				sb.Append("                  BOOKING_REF_NO,");
				sb.Append("                  BOOKING_REF_NO1,");
				sb.Append("                  VESSEL_NAME,");
				sb.Append("                  VOYAGE,");
				sb.Append("                  POL_ID,");
				sb.Append("                  POD_ID) T");


				sb.Append(" WHERE  T.INCOME >0 ");
				if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.VOLUME_IN_CBM <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.VOLUME_IN_CBM >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.VOLUME_IN_CBM =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.NET_WEIGHT<" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.NET_WEIGHT >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.NET_WEIGHT=" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.INCOME <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.INCOME>" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.INCOME =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.EXPENSES <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.EXPENSES  >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.EXPENSES  =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.PROFIT_MARGIN <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.PROFIT_MARGIN >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.PROFIT_MARGIN  =" + txtSymbol);
				}

				DA = objWK.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "LOCATION");

				strBuilder.Append("-1,");
				if (MainDS.Tables[0].Rows.Count > 0) {
					for (Count = 0; Count <= MainDS.Tables[Count].Rows.Count - 1; Count++) {
						strBuilder.Append(Convert.ToString(MainDS.Tables[0].Rows[Count]["BOOKING_MST_PK"]).Trim() + ",");
					}
				}
				strBuilder.Remove(strBuilder.Length - 1, 1);
				BookingPKs = strBuilder.ToString();

				sb.Remove(0, sb.Length);
				sb.Append(" SELECT Q.* FROM(");
				sb.Append(" SELECT * FROM(");
				sb.Append("SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                FREIGHT_ELEMENT_ID,");
				sb.Append("                FREIGHT_ELEMENT_NAME,");
				//sb.Append("                CURRENCY_ID,")
				sb.Append(BaseCurrFk + " as CURRENCY_ID,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(SUM(INCOME * (NO_OF_BOXES)), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(SUM(INCOME * (VOLUME_IN_CBM)), 0)");
				sb.Append("                END INCOME,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(MAX(EXPENSES * (NO_OF_BOXES)), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(MAX(EXPENSES * (VOLUME_IN_CBM)), 0)");
				sb.Append("                END EXPENSES,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(SUM(INCOME * NO_OF_BOXES),0) - NVL(MAX(EXPENSES * NO_OF_BOXES),");
				sb.Append("                       0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(SUM(INCOME * VOLUME_IN_CBM),0) - ");
				sb.Append("                           NVL(MAX(EXPENSES * VOLUME_IN_CBM),");
				sb.Append("                       0)");
				sb.Append("                END PROFIT_MARGIN,");
				sb.Append("                ");
				sb.Append("                NVL(MAX(ALLOCTEU * (NO_OF_BOXES)), 0) ALLOCTEU,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                END NET_WEIGHT,");
				sb.Append("                ");
				sb.Append("                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM");
				sb.Append("  FROM (SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as CURRENCY_ID,");
				sb.Append("                        NVL(BTSFD.TARIFF_RATE,0)*");
				sb.Append("                        GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) INCOME,");
				sb.Append("                        (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)), 0) ");
				sb.Append("                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                            AND CST.COST_ELEMENT_ID = FREIGHT_ELEMENT_ID) EXPENSES,");
				sb.Append("                        NVL(TARIFF_RATE*GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) -");
				sb.Append("                            (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append("                             GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)), 0) ");
				sb.Append("                               FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                    COST_ELEMENT_MST_TBL        CST");
				sb.Append("                              WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                    Q.COST_ELEMENT_MST_FK");
				sb.Append("                                AND CST.COST_ELEMENT_ID = FREIGHT_ELEMENT_ID),");
				sb.Append("                            0) PROFIT_MARGIN,");
				sb.Append("                        BTSFL.NO_OF_BOXES,");
				sb.Append("                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                        BST.NET_WEIGHT,");
				sb.Append("                        BST.VOLUME_IN_CBM,");
				sb.Append("                        BST.GROSS_WEIGHT,");
				sb.Append("                        BST.CARGO_TYPE");
				sb.Append("          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("               CUSTOMER_MST_TBL         CMT,");
				sb.Append("               BOOKING_MST_TBL          BST,");
				sb.Append("               BOOKING_TRN  BTSFL,");
				sb.Append("               BOOKING_TRN_FRT_DTLS BTSFD,");
				sb.Append("               CONTAINER_TYPE_MST_TBL   CTMT,");
				sb.Append("               VESSEL_VOYAGE_TBL        VST,");
				sb.Append("               VESSEL_VOYAGE_TRN        VVT,");
				sb.Append("               PORT_MST_TBL             POL,");
				sb.Append("               PORT_MST_TBL             POD,");
				sb.Append("               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("               CURRENCY_TYPE_MST_TBL    CUMT ");
				//sb.Append("               ,COMMODITY_GROUP_MST_TBL  CGMT,")
				//sb.Append("               COMMODITY_MST_TBL        CGT")
				sb.Append("         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("           AND FMT.FREIGHT_ELEMENT_MST_PK = BTSFD.FREIGHT_ELEMENT_MST_FK");
				sb.Append("           AND CUMT.CURRENCY_MST_PK = BTSFD.CURRENCY_MST_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK(+) = BST.CUSTOMER_MST_FK");
				// sb.Append("           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("           AND BTSFD.BOOKING_TRN_FK = BTSFL.BOOKING_TRN_PK");
				sb.Append("           AND VST.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				//sb.Append("           AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK")
				//sb.Append("           AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("           AND CTMT.CONTAINER_TYPE_MST_PK(+) = BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
				sb.Append("           AND POL.LOCATION_MST_FK IN");
				sb.Append("               (SELECT L.LOCATION_MST_PK");
				sb.Append("                  FROM LOCATION_MST_TBL L");
				sb.Append("                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("           AND BST.IS_EBOOKING = 0");
				sb.Append("           AND BST.CARGO_TYPE <> 4");
				sb.Append("           AND BST.STATUS IN (2, 6) ");
				sb.Append("           AND BTSFD.TARIFF_RATE > 0");
				sb.Append("           AND BST.BUSINESS_TYPE = 2 ");
				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				sb.Append(" AND BST.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("        UNION");

				sb.Append("        SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                        CST.COST_ELEMENT_ID   FREIGHT_ELEMENT_ID,");
				sb.Append("                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as CURRENCY_ID,");
				sb.Append("                        0                     INCOME,");
				sb.Append("                        NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES,");
				sb.Append("                        (0 - NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)) PROFIT_MARGIN,");
				sb.Append("                        BTSFL.NO_OF_BOXES,");
				sb.Append("                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                        BST.NET_WEIGHT,");
				sb.Append("                        BST.VOLUME_IN_CBM,");
				sb.Append("                        BST.GROSS_WEIGHT,");
				sb.Append("                        BST.CARGO_TYPE");
				sb.Append("          FROM LOCATION_MST_TBL            LMT,");
				sb.Append("               EMPLOYEE_MST_TBL            EMT,");
				sb.Append("               CUSTOMER_MST_TBL            CMT,");
				sb.Append("               BOOKING_MST_TBL             BST,");
				sb.Append("               BOOKING_TRN     BTSFL,");
				sb.Append("               BOOKING_TRN_FRT_DTLS    BTSFD,");
				sb.Append("               CONTAINER_TYPE_MST_TBL      CTMT,");
				sb.Append("               VESSEL_VOYAGE_TBL           VST,");
				sb.Append("               VESSEL_VOYAGE_TRN           VVT,");
				sb.Append("               PORT_MST_TBL                POL,");
				sb.Append("               PORT_MST_TBL                POD,");
				sb.Append("               CURRENCY_TYPE_MST_TBL       CUMT,");
				//sb.Append("               COMMODITY_GROUP_MST_TBL     CGMT,")
				//sb.Append("               COMMODITY_MST_TBL           CGT,")
				sb.Append("               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("               QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("           AND Q.QUOTATION_FK = BST.BOOKING_MST_PK");
				sb.Append("           AND CUMT.CURRENCY_MST_PK = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK(+) = BST.CUSTOMER_MST_FK");
				// sb.Append("           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("           AND BTSFD.BOOKING_TRN_FK = BTSFL.BOOKING_TRN_PK");
				sb.Append("           AND VST.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				//sb.Append("           AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK")
				//sb.Append("           AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("           AND CTMT.CONTAINER_TYPE_MST_PK(+) = BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
				sb.Append("           AND POL.LOCATION_MST_FK IN");
				sb.Append("               (SELECT L.LOCATION_MST_PK");
				sb.Append("                  FROM LOCATION_MST_TBL L");
				sb.Append("                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("           AND BST.IS_EBOOKING = 0");
				sb.Append("           AND BST.CARGO_TYPE <> 4");
				sb.Append("           AND BST.STATUS IN (2, 6) ");
				sb.Append("           AND BST.BUSINESS_TYPE = 2 ");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				sb.Append(" AND BST.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("                   )");

				//sb.Append(" GROUP BY BOOKING_MST_PK, FREIGHT_ELEMENT_ID,FREIGHT_ELEMENT_NAME, CARGO_TYPE,CURRENCY_ID)T")
				sb.Append(" GROUP BY BOOKING_MST_PK, FREIGHT_ELEMENT_ID,FREIGHT_ELEMENT_NAME, CARGO_TYPE)T");
				sb.Append("  UNION ");

				sb.Append(" SELECT * ");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        FREIGHT_ELEMENT_ID,");
				sb.Append("                        FREIGHT_ELEMENT_NAME,");
				//sb.Append("                        CURRENCY_ID,")
				sb.Append(BaseCurrFk + " as        CURRENCY_ID,");
				sb.Append("                        NVL(SUM(INCOME * NET_WEIGHT), 0) INCOME,");
				sb.Append("                        NVL(MAX(EXPENSES * NET_WEIGHT), 0) EXPENSES,");
				sb.Append("                        NVL(SUM(INCOME * NET_WEIGHT),0) - ");
				sb.Append("                                NVL(MAX(EXPENSES * NET_WEIGHT),");
				sb.Append("                            0) PROFIT_MARGIN,");
				sb.Append("                        NVL(SUM(ALLOCTEU), '') ALLOCTEU,");
				sb.Append("                        NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM");
				sb.Append("          FROM (SELECT DISTINCT BMT.BOOKING_MST_PK BOOKING_MST_PK,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_MST_PK,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as                CURRENCY_ID,");
				sb.Append("                                '' ALLOCTEU,");
				sb.Append("                                BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                BMT.VOLUME_IN_CBM,");
				sb.Append("                                NVL(BFT.MIN_BASIS_RATE,0)*");
				sb.Append("                                GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) INCOME,");
				sb.Append("                                (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append("                                GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),");
				sb.Append("                                            0)");
				sb.Append("                                   FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                        COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                  WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                    AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                        Q.COST_ELEMENT_MST_FK");
				sb.Append("                                    AND CST.COST_ELEMENT_ID =");
				sb.Append("                                        FREIGHT_ELEMENT_ID) EXPENSES,");
				sb.Append("                                NVL(TARIFF_RATE*GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) -");
				sb.Append("                                    (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                    GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),");
				sb.Append("                                                0)");
				sb.Append("                                       FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                            COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                      WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                        AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                            Q.COST_ELEMENT_MST_FK");
				sb.Append("                                        AND CST.COST_ELEMENT_ID =");
				sb.Append("                                            FREIGHT_ELEMENT_ID),");
				sb.Append("                                    0) PROFIT_MARGIN");
				sb.Append("                  FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                       EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                       CUSTOMER_MST_TBL         CMT,");
				sb.Append("                       BOOKING_MST_TBL          BMT,");
				sb.Append("                       BOOKING_TRN          BTT,");
				sb.Append("                       BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                       FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                       CURRENCY_TYPE_MST_TBL    CUMT,");
				sb.Append("                       PORT_MST_TBL             POL,");
				sb.Append("                       PORT_MST_TBL             POD,");
				sb.Append("                       AIRLINE_MST_TBL          AMT,");
				sb.Append("                       JOB_CARD_TRN     JOB ");
				//sb.Append("                       ,COMMODITY_GROUP_MST_TBL  CGMT,")
				//sb.Append("                       COMMODITY_MST_TBL        CGT")
				sb.Append("                 WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                   AND FMT.FREIGHT_ELEMENT_MST_PK(+) = BFT.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                   AND CUMT.CURRENCY_MST_PK(+) = BFT.CURRENCY_MST_FK");
				sb.Append("                   AND CMT.CUSTOMER_MST_PK(+) = BMT.CUSTOMER_MST_FK");
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				// sb.Append("                   AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("                   AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                   AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                   AND BTT.BOOKING_TRN_PK = BFT.BOOKING_TRN_FK");
				sb.Append("                   AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                   AND BMT.BOOKING_MST_PK = BTT.BOOKING_MST_FK");
				//sb.Append("                   AND CGMT.COMMODITY_GROUP_PK(+) = BMT.COMMODITY_GROUP_FK")
				//sb.Append("                   AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("                   AND POL.LOCATION_MST_FK IN");
				sb.Append("                       (SELECT L.LOCATION_MST_PK");
				sb.Append("                          FROM LOCATION_MST_TBL L");
				sb.Append("                         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                   AND BMT.IS_EBOOKING = 0");
				sb.Append("                   AND BMT.STATUS IN (2, 6) ");
				sb.Append("                   AND BFT.MIN_BASIS_RATE > 0");
				sb.Append("                   AND BMT.BUSINESS_TYPE = 1 ");

				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				sb.Append(" AND BMT.BOOKING_MST_PK  IN(" + BookingPKs + ")");

				sb.Append("               UNION");

				sb.Append("                SELECT DISTINCT BMT.BOOKING_MST_PK      BOOKING_MST_PK,");
				sb.Append("                                CST.COST_ELEMENT_MST_PK FREIGHT_ELEMENT_MST_PK,");
				sb.Append("                                CST.COST_ELEMENT_ID     FREIGHT_ELEMENT_ID,");
				sb.Append("                                CST.COST_ELEMENT_NAME   FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as                CURRENCY_ID,");
				sb.Append("                                ''                       ALLOCTEU,");
				sb.Append("                                BMT.CHARGEABLE_WEIGHT   NET_WEIGHT,");
				sb.Append("                                BMT.VOLUME_IN_CBM,");
				sb.Append("                                0 INCOME,");
				sb.Append("                                NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES,");
				sb.Append("                                (0 - NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)) PROFIT_MARGIN ");
				sb.Append("                  FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                       EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                       CUSTOMER_MST_TBL         CMT,");
				sb.Append("                       BOOKING_MST_TBL          BMT,");
				sb.Append("                       BOOKING_TRN          BTT,");
				sb.Append("                       BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                       CURRENCY_TYPE_MST_TBL       CUMT,");
				sb.Append("                       PORT_MST_TBL                POL,");
				sb.Append("                       PORT_MST_TBL                POD,");
				sb.Append("                       AIRLINE_MST_TBL             AMT,");
				sb.Append("                       JOB_CARD_TRN        JOB,");
				//sb.Append("                       COMMODITY_GROUP_MST_TBL     CGMT,")
				//sb.Append("                       COMMODITY_MST_TBL           CGT,")
				sb.Append("                       COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                       QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("                 WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                   AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                   AND Q.QUOTATION_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND CUMT.CURRENCY_MST_PK(+) = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                   AND CMT.CUSTOMER_MST_PK(+) = BMT.CUSTOMER_MST_FK");
				//sb.Append("                   AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                   AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                   AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                   AND BTT.BOOKING_TRN_PK = BFT.BOOKING_TRN_FK(+)");
				sb.Append("                   AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                   AND BMT.BOOKING_MST_PK = BTT.BOOKING_MST_FK");
				//sb.Append("                   AND CGMT.COMMODITY_GROUP_PK(+) = BMT.COMMODITY_GROUP_FK")
				//sb.Append("                   AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("                   AND POL.LOCATION_MST_FK IN");
				sb.Append("                       (SELECT L.LOCATION_MST_PK");
				sb.Append("                          FROM LOCATION_MST_TBL L");
				sb.Append("                         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                   AND BMT.IS_EBOOKING = 0");
				sb.Append("                   AND BMT.STATUS IN (2, 6) ");
				sb.Append("                   AND BMT.BUSINESS_TYPE = 1 ");
				if (flag == 0) {
					sb.Append(" AND 1=2");
				}

				sb.Append(" AND BMT.BOOKING_MST_PK  IN(" + BookingPKs + ")");

				sb.Append("                   )");

				sb.Append(" GROUP BY BOOKING_MST_PK,");
				sb.Append("          FREIGHT_ELEMENT_ID,");
				//sb.Append("          FREIGHT_ELEMENT_NAME,")
				//sb.Append("          CURRENCY_ID)T")
				sb.Append("          FREIGHT_ELEMENT_NAME)T)Q ,freight_element_mst_tbl femt WHERE");
				sb.Append(" Q.FREIGHT_ELEMENT_ID=FEMT.FREIGHT_ELEMENT_ID ORDER BY FEMT.PREFERENCE");

				DA = objWK.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "FREIGHT");

				DataRelation rel = new DataRelation("LOCATION", new DataColumn[] { MainDS.Tables[0].Columns["BOOKING_MST_PK"] }, new DataColumn[] { MainDS.Tables[1].Columns["BOOKING_MST_PK"] });

				rel.Nested = true;
				MainDS.Relations.Add(rel);

				return MainDS;

			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Print Function"
		public DataSet FetchSeaReport(string ddBasis = "0", string ddSymbols = "0", string Commodityfk = "0", double txtSymbol = 0.0, string BkgPK = "", string CustPK = "", string CommPK = "", string ExecPK = "", Int32 LocFk = 0, string fromDate = "",
		string toDate = "")
		{

			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			string strSQL = null;
			int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
			try {
				sb.Append("SELECT *");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        LOCATION_NAME,");
				sb.Append("                        EMPLOYEE_ID,");
				sb.Append("                        EMPLOYEE_NAME,");
				sb.Append("                        CUSTOMER_ID,");
				sb.Append("                        CUSTOMER_NAME,");
				sb.Append("                        BOOKING_REF_NO1,");
				sb.Append("                        BOOKING_REF_NO,");
				sb.Append("                        VESSEL_NAME,");
				sb.Append("                        VOYAGE,");
				sb.Append("                        POL_ID,");
				sb.Append("                        POD_ID,");
				sb.Append("                        NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                        NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                          NVL(SUM(DISTINCT ALLOCTEU), 0) ALLOCTEU,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                        END NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(INCOME), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(INCOME), 0)");
				sb.Append("                        END INCOME,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(EXPENSES), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(EXPENSES), 0)");
				sb.Append("                        END EXPENSES,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(INCOME  -");
				sb.Append("                                   EXPENSES),");
				sb.Append("                               0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(INCOME  -");
				sb.Append("                                   EXPENSES ),");
				sb.Append("                               0)");
				sb.Append("                        END PROFIT_MARGIN");
				sb.Append("          FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                                CONTAINER_TYPE_MST_ID,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                                LOCATION_NAME,");
				sb.Append("                                EMPLOYEE_ID,");
				sb.Append("                                EMPLOYEE_NAME,");
				sb.Append("                                CUSTOMER_ID,");
				sb.Append("                                CUSTOMER_NAME,");
				sb.Append("                                BOOKING_REF_NO1,");
				sb.Append("                                BOOKING_REF_NO,");
				sb.Append("                                VESSEL_NAME,");
				sb.Append("                                VOYAGE,");
				sb.Append("                                POL_ID,");
				sb.Append("                                POD_ID,");
				sb.Append("                                NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                                NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                                 NVL(SUM(DISTINCT ALLOCTEU * (NO_OF_BOXES)), 0) ALLOCTEU,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                                END NET_WEIGHT,");
				sb.Append("                                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(SUM(INCOME * (NO_OF_BOXES)), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(SUM(INCOME * (VOLUME_IN_CBM)), 0)");
				sb.Append("                                END INCOME,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(MAX(EXPENSES * (NO_OF_BOXES)), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(MAX(EXPENSES * (VOLUME_IN_CBM)), 0)");
				sb.Append("                                END EXPENSES,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(MAX(INCOME * (NO_OF_BOXES) -");
				sb.Append("                                           EXPENSES * (NO_OF_BOXES)),");
				sb.Append("                                       0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(MAX(INCOME * (VOLUME_IN_CBM) -");
				sb.Append("                                           EXPENSES * (VOLUME_IN_CBM)),");
				sb.Append("                                       0)");
				sb.Append("                                END PROFIT_MARGIN,");
				sb.Append("                                NO_OF_BOXES,");
				sb.Append("                                CARGO_TYPE,");
				sb.Append("                                GROSS_WEIGHT");
				sb.Append("                  FROM (SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BST.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BST.BOOKING_REF_NO,");
				sb.Append("                                        VST.VESSEL_NAME,");
				sb.Append("                                        VVT.VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '20' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC20DV,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '40' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC40DV,");
				sb.Append("                                        CTMT.CONTAINER_KIND,");
				sb.Append("                                        CTMT.CONTAINER_TYPE_MST_ID,");
				sb.Append("                                        BTSFL.NO_OF_BOXES,");
				sb.Append("                                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                                        BST.NET_WEIGHT,");
				sb.Append("                                        BST.VOLUME_IN_CBM,");
				sb.Append("                                        NVL(BTSFD.TARIFF_RATE,0)*");
				sb.Append("                                        GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) INCOME,");
				sb.Append("                                        BST.GROSS_WEIGHT,");
				sb.Append("                                        BST.CARGO_TYPE,");
				sb.Append("                                        (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                         GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),0) ");
				sb.Append("                                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                                            AND CST.COST_ELEMENT_ID =");
				sb.Append("                                                FREIGHT_ELEMENT_ID) EXPENSES");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BST,");
				sb.Append("                               BOOKING_TRN  BTSFL,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BTSFD,");
				sb.Append("                               CONTAINER_TYPE_MST_TBL   CTMT,");
				sb.Append("                               VESSEL_VOYAGE_TBL        VST,");
				sb.Append("                               VESSEL_VOYAGE_TRN        VVT,");
				sb.Append("                               PORT_MST_TBL             POL,");
				sb.Append("                               PORT_MST_TBL             POD,");
				sb.Append("                               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL    CUMT");
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND FMT.FREIGHT_ELEMENT_MST_PK =");
				sb.Append("                               BTSFD.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK = BTSFD.CURRENCY_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BST.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("                           AND BTSFD.BOOKING_TRN_FK =");
				sb.Append("                               BTSFL.BOOKING_TRN_PK");
				sb.Append("                           AND VST.VESSEL_VOYAGE_TBL_PK(+) =");
				sb.Append("                               VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("                           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				sb.Append("                           AND CTMT.CONTAINER_TYPE_MST_PK(+) =");
				sb.Append("                               BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("                           AND BST.BOOKING_MST_PK(+) = BTSFL.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BST.IS_EBOOKING = 0");
				sb.Append("                           AND BST.CARGO_TYPE <> 4");
				sb.Append("                           AND BST.STATUS = 2");
				sb.Append("                           AND BTSFD.TARIFF_RATE > 0");


				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BST.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BST.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND CGT.COMMODITY_MST_PK IN(" + CommPK + ")");
				}

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BST.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                         UNION");

				sb.Append("                        SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                                        CST.COST_ELEMENT_ID FREIGHT_ELEMENT_ID,");
				sb.Append("                                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BST.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BST.BOOKING_REF_NO,");
				sb.Append("                                        VST.VESSEL_NAME,");
				sb.Append("                                        VVT.VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '20' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC20DV,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '40' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC40DV,");
				sb.Append("                                        CTMT.CONTAINER_KIND,");
				sb.Append("                                        CTMT.CONTAINER_TYPE_MST_ID,");
				sb.Append("                                        BTSFL.NO_OF_BOXES,");
				sb.Append("                                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                                        BST.NET_WEIGHT,");
				sb.Append("                                        BST.VOLUME_IN_CBM,");
				sb.Append("                                        0 INCOME,");
				sb.Append("                                        BST.GROSS_WEIGHT,");
				sb.Append("                                        BST.CARGO_TYPE,");
				sb.Append("                                        Q.PROFITABILITY_RATE EXPENSES");
				sb.Append("                          FROM LOCATION_MST_TBL            LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL            EMT,");
				sb.Append("                               CUSTOMER_MST_TBL            CMT,");
				sb.Append("                               BOOKING_MST_TBL             BST,");
				sb.Append("                               BOOKING_TRN     BTSFL,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS    BTSFD,");
				sb.Append("                               CONTAINER_TYPE_MST_TBL      CTMT,");
				sb.Append("                               VESSEL_VOYAGE_TBL           VST,");
				sb.Append("                               VESSEL_VOYAGE_TRN           VVT,");
				sb.Append("                               PORT_MST_TBL                POL,");
				sb.Append("                               PORT_MST_TBL                POD,");
				sb.Append("                               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                               QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL       CUMT");
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                           AND Q.QUOTATION_FK = BST.BOOKING_MST_PK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BST.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("                           AND BTSFD.BOOKING_TRN_FK =");
				sb.Append("                               BTSFL.BOOKING_TRN_PK");
				sb.Append("                           AND VST.VESSEL_VOYAGE_TBL_PK(+) =");
				sb.Append("                               VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("                           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				sb.Append("                           AND CTMT.CONTAINER_TYPE_MST_PK(+) =");
				sb.Append("                               BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("                           AND BST.BOOKING_MST_PK(+) = BTSFL.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = 1521)");
				sb.Append("                           AND BST.IS_EBOOKING = 0");
				sb.Append("                           AND BST.CARGO_TYPE <> 4");
				sb.Append("                           AND BST.STATUS = 2");


				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BST.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BST.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND CGT.COMMODITY_MST_PK IN(" + CommPK + ")");
				}

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BST.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                        )");
				sb.Append("                 GROUP BY BOOKING_MST_PK,");
				sb.Append("                                CONTAINER_TYPE_MST_ID,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                          LOCATION_NAME,");
				sb.Append("                          EMPLOYEE_ID,");
				sb.Append("                          EMPLOYEE_NAME,");
				sb.Append("                          CUSTOMER_ID,");
				sb.Append("                          CUSTOMER_NAME,");
				sb.Append("                          BOOKING_REF_NO,");
				sb.Append("                          BOOKING_REF_NO1,");
				sb.Append("                          VESSEL_NAME,");
				sb.Append("                          VOYAGE,");
				sb.Append("                          POL_ID,");
				sb.Append("                          CARGO_TYPE,");
				sb.Append("                          NO_OF_BOXES,");
				sb.Append("                          GROSS_WEIGHT,");
				sb.Append("                          POD_ID)");

				sb.Append(" GROUP BY BOOKING_MST_PK,");
				sb.Append("          LOCATION_NAME,");
				sb.Append("          EMPLOYEE_ID,");
				sb.Append("          EMPLOYEE_NAME,");
				sb.Append("          CUSTOMER_ID,");
				sb.Append("          CUSTOMER_NAME,");
				sb.Append("          BOOKING_REF_NO,");
				sb.Append("          BOOKING_REF_NO1,");
				sb.Append("          VESSEL_NAME,");
				sb.Append("          VOYAGE,");
				sb.Append("          POL_ID,");
				sb.Append("          CARGO_TYPE,");
				sb.Append("          POD_ID)T");


				sb.Append(" WHERE  T.INCOME > 0 ");
				if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.VOLUME_IN_CBM <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.VOLUME_IN_CBM >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.VOLUME_IN_CBM =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.NET_WEIGHT <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.NET_WEIGHT >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.NET_WEIGHT =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.ALLOCTEU <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.ALLOCTEU >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.ALLOCTEU =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.INCOME <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.INCOME>" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.INCOME =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.EXPENSES <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.EXPENSES  >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.EXPENSES  =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.PROFIT_MARGIN <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.PROFIT_MARGIN >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.PROFIT_MARGIN  =" + txtSymbol);
				}

				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException SqlExp) {
				throw SqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}

		public DataSet FetchSeaFreightDetail(string BookingPKs = "", Int32 LocFk = 0)
		{

			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			string strSQL = null;
			int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
			try {
				sb.Append(" SELECT Q.* FROM(");
				sb.Append(" SELECT * FROM(");
				sb.Append("SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                FREIGHT_ELEMENT_ID,");
				sb.Append("                FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + "  as    CURRENCY_ID,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(SUM(INCOME * (NO_OF_BOXES)), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(SUM(INCOME * (VOLUME_IN_CBM)), 0)");
				sb.Append("                END INCOME,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(MAX(EXPENSES * (NO_OF_BOXES)), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(MAX(EXPENSES * (VOLUME_IN_CBM)), 0)");
				sb.Append("                END EXPENSES,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(MAX(INCOME * (NO_OF_BOXES) - EXPENSES * (NO_OF_BOXES)),");
				sb.Append("                       0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(MAX(INCOME * (VOLUME_IN_CBM) -");
				sb.Append("                           EXPENSES * (VOLUME_IN_CBM)),");
				sb.Append("                       0)");
				sb.Append("                END PROFIT_MARGIN,");
				sb.Append("                NVL(MAX(ALLOCTEU * (NO_OF_BOXES)), 0) ALLOCTEU,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                END NET_WEIGHT,");
				sb.Append("                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM");
				sb.Append("  FROM (SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as        CURRENCY_ID,");
				sb.Append("                        NVL(BTSFD.TARIFF_RATE,0)*");
				sb.Append("                        GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) INCOME,");
				sb.Append("                         (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)), 0)");
				sb.Append("                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                            AND CST.COST_ELEMENT_ID = FREIGHT_ELEMENT_ID) EXPENSES,");
				sb.Append("                         NVL(BTSFD.TARIFF_RATE*GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) -");
				sb.Append("                            (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append("                             GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)), 0)");
				sb.Append("                               FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                    COST_ELEMENT_MST_TBL        CST");
				sb.Append("                              WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                    Q.COST_ELEMENT_MST_FK");
				sb.Append("                                AND CST.COST_ELEMENT_ID = FREIGHT_ELEMENT_ID),");
				sb.Append("                            0) PROFIT_MARGIN,");
				sb.Append("                        BTSFL.NO_OF_BOXES,");
				sb.Append("                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                        BST.NET_WEIGHT,");
				sb.Append("                        BST.VOLUME_IN_CBM,");
				sb.Append("                        BST.GROSS_WEIGHT,");
				sb.Append("                        BST.CARGO_TYPE");
				sb.Append("          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("               CUSTOMER_MST_TBL         CMT,");
				sb.Append("               BOOKING_MST_TBL          BST,");
				sb.Append("               BOOKING_TRN  BTSFL,");
				sb.Append("               BOOKING_TRN_FRT_DTLS BTSFD,");
				sb.Append("               CONTAINER_TYPE_MST_TBL   CTMT,");
				sb.Append("               VESSEL_VOYAGE_TBL        VST,");
				sb.Append("               VESSEL_VOYAGE_TRN        VVT,");
				sb.Append("               PORT_MST_TBL             POL,");
				sb.Append("               PORT_MST_TBL             POD,");
				sb.Append("               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("               CURRENCY_TYPE_MST_TBL    CUMT");
				//sb.Append("               COMMODITY_GROUP_MST_TBL  CGMT,")
				//sb.Append("               COMMODITY_MST_TBL        CGT")
				sb.Append("         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("           AND FMT.FREIGHT_ELEMENT_MST_PK = BTSFD.FREIGHT_ELEMENT_MST_FK");
				sb.Append("           AND CUMT.CURRENCY_MST_PK = BTSFD.CURRENCY_MST_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK(+) = BST.CUSTOMER_MST_FK");
				//sb.Append("           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("           AND BTSFD.BOOKING_TRN_FK = BTSFL.BOOKING_TRN_PK");
				sb.Append("           AND VST.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				//sb.Append("           AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK")
				//sb.Append("           AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("           AND CTMT.CONTAINER_TYPE_MST_PK(+) = BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("           AND BST.BOOKING_MST_PK(+) = BTSFL.BOOKING_MST_FK");
				sb.Append("           AND POL.LOCATION_MST_FK IN");
				sb.Append("               (SELECT L.LOCATION_MST_PK");
				sb.Append("                  FROM LOCATION_MST_TBL L");
				sb.Append("                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("           AND BST.IS_EBOOKING = 0");
				sb.Append("           AND BST.CARGO_TYPE <> 4");
				sb.Append("           AND BST.STATUS = 2");
				sb.Append("           AND BTSFD.TARIFF_RATE > 0");

				sb.Append(" AND BST.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("        UNION");

				sb.Append("        SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                        CST.COST_ELEMENT_ID   FREIGHT_ELEMENT_ID,");
				sb.Append("                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as        CURRENCY_ID,");
				sb.Append("                        0                     INCOME,");
				sb.Append("                         NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES,");
				//sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK,BTSFD.CURRENCY_MST_FK,Q.CREATED_DT) EXPENSES, ")
				sb.Append("                        (0 - NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)) PROFIT_MARGIN,");
				//sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK,BTSFD.CURRENCY_MST_FK,Q.CREATED_DT)) PROFIT_MARGIN, ")
				sb.Append("                        BTSFL.NO_OF_BOXES,");
				sb.Append("                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                        BST.NET_WEIGHT,");
				sb.Append("                        BST.VOLUME_IN_CBM,");
				sb.Append("                        BST.GROSS_WEIGHT,");
				sb.Append("                        BST.CARGO_TYPE");
				sb.Append("          FROM LOCATION_MST_TBL            LMT,");
				sb.Append("               EMPLOYEE_MST_TBL            EMT,");
				sb.Append("               CUSTOMER_MST_TBL            CMT,");
				sb.Append("               BOOKING_MST_TBL             BST,");
				sb.Append("               BOOKING_TRN     BTSFL,");
				sb.Append("               BOOKING_TRN_FRT_DTLS    BTSFD,");
				sb.Append("               CONTAINER_TYPE_MST_TBL      CTMT,");
				sb.Append("               VESSEL_VOYAGE_TBL           VST,");
				sb.Append("               VESSEL_VOYAGE_TRN           VVT,");
				sb.Append("               PORT_MST_TBL                POL,");
				sb.Append("               PORT_MST_TBL                POD,");
				sb.Append("               CURRENCY_TYPE_MST_TBL       CUMT,");
				//sb.Append("               COMMODITY_GROUP_MST_TBL     CGMT,")
				//sb.Append("               COMMODITY_MST_TBL           CGT,")
				sb.Append("               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("               QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("           AND Q.QUOTATION_FK = BST.BOOKING_MST_PK");
				sb.Append("           AND CUMT.CURRENCY_MST_PK = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK(+) = BST.CUSTOMER_MST_FK");
				//sb.Append("           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("           AND BTSFD.BOOKING_TRN_FK = BTSFL.BOOKING_TRN_PK");
				sb.Append("           AND VST.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				//sb.Append("           AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK")
				//sb.Append("           AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("           AND CTMT.CONTAINER_TYPE_MST_PK(+) = BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("           AND BST.BOOKING_MST_PK(+) = BTSFL.BOOKING_MST_FK");
				sb.Append("           AND POL.LOCATION_MST_FK IN");
				sb.Append("               (SELECT L.LOCATION_MST_PK");
				sb.Append("                  FROM LOCATION_MST_TBL L");
				sb.Append("                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("           AND BST.IS_EBOOKING = 0");
				sb.Append("           AND BST.CARGO_TYPE <> 4");
				sb.Append("           AND BST.STATUS = 2");

				sb.Append(" AND BST.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("                   )");

				sb.Append(" GROUP BY BOOKING_MST_PK, FREIGHT_ELEMENT_ID,FREIGHT_ELEMENT_NAME, CARGO_TYPE)T)Q ,freight_element_mst_tbl femt WHERE");
				sb.Append(" q.freight_element_id=femt.freight_element_id ORDER BY femt.preference");



				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException SqlExp) {
				throw SqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}

		public DataSet FetchAirReport(string ddBasis = "0", string ddSymbols = "0", string Commodityfk = "0", double txtSymbol = 0.0, string BkgPK = "", string CustPK = "", string CommPK = "", string ExecPK = "", Int32 LocFk = 0, string fromDate = "",
		string toDate = "")
		{

			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
			string strSQL = null;
			try {
				sb.Append("SELECT *");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        LOCATION_NAME,");
				sb.Append("                        EMPLOYEE_ID,");
				sb.Append("                        EMPLOYEE_NAME,");
				sb.Append("                        CUSTOMER_ID,");
				sb.Append("                        CUSTOMER_NAME,");
				sb.Append("                        BOOKING_REF_NO1,");
				sb.Append("                        BOOKING_REF_NO,");
				sb.Append("                        VESSEL_NAME,");
				sb.Append("                        VOYAGE,");
				sb.Append("                        POL_ID,");
				sb.Append("                        POD_ID,");
				sb.Append("                        NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                        NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                        NVL(MAX(ALLOCTEU), 0) ALLOCTEU,");
				sb.Append("                        NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                        NVL(SUM(INCOME), 0) INCOME,");
				sb.Append("                        NVL(SUM(EXPENSES), 0) EXPENSES,");
				sb.Append("                        NVL(SUM(INCOME - EXPENSES), 0) PROFIT_MARGIN");
				sb.Append("          FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                                 '' CONTAINER_TYPE_MST_ID,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                                LOCATION_NAME,");
				sb.Append("                                EMPLOYEE_ID,");
				sb.Append("                                EMPLOYEE_NAME,");
				sb.Append("                                CUSTOMER_ID,");
				sb.Append("                                CUSTOMER_NAME,");
				sb.Append("                                BOOKING_REF_NO1,");
				sb.Append("                                BOOKING_REF_NO,");
				sb.Append("                                VESSEL_NAME,");
				sb.Append("                                VOYAGE,");
				sb.Append("                                POL_ID,");
				sb.Append("                                POD_ID,");
				sb.Append("                                NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                                NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                                NVL(MAX(ALLOCTEU), 0) ALLOCTEU,");
				sb.Append("                                NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                                NVL(SUM(INCOME * NET_WEIGHT), 0) INCOME,");
				sb.Append("                                NVL(MAX(EXPENSES * NET_WEIGHT), 0) EXPENSES,");
				sb.Append("                                NVL(MAX((INCOME * NET_WEIGHT) -");
				sb.Append("                                        (EXPENSES * NET_WEIGHT)),");
				sb.Append("                                    0)");
				sb.Append("                  FROM (SELECT DISTINCT BMT.BOOKING_MST_PK BOOKING_MST_PK,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BMT.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BMT.BOOKING_REF_NO,");
				sb.Append("                                        AMT.AIRLINE_NAME VESSEL_NAME,");
				sb.Append("                                        JOB.VOYAGE_FLIGHT_NO VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        0 ALLOC20DV,");
				sb.Append("                                        0 ALLOC40DV,");
				sb.Append("                                        0 ALLOCTEU,");
				sb.Append("                                        BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                        BMT.VOLUME_IN_CBM VOLUME_IN_CBM,");
				sb.Append("                                        NVL(BFT.MIN_BASIS_RATE,0)*");
				sb.Append(" GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) INCOME,");
				sb.Append("                                        (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append(" GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),0) ");
				sb.Append("                                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                                            AND CST.COST_ELEMENT_ID =");
				sb.Append("                                                FREIGHT_ELEMENT_ID) EXPENSES");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BMT,");
				sb.Append("                               BOOKING_TRN          BTT,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL    CUMT,");
				sb.Append("                               PORT_MST_TBL             POL,");
				sb.Append("                               PORT_MST_TBL             POD,");
				sb.Append("                               AIRLINE_MST_TBL          AMT,");
				sb.Append("                               JOB_CARD_TRN     JOB");
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND FMT.FREIGHT_ELEMENT_MST_PK(+) =");
				sb.Append("                               BFT.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK(+) = BFT.CURRENCY_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BMT.CUSTOMER_MST_FK");

				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                           AND BTT.BOOKING_TRN_PK(+) =");
				sb.Append("                               BFT.BOOKING_TRN_FK");
				sb.Append("                           AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                           AND BMT.BOOKING_MST_PK(+) = BTT.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BMT.IS_EBOOKING = 0");
				sb.Append("                           AND BMT.STATUS = 2");
				sb.Append("                           AND BFT.MIN_BASIS_RATE > 0");

				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BMT.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND CGT.COMMODITY_MST_PK IN(" + CommPK + ")");
				}

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BMT.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                        UNION");

				sb.Append("                        SELECT DISTINCT BMT.BOOKING_MST_PK,");
				sb.Append("                                        CST.COST_ELEMENT_ID   FREIGHT_ELEMENT_ID,");
				sb.Append("                                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BMT.BOOKING_REF_NO    BOOKING_REF_NO1,");
				sb.Append("                                        BMT.BOOKING_REF_NO,");
				sb.Append("                                        AMT.AIRLINE_NAME      VESSEL_NAME,");
				sb.Append("                                        JOB.VOYAGE_FLIGHT_NO         VOYAGE,");
				sb.Append("                                        POL.PORT_NAME         POL_ID,");
				sb.Append("                                        POD.PORT_NAME         POD_ID,");
				sb.Append("                                        0                     ALLOC20DV,");
				sb.Append("                                        0                     ALLOC40DV,");
				sb.Append("                                        0                     ALLOCTEU,");
				sb.Append("                                        BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                        BMT.VOLUME_IN_CBM     VOLUME_IN_CBM,");
				sb.Append("                                        0                     INCOME,");
				sb.Append("                                        NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append(" GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES ");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BMT,");
				sb.Append("                               BOOKING_TRN          BTT,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL       CUMT,");
				sb.Append("                               PORT_MST_TBL                POL,");
				sb.Append("                               PORT_MST_TBL                POD,");
				sb.Append("                               AIRLINE_MST_TBL             AMT,");
				sb.Append("                               JOB_CARD_TRN        JOB,");
				sb.Append("                               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                               QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                           AND Q.QUOTATION_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK(+) = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BMT.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")

				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                           AND BTT.BOOKING_TRN_PK(+) =");
				sb.Append("                               BFT.BOOKING_TRN_FK");
				sb.Append("                           AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                           AND BMT.BOOKING_MST_PK(+) = BTT.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BMT.IS_EBOOKING = 0");
				sb.Append("                           AND BMT.STATUS = 2");


				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BMT.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND CGT.COMMODITY_MST_PK IN(" + CommPK + ")");
				}

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BMT.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                 )");
				sb.Append("                 GROUP BY BOOKING_MST_PK,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                          LOCATION_NAME,");
				sb.Append("                          EMPLOYEE_ID,");
				sb.Append("                          EMPLOYEE_NAME,");
				sb.Append("                          CUSTOMER_ID,");
				sb.Append("                          CUSTOMER_NAME,");
				sb.Append("                          BOOKING_REF_NO,");
				sb.Append("                          BOOKING_REF_NO1,");
				sb.Append("                          VESSEL_NAME,");
				sb.Append("                          VOYAGE,");
				sb.Append("                          POL_ID,");
				sb.Append("                          POD_ID");
				sb.Append("                         )");

				sb.Append("         GROUP BY BOOKING_MST_PK,");
				sb.Append("                  LOCATION_NAME,");
				sb.Append("                  EMPLOYEE_ID,");
				sb.Append("                  EMPLOYEE_NAME,");
				sb.Append("                  CUSTOMER_ID,");
				sb.Append("                  CUSTOMER_NAME,");
				sb.Append("                  BOOKING_REF_NO,");
				sb.Append("                  BOOKING_REF_NO1,");
				sb.Append("                  VESSEL_NAME,");
				sb.Append("                  VOYAGE,");
				sb.Append("                  POL_ID,");
				sb.Append("                  POD_ID) T");


				sb.Append(" WHERE  T.INCOME >0 ");
				if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.VOLUME_IN_CBM <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.VOLUME_IN_CBM >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.VOLUME_IN_CBM =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.NET_WEIGHT<" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.NET_WEIGHT >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.NET_WEIGHT=" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.INCOME <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.INCOME>" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.INCOME =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.EXPENSES <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.EXPENSES  >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.EXPENSES  =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.PROFIT_MARGIN <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.PROFIT_MARGIN >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.PROFIT_MARGIN  =" + txtSymbol);
				}
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException SqlExp) {
				throw SqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}

		public DataSet FetchAirFreightDetail(string BookingPKs = "", Int32 LocFk = 0)
		{

			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
			string strSQL = null;
			try {
				sb.Append("SELECT Q.* FROM (");
				sb.Append("SELECT * ");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        FREIGHT_ELEMENT_ID,");
				sb.Append("                        FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + "   as      CURRENCY_ID,");
				sb.Append("                        NVL(SUM(INCOME * NET_WEIGHT), 0) INCOME,");
				sb.Append("                        NVL(MAX(EXPENSES * NET_WEIGHT), 0) EXPENSES,");
				sb.Append("                        NVL(MAX((INCOME * NET_WEIGHT) -");
				sb.Append("                                (EXPENSES * NET_WEIGHT)),");
				sb.Append("                            0) PROFIT_MARGIN,");
				sb.Append("                        NVL(SUM(ALLOCTEU), 0) ALLOCTEU,");
				sb.Append("                        NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM");
				sb.Append("          FROM (SELECT DISTINCT BMT.BOOKING_MST_PK BOOKING_MST_PK,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_MST_PK,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as                CURRENCY_ID,");
				sb.Append("                                0 ALLOCTEU,");
				sb.Append("                                BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                BMT.VOLUME_IN_CBM,");
				sb.Append("                                NVL(BFT.MIN_BASIS_RATE,0)*");
				sb.Append("                                GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) INCOME,");
				sb.Append("                                (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append("                                 GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),");
				sb.Append("                                            0)");
				sb.Append("                                   FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                        COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                  WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                    AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                        Q.COST_ELEMENT_MST_FK");
				sb.Append("                                    AND CST.COST_ELEMENT_ID =");
				sb.Append("                                        FREIGHT_ELEMENT_ID) EXPENSES,");
				sb.Append("                                NVL(TARIFF_RATE*GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) -");
				sb.Append("                                    (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                    GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),");
				sb.Append("                                                0)");
				sb.Append("                                       FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                            COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                      WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                        AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                            Q.COST_ELEMENT_MST_FK");
				sb.Append("                                        AND CST.COST_ELEMENT_ID =");
				sb.Append("                                            FREIGHT_ELEMENT_ID),");
				sb.Append("                                    0) PROFIT_MARGIN");
				sb.Append("                  FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                       EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                       CUSTOMER_MST_TBL         CMT,");
				sb.Append("                       BOOKING_MST_TBL          BMT,");
				sb.Append("                       BOOKING_TRN          BTT,");
				sb.Append("                       BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                       FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                       CURRENCY_TYPE_MST_TBL    CUMT,");
				sb.Append("                       PORT_MST_TBL             POL,");
				sb.Append("                       PORT_MST_TBL             POD,");
				sb.Append("                       AIRLINE_MST_TBL          AMT,");
				sb.Append("                       JOB_CARD_TRN     JOB");
				sb.Append("                 WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                   AND FMT.FREIGHT_ELEMENT_MST_PK(+) =");
				sb.Append("                       BFT.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                   AND CUMT.CURRENCY_MST_PK(+) = BFT.CURRENCY_MST_FK");
				sb.Append("                   AND CMT.CUSTOMER_MST_PK(+) = BMT.CUSTOMER_MST_FK");
				//sb.Append("                   AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                   AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                   AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                   AND BTT.BOOKING_TRN_PK(+) = BFT.BOOKING_TRN_FK");
				sb.Append("                   AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                   AND BMT.BOOKING_MST_PK(+) = BTT.BOOKING_MST_FK");
				sb.Append("                   AND POL.LOCATION_MST_FK IN");
				sb.Append("                       (SELECT L.LOCATION_MST_PK");
				sb.Append("                          FROM LOCATION_MST_TBL L");
				sb.Append("                         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                   AND BMT.IS_EBOOKING = 0");
				sb.Append("                   AND BMT.STATUS = 2");
				sb.Append("                   AND BFT.MIN_BASIS_RATE > 0");

				sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("               UNION");

				sb.Append("                SELECT DISTINCT BMT.BOOKING_MST_PK      BOOKING_MST_PK,");
				sb.Append("                                CST.COST_ELEMENT_MST_PK FREIGHT_ELEMENT_MST_PK,");
				sb.Append("                                CST.COST_ELEMENT_ID     FREIGHT_ELEMENT_ID,");
				sb.Append("                                CST.COST_ELEMENT_NAME   FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + "  as               CURRENCY_ID,");
				sb.Append("                                0                       ALLOCTEU,");
				sb.Append("                                BMT.CHARGEABLE_WEIGHT   NET_WEIGHT,");
				sb.Append("                                BMT.VOLUME_IN_CBM,");
				sb.Append("                                0 INCOME,");
				sb.Append("                                NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES,");
				sb.Append("                                (0 - NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)) PROFIT_MARGIN ");
				sb.Append("                  FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                       EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                       CUSTOMER_MST_TBL         CMT,");
				sb.Append("                       BOOKING_MST_TBL          BMT,");
				sb.Append("                       BOOKING_TRN          BTT,");
				sb.Append("                       BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                       CURRENCY_TYPE_MST_TBL       CUMT,");
				sb.Append("                       PORT_MST_TBL                POL,");
				sb.Append("                       PORT_MST_TBL                POD,");
				sb.Append("                       AIRLINE_MST_TBL             AMT,");
				sb.Append("                       JOB_CARD_TRN        JOB,");
				sb.Append("                       COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                       QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("                 WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                   AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                   AND Q.QUOTATION_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND CUMT.CURRENCY_MST_PK(+) = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                   AND CMT.CUSTOMER_MST_PK(+) = BMT.CUSTOMER_MST_FK");
				//sb.Append("                   AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                   AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                   AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                   AND BTT.BOOKING_TRN_PK(+) = BFT.BOOKING_TRN_FK");
				sb.Append("                   AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                   AND BMT.BOOKING_MST_PK(+) = BTT.BOOKING_MST_FK");
				sb.Append("                   AND POL.LOCATION_MST_FK IN");
				sb.Append("                       (SELECT L.LOCATION_MST_PK");
				sb.Append("                          FROM LOCATION_MST_TBL L");
				sb.Append("                         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                   AND BMT.IS_EBOOKING = 0");
				sb.Append("                   AND BMT.STATUS = 2");

				sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("                   )");

				sb.Append(" GROUP BY BOOKING_MST_PK,");
				sb.Append("          FREIGHT_ELEMENT_ID,");
				sb.Append("          FREIGHT_ELEMENT_NAME)T)Q ,freight_element_mst_tbl femt WHERE");
				sb.Append(" q.freight_element_id=femt.freight_element_id ORDER BY femt.preference");


				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException SqlExp) {
				throw SqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}

		public DataSet FetchBothReport(string ddBasis = "0", string ddSymbols = "0", string Commodityfk = "0", double txtSymbol = 0.0, string BkgPK = "", string CustPK = "", string CommPK = "", string ExecPK = "", Int32 LocFk = 0, string fromDate = "",
		string toDate = "")
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
			string strSQL = null;
			try {
				sb.Append("SELECT *");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        LOCATION_NAME,");
				sb.Append("                        EMPLOYEE_ID,");
				sb.Append("                        EMPLOYEE_NAME,");
				sb.Append("                        CUSTOMER_ID,");
				sb.Append("                        CUSTOMER_NAME,");
				sb.Append("                        BOOKING_REF_NO1,");
				sb.Append("                        BOOKING_REF_NO,");
				sb.Append("                        VESSEL_NAME,");
				sb.Append("                        VOYAGE,");
				sb.Append("                        POL_ID,");
				sb.Append("                        POD_ID,");
				sb.Append("                        NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                        NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                        NVL(SUM(DISTINCT ALLOCTEU), 0) ALLOCTEU,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                        END NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(INCOME ), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(INCOME ), 0)");
				sb.Append("                        END INCOME,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(EXPENSES ), 0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(EXPENSES ), 0)");
				sb.Append("                        END EXPENSES,");
				sb.Append("                        CASE");
				sb.Append("                          WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                           NVL(SUM(INCOME  -");
				sb.Append("                                   EXPENSES ),");
				sb.Append("                               0)");
				sb.Append("                          ELSE");
				sb.Append("                           NVL(SUM(INCOME  -");
				sb.Append("                                   EXPENSES ),");
				sb.Append("                               0)");
				sb.Append("                        END PROFIT_MARGIN");
				sb.Append("          FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                                CONTAINER_TYPE_MST_ID,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                                LOCATION_NAME,");
				sb.Append("                                EMPLOYEE_ID,");
				sb.Append("                                EMPLOYEE_NAME,");
				sb.Append("                                CUSTOMER_ID,");
				sb.Append("                                CUSTOMER_NAME,");
				sb.Append("                                BOOKING_REF_NO1,");
				sb.Append("                                BOOKING_REF_NO,");
				sb.Append("                                VESSEL_NAME,");
				sb.Append("                                VOYAGE,");
				sb.Append("                                POL_ID,");
				sb.Append("                                POD_ID,");
				sb.Append("                                NVL(MAX(ALLOC20DV), 0) ALLOC20DV,");
				sb.Append("                                NVL(MAX(ALLOC40DV), 0) ALLOC40DV,");
				sb.Append("                                NVL(SUM(DISTINCT ALLOCTEU * (NO_OF_BOXES)), 0) ALLOCTEU,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                                END NET_WEIGHT,");
				sb.Append("                                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(SUM(INCOME * (NO_OF_BOXES)), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(SUM(INCOME * (VOLUME_IN_CBM)), 0)");
				sb.Append("                                END INCOME,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(MAX(EXPENSES * (NO_OF_BOXES)), 0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(MAX(EXPENSES * (VOLUME_IN_CBM)), 0)");
				sb.Append("                                END EXPENSES,");
				sb.Append("                                CASE");
				sb.Append("                                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                                   NVL(MAX(INCOME * (NO_OF_BOXES) -");
				sb.Append("                                           EXPENSES * (NO_OF_BOXES)),");
				sb.Append("                                       0)");
				sb.Append("                                  ELSE");
				sb.Append("                                   NVL(MAX(INCOME * (VOLUME_IN_CBM) -");
				sb.Append("                                           EXPENSES * (VOLUME_IN_CBM)),");
				sb.Append("                                       0)");
				sb.Append("                                END PROFIT_MARGIN,");
				sb.Append("                                NO_OF_BOXES,");
				sb.Append("                                CARGO_TYPE,");
				sb.Append("                                GROSS_WEIGHT");
				sb.Append("                  FROM (SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BST.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BST.BOOKING_REF_NO,");
				sb.Append("                                        VST.VESSEL_NAME,");
				sb.Append("                                        VVT.VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '20' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC20DV,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '40' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC40DV,");
				sb.Append("                                        CTMT.CONTAINER_KIND,");
				sb.Append("                                        CTMT.CONTAINER_TYPE_MST_ID,");
				sb.Append("                                        BTSFL.NO_OF_BOXES,");
				sb.Append("                                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                                        BST.NET_WEIGHT,");
				sb.Append("                                        BST.VOLUME_IN_CBM,");
				sb.Append("                                        NVL(BTSFD.TARIFF_RATE,0)*");
				sb.Append(" GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) INCOME,");
				sb.Append("                                        BST.GROSS_WEIGHT,");
				sb.Append("                                        BST.CARGO_TYPE,");
				sb.Append("                                        (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append(" GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),0) ");
				sb.Append("                                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                                            AND CST.COST_ELEMENT_ID =");
				sb.Append("                                                FREIGHT_ELEMENT_ID) EXPENSES");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BST,");
				sb.Append("                               BOOKING_TRN  BTSFL,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BTSFD,");
				sb.Append("                               CONTAINER_TYPE_MST_TBL   CTMT,");
				sb.Append("                               VESSEL_VOYAGE_TBL        VST,");
				sb.Append("                               VESSEL_VOYAGE_TRN        VVT,");
				sb.Append("                               PORT_MST_TBL             POL,");
				sb.Append("                               PORT_MST_TBL             POD,");
				sb.Append("                               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL    CUMT");
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND FMT.FREIGHT_ELEMENT_MST_PK =");
				sb.Append("                               BTSFD.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK = BTSFD.CURRENCY_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BST.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")

				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("                           AND BTSFD.BOOKING_TRN_FK =");
				sb.Append("                               BTSFL.BOOKING_TRN_PK");
				sb.Append("                           AND VST.VESSEL_VOYAGE_TBL_PK(+) =");
				sb.Append("                               VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("                           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				sb.Append("                           AND CTMT.CONTAINER_TYPE_MST_PK(+) =");
				sb.Append("                               BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("                           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BST.IS_EBOOKING = 0");
				sb.Append("                           AND BST.CARGO_TYPE <> 4");
				sb.Append("                          AND BST.STATUS IN (2, 6)");
				sb.Append("                           AND BTSFD.TARIFF_RATE > 0 AND BST.BUSINESS_TYPE = 2");


				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BST.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BST.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND CGT.COMMODITY_MST_PK IN(" + CommPK + ")");
				}

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BST.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                         UNION ");

				sb.Append("                        SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                                        CST.COST_ELEMENT_ID FREIGHT_ELEMENT_ID,");
				sb.Append("                                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BST.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BST.BOOKING_REF_NO,");
				sb.Append("                                        VST.VESSEL_NAME,");
				sb.Append("                                        VVT.VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '20' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC20DV,");
				sb.Append("                                        CASE");
				sb.Append("                                          WHEN CONTAINER_KIND = 1 AND");
				sb.Append("                                               SUBSTR(CONTAINER_TYPE_MST_ID,");
				sb.Append("                                                      0,");
				sb.Append("                                                      2) = '40' THEN");
				sb.Append("                                           NVL(BTSFL.NO_OF_BOXES, 0)");
				sb.Append("                                          ELSE");
				sb.Append("                                           0");
				sb.Append("                                        END ALLOC40DV,");
				sb.Append("                                        CTMT.CONTAINER_KIND,");
				sb.Append("                                        CTMT.CONTAINER_TYPE_MST_ID,");
				sb.Append("                                        BTSFL.NO_OF_BOXES,");
				sb.Append("                                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                                        BST.NET_WEIGHT,");
				sb.Append("                                        BST.VOLUME_IN_CBM,");
				sb.Append("                                        0 INCOME,");
				sb.Append("                                        BST.GROSS_WEIGHT,");
				sb.Append("                                        BST.CARGO_TYPE,");
				sb.Append("                                        NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append(" GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES ");
				sb.Append("                          FROM LOCATION_MST_TBL            LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL            EMT,");
				sb.Append("                               CUSTOMER_MST_TBL            CMT,");
				sb.Append("                               BOOKING_MST_TBL             BST,");
				sb.Append("                               BOOKING_TRN     BTSFL,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS    BTSFD,");
				sb.Append("                               CONTAINER_TYPE_MST_TBL      CTMT,");
				sb.Append("                               VESSEL_VOYAGE_TBL           VST,");
				sb.Append("                               VESSEL_VOYAGE_TRN           VVT,");
				sb.Append("                               PORT_MST_TBL                POL,");
				sb.Append("                               PORT_MST_TBL                POD,");
				sb.Append("                               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                               QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL       CUMT");
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                           AND Q.QUOTATION_FK = BST.BOOKING_MST_PK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BST.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("                           AND BTSFD.BOOKING_TRN_FK =");
				sb.Append("                               BTSFL.BOOKING_TRN_PK");
				sb.Append("                           AND VST.VESSEL_VOYAGE_TBL_PK(+) =");
				sb.Append("                               VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("                           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				sb.Append("                           AND CTMT.CONTAINER_TYPE_MST_PK(+) =");
				sb.Append("                               BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("                           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = 1521)");
				sb.Append("                           AND BST.IS_EBOOKING = 0");
				sb.Append("                           AND BST.CARGO_TYPE <> 4");
				sb.Append("                          AND BST.STATUS IN (2, 6)");
				sb.Append("                           AND BTSFD.TARIFF_RATE > 0 AND BST.BUSINESS_TYPE = 2");

				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BST.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BST.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND CGT.COMMODITY_MST_PK IN(" + CommPK + ")");
				}

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BST.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BST.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                        )");
				sb.Append("                 GROUP BY BOOKING_MST_PK,");
				sb.Append("                                CONTAINER_TYPE_MST_ID,");
				sb.Append("                               FREIGHT_ELEMENT_ID,");
				sb.Append("                          LOCATION_NAME,");
				sb.Append("                          EMPLOYEE_ID,");
				sb.Append("                          EMPLOYEE_NAME,");
				sb.Append("                          CUSTOMER_ID,");
				sb.Append("                          CUSTOMER_NAME,");
				sb.Append("                          BOOKING_REF_NO,");
				sb.Append("                          BOOKING_REF_NO1,");
				sb.Append("                          VESSEL_NAME,");
				sb.Append("                          VOYAGE,");
				sb.Append("                          POL_ID,");
				sb.Append("                          CARGO_TYPE,");
				sb.Append("                          NO_OF_BOXES,");
				sb.Append("                          GROSS_WEIGHT,");
				sb.Append("                          POD_ID)");

				sb.Append(" GROUP BY BOOKING_MST_PK,");
				sb.Append("          LOCATION_NAME,");
				sb.Append("          EMPLOYEE_ID,");
				sb.Append("          EMPLOYEE_NAME,");
				sb.Append("          CUSTOMER_ID,");
				sb.Append("          CUSTOMER_NAME,");
				sb.Append("          BOOKING_REF_NO,");
				sb.Append("          BOOKING_REF_NO1,");
				sb.Append("          VESSEL_NAME,");
				sb.Append("          VOYAGE,");
				sb.Append("          POL_ID,");
				sb.Append("          CARGO_TYPE,");
				sb.Append("          POD_ID)T");


				sb.Append(" WHERE  T.INCOME > 0 ");
				if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.VOLUME_IN_CBM <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.VOLUME_IN_CBM >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.VOLUME_IN_CBM =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.NET_WEIGHT <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.NET_WEIGHT >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.NET_WEIGHT =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.ALLOCTEU <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.ALLOCTEU >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 2 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.ALLOCTEU =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.INCOME <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.INCOME>" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.INCOME =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.EXPENSES <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.EXPENSES  >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.EXPENSES  =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.PROFIT_MARGIN <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.PROFIT_MARGIN >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.PROFIT_MARGIN  =" + txtSymbol);
				}

				sb.Append("  UNION ");

				sb.Append(" SELECT *");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        LOCATION_NAME,");
				sb.Append("                        EMPLOYEE_ID,");
				sb.Append("                        EMPLOYEE_NAME,");
				sb.Append("                        CUSTOMER_ID,");
				sb.Append("                        CUSTOMER_NAME,");
				sb.Append("                        BOOKING_REF_NO1,");
				sb.Append("                        BOOKING_REF_NO,");
				sb.Append("                        VESSEL_NAME,");
				sb.Append("                        VOYAGE,");
				sb.Append("                        POL_ID,");
				sb.Append("                        POD_ID,");
				sb.Append("                        NVL(MAX(ALLOC20DV), '') ALLOC20DV,");
				sb.Append("                        NVL(MAX(ALLOC40DV), '') ALLOC40DV,");
				sb.Append("                        NVL(MAX(ALLOCTEU), '') ALLOCTEU,");
				sb.Append("                        NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                        NVL(SUM(INCOME), 0) INCOME,");
				sb.Append("                        NVL(SUM(EXPENSES), 0) EXPENSES,");
				sb.Append("                        NVL(SUM(INCOME - EXPENSES), 0) PROFIT_MARGIN");
				sb.Append("          FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                                FREIGHT_ELEMENT_ID,");
				sb.Append("                                '' CONTAINER_TYPE_MST_ID,");
				sb.Append("                                LOCATION_NAME,");
				sb.Append("                                EMPLOYEE_ID,");
				sb.Append("                                EMPLOYEE_NAME,");
				sb.Append("                                CUSTOMER_ID,");
				sb.Append("                                CUSTOMER_NAME,");
				sb.Append("                                BOOKING_REF_NO1,");
				sb.Append("                                BOOKING_REF_NO,");
				sb.Append("                                VESSEL_NAME,");
				sb.Append("                                VOYAGE,");
				sb.Append("                                POL_ID,");
				sb.Append("                                POD_ID,");
				sb.Append("                                NVL(SUM(ALLOC20DV), '') ALLOC20DV,");
				sb.Append("                                NVL(SUM(ALLOC40DV), '') ALLOC40DV,");
				sb.Append("                                NVL(SUM(ALLOCTEU), '') ALLOCTEU,");
				sb.Append("                                NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM,");
				sb.Append("                                NVL(SUM(INCOME * NET_WEIGHT), 0) INCOME,");
				sb.Append("                                NVL(MAX(EXPENSES * NET_WEIGHT), 0) EXPENSES,");
				sb.Append("                                NVL(MAX((INCOME * NET_WEIGHT) -");
				sb.Append("                                        (EXPENSES * NET_WEIGHT)),");
				sb.Append("                                    0)");
				sb.Append("                  FROM (SELECT DISTINCT BMT.BOOKING_MST_PK BOOKING_MST_PK,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BMT.BOOKING_REF_NO BOOKING_REF_NO1,");
				sb.Append("                                        BMT.BOOKING_REF_NO,");
				sb.Append("                                        AMT.AIRLINE_NAME VESSEL_NAME,");
				sb.Append("                                        JOB.VOYAGE_FLIGHT_NO VOYAGE,");
				sb.Append("                                        POL.PORT_NAME POL_ID,");
				sb.Append("                                        POD.PORT_NAME POD_ID,");
				sb.Append("                                        '' ALLOC20DV,");
				sb.Append("                                        '' ALLOC40DV,");
				sb.Append("                                        '' ALLOCTEU,");
				sb.Append("                                        BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                        BMT.VOLUME_IN_CBM VOLUME_IN_CBM,");
				sb.Append("                                        NVL(BFT.MIN_BASIS_RATE,0)*");
				sb.Append(" GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) INCOME,");
				sb.Append("                                        (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append(" GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),0) ");
				sb.Append("                                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                                            AND CST.COST_ELEMENT_ID =");
				sb.Append("                                                FREIGHT_ELEMENT_ID) EXPENSES");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BMT,");
				sb.Append("                               BOOKING_TRN          BTT,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL    CUMT,");
				sb.Append("                               PORT_MST_TBL             POL,");
				sb.Append("                               PORT_MST_TBL             POD,");
				sb.Append("                               AIRLINE_MST_TBL          AMT,");
				sb.Append("                               JOB_CARD_TRN     JOB");
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND FMT.FREIGHT_ELEMENT_MST_PK(+) =");
				sb.Append("                               BFT.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK(+) = BFT.CURRENCY_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BMT.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                           AND BTT.BOOKING_TRN_PK =");
				sb.Append("                               BFT.BOOKING_TRN_FK(+)");
				sb.Append("                           AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                           AND BMT.BOOKING_MST_PK = BTT.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BMT.IS_EBOOKING = 0");
				sb.Append("                          AND BMT.STATUS IN (2, 6)");
				sb.Append("                         AND BMT.BUSINESS_TYPE = 1");
				sb.Append("                           AND BFT.MIN_BASIS_RATE > 0");

				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BMT.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND CGT.COMMODITY_MST_PK IN(" + CommPK + ")");
				}

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BMT.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                        UNION");

				sb.Append("                        SELECT DISTINCT BMT.BOOKING_MST_PK,");
				sb.Append("                                        CST.COST_ELEMENT_ID   FREIGHT_ELEMENT_ID,");
				sb.Append("                                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append("                                        CUMT.CURRENCY_ID,");
				sb.Append("                                        LMT.LOCATION_NAME,");
				sb.Append("                                        EMT.EMPLOYEE_ID,");
				sb.Append("                                        EMT.EMPLOYEE_NAME,");
				sb.Append("                                        CMT.CUSTOMER_ID,");
				sb.Append("                                        CMT.CUSTOMER_NAME,");
				sb.Append("                                        BMT.BOOKING_REF_NO    BOOKING_REF_NO1,");
				sb.Append("                                        BMT.BOOKING_REF_NO,");
				sb.Append("                                        AMT.AIRLINE_NAME      VESSEL_NAME,");
				sb.Append("                                        JOB.VOYAGE_FLIGHT_NO         VOYAGE,");
				sb.Append("                                        POL.PORT_NAME         POL_ID,");
				sb.Append("                                        POD.PORT_NAME         POD_ID,");
				sb.Append("                                        ''                    ALLOC20DV,");
				sb.Append("                                        ''                     ALLOC40DV,");
				sb.Append("                                        ''                     ALLOCTEU,");
				sb.Append("                                        BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                        BMT.VOLUME_IN_CBM     VOLUME_IN_CBM,");
				sb.Append("                                        0                     INCOME,");
				sb.Append("                                        NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append(" GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES ");
				sb.Append("                          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                               CUSTOMER_MST_TBL         CMT,");
				sb.Append("                               BOOKING_MST_TBL          BMT,");
				sb.Append("                               BOOKING_TRN          BTT,");
				sb.Append("                               BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                               CURRENCY_TYPE_MST_TBL       CUMT,");
				sb.Append("                               PORT_MST_TBL                POL,");
				sb.Append("                               PORT_MST_TBL                POD,");
				sb.Append("                               AIRLINE_MST_TBL             AMT,");
				sb.Append("                               JOB_CARD_TRN        JOB,");
				sb.Append("                               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                               QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("                         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                           AND Q.QUOTATION_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND CUMT.CURRENCY_MST_PK(+) = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                           AND CMT.CUSTOMER_MST_PK(+) =");
				sb.Append("                               BMT.CUSTOMER_MST_FK");
				//sb.Append("                           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                           AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                           AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                           AND BTT.BOOKING_TRN_PK =");
				sb.Append("                               BFT.BOOKING_TRN_FK(+)");
				sb.Append("                           AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                           AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                           AND BMT.BOOKING_MST_PK = BTT.BOOKING_MST_FK");
				sb.Append("                           AND POL.LOCATION_MST_FK IN");
				sb.Append("                               (SELECT L.LOCATION_MST_PK");
				sb.Append("                                  FROM LOCATION_MST_TBL L");
				sb.Append("                                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                           AND BMT.IS_EBOOKING = 0");
				sb.Append("                            AND BMT.STATUS IN (2, 6)   AND BMT.BUSINESS_TYPE = 1 ");


				if (BkgPK.Trim().Length > 0) {
					sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BkgPK + ")");
				}

				if (Convert.ToInt32(Commodityfk) > 0) {
					sb.Append(" AND BMT.Commodity_Group_Fk IN(" + Commodityfk + ")");
				}

				if (CustPK.Trim().Length > 0) {
					sb.Append(" AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}

				if (CommPK.Trim().Length > 0) {
					sb.Append(" AND CGT.COMMODITY_MST_PK IN(" + CommPK + ")");
				}

				if (ExecPK.Trim().Length > 0) {
					sb.Append(" AND EMT.EMPLOYEE_MST_PK IN(" + ExecPK + ")");
				}

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND BMT.BOOKING_DATE BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND BMT.BOOKING_DATE >= TO_DATE('" + toDate + "',dateformat) ");
				}

				sb.Append("                 )");

				sb.Append("                 GROUP BY BOOKING_MST_PK,");
				sb.Append("                          LOCATION_NAME,");
				sb.Append("                          EMPLOYEE_ID,");
				sb.Append("                          EMPLOYEE_NAME,");
				sb.Append("                          CUSTOMER_ID,");
				sb.Append("                          CUSTOMER_NAME,");
				sb.Append("                          BOOKING_REF_NO,");
				sb.Append("                          BOOKING_REF_NO1,");
				sb.Append("                          VESSEL_NAME,");
				sb.Append("                          VOYAGE,");
				sb.Append("                          POL_ID,");
				sb.Append("                          POD_ID,");
				sb.Append("                          FREIGHT_ELEMENT_ID)");

				sb.Append("         GROUP BY BOOKING_MST_PK,");
				sb.Append("                  LOCATION_NAME,");
				sb.Append("                  EMPLOYEE_ID,");
				sb.Append("                  EMPLOYEE_NAME,");
				sb.Append("                  CUSTOMER_ID,");
				sb.Append("                  CUSTOMER_NAME,");
				sb.Append("                  BOOKING_REF_NO,");
				sb.Append("                  BOOKING_REF_NO1,");
				sb.Append("                  VESSEL_NAME,");
				sb.Append("                  VOYAGE,");
				sb.Append("                  POL_ID,");
				sb.Append("                  POD_ID) T");


				sb.Append(" WHERE  T.INCOME >0 ");
				if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND T.VOLUME_IN_CBM <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND T.VOLUME_IN_CBM >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 0 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND T.VOLUME_IN_CBM =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.NET_WEIGHT<" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.NET_WEIGHT >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 1 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.NET_WEIGHT=" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.INCOME <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.INCOME>" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 3 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.INCOME =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.EXPENSES <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.EXPENSES  >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 4 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.EXPENSES  =" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 0) {
					sb.Append(" AND  T.PROFIT_MARGIN <" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 1) {
					sb.Append(" AND  T.PROFIT_MARGIN >" + txtSymbol);
				} else if (txtSymbol > 0 & Convert.ToInt32(ddBasis) == 5 & Convert.ToInt32(ddSymbols) == 2) {
					sb.Append(" AND  T.PROFIT_MARGIN  =" + txtSymbol);
				}

				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException SqlExp) {
				throw SqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}

		public DataSet FetchBothFreightDetail(string BookingPKs = "", Int32 LocFk = 0)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
			string strSQL = null;
			try {
				sb.Append(" SELECT Q.* FROM(");
				sb.Append(" SELECT * FROM(");
				sb.Append("SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                FREIGHT_ELEMENT_ID,");
				sb.Append("                FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as CURRENCY_ID,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(SUM(INCOME * (NO_OF_BOXES)), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(SUM(INCOME * (VOLUME_IN_CBM)), 0)");
				sb.Append("                END INCOME,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(MAX(EXPENSES * (NO_OF_BOXES)), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(MAX(EXPENSES * (VOLUME_IN_CBM)), 0)");
				sb.Append("                END EXPENSES,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(MAX(INCOME * (NO_OF_BOXES) - EXPENSES * (NO_OF_BOXES)),");
				sb.Append("                       0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(MAX(INCOME * (VOLUME_IN_CBM) -");
				sb.Append("                           EXPENSES * (VOLUME_IN_CBM)),");
				sb.Append("                       0)");
				sb.Append("                END PROFIT_MARGIN,");
				sb.Append("                NVL(MAX(ALLOCTEU * (NO_OF_BOXES)), 0) ALLOCTEU,");
				sb.Append("                CASE");
				sb.Append("                  WHEN CARGO_TYPE = 1 THEN");
				sb.Append("                   NVL(MAX(NET_WEIGHT), 0)");
				sb.Append("                  ELSE");
				sb.Append("                   NVL(MAX(GROSS_WEIGHT), 0)");
				sb.Append("                END NET_WEIGHT,");
				sb.Append("                NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM");
				sb.Append("  FROM (SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                        FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                        FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as        CURRENCY_ID,");
				sb.Append("                        NVL(BTSFD.TARIFF_RATE,0)*");
				sb.Append("                        GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) INCOME,");
				sb.Append("                        (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append(" GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),0) ");
				sb.Append("                           FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                COST_ELEMENT_MST_TBL        CST");
				sb.Append("                          WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                            AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                Q.COST_ELEMENT_MST_FK");
				sb.Append("                            AND CST.COST_ELEMENT_ID = FREIGHT_ELEMENT_ID) EXPENSES,");
				sb.Append("                        NVL(TARIFF_RATE * GET_EX_RATE(BTSFD.CURRENCY_MST_FK," + BaseCurrFk + ",BST.BOOKING_DATE) -");
				sb.Append("                            (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append(" GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),0) ");
				sb.Append("                               FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                    COST_ELEMENT_MST_TBL        CST");
				sb.Append("                              WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                    Q.COST_ELEMENT_MST_FK");
				sb.Append("                                AND CST.COST_ELEMENT_ID = FREIGHT_ELEMENT_ID),");
				sb.Append("                            0) PROFIT_MARGIN,");
				sb.Append("                        BTSFL.NO_OF_BOXES,");
				sb.Append("                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                        BST.NET_WEIGHT,");
				sb.Append("                        BST.VOLUME_IN_CBM,");
				sb.Append("                        BST.GROSS_WEIGHT,");
				sb.Append("                        BST.CARGO_TYPE");
				sb.Append("          FROM LOCATION_MST_TBL         LMT,");
				sb.Append("               EMPLOYEE_MST_TBL         EMT,");
				sb.Append("               CUSTOMER_MST_TBL         CMT,");
				sb.Append("               BOOKING_MST_TBL          BST,");
				sb.Append("               BOOKING_TRN  BTSFL,");
				sb.Append("               BOOKING_TRN_FRT_DTLS BTSFD,");
				sb.Append("               CONTAINER_TYPE_MST_TBL   CTMT,");
				sb.Append("               VESSEL_VOYAGE_TBL        VST,");
				sb.Append("               VESSEL_VOYAGE_TRN        VVT,");
				sb.Append("               PORT_MST_TBL             POL,");
				sb.Append("               PORT_MST_TBL             POD,");
				sb.Append("               FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("               CURRENCY_TYPE_MST_TBL    CUMT");
				//sb.Append("               COMMODITY_GROUP_MST_TBL  CGMT,")
				//sb.Append("               COMMODITY_MST_TBL        CGT")
				sb.Append("         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("           AND FMT.FREIGHT_ELEMENT_MST_PK = BTSFD.FREIGHT_ELEMENT_MST_FK");
				sb.Append("           AND CUMT.CURRENCY_MST_PK = BTSFD.CURRENCY_MST_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK(+) = BST.CUSTOMER_MST_FK");
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				//sb.Append("           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("           AND BTSFD.BOOKING_TRN_FK = BTSFL.BOOKING_TRN_PK");
				sb.Append("           AND VST.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				//sb.Append("           AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK")
				//sb.Append("           AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("           AND CTMT.CONTAINER_TYPE_MST_PK(+) = BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("           AND BST.BOOKING_MST_PK(+) = BTSFL.BOOKING_MST_FK");
				sb.Append("           AND POL.LOCATION_MST_FK IN");
				sb.Append("               (SELECT L.LOCATION_MST_PK");
				sb.Append("                  FROM LOCATION_MST_TBL L");
				sb.Append("                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("           AND BST.IS_EBOOKING = 0");
				sb.Append("           AND BST.CARGO_TYPE <> 4");
				sb.Append("           AND BST.STATUS IN (2, 6)");
				sb.Append("           AND BTSFD.TARIFF_RATE > 0");
				sb.Append("           AND BST.BUSINESS_TYPE = 2 ");

				sb.Append(" AND BST.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("        UNION");

				sb.Append("        SELECT DISTINCT BST.BOOKING_MST_PK,");
				sb.Append("                        CST.COST_ELEMENT_ID   FREIGHT_ELEMENT_ID,");
				sb.Append("                        CST.COST_ELEMENT_NAME FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as        CURRENCY_ID,");
				sb.Append("                        0                     INCOME,");
				sb.Append("                         NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES,");
				sb.Append("                        (0 - NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                        GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)) PROFIT_MARGIN,");
				sb.Append("                        BTSFL.NO_OF_BOXES,");
				sb.Append("                        CTMT.TEU_FACTOR ALLOCTEU,");
				sb.Append("                        BST.NET_WEIGHT,");
				sb.Append("                        BST.VOLUME_IN_CBM,");
				sb.Append("                        BST.GROSS_WEIGHT,");
				sb.Append("                        BST.CARGO_TYPE");
				sb.Append("          FROM LOCATION_MST_TBL            LMT,");
				sb.Append("               EMPLOYEE_MST_TBL            EMT,");
				sb.Append("               CUSTOMER_MST_TBL            CMT,");
				sb.Append("               BOOKING_MST_TBL             BST,");
				sb.Append("               BOOKING_TRN     BTSFL,");
				sb.Append("               BOOKING_TRN_FRT_DTLS    BTSFD,");
				sb.Append("               CONTAINER_TYPE_MST_TBL      CTMT,");
				sb.Append("               VESSEL_VOYAGE_TBL           VST,");
				sb.Append("               VESSEL_VOYAGE_TRN           VVT,");
				sb.Append("               PORT_MST_TBL                POL,");
				sb.Append("               PORT_MST_TBL                POD,");
				sb.Append("               CURRENCY_TYPE_MST_TBL       CUMT,");
				//sb.Append("               COMMODITY_GROUP_MST_TBL     CGMT,")
				//sb.Append("               COMMODITY_MST_TBL           CGT,")
				sb.Append("               COST_ELEMENT_MST_TBL        CST,");
				sb.Append("               QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("         WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("           AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("           AND Q.QUOTATION_FK = BST.BOOKING_MST_PK");
				sb.Append("           AND CUMT.CURRENCY_MST_PK = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK(+) = BST.CUSTOMER_MST_FK");
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
				//sb.Append("           AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("           AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK");
				sb.Append("           AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK");
				sb.Append("           AND BTSFD.BOOKING_TRN_FK = BTSFL.BOOKING_TRN_PK");
				sb.Append("           AND VST.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
				sb.Append("           AND VVT.VOYAGE_TRN_PK(+) = BST.VESSEL_VOYAGE_FK");
				//sb.Append("           AND CGMT.COMMODITY_GROUP_PK(+) = BST.COMMODITY_GROUP_FK")
				//sb.Append("           AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("           AND CTMT.CONTAINER_TYPE_MST_PK(+) = BTSFL.CONTAINER_TYPE_MST_FK");
				sb.Append("           AND BST.BOOKING_MST_PK(+) = BTSFL.BOOKING_MST_FK");
				sb.Append("           AND POL.LOCATION_MST_FK IN");
				sb.Append("               (SELECT L.LOCATION_MST_PK");
				sb.Append("                  FROM LOCATION_MST_TBL L");
				sb.Append("                 WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("           AND BST.IS_EBOOKING = 0");
				sb.Append("           AND BST.CARGO_TYPE <> 4");
				sb.Append("           AND BST.STATUS IN (2, 6)");
				sb.Append("           AND BST.BUSINESS_TYPE = 2 ");
				sb.Append(" AND BST.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("                   )");

				sb.Append(" GROUP BY BOOKING_MST_PK, FREIGHT_ELEMENT_ID,FREIGHT_ELEMENT_NAME, CARGO_TYPE)T");

				sb.Append("  UNION ");

				sb.Append(" SELECT * ");
				sb.Append("  FROM (SELECT DISTINCT BOOKING_MST_PK,");
				sb.Append("                        FREIGHT_ELEMENT_ID,");
				sb.Append("                        FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as        CURRENCY_ID,");
				sb.Append("                        NVL(SUM(INCOME * NET_WEIGHT), 0) INCOME,");
				sb.Append("                        NVL(MAX(EXPENSES * NET_WEIGHT), 0) EXPENSES,");
				sb.Append("                        NVL(MAX((INCOME * NET_WEIGHT) -");
				sb.Append("                                (EXPENSES * NET_WEIGHT)),");
				sb.Append("                            0) PROFIT_MARGIN,");
				sb.Append("                        NVL(SUM(ALLOCTEU), '') ALLOCTEU,");
				sb.Append("                        NVL(MAX(NET_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("                        NVL(MAX(VOLUME_IN_CBM), 0) VOLUME_IN_CBM");
				sb.Append("          FROM (SELECT DISTINCT BMT.BOOKING_MST_PK BOOKING_MST_PK,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_MST_PK,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_ID,");
				sb.Append("                                FMT.FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as                CURRENCY_ID,");
				sb.Append("                                '' ALLOCTEU,");
				sb.Append("                                BMT.CHARGEABLE_WEIGHT NET_WEIGHT,");
				sb.Append("                                BMT.VOLUME_IN_CBM,");
				sb.Append("                                 NVL(BFT.MIN_BASIS_RATE,0)*");
				sb.Append("                                GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) INCOME,");
				sb.Append("                                (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE, 0)*");
				sb.Append(" GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),0) ");
				sb.Append("                                   FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                        COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                  WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                    AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                        Q.COST_ELEMENT_MST_FK");
				sb.Append("                                    AND CST.COST_ELEMENT_ID =");
				sb.Append("                                        FREIGHT_ELEMENT_ID) EXPENSES,");
				sb.Append("                                NVL(TARIFF_RATE*GET_EX_RATE(BFT.CURRENCY_MST_FK," + BaseCurrFk + ",BMT.BOOKING_DATE) -");
				sb.Append("                                    (SELECT NVL(SUM(NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append(" GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)),0) ");
				sb.Append("                                       FROM QUOTATION_PROFITABILITY_TBL Q,");
				sb.Append("                                            COST_ELEMENT_MST_TBL        CST");
				sb.Append("                                      WHERE Q.QUOTATION_FK = BOOKING_MST_PK");
				sb.Append("                                        AND CST.COST_ELEMENT_MST_PK =");
				sb.Append("                                            Q.COST_ELEMENT_MST_FK");
				sb.Append("                                        AND CST.COST_ELEMENT_ID =");
				sb.Append("                                            FREIGHT_ELEMENT_ID),");
				sb.Append("                                    0) PROFIT_MARGIN");
				sb.Append("                  FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                       EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                       CUSTOMER_MST_TBL         CMT,");
				sb.Append("                       BOOKING_MST_TBL          BMT,");
				sb.Append("                       BOOKING_TRN          BTT,");
				sb.Append("                       BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                       FREIGHT_ELEMENT_MST_TBL  FMT,");
				sb.Append("                       CURRENCY_TYPE_MST_TBL    CUMT,");
				sb.Append("                       PORT_MST_TBL             POL,");
				sb.Append("                       PORT_MST_TBL             POD,");
				sb.Append("                       AIRLINE_MST_TBL          AMT,");
				sb.Append("                       JOB_CARD_TRN     JOB");
				//sb.Append("                       COMMODITY_GROUP_MST_TBL  CGMT,")
				//sb.Append("                       COMMODITY_MST_TBL        CGT")
				sb.Append("                 WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                   AND FMT.FREIGHT_ELEMENT_MST_PK(+) =");
				sb.Append("                       BFT.FREIGHT_ELEMENT_MST_FK");
				sb.Append("                   AND CUMT.CURRENCY_MST_PK(+) = BFT.CURRENCY_MST_FK");
				sb.Append("                   AND CMT.CUSTOMER_MST_PK(+) = BMT.CUSTOMER_MST_FK");
				//sb.Append("                   AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                   AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                   AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                   AND BTT.BOOKING_TRN_PK(+) = BFT.BOOKING_TRN_FK");
				sb.Append("                   AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                   AND BMT.BOOKING_MST_PK(+) = BTT.BOOKING_MST_FK");
				//sb.Append("                   AND CGMT.COMMODITY_GROUP_PK(+) = BMT.COMMODITY_GROUP_FK")
				//sb.Append("                   AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("                   AND POL.LOCATION_MST_FK IN");
				sb.Append("                       (SELECT L.LOCATION_MST_PK");
				sb.Append("                          FROM LOCATION_MST_TBL L");
				sb.Append("                         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                   AND BMT.IS_EBOOKING = 0");
				sb.Append("                   AND BMT.STATUS IN (2, 6)");
				sb.Append("                   AND BMT.BUSINESS_TYPE = 1 ");
				sb.Append("                   AND BFT.MIN_BASIS_RATE > 0");

				sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("               UNION");

				sb.Append("                SELECT DISTINCT BMT.BOOKING_MST_PK      BOOKING_MST_PK,");
				sb.Append("                                CST.COST_ELEMENT_MST_PK FREIGHT_ELEMENT_MST_PK,");
				sb.Append("                                CST.COST_ELEMENT_ID     FREIGHT_ELEMENT_ID,");
				sb.Append("                                CST.COST_ELEMENT_NAME   FREIGHT_ELEMENT_NAME,");
				sb.Append(BaseCurrFk + " as                CURRENCY_ID,");
				sb.Append("                                ''                       ALLOCTEU,");
				sb.Append("                                BMT.CHARGEABLE_WEIGHT   NET_WEIGHT,");
				sb.Append("                                BMT.VOLUME_IN_CBM,");
				sb.Append("                                0 INCOME,");
				sb.Append("                                NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT) EXPENSES,");
				sb.Append("                                (0 - NVL(Q.PROFITABILITY_RATE,0)*");
				sb.Append("                                GET_EX_RATE_BUY(Q.CURRENCY_TYPE_MST_FK," + BaseCurrFk + ",Q.CREATED_DT)) PROFIT_MARGIN ");
				sb.Append("                  FROM LOCATION_MST_TBL         LMT,");
				sb.Append("                       EMPLOYEE_MST_TBL         EMT,");
				sb.Append("                       CUSTOMER_MST_TBL         CMT,");
				sb.Append("                       BOOKING_MST_TBL          BMT,");
				sb.Append("                       BOOKING_TRN          BTT,");
				sb.Append("                       BOOKING_TRN_FRT_DTLS BFT,");
				sb.Append("                       CURRENCY_TYPE_MST_TBL       CUMT,");
				sb.Append("                       PORT_MST_TBL                POL,");
				sb.Append("                       PORT_MST_TBL                POD,");
				sb.Append("                       AIRLINE_MST_TBL             AMT,");
				sb.Append("                       JOB_CARD_TRN        JOB,");
				//sb.Append("                       COMMODITY_GROUP_MST_TBL     CGMT,")
				//sb.Append("                       COMMODITY_MST_TBL           CGT,")
				sb.Append("                       COST_ELEMENT_MST_TBL        CST,");
				sb.Append("                       QUOTATION_PROFITABILITY_TBL Q");
				sb.Append("                 WHERE LMT.LOCATION_MST_PK(+) = POL.LOCATION_MST_FK");
				sb.Append("                   AND CST.COST_ELEMENT_MST_PK = Q.COST_ELEMENT_MST_FK");
				sb.Append("                   AND Q.QUOTATION_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND CUMT.CURRENCY_MST_PK(+) = Q.CURRENCY_TYPE_MST_FK");
				sb.Append("                   AND CMT.CUSTOMER_MST_PK(+) = BMT.CUSTOMER_MST_FK");
				//sb.Append("                   AND EMT.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
				sb.Append("   AND EMT.EMPLOYEE_MST_PK(+) = BMT.EXECUTIVE_MST_FK");
				sb.Append("                   AND POL.PORT_MST_PK(+) = BMT.PORT_MST_POL_FK");
				sb.Append("                   AND POD.PORT_MST_PK(+) = BMT.PORT_MST_POD_FK");
				sb.Append("                   AND BTT.BOOKING_TRN_PK(+) = BFT.BOOKING_TRN_FK");
				sb.Append("                   AND JOB.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
				sb.Append("                   AND AMT.AIRLINE_MST_PK(+) = BMT.CARRIER_MST_FK");
				sb.Append("                   AND BMT.BOOKING_MST_PK(+) = BTT.BOOKING_MST_FK");
				//sb.Append("                   AND CGMT.COMMODITY_GROUP_PK(+) = BMT.COMMODITY_GROUP_FK")
				//sb.Append("                   AND CGT.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK")
				sb.Append("                   AND POL.LOCATION_MST_FK IN");
				sb.Append("                       (SELECT L.LOCATION_MST_PK");
				sb.Append("                          FROM LOCATION_MST_TBL L");
				sb.Append("                         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
				sb.Append("                   AND BMT.IS_EBOOKING = 0");
				sb.Append("                   AND BMT.STATUS IN (2, 6)");
				sb.Append("                   AND BMT.BUSINESS_TYPE = 1 ");
				sb.Append(" AND BMT.BOOKING_MST_PK IN(" + BookingPKs + ")");

				sb.Append("                   )");

				sb.Append(" GROUP BY BOOKING_MST_PK,");
				sb.Append("          FREIGHT_ELEMENT_ID,");
				sb.Append("          FREIGHT_ELEMENT_NAME)T)Q ,freight_element_mst_tbl femt WHERE");
				sb.Append("  q.freight_element_id=femt.freight_element_id ORDER BY femt.preference");



				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException SqlExp) {
				throw SqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region "GetCustomer"
		public DataSet GetCustomer(string CustPK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			try {
				sb.Append("  SELECT CMT.CUSTOMER_MST_PK, CMT.CUSTOMER_ID, CMT.CUSTOMER_NAME");
				sb.Append("   FROM CUSTOMER_MST_TBL CMT");
				sb.Append("    WHERE CMT.CUSTOMER_MST_PK =" + CustPK);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException SqlExp) {
				throw SqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}

		#endregion

	}
}




