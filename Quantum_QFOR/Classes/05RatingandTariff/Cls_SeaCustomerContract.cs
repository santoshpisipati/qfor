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
    public class Cls_SeaCustomerContract : CommonFeatures
	{

		#region "Private Variables"
		private long _PkValue;
			#endregion
		private long _logPkValue;

		#region "Property"
		public long PkValue {
			get { return _PkValue; }
		}
		#endregion

		#region "Fetch Queries"

		public void FetchOperatorTariffFCL(long TariffPk, string strSearch, DataSet dsMain, string Valid_From, string Valid_To, string bas_curr_fk, string oGroup)
		{
			string strSQL = null;
			Int16 nBOF = default(Int16);
			Int32 nMain = default(Int32);
			DataTable dtTableBOF = new DataTable("BOF");
			DataColumn dcColumn = null;
			DataTable dtTableAllIn = new DataTable("All_In");
			WorkFlow objWF = new WorkFlow();
			try {
				if (strSearch.Trim().Length <= 0) {
					strSearch = "(0,0,0)";
				} else {
					MakeConditionString(strSearch);
				}

				strSQL = "";
				if (Convert.ToInt32(oGroup) == 0) {
					strSQL = " SELECT P.ALL_IN,PORT_MST_POL_FK,PORT_MST_POD_FK,CONT_BASIS FROM (SELECT SUM(ALLIN) AS All_In ,PORT_MST_POL_FK,PORT_MST_POD_FK,CONT_BASIS,PREFERENCES  " + " FROM ( " + "  SELECT ( ";
					strSQL = strSQL + " Allin * CASE WHEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", sysdate)>0 THEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", sysdate) ELSE 1 END " + "  ) AS ALLIN,CURRENCY_MST_PK,PORT_MST_POL_FK,PORT_MST_POD_FK,CONT_BASIS,PREFERENCES " + "  FROM    " + "  ( " + "  SELECT SUM(CONT.FCL_REQ_RATE) AS Allin,CURR.CURRENCY_MST_PK, " + "  T.VALID_FROM,T.PORT_MST_POL_FK,T.PORT_MST_POD_FK, " + "  CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS, CTMT.PREFERENCES " + "  FROM TARIFF_TRN_SEA_FCL_LCL T,  " + "  TARIFF_MAIN_SEA_TBL    TM, ";

					strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + "  PORT_MST_TBL POL, " + "  PORT_MST_TBL POD, " + "  CURRENCY_TYPE_MST_TBL CURR, " + "  CONTAINER_TYPE_MST_TBL CTMT, " + "  FREIGHT_ELEMENT_MST_TBL FRT, " + "  CORPORATE_MST_TBL CORP " + "  WHERE T.TARIFF_MAIN_SEA_FK= " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK  " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + "  AND T.CHECK_FOR_ALL_IN_RT =1 " + "  AND TM.STATUS=1 " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) " + "  IN (" + strSearch + ") " + "  GROUP BY CURR.CURRENCY_ID,CURR.CURRENCY_MST_PK,T.VALID_FROM, " + "  T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CTMT.CONTAINER_TYPE_MST_ID,CTMT.PREFERENCES ORDER BY CTMT.PREFERENCES " + "  )Q,CORPORATE_MST_TBL CORP " + "  ) A WHERE A.Allin > 0  GROUP BY PORT_MST_POL_FK,PORT_MST_POD_FK,CONT_BASIS,PREFERENCES ORDER BY PREFERENCES) P ";


				} else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2) {
					strSQL = strSQL + " SELECT SUM(ALLIN) AS ALL_IN, PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS";
					strSQL = strSQL + "   FROM (SELECT (ALLIN * GET_EX_RATE(CURRENCY_MST_PK, 173, SYSDATE)) AS ALLIN,";
					strSQL = strSQL + "                CURRENCY_MST_PK,";
					strSQL = strSQL + "                PORT_MST_POL_FK,";
					strSQL = strSQL + "                PORT_MST_POD_FK,";
					strSQL = strSQL + "                CONT_BASIS";
					strSQL = strSQL + "           FROM (SELECT DISTINCT SUM(CONT.FCL_REQ_RATE) AS ALLIN,";
					strSQL = strSQL + "                                 CURR.CURRENCY_MST_PK,";
					strSQL = strSQL + "                                 T.VALID_FROM,";
					strSQL = strSQL + "                                 T.POL_GRP_FK PORT_MST_POL_FK,";
					strSQL = strSQL + "                                 T.POD_GRP_FK PORT_MST_POD_FK,";
					strSQL = strSQL + "                                 CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS";
					strSQL = strSQL + "                   FROM TARIFF_TRN_SEA_FCL_LCL  T,";
					strSQL = strSQL + "                        TARIFF_MAIN_SEA_TBL    TM, ";
					strSQL = strSQL + "                        TARIFF_TRN_SEA_CONT_DTL CONT,";
					strSQL = strSQL + "                        CURRENCY_TYPE_MST_TBL   CURR,";
					strSQL = strSQL + "                        CONTAINER_TYPE_MST_TBL  CTMT";
					strSQL = strSQL + "                  WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk;
					strSQL = strSQL + "                   AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK ";
					strSQL = strSQL + "                    AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK";
					strSQL = strSQL + "                    AND CONT.CONTAINER_TYPE_MST_FK =";
					strSQL = strSQL + "                        CTMT.CONTAINER_TYPE_MST_PK";
					strSQL = strSQL + "                    AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK";
					strSQL = strSQL + "                    AND T.CHECK_FOR_ALL_IN_RT = 1";
					strSQL = strSQL + "                   AND TM.STATUS=1 ";
					strSQL = strSQL + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') ";
					strSQL = strSQL + " BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ";
					strSQL = strSQL + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') ";
					strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) ";
					strSQL = strSQL + "  AND (T.POL_GRP_FK,T.POD_GRP_FK,CONT.CONTAINER_TYPE_MST_FK) ";
					strSQL = strSQL + "  IN (" + strSearch + ") ";
					strSQL = strSQL + "                  GROUP BY CURR.CURRENCY_ID,";
					strSQL = strSQL + "                           CURR.CURRENCY_MST_PK,";
					strSQL = strSQL + "                           T.VALID_FROM,";
					strSQL = strSQL + "                           T.PORT_MST_POL_FK,";
					strSQL = strSQL + "                           T.PORT_MST_POD_FK,";
					strSQL = strSQL + "                           T.POL_GRP_FK,";
					strSQL = strSQL + "                           T.POD_GRP_FK,";
					strSQL = strSQL + "                           CTMT.CONTAINER_TYPE_MST_ID) Q) A";
					strSQL = strSQL + "  WHERE A.ALLIN > 0";
					strSQL = strSQL + "  GROUP BY PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS";

				}
				dtTableAllIn = objWF.GetDataTable(strSQL);

				strSQL = "";

				if (Convert.ToInt32(oGroup) == 0) {
					strSQL = "  SELECT " + "  CONT.FCL_REQ_RATE  AS  BOF,CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS, " + "  T.PORT_MST_POL_FK,T.PORT_MST_POD_FK " + "  FROM TARIFF_TRN_SEA_FCL_LCL T,  " + "  TARIFF_MAIN_SEA_TBL    TM, ";
					strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + "  PORT_MST_TBL POL, " + "  PORT_MST_TBL POD, " + "  CURRENCY_TYPE_MST_TBL CURR, " + "  CONTAINER_TYPE_MST_TBL CTMT, " + "  FREIGHT_ELEMENT_MST_TBL FRT, " + "  CORPORATE_MST_TBL CORP " + "  WHERE T.TARIFF_MAIN_SEA_FK=" + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK  " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + "  AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND FRT.FREIGHT_ELEMENT_MST_PK =" + getCommodityGrp(16) + " " + "  AND CONT.FCL_REQ_RATE > 0 " + "  AND TM.STATUS=1 " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) IN  " + "  (" + strSearch + ")";
				} else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2) {
					strSQL = strSQL + "SELECT DISTINCT CONT.FCL_REQ_RATE          AS BOF,";
					strSQL = strSQL + "                CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS,";
					strSQL = strSQL + "                T.POL_GRP_FK               PORT_MST_POL_FK,";
					strSQL = strSQL + "                T.POD_GRP_FK               PORT_MST_POD_FK";
					strSQL = strSQL + "  FROM TARIFF_TRN_SEA_FCL_LCL  T,";
					strSQL = strSQL + "       TARIFF_MAIN_SEA_TBL    TM, ";
					strSQL = strSQL + "       TARIFF_TRN_SEA_CONT_DTL CONT,";
					strSQL = strSQL + "       CURRENCY_TYPE_MST_TBL   CURR,";
					strSQL = strSQL + "       CONTAINER_TYPE_MST_TBL  CTMT,";
					strSQL = strSQL + "       FREIGHT_ELEMENT_MST_TBL FRT";
					strSQL = strSQL + " WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk;
					strSQL = strSQL + "   AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK ";
					strSQL = strSQL + "   AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK";
					strSQL = strSQL + "   AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK";
					strSQL = strSQL + "   AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK";
					strSQL = strSQL + "   AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK";
					strSQL = strSQL + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') ";
					strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ";
					strSQL = strSQL + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') ";
					strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) ";
					strSQL = strSQL + "   AND FRT.FREIGHT_ELEMENT_MST_PK = " + getCommodityGrp(16) + "";
					strSQL = strSQL + "   AND CONT.FCL_REQ_RATE > 0";
					strSQL = strSQL + "   AND TM.STATUS=1 ";
					strSQL = strSQL + "   AND (T.POL_GRP_FK, T.POD_GRP_FK, CONT.CONTAINER_TYPE_MST_FK) IN";
					strSQL = strSQL + "       (" + strSearch + ")";

				}

				dtTableBOF = objWF.GetDataTable(strSQL);

				strSQL = "";

				if (Convert.ToInt32(oGroup) == 0) {
					strSQL = "  SELECT DISTINCT POL.PORT_MST_PK AS POLPK,POL.PORT_ID AS POL," + "  POD.PORT_MST_PK AS PODPK,POD.PORT_ID AS POD, " + "  CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS,CURR.CURRENCY_ID, " + "  0.00 BOF, 0.00 AS ALL_IN," + "  0.00 REQ_BOF, 0.00 REQ_ALLIN,0.00 APP_BOF, 0.00 APP_ALLIN, 0 THL, 0 THD, 0 VOLUME, " + "  '" + Valid_From + "' AS FROMDATE, " + "  '" + Valid_To + "' AS TODATE,CONT.CONTAINER_TYPE_MST_FK,CURR.CURRENCY_MST_PK,CTMT.PREFERENCES " + "  FROM TARIFF_TRN_SEA_FCL_LCL T, " + "  TARIFF_MAIN_SEA_TBL    TM, ";
					strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + "  PORT_MST_TBL POL, " + "  PORT_MST_TBL POD, " + "  CURRENCY_TYPE_MST_TBL CURR, " + "  CONTAINER_TYPE_MST_TBL CTMT, " + "  CORPORATE_MST_TBL CORP " + "  WHERE T.TARIFF_MAIN_SEA_FK=" + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK  " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND CURR.CURRENCY_MST_PK = T.CURRENCY_MST_FK " + "  AND T.CURRENCY_MST_FK = (SELECT DISTINCT CURRENCY_MST_PK FROM TARIFF_TRN_SEA_FCL_LCL TR," + "   CURRENCY_TYPE_MST_TBL CR,FREIGHT_ELEMENT_MST_TBL FR" + "   where " + "   CR.CURRENCY_MST_PK = TR.CURRENCY_MST_FK AND" + "   TR.Freight_Element_Mst_Fk = FR.FREIGHT_ELEMENT_MST_PK AND " + "   TR.TARIFF_MAIN_SEA_FK= " + TariffPk + "  AND " + "   FR.FREIGHT_ELEMENT_MST_PK = " + getCommodityGrp(16) + " " + "  AND TR.PORT_MST_POL_FK=T.PORT_MST_POL_FK" + "  AND TR.PORT_MST_POD_FK=T.PORT_MST_POD_FK ) ";
					strSQL = strSQL + "    AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + "   AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + "  AND TM.STATUS=1 " + "   AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) " + "  IN (" + strSearch + ")" + " ORDER BY PREFERENCES";
				} else if (Convert.ToInt32(oGroup)== 1 | Convert.ToInt32(oGroup) == 2) {
					strSQL = strSQL + "SELECT DISTINCT POLGRP.PORT_GRP_MST_PK AS POLPK,";
					strSQL = strSQL + "                POLGRP.PORT_GRP_ID AS POL,";
					strSQL = strSQL + "                PODGRP.PORT_GRP_MST_PK AS PODPK,";
					strSQL = strSQL + "                PODGRP.PORT_GRP_ID AS POD,";
					strSQL = strSQL + "                CTMT.CONTAINER_TYPE_MST_ID CONT_BASIS,";
					strSQL = strSQL + "                CURR.CURRENCY_ID,";
					strSQL = strSQL + "                0.00 BOF,";
					strSQL = strSQL + "                0.00 AS ALL_IN,";
					strSQL = strSQL + "                0.00 REQ_BOF,";
					strSQL = strSQL + "                0.00 REQ_ALLIN,";
					strSQL = strSQL + "                0.00 APP_BOF,";
					strSQL = strSQL + "                0.00 APP_ALLIN,";
					strSQL = strSQL + "                0 THL,";
					strSQL = strSQL + "                0 THD,";
					strSQL = strSQL + "                0 VOLUME,";
					strSQL = strSQL + "                '" + Valid_From + "' AS FROMDATE,";
					strSQL = strSQL + "                '" + Valid_To + "' AS TODATE,";
					strSQL = strSQL + "                CONT.CONTAINER_TYPE_MST_FK,";
					strSQL = strSQL + "                CURR.CURRENCY_MST_PK,CTMT.PREFERENCES";
					strSQL = strSQL + "  FROM TARIFF_TRN_SEA_FCL_LCL  T,";
					strSQL = strSQL + "       TARIFF_MAIN_SEA_TBL    TM, ";
					strSQL = strSQL + "       TARIFF_TRN_SEA_CONT_DTL CONT,";
					strSQL = strSQL + "       PORT_MST_TBL            POL,";
					strSQL = strSQL + "       PORT_MST_TBL            POD,";
					strSQL = strSQL + "       CURRENCY_TYPE_MST_TBL   CURR,";
					strSQL = strSQL + "       CONTAINER_TYPE_MST_TBL  CTMT,";
					strSQL = strSQL + "       PORT_GRP_MST_TBL        POLGRP,";
					strSQL = strSQL + "       PORT_GRP_MST_TBL        PODGRP";
					strSQL = strSQL + " WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk;
					strSQL = strSQL + "   AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK ";
					strSQL = strSQL + "   AND T.PORT_MST_POL_FK = POL.PORT_MST_PK";
					strSQL = strSQL + "   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK";
					strSQL = strSQL + "   AND T.POL_GRP_FK = POLGRP.PORT_GRP_MST_PK";
					strSQL = strSQL + "   AND T.POD_GRP_FK = PODGRP.PORT_GRP_MST_PK";
					strSQL = strSQL + "   AND CURR.CURRENCY_MST_PK = T.CURRENCY_MST_FK";
					strSQL = strSQL + "   AND T.CURRENCY_MST_FK =";
					strSQL = strSQL + "       (SELECT DISTINCT CURRENCY_MST_PK";
					strSQL = strSQL + "          FROM TARIFF_TRN_SEA_FCL_LCL  TR,";
					strSQL = strSQL + "               CURRENCY_TYPE_MST_TBL   CR,";
					strSQL = strSQL + "               FREIGHT_ELEMENT_MST_TBL FR";
					strSQL = strSQL + "         WHERE CR.CURRENCY_MST_PK = TR.CURRENCY_MST_FK";
					strSQL = strSQL + "           AND TR.FREIGHT_ELEMENT_MST_FK = FR.FREIGHT_ELEMENT_MST_PK";
					strSQL = strSQL + "           AND TR.TARIFF_MAIN_SEA_FK = " + TariffPk;
					strSQL = strSQL + "           AND FR.FREIGHT_ELEMENT_MST_PK = " + getCommodityGrp(16) + "";
					strSQL = strSQL + "           AND TR.PORT_MST_POL_FK = T.PORT_MST_POL_FK";
					strSQL = strSQL + "           AND TR.PORT_MST_POD_FK = T.PORT_MST_POD_FK)";
					strSQL = strSQL + "   AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK";
					strSQL = strSQL + "   AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK";
					strSQL = strSQL + "   AND TM.STATUS=1 ";
					strSQL = strSQL + "   AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') ";
					strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ";
					strSQL = strSQL + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') ";
					strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) ";
					strSQL = strSQL + "   AND (T.POL_GRP_FK, T.POD_GRP_FK, CONT.CONTAINER_TYPE_MST_FK) IN";
					strSQL = strSQL + "       (" + strSearch + ")";

				}
				DataTable DT = new DataTable();
				DT = objWF.GetDataTable(strSQL);
				DT.Columns.Remove("PREFERENCES");
				DT.AcceptChanges();
				dsMain.Tables.Add(DT);
				dsMain.Tables[0].TableName = "Main";

				dcColumn = new DataColumn("Surcharge", typeof(Int16));
				dsMain.Tables["Main"].Columns.Add(dcColumn);

				dcColumn = new DataColumn("Del", typeof(Int16));
				dsMain.Tables["Main"].Columns.Add(dcColumn);

				dcColumn = new DataColumn("Srr_Trn_Pk", typeof(Int64));
				dsMain.Tables["Main"].Columns.Add(dcColumn);


				for (nMain = 0; nMain <= dsMain.Tables["Main"].Rows.Count - 1; nMain++) {

					for (nBOF = 0; nBOF <= dtTableBOF.Rows.Count - 1; nBOF++) {

						if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableBOF.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POD_FK"]) {
							dsMain.Tables["Main"].Rows[nMain]["BOF"] = dtTableBOF.Rows[nBOF]["BOF"];
							dsMain.Tables["Main"].Rows[nMain]["APP_BOF"] = dtTableBOF.Rows[nBOF]["BOF"];
							break; // TODO: might not be correct. Was : Exit For
						}
					}

					for (nBOF = 0; nBOF <= dtTableAllIn.Rows.Count - 1; nBOF++) {

						if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableAllIn.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POD_FK"]) {
							dsMain.Tables["Main"].Rows[nMain]["ALL_IN"] = dtTableAllIn.Rows[nBOF]["All_In"];
							break; // TODO: might not be correct. Was : Exit For
						}
					}

				}

				strSQL = "";

				if (Convert.ToInt32(oGroup) == 0) {
					strSQL = "  SELECT " + "  T.PORT_MST_POL_FK AS POL, T.PORT_MST_POD_FK AS POD, " + "  FRT.FREIGHT_ELEMENT_ID, DECODE(T.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,TO_CHAR(T.CHECK_FOR_ALL_IN_RT) CHK, " + "  CURR.CURRENCY_ID,CTMT.CONTAINER_TYPE_MST_ID AS CONT_BASIS, " + "  NVL(CONT.FCL_REQ_RATE,0.00) AS TARIFF_RATE,0.00 AS REQUESTED_RATE, " + "  NVL(CONT.FCL_REQ_RATE,0.00) AS APPROVED_RATE," + "  CURR.CURRENCY_MST_PK,FRT.FREIGHT_ELEMENT_MST_PK " + "  FROM TARIFF_TRN_SEA_FCL_LCL T, " + "  TARIFF_MAIN_SEA_TBL    TM, ";
					strSQL = strSQL + "  TARIFF_TRN_SEA_CONT_DTL CONT, " + "  CURRENCY_TYPE_MST_TBL CURR, " + "  CONTAINER_TYPE_MST_TBL CTMT, " + "  FREIGHT_ELEMENT_MST_TBL FRT " + "  WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ";
					strSQL = strSQL + "   AND T.CURRENCY_MST_FK IN ";
					strSQL = strSQL + "       (SELECT DISTINCT CURRENCY_MST_PK";
					strSQL = strSQL + "          FROM TARIFF_TRN_SEA_FCL_LCL  TR,";
					strSQL = strSQL + "               CURRENCY_TYPE_MST_TBL   CR,";
					strSQL = strSQL + "               FREIGHT_ELEMENT_MST_TBL FR";
					strSQL = strSQL + "         WHERE CR.CURRENCY_MST_PK = TR.CURRENCY_MST_FK";
					strSQL = strSQL + "           AND TR.FREIGHT_ELEMENT_MST_FK = FR.FREIGHT_ELEMENT_MST_PK";
					strSQL = strSQL + "           AND TR.TARIFF_MAIN_SEA_FK = " + TariffPk;
					//strSQL = strSQL & "           AND FR.FREIGHT_ELEMENT_MST_PK = " & getCommodityGrp(16) & ""
					strSQL = strSQL + "           AND TR.PORT_MST_POL_FK = T.PORT_MST_POL_FK";
					strSQL = strSQL + "           AND TR.PORT_MST_POD_FK = T.PORT_MST_POD_FK)" + "  AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK " + "  AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND FRT.FREIGHT_ELEMENT_MST_PK <> " + getCommodityGrp(16) + " " + "  AND CONT.FCL_REQ_RATE > 0 " + "  AND TM.STATUS=1 " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,CONT.CONTAINER_TYPE_MST_FK) " + "  IN (" + strSearch + ")" + "  ORDER BY FRT.PREFERENCE ";
				} else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2) {
					strSQL = strSQL + "SELECT DISTINCT *";
					strSQL = strSQL + "  FROM (SELECT T.POL_GRP_FK AS POL,";
					strSQL = strSQL + "               T.POD_GRP_FK AS POD,";
					strSQL = strSQL + "               FRT.FREIGHT_ELEMENT_ID,";
					strSQL = strSQL + "               DECODE(T.CHARGE_BASIS,";
					strSQL = strSQL + "                      '1',";
					strSQL = strSQL + "                      '%',";
					strSQL = strSQL + "                      '2',";
					strSQL = strSQL + "                      'Flat Rate',";
					strSQL = strSQL + "                      '3',";
					strSQL = strSQL + "                      'Kgs',";
					strSQL = strSQL + "                      '4',";
					strSQL = strSQL + "                      'Unit') CHARGE_BASIS,";
					strSQL = strSQL + "               TO_CHAR(T.CHECK_FOR_ALL_IN_RT) CHK,";
					strSQL = strSQL + "               CURR.CURRENCY_ID,";
					strSQL = strSQL + "               CTMT.CONTAINER_TYPE_MST_ID AS CONT_BASIS,";
					strSQL = strSQL + "               NVL(CONT.FCL_REQ_RATE, 0.00) AS TARIFF_RATE,";
					strSQL = strSQL + "               0.00 AS REQUESTED_RATE,";
					strSQL = strSQL + "               NVL(CONT.FCL_REQ_RATE, 0.00) AS APPROVED_RATE,";
					strSQL = strSQL + "               CURR.CURRENCY_MST_PK,";
					strSQL = strSQL + "               FRT.FREIGHT_ELEMENT_MST_PK";
					strSQL = strSQL + "          FROM TARIFF_TRN_SEA_FCL_LCL  T,";
					strSQL = strSQL + "               TARIFF_MAIN_SEA_TBL    TM, ";
					strSQL = strSQL + "               TARIFF_TRN_SEA_CONT_DTL CONT,";
					strSQL = strSQL + "               CURRENCY_TYPE_MST_TBL   CURR,";
					strSQL = strSQL + "               CONTAINER_TYPE_MST_TBL  CTMT,";
					strSQL = strSQL + "               FREIGHT_ELEMENT_MST_TBL FRT";
					strSQL = strSQL + "         WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk;
					strSQL = strSQL + "           AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK ";
					strSQL = strSQL + "           AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK";
					strSQL = strSQL + "           AND CONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK";
					strSQL = strSQL + "           AND CONT.TARIFF_TRN_SEA_FK = T.TARIFF_TRN_SEA_PK";
					strSQL = strSQL + "           AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK";
					strSQL = strSQL + "           AND FRT.FREIGHT_ELEMENT_MST_PK <> " + getCommodityGrp(16) + "";
					strSQL = strSQL + "           AND CONT.FCL_REQ_RATE > 0";
					strSQL = strSQL + "           AND TM.STATUS=1 ";
					strSQL = strSQL + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') ";
					strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ";
					strSQL = strSQL + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') ";
					strSQL = strSQL + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) ";
					strSQL = strSQL + "           AND (T.POL_GRP_FK, T.POD_GRP_FK, CONT.CONTAINER_TYPE_MST_FK) IN";
					strSQL = strSQL + "               (" + strSearch + ")";
					strSQL = strSQL + "         ORDER BY FRT.PREFERENCE)";

				}

				dsMain.Tables.Add(objWF.GetDataTable(strSQL));
				dsMain.Tables[1].TableName = "Frt";

				dcColumn = new DataColumn("SRR_SUR_CHRG_SEA_PK", typeof(Int64));
				dsMain.Tables["Frt"].Columns.Add(dcColumn);

				//Dim validRowCollChild As New List(Of DataRow)
				//Dim parentTableName As String = "Main"
				//Dim ChildTableName As String = "Frt"
				//If dsMain.Tables(parentTableName).Rows.Count = 0 Then
				//    dsMain.Tables(ChildTableName).Rows.Clear()
				//Else
				//    For Each _childRow As DataRow In dsMain.Tables(ChildTableName).Rows
				//        Dim parentExist As Boolean = False
				//        For Each _parentRow As DataRow In dsMain.Tables(ChildTableName).Rows
				//            If _childRow("") = _parentRow("") Then
				//                parentExist = True
				//                Exit For
				//            End If
				//        Next

				//        If parentExist Then
				//            validRowCollChild.Add(_childRow)
				//        End If
				//    Next
				//End If

				//dsMain.Tables(ChildTableName).Rows.Clear()
				//For Each _row As DataRow In validRowCollChild
				//    dsMain.Tables(ChildTableName).Rows.Add(_row)
				//Next

				DataColumn[] dcParent = null;
				DataColumn[] dcChild = null;
				DataRelation re = null;
				dcParent = new DataColumn[] {
					dsMain.Tables["Main"].Columns["POLPK"],
					dsMain.Tables["Main"].Columns["PODPK"],
					dsMain.Tables["Main"].Columns["CONT_BASIS"]
				};
				dcChild = new DataColumn[] {
					dsMain.Tables["Frt"].Columns["POL"],
					dsMain.Tables["Frt"].Columns["POD"],
					dsMain.Tables["Frt"].Columns["CONT_BASIS"]
				};
				re = new DataRelation("rl_Port", dcParent, dcChild);

				dsMain.Relations.Add(re);
			} catch (OracleException oraEx) {
				throw oraEx;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public void FetchOperatorTariffLCL(long TariffPk, string strSearch, DataSet dsMain, string Valid_From, string Valid_To, string bas_curr_fk, string oGroup)
		{
			string strSQL = null;
			Int16 nBOF = default(Int16);
			Int32 nMain = default(Int32);
			DataTable dtTableBOF = new DataTable("BOF");
			DataColumn dcColumn = null;
			DataTable dtTableAllIn = new DataTable("All_In");
			WorkFlow objWF = new WorkFlow();
			try {
				if (strSearch.Trim().Length <= 0) {
					strSearch = "(0,0,0)";
				} else {
					MakeConditionString(strSearch);
				}

				strSQL = "";
				if (Convert.ToInt32(oGroup) == 0) {
					strSQL = " SELECT SUM(ALLIN) AS ALL_IN, PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS " + " FROM ( " + "  SELECT ( ";

					strSQL = strSQL + " Allin * CASE WHEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", VALID_FROM)>0 THEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", VALID_FROM) ELSE 1 END " + "              ) AS ALLIN, " + "              CURRENCY_MST_PK, " + "              PORT_MST_POL_FK, " + "              PORT_MST_POD_FK, " + "              CONT_BASIS " + "         FROM (SELECT SUM(T.LCL_TARIFF_RATE) AS ALLIN, " + "                      CURR.CURRENCY_MST_PK, " + "                      T.VALID_FROM, " + "                      T.PORT_MST_POL_FK, " + "                      T.PORT_MST_POD_FK, " + "                      UOM.DIMENTION_ID CONT_BASIS " + "                 FROM TARIFF_TRN_SEA_FCL_LCL T, " + "                      TARIFF_MAIN_SEA_TBL    TM, " + "                      DIMENTION_UNIT_MST_TBL UOM, " + "                      PORT_MST_TBL POL, " + "                      PORT_MST_TBL POD, " + "                      CURRENCY_TYPE_MST_TBL CURR, " + "                      FREIGHT_ELEMENT_MST_TBL FRT, " + "                      CORPORATE_MST_TBL CORP " + "                WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "                  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + "                  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + "                  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "                  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "                  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "                  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "                  AND T.CHECK_FOR_ALL_IN_RT = 1 " + "                  AND TM.STATUS=1 " + "                  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "                            BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "                       OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "                            BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "                  AND (T.PORT_MST_POL_FK, T.PORT_MST_POD_FK,T.LCL_BASIS) IN " + "                      (" + strSearch + ") " + "                GROUP BY CURR.CURRENCY_ID, " + "                         CURR.CURRENCY_MST_PK, " + "                         T.VALID_FROM, " + "                         T.PORT_MST_POL_FK, " + "                         T.PORT_MST_POD_FK, " + "                         UOM.DIMENTION_ID ) Q, " + "              CORPORATE_MST_TBL CORP) A " + " WHERE(A.ALLIN > 0) " + " GROUP BY PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS";
				} else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2) {
					strSQL = " SELECT SUM(ALLIN) AS ALL_IN, PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS " + " FROM ( " + "  SELECT ( ";

					strSQL = strSQL + " Allin * CASE WHEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", VALID_FROM)>0 THEN get_ex_rate(CURRENCY_MST_PK, " + bas_curr_fk + ", VALID_FROM) ELSE 1 END " + "              ) AS ALLIN, " + "              CURRENCY_MST_PK, " + "              PORT_MST_POL_FK, " + "              PORT_MST_POD_FK, " + "              CONT_BASIS " + "         FROM (SELECT DISTINCT SUM(T.LCL_TARIFF_RATE) AS ALLIN, " + "                      CURR.CURRENCY_MST_PK, " + "                      T.VALID_FROM, " + "                      t.pol_grp_fk PORT_MST_POL_FK, " + "                      t.pod_grp_fk PORT_MST_POD_FK, " + "                      UOM.DIMENTION_ID CONT_BASIS " + "                 FROM TARIFF_TRN_SEA_FCL_LCL T, " + "                      TARIFF_MAIN_SEA_TBL    TM, " + "                      DIMENTION_UNIT_MST_TBL UOM, " + "                      PORT_MST_TBL POL, " + "                      PORT_MST_TBL POD, " + "                      CURRENCY_TYPE_MST_TBL CURR, " + "                      FREIGHT_ELEMENT_MST_TBL FRT, " + "                      CORPORATE_MST_TBL CORP " + "                WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "                  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + "                  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + "                  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "                  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "                  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "                  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "                  AND T.CHECK_FOR_ALL_IN_RT = 1 " + "                  AND TM.STATUS=1 " + "                  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "                            BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "                       OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "                            BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "                  AND (T.POL_GRP_FK, T.POD_GRP_FK, T.LCL_BASIS) IN " + "                      (" + strSearch + ") " + "                GROUP BY CURR.CURRENCY_ID, " + "                         CURR.CURRENCY_MST_PK, " + "                         T.VALID_FROM, " + "                         T.PORT_MST_POL_FK, " + "                         T.PORT_MST_POD_FK, t.pol_grp_fk, t.pod_grp_fk, " + "                         UOM.DIMENTION_ID ) Q, " + "              CORPORATE_MST_TBL CORP) A " + " WHERE(A.ALLIN > 0) " + " GROUP BY PORT_MST_POL_FK, PORT_MST_POD_FK, CONT_BASIS";
				}

				dtTableAllIn = objWF.GetDataTable(strSQL);

				if (Convert.ToInt32(oGroup) == 0) {
					strSQL = "SELECT " + " T.LCL_TARIFF_RATE AS BOF, " + "     UOM.DIMENTION_ID CONT_BASIS, " + "     T.PORT_MST_POL_FK, " + "     T.PORT_MST_POD_FK, " + "     T.Currency_Mst_Fk  CURRENCY_MST_PK  " + " FROM TARIFF_TRN_SEA_FCL_LCL T, " + "     TARIFF_MAIN_SEA_TBL    TM, " + "     DIMENTION_UNIT_MST_TBL UOM, " + "     PORT_MST_TBL POL, " + "     PORT_MST_TBL POD, " + "     CURRENCY_TYPE_MST_TBL CURR, " + "     FREIGHT_ELEMENT_MST_TBL FRT, " + "     CORPORATE_MST_TBL CORP " + " WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND FRT.FREIGHT_ELEMENT_MST_PK = " + getCommodityGrp(16) + " " + "  AND T.LCL_BASIS>0 " + "  AND TM.STATUS=1 " + "  AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,T.LCL_BASIS) IN " + "      (" + strSearch + ") ";
				} else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2) {
					strSQL = "SELECT DISTINCT " + " T.LCL_TARIFF_RATE AS BOF, " + "     UOM.DIMENTION_ID CONT_BASIS, " + "     T.POL_GRP_FK PORT_MST_POL_FK, " + "     T.POD_GRP_FK PORT_MST_POD_FK, " + "     T.Currency_Mst_Fk  CURRENCY_MST_PK  " + " FROM TARIFF_TRN_SEA_FCL_LCL T, " + "     TARIFF_MAIN_SEA_TBL    TM, " + "     DIMENTION_UNIT_MST_TBL UOM, " + "     PORT_MST_TBL POL, " + "     PORT_MST_TBL POD, " + "     CURRENCY_TYPE_MST_TBL CURR, " + "     FREIGHT_ELEMENT_MST_TBL FRT, " + "     CORPORATE_MST_TBL CORP " + "WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + "  AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + "  AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND FRT.FREIGHT_ELEMENT_ID = 'BOF' " + "  AND T.LCL_BASIS>0 " + "  AND TM.STATUS=1 " + "  AND (T.POL_GRP_FK, T.POD_GRP_FK, T.LCL_BASIS) IN " + "      (" + strSearch + ") ";
				}

				dtTableBOF = objWF.GetDataTable(strSQL);

				strSQL = "";
				if (Convert.ToInt32(oGroup) == 0) {
					strSQL = "SELECT DISTINCT " + "         POL.PORT_MST_PK AS POLPK, " + "         POL.PORT_ID AS POL, " + "         POD.PORT_MST_PK AS PODPK, " + "         POD.PORT_ID AS POD, " + "         UOM.DIMENTION_ID CONT_BASIS, " + "         CURR.CURRENCY_ID, " + "         0.00 BOF, " + "         0.00 AS ALL_IN, " + "         0.00 REQ_BOF, " + "         0.00 REQ_ALLIN,0.00 APP_BOF, 0.00 APP_ALLIN, " + "         0 THL, " + "         0 THD, " + "         0 VOLUME, " + "         '" + Valid_From + "' AS FROMDATE, " + "         '" + Valid_To + "' AS TODATE, " + "         T.LCL_BASIS, " + "         CURR.CURRENCY_MST_PK " + " FROM TARIFF_TRN_SEA_FCL_LCL T, " + "     TARIFF_MAIN_SEA_TBL    TM, " + "     DIMENTION_UNIT_MST_TBL UOM, " + "     PORT_MST_TBL POL, " + "     PORT_MST_TBL POD, " + "     CURRENCY_TYPE_MST_TBL CURR, " + "     CORPORATE_MST_TBL CORP " + "     WHERE T.TARIFF_MAIN_SEA_FK =" + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + " AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + " AND T.PORT_MST_POD_FK = POD.PORT_MST_PK " + "  AND CURR.CURRENCY_MST_PK = T.CURRENCY_MST_FK " + "  AND T.CURRENCY_MST_FK IN(SELECT DISTINCT CURRENCY_MST_PK FROM TARIFF_TRN_SEA_FCL_LCL TR," + "   CURRENCY_TYPE_MST_TBL CR,FREIGHT_ELEMENT_MST_TBL FR" + "   where " + "   CR.CURRENCY_MST_PK = TR.CURRENCY_MST_FK AND" + "   TR.Freight_Element_Mst_Fk = FR.FREIGHT_ELEMENT_MST_PK AND " + "   TR.TARIFF_MAIN_SEA_FK= " + TariffPk + "  AND " + "   FR.FREIGHT_ELEMENT_ID = 'BOF' " + "  AND TR.PORT_MST_POL_FK=T.PORT_MST_POL_FK" + "  AND TR.PORT_MST_POD_FK=T.PORT_MST_POD_FK ) " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND TM.STATUS=1 " + " AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "          BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "     OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "          BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + " AND (T.PORT_MST_POL_FK,T.PORT_MST_POD_FK,T.LCL_BASIS) IN " + "     (" + strSearch + ") ";
				} else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2) {
					strSQL = "SELECT DISTINCT " + "         POLGRP.PORT_GRP_MST_PK AS POLPK, " + "         POLGRP.PORT_GRP_ID AS POL, " + "         PODGRP.PORT_GRP_MST_PK AS PODPK, " + "         PODGRP.PORT_GRP_ID AS POD, " + "         UOM.DIMENTION_ID CONT_BASIS, " + "         CURR.CURRENCY_ID, " + "         0.00 BOF, " + "         0.00 AS ALL_IN, " + "         0.00 REQ_BOF, " + "         0.00 REQ_ALLIN,0.00 APP_BOF, 0.00 APP_ALLIN, " + "         0 THL, " + "         0 THD, " + "         0 VOLUME, " + "         '" + Valid_From + "' AS FROMDATE, " + "         '" + Valid_To + "' AS TODATE, " + "         T.LCL_BASIS, " + "         CURR.CURRENCY_MST_PK " + " FROM TARIFF_TRN_SEA_FCL_LCL T, " + "     TARIFF_MAIN_SEA_TBL    TM, " + "     DIMENTION_UNIT_MST_TBL UOM, " + "     PORT_MST_TBL POL, " + "     PORT_MST_TBL POD, PORT_GRP_MST_TBL       POLGRP, PORT_GRP_MST_TBL       PODGRP, " + "     CURRENCY_TYPE_MST_TBL CURR, " + "     CORPORATE_MST_TBL CORP " + "     WHERE T.TARIFF_MAIN_SEA_FK =" + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + " AND T.PORT_MST_POL_FK = POL.PORT_MST_PK " + " AND T.PORT_MST_POD_FK = POD.PORT_MST_PK AND T.POL_GRP_FK = POLGRP.PORT_GRP_MST_PK AND T.POD_GRP_FK = PODGRP.PORT_GRP_MST_PK " + "  AND CURR.CURRENCY_MST_PK = T.CURRENCY_MST_FK " + "  AND T.CURRENCY_MST_FK IN(SELECT DISTINCT CURRENCY_MST_PK FROM TARIFF_TRN_SEA_FCL_LCL TR," + "   CURRENCY_TYPE_MST_TBL CR,FREIGHT_ELEMENT_MST_TBL FR" + "   where " + "   CR.CURRENCY_MST_PK = TR.CURRENCY_MST_FK AND" + "   TR.Freight_Element_Mst_Fk = FR.FREIGHT_ELEMENT_MST_PK AND " + "   TR.TARIFF_MAIN_SEA_FK= " + TariffPk + "  AND " + "   FR.FREIGHT_ELEMENT_ID = 'BOF' " + "  AND TR.PORT_MST_POL_FK=T.PORT_MST_POL_FK" + "  AND TR.PORT_MST_POD_FK=T.PORT_MST_POD_FK ) " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND TM.STATUS=1 " + " AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "          BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "     OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "          BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + " AND (T.POL_GRP_FK, T.POD_GRP_FK, T.LCL_BASIS) IN " + "     (" + strSearch + ") ";
				}

				dsMain.Tables.Add(objWF.GetDataTable(strSQL));
				dsMain.Tables[0].TableName = "Main";
				dcColumn = new DataColumn("Surcharge", typeof(Int16));
				dsMain.Tables["Main"].Columns.Add(dcColumn);

				dcColumn = new DataColumn("Del", typeof(Int16));
				dsMain.Tables["Main"].Columns.Add(dcColumn);

				dcColumn = new DataColumn("Srr_Trn_Pk", typeof(Int64));
				dsMain.Tables["Main"].Columns.Add(dcColumn);


				for (nMain = 0; nMain <= dsMain.Tables["Main"].Rows.Count - 1; nMain++) {
					for (nBOF = 0; nBOF <= dtTableBOF.Rows.Count - 1; nBOF++) {

						if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableBOF.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableBOF.Rows[nBOF]["PORT_MST_POD_FK"]) {
							dsMain.Tables["Main"].Rows[nMain]["BOF"] = dtTableBOF.Rows[nBOF]["BOF"];
							dsMain.Tables["Main"].Rows[nMain]["APP_BOF"] = dtTableBOF.Rows[nBOF]["BOF"];
							break; // TODO: might not be correct. Was : Exit For
						}
					}

					for (nBOF = 0; nBOF <= dtTableAllIn.Rows.Count - 1; nBOF++) {

						if (dsMain.Tables["Main"].Rows[nMain]["CONT_BASIS"] == dtTableAllIn.Rows[nBOF]["CONT_BASIS"] & dsMain.Tables["Main"].Rows[nMain]["POLPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POL_FK"] & dsMain.Tables["Main"].Rows[nMain]["PODPK"] == dtTableAllIn.Rows[nBOF]["PORT_MST_POD_FK"]) {
							dsMain.Tables["Main"].Rows[nMain]["ALL_IN"] = dtTableAllIn.Rows[nBOF]["All_In"];
							break; // TODO: might not be correct. Was : Exit For
						}
					}

				}

				strSQL = "";

				if (Convert.ToInt32(oGroup) == 0) {
					strSQL = " SELECT T.PORT_MST_POL_FK AS POL, " + "      T.PORT_MST_POD_FK AS POD, " + "      FRT.FREIGHT_ELEMENT_ID,DECODE(T.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, " + "      TO_CHAR(T.CHECK_FOR_ALL_IN_RT) CHK, " + "      CURR.CURRENCY_ID, " + "      UOM.DIMENTION_ID AS CONT_BASIS, " + "      NVL(T.LCL_TARIFF_RATE,0.00) AS TARIFF_RATE, " + "      0.00 AS REQUESTED_RATE,NVL(T.LCL_TARIFF_RATE,0.00) AS APPROVED_RATE, " + "      CURR.CURRENCY_MST_PK, " + "      FRT.FREIGHT_ELEMENT_MST_PK " + " FROM TARIFF_TRN_SEA_FCL_LCL T, " + "     TARIFF_MAIN_SEA_TBL    TM, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      FREIGHT_ELEMENT_MST_TBL FRT " + " WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND FRT.FREIGHT_ELEMENT_ID <> 'BOF' " + "  AND T.LCL_BASIS > 0 " + "  AND TM.STATUS=1 " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.PORT_MST_POL_FK, T.PORT_MST_POD_FK,T.LCL_BASIS ) IN " + "      (" + strSearch + ") ";
				} else if (Convert.ToInt32(oGroup) == 1 | Convert.ToInt32(oGroup) == 2) {
					strSQL = " SELECT DISTINCT T.POL_GRP_FK AS POL, " + "      T.POD_GRP_FK AS POD, " + "      FRT.FREIGHT_ELEMENT_ID,DECODE(T.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, " + "      TO_CHAR(T.CHECK_FOR_ALL_IN_RT) CHK, " + "      CURR.CURRENCY_ID, " + "      UOM.DIMENTION_ID AS CONT_BASIS, " + "      NVL(T.LCL_TARIFF_RATE,0.00) AS TARIFF_RATE, " + "      0.00 AS REQUESTED_RATE,NVL(T.LCL_TARIFF_RATE,0.00) AS APPROVED_RATE, " + "      CURR.CURRENCY_MST_PK, " + "      FRT.FREIGHT_ELEMENT_MST_PK " + " FROM TARIFF_TRN_SEA_FCL_LCL T, " + "     TARIFF_MAIN_SEA_TBL    TM, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      FREIGHT_ELEMENT_MST_TBL FRT " + " WHERE T.TARIFF_MAIN_SEA_FK = " + TariffPk + "  AND T.TARIFF_MAIN_SEA_FK=TM.TARIFF_MAIN_SEA_PK " + "  AND T.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK " + "  AND T.LCL_BASIS = UOM.DIMENTION_UNIT_MST_PK " + "  AND T.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "  AND FRT.FREIGHT_ELEMENT_ID <> 'BOF' " + "  AND T.LCL_BASIS > 0 " + "  AND TM.STATUS=1 " + "  AND      ( TO_DATE('" + Valid_From + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) " + "      OR   TO_DATE('" + Valid_To + "','" + dateFormat + "') " + "           BETWEEN T.VALID_FROM AND NVL(T.VALID_TO, NULL_DATE_FORMAT) ) " + "  AND (T.POL_GRP_FK, T.POD_GRP_FK, T.LCL_BASIS ) IN " + "      (" + strSearch + ") ";
				}

				dsMain.Tables.Add(objWF.GetDataTable(strSQL));
				dsMain.Tables[1].TableName = "Frt";

				dcColumn = new DataColumn("SRR_SUR_CHRG_SEA_PK", typeof(Int64));
				dsMain.Tables["Frt"].Columns.Add(dcColumn);

				DataColumn[] dcParent = null;
				DataColumn[] dcChild = null;
				DataRelation re = null;
				dcParent = new DataColumn[] {
					dsMain.Tables["Main"].Columns["POLPK"],
					dsMain.Tables["Main"].Columns["PODPK"],
					dsMain.Tables["Main"].Columns["CONT_BASIS"]
				};
				dcChild = new DataColumn[] {
					dsMain.Tables["Frt"].Columns["POL"],
					dsMain.Tables["Frt"].Columns["POD"],
					dsMain.Tables["Frt"].Columns["CONT_BASIS"]
				};
				re = new DataRelation("rl_Port", dcParent, dcChild);

				dsMain.Relations.Add(re);
			} catch (OracleException oraEx) {
				throw oraEx;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public void Fetch_Contract(long ContractPk, DataSet dsGrid, DataTable dtMain, bool IsLCL)
		{
			string strSql = null;
			WorkFlow objWF = new WorkFlow();
			try {
				strSql = "";
				strSql = "SELECT " + "      OPR.OPERATOR_ID, OPR.OPERATOR_NAME, OPR.OPERATOR_MST_PK, " + "      TARIFF.TARIFF_REF_NO,TARIFF.TARIFF_MAIN_SEA_PK, " + "      COMM_GRP.COMMODITY_GROUP_PK, SRR.SRR_SEA_PK,SRR.SRR_REF_NO, " + "      COMM.COMMODITY_NAME,COMM.COMMODITY_ID, COMM.COMMODITY_MST_PK, " + "      CUST.CUSTOMER_ID,CUST.CUSTOMER_NAME,CUST.CUSTOMER_MST_PK, " + "      LOC.LOCATION_ID,LOC.LOCATION_MST_PK,LOC.LOCATION_NAME, " + "      HDR.CONT_REF_NO,TO_CHAR(HDR.CONT_DATE,'" + dateFormat + "') AS CONT_DATE," + "      HDR.CARGO_TYPE,TO_CHAR(HDR.VALID_FROM,'" + dateFormat + "') AS VALID_FROM, " + "      TO_CHAR(HDR.VALID_TO,'" + dateFormat + "') AS VALID_TO, " + "      HDR.CONT_CLAUSE,HDR.CREDIT_PERIOD,HDR.VERSION_NO,HDR.STATUS,HDR.ACTIVE, " + "      DECODE(HDR.APP_STATUS,1,USR1.USER_ID,2,USR1.USER_ID) AS  USER_ID, " + "      DECODE(HDR.APP_STATUS,1,TO_CHAR(HDR.APP_DT,'" + dateFormat + "'),2,TO_CHAR(HDR.APP_DT,'" + dateFormat + "')) AS  APP_DATE, " + "     CURR.CURRENCY_MST_PK BASE_CURENCY_FK, CURR.CURRENCY_ID,HDR.REF_NR, HDR.APP_STATUS, HDR.Port_Group " + "    FROM " + "      CONT_CUST_SEA_TBL HDR, " + "      TARIFF_MAIN_SEA_TBL TARIFF, " + "      OPERATOR_MST_TBL OPR, " + "      COMMODITY_GROUP_MST_TBL COMM_GRP, " + "      COMMODITY_MST_TBL COMM, " + "      CUSTOMER_MST_TBL CUST, " + "      LOCATION_MST_TBL Loc, " + "      SRR_SEA_TBL SRR, " + "      USER_MST_TBL USR, " + "      USER_MST_TBL USR1, " + "      CURRENCY_TYPE_MST_TBL   CURR " + "WHERE HDR.TARIFF_MAIN_SEA_FK     = TARIFF.TARIFF_MAIN_SEA_PK (+)" + "AND   HDR.SRR_SEA_FK             = SRR.SRR_SEA_PK (+) " + "AND   HDR.OPERATOR_MST_FK        = OPR.OPERATOR_MST_PK(+) " + "AND   HDR.COMMODITY_GROUP_MST_FK = COMM_GRP.COMMODITY_GROUP_PK(+) " + "AND   HDR.COMMODITY_MST_FK       = COMM.COMMODITY_MST_PK (+) " + "AND   HDR.CUSTOMER_MST_FK        = CUST.CUSTOMER_MST_PK " + "AND   HDR.PYMT_LOCATION_MST_FK   = LOC.LOCATION_MST_PK (+) " + "AND   HDR.APP_BY                 = USR1.USER_MST_PK (+) " + "AND   HDR.CREATED_BY_FK          = USR.USER_MST_PK " + "AND   HDR.CREATED_BY_FK          = USR.USER_MST_PK " + "AND   CURR.CURRENCY_MST_PK(+)    = HDR.BASE_CURRENCY_FK" + "AND   TARIFF.STATUS    = 1 " + "AND   HDR.CONT_CUST_SEA_PK       = " + ContractPk;

				dtMain = objWF.GetDataTable(strSql);

				strSql = "";
				if (!IsLCL) {
					strSql = "SELECT " + "      Q.POLPK,Q.POLID,Q.PODPK,Q.PODID,Q.CONT_BASIS,Q.CURRENCY_ID,Q.CURRENT_BOF_RATE,Q.CURRENT_ALL_IN_RATE,Q.REQUESTED_BOF_RATE,Q.REQUESTED_ALL_IN_RATE,Q.APPROVED_BOF_RATE,Q.APPROVED_ALL_IN_RATE, " + "      Q.THL,Q.THD,Q.EXPECTED_BOXES,Q.VALID_FROM,Q.VALID_TO,Q.CONTAINER_TYPE_MST_FK,Q.CURRENCY_MST_FK,Q.SURCHARGE,Q.DEL,Q.CONT_CUST_TRN_SEA_PK FROM (SELECT  DISTINCT " + "      CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PLG.Port_Grp_Mst_Pk ELSE POL.Port_Mst_Pk END AS POLPK,CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PLG.PORT_GRP_ID ELSE POL.PORT_NAME END AS POLID, " + "      CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PDG.Port_Grp_Mst_Pk ELSE POD.Port_Mst_Pk END AS PODPK,CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PDG.PORT_GRP_ID ELSE POD.PORT_NAME END AS PODID, " + "      CONT.CONTAINER_TYPE_MST_ID CONT_BASIS,  " + "      CURR.CURRENCY_ID, CUST.CURRENT_BOF_RATE,  " + "      CUST.CURRENT_ALL_IN_RATE, CUST.REQUESTED_BOF_RATE, " + "      CUST.REQUESTED_ALL_IN_RATE, " + "      CUST.APPROVED_BOF_RATE, " + "      CUST.APPROVED_ALL_IN_RATE, " + "      (CASE WHEN (CUST.ON_THL_OR_THD IN (1,3)) THEN '1' ELSE '0' END) AS THL, " + "      (CASE WHEN (CUST.ON_THL_OR_THD IN (2,3)) THEN '1' ELSE '0' END) AS THD, " + "      CUST.EXPECTED_BOXES, TO_CHAR(CUST.VALID_FROM,'" + dateFormat + "') VALID_FROM, " + "      TO_CHAR(CUST.VALID_TO,'" + dateFormat + "') VALID_TO, CUST.CONTAINER_TYPE_MST_FK, " + "      CUST.CURRENCY_MST_FK, " + "      (CASE WHEN CUST.SUBJECT_TO_SURCHG_CHG =1 THEN '1' ELSE '0' END) AS SURCHARGE," + "      '0' DEL,CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN 0 ELSE CUST.CONT_CUST_TRN_SEA_PK END CONT_CUST_TRN_SEA_PK,CONT.PREFERENCES " + "FROM  CONT_CUST_SEA_TBL        CCMT," + "      CONT_CUST_TRN_SEA_TBL    CUST, " + "      PORT_MST_TBL             POL, " + "      PORT_MST_TBL             POD, " + "      CONTAINER_TYPE_MST_TBL   CONT, " + "      CURRENCY_TYPE_MST_TBL    CURR, " + "      PORT_GRP_MST_TBL         PLG, " + "      PORT_GRP_MST_TBL         PDG " + "WHERE CCMT.CONT_CUST_SEA_PK      = CUST.CONT_CUST_SEA_FK " + "AND   CUST.PORT_MST_POL_FK       = POL.PORT_MST_PK(+) " + "AND   CUST.PORT_MST_POD_FK       = POD.PORT_MST_PK(+) " + "AND   PLG.PORT_GRP_MST_PK(+)     = CUST.POL_GRP_FK " + "AND   PDG.PORT_GRP_MST_PK(+)     = CUST.POD_GRP_FK " + "AND   CUST.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK " + "AND   CUST.CURRENCY_MST_FK       = CURR.CURRENCY_MST_PK " + "AND   CUST.CONT_CUST_SEA_FK      = " + ContractPk + " ORDER BY CONT.PREFERENCES)Q";
				} else {
					strSql = "SELECT  DISTINCT " + "      CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PLG.Port_Grp_Mst_Pk ELSE POL.Port_Mst_Pk END AS POLPK,CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PLG.PORT_GRP_ID ELSE POL.PORT_NAME END AS POLID, " + "      CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PDG.Port_Grp_Mst_Pk ELSE POD.Port_Mst_Pk END AS PODPK,CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PDG.PORT_GRP_ID ELSE POD.PORT_NAME END AS PODID, " + "      UOM.DIMENTION_ID CONT_BASIS, " + "      CURR.CURRENCY_ID, CUST.CURRENT_BOF_RATE, " + "      CUST.CURRENT_ALL_IN_RATE, CUST.REQUESTED_BOF_RATE, " + "      CUST.REQUESTED_ALL_IN_RATE, " + "      CUST.APPROVED_BOF_RATE, " + "      CUST.APPROVED_ALL_IN_RATE, " + "      (CASE WHEN (CUST.ON_THL_OR_THD IN (1,3)) THEN '1' ELSE '0' END) AS THL, " + "      (CASE WHEN (CUST.ON_THL_OR_THD IN (2,3)) THEN '1' ELSE '0' END) AS THD, " + "      CUST.EXPECTED_VOLUME, TO_CHAR(CUST.VALID_FROM,'" + dateFormat + "') VALID_FROM, " + "      TO_CHAR(CUST.VALID_TO,'" + dateFormat + "') VALID_TO, CUST.LCL_BASIS, " + "      CUST.CURRENCY_MST_FK, " + "  (CASE WHEN CUST.SUBJECT_TO_SURCHG_CHG =1 THEN '1' ELSE '0' END) AS SURCHARGE, " + "      '0' DEL,CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN 0 ELSE CUST.CONT_CUST_TRN_SEA_PK END CONT_CUST_TRN_SEA_PK " + "FROM  CONT_CUST_SEA_TBL        CCMT," + "      CONT_CUST_TRN_SEA_TBL CUST, " + "      PORT_MST_TBL POL, " + "      PORT_MST_TBL POD, " + "      DIMENTION_UNIT_MST_TBL UOM, " + "      CURRENCY_TYPE_MST_TBL CURR, " + "      PORT_GRP_MST_TBL         PLG, " + "      PORT_GRP_MST_TBL         PDG " + "WHERE CCMT.CONT_CUST_SEA_PK      = CUST.CONT_CUST_SEA_FK " + "AND   CUST.PORT_MST_POL_FK       = POL.PORT_MST_PK(+) " + "AND   CUST.PORT_MST_POD_FK       = POD.PORT_MST_PK(+) " + "AND   PLG.PORT_GRP_MST_PK(+)     = CUST.POL_GRP_FK " + "AND   PDG.PORT_GRP_MST_PK(+)     = CUST.POD_GRP_FK " + "AND   CUST.LCL_BASIS             = UOM.DIMENTION_UNIT_MST_PK " + "AND   CUST.CURRENCY_MST_FK       = CURR.CURRENCY_MST_PK " + "AND   CUST.CONT_CUST_SEA_FK      = " + ContractPk;

				}
				dsGrid.Tables.Add(objWF.GetDataTable(strSql));
				dsGrid.Tables[0].TableName = "Main";
				strSql = "";
				if (!IsLCL) {
					strSql = " SELECT Q.POLPK,Q.PODPK,Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,Q.CHK,Q.CURRENCY_ID,Q.CONT_BASIS, " + " Q.CURR_SURCHARGE_AMT,Q.REQ_SURCHARGE_AMT,Q.APP_SURCHARGE_AMT,Q.CURRENCY_MST_FK,Q.FREIGHT_ELEMENT_MST_FK,Q.CONT_SUR_CHRG_SEA_PK FROM( " + " SELECT DISTINCT * FROM (SELECT CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PLG.PORT_GRP_MST_PK ELSE POL.PORT_MST_PK END POLPK, " + "      CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PDG.PORT_GRP_MST_PK ELSE POD.PORT_MST_PK END PODPK, " + "      FRT.FREIGHT_ELEMENT_ID,DECODE(SUR.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, TO_CHAR(SUR.CHECK_FOR_ALL_IN_RT) AS CHK, " + "      CURR.CURRENCY_ID, CONT.CONTAINER_TYPE_MST_ID AS CONT_BASIS, " + "      SUR.CURR_SURCHARGE_AMT, SUR.REQ_SURCHARGE_AMT,SUR.APP_SURCHARGE_AMT," + "      SUR.CURRENCY_MST_FK, " + "      SUR.FREIGHT_ELEMENT_MST_FK,CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN 0 ELSE SUR.CONT_SUR_CHRG_SEA_PK END CONT_SUR_CHRG_SEA_PK,FRT.PREFERENCE,CONT.PREFERENCES " + "FROM CONT_CUST_SEA_TBL         CCMT, " + "      CONT_CUST_TRN_SEA_TBL    CUST,  " + "      CONT_SUR_CHRG_SEA_TBL    SUR, " + "      PORT_MST_TBL             POL, " + "      PORT_MST_TBL             POD, " + "      CURRENCY_TYPE_MST_TBL    CURR, " + "      CONTAINER_TYPE_MST_TBL   CONT, " + "      FREIGHT_ELEMENT_MST_TBL  FRT, " + "      PORT_GRP_MST_TBL         PLG, " + "      PORT_GRP_MST_TBL         PDG " + "WHERE CCMT.CONT_CUST_SEA_PK = CUST.CONT_CUST_SEA_FK " + "AND   CUST.CONT_CUST_TRN_SEA_PK  = SUR.CONT_CUST_TRN_SEA_FK " + "AND   CUST.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK " + "AND   CUST.PORT_MST_POL_FK       = POL.PORT_MST_PK(+) " + "AND   CUST.PORT_MST_POD_FK       = POD.PORT_MST_PK(+) " + "AND   PLG.PORT_GRP_MST_PK(+)     = CUST.POL_GRP_FK " + "AND   PDG.PORT_GRP_MST_PK(+)     = CUST.POD_GRP_FK " + "AND   SUR.CURRENCY_MST_FK        = CURR.CURRENCY_MST_PK " + "AND   SUR.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "AND   CUST.CONT_CUST_SEA_FK      = " + ContractPk + " ) ORDER BY PREFERENCES, PREFERENCE) Q ";
				} else {
					strSql = " SELECT Q.POLPK," + "      Q.PODPK," + "        Q.FREIGHT_ELEMENT_ID," + "        Q.CHARGE_BASIS," + "        Q.CHK," + "        Q.CURRENCY_ID," + "        Q.CONT_BASIS," + "        Q.CURR_SURCHARGE_AMT," + "        Q.REQ_SURCHARGE_AMT," + "        Q.APP_SURCHARGE_AMT," + "        Q.CURRENCY_MST_FK," + "        Q.FREIGHT_ELEMENT_MST_FK," + "        CONT_SUR_CHRG_SEA_PK" + " FROM (SELECT DISTINCT CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PLG.PORT_GRP_MST_PK ELSE POL.PORT_MST_PK END POLPK, " + "      CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN PDG.PORT_GRP_MST_PK ELSE POD.PORT_MST_PK END PODPK, " + "      FRT.FREIGHT_ELEMENT_ID,DECODE(SUR.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, TO_CHAR(SUR.CHECK_FOR_ALL_IN_RT) AS CHK, " + "      CURR.CURRENCY_ID, UOM.DIMENTION_ID AS CONT_BASIS, " + "      SUR.CURR_SURCHARGE_AMT, SUR.REQ_SURCHARGE_AMT,SUR.APP_SURCHARGE_AMT, " + "      SUR.CURRENCY_MST_FK, " + "      SUR.FREIGHT_ELEMENT_MST_FK,CASE WHEN NVL(CCMT.PORT_GROUP, 0) <> 0 THEN 0 ELSE SUR.CONT_SUR_CHRG_SEA_PK END CONT_SUR_CHRG_SEA_PK , FRT.PREFERENCE " + "FROM  CONT_CUST_SEA_TBL        CCMT, " + "      CONT_CUST_TRN_SEA_TBL    CUST, " + "      CONT_SUR_CHRG_SEA_TBL    SUR, " + "      PORT_MST_TBL             POL, " + "      PORT_MST_TBL             POD, " + "      CURRENCY_TYPE_MST_TBL    CURR, " + "      DIMENTION_UNIT_MST_TBL   UOM, " + "      FREIGHT_ELEMENT_MST_TBL  FRT, " + "      PORT_GRP_MST_TBL         PLG, " + "      PORT_GRP_MST_TBL         PDG  " + "WHERE CCMT.CONT_CUST_SEA_PK      = CUST.CONT_CUST_SEA_FK " + "AND   CUST.CONT_CUST_TRN_SEA_PK  = SUR.CONT_CUST_TRN_SEA_FK " + "AND   CUST.LCL_BASIS             = UOM.DIMENTION_UNIT_MST_PK " + "AND   CUST.PORT_MST_POL_FK       = POL.PORT_MST_PK(+) " + "AND   CUST.PORT_MST_POD_FK       = POD.PORT_MST_PK(+) " + "AND   PLG.PORT_GRP_MST_PK(+)     = CUST.POL_GRP_FK " + "AND   PDG.PORT_GRP_MST_PK(+)     = CUST.POD_GRP_FK " + "AND   SUR.CURRENCY_MST_FK        = CURR.CURRENCY_MST_PK " + "AND   SUR.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK " + "AND   CUST.CONT_CUST_SEA_FK      = " + ContractPk + " ) Q  ORDER BY Q.PREFERENCE ";
				}
				dsGrid.Tables.Add(objWF.GetDataTable(strSql));
				dsGrid.Tables[1].TableName = "Frt";

				DataColumn[] dcParent = null;
				DataColumn[] dcChild = null;
				DataRelation re = null;

				dcParent = new DataColumn[] {
					dsGrid.Tables["Main"].Columns["POLPK"],
					dsGrid.Tables["Main"].Columns["PODPK"],
					dsGrid.Tables["Main"].Columns["CONT_BASIS"]
				};

				dcChild = new DataColumn[] {
					dsGrid.Tables["Frt"].Columns["POLPK"],
					dsGrid.Tables["Frt"].Columns["PODPK"],
					dsGrid.Tables["Frt"].Columns["CONT_BASIS"]
				};

				re = new DataRelation("rl_Port", dcParent, dcChild);
				dsGrid.Relations.Add(re);

			} catch (OracleException orExc) {
				throw orExc;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Save"
		public ArrayList SaveHDR(DataSet dsMain, object txtSRRRefNo, long nLocationId, long nEmpId, string Mode, bool IsLcl, string RefNr, string ContrctDt = "", Int16 Restricted = 0, string polid = "",
		string podid = "")
		{
			string ContRefNo = null;
			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;
			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();
			arrMessage.Clear();
			objWK.MyCommand.Transaction = TRAN;
			bool IsUpdate = false;
			try {
				if (string.IsNullOrEmpty(txtSRRRefNo.ToString())) {
					ContRefNo = GenerateContractNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, polid, podid);
					if (ContRefNo == "Protocol Not Defined.") {
						arrMessage.Add("Protocol Not Defined.");
						return arrMessage;
					}
				} else {
					ContRefNo = txtSRRRefNo.ToString();
				}

				objWK.MyCommand.Parameters.Clear();

				var _with1 = objWK.MyCommand;

				if (Mode == "NEW" | Mode == "FETCHED") {
					_with1.CommandType = CommandType.StoredProcedure;
					_with1.CommandText = objWK.MyUserName + ".CONT_CUST_SEA_TBL_PKG.CONT_CUST_SEA_TBL_INS";

					_with1.Parameters.Add("CONT_REF_NO_IN", ContRefNo).Direction = ParameterDirection.Input;

					_with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					_with1.Parameters.Add("REF_NR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;

					if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["TARIFF_MAIN_SEA_FK"].ToString())) {
						_with1.Parameters.Add("TARIFF_MAIN_SEA_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with1.Parameters.Add("TARIFF_MAIN_SEA_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["TARIFF_MAIN_SEA_FK"])).Direction = ParameterDirection.Input;
					}
					if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["BASE_CURENCY_FK"].ToString())) {
						_with1.Parameters.Add("BASE_CURRENCY_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with1.Parameters.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["BASE_CURENCY_FK"])).Direction = ParameterDirection.Input;
					}
				} else {
					_with1.CommandType = CommandType.StoredProcedure;
					_with1.CommandText = objWK.MyUserName + ".CONT_CUST_SEA_TBL_PKG.CONT_CUST_SEA_TBL_UPD";

					_with1.Parameters.Add("CONT_CUST_SEA_PK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["CONT_CUST_SEA_PK"])).Direction = ParameterDirection.Input;

					_with1.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

					_with1.Parameters.Add("VERSION_NO_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["VERSION_NO"])).Direction = ParameterDirection.Input;

				}

				if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["OPERATOR_MST_FK"].ToString()) | Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["OPERATOR_MST_FK"].ToString()) == 0) {
					_with1.Parameters.Add("OPERATOR_MST_FK_IN", "").Direction = ParameterDirection.Input;
				} else {
					_with1.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;
				}

				_with1.Parameters.Add("CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;

				if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["SRR_SEA_FK"].ToString())) {
					_with1.Parameters.Add("SRR_SEA_FK_IN", "").Direction = ParameterDirection.Input;

				} else {
					_with1.Parameters.Add("SRR_SEA_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["SRR_SEA_FK"])).Direction = ParameterDirection.Input;
				}

				_with1.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;

				_with1.Parameters.Add("APP_STATUS_IN", Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["APP_STATUS"])).Direction = ParameterDirection.Input;

				_with1.Parameters.Add("VALID_FROM_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["VALID_FROM"])).Direction = ParameterDirection.Input;

				if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["VALID_TO"].ToString())) {
					_with1.Parameters.Add("VALID_TO_IN", "").Direction = ParameterDirection.Input;
				} else {
					_with1.Parameters.Add("VALID_TO_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["VALID_TO"])).Direction = ParameterDirection.Input;
				}

				_with1.Parameters.Add("STRCONDITION", Convert.ToString(dsMain.Tables["Master"].Rows[0]["STRCONDITION"])).Direction = ParameterDirection.Input;

				_with1.Parameters.Add("COMMODITY_GROUP_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["COMMODITY_GROUP_MST_FK"])).Direction = ParameterDirection.Input;

				if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["COMMODITY_MST_FK"].ToString())) {
					_with1.Parameters.Add("COMMODITY_MST_FK_IN", "").Direction = ParameterDirection.Input;
				} else {
					_with1.Parameters.Add("COMMODITY_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["COMMODITY_MST_FK"])).Direction = ParameterDirection.Input;
				}

				if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["CONT_CLAUSE"].ToString())) {
					_with1.Parameters.Add("CONT_CLAUSE_IN", "").Direction = ParameterDirection.Input;
				} else {
					_with1.Parameters.Add("CONT_CLAUSE_IN", Convert.ToString(dsMain.Tables["Master"].Rows[0]["CONT_CLAUSE"])).Direction = ParameterDirection.Input;
				}

				if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["PYMT_LOCATION_MST_FK"].ToString())) {
					_with1.Parameters.Add("PYMT_LOCATION_MST_FK_IN", "").Direction = ParameterDirection.Input;
				} else {
					_with1.Parameters.Add("PYMT_LOCATION_MST_FK_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["PYMT_LOCATION_MST_FK"])).Direction = ParameterDirection.Input;
				}

				if (string.IsNullOrEmpty(dsMain.Tables["Master"].Rows[0]["CREDIT_PERIOD"].ToString())) {
					_with1.Parameters.Add("CREDIT_PERIOD_IN", "").Direction = ParameterDirection.Input;
				} else {
					_with1.Parameters.Add("CREDIT_PERIOD_IN", Convert.ToInt64(dsMain.Tables["Master"].Rows[0]["CREDIT_PERIOD"])).Direction = ParameterDirection.Input;
				}

				_with1.Parameters.Add("STATUS_IN", Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["STATUS"])).Direction = ParameterDirection.Input;

				_with1.Parameters.Add("ACTIVE_IN", Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["ACTIVE"])).Direction = ParameterDirection.Input;

				_with1.Parameters.Add("PORT_GROUP_IN", Convert.ToInt32(dsMain.Tables["Master"].Rows[0]["PORT_GROUP"])).Direction = ParameterDirection.Input;

				_with1.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("RESTRICTED_IN", Restricted).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;

				_with1.ExecuteNonQuery();

				if (string.Compare(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString(), "Contract")>0 | string.Compare(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString(), "modified")>0) {
					arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));

					if (!IsUpdate) {
						RollbackProtocolKey("CUSTOMER CONTRACT OPERATOR", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), ContRefNo, System.DateTime.Now);
					}

					TRAN.Rollback();
					arrMessage.Add("Customer Contract already exists.");
					return arrMessage;
				} else {
					_PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
				}

				arrMessage = SaveSrrTRN(dsMain, objWK, IsLcl);
				//'
				string CurrFKs = "0";
				Quantum_QFOR.cls_Operator_Contract objContract = new Quantum_QFOR.cls_Operator_Contract();
				for (int nRowCnt = 0; nRowCnt <= dsMain.Tables["Surcharge"].Rows.Count - 1; nRowCnt++) {
					CurrFKs += "," + dsMain.Tables["Surcharge"].Rows[nRowCnt]["CURRENCY_MST_FK"];
				}
				objContract.UpdateTempExRate(_PkValue, objWK, CurrFKs, Convert.ToDateTime(ContrctDt), "CUSTCONTRACTSEA");
				//'
				if (arrMessage.Count > 0) {
					if (string.Compare(arrMessage[0].ToString(), "saved") >0) {
						arrMessage.Add("All data saved successfully");
						TRAN.Commit();
						txtSRRRefNo = ContRefNo;
						return arrMessage;

					} else {
						if (!IsUpdate) {
							RollbackProtocolKey("CUSTOMER CONTRACT OPERATOR", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), ContRefNo, System.DateTime.Now);
						}

						TRAN.Rollback();
						return arrMessage;
					}
				}
			} catch (OracleException OraExp) {
				throw OraExp;

			} catch (Exception ex) {
				if (!IsUpdate) {
					RollbackProtocolKey("CUSTOMER CONTRACT OPERATOR", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), ContRefNo, System.DateTime.Now);
				}

				TRAN.Rollback();
				throw ex;
			} finally {
				objWK.MyConnection.Close();
			}
            return new ArrayList();
		}

		private ArrayList SaveSrrTRN(DataSet dsMain, WorkFlow objWK, bool IsLCL)
		{
			Int32 nTransactionRowCnt = default(Int32);
			DataTable dtTransaction = new DataTable();
			long nTransactionPk = 0;
			dtTransaction = dsMain.Tables["Transaction"];
			bool IsUpdate = false;
			string Cont_BasisPk = null;
			if (IsLCL) {
				Cont_BasisPk = "LCL_BASIS";
			} else {
				Cont_BasisPk = "CONTAINER_TYPE_MST_FK";
			}
			try {

				for (nTransactionRowCnt = 0; nTransactionRowCnt <= dsMain.Tables["Transaction"].Rows.Count - 1; nTransactionRowCnt++) {
					objWK.MyCommand.Parameters.Clear();
					arrMessage.Clear();
					nTransactionPk = 0;

					var _with2 = objWK.MyCommand;
					if (Convert.ToInt32(dtTransaction.Rows[nTransactionRowCnt]["DEL"]) < 1) {

						if (Convert.ToInt32(dsMain.Tables["Transaction"].Rows[nTransactionRowCnt]["CONT_CUST_TRN_SEA_PK"]) <= 0) {
							IsUpdate = false;
							_with2.CommandType = CommandType.StoredProcedure;
							_with2.CommandText = objWK.MyUserName + ".CONT_CUST_SEA_TBL_PKG.CONT_CUST_TRN_SEA_TBL_INS";

							_with2.Parameters.Add("CONT_CUST_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;

							_with2.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;

							_with2.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;

							_with2.Parameters.Add("POL_GRP_FK_IN", (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["POL_GRP_FK"].ToString()) ? "" : dtTransaction.Rows[nTransactionRowCnt]["POL_GRP_FK"])).Direction = ParameterDirection.Input;

							_with2.Parameters.Add("POD_GRP_FK_IN", (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["POD_GRP_FK"].ToString()) ? "" : dtTransaction.Rows[nTransactionRowCnt]["POD_GRP_FK"])).Direction = ParameterDirection.Input;

							_with2.Parameters.Add("TARIFF_GRP_FK_IN", (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["TARIFF_GRP_MST_FK"].ToString()) ? "" : dtTransaction.Rows[nTransactionRowCnt]["TARIFF_GRP_MST_FK"])).Direction = ParameterDirection.Input;

							if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["CONTAINER_TYPE_MST_FK"].ToString())) {
								_with2.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with2.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["CONTAINER_TYPE_MST_FK"])).Direction = ParameterDirection.Input;
							}

							if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["LCL_BASIS"].ToString())) {
								_with2.Parameters.Add("LCL_BASIS_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with2.Parameters.Add("LCL_BASIS_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["LCL_BASIS"])).Direction = ParameterDirection.Input;
							}

							_with2.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["CURRENCY_MST_FK"])).Direction = ParameterDirection.Input;

							_with2.Parameters.Add("CURRENT_BOF_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["CURRENT_BOF_RATE"])).Direction = ParameterDirection.Input;

							_with2.Parameters.Add("CURRENT_ALL_IN_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["CURRENT_ALL_IN_RATE"])).Direction = ParameterDirection.Input;

							if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_BOF_RATE"].ToString())) {
								_with2.Parameters.Add("REQUESTED_BOF_RATE_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with2.Parameters.Add("REQUESTED_BOF_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_BOF_RATE"])).Direction = ParameterDirection.Input;
							}

							if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_ALL_IN_RATE"].ToString())) {
								_with2.Parameters.Add("REQUESTED_ALL_IN_RATE_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with2.Parameters.Add("REQUESTED_ALL_IN_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["REQUESTED_ALL_IN_RATE"])).Direction = ParameterDirection.Input;
							}

						} else {
							IsUpdate = true;

							_with2.CommandType = CommandType.StoredProcedure;
							_with2.CommandText = objWK.MyUserName + ".CONT_CUST_SEA_TBL_PKG.CONT_CUST_TRN_SEA_TBL_UPD";

							_with2.Parameters.Add("CONT_CUST_TRN_SEA_PK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["CONT_CUST_TRN_SEA_PK"])).Direction = ParameterDirection.Input;
						}
						_with2.Parameters.Add("VALID_FROM_IN", Convert.ToString(dtTransaction.Rows[nTransactionRowCnt]["VALID_FROM"])).Direction = ParameterDirection.Input;

						_with2.Parameters.Add("VALID_TO_IN", Convert.ToString(dtTransaction.Rows[nTransactionRowCnt]["VALID_TO"])).Direction = ParameterDirection.Input;

						if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["APPROVED_BOF_RATE"].ToString())) {
							_with2.Parameters.Add("APPROVED_BOF_RATE_IN", "").Direction = ParameterDirection.Input;
						} else {
							_with2.Parameters.Add("APPROVED_BOF_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["APPROVED_BOF_RATE"])).Direction = ParameterDirection.Input;
						}

						if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["APPROVED_ALL_IN_RATE"].ToString())) {
							_with2.Parameters.Add("APPROVED_ALL_IN_RATE_IN", "").Direction = ParameterDirection.Input;
						} else {
							_with2.Parameters.Add("APPROVED_ALL_IN_RATE_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["APPROVED_ALL_IN_RATE"])).Direction = ParameterDirection.Input;
						}

						_with2.Parameters.Add("ON_THL_OR_THD_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["ON_THL_OR_THD"])).Direction = ParameterDirection.Input;

						if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_VOLUME"].ToString())) {
							_with2.Parameters.Add("EXPECTED_VOLUME_IN", "").Direction = ParameterDirection.Input;
						} else {
							_with2.Parameters.Add("EXPECTED_VOLUME_IN", Convert.ToDouble(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_VOLUME"])).Direction = ParameterDirection.Input;
						}

						if (string.IsNullOrEmpty(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_BOXES"].ToString())) {
							_with2.Parameters.Add("EXPECTED_BOXES_IN", "").Direction = ParameterDirection.Input;
						} else {
							_with2.Parameters.Add("EXPECTED_BOXES_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["EXPECTED_BOXES"])).Direction = ParameterDirection.Input;
						}

						_with2.Parameters.Add("SUBJECT_TO_SURCHRG_CHG_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["SUBJECT_TO_SURCHRG_CHG"])).Direction = ParameterDirection.Input;

						//OPERATOR_SPEC_SURCHRG_IN   '("OPERATOR_SPEC_SURCHRG")
						_with2.Parameters.Add("OPERATOR_SPEC_SURCHRG_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["OPERATOR_SPEC_SURCHRG"])).Direction = ParameterDirection.Input;

						_with2.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

						_with2.ExecuteNonQuery();

						nTransactionPk = Convert.ToInt64(_with2.Parameters["RETURN_VALUE"].Value);
						arrMessage = SaveSrrSurcharge(dsMain, objWK, nTransactionPk, IsUpdate, Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["PORT_MST_POL_FK"]), Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["PORT_MST_POD_FK"]), Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt][Cont_BasisPk]));

						if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0)) {
							return arrMessage;
						}
					} else {

						if (Convert.ToInt32(dsMain.Tables["Transaction"].Rows[nTransactionRowCnt]["CONT_CUST_TRN_SEA_PK"]) > 0) {
							IsUpdate = false;

							_with2.CommandType = CommandType.StoredProcedure;
							_with2.CommandText = objWK.MyUserName + ".CONT_CUST_SEA_TBL_PKG.CONT_TRN_SEA_TBL_DEL";

							_with2.Parameters.Add("CONT_CUST_TRN_SEA_PK_IN", Convert.ToInt64(dtTransaction.Rows[nTransactionRowCnt]["CONT_CUST_TRN_SEA_PK"])).Direction = ParameterDirection.Input;

							_with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

							_with2.ExecuteNonQuery();
							if (string.Compare(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString(), "deleted") > 0) {
								arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
							}
						}
					}
				}
				arrMessage.Clear();
				arrMessage.Add("All data saved successfully");
				return arrMessage;

			} catch (OracleException oraexp) {
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				return arrMessage;
			}
		}

		private ArrayList SaveSrrSurcharge(DataSet dsMain, WorkFlow objWK, long TransactionPkValue, bool IsUpdate, long PolPk, long PodPk, long CONT_BASIS)
		{
			Int32 nSurchargeRowCnt = default(Int32);
			DataView dv_Surcharge = new DataView();

			dv_Surcharge = getDataView(dsMain.Tables["Surcharge"], PolPk, PodPk, CONT_BASIS);

			arrMessage.Clear();

			try {
				for (nSurchargeRowCnt = 0; nSurchargeRowCnt <= dv_Surcharge.Table.Rows.Count - 1; nSurchargeRowCnt++) {
					objWK.MyCommand.Parameters.Clear();
					var _with3 = objWK.MyCommand;
					if (!IsUpdate) {
						_with3.CommandType = CommandType.StoredProcedure;
						_with3.CommandText = objWK.MyUserName + ".CONT_CUST_SEA_TBL_PKG.CONT_SUR_CHRG_SEA_TBL_INS";

						_with3.Parameters.Add("CONT_CUST_TRN_SEA_FK_IN", TransactionPkValue).Direction = ParameterDirection.Input;

						_with3.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", Convert.ToInt64(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["FREIGHT_ELEMENT_MST_FK"])).Direction = ParameterDirection.Input;
						_with3.Parameters.Add("CHARGE_BASIS_IN", Convert.ToInt64(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CHARGE_BASIS"])).Direction = ParameterDirection.Input;

						_with3.Parameters.Add("CURR_SURCHARGE_AMT_IN", Convert.ToDouble(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CURR_SURCHARGE_AMT"])).Direction = ParameterDirection.Input;

						_with3.Parameters.Add("REQ_SURCHARGE_AMT_IN", Convert.ToDouble(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["REQ_SURCHARGE_AMT"])).Direction = ParameterDirection.Input;

						_with3.Parameters.Add("SURCHARGE_IN", (dv_Surcharge.Table.Rows[nSurchargeRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;


					} else {
						_with3.CommandType = CommandType.StoredProcedure;
						_with3.CommandText = objWK.MyUserName + ".CONT_CUST_SEA_TBL_PKG.CONT_SUR_CHRG_SEA_TBL_UPD";

						_with3.Parameters.Add("CONT_SUR_CHRG_SEA_PK_IN", Convert.ToDouble(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CONT_SUR_CHRG_SEA_PK"])).Direction = ParameterDirection.Input;
						//'ADDED FOR SURCHARGE
						_with3.Parameters.Add("SURCHARGE_IN", (dv_Surcharge.Table.Rows[nSurchargeRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;

					}
					//************************************COMMON**********************************************

					//APP_SURCHARGE_AMT_IN           '("APP_SURCHARGE_AMT")
					_with3.Parameters.Add("APP_SURCHARGE_AMT_IN", Convert.ToDouble(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["APP_SURCHARGE_AMT"])).Direction = ParameterDirection.Input;

					//CHECK_FOR_ALL_IN_RT_IN         '("CHECK_FOR_ALL_IN_RT")
					_with3.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", Convert.ToInt64(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CHECK_FOR_ALL_IN_RT"])).Direction = ParameterDirection.Input;


					//CURRENCY_MST_FK_IN             '("CURRENCY_MST_FK")
					_with3.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(dv_Surcharge.Table.Rows[nSurchargeRowCnt]["CURRENCY_MST_FK"])).Direction = ParameterDirection.Input;

					_with3.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

					_with3.ExecuteNonQuery();
				}

				arrMessage.Add("All data saved successfully");
				return arrMessage;

			} catch (OracleException oraexp) {
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				return arrMessage;
			}
		}

		//This function generates the SRR Referrence no. as per the protocol saved by the user.
		public string GenerateContractNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK, string POLID = "", string PODID = "")
		{
			string functionReturnValue = null;
			try {
				functionReturnValue = GenerateProtocolKey("CUSTOMER CONTRACT OPERATOR", nLocationId, nEmployeeId, DateTime.Now,"","" , POLID, nCreatedBy, objWK, "",
				PODID);
				return functionReturnValue;
			//Manjunath  PTS ID:Sep-02  17/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
			return functionReturnValue;
		}

		private DataView getDataView(DataTable dtSurcharge, long POLPK, long PODPK, long CONT_BASIS)
		{
			DataTable dstemp = new DataTable();
			DataRow dr = null;
			Int32 nRowCnt = default(Int32);
			Int32 nColCnt = default(Int32);
			string Cont_BasisPk = null;
			try {
				dstemp = dtSurcharge.Clone();
				for (nRowCnt = 0; nRowCnt <= dtSurcharge.Rows.Count - 1; nRowCnt++) {
					if (POLPK == Convert.ToInt64(dtSurcharge.Rows[nRowCnt]["PORT_MST_POL_FK"]) & PODPK == Convert.ToInt64(dtSurcharge.Rows[nRowCnt]["PORT_MST_POD_FK"]) & CONT_BASIS == Convert.ToInt64(dtSurcharge.Rows[nRowCnt]["CONT_BASIS"])) {
						dr = dstemp.NewRow();
						for (nColCnt = 0; nColCnt <= dtSurcharge.Columns.Count - 1; nColCnt++) {
							dr[nColCnt] = dtSurcharge.Rows[nRowCnt][nColCnt];
						}
						dstemp.Rows.Add(dr);
					}
				}
				return dstemp.DefaultView;
			//Manjunath  PTS ID:Sep-02  17/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Logged In Location Ports"
		public DataSet Fetch_LoginPort(long Log_Loc, long Cust_Fk)
		{
			WorkFlow objWF = new WorkFlow();
			string strSQL = null;
			strSQL = "";
			strSQL += " SELECT PMT.PORT_MST_PK,PMT.PORT_ID,PMT.PORT_NAME  ";
			strSQL += " FROM PORT_MST_TBL PMT,CUSTOMER_MST_TBL CMT,CUSTOMER_CONTACT_DTLS CDL ";
			strSQL += " WHERE PMT.PORT_MST_PK IN (SELECT LWPT.PORT_MST_FK FROM LOCATION_WORKING_PORTS_TRN LWPT WHERE LWPT.LOCATION_MST_FK=" + Log_Loc + " ) ";
			strSQL += "  AND CMT.CUSTOMER_MST_PK = " + Cust_Fk;
			strSQL += "  AND CDL.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ";
			strSQL += "  AND CDL.ADM_LOCATION_MST_FK =PMT.LOCATION_MST_FK ";
			strSQL += "  AND PMT.BUSINESS_TYPE = 2 ";
			strSQL += "   AND PMT.ACTIVE_FLAG = 1  ";
			strSQL += "  AND ROWNUM =1 ";

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

		#region "Fetch Header Details"
		public DataSet FetchHeader(int CustContractPk)
		{
			WorkFlow ObjWk = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT CMT.CUSTOMER_NAME,");
			sb.Append("       CCD.ADM_ADDRESS_1,");
			sb.Append("       CCD.ADM_ADDRESS_2,");
			sb.Append("       CCD.ADM_ADDRESS_3,");
			sb.Append("       CCD.ADM_ZIP_CODE,");
			sb.Append("       CMT.COUNTRY_NAME,");
			sb.Append("       OMT.OPERATOR_NAME,");
			sb.Append("       'SEA' BIZ_TYPE,");
			sb.Append("       DECODE(CCST.CARGO_TYPE,1,'FCL',2,'LCL') CARGO_TYPE,");
			sb.Append("       CCST.CONT_REF_NO,");
			sb.Append("       CCST.CONT_DATE,");
			sb.Append("       CCST.VALID_FROM,");
			sb.Append("       CCST.VALID_TO,");
			sb.Append("       DECODE(CMT.BUSINESS_TYPE,3,'Both',1,'Air',2,'Sea')BUSINESS_TYPE,");
			sb.Append("       CT.COMMODITY_NAME,");
			sb.Append("       CGMT.COMMODITY_GROUP_DESC,");
			sb.Append("       CCST.CREDIT_PERIOD,");
			sb.Append("CASE");
			sb.Append("         WHEN CCST.STATUS <> 2 THEN");
			sb.Append("          DECODE(CCST.APP_STATUS,");
			sb.Append("                 0,");
			sb.Append("                 'Requested',");
			sb.Append("                 1,");
			sb.Append("                 'Approved',");
			sb.Append("                 2,");
			sb.Append("                 'Rejected')");
			sb.Append("         ELSE");
			sb.Append("          DECODE(CCST.STATUS,2,'Customer Approved')");
			sb.Append("       END STATUS,");
			sb.Append("       PLMT.LOCATION_NAME,");
			sb.Append("       TMS.TARIFF_REF_NO,");
			sb.Append("       DECODE(CCST.STATUS,0,CUMT.USER_NAME,1,LUMT.USER_NAME,2,LUMT.USER_NAME) USER_ID, ");
			sb.Append("       LUMT.USER_NAME APPD_BY,");
			sb.Append("       CCST.LAST_MODIFIED_DT APPD_DT,");
			sb.Append("       CCST.CONT_CLAUSE ");
			sb.Append("  FROM CONT_CUST_SEA_TBL     CCST,");
			sb.Append("       CUSTOMER_MST_TBL      CMT,");
			sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
			sb.Append("       LOCATION_MST_TBL      LMT,");
			sb.Append("       COUNTRY_MST_TBL       CMT,");
			sb.Append("       LOCATION_MST_TBL      PLMT,");
			sb.Append("       OPERATOR_MST_TBL      OMT,");
			sb.Append("       COMMODITY_MST_TBL     CT,");
			sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
			sb.Append("       USER_MST_TBL            CUMT,");
			sb.Append("       USER_MST_TBL            LUMT,");
			sb.Append("       TARIFF_MAIN_SEA_TBL   TMS");
			sb.Append(" WHERE CCST.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
			sb.Append("   AND CCD.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
			sb.Append("   AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
			sb.Append("   AND LMT.COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
			sb.Append("   AND CCST.OPERATOR_MST_FK = OMT.OPERATOR_MST_PK(+)");
			sb.Append("   AND CCST.COMMODITY_MST_FK = CT.COMMODITY_MST_PK(+)");
			sb.Append("   AND CCST.COMMODITY_GROUP_MST_FK = CGMT.COMMODITY_GROUP_PK(+)");
			sb.Append("   AND CCST.TARIFF_MAIN_SEA_FK=TMS.TARIFF_MAIN_SEA_PK");
			sb.Append("   AND CCST.CREATED_BY_FK = CUMT.USER_MST_PK(+)");
			sb.Append("   AND PLMT.LOCATION_MST_PK(+) = CCST.PYMT_LOCATION_MST_FK");
			sb.Append("   AND CCST.LAST_MODIFIED_BY_FK = LUMT.USER_MST_PK(+)");
			sb.Append("   AND CCST.CONT_CUST_SEA_PK =" + CustContractPk);
			try {
				return ObjWk.GetDataSet(sb.ToString());
			//Manjunath  PTS ID:Sep-02  17/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
		#region "Fetch FreightDetails"
		public DataSet FetchFreightDetails(string CustContractSeaPk)
		{
			WorkFlow objWK = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT CCTS.CONT_CUST_TRN_SEA_PK,");
			sb.Append("       POL.PORT_NAME PORT_ID,");
			sb.Append("       POD.PORT_NAME PORT_ID,");
			sb.Append("       CCTS.VALID_FROM,");
			sb.Append("       CCTS.VALID_TO,");
			sb.Append("       CASE WHEN CCST.CARGO_TYPE =1 THEN");
			sb.Append("       CTMT.CONTAINER_TYPE_MST_ID");
			sb.Append("       ELSE");
			sb.Append("         DUMT.DIMENTION_ID");
			sb.Append("       END CONTAINER_TYPE_MST_ID,");
			sb.Append("       CTM.CURRENCY_ID,");
			sb.Append("       CCTS.CURRENT_BOF_RATE,");
			sb.Append("       CCTS.CURRENT_ALL_IN_RATE,");
			sb.Append("       CCTS.EXPECTED_VOLUME,");
			sb.Append("       CASE");
			sb.Append("         WHEN CCTS.EXPECTED_VOLUME IS NULL THEN");
			sb.Append("          CCTS.EXPECTED_BOXES");
			sb.Append("         ELSE");
			sb.Append("          CCTS.EXPECTED_VOLUME");
			sb.Append("       END EXPECTED_VOLUME,");
			sb.Append("       CCTS.APPROVED_BOF_RATE,");
			sb.Append("       CCTS.APPROVED_ALL_IN_RATE,");
			sb.Append("       FEMT.FREIGHT_ELEMENT_ID,");
			sb.Append("       FEMT.FREIGHT_ELEMENT_NAME,");
			sb.Append("       CTM.CURRENCY_ID,");
			sb.Append("       CSCS.CURR_SURCHARGE_AMT TARIFF_RATE,");
			sb.Append("       CSCS.APP_SURCHARGE_AMT APP_RATE,");
			sb.Append("       DECODE(FEMT.CHARGE_BASIS, 1, '%', 2, 'Flat', 3, 'Kgs', 4, 'Unit') CHARGE_BASIS");
			sb.Append("  FROM CONT_CUST_SEA_TBL       CCST,");
			sb.Append("       CONT_CUST_TRN_SEA_TBL   CCTS,");
			sb.Append("       CONT_SUR_CHRG_SEA_TBL   CSCS,");
			sb.Append("       PORT_MST_TBL            POL,");
			sb.Append("       PORT_MST_TBL            POD,");
			sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
			sb.Append("       DIMENTION_UNIT_MST_TBL DUMT,");
			sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
			sb.Append("       CURRENCY_TYPE_MST_TBL   CTM");
			sb.Append(" WHERE CCTS.CONT_CUST_SEA_FK = CCST.CONT_CUST_SEA_PK");
			sb.Append("   AND CSCS.CONT_CUST_TRN_SEA_FK = CCTS.CONT_CUST_TRN_SEA_PK");
			sb.Append("   AND CCTS.PORT_MST_POL_FK = POL.PORT_MST_PK");
			sb.Append("   AND CCTS.PORT_MST_POD_FK = POD.PORT_MST_PK");
			sb.Append("   AND CCTS.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
			sb.Append("   AND CCTS.LCL_BASIS = DUMT.DIMENTION_UNIT_MST_PK(+)");
			sb.Append("   AND CSCS.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
			sb.Append("   AND CSCS.CURRENCY_MST_FK = CTM.CURRENCY_MST_PK");
			sb.Append("   AND CCST.CONT_CUST_SEA_PK =" + CustContractSeaPk);
			try {
				return objWK.GetDataSet(sb.ToString());
			//Manjunath  PTS ID:Sep-02  17/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Current Status"
		public int FetchStatus(int PK_VAL)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			try {
				sb.Append(" SELECT CT.APP_STATUS ");
				sb.Append("  FROM CONT_CUST_SEA_TBL CT ");
				sb.Append("   WHERE CT.CONT_CUST_SEA_PK =  " + PK_VAL);
				return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
			//Manjunath  PTS ID:Sep-02  17/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region " Port Group "

		public string FetchPrtGroup(int ContPK)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();

			try {
				strSQL = " SELECT NVL(Q.PORT_GROUP,0) PORT_GROUP FROM CONT_CUST_SEA_TBL Q WHERE Q.CONT_CUST_SEA_PK = " + ContPK;

				return objWF.ExecuteScaler(strSQL);

			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}

		}

		public DataSet FetchFromPortGroup(int QuotPK = 0, int PortGrpPK = 0, string POLPK = "0")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				if (QuotPK != 0) {
					sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POL_GRP_FK FROM CONT_CUST_TRN_SEA_TBL T, PORT_MST_TBL P WHERE T.PORT_MST_POL_FK = P.PORT_MST_PK AND T.POL_GRP_FK = " + PortGrpPK);
					sb.Append(" AND T.CONT_CUST_SEA_FK = " + QuotPK);
				} else {
					sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POL_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
					sb.Append(" AND P.BUSINESS_TYPE = 2 AND P.ACTIVE_FLAG = 1 ");
					if (POLPK != "0") {
						sb.Append(" AND PGT.PORT_MST_FK IN (" + POLPK + ") ");
					}
				}
				//sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID FROM PORT_MST_TBL P WHERE P.PORT_GRP_MST_FK=" & Quotationpk)
				//sb.Append("AND P.BUSINESS_TYPE=2 AND P.ACTIVE_FLAG=1")

				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchToPortGroup(int QuotPK = 0, int PortGrpPK = 0, string PODPK = "0")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				if (QuotPK != 0) {
					sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK FROM CONT_CUST_TRN_SEA_TBL T, PORT_MST_TBL P WHERE T.PORT_MST_POD_FK = P.PORT_MST_PK AND T.POD_GRP_FK = " + PortGrpPK);
					sb.Append(" AND T.CONT_CUST_SEA_FK = " + QuotPK);
				} else {
					sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POD_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
					sb.Append(" AND P.BUSINESS_TYPE = 2 AND P.ACTIVE_FLAG = 1");
					if (PODPK != "0") {
						sb.Append(" AND PGT.PORT_MST_FK IN (" + PODPK + ") ");
					}
				}
				//sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID FROM PORT_MST_TBL P WHERE P.PORT_GRP_MST_FK=" & Quotationpk)
				//sb.Append("AND P.BUSINESS_TYPE=2 AND P.ACTIVE_FLAG=1")

				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public DataSet FetchTariffGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append(" SELECT DISTINCT * FROM (");
				sb.Append(" SELECT POL.PORT_MST_PK   POL_PK,");
				sb.Append("       POL.PORT_ID       POL_ID,");
				sb.Append("       POD.PORT_MST_PK   POD_PK,");
				sb.Append("       POD.PORT_ID       POD_ID,");
				sb.Append("       T.POL_GRP_FK      POL_GRP_FK,");
				sb.Append("       T.PORT_MST_POD_FK POD_GRP_FK,");
				sb.Append("       T.TARIFF_GRP_FK   TARIFF_GRP_MST_PK");
				sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD, CONT_CUST_TRN_SEA_TBL T");
				sb.Append(" WHERE T.PORT_MST_POL_FK = POL.PORT_MST_PK");
				sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK");
				sb.Append("   AND T.CONT_CUST_SEA_FK =" + QuotPK);
				sb.Append("   UNION");
				sb.Append(" SELECT POL.PORT_MST_PK       POL_PK,");
				sb.Append("       POL.PORT_ID           POL_ID,");
				sb.Append("       POD.PORT_MST_PK       POD_PK,");
				sb.Append("       POD.PORT_ID           POD_ID,");
				sb.Append("       TGM.POL_GRP_MST_FK    POL_GRP_FK,");
				sb.Append("       TGM.POD_GRP_MST_FK    POD_GRP_FK,");
				sb.Append("       TGM.TARIFF_GRP_MST_PK");
				sb.Append("  FROM PORT_MST_TBL       POL,");
				sb.Append("       PORT_MST_TBL       POD,");
				sb.Append("       TARIFF_GRP_TRN_TBL TGT,");
				sb.Append("       TARIFF_GRP_MST_TBL TGM");
				sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
				sb.Append("   AND POL.PORT_MST_PK = TGT.POL_MST_FK");
				sb.Append("   AND POD.PORT_MST_PK = TGT.POD_MST_FK");
				sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
				sb.Append("   AND POL.BUSINESS_TYPE = 2");
				sb.Append("   AND POL.ACTIVE_FLAG = 1");
				sb.Append("   )");

				//'Comeented if we are fetching from tariff screen records not displaying
				//If QuotPK <> 0 Then
				//    sb.Append(" SELECT POL.PORT_MST_PK   POL_PK,")
				//    sb.Append("       POL.PORT_ID       POL_ID,")
				//    sb.Append("       POD.PORT_MST_PK   POD_PK,")
				//    sb.Append("       POD.PORT_ID       POD_ID,")
				//    sb.Append("       T.POL_GRP_FK      POL_GRP_FK,")
				//    sb.Append("       T.PORT_MST_POD_FK POD_GRP_FK,")
				//    sb.Append("       T.TARIFF_GRP_FK   TARIFF_GRP_MST_PK")
				//    sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD, QUOTATION_TRN_SEA_FCL_LCL T")
				//    sb.Append(" WHERE T.PORT_MST_POD_FK = POL.PORT_MST_PK")
				//    sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK")
				//    sb.Append("   AND T.QUOTATION_SEA_FK =" & QuotPK)
				//Else
				//    sb.Append(" SELECT POL.PORT_MST_PK       POL_PK,")
				//    sb.Append("       POL.PORT_ID           POL_ID,")
				//    sb.Append("       POD.PORT_MST_PK       POD_PK,")
				//    sb.Append("       POD.PORT_ID           POD_ID,")
				//    sb.Append("       TGM.POL_GRP_MST_FK    POL_GRP_FK,")
				//    sb.Append("       TGM.POD_GRP_MST_FK    POD_GRP_FK,")
				//    sb.Append("       TGM.TARIFF_GRP_MST_PK")
				//    sb.Append("  FROM PORT_MST_TBL       POL,")
				//    sb.Append("       PORT_MST_TBL       POD,")
				//    sb.Append("       TARIFF_GRP_TRN_TBL TGT,")
				//    sb.Append("       TARIFF_GRP_MST_TBL TGM")
				//    sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK")
				//    sb.Append("   AND POL.PORT_MST_PK = TGT.POL_MST_FK")
				//    sb.Append("   AND POD.PORT_MST_PK = TGT.POD_MST_FK")
				//    sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" & TariffPK)
				//    sb.Append("   AND POL.BUSINESS_TYPE = 2")
				//    sb.Append("   AND POL.ACTIVE_FLAG = 1")
				//End If


				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchTariffPODGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				if (QuotPK != 0) {
					sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK, T.TARIFF_GRP_FK FROM CONT_CUST_TRN_SEA_TBL T, PORT_MST_TBL P WHERE T.PORT_MST_POD_FK = P.PORT_MST_PK AND T.TARIFF_GRP_FK = " + TariffPK);
					sb.Append(" AND T.CONT_CUST_SEA_FK =" + QuotPK);
				} else {
					sb.Append(" SELECT P.PORT_MST_PK,");
					sb.Append("        P.PORT_ID,");
					sb.Append("        TGM.POD_GRP_MST_FK POD_GRP_FK,");
					sb.Append("        TGM.TARIFF_GRP_MST_PK");
					sb.Append("  FROM PORT_MST_TBL P, TARIFF_GRP_TRN_TBL TGT, TARIFF_GRP_MST_TBL TGM");
					sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
					sb.Append("   AND P.PORT_MST_PK = TGT.POD_MST_FK");
					sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
					sb.Append("   AND P.BUSINESS_TYPE = 2");
					sb.Append("   AND P.ACTIVE_FLAG = 1");
				}
				//sb.Append("SELECT P.PORT_MST_PK, P.PORT_ID FROM PORT_MST_TBL P WHERE P.PORT_GRP_MST_FK=" & Quotationpk)
				//sb.Append("AND P.BUSINESS_TYPE=2 AND P.ACTIVE_FLAG=1")

				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public long FetchFreightGridPK(string CCPK, int CCTrnFK, int CCFreightFK)
		{

			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();


			try {
				sb.Append("SELECT CFT.CONT_SUR_CHRG_SEA_PK ");
				sb.Append("  FROM CONT_SUR_CHRG_SEA_TBL CFT,");
				sb.Append("       CONT_CUST_SEA_TBL     CMT,");
				sb.Append("       CONT_CUST_TRN_SEA_TBL CTT");
				sb.Append(" WHERE CMT.CONT_CUST_SEA_PK = CTT.CONT_CUST_SEA_FK");
				sb.Append("   AND CTT.CONT_CUST_TRN_SEA_PK = CFT.CONT_CUST_TRN_SEA_FK");
				sb.Append("   AND CMT.CONT_CUST_SEA_PK = " + CCPK);
				sb.Append("   AND CTT.CONT_CUST_TRN_SEA_PK = " + CCTrnFK);
				sb.Append("   AND CFT.FREIGHT_ELEMENT_MST_FK = " + CCFreightFK);

				return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));

			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}

		}

		public long FetchTrnGridPK(string CCPK, int CCPOLFK, int CCPODFK)
		{

			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();


			try {
				sb.Append("SELECT CTT.CONT_CUST_TRN_SEA_PK");
				sb.Append("  FROM CONT_CUST_SEA_TBL CMT, CONT_CUST_TRN_SEA_TBL CTT");
				sb.Append(" WHERE CMT.CONT_CUST_SEA_PK = CTT.CONT_CUST_SEA_FK");
				sb.Append("   AND CMT.CONT_CUST_SEA_PK = " + CCPK);
				sb.Append("   AND CTT.PORT_MST_POL_FK = " + CCPOLFK);
				sb.Append("   AND CTT.PORT_MST_POD_FK = " + CCPODFK);

				return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));

			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}

		}

		#endregion

		//'Vasava

		#region "Fetch COntract Nmber"
		public string FetchContract(string strRFQNo)
		{
			try {
				string strSQL = null;
				strSQL = "SELECT NVL(MAX(CCST.CONT_REF_NO),0) FROM CONT_CUST_SEA_TBL CCST " + "WHERE CCST.CONT_REF_NO LIKE '" + strRFQNo + "/%' " + "ORDER BY CCST.CONT_REF_NO";
				return (new WorkFlow()).ExecuteScaler(strSQL);
			} catch (OracleException oraexp) {
				throw oraexp;
				//'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Deactive Existing Cntract"
		public ArrayList Deactivate(long ContractPk)
		{
			WorkFlow objWK = new WorkFlow();
			string strSQL = null;
			OracleTransaction TRAN = null;
			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();
			arrMessage.Clear();
			objWK.MyCommand.Transaction = TRAN;

			strSQL = "UPDATE CONT_CUST_SEA_TBL T " + "SET T.STATUS = 4 ,T.APP_STATUS = 4,T.ACTIVE = 0,T.VALID_TO = TO_DATE(SYSDATE,'dd/mm/yyyy')," + "T.LAST_MODIFIED_BY_FK =" + M_CREATED_BY_FK + "," + "T.LAST_MODIFIED_DT = SYSDATE," + "T.VERSION_NO = T.VERSION_NO + 1" + "WHERE T.CONT_CUST_SEA_PK =" + ContractPk;
			objWK.MyCommand.CommandType = CommandType.Text;
			objWK.MyCommand.CommandText = strSQL;
			try {
				objWK.MyCommand.ExecuteScalar();
				_PkValue = ContractPk;
				arrMessage.Add("All data saved successfully");
				TRAN.Commit();
				return arrMessage;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				TRAN.Rollback();
				return arrMessage;
			} finally {
				objWK.MyConnection.Close();
				//Added by sivachandran - To close the connection after Transaction
			}
		}
		#endregion

		#region "For Fetching Log Deatils"
		public DataSet FetchLogDetails(Int64 Logpk)
		{
			try {
				string strSQL = null;
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				WorkFlow objWF = new WorkFlow();

				sb.Append(" SELECT ROWNUM AS Sl_No,CLOG.CUSTOMER_CONTRACT_LOG_PK CONTRACT_LOG_PK, ");
				sb.Append("  CLOG.CUSTOMER_CONTRACT_FK  CONTRACT_FK, ");
				sb.Append("  CLOG.USER_COMMENTS, ");
				sb.Append("  CLOG.USER_MST_FK, ");
				sb.Append("  CLOG.LOG_DATE, ");
				sb.Append(" CLOG.LOCATION_MST_FK, ");
				sb.Append("  UMT.USER_NAME, ");
				sb.Append(" LMT.LOCATION_NAME, ");
				sb.Append(" CLOG.VERSION_NO ");
				sb.Append("  FROM CONT_CUST_SEA_TBL  CCS, ");
				sb.Append(" CUSTOMER_CONTACT_LOG_TRN CLOG, ");
				sb.Append(" USER_MST_TBL  UMT,");
				sb.Append(" LOCATION_MST_TBL LMT ");
				sb.Append(" WHERE CCS.CONT_CUST_SEA_PK = CLOG.CUSTOMER_CONTRACT_FK ");
				sb.Append(" AND CLOG.USER_MST_FK = UMT.USER_MST_PK ");
				sb.Append(" AND CLOG.LOCATION_MST_FK = LMT.LOCATION_MST_PK ");
				sb.Append(" AND clog.customer_contract_log_pk = 1 ");

				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException oraexp) {
				throw oraexp;
				//'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "For Saving Log Details"
		public ArrayList SaveLogDtls(DataSet dsMain, Int64 ContractPk, Int64 CreatedBy, Int64 Configfk)
		{
			WorkFlow objWF = new WorkFlow();
			OracleTransaction TRAN = null;
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			objWF.OpenConnection();
			TRAN = objWF.MyConnection.BeginTransaction();
			DataTable DtTbl = new DataTable();
			DataRow DtRw = null;
			int nRowCnt = 0;
			Int32 RecAfct = default(Int32);
			int logPK = 0;
			arrMessage.Clear();

			try {
				for (nRowCnt = 0; nRowCnt <= dsMain.Tables[0].Rows.Count - 1; nRowCnt++) {
					if (string.IsNullOrEmpty(dsMain.Tables[0].Rows[nRowCnt]["CONTRACT_LOG_PK"].ToString())) {
						var _with4 = insCommand;
						insCommand.Parameters.Clear();
						_with4.Transaction = TRAN;
						_with4.Connection = objWF.MyConnection;
						_with4.CommandType = CommandType.StoredProcedure;
						_with4.CommandText = objWF.MyUserName + ".CUSTOMER_CONTACT_LOG_TRN_PKG.CUSTOMER_CONTACT_LOG_TRN_INS";
						_with4.Parameters.Add("CUSTOMER_CONTRACT_FK_IN", ContractPk).Direction = ParameterDirection.Input;
						_with4.Parameters.Add("USER_COMMENTS_IN", dsMain.Tables[0].Rows[nRowCnt]["USER_COMMENTS"]).Direction = ParameterDirection.Input;
						_with4.Parameters.Add("LOG_DATE_IN", Convert.ToDateTime(dsMain.Tables[0].Rows[nRowCnt]["LOG_DATE"])).Direction = ParameterDirection.Input;
						_with4.Parameters.Add("USER_MST_FK_IN", Convert.ToInt32(dsMain.Tables[0].Rows[nRowCnt]["USER_MST_FK"])).Direction = ParameterDirection.Input;
						_with4.Parameters.Add("LOCATION_MST_FK_IN", Convert.ToInt32(dsMain.Tables[0].Rows[nRowCnt]["LOCATION_MST_FK"])).Direction = ParameterDirection.Input;
						_with4.Parameters.Add("CREATED_BY_FK_IN", CreatedBy).Direction = ParameterDirection.Input;
						_with4.Parameters.Add("CONFIG_MST_FK_IN", Configfk).Direction = ParameterDirection.Input;
						_with4.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
						_with4.ExecuteNonQuery();
						//'Update
					} else {
						var _with5 = updCommand;
						_with5.Transaction = TRAN;
						_with5.CommandType = CommandType.StoredProcedure;
						_with5.CommandText = objWF.MyUserName + ".CUSTOMER_CONTACT_LOG_TRN_PKG.CUSTOMER_CONTACT_LOG_TRN_UPD";
						updCommand.Parameters.Clear();
						_with5.Parameters.Add("CUSTOMER_CONTRACT_LOG_PK_IN", dsMain.Tables[0].Rows[nRowCnt]["CONTRACT_LOG_PK"]).Direction = ParameterDirection.Input;
						_with5.Parameters.Add("CUSTOMER_CONTRACT_FK_IN", dsMain.Tables[0].Rows[nRowCnt]["CONTRACT_FK"]).Direction = ParameterDirection.Input;
						_with5.Parameters.Add("USER_COMMENTS_IN", dsMain.Tables[0].Rows[nRowCnt]["USER_COMMENTS"]).Direction = ParameterDirection.Input;
						_with5.Parameters.Add("LOG_DATE_IN", dsMain.Tables[0].Rows[nRowCnt]["LOG_DATE"]).Direction = ParameterDirection.Input;
						_with5.Parameters.Add("USER_MST_FK_IN", dsMain.Tables[0].Rows[nRowCnt]["USER_NAME"]).Direction = ParameterDirection.Input;
						_with5.Parameters.Add("LOCATION_MST_FK_IN", dsMain.Tables[0].Rows[nRowCnt]["LOCATION_NAME"]).Direction = ParameterDirection.Input;
						_with5.Parameters.Add("LAST_MODIFIED_BY_FK_IN", CreatedBy).Direction = ParameterDirection.Input;
						_with5.Parameters.Add("VERSION_NO_IN", CreatedBy).Direction = ParameterDirection.Input;

						_with5.ExecuteNonQuery();

					}

				}
				var _with6 = objWF.MyDataAdapter;
				if (logPK == 0) {
					_with6.InsertCommand = insCommand;
					_with6.InsertCommand.Transaction = TRAN;
					_with6.InsertCommand.ExecuteNonQuery();
					try {
						_logPkValue = Convert.ToInt64(objWF.MyCommand.Parameters["RETURN_VALUE"].Value);
					} catch (Exception ex) {
					}
				}
				if (arrMessage.Count == 0) {
					arrMessage.Clear();
					arrMessage.Add("All data Saved successfully");
					TRAN.Commit();
				} else {
					TRAN.Rollback();
				}
			} catch (OracleException Ex) {
				throw Ex;
			} finally {
				objWF.CloseConnection();
			}
			return arrMessage;
		}
		#endregion

		#region "Property"
		public long LogPkValue {
			get { return _logPkValue; }
		}
		#endregion

		#region "Fetching Crago type and assiging based on PK "
		public object FetchCargo(Int64 ContractPk)
		{
			try {
				string strSQL = null;
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				WorkFlow objWF = new WorkFlow();
				sb.Append(" SELECT CSEA.CARGO_TYPE FROM CONT_CUST_SEA_TBL CSEA ");
				sb.Append("WHERE CSEA.CONT_CUST_SEA_PK=" + ContractPk);
				return objWF.ExecuteScaler(sb.ToString());
			} catch (OracleException oraexp) {
				throw oraexp;
				//'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

	}
}
