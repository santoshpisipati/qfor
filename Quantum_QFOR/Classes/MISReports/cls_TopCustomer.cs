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
using System.Web;

namespace Quantum_QFOR
{
    public class clsTopCustomer : CommonFeatures
	{
		string type;
		public string GetProcedure {
			//'Vasava for PTS:OCT-033
			get {
				if (type == "ALL") {
					return "FETCH_TOPCUSTOMER,FETCH_DATA_ALL";
				}
                return "";
			}
			set { type = value; }
		}

		public DataSet Fetch_Data(string BizType = "", string ProcessType = "", string FROM_DATE = "", string TO_DATE = "", string LCL_FCL = "1", string POL_POD = "", string CUSTOMER = "", string LOCATION = "", string CURRENCY = "", string COMMODITY_GROUP = "",
		string TOP = "", string SortColumn = "BOOKING", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", int Column = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataTable dtTrade = null;
			DataTable dtCust = null;
			DataTable dtLocation = null;
			DataTable dtCommodity = null;
			DataSet dsAll = null;
			string[] strPKGProc = null;
			try {
				SortColumn = (SortColumn == "Teu's" ? "TEUS" : SortColumn.ToUpper());
				objWF.MyCommand.Parameters.Clear();
				//adding by thiyagarajan on 16/12/08 for location base currency task
				CURRENCY = Convert.ToString(HttpContext.Current.Session["Currency_mst_pk"]);


				var _with1 = objWF.MyCommand.Parameters;
				//FROM_DATE
				_with1.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;
				//TO_DATE()
				_with1.Add("TO_DATE", getDefault(TO_DATE, "")).Direction = ParameterDirection.Input;
				//LCL_FCL()
				_with1.Add("LCL_FCL", getDefault(LCL_FCL, 1)).Direction = ParameterDirection.Input;
				//CUSTOMER()
				_with1.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;
				//LOCATION()
				_with1.Add("LOCATION", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input;
				//SECTOR()
				_with1.Add("SECTOR", getDefault(POL_POD, 0)).Direction = ParameterDirection.Input;
				//COMMODITY_GROUP()
				_with1.Add("COMMODITY_GROUP", getDefault(COMMODITY_GROUP, 0)).Direction = ParameterDirection.Input;
				//TOP()
				_with1.Add("TOP", getDefault(TOP, 0)).Direction = ParameterDirection.Input;
				_with1.Add("M_MASTERPAGESIZE_IN", 50).Direction = ParameterDirection.Input;
				_with1.Add("COLUMN", Column).Direction = ParameterDirection.Input;
				//SORT()
				_with1.Add("SORT", getDefault(SortType, " DESC")).Direction = ParameterDirection.Input;

                _with1.Add("BIZ_TYPE_IN", getDefault(Convert.ToInt32(BizType, 0).ToString(), 0)).Direction = ParameterDirection.Input;
				_with1.Add("PROCESS_TYPE_IN", getDefault(ProcessType, 0)).Direction = ParameterDirection.Input;
				//TOTALPAGE_IN()
				//adding by thiyagarajan on 16/12/08 for location base currency task
				_with1.Add("CURRENCY_IN", CURRENCY).Direction = ParameterDirection.Input;
				//end
				_with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
				//CURRENTPAGE_IN()
				_with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				//BL_CUR()
				_with1.Add("TRADE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with1.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with1.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with1.Add("COMMODITY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				strPKGProc = GetProcedure.Split(',');
				dsAll = objWF.GetDataSet(strPKGProc[0], strPKGProc[1]);
				TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
				CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				if (TotalPage == 0) {
					CurrentPage = 0;
				} else {
					CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				}
				CreateRelation(dsAll);
				//dsAll.WriteXml(server.mappath(.)&"\ds.xml")
				return dsAll;
			//'Manjunath  PTS ID:Sep-02  26/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		private void CreateRelation(DataSet dsMain)
		{
			// Get the DataColumn objects from two DataTable objects in a DataSet.
			DataColumn parentCol = null;
			DataColumn childCol = null;
			// Dim dataset1 As New DataSet
			// Code to get the DataSet not shown here.
			parentCol = dsMain.Tables[0].Columns["CUSTOMER_NAME"];
			childCol = dsMain.Tables[1].Columns["CUSTOMER_NAME"];
			try {
				// Create DataRelation.
				DataRelation relTrade = null;
				relTrade = new DataRelation("Trade", parentCol, childCol);
				DataRelation relCust = null;
				relCust = new DataRelation("Cust", new DataColumn[] {
					dsMain.Tables[1].Columns["TRADE_CODE"],
					dsMain.Tables[1].Columns["CUSTOMER_NAME"]
				}, new DataColumn[] {
					dsMain.Tables[2].Columns["TRADE_CODE"],
					dsMain.Tables[2].Columns["CUSTOMER_NAME"]
				});
				DataRelation relLoc = null;
				relLoc = new DataRelation("Loc", new DataColumn[] {
					dsMain.Tables[2].Columns["TRADE_CODE"],
					dsMain.Tables[2].Columns["CUSTOMER_NAME"],
					dsMain.Tables[2].Columns["POL"],
					dsMain.Tables[2].Columns["POD"],
					dsMain.Tables[2].Columns["LOCATION_ID"]
				}, new DataColumn[] {
					dsMain.Tables[3].Columns["TRADE_CODE"],
					dsMain.Tables[3].Columns["CUSTOMER_NAME"],
					dsMain.Tables[3].Columns["POL"],
					dsMain.Tables[3].Columns["POD"],
					dsMain.Tables[3].Columns["LOCATION_ID"]
				});
				//parentCol = dsMain.Tables(3).Columns("LOCATION_ID")
				//childCol = dsMain.Tables(4).Columns("LOCATION_ID")
				//Dim relCom As DataRelation
				//relCom = New DataRelation("Com", parentCol, childCol)
				// Add the relation to the DataSet.
				relTrade.Nested = true;
				relCust.Nested = true;
				relLoc.Nested = true;
				dsMain.Relations.Add(relTrade);
				dsMain.Relations.Add(relCust);
				dsMain.Relations.Add(relLoc);
				//dsMain.Relations.Add(relCom)
			//'Manjunath  PTS ID:Sep-02  26/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public string GetSector(string tradePk)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			string strReturn = null;
			WorkFlow objWF = new WorkFlow();
			OracleDataReader dr = null;
			try {
				strQuery.Append("");
				strQuery.Append("SELECT S.FROM_PORT_FK, S.TO_PORT_FK ");
				strQuery.Append("FROM TRADE_MST_TBL T, SECTOR_MST_TBL S ");
				strQuery.Append("WHERE S.TRADE_MST_FK = T.TRADE_MST_PK ");
				strQuery.Append("AND T.TRADE_MST_PK =" + tradePk);
				dr = objWF.GetDataReader(strQuery.ToString());
				while (dr.Read()) {
					strReturn += dr["FROM_PORT_FK"] + "~" + dr["TO_PORT_FK"] + "~$";
				}
				dr.Close();
				return strReturn;
			//'Manjunath  PTS ID:Sep-02  26/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet GetLocation(string userLocPK, string strALL)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			string strReturn = null;
			WorkFlow objWF = new WorkFlow();
			OracleDataReader dr = null;
			try {
				strQuery.Append("");
				strQuery.Append("   SELECT '<ALL>' LOCATION_ID, ");
				strQuery.Append("       0 LOCATION_MST_PK, ");
				strQuery.Append("       0 REPORTING_TO_FK, ");
				strQuery.Append("       0 LOCATION_TYPE_FK ");
				strQuery.Append("  FROM DUAL ");
				strQuery.Append("UNION ");
				strQuery.Append(" SELECT L.LOCATION_ID, ");
				strQuery.Append("       L.LOCATION_MST_PK, ");
				strQuery.Append("       L.REPORTING_TO_FK, ");
				strQuery.Append("       L.LOCATION_TYPE_FK ");
				strQuery.Append("  FROM LOCATION_MST_TBL L ");
				strQuery.Append(" START WITH L.LOCATION_TYPE_FK = 1 ");
				strQuery.Append("        AND L.ACTIVE_FLAG = 1 ");
				strQuery.Append("        AND L.LOCATION_MST_PK =" + userLocPK);
				strQuery.Append(" CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK ");
				dr = objWF.GetDataReader(strQuery.ToString());
				while (dr.Read()) {
					strReturn += dr["LOCATION_MST_PK"] + "~$";
				}
				dr.Close();
				if (strReturn == "0~$") {
					strQuery = new System.Text.StringBuilder();

					strQuery.Append(" SELECT L.LOCATION_ID, ");
					strQuery.Append("       L.LOCATION_MST_PK, ");
					strQuery.Append("       L.REPORTING_TO_FK, ");
					strQuery.Append("       L.LOCATION_TYPE_FK ");
					strQuery.Append("  FROM LOCATION_MST_TBL L ");
					strQuery.Append("  WHERE L.LOCATION_MST_PK = " + userLocPK);
					strQuery.Append("UNION ");
					strQuery.Append(" SELECT L.LOCATION_ID, ");
					strQuery.Append("       L.LOCATION_MST_PK, ");
					strQuery.Append("       L.REPORTING_TO_FK, ");
					strQuery.Append("       L.LOCATION_TYPE_FK ");
					strQuery.Append("  FROM LOCATION_MST_TBL L ");
					//strQuery.Append("  WHERE L.REPORTING_TO_FK = " & userLocPK)
					strQuery.Append("  WHERE L.LOCATION_MST_PK IN ");
					strQuery.Append("  (SELECT LMT.LOCATION_MST_PK ");
					strQuery.Append("  FROM LOCATION_MST_TBL LMT  ");
					strQuery.Append(" WHERE LMT.ACTIVE_FLAG = 1 ");
					strQuery.Append("  START WITH LMT.LOCATION_MST_PK = " + userLocPK);
					strQuery.Append("  CONNECT BY PRIOR LMT.LOCATION_MST_PK = LMT.REPORTING_TO_FK) ");

					dr = objWF.GetDataReader(strQuery.ToString());
					while (dr.Read()) {
						strReturn += dr["LOCATION_MST_PK"] + "~$";
					}
					dr.Close();
				}

				strALL = strReturn;
				//1481~$1123~$'
				return objWF.GetDataSet(strQuery.ToString());
			//'Manjunath  PTS ID:Sep-02  26/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public DataSet GetLocationName(string userLocPK, string strALL)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			string strReturn = null;
			WorkFlow objWF = new WorkFlow();
			OracleDataReader dr = null;
			try {
				strQuery.Append("");
				strQuery.Append("   SELECT '<ALL>' LOCATION_NAME, ");
				strQuery.Append("       0 LOCATION_MST_PK, ");
				strQuery.Append("       0 REPORTING_TO_FK, ");
				strQuery.Append("       0 LOCATION_TYPE_FK ");
				strQuery.Append("  FROM DUAL ");
				strQuery.Append("UNION ");
				strQuery.Append(" SELECT L.LOCATION_NAME, ");
				strQuery.Append("       L.LOCATION_MST_PK, ");
				strQuery.Append("       L.REPORTING_TO_FK, ");
				strQuery.Append("       L.LOCATION_TYPE_FK ");
				strQuery.Append("  FROM LOCATION_MST_TBL L ");
				strQuery.Append(" START WITH L.LOCATION_TYPE_FK = 1 ");
				strQuery.Append("        AND L.ACTIVE_FLAG = 1 ");
				strQuery.Append("        AND L.LOCATION_MST_PK =" + userLocPK);
				strQuery.Append(" CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK ");
				dr = objWF.GetDataReader(strQuery.ToString());
				while (dr.Read()) {
					strReturn += dr["LOCATION_MST_PK"] + "~$";
				}
				dr.Close();
				if (strReturn == "0~$") {
					strQuery = new System.Text.StringBuilder();
					//strQuery.Append(" SELECT L.LOCATION_ID, ")
					//strQuery.Append("       L.LOCATION_MST_PK, ")
					//strQuery.Append("       L.REPORTING_TO_FK, ")
					//strQuery.Append("       L.LOCATION_TYPE_FK ")
					//strQuery.Append("  FROM LOCATION_MST_TBL L ")
					//strQuery.Append("  WHERE L.LOCATION_MST_PK = " & userLocPK)

					strQuery.Append(" SELECT L.LOCATION_NAME, ");
					strQuery.Append("       L.LOCATION_MST_PK, ");
					strQuery.Append("       L.REPORTING_TO_FK, ");
					strQuery.Append("       L.LOCATION_TYPE_FK ");
					strQuery.Append("  FROM LOCATION_MST_TBL L ");
					strQuery.Append("  WHERE L.LOCATION_MST_PK = " + userLocPK);
					strQuery.Append("UNION ");
					strQuery.Append(" SELECT L.LOCATION_NAME, ");
					strQuery.Append("       L.LOCATION_MST_PK, ");
					strQuery.Append("       L.REPORTING_TO_FK, ");
					strQuery.Append("       L.LOCATION_TYPE_FK ");
					strQuery.Append("  FROM LOCATION_MST_TBL L ");
					strQuery.Append("  WHERE L.REPORTING_TO_FK = " + userLocPK);

					dr = objWF.GetDataReader(strQuery.ToString());
					while (dr.Read()) {
						strReturn += dr["LOCATION_MST_PK"] + "~$";
					}
					dr.Close();
				}

				strALL = strReturn;
				//1481~$1123~$'
				return objWF.GetDataSet(strQuery.ToString());
			//'Manjunath  PTS ID:Sep-02  26/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#region "LOCATION INFORMATION"
		public DataSet FetchLocationInformation(string Fromdate = "", string Todate = "", string POLPK = "", string PODPK = "", string CountryPK = "", string LocPK = "", string CustPK = "", string BizType = "", string ProcessType = "", string CustGroupPK = "",
		string CarrierPK = "", string VslVoyPK = "", string FlightID = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int CargoType = 0, string CurrPK = "", string BaseCurrPK = "")
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			DataSet MainDS = new DataSet();
			OracleDataAdapter DA = new OracleDataAdapter();
			Int32 TotalRecords = default(Int32);
			string strSQL = null;
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			try {
				sb.Append(" SELECT DISTINCT  GROUPID  FROM");
				sb.Append("  (SELECT DISTINCT LOCATION_MST_PK, LOCATION_ID, BALANCE,GROUPID");
				sb.Append("  FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE),2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                    
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}

				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("                HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("     AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("     AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("         AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("         AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else {
					sb.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')))");
				}
				//sb.Append("  ORDER BY LOCATION_ID")

				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "GROUP");
				///'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
				///'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
				sb.Remove(0, sb.Length);

				sb.Append("SELECT DISTINCT LOCATION_MST_PK, LOCATION_ID, BALANCE,GROUPID");
				sb.Append("  FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}

				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                       CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("     AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("     AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("         AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("         AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else {
					sb.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "'))");
				}
				sb.Append("  ORDER BY LOCATION_ID");

				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "LOCATION");
				///'''''''''''
				sb.Remove(0, sb.Length);
				sb.Append("SELECT DISTINCT ADM_LOCATION_MST_FK,");
				sb.Append("                CUSTOMER_MST_PK,");
				sb.Append("                CUSTOMER_NAME,");
				sb.Append("                CUST_REG_NO,");
				sb.Append("                BALANCE");
				sb.Append("  FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}


				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else {
					sb.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "'))");
				}
				sb.Append("  ORDER BY CUSTOMER_NAME");

				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "CUSTOMER");

				///'''''''''''''''''''''
				///'''''''''''''''''''''
				sb.Remove(0, sb.Length);
				sb.Append("SELECT DISTINCT  C.CUSTOMER_MST_PK,");
				sb.Append("       0,");
				sb.Append("       TO_DATE('" + Fromdate + "','dd/MM/yyyy HH24:Mi:ss') REF_DATE,");
				sb.Append("       '',");
				sb.Append("       'OPENING BALANCE' TRANSACTION,");
				sb.Append("       '',");
				sb.Append("       '' DOCREFNR,");
				sb.Append("       0 DEBIT,");
				sb.Append("       0 CREDIT,");
				sb.Append("       SUM(B.DEBIT - B.CREDIT) BALANCE");
				sb.Append("  FROM CUSTOMER_MST_TBL C,");
				sb.Append("       (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}

				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else {
					sb.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')) A,");
				}

				sb.Append("       (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("           AND INV.invoice_date <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}

				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}

				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else {
					sb.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')) B");
				}

				sb.Append(" WHERE C.CUSTOMER_MST_PK = A.CUSTOMER_MST_PK");
				sb.Append("   AND A.CUSTOMER_MST_PK = B.CUSTOMER_MST_PK(+)");
				sb.Append(" GROUP BY C.CUSTOMER_MST_PK");
				sb.Append("   UNION ");
				///'''''''''''''''''''''''''
				sb.Append("SELECT CUSTOMER_MST_PK,");
				sb.Append("       ADM_LOCATION_MST_FK,");
				sb.Append("       REF_DATE,");
				sb.Append("       PROCESS,");
				sb.Append("       TRANSACTION,");
				sb.Append("       HBL_REF_NO,");
				sb.Append("       DOCREFNR,");
				sb.Append("       DEBIT,");
				sb.Append("       CREDIT,");
				sb.Append("       BALANCE");
				sb.Append("     FROM ( ");
				sb.Append("       SELECT CUSTOMER_MST_PK,ADM_LOCATION_MST_FK,");
				sb.Append("       REF_DATE,");
				sb.Append("       PROCESS,");
				sb.Append("       TRANSACTION,");
				sb.Append("       HBL_REF_NO,");
				sb.Append("       DOCREFNR,");
				sb.Append("       DEBIT,");
				sb.Append("       CREDIT,");
				sb.Append("       BALANCE");
				sb.Append("  FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK, CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("            AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}

				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK, CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                    
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("            AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else {
					sb.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}

				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK, CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                    
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = 1");
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("            AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else {
					sb.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "'))");
				}
				sb.Append("     ORDER BY REF_DATE) ");

				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "DETAILS");
				DataRelation relLocGroup_Details = new DataRelation("LOCGROUP", new DataColumn[] { MainDS.Tables[0].Columns["GROUPID"] }, new DataColumn[] { MainDS.Tables[1].Columns["GROUPID"] });
				DataRelation relLocCustomer_Details = new DataRelation("LOCCUSTOMER", new DataColumn[] { MainDS.Tables[1].Columns["LOCATION_MST_PK"] }, new DataColumn[] { MainDS.Tables[2].Columns["ADM_LOCATION_MST_FK"] });
				DataRelation relCustomer_Details = new DataRelation("CUSTOMERDETAILS", new DataColumn[] { MainDS.Tables[2].Columns["CUSTOMER_MST_PK"] }, new DataColumn[] { MainDS.Tables[3].Columns["CUSTOMER_MST_PK"] });

				relLocGroup_Details.Nested = true;
				relLocCustomer_Details.Nested = true;
				relCustomer_Details.Nested = true;
				MainDS.Relations.Add(relLocGroup_Details);
				MainDS.Relations.Add(relLocCustomer_Details);
				MainDS.Relations.Add(relCustomer_Details);
				return MainDS;
				//Return objWF.GetDataSet(strQuery.ToString)
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		#endregion
		#region "LOCATION INFORMATION1"
		//'This Function is added by Ashish Arya on 4th Nov 2011 for all biz type
		public DataSet FetchLocationInformation_1(string Fromdate = "", string Todate = "", string POLPK = "", string PODPK = "", string CountryPK = "", string LocPK = "", string CustPK = "", string BizType = "", string ProcessType = "", string CustGroupPK = "",
		string CarrierPK = "", string VslVoyPK = "", string FlightID = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int CargoType = 0, string CurrPK = "", string BaseCurrPK = "")
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sb = new System.Text.StringBuilder(50000);
            var sbNew1 = new System.Text.StringBuilder();
			var sbNew2 = new System.Text.StringBuilder();
            System.Text.StringBuilder sbNew3 = new System.Text.StringBuilder(5000);
			DataSet MainDS = new DataSet();
			OracleDataAdapter DA = new OracleDataAdapter();
			Int32 TotalRecords = default(Int32);
			string strSQL = null;
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			try {
				sb.Append(" SELECT DISTINCT  GROUPID  FROM");
				sb.Append("  (SELECT DISTINCT LOCATION_MST_PK, LOCATION_ID, BALANCE,GROUPID");
				sb.Append("  FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sb.Append("                        0 CARGO_TYPE, ");
					}
				}
				//sb.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," & CurrPK & ", INV.INVOICE_DATE,1),2) DEBIT,")
				sb.Append("                        ROUND( (select sum(round(t.amt_in_inv_curr,2)) from consol_invoice_trn_tbl t where t.job_card_fk=job.job_card_trn_pk)   * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("         AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                    
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = " + Convert.ToInt32(ProcessType));
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 1");
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("       AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("   AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sb.Append("                        0 CARGO_TYPE, ");
					}
				}
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("         AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INVTRN.JOB_TYPE = 1");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sb.Append("                        0 CARGO_TYPE, ");
					}
				}
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("         AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INVTRN.JOB_TYPE = 1");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("     AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("     AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("         AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("         AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("           0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				//sb.Append("  ORDER BY LOCATION_ID")

				///Added by Sushama for Integrating Links to CBJC, TPT and DET&DEM
				/// ---CBJC Invoice
				sb.Append(" UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM CBJC_TBL   CBJC,");
				sb.Append("              CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				///TPT Invoice
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST  ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 3");

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				///DET&DEM Invoice CBJC
				sb.Append("           UNION");
				sb.Append("            SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//' DE&DEM INVOICE TPT
				sb.Append("           UNION");
				sb.Append("            SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL               TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK ");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				/// CBJC Collection
				sb.Append("            UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK =  CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);

				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 2");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//' ----TPT-(Collection)---------
				sb.Append("        UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//' ---Det & DEM CBJC (Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                       LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 4");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//' ---Det & DEM TPT (Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                       LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + Convert.ToInt32(BizType));
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				///CBJC CR Note
				sb.Append("               UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                         LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM CBJC_TBL CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND CBJC.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				///    ---TPT CrNote-----
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                         LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				///  --- DET & DEM CBJC (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                         LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				///  --- DET & DEM TPT (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                         LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("                TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				///END Sushama
				/// 
				///'--------------------Deposit Amount---------------------------
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        TO_CHAR(CDMT.DEPOSIT_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        'IMPORT'  PROCESS,");
				sb.Append("                        'CONTAINER DEPOSIT' TRANSACTION,");
				sb.Append("                         JOB.HBL_HAWB_REF_NO   HBL_REF_NO, ");
				sb.Append("                        CDMT.DEPOSIT_REF_NR DOCREFNR,");
				sb.Append("                        CDDT.CONTAINER_DEPOSIT_DTL_PK  REF_PK,");
				sb.Append("                        JOB.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        JOB.JOB_CARD_TRN_PK CARGO_TYPE, ");
				sb.Append("                        CASE WHEN CDDT.STATUS = 1 THEN ");
				sb.Append("                        ROUND(CDDT.AMOUNT * GET_EX_RATE(CDDT.CURRENCY_MST_FK, ");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CDMT.DEPOSIT_DATE,1),");
				sb.Append("                        2) ELSE 0 END DEBIT,");
				sb.Append("                       CASE WHEN CDDT.STATUS = 3 THEN ");
				sb.Append("                        ROUND(CDDT.AMOUNT * GET_EX_RATE(CDDT.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CDMT.DEPOSIT_DATE,1),");
				sb.Append("                        2) ELSE 0 END CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN           JOB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONTAINER_DEPOSIT_MST_TBL CDMT,");
				sb.Append("               CONTAINER_DEPOSIT_DTL_TBL CDDT");
				sb.Append("         WHERE LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND CDMT.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
				sb.Append("           AND CDDT.CONTAINER_DEPOSIT_MST_FK = CDMT.CONTAINER_DEPOSIT_MST_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				sb.Append("           AND JOB.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND CDDT.STATUS IN (1,3) ");
				//'Import
				if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("         AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("         AND JOB.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				} else {
					sb.Append("         AND JOB.PROCESS_TYPE = 1");
				}

				//'SEA
				if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND JOB.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN (" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else if (Convert.ToInt32(BizType) == 1) {
					sb.Append("         AND JOB.BUSINESS_TYPE = 1");
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CDMT.DEPOSIT_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				///'--------------------Deposit Amount---------------------------
				sb.Append("))");
				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "GROUP");
				///'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
				///'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
				sb.Remove(0, sb.Length);

				sb.Append("SELECT DISTINCT LOCATION_MST_PK, LOCATION_ID, BALANCE,GROUPID");
				sb.Append("  FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE  BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sb.Append("                        0 CARGO_TYPE, ");
					}
				}
				// sb.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," & CurrPK & ", INV.INVOICE_DATE,1),2) DEBIT,")
				sb.Append("                        ROUND( (select sum(round(t.amt_in_inv_curr,2)) from consol_invoice_trn_tbl t where t.job_card_fk=job.job_card_trn_pk)   * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("         AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 1");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sb.Append("                        0 CARGO_TYPE, ");
					}
				}
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("         AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                    
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 1");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("  AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                       TO_CHAR(CNT.CREDIT_NOTE_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sb.Append("                        0 CARGO_TYPE, ");
					}
				}
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("         AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("     AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("     AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("         AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("         AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 1");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("           0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}

				///Added by Sushama for Integrating Links to CBJC, TPT and DET&DEM
				/// ---CBJC Invoice
				sb.Append(" UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM CBJC_TBL   CBJC,");
				sb.Append("              CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				///TPT Invoice
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST  ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				///DET&DEM TPT Invoice
				sb.Append("           UNION");
				sb.Append("            SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				///DET&DEM CBJC Invoice
				sb.Append("           UNION");
				sb.Append("            SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				/// CBJC Collection
				sb.Append("            UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK =  CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 2");
				//' ----TPT-(Collection)---------
				sb.Append("        UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				//' ---Det & DEM CBJC (Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                       LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 4");
				//' ---Det & DEM TPT (Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                       LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				///CBJC CR Note
				sb.Append("               UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                         LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM CBJC_TBL CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND CBJC.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				///    ---TPT CrNote-----
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                         LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				///  --- DET & DEM CBJC (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                         LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				///  --- DET & DEM TPT (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                         LMT.LOCATION_ID,");
				sb.Append("                        '' GROUPID");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				///END Sushama
				/// 
				///'--------------------Deposit Amount---------------------------
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        TO_CHAR(CDMT.DEPOSIT_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        'IMPORT'  PROCESS,");
				sb.Append("                        'CONTAINER DEPOSIT' TRANSACTION,");
				sb.Append("                         JOB.HBL_HAWB_REF_NO   HBL_REF_NO, ");
				sb.Append("                        CDMT.DEPOSIT_REF_NR DOCREFNR,");
				sb.Append("                        CDDT.CONTAINER_DEPOSIT_DTL_PK  REF_PK,");
				sb.Append("                        JOB.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        JOB.JOB_CARD_TRN_PK CARGO_TYPE, ");
				sb.Append("                        CASE WHEN CDDT.STATUS = 1 THEN ");
				sb.Append("                        ROUND(CDDT.AMOUNT * GET_EX_RATE(CDDT.CURRENCY_MST_FK, ");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CDMT.DEPOSIT_DATE,1),");
				sb.Append("                        2) ELSE 0 END DEBIT,");
				sb.Append("                       CASE WHEN CDDT.STATUS = 3 THEN ");
				sb.Append("                        ROUND(CDDT.AMOUNT * GET_EX_RATE(CDDT.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CDMT.DEPOSIT_DATE,1),");
				sb.Append("                        2) ELSE 0 END CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        LMT.LOCATION_ID,''GROUPID");
				sb.Append("          FROM JOB_CARD_TRN           JOB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONTAINER_DEPOSIT_MST_TBL CDMT,");
				sb.Append("               CONTAINER_DEPOSIT_DTL_TBL CDDT");
				sb.Append("         WHERE LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND CDMT.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
				sb.Append("           AND CDDT.CONTAINER_DEPOSIT_MST_FK = CDMT.CONTAINER_DEPOSIT_MST_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				sb.Append("           AND JOB.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND CDDT.STATUS IN (1,3) ");
				//'Import
				if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("         AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("         AND JOB.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				} else {
					sb.Append("         AND JOB.PROCESS_TYPE = 1");
				}

				//'SEA
				if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND JOB.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN (" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}

				} else if (Convert.ToInt32(BizType) == 1) {
					sb.Append("         AND JOB.BUSINESS_TYPE = 1");
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CDMT.DEPOSIT_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				///'--------------------Deposit Amount---------------------------
				sb.Append(" )");

				sb.Append("  ORDER BY LOCATION_ID");

				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "LOCATION");
				///'''''''''''
				sb.Remove(0, sb.Length);
				sb.Append("SELECT DISTINCT ADM_LOCATION_MST_FK,");
				sb.Append("                CUSTOMER_MST_PK,");
				sb.Append("                CUSTOMER_NAME,");
				sb.Append("                CUST_REG_NO,");
				sb.Append("                BALANCE,");
				sb.Append("                CHKFLAG");
				sb.Append("  FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sb.Append("                        0 CARGO_TYPE, ");
					}
				}
				//  sb.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," & CurrPK & ", INV.INVOICE_DATE,1),2) DEBIT,")
				sb.Append("                        ROUND(   (select sum(round(t.amt_in_inv_curr,2)) from consol_invoice_trn_tbl t where t.job_card_fk=job.job_card_trn_pk) * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("         AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 1");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}


				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sb.Append("                        0 CARGO_TYPE, ");
					}
				}
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("         AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 1");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("  ( CASE ");
				sb.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("      HBL.HBL_REF_NO ");
				sb.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sb.Append("         HAWB.HAWB_REF_NO ");
				sb.Append("        ELSE ");
				sb.Append("       JOB.HBL_HAWB_REF_NO  ");
				sb.Append("      END) HBL_REF_NO , ");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sb.Append("                        0 CARGO_TYPE, ");
					}
				}
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("               BOOKING_MST_TBL        BKG,");
				}
				sb.Append("               HBL_EXP_TBL            HBL,");
				sb.Append("               HAWB_EXP_TBL            HAWB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sb.Append("         AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sb.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                 
				if (Convert.ToInt32(ProcessType) == 1) {
					sb.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sb.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sb.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("           AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 1");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("           0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("            AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}

				///Added by Sushama for Integrating Links to CBJC, TPT and DET&DEM
				/// ---CBJC Invoice
				sb.Append(" UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM CBJC_TBL   CBJC,");
				sb.Append("              CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				///TPT Invoice
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST  ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				///DET&DEM TPT Invoice
				sb.Append("           UNION");
				sb.Append("            SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				///DET&DEM CBJC Invoice
				sb.Append("           UNION");
				sb.Append("            SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				/// CBJC Collection
				sb.Append("            UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK =  CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 2");
				//' ----TPT-(Collection)---------
				sb.Append("        UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				//' ---Det & DEM TPT (Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK =  TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				//' ---Det & DEM CBJC (Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 4");
				///CBJC CR Note
				sb.Append("               UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM CBJC_TBL CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND CBJC.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				///    ---TPT CrNote-----
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				///  --- DET & DEM CBJC (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("           AND INVTRN.JOB_TYPE =3");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				///  --- DET & DEM CBJC (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType )== 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				///'--------------------Deposit Amount---------------------------
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        TO_CHAR(CDMT.DEPOSIT_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        'IMPORT'  PROCESS,");
				sb.Append("                        'CONTAINER DEPOSIT' TRANSACTION,");
				sb.Append("                         JOB.HBL_HAWB_REF_NO   HBL_REF_NO, ");
				sb.Append("                        CDMT.DEPOSIT_REF_NR DOCREFNR,");
				sb.Append("                        CDDT.CONTAINER_DEPOSIT_DTL_PK  REF_PK,");
				sb.Append("                        JOB.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        JOB.JOB_CARD_TRN_PK CARGO_TYPE, ");
				sb.Append("                        CASE WHEN CDDT.STATUS = 1 THEN ");
				sb.Append("                        ROUND(CDDT.AMOUNT * GET_EX_RATE(CDDT.CURRENCY_MST_FK, ");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CDMT.DEPOSIT_DATE,1),");
				sb.Append("                        2) ELSE 0 END DEBIT,");
				sb.Append("                       CASE WHEN CDDT.STATUS = 3 THEN ");
				sb.Append("                        ROUND(CDDT.AMOUNT * GET_EX_RATE(CDDT.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CDMT.DEPOSIT_DATE,1),");
				sb.Append("                        2) ELSE 0 END CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        CMT.CUSTOMER_NAME,");
				sb.Append("                        CMT.CUST_REG_NO,");
				sb.Append("                        'false'CHKFLAG");
				sb.Append("          FROM JOB_CARD_TRN           JOB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONTAINER_DEPOSIT_MST_TBL CDMT,");
				sb.Append("               CONTAINER_DEPOSIT_DTL_TBL CDDT");
				sb.Append("         WHERE LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND CDMT.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
				sb.Append("           AND CDDT.CONTAINER_DEPOSIT_MST_FK = CDMT.CONTAINER_DEPOSIT_MST_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				sb.Append("           AND JOB.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND CDDT.STATUS IN (1,3) ");
				//'Import
				if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("         AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("         AND JOB.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				} else {
					sb.Append("         AND JOB.PROCESS_TYPE = 1");
				}

				//'SEA
				if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND JOB.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN (" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else if (Convert.ToInt32(BizType) == 1) {
					sb.Append("         AND JOB.BUSINESS_TYPE = 1");
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CDMT.DEPOSIT_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				///'--------------------Deposit Amount---------------------------
				sb.Append("  )");
				sb.Append("  ORDER BY CUSTOMER_NAME");
				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "CUSTOMER");

				///'''''''''''''''''''''
				///'''''''''''''''''''''
				sb.Remove(0, sb.Length);
				sbNew1.Append("SELECT DISTINCT  C.CUSTOMER_MST_PK,");
				sbNew1.Append("       0,");
				sbNew1.Append("       TO_CHAR(TO_DATE('" + Fromdate + "'),DATEFORMAT) REF_DATE,");
				sbNew1.Append("       '' PROCESS,");
				sbNew1.Append("       'OPENING BALANCE' TRANSACTION,");
				sbNew1.Append("       '',");
				sbNew1.Append("       '' DOCREFNR,");
				sbNew1.Append("       0 DEBIT,");
				sbNew1.Append("       0 CREDIT,");
				sbNew1.Append("       SUM(B.DEBIT - B.CREDIT) BALANCE,''OUTSTANDING_DAYS,");
				sbNew1.Append("        0 REF_PK,");
				sbNew1.Append("        0 BIZ_TYPE,");
				sbNew1.Append("        0 CARGO_TYPE ");

				sbNew1.Append("  FROM CUSTOMER_MST_TBL C,");
				sbNew1.Append("       (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew1.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew1.Append("                        TO_CHAR(INV.INVOICE_DATE,DATEFORMAT) REF_DATE,");
				sbNew1.Append("                        (CASE");
				sbNew1.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sbNew1.Append("                           'EXPORT'");
				sbNew1.Append("                          ELSE");
				sbNew1.Append("                           'IMPORT'");
				sbNew1.Append("                        END) PROCESS,");
				sbNew1.Append("                        'INVOICE' TRANSACTION,");
				sbNew1.Append("  ( CASE ");
				sbNew1.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("      HBL.HBL_REF_NO ");
				sbNew1.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("         HAWB.HAWB_REF_NO ");
				sbNew1.Append("        ELSE ");
				sbNew1.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew1.Append("      END) HBL_REF_NO , ");
				sbNew1.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sbNew1.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sbNew1.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sbNew1.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sbNew1.Append("                        0 CARGO_TYPE, ");
					}
				}
				// sbNew1.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," & CurrPK & ", INV.INVOICE_DATE,1),2) DEBIT,")
				sbNew1.Append("                        ROUND(   (select sum(round(t.amt_in_inv_curr,2)) from consol_invoice_trn_tbl t where t.job_card_fk=job.job_card_trn_pk)   * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sbNew1.Append("                        0 CREDIT,");
				sbNew1.Append("                        0 BALANCE,");
				sbNew1.Append("(SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sbNew1.Append("                             'DD/MM/YYYY') -");
				sbNew1.Append("                              TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");


				sbNew1.Append("                                   FROM COLLECTIONS_TRN_TBL COLL,");
				sbNew1.Append("                                        CONSOL_INVOICE_TBL  INV1");
				sbNew1.Append("                                  WHERE INV1.INVOICE_REF_NO =");
				sbNew1.Append("                                        COLL.INVOICE_REF_NR(+)");
				sbNew1.Append("                                    AND INV1.INVOICE_REF_NO =");
				sbNew1.Append("                                        INV.INVOICE_REF_NO");
				sbNew1.Append("                                    AND NVL((INV1.NET_RECEIVABLE -");
				sbNew1.Append("                                            NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sbNew1.Append("                                                   FROM COLLECTIONS_TRN_TBL CTRN");
				sbNew1.Append("                                                  WHERE CTRN.INVOICE_REF_NR LIKE");
				sbNew1.Append("                                                        INV1.INVOICE_REF_NO),");
				sbNew1.Append("                                                 0.00)),");
				sbNew1.Append("                                            0) > 0)");
				sbNew1.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew1.Append("               HBL_EXP_TBL            HBL,");
				sbNew1.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew1.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew1.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew1.Append("               LOCATION_MST_TBL       LMT,");
				sbNew1.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew1.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sbNew1.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew1.Append("         AND INV.IS_FAC_INV <> 1");
				sbNew1.Append("           AND INVTRN.JOB_TYPE = 1");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew1.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew1.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew1.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}

				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sbNew1.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew1.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew1.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew1.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew1.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew1.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew1.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew1.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew1.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sbNew1.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("            AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew1.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}
				sbNew1.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew1.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew1.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew1.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew1.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sbNew1.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sbNew1.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sbNew1.Append("        UNION");
				sbNew1.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew1.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew1.Append("                        TO_CHAR(COL.COLLECTIONS_DATE,DATEFORMAT) REF_DATE,");
				sbNew1.Append("                        (CASE");
				sbNew1.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sbNew1.Append("                           'EXPORT'");
				sbNew1.Append("                          ELSE");
				sbNew1.Append("                           'IMPORT'");
				sbNew1.Append("                        END) PROCESS,");
				sbNew1.Append("                        'COLLECTION' TRANSACTION,");
				sbNew1.Append("  ( CASE ");
				sbNew1.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("      HBL.HBL_REF_NO ");
				sbNew1.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("         HAWB.HAWB_REF_NO ");
				sbNew1.Append("        ELSE ");
				sbNew1.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew1.Append("      END) HBL_REF_NO , ");
				sbNew1.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sbNew1.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sbNew1.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sbNew1.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sbNew1.Append("                        0 CARGO_TYPE, ");
					}
				}
				sbNew1.Append("                        0 DEBIT,");
				sbNew1.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sbNew1.Append("                        0 BALANCE,''OUTSTANDING_DAYS");
				sbNew1.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew1.Append("               HBL_EXP_TBL            HBL,");
				sbNew1.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew1.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew1.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew1.Append("               LOCATION_MST_TBL       LMT,");
				sbNew1.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew1.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sbNew1.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sbNew1.Append("               COLLECTIONS_TBL        COL");
				sbNew1.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew1.Append("         AND INV.IS_FAC_INV <> 1");
				sbNew1.Append("           AND INVTRN.JOB_TYPE = 1");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew1.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew1.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew1.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sbNew1.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew1.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew1.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew1.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew1.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sbNew1.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew1.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew1.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew1.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew1.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew1.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sbNew1.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("            AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew1.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}
				sbNew1.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew1.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew1.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew1.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew1.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sbNew1.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sbNew1.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sbNew1.Append("        UNION");
				sbNew1.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew1.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew1.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE,DATEFORMAT) REF_DATE,");
				sbNew1.Append("                        (CASE");
				sbNew1.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sbNew1.Append("                           'EXPORT'");
				sbNew1.Append("                          ELSE");
				sbNew1.Append("                           'IMPORT'");
				sbNew1.Append("                        END) PROCESS,");
				sbNew1.Append("                        'CREDITNOTE' TRANSACTION,");
				sbNew1.Append("  ( CASE ");
				sbNew1.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("      HBL.HBL_REF_NO ");
				sbNew1.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("         HAWB.HAWB_REF_NO ");
				sbNew1.Append("        ELSE ");
				sbNew1.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew1.Append("      END) HBL_REF_NO , ");
				sbNew1.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sbNew1.Append("                         CNT.CRN_TBL_PK REF_PK,");
				sbNew1.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sbNew1.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sbNew1.Append("                        0 CARGO_TYPE, ");
					}
				}
				sbNew1.Append("                        0 DEBIT,");
				sbNew1.Append("                        ROUND(CNT.CRN_AMMOUNT * GET_EX_RATE(CNT.CURRENCY_MST_FK," + CurrPK + ",CNT.CREDIT_NOTE_DATE,1),2) CREDIT,");
				sbNew1.Append("                        0 BALANCE,''OUTSTANDING_DAYS");
				sbNew1.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew1.Append("               HBL_EXP_TBL            HBL,");
				sbNew1.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew1.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew1.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew1.Append("               LOCATION_MST_TBL       LMT,");
				sbNew1.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew1.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sbNew1.Append("               CREDIT_NOTE_TBL        CNT,");
				sbNew1.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sbNew1.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew1.Append("         AND INV.IS_FAC_INV <> 1");
				sbNew1.Append("           AND INVTRN.JOB_TYPE = 1");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew1.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew1.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew1.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sbNew1.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew1.Append("           AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew1.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew1.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew1.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sbNew1.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew1.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew1.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew1.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew1.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew1.Append("           0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType )== 1) {
					sbNew1.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("            AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew1.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}
				sbNew1.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew1.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew1.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew1.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew1.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sbNew1.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sbNew1.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}

				//-----Added By Sushama----
				//' ----CBJC(Invoice)--'
				sb.Append(sbNew1.ToString() + "        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM CBJC_TBL   CBJC,");
				sb.Append("              CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType )== 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2       ");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'    ----TPT(Tranport Note)(Invoice)--
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST  ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'       ----DET & DEM-TPT (Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.Biz_Type =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'       ----DET & DEM-CBJC (Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.Biz_Type =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'          ---CBJC(Collection)---------'
				sb.Append("              UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM ");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)          ");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 2");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'              ----TPT-(Collection)---------'
				sb.Append("        UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'         ---Det & DEM TPT (Collection)'
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK =  TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.biz_type =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND  COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'         ---Det & DEM CBJC (Collection)'
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.biz_type =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND  COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 4");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'   --------CBJC CR Note--
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM CBJC_TBL CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND CBJC.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND  CNT.CREDIT_NOTE_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'           ---TPT CrNote------
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'         --- DET & DEM TPT (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK =  TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.biz_type =" + BizType);
				}
				//'         --- DET & DEM CBJC (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.biz_type =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//----- End----
				sb.Append(") A,");
				sbNew2.Append("       (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew2.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew2.Append("                        TO_CHAR(INV.INVOICE_DATE,DATEFORMAT) REF_DATE,");
				sbNew2.Append("                        (CASE");
				sbNew2.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sbNew2.Append("                           'EXPORT'");
				sbNew2.Append("                          ELSE");
				sbNew2.Append("                           'IMPORT'");
				sbNew2.Append("                        END) PROCESS,");
				sbNew2.Append("                        'INVOICE' TRANSACTION,");
				sbNew2.Append("  ( CASE ");
				sbNew2.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("      HBL.HBL_REF_NO ");
				sbNew2.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("         HAWB.HAWB_REF_NO ");
				sbNew2.Append("        ELSE ");
				sbNew2.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew2.Append("      END) HBL_REF_NO , ");
				sbNew2.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sbNew2.Append("                        INV.CONSOL_INVOICE_PK  REF_PK,");
				sbNew2.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sbNew2.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sbNew2.Append("                        0 CARGO_TYPE, ");
					}
				}
				//  sbNew2.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," & CurrPK & ", INV.INVOICE_DATE,1),2) DEBIT,")
				sbNew2.Append("                        ROUND(   (select sum(round(t.amt_in_inv_curr,2)) from consol_invoice_trn_tbl t where t.job_card_fk=job.job_card_trn_pk)         * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sbNew2.Append("                        0 CREDIT,");
				sbNew2.Append("                        0 BALANCE,");
				sbNew2.Append("(SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sbNew2.Append("                             'DD/MM/YYYY') -");
				sbNew2.Append("                              TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");

				sbNew2.Append("                                   FROM COLLECTIONS_TRN_TBL COLL,");
				sbNew2.Append("                                        CONSOL_INVOICE_TBL  INV1");
				sbNew2.Append("                                  WHERE INV1.INVOICE_REF_NO =");
				sbNew2.Append("                                        COLL.INVOICE_REF_NR(+)");
				sbNew2.Append("                                    AND INV1.INVOICE_REF_NO =");
				sbNew2.Append("                                        INV.INVOICE_REF_NO");
				sbNew2.Append("                                    AND NVL((INV1.NET_RECEIVABLE -");
				sbNew2.Append("                                            NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sbNew2.Append("                                                   FROM COLLECTIONS_TRN_TBL CTRN");
				sbNew2.Append("                                                  WHERE CTRN.INVOICE_REF_NR LIKE");
				sbNew2.Append("                                                        INV1.INVOICE_REF_NO),");
				sbNew2.Append("                                                 0.00)),");
				sbNew2.Append("                                            0) > 0)");
				sbNew2.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew2.Append("               HBL_EXP_TBL            HBL,");
				sbNew2.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew2.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew2.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew2.Append("               LOCATION_MST_TBL       LMT,");
				sbNew2.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew2.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sbNew2.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew2.Append("         AND INV.IS_FAC_INV <> 1");
				sbNew2.Append("           AND INVTRN.JOB_TYPE = 1");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew2.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew2.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew2.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sbNew2.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew2.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew2.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew2.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew2.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew2.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew2.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew2.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew2.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sbNew2.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew2.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew2.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew2.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew2.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew2.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew2.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew2.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}
				sbNew2.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew2.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew2.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew2.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew2.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sbNew2.Append("           AND INV.invoice_date <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}

				sbNew2.Append("        UNION");
				sbNew2.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew2.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew2.Append("                        TO_CHAR(COL.COLLECTIONS_DATE,DATEFORMAT) REF_DATE,");
				sbNew2.Append("                        (CASE");
				sbNew2.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sbNew2.Append("                           'EXPORT'");
				sbNew2.Append("                          ELSE");
				sbNew2.Append("                           'IMPORT'");
				sbNew2.Append("                        END) PROCESS,");
				sbNew2.Append("                        'COLLECTION' TRANSACTION,");
				sbNew2.Append("  ( CASE ");
				sbNew2.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("      HBL.HBL_REF_NO ");
				sbNew2.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("         HAWB.HAWB_REF_NO ");
				sbNew2.Append("        ELSE ");
				sbNew2.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew2.Append("      END) HBL_REF_NO , ");
				sbNew2.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sbNew2.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sbNew2.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sbNew2.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sbNew2.Append("                        0 CARGO_TYPE, ");
					}
				}
				sbNew2.Append("                        0 DEBIT,");
				sbNew2.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sbNew2.Append("                        0 BALANCE,''OUTSTANDING_DAYS");
				sbNew2.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew2.Append("               HBL_EXP_TBL            HBL,");
				sbNew2.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew2.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew2.Append("               LOCATION_MST_TBL       LMT,");
				sbNew2.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew2.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew2.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sbNew2.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sbNew2.Append("               COLLECTIONS_TBL        COL");
				sbNew2.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew2.Append("         AND INV.IS_FAC_INV <> 1");
				sbNew2.Append("           AND INVTRN.JOB_TYPE = 1");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew2.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew2.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew2.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sbNew2.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew2.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew2.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew2.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew2.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sbNew2.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew2.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew2.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew2.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew2.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew2.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sbNew2.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew2.Append("            AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew2.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew2.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				}
				sbNew2.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew2.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew2.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew2.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew2.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sbNew2.Append("           AND COL.COLLECTIONS_DATE <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}

				sbNew2.Append("        UNION");
				sbNew2.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew2.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew2.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE,DATEFORMAT) REF_DATE,");
				sbNew2.Append("                        (CASE");
				sbNew2.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sbNew2.Append("                           'EXPORT'");
				sbNew2.Append("                          ELSE");
				sbNew2.Append("                           'IMPORT'");
				sbNew2.Append("                        END) PROCESS,");
				sbNew2.Append("                        'CREDITNOTE' TRANSACTION,");
				sbNew2.Append("  ( CASE ");
				sbNew2.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("      HBL.HBL_REF_NO ");
				sbNew2.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("         HAWB.HAWB_REF_NO ");
				sbNew2.Append("        ELSE ");
				sbNew2.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew2.Append("      END) HBL_REF_NO , ");
				sbNew2.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sbNew2.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sbNew2.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sbNew2.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sbNew2.Append("                        0 CARGO_TYPE, ");
					}
				}
				sbNew2.Append("                        0 DEBIT,");
				sbNew2.Append("                        ROUND(CNT.CRN_AMMOUNT * GET_EX_RATE(CNT.CURRENCY_MST_FK," + CurrPK + ",CNT.CREDIT_NOTE_DATE,1),2) CREDIT,");
				sbNew2.Append("                        0 BALANCE,''OUTSTANDING_DAYS");
				sbNew2.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew2.Append("               HBL_EXP_TBL            HBL,");
				sbNew2.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew2.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew2.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew2.Append("               LOCATION_MST_TBL       LMT,");
				sbNew2.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew2.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sbNew2.Append("               CREDIT_NOTE_TBL        CNT,");
				sbNew2.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sbNew2.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew2.Append("         AND INV.IS_FAC_INV <> 1");
				sbNew2.Append("           AND INVTRN.JOB_TYPE = 1");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                    
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew2.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew2.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew2.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sbNew2.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew2.Append("           AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew2.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}

				sbNew2.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew2.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sbNew2.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew2.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew2.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew2.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew2.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew2.Append("           0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sbNew2.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew2.Append("            AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew2.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew2.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				}
				sbNew2.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew2.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew2.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew2.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew2.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sbNew2.Append("           AND CNT.CREDIT_NOTE_DATE <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}

				//-----Added By Sushama----
				//' ----CBJC(Invoice)--'
				sb.Append(sbNew2.ToString());
				sb.Append(" UNION ");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM CBJC_TBL   CBJC,");
				sb.Append("              CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("           AND INV.invoice_date <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2       ");

				//'    ----TPT(Tranport Note)(Invoice)--
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST  ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("           AND INV.invoice_date  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'       ----DET & DEM TPT-(Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("              TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.Biz_Type =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("       AND INV.invoice_date  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'       ----DET & DEM-CBJC(Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.Biz_Type =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("       AND INV.invoice_date  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'          ---CBJC(Collection)---------'
				sb.Append("              UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM ");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)          ");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 2");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'              ----TPT-(Collection)---------'
				sb.Append("        UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'         ---Det & DEM TPT (Collection)'
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("                TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK =TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.biz_type =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("       AND  COL.COLLECTIONS_DATE   <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3     ");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'         ---Det & DEM CBJC (Collection)'
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.biz_type =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("       AND  COL.COLLECTIONS_DATE   <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4     ");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'   --------CBJC CR Note--
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM CBJC_TBL CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND CBJC.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("           AND  CNT.CREDIT_NOTE_DATE   <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'           ---TPT CrNote------
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'         --- DET & DEM TPT (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("                TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("       AND CNT.CREDIT_NOTE_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.biz_type =" + BizType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'         --- DET & DEM CBJC (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                       CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sb.Append("       AND CNT.CREDIT_NOTE_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.biz_type =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				sb.Append(") B");


				sb.Append(" WHERE C.CUSTOMER_MST_PK = A.CUSTOMER_MST_PK");
				sb.Append("   AND A.CUSTOMER_MST_PK = B.CUSTOMER_MST_PK(+)");
				sb.Append(" GROUP BY C.CUSTOMER_MST_PK,");
				sb.Append(" A.REF_PK,");
				sb.Append(" A.BIZ_TYPE,");
				sb.Append(" A.PROCESS,");
				sb.Append(" A.CARGO_TYPE");


				sb.Append("   UNION ");
				///'''''''''''''''''''''''''
				sbNew3.Append("SELECT CUSTOMER_MST_PK,");
				sbNew3.Append("       ADM_LOCATION_MST_FK,");
				sbNew3.Append("       REF_DATE,");
				sbNew3.Append("       PROCESS,");
				sbNew3.Append("       TRANSACTION,");
				sbNew3.Append("       HBL_REF_NO,");
				sbNew3.Append("       DOCREFNR,");
				sbNew3.Append("       DEBIT,");
				sbNew3.Append("       CREDIT,");
				sbNew3.Append("       BALANCE,");
				sbNew3.Append("       OUTSTANDING_DAYS,");
				sbNew3.Append("       REF_PK,");
				sbNew3.Append("       BIZ_TYPE,");
				sbNew3.Append("       CARGO_TYPE");

				sbNew3.Append("     FROM ( ");
				sbNew3.Append("       SELECT CUSTOMER_MST_PK, ADM_LOCATION_MST_FK,");
				sbNew3.Append("       REF_DATE,");
				sbNew3.Append("       PROCESS,");
				sbNew3.Append("       TRANSACTION,");
				sbNew3.Append("       HBL_REF_NO,");
				sbNew3.Append("       DOCREFNR,");
				sbNew3.Append("       DEBIT,");
				sbNew3.Append("       CREDIT,");
				sbNew3.Append("       BALANCE,");
				sbNew3.Append("       OUTSTANDING_DAYS,");
				sbNew3.Append("       REF_PK,");
				sbNew3.Append("       BIZ_TYPE,");
				sbNew3.Append("       CARGO_TYPE");

				sbNew3.Append("  FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK, CCD.ADM_LOCATION_MST_FK,");
				sbNew3.Append("                        TO_CHAR(INV.INVOICE_DATE,DATEFORMAT) REF_DATE,");
				sbNew3.Append("                        (CASE");
				sbNew3.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sbNew3.Append("                           'EXPORT'");
				sbNew3.Append("                          ELSE");
				sbNew3.Append("                           'IMPORT'");
				sbNew3.Append("                        END) PROCESS,");
				sbNew3.Append("                        'INVOICE' TRANSACTION,");
				sbNew3.Append("  ( CASE ");
				sbNew3.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew3.Append("      HBL.HBL_REF_NO ");
				sbNew3.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew3.Append("         HAWB.HAWB_REF_NO ");
				sbNew3.Append("        ELSE ");
				sbNew3.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew3.Append("      END) HBL_REF_NO , ");
				sbNew3.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sbNew3.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sbNew3.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew3.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sbNew3.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sbNew3.Append("                        0 CARGO_TYPE, ");
					}
				}
				//  sbNew3.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," & CurrPK & ", INV.INVOICE_DATE,1),2) DEBIT,")
				sbNew3.Append("                        ROUND(   (select sum(round(t.amt_in_inv_curr,2)) from consol_invoice_trn_tbl t where t.job_card_fk=job.job_card_trn_pk) * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sbNew3.Append("                        0 CREDIT,");
				sbNew3.Append("                        0 BALANCE,");
				sbNew3.Append("(SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sbNew3.Append("                             'DD/MM/YYYY') -");
				sbNew3.Append("                              TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sbNew3.Append("                                   FROM COLLECTIONS_TRN_TBL COLL,");
				sbNew3.Append("                                        CONSOL_INVOICE_TBL  INV1");
				sbNew3.Append("                                  WHERE INV1.INVOICE_REF_NO =");
				sbNew3.Append("                                        COLL.INVOICE_REF_NR(+)");
				sbNew3.Append("                                    AND INV1.INVOICE_REF_NO =");
				sbNew3.Append("                                        INV.INVOICE_REF_NO");
				sbNew3.Append("                                    AND NVL((INV1.NET_RECEIVABLE -");
				sbNew3.Append("                                            NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sbNew3.Append("                                                   FROM COLLECTIONS_TRN_TBL CTRN");
				sbNew3.Append("                                                  WHERE CTRN.INVOICE_REF_NR LIKE");
				sbNew3.Append("                                                        INV1.INVOICE_REF_NO),");
				sbNew3.Append("                                                 0.00)),");
				sbNew3.Append("                                            0) > 0)OUTSTANDING_DAYS");
				sbNew3.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew3.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew3.Append("               HBL_EXP_TBL            HBL,");
				sbNew3.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew3.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew3.Append("               CUSTOMER_CONTACT_DTLS CCD,");
				sbNew3.Append("               LOCATION_MST_TBL       LMT,");
				sbNew3.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew3.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sbNew3.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew3.Append("         AND INV.IS_FAC_INV <> 1");
				sbNew3.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew3.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                    
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew3.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew3.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew3.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew3.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew3.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew3.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sbNew3.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew3.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew3.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew3.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew3.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew3.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew3.Append("            AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				sbNew3.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew3.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew3.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew3.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew3.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sbNew3.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew3.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew3.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew3.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew3.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew3.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew3.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew3.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew3.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew3.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew3.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew3.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sbNew3.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sbNew3.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sbNew3.Append("           AND INVTRN.JOB_TYPE = 1 ");
				sbNew3.Append("           AND INV.CHK_INVOICE<>2 ");
				sbNew3.Append("        UNION");
				sbNew3.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK, CCD.ADM_LOCATION_MST_FK,");
				sbNew3.Append("                        TO_CHAR(COL.COLLECTIONS_DATE,DATEFORMAT) REF_DATE,");
				sbNew3.Append("                        (CASE");
				sbNew3.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sbNew3.Append("                           'EXPORT'");
				sbNew3.Append("                          ELSE");
				sbNew3.Append("                           'IMPORT'");
				sbNew3.Append("                        END) PROCESS,");
				sbNew3.Append("                        'COLLECTION' TRANSACTION,");
				sbNew3.Append("  ( CASE ");
				sbNew3.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew3.Append("      HBL.HBL_REF_NO ");
				sbNew3.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew3.Append("         HAWB.HAWB_REF_NO ");
				sbNew3.Append("        ELSE ");
				sbNew3.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew3.Append("      END) HBL_REF_NO , ");
				sbNew3.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sbNew3.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sbNew3.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew3.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sbNew3.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sbNew3.Append("                        0 CARGO_TYPE, ");
					}
				}
				sbNew3.Append("                        0 DEBIT,");
				sbNew3.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sbNew3.Append("                        0 BALANCE,''OUTSTANDING_DAYS");
				sbNew3.Append("          FROM JOB_CARD_TRN   JOB,");
				sbNew3.Append("               HBL_EXP_TBL            HBL,");
				sbNew3.Append("               HAWB_EXP_TBL            HAWB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew3.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew3.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew3.Append("               CUSTOMER_CONTACT_DTLS CCD,");
				sbNew3.Append("               LOCATION_MST_TBL       LMT,");
				sbNew3.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew3.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sbNew3.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sbNew3.Append("               COLLECTIONS_TBL        COL");
				sbNew3.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew3.Append("         AND INV.IS_FAC_INV <> 1");
				sbNew3.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew3.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew3.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew3.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew3.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew3.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew3.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew3.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sbNew3.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew3.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew3.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew3.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew3.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew3.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew3.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sbNew3.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sbNew3.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				sbNew3.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew3.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew3.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew3.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");

				sbNew3.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sbNew3.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew3.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew3.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew3.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew3.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew3.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew3.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew3.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}
				sbNew3.Append("           AND INVTRN.JOB_TYPE = 1 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew3.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew3.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew3.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				sbNew3.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew3.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sbNew3.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sbNew3.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}

				sbNew3.Append("        UNION");
				sbNew3.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK, CCD.ADM_LOCATION_MST_FK,");
				sbNew3.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE,DATEFORMAT) REF_DATE,");
				sbNew3.Append("                        (CASE");
				sbNew3.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sbNew3.Append("                           'EXPORT'");
				sbNew3.Append("                          ELSE");
				sbNew3.Append("                           'IMPORT'");
				sbNew3.Append("                        END) PROCESS,");
				sbNew3.Append("                        'CREDITNOTE' TRANSACTION,");
				sbNew3.Append("  ( CASE ");
				sbNew3.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew3.Append("      HBL.HBL_REF_NO ");
				sbNew3.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew3.Append("         HAWB.HAWB_REF_NO ");
				sbNew3.Append("        ELSE ");
				sbNew3.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew3.Append("      END) HBL_REF_NO , ");
				sbNew3.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sbNew3.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sbNew3.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew3.Append("                        BKG.CARGO_TYPE CARGO_TYPE,");
				} else {
					if (Convert.ToInt32(BizType) == 2) {
						sbNew3.Append("                        JOB.CARGO_TYPE CARGO_TYPE, ");
					} else {
						sbNew3.Append("                        0 CARGO_TYPE, ");
					}
				}
				sbNew3.Append("                        0 DEBIT,");
				sbNew3.Append("                        ROUND(CNT.CRN_AMMOUNT * GET_EX_RATE(CNT.CURRENCY_MST_FK," + CurrPK + ",CNT.CREDIT_NOTE_DATE,1),2) CREDIT,");
				sbNew3.Append("                        0 BALANCE,''OUTSTANDING_DAYS");
				sbNew3.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew3.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew3.Append("               HBL_EXP_TBL            HBL,");
				sbNew3.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew3.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew3.Append("               CUSTOMER_CONTACT_DTLS CCD,");
				sbNew3.Append("               LOCATION_MST_TBL       LMT,");
				sbNew3.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew3.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sbNew3.Append("               CREDIT_NOTE_TBL        CNT,");
				sbNew3.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sbNew3.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew3.Append("         AND INV.IS_FAC_INV <> 1");
				sbNew3.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew3.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                 
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew3.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew3.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew3.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew3.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew3.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew3.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else if (Convert.ToInt32(ProcessType) == 2) {
					sbNew3.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew3.Append("           AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew3.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew3.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew3.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew3.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sbNew3.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sbNew3.Append("            AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				sbNew3.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew3.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew3.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew3.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew3.Append("           0.00) = 0");
				sbNew3.Append("           AND INVTRN.JOB_TYPE = 1 ");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sbNew3.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew3.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew3.Append("  AND JOB.CARRIER_MST_FK IN(" + VslVoyPK + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew3.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew3.Append("            AND JOB.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew3.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew3.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew3.Append("  AND CASE WHEN JOB.BUSINESS_TYPE=1 THEN JOB.CARRIER_MST_FK ELSE JOB.CARRIER_MST_FK END IN(" + VslVoyPK + ")");
					}
				}
				sbNew3.Append("           AND INV.CHK_INVOICE<>2 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew3.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew3.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew3.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew3.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sbNew3.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sbNew3.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}

				//-----Added By Sushama----
				//' ----CBJC(Invoice)--'
				sb.Append(sbNew3.ToString() + "          UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM CBJC_TBL   CBJC,");
				sb.Append("              CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}

				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2       ");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");

				//'    ----TPT(Tranport Note)(Invoice)--
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST  ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'       ----DET & DEM-TPT(Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("                TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.Biz_Type =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'       ----DET & DEM-CBJC(Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(INV.INVOICE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        INV.CONSOL_INVOICE_PK REF_PK,");
				sb.Append("                        INV.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        (SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE,");
				sb.Append("                                                         'DD/MM/YYYY') -");
				sb.Append("                                                 TO_DATE(INV.INVOICE_DATE)) OUTSTANDING_DAYS");
				sb.Append("                           FROM COLLECTIONS_TRN_TBL COLL,");
				sb.Append("                                CONSOL_INVOICE_TBL  INV1");
				sb.Append("                          WHERE INV1.INVOICE_REF_NO = COLL.INVOICE_REF_NR(+)");
				sb.Append("                            AND INV1.INVOICE_REF_NO = INV.INVOICE_REF_NO");
				sb.Append("                            AND NVL((INV1.NET_RECEIVABLE -");
				sb.Append("                                    NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
				sb.Append("                                           FROM COLLECTIONS_TRN_TBL CTRN");
				sb.Append("                                          WHERE CTRN.INVOICE_REF_NR LIKE");
				sb.Append("                                                INV1.INVOICE_REF_NO),");
				sb.Append("                                         0.00)),");
				sb.Append("                                    0) > 0)");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.Biz_Type =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");

				//'          ---CBJC(Collection)---------'
				sb.Append("              UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM ");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)          ");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 2");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'              ----TPT-(Collection)---------'
				sb.Append("        UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");

				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'         ---Det & DEM TPT (Collection)'
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK(+) = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND  COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3 ");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'         ---Det & DEM CBJC (Collection)'
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(COL.COLLECTIONS_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        COL.COLLECTIONS_TBL_PK REF_PK,");
				sb.Append("                        COL.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND  COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4 ");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'   --------CBJC CR Note--
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        CBJC.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM CBJC_TBL CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND CBJC.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND  CNT.CREDIT_NOTE_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND CBJC.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND CBJC.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'           ---TPT CrNote------
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        TIST.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				if (CargoType != 0) {
					sb.Append("     AND TIST.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'         --- DET & DEM TPT (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND TIST.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN TIST.BUSINESS_TYPE=1 THEN TIST.OPERATOR_MST_FK ELSE TIST.Vsl_Voy_Fk END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND tist.pol_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND tist.pod_fk IN(" + PODPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				//'         --- DET & DEM CBJC(CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                        TO_CHAR(CNT.CREDIT_NOTE_DATE, DATEFORMAT) REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        CNT.CRN_TBL_PK REF_PK,");
				sb.Append("                        CNT.BIZ_TYPE BIZ_TYPE,");
				sb.Append("                        DCH.CARGO_TYPE CARGO_TYPE,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK IN(" + VslVoyPK + ")");
					}
				} else {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND CBJC.FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("  AND CASE WHEN CBJC.BIZ_TYPE=1 THEN CBJC.OPERATOR_MST_FK ELSE CBJC.VOYAGE_TRN_FK END IN(" + VslVoyPK + ")");
					}
				}
				if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
					sb.Append("         AND cbjc.pol_mst_fk IN(" + POLPK + ")");
				}
				if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
					sb.Append("         AND cbjc.pod_mst_fk IN(" + PODPK + ")");
				}
				if (CargoType != 0) {
					sb.Append("     AND DCH.CARGO_TYPE=" + CargoType);
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("       AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}
				sb.Append("           AND INV.CHK_INVOICE<>2 ");
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//----- End----
				///'--------------------Deposit Amount---------------------------
				sb.Append("        UNION");
				sb.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        LMT.LOCATION_MST_PK,");
				sb.Append("                        TO_CHAR(CDMT.DEPOSIT_DATE,DATEFORMAT) REF_DATE,");
				sb.Append("                        'IMPORT'  PROCESS,");
				sb.Append("                        'CONTAINER DEPOSIT' TRANSACTION,");
				sb.Append("                         JOB.HBL_HAWB_REF_NO   HBL_REF_NO, ");
				sb.Append("                        CDMT.DEPOSIT_REF_NR DOCREFNR,");
				sb.Append("                        CDDT.CONTAINER_DEPOSIT_DTL_PK  REF_PK,");
				sb.Append("                        JOB.BUSINESS_TYPE BIZ_TYPE,");
				sb.Append("                        JOB.JOB_CARD_TRN_PK CARGO_TYPE, ");
				sb.Append("                        CASE WHEN CDDT.STATUS = 1 THEN ");
				sb.Append("                        ROUND(CDDT.AMOUNT * GET_EX_RATE(CDDT.CURRENCY_MST_FK, ");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CDMT.DEPOSIT_DATE,1),");
				sb.Append("                        2) ELSE 0 END DEBIT,");
				sb.Append("                       CASE WHEN CDDT.STATUS = 3 THEN ");
				sb.Append("                        ROUND(CDDT.AMOUNT * GET_EX_RATE(CDDT.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CDMT.DEPOSIT_DATE,1),");
				sb.Append("                        2) ELSE 0 END CREDIT,");
				sb.Append("                        0 BALANCE,");
				sb.Append("                        '' OUTSTANDING_DAYS");
				sb.Append("          FROM JOB_CARD_TRN           JOB,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONTAINER_DEPOSIT_MST_TBL CDMT,");
				sb.Append("               CONTAINER_DEPOSIT_DTL_TBL CDDT");
				sb.Append("         WHERE LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND CDMT.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
				sb.Append("           AND CDDT.CONTAINER_DEPOSIT_MST_FK = CDMT.CONTAINER_DEPOSIT_MST_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				sb.Append("           AND JOB.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND CDDT.STATUS IN (1,3) ");
				//'Import
				if (Convert.ToInt32(ProcessType) == 2) {
					sb.Append("         AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sb.Append("         AND JOB.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sb.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sb.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				} else {
					sb.Append("         AND JOB.PROCESS_TYPE = 1");
				}

				//'SEA
				if (Convert.ToInt32(BizType) == 2) {
					sb.Append("           AND JOB.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("            AND JOB.VOYAGE_TRN_FK IN (" + VslVoyPK + ")");
					}
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN ('" + FlightID.Replace(",", "','") + "')");
					}
				} else if (Convert.ToInt32(BizType) == 1) {
					sb.Append("         AND JOB.BUSINESS_TYPE = 1");
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.REF_GROUP_CUST_PK IN(" + CustGroupPK + ")");
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CDMT.DEPOSIT_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				///'--------------------Deposit Amount---------------------------
				sb.Append("    ) ORDER BY REF_DATE) ");

				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "DETAILS");
				DataRelation relLocGroup_Details = new DataRelation("LOCGROUP", new DataColumn[] { MainDS.Tables[0].Columns["GROUPID"] }, new DataColumn[] { MainDS.Tables[1].Columns["GROUPID"] });
				DataRelation relLocCustomer_Details = new DataRelation("LOCCUSTOMER", new DataColumn[] { MainDS.Tables[1].Columns["LOCATION_MST_PK"] }, new DataColumn[] { MainDS.Tables[2].Columns["ADM_LOCATION_MST_FK"] });
				DataRelation relCustomer_Details = new DataRelation("CUSTOMERDETAILS", new DataColumn[] { MainDS.Tables[2].Columns["CUSTOMER_MST_PK"] }, new DataColumn[] { MainDS.Tables[3].Columns["CUSTOMER_MST_PK"] });

				relLocGroup_Details.Nested = true;
				relLocCustomer_Details.Nested = true;
				relCustomer_Details.Nested = true;
				MainDS.Relations.Add(relLocGroup_Details);
				MainDS.Relations.Add(relLocCustomer_Details);
				MainDS.Relations.Add(relCustomer_Details);
				return MainDS;
				//Return objWF.GetDataSet(strQuery.ToString)
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		#endregion
		#region "Report Information"
		public DataSet FetchLocationReport(string Fromdate = "", string Todate = "", string POLPK = "", string PODPK = "", string CountryPK = "", string LocPK = "", string CustPK = "", string BizType = "", string ProcessType = "", string CustGroupPK = "",
		string CarrierPK = "", string VslVoyPK = "", string FlightID = "", int CargoType = 0, string CurrPK = "")
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			var sbNew1 = new System.Text.StringBuilder();
			var sbNew2 = new System.Text.StringBuilder();
			System.Text.StringBuilder SbNew3 = new System.Text.StringBuilder(5000);
			try {
				sb.Append("SELECT ROWNUM \"SL.NR\", T.*");
				sb.Append(" From (SELECT DISTINCT  C.CUSTOMER_MST_PK,");
				sbNew1.Append("       D.ADM_LOCATION_MST_FK,");
				sbNew1.Append("       ''CUSTOMER_ID, ");
				sbNew1.Append("       C.CUSTOMER_NAME,");
				sbNew1.Append("       ''LOCATION_ID,");
				sbNew1.Append("       L.LOCATION_NAME,");
				sbNew1.Append("       TO_DATE('" + Fromdate + "') REF_DATE,");
				sbNew1.Append("       ''PROCESS,");
				sbNew1.Append("       'OPENING BALANCE' TRANSACTION,");
				sbNew1.Append("       ''SHIPMENT_REF_NR,");
				sbNew1.Append("       ''DOCREFNR,");
				sbNew1.Append("       0 DEBIT,");
				sbNew1.Append("       0 CREDIT,");
				sbNew1.Append("       0 BALANCE");
				sbNew1.Append("  FROM CUSTOMER_MST_TBL C,CUSTOMER_CONTACT_DTLS D, LOCATION_MST_TBL L,");
				sbNew1.Append("       (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew1.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew1.Append("        CMT.CUSTOMER_ID,");
				sbNew1.Append("        CMT.CUSTOMER_NAME,");
				sbNew1.Append("        LMT.LOCATION_ID, ");
				sbNew1.Append("        LMT.LOCATION_NAME,");
				sbNew1.Append("                        INV.INVOICE_DATE REF_DATE,");
				sbNew1.Append("                        (CASE");
				sbNew1.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sbNew1.Append("                           'EXPORT'");
				sbNew1.Append("                          ELSE");
				sbNew1.Append("                           'IMPORT'");
				sbNew1.Append("                        END) PROCESS,");
				sbNew1.Append("                        'INVOICE' TRANSACTION,");
				sbNew1.Append("  ( CASE ");
				sbNew1.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("      HBL.HBL_REF_NO ");
				sbNew1.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("         HAWB.HAWB_REF_NO ");
				sbNew1.Append("        ELSE ");
				sbNew1.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew1.Append("      END) SHIPMENT_REF_NR , ");
				sbNew1.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				// sbNew1.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," & CurrPK & ", INV.INVOICE_DATE,1),2) DEBIT,")
				sbNew1.Append("                        ROUND(   (select sum(round(t.amt_in_inv_curr,2)) from consol_invoice_trn_tbl t where t.job_card_fk=job.job_card_trn_pk)         * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sbNew1.Append("                        0 CREDIT,");
				sbNew1.Append("                        0 BALANCE");
				sbNew1.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew1.Append("               HBL_EXP_TBL            HBL,");
				sbNew1.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew1.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew1.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew1.Append("               LOCATION_MST_TBL       LMT,");
				sbNew1.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew1.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sbNew1.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew1.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew1.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew1.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}

				//'Import
				} else {
					sbNew1.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew1.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew1.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew1.Append("       AND INV.IS_FAC_INV <> 1    AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew1.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew1.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew1.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew1.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew1.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sbNew1.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew1.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew1.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew1.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew1.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew1.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				sbNew1.Append("           AND INVTRN.JOB_TYPE = 1      ");
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sbNew1.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sbNew1.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sbNew1.Append("        UNION");
				sbNew1.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew1.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew1.Append("        CMT.CUSTOMER_ID,");
				sbNew1.Append("        CMT.CUSTOMER_NAME,");
				sbNew1.Append("        LMT.LOCATION_ID, ");
				sbNew1.Append("        LMT.LOCATION_NAME,");
				sbNew1.Append("                        COL.COLLECTIONS_DATE REF_DATE,");
				sbNew1.Append("                        (CASE");
				sbNew1.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sbNew1.Append("                           'EXPORT'");
				sbNew1.Append("                          ELSE");
				sbNew1.Append("                           'IMPORT'");
				sbNew1.Append("                        END) PROCESS,");
				sbNew1.Append("                        'COLLECTION' TRANSACTION,");
				sbNew1.Append("  ( CASE ");
				sbNew1.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("      HBL.HBL_REF_NO ");
				sbNew1.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("         HAWB.HAWB_REF_NO ");
				sbNew1.Append("        ELSE ");
				sbNew1.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew1.Append("      END) SHIPMENT_REF_NR , ");
				sbNew1.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sbNew1.Append("                        0 DEBIT,");
				sbNew1.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sbNew1.Append("                        0 BALANCE");
				sbNew1.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew1.Append("               HBL_EXP_TBL            HBL,");
				sbNew1.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew1.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew1.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew1.Append("               LOCATION_MST_TBL       LMT,");
				sbNew1.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew1.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sbNew1.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sbNew1.Append("               COLLECTIONS_TBL        COL");
				sbNew1.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew1.Append("           AND INVTRN.JOB_TYPE = 1      ");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew1.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew1.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew1.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sbNew1.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew1.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew1.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew1.Append("           AND INV.IS_FAC_INV <> 1 AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew1.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sbNew1.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew1.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew1.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew1.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew1.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew1.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sbNew1.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew1.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew1.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew1.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew1.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew1.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sbNew1.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sbNew1.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				sbNew1.Append("        UNION");
				sbNew1.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew1.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew1.Append("        CMT.CUSTOMER_ID,");
				sbNew1.Append("        CMT.CUSTOMER_NAME,");
				sbNew1.Append("        LMT.LOCATION_ID, ");
				sbNew1.Append("        LMT.LOCATION_NAME,");
				sbNew1.Append("                        CNT.CREDIT_NOTE_DATE REF_DATE,");
				sbNew1.Append("                        (CASE");
				sbNew1.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sbNew1.Append("                           'EXPORT'");
				sbNew1.Append("                          ELSE");
				sbNew1.Append("                           'IMPORT'");
				sbNew1.Append("                        END) PROCESS,");
				sbNew1.Append("                        'CREDITNOTE' TRANSACTION,");
				sbNew1.Append("  ( CASE ");
				sbNew1.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("      HBL.HBL_REF_NO ");
				sbNew1.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew1.Append("         HAWB.HAWB_REF_NO ");
				sbNew1.Append("        ELSE ");
				sbNew1.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew1.Append("      END) SHIPMENT_REF_NR , ");
				sbNew1.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sbNew1.Append("                        0 DEBIT,");
				sbNew1.Append("                        ROUND(CNT.CRN_AMMOUNT * GET_EX_RATE(CNT.CURRENCY_MST_FK," + CurrPK + ",CNT.CREDIT_NOTE_DATE,1),2) CREDIT,");
				sbNew1.Append("                        0 BALANCE");
				sbNew1.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew1.Append("               HBL_EXP_TBL            HBL,");
				sbNew1.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew1.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew1.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew1.Append("               LOCATION_MST_TBL       LMT,");
				sbNew1.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew1.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sbNew1.Append("               CREDIT_NOTE_TBL        CNT,");
				sbNew1.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sbNew1.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew1.Append("           AND INVTRN.JOB_TYPE = 1      ");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew1.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew1.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew1.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew1.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sbNew1.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew1.Append("           AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew1.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew1.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew1.Append("         AND INV.IS_FAC_INV <> 1  AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew1.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sbNew1.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sbNew1.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew1.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew1.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew1.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew1.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew1.Append("           0.00) = 0");

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sbNew1.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew1.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew1.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew1.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew1.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew1.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew1.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew1.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sbNew1.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sbNew1.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}

				sb.Append(sbNew1.ToString());
				///Added By Sushama
				sb.Append(" UNION");
				//'        ----CBJC(Invoice)--     
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                          CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO SHIPMENT_REF_NR,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("         FROM CBJC_TBL   CBJC,");
				sb.Append("              CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("        AND    CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				sb.Append("           AND INVTRN.JOB_TYPE = 2      ");

				//'     ----TPT(Tranport Note)(Invoice)--
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT  CMT.CUSTOMER_MST_PK,");
				sb.Append("                            CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                            CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("         FROM TRANSPORT_INST_SEA_TBL TIST  ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'      ----DET & DEM- TPT(Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                            CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                            CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                         ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'      ----DET & DEM- CBJC(Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                            CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                            CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                         ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("         AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				//'      ----CBJC (collection)-
				sb.Append("              UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                     ");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE                      ");
				sb.Append("          FROM ");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK        ");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("     AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("               AND INVTRN.JOB_TYPE = 2");
				//'         ----TPT-(Collection)--------
				sb.Append("        UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                         0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				//'       ---Det & DEM TPT (Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK =TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("               AND INVTRN.JOB_TYPE = 4         ");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				//'       ---Det & DEM CBJC (Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("               AND INVTRN.JOB_TYPE = 4         ");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("         AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				//'  ---CBJC (CR Note)----
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM CBJC_TBL CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND CBJC.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("        AND    CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				//'         ---TPT CrNote------
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'          --- DET & DEM TPT (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK =  TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'          --- DET & DEM CBJC (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("         AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				//---- End Sushama -----
				sb.Append(" ) A,");
				/// 
				sb.Append("       (SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew2.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew2.Append("        CMT.CUSTOMER_ID,");
				sbNew2.Append("        CMT.CUSTOMER_NAME,");
				sbNew2.Append("        LMT.LOCATION_ID, ");
				sbNew2.Append("        LMT.LOCATION_NAME,");
				sbNew2.Append("                       INV.INVOICE_DATE REF_DATE,");
				sbNew2.Append("                        (CASE");
				sbNew2.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sbNew2.Append("                           'EXPORT'");
				sbNew2.Append("                          ELSE");
				sbNew2.Append("                           'IMPORT'");
				sbNew2.Append("                        END) PROCESS,");
				sbNew2.Append("                        'INVOICE' TRANSACTION,");
				sbNew2.Append("  ( CASE ");
				sbNew2.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("      HBL.HBL_REF_NO ");
				sbNew2.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("         HAWB.HAWB_REF_NO ");
				sbNew2.Append("        ELSE ");
				sbNew2.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew2.Append("      END) SHIPMENT_REF_NR , ");
				sbNew2.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				// sbNew2.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," & CurrPK & ", INV.INVOICE_DATE,1),2) DEBIT,")
				sbNew2.Append("                        ROUND(   (select sum(round(t.amt_in_inv_curr,2)) from consol_invoice_trn_tbl t where t.job_card_fk=job.job_card_trn_pk)         * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");
				sbNew2.Append("                        0 CREDIT,");
				sbNew2.Append("                        0 BALANCE");
				sbNew2.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew2.Append("               HBL_EXP_TBL            HBL,");
				sbNew2.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew2.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew2.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew2.Append("               LOCATION_MST_TBL       LMT,");
				sbNew2.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew2.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sbNew2.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew2.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew2.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew2.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sbNew2.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew2.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew2.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew2.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew2.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew2.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew2.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew2.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew2.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sbNew2.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew2.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew2.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew2.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew2.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew2.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew2.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew2.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sbNew2.Append("           AND INV.invoice_date <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sbNew2.Append("         AND INV.IS_FAC_INV <> 1   AND INVTRN.JOB_TYPE = 1 ");
				sbNew2.Append("        UNION");
				sbNew2.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew2.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew2.Append("        CMT.CUSTOMER_ID,");
				sbNew2.Append("        CMT.CUSTOMER_NAME,");
				sbNew2.Append("        LMT.LOCATION_ID, ");
				sbNew2.Append("        LMT.LOCATION_NAME,");
				sbNew2.Append("                        COL.COLLECTIONS_DATE REF_DATE,");
				sbNew2.Append("                        (CASE");
				sbNew2.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sbNew2.Append("                           'EXPORT'");
				sbNew2.Append("                          ELSE");
				sbNew2.Append("                           'IMPORT'");
				sbNew2.Append("                        END) PROCESS,");
				sbNew2.Append("                        'COLLECTION' TRANSACTION,");
				sbNew2.Append("  ( CASE ");
				sbNew2.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("      HBL.HBL_REF_NO ");
				sbNew2.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("         HAWB.HAWB_REF_NO ");
				sbNew2.Append("        ELSE ");
				sbNew2.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew2.Append("      END) SHIPMENT_REF_NR , ");
				sbNew2.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sbNew2.Append("                        0 DEBIT,");
				sbNew2.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				sbNew2.Append("                        0 BALANCE");
				sbNew2.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew2.Append("               HBL_EXP_TBL            HBL,");
				sbNew2.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew2.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew2.Append("               LOCATION_MST_TBL       LMT,");
				sbNew2.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew2.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew2.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sbNew2.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sbNew2.Append("               COLLECTIONS_TBL        COL");
				sbNew2.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew2.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew2.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew2.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sbNew2.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew2.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew2.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew2.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew2.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sbNew2.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew2.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew2.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew2.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew2.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew2.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					sbNew2.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew2.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew2.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew2.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew2.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew2.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew2.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew2.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sbNew2.Append("           AND COL.COLLECTIONS_DATE <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sbNew2.Append("          AND INV.IS_FAC_INV <> 1  AND INVTRN.JOB_TYPE = 1 ");
				sbNew2.Append("        UNION");
				sbNew2.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sbNew2.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sbNew2.Append("        CMT.CUSTOMER_ID,");
				sbNew2.Append("        CMT.CUSTOMER_NAME,");
				sbNew2.Append("        LMT.LOCATION_ID, ");
				sbNew2.Append("        LMT.LOCATION_NAME,");
				sbNew2.Append("                       CNT.CREDIT_NOTE_DATE REF_DATE,");
				sbNew2.Append("                        (CASE");
				sbNew2.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sbNew2.Append("                           'EXPORT'");
				sbNew2.Append("                          ELSE");
				sbNew2.Append("                           'IMPORT'");
				sbNew2.Append("                        END) PROCESS,");
				sbNew2.Append("                        'CREDITNOTE' TRANSACTION,");
				sbNew2.Append("  ( CASE ");
				sbNew2.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("      HBL.HBL_REF_NO ");
				sbNew2.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				sbNew2.Append("         HAWB.HAWB_REF_NO ");
				sbNew2.Append("        ELSE ");
				sbNew2.Append("       JOB.HBL_HAWB_REF_NO  ");
				sbNew2.Append("      END) SHIPMENT_REF_NR , ");
				sbNew2.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sbNew2.Append("                        0 DEBIT,");
				sbNew2.Append("                        ROUND(CNT.CRN_AMMOUNT * GET_EX_RATE(CNT.CURRENCY_MST_FK," + CurrPK + ",CNT.CREDIT_NOTE_DATE,1),2) CREDIT,");
				sbNew2.Append("                        0 BALANCE");
				sbNew2.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("               BOOKING_MST_TBL        BKG,");
				}
				sbNew2.Append("               HBL_EXP_TBL            HBL,");
				sbNew2.Append("               HAWB_EXP_TBL            HAWB,");
				sbNew2.Append("               CUSTOMER_MST_TBL       CMT,");
				sbNew2.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sbNew2.Append("               LOCATION_MST_TBL       LMT,");
				sbNew2.Append("               CONSOL_INVOICE_TBL     INV,");
				sbNew2.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sbNew2.Append("               CREDIT_NOTE_TBL        CNT,");
				sbNew2.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sbNew2.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				sbNew2.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                   
				if (Convert.ToInt32(ProcessType) == 1) {
					sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					sbNew2.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					sbNew2.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						sbNew2.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					sbNew2.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					sbNew2.Append("           AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						sbNew2.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						sbNew2.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				sbNew2.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sbNew2.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sbNew2.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sbNew2.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sbNew2.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sbNew2.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sbNew2.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				sbNew2.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sbNew2.Append("           0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					sbNew2.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						sbNew2.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				} else if (Convert.ToInt32(BizType) == 2) {
					sbNew2.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sbNew2.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sbNew2.Append("       AND INV.IS_FAC_INV <> 1    AND INVTRN.JOB_TYPE = 1 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sbNew2.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sbNew2.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sbNew2.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sbNew2.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString())) {
					sbNew2.Append("           AND CNT.CREDIT_NOTE_DATE <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				///Added By Sushama

				sb.Append(sbNew2.ToString());
				sb.Append(" UNION");
				//'        ----CBJC(Invoice)--     
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                          CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO SHIPMENT_REF_NR,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("         FROM CBJC_TBL   CBJC,");
				sb.Append("              CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");

				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("        AND    CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				sb.Append("           AND INVTRN.JOB_TYPE = 2      ");

				//'     ----TPT(Tranport Note)(Invoice)--
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT  CMT.CUSTOMER_MST_PK,");
				sb.Append("                            CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                            CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("         FROM TRANSPORT_INST_SEA_TBL TIST  ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'      ----DET & DEM-(Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                            CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                            CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                         ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);

				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'      ----DET & DEM-CBJC(Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                            CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                            CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                         ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);

				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("         AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				//'      ----CBJC (collection)-
				sb.Append("              UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                     ");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE                      ");
				sb.Append("          FROM ");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK        ");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("     AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 2");
				//'         ----TPT-(Collection)--------
				sb.Append("        UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                         0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				//'       ---Det & DEM TPT (Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3         ");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				//'       ---Det & DEM CBJC(Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 4         ");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("         AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				//'  ---CBJC (CR Note)----
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM CBJC_TBL CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND CBJC.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("        AND    CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				//'         ---TPT CrNote------
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'          --- DET & DEM TPT (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'          --- DET & DEM CBJC (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  <= TO_DATE('" + Fromdate + "', '" + dateFormat + "')");
				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("         AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				//'-----Sushama-end-----
				sb.Append(") B");
				sb.Append(" WHERE C.CUSTOMER_MST_PK = A.CUSTOMER_MST_PK");
				sb.Append("    AND A.CUSTOMER_MST_PK = B.CUSTOMER_MST_PK(+)");
				sb.Append("    AND D.ADM_LOCATION_MST_FK = A.ADM_LOCATION_MST_FK");
				// sb.Append("    AND D.ADM_LOCATION_MST_FK = B.ADM_LOCATION_MST_FK")
				sb.Append("   AND L.LOCATION_MST_PK = D.ADM_LOCATION_MST_FK");
				sb.Append("   AND L.LOCATION_MST_PK = A.ADM_LOCATION_MST_FK");
				//sb.Append("   AND L.LOCATION_MST_PK = B.ADM_LOCATION_MST_FK")
				//sb.Append("     GROUP BY C.CUSTOMER_MST_PK,D.ADM_LOCATION_MST_FK ")
				sb.Append("   UNION ");
				///'''''''''''''''''''''''''
				sb.Append("SELECT CUSTOMER_MST_PK,");
				sb.Append("               ADM_LOCATION_MST_FK,");
				sb.Append("               CUSTOMER_ID,");
				sb.Append("               CUSTOMER_NAME,");
				sb.Append("               LOCATION_ID,");
				sb.Append("               LOCATION_NAME,");
				sb.Append("               REF_DATE,");
				sb.Append("               PROCESS,");
				sb.Append("               TRANSACTION,");
				sb.Append("               SHIPMENT_REF_NR,");
				sb.Append("               DOCREFNR,");
				sb.Append("               DEBIT,");
				sb.Append("               CREDIT,");
				sb.Append("               BALANCE");
				sb.Append("               FROM(");
				sb.Append("       SELECT CUSTOMER_MST_PK,ADM_LOCATION_MST_FK,");
				sb.Append("       CUSTOMER_ID,");
				sb.Append("       CUSTOMER_NAME,");
				sb.Append("       LOCATION_ID, ");
				sb.Append("       LOCATION_NAME,");
				sb.Append("       REF_DATE,");
				sb.Append("       PROCESS,");
				sb.Append("       TRANSACTION,");
				sb.Append("       SHIPMENT_REF_NR,");
				sb.Append("       DOCREFNR,");
				sb.Append("       DEBIT,");
				sb.Append("       CREDIT,");
				sb.Append("       BALANCE");
				sb.Append("  FROM (SELECT DISTINCT CMT.CUSTOMER_MST_PK, CCD.ADM_LOCATION_MST_FK,");
				SbNew3.Append("        CMT.CUSTOMER_ID,");
				SbNew3.Append("        CMT.CUSTOMER_NAME,");
				SbNew3.Append("        LMT.LOCATION_ID, ");
				SbNew3.Append("        LMT.LOCATION_NAME,");
				SbNew3.Append("                        INV.INVOICE_DATE REF_DATE,");
				SbNew3.Append("                        (CASE");
				SbNew3.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				SbNew3.Append("                           'EXPORT'");
				SbNew3.Append("                          ELSE");
				SbNew3.Append("                           'IMPORT'");
				SbNew3.Append("                        END) PROCESS,");
				SbNew3.Append("                        'INVOICE' TRANSACTION,");
				SbNew3.Append("  ( CASE ");
				SbNew3.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				SbNew3.Append("      HBL.HBL_REF_NO ");
				SbNew3.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				SbNew3.Append("         HAWB.HAWB_REF_NO ");
				SbNew3.Append("        ELSE ");
				SbNew3.Append("       JOB.HBL_HAWB_REF_NO  ");
				SbNew3.Append("      END) SHIPMENT_REF_NR , ");
				SbNew3.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				// SbNew3.Append("                        ROUND(INV.NET_RECEIVABLE * GET_EX_RATE(INV.CURRENCY_MST_FK," & CurrPK & ", INV.INVOICE_DATE,1),2) DEBIT,")
				SbNew3.Append("                        ROUND(   (select sum(round(t.amt_in_inv_curr,2)) from consol_invoice_trn_tbl t where t.job_card_fk=job.job_card_trn_pk)         * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE,1),2) DEBIT,");

				SbNew3.Append("                        0 CREDIT,");
				SbNew3.Append("                        0 BALANCE");
				SbNew3.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					SbNew3.Append("               BOOKING_MST_TBL        BKG,");
				}
				SbNew3.Append("               HBL_EXP_TBL            HBL,");
				SbNew3.Append("               HAWB_EXP_TBL            HAWB,");
				SbNew3.Append("               CUSTOMER_MST_TBL       CMT,");
				SbNew3.Append("               CUSTOMER_CONTACT_DTLS CCD,");
				SbNew3.Append("               LOCATION_MST_TBL       LMT,");
				SbNew3.Append("               CONSOL_INVOICE_TBL     INV,");
				SbNew3.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				SbNew3.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				SbNew3.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				SbNew3.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                    
				if (Convert.ToInt32(ProcessType) == 1) {
					SbNew3.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					SbNew3.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					SbNew3.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						SbNew3.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						SbNew3.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						SbNew3.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					SbNew3.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					SbNew3.Append("           AND INV.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						SbNew3.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						SbNew3.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						SbNew3.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				SbNew3.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				SbNew3.Append("            AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				SbNew3.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				SbNew3.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				SbNew3.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				SbNew3.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				SbNew3.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					SbNew3.Append("           AND INV.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						SbNew3.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					SbNew3.Append("           AND INV.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						SbNew3.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					SbNew3.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					SbNew3.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					SbNew3.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					SbNew3.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					SbNew3.Append("       AND INV.invoice_date BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					SbNew3.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				SbNew3.Append("       AND INV.IS_FAC_INV <> 1    AND INVTRN.JOB_TYPE = 1 ");
				SbNew3.Append("        UNION");
				SbNew3.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK, CCD.ADM_LOCATION_MST_FK,");
				SbNew3.Append("        CMT.CUSTOMER_ID,");
				SbNew3.Append("        CMT.CUSTOMER_NAME,");
				SbNew3.Append("        LMT.LOCATION_ID, ");
				SbNew3.Append("        LMT.LOCATION_NAME,");
				SbNew3.Append("                        COL.COLLECTIONS_DATE REF_DATE,");
				SbNew3.Append("                        (CASE");
				SbNew3.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				SbNew3.Append("                           'EXPORT'");
				SbNew3.Append("                          ELSE");
				SbNew3.Append("                           'IMPORT'");
				SbNew3.Append("                        END) PROCESS,");
				SbNew3.Append("                        'COLLECTION' TRANSACTION,");
				SbNew3.Append("  ( CASE ");
				SbNew3.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				SbNew3.Append("      HBL.HBL_REF_NO ");
				SbNew3.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				SbNew3.Append("         HAWB.HAWB_REF_NO ");
				SbNew3.Append("        ELSE ");
				SbNew3.Append("       JOB.HBL_HAWB_REF_NO  ");
				SbNew3.Append("      END) SHIPMENT_REF_NR , ");
				SbNew3.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				SbNew3.Append("                        0 DEBIT,");
				SbNew3.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPK + ",COL.COLLECTIONS_DATE,1),2) CREDIT,");
				SbNew3.Append("                        0 BALANCE");
				SbNew3.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					SbNew3.Append("               BOOKING_MST_TBL        BKG,");
				}
				SbNew3.Append("               HBL_EXP_TBL            HBL,");
				SbNew3.Append("               HAWB_EXP_TBL            HAWB,");
				SbNew3.Append("               CUSTOMER_MST_TBL       CMT,");
				SbNew3.Append("               CUSTOMER_CONTACT_DTLS CCD,");
				SbNew3.Append("               LOCATION_MST_TBL       LMT,");
				SbNew3.Append("               CONSOL_INVOICE_TBL     INV,");
				SbNew3.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				SbNew3.Append("               COLLECTIONS_TRN_TBL    CLT,");
				SbNew3.Append("               COLLECTIONS_TBL        COL");
				SbNew3.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				SbNew3.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				SbNew3.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                    
				if (Convert.ToInt32(ProcessType) == 1) {
					SbNew3.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					SbNew3.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					SbNew3.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						SbNew3.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						SbNew3.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						SbNew3.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					SbNew3.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					SbNew3.Append("           AND COL.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						SbNew3.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						SbNew3.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						SbNew3.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				SbNew3.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				SbNew3.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				SbNew3.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				SbNew3.Append("            AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				SbNew3.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				SbNew3.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				SbNew3.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				SbNew3.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				SbNew3.Append("           0.00) = 0");
				if (Convert.ToInt32(BizType) == 1) {
					SbNew3.Append("           AND COL.BUSINESS_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						SbNew3.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					SbNew3.Append("           AND COL.BUSINESS_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						SbNew3.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					SbNew3.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					SbNew3.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					SbNew3.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					SbNew3.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					SbNew3.Append("           AND COL.COLLECTIONS_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					SbNew3.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}
				SbNew3.Append("          AND INV.IS_FAC_INV <> 1  AND INVTRN.JOB_TYPE = 1 ");
				SbNew3.Append("        UNION");
				SbNew3.Append("        SELECT DISTINCT CMT.CUSTOMER_MST_PK, CCD.ADM_LOCATION_MST_FK,");
				SbNew3.Append("        CMT.CUSTOMER_ID,");
				SbNew3.Append("        CMT.CUSTOMER_NAME,");
				SbNew3.Append("        LMT.LOCATION_ID, ");
				SbNew3.Append("        LMT.LOCATION_NAME,");
				SbNew3.Append("                        CNT.CREDIT_NOTE_DATE REF_DATE,");
				SbNew3.Append("                        (CASE");
				SbNew3.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				SbNew3.Append("                           'EXPORT'");
				SbNew3.Append("                          ELSE");
				SbNew3.Append("                           'IMPORT'");
				SbNew3.Append("                        END) PROCESS,");
				SbNew3.Append("                        'CREDITNOTE' TRANSACTION,");
				SbNew3.Append("  ( CASE ");
				SbNew3.Append("    WHEN JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 THEN ");
				SbNew3.Append("      HBL.HBL_REF_NO ");
				SbNew3.Append("     WHEN JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1 THEN ");
				SbNew3.Append("         HAWB.HAWB_REF_NO ");
				SbNew3.Append("        ELSE ");
				SbNew3.Append("       JOB.HBL_HAWB_REF_NO  ");
				SbNew3.Append("      END) SHIPMENT_REF_NR , ");
				SbNew3.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				SbNew3.Append("                        0 DEBIT,");
				SbNew3.Append("                        ROUND(CNT.CRN_AMMOUNT * GET_EX_RATE(CNT.CURRENCY_MST_FK," + CurrPK + ",CNT.CREDIT_NOTE_DATE,1),2) CREDIT,");
				SbNew3.Append("                        0 BALANCE");
				SbNew3.Append("          FROM JOB_CARD_TRN   JOB,");
				//'Export
				if (Convert.ToInt32(ProcessType) == 1) {
					SbNew3.Append("               BOOKING_MST_TBL        BKG,");
				}
				SbNew3.Append("               HBL_EXP_TBL            HBL,");
				SbNew3.Append("               HAWB_EXP_TBL            HAWB,");
				SbNew3.Append("               CUSTOMER_MST_TBL       CMT,");
				SbNew3.Append("               CUSTOMER_CONTACT_DTLS CCD,");
				SbNew3.Append("               LOCATION_MST_TBL       LMT,");
				SbNew3.Append("               CONSOL_INVOICE_TBL     INV,");
				SbNew3.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				SbNew3.Append("               CREDIT_NOTE_TBL        CNT,");
				SbNew3.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				SbNew3.Append("         WHERE INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
				SbNew3.Append("           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
				SbNew3.Append("           AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
				//'Export                  
				if (Convert.ToInt32(ProcessType) == 1) {
					SbNew3.Append("           AND CMT.CUSTOMER_MST_PK = BKG.CUST_CUSTOMER_MST_FK");
					SbNew3.Append("           AND JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
					SbNew3.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
					if (CargoType != 0) {
						SbNew3.Append("     AND BKG.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						SbNew3.Append("       AND BKG.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						SbNew3.Append("       AND BKG.PORT_MST_POD_FK IN( " + PODPK + ")");
					}
				//'Import
				} else {
					SbNew3.Append("        AND JOB.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK");
					SbNew3.Append("           AND CNT.PROCESS_TYPE = 2");
					if (CargoType != 0) {
						SbNew3.Append("     AND JOB.CARGO_TYPE=" + CargoType);
					}
					if (!string.IsNullOrEmpty(POLPK) & POLPK != "0") {
						SbNew3.Append("         AND JOB.PORT_MST_POL_FK IN(" + POLPK + ")");
					}
					if (!string.IsNullOrEmpty(PODPK) & PODPK != "0") {
						SbNew3.Append("         AND JOB.PORT_MST_POD_FK IN(" + PODPK + ")");
					}
				}
				SbNew3.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				SbNew3.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				SbNew3.Append("            AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
				SbNew3.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				SbNew3.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				SbNew3.Append("             FROM WRITEOFF_MANUAL_TBL WMT");
				SbNew3.Append("            WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				SbNew3.Append("           0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					SbNew3.Append("           AND CNT.BIZ_TYPE = 1");
					if (!string.IsNullOrEmpty(FlightID)) {
						SbNew3.Append("            AND JOB.VOYAGE_FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					SbNew3.Append("           AND CNT.BIZ_TYPE = 2");
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						SbNew3.Append("            AND JOB.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				SbNew3.Append("       AND INV.IS_FAC_INV <> 1     AND INVTRN.JOB_TYPE = 1 ");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					SbNew3.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					SbNew3.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					SbNew3.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					SbNew3.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					SbNew3.Append("           AND CNT.CREDIT_NOTE_DATE BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					SbNew3.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");
				}

				sb.Append(SbNew3.ToString());

				///Added By Sushama
				sb.Append(" UNION");
				//'        ----CBJC(Invoice)--     
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                        CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                          CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO SHIPMENT_REF_NR,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("         FROM CBJC_TBL   CBJC,");
				sb.Append("              CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("        AND    CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}

				sb.Append("           AND INVTRN.JOB_TYPE = 2      ");

				//'     ----TPT(Tranport Note)(Invoice)--
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT  CMT.CUSTOMER_MST_PK,");
				sb.Append("                            CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                            CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                        ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("         FROM TRANSPORT_INST_SEA_TBL TIST  ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);

				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'      ----DET & DEM-TPT (Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                            CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                            CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                         ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'      ----DET & DEM-CBJC (Invoice)-
				sb.Append("        UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                            CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                            CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                INV.INVOICE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN INV.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'INVOICE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        INV.INVOICE_REF_NO DOCREFNR,");
				sb.Append("                         ROUND(INV.NET_RECEIVABLE *");
				sb.Append("                              GET_EX_RATE(INV.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          INV.INVOICE_DATE,1),");
				sb.Append("                              2) DEBIT,");
				sb.Append("                        0 CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("           AND INV.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND INV.invoice_date  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("         AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				//'      ----CBJC (collection)-
				sb.Append("              UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                     ");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE                      ");
				sb.Append("          FROM ");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)        ");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR(+) = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK(+) = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("     AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}

				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("               AND INVTRN.JOB_TYPE = 2");
				//'         ----TPT-(Collection)--------
				sb.Append("        UNION");
				sb.Append("              SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                         0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST ,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3");
				//'       ---Det & DEM TPT(Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        (SELECT CON.BL_NUMBER FROM TRANSPORT_TRN_CONT CON WHERE CON.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("               AND INVTRN.JOB_TYPE = 3         ");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				//'       ---Det & DEM CBJC (Collection)
				sb.Append("               UNION");
				sb.Append("               SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                COL.COLLECTIONS_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN COL.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'COLLECTION' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        COL.COLLECTIONS_REF_NO DOCREFNR,");
				sb.Append("                        0 DEBIT,");
				sb.Append("                        ROUND(CLT.RECD_AMOUNT_HDR_CURR *");
				sb.Append("                              GET_EX_RATE(COL.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          COL.COLLECTIONS_DATE,1),");
				sb.Append("                              2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               COLLECTIONS_TRN_TBL    CLT,");
				sb.Append("               COLLECTIONS_TBL        COL");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
				sb.Append("           AND COL.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CLT.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				sb.Append("           AND COL.COLLECTIONS_TBL_PK = CLT.COLLECTIONS_TBL_FK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND COL.COLLECTIONS_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("               AND INVTRN.JOB_TYPE = 4         ");
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("         AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				//'  ---CBJC (CR Note)----
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM CBJC_TBL CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = CBJC.CBJC_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND CBJC.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("        AND    CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("     AND       CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("          AND CBJC.PROCESS_TYPE=" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND CBJC.BIZ_TYPE=" + BizType);
				}

				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("           AND INVTRN.JOB_TYPE = 2");
				//'         ---TPT CrNote------
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK(+)");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND TIST.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND TIST.BUSINESS_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'          --- DET & DEM TPT(CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        (SELECT CONT.BL_NUMBER FROM TRANSPORT_TRN_CONT CONT WHERE CONT.TRANSPORT_INST_FK = TIST.TRANSPORT_INST_SEA_PK AND ROWNUM=1) HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               TRANSPORT_INST_SEA_TBL TIST,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = TIST.TRANSPORT_INST_SEA_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("       AND TIST.Flight_No IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND  TIST.Vsl_Voy_Fk=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 3");
				//'          --- DET & DEM CBJC (CR Note)
				sb.Append("           UNION");
				sb.Append("           SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
				sb.Append("                                CCD.ADM_LOCATION_MST_FK,");
				sb.Append("                                CMT.CUSTOMER_ID,");
				sb.Append("                                CMT.CUSTOMER_NAME,");
				sb.Append("                                LMT.LOCATION_ID,");
				sb.Append("                                LMT.LOCATION_NAME,");
				sb.Append("                                CNT.CREDIT_NOTE_DATE REF_DATE,");
				sb.Append("                        (CASE");
				sb.Append("                          WHEN CNT.PROCESS_TYPE = 1 THEN");
				sb.Append("                           'EXPORT'");
				sb.Append("                          ELSE");
				sb.Append("                           'IMPORT'");
				sb.Append("                        END) PROCESS,");
				sb.Append("                        'CREDITNOTE' TRANSACTION,");
				sb.Append("                        CBJC.HBL_NO HBL_REF_NO,");
				sb.Append("                        CNT.CREDIT_NOTE_REF_NR DOCREFNR,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) = 2) DEBIT,");
				sb.Append("                        (SELECT  ROUND(CNT1.CRN_AMMOUNT * ");
				sb.Append("                        GET_EX_RATE(CNT1.CURRENCY_MST_FK,");
				sb.Append(CurrPK + ",");
				sb.Append("                                          CNT.CREDIT_NOTE_DATE,1),");
				sb.Append("                        2) FROM CREDIT_NOTE_TBL CNT1 WHERE CNT1.CRN_TBL_PK =  CNT.CRN_TBL_PK AND NVL(CNT1.CRN_STATUS,0) <> 2) CREDIT,");
				sb.Append("                        0 BALANCE");
				sb.Append("          FROM DEM_CALC_HDR DCH  ,");
				sb.Append("               CBJC_TBL               CBJC,");
				sb.Append("               CUSTOMER_MST_TBL       CMT,");
				sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
				sb.Append("               LOCATION_MST_TBL       LMT,");
				sb.Append("               CONSOL_INVOICE_TBL     INV,");
				sb.Append("               CONSOL_INVOICE_TRN_TBL INVTRN,");
				sb.Append("               CREDIT_NOTE_TBL        CNT,");
				sb.Append("               CREDIT_NOTE_TRN_TBL    CNTT");
				sb.Append("         WHERE INVTRN.JOB_CARD_FK = DCH.DEM_CALC_HDR_PK(+)");
				sb.Append("           AND DCH.DOC_REF_FK = CBJC.CBJC_PK");
				sb.Append("           AND INV.IS_FAC_INV <> 1");
				sb.Append("           AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
				sb.Append("           AND CNT.PROCESS_TYPE = " + ProcessType);
				sb.Append("           AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
				sb.Append("           AND CNT.CRN_TBL_PK = CNTT.CRN_TBL_FK");
				sb.Append("           AND CNTT.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");
				sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
				sb.Append("           AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
				sb.Append("           AND NVL((SELECT SUM(WMT.WRITEOFF_AMOUNT)");
				sb.Append("                     FROM WRITEOFF_MANUAL_TBL WMT");
				sb.Append("                    WHERE WMT.CONSOL_INVOICE_FK = INV.CONSOL_INVOICE_PK),");
				sb.Append("                   0.00) = 0");
				if (!string.IsNullOrEmpty(CountryPK) & CountryPK != "0") {
					sb.Append("       AND LMT.COUNTRY_MST_FK IN(" + CountryPK + ")");
				}
				if (!string.IsNullOrEmpty(LocPK) & LocPK != "0") {
					sb.Append("        AND LMT.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(CustPK) & CustPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustPK + ")");
				}
				if (!string.IsNullOrEmpty(CustGroupPK) & CustGroupPK != "0") {
					sb.Append("        AND CMT.CUSTOMER_MST_PK IN(" + CustGroupPK + ")");
				}
				if (!string.IsNullOrEmpty(getDefault(Fromdate, "").ToString()) & !string.IsNullOrEmpty(getDefault(Todate, "").ToString())) {
					sb.Append("           AND CNT.CREDIT_NOTE_DATE  BETWEEN TO_DATE('" + Fromdate + "', '" + dateFormat + "') AND");
					sb.Append("               TO_DATE('" + Todate + "', '" + dateFormat + "')");

				}
				sb.Append("          AND DCH.PROCESS_TYPE =" + ProcessType);
				if (Convert.ToInt32(BizType) != 3) {
					sb.Append("          AND DCH.BIZ_TYPE =" + BizType);
				}

				//'AIR
				if (Convert.ToInt32(BizType) == 1) {
					if (!string.IsNullOrEmpty(FlightID)) {
						sb.Append("         AND   CBJC.FLIGHT_NO IN (" + FlightID + ")");
					}
				//'SEA
				} else if (Convert.ToInt32(BizType) == 2) {
					if (!string.IsNullOrEmpty(VslVoyPK)) {
						sb.Append("         AND   CBJC.VOYAGE_TRN_FK=" + VslVoyPK + "");
					}
				}
				sb.Append("           AND INVTRN.JOB_TYPE = 4");
				/// END SUSH---------------
				sb.Append(")");
				sb.Append("   ORDER BY REF_DATE)");
				sb.Append(" ORDER BY CUSTOMER_MST_PK,CUSTOMER_ID DESC)T");


				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		#endregion

		public DataSet GetInviceCurrency(string InvRefNr = "", string ColRefNr = "", int CurrFK = 0)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				if (CurrFK == 0) {
					CurrFK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
				}
				if (!string.IsNullOrEmpty(InvRefNr)) {
					sb.Append(" SELECT DISTINCT CMT.CURRENCY_ID,(GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrFK + ",INV.INVOICE_DATE))ROE");
					sb.Append("  FROM CONSOL_INVOICE_TBL INV,CURRENCY_TYPE_MST_TBL CMT");
					sb.Append(" WHERE INV.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
					sb.Append("   AND INV.INVOICE_REF_NO IN (" + InvRefNr + ")");
				}
				if (!string.IsNullOrEmpty(InvRefNr) & !string.IsNullOrEmpty(ColRefNr)) {
					sb.Append("   UNION ");
				}
				if (!string.IsNullOrEmpty(ColRefNr)) {
					sb.Append(" SELECT DISTINCT CMT.CURRENCY_ID,(GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrFK + ",COL.COLLECTIONS_DATE))ROE");
					sb.Append("  FROM COLLECTIONS_TBL COL, CURRENCY_TYPE_MST_TBL CMT");
					sb.Append(" WHERE COL.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
					sb.Append("   AND COL.COLLECTIONS_REF_NO IN (" + ColRefNr + ")");
				}
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		public DataSet GetAgentInvoiceCurrency(string InvRefNr = "", string ColRefNr = "", int CurrFK = 0)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				if (CurrFK == 0) {
					CurrFK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
				}
				if (!string.IsNullOrEmpty(InvRefNr)) {
					sb.Append(" SELECT DISTINCT CMT.CURRENCY_ID,(GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrFK + ",INV.INVOICE_DATE))ROE");
					sb.Append("  FROM INV_AGENT_TBL INV,CURRENCY_TYPE_MST_TBL CMT");
					sb.Append(" WHERE INV.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
					sb.Append("   AND INV.INVOICE_REF_NO IN (" + InvRefNr + ")");
				}
				if (!string.IsNullOrEmpty(InvRefNr) & !string.IsNullOrEmpty(ColRefNr)) {
					sb.Append("   UNION ");
				}
				if (!string.IsNullOrEmpty(ColRefNr)) {
					sb.Append(" SELECT DISTINCT CMT.CURRENCY_ID,(GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrFK + ",COL.COLLECTIONS_DATE))ROE");
					sb.Append("  FROM COLLECTIONS_TBL COL, CURRENCY_TYPE_MST_TBL CMT");
					sb.Append(" WHERE COL.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
					sb.Append("   AND COL.COLLECTIONS_REF_NO IN (" + ColRefNr + ")");
				}
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		public DataSet GetCollectionCurrency(int ColRefNr)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT DISTINCT CMT.CURRENCY_ID");
				sb.Append("  FROM COLLECTIONS_TBL COL, CURRENCY_TYPE_MST_TBL CMT");
				sb.Append(" WHERE COL.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
				sb.Append("   AND COL.COLLECTIONS_REF_NO IN (" + ColRefNr + ")");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		#region "Fetch Customer Statement Of A/C By Tran"
		public DataSet FetchCustSOAByTrans(string CountryPK = "", string LocPK = "", int CurPK = 0, string CustPK = "", string CustGroupPK = "", string Fromdate = "", string Todate = "", string BizType = "", string ProsessType = "", int CargoType = 0,
		int BaseCurrPK = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataSet MainDS = new DataSet();

			try {
				objWF.MyDataAdapter = new OracleDataAdapter();
				objWF.MyDataAdapter.SelectCommand = new OracleCommand();
				objWF.MyDataAdapter.SelectCommand.Connection = objWF.MyConnection;
				objWF.MyDataAdapter.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_CUSTOMER_SOA_TRAN_PKG.FETCH_CUSTOMER_SOA_TRANS";
				objWF.MyDataAdapter.SelectCommand.CommandType = CommandType.StoredProcedure;

				objWF.MyDataAdapter.SelectCommand.Parameters.Add("COUNTRY_PK_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(CountryPK) ? "" : CountryPK);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("LOCATION_PK_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(LocPK) ? "" : LocPK);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("CURRENCY_PK_IN", OracleDbType.Int32).Value = CurPK;
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("CUSTOMER_PK_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(CustPK) ? "" : CustPK);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("FROM_DATE_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(Fromdate) ? "" : Fromdate);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("TODATE_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(Todate) ? "" : Todate);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("USER_PK_IN", OracleDbType.Int32).Value = Convert.ToInt32(HttpContext.Current.Session["USER_PK"]);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("CUST_GROUP_PK_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(CustGroupPK) ? "" : CustGroupPK);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Varchar2).Value = BizType;
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Varchar2).Value = ProsessType;
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32).Value = CargoType;
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("GROUP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("CUSTOMER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("TRANS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

				objWF.MyDataAdapter.Fill(MainDS);
				DataRelation relLocGroup_Details = new DataRelation("LOCGROUP", new DataColumn[] { MainDS.Tables[0].Columns["GROUPID"] }, new DataColumn[] { MainDS.Tables[1].Columns["GROUPID"] });
				DataRelation relLocCustomer_Details = new DataRelation("LOCCUSTOMER", new DataColumn[] { MainDS.Tables[1].Columns["LOCATION_MST_FK"] }, new DataColumn[] { MainDS.Tables[2].Columns["LOCATION_MST_FK"] });
				DataRelation relCustomer_Details = new DataRelation("CUSTOMERDETAILS", new DataColumn[] { MainDS.Tables[2].Columns["CUSTOMER_MST_FK"] }, new DataColumn[] { MainDS.Tables[3].Columns["CUSTOMER_MST_PK"] });

				relLocGroup_Details.Nested = true;
				relLocCustomer_Details.Nested = true;
				relCustomer_Details.Nested = true;
				MainDS.Relations.Add(relLocGroup_Details);
				MainDS.Relations.Add(relLocCustomer_Details);
				MainDS.Relations.Add(relCustomer_Details);
				return MainDS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		#endregion

		#region "Fetch Customer Statement Of A/C By Tran Repor"
		public DataSet FetchCustSOAByTransReport(string CountryPK = "0", string LocPK = "0", int CurPK = 0, string CustPK = "0", string CustGroupPK = "0", string Fromdate = "", string Todate = "", string BizType = "", string ProsessType = "", int CargoType = 0,
		int BaseCurrPK = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataSet MainDS = new DataSet();

			try {
				objWF.MyDataAdapter = new OracleDataAdapter();
				objWF.MyDataAdapter.SelectCommand = new OracleCommand();
				objWF.MyDataAdapter.SelectCommand.Connection = objWF.MyConnection;
				objWF.MyDataAdapter.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_CUSTOMER_SOA_TRAN_PKG.FETCH_CUSTOMER_SOA_TRANS_RPT";
				objWF.MyDataAdapter.SelectCommand.CommandType = CommandType.StoredProcedure;

				objWF.MyDataAdapter.SelectCommand.Parameters.Add("COUNTRY_PK_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(CountryPK) ? "" : CountryPK);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("LOCATION_PK_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(LocPK) ? "" : LocPK);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("CURRENCY_PK_IN", OracleDbType.Int32).Value = CurPK;
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("CUSTOMER_PK_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(CustPK) ? "" : CustPK);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("FROM_DATE_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(Fromdate) ? "" : Fromdate);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("TODATE_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(Todate) ? "" : Todate);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("USER_PK_IN", OracleDbType.Int32).Value = Convert.ToInt32(HttpContext.Current.Session["USER_PK"]);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("CUST_GROUP_PK_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(CustGroupPK) ? "" : CustGroupPK);
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Varchar2).Value = BizType;
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Varchar2).Value = ProsessType;
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32).Value = CargoType;
				objWF.MyDataAdapter.SelectCommand.Parameters.Add("TRANS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

				objWF.MyDataAdapter.Fill(MainDS);
				return MainDS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		#endregion

		#region "Collection Details"
		public DataSet FetchColDetails(string InvRefNr, int CurrFk)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append(" SELECT DISTINCT COL.COLLECTIONS_REF_NO,");
				sb.Append("  (COLTRN.RECD_AMOUNT_HDR_CURR * GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrFk + ", COL.COLLECTIONS_DATE)) COLLETED_AMT");
				sb.Append("  FROM COLLECTIONS_TBL COL,COLLECTIONS_TRN_TBL COLTRN,CONSOL_INVOICE_TBL  INV");
				sb.Append("  WHERE COL.COLLECTIONS_TBL_PK = COLTRN.COLLECTIONS_TBL_FK");
				sb.Append("  AND INV.INVOICE_REF_NO = COLTRN.INVOICE_REF_NR");
				sb.Append("     AND INV.INVOICE_REF_NO= '" + InvRefNr + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		public int GetColCount(string InvRefNr)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT COUNT(*) ");
				sb.Append("FROM CONSOL_INVOICE_TBL  INV,");
				sb.Append(" COLLECTIONS_TBL     COL,");
				sb.Append(" COLLECTIONS_TRN_TBL COLTRN");
				sb.Append(" WHERE COL.COLLECTIONS_TBL_PK = COLTRN.COLLECTIONS_TBL_FK");
				sb.Append(" AND INV.INVOICE_REF_NO = COLTRN.INVOICE_REF_NR");
				sb.Append(" AND INV.INVOICE_REF_NO = '" + InvRefNr + "' ");
                return Convert.ToInt32((objWF.ExecuteScaler(sb.ToString())));
            } catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		#endregion

		#region "Get ShipmentDetails"
		public DataSet GetShipmentDt(string InvRefNr)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			try {
				WorkFlow objWF = new WorkFlow();
				int BizType = 0;
				int Process = 0;
				BizType = GetBizType(InvRefNr);
				Process = GetProcessType(InvRefNr);
				if (Process == 1) {
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append(" SELECT DISTINCT HBL.HBL_REF_NO, CON.INVOICE_REF_NO");
					} else {
						sb.Append(" SELECT DISTINCT HBL.HAWB_REF_NO HBL_REF_NO, CON.INVOICE_REF_NO");
					}
					sb.Append(" FROM CONSOL_INVOICE_TBL     CON,");
					sb.Append(" CONSOL_INVOICE_TRN_TBL CONTRN,");
					sb.Append(" JOB_CARD_TRN   JOB,");
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append(" HBL_EXP_TBL HBL");
					} else {
						sb.Append(" HAWB_EXP_TBL HBL");
					}
					sb.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
					sb.Append(" AND CONTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
					} else {
						sb.Append(" AND JOB.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK");
					}
					sb.Append(" AND CON.PROCESS_TYPE = 1");
					sb.Append(" AND CON.INVOICE_REF_NO = '" + InvRefNr + "'");
				} else {
					sb.Append(" SELECT DISTINCT JOB.HBL_HAWB_REF_NO HBL_REF_NO, CON.INVOICE_REF_NO");
					sb.Append(" FROM CONSOL_INVOICE_TBL     CON,");
					sb.Append(" CONSOL_INVOICE_TRN_TBL CONTRN,");
					sb.Append(" JOB_CARD_TRN JOB");
					sb.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
					sb.Append(" AND CONTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
					sb.Append(" AND CON.PROCESS_TYPE = 2");
					sb.Append(" AND CON.INVOICE_REF_NO = '" + InvRefNr + "'");
				}
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append(" AND CON.BUSINESS_TYPE= 1");
				} else {
					sb.Append(" AND CON.BUSINESS_TYPE = 2");
				}
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		public int GetShipmentCnt(string InvRefNr, string Process)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			try {
				WorkFlow objWF = new WorkFlow();
				int BizType = 0;
				BizType = GetBizType(InvRefNr);
				if (Process == "EXPORT") {
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append(" SELECT DISTINCT COUNT(DISTINCT HBL.HBL_REF_NO) ");
					} else {
						sb.Append(" SELECT DISTINCT COUNT(DISTINCT HBL.HAWB_REF_NO) ");
					}
					sb.Append(" FROM CONSOL_INVOICE_TBL     CON,");
					sb.Append(" CONSOL_INVOICE_TRN_TBL CONTRN,");
					sb.Append(" JOB_CARD_TRN   JOB,");
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append(" HBL_EXP_TBL HBL");
					} else {
						sb.Append(" HAWB_EXP_TBL HBL");
					}
					sb.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
					sb.Append(" AND CONTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
					sb.Append(" AND CON.PROCESS_TYPE=1");
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
					} else {
						sb.Append(" AND JOB.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK");
					}
					sb.Append(" AND CON.INVOICE_REF_NO = '" + InvRefNr + "'");
				} else {
					sb.Append("SELECT COUNT(DISTINCT JOB.HBL_HAWB_REF_NO) ");
					sb.Append(" FROM CONSOL_INVOICE_TBL     CON,");
					sb.Append(" CONSOL_INVOICE_TRN_TBL CONTRN,");
					sb.Append(" JOB_CARD_TRN JOB");
					sb.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
					sb.Append(" AND CONTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
					sb.Append(" AND CON.PROCESS_TYPE=2");
					sb.Append(" AND CON.INVOICE_REF_NO = '" + InvRefNr + "'");
				}
              

                return Convert.ToInt32((objWF.ExecuteScaler(sb.ToString())));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		public int GetBizType(string InvRefNr)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			try {
				WorkFlow objWF = new WorkFlow();
				sb.Append(" Select CON.BUSINESS_TYPE");
				sb.Append(" FROM CONSOL_INVOICE_TBL CON");
				sb.Append(" WHERE CON.INVOICE_REF_NO = '" + InvRefNr + "'");

                return Convert.ToInt32((objWF.ExecuteScaler(sb.ToString())));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		public int GetProcessType(string InvRefNr)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			try {
				WorkFlow objWF = new WorkFlow();
				sb.Append(" Select CON.PROCESS_TYPE");
				sb.Append(" FROM CONSOL_INVOICE_TBL CON");
				sb.Append(" WHERE CON.INVOICE_REF_NO = '" + InvRefNr + "'");
                return Convert.ToInt32((objWF.ExecuteScaler(sb.ToString())));
            } catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		#endregion

		#region "Get Invoice Deatils"
		public DataSet GetInvDetails(string InvRefNr, string Process)
		{
			WorkFlow objWF = new WorkFlow();
			try {
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				int BizType = 0;
				BizType = GetBizType(InvRefNr);
				if (Process == "EXPORT") {
					sb.Append("SELECT DISTINCT CON.CONSOL_INVOICE_PK,BKG.CARGO_TYPE, CON.CUSTOMER_MST_FK, ");
					sb.Append("   CON.BUSINESS_TYPE, CON.PROCESS_TYPE ");
					sb.Append("  FROM CONSOL_INVOICE_TBL     CON,");
					sb.Append("       CONSOL_INVOICE_TRN_TBL CONTRN,");
					sb.Append("       JOB_CARD_TRN   JOB,");
					sb.Append("       BOOKING_MST_TBL BKG");
					sb.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
					sb.Append("   AND CONTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
					sb.Append("   AND BKG.BOOKING_MST_PK=JOB.BOOKING_MST_FK");
					sb.Append("   AND CON.INVOICE_REF_NO = '" + InvRefNr + "'");
					sb.Append("   AND CON.PROCESS_TYPE = 1 ");
				} else {
					sb.Append("SELECT DISTINCT CON.CONSOL_INVOICE_PK,");
					if (Convert.ToInt32(BizType) == 2) {
						sb.Append(" JOB.CARGO_TYPE,");
					} else {
						sb.Append(" 0 CARGO_TYPE,");
					}
					sb.Append("  CON.CUSTOMER_MST_FK,CON.BUSINESS_TYPE, CON.PROCESS_TYPE ");
					sb.Append("  FROM CONSOL_INVOICE_TBL     CON,");
					sb.Append("       CONSOL_INVOICE_TRN_TBL CONTRN,");
					sb.Append("       JOB_CARD_TRN   JOB");
					sb.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
					sb.Append("   AND CONTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
					sb.Append("   AND CON.INVOICE_REF_NO = '" + InvRefNr + "'");
					sb.Append("   AND CON.PROCESS_TYPE =2");
				}
				if (Convert.ToInt32(BizType) == 1) {
					sb.Append(" AND CON.BUSINESS_TYPE= 1");
					//sb.Replace("SEA", "AIR")
				} else {
					sb.Append(" AND CON.BUSINESS_TYPE= 2");
				}
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		#endregion

		#region "AGENT SOA"
		public DataSet FetchAgentSOALocInformation(string Fromdate = "", string Todate = "", string POLPK = "", string PODPK = "", string CountryPK = "", string LocPK = "", string AgentFk = "", string BizType = "", string ProcessType = "", string CarrierPK = "",
		string VslVoyPK = "", string FlightID = "", int CargoType = 0, string CurrPK = "", bool SOAByTransaction = false)
		{

			//CurrPK: SELECTED CURRENCY
			int BaseCurrPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder strCond = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			DataSet MainDS = new DataSet();
			OracleDataAdapter DA = new OracleDataAdapter();
			Int32 TotalRecords = default(Int32);
			string strSQL = null;
			Int32 last = default(Int32);
			Int32 start = default(Int32);

			try {
				if (!string.IsNullOrEmpty(Fromdate.Trim())) {
					strCond.Append(" AND TO_DATE(VAS.REF_DATE,DATEFORMAT)>=TO_DATE('" + Fromdate + "',DATEFORMAT) ");
				}
				if (!string.IsNullOrEmpty(Todate.Trim())) {
					strCond.Append(" AND TO_DATE(VAS.REF_DATE,DATEFORMAT)<=TO_DATE('" + Todate + "',DATEFORMAT) ");
				}
				if (!string.IsNullOrEmpty(POLPK.Trim())) {
					strCond.Append(" AND VAS.POL_FK IN (" + POLPK + ") ");
				}
				if (!string.IsNullOrEmpty(PODPK.Trim())) {
					strCond.Append(" AND VAS.POD_FK IN (" + PODPK + ") ");
				}
				if (!string.IsNullOrEmpty(CountryPK.Trim())) {
					strCond.Append(" AND VAS.COUNTRY_MST_FK IN (" + CountryPK + ") ");
				}
				if (!string.IsNullOrEmpty(LocPK.Trim())) {
					strCond.Append(" AND VAS.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(AgentFk.Trim())) {
					strCond.Append(" AND VAS.AGENT_MST_PK IN (" + AgentFk + ") ");
				}
				if (Convert.ToInt32(BizType) == 1 | Convert.ToInt32(BizType) == 2) {
					strCond.Append(" AND VAS.BIZ_TYPE = " + BizType);
				}
				if (Convert.ToInt32(ProcessType) == 1 | Convert.ToInt32(ProcessType) == 2) {
					strCond.Append(" AND VAS.PROCESS_TYPE = " + ProcessType);
				}
				if (Convert.ToInt32(CargoType) > 0) {
					strCond.Append(" AND VAS.CARGO_TYPE = " + CargoType);
				}
				if (!string.IsNullOrEmpty(VslVoyPK.Trim())) {
					strCond.Append(" AND VAS.VOYAGE_TRN_FK IN (" + VslVoyPK + ") ");
				}
				if (!string.IsNullOrEmpty(FlightID.Trim())) {
					strCond.Append(" AND VAS.VOY_FLIGHT_NO_JC IN (" + FlightID + ") ");
				}
				if (!string.IsNullOrEmpty(CarrierPK.Trim())) {
					strCond.Append(" AND VAS.CARRIER_FK IN (" + CarrierPK + ") ");
				}

				//--------------------------------------------------------------------------------------------
				objWF.MyConnection.Open();
				objWF.MyCommand = new OracleCommand();
				objWF.MyCommand.Connection = objWF.MyConnection;
				//__________________________________BAND 0, GROUP_____________________________________________
				sb.Append(" SELECT DISTINCT VAS.GROUPID FROM VIEW_AGENT_SOA VAS WHERE 1=1 ");
				sb.Append(strCond.ToString());
				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "GROUP");
				//__________________________________BAND 1, LOCATION_____________________________________________
				sb.Clear();
				sb.Append(" SELECT DISTINCT VAS.LOCATION_MST_PK,VAS.LOCATION_NAME,VAS.BALANCE, VAS.GROUPID FROM VIEW_AGENT_SOA VAS WHERE 1=1 ");
				sb.Append(strCond.ToString());
				sb.Append(" ORDER BY VAS.LOCATION_NAME ");
				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "LOCATION");
				//__________________________________BAND 2, CUSTOMER_____________________________________________
				sb.Clear();
				sb.Append(" SELECT DISTINCT VAS.LOCATION_MST_PK,VAS.AGENT_MST_PK,VAS.AGENT_NAME,VAS.AGENT_REF_NR,VAS.BALANCE,'' FLAG FROM VIEW_AGENT_SOA VAS WHERE 1=1 ");
				sb.Append(strCond.ToString());
				sb.Append(" ORDER BY VAS.AGENT_NAME ");
				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "CUSTOMER");
				//__________________________________BAND 3, DETAILS_____________________________________________
				sb.Clear();
				if (SOAByTransaction) {
					sb.Append("SELECT DISTINCT VAS.AGENT_MST_PK,");
					sb.Append("                VAS.LOCATION_MST_PK,");
					sb.Append("                TO_DATE('" + Fromdate + "', DATEFORMAT) REF_DATE,");
					sb.Append("                '' PROCESS,");
					sb.Append("                'OPENING BALANCE' HBL_REF_NO,");
					sb.Append("                '' INVOICE_REF_NO,");
					sb.Append("                SUM(NVL(VAS.DEBIT, 0)) INV_AMOUNT,");
					sb.Append("                '' COLLECTION_REF_NO,");
					sb.Append("                SUM(NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) COLLECTED_AMT,");
					sb.Append("                SUM(NVL(VAS.DEBIT, 0) - NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) BALANCE,");
					sb.Append("                SUM(NVL(VAS.DEBIT, 0) - NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) OUTSTANDING");
					sb.Append("  FROM COLLECTIONS_TBL CT, COLLECTIONS_TRN_TBL CTRN, VIEW_AGENT_SOA VAS");
					sb.Append(" WHERE VAS.TRANSACTION = 'INVOICE'");
					sb.Append("   AND VAS.DOCREFNR = CTRN.INVOICE_REF_NR(+)");
					sb.Append("   AND CTRN.COLLECTIONS_TBL_FK = CT.COLLECTIONS_TBL_PK(+) ");
					sb.Append(strCond.ToString());
					sb.Append(" GROUP BY VAS.AGENT_MST_PK,VAS.LOCATION_MST_PK ");
					//sb.Append("----------------------------------------------")
					sb.Append(" UNION ");
					//sb.Append("----------------------------------------------")
					sb.Append("SELECT *");
					sb.Append("  FROM (SELECT DISTINCT VAS.AGENT_MST_PK,");
					sb.Append("                        VAS.LOCATION_MST_PK,");
					sb.Append("                        VAS.REF_DATE,");
					sb.Append("                        DECODE(VAS.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT') PROCESS,");
					sb.Append("                        VAS.HBL_REF_NO,");
					sb.Append("                        VAS.DOCREFNR INVOICE_REF_NO,");
					sb.Append("                        SUM(NVL(VAS.DEBIT, 0)) INV_AMOUNT,");
					sb.Append("                        CT.COLLECTIONS_REF_NO,");
					sb.Append("                        SUM(NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) COLLECTED_AMT,");
					sb.Append("                        SUM(NVL(VAS.DEBIT, 0) -");
					sb.Append("                            NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) BALANCE,");
					sb.Append("                        SUM(NVL(VAS.DEBIT, 0) -");
					sb.Append("                            NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) OUTSTANDING");
					sb.Append("          FROM COLLECTIONS_TBL     CT,");
					sb.Append("               COLLECTIONS_TRN_TBL CTRN,");
					sb.Append("               VIEW_AGENT_SOA      VAS");
					sb.Append("         WHERE VAS.TRANSACTION = 'INVOICE'");
					sb.Append("           AND VAS.DOCREFNR = CTRN.INVOICE_REF_NR(+)");
					sb.Append("           AND CTRN.COLLECTIONS_TBL_FK = CT.COLLECTIONS_TBL_PK(+) ");
					sb.Append(strCond.ToString());
					sb.Append("         GROUP BY VAS.AGENT_MST_PK,");
					sb.Append("                  VAS.LOCATION_MST_PK,");
					sb.Append("                  VAS.REF_DATE,");
					sb.Append("                  VAS.PROCESS_TYPE,");
					sb.Append("                  VAS.HBL_REF_NO,");
					sb.Append("                  VAS.DOCREFNR,");
					sb.Append("                  CT.COLLECTIONS_REF_NO");
					sb.Append("         ORDER BY VAS.REF_DATE DESC) T");
				} else {
					sb.Append("SELECT DISTINCT VAS.AGENT_MST_PK,");
					sb.Append("       VAS.LOCATION_MST_PK,");
					sb.Append("       TO_DATE('" + Fromdate + "',DATEFORMAT) REF_DATE,");
					sb.Append("       '' PROCESS_TYPE,");
					sb.Append("       'OPENING BALANCE' TRANSACTION,");
					sb.Append("       '' HBL_REF_NO,");
					sb.Append("       '' DOCREFNR,");
					sb.Append("       NULL DEBIT,");
					sb.Append("       NULL CREDIT,");
					sb.Append("       ROUND(SUM((VAS.DEBIT-VAS.CREDIT) * GET_EX_RATE(VAS.REF_CURRENCY_FK, " + CurrPK + ", VAS.REF_DATE)),2) BALANCE,");
					sb.Append("       NULL OUTSTANDINGDAYS,");
					sb.Append("       NULL REF_PK,");
					sb.Append("       NULL BIZ_TYPE,");
					sb.Append("       NULL CARGO_TYPE");
					sb.Append("  FROM VIEW_AGENT_SOA VAS WHERE 1=1 ");
					sb.Append(strCond.ToString());
					sb.Append("  GROUP BY VAS.AGENT_MST_PK,VAS.LOCATION_MST_PK");
					sb.Append("         UNION           ");
					sb.Append("SELECT * FROM(SELECT VAS.AGENT_MST_PK,");
					sb.Append("       VAS.LOCATION_MST_PK,");
					sb.Append("       VAS.REF_DATE,");
					sb.Append("       DECODE(VAS.PROCESS_TYPE,1,'EXPORT',2,'IMPORT') PROCESS_TYPE,");
					sb.Append("       VAS.TRANSACTION,");
					sb.Append("       VAS.HBL_REF_NO,");
					sb.Append("       VAS.DOCREFNR,");
					sb.Append("       ROUND(VAS.DEBIT * GET_EX_RATE(VAS.REF_CURRENCY_FK, " + CurrPK + ", VAS.REF_DATE),");
					sb.Append("             2) DEBIT,");
					sb.Append("       ROUND(VAS.CREDIT * GET_EX_RATE(VAS.REF_CURRENCY_FK, " + CurrPK + ", VAS.REF_DATE),");
					sb.Append("             2) CREDIT,");
					sb.Append("       VAS.BALANCE,");
					sb.Append("       (CASE");
					sb.Append("               WHEN VAS.TRANSACTION = 'INVOICE' THEN");
					sb.Append("                NVL((SELECT DISTINCT TO_CHAR(TO_DATE(SYSDATE, DATEFORMAT) -");
					sb.Append("                                         TO_DATE(VAS.REF_DATE, DATEFORMAT)) OUTSTANDING_DAYS");
					sb.Append("                   FROM INV_AGENT_TBL INV");
					sb.Append("                  WHERE INV.INVOICE_REF_NO(+) = VAS.DOCREFNR");
					sb.Append("                    AND NVL((INV.NET_INV_AMT - NVL((SELECT SUM(CTRN.RECD_AMOUNT_HDR_CURR)");
					sb.Append("                                                     FROM COLLECTIONS_TRN_TBL CTRN");
					sb.Append("                                                    WHERE CTRN.INVOICE_REF_NR LIKE");
					sb.Append("                                                          INV.INVOICE_REF_NO),");
					sb.Append("                                                   0.00)),");
					sb.Append("                            0) > 0),0)");
					sb.Append("               ELSE");
					sb.Append("              NULL");
					sb.Append("       END) OUTSTANDINGDAYS,");
					sb.Append("       VAS.REF_PK,");
					sb.Append("       VAS.BIZ_TYPE,");
					sb.Append("       VAS.CARGO_TYPE");
					sb.Append("  FROM VIEW_AGENT_SOA VAS WHERE 1=1 ");
					sb.Append(strCond.ToString());
					sb.Append(" ORDER BY REF_DATE DESC) QRY ");
				}

				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "DETAILS");
				DataRelation relLocGroup_Details = new DataRelation("LOCGROUP", new DataColumn[] { MainDS.Tables[0].Columns["GROUPID"] }, new DataColumn[] { MainDS.Tables[1].Columns["GROUPID"] });
				DataRelation relLocCustomer_Details = new DataRelation("LOCCUSTOMER", new DataColumn[] { MainDS.Tables[1].Columns["LOCATION_MST_PK"] }, new DataColumn[] { MainDS.Tables[2].Columns["LOCATION_MST_PK"] });
				DataRelation relCustomer_Details = new DataRelation("CUSTOMERDETAILS", new DataColumn[] { MainDS.Tables[2].Columns["AGENT_MST_PK"] }, new DataColumn[] { MainDS.Tables[3].Columns["AGENT_MST_PK"] });

				relLocGroup_Details.Nested = true;
				relLocCustomer_Details.Nested = true;
				relCustomer_Details.Nested = true;
				MainDS.Relations.Add(relLocGroup_Details);
				MainDS.Relations.Add(relLocCustomer_Details);
				MainDS.Relations.Add(relCustomer_Details);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
			return MainDS;
		}

		public DataSet FetchLocationReportAgent(string Fromdate = "", string Todate = "", string POLPK = "", string PODPK = "", string CountryPK = "", string LocPK = "", string AgentFk = "", string BizType = "", string ProcessType = "", string CarrierPK = "",
		string VslVoyPK = "", string FlightID = "", int CargoType = 0, string CurrPK = "", bool SOAByTransaction = false)
		{

			//CurrPK: SELECTED CURRENCY
			int BaseCurrPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder strCond = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			DataSet MainDS = new DataSet();

			try {
				if (!string.IsNullOrEmpty(Fromdate.Trim())) {
					strCond.Append(" AND TO_DATE(VAS.REF_DATE,DATEFORMAT)>=TO_DATE('" + Fromdate + "',DATEFORMAT) ");
				}
				if (!string.IsNullOrEmpty(Todate.Trim())) {
					strCond.Append(" AND TO_DATE(VAS.REF_DATE,DATEFORMAT)<=TO_DATE('" + Todate + "',DATEFORMAT) ");
				}
				if (!string.IsNullOrEmpty(POLPK.Trim())) {
					strCond.Append(" AND VAS.POL_FK IN (" + POLPK + ") ");
				}
				if (!string.IsNullOrEmpty(PODPK.Trim())) {
					strCond.Append(" AND VAS.POD_FK IN (" + PODPK + ") ");
				}
				if (!string.IsNullOrEmpty(CountryPK.Trim())) {
					strCond.Append(" AND VAS.COUNTRY_MST_FK IN (" + CountryPK + ") ");
				}
				if (!string.IsNullOrEmpty(LocPK.Trim())) {
					strCond.Append(" AND VAS.LOCATION_MST_PK IN (" + LocPK + ") ");
				}
				if (!string.IsNullOrEmpty(AgentFk.Trim())) {
					strCond.Append(" AND VAS.AGENT_MST_PK IN (" + AgentFk + ") ");
				}
				if (Convert.ToInt32(BizType) == 1 | Convert.ToInt32(BizType) == 2) {
					strCond.Append(" AND VAS.BIZ_TYPE = " + BizType);
				}
				if (Convert.ToInt32(ProcessType) == 1 | Convert.ToInt32(ProcessType) == 2) {
					strCond.Append(" AND VAS.PROCESS_TYPE = " + ProcessType);
				}
				if (Convert.ToInt32(CargoType) > 0) {
					strCond.Append(" AND VAS.CARGO_TYPE = " + CargoType);
				}
				if (!string.IsNullOrEmpty(VslVoyPK.Trim())) {
					strCond.Append(" AND VAS.VOYAGE_TRN_FK IN (" + VslVoyPK + ") ");
				}
				if (!string.IsNullOrEmpty(FlightID.Trim())) {
					strCond.Append(" AND VAS.VOY_FLIGHT_NO_JC IN (" + FlightID + ") ");
				}
				if (!string.IsNullOrEmpty(CarrierPK.Trim())) {
					strCond.Append(" AND VAS.CARRIER_FK IN (" + CarrierPK + ") ");
				}

				//--------------------------------------------------------------------------------------------
				if (SOAByTransaction) {
					sb.Append("select * from (SELECT DISTINCT VAS.AGENT_MST_PK CUSTOMER_MST_PK,");
					sb.Append("                VAS.LOCATION_MST_PK LOCATION_MST_FK,");
					sb.Append("                TO_CHAR(TO_DATE('" + Fromdate + "',DATEFORMAT),DATEFORMAT) REF_DATE,");
					sb.Append("                '' PROCESS,");
					sb.Append("                'OPENING BALANCE' HBL_REF_NO,");
					sb.Append("                '' INVOICE_REF_NO,");
					sb.Append("                SUM(NVL(VAS.DEBIT, 0)) INV_AMOUNT,");
					sb.Append("                '' COLLECTION_REF_NO,");
					sb.Append("                SUM(NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) COLLECTED_AMT,");
					sb.Append("                SUM(NVL(VAS.DEBIT, 0) - NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) BALANCE,");
					sb.Append("                SUM(NVL(VAS.DEBIT, 0) - NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) OUTSTANDING,");
					sb.Append("                VAS.AGENT_NAME CUSTOMER_NAME, ");
					sb.Append("                VAS.LOCATION_NAME ");
					sb.Append("  FROM COLLECTIONS_TBL CT, COLLECTIONS_TRN_TBL CTRN, VIEW_AGENT_SOA VAS");
					sb.Append(" WHERE VAS.TRANSACTION = 'INVOICE'");
					sb.Append("   AND VAS.DOCREFNR = CTRN.INVOICE_REF_NR(+)");
					sb.Append("   AND CTRN.COLLECTIONS_TBL_FK = CT.COLLECTIONS_TBL_PK(+) ");
					sb.Append(strCond.ToString());
					sb.Append(" GROUP BY VAS.AGENT_MST_PK, ");
					sb.Append("          VAS.LOCATION_MST_PK,");
					sb.Append("          VAS.AGENT_NAME, ");
					sb.Append("          VAS.LOCATION_NAME ");
					//sb.Append("----------------------------------------------")
					sb.Append(" UNION ");
					//sb.Append("----------------------------------------------")
					sb.Append("SELECT *");
					sb.Append("  FROM (SELECT DISTINCT VAS.AGENT_MST_PK,");
					sb.Append("                        VAS.LOCATION_MST_PK,");
					sb.Append("                        TO_CHAR(VAS.REF_DATE,DATEFORMAT) REF_DATE,");
					sb.Append("                        DECODE(VAS.PROCESS_TYPE, 1, 'EXPORT', 2, 'IMPORT') PROCESS,");
					sb.Append("                        VAS.HBL_REF_NO,");
					sb.Append("                        VAS.DOCREFNR INVOICE_REF_NO,");
					sb.Append("                        SUM(NVL(VAS.DEBIT, 0)) INV_AMOUNT,");
					sb.Append("                        CT.COLLECTIONS_REF_NO,");
					sb.Append("                        SUM(NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) COLLECTED_AMT,");
					sb.Append("                        SUM(NVL(VAS.DEBIT, 0) -");
					sb.Append("                            NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) BALANCE,");
					sb.Append("                        SUM(NVL(VAS.DEBIT, 0) -");
					sb.Append("                            NVL(CTRN.RECD_AMOUNT_HDR_CURR, 0)) OUTSTANDING,");
					sb.Append("                        VAS.AGENT_NAME CUSTOMER_NAME, ");
					sb.Append("                        VAS.LOCATION_NAME ");
					sb.Append("          FROM COLLECTIONS_TBL     CT,");
					sb.Append("               COLLECTIONS_TRN_TBL CTRN,");
					sb.Append("               VIEW_AGENT_SOA      VAS");
					sb.Append("         WHERE VAS.TRANSACTION = 'INVOICE'");
					sb.Append("           AND VAS.DOCREFNR = CTRN.INVOICE_REF_NR(+)");
					sb.Append("           AND CTRN.COLLECTIONS_TBL_FK = CT.COLLECTIONS_TBL_PK(+) ");
					sb.Append(strCond.ToString());
					sb.Append("         GROUP BY VAS.AGENT_MST_PK,");
					sb.Append("                  VAS.LOCATION_MST_PK,");
					sb.Append("                  VAS.AGENT_NAME, ");
					sb.Append("                  VAS.LOCATION_NAME, ");
					sb.Append("                  VAS.REF_DATE,");
					sb.Append("                  VAS.PROCESS_TYPE,");
					sb.Append("                  VAS.HBL_REF_NO,");
					sb.Append("                  VAS.DOCREFNR,");
					sb.Append("                  CT.COLLECTIONS_REF_NO");
					sb.Append("         ) T) ORDER BY TO_DATE(REF_DATE),invoice_ref_no");
				} else {
					sb.Append("SELECT ROWNUM \"SL.NR\", T.* ");
					sb.Append(" FROM (SELECT DISTINCT VAS.AGENT_MST_PK CUSTOMER_MST_PK,");
					sb.Append("                        VAS.LOCATION_MST_PK ADM_LOCATION_MST_FK,");
					sb.Append("                        '' CUSTOMER_ID,");
					sb.Append("                        VAS.AGENT_NAME CUSTOMER_NAME,");
					sb.Append("                        '' LOCATION_ID,");
					sb.Append("                        VAS.LOCATION_NAME,");
					sb.Append("                        TO_DATE('" + Fromdate + "',DATEFORMAT) REF_DATE,");
					sb.Append("                        '' PROCESS,");
					sb.Append("                        'OPENING BALANCE' TRANSACTION,");
					sb.Append("                        '' SHIPMENT_REF_NR,");
					sb.Append("                        '' DOCREFNR,");
					sb.Append("                        0 DEBIT,");
					sb.Append("                        0 CREDIT,");
					sb.Append("                        ROUND(SUM((VAS.DEBIT-VAS.CREDIT) * GET_EX_RATE(VAS.REF_CURRENCY_FK, " + CurrPK + ", VAS.REF_DATE)),2) BALANCE ");
					sb.Append("         FROM VIEW_AGENT_SOA VAS WHERE 1=1 ");
					sb.Append(strCond.ToString());
					sb.Append("  GROUP BY VAS.AGENT_MST_PK,");
					sb.Append("           VAS.LOCATION_MST_PK,");
					sb.Append("           VAS.AGENT_ID,");
					sb.Append("           VAS.AGENT_NAME,");
					sb.Append("           VAS.LOCATION_ID,");
					sb.Append("           VAS.LOCATION_NAME ");
					sb.Append("         UNION           ");
					sb.Append("  SELECT * FROM (SELECT DISTINCT VAS.AGENT_MST_PK,");
					sb.Append("                        VAS.LOCATION_MST_PK,");
					sb.Append("                        VAS.AGENT_ID,");
					sb.Append("                        VAS.AGENT_NAME,");
					sb.Append("                        VAS.LOCATION_ID,");
					sb.Append("                        VAS.LOCATION_NAME,");
					sb.Append("                        VAS.REF_DATE,");
					sb.Append("                        DECODE(VAS.PROCESS_TYPE,1,'EXPORT',2,'IMPORT') PROCESS,");
					sb.Append("                        VAS.TRANSACTION,");
					sb.Append("                        VAS.HBL_REF_NO SHIPMENT_REF_NR,");
					sb.Append("                        VAS.DOCREFNR,");
					sb.Append("                        VAS.DEBIT,");
					sb.Append("                        VAS.CREDIT,");
					sb.Append("                        VAS.BALANCE ");
					sb.Append("          FROM VIEW_AGENT_SOA VAS WHERE 1=1 " + strCond.ToString());
					sb.Append("         )order by ref_date) T ");
				}

				MainDS = objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
			return MainDS;
		}
		#endregion

		#region "Trigger Data"
		#region "Report Information"
		public DataSet FetchTrigrerData(string TriggerTime = "", string SchDate = "", string Month = "", string Day = "")
		{
			WorkFlow objWF = new WorkFlow();
			try {
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				sb.Append(" SELECT RH.SCH_PK, RH.SCH_ID,RH.REPORT_TYPE,RH.SCH_TYPE,RH.CUSTOMER_PK,RH.COUNTRY_PK,RH.LOCATION_PK, RH.BIZ_TYPE,RH.PROCESS_TYPE,RH.CURENCY,RH.REPORT_BY,RH.POLAOOPK,");
				sb.Append(" RH.PODAODPK,RH.FREQUENCY_TYPE,RH.MONTH_SCH,RH.DATE_SCH,RH.DAY_SCH,RH.TRIGGER_TIME, RT.GROUP_PK,RT.CUSTOMER_PK,RT.CUST_EMAILID,RH.ACTIVE ");
				sb.Append(" FROM REPORT_HEADER_TBL RH, REPORT_HEADER_TRN RT WHERE RH.SCH_PK = RT.SCH_FK");
				if (!string.IsNullOrEmpty(SchDate)) {
					sb.Append("   AND RH.DATE_SCH  = '" + SchDate + "'");
				}
				if (!string.IsNullOrEmpty(TriggerTime)) {
					sb.Append("   AND RH.TRIGGER_TIME = '" + TriggerTime + "'");
				}
				if (!string.IsNullOrEmpty(Month)) {
					sb.Append("   AND RH.MONTH_SCH=  '" + Month + "'");
				}
				if (!string.IsNullOrEmpty(Day)) {
					sb.Append("   AND RH.DAY_SCH = '" + Day + "'");
				}
				sb.Append("    AND RH.ACTIVE = 1 ");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw;
			}
		}
		#endregion
		#endregion
	}
}
