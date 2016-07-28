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
    public class Cls_Supplier_Invoice : CommonFeatures
	{
		DataSet ObjTrn_DataSet;
		public DataSet M_dataset {
			set { ObjTrn_DataSet = value; }
		}
		#region "Grid Header"
		public enum Header
		{
			SrNo = 0,
			JobCardRefNo = 1,
			JobCardSeaExpPk = 2,
			JobCardPiaPK = 3,
			CostElementId = 4,
			CostElementPK = 5,
			InvoiceSupplierFk = 6,
			EstimatedCost = 7,
			ActualCost = 8,
			Difference = 9,
			Check = 10
		}
		#endregion

		#region "Fetch JobCardInvoice"
		public DataSet FetchJobCardInv(Int32 VendorPK, Int16 Business_Type, Int16 Process_Type, string FromDt = "", string ToDt = "", Int32 CostElementPk = 0, Int32 CurrencyPK = 0, Int32 TradePK = 0, string Vsl = "", string INVOICENO = "",
		Int64 lblInvSupplierPK = 0, string General = "")
		{

			StringBuilder strSql = new StringBuilder();
			string strCondition = null;
			string BusinessProcess = null;
			string VslFlight = null;
			if (Business_Type == 2) {
				BusinessProcess = "SEA";
				VslFlight = "VESSEL_NAME";
			} else {
				BusinessProcess = "AIR";
				VslFlight = "FLIGHT_NO";
			}
			if (Process_Type == 1) {
				BusinessProcess += "_EXP";
			} else {
				BusinessProcess += "_IMP";
			}
			if (((FromDt != null) & (ToDt != null)) & FromDt != " " & ToDt != " ") {
				strCondition += " AND JOB_EXP.JOBCARD_DATE BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
			} else if (((FromDt != null)) & FromDt != " ") {
				strCondition += " AND JOB_EXP.JOBCARD_DATE >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
			} else if (((ToDt != null)) & ToDt != " ") {
				strCondition += " AND JOB_EXP.JOBCARD_DATE <= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
			}

			//If Not IsNothing(FromDt) And Not IsNothing(ToDt) Then
			//    strCondition = " AND JOB_EXP.JOBCARD_DATE BETWEEN TO_DATE('" & FromDt & "','" & dateFormat & "')  AND TO_DATE('" & ToDt & "','" & dateFormat & "')  "
			//ElseIf Not IsNothing(FromDt) Then
			//    strCondition = " AND JOB_EXP.JOBCARD_DATE >= TO_DATE('" & FromDt & "','" & dateFormat & "') "
			//ElseIf Not IsNothing(ToDt) Then
			//    strCondition = " AND JOB_EXP.JOBCARD_DATE >= TO_DATE('" & ToDt & "','" & dateFormat & "') "
			//End If

			if (!string.IsNullOrEmpty(INVOICENO)) {
				strCondition += " AND INV_SUP.INVOICE_REF_NO ='" + INVOICENO + "'";
			}

			if (CostElementPk != 0) {
				strCondition += " AND COST_ELE.COST_ELEMENT_MST_PK =" + CostElementPk;
			}

			if (!string.IsNullOrEmpty(Vsl)) {
				if (Business_Type == 2) {
					strCondition += " AND JOB_EXP.VESSEL_NAME ||" + "'-'" + " || JOB_EXP.VOYAGE_FLIGHT_NO like '" + Vsl.Trim() + "'";
				} else {
					strCondition += " AND JOB_EXP.VOYAGE_FLIGHT_NO like '" + Vsl.Trim() + "'";
				}
			}

			//If BlankGrid = 0 Then
			//    strCondition &= vbCrLf & " AND 1=2 "
			//End If

			if (lblInvSupplierPK != 0) {
				//'Modified BY Koteshwari 
				strSql.Append("SELECT ROWNUM SrNO,");
				strSql.Append("JOB_EXP.JOBCARD_REF_NO AS REF_NR,");
				strSql.Append("JOB_EXP.JOB_CARD_TRN_PK AS JOB_CARD_SEA_EXP_PK,");
				//strSql.Append("JOB_TRN_PIA.JOB_TRN_" & BusinessProcess & "_PIA_PK AS JOB_TRN_SEA_EXP_PIA_PK,")
				strSql.Append("JOB_COST.JOB_TRN_COST_PK AS JOB_TRN_EST_PK,");
				strSql.Append("INV_SUP.INV_SUPPLIER_PK INV_SUPPLIER_FK ,");
				strSql.Append("INV_TRN.INV_SUPPLIER_TRN_PK INV_SUPPLIER_TRN_FK ,");
				strSql.Append("COST_ELE.COST_ELEMENT_ID AS COST_ELEMENT_ID,");
				strSql.Append("COST_ELE.COST_ELEMENT_MST_PK AS COST_ELEMENT_MST_PK,");
				//strSql.Append(" JOB_TRN_PIA.VENDOR_MST_FK AS VENDOR_MST_FK,")
				strSql.Append(" JOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
				//strSql.Append(" DECODE(INV_SUP.INETRNAL_REF,")
				//strSql.Append(" 1,'JObcard', 2,'General) INETRNAL_REF,")

				if (Convert.ToInt32(General) == 2) {
					//strSql.Append(" '' ESTIMATED_AMT,")
					strSql.Append("ROUND(INV_TRN.ESTIMATED_AMT * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",INV_SUP.SUPPLIER_INV_DT),2) AS ESTIMATED_AMT,");
				} else {
					strSql.Append("ROUND(INV_TRN.ESTIMATED_AMT * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",INV_SUP.SUPPLIER_INV_DT),2) AS ESTIMATED_AMT,");
				}

				strSql.Append("ROUND(INV_TRN.ACTUAL_Amt * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",INV_SUP.SUPPLIER_INV_DT),2) AS ACTUAL_AMT,");

				if (Convert.ToInt32(General) == 2) {
					strSql.Append(" INV_TRN.TAX_PERCENT AS TAX_PERCENTAGE,INV_TRN.TAX_AMOUNT AS TAX_AMT,");
					strSql.Append(" INV_TRN.TOTAL_COST,");
					//strSql.Append(" '' DIFFERENCE_AMT,")
					strSql.Append(" (CASE WHEN JOB_COST.ESTIMATED_COST IS NOT NULL AND INV_TRN.TOTAL_COST IS NOT NULL THEN ");
					strSql.Append(" NVL(ROUND(INV_TRN.TOTAL_COST * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",INV_SUP.SUPPLIER_INV_DT),2),0) - NVL(ROUND(JOB_COST.ESTIMATED_COST * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",INV_SUP.SUPPLIER_INV_DT),2),0) ");
					strSql.Append(" ELSE NULL END) DIFFERENCE_AMT, ");
				} else {
					//strSql.Append(" JOB_TRN_PIA.TAX_PERCENTAGE,JOB_TRN_PIA.TAX_AMT,")
					strSql.Append(" INV_TRN.TAX_PERCENT AS TAX_PERCENTAGE, INV_TRN.TAX_AMOUNT AS TAX_AMT,");
					strSql.Append(" INV_TRN.TOTAL_COST,");
					// added by Manohar 08May07: to get null in Diff if any of the cost is null
					//strSql.Append(" (CASE WHEN INV_TRN.ESTIMATED_AMT IS NOT NULL AND INV_TRN.ACTUAL_AMT IS NOT NULL THEN ")
					//strSql.Append("NVL(ROUND(INV_TRN.ACTUAL_AMT * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," & CStr(CurrencyPK) & ",SYSDATE),2),0) - NVL(ROUND(INV_TRN.ESTIMATED_AMT * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," & CStr(CurrencyPK) & ",SYSDATE),2),0) ")
					//strSql.Append(" ELSE NULL END) DIFFERENCE_AMT, ")
					strSql.Append(" (CASE WHEN JOB_COST.ESTIMATED_COST IS NOT NULL AND INV_TRN.TOTAL_COST IS NOT NULL THEN ");
					strSql.Append(" NVL(ROUND(INV_TRN.TOTAL_COST * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",INV_SUP.SUPPLIER_INV_DT),2),0) - NVL(ROUND(JOB_COST.ESTIMATED_COST * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",INV_SUP.SUPPLIER_INV_DT),2),0) ");
					strSql.Append(" ELSE NULL END) DIFFERENCE_AMT, ");
				}
				strSql.Append(" 'true' Sel");
				strSql.Append(" FROM INV_SUPPLIER_TBL INV_SUP,");
				strSql.Append("INV_SUPPLIER_TRN_TBL INV_TRN,");
				//strSql.Append("JOB_TRN_" & BusinessProcess & "_PIA JOB_TRN_PIA,")
				strSql.Append("JOB_TRN_TRN_COST JOB_COST,");
				strSql.Append("JOB_CARD_TRN JOB_EXP,");
				strSql.Append(" COST_ELEMENT_MST_TBL COST_ELE");
				strSql.Append(" WHERE INV_SUP.INV_SUPPLIER_PK = INV_TRN.INV_SUPPLIER_TBL_FK");
				strSql.Append(" AND INV_SUP.VENDOR_MST_FK=" + VendorPK);
				//strSql.Append(" AND INV_TRN.JOB_CARD_PIA_FK=JOB_TRN_PIA.JOB_TRN_" & BusinessProcess & "_PIA_PK(+)")
				strSql.Append(" AND INV_TRN.JOB_TRN_EST_FK=JOB_COST.JOB_TRN_COST_PK(+)");
				//strSql.Append(" AND JOB_TRN_PIA.JOB_CARD_" & BusinessProcess & "_FK=JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK(+)")
				strSql.Append(" AND JOB_COST.JOB_CARD_TRN_FK=JOB_EXP.JOB_CARD_TRN_PK(+)");
				strSql.Append(" AND COST_ELE.COST_ELEMENT_MST_PK=INV_TRN.COST_ELEMENT_MST_FK");

			} else {
				strSql.Append("SELECT ROWNUM SrNO ,");
				strSql.Append("JOB_EXP.JOBCARD_REF_NO AS REF_NR,");
				strSql.Append("JOB_EXP.JOB_CARD_TRN_PK AS JOB_CARD_SEA_EXP_PK,");
				//strSql.Append("JOB_TRN_PIA.JOB_TRN_" & BusinessProcess & "_PIA_PK AS JOB_TRN_SEA_EXP_PIA_PK,")
				strSql.Append(" JOB_COST.JOB_TRN_COST_PK AS JOB_TRN_EST_PK,");
				//strSql.Append("JOB_TRN_PIA.INV_SUPPLIER_FK ,")
				strSql.Append(" JOB_COST.INV_SUPPLIER_FK,");
				strSql.Append("'' INV_SUPPLIER_TRN_FK ,");
				strSql.Append("COST_ELE.COST_ELEMENT_ID AS COST_ELEMENT_ID,");
				strSql.Append("COST_ELE.COST_ELEMENT_MST_PK AS COST_ELEMENT_MST_PK,");
				//strSql.Append("JOB_TRN_PIA.VENDOR_MST_FK AS VENDOR_MST_FK,")
				strSql.Append(" JOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
				//strSql.Append("ROUND(JOB_TRN_PIA.ESTIMATED_AMT * GET_EX_RATE(JOB_TRN_PIA.Currency_Mst_Fk," & CStr(CurrencyPK) & ",SYSDATE),2) AS ESTIMATED_AMT,")
				strSql.Append("  ROUND(JOB_COST.ESTIMATED_COST * GET_EX_RATE(JOB_COST.CURRENCY_MST_FK, " + Convert.ToString(CurrencyPK) + ", SYSDATE),2) AS ESTIMATED_AMT,");
				//strSql.Append("ROUND(JOB_TRN_PIA.Invoice_Amt * GET_EX_RATE(JOB_TRN_PIA.Currency_Mst_Fk," & CStr(CurrencyPK) & ",SYSDATE),2) AS ACTUAL_AMT,")
				//strSql.Append("  ROUND(INV_TRN.ACTUAL_AMT * GET_EX_RATE(JOB_COST.CURRENCY_MST_FK, 173, SYSDATE), 2) AS ACTUAL_AMT,")
				strSql.Append(" 0 ACTUAL_AMT,");
				//strSql.Append(" JOB_TRN_PIA.TAX_PERCENTAGE,JOB_TRN_PIA.TAX_AMT,")
				//strSql.Append(" INV_TRN.TAX_PERCENT AS TAX_PERCENTAGE, INV_TRN.TAX_AMOUNT AS TAX_AMT,")
				strSql.Append(" 0 TAX_PERCENTAGE,0 TAX_AMT,");
				//strSql.Append(" INV_TRN.TOTAL_COST,")
				strSql.Append(" JOB_COST.TOTAL_COST,");
				// added by Manohar 08May07: to get null in Diff if any of the cost is null
				//strSql.Append(" (CASE WHEN JOB_TRN_PIA.ESTIMATED_AMT IS NOT NULL AND JOB_TRN_PIA.Invoice_Amt IS NOT NULL THEN ")
				//strSql.Append("NVL(ROUND(JOB_TRN_PIA.Invoice_Amt * GET_EX_RATE(JOB_TRN_PIA.Currency_Mst_Fk," & CStr(CurrencyPK) & ",SYSDATE),2),0) - NVL(ROUND(JOB_TRN_PIA.ESTIMATED_AMT * GET_EX_RATE(JOB_TRN_PIA.Currency_Mst_Fk," & CStr(CurrencyPK) & ",SYSDATE),2),0)")
				//strSql.Append(" ELSE NULL END) DIFFERENCE_AMT, ")
				strSql.Append(" (CASE WHEN JOB_COST.ESTIMATED_COST IS NOT NULL AND JOB_COST.TOTAL_COST IS NOT NULL THEN ");
				//strSql.Append(" NVL(ROUND(JOB_COST.TOTAL_COST * GET_EX_RATE(JOB_COST.Currency_Mst_Fk," & CStr(CurrencyPK) & ",SYSDATE),2),0) - NVL(ROUND(JOB_COST.ESTIMATED_COST * GET_EX_RATE(JOB_COST.Currency_Mst_Fk," & CStr(CurrencyPK) & ",SYSDATE),2),0) ")
				strSql.Append(" 0 - NVL(ROUND(JOB_COST.ESTIMATED_COST * GET_EX_RATE(JOB_COST.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",SYSDATE),2),0) ");
				strSql.Append(" ELSE NULL END) DIFFERENCE_AMT,");
				strSql.Append(" '' Sel");
				strSql.Append(" FROM ");
				//  If currentBusinessType = 2 Or currentBusinessType = 3 Then
				// strSql &= vbCrLf & "JOB_TRN_SEA_EXP_PIA JOB_TRN_PIA,"
				// End If

				//strSql.Append("COST_ELEMENT_MST_TBL COST_ELE, INV_SUPPLIER_TRN_TBL  IST,")
				strSql.Append("COST_ELEMENT_MST_TBL COST_ELE, ");
				//strSql &= vbCrLf & "CURRENCY_TYPE_MST_TBL CURR, JOB_TRN_SEA_EXP_FD JOBFRT,"
				strSql.Append("CURRENCY_TYPE_MST_TBL CURR, ");
				//strSql.Append(" INV_SUPPLIER_TBL      INV_SUP,")
				// strSql.Append(" INV_SUPPLIER_TRN_TBL  INV_TRN,")
				//strSql &= vbCrLf & "JOB_CARD_SEA_EXP_TBL JOB_EXP, FREIGHT_ELEMENT_MST_TBL FMT "
				//strSql.Append("JOB_CARD_" & BusinessProcess & "_TBL JOB_EXP,  JOB_TRN_" & BusinessProcess & "_PIA JOB_TRN_PIA ")
				strSql.Append("JOB_CARD_TRN JOB_EXP,  JOB_TRN_COST JOB_COST ");
				strSql.Append("WHERE");
				//strSql &= vbCrLf & "JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK"
				// strSql.Append(" JOB_TRN_PIA.JOB_CARD_" & BusinessProcess & "_FK = JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK")
				strSql.Append(" JOB_COST.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
				//strSql.Append(" AND JOB_COST.JOB_TRN_" & BusinessProcess & "_COST_PK = IST.JOB_TRN_EST_FK(+) ")
				strSql.Append(" AND JOB_COST.JOB_TRN_COST_PK NOT IN (SELECT IST.JOB_TRN_EST_FK ");
				strSql.Append(" FROM INV_SUPPLIER_TRN_TBL IST, INV_SUPPLIER_TBL IT ");
				strSql.Append(" WHERE IST.INV_SUPPLIER_TBL_FK = IT.INV_SUPPLIER_PK ");
				strSql.Append(" AND IT.BUSINESS_TYPE=" + Business_Type);
				strSql.Append(" AND IT.PROCESS_TYPE=" + Process_Type);
				strSql.Append(" AND IST.JOB_TRN_EST_FK IS NOT NULL)  ");
				//strSql.Append(" AND JOB_TRN_PIA.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK")
				strSql.Append(" AND JOB_COST.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
				//strSql.Append(" AND JOB_TRN_PIA.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ")
				strSql.Append("  AND JOB_COST.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
				//strSql.Append(" AND JOB_TRN_PIA.INVOICE_SEA_TBL_FK IS NULL")
				//strSql.Append("  AND IST.JOB_TRN_EST_FK IS NULL ")
				//strSql.Append(" AND JOB_TRN_PIA.INV_SUPPLIER_FK IS NULL")
				strSql.Append("  AND JOB_COST.INV_SUPPLIER_FK IS NULL");
				// strSql.Append("  AND INV_SUP.INV_SUPPLIER_PK = INV_TRN.INV_SUPPLIER_TBL_FK")
				// strSql.Append("  AND INV_TRN.JOB_TRN_EST_FK = JOB_COST.JOB_TRN_" & BusinessProcess & "_COST_PK")
				//strSql.Append(" AND JOB_TRN_PIA.VENDOR_MST_FK=" & VendorPK)
				strSql.Append(" AND JOB_COST.VENDOR_MST_FK=" + VendorPK);
				//'End 
			}

			strSql.Append(strCondition);
			try {
				Int32 Count = 0;
				WorkFlow objWF = new WorkFlow();
				return objWF.GetDataSet(strSql.ToString());
				//Count = ROECount(CurrencyPK, InvoiceDt)
			} catch (OracleException OraExp) {
				throw OraExp;
				//'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
		public int FETCH_ROE(int Currpk, string INVSUP_DT)
		{
			StringBuilder strSql = new StringBuilder();
			WorkFlow objWF = new WorkFlow();
			strSql.Append("  SELECT  GET_EX_RATE_BUY(" + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", " + Currpk + ", " + INVSUP_DT + ") ROE FROM DUAL");
			try {
				return Convert.ToInt32(objWF.ExecuteScaler(strSql.ToString()));
			} catch (Exception ex) {
				throw ex;
			}

		}

		#region "Fetch JobCard_Invoice"
		public DataSet FetchJobCard_Inv(Int32 VendorPK, Int16 Business_Type, Int16 Process_Type, string FromDt = "", string ToDt = "", string RefNr = "", Int32 CostElementPk = 0, Int32 CurrencyPK = 0, string CurrencyID = "", Int32 TradePK = 0,
		string Vsl = "", string INVOICENO = "", Int64 lblInvSupplierPK = 0, string General = "", int MBLPK = 0, string MBLNR = "", int HBLPK = 0, string HBLNR = "", string INVSUP_DT = "", int ChkExRate = 0)
		{
			WorkFlow objWF = new WorkFlow();
			StringBuilder strSql = new StringBuilder();
			string strCondition = null;
			DataSet MainDS = new DataSet();
			OracleDataAdapter DA = new OracleDataAdapter();
			string BusinessProcess = null;
			string VslFlight = null;
			int chk_ExRATE = 0;
			int ROE = 0;
			try {
				if (ChkExRate == 1) {
					chk_ExRATE = 0;
				} else {
                    chk_ExRATE = Convert.ToInt32(HttpContext.Current.Session["CHECK_EXRATE"]);
				}

				INVSUP_DT = INVSUP_DT.Trim();
				if (string.IsNullOrEmpty(INVSUP_DT)) {
					INVSUP_DT = " TO_DATE(SYSDATE,DATEFORMAT) ";
				} else {
					INVSUP_DT = " TO_DATE('" + INVSUP_DT + "',DATEFORMAT) ";
				}
				ROE = FETCH_ROE(CurrencyPK, INVSUP_DT);
				//Added By Arun as Header Currency can be any selected currency.
				chk_ExRATE = 1;
				ROE = 0;
				//****************************************************************
				if (Business_Type == 2) {
					BusinessProcess = "SEA";
					VslFlight = "VESSEL_NAME";
				} else {
					BusinessProcess = "AIR";
					VslFlight = "FLIGHT_NO";
				}
				if (Process_Type == 1) {
					BusinessProcess += "_EXP";
				} else {
					BusinessProcess += "_IMP";
				}
				if (((FromDt != null) & (ToDt != null)) & FromDt != " " & ToDt != " ") {
					strCondition += " AND TO_DATE(JOB_EXP.JOBCARD_DATE,'" + dateFormat + "') BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
				} else if (((FromDt != null)) & FromDt != " ") {
					strCondition += " AND TO_DATE(JOB_EXP.JOBCARD_DATE,'" + dateFormat + "') >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
				} else if (((ToDt != null)) & ToDt != " ") {
					strCondition += " AND TO_DATE(JOB_EXP.JOBCARD_DATE,'" + dateFormat + "') <= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
				}
				if (!string.IsNullOrEmpty(INVOICENO)) {
					strCondition += " AND INV_SUP.INVOICE_REF_NO ='" + INVOICENO + "'";
				}
				if (CostElementPk != 0) {
					strCondition += " AND COST_ELE.COST_ELEMENT_MST_PK =" + CostElementPk;
				}
				if (!string.IsNullOrEmpty(RefNr)) {
					strCondition += " AND JOB_EXP.JOBCARD_REF_NO ='" + RefNr + "'";
				}
				if (!string.IsNullOrEmpty(Vsl)) {
					if (Business_Type == 2) {
						//strCondition &= vbCrLf & " AND JOB_EXP.VESSEL_NAME ||" & "'-'" & " || JOB_EXP.VOYAGE like '" & Vsl.Trim & "'"
						strCondition += " AND JOB_EXP.VESSEL_NAME  like '" + Vsl.Trim() + "'";
					} else {
						strCondition += " AND JOB_EXP.VOYAGE_FLIGHT_NO like '" + Vsl.Trim() + "'";
					}
				}
				if (MBLPK != 0) {
					if (Process_Type == 1) {
						strCondition += "  AND JOB_EXP.MBL_MAWB_FK=" + MBLPK;
					}
				}
				if (!string.IsNullOrEmpty(MBLNR)) {
					if (Process_Type == 2) {
						strCondition += " AND JOB_EXP.MBL_REF_NO like '" + MBLNR.Trim() + "'";
					}
				}
				if (HBLPK != 0) {
					if (Process_Type == 1) {
						strCondition += "  AND JOB_EXP.HBL_HAWB_FK=" + HBLPK;
					}
				}
				if (!string.IsNullOrEmpty(HBLNR)) {
					if (Process_Type == 2) {
						strCondition += " AND JOB_EXP.HBL_HAWB_REF_NO like '" + HBLNR.Trim() + "'";
					}
				}
				strCondition += " AND NVL(JOB_COST.EXT_INT_FLAG,0)<>1 ";

				strCondition += " AND JOB_EXP.BUSINESS_TYPE = " + Business_Type;
				strCondition += " AND JOB_EXP.PROCESS_TYPE = " + Process_Type;

				//'GOUTAM : DTS : 13213
				if (Process_Type == 1) {
					strCondition += " AND BOOK.STATUS <> 3 ";
				}
				//'
				if (lblInvSupplierPK != 0) {
					strSql.Append("SELECT '' SRNO,");
					strSql.Append("       Q.REF_NR AS REF_NR,");
					strSql.Append("       Q.JOB_CARD_SEA_EXP_PK,");
					strSql.Append("       Q.INV_SUPPLIER_FK AS INV_SUPPLIER_FK,");
					strSql.Append("       Q.INV_SUPPLIER_TRN_FK INV_SUPPLIER_TRN_FK,");
					strSql.Append("       Q.VENDOR_MST_FK AS VENDOR_MST_FK,");
					strSql.Append("       Q.VESVOYAGE,");
					strSql.Append("       NVL(Q.TEU_FACTOR, 0) TEUS,");
					strSql.Append("       NVL(Q.PACK_COUNT, 0) QUANTITY,");
					strSql.Append("       NVL(Q.VOLUME_IN_CBM, 0) VOLUME,");
					strSql.Append("       NVL(Q.GROSS_WEIGHT, 0) GROSS_WEIGHT,");
					strSql.Append("       NVL(Q.NET_WEIGHT, 0) NET_WEIGHT,");
					strSql.Append("       NVL(SUM(Q.ESTIMATED_AMT), 0) ESTIMATED_AMT,");
					strSql.Append("       NVL(SUM(Q.ACTUAL_AMT), 0) ACTUAL_AMT,");
					strSql.Append("       NVL(SUM(Q.TAX_PERCENTAGE), 0) TAX_PERCENTAGE,");
					strSql.Append("       NVL(SUM(Q.TAX_AMT), 0) TAX_AMT,");
					strSql.Append("       NVL(SUM(Q.TOTAL_COST), 0) TOTAL_COST,");
					strSql.Append("       Q.FACCURR,");
					strSql.Append("       NVL(SUM(Q.FAC_AMT), 0) FAC_AMT,");
					strSql.Append("       NVL(SUM(Q.ROE), 0) ROE,");
					strSql.Append("       NVL(SUM(Q.FAC_AMT * Q.ROE), 0) FAC_AMT_VOU,");
					strSql.Append("       NVL(SUM(Q.PAYABLE_AMT),0)PAYABLE_AMT,");
					strSql.Append("       NVL(SUM(Q.PAYABLE_AMT)- SUM(Q.ESTIMATED_AMT), 0) DIFF,");
					strSql.Append("       Q.SEL,");
					strSql.Append("       Q.CHK");
					strSql.Append("  FROM (SELECT DISTINCT '' SRNO,");
					strSql.Append("                        JOB_EXP.JOBCARD_REF_NO AS REF_NR,");
					strSql.Append("                        JOB_EXP.JOB_CARD_TRN_PK AS JOB_CARD_SEA_EXP_PK,");
					strSql.Append("                        '' JOB_TRN_EST_PK,");
					strSql.Append("                        INV_SUP.INV_SUPPLIER_PK INV_SUPPLIER_FK,");
					strSql.Append("                        '' INV_SUPPLIER_TRN_FK,");
					if (Business_Type == 2) {
						strSql.Append("                        (CASE");
						strSql.Append("                          WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						strSql.Append("                               NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
						strSql.Append("                           ''");
						strSql.Append("                          ELSE");
						strSql.Append("                           NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						strSql.Append("                           NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
						strSql.Append("                        END) AS VESVOYAGE,");
						strSql.Append("                        JOBCONT.TEU_FACTOR,");
					} else {
						strSql.Append("       JOB_EXP.VOYAGE_FLIGHT_NO AS VESVOYAGE,");
						strSql.Append("                        0 TEU_FACTOR,");
					}
					strSql.Append("                        JOBCONT.PACK_COUNT,");
					strSql.Append("                        JOBCONT.VOLUME_IN_CBM,");
					strSql.Append("                        JOBCONT.GROSS_WEIGHT,");
					strSql.Append("                        JOBCONT.NET_WEIGHT,");
					strSql.Append("                        JOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
					strSql.Append("                        ROUND(INV_TRN.ESTIMATED_AMT *");
					strSql.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					//CHANGED FROM SELL TO BUY RATE
					strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					strSql.Append("                                           " + INVSUP_DT + "),");
					strSql.Append("                              2) AS ESTIMATED_AMT,");
					strSql.Append("                        ROUND(INV_TRN.ACTUAL_AMT *");
					strSql.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					//CHANGED FROM SELL TO BUY RATE
					strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					strSql.Append("                                          " + INVSUP_DT + "),");
					strSql.Append("                              2) AS ACTUAL_AMT,");
					strSql.Append("                        INV_TRN.TAX_PERCENT AS TAX_PERCENTAGE,");

					strSql.Append("                        ROUND(((INV_TRN.ACTUAL_AMT * NVL(INV_TRN.TAX_PERCENT,0) * ");
					strSql.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					strSql.Append("                                          " + INVSUP_DT + "))/100),2) TAX_AMT,");

					//strSql.Append("                        INV_TRN.TAX_AMOUNT AS TAX_AMT,")

					strSql.Append("                        ROUND((INV_TRN.TOTAL_COST+(INV_TRN.ACTUAL_AMT * NVL(INV_TRN.TAX_PERCENT,0)))*");
					strSql.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					strSql.Append("                                          " + INVSUP_DT + "),2) TOTAL_COST,");

					//strSql.Append("                        INV_TRN.TOTAL_COST,")

					//strSql.Append("                        CURR.CURRENCY_ID FACCURR,")CurrencyID
					strSql.Append("                        '" + CurrencyID + "' FACCURR,");
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
						strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
						strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					//AIR
					} else if (Business_Type == 1 & Process_Type == 1) {
						strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
						strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					} else if (Business_Type == 1 & Process_Type == 2) {
						strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
						strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					}
					strSql.Append("                        GET_EX_RATE(INV_SUP.CURRENCY_MST_FK, " + Convert.ToString(CurrencyPK) + ", " + INVSUP_DT + ") ROE,");
					strSql.Append("                       INV_TRN.PAYABLE_AMT,");
					strSql.Append("                        'true' SEL,");
					strSql.Append("                        '' CHK ");

					strSql.Append("          FROM CURRENCY_TYPE_MST_TBL  CURR,");
					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("               BOOKING_MST_TBL        BOOK,");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("               BOOKING_MST_TBL        BOOK,");
					}
					strSql.Append("               JOB_CARD_TRN   JOB_EXP,");
					strSql.Append("               JOB_TRN_COST   JOB_COST,");
					if (Process_Type == 1) {
						strSql.Append("           HBL_EXP_TBL            HBL,");
					}
					///'
					strSql.Append("(SELECT DISTINCT JOB_CONT.JOB_CARD_TRN_FK,");
					if (Business_Type == 2) {
						strSql.Append("                                SUM(CTMT.TEU_FACTOR) TEU_FACTOR,");
					} else {
						strSql.Append("                                0 TEU_FACTOR,");
					}
					strSql.Append("                                SUM(JOB_CONT.PACK_COUNT) PACK_COUNT,");
					strSql.Append("                                SUM(JOB_CONT.VOLUME_IN_CBM) VOLUME_IN_CBM,");
					strSql.Append("                                SUM(JOB_CONT.GROSS_WEIGHT) GROSS_WEIGHT,");
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						strSql.Append("                        (CASE  WHEN BKG.CARGO_TYPE = 1 THEN");
						strSql.Append("                          SUM(JOB_CONT.NET_WEIGHT)");
						strSql.Append("                         ELSE SUM(JOB_CONT.CHARGEABLE_WEIGHT)");
						strSql.Append("                         END) NET_WEIGHT");
					} else if (Business_Type == 2 & Process_Type == 2) {
						strSql.Append("                        (CASE  WHEN JOB.CARGO_TYPE = 1 THEN");
						strSql.Append("                          SUM(JOB_CONT.NET_WEIGHT)");
						strSql.Append("                         ELSE SUM(JOB_CONT.CHARGEABLE_WEIGHT)");
						strSql.Append("                         END) NET_WEIGHT");
					//'AIR
					} else if (Business_Type == 1) {
						strSql.Append("     SUM(JOB_CONT.CHARGEABLE_WEIGHT) NET_WEIGHT");
					}
					strSql.Append("                  FROM JOB_CARD_TRN   JOB,");
					strSql.Append("                       JOB_TRN_CONT   JOB_CONT");
					if (Business_Type == 2) {
						strSql.Append("                       ,CONTAINER_TYPE_MST_TBL CTMT ");
					}

					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("                       ,BOOKING_MST_TBL        BKG");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("                       ,BOOKING_MST_TBL        BKG");
					}

					strSql.Append("                 WHERE JOB.JOB_CARD_TRN_PK = JOB_CONT.JOB_CARD_TRN_FK ");
					if (Business_Type == 2) {
						strSql.Append("                   AND JOB_CONT.CONTAINER_TYPE_MST_FK =");
						strSql.Append("                       CTMT.CONTAINER_TYPE_MST_PK(+)");
					}
					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("           AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("           AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
					}
					strSql.Append("                 GROUP BY JOB_CONT.JOB_CARD_TRN_FK");
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						strSql.Append("   , BKG.CARGO_TYPE) JOBCONT,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						strSql.Append("   , JOB.CARGO_TYPE ) JOBCONT,");
					//'AIR
					} else if (Business_Type == 1) {
						strSql.Append("  ) JOBCONT,");
					}

					strSql.Append("               COST_ELEMENT_MST_TBL   COST_ELE,");
					strSql.Append("               INV_SUPPLIER_TBL       INV_SUP,");
					strSql.Append("               INV_SUPPLIER_TRN_TBL   INV_TRN");
					strSql.Append("         WHERE JOB_COST.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK(+) ");
					strSql.Append("           AND JOB_EXP.JOB_CARD_TRN_PK = JOBCONT.JOB_CARD_TRN_FK ");
					if (Process_Type == 1) {
						strSql.Append("           AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
					}
					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					}
					strSql.Append("           AND INV_TRN.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
					strSql.Append("           AND JOB_COST.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
					strSql.Append("           AND JOB_COST.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
					strSql.Append("           AND INV_SUP.INV_SUPPLIER_PK = INV_TRN.INV_SUPPLIER_TBL_FK");
					strSql.Append("           AND INV_SUP.VENDOR_MST_FK = " + VendorPK);
					strSql.Append("           AND INV_TRN.JOB_TRN_EST_FK = JOB_COST.JOB_TRN_COST_PK(+)");
					strSql.Append(strCondition);
					strSql.Append("    ) Q ");
					strSql.Append(" GROUP BY Q.VENDOR_MST_FK,");
					strSql.Append("          Q.REF_NR,");
					strSql.Append("          Q.JOB_CARD_SEA_EXP_PK,");
					strSql.Append("          Q.INV_SUPPLIER_FK,");
					strSql.Append("          Q.INV_SUPPLIER_TRN_FK,");
					strSql.Append("          Q.VESVOYAGE,");
					strSql.Append("          Q.VENDOR_MST_FK,");
					strSql.Append("          Q.FACCURR,");
					strSql.Append("          Q.TEU_FACTOR,");
					strSql.Append("          Q.PACK_COUNT,");
					strSql.Append("          Q.VOLUME_IN_CBM,");
					strSql.Append("          Q.GROSS_WEIGHT,");
					strSql.Append("          Q.NET_WEIGHT,Q.SEL");
					strSql.Append("         ORDER BY REF_NR DESC");

					//If Business_Type = 1 Then
					//    strSql.Replace("HBL", "HAWB")
					//    strSql.Replace("MBL", "MAWB")
					//End If

					DA = objWF.GetDataAdapter(strSql.ToString());
					DA.Fill(MainDS, "PARENT");

					strSql.Remove(0, strSql.Length);

					strSql.Append("SELECT Q.SHIP_REF_NR,");
					strSql.Append("       Q.REF_DATE,");
					strSql.Append("       Q.CARGOTYPE,");
					strSql.Append("       Q.JOB_CARD_SEA_EXP_PK,");
					strSql.Append("       Q.JOB_TRN_EST_PK,");
					strSql.Append("       Q.INV_SUPPLIER_FK,");
					strSql.Append("       Q.INV_SUPPLIER_TRN_FK,");
					strSql.Append("       Q.VENDOR_MST_FK,");
					strSql.Append("       Q.COST_ELEMENT_ID,");
					strSql.Append("       Q.COST_ELEMENT_MST_PK,");
					strSql.Append("       Q.VESVOYAGE,");
					strSql.Append("       Q.CONTAINER_TYPE_MST_ID, ");
					strSql.Append("       NVL(Q.ESTIMATED_AMT, 0) ESTIMATED_AMT,");
					strSql.Append("       NVL(Q.ACTUAL_AMT, 0) ACTUAL_AMT,");
					strSql.Append("       NVL(Q.TAX_PERCENTAGE, 0) TAX_PERCENTAGE,");
					strSql.Append("       NVL(Q.TAX_AMT, 0) TAX_AMT,");
					strSql.Append("       NVL(Q.TOTAL_COST, 0) TOTAL_COST,");
					strSql.Append("       Q.FACCURR,");
					strSql.Append("       NVL(Q.FAC_AMT, 0) FAC_AMT,");
					strSql.Append("       NVL(Q.ROE, 0) ROE,");
					strSql.Append("       NVL(Q.FAC_AMT * Q.ROE, 0) FAC_AMT_VOU,");
					strSql.Append("       NVL(Q.PAYABLE_AMT,0)PAYABLE_AMT,");
					strSql.Append("       NVL((Q.PAYABLE_AMT - Q.ESTIMATED_AMT), 0) DIFF,");
					strSql.Append("       Q.SEL,Q.REF_NR");
					strSql.Append("  FROM (SELECT DISTINCT ");
					if (Process_Type == 1) {
						strSql.Append("                        HBL.HBL_REF_NO SHIP_REF_NR,");
						strSql.Append("                        TO_CHAR(HBL.HBL_DATE,DATEFORMAT) AS REF_DATE,");
					} else {
						strSql.Append("            JOB_EXP.HBL_HAWB_REF_NO SHIP_REF_NR,");
						strSql.Append("            JOB_EXP.HBL_HAWB_DATE AS REF_DATE, ");
					}
					if (Business_Type == 2 & Process_Type == 1) {
						strSql.Append("                        DECODE(BOOK.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGOTYPE,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						strSql.Append("                        DECODE(JOB_EXP.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGOTYPE,");
					} else if (Business_Type == 1) {
						strSql.Append(" '' CARGOTYPE,");
					}
					strSql.Append("                        JOB_EXP.JOB_CARD_TRN_PK AS JOB_CARD_SEA_EXP_PK,");
					strSql.Append("                        JOB_COST.JOB_TRN_COST_PK AS JOB_TRN_EST_PK,");
					strSql.Append("                        JOB_COST.INV_SUPPLIER_FK,");
					strSql.Append("                        INV_TRN.INV_SUPPLIER_TRN_PK INV_SUPPLIER_TRN_FK,");
					strSql.Append("                        COST_ELE.COST_ELEMENT_ID,");
					strSql.Append("                        COST_ELE.COST_ELEMENT_MST_PK,");
					if (Business_Type == 2) {
						strSql.Append("                        (CASE");
						strSql.Append("                          WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						strSql.Append("                               NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
						strSql.Append("                           ''");
						strSql.Append("                          ELSE");
						strSql.Append("                           NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						strSql.Append("                           NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
						strSql.Append("                        END) AS VESVOYAGE,");
					} else {
						strSql.Append("       JOB_EXP.VOYAGE_FLIGHT_NO AS VESVOYAGE,");
					}
					strSql.Append("                        JOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
					strSql.Append("                       ROUND(INV_TRN.ESTIMATED_AMT *");
					strSql.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					//SELL TO BUY RATE
					strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					strSql.Append("                                          " + INVSUP_DT + "),");
					strSql.Append("                              2) AS ESTIMATED_AMT,");
					strSql.Append("                        ROUND(INV_TRN.ACTUAL_AMT *");
					strSql.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					//SELL TO BUY RATE
					strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					strSql.Append("                                          " + INVSUP_DT + "),");
					strSql.Append("                              2) AS ACTUAL_AMT,");
					strSql.Append("                        INV_TRN.TAX_PERCENT AS TAX_PERCENTAGE,");
					//*******************************************************************************
					//strSql.Append("                        INV_TRN.TAX_AMOUNT AS TAX_AMT,")
					//strSql.Append("                        INV_TRN.TOTAL_COST,")

					strSql.Append("                        ROUND(((INV_TRN.ACTUAL_AMT * NVL(INV_TRN.TAX_PERCENT,0) * ");
					strSql.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					strSql.Append("                                          " + INVSUP_DT + "))/100),2) TAX_AMT,");

					//strSql.Append("                        ROUND((INV_TRN.TOTAL_COST+(INV_TRN.ACTUAL_AMT * NVL(INV_TRN.TAX_PERCENT,0)))*")
					//strSql.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,")
					//strSql.Append("                                          " & CStr(CurrencyPK) & ",")
					//strSql.Append("                                          " & INVSUP_DT & "),2) TOTAL_COST,")
					//'GOUTAM(REF. DTS ID : 13215) 
					strSql.Append("                        ROUND(INV_TRN.TOTAL_COST * ");
					strSql.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					strSql.Append("                                          " + INVSUP_DT + "),2) TOTAL_COST,");
					//*******************************************************************************
					//strSql.Append("                        CURR.CURRENCY_ID FACCURR,")
					strSql.Append("                        '" + CurrencyID + "' FACCURR,");
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
						strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
						strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					//AIR
					} else if (Business_Type == 1 & Process_Type == 1) {
						strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
						strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					} else if (Business_Type == 1 & Process_Type == 2) {
						strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
						strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					}
					strSql.Append("                        GET_EX_RATE(INV_SUP.CURRENCY_MST_FK, " + Convert.ToString(CurrencyPK) + ", " + INVSUP_DT + ") ROE,");
					strSql.Append("                 INV_TRN.PAYABLE_AMT,");
					strSql.Append("                        'true' SEL,JOB_EXP.JOBCARD_REF_NO AS REF_NR,COST_ELE.Preference,");
					if (Business_Type == 2) {
						strSql.Append("   CASE WHEN  JOB_EXP.CARGO_TYPE = 4 THEN  ");
						strSql.Append("    COMM.COMMODITY_NAME ");
						strSql.Append("  ELSE CTMT.CONTAINER_TYPE_MST_ID END CONTAINER_TYPE_MST_ID ");
					} else {
						strSql.Append("     ''CONTAINER_TYPE_MST_ID ");
					}
					strSql.Append("          FROM COST_ELEMENT_MST_TBL   COST_ELE,");
					strSql.Append("               CURRENCY_TYPE_MST_TBL  CURR,");
					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("               BOOKING_MST_TBL        BOOK,");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("               BOOKING_MST_TBL        BOOK,");
					}
					strSql.Append("               JOB_CARD_TRN   JOB_EXP,");
					strSql.Append("               JOB_TRN_COST   JOB_COST,");
					strSql.Append("               JOB_TRN_CONT   JOB_CONT,");
					if (Business_Type == 2) {
						strSql.Append("                       CONTAINER_TYPE_MST_TBL CTMT, ");
						strSql.Append("                        COMMODITY_MST_TBL COMM, ");
					}
					if (Process_Type == 1) {
						strSql.Append("           HBL_EXP_TBL            HBL,");
					}
					strSql.Append("               INV_SUPPLIER_TBL       INV_SUP,");
					strSql.Append("               INV_SUPPLIER_TRN_TBL   INV_TRN");
					strSql.Append("         WHERE JOB_COST.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK(+)");
					strSql.Append("           AND JOB_EXP.JOB_CARD_TRN_PK = JOB_CONT.JOB_CARD_TRN_FK ");
					strSql.Append("           AND JOB_COST.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					}
					if (Process_Type == 1) {
						strSql.Append("           AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
					}
					if (Business_Type == 2) {
						strSql.Append("                   AND JOB_COST.CONTAINER_TYPE_MST_FK =");
						strSql.Append("                       CTMT.CONTAINER_TYPE_MST_PK(+)");
						strSql.Append("            AND JOB_COST.CONTAINER_TYPE_MST_FK = COMM.COMMODITY_MST_PK(+) ");
					}
					strSql.Append("           AND INV_SUP.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
					strSql.Append("           AND INV_TRN.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
					strSql.Append("           AND INV_SUP.INV_SUPPLIER_PK = INV_TRN.INV_SUPPLIER_TBL_FK");
					strSql.Append("           AND INV_SUP.VENDOR_MST_FK = " + VendorPK);
					strSql.Append("           AND INV_TRN.JOB_TRN_EST_FK = JOB_COST.JOB_TRN_COST_PK(+)");
					strSql.Append(strCondition);
					strSql.Append("     ORDER BY COST_ELE.Preference) Q ");
				} else {
					//'
					strSql.Append("SELECT '' SRNO,");
					strSql.Append("       Q.REF_NR AS REF_NR,");
					strSql.Append("       Q.JOB_CARD_SEA_EXP_PK,");
					strSql.Append("       Q.INV_SUPPLIER_FK AS INV_SUPPLIER_FK,");
					strSql.Append("       Q.INV_SUPPLIER_TRN_FK INV_SUPPLIER_TRN_FK,");
					strSql.Append("       Q.VENDOR_MST_FK AS VENDOR_MST_FK,");
					strSql.Append("       Q.VESVOYAGE,");
					strSql.Append("       NVL(Q.TEU_FACTOR, 0) TEUS,");
					strSql.Append("       NVL(Q.PACK_COUNT, 0) QUANTITY,");
					strSql.Append("       NVL(Q.VOLUME_IN_CBM, 0) VOLUME,");
					strSql.Append("       NVL(Q.GROSS_WEIGHT, 0) GROSS_WEIGHT,");
					strSql.Append("       NVL(Q.NET_WEIGHT, 0) NET_WEIGHT,");
					strSql.Append("       NVL(SUM(Q.ESTIMATED_AMT), 0) ESTIMATED_AMT,");
					strSql.Append("       NVL(SUM(Q.ACTUAL_AMT), 0) ACTUAL_AMT,");
					strSql.Append("       NVL(SUM(Q.TAX_PERCENTAGE), 0) TAX_PERCENTAGE,");
					strSql.Append("       NVL(SUM(Q.TAX_AMT), 0) TAX_AMT,");
					strSql.Append("       NVL(SUM(Q.TOTAL_COST), 0) TOTAL_COST,");
					strSql.Append("       Q.FACCURR,");
					strSql.Append("       NVL(SUM(Q.FAC_AMT), 0) FAC_AMT,");
					strSql.Append("       NVL(SUM(Q.ROE), 0) ROE,");
					if (chk_ExRATE == 1 & ROE == 0) {
						strSql.Append("       NVL(SUM(Q.FAC_AMT), 0) FAC_AMT_VOU,");
						strSql.Append("       NVL(SUM(Q.TOTAL_COST - NVL(Q.FAC_AMT,0)), 0) PAYABLE_AMT,");
						//strSql.Append("       NVL(SUM((Q.TOTAL_COST - (Q.FAC_AMT)) - (Q.ESTIMATED_AMT)), 0) DIFF,")
						strSql.Append("       NVL(SUM(Q.FAC_AMT), 0) DIFF,");
					} else {
						strSql.Append("       NVL(SUM(Q.FAC_AMT * Q.ROE), 0) FAC_AMT_VOU,");
						strSql.Append("       NVL(SUM(Q.TOTAL_COST - (NVL(Q.FAC_AMT,0) * Q.ROE)), 0) PAYABLE_AMT,");
						//strSql.Append("       NVL(SUM((Q.TOTAL_COST - (Q.FAC_AMT * Q.ROE)) - (Q.ESTIMATED_AMT)), 0) DIFF,")
						strSql.Append("       NVL(SUM(Q.FAC_AMT * Q.ROE), 0) DIFF,");
					}

					strSql.Append("       Q.SEL,Q.Chk");
					strSql.Append("  FROM (SELECT DISTINCT ''SRNO,");
					strSql.Append("                        JOB_EXP.JOBCARD_REF_NO AS REF_NR,");
					strSql.Append("                        JOB_EXP.JOB_CARD_TRN_PK AS JOB_CARD_SEA_EXP_PK,");
					strSql.Append("                        ' 'JOB_TRN_EST_PK,");
					strSql.Append("                        JOB_COST.INV_SUPPLIER_FK,");
					strSql.Append("                        '' INV_SUPPLIER_TRN_FK,");
					if (Business_Type == 2) {
						strSql.Append("                        (CASE");
						strSql.Append("                          WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						strSql.Append("                               NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
						strSql.Append("                           ''");
						strSql.Append("                          ELSE");
						strSql.Append("                           NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						strSql.Append("                           NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
						strSql.Append("                        END) AS VESVOYAGE,");
						strSql.Append("                        JOBCONT.TEU_FACTOR,");
					} else {
						strSql.Append("       JOB_EXP.VOYAGE_FLIGHT_NO AS VESVOYAGE,");
						strSql.Append("                        0 TEU_FACTOR,");
					}
					strSql.Append("                        JOBCONT.PACK_COUNT,");
					strSql.Append("                        JOBCONT.VOLUME_IN_CBM,");
					strSql.Append("                        JOBCONT.GROSS_WEIGHT,");
					strSql.Append("                        JOBCONT.NET_WEIGHT,");
					strSql.Append("                        JOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
					if (chk_ExRATE == 1 & ROE == 0) {
						strSql.Append("                        ROUND(JOB_COST.ESTIMATED_COST, ");
						strSql.Append("                              2) AS ESTIMATED_AMT,");
						strSql.Append("                        ROUND(JOB_COST.ESTIMATED_COST,");
						strSql.Append("                              2) AS ACTUAL_AMT,");
					} else {
						strSql.Append("                        ROUND(JOB_COST.ESTIMATED_COST *");
						strSql.Append("                              GET_EX_RATE_BUY(JOB_COST.CURRENCY_MST_FK,");
						// CHANGED FROM SELL TO BUY RATE
						strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                          " + INVSUP_DT + "),");
						strSql.Append("                              2) AS ESTIMATED_AMT,");
						//strSql.Append("                        0 ACTUAL_AMT,") ''Commeneted By Vasava For DTS:13802
						strSql.Append("                        ROUND(JOB_COST.ESTIMATED_COST *");
						strSql.Append("                              GET_EX_RATE_BUY(JOB_COST.CURRENCY_MST_FK,");
						// CHANGED FROM SELL TO BUY RATE
						strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                          " + INVSUP_DT + "),");
						strSql.Append("                              2) AS ACTUAL_AMT,");
					}

					strSql.Append("                        0 TAX_PERCENTAGE,");
					strSql.Append("                        0 TAX_AMT,");
					if (chk_ExRATE == 1 & ROE == 0) {
						strSql.Append("                        ROUND(JOB_COST.ESTIMATED_COST ");
						strSql.Append("                                    *  ");
						strSql.Append("                              GET_EX_RATE_BUY(JOB_COST.CURRENCY_MST_FK,");
						strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                          " + INVSUP_DT + "),2) TOTAL_COST,");
					} else {
						strSql.Append("                        ROUND(JOB_COST.TOTAL_COST*");
						strSql.Append("                              GET_EX_RATE_BUY(JOB_COST.CURRENCY_MST_FK,");
						strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                          " + INVSUP_DT + "),2) TOTAL_COST,");
					}

					strSql.Append("                        '" + CurrencyID + "' FACCURR,");
					if (chk_ExRATE == 1 & ROE == 0) {
						//'SEA
						if (Business_Type == 2 & Process_Type == 1) {
							strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       0,");
							strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 2 & Process_Type == 2) {
							strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       0,");
							strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						//AIR
						} else if (Business_Type == 1 & Process_Type == 1) {
							strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       0,");
							strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 1 & Process_Type == 2) {
							strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       0,");
							strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						}
					} else {
						//'SEA
						if (Business_Type == 2 & Process_Type == 1) {
							strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 2 & Process_Type == 2) {
							strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						//AIR
						} else if (Business_Type == 1 & Process_Type == 1) {
							strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 1 & Process_Type == 2) {
							strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						}
					}


					strSql.Append("                        GET_EX_RATE_BUY(JOB_COST.CURRENCY_MST_FK, " + Convert.ToString(CurrencyPK) + ", " + INVSUP_DT + ") ROE,");
					strSql.Append("                        '' SEL,'' Chk");
					strSql.Append("          FROM CURRENCY_TYPE_MST_TBL  CURR,");
					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("               BOOKING_MST_TBL        BOOK,");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("               BOOKING_MST_TBL        BOOK,");
					}
					strSql.Append("               JOB_CARD_TRN   JOB_EXP,");
					strSql.Append("               JOB_TRN_COST   JOB_COST,");
					if (Process_Type == 1) {
						strSql.Append("               HBL_EXP_TBL      HBL,");
					}
					///'
					strSql.Append("(SELECT DISTINCT JOB_CONT.JOB_CARD_TRN_FK,");
					if (Business_Type == 2) {
						strSql.Append("                                SUM(CTMT.TEU_FACTOR) TEU_FACTOR,");
					} else {
						strSql.Append("                                0 TEU_FACTOR,");
					}
					strSql.Append("                                SUM(JOB_CONT.PACK_COUNT) PACK_COUNT,");
					strSql.Append("                                SUM(JOB_CONT.VOLUME_IN_CBM) VOLUME_IN_CBM,");
					strSql.Append("                                SUM(JOB_CONT.GROSS_WEIGHT) GROSS_WEIGHT,");
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						strSql.Append("                        (CASE  WHEN BKG.CARGO_TYPE = 1 THEN");
						strSql.Append("                          SUM(JOB_CONT.NET_WEIGHT)");
						strSql.Append("                         ELSE SUM(JOB_CONT.CHARGEABLE_WEIGHT)");
						strSql.Append("                         END) NET_WEIGHT");
					} else if (Business_Type == 2 & Process_Type == 2) {
						strSql.Append("                        (CASE  WHEN JOB.CARGO_TYPE = 1 THEN");
						strSql.Append("                          SUM(JOB_CONT.NET_WEIGHT)");
						strSql.Append("                         ELSE SUM(JOB_CONT.CHARGEABLE_WEIGHT)");
						strSql.Append("                         END) NET_WEIGHT");
					//'AIR
					} else if (Business_Type == 1) {
						strSql.Append("     SUM(JOB_CONT.CHARGEABLE_WEIGHT) NET_WEIGHT");
					}
					strSql.Append("                  FROM JOB_CARD_TRN   JOB,");
					strSql.Append("                       JOB_TRN_CONT   JOB_CONT");
					if (Business_Type == 2) {
						strSql.Append("                       ,CONTAINER_TYPE_MST_TBL CTMT");
					}
					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("                       ,BOOKING_MST_TBL        BKG");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("                       ,BOOKING_MST_TBL        BKG");
					}

					strSql.Append("                 WHERE JOB.JOB_CARD_TRN_PK = JOB_CONT.JOB_CARD_TRN_FK");
					if (Business_Type == 2) {
						strSql.Append("                   AND JOB_CONT.CONTAINER_TYPE_MST_FK =");
						strSql.Append("                       CTMT.CONTAINER_TYPE_MST_PK(+)");
					}
					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("           AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("           AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
					}
					strSql.Append("                 GROUP BY JOB_CONT.JOB_CARD_TRN_FK");
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						strSql.Append("   , BKG.CARGO_TYPE) JOBCONT,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						strSql.Append("   , JOB.CARGO_TYPE ) JOBCONT,");
					//'AIR
					} else if (Business_Type == 1) {
						strSql.Append("  ) JOBCONT,");
					}
					///''
					strSql.Append("               COST_ELEMENT_MST_TBL   COST_ELE");
					strSql.Append("         WHERE JOB_COST.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
					strSql.Append("           AND JOB_EXP.JOB_CARD_TRN_PK = JOBCONT.JOB_CARD_TRN_FK");
					if (Process_Type == 1) {
						strSql.Append("           AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
					}
					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					}
					//strSql.Append("           AND JOB_COST.JOB_TRN_" & BusinessProcess & "_COST_PK NOT IN")
					//strSql.Append("               (SELECT IST.JOB_TRN_EST_FK")
					//strSql.Append("                  FROM INV_SUPPLIER_TRN_TBL IST, INV_SUPPLIER_TBL IT")
					//strSql.Append("                 WHERE IST.INV_SUPPLIER_TBL_FK = IT.INV_SUPPLIER_PK")
					//strSql.Append("                   AND IT.BUSINESS_TYPE = " & Business_Type)
					//strSql.Append("                   AND IT.PROCESS_TYPE = " & Process_Type)
					//strSql.Append("                   AND IST.JOB_TRN_EST_FK IS NOT NULL)")

					//'Vasava for Fetching Rejected Voucher JC's Also
					strSql.Append("           AND ((JOB_COST.JOB_TRN_COST_PK NOT IN");
					strSql.Append("               (SELECT IST.JOB_TRN_EST_FK");
					strSql.Append("                  FROM INV_SUPPLIER_TRN_TBL IST, INV_SUPPLIER_TBL IT");
					strSql.Append("                 WHERE IST.INV_SUPPLIER_TBL_FK = IT.INV_SUPPLIER_PK");
					strSql.Append("                   AND IT.BUSINESS_TYPE = " + Business_Type);
					strSql.Append("                   AND IT.PROCESS_TYPE = " + Process_Type);
					strSql.Append("                   AND IST.JOB_TRN_EST_FK IS NOT NULL))");
					strSql.Append("           OR (JOB_COST.JOB_TRN_COST_PK IN");
					strSql.Append("               (SELECT IST.JOB_TRN_EST_FK");
					strSql.Append("                  FROM INV_SUPPLIER_TRN_TBL IST, INV_SUPPLIER_TBL IT");
					strSql.Append("                 WHERE IST.INV_SUPPLIER_TBL_FK = IT.INV_SUPPLIER_PK");
					strSql.Append("                   AND IT.BUSINESS_TYPE = " + Business_Type);
					strSql.Append("                   AND IT.PROCESS_TYPE = " + Process_Type);
					strSql.Append("                   AND IT.APPROVED = 2 ");
					strSql.Append("                   AND IST.JOB_TRN_EST_FK IS NOT NULL)))");
					//'End
					strSql.Append("           AND JOB_COST.COST_ELEMENT_MST_FK=COST_ELE.COST_ELEMENT_MST_PK");
					strSql.Append("           AND JOB_COST.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
					strSql.Append("           AND JOB_COST.INV_SUPPLIER_FK IS NULL");
					strSql.Append("           AND JOB_COST.VENDOR_MST_FK =" + VendorPK);
					strSql.Append(strCondition);
					strSql.Append("          ) Q");
					strSql.Append(" GROUP BY Q.VENDOR_MST_FK,");
					strSql.Append("          Q.REF_NR,");
					strSql.Append("          Q.JOB_CARD_SEA_EXP_PK,");
					strSql.Append("          Q.INV_SUPPLIER_FK,");
					strSql.Append("          Q.INV_SUPPLIER_TRN_FK,");
					strSql.Append("          Q.VESVOYAGE,");
					strSql.Append("          Q.VENDOR_MST_FK,");
					strSql.Append("          Q.FACCURR,");
					strSql.Append("          Q.TEU_FACTOR,");
					strSql.Append("          Q.PACK_COUNT,");
					strSql.Append("          Q.VOLUME_IN_CBM,");
					strSql.Append("          Q.GROSS_WEIGHT,");
					strSql.Append("          Q.NET_WEIGHT");
					strSql.Append("         ORDER BY REF_NR DESC");

					//If Business_Type = 1 Then
					//    strSql.Replace("HBL", "HAWB")
					//    strSql.Replace("MBL", "MAWB")
					//End If
					DA = objWF.GetDataAdapter(strSql.ToString());
					DA.Fill(MainDS, "PARENT");
					///
					strSql.Remove(0, strSql.Length);

					strSql.Append("SELECT Q.SHIP_REF_NR,");
					strSql.Append("       Q.REF_DATE,");
					strSql.Append("       Q.CARGOTYPE,");
					strSql.Append("       Q.JOB_CARD_SEA_EXP_PK,");
					strSql.Append("       Q.JOB_TRN_EST_PK,");
					strSql.Append("       Q.INV_SUPPLIER_FK,");
					strSql.Append("       Q.INV_SUPPLIER_TRN_FK,");
					strSql.Append("       Q.VENDOR_MST_FK,");
					strSql.Append("       Q.COST_ELEMENT_ID,");
					strSql.Append("       Q.COST_ELEMENT_MST_PK,");
					strSql.Append("       Q.VESVOYAGE,");
					strSql.Append("       Q.CONTAINER_TYPE_MST_ID, ");
					strSql.Append("       NVL(Q.ESTIMATED_AMT, 0) ESTIMATED_AMT,");
					strSql.Append("       NVL(Q.ACTUAL_AMT, 0) ACTUAL_AMT,");
					strSql.Append("       NVL(Q.TAX_PERCENTAGE, 0) TAX_PERCENTAGE,");
					strSql.Append("       NVL(Q.TAX_AMT, 0) TAX_AMT,");
					strSql.Append("       NVL(Q.TOTAL_COST, 0) TOTAL_COST,");
					strSql.Append("       Q.FACCURR,");
					strSql.Append("       NVL(Q.FAC_AMT, 0) FAC_AMT,");
					strSql.Append("       NVL(Q.ROE, 0) ROE,");
					if (chk_ExRATE == 1 & ROE == 0) {
						strSql.Append("       NVL(Q.FAC_AMT , 0) FAC_AMT_VOU,");
						strSql.Append("       NVL(Q.TOTAL_COST - NVL(Q.FAC_AMT,0), 0) PAYABLE_AMT,");
						//strSql.Append("       NVL((Q.TOTAL_COST - (Q.FAC_AMT)) - (Q.ESTIMATED_AMT), 0) DIFF,")
						strSql.Append("       NVL(Q.FAC_AMT , 0) DIFF,");
					} else {
						strSql.Append("       NVL(Q.FAC_AMT * Q.ROE, 0) FAC_AMT_VOU,");
						strSql.Append("       NVL(Q.TOTAL_COST - (NVL(Q.FAC_AMT,0) * Q.ROE), 0) PAYABLE_AMT,");
						//strSql.Append("       NVL((Q.TOTAL_COST - (Q.FAC_AMT * Q.ROE)) - (Q.ESTIMATED_AMT), 0) DIFF,")
						strSql.Append("       NVL(Q.FAC_AMT * Q.ROE, 0)  DIFF,");
					}

					strSql.Append("       Q.SEL,Q.REF_NR");
					strSql.Append("  FROM (SELECT  DISTINCT");
					if (Process_Type == 1) {
						strSql.Append("                        HBL.HBL_REF_NO SHIP_REF_NR,");
						strSql.Append("                        TO_CHAR(HBL.HBL_DATE,DATEFORMAT) AS REF_DATE,");
					} else {
						strSql.Append("            JOB_EXP.HBL_HAWB_REF_NO SHIP_REF_NR,");
						strSql.Append("            JOB_EXP.HBL_HAWB_DATE AS REF_DATE, ");
					}
					if (Business_Type == 2 & Process_Type == 1) {
						strSql.Append("                        DECODE(BOOK.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGOTYPE,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						strSql.Append("                        DECODE(JOB_EXP.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGOTYPE,");
					} else if (Business_Type == 1) {
						strSql.Append(" '' CARGOTYPE,");
					}
					strSql.Append("                        JOB_EXP.JOB_CARD_TRN_PK AS JOB_CARD_SEA_EXP_PK,");
					strSql.Append("                        JOB_COST.JOB_TRN_COST_PK AS JOB_TRN_EST_PK,");
					strSql.Append("                        JOB_COST.INV_SUPPLIER_FK,");
					strSql.Append("                        '' INV_SUPPLIER_TRN_FK,");
					strSql.Append("                        COST_ELE.COST_ELEMENT_ID,");
					strSql.Append("                        COST_ELE.COST_ELEMENT_MST_PK,");
					if (Business_Type == 2) {
						strSql.Append("                        (CASE");
						strSql.Append("                          WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						strSql.Append("                               NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
						strSql.Append("                           ''");
						strSql.Append("                          ELSE");
						strSql.Append("                           NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						strSql.Append("                           NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
						strSql.Append("                        END) AS VESVOYAGE,");
					} else {
						strSql.Append("                    JOB_EXP.VOYAGE_FLIGHT_NO AS VESVOYAGE,");
					}
					strSql.Append("                        JOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
					if (chk_ExRATE == 1 & ROE == 0) {
						strSql.Append("                        ROUND(JOB_COST.ESTIMATED_COST , ");
						strSql.Append("                              2) AS ESTIMATED_AMT,");
						strSql.Append("                        ROUND(JOB_COST.ESTIMATED_COST ,");
						strSql.Append("                              2) AS ACTUAL_AMT,");
					} else {
						strSql.Append("                        ROUND(JOB_COST.ESTIMATED_COST *");
						strSql.Append("                              GET_EX_RATE_BUY(JOB_COST.CURRENCY_MST_FK,");
						strSql.Append("                                           " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                          " + INVSUP_DT + "),");
						strSql.Append("                              2) AS ESTIMATED_AMT,");
						//strSql.Append("                        0 ACTUAL_AMT,")
						strSql.Append("                        ROUND(JOB_COST.ESTIMATED_COST *");
						strSql.Append("                              GET_EX_RATE_BUY(JOB_COST.CURRENCY_MST_FK,");
						strSql.Append("                                           " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                          " + INVSUP_DT + "),");
						strSql.Append("                              2) AS ACTUAL_AMT,");
					}

					strSql.Append("                        0 TAX_PERCENTAGE,");
					strSql.Append("                        0 TAX_AMT,");
					if (chk_ExRATE == 1) {
						strSql.Append("                        ROUND(JOB_COST.ESTIMATED_COST ");
						strSql.Append("                          * ");
						strSql.Append("                              GET_EX_RATE_BUY(JOB_COST.CURRENCY_MST_FK,");
						strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                          " + INVSUP_DT + "),2) TOTAL_COST,");
					} else {
						strSql.Append("                        ROUND(JOB_COST.TOTAL_COST*");
						strSql.Append("                              GET_EX_RATE_BUY(JOB_COST.CURRENCY_MST_FK,");
						strSql.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
						strSql.Append("                                          " + INVSUP_DT + "),2) TOTAL_COST,");
					}
					strSql.Append("                        CURR.CURRENCY_ID FACCURR,");
					if (chk_ExRATE == 1 & ROE == 0) {
						//'SEA
						if (Business_Type == 2 & Process_Type == 1) {
							strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       0,");
							strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 2 & Process_Type == 2) {
							strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       0,");
							strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						//AIR
						} else if (Business_Type == 1 & Process_Type == 1) {
							strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       0,");
							strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 1 & Process_Type == 2) {
							strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       0,");
							strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						}
					} else {
						//'SEA
						if (Business_Type == 2 & Process_Type == 1) {
							strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 2 & Process_Type == 2) {
							strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							strSql.Append("                                       2,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						//AIR
						} else if (Business_Type == 1 & Process_Type == 1) {
							strSql.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 1 & Process_Type == 2) {
							strSql.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							strSql.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							strSql.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							strSql.Append("                                       1,JOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						}
					}

					strSql.Append("                        GET_EX_RATE_BUY(JOB_COST.CURRENCY_MST_FK,  " + Convert.ToString(CurrencyPK) + ", " + INVSUP_DT + ") ROE,");
					strSql.Append("                        ''SEL,JOB_EXP.JOBCARD_REF_NO AS REF_NR,COST_ELE.Preference, ");
					if (Business_Type == 2) {
						strSql.Append("   CASE WHEN  JOB_EXP.CARGO_TYPE = 4 THEN  ");
						strSql.Append("    COMM.COMMODITY_NAME ");
						strSql.Append("  ELSE CTMT.CONTAINER_TYPE_MST_ID END CONTAINER_TYPE_MST_ID ");
					} else {
						strSql.Append("     ''CONTAINER_TYPE_MST_ID ");
					}
					strSql.Append("          FROM COST_ELEMENT_MST_TBL   COST_ELE,");
					strSql.Append("               CURRENCY_TYPE_MST_TBL  CURR,");
					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("               BOOKING_MST_TBL        BOOK,");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("               BOOKING_MST_TBL        BOOK,");
					}
					strSql.Append("               JOB_CARD_TRN   JOB_EXP,");
					strSql.Append("               JOB_TRN_COST   JOB_COST,");
					strSql.Append("               JOB_TRN_CONT   JOB_CONT");
					if (Business_Type == 2) {
						strSql.Append("                       ,CONTAINER_TYPE_MST_TBL CTMT ");
						strSql.Append("                       , COMMODITY_MST_TBL COMM ");
					}
					if (Process_Type == 1) {
						strSql.Append("              , HBL_EXP_TBL      HBL");
					}
					strSql.Append("         WHERE JOB_COST.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
					strSql.Append("           AND JOB_EXP.JOB_CARD_TRN_PK = JOB_CONT.JOB_CARD_TRN_FK");
					if (Process_Type == 1 & Business_Type == 2) {
						strSql.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					} else if (Process_Type == 1 & Business_Type == 1) {
						strSql.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					}
					if (Process_Type == 1) {
						strSql.Append("           AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
					}
					if (Business_Type == 2) {
						strSql.Append("                   AND JOB_COST.CONTAINER_TYPE_MST_FK =");
						strSql.Append("                       CTMT.CONTAINER_TYPE_MST_PK(+)");
						strSql.Append("           AND JOB_COST.CONTAINER_TYPE_MST_FK = COMM.COMMODITY_MST_PK(+) ");
					}
					//strSql.Append("           AND JOB_COST.JOB_TRN_" & BusinessProcess & "_COST_PK NOT IN")
					//strSql.Append("               (SELECT IST.JOB_TRN_EST_FK")
					//strSql.Append("                  FROM INV_SUPPLIER_TRN_TBL IST, INV_SUPPLIER_TBL IT")
					//strSql.Append("                 WHERE IST.INV_SUPPLIER_TBL_FK = IT.INV_SUPPLIER_PK")
					//strSql.Append("                   AND IT.BUSINESS_TYPE = " & Business_Type)
					//strSql.Append("                   AND IT.PROCESS_TYPE = " & Process_Type)
					//strSql.Append("                   AND IST.JOB_TRN_EST_FK IS NOT NULL)")
					//'Vasava
					strSql.Append("           AND ((JOB_COST.JOB_TRN_COST_PK NOT IN");
					strSql.Append("               (SELECT IST.JOB_TRN_EST_FK");
					strSql.Append("                  FROM INV_SUPPLIER_TRN_TBL IST, INV_SUPPLIER_TBL IT");
					strSql.Append("                 WHERE IST.INV_SUPPLIER_TBL_FK = IT.INV_SUPPLIER_PK");
					strSql.Append("                   AND IT.BUSINESS_TYPE = " + Business_Type);
					strSql.Append("                   AND IT.PROCESS_TYPE = " + Process_Type);
					strSql.Append("                   AND IST.JOB_TRN_EST_FK IS NOT NULL))");
					strSql.Append("           OR (JOB_COST.JOB_TRN_COST_PK IN");
					strSql.Append("               (SELECT IST.JOB_TRN_EST_FK");
					strSql.Append("                  FROM INV_SUPPLIER_TRN_TBL IST, INV_SUPPLIER_TBL IT");
					strSql.Append("                 WHERE IST.INV_SUPPLIER_TBL_FK = IT.INV_SUPPLIER_PK");
					strSql.Append("                   AND IT.BUSINESS_TYPE = " + Business_Type);
					strSql.Append("                   AND IT.PROCESS_TYPE = " + Process_Type);
					strSql.Append("                   AND IT.APPROVED = 2 ");
					strSql.Append("                   AND IST.JOB_TRN_EST_FK IS NOT NULL)))");

					//'End
					strSql.Append("           AND JOB_COST.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
					strSql.Append("           AND JOB_COST.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
					strSql.Append("           AND JOB_COST.INV_SUPPLIER_FK IS NULL");
					strSql.Append("           AND JOB_COST.VENDOR_MST_FK =" + VendorPK);
					strSql.Append(strCondition);
					strSql.Append("         ORDER BY COST_ELE.Preference ) Q");
				}
				//If Business_Type = 1 Then
				//    strSql.Replace("HBL", "HAWB")
				//    strSql.Replace("MBL", "MAWB")
				//End If
				DA = objWF.GetDataAdapter(strSql.ToString());
				DA.Fill(MainDS, "CHILD");
				DataRelation relataion_Details = new DataRelation("LOCGROUP", new DataColumn[] { MainDS.Tables[0].Columns["JOB_CARD_SEA_EXP_PK"] }, new DataColumn[] { MainDS.Tables[1].Columns["JOB_CARD_SEA_EXP_PK"] });
				relataion_Details.Nested = true;
				MainDS.Relations.Add(relataion_Details);
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

		#region "Fetch"
		public DataSet FetchCBJC_Inv(Int32 VendorPK, Int16 Business_Type, Int16 Process_Type, string FromDt = "", string ToDt = "", string RefNr = "", Int32 CostElementPk = 0, Int32 CurrencyPK = 0, string CurrencyID = "", Int32 TradePK = 0,
		string Vsl = "", string INVOICENO = "", Int64 lblInvSupplierPK = 0, string General = "", int MBLPK = 0, string MBLNR = "", int HBLPK = 0, string HBLNR = "", string INVSUP_DT = "", int ChkExRate = 0)
		{
			WorkFlow objWK = new WorkFlow();
			OracleCommand objCommand = new OracleCommand();
			DataSet dsData = new DataSet();

			try {
				if (string.IsNullOrEmpty(INVSUP_DT)) {
					INVSUP_DT = " TO_DATE(SYSDATE,DATEFORMAT) ";
				}
				objWK.OpenConnection();
				objWK.MyCommand.Connection = objWK.MyConnection;

				var _with1 = objWK.MyCommand;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".INV_SUPPLIER_TBL_PKG.CBJC_FETCH";

				objWK.MyCommand.Parameters.Clear();
				var _with2 = objWK.MyCommand.Parameters;
				_with2.Add("VENDOR_PK_IN", VendorPK).Direction = ParameterDirection.Input;
				_with2.Add("BIZ_TYPE_IN", Convert.ToInt32(Business_Type)).Direction = ParameterDirection.Input;
				_with2.Add("PROCESS_TYPE_IN", Convert.ToInt32(Process_Type)).Direction = ParameterDirection.Input;
				_with2.Add("FDATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
				_with2.Add("TDATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
				_with2.Add("REFNR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
				_with2.Add("COSTELEMENT_PK_IN", CostElementPk).Direction = ParameterDirection.Input;
				_with2.Add("CURRENCY_PK_IN", CurrencyPK).Direction = ParameterDirection.Input;
				_with2.Add("CURRENCYID_IN", (string.IsNullOrEmpty(CurrencyID) ? "" : CurrencyID)).Direction = ParameterDirection.Input;
				_with2.Add("TRADE_PK_IN", TradePK).Direction = ParameterDirection.Input;
				_with2.Add("VSL_IN", (string.IsNullOrEmpty(Vsl) ? "" : Vsl)).Direction = ParameterDirection.Input;
				_with2.Add("INVOICE_IN", (string.IsNullOrEmpty(INVOICENO) ? "" : INVOICENO)).Direction = ParameterDirection.Input;
				_with2.Add("SUPPLIER_PK_IN", lblInvSupplierPK).Direction = ParameterDirection.Input;
				_with2.Add("GENERAL_IN", (string.IsNullOrEmpty(General) ? "" : General)).Direction = ParameterDirection.Input;
				_with2.Add("MBLPK_IN", MBLPK).Direction = ParameterDirection.Input;
				_with2.Add("MBLNR_IN", (string.IsNullOrEmpty(MBLNR) ? "" : MBLNR)).Direction = ParameterDirection.Input;
				_with2.Add("HBLPK_IN", HBLPK).Direction = ParameterDirection.Input;
				_with2.Add("HBLNR_IN", (string.IsNullOrEmpty(HBLNR) ? "" : HBLNR)).Direction = ParameterDirection.Input;
				if (ChkExRate == 1) {
					_with2.Add("CHECK_EXRATE_IN", 0).Direction = ParameterDirection.Input;
				} else {
					_with2.Add("CHECK_EXRATE_IN", getDefault(HttpContext.Current.Session["CHECK_EXRATE"], 0)).Direction = ParameterDirection.Input;
				}

				_with2.Add("INVSUP_DT_IN", (string.IsNullOrEmpty(INVSUP_DT) ? "" : INVSUP_DT)).Direction = ParameterDirection.Input;
				_with2.Add("DETAIL0_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with2.Add("DETAIL1_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
				objWK.MyDataAdapter.Fill(dsData);

				DataRelation CONTRel = null;
				CONTRel = new DataRelation("CONTRelation", dsData.Tables[0].Columns["JOB_CARD_SEA_EXP_PK"], dsData.Tables[1].Columns["JOB_CARD_SEA_EXP_PK"], true);

				CONTRel.Nested = true;
				dsData.Relations.Add(CONTRel);
				return dsData;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchTPT_Inv(Int32 VendorPK, Int16 Business_Type, Int16 Process_Type, string FromDt = "", string ToDt = "", string RefNr = "", Int32 CostElementPk = 0, Int32 CurrencyPK = 0, string CurrencyID = "", Int32 TradePK = 0,
		string Vsl = "", string INVOICENO = "", Int64 lblInvSupplierPK = 0, string General = "", int MBLPK = 0, string MBLNR = "", int HBLPK = 0, string HBLNR = "", string INVSUP_DT = "", int ChkExRate = 0)
		{
			WorkFlow objWK = new WorkFlow();
			OracleCommand objCommand = new OracleCommand();
			DataSet dsData = new DataSet();

			try {
				if (string.IsNullOrEmpty(INVSUP_DT)) {
					INVSUP_DT = " TO_DATE(SYSDATE,DATEFORMAT) ";
				}
				objWK.OpenConnection();
				objWK.MyCommand.Connection = objWK.MyConnection;

				var _with3 = objWK.MyCommand;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWK.MyUserName + ".INV_SUPPLIER_TBL_PKG.TPT_FETCH";

				objWK.MyCommand.Parameters.Clear();
				var _with4 = objWK.MyCommand.Parameters;
				_with4.Add("VENDOR_PK_IN", VendorPK).Direction = ParameterDirection.Input;
				_with4.Add("BIZ_TYPE_IN", Convert.ToInt32(Business_Type)).Direction = ParameterDirection.Input;
				_with4.Add("PROCESS_TYPE_IN", Convert.ToInt32(Process_Type)).Direction = ParameterDirection.Input;
				_with4.Add("FDATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
				_with4.Add("TDATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
				_with4.Add("REFNR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
				_with4.Add("COSTELEMENT_PK_IN", CostElementPk).Direction = ParameterDirection.Input;
				_with4.Add("CURRENCY_PK_IN", CurrencyPK).Direction = ParameterDirection.Input;
				_with4.Add("CURRENCYID_IN", (string.IsNullOrEmpty(CurrencyID) ? "" : CurrencyID)).Direction = ParameterDirection.Input;
				_with4.Add("TRADE_PK_IN", TradePK).Direction = ParameterDirection.Input;
				_with4.Add("VSL_IN", (string.IsNullOrEmpty(Vsl) ? "" : Vsl)).Direction = ParameterDirection.Input;
				_with4.Add("INVOICE_IN", (string.IsNullOrEmpty(INVOICENO) ? "" : INVOICENO)).Direction = ParameterDirection.Input;
				_with4.Add("SUPPLIER_PK_IN", lblInvSupplierPK).Direction = ParameterDirection.Input;
				_with4.Add("GENERAL_IN", (string.IsNullOrEmpty(General) ? "" : General)).Direction = ParameterDirection.Input;
				_with4.Add("MBLPK_IN", MBLPK).Direction = ParameterDirection.Input;
				_with4.Add("MBLNR_IN", (string.IsNullOrEmpty(MBLNR) ? "" : MBLNR)).Direction = ParameterDirection.Input;
				_with4.Add("HBLPK_IN", HBLPK).Direction = ParameterDirection.Input;
				_with4.Add("HBLNR_IN", (string.IsNullOrEmpty(HBLNR) ? "" : HBLNR)).Direction = ParameterDirection.Input;
				if (ChkExRate == 1) {
					_with4.Add("CHECK_EXRATE_IN", 0).Direction = ParameterDirection.Input;
				} else {
					_with4.Add("CHECK_EXRATE_IN", getDefault(HttpContext.Current.Session["CHECK_EXRATE"], 0)).Direction = ParameterDirection.Input;
				}
				_with4.Add("INVSUP_DT_IN", (string.IsNullOrEmpty(INVSUP_DT) ? "" : INVSUP_DT)).Direction = ParameterDirection.Input;
				_with4.Add("DETAIL0_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with4.Add("DETAIL1_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
				objWK.MyDataAdapter.Fill(dsData);

				DataRelation CONTRel = null;
				CONTRel = new DataRelation("CONTRelation", dsData.Tables[0].Columns["JOB_CARD_SEA_EXP_PK"], dsData.Tables[1].Columns["JOB_CARD_SEA_EXP_PK"], true);

				CONTRel.Nested = true;
				dsData.Relations.Add(CONTRel);
				return dsData;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Master Jobcard Invoice"
		public DataSet FetchMasterJobCardInv(Int32 VendorPK, Int16 Business_Type, Int16 Process_Type, string FromDt = "", string ToDt = "", Int32 CostElementPk = 0, Int32 CurrencyPK = 0, Int32 TradePK = 0, string Vsl = "", string INVOICENO = "",
		Int64 lblInvSupplierPK = 0, string General = "")
		{

			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			string strCondition = null;

			string BusinessProcess = null;
			string VslFlight = null;
			if (Business_Type == 2) {
				BusinessProcess = "SEA";
				VslFlight = "VESSEL_NAME";
			} else {
				BusinessProcess = "AIR";
				VslFlight = "FLIGHT_NO";
			}
			if (Process_Type == 1) {
				BusinessProcess += "_EXP";
			} else {
				BusinessProcess += "_IMP";
			}
			if (((FromDt != null) & (ToDt != null)) & FromDt != " " & ToDt != " ") {
				strCondition += " AND MJOB_EXP.MASTER_JC_DATE BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
			} else if (((FromDt != null)) & FromDt != " ") {
				strCondition += " AND MJOB_EXP.MASTER_JC_DATE >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
			} else if (((ToDt != null)) & ToDt != " ") {
				strCondition += " AND MJOB_EXP.MASTER_JC_DATE <= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
			}
			if (!string.IsNullOrEmpty(INVOICENO)) {
				strCondition += " AND INV_SUP.INVOICE_REF_NO ='" + INVOICENO + "'";
			}

			if (CostElementPk != 0) {
				strCondition += " AND COST_ELE.COST_ELEMENT_MST_PK =" + CostElementPk;
			}

			//If Vsl <> "" Then
			//    If Business_Type = 2 Then
			//        strCondition &= vbCrLf & " AND JOB_EXP.VESSEL_NAME ||" & "'-'" & " || JOB_EXP.VOYAGE like '" & Vsl.Trim & "'"
			//    Else
			//        strCondition &= vbCrLf & " AND JOB_EXP.FLIGHT_NO like '" & Vsl.Trim & "'"
			//    End If
			//End If
			if (lblInvSupplierPK != 0) {
				sb.Append("SELECT ROWNUM SrNO,");
				sb.Append("       MJOB_EXP.MASTER_JC_REF_NO AS REF_NR,");
				sb.Append("       MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK AS MASTER_JC_SEA_EXP_PK,");
				sb.Append("       MJOB_COST.MJC_TRN_" + BusinessProcess + "_COST_PK AS JOB_TRN_EST_PK,");
				sb.Append("       INV_SUP.INV_SUPPLIER_PK INV_SUPPLIER_FK,");
				sb.Append("       INV_TRN.INV_SUPPLIER_TRN_PK INV_SUPPLIER_TRN_FK,");
				sb.Append("       COST_ELE.COST_ELEMENT_ID AS COST_ELEMENT_ID,");
				sb.Append("       COST_ELE.COST_ELEMENT_MST_PK AS COST_ELEMENT_MST_PK,");
				sb.Append("       MJOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
				sb.Append("       ROUND(INV_TRN.ESTIMATED_AMT *");
				sb.Append("             GET_EX_RATE(INV_SUP.Currency_Mst_Fk, " + Convert.ToString(CurrencyPK) + ",INV_SUP.SUPPLIER_INV_DT),");
				sb.Append("             2) AS ESTIMATED_AMT,");
				sb.Append("       ROUND(INV_TRN.ACTUAL_AMT *");
				sb.Append("             GET_EX_RATE(INV_SUP.Currency_Mst_Fk, " + Convert.ToString(CurrencyPK) + ", INV_SUP.SUPPLIER_INV_DT),");
				sb.Append("             2) AS ACTUAL_AMT,");
				sb.Append("       INV_TRN.TAX_PERCENT AS TAX_PERCENTAGE,");
				sb.Append("       INV_TRN.TAX_AMOUNT AS TAX_AMT,");
				sb.Append("       INV_TRN.TOTAL_COST,");
				sb.Append("       (CASE");
				sb.Append("         WHEN MJOB_COST.ESTIMATED_COST IS NOT NULL AND");
				sb.Append("              INV_TRN.TOTAL_COST IS NOT NULL THEN");
				sb.Append("          NVL(ROUND(INV_TRN.TOTAL_COST *");
				sb.Append("                    GET_EX_RATE(INV_SUP.Currency_Mst_Fk, " + Convert.ToString(CurrencyPK) + ", INV_SUP.SUPPLIER_INV_DT),");
				sb.Append("                    2),");
				sb.Append("              0) -");
				sb.Append("          NVL(ROUND(MJOB_COST.ESTIMATED_COST *");
				sb.Append("                    GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ", INV_SUP.SUPPLIER_INV_DT),");
				sb.Append("                    2),");
				sb.Append("              0)");
				sb.Append("         ELSE");
				sb.Append("          NULL");
				sb.Append("       END) DIFFERENCE_AMT,");
				sb.Append("       'true' Sel");
				sb.Append("  FROM INV_SUPPLIER_TBL      INV_SUP,");
				sb.Append("       INV_SUPPLIER_TRN_TBL  INV_TRN,");
				sb.Append("       MJC_TRN_" + BusinessProcess + "_COST  MJOB_COST,");
				sb.Append("       MASTER_JC_" + BusinessProcess + "_TBL MJOB_EXP,");
				sb.Append("       COST_ELEMENT_MST_TBL  COST_ELE");
				sb.Append(" WHERE INV_SUP.INV_SUPPLIER_PK = INV_TRN.INV_SUPPLIER_TBL_FK");
				sb.Append("   AND INV_SUP.VENDOR_MST_FK = " + VendorPK);
				sb.Append("   AND INV_TRN.MJC_TRN_COST_FK = MJOB_COST.MJC_TRN_" + BusinessProcess + "_COST_PK(+)");
				sb.Append("   AND MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK = MJOB_COST.MASTER_JC_" + BusinessProcess + "_FK");
				sb.Append("   AND COST_ELE.COST_ELEMENT_MST_PK = INV_TRN.COST_ELEMENT_MST_FK");
			} else {
				sb.Append("SELECT ROWNUM SrNO,");
				sb.Append("       MJOB_EXP.MASTER_JC_REF_NO AS REF_NR,");
				sb.Append("       MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK AS MASTER_JC_SEA_EXP_PK,");
				sb.Append("       MJOB_COST.MJC_TRN_" + BusinessProcess + "_COST_PK AS JOB_TRN_EST_PK,");
				sb.Append("       MJOB_COST.INV_SUPPLIER_FK,");
				sb.Append("       '' INV_SUPPLIER_TRN_FK,");
				sb.Append("       COST_ELE.COST_ELEMENT_ID AS COST_ELEMENT_ID,");
				sb.Append("       COST_ELE.COST_ELEMENT_MST_PK AS COST_ELEMENT_MST_PK,");
				sb.Append("       MJOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
				sb.Append("       ROUND(MJOB_COST.ESTIMATED_COST *");
				sb.Append("             GET_EX_RATE(MJOB_COST.CURRENCY_MST_FK, " + Convert.ToString(CurrencyPK) + ", INV_SUP.SUPPLIER_INV_DT),");
				sb.Append("             2) AS ESTIMATED_AMT,");
				sb.Append("       0 ACTUAL_AMT,");
				sb.Append("       0 TAX_PERCENTAGE,");
				sb.Append("       0 TAX_AMT,");
				sb.Append("       MJOB_COST.TOTAL_COST,");
				sb.Append("       (CASE");
				sb.Append("         WHEN MJOB_COST.ESTIMATED_COST IS NOT NULL AND");
				sb.Append("              MJOB_COST.TOTAL_COST IS NOT NULL THEN");
				sb.Append("          NVL(ROUND(MJOB_COST.TOTAL_COST *");
				sb.Append("                    GET_EX_RATE(MJOB_COST.Currency_Mst_Fk, " + Convert.ToString(CurrencyPK) + ", INV_SUP.SUPPLIER_INV_DT),");
				sb.Append("                    2),");
				sb.Append("              0) -");
				sb.Append("          NVL(ROUND(MJOB_COST.ESTIMATED_COST *");
				sb.Append("                    GET_EX_RATE(MJOB_COST.Currency_Mst_Fk, " + Convert.ToString(CurrencyPK) + ", INV_SUP.SUPPLIER_INV_DT),");
				sb.Append("                    2),");
				sb.Append("              0)");
				sb.Append("         ELSE");
				sb.Append("          NULL");
				sb.Append("       END) DIFFERENCE_AMT,");
				sb.Append("       '' Sel");
				sb.Append("  FROM MASTER_JC_" + BusinessProcess + "_TBL MJOB_EXP,");
				sb.Append("       MJC_TRN_" + BusinessProcess + "_COST  MJOB_COST,");
				sb.Append("       COST_ELEMENT_MST_TBL  COST_ELE,");
				sb.Append("       CURRENCY_TYPE_MST_TBL CURR");
				sb.Append("  WHERE MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK = MJOB_COST.MASTER_JC_" + BusinessProcess + "_FK");
				sb.Append("   AND MJOB_COST.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
				sb.Append("   AND MJOB_COST.VENDOR_MST_FK = " + VendorPK);
				sb.Append("   AND MJOB_COST.INV_SUPPLIER_FK IS NULL");
				sb.Append("   AND MJOB_COST.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
			}
			sb.Append(strCondition);
			try {
				WorkFlow objWF = new WorkFlow();
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Master_Jobcard Invoice"
		public DataSet FetchMasterJobCard_Inv(Int32 VendorPK, Int16 Business_Type, Int16 Process_Type, string FromDt = "", string ToDt = "", string RefNr = "", Int32 CostElementPk = 0, Int32 CurrencyPK = 0, string CurrencyID = "", Int32 TradePK = 0,
		string Vsl = "", string INVOICENO = "", Int64 lblInvSupplierPK = 0, string General = "", int MBLPK = 0, string MBLNR = "", int HBLPK = 0, string HBLNR = "", string INVSUP_DT = "", int ChkExRate = 0)
		{

			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			string strCondition = null;
			DataSet MainDS = new DataSet();
			OracleDataAdapter DA = new OracleDataAdapter();
			WorkFlow objWF = new WorkFlow();

			string BusinessProcess = null;
			string VslFlight = null;
			int chk_ExRATE = 0;
			int ROE = 0;
			try {
				if (ChkExRate == 1) {
					chk_ExRATE = 0;
				} else {
					chk_ExRATE = Convert.ToInt32(HttpContext.Current.Session["CHECK_EXRATE"]);
				}
				INVSUP_DT = INVSUP_DT.Trim();
				if (string.IsNullOrEmpty(INVSUP_DT)) {
					INVSUP_DT = " TO_DATE(SYSDATE,DATEFORMAT) ";
				} else {
					INVSUP_DT = " TO_DATE('" + INVSUP_DT + "',DATEFORMAT) ";
				}
				ROE = FETCH_ROE(CurrencyPK, INVSUP_DT);
				if (Business_Type == 2) {
					BusinessProcess = "SEA";
					VslFlight = "VESSEL_NAME";
				} else {
					BusinessProcess = "AIR";
					VslFlight = "FLIGHT_NO";
				}
				if (Process_Type == 1) {
					BusinessProcess += "_EXP";
				} else {
					BusinessProcess += "_IMP";
				}
				if (((FromDt != null) & (ToDt != null)) & FromDt != " " & ToDt != " ") {
					strCondition += " AND MJOB_EXP.MASTER_JC_DATE BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
				} else if (((FromDt != null)) & FromDt != " ") {
					strCondition += " AND MJOB_EXP.MASTER_JC_DATE >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
				} else if (((ToDt != null)) & ToDt != " ") {
					strCondition += " AND MJOB_EXP.MASTER_JC_DATE <= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
				}
				if (!string.IsNullOrEmpty(INVOICENO)) {
					strCondition += " AND INV_SUP.INVOICE_REF_NO ='" + INVOICENO + "'";
				}

				if (CostElementPk != 0) {
					strCondition += " AND COST_ELE.COST_ELEMENT_MST_PK =" + CostElementPk;
				}
				if (!string.IsNullOrEmpty(RefNr)) {
					strCondition += " AND JOB_EXP.JOBCARD_REF_NO ='" + RefNr + "'";
				}
				if (!string.IsNullOrEmpty(Vsl)) {
					if (Business_Type == 2) {
						//strCondition &= vbCrLf & " AND JOB_EXP.VESSEL_NAME ||" & "'-'" & " || JOB_EXP.VOYAGE like '" & Vsl.Trim & "'"
						strCondition += " AND JOB_EXP.VESSEL_NAME  like '" + Vsl.Trim() + "'";
					} else {
						strCondition += " AND JOB_EXP.VOYAGE_FLIGHT_NO like '" + Vsl.Trim() + "'";
					}
				}
				if (MBLPK != 0) {
					if (Process_Type == 1) {
						strCondition += "  AND JOB_EXP.MBL_MAWB_FK=" + MBLPK;
					}
				}
				if (!string.IsNullOrEmpty(MBLNR)) {
					if (Process_Type == 2) {
						strCondition += " AND JOB_EXP.MBL_MAWB_REF_NO like '" + MBLNR.Trim() + "'";
					}
				}
				if (HBLPK != 0) {
					if (Process_Type == 1) {
						strCondition += "  AND JOB_EXP.HBL_HAWB_FK=" + HBLPK;
					}
				}
				if (!string.IsNullOrEmpty(HBLNR)) {
					if (Process_Type == 2) {
						strCondition += " AND JOB_EXP.HBL_HAWB_REF_NO like '" + HBLNR.Trim() + "'";
					}
				}

				strCondition += " AND JOB_EXP.BUSINESS_TYPE = " + Business_Type;
				strCondition += " AND JOB_EXP.PROCESS_TYPE = " + Process_Type;

				if (lblInvSupplierPK != 0) {
					sb.Append("SELECT '' SRNO,");
					sb.Append("       Q.REF_NR AS REF_NR,");
					sb.Append("       Q.MASTER_JC_SEA_EXP_PK,");
					sb.Append("       Q.INV_SUPPLIER_FK AS INV_SUPPLIER_FK,");
					sb.Append("       Q.INV_SUPPLIER_TRN_FK INV_SUPPLIER_TRN_FK,");
					sb.Append("       Q.VENDOR_MST_FK AS VENDOR_MST_FK,");
					sb.Append("       Q.VESVOYAGE,");
					sb.Append("       NVL(Q.TEU_FACTOR, 0) TEUS,");
					sb.Append("       NVL(Q.PACK_COUNT, 0) QUANTITY,");
					sb.Append("       NVL(Q.VOLUME_IN_CBM, 0) VOLUME,");
					sb.Append("       NVL(Q.GROSS_WEIGHT, 0) GROSS_WEIGHT,");
					sb.Append("       NVL(Q.NET_WEIGHT, 0) NET_WEIGHT,");
					sb.Append("       NVL(SUM(Q.ESTIMATED_AMT), 0) ESTIMATED_AMT,");
					sb.Append("       NVL(SUM(Q.ACTUAL_AMT), 0) ACTUAL_AMT,");
					sb.Append("       NVL(SUM(Q.TAX_PERCENTAGE), 0) TAX_PERCENTAGE,");
					sb.Append("       NVL(SUM(Q.TAX_AMT), 0) TAX_AMT,");
					sb.Append("       NVL(SUM(Q.TOTAL_COST), 0) TOTAL_COST,");
					sb.Append("       Q.FACCURR,");
					sb.Append("       NVL(SUM(Q.FAC_AMT), 0) FAC_AMT,");
					sb.Append("       NVL(SUM(Q.ROE), 0) ROE,");
					sb.Append("       NVL(SUM(Q.FAC_AMT * Q.ROE), 0) FAC_AMT_VOU,");
					sb.Append("       NVL(SUM(Q.PAYABLE_AMT),0)PAYABLE_AMT,");
					sb.Append("       NVL(SUM(Q.PAYABLE_AMT)- SUM(Q.ESTIMATED_AMT), 0) DIFF,");
					sb.Append("       Q.SEL,");
					sb.Append("       Q.CHK");
					sb.Append("  FROM (SELECT DISTINCT '' SRNO,");
					sb.Append("                        MJOB_EXP.MASTER_JC_REF_NO AS REF_NR,");
					sb.Append("                        MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK AS MASTER_JC_SEA_EXP_PK,");
					sb.Append("                        '' JOB_TRN_EST_PK,");
					sb.Append("                        INV_SUP.INV_SUPPLIER_PK INV_SUPPLIER_FK,");
					sb.Append("                        '' INV_SUPPLIER_TRN_FK,");
					if (Business_Type == 2) {
						sb.Append("                        (CASE");
						sb.Append("                          WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						sb.Append("                               NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
						sb.Append("                           ''");
						sb.Append("                          ELSE");
						sb.Append("                           NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						sb.Append("                           NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
						sb.Append("                        END) AS VESVOYAGE,");
						sb.Append("                        JOBCONT.TEU_FACTOR,");
					} else {
						sb.Append("       JOB_EXP.VOYAGE_FLIGHT_NO AS VESVOYAGE,");
						sb.Append("                        0 TEU_FACTOR,");
					}
					sb.Append("                        JOBCONT.PACK_COUNT,");
					sb.Append("                        JOBCONT.VOLUME_IN_CBM,");
					sb.Append("                        JOBCONT.GROSS_WEIGHT,");
					sb.Append("                        JOBCONT.NET_WEIGHT,");
					sb.Append("                        MJOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
					sb.Append("                        ROUND(INV_TRN.ESTIMATED_AMT *");
					sb.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					sb.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					sb.Append("                                          " + INVSUP_DT + "),");
					sb.Append("                              2) AS ESTIMATED_AMT,");
					sb.Append("                        ROUND(INV_TRN.ACTUAL_AMT *");
					sb.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					sb.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					sb.Append("                                          " + INVSUP_DT + "),");
					sb.Append("                              2) AS ACTUAL_AMT,");
					sb.Append("                        INV_TRN.TAX_PERCENT AS TAX_PERCENTAGE,");
					sb.Append("                        INV_TRN.TAX_AMOUNT AS TAX_AMT,");
					sb.Append("                        INV_TRN.TOTAL_COST,");
					//sb.Append("                        CURR.CURRENCY_ID FACCURR,")
					sb.Append("                        '" + CurrencyID + "' FACCURR,");
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
						sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                       2,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
						sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                       2,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					//AIR
					} else if (Business_Type == 1 & Process_Type == 1) {
						sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
						sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					} else if (Business_Type == 1 & Process_Type == 2) {
						sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
						sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					}
					sb.Append("                        GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK, " + Convert.ToString(CurrencyPK) + ", " + INVSUP_DT + ") ROE,");
					sb.Append("                        INV_TRN.PAYABLE_AMT,");
					sb.Append("                        'true' SEL,");
					sb.Append("                        '' CHK");
					sb.Append("          FROM CURRENCY_TYPE_MST_TBL  CURR,");
					if (Process_Type == 1 & Business_Type == 2) {
						sb.Append("               BOOKING_MST_TBL        BOOK,");
					} else if (Process_Type == 1 & Business_Type == 1) {
						sb.Append("               BOOKING_MST_TBL        BOOK,");
					}
					sb.Append("               JOB_CARD_TRN   JOB_EXP,");
					sb.Append("               MASTER_JC_" + BusinessProcess + "_TBL  MJOB_EXP,");
					sb.Append("               MJC_TRN_" + BusinessProcess + "_COST   MJOB_COST,");
					if (Process_Type == 1) {
						sb.Append("           HBL_EXP_TBL           HBL,");
					}
					///'
					sb.Append("(SELECT DISTINCT JOB.MASTER_JC_FK,");
					if (Business_Type == 2) {
						sb.Append("                                SUM(CTMT.TEU_FACTOR) TEU_FACTOR,");
					} else {
						sb.Append("                                0 TEU_FACTOR,");
					}
					sb.Append("                                SUM(JOB_CONT.PACK_COUNT) PACK_COUNT,");
					sb.Append("                                SUM(JOB_CONT.VOLUME_IN_CBM) VOLUME_IN_CBM,");
					sb.Append("                                SUM(JOB_CONT.GROSS_WEIGHT) GROSS_WEIGHT,");
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						sb.Append("                        (CASE  WHEN BKG.CARGO_TYPE = 1 THEN");
						sb.Append("                          SUM(JOB_CONT.NET_WEIGHT)");
						sb.Append("                         ELSE SUM(JOB_CONT.CHARGEABLE_WEIGHT)");
						sb.Append("                         END) NET_WEIGHT");
					} else if (Business_Type == 2 & Process_Type == 2) {
						sb.Append("                        (CASE  WHEN JOB.CARGO_TYPE = 1 THEN");
						sb.Append("                          SUM(JOB_CONT.NET_WEIGHT)");
						sb.Append("                         ELSE SUM(JOB_CONT.CHARGEABLE_WEIGHT)");
						sb.Append("                         END) NET_WEIGHT");
					//'AIR
					} else if (Business_Type == 1) {
						sb.Append("     SUM(JOB_CONT.CHARGEABLE_WEIGHT) NET_WEIGHT");
					}
					sb.Append("                  FROM JOB_CARD_TRN   JOB,");
					sb.Append("                       JOB_TRN_CONT   JOB_CONT");
					if (Business_Type == 2) {
						sb.Append("                       ,CONTAINER_TYPE_MST_TBL CTMT");
					}
					if (Process_Type == 1 & Business_Type == 2) {
						sb.Append("                       ,BOOKING_MST_TBL        BKG");
					} else if (Process_Type == 1 & Business_Type == 1) {
						sb.Append("                       ,BOOKING_MST_TBL        BKG");
					}
					sb.Append("                 WHERE JOB.JOB_CARD_TRN_PK = JOB_CONT.JOB_CARD_TRN_FK");
					if (Business_Type == 2) {
						sb.Append("                   AND JOB_CONT.CONTAINER_TYPE_MST_FK =");
						sb.Append("                       CTMT.CONTAINER_TYPE_MST_PK(+)");
					}
					if (Process_Type == 1 & Business_Type == 2) {
						sb.Append("           AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
					} else if (Process_Type == 1 & Business_Type == 1) {
						sb.Append("           AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
					}
					sb.Append("                 GROUP BY JOB.MASTER_JC_FK");
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						sb.Append("   , BKG.CARGO_TYPE) JOBCONT,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						sb.Append("   , JOB.CARGO_TYPE ) JOBCONT,");
					//'AIR
					} else if (Business_Type == 1) {
						sb.Append("  ) JOBCONT,");
					}
					///''
					sb.Append("               COST_ELEMENT_MST_TBL   COST_ELE,");
					sb.Append("               INV_SUPPLIER_TBL       INV_SUP,");
					sb.Append("               INV_SUPPLIER_TRN_TBL   INV_TRN");
					sb.Append("         WHERE MJOB_COST.MASTER_JC_" + BusinessProcess + "_FK = MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK(+)");
					sb.Append("           AND JOB_EXP.MASTER_JC_FK = JOBCONT.MASTER_JC_FK ");
					sb.Append("           AND JOB_EXP.MASTER_JC_FK= MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK");
					if (Process_Type == 1) {
						sb.Append("           AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
					}
					if (Process_Type == 1 & Business_Type == 2) {
						sb.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					} else if (Process_Type == 1 & Business_Type == 1) {
						sb.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					}
					sb.Append("           AND INV_TRN.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
					sb.Append("           AND MJOB_COST.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
					sb.Append("           AND MJOB_COST.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
					sb.Append("           AND INV_SUP.INV_SUPPLIER_PK = INV_TRN.INV_SUPPLIER_TBL_FK");
					sb.Append("           AND INV_SUP.VENDOR_MST_FK = " + VendorPK);
					sb.Append("           AND INV_TRN.MJC_TRN_COST_FK = MJOB_COST.MJC_TRN_" + BusinessProcess + "_COST_PK(+)");
					sb.Append(strCondition);
					sb.Append("           ) Q");
					sb.Append(" GROUP BY Q.VENDOR_MST_FK,");
					sb.Append("          Q.REF_NR,");
					sb.Append("          Q.MASTER_JC_SEA_EXP_PK,");
					sb.Append("          Q.INV_SUPPLIER_FK,");
					sb.Append("          Q.VESVOYAGE,");
					sb.Append("          Q.VENDOR_MST_FK,");
					sb.Append("          Q.FACCURR,");
					sb.Append("          Q.TEU_FACTOR,");
					sb.Append("          Q.PACK_COUNT,");
					sb.Append("          Q.VOLUME_IN_CBM,");
					sb.Append("          Q.GROSS_WEIGHT,");
					sb.Append("          Q.NET_WEIGHT,");
					sb.Append("          Q.SEL,");
					sb.Append("          Q.CHK");

					//If Business_Type = 1 Then
					//    sb.Replace("HBL", "HAWB")
					//    sb.Replace("MBL", "MAWB")
					//End If
					DA = objWF.GetDataAdapter(sb.ToString());
					DA.Fill(MainDS, "PARENT");
					///
					sb.Remove(0, sb.Length);

					sb.Append("SELECT Q.SHIP_REF_NR,");
					sb.Append("       Q.REF_DATE,");
					sb.Append("       Q.CARGOTYPE,");
					sb.Append("       Q.MASTER_JC_SEA_EXP_PK,");
					sb.Append("       Q.JOB_TRN_EST_PK,");
					sb.Append("       Q.INV_SUPPLIER_FK,");
					sb.Append("       Q.INV_SUPPLIER_TRN_FK,");
					sb.Append("       Q.VENDOR_MST_FK,");
					sb.Append("       Q.COST_ELEMENT_ID,");
					sb.Append("       Q.COST_ELEMENT_MST_PK,");
					sb.Append("       Q.VESVOYAGE,");
					sb.Append("      '' CONTAINER_TYPE_MST_ID, ");
					sb.Append("       NVL(Q.ESTIMATED_AMT, 0) ESTIMATED_AMT,");
					sb.Append("       NVL(Q.ACTUAL_AMT, 0) ACTUAL_AMT,");
					sb.Append("       NVL(Q.TAX_PERCENTAGE, 0) TAX_PERCENTAGE,");
					sb.Append("       NVL(Q.TAX_AMT, 0) TAX_AMT,");
					sb.Append("       NVL(Q.TOTAL_COST, 0) TOTAL_COST,");
					sb.Append("       Q.FACCURR,");
					sb.Append("       NVL(Q.FAC_AMT, 0) FAC_AMT,");
					sb.Append("       NVL(Q.ROE, 0) ROE,");
					sb.Append("       NVL(Q.FAC_AMT * Q.ROE, 0) FAC_AMT_VOU,");
					sb.Append("       NVL(Q.PAYABLE_AMT,0)PAYABLE_AMT,");
					sb.Append("       NVL((Q.PAYABLE_AMT - Q.ESTIMATED_AMT), 0) DIFF,");
					sb.Append("       Q.SEL,Q.REF_NR");
					sb.Append("  FROM (SELECT DISTINCT");
					if (Process_Type == 1) {
						sb.Append("           HBL.HBL_REF_NO SHIP_REF_NR,");
						sb.Append("           HBL.HBL_DATE AS REF_DATE,");
					} else {
						sb.Append("            JOB_EXP.HBL_HAWB_REF_NO SHIP_REF_NR,");
						sb.Append("            JOB_EXP.HBL_HAWB_DATE AS REF_DATE, ");
					}
					if (Business_Type == 2 & Process_Type == 1) {
						sb.Append("                        DECODE(BOOK.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGOTYPE,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						sb.Append("                        DECODE(JOB_EXP.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGOTYPE,");
					} else if (Business_Type == 1) {
						sb.Append(" '' CARGOTYPE,");
					}
					sb.Append("                        MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK AS MASTER_JC_" + BusinessProcess + "_PK,");
					sb.Append("                        MJOB_COST.MJC_TRN_" + BusinessProcess + "_COST_PK AS JOB_TRN_EST_PK,");
					sb.Append("                        MJOB_COST.INV_SUPPLIER_FK,");
					sb.Append("                        INV_TRN.INV_SUPPLIER_TRN_PK INV_SUPPLIER_TRN_FK,");
					sb.Append("                        COST_ELE.COST_ELEMENT_ID,");
					sb.Append("                        COST_ELE.COST_ELEMENT_MST_PK,");
					if (Business_Type == 2) {
						sb.Append("                        (CASE");
						sb.Append("                          WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						sb.Append("                               NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
						sb.Append("                           ''");
						sb.Append("                          ELSE");
						sb.Append("                           NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						sb.Append("                           NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
						sb.Append("                        END) AS VESVOYAGE,");
					} else {
						sb.Append("                    JOB_EXP.VOYAGE_FLIGHT_NO AS VESVOYAGE,");
					}
					sb.Append("                        MJOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
					sb.Append("                        ROUND(INV_TRN.ESTIMATED_AMT *");
					sb.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					sb.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					sb.Append("                                          " + INVSUP_DT + "),");
					sb.Append("                              2) AS ESTIMATED_AMT,");
					sb.Append("                        ROUND(INV_TRN.ACTUAL_AMT *");
					sb.Append("                              GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK,");
					sb.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					sb.Append("                                          " + INVSUP_DT + "),");
					sb.Append("                              2) AS ACTUAL_AMT,");
					sb.Append("                        INV_TRN.TAX_PERCENT AS TAX_PERCENTAGE,");
					sb.Append("                        INV_TRN.TAX_AMOUNT AS TAX_AMT,");
					sb.Append("                        INV_TRN.TOTAL_COST,");
					//sb.Append("                        CURR.CURRENCY_ID FACCURR,")
					sb.Append("                        '" + CurrencyID + "' FACCURR,");
					//sb.Append("                        GET_FAC_AMOUNT(BOOK.OPERATOR_MST_FK,")
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
						sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
						sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                       2,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					//AIR
					} else if (Business_Type == 1 & Process_Type == 1) {
						sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
						sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					} else if (Business_Type == 1 & Process_Type == 2) {
						sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
						sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
						sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
					}
					sb.Append("                        GET_EX_RATE_BUY(INV_SUP.CURRENCY_MST_FK, " + Convert.ToString(CurrencyPK) + ", " + INVSUP_DT + ") ROE,");
					sb.Append("                 INV_TRN.PAYABLE_AMT,");
					sb.Append("                        'true' SEL,JOB_EXP.JOBCARD_REF_NO AS REF_NR");
					sb.Append("          FROM COST_ELEMENT_MST_TBL  COST_ELE,");
					sb.Append("               CURRENCY_TYPE_MST_TBL CURR,");
					if (Process_Type == 1 & Business_Type == 2) {
						sb.Append("               BOOKING_MST_TBL        BOOK,");
					} else if (Process_Type == 1 & Business_Type == 1) {
						sb.Append("               BOOKING_MST_TBL        BOOK,");
					}
					sb.Append("               JOB_CARD_TRN  JOB_EXP,");
					sb.Append("               MASTER_JC_" + BusinessProcess + "_TBL MJOB_EXP,");
					sb.Append("               MJC_TRN_" + BusinessProcess + "_COST  MJOB_COST,");
					sb.Append("               JOB_TRN_CONT  JOB_CONT,");
					if (Process_Type == 1) {
						sb.Append("           HBL_EXP_TBL           HBL,");
					}
					sb.Append("               INV_SUPPLIER_TBL      INV_SUP,");
					sb.Append("               INV_SUPPLIER_TRN_TBL  INV_TRN");
					sb.Append("         WHERE MJOB_COST.MASTER_JC_" + BusinessProcess + "_FK = MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK(+)");
					sb.Append("           AND JOB_EXP.JOB_CARD_TRN_PK = JOB_CONT.JOB_CARD_TRN_FK");
					sb.Append("           AND MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK=MJOB_COST.MASTER_JC_" + BusinessProcess + "_FK");
					sb.Append("           AND JOB_EXP.MASTER_JC_FK = MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK");
					sb.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					if (Process_Type == 1) {
						sb.Append("           AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
					}
					sb.Append("           AND INV_SUP.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
					sb.Append("           AND INV_TRN.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
					sb.Append("           AND INV_SUP.INV_SUPPLIER_PK = INV_TRN.INV_SUPPLIER_TBL_FK");
					sb.Append("           AND INV_SUP.VENDOR_MST_FK = " + VendorPK);
					sb.Append("           AND INV_TRN.MJC_TRN_COST_FK = MJOB_COST.MJC_TRN_" + BusinessProcess + "_COST_PK(+)");
					sb.Append(strCondition);
					sb.Append("           ) Q ");
				} else {
					sb.Append("SELECT '' SRNO,");
					sb.Append("       Q.REF_NR AS REF_NR,");
					sb.Append("       Q.MASTER_JC_SEA_EXP_PK,");
					sb.Append("       Q.INV_SUPPLIER_FK AS INV_SUPPLIER_FK,");
					sb.Append("       Q.INV_SUPPLIER_TRN_FK INV_SUPPLIER_TRN_FK,");
					sb.Append("       Q.VENDOR_MST_FK AS VENDOR_MST_FK,");
					sb.Append("       Q.VESVOYAGE,");
					sb.Append("       NVL(Q.TEU_FACTOR, 0) TEUS,");
					sb.Append("       NVL(Q.PACK_COUNT, 0) QUANTITY,");
					sb.Append("       NVL(Q.VOLUME_IN_CBM, 0) VOLUME,");
					sb.Append("       NVL(Q.GROSS_WEIGHT, 0) GROSS_WEIGHT,");
					sb.Append("       NVL(Q.NET_WEIGHT, 0) NET_WEIGHT,");
					sb.Append("       NVL(SUM(Q.ESTIMATED_AMT), 0) ESTIMATED_AMT,");
					sb.Append("       NVL(SUM(Q.ACTUAL_AMT), 0) ACTUAL_AMT,");
					sb.Append("       NVL(SUM(Q.TAX_PERCENTAGE), 0) TAX_PERCENTAGE,");
					sb.Append("       NVL(SUM(Q.TAX_AMT), 0) TAX_AMT,");
					sb.Append("       NVL(SUM(Q.TOTAL_COST), 0) TOTAL_COST,");
					sb.Append("       Q.FACCURR,");
					sb.Append("       NVL(SUM(Q.FAC_AMT), 0) FAC_AMT,");
					sb.Append("       NVL(SUM(Q.ROE), 0) ROE,");
					if (chk_ExRATE == 1 & ROE == 0) {
						sb.Append("       NVL(SUM(Q.FAC_AMT), 0) FAC_AMT_VOU,");
						sb.Append("       NVL(SUM(Q.TOTAL_COST - NVL(Q.FAC_AMT,0)), 0) PAYABLE_AMT,");
						//sb.Append("       NVL(SUM((Q.TOTAL_COST - (Q.FAC_AMT)) - (Q.ESTIMATED_AMT)), 0) DIFF,")
						sb.Append("       NVL(SUM(Q.FAC_AMT), 0) DIFF,");
					} else {
						sb.Append("       NVL(SUM(Q.FAC_AMT * Q.ROE), 0) FAC_AMT_VOU,");
						sb.Append("       NVL(SUM(Q.TOTAL_COST - (NVL(Q.FAC_AMT,0) * Q.ROE)), 0) PAYABLE_AMT,");
						//sb.Append("       NVL(SUM((Q.TOTAL_COST - (Q.FAC_AMT * Q.ROE)) - (Q.ESTIMATED_AMT)), 0) DIFF,")
						sb.Append("       NVL(SUM(Q.FAC_AMT * Q.ROE), 0)  DIFF,");
					}
					sb.Append("       Q.SEL,");
					sb.Append("       Q.CHK");
					sb.Append("  FROM (SELECT DISTINCT '' SRNO,");
					sb.Append("                        MJOB_EXP.MASTER_JC_REF_NO AS REF_NR,");
					sb.Append("                        MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK AS MASTER_JC_SEA_EXP_PK,");
					sb.Append("                        '' JOB_TRN_EST_PK,");
					sb.Append("                        MJOB_COST.INV_SUPPLIER_FK,");
					sb.Append("                        '' INV_SUPPLIER_TRN_FK,");
					if (Business_Type == 2) {
						sb.Append("                        (CASE");
						sb.Append("                          WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						sb.Append("                               NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
						sb.Append("                           ''");
						sb.Append("                          ELSE");
						sb.Append("                           NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						sb.Append("                           NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
						sb.Append("                        END) AS VESVOYAGE,");
						sb.Append("                        JOBCONT.TEU_FACTOR,");
					} else {
						sb.Append("       JOB_EXP.VOYAGE_FLIGHT_NO AS VESVOYAGE,");
						sb.Append("       0 TEU_FACTOR,");
					}
					sb.Append("                        JOBCONT.PACK_COUNT,");
					sb.Append("                        JOBCONT.VOLUME_IN_CBM,");
					sb.Append("                        JOBCONT.GROSS_WEIGHT,");
					sb.Append("                        JOBCONT.NET_WEIGHT,");
					sb.Append("                        MJOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
					if (chk_ExRATE == 1 & ROE == 0) {
						sb.Append("                        ROUND(MJOB_COST.ESTIMATED_COST ,");
						sb.Append("                              2) AS ESTIMATED_AMT,");
						sb.Append("                        ROUND(MJOB_COST.ESTIMATED_COST , ");
						sb.Append("                              2) AS ACTUAL_AMT,");
					} else {
						sb.Append("                        ROUND(MJOB_COST.ESTIMATED_COST *");
						sb.Append("                              GET_EX_RATE_BUY(MJOB_COST.CURRENCY_MST_FK,");
						sb.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                          " + INVSUP_DT + "),");
						sb.Append("                              2) AS ESTIMATED_AMT,");
						sb.Append("                        ROUND(MJOB_COST.ESTIMATED_COST *");
						sb.Append("                              GET_EX_RATE_BUY(MJOB_COST.CURRENCY_MST_FK,");
						sb.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                          " + INVSUP_DT + "),");
						sb.Append("                              2) AS ACTUAL_AMT,");
					}


					sb.Append("                        0 TAX_PERCENTAGE,");
					sb.Append("                        0 TAX_AMT,");
					//sb.Append("                        MJOB_COST.TOTAL_COST,")
					sb.Append("                        ROUND(MJOB_COST.ESTIMATED_COST *");
					sb.Append("                              GET_EX_RATE_BUY(MJOB_COST.CURRENCY_MST_FK,");
					sb.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					sb.Append("                                          " + INVSUP_DT + "),");
					sb.Append("                              2) AS TOTAL_COST,");
					sb.Append("                        '" + CurrencyID + "' FACCURR,");
					if (chk_ExRATE == 1 & ROE == 0) {
						//'SEA
						if (Business_Type == 2 & Process_Type == 1) {
							sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       0");
							sb.Append("                                       2,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 2 & Process_Type == 2) {
							sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                      0,");
							sb.Append("                                       2,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						//AIR
						} else if (Business_Type == 1 & Process_Type == 1) {
							sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       0,");
							sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 1 & Process_Type == 2) {
							sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       0,");
							sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						}
					} else {
						//'SEA
						if (Business_Type == 2 & Process_Type == 1) {
							sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							sb.Append("                                       2,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 2 & Process_Type == 2) {
							sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							sb.Append("                                       2,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						//AIR
						} else if (Business_Type == 1 & Process_Type == 1) {
							sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 1 & Process_Type == 2) {
							sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						}
					}

					sb.Append("                        GET_EX_RATE_BUY(MJOB_COST.CURRENCY_MST_FK, " + Convert.ToString(CurrencyPK) + ", " + INVSUP_DT + ") ROE,");
					sb.Append("                        '' SEL,");
					sb.Append("                        '' CHK");
					sb.Append("          FROM CURRENCY_TYPE_MST_TBL  CURR,");
					if (Process_Type == 1) {
						sb.Append("           HBL_EXP_TBL           HBL,");
					}
					if (Process_Type == 1 & Business_Type == 2) {
						sb.Append("               BOOKING_MST_TBL        BOOK,");
					} else if (Process_Type == 1 & Business_Type == 1) {
						sb.Append("               BOOKING_MST_TBL        BOOK,");
					}
					sb.Append("               JOB_CARD_TRN   JOB_EXP,");
					sb.Append("               MASTER_JC_" + BusinessProcess + "_TBL  MJOB_EXP,");
					sb.Append("               MJC_TRN_" + BusinessProcess + "_COST   MJOB_COST,");
					///'
					sb.Append("(SELECT DISTINCT JOB.MASTER_JC_FK,");
					if (Business_Type == 2) {
						sb.Append("                                SUM(CTMT.TEU_FACTOR) TEU_FACTOR,");
					} else {
						sb.Append("                                0 TEU_FACTOR,");
					}
					sb.Append("                                SUM(JOB_CONT.PACK_COUNT) PACK_COUNT,");
					sb.Append("                                SUM(JOB_CONT.VOLUME_IN_CBM) VOLUME_IN_CBM,");
					sb.Append("                                SUM(JOB_CONT.GROSS_WEIGHT) GROSS_WEIGHT,");
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						sb.Append("                        (CASE  WHEN BKG.CARGO_TYPE = 1 THEN");
						sb.Append("                          SUM(JOB_CONT.NET_WEIGHT)");
						sb.Append("                         ELSE SUM(JOB_CONT.CHARGEABLE_WEIGHT)");
						sb.Append("                         END) NET_WEIGHT");
					} else if (Business_Type == 2 & Process_Type == 2) {
						sb.Append("                        (CASE  WHEN JOB.CARGO_TYPE = 1 THEN");
						sb.Append("                          SUM(JOB_CONT.NET_WEIGHT)");
						sb.Append("                         ELSE SUM(JOB_CONT.CHARGEABLE_WEIGHT)");
						sb.Append("                         END) NET_WEIGHT");
					//'AIR
					} else if (Business_Type == 1) {
						sb.Append("     SUM(JOB_CONT.CHARGEABLE_WEIGHT) NET_WEIGHT");
					}
					sb.Append("                  FROM JOB_CARD_TRN   JOB,");
					sb.Append("                       JOB_TRN_CONT   JOB_CONT");
					if (Business_Type == 2) {
						sb.Append("                       ,CONTAINER_TYPE_MST_TBL CTMT");
					}
					if (Process_Type == 1 & Business_Type == 2) {
						sb.Append("                       ,BOOKING_MST_TBL        BKG");
					} else if (Process_Type == 1 & Business_Type == 1) {
						sb.Append("                       ,BOOKING_MST_TBL        BKG");
					}
					sb.Append("                 WHERE JOB.JOB_CARD_TRN_PK = JOB_CONT.JOB_CARD_TRN_FK");
					if (Business_Type == 2) {
						sb.Append("                   AND JOB_CONT.CONTAINER_TYPE_MST_FK =");
						sb.Append("                       CTMT.CONTAINER_TYPE_MST_PK(+)");
					}
					if (Process_Type == 1 & Business_Type == 2) {
						sb.Append("           AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
					} else if (Process_Type == 1 & Business_Type == 1) {
						sb.Append("           AND BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
					}
					sb.Append("                 GROUP BY JOB.MASTER_JC_FK");
					//'SEA
					if (Business_Type == 2 & Process_Type == 1) {
						sb.Append("   , BKG.CARGO_TYPE) JOBCONT,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						sb.Append("   , JOB.CARGO_TYPE ) JOBCONT,");
					//'AIR
					} else if (Business_Type == 1) {
						sb.Append("  ) JOBCONT,");
					}
					///''
					sb.Append("               COST_ELEMENT_MST_TBL   COST_ELE");
					sb.Append("         WHERE MJOB_COST.MASTER_JC_" + BusinessProcess + "_FK = MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK");
					sb.Append("           AND JOB_EXP.MASTER_JC_FK = JOBCONT.MASTER_JC_FK");
					sb.Append("           AND JOB_EXP.MASTER_JC_FK = MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK");
					if (Process_Type == 1 & Business_Type == 2) {
						sb.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					} else if (Process_Type == 1 & Business_Type == 1) {
						sb.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					}
					if (Process_Type == 1) {
						sb.Append("           AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
					}
					sb.Append("           AND MJOB_COST.MJC_TRN_" + BusinessProcess + "_COST_PK NOT IN");
					sb.Append("               (SELECT IST.MJC_TRN_COST_FK");
					sb.Append("                  FROM INV_SUPPLIER_TRN_TBL IST, INV_SUPPLIER_TBL IT");
					sb.Append("                 WHERE IST.INV_SUPPLIER_TBL_FK = IT.INV_SUPPLIER_PK");
					sb.Append("                   AND IT.BUSINESS_TYPE = 2");
					sb.Append("                   AND IT.PROCESS_TYPE = 1");
					sb.Append("                   AND IST.MJC_TRN_COST_FK IS NOT NULL)");
					sb.Append("           AND MJOB_COST.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
					sb.Append("           AND MJOB_COST.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
					sb.Append("           AND MJOB_COST.INV_SUPPLIER_FK IS NULL");
					sb.Append("           AND MJOB_COST.VENDOR_MST_FK = " + VendorPK);
					sb.Append(strCondition);
					sb.Append("           ) Q");
					sb.Append(" GROUP BY Q.VENDOR_MST_FK,");
					sb.Append("          Q.REF_NR,");
					sb.Append("          Q.MASTER_JC_SEA_EXP_PK,");
					sb.Append("          Q.INV_SUPPLIER_FK,");
					sb.Append("          Q.INV_SUPPLIER_TRN_FK,");
					sb.Append("          Q.VESVOYAGE,");
					sb.Append("          Q.VENDOR_MST_FK,");
					sb.Append("          Q.FACCURR,");
					sb.Append("          Q.TEU_FACTOR,");
					sb.Append("          Q.PACK_COUNT,");
					sb.Append("          Q.VOLUME_IN_CBM,");
					sb.Append("          Q.GROSS_WEIGHT,");
					sb.Append("          Q.NET_WEIGHT");
					//If Business_Type = 1 Then
					//    sb.Replace("HBL", "HAWB")
					//    sb.Replace("MBL", "MAWB")
					//End If
					DA = objWF.GetDataAdapter(sb.ToString());
					DA.Fill(MainDS, "PARENT");
					///
					sb.Remove(0, sb.Length);
					sb.Append("SELECT Q.SHIP_REF_NR HBL_REF_NR,");
					sb.Append("       Q.REF_DATE,");
					sb.Append("       Q.CARGOTYPE,");
					sb.Append("       Q.MASTER_JC_SEA_EXP_PK,");
					sb.Append("       Q.JOB_TRN_EST_PK,");
					sb.Append("       Q.INV_SUPPLIER_FK,");
					sb.Append("       Q.INV_SUPPLIER_TRN_FK,");
					sb.Append("       Q.VENDOR_MST_FK,");
					sb.Append("       Q.COST_ELEMENT_ID,");
					sb.Append("       Q.COST_ELEMENT_MST_PK,");
					sb.Append("       Q.VESVOYAGE,");
					sb.Append("      '' CONTAINER_TYPE_MST_ID, ");
					sb.Append("       NVL(Q.ESTIMATED_AMT, 0) ESTIMATED_AMT,");
					sb.Append("       NVL(Q.ACTUAL_AMT, 0) ACTUAL_AMT,");
					sb.Append("       NVL(Q.TAX_PERCENTAGE, 0) TAX_PERCENTAGE,");
					sb.Append("       NVL(Q.TAX_AMT, 0) TAX_AMT,");
					sb.Append("       NVL(Q.TOTAL_COST, 0) TOTAL_COST,");
					sb.Append("       Q.FACCURR,");
					sb.Append("       NVL(Q.FAC_AMT, 0) FAC_AMT,");
					sb.Append("       NVL(Q.ROE, 0) ROE,");
					if (chk_ExRATE == 1 & ROE == 0) {
						sb.Append("       NVL(Q.FAC_AMT, 0) FAC_AMT_VOU,");
						sb.Append("       NVL(Q.TOTAL_COST - NVL(Q.FAC_AMT,0), 0) PAYABLE_AMT,");
						//sb.Append("       NVL((Q.TOTAL_COST - (Q.FAC_AMT)) - (Q.ESTIMATED_AMT), 0) DIFF,")
						sb.Append("       NVL(Q.FAC_AMT, 0) DIFF,");
					} else {
						sb.Append("       NVL(Q.FAC_AMT * Q.ROE, 0) FAC_AMT_VOU,");
						sb.Append("       NVL(Q.TOTAL_COST - (NVL(Q.FAC_AMT,0) * Q.ROE), 0) PAYABLE_AMT,");
						//sb.Append("       NVL((Q.TOTAL_COST - (Q.FAC_AMT * Q.ROE)) - (Q.ESTIMATED_AMT), 0) DIFF,")
						sb.Append("       NVL(Q.FAC_AMT * Q.ROE, 0)  DIFF,");
					}

					sb.Append("       Q.SEL,Q.REF_NR");
					sb.Append("  FROM (SELECT DISTINCT ");
					if (Process_Type == 1) {
						sb.Append("           HBL.HBL_REF_NO SHIP_REF_NR,");
						sb.Append("           HBL.HBL_DATE AS REF_DATE,");
					} else {
						sb.Append("            JOB_EXP.HBL_HAWB_REF_NO SHIP_REF_NR,");
						sb.Append("            JOB_EXP.HBL_HAWB_DATE AS REF_DATE, ");
					}
					if (Business_Type == 2 & Process_Type == 1) {
						sb.Append("                        DECODE(BOOK.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGOTYPE,");
					} else if (Business_Type == 2 & Process_Type == 2) {
						sb.Append("                        DECODE(JOB_EXP.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGOTYPE,");
					} else if (Business_Type == 1) {
						sb.Append(" '' CARGOTYPE,");
					}
					sb.Append("                        MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK AS MASTER_JC_SEA_EXP_PK,");
					sb.Append("                        MJOB_COST.MJC_TRN_" + BusinessProcess + "_COST_PK AS JOB_TRN_EST_PK,");
					sb.Append("                        MJOB_COST.INV_SUPPLIER_FK,");
					sb.Append("                        '' INV_SUPPLIER_TRN_FK,");
					sb.Append("                        COST_ELE.COST_ELEMENT_ID,");
					sb.Append("                        COST_ELE.COST_ELEMENT_MST_PK,");
					if (Business_Type == 2) {
						sb.Append("                        (CASE");
						sb.Append("                          WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						sb.Append("                               NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
						sb.Append("                           ''");
						sb.Append("                          ELSE");
						sb.Append("                           NVL(JOB_EXP.VESSEL_NAME, '') || '/' ||");
						sb.Append("                           NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
						sb.Append("                        END) AS VESVOYAGE,");
					} else {
						sb.Append("                    JOB_EXP.VOYAGE_FLIGHT_NO AS VESVOYAGE,");
					}
					if (chk_ExRATE == 1 & ROE == 0) {
						sb.Append("                        MJOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
						sb.Append("                        ROUND(MJOB_COST.ESTIMATED_COST ,");
						sb.Append("                              2) AS ESTIMATED_AMT,");
						sb.Append("                        ROUND(MJOB_COST.ESTIMATED_COST ,");
						sb.Append("                              2) AS ACTUAL_AMT,");
					} else {
						sb.Append("                        MJOB_COST.VENDOR_MST_FK AS VENDOR_MST_FK,");
						sb.Append("                        ROUND(MJOB_COST.ESTIMATED_COST *");
						sb.Append("                              GET_EX_RATE_BUY(MJOB_COST.CURRENCY_MST_FK,");
						sb.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                          " + INVSUP_DT + "),");
						sb.Append("                              2) AS ESTIMATED_AMT,");
						//sb.Append("                        0 ACTUAL_AMT,")
						sb.Append("                        ROUND(MJOB_COST.ESTIMATED_COST *");
						sb.Append("                              GET_EX_RATE_BUY(MJOB_COST.CURRENCY_MST_FK,");
						sb.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
						sb.Append("                                          " + INVSUP_DT + "),");
						sb.Append("                              2) AS ACTUAL_AMT,");
					}

					sb.Append("                        0 TAX_PERCENTAGE,");
					sb.Append("                        0 TAX_AMT,");
					//sb.Append("                        MJOB_COST.TOTAL_COST,")
					sb.Append("                        ROUND(MJOB_COST.ESTIMATED_COST *");
					sb.Append("                              GET_EX_RATE_BUY(MJOB_COST.CURRENCY_MST_FK,");
					sb.Append("                                          " + Convert.ToString(CurrencyPK) + ",");
					sb.Append("                                          " + INVSUP_DT + "),");
					sb.Append("                              2) AS TOTAL_COST,");
					sb.Append("                        '" + CurrencyID + "' FACCURR,");
					if (chk_ExRATE == 1 & ROE == 0) {
						//'SEA
						if (Business_Type == 2 & Process_Type == 1) {
							sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       0,");
							sb.Append("                                       2,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 2 & Process_Type == 2) {
							sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       0,");
							sb.Append("                                       2,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						//AIR
						} else if (Business_Type == 1 & Process_Type == 1) {
							sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       0,");
							sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 1 & Process_Type == 2) {
							sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       0,");
							sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						}
					} else {
						//'SEA
						if (Business_Type == 2 & Process_Type == 1) {
							sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							sb.Append("                                       2,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 2 & Process_Type == 2) {
							sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							sb.Append("                                       2,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						//AIR
						} else if (Business_Type == 1 & Process_Type == 1) {
							sb.Append("                        GET_FAC_AMOUNT(BOOK.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						} else if (Business_Type == 1 & Process_Type == 2) {
							sb.Append("                        GET_FAC_AMOUNT(JOB_EXP.CARRIER_MST_FK,");
							sb.Append("                                       JOB_EXP.JOB_CARD_TRN_PK,");
							sb.Append("                                       " + Convert.ToString(CurrencyPK) + ",");
							sb.Append("                                       1,MJOB_COST.COST_ELEMENT_MST_FK," + Process_Type + ",1) FAC_AMT,");
						}
					}

					sb.Append("                        GET_EX_RATE_BUY(MJOB_COST.CURRENCY_MST_FK,  " + Convert.ToString(CurrencyPK) + ", " + INVSUP_DT + ") ROE,");
					sb.Append("                        '' SEL,JOB_EXP.JOBCARD_REF_NO AS REF_NR");
					sb.Append("          FROM COST_ELEMENT_MST_TBL  COST_ELE,");
					sb.Append("               CURRENCY_TYPE_MST_TBL CURR,");
					if (Process_Type == 1 & Business_Type == 2) {
						sb.Append("               BOOKING_MST_TBL        BOOK,");
					} else if (Process_Type == 1 & Business_Type == 1) {
						sb.Append("               BOOKING_MST_TBL        BOOK,");
					}
					sb.Append("               JOB_CARD_TRN  JOB_EXP,");
					sb.Append("               MASTER_JC_" + BusinessProcess + "_TBL MJOB_EXP,");
					sb.Append("               MJC_TRN_" + BusinessProcess + "_COST  MJOB_COST,");
					sb.Append("               JOB_TRN_CONT  JOB_CONT");
					if (Process_Type == 1) {
						sb.Append("               ,HBL_EXP_TBL           HBL");
					}
					sb.Append("         WHERE MJOB_COST.MASTER_JC_" + BusinessProcess + "_FK= JOB_EXP.MASTER_JC_FK");
					sb.Append("           AND JOB_EXP.JOB_CARD_TRN_PK = JOB_CONT.JOB_CARD_TRN_FK");
					sb.Append("           AND MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK=MJOB_COST.MASTER_JC_" + BusinessProcess + "_FK");
					if (Process_Type == 1 & Business_Type == 2) {
						sb.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					} else if (Process_Type == 1 & Business_Type == 1) {
						sb.Append("           AND BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
					}
					if (Process_Type == 1) {
						sb.Append("           AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
					}
					sb.Append("           AND MJOB_COST.MJC_TRN_" + BusinessProcess + "_COST_PK NOT IN");
					sb.Append("               (SELECT IST.MJC_TRN_COST_FK");
					sb.Append("                  FROM INV_SUPPLIER_TRN_TBL IST, INV_SUPPLIER_TBL IT");
					sb.Append("                 WHERE IST.INV_SUPPLIER_TBL_FK = IT.INV_SUPPLIER_PK");
					sb.Append("                   AND IT.BUSINESS_TYPE = 2");
					sb.Append("                   AND IT.PROCESS_TYPE = 1");
					sb.Append("                   AND IST.MJC_TRN_COST_FK IS NOT NULL)");
					sb.Append("           AND MJOB_COST.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
					sb.Append("           AND MJOB_COST.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
					sb.Append("           AND MJOB_COST.INV_SUPPLIER_FK IS NULL");
					sb.Append(strCondition);
					sb.Append("           AND MJOB_COST.VENDOR_MST_FK =" + VendorPK);
					sb.Append("           ) Q");
				}
				//If Business_Type = 1 Then
				//    sb.Replace("HBL", "HAWB")
				//End If
				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "CHILD");
				DataRelation relataion_Details = new DataRelation("LOCGROUP", new DataColumn[] { MainDS.Tables[0].Columns["MASTER_JC_SEA_EXP_PK"] }, new DataColumn[] { MainDS.Tables[1].Columns["MASTER_JC_SEA_EXP_PK"] });
				relataion_Details.Nested = true;
				MainDS.Relations.Add(relataion_Details);
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

		#region "ROE Count"
		public int ROECount(Int32 CurrencyPK = 0, string InvoiceDt = "")
		{
			try {
				string strSQL = null;
				Int32 totRecords = 0;
				WorkFlow objTotRecCount = new WorkFlow();
				strSQL = "select count(*) from exchange_rate_trn ex, currency_type_mst_tbl cur";
				strSQL += "where cur.currency_mst_pk =" + CurrencyPK;
				strSQL += " and ex.currency_mst_fk=cur.currency_mst_pk";
				strSQL += " and ex.exch_rate_type_fk = 1 ";
				strSQL += "AND TO_DATE('" + InvoiceDt + "','" + dateFormat + "') BETWEEN ex.from_date AND ex.to_date";

				totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSQL.ToString()));
				return totRecords;
			} catch (OracleException OraExp) {
				throw OraExp;
				//'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Inv details for report"
		public DataSet FetchJobCardInvDetails(Int32 VendorPK, Int16 Business_Type, Int16 Process_Type, Int32 CurrencyPK = 0, string INVOICENO = "", int JobType = 1)
		{
			WorkFlow objWK = new WorkFlow();
			string BusinessProcess = null;
			string VslFlight = null;
			if (Business_Type == 2) {
				BusinessProcess = "SEA";
				VslFlight = "VESSEL_NAME";
			} else {
				BusinessProcess = "AIR";
				VslFlight = "FLIGHT_NO";
			}
			if (Process_Type == 1) {
				BusinessProcess += "_EXP";
			} else {
				BusinessProcess += "_IMP";
			}
			try {
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				//sb.Append("SELECT JOB_EXP.JOBCARD_REF_NO AS REF_NR,")
				//sb.Append("       INV_SUP.INVOICE_REF_NO AS VOUCHER_NUMBER,")
				//sb.Append("       INV_SUP.INVOICE_DATE   AS VOUCHER_DATE,")
				//sb.Append("      ")
				//sb.Append("       ROUND(INV_TRN.ACTUAL_Amt *")
				//sb.Append("             GET_EX_RATE(INV_SUP.Currency_Mst_Fk, 173, SYSDATE),")
				//sb.Append("             2) AS ACTUAL_AMT,")
				//sb.Append("       INV_TRN.TAX_PERCENT AS TAX_PERCENTAGE,")
				//sb.Append("       INV_TRN.TAX_AMOUNT AS TAX_AMT,")
				//sb.Append("       INV_TRN.TOTAL_COST")
				//sb.Append("              ")
				sb.Append("SELECT INV_SUP.SUPPLIER_INV_NO AS VOUCHER_NUMBER,");
				sb.Append("       INV_SUP.SUPPLIER_INV_DT AS VOUCHER_DATE,");
				sb.Append("       INV_SUP.SUPPLIER_DUE_DT AS VOUCHER_DUE_DATE,");
				sb.Append("       COST_ELE.COST_ELEMENT_NAME AS SERVICES,");
				//sb.Append("       ROUND(INV_TRN.ACTUAL_Amt *")
				//sb.Append("             GET_EX_RATE_BUY(INV_SUP.Currency_Mst_Fk, " & CStr(CurrencyPK) & ", ")
				//sb.Append("                                   TO_DATE(INV_SUP.SUPPLIER_INV_DT,DATEFORMAT)), ")
				//sb.Append("             2) + INV_TRN.TAX_AMOUNT AS ACTUAL_AMT")
				sb.Append("       ROUND(INV_TRN.PAYABLE_AMT *");
				sb.Append("             GET_EX_RATE_BUY(INV_SUP.Currency_Mst_Fk, " + Convert.ToString(CurrencyPK) + ", ");
				sb.Append("                                   TO_DATE(INV_SUP.SUPPLIER_INV_DT,DATEFORMAT)), ");
				sb.Append("             2) AS ACTUAL_AMT,");
				if (Business_Type == 2 & JobType == 1) {
					sb.Append("   CASE WHEN  JOB_EXP.CARGO_TYPE = 4 THEN  ");
					sb.Append("    COMM.COMMODITY_NAME ");
					sb.Append("  ELSE CTMT.CONTAINER_TYPE_MST_ID END CONTAINER_TYPE_MST_ID ");
				} else {
					sb.Append("     ''CONTAINER_TYPE_MST_ID ");
				}
				sb.Append("  FROM INV_SUPPLIER_TBL     INV_SUP,");
				sb.Append("       INV_SUPPLIER_TRN_TBL INV_TRN,");
				//sb.Append("       JOB_TRN_SEA_EXP_COST JOB_COST,")
				//sb.Append("       JOB_CARD_SEA_EXP_TBL JOB_EXP,")
				if (JobType == 1) {
					sb.Append("JOB_TRN_COST JOB_COST,");
					sb.Append("JOB_CARD_TRN JOB_EXP,");
					if (Business_Type == 2) {
						sb.Append("  COMMODITY_MST_TBL COMM, ");
						sb.Append(" CONTAINER_TYPE_MST_TBL CTMT,");
					}
				} else if (JobType == 2) {
					sb.Append("CBJC_TRN_COST JOB_COST,");
					sb.Append("CBJC_TBL JOB_EXP,");
				} else if (JobType == 3) {
					sb.Append("TRANSPORT_TRN_COST JOB_COST,");
					sb.Append("TRANSPORT_INST_SEA_TBL JOB_EXP,");
				}
				sb.Append("       COST_ELEMENT_MST_TBL COST_ELE");
				sb.Append(" WHERE INV_SUP.INV_SUPPLIER_PK = INV_TRN.INV_SUPPLIER_TBL_FK");
				sb.Append("   AND INV_SUP.VENDOR_MST_FK = " + VendorPK);
				//sb.Append("   AND INV_TRN.JOB_TRN_EST_FK = JOB_COST.JOB_TRN_SEA_EXP_COST_PK(+)")
				if (JobType == 1) {
					sb.Append(" AND INV_TRN.JOB_TRN_EST_FK=JOB_COST.JOB_TRN_COST_PK(+)");
					sb.Append(" AND JOB_COST.JOB_CARD_TRN_FK=JOB_EXP.JOB_CARD_TRN_PK(+)");
				} else if (JobType == 2) {
					sb.Append(" AND INV_TRN.JOB_TRN_EST_FK=JOB_COST.CBJC_TRN_COST_PK(+)");
					sb.Append(" AND JOB_COST.CBJC_FK = JOB_EXP.CBJC_PK(+)");
				} else if (JobType == 3) {
					sb.Append(" AND INV_TRN.JOB_TRN_EST_FK=JOB_COST.TRANSPORT_TRN_COST_PK(+)");
					sb.Append(" AND JOB_COST.TRANSPORT_INST_FK = JOB_EXP.TRANSPORT_INST_SEA_PK(+)");
				}
				if (Business_Type == 2 & JobType == 1) {
					sb.Append("  AND  JOB_COST.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
					sb.Append("    AND JOB_COST.CONTAINER_TYPE_MST_FK = COMM.COMMODITY_MST_PK(+) ");
				}
				//sb.Append("   AND JOB_COST.JOB_CARD_SEA_EXP_FK = JOB_EXP.JOB_CARD_SEA_EXP_PK(+)")
				sb.Append("   AND COST_ELE.COST_ELEMENT_MST_PK = INV_TRN.COST_ELEMENT_MST_FK");
				sb.Append("           AND JOB_COST.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
				sb.Append("   AND INV_SUP.INVOICE_REF_NO ='" + INVOICENO + "'");
				sb.Append("   ORDER BY COST_ELE.PREFERENCE ");

				return objWK.GetDataSet(sb.ToString());
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchMstJobCardInvDetails(Int32 VendorPK, Int16 Business_Type, Int16 Process_Type, Int32 CurrencyPK = 0, string INVOICENO = "", Int64 lblInvSupplierPK = 0, string General = "")
		{
			string BusinessProcess = null;
			string VslFlight = null;
			if (Business_Type == 2) {
				BusinessProcess = "SEA";
				VslFlight = "VESSEL_NAME";
			} else {
				BusinessProcess = "AIR";
				VslFlight = "FLIGHT_NO";
			}
			if (Process_Type == 1) {
				BusinessProcess += "_EXP";
			} else {
				BusinessProcess += "_IMP";
			}
			WorkFlow objWK = new WorkFlow();
			try {
				System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
				//sb.Append("SELECT MJOB_EXP.MASTER_JC_REF_NO AS SHIP_REF_NR,")
				//sb.Append("       INV_SUP.INVOICE_REF_NO AS VOUCHER_NUMBER,")
				//sb.Append("       INV_SUP.INVOICE_DATE AS VOUCHER_DATE,")
				//sb.Append("       ROUND(INV_TRN.ACTUAL_AMT *")
				//sb.Append("             GET_EX_RATE(INV_SUP.Currency_Mst_Fk, 173, SYSDATE),")
				//sb.Append("             2) AS ACTUAL_AMT,")
				//sb.Append("       INV_TRN.TAX_PERCENT AS TAX_PERCENTAGE,")
				//sb.Append("       INV_TRN.TAX_AMOUNT AS TAX_AMT,")
				//sb.Append("       INV_TRN.TOTAL_COST")
				sb.Append("SELECT INV_SUP.SUPPLIER_INV_NO AS VOUCHER_NUMBER,");
				sb.Append("       INV_SUP.SUPPLIER_INV_DT AS VOUCHER_DATE,");
				sb.Append("       INV_SUP.SUPPLIER_DUE_DT AS VOUCHER_DUE_DATE,");
				sb.Append("       COST_ELE.COST_ELEMENT_NAME AS SERVICES,");
				sb.Append("       ROUND(INV_TRN.ACTUAL_AMT *");
				sb.Append("             GET_EX_RATE_BUY(INV_SUP.Currency_Mst_Fk, " + Convert.ToString(CurrencyPK) + ", ");
				sb.Append("                                  TO_DATE(INV_SUP.SUPPLIER_INV_DT,DATEFORMAT)), ");
				sb.Append("             2) AS ACTUAL_AMT,");
				sb.Append("       ''CONTAINER_TYPE_MST_ID ");
				sb.Append("  FROM INV_SUPPLIER_TBL      INV_SUP,");
				sb.Append("       INV_SUPPLIER_TRN_TBL  INV_TRN,");
				//sb.Append("       MJC_TRN_SEA_EXP_COST  MJOB_COST,")
				//sb.Append("       MASTER_JC_SEA_EXP_TBL MJOB_EXP,")
				sb.Append("       MJC_TRN_" + BusinessProcess + "_COST  MJOB_COST,");
				sb.Append("       MASTER_JC_" + BusinessProcess + "_TBL MJOB_EXP,");
				sb.Append("       COST_ELEMENT_MST_TBL  COST_ELE");
				sb.Append(" WHERE INV_SUP.INV_SUPPLIER_PK = INV_TRN.INV_SUPPLIER_TBL_FK");
				sb.Append("   AND INV_SUP.VENDOR_MST_FK = " + VendorPK);
				//sb.Append("   AND INV_TRN.MJC_TRN_COST_FK = MJOB_COST.MJC_TRN_SEA_EXP_COST_PK(+)")
				//sb.Append("   AND MJOB_EXP.MASTER_JC_SEA_EXP_PK = MJOB_COST.MASTER_JC_SEA_EXP_FK")
				sb.Append("   AND INV_TRN.MJC_TRN_COST_FK = MJOB_COST.MJC_TRN_" + BusinessProcess + "_COST_PK(+)");
				sb.Append("   AND MJOB_EXP.MASTER_JC_" + BusinessProcess + "_PK(+) = MJOB_COST.MASTER_JC_" + BusinessProcess + "_FK");
				sb.Append("   AND COST_ELE.COST_ELEMENT_MST_PK = INV_TRN.COST_ELEMENT_MST_FK");
				sb.Append("           AND MJOB_COST.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
				sb.Append("   AND INV_SUP.INVOICE_REF_NO = '" + INVOICENO + "' ");

				return objWK.GetDataSet(sb.ToString());
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}

		}

		#endregion

		#region "Billing Address"
		public DataSet FetchSupADRDetails(Int32 VendorPK)
		{
			WorkFlow objWK = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT VMT.VENDOR_NAME,");
			sb.Append("       VCD.Bill_Address_1,");
			sb.Append("       VCD.BILL_ADDRESS_2,");
			sb.Append("       VCD.BILL_ADDRESS_3,");
			sb.Append("       VCD.BILL_ZIP_CODE,");
			sb.Append("       VCD.BILL_CITY,");
			sb.Append("       VCD.BILL_FAX_NO,");
			sb.Append("       VCD.BILL_EMAIL_ID,");
			sb.Append("       VCD.BILL_URL");
			sb.Append("  FROM VENDOR_MST_TBL VMT,");
			sb.Append("   VENDOR_CONTACT_DTLS VCD");
			sb.Append(" WHERE VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
			sb.Append("   AND VMT.VENDOR_MST_PK = '" + VendorPK + "'");

			try {
				return objWK.GetDataSet(sb.ToString());
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		#endregion

		#region "Save SupplierInvoice"
		public ArrayList SaveSupplierInvoice(DataSet M_DataSet, long Inv_Supplier_Pk, Int16 Business_Type, Int16 Process_Type, Int32 Approved, OracleCommand SelectCommand, OracleTransaction TRAN, int CurrPk, string Flag = "", int CloseFlag = 0,
		string DelPks = "")
		{

			WorkFlow objWK = new WorkFlow();
			int intPKVal = 0;
			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand delCommand = new OracleCommand();
			int InvTrnPk = 0;
			string str = null;
			DataSet DS = null;
			int MjcPiaPk = 0;
			Array DelArray = null;
			int i = 0;

			try {
				DataTable DtTbl = new DataTable();
				DataRow DtRw = null;
				int nRowCnt = 0;

				for (nRowCnt = 0; nRowCnt <= M_DataSet.Tables[0].Rows.Count - 1; nRowCnt++) {
					//If M_DataSet.Tables(0).Rows(nRowCnt).Item("INV_SUPPLIER_TRN_FK") = 0 Then
					if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[nRowCnt]["INV_SUPPLIER_TRN_FK"].ToString())) {
						var _with5 = SelectCommand;

						_with5.CommandType = CommandType.StoredProcedure;
						_with5.CommandText = objWK.MyUserName + ".INV_SUPPLIER_TRN_TBL_PKG.INV_SUPPLIER_TRN_TBL_INS";

						SelectCommand.Parameters.Clear();
						_with5.Parameters.Add("INV_SUPPLIER_TBL_FK_IN", Inv_Supplier_Pk).Direction = ParameterDirection.Input;

						//.Parameters.Add("JOB_CARD_PIA_FK_IN", M_DataSet.Tables(0).Rows(nRowCnt).Item("JOB_TRN_SEA_EXP_PIA_PK")).Direction = ParameterDirection.Input
						//.Parameters["JOB_CARD_PIA_FK_IN"].SourceVersion = DataRowVersion.Current

						_with5.Parameters.Add("JOB_TRN_EST_PK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["JOB_TRN_EST_PK"]).Direction = ParameterDirection.Input;
						_with5.Parameters["JOB_TRN_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

						_with5.Parameters.Add("JOBCARD_REF_NO_IN", M_DataSet.Tables[0].Rows[nRowCnt]["REF_NR"]).Direction = ParameterDirection.Input;
						_with5.Parameters["JOBCARD_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

						_with5.Parameters.Add("COST_ELEMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
						_with5.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

						_with5.Parameters.Add("ESTIMATED_AMT_IN", Convert.ToDecimal(M_DataSet.Tables[0].Rows[nRowCnt]["ESTIMATED_AMT"])).Direction = ParameterDirection.Input;
						_with5.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

						_with5.Parameters.Add("ACTUAL_AMT_IN", Convert.ToDecimal(M_DataSet.Tables[0].Rows[nRowCnt]["ACTUAL_AMT"])).Direction = ParameterDirection.Input;
						_with5.Parameters["ACTUAL_AMT_IN"].SourceVersion = DataRowVersion.Current;

						_with5.Parameters.Add("TAX_PERCENT_IN", Convert.ToDecimal(M_DataSet.Tables[0].Rows[nRowCnt]["TAX_PERCENTAGE"])).Direction = ParameterDirection.Input;
						_with5.Parameters["TAX_PERCENT_IN"].SourceVersion = DataRowVersion.Current;

						_with5.Parameters.Add("TAX_AMOUNT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[nRowCnt]["TAX_AMT"].ToString()) ? 0 : M_DataSet.Tables[0].Rows[nRowCnt]["TAX_AMT"])).Direction = ParameterDirection.Input;
						_with5.Parameters["TAX_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

						_with5.Parameters.Add("TOTAL_COST_IN", Convert.ToDecimal(M_DataSet.Tables[0].Rows[nRowCnt]["TOTAL_COST"])).Direction = ParameterDirection.Input;
						//'Added By Koteshwari on 5/4/2011
						_with5.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

						_with5.Parameters.Add("PAYABLE_AMT_IN", Convert.ToDecimal(M_DataSet.Tables[0].Rows[nRowCnt]["PAYABLE_AMT"])).Direction = ParameterDirection.Input;
						_with5.Parameters["PAYABLE_AMT_IN"].SourceVersion = DataRowVersion.Current;

						_with5.Parameters.Add("ELEMENT_APPROVED_IN", Approved).Direction = ParameterDirection.Input;
						_with5.Parameters.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
						_with5.Parameters.Add("BUSINESS_TYPE_IN", Business_Type).Direction = ParameterDirection.Input;
						_with5.Parameters.Add("CURRENCY_MST_FK_IN", CurrPk).Direction = ParameterDirection.Input;
						//Snigdharani - 07/03/2009
						_with5.Parameters.Add("FLAG_IN", Flag).Direction = ParameterDirection.Input;
						//Koteshwari - 16/5/2011
						_with5.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
						_with5.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
						_with5.Parameters.Add("RETURN_PIA_VALUE", OracleDbType.Int32, 10, "RETURN_PIA_VALUE").Direction = ParameterDirection.Output;
						_with5.Parameters["RETURN_PIA_VALUE"].SourceVersion = DataRowVersion.Current;
						//.ExecuteNonQuery()
						RecAfct = _with5.ExecuteNonQuery();

						//Added By Koteshwari on 17/5/2011
						if (RecAfct > 0) {
							if (!string.IsNullOrEmpty(SelectCommand.Parameters["RETURN_PIA_VALUE"].Value.ToString())) {
								MjcPiaPk = Convert.ToInt32(SelectCommand.Parameters["RETURN_PIA_VALUE"].Value);
							} else {
								MjcPiaPk = 0;
							}
						}
						//End
						//commented by thiyagarajan on 2/1/09 to avoid defect
						//arrMessage.Add("All data saved successfully")


					} else {
						var _with6 = SelectCommand;
						// .Transaction = TRAN
						_with6.CommandType = CommandType.StoredProcedure;
						_with6.CommandText = objWK.MyUserName + ".INV_SUPPLIER_TRN_TBL_PKG.INV_SUPPLIER_TRN_TBL_UPD";
						SelectCommand.Parameters.Clear();

						_with6.Parameters.Add("INV_SUPPLIER_TRN_PK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["INV_SUPPLIER_TRN_FK"]).Direction = ParameterDirection.Input;
						_with6.Parameters["INV_SUPPLIER_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;

						_with6.Parameters.Add("INV_SUPPLIER_TBL_FK_IN", Inv_Supplier_Pk).Direction = ParameterDirection.Input;

						//.Parameters.Add("JOB_CARD_PIA_FK_IN", M_DataSet.Tables(0).Rows(nRowCnt).Item("JOB_TRN_SEA_EXP_PIA_PK")).Direction = ParameterDirection.Input
						//.Parameters["JOB_CARD_PIA_FK_IN"].SourceVersion = DataRowVersion.Current

						_with6.Parameters.Add("JOB_TRN_EST_PK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["JOB_TRN_EST_PK"]).Direction = ParameterDirection.Input;
						_with6.Parameters["JOB_TRN_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

						_with6.Parameters.Add("JOBCARD_REF_NO_IN", M_DataSet.Tables[0].Rows[nRowCnt]["REF_NR"]).Direction = ParameterDirection.Input;
						_with6.Parameters["JOBCARD_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

						_with6.Parameters.Add("COST_ELEMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
						_with6.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

						_with6.Parameters.Add("ESTIMATED_AMT_IN", M_DataSet.Tables[0].Rows[nRowCnt]["ESTIMATED_AMT"]).Direction = ParameterDirection.Input;
						_with6.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

						_with6.Parameters.Add("ACTUAL_AMT_IN", Convert.ToDecimal(M_DataSet.Tables[0].Rows[nRowCnt]["ACTUAL_AMT"])).Direction = ParameterDirection.Input;
						_with6.Parameters["ACTUAL_AMT_IN"].SourceVersion = DataRowVersion.Current;

						_with6.Parameters.Add("TAX_PERCENT_IN", Convert.ToDecimal(M_DataSet.Tables[0].Rows[nRowCnt]["TAX_PERCENTAGE"])).Direction = ParameterDirection.Input;
						_with6.Parameters["TAX_PERCENT_IN"].SourceVersion = DataRowVersion.Current;

						_with6.Parameters.Add("TAX_AMOUNT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[nRowCnt]["TAX_AMT"].ToString()) ? 0 : M_DataSet.Tables[0].Rows[nRowCnt]["TAX_AMT"])).Direction = ParameterDirection.Input;
						_with6.Parameters["TAX_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

						_with6.Parameters.Add("TOTAL_COST_IN", Convert.ToDecimal(M_DataSet.Tables[0].Rows[nRowCnt]["TOTAL_COST"])).Direction = ParameterDirection.Input;
						_with6.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

						_with6.Parameters.Add("PAYABLE_AMT_IN", Convert.ToDecimal(M_DataSet.Tables[0].Rows[nRowCnt]["PAYABLE_AMT"])).Direction = ParameterDirection.Input;
						_with6.Parameters["PAYABLE_AMT_IN"].SourceVersion = DataRowVersion.Current;

						_with6.Parameters.Add("ELEMENT_APPROVED_IN", Approved).Direction = ParameterDirection.Input;
						_with6.Parameters.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
						_with6.Parameters.Add("BUSINESS_TYPE_IN", Business_Type).Direction = ParameterDirection.Input;
						_with6.Parameters.Add("CURRENCY_MST_FK_IN", CurrPk).Direction = ParameterDirection.Input;
						//Snigdharani - 07/03/2009
						_with6.Parameters.Add("FLAG_IN", Flag).Direction = ParameterDirection.Input;
						//'Added By Koteshwari on 16/5/2011
						_with6.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
						_with6.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
						_with6.Parameters.Add("RETURN_PIA_VALUE", OracleDbType.Varchar2, 50, "RETURN_PIA_VALUE").Direction = ParameterDirection.Output;
						_with6.Parameters["RETURN_PIA_VALUE"].SourceVersion = DataRowVersion.Current;

						//.ExecuteNonQuery()
						RecAfct = _with6.ExecuteNonQuery();
						//Added By Koteshwari on 17/5/2011
						if (RecAfct > 0) {
							if (!string.IsNullOrEmpty(SelectCommand.Parameters["RETURN_PIA_VALUE"].Value.ToString())) {
								MjcPiaPk = Convert.ToInt32(SelectCommand.Parameters["RETURN_PIA_VALUE"].Value);
							} else {
								MjcPiaPk = 0;
							}
						}
						//End
						//commented by thiyagarajan on 2/1/09 to avoid defect
						//arrMessage.Add("All data saved successfully")
					}
				}

				//'Vasava For Deleting Cost Elements 
				if ((DelPks != null)) {
					DelArray = DelPks.Split(',');
					for (i = 0; i <= DelArray.Length - 1; i++) {
						var _with7 = SelectCommand;
						_with7.CommandType = CommandType.StoredProcedure;
						_with7.CommandText = objWK.MyUserName + ".INV_SUPPLIER_TRN_TBL_PKG.INV_SUPPLIER_TRN_TBL_DEL";
						var _with8 = _with7.Parameters;
						SelectCommand.Parameters.Clear();
						_with8.Add("INV_SUPPLIER_TRN_PK_IN", DelArray.GetValue(i)).Direction = ParameterDirection.Input;
						_with8.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
						_with7.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
						_with7.ExecuteNonQuery();
					}
				}
				//'End

				if (CloseFlag == 1) {
					//Dim ObjCommand As New OracleClient.OracleCommand
					if (MjcPiaPk > 0) {
						var _with9 = SelectCommand;
						//.Connection = objWK.MyConnection
						_with9.CommandType = CommandType.StoredProcedure;
						_with9.CommandText = objWK.MyUserName + ".MJC_ACTUALCOST_CALCULATION_PKG.MJC_SEA_EXP_COST_CALC";
						var _with10 = _with9.Parameters;
						SelectCommand.Parameters.Clear();
						_with10.Add("MJC_TRN_PIA_PK_IN", Convert.ToInt64(MjcPiaPk)).Direction = ParameterDirection.Input;
						_with10.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
						_with9.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
						_with9.ExecuteNonQuery();
					}
				}
				//End
				arrMessage.Add("All data saved successfully");
				return arrMessage;

			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Save Supplier Invoice Header"
		//Added SuppDueDate By Prakash Chandra on 23/05/2008
		public ArrayList SaveHeader(string InvoiceNo, string InvoiceDate, string SupplierInvNo, string SupplierInvDate, string SupplierDueDate, Int32 Internal_Ref, Int32 VendorPK, Int32 CurrencyPK, string SupplierInvAmt, Int16 Business_Type,
		Int16 Process_Type, Int16 Job_Type, int Status, Int32 Version_No, int Inv_Supplier_Pk, string Remarks = "", Int32 ApprovedBy = 3, string ApprovedDate = "", string Flag = "", int CloseFlag = 0,
		string DelPks = "", double CrLimit = 0, int VendPK = 0, double NetAmt = 0)
		{

			WorkFlow objWK = new WorkFlow();
			OracleTransaction insertTrans = null;
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			int intPkValue = 0;
			bool isUpdate = false;
			int Afct = 0;
			int SupplierPkValue = 0;

			objWK.OpenConnection();
			insertTrans = objWK.MyConnection.BeginTransaction();
			try {
				//On Insert New Record
				if (Inv_Supplier_Pk == 0) {
					//  If ViewState.Item("SupplierPkValue") = 0 Then
					isUpdate = false;
					var _with11 = objWK.MyCommand;
					_with11.Transaction = insertTrans;
					_with11.Connection = objWK.MyConnection;
					_with11.CommandType = CommandType.StoredProcedure;
					_with11.CommandText = objWK.MyUserName + ".INV_SUPPLIER_TBL_PKG.INV_SUPPLIER_TBL_INS";
					_with11.Parameters.Clear();
					var _with12 = _with11.Parameters;
					_with12.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
					_with12.Add("BUSINESS_TYPE_IN", Business_Type).Direction = ParameterDirection.Input;
					_with12.Add("JOB_TYPE_IN", Job_Type).Direction = ParameterDirection.Input;
					_with12.Add("INVOICE_REF_NO_IN", InvoiceNo).Direction = ParameterDirection.Input;
					_with12.Add("INVOICE_DATE_IN", Convert.ToDateTime(InvoiceDate)).Direction = ParameterDirection.Input;
					_with12.Add("SUPPLIER_INV_NO_IN", SupplierInvNo).Direction = ParameterDirection.Input;
					_with12.Add("SUPPLIER_INV_DT_IN", Convert.ToDateTime(SupplierInvDate)).Direction = ParameterDirection.Input;
					_with12.Add("SUPPLIER_DUE_DT_IN", Convert.ToDateTime(SupplierDueDate)).Direction = ParameterDirection.Input;
					//Added By Prakash Chandra on 22/5/2008
					_with12.Add("INTERNAL_REF_IN", Internal_Ref).Direction = ParameterDirection.Input;
					_with12.Add("VENDOR_MST_FK_IN", VendorPK).Direction = ParameterDirection.Input;
					_with12.Add("CURRENCY_MST_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
					_with12.Add("INVOICE_AMT_IN", Convert.ToDouble(SupplierInvAmt)).Direction = ParameterDirection.Input;
					//.Add("REMARKS_IN", Remarks).Direction = ParameterDirection.Input
					_with12.Add("REMARKS_IN", (!string.IsNullOrEmpty(Remarks) ? Remarks : "")).Direction = ParameterDirection.Input;
					_with12.Add("APPROVED_IN", Status).Direction = ParameterDirection.Input;
					//.Add("APPROVED_BY_FK_IN", ApprovedBy).Direction = ParameterDirection.Input
					_with12.Add("APPROVED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					// .Add("APPROVED_DATE_IN", "").Direction = ParameterDirection.Input
					_with12.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					_with12.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
					_with12.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					Afct = _with11.ExecuteNonQuery();
					//With objWK.MyDataAdapter
					//    .InsertCommand = insCommand
					//    .InsertCommand.Transaction = insertTrans
					//    .InsertCommand.ExecuteNonQuery()
					if (Afct > 0) {
						//ViewState["SupplierPkValue"] = Afct;
						//Added By prakash Chnadra on 22/09/2008
						intPkValue = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
					} else {
						insertTrans.Rollback();
					}
				} else {
					isUpdate = true;
					var _with13 = objWK.MyCommand;
					_with13.Transaction = insertTrans;
					_with13.Connection = objWK.MyConnection;
					_with13.CommandType = CommandType.StoredProcedure;
					_with13.CommandText = objWK.MyUserName + ".INV_SUPPLIER_TBL_PKG.INV_SUPPLIER_TBL_UPD";
					_with13.Parameters.Clear();
					var _with14 = _with13.Parameters;
					_with14.Add("INV_SUPPLIER_PK_IN", Inv_Supplier_Pk).Direction = ParameterDirection.Input;
					_with14.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
					_with14.Add("BUSINESS_TYPE_IN", Business_Type).Direction = ParameterDirection.Input;
					_with14.Add("INVOICE_REF_NO_IN", InvoiceNo).Direction = ParameterDirection.Input;
					_with14.Add("INVOICE_DATE_IN", Convert.ToDateTime(InvoiceDate)).Direction = ParameterDirection.Input;
					_with14.Add("SUPPLIER_INV_NO_IN", SupplierInvNo).Direction = ParameterDirection.Input;
					_with14.Add("SUPPLIER_INV_DT_IN", Convert.ToDateTime(SupplierInvDate)).Direction = ParameterDirection.Input;
					_with14.Add("SUPPLIER_DUE_DT_IN", Convert.ToDateTime(SupplierDueDate)).Direction = ParameterDirection.Input;
					//Added By Prakash Chandra on 22/5/2008
					_with14.Add("INTERNAL_REF_IN", Internal_Ref).Direction = ParameterDirection.Input;
					_with14.Add("VENDOR_MST_FK_IN", VendorPK).Direction = ParameterDirection.Input;
					_with14.Add("CURRENCY_MST_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
					_with14.Add("INVOICE_AMT_IN", Convert.ToDouble(SupplierInvAmt)).Direction = ParameterDirection.Input;
					//.Add("REMARKS_IN", Remarks).Direction = ParameterDirection.Input
					_with14.Add("REMARKS_IN", (!string.IsNullOrEmpty(Remarks) ? Remarks : "")).Direction = ParameterDirection.Input;
					_with14.Add("APPROVED_IN", Status).Direction = ParameterDirection.Input;
					//.Add("APPROVED_BY_FK_IN", ApprovedBy).Direction = ParameterDirection.Input
					_with14.Add("APPROVED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					//.Add("APPROVED_DATE_IN", "").Direction = ParameterDirection.Input
					_with14.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
					_with14.Add("VERSION_NO_IN", Version_No).Direction = ParameterDirection.Input;
					_with14.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
					_with14.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
					Afct = _with13.ExecuteNonQuery();
					if (Afct > 0) {
						intPkValue = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
					} else {
						insertTrans.Rollback();
					}

				}
				if (intPkValue > 0) {
					//arrMessage = SaveSupplierInvoice(ObjTrn_DataSet, intPkValue, Business_Type, Process_Type, Status, objWK.MyCommand, insertTrans, CurrencyPK)
					arrMessage = SaveSupplierInvoice(ObjTrn_DataSet, intPkValue, Business_Type, Process_Type, Status, objWK.MyCommand, insertTrans, CurrencyPK, Flag, CloseFlag,
					DelPks);
				}
				if (arrMessage.Count > 0) {
					if (Status == 1) {
						if (CrLimit > 0) {
							SaveCreditLimit(NetAmt, VendPK, insertTrans, CurrencyPK, Convert.ToDateTime(InvoiceDate));
						}
					}
					insertTrans.Commit();
					if (Status == 1) {
						if (intPkValue > 0) {
							//Push to financial system if realtime is selected
							try {
								insertTrans = objWK.MyConnection.BeginTransaction();
								objWK.MyCommand.Transaction = insertTrans;
								objWK.MyCommand.Parameters.Clear();
								objWK.MyCommand.CommandText = objWK.MyUserName + ".ACCOUNTING_INTEGREATION_PKG.DA_VOUCHER_APPROVE";
								objWK.MyCommand.Parameters.Add("DA_VOUCHER_PK_IN", Inv_Supplier_Pk).Direction = ParameterDirection.Input;
								objWK.MyCommand.Parameters.Add("LOCAL_CUR_FK", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
								objWK.MyCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
								objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
								objWK.MyCommand.ExecuteNonQuery();
								insertTrans.Commit();

							} catch (Exception ex) {
							}
							Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
							ArrayList schDtls = null;
							bool errGen = false;
							if (objSch.GetSchedulerPushType() == true) {
								//QFSIService.serFinApp objPush = new QFSIService.serFinApp();
								//try {
								//	schDtls = objSch.FetchSchDtls();
								//	//'Used to Fetch the Sch Dtls
								//	objPush.UpdateTaxElement(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
								//	objPush.UpdatebankAcc(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
								//	objPush.UpdateCurrency(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
								//	objPush.UpdateTransaction(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, intPkValue);
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

					if (isUpdate == false) {
						//Air - Export
						if (Business_Type == 1 & Process_Type == 1) {
							SaveTrackAndTraceForInv(insertTrans, intPkValue, 1, 1, "Voucher", "SUPINV-AIR-EXP", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), objWK, "INS", M_CREATED_BY_FK,
							"O");
						//Air - Import
						} else if (Business_Type == 1 & Process_Type == 2) {
							SaveTrackAndTraceForInv(insertTrans, intPkValue, 1, 2, "Voucher", "SUPINV-AIR-IMP", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), objWK, "INS", M_CREATED_BY_FK,
							"O");
						//Sea - Export
						} else if (Business_Type == 2 & Process_Type == 1) {
							SaveTrackAndTraceForInv(insertTrans, intPkValue, 2, 1, "Voucher", "SUPINV-SEA-EXP", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), objWK, "INS", M_CREATED_BY_FK,
							"O");
						//Sea - Import
						} else if (Business_Type == 2 & Process_Type == 2) {
							SaveTrackAndTraceForInv(insertTrans, intPkValue, 2, 2, "Voucher", "SUPINV-SEA-IMP", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), objWK, "INS", M_CREATED_BY_FK,
							"O");
						}
					}
					//arrMessage.Add("All Data Saved Successfully")
					arrMessage.Add(intPkValue);
					return arrMessage;
				} else {
					insertTrans.Rollback();
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				insertTrans.Rollback();
				throw oraexp;
				arrMessage.Add(oraexp.Message);
			} catch (Exception ex) {
				insertTrans.Rollback();
				arrMessage.Add(ex.Message);
				throw ex;
			} finally {
				objWK.MyCommand.Connection.Close();
			}

		}

		#endregion

		#region "save TrackAndTrace"
		public ArrayList SaveTrackAndTraceForInv(OracleTransaction TRAN, int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, WorkFlow objWF, string flagInsUpd, long lngCreatedby,
		string PkStatus)
		{

			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			objWF.OpenConnection();

			OracleTransaction TRAN1 = null;
			TRAN1 = objWF.MyConnection.BeginTransaction();
			objWF.MyCommand.Transaction = TRAN1;
			try {
				//arrMessage.Clear()
				var _with15 = objWF.MyCommand;
				_with15.CommandType = CommandType.StoredProcedure;
				_with15.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
				_with15.Transaction = TRAN1;
				_with15.Parameters.Clear();
				_with15.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
				_with15.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with15.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
				_with15.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
				_with15.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
				_with15.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
				_with15.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
				_with15.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
				_with15.Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input;
				_with15.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
				_with15.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with15.ExecuteNonQuery();
				TRAN1.Commit();
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyCommand.Connection.Close();

			}
            return new ArrayList();
		}
		#endregion

		#region "GenerateKey"

		public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
		{
			return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
		}

		#endregion

		#region "Fetch JobCardListing"
		public DataSet FetchJobCardListing(Int32 VendorPK, Int16 Status, Int16 Business_Type, Int16 Process_Type, string FromDt = "", string ToDt = "", Int32 TradePK = 0, int JobPK = 0, string Vsl = "", string VendorInvNr = "",
		string InvDt = "", string SupplierRefNr = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SearchType = "", Int32 flag = 0)
		{

			StringBuilder strSql = new StringBuilder();
			string strCondition = null;
			string strCondition1 = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
			//Dim SrOP As String = IIf(SearchType = "C", "%", "") '  SrOP is Search Operator..[% or Nothing] >
			string BusinessProcess = null;
			string VslFlight = null;
			if (Business_Type == 2) {
				BusinessProcess = "SEA";
				VslFlight = "VESSEL_NAME";
			} else {
				BusinessProcess = "AIR";
				VslFlight = "FLIGHT_NO";
			}
			if (Process_Type == 1) {
				BusinessProcess += "_EXP";
			} else {
				BusinessProcess += "_IMP";
			}

			if (((FromDt != null) & (ToDt != null)) & FromDt != " " & ToDt != " ") {
				strCondition += " AND TO_DATE(JOB_EXP.JOBCARD_DATE " + " ,'" + dateFormat + "') BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
			} else if (((FromDt != null)) & FromDt != " ") {
				strCondition += " AND TO_DATE(JOB_EXP.JOBCARD_DATE " + " ,'" + dateFormat + "') >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
			} else if (((ToDt != null)) & ToDt != " ") {
				strCondition += " AND TO_DATE(JOB_EXP.JOBCARD_DATE " + " ,'" + dateFormat + "') <= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
			}

			if (((FromDt != null) & (ToDt != null)) & FromDt != " " & ToDt != " ") {
				strCondition1 += " AND TO_DATE(MJOB_EXP.MASTER_JC_DATE " + " ,'" + dateFormat + "') BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
			} else if (((FromDt != null)) & FromDt != " ") {
				strCondition1 += " AND TO_DATE(MJOB_EXP.MASTER_JC_DATE " + " ,'" + dateFormat + "') >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
			} else if (((ToDt != null)) & ToDt != " ") {
				strCondition1 += " AND TO_DATE(MJOB_EXP.MASTER_JC_DATE " + " ,'" + dateFormat + "') <= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
			}
			//If P_Customer_Mst_Pk > 0 And SearchType = "C" Then
			//    strCondition &= vbCrLf & " And cmt.Customer_Mst_Pk =" & P_Customer_Mst_Pk
			//ElseIf P_Customer_Mst_Pk > 0 And SearchType = "S" Then
			//    strCondition &= vbCrLf & " And cmt.Customer_Mst_Pk =" & P_Customer_Mst_Pk
			//End If

			if (VendorPK != 0 & SearchType == "C") {
				strCondition += " AND VMST.VENDOR_MST_PK =" + VendorPK;
			} else if (VendorPK != 0 & SearchType == "S") {
				strCondition += " AND VMST.VENDOR_MST_PK =" + VendorPK;
			}

			if (!string.IsNullOrEmpty(Vsl) & SearchType == "C") {
				if (Business_Type == 2) {
					strCondition += " AND JOB_EXP.VESSEL_NAME ||" + "'-'" + " || JOB_EXP.VOYAGE_FLIGHT_NO like '%" + Vsl.Trim() + "%'";
				} else {
					strCondition += " AND JOB_EXP.VOYAGE_FLIGHT_NO like '%" + Vsl.Trim() + "%'";
				}
			} else if (!string.IsNullOrEmpty(Vsl) & SearchType == "S") {
				if (Business_Type == 2) {
					strCondition += " AND JOB_EXP.VESSEL_NAME ||" + "'-'" + " || JOB_EXP.VOYAGE_FLIGHT_NO like '" + Vsl.Trim() + "%'";
				} else {
					strCondition += " AND JOB_EXP.VOYAGE_FLIGHT_NO like '" + Vsl.Trim() + "%'";
				}
			}


			if (VendorInvNr.Trim().Length > 0 & SearchType == "C") {
				strCondition += " AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '%" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'";
			} else if (VendorInvNr.Trim().Length > 0 & SearchType == "S") {
				strCondition += " AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'";
			}


			if (JobPK != 0 & SearchType == "C") {
				strCondition += " AND JOB_EXP.JOB_CARD_TRN_PK =" + JobPK;
			} else if (JobPK != 0 & SearchType == "S") {
				strCondition += " AND JOB_EXP.JOB_CARD_TRN_PK =" + JobPK;
			}

			if ((InvDt != null)) {
				strCondition += " AND INVTBL.INVOICE_DATE = TO_DATE('" + InvDt + "','" + dateFormat + "')";
			}
			//strCondition = strCondition & " And upper(Container_Type_Mst_Id) like '%" & P_Container_Type_Mst_Id.ToUpper.Replace("'", "''") & "%' " & vbCrLf

			if (SupplierRefNr.Trim().Length > 0 & SearchType == "C") {
				strCondition += " AND  LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '%" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'";
			} else if (SupplierRefNr.Trim().Length > 0 & SearchType == "S") {
				strCondition += " AND  LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'";
			}
			if (flag == 0) {
				strCondition += " AND 1=2 ";
			}

			strCondition += " AND JOB_EXP.BUSINESS_TYPE = " + Business_Type;
			strCondition += " AND JOB_EXP.PROCESS_TYPE = " + Process_Type;

			strSql.Append("select count(*) from(");
			strSql.Append("SELECT ROWNUM SrNO ,Q.*FROM ( SELECT");
			strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
			strSql.Append("INVTBL.INVOICE_REF_NO VOUCHERNO,");
			strSql.Append("TO_DATE(INVTBL.INVOICE_DATE,dateformat) VOUCHERDATE,");
			strSql.Append("INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
			strSql.Append("VMST.VENDOR_NAME VENDOR,");
			//strSql.Append("JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK,")
			//strSql.Append("JOB_EXP.JOB_CARD_SEA_EXP_PK,")
			//strSql.Append("JOB_EXP.JOBCARD_REF_NO JOBNO,")

			//If Business_Type = 2 Then
			//    strSql.Append("JOB_EXP.VESSEL_NAME ||" & "'-'" & " || JOB_EXP.VOYAGE VSL_FLIGHT,")
			//Else
			//    strSql.Append("JOB_EXP.FLIGHT_NO VSL_FLIGHT,")
			//End If
			strSql.Append("  CURR.CURRENCY_ID CUR,");
			//strSql.Append(" ROUND(SUM(INVTRNTBL.ACTUAL_AMT),2) AMOUNT,")

			//strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.TOTAL_COST,0)),2) AMOUNT,") ''COMMENTED BY ASHISH TO TALLY THE LISTING AND ENTRY INV AMOUNT
			strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.PAYABLE_AMT,0)),2) AMOUNT,");
			//strSql.Append(" ROUND(SUM(INVTRNTBL.TOTAL_COST * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," & BaseCurrFk & " ,INVTBL.INVOICE_DATE)),2) AMOUNT,")
			strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED,1,'Approved',2,'Reject', 0,'Pending') STATUS, ");
			strSql.Append(" 0 Sel ");
			strSql.Append(" FROM ");
			strSql.Append(" INV_SUPPLIER_TBL INVTBL,");
			//adding by thangadurai on 4/8/08
			//strSql.Append(" vendor_contact_dtls VCD,vendor_type_mst_tbl vt,vendor_services_trn VST, ")
			//end
			strSql.Append("INV_SUPPLIER_TRN_TBL INVTRNTBL,");
			strSql.Append("VENDOR_MST_TBL VMST,");
			strSql.Append("CURRENCY_TYPE_MST_TBL CURR,");
			strSql.Append("JOB_CARD_TRN JOB_EXP,");
			///strSql.Append("JOB_TRN_" & BusinessProcess & "_PIA   JOB_TRN_PIA,") 'COMMENTED BY SIVACHANDRAN FOR BUG 4875 TO FETCH SAME DATA IN da REPORT AND VOUCHER LISTING
			strSql.Append(" USER_MST_TBL USRTBL");
			strSql.Append(" WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
			strSql.Append(" AND   VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
			///strSql.Append(" AND   INVTRNTBL.JOB_CARD_PIA_FK= JOB_TRN_PIA.JOB_TRN_" & BusinessProcess & "_PIA_PK")'COMMENTED BY SIVACHANDRAN FOR BUG 4875 TO FETCH SAME DATA IN da REPORT AND VOUCHER LISTING
			///strSql.Append(" AND   JOB_TRN_PIA.JOB_CARD_" & BusinessProcess & "_FK=JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK")
			strSql.Append(" AND INVTRNTBL.JOBCARD_REF_NO = JOB_EXP.JOBCARD_REF_NO ");
			//ADDED BY SIVACHANDRAN FOR BUG 4875 TO FETCH SAME DATA IN da REPORT AND VOUCHER LISTING
			strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
			strSql.Append(" AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK");
			//Manoharan 27Apr2009: To filter based on the Business and process type
			strSql.Append(" AND INVTBL.BUSINESS_TYPE=" + Business_Type);
			strSql.Append(" AND INVTBL.PROCESS_TYPE=" + Process_Type);
			//end
			///strSql.Append(" AND USRTBL.DEFAULT_LOCATION_FK = " & LoggedIn_Loc_FK) 'Commented Sivachandran Its not Locaction Based - Discussed with Magesh
			if (Status < 3) {
				if (SearchType == "C") {
					strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
				} else if (SearchType == "S") {
					strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
				}
			}

			strSql.Append(strCondition);
			strSql.Append(" GROUP BY INVTBL.INVOICE_REF_NO  ,");
			strSql.Append("INVTBL.INVOICE_DATE,");
			strSql.Append(" VMST.VENDOR_NAME ,");
			//strSql.Append("JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK,")
			//strSql.Append("JOB_EXP.JOBCARD_REF_NO ,")
			//If Business_Type = 2 Then
			//    strSql.Append("JOB_EXP.VESSEL_NAME ||" & "'-'" & " || JOB_EXP.VOYAGE,")
			//Else
			//    strSql.Append("JOB_EXP.FLIGHT_NO,")
			//End If
			strSql.Append("INVTRNTBL.ELEMENT_APPROVED,");
			strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
			//strSql.Append("INVTRNTBL.ACTUAL_AMT,")
			strSql.Append("CURR.CURRENCY_ID,");
			strSql.Append("INVTBL.SUPPLIER_INV_NO");

			if (JobPK == 0 & string.IsNullOrEmpty(Vsl) & (string.IsNullOrEmpty(FromDt) | FromDt == " ") & (string.IsNullOrEmpty(ToDt) | ToDt == " ")) {
				strSql.Append(" UNION");

				strSql.Append(" SELECT distinct INVTBL.INV_SUPPLIER_PK,");
				strSql.Append(" INVTBL.INVOICE_REF_NO VOUCHERNO,");
				strSql.Append(" TO_DATE(INVTBL.INVOICE_DATE, dateformat) VOUCHERDATE,");
				strSql.Append("INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
				strSql.Append(" VMST.VENDOR_NAME VENDOR,");
				//strSql.Append(" 0 JOB_CARD_SEA_EXP_PK,")
				//strSql.Append(" '' JOBNO,")
				//strSql.Append(" '' VSL_FLIGHT,")
				strSql.Append("  CURR.CURRENCY_ID CUR,");
				//strSql.Append(" ROUND(SUM(INVTRNTBL.ACTUAL_AMT),2) AMOUNT,")

				//strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.TOTAL_COST,0)),2) AMOUNT,") ''COMMENTED BY ASHISH TO TALLY THE LISTING AND ENTRY INV AMOUNT
				strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.PAYABLE_AMT,0)),2) AMOUNT,");

				//strSql.Append(" ROUND(SUM(INVTRNTBL.TOTAL_COST * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," & BaseCurrFk & " ,INVTBL.INVOICE_DATE)),2) AMOUNT,")
				strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED,");
				strSql.Append(" 1,'Approved', 2,'Reject',0,'Pending') STATUS, ");
				strSql.Append(" 0 Sel ");
				strSql.Append(" FROM INV_SUPPLIER_TBL     INVTBL,");
				strSql.Append(" INV_SUPPLIER_TRN_TBL INVTRNTBL,");
				strSql.Append(" CURRENCY_TYPE_MST_TBL CURR,");
				strSql.Append(" VENDOR_MST_TBL VMST,");
				strSql.Append(" USER_MST_TBL USRTBL");
				//strSql.Append("JOB_CARD_" & BusinessProcess & "_TBL  JOB_EXP,")
				//strSql.Append("JOB_TRN_" & BusinessProcess & "_PIA   JOB_TRN_PIA")
				strSql.Append(" WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
				strSql.Append(" AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
				strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
				//Commented By Koteshwari on 6/5/2011
				//strSql.Append(" and invtrntbl.job_card_pia_fk=0")
				strSql.Append(" AND INVTRNTBL.JOB_TRN_EST_FK=0");

				//'End
				strSql.Append(" AND INVTBL.BUSINESS_TYPE=" + Business_Type);
				strSql.Append(" AND INVTBL.PROCESS_TYPE=" + Process_Type);
				strSql.Append(" AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK");
				///strSql.Append(" AND USRTBL.DEFAULT_LOCATION_FK = " & LoggedIn_Loc_FK) 'Commented Sivachandran Its not Locaction Based - Discussed with Magesh

				if (Status < 3) {
					if (SearchType == "C") {
						strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
					} else if (SearchType == "S") {
						strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
					}
				}

				if (VendorPK != 0 & SearchType == "C") {
					strSql.Append(" AND VMST.VENDOR_MST_PK = " + VendorPK);
				} else if (VendorPK != 0 & SearchType == "S") {
					strSql.Append(" AND VMST.VENDOR_MST_PK = " + VendorPK);
				}

				if (VendorInvNr.Trim().Length > 0 & SearchType == "C") {
					strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '%" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
				} else if (VendorInvNr.Trim().Length > 0 & SearchType == "S") {
					strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
				}
				if ((InvDt != null)) {
					strSql.Append(" AND INVTBL.INVOICE_DATE = TO_DATE('" + InvDt + "','" + dateFormat + "')");
				}

				if (SupplierRefNr.Trim().Length > 0 & SearchType == "C") {
					strSql.Append(" AND LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '%" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'");
				} else if (SupplierRefNr.Trim().Length > 0 & SearchType == "S") {
					strSql.Append(" AND LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'");
				}
				//strSql.Append(" AND INVTRNTBL.JOB_CARD_PIA_FK= JOB_TRN_PIA.JOB_TRN_" & BusinessProcess & "_PIA_PK")
				//strSql.Append(" AND JOB_TRN_PIA.JOB_CARD_" & BusinessProcess & "_FK=JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK")
				strSql.Append(" GROUP BY INVTBL.INVOICE_REF_NO  ,");
				strSql.Append("INVTBL.INVOICE_DATE,");
				strSql.Append(" VMST.VENDOR_NAME ,");
				strSql.Append("INVTRNTBL.ELEMENT_APPROVED,");
				strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
				strSql.Append("CURR.CURRENCY_ID,");
				strSql.Append("INVTBL.SUPPLIER_INV_NO");
			}
			//'Added By Koteshwari on 16/5/2011
			strSql.Append(" UNION ");
			strSql.Append(" SELECT distinct");
			strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
			strSql.Append("INVTBL.INVOICE_REF_NO VOUCHERNO,");
			strSql.Append("TO_DATE(INVTBL.INVOICE_DATE,dateformat) VOUCHERDATE,");
			strSql.Append("INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
			strSql.Append("VMST.VENDOR_NAME VENDOR,");
			strSql.Append("  CURR.CURRENCY_ID CUR,");
			//strSql.Append(" ROUND(SUM(INVTRNTBL.ACTUAL_AMT),2) AMOUNT,")
			//strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.TOTAL_COST,0)),2) AMOUNT,") ''COMMENTED BY ASHISH TO TALLY THE LISTING AND ENTRY INV AMOUNT
			strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.PAYABLE_AMT,0)),2) AMOUNT,");
			//strSql.Append(" ROUND(SUM( INVTRNTBL.TOTAL_COST * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," & BaseCurrFk & " ,INVTBL.INVOICE_DATE)),2) AMOUNT,")
			strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED,1,'Approved',2,'Reject', 0,'Pending') STATUS, ");
			strSql.Append(" 0 Sel ");
			strSql.Append(" FROM ");
			strSql.Append(" INV_SUPPLIER_TBL INVTBL,");
			strSql.Append(" INV_SUPPLIER_TRN_TBL INVTRNTBL,");
			strSql.Append(" VENDOR_MST_TBL VMST,");
			strSql.Append(" CURRENCY_TYPE_MST_TBL CURR,");
			strSql.Append(" MASTER_JC_" + BusinessProcess + "_TBL  MJOB_EXP,");
			strSql.Append(" USER_MST_TBL USRTBL");
			strSql.Append(" WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
			strSql.Append(" AND  VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
			strSql.Append(" AND INVTRNTBL.JOBCARD_REF_NO = MJOB_EXP.MASTER_JC_REF_NO ");
			strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
			strSql.Append(" AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK");
			strSql.Append(" AND INVTBL.BUSINESS_TYPE=" + Business_Type);
			strSql.Append(" AND INVTBL.PROCESS_TYPE=" + Process_Type);

			if (Status < 3) {
				if (SearchType == "C") {
					strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
				} else if (SearchType == "S") {
					strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
				}
			}
			if (VendorInvNr.Trim().Length > 0 & SearchType == "C") {
				strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '%" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
			} else if (VendorInvNr.Trim().Length > 0 & SearchType == "S") {
				strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
			}
			strSql.Append(strCondition1);
			strSql.Append(" GROUP BY INVTBL.INVOICE_REF_NO  ,");
			strSql.Append("INVTBL.INVOICE_DATE,");
			strSql.Append(" VMST.VENDOR_NAME ,");
			strSql.Append("INVTRNTBL.ELEMENT_APPROVED,");
			strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
			strSql.Append("CURR.CURRENCY_ID,");
			strSql.Append("INVTBL.SUPPLIER_INV_NO");
			//'END
			strSql.Append(")Q)");
			DataSet dscount = new DataSet();
			dscount = objWF.GetDataSet(strSql.ToString());
			if (dscount.Tables[0].Rows.Count > 0) {
				TotalRecords = Convert.ToInt32(dscount.Tables[0].Rows[0][0]);
			} else {
				TotalRecords = 0;
			}

			//TotalRecords = CType(objWF.ExecuteScaler(strSql.ToString), Int32)
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			strSql = new StringBuilder();
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;

			strSql.Append("SELECT * FROM(");
			strSql.Append("SELECT ROWNUM SrNO ,Q.*FROM(SELECT * FROM ( SELECT");
			strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
			strSql.Append("INVTBL.INVOICE_REF_NO VOUCHERNO,");
			strSql.Append("TO_DATE(INVTBL.INVOICE_DATE,'DD/MM/RRRR') VOUCHERDATE,");
			strSql.Append("INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
			strSql.Append("VMST.VENDOR_NAME VENDOR,");
			//strSql.Append("JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK,")
			//strSql.Append("JOB_EXP.JOBCARD_REF_NO JOBNO,")

			//If Business_Type = 2 Then
			//    strSql.Append("JOB_EXP.VESSEL_NAME ||" & "'-'" & " || JOB_EXP.VOYAGE VSL_FLIGHT,")
			//Else
			//    strSql.Append("JOB_EXP.FLIGHT_NO VSL_FLIGHT,")
			//End If
			strSql.Append("  CURR.CURRENCY_ID CUR,");
			//strSql.Append(" ROUND(SUM(INVTRNTBL.ACTUAL_AMT),2) AMOUNT,")

			//strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.TOTAL_COST,0)),2) AMOUNT,") ''COMMENTED BY ASHISH TO TALLY THE LISTING AND ENTRY INV AMOUNT
			strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.PAYABLE_AMT,0)),2) AMOUNT,");

			//strSql.Append(" ROUND(SUM(INVTRNTBL.TOTAL_COST * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," & BaseCurrFk & " ,INVTBL.INVOICE_DATE)),2) AMOUNT,")
			strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED ,1,'Approved',2,'Reject',0,'Pending') STATUS, ");
			strSql.Append(" 0 Sel ");
			strSql.Append(" FROM ");
			strSql.Append("INV_SUPPLIER_TBL INVTBL,");
			strSql.Append("INV_SUPPLIER_TRN_TBL INVTRNTBL,");
			strSql.Append("VENDOR_MST_TBL VMST,");
			strSql.Append("JOB_CARD_TRN  JOB_EXP,");
			///strSql.Append("JOB_TRN_" & BusinessProcess & "_PIA   JOB_TRN_PIA,")'COMMENTED BY SIVACHANDRAN FOR BUG 4875 TO FETCH SAME DATA IN da REPORT AND VOUCHER LISTING
			strSql.Append("CURRENCY_TYPE_MST_TBL CURR,");
			strSql.Append("USER_MST_TBL USRTBL");
			strSql.Append(" WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
			strSql.Append(" AND   VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
			///strSql.Append(" AND   INVTRNTBL.JOB_CARD_PIA_FK= JOB_TRN_PIA.JOB_TRN_" & BusinessProcess & "_PIA_PK")'COMMENTED BY SIVACHANDRAN FOR BUG 4875 TO FETCH SAME DATA IN da REPORT AND VOUCHER LISTING
			///strSql.Append(" AND   JOB_TRN_PIA.JOB_CARD_" & BusinessProcess & "_FK=JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK")
			strSql.Append(" AND INVTRNTBL.JOBCARD_REF_NO = JOB_EXP.JOBCARD_REF_NO ");
			//ADDED BY SIVACHANDRAN FOR BUG 4875 TO FETCH SAME DATA IN da REPORT AND VOUCHER LISTING
			strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
			//Manoharan 27Apr2009: To filter based on the Business and process type
			strSql.Append(" AND INVTBL.BUSINESS_TYPE=" + Business_Type);
			strSql.Append(" AND INVTBL.PROCESS_TYPE=" + Process_Type);
			//end
			if (Status < 3) {
				if (SearchType == "C") {
					strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
				} else if (SearchType == "S") {
					strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
				}
			}

			strSql.Append(" AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK");
			///strSql.Append(" AND USRTBL.DEFAULT_LOCATION_FK = " & LoggedIn_Loc_FK)'Commented Sivachandran Its not Locaction Based - Discussed with Magesh
			strSql.Append(strCondition);
			strSql.Append(" GROUP BY INVTBL.INVOICE_REF_NO  ,");
			strSql.Append("INVTBL.INVOICE_DATE,");
			strSql.Append(" VMST.VENDOR_NAME ,");
			//strSql.Append("JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK,")
			//strSql.Append("JOB_EXP.JOBCARD_REF_NO ,")
			//If Business_Type = 2 Then
			//    strSql.Append("JOB_EXP.VESSEL_NAME ||" & "'-'" & " || JOB_EXP.VOYAGE,")
			//Else
			//    strSql.Append("JOB_EXP.FLIGHT_NO,")
			//End If
			strSql.Append("INVTRNTBL.ELEMENT_APPROVED,");
			strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
			strSql.Append("CURR.CURRENCY_ID,");
			strSql.Append("INVTBL.SUPPLIER_INV_NO");

			if (JobPK == 0 & string.IsNullOrEmpty(Vsl) & (string.IsNullOrEmpty(FromDt) | FromDt == " ") & (string.IsNullOrEmpty(ToDt) | ToDt == " ")) {
				strSql.Append(" UNION");
				strSql.Append(" SELECT distinct INVTBL.INV_SUPPLIER_PK,");
				strSql.Append(" INVTBL.INVOICE_REF_NO VOUCHERNO,");
				strSql.Append(" TO_DATE(INVTBL.INVOICE_DATE, 'DD/MM/RRRR') VOUCHERDATE,");
				strSql.Append("INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
				strSql.Append(" VMST.VENDOR_NAME VENDOR,");
				//strSql.Append(" 0 JOB_CARD_SEA_EXP_PK,")
				//strSql.Append(" '' JOBNO,")
				//strSql.Append(" '' VSL_FLIGHT,")
				strSql.Append("  CURR.CURRENCY_ID CUR,");
				//strSql.Append(" ROUND(SUM(INVTRNTBL.ACTUAL_AMT),2) AMOUNT,")

				//strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.TOTAL_COST,0)),2) AMOUNT,") ''COMMENTED BY ASHISH TO TALLY THE LISTING AND ENTRY INV AMOUNT
				strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.PAYABLE_AMT,0)),2) AMOUNT,");

				//strSql.Append(" ROUND(SUM(INVTRNTBL.TOTAL_COST * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," & BaseCurrFk & " ,INVTBL.INVOICE_DATE)),2) AMOUNT,")
				strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED,");
				//modified by latha
				strSql.Append(" 1,'Approved', 2,'Reject',0,'Pending') STATUS, ");
				strSql.Append(" 0 Sel ");
				strSql.Append(" FROM INV_SUPPLIER_TBL     INVTBL,");
				strSql.Append(" INV_SUPPLIER_TRN_TBL INVTRNTBL,");
				strSql.Append(" VENDOR_MST_TBL VMST,");
				strSql.Append("CURRENCY_TYPE_MST_TBL CURR,");
				strSql.Append("USER_MST_TBL USRTBL");
				//strSql.Append("JOB_CARD_" & BusinessProcess & "_TBL  JOB_EXP,")
				//strSql.Append("JOB_TRN_" & BusinessProcess & "_PIA   JOB_TRN_PIA")
				strSql.Append(" WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
				strSql.Append(" AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
				//'Modified By Koteshwari on 6/5/2011
				//strSql.Append(" and invtrntbl.job_card_pia_fk=0")
				strSql.Append(" AND INVTRNTBL.JOB_TRN_EST_FK=0");
				//'End
				strSql.Append(" AND INVTBL.BUSINESS_TYPE=" + Business_Type);
				strSql.Append(" AND INVTBL.PROCESS_TYPE=" + Process_Type);
				//strSql.Append(" AND INVTRNTBL.JOB_CARD_PIA_FK= JOB_TRN_PIA.JOB_TRN_" & BusinessProcess & "_PIA_PK")
				//strSql.Append(" AND JOB_TRN_PIA.JOB_CARD_" & BusinessProcess & "_FK=JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK")
				strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
				strSql.Append(" AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK");
				///strSql.Append(" AND USRTBL.DEFAULT_LOCATION_FK = " & LoggedIn_Loc_FK)'Commented Sivachandran Its not Locaction Based - Discussed with Magesh
				if (BlankGrid == 0) {
					strSql.Append(" AND 1=2 ");
				}
				if (Status < 3) {
					if (SearchType == "C") {
						strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
					} else if (SearchType == "S") {
						strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
					}
				}

				if (VendorPK != 0 & SearchType == "C") {
					strSql.Append(" AND VMST.VENDOR_MST_PK = " + VendorPK);
				} else if (VendorPK != 0 & SearchType == "S") {
					strSql.Append(" AND VMST.VENDOR_MST_PK = " + VendorPK);
				}

				if (VendorInvNr.Trim().Length > 0 & SearchType == "C") {
					strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '%" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
				} else if (VendorInvNr.Trim().Length > 0 & SearchType == "S") {
					strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
				}

				if ((InvDt != null)) {
					strSql.Append(" AND INVTBL.INVOICE_DATE = TO_DATE('" + InvDt + "','" + dateFormat + "')");
				}

				if (SupplierRefNr.Trim().Length > 0 & SearchType == "C") {
					strSql.Append(" AND LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '%" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'");
				} else if (SupplierRefNr.Trim().Length > 0 & SearchType == "S") {
					strSql.Append(" AND LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'");
				}

				strSql.Append(" GROUP BY INVTBL.INVOICE_REF_NO  ,");
				strSql.Append("INVTBL.INVOICE_DATE,");
				strSql.Append(" VMST.VENDOR_NAME ,");
				strSql.Append("INVTRNTBL.ELEMENT_APPROVED,");
				strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
				strSql.Append("CURR.CURRENCY_ID,");
				strSql.Append("INVTBL.SUPPLIER_INV_NO");
			}
			//Added By Koteshwari on 16/5/2011
			strSql.Append(" UNION ");
			strSql.Append(" SELECT distinct");
			strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
			strSql.Append("INVTBL.INVOICE_REF_NO VOUCHERNO,");
			strSql.Append("TO_DATE(INVTBL.INVOICE_DATE,dateformat) VOUCHERDATE,");
			strSql.Append("INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
			strSql.Append("VMST.VENDOR_NAME VENDOR,");
			strSql.Append("  CURR.CURRENCY_ID CUR,");
			//strSql.Append(" ROUND(SUM(INVTRNTBL.ACTUAL_AMT),2) AMOUNT,")

			//strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.TOTAL_COST,0)),2) AMOUNT,") ''COMMENTED BY ASHISH TO TALLY THE LISTING AND ENTRY INV AMOUNT
			strSql.Append(" ROUND(SUM(NVL(INVTRNTBL.PAYABLE_AMT,0)),2) AMOUNT,");

			//strSql.Append(" ROUND(SUM(INVTRNTBL.TOTAL_COST * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," & BaseCurrFk & " ,INVTBL.INVOICE_DATE)),2) AMOUNT,")
			strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED,1,'Approved',2,'Reject', 0,'Pending') STATUS, ");
			strSql.Append(" 0 Sel ");
			strSql.Append(" FROM ");
			strSql.Append(" INV_SUPPLIER_TBL INVTBL,");
			strSql.Append(" INV_SUPPLIER_TRN_TBL INVTRNTBL,");
			strSql.Append(" VENDOR_MST_TBL VMST,");
			strSql.Append(" CURRENCY_TYPE_MST_TBL CURR,");
			strSql.Append(" MASTER_JC_" + BusinessProcess + "_TBL  MJOB_EXP,");
			strSql.Append(" USER_MST_TBL USRTBL");
			strSql.Append(" WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
			strSql.Append(" AND  VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
			strSql.Append(" AND INVTRNTBL.JOBCARD_REF_NO = MJOB_EXP.MASTER_JC_REF_NO ");
			strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
			strSql.Append(" AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK");
			strSql.Append(" AND INVTBL.BUSINESS_TYPE=" + Business_Type);
			strSql.Append(" AND INVTBL.PROCESS_TYPE=" + Process_Type);

			if (Status < 3) {
				if (SearchType == "C") {
					strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
				} else if (SearchType == "S") {
					strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
				}
			}

			strSql.Append(strCondition1);
			if (VendorInvNr.Trim().Length > 0 & SearchType == "C") {
				strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '%" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
			} else if (VendorInvNr.Trim().Length > 0 & SearchType == "S") {
				strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
			}
			strSql.Append(" GROUP BY INVTBL.INVOICE_REF_NO  ,");
			strSql.Append("INVTBL.INVOICE_DATE,");
			strSql.Append(" VMST.VENDOR_NAME ,");
			strSql.Append("INVTRNTBL.ELEMENT_APPROVED,");
			strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
			strSql.Append("CURR.CURRENCY_ID,");
			strSql.Append("INVTBL.SUPPLIER_INV_NO");
			//'End Koteshwari
			strSql.Append(")ORDER BY  VOUCHERDATE  DESC , VOUCHERNO  DESC)q)");
			strSql.Append("WHERE SrNO  Between " + start + " and " + last);
			string sql = null;
			sql = strSql.ToString();

			DataSet DS = null;
			try {
				DS = objWF.GetDataSet(sql);
				DataRelation CONTRel = null;
				DS.Tables.Add(Fetchchildlist(AllMasterPKs(DS), VendorPK, Status, Business_Type, Process_Type, FromDt, ToDt, JobPK, Vsl, VendorInvNr,
				InvDt, SearchType));
				CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["INV_SUPPLIER_PK"], DS.Tables[1].Columns["INV_SUPPLIER_PK"], true);
				CONTRel.Nested = true;
				DS.Relations.Add(CONTRel);
				return DS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region " All Master Supplier PKs "

		private string AllMasterPKs(DataSet ds)
		{
			try {
				Int16 RowCnt = default(Int16);
				Int16 ln = default(Int16);
				System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
				strBuilder.Append("-1,");
				for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++) {
					strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["INV_SUPPLIER_PK"]).Trim() + ",");
				}
				strBuilder.Remove(strBuilder.Length - 1, 1);
				return strBuilder.ToString();
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}

		}

		#endregion

		#region "Child Table"

		private DataTable Fetchchildlist(string SUPPLIERPKs = "", Int32 VendorPK = 0, Int16 Status = 0, Int16 Business_Type = 0, Int16 Process_Type = 0, string FromDt = "", string ToDt = "", int JobPK = 0, string Vsl = "", string VendorInvNr = "",
		string InvDt = "", string SearchType = "")
		{

			StringBuilder strSql = new StringBuilder();
			string strCondition = null;
			DataTable dt = null;
			WorkFlow objWF = new WorkFlow();
			int RowCnt = 0;
			int Rno = 0;
			int pk = 0;
			string BusinessProcess = null;
			string VslFlight = null;
			int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
			if (Business_Type == 2) {
				BusinessProcess = "SEA";
				VslFlight = "VESSEL_NAME";
			} else {
				BusinessProcess = "AIR";
				VslFlight = "FLIGHT_NO";
			}
			if (Process_Type == 1) {
				BusinessProcess += "_EXP";
			} else {
				BusinessProcess += "_IMP";
			}

			if (((FromDt != null) & (ToDt != null)) & FromDt != " " & ToDt != " ") {
				strCondition += " AND TO_DATE(JOB_EXP.JOBCARD_DATE " + ",'" + dateFormat + "') BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
			} else if (((FromDt != null)) & FromDt != " ") {
				strCondition += " AND TO_DATE(JOB_EXP.JOBCARD_DATE " + ",'" + dateFormat + "') >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
			} else if (((ToDt != null)) & ToDt != " ") {
				strCondition += " AND TO_DATE(JOB_EXP.JOBCARD_DATE " + ",'" + dateFormat + "') <= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
			}

			//If Not IsNothing(FromDt) And Not IsNothing(ToDt) Then
			//    strCondition &= vbCrLf & " AND JOB_EXP.JOBCARD_DATE BETWEEN TO_DATE('" & FromDt & "','" & dateFormat & "')  AND TO_DATE('" & ToDt & "','" & dateFormat & "')  "
			//ElseIf Not IsNothing(FromDt) Then
			//    strCondition &= vbCrLf & " AND JOB_EXP.JOBCARD_DATE >= TO_DATE('" & FromDt & "','" & dateFormat & "') "
			//ElseIf Not IsNothing(ToDt) Then
			//    strCondition &= vbCrLf & " AND JOB_EXP.JOBCARD_DATE <= TO_DATE('" & ToDt & "','" & dateFormat & "') "
			//End If

			if (VendorPK != 0 & SearchType == "C") {
				strCondition += " AND VMST.VENDOR_MST_PK =" + VendorPK;
			} else if (VendorPK != 0 & SearchType == "S") {
				strCondition += " AND VMST.VENDOR_MST_PK =" + VendorPK;
			}

			if (!string.IsNullOrEmpty(Vsl) & SearchType == "C") {
				if (Business_Type == 2) {
					strCondition += " AND JOB_EXP.VESSEL_NAME ||" + "'-'" + " || JOB_EXP.VOYAGE_FLIGHT_NO like '%" + Vsl.Trim() + "%'";
				} else {
					strCondition += " AND JOB_EXP.VOYAGE_FLIGHT_NO like '%" + Vsl.Trim() + "%'";
				}
			} else if (!string.IsNullOrEmpty(Vsl) & SearchType == "S") {
				if (Business_Type == 2) {
					strCondition += " AND JOB_EXP.VESSEL_NAME ||" + "'-'" + " || JOB_EXP.VOYAGE_FLIGHT_NO like '" + Vsl.Trim() + "%'";
				} else {
					strCondition += " AND JOB_EXP.VOYAGE_FLIGHT_NO like '" + Vsl.Trim() + "%'";
				}
			}

			if (VendorInvNr.Trim().Length > 0 & SearchType == "C") {
				strCondition += " AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '%" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'";
			} else if (VendorInvNr.Trim().Length > 0 & SearchType == "S") {
				strCondition += " AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'";
			}

			if (JobPK != 0 & SearchType == "C") {
				strCondition += " AND JOB_EXP.JOB_CARD_TRN_PK =" + JobPK;
			} else if (JobPK != 0 & SearchType == "S") {
				strCondition += " AND JOB_EXP.JOB_CARD_TRN_PK =" + JobPK;
			}

			strCondition += " AND JOB_EXP.BUSINESS_TYPE = " + Business_Type;
			strCondition += " AND JOB_EXP.PROCESS_TYPE = " + Process_Type;

			if ((InvDt != null)) {
				strCondition += " AND INVTBL.INVOICE_DATE = TO_DATE('" + InvDt + "','" + dateFormat + "')";
			}
			strSql.Append("select * from(");
			strSql.Append("SELECT ROWNUM SrNO ,Q.*FROM ( SELECT");
			strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
			//strSql.Append("INVTBL.INVOICE_REF_NO VOUCHERNO,")
			//strSql.Append("TO_DATE(INVTBL.INVOICE_DATE,dateformat) VOUCHERDATE,")
			//strSql.Append("VMST.VENDOR_ID VENDOR,")
			strSql.Append("JOB_EXP.JOB_CARD_TRN_PK,");
			//strSql.Append("JOB_EXP.JOB_CARD_SEA_EXP_PK,")
			strSql.Append("JOB_EXP.JOBCARD_REF_NO JOBNO,");

			if (Business_Type == 2) {
				strSql.Append("JOB_EXP.VESSEL_NAME ||" + "'-'" + " || JOB_EXP.VOYAGE_FLIGHT_NO VSL_FLIGHT,");
			} else {
				strSql.Append("JOB_EXP.VOYAGE_FLIGHT_NO VSL_FLIGHT,");
			}
			strSql.Append("  CURR.CURRENCY_ID CUR,");
			//strSql.Append(" SUM(INVTRNTBL.ACTUAL_AMT) AMOUNT ")

			//strSql.Append(" SUM(NVL(INVTRNTBL.TOTAL_COST,0)) AMOUNT ") ''COMMENTED BY ASHISH TO TALLY THE LISTING AND ENTRY INV AMOUNT
			strSql.Append(" SUM(NVL(INVTRNTBL.PAYABLE_AMT,0)) AMOUNT ");

			//strSql.Append(" SUM(INVTRNTBL.TOTAL_COST * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," & BaseCurrFk & " ,INVTBL.INVOICE_DATE)) AMOUNT")
			//strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED ,1,'Approved',2,'Reject',0,'') STATUS")
			strSql.Append(" FROM ");
			strSql.Append("INV_SUPPLIER_TBL INVTBL,");
			strSql.Append("INV_SUPPLIER_TRN_TBL INVTRNTBL,");
			strSql.Append("VENDOR_MST_TBL VMST,");
			strSql.Append("JOB_CARD_TRN  JOB_EXP,");
			//strSql.Append("JOB_CARD_SEA_EXP_TBL  JOB_EXP,")
			//Modified By Koteshwari on 6/5/2011
			//strSql.Append("JOB_TRN_" & BusinessProcess & "_PIA   JOB_TRN_PIA,")
			strSql.Append(" JOB_TRN_COST      JOB_COST,");
			//End
			strSql.Append("CURRENCY_TYPE_MST_TBL CURR");
			// strSql.Append("JOB_TRN_SEA_EXP_PIA   JOB_TRN_PIA")
			strSql.Append(" WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
			strSql.Append(" AND   VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
			//'Modified By Koteshwari on 6/5/2011
			//strSql.Append(" AND   INVTRNTBL.JOB_CARD_PIA_FK= JOB_TRN_PIA.JOB_TRN_" & BusinessProcess & "_PIA_PK")
			strSql.Append(" AND   INVTRNTBL.JOB_TRN_EST_FK= JOB_COST.JOB_TRN_COST_PK");
			//strSql.Append(" AND   JOB_TRN_PIA.JOB_CARD_" & BusinessProcess & "_FK=JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK")
			strSql.Append(" AND   JOB_COST.JOB_CARD_TRN_FK=JOB_EXP.JOB_CARD_TRN_PK");
			strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
			if (Status < 3 & SearchType == "C") {
				strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
			} else if (Status < 3 & SearchType == "S") {
				strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
			}

			if (SUPPLIERPKs.Trim().Length > 0 & SearchType == "C") {
				strSql.Append(" AND INVTBL.INV_SUPPLIER_PK  in (" + SUPPLIERPKs + ") ");
			} else if (SUPPLIERPKs.Trim().Length > 0 & SearchType == "S") {
				strSql.Append(" AND INVTBL.INV_SUPPLIER_PK  in (" + SUPPLIERPKs + ") ");
			}

			strSql.Append(strCondition);
			strSql.Append(" GROUP BY JOB_EXP.JOB_CARD_TRN_PK,");
			//strSql.Append("INVTBL.INVOICE_DATE,")
			//strSql.Append(" VMST.VENDOR_ID ,")
			//strSql.Append("JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK,")
			strSql.Append("JOB_EXP.JOBCARD_REF_NO ,");
			if (Business_Type == 2) {
				strSql.Append("JOB_EXP.VESSEL_NAME ||" + "'-'" + " || JOB_EXP.VOYAGE_FLIGHT_NO,");
			} else {
				strSql.Append("JOB_EXP.VOYAGE_FLIGHT_NO,");
			}
			//strSql.Append("INVTRNTBL.ELEMENT_APPROVED,")
			strSql.Append(" INVTBL.INV_SUPPLIER_PK,");
			strSql.Append("CURR.CURRENCY_ID");

			strSql.Append(" UNION");

			strSql.Append(" SELECT distinct INVTBL.INV_SUPPLIER_PK,");
			//strSql.Append(" INVTBL.INVOICE_REF_NO VOUCHERNO,")
			//strSql.Append(" TO_DATE(INVTBL.INVOICE_DATE, dateformat) VOUCHERDATE,")
			//strSql.Append(" VMST.VENDOR_ID VENDOR,")
			strSql.Append(" 0 JOB_CARD_SEA_EXP_PK,");
			strSql.Append(" '' JOBNO,");
			strSql.Append(" '' VSL_FLIGHT,");
			strSql.Append("  CURR.CURRENCY_ID CUR,");
			//strSql.Append(" SUM(INVTRNTBL.ACTUAL_AMT) AMOUNT ")

			//strSql.Append(" SUM(NVL(INVTRNTBL.TOTAL_COST,0)) AMOUNT ") ''COMMENTED BY ASHISH TO TALLY THE LISTING AND ENTRY INV AMOUNT
			strSql.Append(" SUM(NVL(INVTRNTBL.PAYABLE_AMT,0)) AMOUNT ");

			//strSql.Append(" SUM(INVTRNTBL.TOTAL_COST * GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," & BaseCurrFk & " ,INVTBL.INVOICE_DATE)) AMOUNT")
			//strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED,")
			//strSql.Append(" 1,'Approved', 2,'Reject', 0, '') STATUS")
			strSql.Append(" FROM INV_SUPPLIER_TBL     INVTBL,");
			strSql.Append(" INV_SUPPLIER_TRN_TBL INVTRNTBL,");
			strSql.Append(" VENDOR_MST_TBL VMST,");
			strSql.Append(" CURRENCY_TYPE_MST_TBL CURR");
			strSql.Append(" WHERE INVTBL.INV_SUPPLIER_PK = INVTRNTBL.INV_SUPPLIER_TBL_FK");
			strSql.Append(" AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
			strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");

			if (Status < 3 & SearchType == "C") {
				strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
			} else if (Status < 3 & SearchType == "S") {
				strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
			}
			//'Modified By Koteshwari on 6/5/2011
			//strSql.Append(" and invtrntbl.job_card_pia_fk=0")
			strSql.Append(" AND INVTRNTBL.JOB_TRN_EST_FK=0");
			//End
			strSql.Append(" AND INVTBL.BUSINESS_TYPE=" + Business_Type);
			strSql.Append(" AND INVTBL.PROCESS_TYPE=" + Process_Type);

			if (SUPPLIERPKs.Trim().Length > 0 | SUPPLIERPKs != "-1" & SearchType == "C") {
				strSql.Append(" AND INVTBL.INV_SUPPLIER_PK  in (" + SUPPLIERPKs + ") ");
			} else if (SUPPLIERPKs.Trim().Length > 0 | SUPPLIERPKs != "-1" & SearchType == "S") {
				strSql.Append(" AND INVTBL.INV_SUPPLIER_PK  in (" + SUPPLIERPKs + ") ");
			}

			strSql.Append(" GROUP BY INVTBL.INV_SUPPLIER_PK,");
			strSql.Append(" CURR.CURRENCY_ID");
			strSql.Append(" ORDER BY INV_SUPPLIER_PK DESC, JOBNO DESC)Q)");
			string sql = null;
			sql = strSql.ToString();
			try {
				pk = -1;
				dt = objWF.GetDataTable(sql);
				for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++) {
					if (Convert.ToInt32(dt.Rows[RowCnt]["INV_SUPPLIER_PK"]) != pk) {
						pk = Convert.ToInt32(dt.Rows[RowCnt]["INV_SUPPLIER_PK"]);
						Rno = 0;
					}
					Rno += 1;
					dt.Rows[RowCnt]["SrNO"] = Rno;
				}
				return dt;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Fetch JobCardListing"
		public DataSet FetchExistingInvoice(string InvPK, Int16 Business_Type, Int16 Process_Type)
		{
			StringBuilder strSql = new StringBuilder();

			string BusinessProcess = null;
			string VslFlight = null;
			if (Business_Type == 2) {
				BusinessProcess = "SEA";
				VslFlight = "VESSEL_NAME";
			} else {
				BusinessProcess = "AIR";
				VslFlight = "FLIGHT_NO";
			}
			if (Process_Type == 1) {
				BusinessProcess += "_EXP";
			} else {
				BusinessProcess += "_IMP";
			}

			strSql.Append("SELECT DISTINCT INVS.INV_SUPPLIER_PK,");
			strSql.Append("INVS.INVOICE_REF_NO,");
			strSql.Append("INVS.INVOICE_DATE,");
			strSql.Append("INVS.SUPPLIER_INV_NO,");
			strSql.Append("INVS.SUPPLIER_INV_DT,");
			strSql.Append("INVS.SUPPLIER_DUE_DT,");
			//Added By Prakash Chandra on 22/05/2008
			strSql.Append("INVS.INVOICE_AMT,");
			strSql.Append("INVS.REMARKS,");
			strSql.Append("VMST.VENDOR_MST_PK,");
			strSql.Append("VMST.VENDOR_ID,");
			strSql.Append("VMST.VENDOR_NAME,");
			strSql.Append("CURR.CURRENCY_MST_PK,");
			strSql.Append("CURR.CURRENCY_ID,");
			strSql.Append("CURR.CURRENCY_NAME,");
			//'Added by Koteshwari on 16/5/2011
			strSql.Append("INVTRN.FLAG,");
			//'End
			//If Business_Type = 2 Then
			//    strSql.Append("JOBESEA.VESSEL_NAME ||" & "'-'" & " || JOBESEA.VOYAGE VSL_FLIGHT,")
			//Else
			//    strSql.Append("JOBESEA.FLIGHT_NO VSL_FLIGHT,")
			//End If
			strSql.Append("INVS.INTERNAL_REF,INVS.VERSION_NO,INVS.APPROVED,INVTRN.ELEMENT_APPROVED, ");

			strSql.Append("   UMTCRT.USER_NAME    AS CREATED_BY, ");
			strSql.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
			strSql.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
			strSql.Append("   TO_DATE(INVS.CREATED_DT) CREATED_BY_DT, ");
			strSql.Append("   TO_DATE(INVS.LAST_MODIFIED_DT) LAST_MODIFIEDBY_DT, ");
			strSql.Append("   TO_DATE(INVS.APPROVED_DATE) APPROVED_DT ");
			strSql.Append(" FROM INV_SUPPLIER_TBL INVS,");
			strSql.Append("INV_SUPPLIER_TRN_TBL INVTRN,");
			strSql.Append(" VENDOR_MST_TBL VMST,");
			strSql.Append(" CURRENCY_TYPE_MST_TBL CURR,");
			strSql.Append("  USER_MST_TBL UMTCRT, ");
			strSql.Append("  USER_MST_TBL UMTUPD, ");
			strSql.Append("  USER_MST_TBL UMTAPP ");
			// strSql.Append(" JOB_TRN_" & BusinessProcess & "_PIA JOB_PIA,")
			// strSql.Append(" JOB_CARD_" & BusinessProcess & "_TBL JOBESEA")
			strSql.Append(" WHERE INVS.INV_SUPPLIER_PK =" + InvPK);
			strSql.Append(" AND   INVS.INV_SUPPLIER_PK=INVTRN.INV_SUPPLIER_TBL_FK");
			strSql.Append(" AND   INVS.VENDOR_MST_FK =VMST.VENDOR_MST_PK");
			strSql.Append(" AND   CURR.CURRENCY_MST_PK=INVS.CURRENCY_MST_FK");
			strSql.Append(" AND UMTCRT.USER_MST_PK(+) = INVS.CREATED_BY_FK ");
			strSql.Append(" AND UMTUPD.USER_MST_PK(+) = INVS.LAST_MODIFIED_BY_FK  ");
			strSql.Append(" AND UMTAPP.USER_MST_PK(+) = INVS.APPROVED_BY_FK  ");
			// strSql.Append(" AND   JOB_PIA.JOB_TRN_" & BusinessProcess & "_PIA_PK=INVTRN.JOB_CARD_PIA_FK")
			// strSql.Append(" AND   JOB_PIA.JOB_CARD_" & BusinessProcess & "_FK=JOBESEA.JOB_CARD_" & BusinessProcess & "_PK")


			try {
				WorkFlow objWF = new WorkFlow();
				return objWF.GetDataSet(strSql.ToString());
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}

		}

		#endregion

		#region "FetchVoucher"
		//added by jayasimha 2/7/11
		public DataSet fetchvoucher(long pk)
		{
			string strSQL = null;
			strSQL = "SELECT COUNT(*)";
			strSQL += " From DOCUMENT_PREF_LOC_MST_TBL D, DOCUMENT_PREFERENCE_MST_TBL DP ";
			strSQL += "WHERE " ;
			strSQL += " D.LOCATION_MST_FK = " + pk + " " ;
			strSQL += "AND D.DOC_PREFERENCE_FK = DP.DOCUMENT_PREFERENCE_MST_PK" ;
			strSQL += "AND  DP.DOCUMENT_PREFERENCE_NAME='Voucher'";

			WorkFlow objWF = new WorkFlow();
			DataSet objDS = null;
			try {
				objDS = objWF.GetDataSet(strSQL);
				return objDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion

		#region "GetCorpCurrency"
		public DataSet GetCorpCurrency()
		{
			string strSQL = null;
			strSQL = "SELECT CMT.CURRENCY_MST_FK,CUMT.CURRENCY_ID,CUMT.CURRENCY_NAME FROM CORPORATE_MST_TBL CMT,CURRENCY_TYPE_MST_TBL CUMT";
			strSQL += "WHERE CMT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
			try {
				DataSet DS = null;
				DS = (new WorkFlow()).GetDataSet(strSQL);
				return DS;
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}

		}
		#endregion

		#region "Enhance Search "
		public string FetchCostElement(string strCond)
		{

			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strLOC_MST_IN = null;
			string strSERACH_IN = null;
			string strBizType = null;
			string strReq = null;
			var strNull = "";
			arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
			strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));

            try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_COST_ELEMENT_PKG.GET_COST_ID";

				var _with16 = selectCommand.Parameters;
				_with16.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
				_with16.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with16.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
				_with16.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}

		// Written By : -> Amit Singh
		// Date       : -> 28-May-07
		// To fetch the CostElement ID Based in Supplier

		public string FetchCostElementID(string strCond)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strLOC_MST_IN = null;
			string strSERACH_IN = null;
			string strBizType = null;
			string strVendorPK = null;
			string strReq = null;
			var strNull = "";
			arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            strVendorPK = Convert.ToString(arr.GetValue(3));

            try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_COST_ELEMENT_PKG.GET_COSTELEMENT_ID";

				var _with17 = selectCommand.Parameters;
				_with17.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
				_with17.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with17.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
				_with17.Add("VENDOR_MST_FK_IN", strVendorPK).Direction = ParameterDirection.Input;
				_with17.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}
		#endregion

		#region "DA Voucher Report"
		public DataSet getData(Int16 biztype, Int16 process, string costpk, string vendorpk, string locpk, string fromdt, string todt, string dueby, Int32 CurrentPage, Int32 TotalPage,
		bool Pending, string Vessel_Name = "", string Voyage_Nr = "", string Flight_Nr = "")
		{

			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sbCBJC = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sbTPN = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sbfrom = new System.Text.StringBuilder(5000);
			System.Text.StringBuilder sbSql = new System.Text.StringBuilder(5000);
			Int64 TotalRec = default(Int64);
			WorkFlow objWF = new WorkFlow();
			Int32 last = default(Int32);
			Int32 start = default(Int32);
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);

			sb.Append(" select ");
			sb.Append(" loc.location_mst_pk \"LocPk\",");
			sb.Append(" loc.location_name \"Location\",");
			sb.Append(" vou.vendor_mst_fk \"Vendor_Pk\",");
			sb.Append(" vendor.vendor_name \"Vendor_Name\",");

			sb.Append(" jcse.job_card_trn_pk \"JC_Pk\",");

			sb.Append(" jcse.jobcard_ref_no \"JC_Nr\",");
			sb.Append(" to_char(jcse.jobcard_date,'DD/MM/YYYY') \"JC_date\",");
			sb.Append(" vou.inv_supplier_pk \"Voucher_Pk\",");
			sb.Append(" vou.invoice_ref_no \"Voucher_Nr\",");
			sb.Append(" to_char(vou.invoice_date,'DD/MM/YYYY') \"Voucher_Date\",");
			sb.Append(" voutrn.cost_element_mst_fk \"Cost_Pk\",");
			sb.Append(" cem.COST_ELEMENT_ID \"Cost_Id\",");
			sb.Append(" (NVL(voutrn.actual_amt,0) + nvl(voutrn.tax_amount,0))* ");
			sb.Append("   GET_EX_RATE_BUY(vou.CURRENCY_MST_FK," + BaseCurrFk + " ,vou.SUPPLIER_INV_DT) \"Actual_Amt\",");
			sb.Append(" NVL(voutrn.estimated_amt,0)*GET_EX_RATE_BUY(vou.CURRENCY_MST_FK," + BaseCurrFk + " ,vou.SUPPLIER_INV_DT) \"Est_Amt\",");

			sb.Append(" ((NVL(voutrn.actual_amt,0) + nvl(voutrn.tax_amount, 0)) - NVL(voutrn.estimated_amt,0)) *");
			sb.Append("   GET_EX_RATE_BUY(vou.CURRENCY_MST_FK," + BaseCurrFk + " ,vou.SUPPLIER_INV_DT) \"Diff\",");
			sb.Append(" to_char(vou.supplier_due_dt,'DD/MM/YYYY') \"Due_On_Date\",");

			sb.Append("  (CASE");
			sb.Append("                         WHEN (NVL(B.VOUCHER_AMT,0)) =");
			sb.Append("                              NVL(A.PAID_AMOUNT_HDR_CURR, 0) THEN");
			sb.Append("                          'Full'");
			sb.Append("                         WHEN (NVL(B.VOUCHER_AMT,0) -");
			sb.Append("                              NVL(A.PAID_AMOUNT_HDR_CURR, 0)) =");
			sb.Append("                              (NVL(B.VOUCHER_AMT,0)) THEN");
			sb.Append("                          'Pending'");
			sb.Append("                         WHEN (NVL(B.VOUCHER_AMT,0) >");
			sb.Append("                              NVL(A.PAID_AMOUNT_HDR_CURR, 0)) THEN");
			sb.Append("                          'Part'");
			sb.Append("                       ");
			sb.Append("                       END) \"PAYMENT\",");
			sb.Append("  NVL(A.PAID_AMOUNT_HDR_CURR, 0) \"PAID_AMT\",");
			sb.Append("   NVL(B.VOUCHER_AMT,0) \"VOUCH_AMT\",");
			sb.Append("  vou.business_type \"BizType\",");
			sb.Append("  vou.process_type  \"ProcessType\",");
			//Sea
			if (biztype == 2) {
				//Export
				if (process == 1) {
					sb.Append("  BKNG.CARGO_TYPE \"CARGOTYPE\" ");
				//Import
				} else {
					sb.Append(" jcse.CARGO_TYPE \"CARGOTYPE\" ");
				}
			//Air
			} else if (Convert.ToString(biztype) == "1") {
				sb.Append("  0 \"CargoType\" ");
			}


			sbfrom.Append(" from");
			sbfrom.Append(" inv_supplier_tbl vou,");
			sbfrom.Append(" inv_supplier_trn_tbl voutrn,");

			//Export
			if (process == 1) {
				sbfrom.Append(" job_card_trn jcse,");
				sbfrom.Append(" BOOKING_MST_TBL  BKNG,");
			//Import
			} else {
				sbfrom.Append(" job_card_trn jcse,");
			}

			sbfrom.Append(" vendor_mst_tbl vendor,");
			sbfrom.Append(" cost_element_mst_tbl cem,");
			sbfrom.Append(" user_mst_tbl usr,");
			sbfrom.Append(" location_mst_tbl loc, ");
			sbfrom.Append(" (select sum(NVL(p.paid_amount_hdr_curr,0)*GET_EX_RATE_BUY(PT.CURRENCY_MST_FK," + BaseCurrFk + " ,PT.PAYMENT_DATE)) paid_amount_hdr_curr, ");
			sbfrom.Append("    p.inv_supplier_tbl_fk from PAYMENTS_TBL PT,payment_trn_tbl p WHERE PT.PAYMENT_TBL_PK=P.PAYMENTS_TBL_FK ");
			sbfrom.Append("  group by inv_supplier_tbl_fk) A, ");
			//Included by sivachandran to get Payment Status
			sbfrom.Append("  (SELECT IST.INV_SUPPLIER_TBL_FK, SUM((NVL(IST.ACTUAL_AMT,0) + NVL(IST.TAX_AMOUNT, 0))* ");
			sbfrom.Append("   GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + BaseCurrFk + " ,INVTBL.SUPPLIER_INV_DT)) VOUCHER_AMT ");
			sbfrom.Append("                          FROM INV_SUPPLIER_TBL INVTBL,INV_SUPPLIER_TRN_TBL IST ");
			sbfrom.Append("                     WHERE INVTBL.INV_SUPPLIER_PK=IST.INV_SUPPLIER_TBL_FK ");
			sbfrom.Append("                         GROUP BY IST.INV_SUPPLIER_TBL_FK) B");
			sbfrom.Append(" where 1 = 1");
			if (process == 1) {
				sbfrom.Append("   AND jcse.booking_mst_fk = BKNG.BOOKING_MST_PK");
			}
			sbfrom.Append(" and vou.inv_supplier_pk = voutrn.inv_supplier_tbl_fk");
			sbfrom.Append(" and vou.vendor_mst_fk = vendor.vendor_mst_pk");
			sbfrom.Append(" and voutrn.jobcard_ref_no = jcse.jobcard_ref_no");
			sbfrom.Append(" and voutrn.cost_element_mst_fk = cem.cost_element_mst_pk");
			sbfrom.Append(" and A.inv_supplier_tbl_fk(+)= VOUTRN.INV_SUPPLIER_TBL_FK  ");
			sbfrom.Append("  AND B.INV_SUPPLIER_TBL_FK(+) = VOU.INV_SUPPLIER_PK ");
			sbfrom.Append("");
			if (!string.IsNullOrEmpty(vendorpk)) {
				if (Convert.ToInt32(vendorpk)> 0) {
					sbfrom.Append(" and vou.vendor_mst_fk in (" + vendorpk + ")");
				}
			}
			if (Convert.ToInt32(costpk) != 0) {
				sbfrom.Append(" and voutrn.cost_element_mst_fk = " + costpk);
				//<costpk>")
			}
			if (biztype != 3) {
				sbfrom.Append(" and vou.business_type = " + biztype);
				//2 --<businesstype> --Sea")
			}
			sbfrom.Append(" and vou.process_type = " + process);
			//1 --<processtype> --Export")
			sbfrom.Append(" and vou.created_by_fk = usr.user_mst_pk");
			sbfrom.Append(" and usr.default_location_fk = loc.location_mst_pk");
			if (Convert.ToInt32(locpk) != 0) {
				sbfrom.Append(" and usr.default_location_fk in (" + locpk + ")");
				//3--<locpk>")
			}
			//sb.Append("")
			if (!string.IsNullOrEmpty(dueby.Trim())) {
				sbfrom.Append(" and to_date(vou.supplier_due_dt,'DD/MM/YYYY') = to_date('" + dueby + "','DD/MM/YYYY')");
				//<duedate>")
			}
			if (((fromdt != null) & (todt != null)) & !string.IsNullOrEmpty(fromdt.Trim()) & !string.IsNullOrEmpty(todt.Trim())) {
				sbfrom.Append(" and vou.invoice_date between to_date('" + fromdt + "','DD/MM/YYYY')");
				sbfrom.Append(" AND TO_DATE(NVL('" + todt + "','1/1/9999'),'DD/MM/YYYY')");
			} else if (((fromdt != null)) & !string.IsNullOrEmpty(fromdt)) {
				sbfrom.Append(" and vou.invoice_date >= to_date('" + fromdt + "','DD/MM/YYYY')");
			} else if (((todt != null)) & !string.IsNullOrEmpty(todt)) {
				sbfrom.Append(" and vou.invoice_date <= to_date('" + todt + "','DD/MM/YYYY')");
			}
			if (biztype == 2) {
				if (!string.IsNullOrEmpty(Vessel_Name))
					sbfrom.Append(" AND UPPER(JCSE.VESSEL_NAME) LIKE '%" + Vessel_Name.ToUpper() + "%'");
				if (!string.IsNullOrEmpty(Voyage_Nr))
					sbfrom.Append(" AND UPPER(JCSE.VOYAGE_FLIGHT_NO) LIKE '%" + Voyage_Nr.ToUpper() + "%'");
			} else if (biztype == 1) {
				if (!string.IsNullOrEmpty(Flight_Nr))
					sbfrom.Append(" AND UPPER(JCSE.VOYAGE_FLIGHT_NO) LIKE '%" + Flight_Nr.ToUpper() + "%'");
			}

			//'Union for CBJC and Transport Note
			sbCBJC.Append(" UNION ");
			sbCBJC.Append(" SELECT LMT.LOCATION_MST_PK, ");
			sbCBJC.Append(" LMT.LOCATION_NAME, ");
			sbCBJC.Append(" VENDOR.VENDOR_MST_PK, ");
			sbCBJC.Append(" VENDOR.VENDOR_NAME, ");
			sbCBJC.Append(" CBJC.CBJC_PK,");
			sbCBJC.Append(" CBJC.CBJC_NO, ");
			sbCBJC.Append(" TO_CHAR(CBJC.CBJC_DATE,DATEFORMAT) CBJC_DATE,");
			sbCBJC.Append(" VOU.INV_SUPPLIER_PK,");
			sbCBJC.Append(" VOU.INVOICE_REF_NO,");
			sbCBJC.Append(" TO_CHAR(VOU.INVOICE_DATE, dateformat) VOUCHER_DATE, ");
			sbCBJC.Append(" CEMT.COST_ELEMENT_MST_PK, ");
			sbCBJC.Append(" CEMT.COST_ELEMENT_ID, ");
			sbCBJC.Append(" (NVL(voutrn.actual_amt,0) + nvl(voutrn.tax_amount,0))* ");
			sbCBJC.Append("   GET_EX_RATE_BUY(vou.CURRENCY_MST_FK," + BaseCurrFk + " ,vou.SUPPLIER_INV_DT) \"Actual_Amt\",");
			sbCBJC.Append(" NVL(voutrn.estimated_amt,0)*GET_EX_RATE_BUY(vou.CURRENCY_MST_FK," + BaseCurrFk + " ,vou.SUPPLIER_INV_DT) \"Est_Amt\",");

			sbCBJC.Append(" ((NVL(voutrn.actual_amt,0) + nvl(voutrn.tax_amount, 0)) - NVL(voutrn.estimated_amt,0)) *");
			sbCBJC.Append("   GET_EX_RATE_BUY(vou.CURRENCY_MST_FK," + BaseCurrFk + " ,vou.SUPPLIER_INV_DT) \"Diff\",");
			sbCBJC.Append(" to_char(vou.supplier_due_dt,'DD/MM/YYYY') \"Due_On_Date\",");

			sbCBJC.Append("  (CASE");
			sbCBJC.Append("                         WHEN (NVL(B.VOUCHER_AMT,0)) =");
			sbCBJC.Append("                              NVL(A.PAID_AMOUNT_HDR_CURR, 0) THEN");
			sbCBJC.Append("                          'Full'");
			sbCBJC.Append("                         WHEN (NVL(B.VOUCHER_AMT,0) -");
			sbCBJC.Append("                              NVL(A.PAID_AMOUNT_HDR_CURR, 0)) =");
			sbCBJC.Append("                              (NVL(B.VOUCHER_AMT,0)) THEN");
			sbCBJC.Append("                          'Pending'");
			sbCBJC.Append("                         WHEN (NVL(B.VOUCHER_AMT,0) >");
			sbCBJC.Append("                              NVL(A.PAID_AMOUNT_HDR_CURR, 0)) THEN");
			sbCBJC.Append("                          'Part'");
			sbCBJC.Append("                       ");
			sbCBJC.Append("                       END) \"PAYMENT\",");
			sbCBJC.Append("  NVL(A.PAID_AMOUNT_HDR_CURR, 0) \"PAID_AMT\",");
			sbCBJC.Append("   NVL(B.VOUCHER_AMT,0) \"VOUCH_AMT\",");
			sbCBJC.Append("  vou.business_type \"BizType\",");
			sbCBJC.Append("  vou.process_type  \"ProcessType\",");
			sbCBJC.Append("  CBJC.CARGO_TYPE ");
			sbCBJC.Append("  FROM CBJC_TBL CBJC,");
			sbCBJC.Append("  USER_MST_TBL UMT,");
			sbCBJC.Append("  LOCATION_MST_TBL LMT,");
			sbCBJC.Append("  VENDOR_MST_TBL VENDOR, ");
			sbCBJC.Append("  INV_SUPPLIER_TBL VOU,");
			sbCBJC.Append("  INV_SUPPLIER_TRN_TBL VOUTRN,");
			sbCBJC.Append("  COST_ELEMENT_MST_TBL CEMT,");
			sbCBJC.Append(" (select sum(NVL(p.paid_amount_hdr_curr,0)*GET_EX_RATE_BUY(PT.CURRENCY_MST_FK," + BaseCurrFk + " ,PT.PAYMENT_DATE)) paid_amount_hdr_curr, ");
			sbCBJC.Append("    p.inv_supplier_tbl_fk from PAYMENTS_TBL PT,payment_trn_tbl p WHERE PT.PAYMENT_TBL_PK=P.PAYMENTS_TBL_FK ");
			sbCBJC.Append("  group by inv_supplier_tbl_fk) A, ");
			sbCBJC.Append("  (SELECT IST.INV_SUPPLIER_TBL_FK, SUM((NVL(IST.ACTUAL_AMT,0) + NVL(IST.TAX_AMOUNT, 0))* ");
			sbCBJC.Append("   GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + BaseCurrFk + " ,INVTBL.SUPPLIER_INV_DT)) VOUCHER_AMT ");
			sbCBJC.Append("                          FROM INV_SUPPLIER_TBL INVTBL,INV_SUPPLIER_TRN_TBL IST ");
			sbCBJC.Append("                     WHERE INVTBL.INV_SUPPLIER_PK=IST.INV_SUPPLIER_TBL_FK ");
			sbCBJC.Append("                         GROUP BY IST.INV_SUPPLIER_TBL_FK) B");
			sbCBJC.Append("  WHERE UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK ");
			sbCBJC.Append("   and vou.created_by_fk = UMT.user_mst_pk ");
			sbCBJC.Append("   AND VOU.INV_SUPPLIER_PK = VOUTRN.INV_SUPPLIER_TBL_FK ");
			sbCBJC.Append("   AND VOU.VENDOR_MST_FK = VENDOR.VENDOR_MST_PK ");
			sbCBJC.Append("   AND VOU.JOB_TYPE = 2 ");
			sbCBJC.Append("   AND VOUTRN.JOBCARD_REF_NO = CBJC.CBJC_NO ");
			sbCBJC.Append("   AND VOUTRN.COST_ELEMENT_MST_FK = CEMT.COST_ELEMENT_MST_PK ");
			sbCBJC.Append("   and A.inv_supplier_tbl_fk(+) = VOUTRN.INV_SUPPLIER_TBL_FK ");
			sbCBJC.Append("   AND B.INV_SUPPLIER_TBL_FK(+) = VOU.INV_SUPPLIER_PK ");
			if (biztype != 3) {
				sbCBJC.Append(" and vou.business_type = " + biztype);
			}
			sbCBJC.Append("  and vou.process_type = " + process);
			if (Convert.ToInt32(locpk) != 0) {
				sbCBJC.Append(" AND LMT.LOCATION_MST_PK IN ( " + locpk + ")");
			}
			if (!string.IsNullOrEmpty(vendorpk)) {
				if (Convert.ToInt32(vendorpk) > 0) {
					sbCBJC.Append(" and vou.vendor_mst_fk in (" + vendorpk + ")");
				}
			}
			if (Convert.ToInt32(costpk) != 0) {
				sbCBJC.Append(" and voutrn.cost_element_mst_fk = " + costpk);
				//<costpk>")
			}

			if (!string.IsNullOrEmpty(dueby.Trim())) {
				sbCBJC.Append(" and to_date(vou.supplier_due_dt,'DD/MM/YYYY') = to_date('" + dueby + "','DD/MM/YYYY')");
			}
			if (((fromdt != null) & (todt != null)) & !string.IsNullOrEmpty(fromdt.Trim()) & !string.IsNullOrEmpty(todt.Trim())) {
				sbCBJC.Append(" and vou.invoice_date between to_date('" + fromdt + "','DD/MM/YYYY')");
				sbCBJC.Append(" AND TO_DATE(NVL('" + todt + "','1/1/9999'),'DD/MM/YYYY')");
			} else if (((fromdt != null)) & !string.IsNullOrEmpty(fromdt)) {
				sbCBJC.Append(" and vou.invoice_date >= to_date('" + fromdt + "','DD/MM/YYYY')");
			} else if (((todt != null)) & !string.IsNullOrEmpty(todt)) {
				sbCBJC.Append(" and vou.invoice_date <= to_date('" + todt + "','DD/MM/YYYY')");
			}

			sbTPN.Append(" UNION ");
			sbTPN.Append(" SELECT LMT.LOCATION_MST_PK, ");
			sbTPN.Append(" LMT.LOCATION_NAME, ");
			sbTPN.Append(" VENDOR.VENDOR_MST_PK, ");
			sbTPN.Append(" VENDOR.VENDOR_NAME, ");
			sbTPN.Append(" TRC.TRANSPORT_INST_SEA_PK,");
			sbTPN.Append(" TRC.TRANS_INST_REF_NO, ");
			sbTPN.Append(" TO_CHAR(TRC.TRANS_INST_DATE, 'DD/MM/YYYY') TRN_DATE,");
			sbTPN.Append(" VOU.INV_SUPPLIER_PK,");
			sbTPN.Append(" VOU.INVOICE_REF_NO,");
			sbTPN.Append(" TO_CHAR(VOU.INVOICE_DATE, dateformat) VOUCHER_DATE, ");
			sbTPN.Append(" CEMT.COST_ELEMENT_MST_PK, ");
			sbTPN.Append(" CEMT.COST_ELEMENT_ID, ");
			sbTPN.Append(" (NVL(voutrn.actual_amt,0) + nvl(voutrn.tax_amount,0))* ");
			sbTPN.Append("   GET_EX_RATE_BUY(vou.CURRENCY_MST_FK," + BaseCurrFk + " ,vou.SUPPLIER_INV_DT) \"Actual_Amt\",");
			sbTPN.Append(" NVL(voutrn.estimated_amt,0)*GET_EX_RATE_BUY(vou.CURRENCY_MST_FK," + BaseCurrFk + " ,vou.SUPPLIER_INV_DT) \"Est_Amt\",");

			sbTPN.Append(" ((NVL(voutrn.actual_amt,0) + nvl(voutrn.tax_amount, 0)) - NVL(voutrn.estimated_amt,0)) *");
			sbTPN.Append("   GET_EX_RATE_BUY(vou.CURRENCY_MST_FK," + BaseCurrFk + " ,vou.SUPPLIER_INV_DT) \"Diff\",");
			sbTPN.Append(" to_char(vou.supplier_due_dt,'DD/MM/YYYY') \"Due_On_Date\",");

			sbTPN.Append("  (CASE");
			sbTPN.Append("                         WHEN (NVL(B.VOUCHER_AMT,0)) =");
			sbTPN.Append("                              NVL(A.PAID_AMOUNT_HDR_CURR, 0) THEN");
			sbTPN.Append("                          'Full'");
			sbTPN.Append("                         WHEN (NVL(B.VOUCHER_AMT,0) -");
			sbTPN.Append("                              NVL(A.PAID_AMOUNT_HDR_CURR, 0)) =");
			sbTPN.Append("                              (NVL(B.VOUCHER_AMT,0)) THEN");
			sbTPN.Append("                          'Pending'");
			sbTPN.Append("                         WHEN (NVL(B.VOUCHER_AMT,0) >");
			sbTPN.Append("                              NVL(A.PAID_AMOUNT_HDR_CURR, 0)) THEN");
			sbTPN.Append("                          'Part'");
			sbTPN.Append("                       ");
			sbTPN.Append("                       END) \"PAYMENT\",");
			sbTPN.Append("  NVL(A.PAID_AMOUNT_HDR_CURR, 0) \"PAID_AMT\",");
			sbTPN.Append("   NVL(B.VOUCHER_AMT,0) \"VOUCH_AMT\",");
			sbTPN.Append("  vou.business_type \"BizType\",");
			sbTPN.Append("  vou.process_type  \"ProcessType\",");
			sbTPN.Append("  TRC.CARGO_TYPE ");
			sbTPN.Append("  FROM TRANSPORT_INST_SEA_TBL TRC,");
			sbTPN.Append("  USER_MST_TBL UMT,");
			sbTPN.Append("  LOCATION_MST_TBL LMT,");
			sbTPN.Append("  VENDOR_MST_TBL VENDOR, ");
			sbTPN.Append("  INV_SUPPLIER_TBL VOU,");
			sbTPN.Append("  INV_SUPPLIER_TRN_TBL VOUTRN,");
			sbTPN.Append("  COST_ELEMENT_MST_TBL CEMT,");
			sbTPN.Append(" (select sum(NVL(p.paid_amount_hdr_curr,0)*GET_EX_RATE_BUY(PT.CURRENCY_MST_FK," + BaseCurrFk + " ,PT.PAYMENT_DATE)) paid_amount_hdr_curr, ");
			sbTPN.Append("    p.inv_supplier_tbl_fk from PAYMENTS_TBL PT,payment_trn_tbl p WHERE PT.PAYMENT_TBL_PK=P.PAYMENTS_TBL_FK ");
			sbTPN.Append("  group by inv_supplier_tbl_fk) A, ");
			sbTPN.Append("  (SELECT IST.INV_SUPPLIER_TBL_FK, SUM((NVL(IST.ACTUAL_AMT,0) + NVL(IST.TAX_AMOUNT, 0))* ");
			sbTPN.Append("   GET_EX_RATE_BUY(INVTBL.CURRENCY_MST_FK," + BaseCurrFk + " ,INVTBL.SUPPLIER_INV_DT)) VOUCHER_AMT ");
			sbTPN.Append("                          FROM INV_SUPPLIER_TBL INVTBL,INV_SUPPLIER_TRN_TBL IST ");
			sbTPN.Append("                     WHERE INVTBL.INV_SUPPLIER_PK=IST.INV_SUPPLIER_TBL_FK ");
			sbTPN.Append("                         GROUP BY IST.INV_SUPPLIER_TBL_FK) B");
			sbTPN.Append("  WHERE UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK ");
			sbTPN.Append("   and vou.created_by_fk = UMT.user_mst_pk ");
			sbTPN.Append("   AND VOU.INV_SUPPLIER_PK = VOUTRN.INV_SUPPLIER_TBL_FK ");
			sbTPN.Append("   AND VOU.VENDOR_MST_FK = VENDOR.VENDOR_MST_PK ");
			sbTPN.Append("   AND VOU.JOB_TYPE = 3 ");
			sbTPN.Append("   AND VOUTRN.JOBCARD_REF_NO = TRC.TRANS_INST_REF_NO ");
			sbTPN.Append("   AND VOUTRN.COST_ELEMENT_MST_FK = CEMT.COST_ELEMENT_MST_PK ");
			sbTPN.Append("   and A.inv_supplier_tbl_fk(+) = VOUTRN.INV_SUPPLIER_TBL_FK ");
			sbTPN.Append("   AND B.INV_SUPPLIER_TBL_FK(+) = VOU.INV_SUPPLIER_PK ");
			if (biztype != 3) {
				sbTPN.Append("   and vou.business_type = " + biztype);
			}
			sbTPN.Append("   and vou.process_type = " + process);
			if (Convert.ToInt32(locpk) != 0) {
				sbTPN.Append(" AND LMT.LOCATION_MST_PK IN ( " + locpk + ")");
			}
			if (!string.IsNullOrEmpty(vendorpk)) {
				if (Convert.ToInt32(vendorpk) > 0) {
					sbTPN.Append(" and vou.vendor_mst_fk in (" + vendorpk + ")");
				}
			}
			if (Convert.ToInt32(costpk) != 0) {
				sbTPN.Append(" and voutrn.cost_element_mst_fk = " + costpk);
				//<costpk>")
			}

			if (!string.IsNullOrEmpty(dueby.Trim())) {
				sbTPN.Append(" and to_date(vou.supplier_due_dt,'DD/MM/YYYY') = to_date('" + dueby + "','DD/MM/YYYY')");
			}
			if (((fromdt != null) & (todt != null)) & !string.IsNullOrEmpty(fromdt.Trim()) & !string.IsNullOrEmpty(todt.Trim())) {
				sbTPN.Append(" and vou.invoice_date between to_date('" + fromdt + "','DD/MM/YYYY')");
				sbTPN.Append(" AND TO_DATE(NVL('" + todt + "','1/1/9999'),'DD/MM/YYYY')");
			} else if (((fromdt != null)) & !string.IsNullOrEmpty(fromdt)) {
				sbTPN.Append(" and vou.invoice_date >= to_date('" + fromdt + "','DD/MM/YYYY')");
			} else if (((todt != null)) & !string.IsNullOrEmpty(todt)) {
				sbTPN.Append(" and vou.invoice_date <= to_date('" + todt + "','DD/MM/YYYY')");
			}

			//'End
			// sbfrom.Append("ORDER BY JCSE.JOBCARD_DATE DESC")
			try {
				sbSql.Append(" select count(*) from ( ");
				sbSql.Append(sb);
				sbSql.Append(sbfrom);
				sbSql.Append(sbCBJC);
				sbSql.Append(sbTPN);
				sbSql.Append(" ) ");
				TotalRec = Convert.ToInt32(objWF.ExecuteScaler(sbSql.ToString()));
				TotalPage = Convert.ToInt32(TotalRec / RecordsPerPage);

				if (TotalRec % RecordsPerPage != 0) {
					TotalPage += 1;
				}
				if (CurrentPage > TotalPage) {
					CurrentPage = 1;
				}
				if (TotalRec == 0) {
					CurrentPage = 0;
				}
				last = CurrentPage * RecordsPerPage;
				start = (CurrentPage - 1) * RecordsPerPage + 1;

				sbSql.Remove(0, sbSql.Length);

				sbSql.Append(" select * from (select rownum SlNr, Q.* from ( ");
				sbSql.Append(sb);
				sbSql.Append(sbfrom);
				sbSql.Append(sbCBJC);
				sbSql.Append(sbTPN);
				sbSql.Append(" )Q  ORDER BY TO_DATE(\"JC_date\",DATEFORMAT) DESC ) ");

				if (Pending)
					sbSql.Append("where Payment = 'Pending'");
				//If start >= 0 Then sbSql.Append(" WHERE SlNr  Between " & start & " and " & last & vbCrLf)

				return objWF.GetDataSet(sbSql.ToString());
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}

		}

		public DataSet FetchDAListing(Int16 biztype, Int16 process, Int16 vendorpk, Int16 locpk, string fromdt, string todt, string dueby, Int16 Payment_st, Int16 JobType, Int32 Flag,
		Int32 CurrentPage, Int32 TotalPage, string Vessel_Name = "", string Voyage_Nr = "", string RefNr = "", int ExportExcel = 0)
		{

			WorkFlow objWF = new WorkFlow();
			DataSet DS = new DataSet();
			try {
				var _with18 = objWF.MyCommand.Parameters;
				_with18.Add("BIZ_TYPE_IN", biztype).Direction = ParameterDirection.Input;
				_with18.Add("PROCESS_IN", process).Direction = ParameterDirection.Input;
				_with18.Add("VENDOR_PK_IN", vendorpk).Direction = ParameterDirection.Input;
				_with18.Add("LOC_PK_IN", locpk).Direction = ParameterDirection.Input;
				_with18.Add("FROM_DT_IN", (string.IsNullOrEmpty(fromdt) ? "" : fromdt)).Direction = ParameterDirection.Input;
				_with18.Add("TO_DT_IN", (string.IsNullOrEmpty(todt) ? "" : todt)).Direction = ParameterDirection.Input;
				_with18.Add("DUE_BY_IN", (string.IsNullOrEmpty(dueby) ? "" : dueby)).Direction = ParameterDirection.Input;
				_with18.Add("PAY_STATUS_IN", Payment_st).Direction = ParameterDirection.Input;
				_with18.Add("VESSEL_NAME_IN", (string.IsNullOrEmpty(Vessel_Name) ? "" : Vessel_Name)).Direction = ParameterDirection.Input;
				_with18.Add("VOYAGE_NR_IN", (string.IsNullOrEmpty(Voyage_Nr) ? "" : Voyage_Nr)).Direction = ParameterDirection.Input;
				_with18.Add("BASE_CURR_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
				_with18.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
				_with18.Add("REF_NR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
				_with18.Add("EXPORT_IN", ExportExcel).Direction = ParameterDirection.Input;
				_with18.Add("POST_BACK_IN", Flag).Direction = ParameterDirection.Input;
				_with18.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
				_with18.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with18.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				DS = objWF.GetDataSet("FETCH_DA_REPORT_PKG", "FETCH_DA_REPORT");
				TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
				CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				if (TotalPage == 0) {
					CurrentPage = 0;
				} else {
					CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				}
				return DS;
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}

		}

		public DataSet FetchDAReportPrint(Int16 biztype, Int16 process, Int16 vendorpk, Int16 locpk, string fromdt, string todt, string dueby, Int16 Payment_st, Int16 JobType, string Vessel_Name = "",
		string Voyage_Nr = "", string RefNr = "")
		{

			WorkFlow objWF = new WorkFlow();
			DataSet DS = new DataSet();
			try {
				var _with19 = objWF.MyCommand.Parameters;
				_with19.Add("BIZ_TYPE_IN", biztype).Direction = ParameterDirection.Input;
				_with19.Add("PROCESS_IN", process).Direction = ParameterDirection.Input;
				_with19.Add("VENDOR_PK_IN", vendorpk).Direction = ParameterDirection.Input;
				_with19.Add("LOC_PK_IN", locpk).Direction = ParameterDirection.Input;
				_with19.Add("FROM_DT_IN", (string.IsNullOrEmpty(fromdt) ? "" : fromdt)).Direction = ParameterDirection.Input;
				_with19.Add("TO_DT_IN", (string.IsNullOrEmpty(todt) ? "" : todt)).Direction = ParameterDirection.Input;
				_with19.Add("DUE_BY_IN", (string.IsNullOrEmpty(dueby) ? "" : dueby)).Direction = ParameterDirection.Input;
				_with19.Add("PAY_STATUS_IN", Payment_st).Direction = ParameterDirection.Input;
				_with19.Add("VESSEL_NAME_IN", (string.IsNullOrEmpty(Vessel_Name) ? "" : Vessel_Name)).Direction = ParameterDirection.Input;
				_with19.Add("VOYAGE_NR_IN", (string.IsNullOrEmpty(Voyage_Nr) ? "" : Voyage_Nr)).Direction = ParameterDirection.Input;
				_with19.Add("BASE_CURR_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
				_with19.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
				_with19.Add("REF_NR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
				_with19.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				DS = objWF.GetDataSet("FETCH_DA_REPORT_PKG", "FETCH_DA_REPORT_PRINT");
				return DS;
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}

		}
		#endregion


		public void Export_To_QFOR(string DA_REF_NR, string strRetVal)
		{
			WorkFlow objWK = new WorkFlow();
			OracleCommand cmd = new OracleCommand();

			objWK.OpenConnection();


			try {
				var _with20 = cmd;

				_with20.Connection = objWK.MyConnection;
				_with20.CommandType = CommandType.StoredProcedure;
				_with20.CommandText = objWK.MyUserName + ".DB_INTEGRATION.EXP_DAVOUCHERS";
				var _with21 = _with20.Parameters;
				_with21.Add("DAVOUCHER_REF_NR", DA_REF_NR).Direction = ParameterDirection.Input;
				_with21.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;


				cmd.ExecuteNonQuery();
				strRetVal = (string.IsNullOrEmpty(cmd.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : cmd.Parameters["RETURN_VALUE"].Value.ToString());

				objWK.CloseConnection();
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}

		}
		#region "FetchVoucher"
		public DataSet FetchAllVouchers(Int32 VendorPK, Int16 Status, Int16 Business_Type, Int16 Process_Type, Int16 Job_Type, string FromDt = "", string ToDt = "", Int32 TradePK = 0, int JobPK = 0, string Vsl = "",
		string Voy = "", string VendorInvNr = "", string InvDt = "", string SupplierRefNr = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SearchType = "", Int32 flag = 0)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			StringBuilder strSQL = new StringBuilder();
			StringBuilder strCount = new StringBuilder();
			WorkFlow objWF = new WorkFlow();
			DataSet objDS = null;
			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);

			sb.Append("SELECT DISTINCT V.INV_SUPPLIER_PK,");
			sb.Append("       V.VOUCHERNO        VOUCHERNO,");
			sb.Append("       V.VOUCHERDATE,");
			sb.Append("       DECODE(V.JOB_TYPE,1,'JobCard',2,'Customs Brokerage',3,'Transport Note') JOB_TYPE,");
			sb.Append("       DECODE(V.BUSINESS_TYPE,1,'Air',2,'Sea') BUSINESS_TYPE,");
			sb.Append("       DECODE(V.PROCESS_TYPE,1,'Export',2,'Import') PROCESS_TYPE,");
			sb.Append("       V.SUPPLIER_REF_NR,");
			sb.Append("       V.VENDOR,");
			sb.Append("       V.CUR              CUR,");
			sb.Append("       V.AMOUNT           AMOUNT,");
			sb.Append("       DECODE(V.ELEMENT_APPROVED,1,'Approved',2,'Rejected',0,'Pending',3,'Cancelled') STATUS,");
			sb.Append("      (SELECT COUNT(*) from PAYMENT_TRN_TBL PTR, PAYMENTS_TBL PT ");
			sb.Append("  WHERE PTR.Inv_Supplier_Tbl_Fk = V.INV_SUPPLIER_PK ");
			sb.Append("  AND PT.PAYMENT_TBL_PK = PTR.PAYMENTS_TBL_FK AND PT.APPROVED in (1,0) ");
			//' To check wheather payment is done or not and paymment status in Approved and Pending.
			sb.Append("  AND PTR.INV_SUPPLIER_TBL_FK = V.INV_SUPPLIER_PK) PAY_STATUS, ");
			sb.Append("       V.SEL              SEL");
			sb.Append("  FROM VIEW_VOUCHER_LISTING  V,VIEW_VOUCHER_LISTING_CHILD VVC");
			sb.Append("  WHERE VVC.INV_SUPPLIER_PK(+) = V.INV_SUPPLIER_PK ");
			sb.Append("   AND V.AMOUNT <>0");
			if (flag == 0) {
				sb.Append(" AND 1=2 ");
			}
			if (Status >= 0) {
				sb.Append("  AND V.ELEMENT_APPROVED = " + Status);
			}
			if (Business_Type > 0) {
				if (Business_Type == 3) {
					sb.Append("  AND V.BUSINESS_TYPE IN (1,2) ");
				} else {
					sb.Append("  AND V.BUSINESS_TYPE =" + Business_Type);
				}

			}
			if (Process_Type > 0) {
				sb.Append("  AND V.PROCESS_TYPE =" + Process_Type);
			}
			if (Job_Type > 0) {
				sb.Append("  AND V.JOB_TYPE =" + Job_Type);
			}
			if (VendorInvNr.Trim().Length > 0) {
				sb.Append("  AND V.VOUCHERNO='" + VendorInvNr + "'");
			}
			if (VendorPK > 0) {
				sb.Append(" AND V.VENDOR_MST_FK =" + VendorPK);
			}
			if (JobPK > 0) {
				sb.Append(" AND VVC.JOB_PK =" + JobPK);
			}
			if (Business_Type != 0) {
				if (Vsl.Trim().Length > 0) {
					sb.Append(" AND VVC.VESSEL ='" + Vsl + "'");
				}
			}
			if (Business_Type > 1) {
				if (Vsl.Trim().Length > 0) {
					sb.Append(" AND VVC.VESSEL ='" + Vsl + "'");
				}
			}
			if (Voy.Trim().Length > 0) {
				sb.Append(" AND VVC.VOYAGE ='" + Voy + "'");
			}
			if (SupplierRefNr.Trim().Length > 0) {
				sb.Append(" AND V.SUPPLIER_REF_NR ='" + SupplierRefNr + "'");
			}
			if ((InvDt != null) & InvDt != " ") {
				sb.Append(" AND V.VOUCHERDATE = TO_DATE('" + InvDt + "','" + dateFormat + "')");
			}

			//If (Not IsNothing(FromDt) And Not IsNothing(ToDt)) And FromDt <> " " And ToDt <> " " Then
			//    sb.Append(" AND TO_DATE(V.JCDATE " & " ,'" & dateFormat & "') BETWEEN TO_DATE('" & FromDt & "','" & dateFormat & "')  AND TO_DATE('" & ToDt & "','" & dateFormat & "')")
			//Else
			if (((FromDt != null)) & FromDt != " ") {
				sb.Append(" AND TO_DATE(V.JCDATE " + " ,'" + dateFormat + "') >= TO_DATE('" + FromDt + "','" + dateFormat + "')");
			}
			if (((ToDt != null)) & ToDt != " ") {
				sb.Append(" AND TO_DATE(V.JCDATE " + " ,'" + dateFormat + "') <= TO_DATE('" + ToDt + "','" + dateFormat + "')");
			}

			strCount.Append(" SELECT COUNT(*)");
			strCount.Append(" FROM ");
			strCount.Append("(" + sb.ToString() + ")");

			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
			TotalPage = TotalRecords / RecordsPerPage;

			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage) {
				CurrentPage = 1;
			}
			if (TotalRecords == 0) {
				CurrentPage = 0;
			}
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;

			strSQL.Append(" SELECT * FROM ( ");
			strSQL.Append(" SELECT ROWNUM SRNO, Q.* FROM ( ");
			strSQL.Append("  SELECT * FROM ( " + sb.ToString() + " ");
			strSQL.Append("  ORDER BY VOUCHERDATE DESC, VOUCHERNO DESC)) Q) ");
			strSQL.Append("  WHERE SRNO BETWEEN " + start + " AND " + last + "");

			DataSet DS = null;
			try {
				DS = objWF.GetDataSet(strSQL.ToString());
				DataRelation CONTRel = null;
				DS.Tables.Add(FetchAllchildlist(AllMasterPKs(DS), VendorPK, Status, Business_Type, Process_Type, Job_Type, FromDt, ToDt, JobPK, Vsl,
				Voy, SupplierRefNr, VendorInvNr, InvDt, SearchType));
				CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["INV_SUPPLIER_PK"], DS.Tables[1].Columns["INV_SUPPLIER_PK"], true);
				CONTRel.Nested = true;
				DS.Relations.Add(CONTRel);
				return DS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		private DataTable FetchAllchildlist(string SUPPLIERPKs = "", Int32 VendorPK = 0, Int16 Status = 0, Int16 Business_Type = 0, Int16 Process_Type = 0, Int16 Job_Type = 0, string FromDt = "", string ToDt = "", int JobPK = 0, string Vsl = "",
		string Voy = "", string SupplierRefNr = "", string VendorInvNr = "", string InvDt = "", string SearchType = "")
		{

			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			StringBuilder strSQL = new StringBuilder();
			int RowCnt = 0;
			int Rno = 0;
			int pk = 0;
			WorkFlow objWF = new WorkFlow();
			DataTable dt = null;
			sb.Append("SELECT DISTINCT VVC.INV_SUPPLIER_PK,");
			sb.Append("                VVC.JOB_PK,");
			sb.Append("                VVC.JOBNO,");
			sb.Append("                VVC.VSL_FLIGHT,");
			sb.Append("                VVC.CUR,");
			sb.Append("                VVC.AMOUNT ");
			sb.Append("  FROM VIEW_VOUCHER_LISTING_CHILD VVC,VIEW_VOUCHER_LISTING V");
			sb.Append("   WHERE VVC.INV_SUPPLIER_PK = V.INV_SUPPLIER_PK(+) ");
			sb.Append("   AND VVC.INV_SUPPLIER_PK IN (" + SUPPLIERPKs + ")");
			sb.Append("   AND V.AMOUNT <>0");
			if (Status >= 0) {
				sb.Append("  AND VVC.ELEMENT_APPROVED = " + Status);
			}
			if (Business_Type > 0) {
				sb.Append(" AND VVC.BUSINESS_TYPE =" + Business_Type);
			}
			if (Process_Type > 0) {
				sb.Append(" AND VVC.PROCESS_TYPE =" + Process_Type);
			}
			if (Job_Type > 0) {
				sb.Append(" AND VVC.JOB_TYPE =" + Job_Type);
			}
			if (VendorInvNr.Trim().Length > 0) {
				sb.Append(" AND V.VOUCHERNO='" + VendorInvNr + "'");
			}
			if (VendorPK > 0) {
				sb.Append(" AND V.VENDOR_MST_FK =" + VendorPK);
			}
			if (JobPK > 0) {
				sb.Append(" AND VVC.JOB_PK =" + JobPK);
			}
			if (Vsl.Trim().Length > 0) {
				sb.Append(" AND VVC.VESSEL ='" + Vsl + "'");
			}
			if (Voy.Trim().Length > 0) {
				sb.Append(" AND VVC.VOYAGE ='" + Voy + "'");

			}
			if (SupplierRefNr.Trim().Length > 0) {
				sb.Append(" AND V.SUPPLIER_REF_NR ='" + SupplierRefNr + "'");
			}
			if ((InvDt != null) & InvDt != " ") {
				sb.Append(" AND V.VOUCHERDATE = TO_DATE('" + InvDt + "','" + dateFormat + "')");
			}

			//If (Not IsNothing(FromDt) And Not IsNothing(ToDt)) And FromDt <> " " And ToDt <> " " Then
			//    sb.Append(" AND TO_DATE(V.JCDATE " & " ,'" & dateFormat & "') BETWEEN TO_DATE('" & FromDt & "','" & dateFormat & "')  AND TO_DATE('" & ToDt & "','" & dateFormat & "')")
			//Else
			if (((FromDt != null)) & FromDt != " ") {
				sb.Append(" AND TO_DATE(V.JCDATE " + " ,'" + dateFormat + "') >= TO_DATE('" + FromDt + "','" + dateFormat + "')");
			}
			if (((ToDt != null)) & ToDt != " ") {
				sb.Append(" AND TO_DATE(V.JCDATE " + " ,'" + dateFormat + "') <= TO_DATE('" + ToDt + "','" + dateFormat + "')");
			}

			strSQL.Append(" SELECT *  FROM ( ");
			strSQL.Append(" SELECT ROWNUM SRNO, Q.* FROM ");
			strSQL.Append(" ( " + sb.ToString() + " ");
			strSQL.Append("  ORDER BY INV_SUPPLIER_PK DESC, JOBNO DESC ) Q)");

			string sql = null;
			sql = strSQL.ToString();
			try {
				pk = -1;
				dt = objWF.GetDataTable(sql);
				for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++) {
					if (Convert.ToInt32(dt.Rows[RowCnt]["INV_SUPPLIER_PK"] )!= pk) {
						pk = Convert.ToInt32(dt.Rows[RowCnt]["INV_SUPPLIER_PK"]);
						Rno = 0;
					}
					Rno += 1;
					dt.Rows[RowCnt]["SrNO"] = Rno;
				}
				return dt;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Fetch Status"
		public DataSet FetchDropDownValues(string Flag, string ConfigID)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT T.DD_VALUE, T.DD_ID");
			sb.Append("  FROM QFOR_DROP_DOWN_TBL T");
			sb.Append(" WHERE T.DD_FLAG = '" + Flag + "'");
			sb.Append(" AND T.CONFIG_ID  = '" + ConfigID + "'");
			sb.Append(" ORDER BY T.DROPDOWN_PK ");
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Fetch Status"
		public DataSet FetchJobType(int PK)
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT INV.PROCESS_TYPE, INV.BUSINESS_TYPE, INV.JOB_TYPE");
			sb.Append("  FROM INV_SUPPLIER_TBL INV ");
			sb.Append(" WHERE INV.INV_SUPPLIER_PK = " + PK);
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "UpdateInvStatus"
		public string UpdateVoucherStatus(string VoucherPk, string remarks)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand updCmd = new OracleCommand();
			Int16 intIns = default(Int16);
			try {
				objWF.OpenConnection();
				var _with22 = updCmd;
				_with22.Connection = objWF.MyConnection;
				_with22.CommandType = CommandType.StoredProcedure;
				_with22.CommandText = objWF.MyUserName + ".VOUCHER_CANCELLATION_PKG.CANCEL_VOUCHER";
				var _with23 = _with22.Parameters;
				updCmd.Parameters.Add("VOUCHER_PK_IN", VoucherPk).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("REMARKS_IN", remarks).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("TYPE_FLAG_IN", "VOUCHER").Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				intIns = Convert.ToInt16(_with22.ExecuteNonQuery());
				return Convert.ToString(updCmd.Parameters["RETURN_VALUE"].Value);
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyConnection.Close();
			}
		}
		#endregion

		#region "Credit Limit"
		public double FetchCreditDays(int VendPK)
		{
			string Strsql = null;
			double CreditDays = 0;
			WorkFlow ObjWF = new WorkFlow();
			try {
				Strsql = "select c.CREDIT_DAYS from VENDOR_MST_TBL c where c.VENDOR_MST_PK = " + VendPK;
				CreditDays = Convert.ToDouble(getDefault(ObjWF.ExecuteScaler(Strsql), 0));
				return CreditDays;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public double FetchCustCreditAmt(int VendPK)
		{
			string Strsql = null;
			double CreditAmt = 0;
			WorkFlow ObjWF = new WorkFlow();
			try {
				Strsql = "select c.credit_limit from VENDOR_MST_TBL c where c.VENDOR_MST_PK = " + VendPK;
				CreditAmt = Convert.ToDouble(getDefault(ObjWF.ExecuteScaler(Strsql), 0));
				return CreditAmt;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public double FetchCustCreditAmtUsed(int VendPK)
		{
			string Strsql = null;
			double CreditAmt = 0;
			WorkFlow ObjWF = new WorkFlow();
			try {
				Strsql = "select c.credit_used from VENDOR_MST_TBL c where c.VENDOR_MST_PK = " + VendPK;
				CreditAmt = Convert.ToDouble(getDefault(ObjWF.ExecuteScaler(Strsql), 0));
				return CreditAmt;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public double FetchcolCustCreditAmtUsed(int VendPK)
		{
			string Strsql = null;
			double CreditAmt = 0;
			WorkFlow ObjWF = new WorkFlow();
			try {
				Strsql = "select c.credit_used from VENDOR_MST_TBL c where c.VENDOR_MST_PK = " + VendPK;
				CreditAmt = Convert.ToDouble(getDefault(ObjWF.ExecuteScaler(Strsql), 0));
				return CreditAmt;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		public void SaveCreditLimit(double NetAmt, int VendPK, OracleTransaction TRAN, int InvCurPk, System.DateTime invdate)
		{
			WorkFlow objWK1 = new WorkFlow();
			Int16 exe = default(Int16);
			double ROE = 0;
			OracleCommand cmd = new OracleCommand();
			string strSQL = null;
			int VenCurPK = 0;
			//Dim Invdate As String
			strSQL = " select nvl(vmt.credit_cur_fk,0) from vendor_mst_tbl vmt where vmt.vendor_mst_pk=" + VendPK;
			VenCurPK = Convert.ToInt32(objWK1.ExecuteScaler(strSQL));
			//invdate = invdate.s
			strSQL = "select get_ex_rate(" + InvCurPk + "," + VenCurPK + ",to_date('" + invdate.ToString("dd/MM/yyyy") + "',DATEFORMAT)) from dual";
			ROE = Convert.ToDouble(objWK1.ExecuteScaler(strSQL));

			try {
				cmd.CommandType = CommandType.Text;
				cmd.Connection = TRAN.Connection;
				cmd.Transaction = TRAN;

				cmd.Parameters.Clear();
				strSQL = "update VENDOR_MST_TBL a set a.credit_used = nvl(a.credit_used,0) + round(" + ROE * NetAmt + ",2)";
				strSQL = strSQL + " where a.VENDOR_MST_PK =" + VendPK;
				cmd.CommandText = strSQL;
				cmd.ExecuteNonQuery();

			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
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
	}
}

