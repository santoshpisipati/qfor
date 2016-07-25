#region "Comments"

//'***************************************************************************************************************
//'*  Company Name            :
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By              :    Santosh on 31-May-16
//'*  Module/Project Leader   :    Santosh Pisipati
//'*  Description             :
//'*  Module/Form/Class Name  :
//'*  Configuration ID        :
//'***************************************************************************************************************
//'*  Revision History
//'***************************************************************************************************************
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
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
    public class cls_RFQ_Listings : CommonFeatures
	{
		Int32 m_last;
		Int32 m_start;

		DataSet DSMain = new DataSet();
		public DataSet FetchAll(string OperatorID = "", string OperatorName = "", string RFQNo = "", string RFQDate = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string ValidFrom = "", string ValidTo = "",
		bool ActiveOnly = true, short CargoType = 1, string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", string CurrBizType = "3")
		{

			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
			string SrOP = (SearchType == "C" ? "%" : "");
			if (OperatorID.Length > 0) {
				buildCondition.Append(" AND UPPER(OPERATOR_ID) LIKE '" + SrOP + OperatorID.ToUpper().Replace("'", "''") + "%'");
			}
			if (OperatorName.Length > 0) {
				buildCondition.Append(" AND UPPER(OPERATOR_NAME) LIKE '" + SrOP + OperatorName.ToUpper().Replace("'", "''") + "%'");
			}
			if (RFQNo.Length > 0) {
				buildCondition.Append(" AND UPPER(RFQ_REF_NO) LIKE '" + SrOP + RFQNo.ToUpper().Replace("'", "''") + "%'");
			}
			if (RFQDate.Length > 0) {
				buildCondition.Append(" AND RFQ_DATE = TO_DATE('" + RFQDate + "' , '" + dateFormat + "') ");
			}

			if (POLID.Length > 0) {
				buildCondition.Append(" AND EXISTS ");
				buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
				buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
				buildCondition.Append("       and PORT_MST_PK IN ");
				buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM RFQ_TRN_SEA_FCL_LCL ");
				buildCondition.Append("           WHERE RFQ_MAIN_SEA_FK = main.RFQ_MAIN_SEA_PK ");
				buildCondition.Append("         ) ");
				buildCondition.Append("     ) ");
			}

			if (POLName.Length > 0) {
				buildCondition.Append(" AND EXISTS ");
				buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
				buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
				buildCondition.Append("       and PORT_MST_PK IN ");
				buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM RFQ_TRN_SEA_FCL_LCL ");
				buildCondition.Append("           WHERE RFQ_MAIN_SEA_FK = main.RFQ_MAIN_SEA_PK ");
				buildCondition.Append("         ) ");
				buildCondition.Append("     ) ");
			}
			if (PODID.Length > 0) {
				buildCondition.Append(" AND EXISTS ");
				buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
				buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%' ");
				buildCondition.Append("       and PORT_MST_PK IN ");
				buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM RFQ_TRN_SEA_FCL_LCL ");
				buildCondition.Append("           WHERE RFQ_MAIN_SEA_FK = main.RFQ_MAIN_SEA_PK ");
				buildCondition.Append("         ) ");
				buildCondition.Append("     ) ");
			}
			if (PODName.Length > 0) {
				buildCondition.Append(" AND EXISTS ");
				buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
				buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
				buildCondition.Append("       and PORT_MST_PK IN ");
				buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM RFQ_TRN_SEA_FCL_LCL ");
				buildCondition.Append("           WHERE RFQ_MAIN_SEA_FK = main.RFQ_MAIN_SEA_PK ");
				buildCondition.Append("         ) ");
				buildCondition.Append("     ) ");
			}

			//If ActiveOnly = True Then
			//    If ValidFrom.Length > 0 And ValidTo.Length > 0 Then
			//        buildCondition.Append(vbCrLf & " AND ( ")
			//        buildCondition.Append(vbCrLf & "        main.VALID_TO >= TO_DATE(SYSDATE , '" & dateFormat & "') ")
			//        buildCondition.Append(vbCrLf & "       OR main.VALID_TO IS NULL ")
			//        buildCondition.Append(vbCrLf & "     ) ")
			//    End If
			//End If
			//If ValidTo.Length > 0 And Not ValidFrom.Length > 0 Then
			//    buildCondition.Append(vbCrLf & " AND ")
			//    buildCondition.Append(vbCrLf & "        ((TO_DATE('" & ValidTo & "' , '" & dateFormat & "') ")
			//    buildCondition.Append(vbCrLf & "       between main.valid_from and main.valid_to) OR main.VALID_TO IS NULL ")
			//    buildCondition.Append(vbCrLf & "     ) ")

			//ElseIf ValidFrom.Length > 0 And Not ValidTo.Length > 0 Then
			//    buildCondition.Append(vbCrLf & " AND ")
			//    buildCondition.Append(vbCrLf & "        (TO_DATE('" & ValidFrom & "' , '" & dateFormat & "') ")
			//    buildCondition.Append(vbCrLf & "       between main.valid_from and main.valid_to ")
			//    buildCondition.Append(vbCrLf & "     ) ")
			//ElseIf ValidFrom.Length > 0 And ValidTo.Length > 0 Then
			//    buildCondition.Append("     AND ((TO_DATE('" & ValidTo & "' , '" & dateFormat & "') between")
			//    buildCondition.Append("     main.valid_from and main.valid_to) OR")
			//    buildCondition.Append("     main.VALID_TO IS NULL")
			//    buildCondition.Append("     OR (TO_DATE('" & ValidFrom & "' , '" & dateFormat & "') between")
			//    buildCondition.Append("     main.valid_from and main.valid_to ))")
			//End If
			if (ActiveOnly == true) {
				buildCondition.Append(" AND MAIN.ACTIVE = 1 ");
				buildCondition.Append(" AND ( ");
				buildCondition.Append("        MAIN.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ");
				buildCondition.Append("       OR MAIN.VALID_TO IS NULL ");
				buildCondition.Append("     ) ");
			}

			//'Goutam : When From and To date are selected, any contract valid b/w selected period should list in the listing screen.
			if (!string.IsNullOrEmpty(ValidFrom) & string.IsNullOrEmpty(ValidTo)) {
				buildCondition.Append(" AND TO_DATE('" + ValidFrom + "', '" + dateFormat + "') BETWEEN TO_DATE(MAIN.VALID_FROM, '" + dateFormat + "') AND TO_DATE(NVL(MAIN.VALID_TO, SYSDATE), '" + dateFormat + "') ");
			}

			if (!string.IsNullOrEmpty(ValidTo) & string.IsNullOrEmpty(ValidFrom)) {
				buildCondition.Append(" AND TO_DATE('" + ValidTo + "', '" + dateFormat + "') BETWEEN TO_DATE(MAIN.VALID_FROM, '" + dateFormat + "') AND TO_DATE(NVL(MAIN.VALID_TO, SYSDATE), '" + dateFormat + "') ");
			}

			if (!string.IsNullOrEmpty(ValidFrom) & !string.IsNullOrEmpty(ValidTo)) {
				buildCondition.Append(" AND (TO_DATE(MAIN.VALID_FROM,'" + dateFormat + "') BETWEEN TO_DATE('" + ValidFrom.Trim() + "','" + dateFormat + "') AND TO_DATE('" + ValidTo.Trim() + "','" + dateFormat + "')  ");
				buildCondition.Append(" OR TO_DATE(NVL(MAIN.VALID_TO, SYSDATE) ,'" + dateFormat + "') BETWEEN TO_DATE('" + ValidFrom.Trim() + "','" + dateFormat + "') AND TO_DATE('" + ValidTo.Trim() + "','" + dateFormat + "'))  ");
			}
			//' End Goutam

			buildCondition.Append("     AND MAIN.CREATED_BY_FK IN");
			buildCondition.Append("     (SELECT UMT.USER_MST_PK");
			buildCondition.Append("     FROM USER_MST_TBL UMT");
			buildCondition.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
			buildCondition.Append("     (SELECT U.DEFAULT_LOCATION_FK");
			buildCondition.Append("     FROM USER_MST_TBL U");
			buildCondition.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
			if (ActiveOnly == true) {
				buildCondition.Append(" AND main.ACTIVE = 1 ");
			}

			if (CargoType == 1) {
				buildCondition.Append(" AND main.CARGO_TYPE = 1 ");
			} else {
				buildCondition.Append(" AND main.CARGO_TYPE = 2 ");
			}
			if (Convert.ToInt32(SortType) == 0) {
				buildCondition.Append(" AND 1=2 ");
			}

			strCondition = buildCondition.ToString();
			buildQuery.Append(" Select count(*) ");
			buildQuery.Append("      from ");
			buildQuery.Append("       RFQ_MAIN_SEA_TBL main, ");
			buildQuery.Append("       OPERATOR_MST_TBL ");
			buildQuery.Append("      where ");
			buildQuery.Append("       main.OPERATOR_MST_FK = OPERATOR_MST_PK ");
			buildQuery.Append("      " + strCondition);

			strSQL = buildQuery.ToString();

			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage)
				CurrentPage = 1;
			if (TotalRecords == 0)
				CurrentPage = 0;

			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;

			buildQuery.Remove(0, buildQuery.Length);

			buildQuery.Append(" Select * from ");
			buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
			buildQuery.Append("    ( Select ");
			buildQuery.Append(" NVL(main.ACTIVE, 0) ACTIVE,");
			buildQuery.Append(" RFQ_REF_NO, ");
			buildQuery.Append(" RFQ_DATE RFQ_DATE, ");
			buildQuery.Append(" OPERATOR_NAME, ");
			buildQuery.Append(" RFQ_MAIN_SEA_PK, ");
			buildQuery.Append("  DECODE(main.CARGO_TYPE,1,'FCL','LCL') As CARGO_TYPE,");
			//'Goutam : Date Sorting should be based on the date in the grid.
			//buildQuery.Append(vbCrLf & " to_Char(main.VALID_FROM, '" & dateFormat & "') VALID_FROM, ")
			//buildQuery.Append(vbCrLf & " to_Char(main.VALID_TO, '" & dateFormat & "') VALID_TO, ")
			buildQuery.Append(" main.VALID_FROM, ");
			buildQuery.Append(" main.VALID_TO, ");
			//'
			buildQuery.Append(" 0 POL, ");
			buildQuery.Append(" 0 POD ");
			buildQuery.Append("      from ");
			buildQuery.Append("       RFQ_MAIN_SEA_TBL main, ");
			buildQuery.Append("       OPERATOR_MST_TBL ");
			buildQuery.Append("      where ");
			buildQuery.Append("       main.OPERATOR_MST_FK = OPERATOR_MST_PK ");
			buildQuery.Append("               " + strCondition);
			buildQuery.Append("     Order By " + SortColumn + " DESC ");
			buildQuery.Append(" , RFQ_REF_NO DESC ");
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");
			buildQuery.Append("  where  ");
			buildQuery.Append("     SR_NO between " + start + " and " + last);
			strSQL = buildQuery.ToString();

			DataSet DS = null;
			try {
				DS = objWF.GetDataSet(strSQL);
				DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), POLID, POLName, PODID, PODName, SrOP));
				DataRelation trfRel = new DataRelation("rfqRelation", DS.Tables[0].Columns["RFQ_MAIN_SEA_PK"], DS.Tables[1].Columns["RFQ_MAIN_SEA_FK"], true);
				DS.Relations.Add(trfRel);
				return DS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

		private DataTable FetchChildFor(string RfqPKs = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string SrOp = "%")
		{

			System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
			string strCondition = "";
			string strSQL = "";


			if (RfqPKs.Trim().Length > 0) {
				buildCondition.Append(" and RFQ_MAIN_SEA_FK in (" + RfqPKs + ") ");
			}
			if (POLID.Trim().Length > 0) {
				buildCondition.Append(" and UPPER(PORTPOL.PORT_ID) LIKE '" + SrOp + POLID.ToUpper().Replace("'", "''") + "%'");
			}
			if (POLName.Trim().Length > 0) {
				buildCondition.Append(" and UPPER(PORTPOL.PORT_NAME) LIKE '" + SrOp + POLName.ToUpper().Replace("'", "''") + "%'");
			}
			if (PODID.Trim().Length > 0) {
				buildCondition.Append(" and UPPER(PORTPOD.PORT_ID) LIKE '" + SrOp + PODID.ToUpper().Replace("'", "''") + "%'");
			}
			if (PODName.Trim().Length > 0) {
				buildCondition.Append(" and UPPER(PORTPOD.PORT_NAME) LIKE '" + SrOp + PODName.ToUpper().Replace("'", "''") + "%'");
			}

			strCondition = buildCondition.ToString();

			buildQuery.Append(" Select * from ");
			buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
			buildQuery.Append("    ( Select DISTINCT ");
			buildQuery.Append("       RFQ_MAIN_SEA_FK, ");
			buildQuery.Append("       PORT_MST_POL_FK, ");
			buildQuery.Append("       PORTPOL.PORT_NAME PORT_POL_NAME, ");
			buildQuery.Append("       PORTPOL.PORT_ID PORT_POL_ID, ");
			buildQuery.Append("       PORTPOD.PORT_NAME PORT_POD_NAME, ");
			buildQuery.Append("       PORT_MST_POD_FK, ");
			buildQuery.Append("       PORTPOD.PORT_ID PORT_POD_ID, ");
			buildQuery.Append("       to_Char(VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
			buildQuery.Append("       to_Char(VALID_TO, '" + dateFormat + "') VALID_TO ");
			buildQuery.Append("      from ");
			buildQuery.Append("       RFQ_TRN_SEA_FCL_LCL, ");
			buildQuery.Append("       PORT_MST_TBL PORTPOL, ");
			buildQuery.Append("       PORT_MST_TBL PORTPOD ");
			buildQuery.Append("      where ");
			// JOIN CONDITION
			buildQuery.Append("       PORT_MST_POL_FK = PORTPOL.PORT_MST_PK AND ");
			buildQuery.Append("       PORT_MST_POD_FK = PORTPOD.PORT_MST_PK ");
			buildQuery.Append("               " + strCondition);
			buildQuery.Append("      Order By RFQ_MAIN_SEA_FK, PORTPOL.PORT_NAME, PORTPOD.PORT_NAME ASC ");
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");

			strSQL = buildQuery.ToString();

			WorkFlow objWF = new WorkFlow();
			DataTable dt = null;
			try {
				dt = objWF.GetDataTable(strSQL);
				int RowCnt = 0;
				int Rno = 0;
				int pk = 0;
				pk = -1;
				for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++) {
					if (Convert.ToInt32(dt.Rows[RowCnt]["RFQ_MAIN_SEA_FK"]) != pk) {
						pk = Convert.ToInt32(dt.Rows[RowCnt]["RFQ_MAIN_SEA_FK"]);
						Rno = 0;
					}
					Rno += 1;
					dt.Rows[RowCnt]["SR_NO"] = Rno;
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

		private string AllMasterPKs(DataSet ds)
		{
			try {
				Int16 RowCnt = default(Int16);
				Int16 ln = default(Int16);
				System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
				strBuilder.Append("-1,");
				for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++) {
					strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["RFQ_MAIN_SEA_PK"]).Trim() + ",");
				}
				strBuilder.Remove(strBuilder.Length - 1, 1);
				return strBuilder.ToString();
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		#region "Select Operator Fk when passing operator ID"
		public int SelectOperatorFk(string OperatorId)
		{
			try {
				string strSql = null;
				int operatorFk = 0;
				WorkFlow objWF = new WorkFlow();
				strSql = "SELECT OPERATOR_MST_PK FROM OPERATOR_MST_TBL WHERE OPERATOR_ID = '" + OperatorId + "' ";
				operatorFk = Convert.ToInt32(objWF.ExecuteScaler(strSql));
				return operatorFk;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Select Port Fk when passing Port ID"
		public int SelectPortFk(string Portid)
		{
			try {
				string strSql = null;
				int PortFk = 0;
				WorkFlow objWF = new WorkFlow();
				strSql = "SELECT PORT_MST_PK FROM PORT_MST_TBL WHERE PORT_ID = '" + Portid + "' ";
				PortFk = Convert.ToInt32(objWF.ExecuteScaler(strSql));
				return PortFk;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Select Trade Fk when passing Trade ID"
		public int SelectTradeFk(string Tradeid)
		{
			try {
				string strSql = null;
				int TradeFk = 0;
				WorkFlow objWF = new WorkFlow();
				strSql = "SELECT TRADE_MST_PK FROM TRADE_MST_TBL WHERE TRADE_ID = '" + Tradeid + "' ";
				TradeFk = Convert.ToInt32(objWF.ExecuteScaler(strSql));
				return TradeFk;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

	}
}
