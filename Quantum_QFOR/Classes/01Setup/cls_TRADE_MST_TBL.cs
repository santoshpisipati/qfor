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
using System.Text;

namespace Quantum_QFOR
{

    public class clsTRADE_MST_TBL : CommonFeatures
	{

		#region " Fetch All "

		public DataSet FetchAll(string TradeCode = "", string TradeName = "", int businessType = 3, int currentBusinessType = 3, string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool ActiveFlag = true, string SortType = " ASC ",
		Int32 flag = 0)
		{

			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			if (flag == 0) {
				strCondition += " AND 1=2";
			}
			if (TradeCode.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition += " AND UPPER(TRADE_CODE) LIKE '" + TradeCode.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition += " AND UPPER(TRADE_CODE) LIKE '%" + TradeCode.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition += " AND UPPER(TRADE_CODE) LIKE '%" + TradeCode.ToUpper().Replace("'", "''") + "%'" ;
				}
			}

			if (TradeName.Trim().Length > 0) {
				if (SearchType.ToString().Trim().Length > 0) {
					if (SearchType == "S") {
						strCondition += " AND UPPER(TRADE_NAME) LIKE '" + TradeName.ToUpper().Replace("'", "''") + "%'" ;
					} else {
						strCondition += " AND UPPER(TRADE_NAME) LIKE '%" + TradeName.ToUpper().Replace("'", "''") + "%'" ;
					}
				} else {
					strCondition += " AND UPPER(TRADE_NAME) LIKE '%" + TradeName.ToUpper().Replace("'", "''") + "%'" ;
				}
			}

			// Business rule
			// 1. Air    2. Sea      3. Both
			// If Current Business Type = Air then
			//   if Selected Business Type = Both then
			//      Show Air + Both
			//   else
			//      Show Air Only
			//   end if
			// If Current Business Type = Sea then
			//   if Selected Business Type = Both then
			//      Show Sea + Both
			//   else
			//      Show Sea Only
			//   end if
			// If Current Business Type = Both then
			//   if Selected Business Type = Both then
			//      Show Air + Sae + Both (All)
			//   else
			//      Selected Business Type Only
			//   end if
			// end if

			if (businessType == 3 & currentBusinessType == 3) {
				strCondition += " AND BUSINESS_TYPE IN (1,2,3) ";
			} else if (businessType == 3 & currentBusinessType == 2) {
				strCondition += " AND BUSINESS_TYPE IN (2,3) ";
			} else if (businessType == 3 & currentBusinessType == 1) {
				strCondition += " AND BUSINESS_TYPE IN (1,3) ";
			} else if (businessType > 0) {
				strCondition += " AND BUSINESS_TYPE = " + businessType + " ";
			}

			if (ActiveFlag == true) {
				strCondition += " AND ACTIVE_FLAG = 1 ";
			} else {
				strCondition += " ";
			}

			strSQL = "SELECT Count(*) from TRADE_MST_TBL where 1 = 1 ";
			strSQL += strCondition;

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

			// SR_NO, TRADE_MST_PK, ACTIVE_FLAG, TRADE_CODE, TRADE_NAME, COUNTRY_NAME, VERSION_NO
			strSQL = " select * from (";
			strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
			strSQL += "(SELECT  ";
			strSQL += "TRADE_MST_PK, ";
			strSQL += "NVL(ACTIVE_FLAG,0) ACTIVE_FLAG , ";
			strSQL += "TRADE_CODE, ";
			strSQL += "TRADE_NAME, ";
			strSQL += "decode(BUSINESS_TYPE,1 ,'Air', 2, 'Sea',3, 'Both') BUSINESS_TYPE, ";
			strSQL += "VERSION_NO ";
			strSQL += "FROM TRADE_MST_TBL where 1=1 ";
			strSQL += strCondition;
			strSQL += " order by " + SortColumn + SortType + " ) q  ) ";
			strSQL += " WHERE SR_NO  Between " + start + " and " + last;
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
		#region " Enhance Search Method "

		public string FetchTrade(string strCond)
		{

			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSERACH_IN = null;
			int LocationPk = 0;
			string strReq = null;
			int bizType = 0;
			var strNull = "";
			arr = strCond.Split('~');
			strReq =  Convert.ToString(arr.GetValue(0));
			strSERACH_IN = Convert.ToString(arr.GetValue(1));

            //adding by Thiyagarajan on 31/3/08 for Statement of Accounts    
            //If arr.Length = 3 Then
            if (arr.Length >= 3) {
				if ((Convert.ToString(arr.GetValue(2)) != null)) {
					bizType = Convert.ToInt32(arr.GetValue(2));
                }
			}

			//adding by Thiyagarajan on 31/3/08 for Statement of Accounts
			if (arr.Length > 3) {
				LocationPk = Convert.ToInt32(arr.GetValue(3));
            } else {
				LocationPk = 0;
			}
			//end

			try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_TRADE_PKG.GETTRADE_COMMON";

				var _with1 = selectCommand.Parameters;
				_with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
				_with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with1.Add("BUSINESS_TYPE_IN", bizType).Direction = ParameterDirection.Input;
				_with1.Add("LocationsPK", LocationPk).Direction = ParameterDirection.Input;
				_with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			//Manjunath  PTS ID:Sep-02  15/09/2011
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}

		#endregion
		#region "Fetch TradeName"
		public DataSet FetchTradeName(long TradePK = 0, long Biztype = 3, string FormName = null, string SrchCrta = null)
		{
			StringBuilder strSQL = new StringBuilder();
			WorkFlow objWF = new WorkFlow();
			Array arr = null;
			if (!string.IsNullOrEmpty(SrchCrta)) {
				arr = SrchCrta.Split('~');
			}

			if ((FormName != null)) {
				if (FormName == "FRTOUT") {
					strSQL.Append(" SELECT * ");
					strSQL.Append(" FROM (SELECT ");
					strSQL.Append(" TRADE_MST_PK, ");
					strSQL.Append(" TRADE_CODE, ");
					strSQL.Append(" TRADE_NAME, ");
					strSQL.Append(" '0' CHK ");
					strSQL.Append(" FROM ");
					strSQL.Append(" TRADE_MST_TBL TRD ");
					strSQL.Append(" WHERE ");
					strSQL.Append(" TRD.TRADE_MST_PK NOT IN (0," + TradePK + ") ");
					strSQL.Append(" AND TRD.ACTIVE_FLAG = 1 ");
					strSQL.Append(" AND TRD.TRADE_MST_PK IN( ");
					strSQL.Append(" SELECT SMT.TRADE_MST_FK FROM SECTOR_MST_TBL SMT WHERE 1=1 ");
					if (arr.Length > 0) {
						strSQL.Append(" AND (SMT.FROM_PORT_FK,SMT.TO_PORT_FK) IN(");
						strSQL.Append(" SELECT T.POLFK,T.PODFK FROM VIEW_FRTOUT_RPT T WHERE 1=1 ");
						if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(0)))) {
							strSQL.Append(" AND (FETCH_FRIEGHT_OUTSTANDING.GETINVOICES(T.JCPK,T.BUSINESS_TYPE," + Convert.ToString(arr.GetValue(2)) + ",T.JOB_TYPE) - (FETCH_FRIEGHT_OUTSTANDING.GETCOLLECTIONS(T.JCPK,T.BUSINESS_TYPE," + Convert.ToString(arr.GetValue(2)) + ")))>0");
						}
						if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(1)))) {
							strSQL.Append(" AND T.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arr.GetValue(2)) + ")");
						}
						if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(2)))) {
							strSQL.Append(" AND T.POLFK IN(" + Convert.ToString(arr.GetValue(2)) + ")");
						}
						if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(3)))) {
							strSQL.Append(" AND T.PODFK IN(" + Convert.ToString(arr.GetValue(3)) + ")");
						}
						if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(4)))) {
							strSQL.Append(" AND T.CUSTFK IN(" + Convert.ToString(arr.GetValue(4)) + ")");
						}
						if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(5)).Trim())) {
							strSQL.Append(" AND UPPER(T.VSL_ID)='" + Convert.ToString(arr.GetValue(5)).ToUpper() + "'");
						}
						if (Convert.ToInt32(arr.GetValue(5)) > 0) {
							strSQL.Append(" AND T.JOB_TYPE =" + Convert.ToInt32(arr.GetValue(6)));
						}
						if (Convert.ToInt32(arr.GetValue(7)) > 0 & Convert.ToInt32(arr.GetValue(7)) != 3) {
							strSQL.Append(" AND T.BUSINESS_TYPE =" + Convert.ToInt32(arr.GetValue(7)));
						}
						if (Convert.ToInt32(arr.GetValue(8)) > 0) {
							strSQL.Append(" AND T.PROCESS_TYPE =" + Convert.ToInt32(arr.GetValue(7)));
						}
						if (!string.IsNullOrEmpty(arr.GetValue(9).ToString().Trim()) & !string.IsNullOrEmpty(arr.GetValue(10).ToString().Trim())) {
							strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + arr.GetValue(9).ToString() + "','DD/MM/YYYY') AND TO_DATE('" + arr.GetValue(10).ToString() + "','DD/MM/YYYY')");
						} else if (!string.IsNullOrEmpty(arr.GetValue(9).ToString().Trim()) & string.IsNullOrEmpty(arr.GetValue(10).ToString().Trim
                            ())) {
							strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') >= TO_DATE('" + arr.GetValue(9) + "','DD/MM/YYYY')");
						} else if (string.IsNullOrEmpty(arr.GetValue(9).ToString().Trim()) & !string.IsNullOrEmpty(arr.GetValue(10).ToString().Trim())) {
							strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') <= TO_DATE('" + arr.GetValue(10) + "','DD/MM/YYYY')");
						}
						if (!string.IsNullOrEmpty(Convert.ToString(arr.GetValue(11)))) {
							strSQL.Append(" AND T.REF_GROUP_CUST_PK IN(" + Convert.ToInt32(arr.GetValue(11) + ")"));
						}
						strSQL.Append(" )");
					}
					strSQL.Append(" )");
					strSQL.Append(" UNION ");
					strSQL.Append(" SELECT ");
					strSQL.Append(" TRADE_MST_PK, ");
					strSQL.Append(" TRADE_CODE, ");
					strSQL.Append(" TRADE_NAME, ");
					strSQL.Append(" '1' CHK ");
					strSQL.Append(" FROM ");
					strSQL.Append(" TRADE_MST_TBL TRD ");
					strSQL.Append(" WHERE ");
					strSQL.Append("  TRD.TRADE_MST_PK IN (0, " + TradePK + ") ");
					strSQL.Append(" AND TRD.ACTIVE_FLAG = 1 ");
					strSQL.Append(" AND TRD.TRADE_MST_PK IN( ");
					strSQL.Append(" SELECT SMT.TRADE_MST_FK FROM SECTOR_MST_TBL SMT WHERE 1=1 ");
					if (arr.Length > 0) {
						strSQL.Append(" AND (SMT.FROM_PORT_FK,SMT.TO_PORT_FK) IN(");
						strSQL.Append(" SELECT T.POLFK,T.PODFK FROM VIEW_FRTOUT_RPT T WHERE 1=1 ");
						if (!string.IsNullOrEmpty(arr.GetValue(0).ToString())) {
							strSQL.Append(" AND (FETCH_FRIEGHT_OUTSTANDING.GETINVOICES(T.JCPK,T.BUSINESS_TYPE," + arr.GetValue(0).ToString() + ",T.JOB_TYPE) - (FETCH_FRIEGHT_OUTSTANDING.GETCOLLECTIONS(T.JCPK,T.BUSINESS_TYPE," + arr.GetValue(0).ToString() + ")))>0");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(1).ToString())) {
							strSQL.Append(" AND T.DEFAULT_LOCATION_FK IN(" + arr.GetValue(1).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(2).ToString())) {
							strSQL.Append(" AND T.POLFK IN(" + arr.GetValue(2).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(3).ToString())) {
							strSQL.Append(" AND T.PODFK IN(" + arr.GetValue(3).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(4).ToString())) {
                            strSQL.Append(" AND T.CUSTFK IN(" + arr.GetValue (4).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(5).ToString().Trim())) {
							strSQL.Append(" AND UPPER(T.VSL_ID)='" + arr.GetValue(5).ToString().ToUpper() + "'");
						}
						if (Convert.ToInt32(arr.GetValue(6).ToString()) > 0) {
							strSQL.Append(" AND T.JOB_TYPE =" + arr.GetValue(6).ToString());
						}
						if (Convert.ToInt32(arr.GetValue(7).ToString()) > 0 & Convert.ToInt32(arr.GetValue(7).ToString()) != 3) {
							strSQL.Append(" AND T.BUSINESS_TYPE =" + Convert.ToInt32(arr.GetValue(7).ToString()));
						}
						if (Convert.ToInt32(arr.GetValue(8).ToString()) > 0) {
							strSQL.Append(" AND T.PROCESS_TYPE =" + Convert.ToInt32(arr.GetValue(8).ToString()));
						}
						if (!string.IsNullOrEmpty(arr.GetValue(9).ToString().Trim()) & !string.IsNullOrEmpty(arr.GetValue(10).ToString().Trim())) {
							strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + arr.GetValue(9).ToString() + "','DD/MM/YYYY') AND TO_DATE('" + arr.GetValue(10).ToString() + "','DD/MM/YYYY')");
						} else if (!string.IsNullOrEmpty(arr.GetValue(9).ToString().Trim()) & string.IsNullOrEmpty(arr.GetValue(10).ToString().Trim())) {
							strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') >= TO_DATE('" + arr.GetValue(9).ToString() + "','DD/MM/YYYY')");
						} else if (string.IsNullOrEmpty(arr.GetValue(9).ToString().Trim()) & !string.IsNullOrEmpty(arr.GetValue(10).ToString().Trim())) {
							strSQL.Append(" AND TO_DATE(T.TRANSDATE,'DD/MM/YYYY') <= TO_DATE('" + arr.GetValue(10).ToString() + "','DD/MM/YYYY')");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(11).ToString())) {
							strSQL.Append(" AND T.REF_GROUP_CUST_PK IN(" + arr.GetValue(11).ToString() + ")");
						}
						strSQL.Append(")");
					}
					strSQL.Append(")) T ");
					strSQL.Append("ORDER BY CHK DESC,TRADE_CODE ");
				} else if (FormName == "TOPCUST") {
					strSQL.Append(" SELECT * ");
					strSQL.Append(" FROM (SELECT ");
					strSQL.Append(" TRADE_MST_PK, ");
					strSQL.Append(" TRADE_CODE, ");
					strSQL.Append(" TRADE_NAME, ");
					strSQL.Append(" '0' CHK ");
					strSQL.Append(" FROM ");
					strSQL.Append(" TRADE_MST_TBL TRD ");
					strSQL.Append(" WHERE ");
					strSQL.Append(" TRD.TRADE_MST_PK NOT IN (0," + TradePK + ") ");
					strSQL.Append(" AND TRD.ACTIVE_FLAG = 1 ");
					strSQL.Append(" AND TRD.TRADE_MST_PK IN( ");
					strSQL.Append(" SELECT SMT.TRADE_MST_FK FROM SECTOR_MST_TBL SMT WHERE 1=1 ");
					if (arr.Length > 0) {
						strSQL.Append(" AND (SMT.FROM_PORT_FK,SMT.TO_PORT_FK) IN(");
						strSQL.Append(" SELECT T.POLFK,T.PODFK FROM VIEW_TOPCUST_RPT T WHERE 1=1 ");
						if (!string.IsNullOrEmpty(arr.GetValue(0).ToString())) {
							strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + arr.GetValue(0).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(1).ToString())) {
							strSQL.Append("  AND T.CUSTFK IN(" + arr.GetValue(1).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(2).ToString())) {
							strSQL.Append("  AND T.POLFK IN(" + arr.GetValue(2).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(3).ToString())) {
							strSQL.Append("  AND T.PODFK IN(" + arr.GetValue(3).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(4).ToString().Trim()) & !string.IsNullOrEmpty(arr.GetValue(5).ToString().Trim())) {
							strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + arr.GetValue(4).ToString() + "','DD/MM/YYYY') AND TO_DATE('" + arr.GetValue(5).ToString() + "','DD/MM/YYYY')");
						} else if (!string.IsNullOrEmpty(arr.GetValue(4).ToString().Trim()) & string.IsNullOrEmpty(arr.GetValue(5).ToString().Trim())) {
							strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + arr.GetValue(4).ToString() + "','DD/MM/YYYY')");
						} else if (string.IsNullOrEmpty(arr.GetValue(4).ToString().Trim()) & !string.IsNullOrEmpty(arr.GetValue(5).ToString().Trim())) {
							strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + arr.GetValue(5).ToString() + "','DD/MM/YYYY')");
						}

						if (Convert.ToInt32(arr.GetValue(6).ToString()) > 0 & Convert.ToInt32(arr.GetValue(6).ToString()) != 3) {
							strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(arr.GetValue(6).ToString()));
						}
						if (Convert.ToInt32(arr.GetValue(7).ToString()) > 0) {
							strSQL.Append("  AND T.COMMODITY_GROUP_FK =" + Convert.ToInt32(arr.GetValue(7).ToString()));
						}
						if (Convert.ToInt32(arr.GetValue(8).ToString()) > 0) {
							strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(arr.GetValue(8).ToString()));
						}

						if (Convert.ToInt32(arr.GetValue(9).ToString()) > 0) {
							if (Convert.ToInt32(arr.GetValue(6).ToString()) > 0) {
								strSQL.Append("  AND T.CARGO_TYPE =" + Convert.ToInt32(arr.GetValue(9).ToString()));
							}
						}
						strSQL.Append(" AND ( NUMBEROF_TEUS(T.JCPK, T.PROCESS_TYPE, T.BUSINESS_TYPE," + Convert.ToInt32(arr.GetValue(9).ToString()) + ",T.JOB_TYPE)>0 OR");
						strSQL.Append(" CASE WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=1 THEN ");
						strSQL.Append(" FETCH_JOB_CARD_SEA_EXP_ACTREV( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ")");
						strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=2 THEN ");
						strSQL.Append(" FETCH_JOB_CARD_SEA_IMP_ACTREV( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ")");
						strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=1 THEN ");
						strSQL.Append(" FETCH_JOB_CARD_AIR_EXP_ACTREV( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ")");
						strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=2 THEN ");
						strSQL.Append(" FETCH_JOB_CARD_AIR_IMP_ACTREV( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ")");
						strSQL.Append(" WHEN T.JOB_TYPE=2 THEN ");
						strSQL.Append(" FETCH_CBJC_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)");
						strSQL.Append(" WHEN T.JOB_TYPE=3 THEN ");
						strSQL.Append(" FETCH_TPT_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)");
						strSQL.Append(" END >0)");
						strSQL.Append(")");
					}
					strSQL.Append(" )");
					strSQL.Append(" UNION ");
					strSQL.Append(" SELECT ");
					strSQL.Append(" TRADE_MST_PK, ");
					strSQL.Append(" TRADE_CODE, ");
					strSQL.Append(" TRADE_NAME, ");
					strSQL.Append(" '1' CHK ");
					strSQL.Append(" FROM ");
					strSQL.Append(" TRADE_MST_TBL TRD ");
					strSQL.Append(" WHERE ");
					strSQL.Append("  TRD.TRADE_MST_PK IN (0, " + TradePK + ") ");
					strSQL.Append(" AND TRD.ACTIVE_FLAG = 1 ");
					strSQL.Append(" AND TRD.TRADE_MST_PK IN( ");
					strSQL.Append(" SELECT SMT.TRADE_MST_FK FROM SECTOR_MST_TBL SMT WHERE 1=1 ");
					if (arr.Length > 0) {
						strSQL.Append(" AND (SMT.FROM_PORT_FK,SMT.TO_PORT_FK) IN(");
						strSQL.Append(" SELECT T.POLFK,T.PODFK FROM VIEW_TOPCUST_RPT T WHERE 1=1 ");
						if (!string.IsNullOrEmpty(arr.GetValue(0).ToString())) {
							strSQL.Append("  AND T.DEFAULT_LOCATION_FK IN(" + arr.GetValue(0).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(1).ToString())) {
							strSQL.Append("  AND T.CUSTFK IN(" + arr.GetValue(1).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(2).ToString())) {
							strSQL.Append("  AND T.POLFK IN(" + arr.GetValue(2).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(3).ToString())) {
							strSQL.Append("  AND T.PODFK IN(" + arr.GetValue(3).ToString() + ")");
						}
						if (!string.IsNullOrEmpty(arr.GetValue(4).ToString().Trim()) & !string.IsNullOrEmpty(arr.GetValue(5).ToString().Trim())) {
							strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') BETWEEN TO_DATE('" + arr.GetValue(4).ToString() + "','DD/MM/YYYY') AND TO_DATE('" + arr.GetValue(5).ToString() + "','DD/MM/YYYY')");
						} else if (!string.IsNullOrEmpty(arr.GetValue(4).ToString().Trim()) & string.IsNullOrEmpty(arr.GetValue(5).ToString().Trim())) {
							strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') >= TO_DATE('" + arr.GetValue(4).ToString() + "','DD/MM/YYYY')");
						} else if (string.IsNullOrEmpty(arr.GetValue(4).ToString().Trim()) & !string.IsNullOrEmpty(arr.GetValue(5).ToString().Trim())) {
							strSQL.Append(" AND TO_DATE(T.JCDATE,'DD/MM/YYYY') <= TO_DATE('" + arr.GetValue(5).ToString() + "','DD/MM/YYYY')");
						}

						if (Convert.ToInt32(arr.GetValue(6).ToString()) > 0 & Convert.ToInt32(arr.GetValue(6).ToString()) != 3) {
							strSQL.Append("  AND T.BUSINESS_TYPE =" + Convert.ToInt32(arr.GetValue(6).ToString()));
						}
						if (Convert.ToInt32(arr.GetValue(7).ToString()) > 0) {
							strSQL.Append("  AND T.COMMODITY_GROUP_FK =" + Convert.ToInt32(arr.GetValue(7).ToString()));
						}
						if (Convert.ToInt32(arr.GetValue(8).ToString()) > 0) {
							strSQL.Append("  AND T.PROCESS_TYPE =" + Convert.ToInt32(arr.GetValue(8).ToString()));
						}

						if (Convert.ToInt32(arr.GetValue(9).ToString()) > 0) {
							if (Convert.ToInt32(arr.GetValue(6).ToString()) > 0) {
								strSQL.Append("  AND T.CARGO_TYPE =" + Convert.ToInt32(arr.GetValue(9).ToString()));
							}
						}
						strSQL.Append(" AND ( NUMBEROF_TEUS(T.JCPK, T.PROCESS_TYPE, T.BUSINESS_TYPE," + Convert.ToInt32(arr.GetValue(9).ToString()) + ",T.JOB_TYPE)>0 OR");
						strSQL.Append(" CASE WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=1 THEN ");
						strSQL.Append(" FETCH_JOB_CARD_SEA_EXP_ACTREV( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ")");
						strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=2 AND T.PROCESS_TYPE=2 THEN ");
						strSQL.Append(" FETCH_JOB_CARD_SEA_IMP_ACTREV( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ")");
						strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=1 THEN ");
						strSQL.Append(" FETCH_JOB_CARD_AIR_EXP_ACTREV( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ")");
						strSQL.Append(" WHEN T.JOB_TYPE=1 AND T.BUSINESS_TYPE=1 AND T.PROCESS_TYPE=2 THEN ");
						strSQL.Append(" FETCH_JOB_CARD_AIR_IMP_ACTREV( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ")");
						strSQL.Append(" WHEN T.JOB_TYPE=2 THEN ");
						strSQL.Append(" FETCH_CBJC_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)");
						strSQL.Append(" WHEN T.JOB_TYPE=3 THEN ");
						strSQL.Append(" FETCH_TPT_ACTUAL_REVENUE( T.JCPK," + Convert.ToInt32(arr.GetValue(10).ToString()) + ",T.BUSINESS_TYPE,T.PROCESS_TYPE)");
						strSQL.Append(" END >0)");
						strSQL.Append(")");
					}
					strSQL.Append(")) T ");
					strSQL.Append("ORDER BY CHK DESC,TRADE_CODE ");
				}
			} else {
				strSQL.Append(" SELECT * ");
				strSQL.Append(" FROM (SELECT ");
				strSQL.Append(" TRADE_MST_PK, ");
				strSQL.Append(" TRADE_CODE, ");
				strSQL.Append(" TRADE_NAME, ");
				strSQL.Append(" '0' CHK ");
				strSQL.Append(" FROM ");
				strSQL.Append(" TRADE_MST_TBL TRD ");
				strSQL.Append(" WHERE ");
				//strSQL.Append(" trd.BUSINESS_TYPE IN (3," & Biztype & ") ")
				strSQL.Append(" TRD.TRADE_MST_PK NOT IN (0," + TradePK + ") ");
				if (Biztype != 3) {
					strSQL.Append(" AND  trd.BUSINESS_TYPE IN (3," + Biztype + ") ");
				}
				strSQL.Append(" AND TRD.ACTIVE_FLAG = 1 ");
				strSQL.Append(" UNION ");
				strSQL.Append(" SELECT ");
				strSQL.Append(" TRADE_MST_PK, ");
				strSQL.Append(" TRADE_CODE, ");
				strSQL.Append(" TRADE_NAME, ");
				strSQL.Append(" '1' CHK ");
				strSQL.Append(" FROM ");
				strSQL.Append(" TRADE_MST_TBL TRD ");
				strSQL.Append(" WHERE ");
				//strSQL.Append(" TRD.BUSINESS_TYPE IN (3," & Biztype & ") ")
				strSQL.Append("  TRD.TRADE_MST_PK IN (0, " + TradePK + ") ");
				if (Biztype != 3) {
					strSQL.Append(" AND  trd.BUSINESS_TYPE IN (3," + Biztype + ") ");
				}
				strSQL.Append(" AND TRD.ACTIVE_FLAG = 1 ");
				strSQL.Append(") T ");
				strSQL.Append("ORDER BY CHK DESC,TRADE_CODE ");
			}
			try {
				return (new WorkFlow()).GetDataSet(strSQL.ToString());
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

