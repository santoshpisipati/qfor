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
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class clsSalesPlanReport : CommonFeatures
	{

		#region "FetchListing"
		public DataSet FetchGridDetail(string LocPK, string fromDate, string toDate, string ExecPK = "", string CustPK = "", Int32 Status = 0, Int32 WeekNr = 0)
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

			try {
				sb.Append(" SELECT DISTINCT EMT.EMPLOYEE_MST_PK,EMT.EMPLOYEE_NAME");
				sb.Append("  FROM SALES_CALL_TRN         SCT,");
				sb.Append("       CUSTOMER_MST_TBL       CMT,");
				sb.Append("       S_CALL_TYPE_MST_TBL    STM,");
				sb.Append("       SAL_CAL_REASON_MST_TBL SRM,");
				sb.Append("       EMPLOYEE_MST_TBL       EMT,");
				sb.Append("       CUSTOMER_CONTACT_TRN   CCT");
				sb.Append(" WHERE SCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("   AND CCT.CUSTOMER_MST_FK(+) = SCT.CUSTOMER_MST_FK");
				sb.Append("   AND SRM.SAL_CAL_REASON_MST_TBL_PK(+) = SCT.SALES_CALL_REASON_FK");
				sb.Append("   AND STM.SALES_CALL_TYPE_PK(+) = SCT.SALES_CALL_TYPE_FK");
				sb.Append("   AND EMT.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK");
				sb.Append("   AND CMT.ACTIVE_FLAG = 1");

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND SCT.SALES_CALL_DT BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND SCT.SALES_CALL_DT >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND SCT.SALES_CALL_DT >= TO_DATE('" + toDate + "',dateformat) ");
				}

				if (ExecPK.Length > 0) {
					sb.Append(" AND SCT.EMPLOYEE_MST_FK in ( " + ExecPK + " ) ");
				}
				if (WeekNr != 0) {
					sb.Append(" AND TO_DATE(SCT.SALES_CALL_DT, 'dd/MM/yyyy') BETWEEN ");
					sb.Append(" TO_DATE(WEEK_DATE(" + WeekNr + "), 'dd/MM/yyyy') AND ");
					sb.Append(" TO_DATE(WEEK_DATE(" + WeekNr + ") + 6, 'dd/MM/yyyy') ");
				}
				if (CustPK.Length > 0) {
					sb.Append(" AND SCT.CUSTOMER_MST_FK in ( " + CustPK + " )");
				}

				if (LocPK.Length > 0) {
					sb.Append(" AND EMT.LOCATION_MST_FK in ( " + LocPK + " )");
				}

				if (Status > 0) {
					sb.Append("  And SCT.CALL_STATUS =" + Status);
				}

				sb.Append(" ORDER BY EMT.EMPLOYEE_NAME ");

				DA = objWK.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "EMPLOYEE");

				sb.Remove(0, sb.Length);
				sb.Append("SELECT CMT.CUSTOMER_NAME,");
				sb.Append("       CCT.NAME PERSON_TO_MEET_NAME,");
				//sb.Append("       TO_CHAR(SCT.SALES_CALL_DT, DATEFORMAT) SALES_CALL_DT,")
				sb.Append("       SCT.SALES_CALL_DT SALES_CALL_DT,");
				sb.Append("       SCT.FR_TIME,");
				sb.Append("       SCT.TO_TIME,");
				sb.Append("       STM.SALES_CALL_TYPE_ID,");
				sb.Append("       DECODE(SCT.PLAN_STATUS,");
				sb.Append("              '1',");
				sb.Append("              'Pending',");
				sb.Append("              '2',");
				sb.Append("              'Approved',");
				sb.Append("              '3',");
				sb.Append("              'Rejected',");
				sb.Append("              '4',");
				sb.Append("              'Cancelled',");
				sb.Append("              '5',");
				sb.Append("              'Closed') PLAN_STATUS,");
				sb.Append("       DECODE(SCT.PLAN_STATUS,");
				sb.Append("              '1',");
				sb.Append("              'Pending',");
				sb.Append("              '2',");
				sb.Append("              'Approved',");
				sb.Append("              '3',");
				sb.Append("              'Rejected',");
				sb.Append("              '4',");
				sb.Append("              'Cancelled',");
				sb.Append("              '5',");
				sb.Append("              'Closed') PLAN_STATUS_EXCEL,");
				sb.Append("       SRM.SAL_CAL_REASON SALES_CALL_REASON,");
				sb.Append("       SCT.NOTES PLANNERREMARKS,");
				sb.Append("       SCT.REMARKS_APPROVER APPROVER,");
				sb.Append("       SCT.REMARKS REMARK,");
				sb.Append("       EMT.EMPLOYEE_MST_PK,");
				sb.Append("       SCT.SALES_CALL_PK");
				sb.Append("  FROM SALES_CALL_TRN         SCT,");
				sb.Append("       CUSTOMER_MST_TBL       CMT,");
				sb.Append("       S_CALL_TYPE_MST_TBL    STM,");
				sb.Append("       SAL_CAL_REASON_MST_TBL SRM,");
				sb.Append("       EMPLOYEE_MST_TBL       EMT,");
				sb.Append("       CUSTOMER_CONTACT_TRN   CCT");
				sb.Append(" WHERE SCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
				sb.Append("   AND CCT.CUST_CONTACT_PK(+) = SCT.PERSON_TO_MEET_PK");
				sb.Append("   AND SRM.SAL_CAL_REASON_MST_TBL_PK(+) = SCT.SALES_CALL_REASON_FK");
				sb.Append("   AND STM.SALES_CALL_TYPE_PK(+) = SCT.SALES_CALL_TYPE_FK");
				sb.Append("   AND EMT.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK");
				sb.Append("   AND CMT.ACTIVE_FLAG = 1");

				if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
					sb.Append(" AND SCT.SALES_CALL_DT BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
				} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
					sb.Append(" AND SCT.SALES_CALL_DT >= TO_DATE('" + fromDate + "',dateformat) ");
				} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
					sb.Append(" AND SCT.SALES_CALL_DT >= TO_DATE('" + toDate + "',dateformat) ");
				}

				if (ExecPK.Length > 0) {
					sb.Append(" AND SCT.EMPLOYEE_MST_FK in ( " + ExecPK + " ) ");
				}
				if (WeekNr != 0) {
					sb.Append(" AND TO_DATE(SCT.SALES_CALL_DT, 'dd/MM/yyyy') BETWEEN ");
					sb.Append(" TO_DATE(WEEK_DATE(" + WeekNr + "), 'dd/MM/yyyy') AND ");
					sb.Append(" TO_DATE(WEEK_DATE(" + WeekNr + ") + 6, 'dd/MM/yyyy') ");
				}
				if (CustPK.Length > 0) {
					sb.Append(" AND SCT.CUSTOMER_MST_FK in ( " + CustPK + " )");
				}

				if (LocPK.Length > 0) {
					sb.Append(" AND EMT.LOCATION_MST_FK in ( " + LocPK + " )");
				}

				if (Status > 0) {
					sb.Append("  And SCT.CALL_STATUS =" + Status);
				}
				sb.Append(" ORDER BY SCT.SALES_CALL_DT desc,SCT.FR_TIME,SCT.TO_TIME ");

				DA = objWK.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "CUSTOMER");

				DataRelation rel = new DataRelation("EMPLOYEE", new DataColumn[] { MainDS.Tables[0].Columns["EMPLOYEE_MST_PK"] }, new DataColumn[] { MainDS.Tables[1].Columns["EMPLOYEE_MST_PK"] });

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

		#region "GetReportData"
		public DataSet GetReportData(string LocPK, string fromDate, string toDate, string ExecPK = "", string CustPK = "", Int32 Status = 0, Int32 WeekNr = 0)
		{

			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

			sb.Append("SELECT emt.employee_name,CMT.CUSTOMER_NAME,");
			sb.Append("       CCT.NAME PERSON_TO_MEET_NAME,");
			sb.Append("       TO_CHAR(SCT.SALES_CALL_DT, DATEFORMAT) SALES_CALL_DT,");
			sb.Append("       SCT.FR_TIME,");
			sb.Append("       SCT.TO_TIME,");
			sb.Append("       STM.SALES_CALL_TYPE_ID,");
			sb.Append("       DECODE(SCT.PLAN_STATUS,");
			sb.Append("              '1',");
			sb.Append("              'Pending',");
			sb.Append("              '2',");
			sb.Append("              'Approved',");
			sb.Append("              '3',");
			sb.Append("              'Rejected',");
			sb.Append("              '4',");
			sb.Append("              'Cancelled',");
			sb.Append("              '5',");
			sb.Append("              'Closed') PLAN_STATUS,");
			sb.Append("       SRM.SAL_CAL_REASON SALES_CALL_REASON,");
			sb.Append("       SCT.NOTES PLANNERREMARKS,");
			sb.Append("       SCT.REMARKS_APPROVER APPROVER,");
			sb.Append("       SCT.REMARKS REMARK,");
			sb.Append("       EMT.EMPLOYEE_MST_PK");
			sb.Append("  FROM SALES_CALL_TRN         SCT,");
			sb.Append("       CUSTOMER_MST_TBL       CMT,");
			sb.Append("       S_CALL_TYPE_MST_TBL    STM,");
			sb.Append("       SAL_CAL_REASON_MST_TBL SRM,");
			sb.Append("       EMPLOYEE_MST_TBL       EMT,");
			sb.Append("       CUSTOMER_CONTACT_TRN   CCT");
			sb.Append(" WHERE SCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
			sb.Append("   AND CCT.CUSTOMER_MST_FK(+) = SCT.CUSTOMER_MST_FK");
			sb.Append("   AND SRM.SAL_CAL_REASON_MST_TBL_PK(+) = SCT.SALES_CALL_REASON_FK");
			sb.Append("   AND STM.SALES_CALL_TYPE_PK(+) = SCT.SALES_CALL_TYPE_FK");
			sb.Append("   AND EMT.EMPLOYEE_MST_PK = SCT.EMPLOYEE_MST_FK");
			sb.Append("   AND CMT.ACTIVE_FLAG = 1");


			if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate)))) {
				sb.Append(" AND SCT.SALES_CALL_DT BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat)  ");
			} else if (!(fromDate == null | string.IsNullOrEmpty(fromDate))) {
				sb.Append(" AND SCT.SALES_CALL_DT >= TO_DATE('" + fromDate + "',dateformat) ");
			} else if (!(toDate == null | string.IsNullOrEmpty(toDate))) {
				sb.Append(" AND SCT.SALES_CALL_DT >= TO_DATE('" + toDate + "',dateformat) ");
			}

			if (ExecPK.Length > 0) {
				sb.Append(" AND SCT.EMPLOYEE_MST_FK in ( " + ExecPK + " ) ");
			}
			if (WeekNr != 0) {
				sb.Append(" AND TO_DATE(SCT.SALES_CALL_DT, 'dd/MM/yyyy') BETWEEN ");
				sb.Append(" TO_DATE(WEEK_DATE(" + WeekNr + "), 'dd/MM/yyyy') AND ");
				sb.Append(" TO_DATE(WEEK_DATE(" + WeekNr + ") + 6, 'dd/MM/yyyy') ");
			}
			if (CustPK.Length > 0) {
				sb.Append(" AND SCT.CUSTOMER_MST_FK in ( " + CustPK + " )");
			}

			if (LocPK.Length > 0) {
				sb.Append(" AND EMT.LOCATION_MST_FK in ( " + LocPK + " )");
			}

			if (Status > 0) {
				sb.Append("  And SCT.CALL_STATUS =" + Status);
			}

			sb.Append(" ORDER BY SCT.SALES_CALL_DT desc");

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

		#region "GetLocation"
		public DataSet GetLocation(string userLocPK)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			string strReturn = null;
			WorkFlow objWF = new WorkFlow();
			OracleDataReader dr = null;
			try {
				strQuery.Append("");
				strQuery.Append("  SELECT ");
				strQuery.Append("      LMT.LOCATION_MST_PK, ");
				strQuery.Append("       LMT.LOCATION_ID  ");
				strQuery.Append("  FROM LOCATION_MST_TBL LMT ");
				strQuery.Append("       where LMT.LOCATION_MST_PK =" + userLocPK);

				return objWF.GetDataSet(strQuery.ToString());
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "fetchexecutive"
		public string fetchexecutive(string exepk)
		{
			WorkFlow objWF = new WorkFlow();
			string SQL = null;
			DataSet dsemp = null;
			string res = "";
			Int32 i = default(Int32);
			try {
				SQL = " select emp.employee_name from employee_mst_tbl emp,sales_call_trn trn where trn.created_by_fk=emp.employee_mst_pk ";
				SQL += " and trn.created_by_fk in (" + exepk + ") group by emp.employee_name ";
				dsemp = objWF.GetDataSet(SQL);
				if (dsemp.Tables.Count > 0) {
					if (dsemp.Tables[0].Rows.Count > 0) {
						for (i = 0; i <= dsemp.Tables[0].Rows.Count - 1; i++) {
							if (i == 0) {
								res = Convert.ToString(dsemp.Tables[0].Rows[i][0]);
							} else {
								res += "," + dsemp.Tables[0].Rows[i][0];
							}
						}
					}
				}
				return res;
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Executive Enhance Search"
		public string FetchCustomer_SalesReport(string strcond, string loc = "0")
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strReq = null;
			string strSERACH_IN = null;
			var strNull = "";
			arr = strcond.Split('~');
			if (arr.Length > 0)
				strReq = Convert.ToString(arr.GetValue(0));
			if (arr.Length > 1)
				strSERACH_IN = Convert.ToString(arr.GetValue(1));

			try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_EXECUTIVE_PKG.GET_CUSTOMER";

				var _with1 = selectCommand.Parameters;
				_with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
				_with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with1.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
				_with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
		public string FetchCustomer_SalesReports(string strcond)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strReq = null;
			string strSERACH_IN = null;
			var strNull = "";
			string location = null;
			string Emp_fk = null;
			arr = strcond.Split('~');
			if (arr.Length > 0)
				strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
				strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
				location = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
				Emp_fk = Convert.ToString(arr.GetValue(3));
            try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_EXECUTIVE_PKG.GET_CUSTOMER";

				var _with2 = selectCommand.Parameters;
				_with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
				_with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with2.Add("LOCATION_MST_FK_IN", getDefault(location, 0)).Direction = ParameterDirection.Input;
				_with2.Add("EMPLOYEE_MST_FK_IN", getDefault(Emp_fk, 0)).Direction = ParameterDirection.Input;
				_with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

		#region "ActiveExecutive"
		public DataTable ActiveExecutive(string loc, string exePk = "0", Int32 CurrentPage = 0, Int32 TotalPage = 0)
		{
			StringBuilder Strsql = new StringBuilder();
			StringBuilder strcount = new StringBuilder();
			string strcondition = "";
			string strcond = "";
			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			WorkFlow objWF = new WorkFlow();
			string DefaultContainers = null;
			try {
				if (exePk.Length > 0 & exePk != "0") {
					Strsql.Append(" select ROWNUM SR_NO, QRY.* from ( select DISTINCT EMP.EMPLOYEE_MST_PK,EMP.EMPLOYEE_ID,EMP.EMPLOYEE_NAME,SCT.PERSON_MET AS \"PIC\" , DECODE(UMT.BUSINESS_TYPE,1,'AIR',2,'SEA') AS \"BIZ TYPE\",'1' CHK ");
					Strsql.Append("  FROM USER_MST_TBL  UMT,EMPLOYEE_MST_TBL EMP,LOCATION_MST_TBL LMT,SALES_CALL_TRN SCT,CUSTOMER_MST_TBL CMT where ");
					Strsql.Append(" UMT.EMPLOYEE_MST_FK = EMP.EMPLOYEE_MST_PK AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK AND UMT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK AND SCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) AND UMT.IS_ACTIVATED = 1 ");
					if (Convert.ToInt32(getDefault(loc, 0)) != 0) {
						Strsql.Append(" and LMT.LOCATION_MST_PK IN (" + objWF.ExecuteScaler("select GET_WORK_PORT_LOCS(" + loc + ") from dual") + ")");
					}
					Strsql.Append(" and EMP.EMPLOYEE_MST_PK in (" + exePk + " )");

					Strsql.Append(" union ");

					Strsql.Append(" select DISTINCT EMP.EMPLOYEE_MST_PK,EMP.EMPLOYEE_ID,EMP.EMPLOYEE_NAME,SCT.PERSON_MET AS \"PIC\" , DECODE(UMT.BUSINESS_TYPE,1,'AIR',2,'SEA') AS \"BIZ TYPE\",'false' CHK from USER_MST_TBL  UMT,EMPLOYEE_MST_TBL EMP,LOCATION_MST_TBL LMT,SALES_CALL_TRN SCT,CUSTOMER_MST_TBL CMT");
					Strsql.Append(" where UMT.EMPLOYEE_MST_FK = EMP.EMPLOYEE_MST_PK AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK AND UMT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK AND SCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) AND UMT.IS_ACTIVATED = 1 ");

					if (Convert.ToInt32(getDefault(loc, 0)) != 0) {
						Strsql.Append(" and LMT.LOCATION_MST_PK IN (" + objWF.ExecuteScaler("select GET_WORK_PORT_LOCS(" + loc + ") from dual") + ")");
					}

					Strsql.Append(" and EMP.EMPLOYEE_MST_PK not in (" + exePk + " ) order by chk,EMPLOYEE_ID ) QRY");
				} else {
					Strsql.Append(" select ROWNUM SR_NO, QRY.* from ( select DISTINCT EMP.EMPLOYEE_MST_PK,EMP.EMPLOYEE_ID,EMP.EMPLOYEE_NAME,SCT.PERSON_MET AS \"PIC\" , DECODE(UMT.BUSINESS_TYPE,1,'AIR',2,'SEA') AS \"BIZ TYPE\",'false' CHK ");
					Strsql.Append("  from USER_MST_TBL  UMT,EMPLOYEE_MST_TBL EMP,LOCATION_MST_TBL LMT,SALES_CALL_TRN SCT,CUSTOMER_MST_TBL CMT where ");
					Strsql.Append(" UMT.EMPLOYEE_MST_FK = EMP.EMPLOYEE_MST_PK AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK AND UMT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK AND SCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+) AND UMT.IS_ACTIVATED = 1 ");
					if (Convert.ToInt32(getDefault(loc, 0)) != 0) {
						Strsql.Append(" and LMT.LOCATION_MST_PK IN (" + objWF.ExecuteScaler("select GET_WORK_PORT_LOCS(" + loc + ") from dual") + ")");
					}
					Strsql.Append(" order by chk, EMPLOYEE_ID ) QRY ");
				}


				TotalRecords = Convert.ToInt32(objWF.ExecuteScaler("SELECT COUNT(*) FROM ( " + Strsql.ToString() + " ) QUE"));
				TotalPage = TotalRecords / 15;

				if (TotalRecords % RecordsPerPage != 0) {
					TotalPage += 1;
				}
				if (CurrentPage > TotalPage) {
					CurrentPage = 1;
				}
				if (TotalRecords == 0) {
					CurrentPage = 0;
				}
				last = CurrentPage * 15;
				start = (CurrentPage - 1) * 15 + 1;
				DataTable dt = objWF.GetDataTable(" SELECT * FROM (" + Strsql.ToString() + " )MainQuery WHERE MainQuery.SR_NO between " + start + " and " + last);
				return dt;
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "CustomerSelection"
		public string FetchExecutive_All(string strcond)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strReq = null;
			string strSERACH_IN = null;
			var strNull = "";
			string location = null;
			arr = strcond.Split('~');
			if (arr.Length > 0)
				strReq = Convert.ToString(arr.GetValue(0));
			if (arr.Length > 1)
				strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
				location = Convert.ToString(arr.GetValue(2));
            try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_EXECUTIVE_PKG.GET_EXECUTIVE_ALL";

				var _with3 = selectCommand.Parameters;
				_with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
				_with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with3.Add("LOCATION_MST_FK_IN", getDefault(location, 0)).Direction = ParameterDirection.Input;
				_with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
		public DataTable CustomerSelection(string loc, string custPk = "0", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 empPk = 0, string formdate = "", string Todate = "", string IngStatus = "", int WeekNr = 0)
		{
			StringBuilder Strsql = new StringBuilder();
			StringBuilder strcount = new StringBuilder();
			string strcondition = "";
			string strcond = "";
			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			WorkFlow objWF = new WorkFlow();
			string DefaultContainers = null;

			try {
				if (custPk.Length > 0 & custPk != "0") {
					Strsql.Append(" select ROWNUM SR_NO, QRY.* from ( select DISTINCT CMT.CUSTOMER_MST_PK,CMT.CUSTOMER_ID,CMT.CUSTOMER_NAME,CCD.ADM_CONTACT_PERSON,'1' CHK ");
					Strsql.Append(" FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD,SALES_CALL_TRN SCT  where ");
					Strsql.Append(" CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND CMT.ACTIVE_FLAG = 1 and SCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK  AND CCD.CUSTOMER_MST_FK = SCT.CUSTOMER_MST_FK ");
					if (Convert.ToInt32(getDefault(loc, 0)) != 0) {
						Strsql.Append(" and CCD.ADM_LOCATION_MST_FK IN  (" + loc + ")");
					}
                    //  Strsql.Append(" and CMT.CUSTOMER_MST_PK in (" & custPk & " )")
                    if (Convert.ToInt32(getDefault(custPk, 0)) != 0)
                    {
                        Strsql.Append(" AND SCT.CUSTOMER_MST_FK in ( " + custPk + " )");
					}
                    if (Convert.ToInt32(getDefault(IngStatus, 0)) != 0)
                    {
                        Strsql.Append("  And SCT.CALL_STATUS =" + IngStatus);
					}
                    if (Convert.ToInt32(getDefault(empPk, 0)) != 0)
                    {
                        Strsql.Append(" AND SCT.EMPLOYEE_MST_FK =" + empPk);
					}
					if (formdate != "0" & Todate != "0") {
						if (!((formdate == null | string.IsNullOrEmpty(formdate)) & (Todate == null | string.IsNullOrEmpty(Todate)))) {
							Strsql.Append(" AND SCT.SALES_CALL_DT BETWEEN TO_DATE('" + formdate + "',dateformat)  AND TO_DATE('" + Todate + "',dateformat)  ");
						}
					}

					if (WeekNr != 0) {
						Strsql.Append(" AND TO_DATE(SCT.SALES_CALL_DT, 'dd/MM/yyyy') BETWEEN ");
						Strsql.Append(" TO_DATE(WEEK_DATE(" + WeekNr + "), 'dd/MM/yyyy') AND ");
						Strsql.Append(" TO_DATE(WEEK_DATE(" + WeekNr + ") + 6, 'dd/MM/yyyy') ");
					}

					Strsql.Append(" union ");
					Strsql.Append(" select DISTINCT CMT.CUSTOMER_MST_PK,CMT.CUSTOMER_ID,CMT.CUSTOMER_NAME,CCD.ADM_CONTACT_PERSON,'false' CHK FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD,SALES_CALL_TRN SCT ");
					Strsql.Append(" where CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND CMT.ACTIVE_FLAG = 1 and SCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK  AND CCD.CUSTOMER_MST_FK = SCT.CUSTOMER_MST_FK  ");
                    if (Convert.ToInt32(getDefault(loc, 0)) != 0)
                    {
                        Strsql.Append(" and CCD.ADM_LOCATION_MST_FK IN  (" + loc + ")");
                    }
                    //  Strsql.Append(" and CMT.CUSTOMER_MST_PK in (" & custPk & " )")
                    if (Convert.ToInt32(getDefault(custPk, 0)) != 0)
                    {
                        Strsql.Append(" AND SCT.CUSTOMER_MST_FK in ( " + custPk + " )");
                    }
                    if (Convert.ToInt32(getDefault(IngStatus, 0)) != 0)
                    {
                        Strsql.Append("  And SCT.CALL_STATUS =" + IngStatus);
                    }
                    if (Convert.ToInt32(getDefault(empPk, 0)) != 0)
                    {
                        Strsql.Append(" AND SCT.EMPLOYEE_MST_FK =" + empPk);
                    }
                    if (formdate != "0" & Todate != "0") {
						if (!((formdate == null | string.IsNullOrEmpty(formdate)) & (Todate == null | string.IsNullOrEmpty(Todate)))) {
							Strsql.Append(" AND SCT.SALES_CALL_DT BETWEEN TO_DATE('" + formdate + "',dateformat)  AND TO_DATE('" + Todate + "',dateformat)  ");
						}
					}
					Strsql.Append(" and CMT.CUSTOMER_MST_PK not in (" + custPk + " ) order by chk,CUSTOMER_ID ) QRY");
				} else {
					Strsql.Append(" select ROWNUM SR_NO, QRY.* from ( select DISTINCT CMT.CUSTOMER_MST_PK,CMT.CUSTOMER_ID,CMT.CUSTOMER_NAME,CCD.ADM_CONTACT_PERSON,'false' CHK ");
					Strsql.Append("  FROM CUSTOMER_MST_TBL CMT, CUSTOMER_CONTACT_DTLS CCD,SALES_CALL_TRN SCT where ");
					Strsql.Append(" CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK(+) AND CMT.ACTIVE_FLAG = 1 and SCT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK  AND CCD.CUSTOMER_MST_FK = SCT.CUSTOMER_MST_FK");
                    if (Convert.ToInt32(getDefault(loc, 0)) != 0)
                    {
                        Strsql.Append(" and CCD.ADM_LOCATION_MST_FK IN  (" + loc + ")");
                    }
                    //  Strsql.Append(" and CMT.CUSTOMER_MST_PK in (" & custPk & " )")
                    if (Convert.ToInt32(getDefault(custPk, 0)) != 0)
                    {
                        Strsql.Append(" AND SCT.CUSTOMER_MST_FK in ( " + custPk + " )");
                    }
                    if (Convert.ToInt32(getDefault(IngStatus, 0)) != 0)
                    {
                        Strsql.Append("  And SCT.CALL_STATUS =" + IngStatus);
                    }
                    if (Convert.ToInt32(getDefault(empPk, 0)) != 0)
                    {
                        Strsql.Append(" AND SCT.EMPLOYEE_MST_FK =" + empPk);
                    }
                    if (formdate != "0" & Todate != "0") {
						if (!((formdate == null | string.IsNullOrEmpty(formdate)) & (Todate == null | string.IsNullOrEmpty(Todate)))) {
							Strsql.Append(" AND SCT.SALES_CALL_DT BETWEEN TO_DATE('" + formdate + "',dateformat)  AND TO_DATE('" + Todate + "',dateformat)  ");
						}
					}
					if (WeekNr != 0) {
						Strsql.Append(" AND TO_DATE(SCT.SALES_CALL_DT, 'dd/MM/yyyy') BETWEEN ");
						Strsql.Append(" TO_DATE(WEEK_DATE(" + WeekNr + "), 'dd/MM/yyyy') AND ");
						Strsql.Append(" TO_DATE(WEEK_DATE(" + WeekNr + ") + 6, 'dd/MM/yyyy') ");
					}
					Strsql.Append(" order by chk, CUSTOMER_ID ) QRY ");
				}


				TotalRecords = Convert.ToInt32(objWF.ExecuteScaler("SELECT COUNT(*) FROM ( " + Strsql.ToString() + " ) QUE"));
				TotalPage = TotalRecords / 15;

				if (TotalRecords % RecordsPerPage != 0) {
					TotalPage += 1;
				}
				if (CurrentPage > TotalPage) {
					CurrentPage = 1;
				}
				if (TotalRecords == 0) {
					CurrentPage = 0;
				}
				last = CurrentPage * 15;
				start = (CurrentPage - 1) * 15 + 1;
				DataTable dt = objWF.GetDataTable(" SELECT * FROM (" + Strsql.ToString() + " )MainQuery WHERE MainQuery.SR_NO between " + start + " and " + last);
				return dt;
			} catch (OracleException OraExp) {
				throw OraExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Function to check whether a user is an administrator or not"
		public int IsAdministrator()
		{
			string strSQL = null;
			Int16 Admin = default(Int16);
			WorkFlow objWF = new WorkFlow();
			strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
			strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
			strSQL = strSQL + "AND U.USER_MST_PK = " + HttpContext.Current.Session["USER_PK"];
			try {
				Admin = Convert.ToInt16(objWF.ExecuteScaler(strSQL.ToString()));
				if (Admin == 1) {
					return 1;
				} else {
					return 0;
				}

			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}

		}
		#endregion


	}
}




