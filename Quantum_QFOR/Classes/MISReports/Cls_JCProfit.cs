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
using System.Web;
namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_JCProfit : CommonFeatures
	{

        /// <summary>
        /// The type
        /// </summary>
        string type;
        #region "Procedure and Functions"

        /// <summary>
        /// Gets or sets the get procedure.
        /// </summary>
        /// <value>
        /// The get procedure.
        /// </value>
        public string GetProcedure {
			get {
				if (type == "SEAEXP") {
					return "FETCH_JCPROFIT_RPT_PKG,FETCH_DATA";
				} else if (type == "AIREXP") {
					return "FETCH_JCPROFIT_RPT_PKG,FETCH_DATA_AIR_EXP";
				} else if (type == "SEAIMP") {
					return "FETCH_JCPROFIT_RPT_PKG,FETCH_DATA_SEA_IMP";
				} else if (type == "AIRIMP") {
					return "FETCH_JCPROFIT_RPT_PKG,FETCH_DATA_AIR_IMP";
				} else if (type == "ALL") {
					return "FETCH_JCPROFIT_RPT_PKG,FETCH_DATA_ALL";
				} else if (type == "ExclPOL") {
					return "FETCH_JCPROFIT_RPT_PKG,FETCH_DATA_ALL_EXCL_POL";
				}
                return "";
			}
			set { type =  value; }
           
		}
        /// <summary>
        /// Gets or sets the get job procedure.
        /// </summary>
        /// <value>
        /// The get job procedure.
        /// </value>
        public string GetJobProcedure {
			get {
				if (type == "SEAEXP") {
					return "FETCH_JCPROFIT_RPT_PKG,JCFETCH_DATA";
				} else if (type == "AIREXP") {
					return "FETCH_JCPROFIT_RPT_PKG,JCFETCH_DATA_AIR_EXP";
				} else if (type == "SEAIMP") {
					return "FETCH_JCPROFIT_RPT_PKG,JCFETCH_DATA_SEA_IMP";
				} else if (type == "AIRIMP") {
					return "FETCH_JCPROFIT_RPT_PKG,JCFETCH_DATA_AIR_IMP";
				} else if (type == "ALL") {
					//Return "FETCH_JCPROFIT_RPT_PKG,JCFETCH_DATA_ALL"
					return "FETCH_JCPROFIT_RPT_PKG,JCFETCH_DATA_ALL_NEW";
				}
                return "";
            }
			set { type = value; }
		}
        /// <summary>
        /// Gets or sets the get BNM procedure customer.
        /// </summary>
        /// <value>
        /// The get BNM procedure customer.
        /// </value>
        public string GetBNMProcedureCust {
			get {
				if (type == "GRPExclPOL") {
					return "FETCH_JCPROFIT_RPT_BNM_PKG,FETCH_DATA_GRP_EXL_POL";
				} else if (type == "GRP") {
					return "FETCH_JCPROFIT_RPT_BNM_PKG,FETCH_DATA_GRP";
				} else if (type == "ExclPOL") {
					return "FETCH_JCPROFIT_RPT_BNM_PKG,FETCH_DATA_ALL_BNM_EXCL_POL";
				} else if (type == "ALL") {
					return "FETCH_JCPROFIT_RPT_BNM_PKG,FETCH_DATA_ALL_BNM";
				}
                return "";
            }
			set { type = value; }
		}
        /// <summary>
        /// Fetch_s the data.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="ChkONLD">The CHK onld.</param>
        /// <param name="FROM_DATE">The fro m_ date.</param>
        /// <param name="TO_DATE">The t o_ date.</param>
        /// <param name="LCL_FCL">The lc l_ FCL.</param>
        /// <param name="POL_POD">The po l_ pod.</param>
        /// <param name="CUSTOMER">The customer.</param>
        /// <param name="LOCATION">The location.</param>
        /// <param name="CURRENCY">The currency.</param>
        /// <param name="COMMODITY_GROUP">The commodit y_ group.</param>
        /// <param name="JObpk">The j obpk.</param>
        /// <param name="TOP">The top.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="Column">The column.</param>
        /// <param name="ServiceType">Type of the service.</param>
        /// <param name="Excel">The excel.</param>
        /// <returns></returns>
        public DataSet Fetch_Data(Int32 type, string SortColumn, Int32 ChkONLD, string FROM_DATE = "", string TO_DATE = "", string LCL_FCL = "1", string POL_POD = "", string CUSTOMER = "", string LOCATION = "", string CURRENCY = "",
		string COMMODITY_GROUP = "", string JObpk = "", string TOP = "", Int32 CurrentPage = 1, Int32 TotalPage = 0, string SortType = " DESC ", int Column = 0, int ServiceType = 0, int Excel = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataTable dtTrade = null;
			DataTable dtCust = null;
			DataTable dtLocation = null;
			DataTable dtCommodity = null;
			DataSet dsAll = null;

			string[] strPKGProc = null;
			try {
				objWF.MyCommand.Parameters.Clear();
				CURRENCY = Convert.ToString(HttpContext.Current.Session["Currency_mst_pk"]);
				var _with1 = objWF.MyCommand.Parameters;
				_with1.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;
				_with1.Add("TO_DATE", getDefault(TO_DATE, "")).Direction = ParameterDirection.Input;
				_with1.Add("LCL_FCL", getDefault(LCL_FCL, 1)).Direction = ParameterDirection.Input;
				_with1.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;
				_with1.Add("LOCATION", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input;
				_with1.Add("SECTOR", getDefault(POL_POD, 0)).Direction = ParameterDirection.Input;
				_with1.Add("COMMODITY_GROUP", getDefault(COMMODITY_GROUP, 0)).Direction = ParameterDirection.Input;
				_with1.Add("JCPK_IN", getDefault(JObpk, 0)).Direction = ParameterDirection.Input;
				_with1.Add("TOP", getDefault(TOP, 0)).Direction = ParameterDirection.Input;
				_with1.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
				if (type == 2) {
					_with1.Add("COLUMN", Column).Direction = ParameterDirection.Input;
				} else {
					SortColumn = SortColumn.Replace("JCDATE", " to_date(JCDATE,dateformat) ");
					_with1.Add("COLUMN", SortColumn).Direction = ParameterDirection.Input;
					_with1.Add("EXCEL_IN", getDefault(Excel, 0)).Direction = ParameterDirection.Input;
				}
				_with1.Add("SORT", getDefault(SortType, " DESC")).Direction = ParameterDirection.Input;
				_with1.Add("CURRENCY_IN", CURRENCY).Direction = ParameterDirection.Input;
				_with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
				_with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with1.Add("ONPAGELD_IN", ChkONLD).Direction = ParameterDirection.InputOutput;
				if (type == 2) {
					_with1.Add("TRADE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with1.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with1.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with1.Add("COMMODITY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					strPKGProc = GetProcedure.Split(',');
					dsAll = objWF.GetDataSet(strPKGProc[0], strPKGProc[1]);
				} else {
					_with1.Add("SERVICE_TYPE_IN", ServiceType).Direction = ParameterDirection.Input;
					_with1.Add("JC_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					strPKGProc = GetJobProcedure.Split(',');
					dsAll = objWF.GetDataSet(strPKGProc[0], strPKGProc[1]);
				}
				TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
				CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				if (TotalPage == 0) {
					CurrentPage = 0;
				} else {
					CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				}

				if (type == 2) {
					CreateRelation(dsAll);
				}

				return dsAll;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "For BizType ALL and Process Type ALL"
        /// <summary>
        /// Fetch_s the data_ all_ customer.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="ChkONLD">The CHK onld.</param>
        /// <param name="FROM_DATE">The fro m_ date.</param>
        /// <param name="TO_DATE">The t o_ date.</param>
        /// <param name="LCL_FCL">The lc l_ FCL.</param>
        /// <param name="POL_POD">The po l_ pod.</param>
        /// <param name="CUSTOMER">The customer.</param>
        /// <param name="LOCATION">The location.</param>
        /// <param name="CURRENCY">The currency.</param>
        /// <param name="COMMODITY_GROUP">The commodit y_ group.</param>
        /// <param name="JObpk">The j obpk.</param>
        /// <param name="TOP">The top.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="Column">The column.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="ServiceType">Type of the service.</param>
        /// <param name="FlagGrand">The flag grand.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <returns></returns>
        public DataSet Fetch_Data_All_Cust(Int32 type, string SortColumn, Int32 ChkONLD, string FROM_DATE = "", string TO_DATE = "", string LCL_FCL = "1", string POL_POD = "", string CUSTOMER = "", string LOCATION = "", string CURRENCY = "",
		string COMMODITY_GROUP = "", string JObpk = "", string TOP = "", Int32 CurrentPage = 1, Int32 TotalPage = 0, string SortType = " DESC ", int Column = 0, int BizType = 0, int ProcessType = 0, int ServiceType = 0,
		int FlagGrand = 0, int JobType = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataTable dtTrade = null;
			DataTable dtCust = null;
			DataTable dtLocation = null;
			DataTable dtCommodity = null;
			DataSet dsAll = null;

			string[] strPKGProc = null;
			try {
				objWF.MyCommand.Parameters.Clear();
				CURRENCY = Convert.ToString(HttpContext.Current.Session["Currency_mst_pk"]);
				var _with2 = objWF.MyCommand.Parameters;
				_with2.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;
				_with2.Add("TO_DATE", getDefault(TO_DATE, "")).Direction = ParameterDirection.Input;
				_with2.Add("LCL_FCL", getDefault(LCL_FCL, 1)).Direction = ParameterDirection.Input;
				_with2.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;
				_with2.Add("LOCATION", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input;
				_with2.Add("SECTOR", getDefault(POL_POD, 0)).Direction = ParameterDirection.Input;
				_with2.Add("BIZ_TYPE_IN", getDefault(BizType, 0)).Direction = ParameterDirection.Input;
				_with2.Add("PROCESS_TYPE_IN", getDefault(ProcessType, 0)).Direction = ParameterDirection.Input;
				_with2.Add("JOB_TYPE_IN", getDefault(JobType, 0)).Direction = ParameterDirection.Input;
				_with2.Add("COMMODITY_GROUP", getDefault(COMMODITY_GROUP, 0)).Direction = ParameterDirection.Input;
				_with2.Add("JCPK_IN", getDefault(JObpk, 0)).Direction = ParameterDirection.Input;
				_with2.Add("TOP", getDefault(TOP, 0)).Direction = ParameterDirection.Input;
				_with2.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
				_with2.Add("COLUMN", Column).Direction = ParameterDirection.Input;
				_with2.Add("SORT", getDefault(SortType, (Column == 0 ? " ASC " : " DESC"))).Direction = ParameterDirection.Input;
				_with2.Add("CURRENCY_IN", CURRENCY).Direction = ParameterDirection.Input;
				_with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
				_with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with2.Add("ONPAGELD_IN", ChkONLD).Direction = ParameterDirection.InputOutput;
				_with2.Add("TRADE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with2.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				//If JobType <> 0 And JobType <> 3 Then
				//    .Add("LOCATION_CUR", OracleClient.OracleDbType.RefCursor).Direction = ParameterDirection.Output
				//End If
				_with2.Add("COMMODITY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				strPKGProc = GetProcedure.Split(',');
				dsAll = objWF.GetDataSet(strPKGProc[0], strPKGProc[1]);
				TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
				CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				if (TotalPage == 0) {
					CurrentPage = 0;
				} else {
					CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				}
				//If JobType = 0 Or JobType = 3 Then
				//    CreateRelationExclPorts(dsAll)
				//Else
				//    CreateRelation(dsAll)
				//End If
				CreateRelationExclPorts(dsAll);
				return dsAll;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Fetch_s the data_ all_ jc.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="ChkONLD">The CHK onld.</param>
        /// <param name="FROM_DATE">The fro m_ date.</param>
        /// <param name="TO_DATE">The t o_ date.</param>
        /// <param name="LCL_FCL">The lc l_ FCL.</param>
        /// <param name="POL_POD">The po l_ pod.</param>
        /// <param name="CUSTOMER">The customer.</param>
        /// <param name="LOCATION">The location.</param>
        /// <param name="CURRENCY">The currency.</param>
        /// <param name="COMMODITY_GROUP">The commodit y_ group.</param>
        /// <param name="JObpk">The j obpk.</param>
        /// <param name="TOP">The top.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="Column">The column.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="ServiceType">Type of the service.</param>
        /// <param name="FlagGrand">The flag grand.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <returns></returns>
        public DataSet Fetch_Data_All_JC(Int32 type, string SortColumn, Int32 ChkONLD, string FROM_DATE = "", string TO_DATE = "", string LCL_FCL = "1", string POL_POD = "", string CUSTOMER = "", string LOCATION = "", string CURRENCY = "",
		string COMMODITY_GROUP = "", string JObpk = "", string TOP = "", Int32 CurrentPage = 1, Int32 TotalPage = 0, string SortType = " DESC ", int Column = 0, int BizType = 0, int ProcessType = 0, int ServiceType = 0,
		int FlagGrand = 0, int JobType = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataTable dtTrade = null;
			DataTable dtCust = null;
			DataTable dtLocation = null;
			DataTable dtCommodity = null;
			DataSet dsAll = null;

			string[] strPKGProc = null;
			try {
				objWF.MyCommand.Parameters.Clear();
				CURRENCY = Convert.ToString(HttpContext.Current.Session["Currency_mst_pk"]);
				var _with3 = objWF.MyCommand.Parameters;
				_with3.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;
				_with3.Add("TO_DATE", getDefault(TO_DATE, "")).Direction = ParameterDirection.Input;
				_with3.Add("LCL_FCL", getDefault(LCL_FCL, 1)).Direction = ParameterDirection.Input;
				_with3.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;
				_with3.Add("LOCATION", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input;
				_with3.Add("SECTOR", getDefault(POL_POD, 0)).Direction = ParameterDirection.Input;
				_with3.Add("BIZ_TYPE_IN", getDefault(BizType, 0)).Direction = ParameterDirection.Input;
				_with3.Add("PROCESS_TYPE_IN", getDefault(ProcessType, 0)).Direction = ParameterDirection.Input;
				_with3.Add("COMMODITY_GROUP", getDefault(COMMODITY_GROUP, 0)).Direction = ParameterDirection.Input;
				_with3.Add("JCPK_IN", getDefault(JObpk, 0)).Direction = ParameterDirection.Input;
				_with3.Add("TOP", getDefault(TOP, 0)).Direction = ParameterDirection.Input;
				_with3.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;

				SortColumn = SortColumn.Replace("JCDATE", " to_date(JCDATE,dateformat) ");
				_with3.Add("COLUMN", SortColumn).Direction = ParameterDirection.Input;

				_with3.Add("SORT", getDefault(SortType, (Column == 0 ? " ASC " : " DESC"))).Direction = ParameterDirection.Input;
				_with3.Add("CURRENCY_IN", CURRENCY).Direction = ParameterDirection.Input;
				_with3.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
				_with3.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with3.Add("ONPAGELD_IN", ChkONLD).Direction = ParameterDirection.InputOutput;
				_with3.Add("FLAG_GRAND_IN", FlagGrand).Direction = ParameterDirection.Input;
				_with3.Add("SERVICE_TYPE_IN", ServiceType).Direction = ParameterDirection.Input;
				_with3.Add("JOB_TYPE_IN", getDefault(JobType, 0)).Direction = ParameterDirection.Input;
				_with3.Add("JC_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				strPKGProc = GetJobProcedure.Split(',');
				dsAll = objWF.GetDataSet(strPKGProc[0], strPKGProc[1]);
				TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
				CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				if (TotalPage == 0) {
					CurrentPage = 0;
				} else {
					CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				}
				return dsAll;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Fetch_s the data_ all_ bn m_ customer.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="ChkONLD">The CHK onld.</param>
        /// <param name="FROM_DATE">The fro m_ date.</param>
        /// <param name="TO_DATE">The t o_ date.</param>
        /// <param name="LCL_FCL">The lc l_ FCL.</param>
        /// <param name="POL_POD">The po l_ pod.</param>
        /// <param name="CUSTOMER">The customer.</param>
        /// <param name="LOCATION">The location.</param>
        /// <param name="CURRENCY">The currency.</param>
        /// <param name="COMMODITY_GROUP">The commodit y_ group.</param>
        /// <param name="JObpk">The j obpk.</param>
        /// <param name="TOP">The top.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="Column">The column.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="ServiceType">Type of the service.</param>
        /// <param name="FlagGrand">The flag grand.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <param name="Group">The group.</param>
        /// <param name="GroupCat">The group cat.</param>
        /// <returns></returns>
        public DataSet Fetch_Data_All_BNM_Cust(Int32 type, string SortColumn, Int32 ChkONLD, string FROM_DATE = "", string TO_DATE = "", string LCL_FCL = "1", string POL_POD = "", string CUSTOMER = "", string LOCATION = "", string CURRENCY = "",
		string COMMODITY_GROUP = "", string JObpk = "", string TOP = "", Int32 CurrentPage = 1, Int32 TotalPage = 0, string SortType = " DESC ", int Column = 0, int BizType = 0, int ProcessType = 0, int ServiceType = 0,
		int FlagGrand = 0, int JobType = 0, int Group = 0, int GroupCat = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataTable dtTrade = null;
			DataTable dtCust = null;
			DataTable dtLocation = null;
			DataTable dtCommodity = null;
			DataSet dsAll = null;

			string[] strPKGProc = null;
			try {
				objWF.MyCommand.Parameters.Clear();
				CURRENCY = Convert.ToString(HttpContext.Current.Session["Currency_mst_pk"]);
				var _with4 = objWF.MyCommand.Parameters;
				_with4.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;
				_with4.Add("TO_DATE", getDefault(TO_DATE, "")).Direction = ParameterDirection.Input;
				_with4.Add("LCL_FCL", getDefault(LCL_FCL, 1)).Direction = ParameterDirection.Input;
				_with4.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;
				_with4.Add("LOCATION", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input;
				_with4.Add("SECTOR", getDefault(POL_POD, 0)).Direction = ParameterDirection.Input;
				_with4.Add("BIZ_TYPE_IN", getDefault(BizType, 0)).Direction = ParameterDirection.Input;
				_with4.Add("PROCESS_TYPE_IN", getDefault(ProcessType, 0)).Direction = ParameterDirection.Input;
				_with4.Add("GROUP_CAT_IN", getDefault(GroupCat, 0)).Direction = ParameterDirection.Input;
				_with4.Add("JOB_TYPE_IN", getDefault(JobType, 0)).Direction = ParameterDirection.Input;
				_with4.Add("COMMODITY_GROUP", getDefault(COMMODITY_GROUP, 0)).Direction = ParameterDirection.Input;
				_with4.Add("JCPK_IN", getDefault(JObpk, 0)).Direction = ParameterDirection.Input;
				_with4.Add("TOP", getDefault(TOP, 0)).Direction = ParameterDirection.Input;
				_with4.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
				_with4.Add("COLUMN", Column).Direction = ParameterDirection.Input;
				_with4.Add("SORT", getDefault(SortType, (Column == 0 ? " ASC " : " DESC"))).Direction = ParameterDirection.Input;
				_with4.Add("CURRENCY_IN", CURRENCY).Direction = ParameterDirection.Input;
				_with4.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
				_with4.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with4.Add("ONPAGELD_IN", ChkONLD).Direction = ParameterDirection.InputOutput;
				if ((JobType == 0 | JobType == 3) & Group == 1) {
					_with4.Add("CUST_GROUP_CAT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with4.Add("CUST_GROUP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with4.Add("TRADE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with4.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				} else if ((JobType != 0 & JobType != 3) & Group == 1) {
					_with4.Add("CUST_GROUP_CAT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with4.Add("CUST_GROUP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with4.Add("TRADE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with4.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with4.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				} else if ((JobType == 0 | JobType == 3) & Group == 0) {
					_with4.Add("TRADE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with4.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				} else {
					_with4.Add("TRADE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with4.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					_with4.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				}
				_with4.Add("COMMODITY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				strPKGProc = GetBNMProcedureCust.Split(',');
				dsAll = objWF.GetDataSet(strPKGProc[0], strPKGProc[1]);
				TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
				CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				if (TotalPage == 0) {
					CurrentPage = 0;
				} else {
					CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				}
				if ((JobType == 0 | JobType == 3) & Group == 1) {
					CreateRelationGrpExclPorts(dsAll);
				} else if ((JobType != 0 & JobType != 3) & Group == 1) {
					CreateRelationGrp(dsAll);
				} else if ((JobType == 0 | JobType == 3) & Group == 0) {
					CreateRelationExclPortsBNM(dsAll);
				} else {
					CreateRelationBNM(dsAll);
				}
				return dsAll;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "Create Relations"



        /// <summary>
        /// Creates the relation.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        private void CreateRelation(DataSet dsMain)
		{
			DataColumn parentCol = null;
			DataColumn childCol = null;

			parentCol = dsMain.Tables[0].Columns["CUSTOMER_NAME"];
			childCol = dsMain.Tables[1].Columns["CUSTOMER_NAME"];

			DataRelation relTrade = null;
			relTrade = new DataRelation("Trade", parentCol, childCol);
			DataRelation relCust = null;
			relCust = new DataRelation("Cust", new DataColumn[] {
				dsMain.Tables[1].Columns["LOCATION_ID"],
				dsMain.Tables[1].Columns["CUSTOMER_NAME"]
			}, new DataColumn[] {
				dsMain.Tables[2].Columns["LOCATION_ID"],
				dsMain.Tables[2].Columns["CUSTOMER_NAME"]
			});
			DataRelation relLoc = null;
			relLoc = new DataRelation("Loc", new DataColumn[] {
				dsMain.Tables[2].Columns["LOCATION_ID"],
				dsMain.Tables[2].Columns["CUSTOMER_NAME"],
				dsMain.Tables[2].Columns["POL"],
				dsMain.Tables[2].Columns["POD"]
			}, new DataColumn[] {
				dsMain.Tables[3].Columns["LOCATION_ID"],
				dsMain.Tables[3].Columns["CUSTOMER_NAME"],
				dsMain.Tables[3].Columns["POL"],
				dsMain.Tables[3].Columns["POD"]
			});
			relTrade.Nested = true;
			relCust.Nested = true;
			relLoc.Nested = true;
			dsMain.Relations.Add(relTrade);
			dsMain.Relations.Add(relCust);
			dsMain.Relations.Add(relLoc);

		}
        /// <summary>
        /// Creates the relation excl ports.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        private void CreateRelationExclPorts(DataSet dsMain)
		{
			DataColumn parentCol = null;
			DataColumn childCol = null;

			parentCol = dsMain.Tables[0].Columns["CUSTOMER_NAME"];
			childCol = dsMain.Tables[1].Columns["CUSTOMER_NAME"];

			DataRelation relTrade = null;
			relTrade = new DataRelation("Trade", parentCol, childCol);
			DataRelation relLoc = null;
			relLoc = new DataRelation("Loc", new DataColumn[] {
				dsMain.Tables[1].Columns["LOCATION_ID"],
				dsMain.Tables[1].Columns["CUSTOMER_NAME"]
			}, new DataColumn[] {
				dsMain.Tables[2].Columns["LOCATION_ID"],
				dsMain.Tables[2].Columns["CUSTOMER_NAME"]
			});
			relTrade.Nested = true;
			relLoc.Nested = true;
			dsMain.Relations.Add(relTrade);
			dsMain.Relations.Add(relLoc);
		}


        /// <summary>
        /// Creates the relation BNM.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        private void CreateRelationBNM(DataSet dsMain)
		{
			DataColumn parentCol = null;
			DataColumn childCol = null;

			parentCol = dsMain.Tables[0].Columns["CUSTOMER_NAME"];
			childCol = dsMain.Tables[1].Columns["CUSTOMER_NAME"];

			DataRelation relTrade = null;
			relTrade = new DataRelation("Trade", parentCol, childCol);
			DataRelation relCust = null;
			relCust = new DataRelation("Cust", new DataColumn[] {
				dsMain.Tables[1].Columns["LOCATION_ID"],
				dsMain.Tables[1].Columns["CUSTOMER_NAME"]
			}, new DataColumn[] {
				dsMain.Tables[2].Columns["LOCATION_ID"],
				dsMain.Tables[2].Columns["CUSTOMER_NAME"]
			});
			DataRelation relLoc = null;
			relLoc = new DataRelation("Loc", new DataColumn[] {
				dsMain.Tables[2].Columns["LOCATION_ID"],
				dsMain.Tables[2].Columns["CUSTOMER_NAME"],
				dsMain.Tables[2].Columns["POL"],
				dsMain.Tables[2].Columns["POD"]
			}, new DataColumn[] {
				dsMain.Tables[3].Columns["LOCATION_ID"],
				dsMain.Tables[3].Columns["CUSTOMER_NAME"],
				dsMain.Tables[3].Columns["POL"],
				dsMain.Tables[3].Columns["POD"]
			});
			relTrade.Nested = true;
			relCust.Nested = true;
			relLoc.Nested = true;
			dsMain.Relations.Add(relTrade);
			dsMain.Relations.Add(relCust);
			dsMain.Relations.Add(relLoc);
		}
        /// <summary>
        /// Creates the relation excl ports BNM.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        private void CreateRelationExclPortsBNM(DataSet dsMain)
		{
			DataColumn parentCol = null;
			DataColumn childCol = null;

			parentCol = dsMain.Tables[0].Columns["CUSTOMER_NAME"];
			childCol = dsMain.Tables[1].Columns["CUSTOMER_NAME"];

			DataRelation relTrade = null;
			relTrade = new DataRelation("Trade", parentCol, childCol);
			DataRelation relLoc = null;
			relLoc = new DataRelation("Loc", new DataColumn[] {
				dsMain.Tables[1].Columns["LOCATION_ID"],
				dsMain.Tables[1].Columns["CUSTOMER_NAME"]
			}, new DataColumn[] {
				dsMain.Tables[2].Columns["LOCATION_ID"],
				dsMain.Tables[2].Columns["CUSTOMER_NAME"]
			});
			relTrade.Nested = true;
			relLoc.Nested = true;
			dsMain.Relations.Add(relTrade);
			dsMain.Relations.Add(relLoc);
		}
        /// <summary>
        /// Creates the relation GRP excl ports.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        private void CreateRelationGrpExclPorts(DataSet dsMain)
		{
			DataColumn parentCol = null;
			DataColumn childCol = null;

			parentCol = dsMain.Tables[0].Columns["GROUP_CATEGORY"];
			childCol = dsMain.Tables[1].Columns["GROUP_CATEGORY"];

			DataRelation relGrpCat = null;
			relGrpCat = new DataRelation("Category", parentCol, childCol);

			parentCol = dsMain.Tables[1].Columns["GRP_HDR_PK"];
			childCol = dsMain.Tables[2].Columns["GRP_HDR_PK"];
			DataRelation relGrpCust = null;
			relGrpCust = new DataRelation("Group", parentCol, childCol);

			parentCol = dsMain.Tables[2].Columns["CUSTOMER_NAME"];
			childCol = dsMain.Tables[3].Columns["CUSTOMER_NAME"];
			DataRelation relTrade = null;
			relTrade = new DataRelation("Trade", parentCol, childCol);
			DataRelation relLoc = null;
			relLoc = new DataRelation("Loc", new DataColumn[] {
				dsMain.Tables[3].Columns["LOCATION_ID"],
				dsMain.Tables[3].Columns["CUSTOMER_NAME"]
			}, new DataColumn[] {
				dsMain.Tables[4].Columns["LOCATION_ID"],
				dsMain.Tables[4].Columns["CUSTOMER_NAME"]
			});

			relGrpCat.Nested = true;
			relGrpCust.Nested = true;
			relTrade.Nested = true;
			relLoc.Nested = true;

			dsMain.Relations.Add(relGrpCat);
			dsMain.Relations.Add(relGrpCust);
			dsMain.Relations.Add(relTrade);
			dsMain.Relations.Add(relLoc);
		}
        /// <summary>
        /// Creates the relation GRP.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        private void CreateRelationGrp(DataSet dsMain)
		{
			DataColumn parentCol = null;
			DataColumn childCol = null;

			parentCol = dsMain.Tables[0].Columns["GROUP_CATEGORY"];
			childCol = dsMain.Tables[1].Columns["GROUP_CATEGORY"];

			DataRelation relGrpCat = null;
			relGrpCat = new DataRelation("Category", parentCol, childCol);

			parentCol = dsMain.Tables[1].Columns["GRP_HDR_PK"];
			childCol = dsMain.Tables[2].Columns["GRP_HDR_PK"];
			DataRelation relGrpCust = null;
			relGrpCust = new DataRelation("Group", parentCol, childCol);

			parentCol = dsMain.Tables[2].Columns["CUSTOMER_NAME"];
			childCol = dsMain.Tables[3].Columns["CUSTOMER_NAME"];

			DataRelation relTrade = null;
			relTrade = new DataRelation("Trade", parentCol, childCol);

			DataRelation relCust = null;
			relCust = new DataRelation("Cust", new DataColumn[] {
				dsMain.Tables[3].Columns["LOCATION_ID"],
				dsMain.Tables[3].Columns["CUSTOMER_NAME"]
			}, new DataColumn[] {
				dsMain.Tables[4].Columns["LOCATION_ID"],
				dsMain.Tables[4].Columns["CUSTOMER_NAME"]
			});

			DataRelation relLoc = null;
			relLoc = new DataRelation("Loc", new DataColumn[] {
				dsMain.Tables[4].Columns["LOCATION_ID"],
				dsMain.Tables[4].Columns["CUSTOMER_NAME"],
				dsMain.Tables[4].Columns["POL"],
				dsMain.Tables[4].Columns["POD"]
			}, new DataColumn[] {
				dsMain.Tables[5].Columns["LOCATION_ID"],
				dsMain.Tables[5].Columns["CUSTOMER_NAME"],
				dsMain.Tables[5].Columns["POL"],
				dsMain.Tables[5].Columns["POD"]
			});

			relGrpCat.Nested = true;
			relGrpCust.Nested = true;
			relTrade.Nested = true;
			relCust.Nested = true;
			relLoc.Nested = true;
			dsMain.Relations.Add(relGrpCat);
			dsMain.Relations.Add(relGrpCust);
			dsMain.Relations.Add(relTrade);
			dsMain.Relations.Add(relCust);
			dsMain.Relations.Add(relLoc);
		}
        /// <summary>
        /// Gets the sector.
        /// </summary>
        /// <param name="tradePk">The trade pk.</param>
        /// <returns></returns>
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
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Gets the location.
        /// </summary>
        /// <param name="userLocPK">The user loc pk.</param>
        /// <param name="strALL">The string all.</param>
        /// <returns></returns>
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
					strQuery.Append("  WHERE L.REPORTING_TO_FK = " + userLocPK);

					dr = objWF.GetDataReader(strQuery.ToString());
					while (dr.Read()) {
						strReturn += dr["LOCATION_MST_PK"] + "~$";
					}
					dr.Close();
				}

				strALL = strReturn;
				return objWF.GetDataSet(strQuery.ToString());
			} catch (Exception ex) {
				throw ex;
			}
		}

        #endregion

        #region "For Fetching DropDown Values From DataBase"
        /// <summary>
        /// Fetches the dd values.
        /// </summary>
        /// <param name="Flag">The flag.</param>
        /// <param name="ConfigID">The configuration identifier.</param>
        /// <returns></returns>
        public static DataSet FetchDDValues(string Flag = "", string ConfigID = "")
		{
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			string ErrorMessage = null;
			sb.Append("SELECT T.DD_VALUE, T.DD_ID");
			sb.Append("  FROM QFOR_DROP_DOWN_TBL T");
			sb.Append(" WHERE T.DD_FLAG = '" + Flag + "'");
			sb.Append(" AND T.CONFIG_ID  = '" + ConfigID + "'");
			sb.Append("    ORDER BY T.DD_VALUE DESC ");
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
	}
}
