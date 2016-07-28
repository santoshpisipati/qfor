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

using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_Removal_Job_Card : CommonFeatures
	{

		#region "Class Level Variables"
		DataSet ListDS = new DataSet();
		DataSet HeaderDS = new DataSet();
		DataSet ItemDS = new DataSet();
		DataSet TrptDS = new DataSet();
		DataSet FrtOthChgDS = new DataSet();
		DataSet CostDS = new DataSet();
		DataSet RevenueDS = new DataSet();
		DataSet SpclInstDS = new DataSet();
		DataSet CMRDS = new DataSet();
		DataSet ProfitabilityDS = new DataSet();
		WorkFlow objWF = new WorkFlow();
		public int JCPK;
		public string JCRefNr;
		public int SpclInstPK;
			#endregion
		public int CMRPK;

		#region "Fetch Data For Listing Screen"
		public DataSet FetchJCRemovalsListData(string JC_REF_NR_IN = "", string QUOTE_REF_NR_IN = "", int PLR_FK_IN = 0, int PFD_FK_IN = 0, int PARTY_FK_IN = 0, int SHIPPER_FK_IN = 0, int CONSIGNEE_FK_IN = 0, double SEARCH_FOR_IN = 0, int SEARCH_FOR_VALUE_IN = 0, int MOVE_TYPE_IN = 0,
		int POST_BACK_IN = 0, string SEARCH_TYPE_IN = "", int TotalPage = 0, int CurrentPage = 0, string SortType = "", string SortColumn = "")
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				int NO = -Convert.ToInt32(SEARCH_FOR_IN);
				System.DateTime Time = default(System.DateTime);
				string strTime = null;
				if (SEARCH_FOR_IN > 0 & SEARCH_FOR_VALUE_IN > 0) {
					switch (SEARCH_FOR_VALUE_IN) {
						case 1:
							Time = DateTime.Today.AddDays(NO);
							break;
						case 2:
							Time = DateTime.Today.AddDays(NO * 7);
							break;
						case 3:
							Time = DateTime.Today.AddMonths(NO);
							break;
						case 4:
							Time = DateTime.Today.AddYears(NO);
							break;
					}
				}
				strTime = Convert.ToString(Time);
				if (strTime == "#12:00:00 AM#" | strTime == "00:00:00") {
					strTime = "";
				}
				var _with1 = objWF.MyCommand.Parameters;
				_with1.Add("JC_REF_NR_IN", getDefault(JC_REF_NR_IN.Trim(), "")).Direction = ParameterDirection.Input;
				_with1.Add("QUOTE_REF_NR_IN", getDefault(QUOTE_REF_NR_IN.Trim(), "")).Direction = ParameterDirection.Input;
				_with1.Add("PLR_FK_IN", getDefault(PLR_FK_IN, "")).Direction = ParameterDirection.Input;
				_with1.Add("PFD_FK_IN", getDefault(PFD_FK_IN, "")).Direction = ParameterDirection.Input;
				_with1.Add("PARTY_FK_IN", getDefault(PARTY_FK_IN, "")).Direction = ParameterDirection.Input;
				_with1.Add("SHIPPER_FK_IN", getDefault(SHIPPER_FK_IN, "")).Direction = ParameterDirection.Input;
				_with1.Add("CONSIGNEE_FK_IN", getDefault(CONSIGNEE_FK_IN, "")).Direction = ParameterDirection.Input;
				_with1.Add("SEARCH_FOR_VALUE_IN", getDefault(strTime, "")).Direction = ParameterDirection.Input;
				_with1.Add("MOVE_TYPE_IN", MOVE_TYPE_IN).Direction = ParameterDirection.Input;
				_with1.Add("POST_BACK_IN", POST_BACK_IN).Direction = ParameterDirection.Input;
				_with1.Add("SEARCH_TYPE_IN", SEARCH_TYPE_IN).Direction = ParameterDirection.Input;
				_with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
				_with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with1.Add("RECORDS_PER_PAGE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
				_with1.Add("SORT_COLUMN_IN", SortColumn).Direction = ParameterDirection.Input;
				_with1.Add("SORT_TYPE_IN", SortType).Direction = ParameterDirection.Input;
				_with1.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
				_with1.Add("JOB_CARD_CURSOR_OUT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				ListDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_JC_LIST");
				TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
				CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				return ListDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Data From Job Card"

		#region "Fetch Data For Entry Screen(Header)"
		public DataSet FetchJCHeaderData(int JCPK)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with2 = objWF.MyCommand.Parameters;
				_with2.Add("JOB_CARD_PK_IN", JCPK).Direction = ParameterDirection.Input;
				_with2.Add("JC_HDR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				HeaderDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_JC_ENTRY_HDR");
				return HeaderDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Item Details"
		public DataSet FetchJCItemData(int JCPK)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with3 = objWF.MyCommand.Parameters;
				_with3.Add("JOB_CARD_PK_IN", JCPK).Direction = ParameterDirection.Input;
				_with3.Add("JC_ITEM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				ItemDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_JC_ITEM_DETAILS");
				return ItemDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Transportation Details"
		public DataSet FetchJCTrptData(int JCPK)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with4 = objWF.MyCommand.Parameters;
				_with4.Add("JOB_CARD_PK_IN", JCPK).Direction = ParameterDirection.Input;
				_with4.Add("JC_TRPT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				TrptDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_JC_TRPT_DETAILS");
				return TrptDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Freight and Other Charges Details"
		public DataSet FetchJCFrtOthData(int JCPK, int FrtOth)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with5 = objWF.MyCommand.Parameters;
				_with5.Add("JOB_CARD_PK_IN", JCPK).Direction = ParameterDirection.Input;
				_with5.Add("FRT_OR_OTHER", FrtOth).Direction = ParameterDirection.Input;
				_with5.Add("JC_FRT_OTH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				FrtOthChgDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_JC_FRT_OTH_DETAILS");
				return FrtOthChgDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Cost Details"
		public DataSet FetchJCCostData(int JCPK)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with6 = objWF.MyCommand.Parameters;
				_with6.Add("JOB_CARD_PK_IN", JCPK).Direction = ParameterDirection.Input;
				_with6.Add("JC_COST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				CostDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_JC_COST_DETAILS");
				return CostDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Debit/Credit Details"
		public DataSet FetchJCRevData(int JCPK)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with7 = objWF.MyCommand.Parameters;
				_with7.Add("JOB_CARD_PK_IN", JCPK).Direction = ParameterDirection.Input;
				_with7.Add("JC_REV_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				RevenueDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_JC_REV_DETAILS");
				return RevenueDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Profitability Details"
		public DataSet GetProfitabilityDetails(decimal actualCost, decimal actualRevenue, decimal estimatedCost, decimal estimatedRevenue, int jobCardPK)
		{

			try {
				objWF.MyCommand.Parameters.Clear();
				var _with8 = objWF.MyCommand.Parameters;
				_with8.Add("JOB_CARD_PK_IN", jobCardPK).Direction = ParameterDirection.Input;
				_with8.Add("CURRENCY_PK_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
				_with8.Add("PROFITABILITY_CURR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				ProfitabilityDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_PROFITABILITY_DETAILS");
				return ProfitabilityDS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#endregion

		#region "Fetch Data From Quotation"

		#region "Fetch Header Data From Quotation"
		public DataSet FetchQTHeaderData(int QTPK)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with9 = objWF.MyCommand.Parameters;
				_with9.Add("QUOTATION_PK_IN", QTPK).Direction = ParameterDirection.Input;
				_with9.Add("QT_HDR_CURR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				HeaderDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_QT_HDR_DETAILS");
				return HeaderDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Item Details From Quotation"
		public DataSet FetchQTItemData(int QTPK)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with10 = objWF.MyCommand.Parameters;
				_with10.Add("QUOTATION_PK_IN", QTPK).Direction = ParameterDirection.Input;
				_with10.Add("QT_ITEM_CURR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				ItemDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_QT_ITEM_DETAILS");
				return ItemDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Freight and Other Charges Details From Quotation"
		public DataSet FetchQTFrtOthData(int QTPK, int FrtOth)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				DataSet CurrDS = null;
				CurrDS = FetchCurrAndExchange();
				int CurrPK = 0;
				if (CurrDS.Tables[0].Rows.Count > 1) {
					CurrPK = Convert.ToInt32(CurrDS.Tables[0].Rows[0]["CURRENCY_MST_PK"]);
				} else {
					CurrPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
				}
				var _with11 = objWF.MyCommand.Parameters;
				_with11.Add("QUOTATION_PK_IN", QTPK).Direction = ParameterDirection.Input;
				_with11.Add("FRT_OR_OTHER", FrtOth).Direction = ParameterDirection.Input;
				_with11.Add("CURR_PK_IN", CurrPK).Direction = ParameterDirection.Input;
				_with11.Add("QT_FRT_OTH_CURR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				FrtOthChgDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_QT_FRT_OTH_DETAILS");
				return FrtOthChgDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#endregion

		#region "Save Job Card Data"
		public ArrayList SaveAll(DataSet HeaderDS, DataSet ItemDS, DataSet TrptDS, DataSet FrtDS, DataSet OthChgDS, DataSet CostDS, bool IsEditing)
		{
			OracleTransaction TRAN = null;
			Int32 nRowCnt = default(Int32);
			Int32 RecAfct = default(Int32);
			//Header Commands
			OracleCommand insHdrCommand = new OracleCommand();
			OracleCommand updHdrCommand = new OracleCommand();
			//Item Details Command
			OracleCommand insItemCommand = new OracleCommand();
			OracleCommand updItemCommand = new OracleCommand();
			//Trpt Details Command
			OracleCommand insTrptCommand = new OracleCommand();
			OracleCommand updTrptCommand = new OracleCommand();
			//Frt Details Command
			OracleCommand insFrtCommand = new OracleCommand();
			OracleCommand updFrtCommand = new OracleCommand();
			OracleCommand delFrtCommand = new OracleCommand();
			//Other Charges Command
			OracleCommand insOthCommand = new OracleCommand();
			OracleCommand updOthCommand = new OracleCommand();
			OracleCommand delOthCommand = new OracleCommand();
			//Cost Details Command
			OracleCommand insCostCommand = new OracleCommand();
			OracleCommand updCostCommand = new OracleCommand();
			OracleCommand delCostCommand = new OracleCommand();
			try {
				objWF.OpenConnection();
				TRAN = objWF.MyConnection.BeginTransaction();
				if (IsEditing == false) {
					JCRefNr = GenerateProtocolKey("REMOVAL JOB CARD", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), System.DateTime.Now, "","" ,"" , M_LAST_MODIFIED_BY_FK);
				}
				var _with12 = insHdrCommand;
				_with12.Connection = objWF.MyConnection;
				_with12.CommandType = CommandType.StoredProcedure;
				_with12.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.INS_REM_JC_HEADER_DET";
				var _with13 = _with12.Parameters;
				insHdrCommand.Parameters.Add("JOB_CARD_REF_IN", JCRefNr).Direction = ParameterDirection.Input;

				insHdrCommand.Parameters.Add("JOB_CARD_PARTY_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PARTY_FK").Direction = ParameterDirection.Input;
				insHdrCommand.Parameters["JOB_CARD_PARTY_FK_IN"].SourceVersion = DataRowVersion.Current;

				insHdrCommand.Parameters.Add("JOB_CARD_QUOT_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_QUOT_FK").Direction = ParameterDirection.Input;
				insHdrCommand.Parameters["JOB_CARD_QUOT_FK_IN"].SourceVersion = DataRowVersion.Current;

				insHdrCommand.Parameters.Add("JOB_CARD_ENQ_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_ENQ_FK").Direction = ParameterDirection.Input;
				insHdrCommand.Parameters["JOB_CARD_ENQ_FK_IN"].SourceVersion = DataRowVersion.Current;

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_SURVEY_FK"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_SURVEY_FK_IN", "").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_SURVEY_FK_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_SURVEY_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_SURVEY_FK").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_SURVEY_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				insHdrCommand.Parameters.Add("JOB_CARD_SHIPPER_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_SHIPPER_FK").Direction = ParameterDirection.Input;
				insHdrCommand.Parameters["JOB_CARD_SHIPPER_FK_IN"].SourceVersion = DataRowVersion.Current;

				insHdrCommand.Parameters.Add("JOB_CARD_CONSINEE_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_CONSINEE_FK").Direction = ParameterDirection.Input;
				insHdrCommand.Parameters["JOB_CARD_CONSINEE_FK_IN"].SourceVersion = DataRowVersion.Current;

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["MOVE_DATE"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_MOVE_DT_IN", "").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_MOVE_DT_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_MOVE_DT_IN", Convert.ToDateTime(HeaderDS.Tables[0].Rows[0]["MOVE_DATE"])).Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_MOVE_DT_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["DELIVERY_DATE"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_DEL_DT_IN", "").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_DEL_DT_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_DEL_DT_IN", Convert.ToDateTime(HeaderDS.Tables[0].Rows[0]["DELIVERY_DATE"])).Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_DEL_DT_IN"].SourceVersion = DataRowVersion.Current;
				}

				insHdrCommand.Parameters.Add("JOB_CARD_CURR_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_CURR_FK").Direction = ParameterDirection.Input;
				insHdrCommand.Parameters["JOB_CARD_CURR_FK_IN"].SourceVersion = DataRowVersion.Current;

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_FK"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PLR_FK").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_FK_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PLR_FK").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_ADDR1"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_ADDR1_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ADDR1").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_ADDR1_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_ADDR1_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ADDR1").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_ADDR1_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_ADDR2"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_ADDR2_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ADDR2").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_ADDR2_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_ADDR2_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ADDR2").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_ADDR2_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_CITY"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_CITY_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_CITY").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_CITY_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_CITY_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_CITY").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_CITY_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_ZIP"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_ZIP_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ZIP").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_ZIP_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_ZIP_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ZIP").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_ZIP_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_COUNTRY_FK"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_COUNTRY_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PLR_COUNTRY_FK").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PLR_COUNTRY_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PLR_COUNTRY_FK").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PLR_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_FK"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PFD_FK").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_FK_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PFD_FK").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_ADDR1"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_ADDR1_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ADDR1").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_ADDR1_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_ADDR1_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ADDR1").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_ADDR1_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_ADDR2"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_ADDR2_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ADDR2").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_ADDR2_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_ADDR2_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ADDR2").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_ADDR2_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_CITY"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_CITY_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_CITY").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_CITY_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_CITY_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_CITY").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_CITY_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_ZIP"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_ZIP_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ZIP").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_ZIP_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_ZIP_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ZIP").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_ZIP_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_COUNTRY_FK"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_COUNTRY_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PFD_COUNTRY_FK").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_PFD_COUNTRY_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PFD_COUNTRY_FK").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_PFD_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				insHdrCommand.Parameters.Add("JOB_CARD_MOVE_TYPE_IN", OracleDbType.Int32, 10, "JOB_CARD_MOVE_TYPE").Direction = ParameterDirection.Input;
				insHdrCommand.Parameters["JOB_CARD_MOVE_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				insHdrCommand.Parameters.Add("JOB_CARD_MOVE_SRV_PKG_IN", OracleDbType.Int32, 10, "JOB_CARD_MOVE_SRV_PKG").Direction = ParameterDirection.Input;
				insHdrCommand.Parameters["JOB_CARD_MOVE_SRV_PKG_IN"].SourceVersion = DataRowVersion.Current;

				insHdrCommand.Parameters.Add("JOB_CARD_MOVE_SRV_MVG_IN", OracleDbType.Int32, 10, "JOB_CARD_MOVE_SRV_MVG").Direction = ParameterDirection.Input;
				insHdrCommand.Parameters["JOB_CARD_MOVE_SRV_MVG_IN"].SourceVersion = DataRowVersion.Current;

				insHdrCommand.Parameters.Add("JOB_CARD_MOVE_SRV_UNPKG_IN", OracleDbType.Int32, 10, "JOB_CARD_MOVE_SRV_UNPKG").Direction = ParameterDirection.Input;
				insHdrCommand.Parameters["JOB_CARD_MOVE_SRV_UNPKG_IN"].SourceVersion = DataRowVersion.Current;

				insHdrCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 10, "JOB_CARD_STATUS").Direction = ParameterDirection.Input;
				insHdrCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JC_CLOSE_DATE"].ToString())) {
					insHdrCommand.Parameters.Add("JOB_CARD_CLOSE_DT_IN", "").Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_CLOSE_DT_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					insHdrCommand.Parameters.Add("JOB_CARD_CLOSE_DT_IN", Convert.ToDateTime(HeaderDS.Tables[0].Rows[0]["JC_CLOSE_DATE"])).Direction = ParameterDirection.Input;
					insHdrCommand.Parameters["JOB_CARD_CLOSE_DT_IN"].SourceVersion = DataRowVersion.Current;
				}

				insHdrCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				insHdrCommand.Parameters.Add("CONFIG_PK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

				insHdrCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_PK").Direction = ParameterDirection.Output;
				insHdrCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with14 = updHdrCommand;
				_with14.Connection = objWF.MyConnection;
				_with14.CommandType = CommandType.StoredProcedure;
				_with14.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.UPD_REM_JC_HEADER_DET";
				var _with15 = _with14.Parameters;
				updHdrCommand.Parameters.Add("JOB_CARD_PK_IN", OracleDbType.Int32, 10, "JOB_CARD_PK").Direction = ParameterDirection.Input;
				updHdrCommand.Parameters["JOB_CARD_PK_IN"].SourceVersion = DataRowVersion.Current;

				updHdrCommand.Parameters.Add("JOB_CARD_SHIPPER_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_SHIPPER_FK").Direction = ParameterDirection.Input;
				updHdrCommand.Parameters["JOB_CARD_SHIPPER_FK_IN"].SourceVersion = DataRowVersion.Current;

				updHdrCommand.Parameters.Add("JOB_CARD_CONSINEE_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_CONSINEE_FK").Direction = ParameterDirection.Input;
				updHdrCommand.Parameters["JOB_CARD_CONSINEE_FK_IN"].SourceVersion = DataRowVersion.Current;

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["MOVE_DATE"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_MOVE_DT_IN", "").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_MOVE_DT_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_MOVE_DT_IN", Convert.ToDateTime(HeaderDS.Tables[0].Rows[0]["MOVE_DATE"])).Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_MOVE_DT_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["DELIVERY_DATE"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_DEL_DT_IN", "").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_DEL_DT_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_DEL_DT_IN", Convert.ToDateTime(HeaderDS.Tables[0].Rows[0]["DELIVERY_DATE"])).Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_DEL_DT_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_FK"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PLR_FK").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_FK_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PLR_FK").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_ADDR1"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_ADDR1_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ADDR1").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_ADDR1_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_ADDR1_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ADDR1").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_ADDR1_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_ADDR2"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_ADDR2_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ADDR2").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_ADDR2_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_ADDR2_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ADDR2").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_ADDR2_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_CITY"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_CITY_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_CITY").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_CITY_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_CITY_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_CITY").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_CITY_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_ZIP"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_ZIP_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ZIP").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_ZIP_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_ZIP_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PLR_ZIP").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_ZIP_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PLR_COUNTRY_FK"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_COUNTRY_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PLR_COUNTRY_FK").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PLR_COUNTRY_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PLR_COUNTRY_FK").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PLR_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_FK"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PFD_FK").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_FK_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PFD_FK").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_ADDR1"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_ADDR1_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ADDR1").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_ADDR1_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_ADDR1_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ADDR1").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_ADDR1_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_ADDR2"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_ADDR2_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ADDR2").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_ADDR2_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_ADDR2_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ADDR2").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_ADDR2_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_CITY"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_CITY_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_CITY").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_CITY_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_CITY_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_CITY").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_CITY_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_ZIP"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_ZIP_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ZIP").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_ZIP_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_ZIP_IN", OracleDbType.Varchar2, 49, "JOB_CARD_PFD_ZIP").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_ZIP_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PFD_COUNTRY_FK"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_COUNTRY_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PFD_COUNTRY_FK").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_PFD_COUNTRY_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_PFD_COUNTRY_FK").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_PFD_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				updHdrCommand.Parameters.Add("JOB_CARD_MOVE_TYPE_IN", OracleDbType.Int32, 10, "JOB_CARD_MOVE_TYPE").Direction = ParameterDirection.Input;
				updHdrCommand.Parameters["JOB_CARD_MOVE_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				updHdrCommand.Parameters.Add("JOB_CARD_MOVE_SRV_PKG_IN", OracleDbType.Int32, 10, "JOB_CARD_MOVE_SRV_PKG").Direction = ParameterDirection.Input;
				updHdrCommand.Parameters["JOB_CARD_MOVE_SRV_PKG_IN"].SourceVersion = DataRowVersion.Current;

				updHdrCommand.Parameters.Add("JOB_CARD_MOVE_SRV_MVG_IN", OracleDbType.Int32, 10, "JOB_CARD_MOVE_SRV_MVG").Direction = ParameterDirection.Input;
				updHdrCommand.Parameters["JOB_CARD_MOVE_SRV_MVG_IN"].SourceVersion = DataRowVersion.Current;

				updHdrCommand.Parameters.Add("JOB_CARD_MOVE_SRV_UNPKG_IN", OracleDbType.Int32, 10, "JOB_CARD_MOVE_SRV_UNPKG").Direction = ParameterDirection.Input;
				updHdrCommand.Parameters["JOB_CARD_MOVE_SRV_UNPKG_IN"].SourceVersion = DataRowVersion.Current;

				updHdrCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 10, "JOB_CARD_STATUS").Direction = ParameterDirection.Input;
				updHdrCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

				if (string.IsNullOrEmpty(HeaderDS.Tables[0].Rows[0]["JC_CLOSE_DATE"].ToString())) {
					updHdrCommand.Parameters.Add("JOB_CARD_CLOSE_DT_IN", "").Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_CLOSE_DT_IN"].SourceVersion = DataRowVersion.Current;
				} else {
					updHdrCommand.Parameters.Add("JOB_CARD_CLOSE_DT_IN", Convert.ToDateTime(HeaderDS.Tables[0].Rows[0]["JC_CLOSE_DATE"])).Direction = ParameterDirection.Input;
					updHdrCommand.Parameters["JOB_CARD_CLOSE_DT_IN"].SourceVersion = DataRowVersion.Current;
				}

				updHdrCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				updHdrCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION").Direction = ParameterDirection.Input;
				updHdrCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				updHdrCommand.Parameters.Add("CONFIG_PK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

				updHdrCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_PK").Direction = ParameterDirection.Output;
				updHdrCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with16 = objWF.MyDataAdapter;
				_with16.InsertCommand = insHdrCommand;
				_with16.InsertCommand.Transaction = TRAN;

				_with16.UpdateCommand = updHdrCommand;
				_with16.UpdateCommand.Transaction = TRAN;

				RecAfct = _with16.Update(HeaderDS);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					//added by surya prasad for implementing protocol rollback
					if (IsEditing == false) {
						RollbackProtocolKey("REMOVAL JOB CARD", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), JCRefNr, System.DateTime.Now);
					}
					return arrMessage;
				} else {
					if (IsEditing == false) {
						JCPK = Convert.ToInt32(insHdrCommand.Parameters["RETURN_VALUE"].Value);
						//adding by thiyagarajan on 23/1/09:TrackNTrace Task:VEK Req.
						string status = null;
						//status = "Cargo Delivered at customer premises " & TrptDS.Tables(0).Rows(0).Item("DELIVERY_DATE")
						SaveInTrackNTrace(JCRefNr, JCPK, "Job Card Generated", TrptDS, 4, TRAN);
						//end
					} else {
						JCPK = Convert.ToInt32(HeaderDS.Tables[0].Rows[0]["JOB_CARD_PK"]);
					}
				}

				var _with17 = insItemCommand;
				_with17.Connection = objWF.MyConnection;
				_with17.CommandType = CommandType.StoredProcedure;
				_with17.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.INS_REM_JC_ITEM_DET";
				var _with18 = _with17.Parameters;
				insItemCommand.Parameters.Add("JOB_CARD_FK_IN", JCPK).Direction = ParameterDirection.Input;

				insItemCommand.Parameters.Add("ITEM_DESC_IN", OracleDbType.Varchar2, 50, "ITEM_DESC").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["ITEM_DESC_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "QTY").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("LENGTH_IN", OracleDbType.Varchar2, 20, "LENGTH").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["LENGTH_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("WIDTH_IN", OracleDbType.Int32, 10, "WIDTH").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("HEIGHT_IN", OracleDbType.Int32, 10, "HEIGHT").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("VOLUME_IN", OracleDbType.Int32, 10, "VOLUME").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("WEIGHT_IN", OracleDbType.Int32, 10, "WEIGHT").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("VIA_MODE_IN", OracleDbType.Int32, 10, "TRANS_MODE").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["VIA_MODE_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("PACK_TYPE_FK_IN", OracleDbType.Int32, 6, "PACK_TYPE_PK").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("PACK_NR_IN", OracleDbType.Int32, 10, "PACK_NUMBER").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["PACK_NR_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("COND_ORIGIN_IN", OracleDbType.Varchar2, 200, "CONDITION_AT_ORIGIN").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["COND_ORIGIN_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("EXP_DESTINATION_IN", OracleDbType.Varchar2, 200, "EXCEPTION_AT_DESTN").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["EXP_DESTINATION_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("CONT_ULD_NR_IN", OracleDbType.Varchar2, 200, "CONTAINER_ULD").Direction = ParameterDirection.Input;
				insItemCommand.Parameters["CONT_ULD_NR_IN"].SourceVersion = DataRowVersion.Current;

				insItemCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				insItemCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ITEM_PK").Direction = ParameterDirection.Output;
				insItemCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with19 = updItemCommand;
				_with19.Connection = objWF.MyConnection;
				_with19.CommandType = CommandType.StoredProcedure;
				_with19.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.UPD_REM_JC_ITEM_DET";
				var _with20 = _with19.Parameters;
				updItemCommand.Parameters.Add("JC_ITEM_DTLS_PK_IN", OracleDbType.Int32, 10, "ITEM_PK").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["JC_ITEM_DTLS_PK_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("JOB_CARD_FK_IN", JCPK).Direction = ParameterDirection.Input;

				updItemCommand.Parameters.Add("ITEM_DESC_IN", OracleDbType.Varchar2, 50, "ITEM_DESC").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["ITEM_DESC_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "QTY").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("LENGTH_IN", OracleDbType.Varchar2, 20, "LENGTH").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["LENGTH_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("WIDTH_IN", OracleDbType.Int32, 10, "WIDTH").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("HEIGHT_IN", OracleDbType.Int32, 10, "HEIGHT").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("VOLUME_IN", OracleDbType.Int32, 10, "VOLUME").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("WEIGHT_IN", OracleDbType.Int32, 10, "WEIGHT").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("VIA_MODE_IN", OracleDbType.Int32, 10, "TRANS_MODE").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["VIA_MODE_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("PACK_TYPE_FK_IN", OracleDbType.Int32, 6, "PACK_TYPE_PK").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("PACK_NR_IN", OracleDbType.Int32, 10, "PACK_NUMBER").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["PACK_NR_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("COND_ORIGIN_IN", OracleDbType.Varchar2, 200, "CONDITION_AT_ORIGIN").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["COND_ORIGIN_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("EXP_DESTINATION_IN", OracleDbType.Varchar2, 200, "EXCEPTION_AT_DESTN").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["EXP_DESTINATION_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("CONT_ULD_NR_IN", OracleDbType.Varchar2, 200, "CONTAINER_ULD").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["CONT_ULD_NR_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				updItemCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION").Direction = ParameterDirection.Input;
				updItemCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				updItemCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "ITEM_PK").Direction = ParameterDirection.Output;
				updItemCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with21 = objWF.MyDataAdapter;

				_with21.InsertCommand = insItemCommand;
				_with21.InsertCommand.Transaction = TRAN;

				_with21.UpdateCommand = updItemCommand;
				_with21.UpdateCommand.Transaction = TRAN;
				RecAfct = _with21.Update(ItemDS.Tables[0]);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					//added by surya prasad for implementing protocol rollback
					if (IsEditing == false) {
						RollbackProtocolKey("REMOVAL JOB CARD", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), JCRefNr, System.DateTime.Now);
					}
					return arrMessage;
				}

				var _with22 = insTrptCommand;
				_with22.Connection = objWF.MyConnection;
				_with22.CommandType = CommandType.StoredProcedure;
				_with22.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.INS_REM_JC_TRPT_DET";
				var _with23 = _with22.Parameters;
				insTrptCommand.Parameters.Add("JOB_CARD_FK_IN", JCPK).Direction = ParameterDirection.Input;

				insTrptCommand.Parameters.Add("TRANS_MODE_IN", OracleDbType.Int32, 10, "TRANS_MODE").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["TRANS_MODE_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("NO_OF_PCS_IN", OracleDbType.Varchar2, 5, "NO_OF_PKG").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["NO_OF_PCS_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("VOLUME_IN", OracleDbType.Int32, 10, "VOLUME").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("WEIGHT_IN", OracleDbType.Int32, 10, "WEIGHT").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("POL_FK_IN", OracleDbType.Int32, 10, "POLPK").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["POL_FK_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("POD_FK_IN", OracleDbType.Int32, 10, "PODPK").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("ETD_IN", OracleDbType.Date, 20, "ETD").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["ETD_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("ATD_IN", OracleDbType.Date, 20, "ATD").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["ATD_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("ETA_IN", OracleDbType.Date, 20, "ETA").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["ETA_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("ATA_IN", OracleDbType.Date, 20, "ATA").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["ATA_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("CARRIER_NR_IN", OracleDbType.Varchar2, 50, "CARRIER_NR").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["CARRIER_NR_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "LOAD_DATE").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("MOVE_DATE_IN", OracleDbType.Date, 20, "MOVE_DATE").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["MOVE_DATE_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("CONT_CLR_DT_IN", OracleDbType.Date, 20, "CUSTOMS_CLEARANCE_DT").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["CONT_CLR_DT_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("DEL_DATE_IN", OracleDbType.Date, 20, "DELIVERY_DATE").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["DEL_DATE_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				insTrptCommand.Parameters.Add("CARRIER_FK_IN", OracleDbType.Int32, 10, "CARRIER_PK").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["CARRIER_FK_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("CARRIER_ID_IN", OracleDbType.Varchar2, 25, "CARRIER_ID").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["CARRIER_ID_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("BL_NUMBER_IN", OracleDbType.Varchar2, 25, "BL_NUMBER").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["BL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("BL_DATE_IN", OracleDbType.Date, 20, "BL_DATE").Direction = ParameterDirection.Input;
				insTrptCommand.Parameters["BL_DATE_IN"].SourceVersion = DataRowVersion.Current;

				insTrptCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRPT_PK").Direction = ParameterDirection.Output;
				insTrptCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with24 = updTrptCommand;
				_with24.Connection = objWF.MyConnection;
				_with24.CommandType = CommandType.StoredProcedure;
				_with24.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.UPD_REM_JC_TRPT_DET";
				var _with25 = _with24.Parameters;
				updTrptCommand.Parameters.Add("JC_TRANSP_DTLS_PK_IN", OracleDbType.Int32, 10, "TRPT_PK").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["JC_TRANSP_DTLS_PK_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("JOB_CARD_FK_IN", JCPK).Direction = ParameterDirection.Input;

				updTrptCommand.Parameters.Add("TRANS_MODE_IN", OracleDbType.Int32, 10, "TRANS_MODE").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["TRANS_MODE_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("NO_OF_PCS_IN", OracleDbType.Varchar2, 5, "NO_OF_PKG").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["NO_OF_PCS_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("VOLUME_IN", OracleDbType.Int32, 10, "VOLUME").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("WEIGHT_IN", OracleDbType.Int32, 10, "WEIGHT").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("POL_FK_IN", OracleDbType.Int32, 10, "POLPK").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["POL_FK_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("POD_FK_IN", OracleDbType.Int32, 10, "PODPK").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("ETD_IN", OracleDbType.Date, 20, "ETD").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["ETD_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("ATD_IN", OracleDbType.Date, 20, "ATD").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["ATD_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("ETA_IN", OracleDbType.Date, 20, "ETA").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["ETA_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("ATA_IN", OracleDbType.Date, 20, "ATA").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["ATA_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("CARRIER_NR_IN", OracleDbType.Varchar2, 200, "CARRIER_NR").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["CARRIER_NR_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "LOAD_DATE").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("MOVE_DATE_IN", OracleDbType.Date, 20, "MOVE_DATE").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["MOVE_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("CONT_CLR_DT_IN", OracleDbType.Date, 20, "CUSTOMS_CLEARANCE_DT").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["CONT_CLR_DT_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("DEL_DATE_IN", OracleDbType.Date, 20, "DELIVERY_DATE").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["DEL_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				updTrptCommand.Parameters.Add("CARRIER_FK_IN", OracleDbType.Int32, 10, "CARRIER_PK").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["CARRIER_FK_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("CARRIER_ID_IN", OracleDbType.Varchar2, 200, "CARRIER_ID").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["CARRIER_ID_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("BL_NUMBER_IN", OracleDbType.Varchar2, 25, "BL_NUMBER").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["BL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("BL_DATE_IN", OracleDbType.Date, 20, "BL_DATE").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["BL_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 5, "VERSION").Direction = ParameterDirection.Input;
				updTrptCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				updTrptCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRPT_PK").Direction = ParameterDirection.Output;
				updTrptCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with26 = objWF.MyDataAdapter;

				_with26.InsertCommand = insTrptCommand;
				_with26.InsertCommand.Transaction = TRAN;

				_with26.UpdateCommand = updTrptCommand;
				_with26.UpdateCommand.Transaction = TRAN;
				RecAfct = _with26.Update(TrptDS.Tables[0]);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					//added by surya prasad for implementing protocol rollback
					if (IsEditing == false) {
						RollbackProtocolKey("REMOVAL JOB CARD", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), JCRefNr, System.DateTime.Now);
					}
					return arrMessage;
				}

				var _with27 = insFrtCommand;
				_with27.Connection = objWF.MyConnection;
				_with27.CommandType = CommandType.StoredProcedure;
				_with27.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.INS_REM_JC_FRT_OTH_DET";
				var _with28 = _with27.Parameters;
				insFrtCommand.Parameters.Add("JOB_CARD_FK_IN", JCPK).Direction = ParameterDirection.Input;

				insFrtCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FRT_ELE_PK").Direction = ParameterDirection.Input;
				insFrtCommand.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insFrtCommand.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "PAYMENT_TYPE").Direction = ParameterDirection.Input;
				insFrtCommand.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				insFrtCommand.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_PAYER_PK").Direction = ParameterDirection.Input;
				insFrtCommand.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insFrtCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_PK").Direction = ParameterDirection.Input;
				insFrtCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insFrtCommand.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 20, "AMOUNT").Direction = ParameterDirection.Input;
				insFrtCommand.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

				insFrtCommand.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 15, "ROE").Direction = ParameterDirection.Input;
				insFrtCommand.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

				insFrtCommand.Parameters.Add("RATE_IN_BASE_CURR_IN", OracleDbType.Int32, 20, "AMOUNT_IN_BASE_CURR").Direction = ParameterDirection.Input;
				insFrtCommand.Parameters["RATE_IN_BASE_CURR_IN"].SourceVersion = DataRowVersion.Current;

				insFrtCommand.Parameters.Add("FRT_OTH_CHRG_FLAG_IN", 1).Direction = ParameterDirection.Input;

				insFrtCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				insFrtCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRPT_PK").Direction = ParameterDirection.Output;
				insFrtCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with29 = updFrtCommand;
				_with29.Connection = objWF.MyConnection;
				_with29.CommandType = CommandType.StoredProcedure;
				_with29.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.UPD_REM_JC_FRT_OTH_DET";
				var _with30 = _with29.Parameters;
				updFrtCommand.Parameters.Add("JC_FRT_DTLS_PK_IN", OracleDbType.Int32, 10, "FRT_PK").Direction = ParameterDirection.Input;
				updFrtCommand.Parameters["JC_FRT_DTLS_PK_IN"].SourceVersion = DataRowVersion.Current;

				updFrtCommand.Parameters.Add("JOB_CARD_FK_IN", JCPK).Direction = ParameterDirection.Input;

				updFrtCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FRT_ELE_PK").Direction = ParameterDirection.Input;
				updFrtCommand.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updFrtCommand.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "PAYMENT_TYPE").Direction = ParameterDirection.Input;
				updFrtCommand.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				updFrtCommand.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_PAYER_PK").Direction = ParameterDirection.Input;
				updFrtCommand.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updFrtCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_PK").Direction = ParameterDirection.Input;
				updFrtCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updFrtCommand.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 20, "AMOUNT").Direction = ParameterDirection.Input;
				updFrtCommand.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

				updFrtCommand.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 15, "ROE").Direction = ParameterDirection.Input;
				updFrtCommand.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

				updFrtCommand.Parameters.Add("RATE_IN_BASE_CURR_IN", OracleDbType.Int32, 20, "AMOUNT_IN_BASE_CURR").Direction = ParameterDirection.Input;
				updFrtCommand.Parameters["RATE_IN_BASE_CURR_IN"].SourceVersion = DataRowVersion.Current;

				updFrtCommand.Parameters.Add("FRT_OTH_CHRG_FLAG_IN", 1).Direction = ParameterDirection.Input;

				updFrtCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				updFrtCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 5, "VERSION").Direction = ParameterDirection.Input;
				updFrtCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				updFrtCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "FRT_PK").Direction = ParameterDirection.Output;
				updFrtCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with31 = delFrtCommand;
				_with31.Connection = objWF.MyConnection;
				_with31.CommandType = CommandType.StoredProcedure;
				_with31.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.DEL_REM_JC_FRT_OTH_DET";

				delFrtCommand.Parameters.Add("JC_FRT_DTLS_PK_IN", OracleDbType.Int32, 10, "FRT_PK").Direction = ParameterDirection.Input;
				delFrtCommand.Parameters["JC_FRT_DTLS_PK_IN"].SourceVersion = DataRowVersion.Current;

				delFrtCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "FRT_PK").Direction = ParameterDirection.Output;
				delFrtCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with32 = objWF.MyDataAdapter;

				_with32.InsertCommand = insFrtCommand;
				_with32.InsertCommand.Transaction = TRAN;

				_with32.UpdateCommand = updFrtCommand;
				_with32.UpdateCommand.Transaction = TRAN;

				_with32.DeleteCommand = delFrtCommand;
				_with32.DeleteCommand.Transaction = TRAN;
				RecAfct = _with32.Update(FrtDS.Tables[0]);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					//added by surya prasad for implementing protocol rollback
					if (IsEditing == false) {
						RollbackProtocolKey("REMOVAL JOB CARD", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), JCRefNr, System.DateTime.Now);
					}
					return arrMessage;
				}

				var _with33 = insOthCommand;
				_with33.Connection = objWF.MyConnection;
				_with33.CommandType = CommandType.StoredProcedure;
				_with33.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.INS_REM_JC_FRT_OTH_DET";
				var _with34 = _with33.Parameters;
				insOthCommand.Parameters.Add("JOB_CARD_FK_IN", JCPK).Direction = ParameterDirection.Input;

				insOthCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FRT_ELE_PK").Direction = ParameterDirection.Input;
				insOthCommand.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insOthCommand.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "PAYMENT_TYPE").Direction = ParameterDirection.Input;
				insOthCommand.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				insOthCommand.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_PAYER_PK").Direction = ParameterDirection.Input;
				insOthCommand.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insOthCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_PK").Direction = ParameterDirection.Input;
				insOthCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insOthCommand.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 20, "AMOUNT").Direction = ParameterDirection.Input;
				insOthCommand.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

				insOthCommand.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 15, "ROE").Direction = ParameterDirection.Input;
				insOthCommand.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

				insOthCommand.Parameters.Add("RATE_IN_BASE_CURR_IN", OracleDbType.Int32, 20, "AMOUNT_IN_BASE_CURR").Direction = ParameterDirection.Input;
				insOthCommand.Parameters["RATE_IN_BASE_CURR_IN"].SourceVersion = DataRowVersion.Current;

				insOthCommand.Parameters.Add("FRT_OTH_CHRG_FLAG_IN", 2).Direction = ParameterDirection.Input;

				insOthCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				insOthCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TRPT_PK").Direction = ParameterDirection.Output;
				insOthCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with35 = updOthCommand;
				_with35.Connection = objWF.MyConnection;
				_with35.CommandType = CommandType.StoredProcedure;
				_with35.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.UPD_REM_JC_FRT_OTH_DET";
				var _with36 = _with35.Parameters;
				updOthCommand.Parameters.Add("JC_FRT_DTLS_PK_IN", OracleDbType.Int32, 10, "FRT_PK").Direction = ParameterDirection.Input;
				updOthCommand.Parameters["JC_FRT_DTLS_PK_IN"].SourceVersion = DataRowVersion.Current;

				updOthCommand.Parameters.Add("JOB_CARD_FK_IN", JCPK).Direction = ParameterDirection.Input;

				updOthCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FRT_ELE_PK").Direction = ParameterDirection.Input;
				updOthCommand.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updOthCommand.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "PAYMENT_TYPE").Direction = ParameterDirection.Input;
				updOthCommand.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				updOthCommand.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_PAYER_PK").Direction = ParameterDirection.Input;
				updOthCommand.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updOthCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_PK").Direction = ParameterDirection.Input;
				updOthCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updOthCommand.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 20, "AMOUNT").Direction = ParameterDirection.Input;
				updOthCommand.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

				updOthCommand.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 15, "ROE").Direction = ParameterDirection.Input;
				updOthCommand.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

				updOthCommand.Parameters.Add("RATE_IN_BASE_CURR_IN", OracleDbType.Int32, 20, "AMOUNT_IN_BASE_CURR").Direction = ParameterDirection.Input;
				updOthCommand.Parameters["RATE_IN_BASE_CURR_IN"].SourceVersion = DataRowVersion.Current;

				updOthCommand.Parameters.Add("FRT_OTH_CHRG_FLAG_IN", 2).Direction = ParameterDirection.Input;

				updOthCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				updOthCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 5, "VERSION").Direction = ParameterDirection.Input;
				updOthCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				updOthCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "FRT_PK").Direction = ParameterDirection.Output;
				updOthCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with37 = delOthCommand;
				_with37.Connection = objWF.MyConnection;
				_with37.CommandType = CommandType.StoredProcedure;
				_with37.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.DEL_REM_JC_FRT_OTH_DET";

				delOthCommand.Parameters.Add("JC_FRT_DTLS_PK_IN", OracleDbType.Int32, 10, "FRT_PK").Direction = ParameterDirection.Input;
				delOthCommand.Parameters["JC_FRT_DTLS_PK_IN"].SourceVersion = DataRowVersion.Current;

				delOthCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "FRT_PK").Direction = ParameterDirection.Output;
				delOthCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with38 = objWF.MyDataAdapter;

				_with38.InsertCommand = insOthCommand;
				_with38.InsertCommand.Transaction = TRAN;

				_with38.UpdateCommand = updOthCommand;
				_with38.UpdateCommand.Transaction = TRAN;

				_with38.DeleteCommand = delOthCommand;
				_with38.DeleteCommand.Transaction = TRAN;
				RecAfct = _with38.Update(OthChgDS.Tables[0]);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					//added by surya prasad for implementing protocol rollback
					if (IsEditing == false) {
						RollbackProtocolKey("REMOVAL JOB CARD", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), JCRefNr, System.DateTime.Now);
					}
					return arrMessage;
				}

				var _with39 = insCostCommand;
				_with39.Connection = objWF.MyConnection;
				_with39.CommandType = CommandType.StoredProcedure;
				_with39.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.INS_REM_JC_COST_DET";
				var _with40 = _with39.Parameters;
				insCostCommand.Parameters.Add("JOB_CARD_FK_IN", JCPK).Direction = ParameterDirection.Input;

				insCostCommand.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_PK").Direction = ParameterDirection.Input;
				insCostCommand.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCostCommand.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR").Direction = ParameterDirection.Input;
				insCostCommand.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

				insCostCommand.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_PK").Direction = ParameterDirection.Input;
				insCostCommand.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCostCommand.Parameters.Add("INVOICE_NUMBER_IN", OracleDbType.Varchar2, 20, "INVOICE_NUMBER").Direction = ParameterDirection.Input;
				insCostCommand.Parameters["INVOICE_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

				insCostCommand.Parameters.Add("INVOICE_DATE_IN", OracleDbType.Date, 20, "INVOICE_DATE").Direction = ParameterDirection.Input;
				insCostCommand.Parameters["INVOICE_DATE_IN"].SourceVersion = DataRowVersion.Current;

				insCostCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_PK").Direction = ParameterDirection.Input;
				insCostCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCostCommand.Parameters.Add("ESTIMATED_AMT_IN", OracleDbType.Int32, 20, "EST_COST").Direction = ParameterDirection.Input;
				insCostCommand.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

				insCostCommand.Parameters.Add("INVOICE_AMT_IN", OracleDbType.Int32, 20, "ACT_COST").Direction = ParameterDirection.Input;
				insCostCommand.Parameters["INVOICE_AMT_IN"].SourceVersion = DataRowVersion.Current;

				insCostCommand.Parameters.Add("TAX_PERCENTAGE_IN", OracleDbType.Int32, 20, "TAX_PERCENTAGE").Direction = ParameterDirection.Input;
				insCostCommand.Parameters["TAX_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;

				insCostCommand.Parameters.Add("TAX_AMT_IN", OracleDbType.Int32, 20, "TAX_AMOUNT").Direction = ParameterDirection.Input;
				insCostCommand.Parameters["TAX_AMT_IN"].SourceVersion = DataRowVersion.Current;

				insCostCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				insCostCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "JC_COST_PK").Direction = ParameterDirection.Output;
				insCostCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with41 = updCostCommand;
				_with41.Connection = objWF.MyConnection;
				_with41.CommandType = CommandType.StoredProcedure;
				_with41.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.UPD_REM_JC_COST_DET";
				var _with42 = _with41.Parameters;
				updCostCommand.Parameters.Add("JC_COST_DTL_PK_IN", OracleDbType.Int32, 10, "JC_COST_PK").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["JC_COST_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("JOB_CARD_FK_IN", JCPK).Direction = ParameterDirection.Input;

				updCostCommand.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_PK").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_PK").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("INVOICE_NUMBER_IN", OracleDbType.Varchar2, 20, "INVOICE_NUMBER").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["INVOICE_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("INVOICE_DATE_IN", OracleDbType.Date, 20, "INVOICE_DATE").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["INVOICE_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_PK").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("ESTIMATED_AMT_IN", OracleDbType.Int32, 20, "EST_COST").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("INVOICE_AMT_IN", OracleDbType.Int32, 20, "ACT_COST").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["INVOICE_AMT_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("TAX_PERCENTAGE_IN", OracleDbType.Int32, 20, "TAX_PERCENTAGE").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["TAX_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("TAX_AMT_IN", OracleDbType.Int32, 20, "TAX_AMOUNT").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["TAX_AMT_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				updCostCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 5, "VERSION").Direction = ParameterDirection.Input;
				updCostCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				updCostCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "JC_COST_PK").Direction = ParameterDirection.Output;
				updCostCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with43 = delCostCommand;
				_with43.Connection = objWF.MyConnection;
				_with43.CommandType = CommandType.StoredProcedure;
				_with43.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.DEL_REM_JC_COST_DET";

				delCostCommand.Parameters.Add("JC_COST_DTL_PK_IN", OracleDbType.Int32, 10, "JC_COST_PK").Direction = ParameterDirection.Input;
				delCostCommand.Parameters["JC_COST_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

				delCostCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JC_COST_PK").Direction = ParameterDirection.Output;
				delCostCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with44 = objWF.MyDataAdapter;

				_with44.InsertCommand = insCostCommand;
				_with44.InsertCommand.Transaction = TRAN;

				_with44.UpdateCommand = updCostCommand;
				_with44.UpdateCommand.Transaction = TRAN;

				_with44.DeleteCommand = delCostCommand;
				_with44.DeleteCommand.Transaction = TRAN;
				RecAfct = _with44.Update(CostDS.Tables[0]);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					//added by surya prasad for implementing protocol rollback
					if (IsEditing == false) {
						RollbackProtocolKey("REMOVAL JOB CARD", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), JCRefNr, System.DateTime.Now);
					}
					return arrMessage;
				} else {
					TRAN.Commit();
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				TRAN.Rollback();
				//added by surya prasad for implementing protocol rollback
				if (IsEditing == false) {
					RollbackProtocolKey("REMOVAL JOB CARD", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), JCRefNr, System.DateTime.Now);
				}
				throw oraexp;
			} catch (Exception ex) {
				TRAN.Rollback();
				//added by surya prasad for implementing protocol rollback
				if (IsEditing == false) {
					RollbackProtocolKey("REMOVAL JOB CARD", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), JCRefNr, System.DateTime.Now);
				}
				throw ex;
			} finally {
				objWF.CloseConnection();
			}
		}
		//adding by thiyagarajan on 23/1/09:TrackNTrace Task:VEK Req.
		public void SaveInTrackNTrace(string refno, Int32 refpk, string status, DataSet TrptDS, Int32 Doctype, OracleTransaction TRAN)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			bool chk = false;
			Int32 Return_value = default(Int32);
			Int32 i = default(Int32);
			Int32 j = default(Int32);
			string stuts = null;
			OracleCommand insCommand = new OracleCommand();
			string modes = null;
			string dstrt = null;
			try {
				objWF.OpenConnection();
				objWF.MyConnection = TRAN.Connection;
				insCommand.Connection = objWF.MyConnection;
				insCommand.Transaction = TRAN;
				//To store the delivery information based on no.of transport mode in item details of job card
				for (i = 0; i <= TrptDS.Tables[0].Rows.Count - 1; i++) {
					//chk = False
					//For j = 0 To i
					//    If TrptDS.Tables(0).Rows(j).Item("TRANS_MODE") = TrptDS.Tables(0).Rows(i).Item("TRANS_MODE") And _
					//    TrptDS.Tables(0).Rows(j).Item("DELIVERY_DATE") = TrptDS.Tables(0).Rows(i).Item("DELIVERY_DATE") Then
					//        chk = True
					//    End If
					//Next
					//If chk = False Then
					switch (Convert.ToString(TrptDS.Tables[0].Rows[i]["TRANS_MODE"])) {
						case "1":
							modes = "Air";
							break;
						case "2":
							modes = "Sea";
							break;
						case "3":
							modes = "Land";
							break;
						//Case 4 : modes = "Rail"
					}
					dstrt = Convert.ToString(getDefault(TrptDS.Tables[0].Rows[i]["DELIVERY_DATE"], ""));
					if (dstrt.Length > 0) {
						dstrt = "on " + dstrt;
					}
					stuts = modes + " Cargo Delivered at customer premises " + dstrt;
					insCommand.CommandType = CommandType.StoredProcedure;
					insCommand.CommandText = objWF.MyUserName + ".REM_TRACK_N_TRACE_PKG.REM_TRACK_N_TRACE_INS";
					var _with45 = insCommand.Parameters;
					_with45.Clear();
					_with45.Add("REF_NO_IN", refno).Direction = ParameterDirection.Input;
					_with45.Add("REF_FK_IN", refpk).Direction = ParameterDirection.Input;
					_with45.Add("LOC_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
					_with45.Add("STATUS_IN", stuts).Direction = ParameterDirection.Input;
					_with45.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
					_with45.Add("DOCTYPE_IN", Doctype).Direction = ParameterDirection.Input;
					insCommand.ExecuteNonQuery();
					//End If
				}
				//To store the Job no specically
				insCommand.CommandType = CommandType.StoredProcedure;
				insCommand.CommandText = objWF.MyUserName + ".REM_TRACK_N_TRACE_PKG.REM_TRACK_N_TRACE_INS";
				var _with46 = insCommand.Parameters;
				_with46.Clear();
				_with46.Add("REF_NO_IN", refno).Direction = ParameterDirection.Input;
				_with46.Add("REF_FK_IN", refpk).Direction = ParameterDirection.Input;
				_with46.Add("LOC_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
				_with46.Add("STATUS_IN", status).Direction = ParameterDirection.Input;
				_with46.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
				_with46.Add("DOCTYPE_IN", Doctype).Direction = ParameterDirection.Input;
				insCommand.ExecuteNonQuery();
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		//end

		#endregion

		#region "Fetch Quotation Look Up"
		public string FetchQuotationForJC(string strCond)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand SCM = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string LookUpValue = null;
			string strSERACH_IN = "";
			arr = strCond.Split('~');
			LookUpValue = Convert.ToString(arr.GetValue(0));
			strSERACH_IN = Convert.ToString(arr.GetValue(1));
			try {
				objWF.OpenConnection();
				SCM.Connection = objWF.MyConnection;
				SCM.CommandType = CommandType.StoredProcedure;
				SCM.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.FETCH_QUOTATION_LOOKUP";
				var _with47 = SCM.Parameters;
				_with47.Add("LOOKUP_VALUE_IN", LookUpValue).Direction = ParameterDirection.Input;
				_with47.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
				_with47.Add("USER_MST_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
				_with47.Add("RETURN_VALUE", OracleDbType.NVarchar2, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				SCM.ExecuteNonQuery();
				strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
				return strReturn;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				SCM.Connection.Close();
			}
		}
		#endregion

		#region "Others"
		private object ifDBNull(object col)
		{
			if (Convert.ToString(col).Length == 0) {
				return "";
			} else {
				return col;
			}
		}
		//This function is called to generate the enquiry reference no.
		//Called for Enquiry on New Booking
		public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
		{
			return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
		}
		public DataSet FetchROE(Int64 baseCurrency)
		{
			StringBuilder strSQL = new StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				strSQL.Append("SELECT");
				strSQL.Append("    CURR.CURRENCY_MST_PK,");
				strSQL.Append("    CURR.CURRENCY_ID,");
				strSQL.Append("    ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK," + baseCurrency + ",round(sysdate - .5)),6) AS ROE");
				strSQL.Append("FROM");
				strSQL.Append("    CURRENCY_TYPE_MST_TBL CURR");
				strSQL.Append("WHERE");
				strSQL.Append("    CURR.ACTIVE_FLAG = 1");
				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Fetch and Save Special Instruction"

		#region "Fetch Special Instruction"
		public DataSet FetchJCSpclinst(int JCPK, int TRANS_MODE_IN, int JC_TRANSP_DTLS_PK_IN)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with48 = objWF.MyCommand.Parameters;
				_with48.Add("JOB_CARD_PK_IN", JCPK).Direction = ParameterDirection.Input;
				_with48.Add("TRANS_MODE_IN", TRANS_MODE_IN).Direction = ParameterDirection.Input;
				_with48.Add("JC_TRANSP_DTLS_PK_IN", JC_TRANSP_DTLS_PK_IN).Direction = ParameterDirection.Input;
				_with48.Add("JC_SPCL_INST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				SpclInstDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_JC_SPCL_INST");
				return SpclInstDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Save Special Instructions"
		public ArrayList SaveSpclInst(DataSet SpclInstDS, bool IsEditing)
		{
			OracleTransaction TRAN = null;
			Int32 RecAfct = default(Int32);
			OracleCommand insSpclCommand = new OracleCommand();
			OracleCommand updSpclCommand = new OracleCommand();
			try {
				objWF.OpenConnection();
				TRAN = objWF.MyConnection.BeginTransaction();
				var _with49 = insSpclCommand;
				_with49.Connection = objWF.MyConnection;
				_with49.CommandType = CommandType.StoredProcedure;
				_with49.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.INS_JC_SPCL_INST";
				var _with50 = _with49.Parameters;
				insSpclCommand.Parameters.Add("JOB_CARD_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_FK").Direction = ParameterDirection.Input;
				insSpclCommand.Parameters["JOB_CARD_FK_IN"].SourceVersion = DataRowVersion.Current;

				insSpclCommand.Parameters.Add("JC_CARRIER_INST_IN", OracleDbType.Varchar2, 500, "JC_CARRIER_INST").Direction = ParameterDirection.Input;
				insSpclCommand.Parameters["JC_CARRIER_INST_IN"].SourceVersion = DataRowVersion.Current;

				insSpclCommand.Parameters.Add("JC_MARKS_NOS_IN", OracleDbType.Varchar2, 500, "JC_MARKS_NOS").Direction = ParameterDirection.Input;
				insSpclCommand.Parameters["JC_MARKS_NOS_IN"].SourceVersion = DataRowVersion.Current;

				insSpclCommand.Parameters.Add("JC_GOODS_DESC_IN", OracleDbType.Varchar2, 500, "JC_GOODS_DESC").Direction = ParameterDirection.Input;
				insSpclCommand.Parameters["JC_GOODS_DESC_IN"].SourceVersion = DataRowVersion.Current;

				insSpclCommand.Parameters.Add("JC_MVMT_CERT_NR_IN", OracleDbType.Varchar2, 50, "JC_MVMT_CERT_NR").Direction = ParameterDirection.Input;
				insSpclCommand.Parameters["JC_MVMT_CERT_NR_IN"].SourceVersion = DataRowVersion.Current;

				insSpclCommand.Parameters.Add("JC_TERMS_DELIVERY_IN", OracleDbType.Varchar2, 100, "JC_TERMS_DELIVERY").Direction = ParameterDirection.Input;
				insSpclCommand.Parameters["JC_TERMS_DELIVERY_IN"].SourceVersion = DataRowVersion.Current;

				insSpclCommand.Parameters.Add("JC_TRANSP_DTLS_FK_IN", OracleDbType.Int32, 10, "JC_TRANSP_DTLS_FK").Direction = ParameterDirection.Input;
				insSpclCommand.Parameters["JC_TRANSP_DTLS_FK_IN"].SourceVersion = DataRowVersion.Current;

				insSpclCommand.Parameters.Add("TRANS_MODE_IN", OracleDbType.Int32, 1, "TRANS_MODE").Direction = ParameterDirection.Input;
				insSpclCommand.Parameters["TRANS_MODE_IN"].SourceVersion = DataRowVersion.Current;

				insSpclCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "JC_SPL_INST_PK").Direction = ParameterDirection.Output;
				insSpclCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with51 = updSpclCommand;
				_with51.Connection = objWF.MyConnection;
				_with51.CommandType = CommandType.StoredProcedure;
				_with51.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.UPD_JC_SPCL_INST";
				var _with52 = _with51.Parameters;
				updSpclCommand.Parameters.Add("JC_SPL_INST_PK_IN", OracleDbType.Int32, 10, "JC_SPL_INST_PK").Direction = ParameterDirection.Input;
				updSpclCommand.Parameters["JC_SPL_INST_PK_IN"].SourceVersion = DataRowVersion.Current;

				updSpclCommand.Parameters.Add("JOB_CARD_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_FK").Direction = ParameterDirection.Input;
				updSpclCommand.Parameters["JOB_CARD_FK_IN"].SourceVersion = DataRowVersion.Current;

				updSpclCommand.Parameters.Add("JC_CARRIER_INST_IN", OracleDbType.Varchar2, 500, "JC_CARRIER_INST").Direction = ParameterDirection.Input;
				updSpclCommand.Parameters["JC_CARRIER_INST_IN"].SourceVersion = DataRowVersion.Current;

				updSpclCommand.Parameters.Add("JC_MARKS_NOS_IN", OracleDbType.Varchar2, 500, "JC_MARKS_NOS").Direction = ParameterDirection.Input;
				updSpclCommand.Parameters["JC_MARKS_NOS_IN"].SourceVersion = DataRowVersion.Current;

				updSpclCommand.Parameters.Add("JC_GOODS_DESC_IN", OracleDbType.Varchar2, 500, "JC_GOODS_DESC").Direction = ParameterDirection.Input;
				updSpclCommand.Parameters["JC_GOODS_DESC_IN"].SourceVersion = DataRowVersion.Current;

				updSpclCommand.Parameters.Add("JC_MVMT_CERT_NR_IN", OracleDbType.Varchar2, 50, "JC_MVMT_CERT_NR").Direction = ParameterDirection.Input;
				updSpclCommand.Parameters["JC_MVMT_CERT_NR_IN"].SourceVersion = DataRowVersion.Current;

				updSpclCommand.Parameters.Add("JC_TERMS_DELIVERY_IN", OracleDbType.Varchar2, 100, "JC_TERMS_DELIVERY").Direction = ParameterDirection.Input;
				updSpclCommand.Parameters["JC_TERMS_DELIVERY_IN"].SourceVersion = DataRowVersion.Current;

				updSpclCommand.Parameters.Add("JC_TRANSP_DTLS_FK_IN", OracleDbType.Int32, 10, "JC_TRANSP_DTLS_FK").Direction = ParameterDirection.Input;
				updSpclCommand.Parameters["JC_TRANSP_DTLS_FK_IN"].SourceVersion = DataRowVersion.Current;

				updSpclCommand.Parameters.Add("TRANS_MODE_IN", OracleDbType.Int32, 1, "TRANS_MODE").Direction = ParameterDirection.Input;
				updSpclCommand.Parameters["TRANS_MODE_IN"].SourceVersion = DataRowVersion.Current;

				updSpclCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "JC_SPL_INST_PK").Direction = ParameterDirection.Output;
				updSpclCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with53 = objWF.MyDataAdapter;
				_with53.InsertCommand = insSpclCommand;
				_with53.InsertCommand.Transaction = TRAN;

				_with53.UpdateCommand = updSpclCommand;
				_with53.UpdateCommand.Transaction = TRAN;

				RecAfct = _with53.Update(SpclInstDS);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					TRAN.Commit();
					if (IsEditing == false) {
						SpclInstPK = Convert.ToInt32(insSpclCommand.Parameters["RETURN_VALUE"].Value);
					} else {
						SpclInstPK = Convert.ToInt32(SpclInstDS.Tables[0].Rows[0]["JC_SPL_INST_PK"]);
					}
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.CloseConnection();
			}
		}
		#endregion

		#endregion

		#region "Fetch and Save CMR Details"

		#region "Fetch CMR Details"
		public DataSet FetchJCCMR(int JCPK)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with54 = objWF.MyCommand.Parameters;
				_with54.Add("JOB_CARD_PK_IN", JCPK).Direction = ParameterDirection.Input;
				_with54.Add("JC_CMR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				CMRDS = objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_JC_CMR");
				return CMRDS;
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Customer Details"
		public DataSet FetchCMRCustDet(int CustPK)
		{
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with55 = objWF.MyCommand.Parameters;
				_with55.Add("CUSTOMER_PK_IN", CustPK).Direction = ParameterDirection.Input;
				_with55.Add("CUST_DET_CURR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataSet("PKG_REMOVAL_JOB_CARD", "FETCH_CUST_DETAILS");
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Save CMR Details"
		public ArrayList SaveCMR(DataSet CMRDS, bool IsEditing)
		{
			OracleTransaction TRAN = null;
			Int32 RecAfct = default(Int32);
			OracleCommand insCMRCommand = new OracleCommand();
			OracleCommand updCMRCommand = new OracleCommand();
			try {
				objWF.OpenConnection();
				TRAN = objWF.MyConnection.BeginTransaction();
				var _with56 = insCMRCommand;
				_with56.Connection = objWF.MyConnection;
				_with56.CommandType = CommandType.StoredProcedure;
				_with56.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.INS_JC_CMR";
				var _with57 = _with56.Parameters;
				insCMRCommand.Parameters.Add("JOB_CARD_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_FK").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JOB_CARD_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("JC_CMR_SHIPPER_NAME_IN", OracleDbType.Varchar2, 50, "JC_CMR_SHIPPER_NAME").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JC_CMR_SHIPPER_NAME_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("JC_CMR_SHIPPER_ADD_IN", OracleDbType.Varchar2, 200, "JC_CMR_SHIPPER_ADD").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JC_CMR_SHIPPER_ADD_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("JC_CMR_CONSIGNEE_NAME_IN", OracleDbType.Varchar2, 50, "JC_CMR_CONSIGNEE_NAME").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JC_CMR_CONSIGNEE_NAME_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("JC_CMR_CONSIGNEE_ADD_IN", OracleDbType.Varchar2, 200, "JC_CMR_CONSIGNEE_ADD").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JC_CMR_CONSIGNEE_ADD_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("JC_CMR_DOC_INVOICE_IN", OracleDbType.Varchar2, 1, "JC_CMR_DOC_INVOICE").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JC_CMR_DOC_INVOICE_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("JC_CMR_DOC_INS_COST_IN", OracleDbType.Int32, 1, "JC_CMR_DOC_INS_COST").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JC_CMR_DOC_INS_COST_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("JC_CMR_DOC_CUST_INV_IN", OracleDbType.Int32, 1, "JC_CMR_DOC_CUST_INV").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JC_CMR_DOC_CUST_INV_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("JC_CMR_DOC_PACK_LIST_IN", OracleDbType.Int32, 1, "JC_CMR_DOC_PACK_LIST").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JC_CMR_DOC_PACK_LIST_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("JC_CMR_PL_ORI_COPY_IN", OracleDbType.Int32, 1, "JC_CMR_PL_ORI_COPY").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JC_CMR_PL_ORI_COPY_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("JC_CMR_DOC_CERT_ORIGIN_IN", OracleDbType.Int32, 1, "JC_CMR_DOC_CERT_ORIGIN").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JC_CMR_DOC_CERT_ORIGIN_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("JC_CMR_CO_ORI_COPY_IN", OracleDbType.Int32, 1, "JC_CMR_CO_ORI_COPY").Direction = ParameterDirection.Input;
				insCMRCommand.Parameters["JC_CMR_CO_ORI_COPY_IN"].SourceVersion = DataRowVersion.Current;

				insCMRCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "JC_CMR_PRINT_PK").Direction = ParameterDirection.Output;
				insCMRCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with58 = updCMRCommand;
				_with58.Connection = objWF.MyConnection;
				_with58.CommandType = CommandType.StoredProcedure;
				_with58.CommandText = objWF.MyUserName + ".PKG_REMOVAL_JOB_CARD.UPD_JC_CMR";
				var _with59 = _with58.Parameters;
				updCMRCommand.Parameters.Add("JC_CMR_PRINT_PK_IN", OracleDbType.Int32, 10, "JC_CMR_PRINT_PK").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_PRINT_PK_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JOB_CARD_FK_IN", OracleDbType.Int32, 10, "JOB_CARD_FK").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JOB_CARD_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JC_CMR_SHIPPER_NAME_IN", OracleDbType.Varchar2, 50, "JC_CMR_SHIPPER_NAME").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_SHIPPER_NAME_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JC_CMR_SHIPPER_ADD_IN", OracleDbType.Varchar2, 200, "JC_CMR_SHIPPER_ADD").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_SHIPPER_ADD_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JC_CMR_CONSIGNEE_NAME_IN", OracleDbType.Varchar2, 50, "JC_CMR_CONSIGNEE_NAME").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_CONSIGNEE_NAME_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JC_CMR_CONSIGNEE_ADD_IN", OracleDbType.Varchar2, 200, "JC_CMR_CONSIGNEE_ADD").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_CONSIGNEE_ADD_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JC_CMR_DOC_INVOICE_IN", OracleDbType.Varchar2, 1, "JC_CMR_DOC_INVOICE").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_DOC_INVOICE_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JC_CMR_DOC_INS_COST_IN", OracleDbType.Int32, 1, "JC_CMR_DOC_INS_COST").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_DOC_INS_COST_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JC_CMR_DOC_CUST_INV_IN", OracleDbType.Int32, 1, "JC_CMR_DOC_CUST_INV").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_DOC_CUST_INV_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JC_CMR_DOC_PACK_LIST_IN", OracleDbType.Int32, 1, "JC_CMR_DOC_PACK_LIST").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_DOC_PACK_LIST_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JC_CMR_PL_ORI_COPY_IN", OracleDbType.Int32, 1, "JC_CMR_PL_ORI_COPY").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_PL_ORI_COPY_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JC_CMR_DOC_CERT_ORIGIN_IN", OracleDbType.Int32, 1, "JC_CMR_DOC_CERT_ORIGIN").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_DOC_CERT_ORIGIN_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("JC_CMR_CO_ORI_COPY_IN", OracleDbType.Int32, 1, "JC_CMR_CO_ORI_COPY").Direction = ParameterDirection.Input;
				updCMRCommand.Parameters["JC_CMR_CO_ORI_COPY_IN"].SourceVersion = DataRowVersion.Current;

				updCMRCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "JC_CMR_PRINT_PK").Direction = ParameterDirection.Output;
				updCMRCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with60 = objWF.MyDataAdapter;
				_with60.InsertCommand = insCMRCommand;
				_with60.InsertCommand.Transaction = TRAN;

				_with60.UpdateCommand = updCMRCommand;
				_with60.UpdateCommand.Transaction = TRAN;

				RecAfct = _with60.Update(CMRDS);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					TRAN.Commit();
					if (IsEditing == false) {
						CMRPK = Convert.ToInt32(insCMRCommand.Parameters["RETURN_VALUE"].Value);
					} else {
						CMRPK = Convert.ToInt32(CMRDS.Tables[0].Rows[0]["JC_CMR_PRINT_PK"]);
					}
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				}
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#endregion

		//ADDED BY MINAKSHI ON 30-JANUARY-09 FOR CMR REPORT
		#region "Fetch For CMR Report"
		public DataSet FetchLocation(long Loc)
		{
			WorkFlow ObjWk = new WorkFlow();
			StringBuilder StrSqlBuilder = new StringBuilder();
			StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
			StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
			StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
			StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID,");
			StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK");
			StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST");
			StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND L.LOCATION_MST_PK = " + Loc + "");

			try {
				return ObjWk.GetDataSet(StrSqlBuilder.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet fetchCMRreport(int PK, long Loc)
		{
			try {
				string strSql = null;
				strSql = strSql + "SELECT DISTINCT ";
				strSql = strSql + "JCCMR.JOB_CARD_FK,";
				strSql = strSql + "LMT.LOCATION_NAME ISSUEDAT,";
				strSql = strSql + "JCCMR.JC_CMR_SHIPPER_NAME,";
				strSql = strSql + "JCCMR.JC_CMR_SHIPPER_ADD,";
				strSql = strSql + "JCMST.JOB_CARD_REF,";
				strSql = strSql + "JCCMR.JC_CMR_CONSIGNEE_NAME,";
				strSql = strSql + "JCCMR.JC_CMR_CONSIGNEE_ADD,";
				strSql = strSql + "JCMST.JOB_CARD_PFD_FK,";
				strSql = strSql + "JCMST.JOB_CARD_PFD_ADDR1,";
				strSql = strSql + "JCMST.JOB_CARD_PFD_ADDR2,";
				strSql = strSql + "JCMST.JOB_CARD_PFD_CITY,";
				strSql = strSql + "JCMST.JOB_CARD_PFD_ZIP,";
				strSql = strSql + "JCMST.JOB_CARD_PFD_COUNTRY_FK,";
				strSql = strSql + "CMT.COUNTRY_NAME,";
				strSql = strSql + "JCTRDE.CARRIER_ID,";
				strSql = strSql + "JCTRDE.CARRIER_NR VECHICLEREG_NO,";
				strSql = strSql + "TO_CHAR(JCTRDE.ATD,DATEFORMAT) DATE_DEPARTURE,";
				strSql = strSql + "DECODE(JCTRDE.TRANS_MODE, '1', 'Air', '2', 'Sea', '3', 'Land') TRANS_MODE,";
				strSql = strSql + "JCTRDE.POL_FK POLPK,";
				strSql = strSql + "(CASE WHEN JCTRDE.TRANS_MODE = 3 THEN PLR.PLACE_CODE  ELSE POL.PORT_ID END) POL,";
				strSql = strSql + "JCTRDE.POD_FK PODPK,";
				strSql = strSql + "(CASE WHEN JCTRDE.TRANS_MODE = 3 THEN PFD.PLACE_CODE  ELSE POD.PORT_ID END) POD,";
				strSql = strSql + " '' VESSEL,";
				//JCTRDE.CARRIER_NR
				strSql = strSql + "POD.PORT_NAME FINALDESTINATION,";
				// strSql = strSql & vbCrLf & "JCSPINST.JC_TERMS_DELIVERY TERMSOF_TRANSPORT,"
				strSql = strSql + "Q.JC_TERMS_DELIVERY TERMSOF_TRANSPORT,";
				strSql = strSql + "SUM(JCTRDE.WEIGHT) WEIGHT,";
				strSql = strSql + "SUM(JCTRDE.VOLUME) VOLUME,";
				strSql = strSql + "Q.JC_CARRIER_INST,";
				strSql = strSql + "Q.JC_MVMT_CERT_NR,";
				// strSql = strSql & vbCrLf & "JCSPINST.JC_CARRIER_INST,"
				// strSql = strSql & vbCrLf & "JCSPINST.JC_MVMT_CERT_NR,"
				strSql = strSql + "JCCMR.JC_CMR_DOC_INVOICE,";
				strSql = strSql + "JCCMR.JC_CMR_DOC_INS_COST,";
				strSql = strSql + "JCCMR.JC_CMR_DOC_CUST_INV,";
				strSql = strSql + "JCCMR.JC_CMR_DOC_PACK_LIST,";
				strSql = strSql + "JCCMR.JC_CMR_PL_ORI_COPY,";
				strSql = strSql + "JCCMR.JC_CMR_DOC_CERT_ORIGIN,";
				strSql = strSql + "JCCMR.JC_CMR_CO_ORI_COPY";
				strSql = strSql + " FROM REM_M_JOB_CARD_MST_TBL JCMST,";
				//strSql = strSql & vbCrLf & " REM_T_JC_ITEM_DTLS_TBL      JCITDE,"
				strSql = strSql + " REM_T_JC_TRANSP_DTLS_TBL    JCTRDE,";
				//strSql = strSql & vbCrLf & " REM_T_JC_FRT_DTLS_TBL       JCFRDE,"
				//strSql = strSql & vbCrLf & " REM_T_JC_FRT_DTLS_TBL       JCFRDE,"
				strSql = strSql + " REM_T_JC_CMR_PRINT_TBL      JCCMR,";
				strSql = strSql + " PORT_MST_TBL POL,";
				strSql = strSql + " PORT_MST_TBL POD,";
				//strSql = strSql & vbCrLf & " REM_T_JC_SPL_INST_TBL      JCSPINST,"
				strSql = strSql + " COUNTRY_MST_TBL            CMT,";
				strSql = strSql + " PLACE_MST_TBL              PLR,";
				strSql = strSql + " PLACE_MST_TBL              PFD,";
				strSql = strSql + " LOCATION_MST_TBL          LMT,";
				strSql = strSql + "(SELECT JTRNSP.JC_TRANSP_DTLS_PK,";
				strSql = strSql + "JSPL.JC_SPL_INST_PK,";
				strSql = strSql + "JSPL.JC_CARRIER_INST,";
				strSql = strSql + "JSPL.JC_MVMT_CERT_NR,";
				strSql = strSql + "JSPL.JC_TERMS_DELIVERY";
				strSql = strSql + " FROM REM_T_JC_TRANSP_DTLS_TBL JTRNSP,";
				strSql = strSql + " REM_T_JC_SPL_INST_TBL JSPL";
				strSql = strSql + " WHERE JTRNSP.JOB_CARD_FK = JSPL.JOB_CARD_FK";
				strSql = strSql + " AND  JTRNSP.JC_TRANSP_DTLS_PK = JSPL.JC_TRANSP_DTLS_FK";
				strSql = strSql + " AND JSPL.JOB_CARD_FK = " + PK + ")Q ";
				strSql = strSql + " WHERE JCMST.JOB_CARD_PK=JCTRDE.JOB_CARD_FK";
				// strSql = strSql & vbCrLf & " AND JCMST.JOB_CARD_PK=JCFRDE.JOB_CARD_FK"
				// strSql = strSql & vbCrLf & " AND JCMST.JOB_CARD_PK=JCSPINST.JOB_CARD_FK"
				strSql = strSql + " AND JCTRDE.POL_FK=POL.PORT_MST_PK(+)";
				strSql = strSql + " AND JCTRDE.POD_FK=POD.PORT_MST_PK(+)";
				strSql = strSql + " AND JCMST.JOB_CARD_PK=JCCMR.JOB_CARD_FK";
				strSql = strSql + " AND JCMST.JOB_CARD_PFD_COUNTRY_FK=CMT.COUNTRY_MST_PK";
				strSql = strSql + " AND JCTRDE.POL_FK = PLR.PLACE_PK(+)";
				strSql = strSql + " AND JCTRDE.POD_FK = PFD.PLACE_PK(+)";
				//strSql = strSql & vbCrLf & " AND JCSPINST.JC_TRANSP_DTLS_FK = JCTRDE.JC_TRANSP_DTLS_PK"
				strSql = strSql + " AND Q.JC_TRANSP_DTLS_PK(+) = JCTRDE.JC_TRANSP_DTLS_PK";
				// strSql = strSql & vbCrLf & " AND JCTRDE.TRANS_MODE = " & ViaMode
				strSql = strSql + " AND JCCMR.JOB_CARD_FK = " + PK;
				strSql = strSql + " AND LMT.LOCATION_MST_PK = " + Loc;
				strSql = strSql + " GROUP BY JCCMR.JOB_CARD_FK,JCCMR.JC_CMR_SHIPPER_NAME,";
				strSql = strSql + " JCCMR.JC_CMR_SHIPPER_ADD,JCMST.JOB_CARD_REF,";
				strSql = strSql + " JCCMR.JC_CMR_CONSIGNEE_NAME,JCCMR.JC_CMR_CONSIGNEE_ADD,";
				strSql = strSql + " JCMST.JOB_CARD_PFD_FK,JCMST.JOB_CARD_PFD_ADDR1,";
				strSql = strSql + " JCMST.JOB_CARD_PFD_ADDR2,JCMST.JOB_CARD_PFD_CITY,";
				strSql = strSql + " JCMST.JOB_CARD_PFD_ZIP,JCMST.JOB_CARD_PFD_COUNTRY_FK,";
				strSql = strSql + " CMT.COUNTRY_NAME,JCTRDE.CARRIER_ID,";
				strSql = strSql + " JCTRDE.ATD,JCTRDE.TRANS_MODE,";
				strSql = strSql + " JCTRDE.POL_FK,PLR.PLACE_CODE,";
				strSql = strSql + " POL.PORT_ID,JCTRDE.POD_FK,";
				strSql = strSql + " PFD.PLACE_CODE,POD.PORT_ID,";
				strSql = strSql + " JCTRDE.CARRIER_NR,POD.PORT_NAME,";
				// strSql = strSql & vbCrLf & " JCSPINST.JC_TERMS_DELIVERY,JCSPINST.JC_CARRIER_INST,"
				//strSql = strSql & vbCrLf & " JCSPINST.JC_MVMT_CERT_NR,JCCMR.JC_CMR_DOC_INVOICE,"
				strSql = strSql + " Q.JC_TERMS_DELIVERY,Q.JC_CARRIER_INST,";
				strSql = strSql + " Q.JC_MVMT_CERT_NR,JCCMR.JC_CMR_DOC_INVOICE,";
				strSql = strSql + " JCCMR.JC_CMR_DOC_INS_COST,JCCMR.JC_CMR_DOC_CUST_INV,";
				strSql = strSql + " JCCMR.JC_CMR_DOC_PACK_LIST,JCCMR.JC_CMR_PL_ORI_COPY,";
				strSql = strSql + " JCCMR.JC_CMR_DOC_CERT_ORIGIN,JCCMR.JC_CMR_CO_ORI_COPY,LMT.LOCATION_NAME";
				WorkFlow objWF = new WorkFlow();
				return objWF.GetDataSet(strSql);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet fetchMarksNos(int PK, int ViaMode, string ItemViaMode, bool CMRPrint = false)
		{
			try {
				string strSql = null;
				strSql = strSql + "SELECT DECODE(JCTRDE.TRANS_MODE, '1', 'Air', '2', 'Sea', '3', 'Land') TRANS_MODE,";
				strSql = strSql + "JCITDE.VIA_MODE,";
				strSql = strSql + "(SELECT JSP.JC_MARKS_NOS FROM REM_T_JC_SPL_INST_TBL JSP WHERE JSP.JC_TRANSP_DTLS_FK = JCTRDE.JC_TRANSP_DTLS_PK)JC_MARKS_NOS ,";
				strSql = strSql + "JCITDE.PACK_TYPE_FK,";
				strSql = strSql + "PTMT.PACK_TYPE_ID TYPEPACKAGES,";
				strSql = strSql + "JCITDE.PACK_NR NOOFPACKAGES,";
				strSql = strSql + "(SELECT JS.JC_GOODS_DESC FROM REM_T_JC_SPL_INST_TBL JS WHERE JS.JC_TRANSP_DTLS_FK = JCTRDE.JC_TRANSP_DTLS_PK)GOODDESC";
				strSql = strSql + " FROM REM_M_JOB_CARD_MST_TBL JCMST,REM_T_JC_TRANSP_DTLS_TBL  JCTRDE,";
				strSql = strSql + " REM_T_JC_ITEM_DTLS_TBL JCITDE,PACK_TYPE_MST_TBL PTMT";
				strSql = strSql + " WHERE JCMST.JOB_CARD_PK = JCTRDE.JOB_CARD_FK";
				strSql = strSql + " AND JCITDE.JOB_CARD_FK = JCMST.JOB_CARD_PK";
				// strSql = strSql & vbCrLf & " AND JCTRDE.JC_TRANSP_DTLS_PK = JCSPINST.JC_TRANSP_DTLS_FK"
				strSql = strSql + " AND JCITDE.PACK_TYPE_FK = PTMT.PACK_TYPE_MST_PK";
				strSql = strSql + " AND JCMST.JOB_CARD_PK = " + PK;
				if (CMRPrint == false) {
					strSql = strSql + " AND JCITDE.VIA_MODE IN ( " + ItemViaMode + ")";
					strSql = strSql + " AND JCTRDE.TRANS_MODE = " + ViaMode;
				}
				strSql = strSql + " AND (DECODE(JCITDE.VIA_MODE, 4, 3)= JCTRDE.TRANS_MODE OR JCITDE.VIA_MODE = JCTRDE.TRANS_MODE)";
				WorkFlow objWF = new WorkFlow();
				return objWF.GetDataSet(strSql);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		// Fetching the Customs Clauses  
		public DataSet FetchClauseCMRSubReport(int PK)
		{
			try {
				string Sql = null;
				string strSql = null;
				WorkFlow objWF = new WorkFlow();
				string pks = null;
				Sql = Sql + " SELECT REMJC.JOB_CARD_CUSTOMS_CLAUSE_FK";
				Sql = Sql + " FROM REM_M_JOB_CARD_MST_TBL REMJC";
				Sql = Sql + " WHERE REMJC.JOB_CARD_PK = " + PK;
				pks = objWF.ExecuteScaler(Sql.ToString());
				if (string.IsNullOrEmpty(pks)) {
					pks = "0";
				}
				strSql = strSql + " SELECT CCMT.CUSTOM_CLAUSE_MST_PK,";
				strSql = strSql + " CCMT.CUSTOM_CLAUSE_DESCRIPTION";
				strSql = strSql + " FROM CUSTOMS_CLAUSE_MST_TBL CCMT,REM_M_JOB_CARD_MST_TBL JCMST";
				strSql = strSql + " WHERE CCMT.CUSTOM_CLAUSE_MST_PK IN (" + pks + ")";
				strSql = strSql + " AND JCMST.JOB_CARD_PK = " + PK;
				return objWF.GetDataSet(strSql);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet fetchShippingInstructionreport(int ViaMode, int PK, long Loc)
		{
			try {
				string strSql = null;
				strSql = strSql + "SELECT DISTINCT ";
				strSql = strSql + "JCMST.JOB_CARD_REF JCPK,";
				strSql = strSql + "JCMST.JOB_CARD_PK  PK,";
				strSql = strSql + "JCMST.JOB_CARD_PFD_ADDR1 CONSIGNADD1,";
				strSql = strSql + "JCMST.JOB_CARD_PFD_ADDR2 CONSIGNADD2,";
				strSql = strSql + "JCMST.JOB_CARD_PFD_CITY  CONSIGNCITY,";
				strSql = strSql + "JCMST.JOB_CARD_PFD_ZIP   CONSIGNZIP,";
				strSql = strSql + "CMTPFD.COUNTRY_ID        CONSIGNCOUNTRY,";
				strSql = strSql + "JCMST.JOB_CARD_PLR_ADDR1 SHIPPERADD1,";
				strSql = strSql + "JCMST.JOB_CARD_PLR_ADDR2 SHIPPERADD2,";
				strSql = strSql + "JCMST.JOB_CARD_PLR_CITY  SHIPPERADDCITY,";
				strSql = strSql + "JCMST.JOB_CARD_PLR_ZIP   SHIPPERZIP,";
				strSql = strSql + "CMTPLR.COUNTRY_ID           SHIPPER_COUNTRY,";
				strSql = strSql + "CMTPLR.COUNTRY_NAME,";
				strSql = strSql + "TO_CHAR(JCTRDE.ATD,DATEFORMAT) DATE_DEPARTURE,";
				strSql = strSql + "PLR.PLACE_CODE PLACE_DEPARTURE,";
				strSql = strSql + "DECODE(JCTRDE.TRANS_MODE, '1', 'Air', '2', 'Sea', '3', 'Land') PRECARRIGE_BY,";
				strSql = strSql + "JCTRDE.POL_FK POLPK,";
				strSql = strSql + "(CASE WHEN JCTRDE.TRANS_MODE = 3 THEN PLR.PLACE_CODE  ELSE POL.PORT_ID END) POL,";
				strSql = strSql + "JCTRDE.POD_FK PODPK,";
				strSql = strSql + "(CASE WHEN JCTRDE.TRANS_MODE = 3 THEN PFD.PLACE_CODE  ELSE POD.PORT_ID END) POD,";
				strSql = strSql + "JCSPINST.JC_TERMS_DELIVERY,";
				strSql = strSql + "SUM(JCTRDE.WEIGHT) WEIGHT,";
				strSql = strSql + "SUM(JCTRDE.VOLUME) VOLUME,";
				strSql = strSql + "JCSPINST.JC_CARRIER_INST,";
				strSql = strSql + "JCSPINST.JC_MVMT_CERT_NR,";
				strSql = strSql + "JCCMR.JC_CMR_DOC_INVOICE,";
				strSql = strSql + "JCCMR.JC_CMR_DOC_INS_COST,";
				strSql = strSql + "JCCMR.JC_CMR_DOC_CUST_INV,";
				strSql = strSql + "JCCMR.JC_CMR_DOC_PACK_LIST,";
				strSql = strSql + "JCCMR.JC_CMR_PL_ORI_COPY,";
				strSql = strSql + "JCCMR.JC_CMR_DOC_CERT_ORIGIN,";
				strSql = strSql + "JCCMR.JC_CMR_CO_ORI_COPY,";
				strSql = strSql + "JCTRDE.BL_DATE BL_DATE,";
				strSql = strSql + "(SELECT LMT.LOCATION_NAME FROM LOCATION_MST_TBL LMT WHERE LMT.LOCATION_MST_PK = " + Loc + ") ISSUEDAT";
				strSql = strSql + " FROM REM_M_JOB_CARD_MST_TBL JCMST,";
				strSql = strSql + " REM_T_JC_TRANSP_DTLS_TBL    JCTRDE,";
				strSql = strSql + " REM_T_JC_CMR_PRINT_TBL      JCCMR,";
				strSql = strSql + " PORT_MST_TBL POL,";
				strSql = strSql + " PORT_MST_TBL POD,";
				strSql = strSql + " REM_T_JC_SPL_INST_TBL      JCSPINST,";
				strSql = strSql + " COUNTRY_MST_TBL            CMTPFD,";
				strSql = strSql + " COUNTRY_MST_TBL            CMTPLR,";
				strSql = strSql + " PLACE_MST_TBL              PLR,";
				strSql = strSql + " PLACE_MST_TBL              PFD";
				// strSql = strSql & vbCrLf & " LOCATION_MST_TBL          LMT"
				// strSql = strSql & vbCrLf & " WHERE JCMST.JOB_CARD_PK=JCITDE.JOB_CARD_FK"
				strSql = strSql + " WHERE JCMST.JOB_CARD_PK=JCTRDE.JOB_CARD_FK";
				// strSql = strSql & vbCrLf & " AND JCMST.JOB_CARD_PK=JCSPINST.JOB_CARD_FK"
				strSql = strSql + " AND JCTRDE.POL_FK=POL.PORT_MST_PK(+)";
				strSql = strSql + " AND JCTRDE.POD_FK=POD.PORT_MST_PK(+)";
				strSql = strSql + " AND JCMST.JOB_CARD_PK=JCCMR.JOB_CARD_FK(+)";
				strSql = strSql + " AND JCMST.JOB_CARD_PFD_COUNTRY_FK=CMTPFD.COUNTRY_MST_PK";
				strSql = strSql + " AND JCMST.JOB_CARD_PLR_COUNTRY_FK=CMTPLR.COUNTRY_MST_PK";
				strSql = strSql + " AND JCTRDE.POL_FK = PLR.PLACE_PK(+)";
				strSql = strSql + " AND JCTRDE.POD_FK = PFD.PLACE_PK(+)";
				strSql = strSql + " AND JCSPINST.JC_TRANSP_DTLS_FK(+)=JCTRDE.JC_TRANSP_DTLS_PK";
				strSql = strSql + " AND JCTRDE.TRANS_MODE = " + ViaMode;
				strSql = strSql + " AND JCMST.JOB_CARD_PK = " + PK;
				// strSql = strSql & vbCrLf & " AND LMT.LOCATION_MST_PK = " & Loc
				strSql = strSql + " GROUP BY JCCMR.JOB_CARD_FK,JCCMR.JC_CMR_SHIPPER_NAME,";
				strSql = strSql + " JCCMR.JC_CMR_SHIPPER_ADD,JCMST.JOB_CARD_REF,";
				strSql = strSql + " JCCMR.JC_CMR_CONSIGNEE_NAME,JCCMR.JC_CMR_CONSIGNEE_ADD,";
				strSql = strSql + " JCMST.JOB_CARD_PFD_FK,JCMST.JOB_CARD_PFD_ADDR1,";
				strSql = strSql + " JCMST.JOB_CARD_PFD_ADDR2,JCMST.JOB_CARD_PFD_CITY,";
				strSql = strSql + " JCMST.JOB_CARD_PFD_ZIP,JCMST.JOB_CARD_PFD_COUNTRY_FK,";
				strSql = strSql + " CMTPLR.COUNTRY_NAME,";
				//PMT.PLACE_CODE,
				strSql = strSql + " JCTRDE.ATD,JCTRDE.TRANS_MODE,";
				strSql = strSql + " JCTRDE.POL_FK,PLR.PLACE_CODE,";
				strSql = strSql + " POL.PORT_ID,JCTRDE.POD_FK,";
				strSql = strSql + " PFD.PLACE_CODE,POD.PORT_ID,";
				strSql = strSql + " JCTRDE.CARRIER_NR,POD.PORT_NAME,";
				strSql = strSql + " JCSPINST.JC_TERMS_DELIVERY,JCSPINST.JC_CARRIER_INST,";
				strSql = strSql + " JCSPINST.JC_MVMT_CERT_NR,JCCMR.JC_CMR_DOC_INVOICE,";
				strSql = strSql + " JCCMR.JC_CMR_DOC_INS_COST,JCCMR.JC_CMR_DOC_CUST_INV,";
				strSql = strSql + " JCCMR.JC_CMR_DOC_PACK_LIST,JCCMR.JC_CMR_PL_ORI_COPY,";
				strSql = strSql + " JCMST.JOB_CARD_PK,JCMST.JOB_CARD_PFD_ADDR1,";
				strSql = strSql + " JCMST.JOB_CARD_PFD_ADDR2,JCMST.JOB_CARD_PFD_CITY,";
				strSql = strSql + " JCMST.JOB_CARD_PFD_ZIP,JCMST.JOB_CARD_PLR_ADDR1,";
				strSql = strSql + " JCMST.JOB_CARD_PLR_ADDR2,JCMST.JOB_CARD_PLR_CITY,";
				strSql = strSql + " JCMST.JOB_CARD_PLR_ZIP,CMTPLR.COUNTRY_ID,CMTPFD.COUNTRY_ID,";
				strSql = strSql + " JCCMR.JC_CMR_DOC_CERT_ORIGIN,JCCMR.JC_CMR_CO_ORI_COPY,";
				strSql = strSql + " JCCMR.JC_CMR_DOC_CERT_ORIGIN,JCCMR.JC_CMR_CO_ORI_COPY,";
				strSql = strSql + " JCCMR.JC_CMR_DOC_CERT_ORIGIN,JCCMR.JC_CMR_CO_ORI_COPY,JCTRDE.BL_DATE";
				WorkFlow objWF = new WorkFlow();
				return objWF.GetDataSet(strSql);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
		//ENDED BY MINAKSHI
		#region "Final Status Report"
		public DataSet fetchFinalStatusReport(int JobPK)
		{
			try {
				string strSql = null;
				WorkFlow objWF = new WorkFlow();
				strSql = strSql + "select jcmst.job_card_pk,";
				strSql = strSql + "jcmst.job_card_ref,";
				strSql = strSql + "cmt.customer_name party_name,";
				strSql = strSql + "decode(jctrde.trans_mode,1,'Air',2,'Sea',3,'Land') Move_Type,";
				strSql = strSql + "jctrde.trans_mode,";
				strSql = strSql + "jctrde.volume,";
				strSql = strSql + "jctrde.weight,";
				strSql = strSql + "to_char(jctrde.eta,DATEFORMAT) eta,";
				strSql = strSql + "to_char(jctrde.cont_clr_dt,DATEFORMAT)cust_clr_dt,";
				strSql = strSql + "to_char(jctrde.del_date,DATEFORMAT) del_dt,";
				strSql = strSql + "jcmst.job_card_pfd_addr1 del_add1,";
				strSql = strSql + "jcmst.job_card_pfd_addr2 del_add2,";
				strSql = strSql + "jcmst.job_card_pfd_city  del_city,";
				strSql = strSql + "jcmst.job_card_pfd_zip del_zip,";
				strSql = strSql + "cdtl.adm_phone_no_1 cont_phone,";
				strSql = strSql + "cdtl.adm_fax_no,  ";
				strSql = strSql + "cdtl.adm_email_id,";
				strSql = strSql + "cty_pfd.country_name,";
				strSql = strSql + " cust_con.adm_contact_person,";
				strSql = strSql + " cust_con.adm_short_name";
				strSql = strSql + "from";
				strSql = strSql + "rem_t_jc_transp_dtls_tbl jctrde,";
				strSql = strSql + "rem_m_job_card_mst_tbl jcmst,";
				strSql = strSql + "customer_mst_tbl     cmt,";
				strSql = strSql + "customer_contact_dtls cdtl,";
				strSql = strSql + "country_mst_tbl      cty_pfd,";
				strSql = strSql + "customer_contact_dtls    cust_con";
				strSql = strSql + "where jcmst.job_card_pk = jctrde.job_card_fk";
				strSql = strSql + "and jcmst.job_card_party_fk = cmt.customer_mst_pk";
				strSql = strSql + "and jcmst.job_card_pfd_country_fk = cty_pfd.country_mst_pk";
				strSql = strSql + "and cdtl.customer_mst_fk = cmt.customer_mst_pk";
				strSql = strSql + "and cust_con.customer_mst_fk = cmt.customer_mst_pk";
				strSql = strSql + "and jcmst.job_card_pk = " + JobPK;
				return objWF.GetDataSet(strSql);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet fetchFinalSubReport(int JobPK)
		{
			try {
				string strSql = null;
				WorkFlow objWF = new WorkFlow();
				strSql = strSql + "select jcmst.job_card_pk,";
				strSql = strSql + "(case when jcitem.via_mode = 3 then";
				strSql = strSql + "3";
				strSql = strSql + "when jcitem.via_mode = 4 then";
				strSql = strSql + "3";
				strSql = strSql + "else";
				strSql = strSql + "jcitem.via_mode";
				strSql = strSql + "end) trnsp_item_mode,";
				strSql = strSql + "jcitem.exp_destination";
				strSql = strSql + "from";
				strSql = strSql + "rem_m_job_card_mst_tbl jcmst,";
				strSql = strSql + "rem_t_jc_item_dtls_tbl   jcitem";
				strSql = strSql + "where jcmst.job_card_pk = jcitem.job_card_fk";
				strSql = strSql + "and jcmst.job_card_pk = " + JobPK;
				return objWF.GetDataSet(strSql);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
		//added by surya prasad for generating Booking Note 
		#region "Booking Note"
		//added by suryaprasad for getting header details for boooking note on 04-mar-2009
		public DataSet fetchBookingNote(int JobPK, Int32 Mode = 0)
		{
			try {
				string strSql = null;
				WorkFlow objWF = new WorkFlow();
				strSql = strSql + " select jc.job_card_ref,";
				strSql = strSql + " jc.job_card_pk,";
				strSql = strSql + " cmt.customer_name,";
				strSql = strSql + " to_date(jc.job_card_date,DATEFORMAT) job_card_date,";
				strSql = strSql + " jtrn.carrier_nr,";
				strSql = strSql + " jspl.jc_carrier_inst,";
				strSql = strSql + " jspl.jc_marks_nos,";
				strSql = strSql + " plr.place_name Place_of_Receipt,";
				strSql = strSql + " pfd.place_name Place_of_Del,";
				strSql = strSql + " pol.port_name POl_Name,";
				strSql = strSql + " pod.port_name POD_Name,";
				strSql = strSql + " to_date(jtrn.eta,DATEFORMAT) eta,";
				strSql = strSql + " jspl.jc_terms_delivery,";
				strSql = strSql + " sum(jtrn.volume) Tot_vol,";
				strSql = strSql + " sum(jtrn.weight) Tot_Wgt,   ";
				strSql = strSql + " (case  when jtrn.trans_mode = 1 then   ";
				strSql = strSql + "  air_mst.airline_name   ";
				strSql = strSql + " else  opr.operator_name   ";
				strSql = strSql + "  end) carrier_name    ";
				strSql = strSql + " from  rem_t_jc_spl_inst_tbl jspl,";
				strSql = strSql + " rem_t_jc_transp_dtls_tbl jtrn,";
				strSql = strSql + " rem_m_job_card_mst_tbl  jc,";
				strSql = strSql + " customer_mst_tbl cmt,";
				strSql = strSql + " place_mst_tbl plr,";
				strSql = strSql + " place_mst_tbl pfd,";
				strSql = strSql + " port_mst_tbl pol,";
				strSql = strSql + " port_mst_tbl pod,";
				strSql = strSql + " operator_mst_tbl  opr,";
				strSql = strSql + " airline_mst_tbl   air_mst";
				strSql = strSql + " where jspl.jc_transp_dtls_fk(+) = jtrn.jc_transp_dtls_pk";
				strSql = strSql + " and jtrn.job_card_fk = jc.job_card_pk";
				strSql = strSql + " and cmt.customer_mst_pk = jc.job_card_party_fk";
				strSql = strSql + " and plr.place_pk = jc.job_card_plr_fk";
				strSql = strSql + " and pfd.place_pk = jc.job_card_pfd_fk";
				strSql = strSql + " and pol.port_mst_pk(+) = jtrn.pol_fk";
				strSql = strSql + " and pod.port_mst_pk(+) = jtrn.pod_fk";
				strSql = strSql + " and opr.operator_id(+) = jtrn.carrier_id";
				strSql = strSql + " and air_mst.airline_id(+) = jtrn.carrier_id";
				strSql = strSql + " and jc.job_card_pk = " + JobPK;
				strSql = strSql + " and jtrn.trans_mode = " + Mode;
				strSql = strSql + " group by  jc.job_card_ref,";
				strSql = strSql + " cmt.customer_name,";
				strSql = strSql + " jc.job_card_date,";
				strSql = strSql + " jtrn.carrier_nr,";
				strSql = strSql + " jspl.jc_carrier_inst,";
				strSql = strSql + " jspl.jc_marks_nos,";
				strSql = strSql + " plr.place_name,";
				strSql = strSql + " pfd.place_name,";
				strSql = strSql + " pol.port_name,";
				strSql = strSql + " pod.port_name,";
				strSql = strSql + " jtrn.eta,";
				strSql = strSql + " jc.job_card_pk,";
				strSql = strSql + " jspl.jc_terms_delivery,jspl.jc_marks_nos,";
				strSql = strSql + " air_mst.airline_name,  opr.operator_name,   ";
				strSql = strSql + " jtrn.trans_mode   ";
				return objWF.GetDataSet(strSql);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		//added by suryaprasad for getting item details for boooking note on 04-mar-2009
		public DataSet fetchBookingNote_ItemDetails(int JobPK, Int32 Mode = 0)
		{
			try {
				string strSql = null;
				WorkFlow objWF = new WorkFlow();
				strSql = strSql + " select jitem.job_card_fk,";
				strSql = strSql + " jitem.item_desc,";
				strSql = strSql + " jitem.quantity,";
				strSql = strSql + " jitem.cont_uld_nr,";
				strSql = strSql + " jitem.volume,";
				strSql = strSql + " jitem.weight,PTY.PACK_TYPE_ID ";
				strSql = strSql + " from";
				strSql = strSql + " rem_m_job_card_mst_tbl  jc,";
				strSql = strSql + " rem_t_jc_item_dtls_tbl jitem,";
				strSql = strSql + " pack_type_mst_tbl pty";
				strSql = strSql + " where jitem.job_card_fk = jc.job_card_pk";
				strSql = strSql + " and jitem.pack_type_fk = pty.pack_type_mst_pk";
				strSql = strSql + " and jc.job_card_pk = " + JobPK;
				strSql = strSql + " and jitem.via_mode = " + Mode;
				return objWF.GetDataSet(strSql);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		//added by suryaprasad for getting custom clause details for boooking note on 04-mar-2009
		public DataSet fetchBookingNote_CustomeDetails(int JobPK)
		{
			try {
				string strSql = null;
				string strSql2 = null;
				WorkFlow objWF = new WorkFlow();
				string strPks = null;
				strSql2 = strSql2 + " (select nvl(jcmst.job_card_customs_clause_fk,0)";
				strSql2 = strSql2 + " from rem_m_job_card_mst_tbl jcmst";
				strSql2 = strSql2 + " where jcmst.job_card_pk =" + JobPK + ")";
				strPks = objWF.ExecuteScaler(strSql2);

				strSql = strSql + " Select  cust.custom_clause_description";
				strSql = strSql + " from customs_clause_mst_tbl cust";
				strSql = strSql + " where cust.custom_clause_mst_pk in (" + strPks + ")";
				return objWF.GetDataSet(strSql);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		//added by suryaprasad for getting freight type details for boooking note on 04-mar-2009
		public DataTable fetchBookingNote_FreightType(int JobPK)
		{
			try {
				string strSql = null;
				string strSql2 = null;
				WorkFlow objWF = new WorkFlow();
				string strPks = null;
				strSql = strSql + " select distinct decode(jfrt.freight_type,1,'Prepaid',2,'Collect') Freight_type ";
				strSql = strSql + " from  rem_m_job_card_mst_tbl jmst, rem_t_jc_frt_dtls_tbl jfrt";
				strSql = strSql + " where jmst.job_card_pk = jfrt.job_card_fk";
				strSql = strSql + " and jfrt.job_card_fk = " + JobPK;
				return objWF.GetDataSet(strSql).Tables[0];
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
		//END
	}
}
