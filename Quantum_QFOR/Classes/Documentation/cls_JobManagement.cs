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
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_JobManagement : CommonFeatures
	{

        /// <summary>
        /// The objwf
        /// </summary>
        private WorkFlow objwf = new WorkFlow();
        /// <summary>
        /// Gets the jm f_ status.
        /// </summary>
        /// <param name="JOBCARD_FK">The jobcar d_ fk.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <returns></returns>
        public string GetJMF_STATUS(int JOBCARD_FK, short BIZ_TYPE, short PROCESS_TYPE)
		{
			//RETURN THE LASTEST ACTIVITY DONE FOR THIS JOBCARD
			string JMF_STATUS = "";
			try {
				objwf.OpenConnection();
				var _with1 = objwf.MyCommand;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objwf.MyUserName + ".JOB_MANAGEMENT_FORM_PKG.FETCH_JMF_STATUS";
				_with1.Parameters.Clear();
				_with1.Parameters.Add("JOBCARD_FK_IN", JOBCARD_FK).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("JMF_STATUS", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Output;
				_with1.ExecuteNonQuery();
				JMF_STATUS = Convert.ToString(_with1.Parameters["JMF_STATUS"].Value);
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
			return JMF_STATUS;
		}

        /// <summary>
        /// Gets the jc activity links.
        /// </summary>
        /// <param name="JOBCARD_FK">The jobcar d_ fk.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <param name="ReturnAsTbl">if set to <c>true</c> [return as table].</param>
        /// <returns></returns>
        public object GetJCActivityLinks(int JOBCARD_FK, short BIZ_TYPE, short PROCESS_TYPE, bool ReturnAsTbl = false)
		{
			try {
				objwf.OpenConnection();
				var _with2 = objwf.MyCommand.Parameters;
				_with2.Clear();
				_with2.Add("JOB_CARD_FK_IN", JOBCARD_FK).Direction = ParameterDirection.Input;
				_with2.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
				_with2.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
				_with2.Add("JMF_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				if (ReturnAsTbl) {
					return objwf.GetDataTable("JOB_MANAGEMENT_FORM_PKG", "GET_ACTIVITYWISE_LINK");
				} else {
					return objwf.GetDataSet("JOB_MANAGEMENT_FORM_PKG", "GET_ACTIVITYWISE_LINK");
				}
			} catch (Exception sqlExp) {
				throw sqlExp;
			} finally {
				objwf.CloseConnection();
			}
		}

        /// <summary>
        /// Gets the ur l_ fo r_ activity.
        /// </summary>
        /// <param name="DOCUMENT_FK">The documen t_ fk.</param>
        /// <param name="FLAG">The flag.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <param name="JOB_CARD_FK">The jo b_ car d_ fk.</param>
        /// <returns></returns>
        public string GetURL_FOR_ACTIVITY(int DOCUMENT_FK, string FLAG, short BIZ_TYPE, short PROCESS_TYPE, int JOB_CARD_FK = 0)
		{
			try {
				objwf.OpenConnection();
				var _with3 = objwf.MyCommand.Parameters;
				_with3.Clear();
				_with3.Add("DOCUMENT_FK_IN", DOCUMENT_FK).Direction = ParameterDirection.Input;
				_with3.Add("JOB_CARD_FK_IN", JOB_CARD_FK).Direction = ParameterDirection.Input;
				_with3.Add("FLAG_IN", FLAG).Direction = ParameterDirection.Input;
				_with3.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
				_with3.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
				_with3.Add("JMF_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return Convert.ToString(objwf.GetDataTable("JOB_MANAGEMENT_FORM_PKG", "FETCH_URL").Rows[0][0]);
			} catch (Exception sqlExp) {
				throw sqlExp;
			} finally {
				objwf.CloseConnection();
			}
			return "";
		}
        /// <summary>
        /// Gets the consol inv details.
        /// </summary>
        /// <param name="INVOICE_PK">The invoic e_ pk.</param>
        /// <param name="INVOICE_REF">The invoic e_ reference.</param>
        /// <returns></returns>
        public DataTable GetConsolInvDetails(int INVOICE_PK = 0, string INVOICE_REF = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT INV.CONSOL_INVOICE_PK,");
			sb.Append("       INV.PROCESS_TYPE,");
			sb.Append("       INV.BUSINESS_TYPE,");
			sb.Append("       INV.CUSTOMER_MST_FK,");
			sb.Append("       INV.INVOICE_REF_NO,");
			sb.Append("       INV.INVOICE_DATE,");
			sb.Append("       INV.CURRENCY_MST_FK,");
			sb.Append("       INV.INVOICE_AMT,");
			sb.Append("       INV.DISCOUNT_AMT,");
			sb.Append("       INV.NET_RECEIVABLE,");
			sb.Append("       INV.REMARKS,");
			sb.Append("       INV.CREATED_BY_FK,");
			sb.Append("       INV.CREATED_DT,");
			sb.Append("       INV.LAST_MODIFIED_BY_FK,");
			sb.Append("       INV.LAST_MODIFIED_DT,");
			sb.Append("       INV.VERSION_NO,");
			sb.Append("       INV.TOTAL_CREDIT_NOTE_AMT,");
			sb.Append("       INV.EXCH_RATE_TYPE_FK,");
			sb.Append("       INV.INV_UNIQUE_REF_NR,");
			sb.Append("       INV.BATCH_MST_FK,");
			sb.Append("       INV.INV_TYPE,");
			sb.Append("       INV.CHK_INVOICE,");
			sb.Append("       INV.INVOICE_DUE_DATE,");
			sb.Append("       INV.EDI_STATUS");
			sb.Append("  FROM CONSOL_INVOICE_TBL INV ");
			sb.Append("  WHERE 1=1 ");
			if (!string.IsNullOrEmpty(INVOICE_REF)) {
				sb.Append("    AND INV.INVOICE_REF_NO='" + INVOICE_REF + "'");
			}
			if (INVOICE_PK > 0) {
				sb.Append("    AND INV.CONSOL_INVOICE_PK=" + INVOICE_PK);
			}
			objwf.MyCommand.Parameters.Clear();
			return objwf.GetDataTable(sb.ToString());
		}
        /// <summary>
        /// Gets the job card det.
        /// </summary>
        /// <param name="DOCUMENT_FK">The documen t_ fk.</param>
        /// <param name="FROM_FLAG">The fro m_ flag.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <param name="ReturnAsTbl">if set to <c>true</c> [return as table].</param>
        /// <returns></returns>
        public object GetJobCardDet(int DOCUMENT_FK, string FROM_FLAG, short BIZ_TYPE, short PROCESS_TYPE, bool ReturnAsTbl = false)
		{
			DataSet dsJCDetails = new DataSet();
			try {
				objwf.OpenConnection();
				var _with4 = objwf.MyCommand.Parameters;
				_with4.Add("DOCUMENT_FK_IN", DOCUMENT_FK).Direction = ParameterDirection.Input;
				_with4.Add("FROM_FLAG_IN", FROM_FLAG.ToUpper()).Direction = ParameterDirection.Input;
				_with4.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
				_with4.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
				_with4.Add("JMF_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				if (ReturnAsTbl) {
					return objwf.GetDataTable("JOB_MANAGEMENT_FORM_PKG", "GET_JOBCARD_DET");
				} else {
					return objwf.GetDataSet("JOB_MANAGEMENT_FORM_PKG", "GET_JOBCARD_DET");
				}
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
			return dsJCDetails;
		}
        /// <summary>
        /// Gets the job card det.
        /// </summary>
        /// <param name="JOBCARD_PKS">The jobcar d_ PKS.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <param name="ReturnAsTbl">if set to <c>true</c> [return as table].</param>
        /// <returns></returns>
        public object GetJobCardDet(string JOBCARD_PKS, short BIZ_TYPE, short PROCESS_TYPE, bool ReturnAsTbl = false)
		{
			DataSet dsJCDetails = new DataSet();
			try {
				objwf.OpenConnection();
				var _with5 = objwf.MyCommand.Parameters;
				_with5.Add("JOBCARD_PKS", JOBCARD_PKS).Direction = ParameterDirection.Input;
				_with5.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
				_with5.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
				_with5.Add("JMF_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				if (ReturnAsTbl) {
					return objwf.GetDataTable("JOB_MANAGEMENT_FORM_PKG", "JOBCARD_DET_BY_JOB_PKS");
				} else {
					return objwf.GetDataSet("JOB_MANAGEMENT_FORM_PKG", "JOBCARD_DET_BY_JOB_PKS");
				}
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
			return dsJCDetails;
		}
        /// <summary>
        /// Fetches the JMF header details.
        /// </summary>
        /// <param name="JOBCARD_FK">The jobcar d_ fk.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <param name="ReturnAsTbl">if set to <c>true</c> [return as table].</param>
        /// <returns></returns>
        public object FetchJMFHeaderDetails(int JOBCARD_FK, short BIZ_TYPE, short PROCESS_TYPE, bool ReturnAsTbl = false)
		{
			DataSet dsJMFActivity = new DataSet();
			DataTable dtJMFActivity = new DataTable();
			try {
				objwf.MyCommand.Parameters.Clear();
				var _with6 = objwf.MyCommand.Parameters;
				_with6.Add("JOBCARD_FK_IN", JOBCARD_FK).Direction = ParameterDirection.Input;
				_with6.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
				_with6.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
				_with6.Add("JMF_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dsJMFActivity = objwf.GetDataSet("JOB_MANAGEMENT_FORM_PKG", "FETCH_JMF_HDR_DET");

				if (dsJMFActivity.Tables[0].Rows.Count > 0 & BIZ_TYPE == 1) {
					string Comm_Pks = Convert.ToString(getDefault(dsJMFActivity.Tables[0].Rows[0]["COMMODITY"], ""));
					DataSet dsComDtls = new DataSet();
					if (!string.IsNullOrEmpty(Comm_Pks.Trim())) {
						dsComDtls = FethCommodityDetails(Comm_Pks);
						Comm_Pks = "";
						foreach (DataRow row in dsComDtls.Tables[0].Rows) {
							if (string.IsNullOrEmpty(Comm_Pks)) {
								Comm_Pks = Convert.ToString(row["COMMODITY_NAME"]);
							} else {
								Comm_Pks += ";" + row["COMMODITY_NAME"];
							}
						}
						dsJMFActivity.Tables[0].Rows[0]["COMMODITY"] = Comm_Pks;
					}
				}
				if (ReturnAsTbl) {
					return dsJMFActivity.Tables[0];
				} else {
					return dsJMFActivity;
				}
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
			return dsJMFActivity;
		}
        /// <summary>
        /// Fethes the commodity details.
        /// </summary>
        /// <param name="COMM_PKS">The COM m_ PKS.</param>
        /// <returns></returns>
        public DataSet FethCommodityDetails(string COMM_PKS = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			sb.Append("SELECT COMM.COMMODITY_MST_PK,");
			sb.Append("       COMM.COMMODITY_GROUP_FK,");
			sb.Append("       COMM.COMMODITY_ID,");
			sb.Append("       COMM.COMMODITY_NAME,");
			sb.Append("       COMM.HAZARDOUS");
			sb.Append("  FROM COMMODITY_MST_TBL COMM");
			sb.Append(" WHERE 1=1 ");
			if (!string.IsNullOrEmpty(COMM_PKS.Trim())) {
				sb.Append(" AND COMM.COMMODITY_MST_PK IN (" + COMM_PKS + ")");
			}
			try {
				objwf.MyCommand.Parameters.Clear();
				return objwf.GetDataSet(sb.ToString());

			} catch (Exception ex) {
			}
            return new DataSet();
		}
        /// <summary>
        /// Fetches the JMF activities.
        /// </summary>
        /// <param name="JOBCARD_FK">The jobcar d_ fk.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <param name="ReturnAsTbl">if set to <c>true</c> [return as table].</param>
        /// <returns></returns>
        public object FetchJMFActivities(int JOBCARD_FK, short BIZ_TYPE, short PROCESS_TYPE, bool ReturnAsTbl = false)
		{
			DataSet dsJMFActivity = new DataSet();
			try {
				objwf.MyCommand.Parameters.Clear();
				var _with7 = objwf.MyCommand.Parameters;
				_with7.Add("JOBCARD_FK_IN", JOBCARD_FK).Direction = ParameterDirection.Input;
				_with7.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
				_with7.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
				_with7.Add("JMF_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				if (ReturnAsTbl) {
					return objwf.GetDataTable("JOB_MANAGEMENT_FORM_PKG", "FETCH_JMF_ACTIVITIES");
				} else {
					return objwf.GetDataSet("JOB_MANAGEMENT_FORM_PKG", "FETCH_JMF_ACTIVITIES");
				}
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
			return dsJMFActivity;
		}

        /// <summary>
        /// Fetches the JMF remarks.
        /// </summary>
        /// <param name="JOBCARD_FK">The jobcar d_ fk.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <param name="ReturnAsTbl">if set to <c>true</c> [return as table].</param>
        /// <returns></returns>
        public object FetchJMFRemarks(int JOBCARD_FK, short BIZ_TYPE, short PROCESS_TYPE, bool ReturnAsTbl = false)
		{
			DataSet dsJMFActivity = new DataSet();
			try {
				objwf.MyCommand.Parameters.Clear();
				var _with8 = objwf.MyCommand.Parameters;
				_with8.Add("JOBCARD_FK_IN", JOBCARD_FK).Direction = ParameterDirection.Input;
				_with8.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
				_with8.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
				_with8.Add("JMF_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				if (ReturnAsTbl) {
					return objwf.GetDataTable("JOB_MANAGEMENT_FORM_PKG", "FETCH_JMF_REMARKS_DTL");
				} else {
					return objwf.GetDataSet("JOB_MANAGEMENT_FORM_PKG", "FETCH_JMF_REMARKS_DTL");
				}
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
			return dsJMFActivity;
		}

        /// <summary>
        /// Fetches the JMF remarks.
        /// </summary>
        /// <param name="REMARK_FKS">The remar k_ FKS.</param>
        /// <param name="ReturnAsTbl">if set to <c>true</c> [return as table].</param>
        /// <returns></returns>
        public object FetchJMFRemarks(string REMARK_FKS, bool ReturnAsTbl = false)
		{
			try {
				objwf.MyCommand.Parameters.Clear();
				var _with9 = objwf.MyCommand.Parameters;
				_with9.Add("REMARKS_PK_IN", REMARK_FKS).Direction = ParameterDirection.Input;
				_with9.Add("JMF_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				if (ReturnAsTbl) {
					return objwf.GetDataTable("JOB_MANAGEMENT_FORM_PKG", "FETCH_JMF_REMARKS");
				} else {
					return objwf.GetDataSet("JOB_MANAGEMENT_FORM_PKG", "FETCH_JMF_REMARKS");
				}
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
		}

        /// <summary>
        /// Fetches the JMF cost details.
        /// </summary>
        /// <param name="JOBCARD_FK">The jobcar d_ fk.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <param name="ReturnAsTbl">if set to <c>true</c> [return as table].</param>
        /// <returns></returns>
        public object FetchJMFCostDetails(int JOBCARD_FK, short BIZ_TYPE, short PROCESS_TYPE, bool ReturnAsTbl = false)
		{
			DataSet dsJMFCost = new DataSet();
			try {
				objwf.MyCommand.Parameters.Clear();
				var _with10 = objwf.MyCommand.Parameters;
				_with10.Add("JOBCARD_FK_IN", JOBCARD_FK).Direction = ParameterDirection.Input;
				_with10.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
				_with10.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
				_with10.Add("BASE_CURRENCY_FK", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
				_with10.Add("JMF_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				if (ReturnAsTbl) {
					return objwf.GetDataTable("JOB_MANAGEMENT_FORM_PKG", "FETCH_JOBCARD_AMOUNT_DET");
				} else {
					return objwf.GetDataSet("JOB_MANAGEMENT_FORM_PKG", "FETCH_JOBCARD_AMOUNT_DET");
				}
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
			return dsJMFCost;
		}

        /// <summary>
        /// Fetches the JMF all charge details.
        /// </summary>
        /// <param name="JOBCARD_FK">The jobcar d_ fk.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <param name="ReturnAsTbl">if set to <c>true</c> [return as table].</param>
        /// <returns></returns>
        public object FetchJMFAllChargeDetails(int JOBCARD_FK, short BIZ_TYPE, short PROCESS_TYPE, bool ReturnAsTbl = false)
		{
			//RETURNS
			//TOTAL INVOICED
			//TOTAL RECEIVED
			//BALANCE TO BE RECEIVED
			//TOTAL VOUCHER
			//TOTAL PAID
			//BALANCE TO BE PAID
			//TOTAL_PREPAID
			//TOTAL_COLLECT
			DataSet dsJMFCost = new DataSet();
			try {
				objwf.MyCommand.Parameters.Clear();
				var _with11 = objwf.MyCommand.Parameters;
				_with11.Add("JOBCARD_FK_IN", JOBCARD_FK).Direction = ParameterDirection.Input;
				_with11.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
				_with11.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
				_with11.Add("BASE_CURRENCY_FK", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
				_with11.Add("JMF_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				if (ReturnAsTbl) {
					return objwf.GetDataTable("JOB_MANAGEMENT_FORM_PKG", "TOTAL_INV_REC_JC_AMT_DET");
				} else {
					return objwf.GetDataSet("JOB_MANAGEMENT_FORM_PKG", "TOTAL_INV_REC_JC_AMT_DET");
				}
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
			return dsJMFCost;
		}

        /// <summary>
        /// Saves the JMF remarks.
        /// </summary>
        /// <param name="dsRemarks">The ds remarks.</param>
        /// <param name="JOB_CARD_FK">The jo b_ car d_ fk.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <returns></returns>
        public object SaveJMFRemarks(DataSet dsRemarks, int JOB_CARD_FK, short BIZ_TYPE, short PROCESS_TYPE)
		{
			objwf.OpenConnection();
			OracleTransaction Tran = null;
			Tran = objwf.MyConnection.BeginTransaction();
			objwf.MyCommand.Transaction = Tran;
			try {
				foreach (DataRow remark in dsRemarks.Tables[0].Rows) {
					var _with12 = objwf.MyCommand;
					_with12.CommandType = CommandType.StoredProcedure;
					_with12.CommandText = objwf.MyUserName + ".JMF_REMARKS_TBL_PKG.JMF_REMARKS_TBL_INS";

					_with12.Parameters.Clear();
					_with12.Parameters.Add("JOB_CARD_FK_IN", JOB_CARD_FK).Direction = ParameterDirection.Input;
					_with12.Parameters.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
					_with12.Parameters.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
                    _with12.Parameters.Add("REM_DATE_IN", (Convert.ToString(remark["REM_DATE"]) != null ? DateTime.Now.Date : remark["REM_DATE"])).Direction = ParameterDirection.Input;
					_with12.Parameters.Add("REM_TIME_IN", remark["REM_TIME"]).Direction = ParameterDirection.Input;
					_with12.Parameters.Add("REMARKS_IN", remark["REMARKS"]).Direction = ParameterDirection.Input;
					_with12.Parameters.Add("CREATED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
					_with12.Parameters.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
					_with12.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
					_with12.ExecuteNonQuery();
					remark["JMF_REMARKS_PK"] = _with12.Parameters["RETURN_VALUE"].Value;
				}
				Tran.Commit();
			} catch (OracleException oraexp) {
				Tran.Rollback();
				throw oraexp;
			} catch (Exception ex) {
				Tran.Rollback();
				throw ex;
			} finally {
				objwf.CloseConnection();
			}
            return new object();
		}

        /// <summary>
        /// Gets the type of the agent.
        /// </summary>
        /// <param name="AgentPK">The agent pk.</param>
        /// <returns></returns>
        public Int16 GetAgentType(int AgentPK)
		{
			if (AgentPK > 0) {
				return Convert.ToInt16(objwf.ExecuteScaler("select agent_type from agent_mst_tbl amt where amt.agent_mst_pk=" + AgentPK));
			} else {
				return 0;
			}
		}

        /// <summary>
        /// Saves the JMF activity ext.
        /// </summary>
        /// <param name="dsActivities">The ds activities.</param>
        /// <param name="JOB_CARD_FK">The jo b_ car d_ fk.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <param name="IsWINSave">The is win save.</param>
        /// <returns></returns>
        public object SaveJMFActivityExt(DataSet dsActivities, int JOB_CARD_FK, short BIZ_TYPE, short PROCESS_TYPE, int IsWINSave = 0)
		{
			objwf.OpenConnection();
			OracleTransaction Tran = null;
			Tran = objwf.MyConnection.BeginTransaction();
			DataSet SelectedUsers = null;
			DataSet UserRemarks = null;
			SelectedUsers = (DataSet)HttpContext.Current.Session["SessionSelUser"];
			UserRemarks = (DataSet)HttpContext.Current.Session["SessionUserRemarks"];
			int TCnt = 0;

			try {
				foreach (DataRow row in dsActivities.Tables[0].Rows) {
					TCnt = Convert.ToInt32(row["SLNR"]) - 1;
					if (!string.IsNullOrEmpty(row["REFERENCE_NR"].ToString())) {
						if (string.Compare(Convert.ToString(row["REFERENCE_NR"]), ">") > 0) {
                            string refNumber = row["REFERENCE_NR"].ToString();
                            string[] refNumber1 = refNumber.Split('>');
                            string[] refNumber2 = refNumber.Split('<');

                            row["REFERENCE_NR"] = refNumber1[1];
							row["REFERENCE_NR"] = refNumber2[0];
                        }
					}
					if (Convert.ToInt32(row["INTERNAL"]) == 1) {
						if (string.IsNullOrEmpty(row["REMINDER_PK"].ToString()) | row["REMINDER_PK"] == "0") {
							var _with13 = objwf.MyCommand;
							_with13.CommandType = CommandType.StoredProcedure;
							_with13.CommandText = objwf.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS_EXT";
							_with13.Transaction = Tran;
							_with13.Parameters.Clear();

							_with13.Parameters.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
							_with13.Parameters.Add("PROCESS_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
							_with13.Parameters.Add("KEY_FK_IN", JOB_CARD_FK).Direction = ParameterDirection.Input;
							_with13.Parameters.Add("LOCATION_FK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
							_with13.Parameters.Add("STATUS_IN", row["ACTIVITY_TYPE"]).Direction = ParameterDirection.Input;
							_with13.Parameters.Add("CREATEDUSER_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
							_with13.Parameters.Add("DOC_REF_IN", row["REFERENCE_NR"]).Direction = ParameterDirection.Input;
							_with13.Parameters.Add("SET_FLAG_IN", row["SET_FLAG"]).Direction = ParameterDirection.Input;
							if ((row["ACTIVITY_DESC"] == null)) {
								_with13.Parameters.Add("ACTIVITY_DESC_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with13.Parameters.Add("ACTIVITY_DESC_IN", row["ACTIVITY_DESC"]).Direction = ParameterDirection.Input;
							}
							if ((row["ACTIVITY_DATE"] == null) | !string.IsNullOrEmpty(row["ACTIVITY_DATE"].ToString())) {
								_with13.Parameters.Add("CREATED_DT_IN", "").Direction = ParameterDirection.Input;
							} else {
                                DateTime datetime = (DateTime)row["ACTIVITY_DATE"];
								_with13.Parameters.Add("CREATED_DT_IN", (DateTime)row["ACTIVITY_DATE"]).Direction = ParameterDirection.Input;
							}
							if ((row["REMINDER_ON"] == null) | string.IsNullOrEmpty(row["REMINDER_ON"].ToString())) {
								_with13.Parameters.Add("REMINDER_ON_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with13.Parameters.Add("REMINDER_ON_IN",  (DateTime)row["REMINDER_ON"]).Direction = ParameterDirection.Input;
							}
							_with13.Parameters.Add("USER_PKS_IN", row["USER_PKS"]).Direction = ParameterDirection.Input;
							_with13.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
							_with13.ExecuteNonQuery();
						} else {
							var _with14 = objwf.MyCommand;
							_with14.CommandType = CommandType.StoredProcedure;
							_with14.CommandText = objwf.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_UPD_EXT";
							_with14.Transaction = Tran;
							_with14.Parameters.Clear();
							_with14.Parameters.Add("REMINDER_PK_IN", row["REMINDER_PK"]).Direction = ParameterDirection.Input;
							_with14.Parameters.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
							_with14.Parameters.Add("PROCESS_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
							_with14.Parameters.Add("KEY_FK_IN", JOB_CARD_FK).Direction = ParameterDirection.Input;
							_with14.Parameters.Add("LOCATION_FK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
							_with14.Parameters.Add("STATUS_IN", row["ACTIVITY_TYPE"]).Direction = ParameterDirection.Input;
							_with14.Parameters.Add("CREATEDUSER_IN", CREATED_BY).Direction = ParameterDirection.Input;
							_with14.Parameters.Add("DOC_REF_IN", row["REFERENCE_NR"]).Direction = ParameterDirection.Input;
							_with14.Parameters.Add("SET_FLAG_IN", row["SET_FLAG"]).Direction = ParameterDirection.Input;
							if ((row["ACTIVITY_DESC"] == null)) {
								_with14.Parameters.Add("ACTIVITY_DESC_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with14.Parameters.Add("ACTIVITY_DESC_IN", row["ACTIVITY_DESC"]).Direction = ParameterDirection.Input;
							}
							if ((row["ACTIVITY_DATE"] == null) | string.IsNullOrEmpty(row["ACTIVITY_DATE"].ToString())) {
								_with14.Parameters.Add("CREATED_DT_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with14.Parameters.Add("CREATED_DT_IN", (DateTime)row["ACTIVITY_DATE"]).Direction = ParameterDirection.Input;
							}
							if ((row["REMINDER_ON"] == null) | string.IsNullOrEmpty(row["REMINDER_ON"].ToString())) {
								_with14.Parameters.Add("REMINDER_ON_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with14.Parameters.Add("REMINDER_ON_IN",  (DateTime)row["REMINDER_ON"]).Direction = ParameterDirection.Input;
							}
							_with14.Parameters.Add("USER_PKS_IN", row["USER_PKS"]).Direction = ParameterDirection.Input;
							_with14.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
							_with14.ExecuteNonQuery();
						}
					} else {
						if (string.IsNullOrEmpty(row["REMINDER_PK"].ToString())) {
							var _with15 = objwf.MyCommand;
							_with15.CommandType = CommandType.StoredProcedure;
							_with15.CommandText = objwf.MyUserName + ".TRACK_N_TRACE_TBL_EXT_PKG.TRACK_N_TRACE_TBL_EXT_INS";
							_with15.Transaction = Tran;
							_with15.Parameters.Clear();
							_with15.Parameters.Add("JOB_CARD_FK_IN", JOB_CARD_FK).Direction = ParameterDirection.Input;
							_with15.Parameters.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
							_with15.Parameters.Add("PROCESS_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
							_with15.Parameters.Add("DOC_REF_NO_IN", row["REFERENCE_NR"]).Direction = ParameterDirection.Input;
							_with15.Parameters.Add("CREATED_ON_IN", "").Direction = ParameterDirection.Input;
							_with15.Parameters.Add("LOCATION_FK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
							_with15.Parameters.Add("STATUS_IN", row["ACTIVITY_TYPE"]).Direction = ParameterDirection.Input;
							_with15.Parameters.Add("ACTIVITY_IN", row["ACTIVITY"]).Direction = ParameterDirection.Input;
							_with15.Parameters.Add("DOCUMENT_URL_FK_IN", "").Direction = ParameterDirection.Input;
							_with15.Parameters.Add("WIN_STATUS_IN", row["FLAG"]).Direction = ParameterDirection.Input;
							_with15.Parameters.Add("SET_FLAG_IN", row["SET_FLAG"]).Direction = ParameterDirection.Input;
							_with15.Parameters.Add("REMARKS_IN", row["REMARKS"]).Direction = ParameterDirection.Input;
							if (IsWINSave == 1) {
								_with15.Parameters.Add("CREATED_BY_IN", (string.IsNullOrEmpty(row["UPDATED_BY"].ToString()) ? CREATED_BY : row["UPDATED_BY"])).Direction = ParameterDirection.Input;
							} else {
								_with15.Parameters.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
							}
							if ((row["ACTIVITY_DESC"] == null)) {
								_with15.Parameters.Add("ACTIVITY_DESC_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with15.Parameters.Add("ACTIVITY_DESC_IN", row["ACTIVITY_DESC"]).Direction = ParameterDirection.Input;
							}
							if ((row["REMINDER_ON"] == null) | string.IsNullOrEmpty(row["REMINDER_ON"].ToString())) {
								_with15.Parameters.Add("REMINDER_ON_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with15.Parameters.Add("REMINDER_ON_IN", (DateTime)row["REMINDER_ON"]).Direction = ParameterDirection.Input;
							}
							_with15.Parameters.Add("USER_PKS_IN", row["USER_PKS"]).Direction = ParameterDirection.Input;
							_with15.Parameters.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
							_with15.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
							_with15.ExecuteNonQuery();
						} else {
							var _with16 = objwf.MyCommand;
							_with16.CommandType = CommandType.StoredProcedure;
							_with16.CommandText = objwf.MyUserName + ".TRACK_N_TRACE_TBL_EXT_PKG.TRACK_N_TRACE_TBL_EXT_UPD";
							_with16.Transaction = Tran;
							_with16.Parameters.Clear();
							_with16.Parameters.Add("REMINDER_PK_IN", row["REMINDER_PK"]).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("TRACK_TRACE_EXT_PK_IN", row["PK"]).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("JOB_CARD_FK_IN", JOB_CARD_FK).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("BIZ_TYPE_IN", BIZ_TYPE).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("PROCESS_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("DOC_REF_NO_IN", row["REFERENCE_NR"]).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("CREATED_ON_IN", "").Direction = ParameterDirection.Input;
							_with16.Parameters.Add("LOCATION_FK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("STATUS_IN", row["ACTIVITY_TYPE"]).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("ACTIVITY_IN", row["ACTIVITY"]).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("DOCUMENT_URL_FK_IN", "").Direction = ParameterDirection.Input;
							_with16.Parameters.Add("WIN_STATUS_IN", row["FLAG"]).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("SET_FLAG_IN", row["SET_FLAG"]).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("REMARKS_IN", row["REMARKS"]).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("CREATED_BY_IN", (CREATED_BY == 0 ? HttpContext.Current.Session["USER_PK"] : CREATED_BY)).Direction = ParameterDirection.Input;
							if ((row["ACTIVITY_DESC"] == null)) {
								_with16.Parameters.Add("ACTIVITY_DESC_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with16.Parameters.Add("ACTIVITY_DESC_IN", row["ACTIVITY_DESC"]).Direction = ParameterDirection.Input;
							}
							if ((row["REMINDER_ON"] == null) | string.IsNullOrEmpty(row["REMINDER_ON"].ToString())) {
								_with16.Parameters.Add("REMINDER_ON_IN", "").Direction = ParameterDirection.Input;
							} else {
								_with16.Parameters.Add("REMINDER_ON_IN", (DateTime)row["REMINDER_ON"]).Direction = ParameterDirection.Input;
							}
							_with16.Parameters.Add("USER_PKS_IN", row["USER_PKS"]).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
							_with16.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100).Direction = ParameterDirection.Output;
							_with16.ExecuteNonQuery();
						}
					}
					long Reminder_Pk = 0;
					Reminder_Pk = Convert.ToInt64(objwf.MyCommand.Parameters["RETURN_VALUE"].Value);
					if (TCnt < SelectedUsers.Tables.Count) {

						if ((SelectedUsers != null)) {
							if (SelectedUsers.Tables[TCnt].Rows.Count > 0) {
								SaveSelectedUsers(objwf.MyCommand, objwf.MyUserName, Tran, SelectedUsers, TCnt, JOB_CARD_FK, Reminder_Pk);
							}
						}

						if ((UserRemarks != null)) {
							if (UserRemarks.Tables[TCnt].Rows.Count > 0) {
								SaveUserStatus(objwf.MyCommand, objwf.MyUserName, Tran, UserRemarks, TCnt, JOB_CARD_FK, Reminder_Pk);
							}
						}
					}
				}
				Tran.Commit();
			} catch (OracleException oraexp) {
				Tran.Rollback();
				throw oraexp;
			} catch (Exception ex) {
				Tran.Rollback();
				throw ex;
			} finally {
				objwf.CloseConnection();
			}
            return new object();
		}
        /// <summary>
        /// Saves the selected users.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="dsSelUsers">The ds sel users.</param>
        /// <param name="TCnt">The t count.</param>
        /// <param name="JOB_CARD_FK">The jo b_ car d_ fk.</param>
        /// <param name="Reminder_Pk">The reminder_ pk.</param>
        /// <returns></returns>
        public ArrayList SaveSelectedUsers(OracleCommand SCM, string UserName, OracleTransaction TRAN, DataSet dsSelUsers, int TCnt = 0, long JOB_CARD_FK = 0, long Reminder_Pk = 0)
		{
			int Rcnt = 0;
			DataRow Odr = null;
			string JMFReminderPKs = "0";
			OracleCommand delCommand = new OracleCommand();
			string strQry = null;
			objwf.OpenConnection();
			objwf.MyConnection = TRAN.Connection;

			try {

				//strQry = "DELETE FROM JMF_REMISNDER_UERS JR "
				//strQry &= " WHERE JR.REMINDER_FK IN (" & Reminder_Pk & ")"
				//With delCommand
				//    .Connection = objwf.MyConnection
				//    .Transaction = TRAN
				//    .CommandType = CommandType.Text
				//    .CommandText = strQry
				//    .ExecuteNonQuery()
				//End With


				if (dsSelUsers.Tables[TCnt].Rows.Count > 0) {
					for (Rcnt = 0; Rcnt <= dsSelUsers.Tables[TCnt].Rows.Count - 1; Rcnt++) {
						int TO_CC = 0;
						if (((dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_TO"]).ToString().ToUpper() == "TRUE" | (dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_TO"].ToString().ToUpper()) == "1" | (dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_CC"].ToString().ToUpper()) == "TRUE" | (dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_CC"].ToString().ToUpper()) == "1")) {
							if (!string.IsNullOrEmpty(dsSelUsers.Tables[TCnt].Rows[Rcnt]["JMF_REM_USERS_PK"].ToString())) {
								SCM.CommandType = CommandType.StoredProcedure;
								SCM.CommandText = UserName + ".TRACK_N_TRACE_PKG.JMF_REMISNDER_UERS_UPD";
								var _with17 = SCM.Parameters;
								_with17.Clear();
								_with17.Add("JMF_REM_USERS_PK_IN", dsSelUsers.Tables[TCnt].Rows[Rcnt]["JMF_REM_USERS_PK"]).Direction = ParameterDirection.Input;
								_with17.Add("REMINDER_FK_IN", Reminder_Pk);
								_with17.Add("JOB_CARD_FK_IN", JOB_CARD_FK);
								_with17.Add("ACTIVITY_STATUS_IN", 0).Direction = ParameterDirection.Input;
								_with17.Add("USER_MST_FK_IN", dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_MST_PK"]).Direction = ParameterDirection.Input;
								if (((dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_TO"].ToString().ToUpper()) == "TRUE" | (dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_TO"].ToString().ToUpper()) == "1")) {
									TO_CC = 1;
								} else if (((dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_CC"].ToString().ToUpper()) == "TRUE" | (dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_CC"].ToString().ToUpper()) == "1")) {
									TO_CC = 2;
								}
								_with17.Add("TO_CC_IN", TO_CC).Direction = ParameterDirection.Input;
								_with17.Add("LAST_MODIFIED_BY_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
								_with17.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
							} else {
								SCM.CommandType = CommandType.StoredProcedure;
								SCM.CommandText = UserName + ".TRACK_N_TRACE_PKG.JMF_REMISNDER_UERS_INS";
								var _with18 = SCM.Parameters;
								_with18.Clear();
								_with18.Add("REMINDER_FK_IN", Reminder_Pk);
								_with18.Add("JOB_CARD_FK_IN", JOB_CARD_FK);
								_with18.Add("ACTIVITY_STATUS_IN", 0).Direction = ParameterDirection.Input;
								_with18.Add("USER_MST_FK_IN", dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_MST_PK"]).Direction = ParameterDirection.Input;
                                if (((dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_TO"].ToString().ToUpper()) == "TRUE" | (dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_TO"].ToString().ToUpper()) == "1"))
                                {
                                    TO_CC = 1;
                                }
                                else if (((dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_CC"].ToString().ToUpper()) == "TRUE" | (dsSelUsers.Tables[TCnt].Rows[Rcnt]["USER_CC"].ToString().ToUpper()) == "1"))
                                {
                                    TO_CC = 2;
                                }
                                _with18.Add("TO_CC_IN", TO_CC).Direction = ParameterDirection.Input;
								_with18.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
								_with18.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
							}
							SCM.ExecuteNonQuery();
							JMFReminderPKs += "," + SCM.Parameters["RETURN_VALUE"].Value;
						}
					}
					arrMessage.Add("Saved");
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				return arrMessage;
			}
            return new ArrayList();
		}
        /// <summary>
        /// Saves the user status.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="dsUserRemarks">The ds user remarks.</param>
        /// <param name="TCnt">The t count.</param>
        /// <param name="JOB_CARD_FK">The jo b_ car d_ fk.</param>
        /// <param name="Reminder_Pk">The reminder_ pk.</param>
        /// <returns></returns>
        public ArrayList SaveUserStatus(OracleCommand SCM, string UserName, OracleTransaction TRAN, DataSet dsUserRemarks, int TCnt = 0, long JOB_CARD_FK = 0, long Reminder_Pk = 0)
		{
			int Rcnt = 0;
			DataRow Odr = null;
			string JMFReminderPKs = "0";
			int STATUS = 0;
			objwf.OpenConnection();
			objwf.MyConnection = TRAN.Connection;

			try {
				if (dsUserRemarks.Tables[TCnt].Rows.Count > 0) {
					for (Rcnt = 0; Rcnt <= dsUserRemarks.Tables[TCnt].Rows.Count - 1; Rcnt++) {
						SCM.CommandType = CommandType.StoredProcedure;
						if (!string.IsNullOrEmpty(dsUserRemarks.Tables[TCnt].Rows[Rcnt]["JMF_REM_USERS_PK"].ToString())) {
							SCM.CommandText = UserName + ".TRACK_N_TRACE_PKG.JMF_REMISNDER_REMARKS_UPD";
							var _with19 = SCM.Parameters;
							_with19.Clear();
							_with19.Add("JMF_REM_USERS_PK_IN", dsUserRemarks.Tables[TCnt].Rows[Rcnt]["JMF_REM_USERS_PK"]).Direction = ParameterDirection.Input;
							_with19.Add("REMINDER_FK_IN", Reminder_Pk);
							_with19.Add("JOB_CARD_FK_IN", JOB_CARD_FK);
							_with19.Add("ACTIVITY_STATUS_IN", 1).Direction = ParameterDirection.Input;
							_with19.Add("USER_MST_FK_IN", dsUserRemarks.Tables[TCnt].Rows[Rcnt]["USER_MST_PK"]).Direction = ParameterDirection.Input;
							if (((dsUserRemarks.Tables[TCnt].Rows[Rcnt]["STATUS"].ToString().ToUpper()) == "TRUE" | (dsUserRemarks.Tables[TCnt].Rows[Rcnt]["STATUS"].ToString().ToUpper()) == "1")) {
								STATUS = 1;
							} else {
								STATUS = 0;
							}
							_with19.Add("STATUS_IN", STATUS).Direction = ParameterDirection.Input;
							_with19.Add("USER_REMARKS_IN", dsUserRemarks.Tables[TCnt].Rows[Rcnt]["USER_REMARKS"]).Direction = ParameterDirection.Input;
							_with19.Add("LAST_MODIFIED_BY_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
							_with19.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
							SCM.ExecuteNonQuery();
						}
					}
				}
				arrMessage.Add("Saved");
				return arrMessage;
			} catch (OracleException oraexp) {
				arrMessage.Add(oraexp.Message);
				return arrMessage;
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				return arrMessage;
			}
		}

        /// <summary>
        /// Gets the locatin users.
        /// </summary>
        /// <param name="LocationPk">The location pk.</param>
        /// <param name="UserPk">The user pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ReminderPk">The reminder pk.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public DataSet GetLocatinUsers(string LocationPk, string UserPk, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 ReminderPk = 0, string Flag = "1")
		{

			DataSet ds = new DataSet();
			try {
				objwf.OpenConnection();
				objwf.MyDataAdapter = new OracleDataAdapter();
				var _with20 = objwf.MyDataAdapter;
				_with20.SelectCommand = new OracleCommand();
				_with20.SelectCommand.Connection = objwf.MyConnection;
				if (Flag == "1") {
					_with20.SelectCommand.CommandText = objwf.MyUserName + ".JOB_MANAGEMENT_FORM_PKG.FETCH_LOCUSERS";
				} else {
					_with20.SelectCommand.CommandText = objwf.MyUserName + ".JOB_MANAGEMENT_FORM_PKG.FETCH_LOCUSERS_SAVED";
				}
				_with20.SelectCommand.CommandType = CommandType.StoredProcedure;
				_with20.SelectCommand.Parameters.Add("LOCATION_PK_IN", OracleDbType.Varchar2).Value = getDefault(LocationPk, "");
				_with20.SelectCommand.Parameters.Add("USER_PK_IN", OracleDbType.Varchar2).Value = getDefault(UserPk, "");
				_with20.SelectCommand.Parameters.Add("REMINDER_PK_IN", OracleDbType.Varchar2).Value = getDefault(ReminderPk, "");
				_with20.SelectCommand.Parameters.Add("FLAG_IN", OracleDbType.Varchar2).Value = getDefault(Flag, "");
				_with20.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with20.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = RecordsPerPage;
				_with20.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
				_with20.SelectCommand.Parameters.Add("LOC_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with20.Fill(ds);
				TotalPage = Convert.ToInt32(_with20.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
				CurrentPage = Convert.ToInt32(_with20.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);
				return ds;
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
		}

        /// <summary>
        /// Gets the user remarks.
        /// </summary>
        /// <param name="ReminderPk">The reminder pk.</param>
        /// <param name="UserPk">The user pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="FROM_FORM">The fro m_ form.</param>
        /// <returns></returns>
        public DataSet GetUserRemarks(string ReminderPk, string UserPk, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 FROM_FORM = 1)
		{

			DataSet ds = new DataSet();
			try {
				objwf.OpenConnection();
				objwf.MyDataAdapter = new OracleDataAdapter();
				var _with21 = objwf.MyDataAdapter;
				_with21.SelectCommand = new OracleCommand();
				_with21.SelectCommand.Connection = objwf.MyConnection;
				_with21.SelectCommand.CommandText = objwf.MyUserName + ".JOB_MANAGEMENT_FORM_PKG.FETCH_USERREMARKS";
				_with21.SelectCommand.CommandType = CommandType.StoredProcedure;
				_with21.SelectCommand.Parameters.Add("REMINDER_PK_IN", OracleDbType.Varchar2).Value = getDefault(ReminderPk, "");
				_with21.SelectCommand.Parameters.Add("USER_PK_IN", OracleDbType.Varchar2).Value = getDefault(UserPk, "");
				_with21.SelectCommand.Parameters.Add("FROM_FORM", OracleDbType.Varchar2).Value = getDefault(FROM_FORM, "");
				_with21.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with21.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = RecordsPerPage;
				_with21.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
				_with21.SelectCommand.Parameters.Add("LOC_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with21.Fill(ds);
                TotalPage = Convert.ToInt32(_with21.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(_with21.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);
                return ds;
			} catch (Exception sqlExp) {
				throw sqlExp;
			}
		}
        #region "FetchRFQ "
        /// <summary>
        /// Fetches the mail details.
        /// </summary>
        /// <param name="lngPkValue">The LNG pk value.</param>
        /// <returns></returns>
        public DataSet FetchMailDetails(long lngPkValue)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			sb.Append("SELECT U.USER_MST_PK,");
			sb.Append("       U.USER_ID,");
			sb.Append("       U.USER_NAME,");
			sb.Append("       E.EMPLOYEE_MST_PK,");
			sb.Append("       E.EMAIL_ID");
			sb.Append("  FROM USER_MST_TBL U,");
			sb.Append("       EMPLOYEE_MST_TBL E,");
			sb.Append("       JMF_REMISNDER_UERS J");
			sb.Append("  WHERE U.EMPLOYEE_MST_FK = E.EMPLOYEE_MST_PK");
			sb.Append("   AND J.USER_MST_FK = U.USER_MST_PK");
			sb.Append("   AND J.REMINDER_FK = " + lngPkValue);
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
        /// <summary>
        /// Fetches the status details.
        /// </summary>
        /// <param name="lngPkValue">The LNG pk value.</param>
        /// <returns></returns>
        public DataSet FetchStatusDetails(long lngPkValue)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			sb.Append(" SELECT (SELECT COUNT(*)");
			sb.Append("       FROM JMF_REMISNDER_UERS T ");
			sb.Append("       WHERE T.REMINDER_FK = " + lngPkValue);
			sb.Append("        ) A,");
			sb.Append("       (SELECT COUNT(*)");
			sb.Append("       FROM JMF_REMISNDER_UERS T ");
			sb.Append("    WHERE T.REMINDER_FK = " + lngPkValue);
			sb.Append("        AND T.STATUS = 1) B ");
			sb.Append("  FROM DUAL ");
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}
		}
		#endregion
	}
}

