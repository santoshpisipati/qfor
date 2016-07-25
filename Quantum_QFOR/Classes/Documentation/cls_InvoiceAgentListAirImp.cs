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
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsInvoiceAgentListAirImp : CommonFeatures
	{

        #region "Fetch Invoice To Agent List Air Import "
        /// <summary>
        /// Gets the italai count.
        /// </summary>
        /// <param name="italaiRefNr">The italai reference nr.</param>
        /// <param name="italaiAgentType">Type of the italai agent.</param>
        /// <param name="italaiPk">The italai pk.</param>
        /// <param name="locPk">The loc pk.</param>
        /// <returns></returns>
        public int GetITALAICount(string italaiRefNr, string italaiAgentType, int italaiPk, long locPk)
		{
			try {
				System.Text.StringBuilder strITALAIQuery = new System.Text.StringBuilder(5000);
				strITALAIQuery.Append("  select iaai.inv_agent_air_imp_pk, iaai.invoice_ref_no");
				strITALAIQuery.Append(" from  inv_agent_air_imp_tbl iaai, user_mst_tbl umt");
				strITALAIQuery.Append(" where iaai.invoice_ref_no like '%" + italaiRefNr + "%'");
				strITALAIQuery.Append(" and iaai.created_by_fk = umt.user_mst_pk");
				strITALAIQuery.Append(" and umt.default_location_fk=" + locPk);
				if (italaiAgentType == "CB") {
					strITALAIQuery.Append(" and iaai.cb_or_load_agent='1'");
				} else if (italaiAgentType == "DP") {
					strITALAIQuery.Append(" and iaai.cb_or_load_agent='2'");
				}
				WorkFlow objWF = new WorkFlow();
				DataSet objITALAIDS = new DataSet();
				objITALAIDS = objWF.GetDataSet(strITALAIQuery.ToString());
				if (objITALAIDS.Tables[0].Rows.Count == 1) {
					italaiPk = Convert.ToInt32(objITALAIDS.Tables[0].Rows[0][0].ToString());
				}
				return objITALAIDS.Tables[0].Rows.Count;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}

        #endregion

        #region "Fetch All"
        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="strInvRefNo">The string inv reference no.</param>
        /// <param name="strJobPK">The string job pk.</param>
        /// <param name="strHBLNo">The string HBL no.</param>
        /// <param name="strMAWBNo">The string mawb no.</param>
        /// <param name="strCustPK">The string customer pk.</param>
        /// <param name="strVoyage">The string voyage.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="InvType">Type of the inv.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string strInvRefNo = "", string strJobPK = "", string strHBLNo = "", string strMAWBNo = "", string strCustPK = "", string strVoyage = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int InvType = 0,
		string SortType = " ASC ", short AgentType = 0, long usrLocFK = 0, Int32 flag = 0)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();

			strCondition = "FROM ";
			strCondition += "INV_AGENT_TBL INV,";
			strCondition += "JOB_CARD_AIR_IMP_TBL JOB,";
			//strCondition &= vbCrLf & "HAWB_EXP_TBL HAWB,"
			//strCondition &= vbCrLf & "MAWB_EXP_TBL MAWB,"
			strCondition += "AGENT_MST_TBL AMT,";
			strCondition += "CURRENCY_TYPE_MST_TBL CUMT,";
			strCondition += "USER_MST_TBL UMT ";
			strCondition += "WHERE";
			strCondition += "INV.JOB_CARD_FK = JOB.JOB_CARD_AIR_IMP_PK";
			strCondition += "AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ";
			strCondition += "AND INV.CREATED_BY_FK = UMT.USER_MST_PK ";
			//strCondition &= vbCrLf & "AND JOB.HAWB_REF_NO = HAWB.HAWB_REF_NO(+)"
			//strCondition &= vbCrLf & "AND JOB.MAWB_REF_NO = MAWB.MAWB_REF_NO(+)"

			if (InvType > 0) {
				if (InvType == 1) {
					strCondition += " and nvl(INV.CHK_INVOICE,0) = 1 ";
				} else if (InvType == 2) {
					strCondition += " and nvl(INV.CHK_INVOICE,0) = 0 ";
				} else {
					strCondition += " and nvl(INV.CHK_INVOICE,0) = 2 ";
				}
			}

			if (AgentType == 1) {
				strCondition += "AND JOB.CB_AGENT_MST_FK = AMT.AGENT_MST_PK(+)";
			} else {
				strCondition += "AND JOB.pol_agent_mst_fk = AMT.AGENT_MST_PK(+)";
			}
			strCondition += "AND INV.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
			//If BlankGrid = 0 Then
			//    strCondition &= vbCrLf & " AND 1=2 "
			//End If
			if (flag == 0) {
				strCondition += " AND 1=2 ";
			}
			if (AgentType == 1) {
				strCondition += " AND INV.CB_DP_LOAD_AGENT=1 ";
			} else if (AgentType == 2) {
				strCondition += " AND INV.CB_DP_LOAD_AGENT=2 ";
			}

			if (!string.IsNullOrEmpty(strInvRefNo.Trim())) {
				strCondition += "AND INV.INVOICE_REF_NO LIKE '%" + strInvRefNo.Trim() + "%'";
			}

			if (!string.IsNullOrEmpty(strJobPK.Trim())) {
				strCondition += "AND UPPER(JOB.JOBCARD_REF_NO) LIKE '%" + strJobPK.Trim().ToUpper() + "%'";
			}

			if (!string.IsNullOrEmpty(strHBLNo.Trim())) {
				strCondition += "AND UPPER(JOB.HAWB_REF_NO) LIKE '%" + strHBLNo.Trim().ToUpper() + "%'";
			}

			if (!string.IsNullOrEmpty(strMAWBNo.Trim())) {
				strCondition += "AND UPPER(JOB.MAWB_REF_NO) LIKE '%" + strMAWBNo.Trim().ToUpper() + "%'";
			}

			if (!string.IsNullOrEmpty(strCustPK.Trim())) {
				strCondition += "AND AMT.AGENT_ID='" + strCustPK.Trim() + "'";
			}

			if (!string.IsNullOrEmpty(strVoyage.Trim())) {
				strCondition += "AND JOB.FLIGHT_NO LIKE '%" + strVoyage.Trim() + "%'";
			}


			string strCount = null;
			strCount = "SELECT COUNT(*)  ";
			strCount += strCondition;
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount));
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


			strSQL = " SELECT * FROM (";
			strSQL += "SELECT ROWNUM SR_NO, q.* FROM ";
			strSQL += "( SELECT ";
			strSQL += "INV.INV_AGENT_PK,";
			strSQL += "INV.JOB_CARD_FK,";
			strSQL += "INV.AGENT_MST_FK,";
			strSQL += "INV.CURRENCY_MST_FK,";
			strSQL += "INV.INVOICE_REF_NO,";
			strSQL += "INV.INVOICE_DATE AS INVDATE,";
			strSQL += "JOB.JOBCARD_REF_NO,";
			strSQL += "JOB.Hawb_Ref_No \"HAWB_REF_NO\" ,";
			strSQL += "JOB.MAWB_REF_NO \"MAWB_REF_NO\",";
			strSQL += "AMT.AGENT_NAME,";
			strSQL += "JOB.FLIGHT_NO,";
			strSQL += "CUMT.CURRENCY_ID,";
			strSQL += "INV.NET_INV_AMT,";
			strSQL += " DECODE(INV.CHK_INVOICE,0,'Pending',1,'Approved',2,'Cancelled') CHK_INVOICE, ";
			strSQL += " NVL((SELECT DISTINCT CT.INVOICE_REF_NR FROM COLLECTIONS_TRN_TBL CT WHERE CT.INVOICE_REF_NR =INV.Invoice_Ref_No ),'0') COLL_STATUS, ";
			strSQL += "'' SEL ";
			strSQL += strCondition;
			if (SortColumn == "INVDATE") {
				SortColumn = "INV.INVOICE_DATE";
			}
			strSQL += " ORDER BY " + SortColumn + SortType + " ,INVOICE_REF_NO DESC ) q  ) ";
			strSQL += " WHERE SR_NO  BETWEEN " + start + " AND " + last;

			DataSet DS = null;
			DS = objWF.GetDataSet(strSQL);
			try {
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


        #region "Enhance Search"
        /// <summary>
        /// Fetches the invoice agent no.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchInvoiceAgentNo(string strCond, string loc = "")
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strSERACH_IN = null;
			string strBizType = null;
			string strReq = null;
			string strLoc = null;
			strLoc = loc;
			arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));


            try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandText = "";
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".EN_INV_REF_NO_PKG.GET_INV_REF_AGENT";

				var _with1 = selectCommand.Parameters;
				_with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
				_with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with1.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
				_with1.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
				_with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}
        #endregion
        #region "UpdateInvStatus"
        /// <summary>
        /// Updates the inv status.
        /// </summary>
        /// <param name="InvoicePkS">The invoice pk s.</param>
        /// <param name="remarks">The remarks.</param>
        /// <param name="CUST_OR_AGENT">The cus t_ o r_ agent.</param>
        /// <returns></returns>
        public string UpdateInvStatus(string InvoicePkS, string remarks, short CUST_OR_AGENT = 0)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand updCmd = new OracleCommand();
			DataSet dsPia = null;
			string str = null;
			string strIns = null;
			string Ret = null;
			Int16 intDel = default(Int16);
			Int16 intIns = default(Int16);
			try {
				objWF.OpenConnection();
				var _with2 = updCmd;
				_with2.Connection = objWF.MyConnection;
				_with2.CommandType = CommandType.StoredProcedure;
				_with2.CommandText = objWF.MyUserName + ".INVOICE_CANCELLATION_PKG.CANCEL_INVOICE";
				var _with3 = _with2.Parameters;
				updCmd.Parameters.Add("INVOICE_PK_IN", InvoicePkS).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("REMARKS_IN", remarks).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("INV_TYPE_FLAG_IN", "INV_AGENT_AIR_IMP").Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				intIns = Convert.ToInt16(_with2.ExecuteNonQuery());
				return Convert.ToString(updCmd.Parameters["RETURN_VALUE"].Value);
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyConnection.Close();
			}
		}
		#endregion
	}
}
