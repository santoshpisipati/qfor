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
    public class clsInvoiceAgentSea : CommonFeatures
	{

        #region "Fetch Invoice List For Sea"
        /// <summary>
        /// Gets the ils count.
        /// </summary>
        /// <param name="ilsRefNr">The ils reference nr.</param>
        /// <param name="ilsPkAgentType">Type of the ils pk agent.</param>
        /// <param name="ilsPk">The ils pk.</param>
        /// <param name="locPK">The loc pk.</param>
        /// <returns></returns>
        public int GetILSCount(string ilsRefNr, string ilsPkAgentType, int ilsPk, long locPK)
		{
			try {
				System.Text.StringBuilder strILSQuery = new System.Text.StringBuilder(4000);
				Int32 iCnt = default(Int32);
				strILSQuery.Append("  select distinct iase.inv_agent_pk, iase.invoice_ref_no");
				strILSQuery.Append("  from inv_agent_tbl iase, user_mst_tbl umt");
				strILSQuery.Append(" where iase.invoice_ref_no like '%" + ilsRefNr + "%' ");
				strILSQuery.Append(" and iase.created_by_fk = umt.user_mst_pk");
				strILSQuery.Append("  and umt.default_location_fk=" + locPK);
				if (ilsPkAgentType == "CB") {
					strILSQuery.Append(" and iase.CB_DP_LOAD_AGENT = '1'");
				} else if (ilsPkAgentType == "DP") {
					strILSQuery.Append(" and iase.CB_DP_LOAD_AGENT = '2'");
				} else if (ilsPkAgentType == "LA") {
					strILSQuery.Append(" and iase.CB_DP_LOAD_AGENT = '3'");
				}
				WorkFlow objWF = new WorkFlow();
				DataSet objILSDS = new DataSet();
				objILSDS = objWF.GetDataSet(strILSQuery.ToString());
				if (objILSDS.Tables[0].Rows.Count == 1) {
					ilsPk = Convert.ToInt32(objILSDS.Tables[0].Rows[0][0]);
				}
				iCnt = objILSDS.Tables[0].Rows.Count;
				return iCnt;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "Fetch All"
        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="strInvPK">The string inv pk.</param>
        /// <param name="strJobPK">The string job pk.</param>
        /// <param name="strHBLPK">The string HBLPK.</param>
        /// <param name="strMBLPK">The string MBLPK.</param>
        /// <param name="strCustPK">The string customer pk.</param>
        /// <param name="strVessel">The string vessel.</param>
        /// <param name="strVoyage">The string voyage.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="InvType">Type of the inv.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchAll(string strInvPK = "", string strJobPK = "", string strHBLPK = "", string strMBLPK = "", string strCustPK = "", string strVessel = "", string strVoyage = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
		int InvType = 0, string SortType = " ASC ", short AgentType = 0, long usrLocFK = 0, Int32 flag = 0, short BizType = 0, short ProcessType = 0)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			strCondition = "FROM ";
			strCondition += "INV_AGENT_TBL INV,";
			strCondition += "JOB_CARD_TRN JOB,";
			if (BizType == 1) {
				strCondition += " HAWB_EXP_TBL     HET,";
				strCondition += "MAWB_EXP_TBL     MET,";
			} else {
				strCondition += "HBL_EXP_TBL HBL,";
				strCondition += "MBL_EXP_TBL MBL,";
			}
			//  strCondition &= vbCrLf & "HBL_EXP_TBL HBL,"
			//  strCondition &= vbCrLf & "MBL_EXP_TBL MBL,"
			strCondition += "AGENT_MST_TBL CMT,";
			strCondition += "CURRENCY_TYPE_MST_TBL CUMT,";
			strCondition += "USER_MST_TBL UMT ";

			strCondition += "WHERE";
			strCondition += "INV.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK";
			strCondition += "AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ";
			strCondition += "AND INV.CREATED_BY_FK = UMT.USER_MST_PK ";
			strCondition += "AND INV.BUSINESS_TYPE=" + BizType + " ";
			strCondition += "AND INV.PROCESS_TYPE=" + ProcessType + " ";

			if (BizType == 1) {
				strCondition += "AND JOB.MBL_MAWB_FK = MET.MAWB_EXP_TBL_PK(+)";
				strCondition += "AND JOB.HBL_HAWB_FK = HET.HAWB_EXP_TBL_PK(+)";
			} else {
				strCondition += "AND JOB.JOB_CARD_TRN_PK=HBL.JOB_CARD_SEA_EXP_FK (+)";
				strCondition += "AND JOB.MBL_MAWB_FK=MBL.MBL_EXP_TBL_PK (+)";
				strCondition += "AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)";
			}
			if (InvType > 0) {
				//'Approved
				if (InvType == 2) {
					strCondition += " and nvl(INV.CHK_INVOICE,0) = 1 ";
				//'Pending
				} else if (InvType == 1) {
					strCondition += " and nvl(INV.CHK_INVOICE,0) = 0 ";
				//'cancelled
				} else {
					strCondition += " and nvl(INV.CHK_INVOICE,0) = 2 ";
				}
			}
			if (AgentType == 1) {
				strCondition += "AND JOB.CB_AGENT_MST_FK=CMT.AGENT_MST_PK (+)";
			} else {
				strCondition += "AND JOB.Dp_Agent_Mst_Fk=CMT.AGENT_MST_PK (+)";
			}
			strCondition += "AND INV.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK(+)";

			if (flag == 0) {
				strCondition += " AND 1=2";
			}
			if (AgentType == 1) {
				strCondition += " AND INV.CB_DP_LOAD_AGENT=1 ";
			} else if (AgentType == 2) {
				strCondition += " AND INV.CB_DP_LOAD_AGENT in (2,4) ";
			} else if (AgentType == 3) {
				strCondition += " AND INV.CB_DP_LOAD_AGENT=3 ";
			}

			if (!string.IsNullOrEmpty(strInvPK.Trim())) {
				strCondition += "AND UPPER(INV.INVOICE_REF_NO)  LIKE '" + "%" + strInvPK.ToUpper().Replace("'", "''") + "%'";
			}

			if (!string.IsNullOrEmpty(strJobPK.Trim())) {
				strCondition += "AND UPPER(JOB.JOBCARD_REF_NO)  LIKE '" + "%" + strJobPK.ToUpper().Replace("'", "''") + "%'";
			}
			if (BizType == 1) {
				if (!string.IsNullOrEmpty(strHBLPK.Trim())) {
					strCondition += "AND UPPER(HET.HAWB_REF_NO)  LIKE '" + "%" + strHBLPK.Trim().ToUpper().Replace("'", "''") + "%'";
				}

				if (!string.IsNullOrEmpty(strMBLPK.Trim())) {
					strCondition += "AND UPPER(MET.MAWB_REF_NO)  LIKE '" + "%" + strMBLPK.Trim().ToUpper().Replace("'", "''") + "%'";
				}
			} else {
				if (!string.IsNullOrEmpty(strHBLPK.Trim())) {
					strCondition += "AND UPPER(HBL.HBL_REF_NO)  LIKE '" + "%" + strHBLPK.Trim().ToUpper().Replace("'", "''") + "%'";
				}

				if (!string.IsNullOrEmpty(strMBLPK.Trim())) {
					strCondition += "AND UPPER(MBL.MBL_REF_NO)  LIKE '" + "%" + strMBLPK.Trim().ToUpper().Replace("'", "''") + "%'";
				}
			}


			if (!string.IsNullOrEmpty(strCustPK.Trim())) {
				strCondition += "AND CMT.AGENT_ID='" + strCustPK.Trim() + "'";
			}

			if (BizType == 1) {
				if (!string.IsNullOrEmpty(strVessel.Trim())) {
					strCondition += "AND JOB.VOYAGE_FLIGHT_NO LIKE '%" + strVessel.Trim() + "%'";
				}
			} else {
				if (!string.IsNullOrEmpty(strVessel.Trim())) {
					strCondition += "AND JOB.VESSEL_NAME LIKE '%" + strVessel.Trim() + "%'";
				}

				if (Convert.ToInt32(strVoyage.Trim()) > 0) {
					strCondition += "AND JOB.VOYAGE_TRN_FK='" + strVoyage + "'";
				}
			}






			string strCount = null;
			strCount = "SELECT COUNT(*)  ";
			strCount += strCondition;
			objWF.MyDataReader = objWF.GetDataReader(strCount);
			while (objWF.MyDataReader.Read()) {
				TotalRecords = objWF.MyDataReader.GetInt32(0);
			}
			objWF.MyDataReader.Close();

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
			strSQL += "JOB.HBL_HAWB_FK HBL_EXP_TBL_FK,";
			strSQL += "JOB.MBL_MAWB_FK MBL_EXP_TBL_FK,";
			strSQL += "JOB.CB_AGENT_MST_FK,";
			strSQL += "INV.CURRENCY_MST_FK,";
			strSQL += "INV.INVOICE_REF_NO,";
			strSQL += "INV.INVOICE_DATE INVDATE,";
			strSQL += "JOB.JOBCARD_REF_NO,";
			if (BizType == 2) {
				strSQL += "HBL.HBL_REF_NO,";
				strSQL += "MBL.MBL_REF_NO,";
			} else {
				//strSQL &= vbCrLf & " JOB.HBL_HAWB_REF_NO AS HBL_REF_NO,"
				//strSQL &= vbCrLf & "JOB.MBL_MAWB_REF_NO AS MBL_REF_NO,"
				strSQL += "  HET.HAWB_REF_NO AS HBL_REF_NO,";
				strSQL += " MET.MAWB_REF_NO AS MBL_REF_NO,";
			}
			strSQL += "CMT.AGENT_NAME,";
			strSQL += "DECODE((JOB.VESSEL_NAME  || '' || JOB.VOYAGE_FLIGHT_NO),'','',JOB.VESSEL_NAME  || '' || JOB.VOYAGE_FLIGHT_NO) VESVOYAGE,";
			//
			strSQL += "CUMT.CURRENCY_ID,";
			strSQL += "INV.NET_INV_AMT,";
			strSQL += "DECODE(INV.CHK_INVOICE,0,'Pending',1,'Approved',2,'Cancelled') CHK_INVOICE, ";
			strSQL += " NVL((SELECT DISTINCT CT.INVOICE_REF_NR FROM COLLECTIONS_TRN_TBL CT WHERE CT.INVOICE_REF_NR =INV.Invoice_Ref_No ),'0') COLL_STATUS, ";
			strSQL += "'' SEL ";
			strSQL += strCondition;
			if (SortColumn == "INVDATE") {
				SortColumn = "INV.INVOICE_DATE";
			} else if (SortColumn == "VESVOYAGE") {
				SortColumn = "JOB.VESSEL_NAME,JOB.VOYAGE_FLIGHT_NO";
			}
			strSQL += " ORDER BY " + SortColumn + SortType + " ,INVOICE_REF_NO DESC  ) q  ) ";
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
				var _with1 = updCmd;
				_with1.Connection = objWF.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWF.MyUserName + ".INVOICE_CANCELLATION_PKG.CANCEL_INVOICE";
				var _with2 = _with1.Parameters;
				updCmd.Parameters.Add("INVOICE_PK_IN", InvoicePkS).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("REMARKS_IN", remarks).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("INV_TYPE_FLAG_IN", "INV_AGENT_SEA_EXP").Direction = ParameterDirection.Input;
				updCmd.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				intIns = Convert.ToInt16(_with1.ExecuteNonQuery());
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

