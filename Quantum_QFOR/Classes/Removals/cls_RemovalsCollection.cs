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
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class clsRemovalsCollection : CommonFeatures
	{
		public int PkVal;

		public string CollRefNr;
		#region " Fetch"
		public DataSet FetchData(int intCustomerFk, string InvRefNo, int intJobPk, int intBaseCurrPk, long lngLocPk, string strFromDt = "", string strToDt = "", int ExType = 1)
		{
			try {
				System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
				WorkFlow objWF = new WorkFlow();

				strBuilder.Append(" SELECT  ");
				strBuilder.Append(" rownum SNo, ");
				strBuilder.Append(" INVPK, INVOICE_REF_NO, ");
				strBuilder.Append(" INVOICE_DATE, CURRENCY_MST_FK, ");
				strBuilder.Append(" CURRENCY_ID,  ");
				strBuilder.Append(" NET_RECEIVABLE, ROE, AMTINLOC, ");
				strBuilder.Append(" recieved, ");
				strBuilder.Append(" receivable, ");
				strBuilder.Append(" CurrReceipt, ");
				strBuilder.Append(" SEL, ");
				strBuilder.Append(" CUST ");
				strBuilder.Append(" FROM ");
				strBuilder.Append(" (  ");
				strBuilder.Append(" SELECT  DISTINCT INV.REMOVALS_INVOICE_PK INVPK, ");
				strBuilder.Append(" INV.INVOICE_REF_NO, ");
				strBuilder.Append(" INV.INVOICE_DATE,");
				strBuilder.Append(" INV.CURRENCY_MST_FK,");
				strBuilder.Append(" CUMT.CURRENCY_ID, ");
				strBuilder.Append(" INV.NET_RECEIVABLE, ");
				//HttpContext.Current.Session("CURRENCY_MST_PK") ADDING by thiyagarajan on 5/3/09
				strBuilder.Append(" (select get_ex_rate(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.INVOICE_DATE) from dual) ROE, ");
				strBuilder.Append(" round((INV.NET_RECEIVABLE)*(select get_ex_rate(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.INVOICE_DATE) from dual),2) AmtinLoc, ");
				strBuilder.Append("   nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR) from ");
				strBuilder.Append("   REM_M_COLLECTIONS_TBL clt, ");
				strBuilder.Append("  REM_T_COLLECTIONS_TRN_TBL clttrn ");
				strBuilder.Append("  where ");
				strBuilder.Append("  clt.collections_tbl_pk = clttrn.collections_tbl_fk ");
				strBuilder.Append("   and Clttrn.Invoice_Ref_Nr   = inv.invoice_ref_no ");
				strBuilder.Append("    ),0) recieved, ");
				//HttpContext.Current.Session("CURRENCY_MST_PK") ADDING by thiyagarajan on 5/3/09
				strBuilder.Append("   round(nvl(inv.NET_RECEIVABLE* (select get_ex_rate(INV.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",INV.INVOICE_DATE) from dual) - nvl(nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR) from  ");
				strBuilder.Append("   REM_M_COLLECTIONS_TBL clt, ");
				strBuilder.Append("   REM_T_COLLECTIONS_TRN_TBL clttrn ");
				strBuilder.Append("   where ");
				strBuilder.Append("  clt.collections_tbl_pk = clttrn.collections_tbl_fk ");
				strBuilder.Append("   and Clttrn.Invoice_Ref_Nr   = inv.invoice_ref_no ");
				strBuilder.Append("   ), 0) ");
				strBuilder.Append("   ,0),0),2) receivable, ");
				strBuilder.Append("  0 CurrReceipt, ");
				strBuilder.Append("  '0' Sel, ");
				strBuilder.Append(" INV.CUSTOMER_MST_FK CUST");

				strBuilder.Append("   FROM REM_M_INVOICE_TBL INV, ");
				strBuilder.Append("   rem_invoice_trn_tbl INVFRT, ");
				strBuilder.Append("   CURRENCY_TYPE_MST_TBL CUMT,USER_MST_TBL UMT ");
				strBuilder.Append("   WHERE  ");
				strBuilder.Append("   INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK ");
				strBuilder.Append("   AND INV.REMOVALS_INVOICE_PK = INVFRT.REMOVALS_INVOICE_FK ");
				//  strBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK = " & lngLocPk & "")
				strBuilder.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK ");


				if (!(intCustomerFk > 0) & !(InvRefNo.Length > 0) & !(intJobPk > 0)) {
					strBuilder.Append(" AND 1=2");
				}
				if (intCustomerFk > 0) {
					strBuilder.Append("   AND INV.CUSTOMER_MST_FK = " + intCustomerFk + " ");
				}
				if (InvRefNo.Length > 0) {
					strBuilder.Append(" AND INV.INVOICE_REF_NO = '" + InvRefNo + "'");
				}

				if (intJobPk > 0) {
					strBuilder.Append(" AND INVFRT.JOB_CARD_FK = " + intJobPk + "");
				}

				if (!(InvRefNo.Length > 0) & !(intJobPk > 0)) {

					if (strFromDt.Length > 0) {
					}
					if (strToDt.Length > 0) {
						strBuilder.Append(" AND INV.INVOICE_DATE <= TO_DATE('" + strToDt + "','" + dateFormat + "') ");
					}
				}

				strBuilder.Append(" ) ");
				strBuilder.Append(" WHERE receivable > 0 ");

				return objWF.GetDataSet(strBuilder.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchData_ROE(int intCustomerFk, string InvRefNo, int intJobPk, int intBaseCurrPk, long lngLocPk, string strFromDt = "", string strToDt = "", int ExType = 1)
		{

			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				strBuilder.Append(" SELECT  ");
				strBuilder.Append(" SNo, ");
				strBuilder.Append(" INVPK, INVOICE_REF_NO, ");
				strBuilder.Append(" INVOICE_DATE, CURRENCY_MST_FK, ");
				strBuilder.Append(" CURRENCY_ID,  ");
				strBuilder.Append(" NET_RECEIVABLE, ROE, AMTINLOC, ");
				strBuilder.Append(" recieved, ");
				strBuilder.Append(" receivable, ");
				strBuilder.Append(" CurrReceipt, ");
				strBuilder.Append(" SEL, ");
				strBuilder.Append(" CUST ");
				strBuilder.Append(" FROM ");
				strBuilder.Append(" (  ");
				strBuilder.Append(" SELECT  DISTINCT '1' sno,INV.REMOVALS_INVOICE_PK INVPK, ");
				strBuilder.Append(" INV.INVOICE_REF_NO, ");
				strBuilder.Append(" INV.INVOICE_DATE,");
				strBuilder.Append(" INV.CURRENCY_MST_FK,");
				strBuilder.Append(" CUMT.CURRENCY_ID, ");
				strBuilder.Append(" INV.NET_RECEIVABLE, ");
				strBuilder.Append(" (select get_ex_rate(INV.CURRENCY_MST_FK," + intBaseCurrPk + ",INV.INVOICE_DATE) from dual) ROE, ");
				strBuilder.Append(" round((INV.NET_RECEIVABLE)*(select get_ex_rate(INV.CURRENCY_MST_FK," + intBaseCurrPk + ",INV.INVOICE_DATE) from dual),2) AmtinLoc, ");
				strBuilder.Append("   nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR) from ");
				strBuilder.Append("   REM_M_COLLECTIONS_TBL clt, ");
				strBuilder.Append("  REM_T_COLLECTIONS_TRN_TBL clttrn ");
				strBuilder.Append("  where ");
				strBuilder.Append("  clt.collections_tbl_pk = clttrn.collections_tbl_fk ");
				strBuilder.Append("   and Clttrn.Invoice_Ref_Nr   = inv.invoice_ref_no ");
				strBuilder.Append("    ),0) recieved, ");
				strBuilder.Append("   round(nvl(inv.NET_RECEIVABLE* (select get_ex_rate(INV.CURRENCY_MST_FK," + intBaseCurrPk + ",INV.INVOICE_DATE) from dual) - nvl(nvl((select sum(clttrn.RECD_AMOUNT_HDR_CURR) from  ");
				strBuilder.Append("   REM_M_COLLECTIONS_TBL clt, ");
				strBuilder.Append("   REM_T_COLLECTIONS_TRN_TBL clttrn ");
				strBuilder.Append("   where ");
				strBuilder.Append("  clt.collections_tbl_pk = clttrn.collections_tbl_fk ");
				strBuilder.Append("   and Clttrn.Invoice_Ref_Nr   = inv.invoice_ref_no ");
				strBuilder.Append("   ), 0) ");
				strBuilder.Append("   ,0),0),2) receivable, ");
				strBuilder.Append("  0 CurrReceipt, ");
				strBuilder.Append("  '0' Sel, ");
				strBuilder.Append(" INV.CUSTOMER_MST_FK CUST");

				strBuilder.Append("   FROM REM_M_INVOICE_TBL INV, ");
				strBuilder.Append("   rem_invoice_trn_tbl INVFRT, ");
				strBuilder.Append("   CURRENCY_TYPE_MST_TBL CUMT,USER_MST_TBL UMT ");
				strBuilder.Append("   WHERE  ");
				strBuilder.Append("   INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK ");
				strBuilder.Append("   AND INV.REMOVALS_INVOICE_PK = INVFRT.REMOVALS_INVOICE_FK ");
				//  strBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK = " & lngLocPk & "")
				strBuilder.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK ");


				if (!(intCustomerFk > 0) & !(InvRefNo.Length > 0) & !(intJobPk > 0)) {
					strBuilder.Append(" AND 1=2");
				}
				if (intCustomerFk > 0) {
					strBuilder.Append("   AND INV.CUSTOMER_MST_FK = " + intCustomerFk + " ");
				}
				if (InvRefNo.Length > 0) {
					strBuilder.Append(" AND INV.INVOICE_REF_NO = '" + InvRefNo + "'");
				}

				if (intJobPk > 0) {
					strBuilder.Append(" AND INVFRT.JOB_CARD_FK = " + intJobPk + "");
				}

				if (!(InvRefNo.Length > 0) & !(intJobPk > 0)) {

					if (strFromDt.Length > 0) {
					}
					if (strToDt.Length > 0) {
						strBuilder.Append(" AND INV.INVOICE_DATE <= TO_DATE('" + strToDt + "','" + dateFormat + "') ");
					}
				}

				strBuilder.Append(" ) ");
				//strBuilder.Append(" WHERE receivable > 0 ")

				return objWF.GetDataSet(strBuilder.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public DataSet FetchSavedData(int intColPk, int intBaseCurr)
		{
			WorkFlow objWF = new WorkFlow();
			try {
				objWF.MyCommand.Parameters.Clear();
				var _with1 = objWF.MyCommand.Parameters;
				_with1.Add("COLPK_IN", intColPk).Direction = ParameterDirection.Input;
				_with1.Add("CUR_IN", intBaseCurr).Direction = ParameterDirection.Input;
				_with1.Add("HDR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with1.Add("INV_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with1.Add("MODE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataSet("REM_FETCH_COLLECTION_PKG", "REM_FETCH_AFTER_SAVE");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}


		public DataSet FetchPaymentDetails(short Mode)
		{
			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			WorkFlow objWf = new WorkFlow();
			try {
				strBuilder.Append(" select rownum,cm.collections_mode_trn_pk,");
				strBuilder.Append(" cm.collections_tbl_fk,");
				strBuilder.Append(" cm.receipt_mode,");
				strBuilder.Append(" decode(receipt_mode,1,'cheque','Cash') \"Mode\",");
				strBuilder.Append(" cm.cheque_number,");
				strBuilder.Append(" to_char(cm.cheque_date,'" + dateFormat + "') cheque_date, ");
				strBuilder.Append(" cm.bank_mst_fk,");
				strBuilder.Append(" cm.currency_mst_fk,");
				strBuilder.Append(" cmt.currency_id,");
				if (Mode == 2) {
					strBuilder.Append(" '' Amount,");
				} else {
					strBuilder.Append("  cm.recd_amount Amount,");
				}
				strBuilder.Append(" cm.exchange_rate,");
				strBuilder.Append(" cm.recd_amount");
				strBuilder.Append("   from REM_T_COLLECTIONS_MODE_TRN_TBL cm,currency_type_mst_tbl cmt where");
				if (Mode == 2) {
					strBuilder.Append(" 1=2 and");
				}
				strBuilder.Append(" cm.currency_mst_fk = cmt.currency_mst_pk");
				return objWf.GetDataSet(strBuilder.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region " Save"
		public int SaveData(DataSet dsSave, string CollectionNo, long nLocationPk, long nEmpId, double NetAmt, string Customer, double CrLimit, double CrLimitUsed, int JobPk, int ExType = 1)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			int intSaveSucceeded = 0;
			OracleTransaction TRAN = null;
			OracleCommand objcmd = new OracleCommand();


			int intPkValue = 0;
			int intChldCnt = 0;
			bool chkFlag = false;

			try {
				if (string.IsNullOrEmpty(Convert.ToString(CollectionNo))) {
					CollectionNo = GenerateCollectionNo(nLocationPk, nEmpId, Convert.ToInt64(dsSave.Tables[0].Rows[0]["CREATED_BY_FK_IN"]), objWK);
					if (Convert.ToString(CollectionNo) == "Protocol Not Defined.") {
						CollectionNo = "";
						return -1;
					}
					chkFlag = true;
				}
				CollRefNr = CollectionNo;
				var _with2 = dsSave.Tables[0].Rows[0];
				objWK.MyCommand.Connection = objWK.MyConnection;
				objWK.MyCommand.CommandType = CommandType.StoredProcedure;
				objWK.MyCommand.CommandText = objWK.MyUserName + ".REM_COLLECTIONS_TBL_PKG.REM_COLLECTIONS_TBL_INS";
				objWK.MyCommand.Parameters.Clear();
				objWK.MyCommand.Parameters.Add("COLLECTIONS_REF_NO_IN", Convert.ToString(CollectionNo));
				objWK.MyCommand.Parameters.Add("COLLECTIONS_DATE_IN", Convert.ToDateTime(_with2["COLLECTIONS_DATE_IN"]));
				objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with2["CURRENCY_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("CUSTOMER_MST_FK_IN", _with2["CUSTOMER_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("CREATED_BY_FK_IN", _with2["CREATED_BY_FK_IN"]);
                objWK.MyCommand.Parameters.Add("CONFIG_MST_PK_IN", _with2["CONFIG_MST_PK_IN"]);
                objWK.MyCommand.Parameters.Add("RETURN_VALUE", _with2["CREATED_BY_FK_IN"]).Direction = ParameterDirection.Output;

				TRAN = objWK.MyConnection.BeginTransaction();
				objWK.MyCommand.Transaction = TRAN;
				objWK.MyCommand.ExecuteNonQuery();
				intPkValue = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
				PkVal = intPkValue;

				for (intChldCnt = 0; intChldCnt <= dsSave.Tables[1].Rows.Count - 1; intChldCnt++) {
					objWK.MyCommand.Parameters.Clear();
					objWK.MyCommand.CommandType = CommandType.StoredProcedure;
					objWK.MyCommand.CommandText = objWK.MyUserName + ".REM_COLLECTIONS_TBL_PKG.REM_COLLECTIONS_TRN_TBL_INS";
					var _with3 = dsSave.Tables[1].Rows[intChldCnt];
					objWK.MyCommand.Parameters.Add("COLLECTIONS_TBL_FK_IN", intPkValue);
					objWK.MyCommand.Parameters.Add("INVOICE_REF_NR_IN", _with3["INVOICE_REF_NR_IN"]);
					objWK.MyCommand.Parameters.Add("RECD_AMOUNT_HDR_CURR_IN", _with3["RECD_AMOUNT_HDR_CURR_IN"]);
                    intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
				}

				intChldCnt = 0;
				for (intChldCnt = 0; intChldCnt <= dsSave.Tables[2].Rows.Count - 1; intChldCnt++) {
					objWK.MyCommand.Parameters.Clear();
					objWK.MyCommand.CommandType = CommandType.StoredProcedure;
					objWK.MyCommand.CommandText = objWK.MyUserName + ".REM_COLLECTIONS_TBL_PKG.REM_COLL_MODE_TRN_TBL_INS";
					var _with4 = dsSave.Tables[2].Rows[intChldCnt];
					objWK.MyCommand.Parameters.Add("COLLECTIONS_TBL_FK_IN", intPkValue);
					objWK.MyCommand.Parameters.Add("RECEIPT_MODE_IN", _with4["RECEIPT_MODE_IN"]);
                    objWK.MyCommand.Parameters.Add("CHEQUE_NUMBER_IN", _with4["CHEQUE_NUMBER_IN"]);
                    objWK.MyCommand.Parameters.Add("CHEQUE_DATE_IN", _with4["CHEQUE_DATE_IN"]);
                    objWK.MyCommand.Parameters.Add("BANK_MST_FK_IN", _with4["BANK_PK_IN"]);
                    objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with4["CURRENCY_MST_FK_IN"]);
                    objWK.MyCommand.Parameters.Add("RECD_AMOUNT_IN", _with4["RECD_AMOUNT_IN"]);
                    objWK.MyCommand.Parameters.Add("EXCHANGE_RATE_IN", _with4["EXCHANGE_RATE_IN"]);
                    objWK.MyCommand.Parameters.Add("RETURN_VALUE", _with4["CURRENCY_MST_FK_IN"]);
                    intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
				}


				if (intSaveSucceeded > 0) {
					if (CrLimit > 0) {
						SaveCreditLimit(NetAmt, Customer, CrLimitUsed, TRAN);
					}
					TRAN.Commit();

					DataSet dsData = new DataSet();
					Int32 NetRec = default(Int32);
					Int32 Rec = default(Int32);
					string Col_Status = null;
					string ColRef = null;

					ColRef = Fetch_Col_Ref_No(intPkValue);

					objWK.MyCommand.Parameters.Clear();
					objWK.MyCommand.CommandType = CommandType.StoredProcedure;
					objWK.MyCommand.CommandText = objWK.MyUserName + ".REM_COLLECTIONS_TBL_PKG.REM_CHECK_COL_DATA";

					var _with5 = objWK.MyCommand.Parameters;
					_with5.Add("COL_REF_IN", ColRef).Direction = ParameterDirection.Input;
					_with5.Add("COL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;


					objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
					objWK.MyDataAdapter.Fill(dsData);

					if ((dsData != null)) {
						NetRec = (string.IsNullOrEmpty(dsData.Tables[0].Rows[0][0].ToString()) ? 0 : Convert.ToInt32(dsData.Tables[0].Rows[0][0]));
						Rec = (string.IsNullOrEmpty(dsData.Tables[0].Rows[0][1].ToString()) ? 0 : Convert.ToInt32(dsData.Tables[0].Rows[0][1]));
                    }

					if (NetRec == Rec) {
						Col_Status = "Collection made for " + Customer;
					} else {
						Col_Status = "Part Collection made for " + Customer;
					}
					//adding by thiyagarajan on 27/1/09:TrackNTrace Task:VEK Req.
					Int32 doctype = 6;
					objcmd.Connection = objWK.MyConnection;
					objcmd.CommandType = CommandType.StoredProcedure;
					objcmd.CommandText = objWK.MyUserName + ".REM_TRACK_N_TRACE_PKG.REM_TRACK_N_TRACE_INS";
					var _with6 = objWK.MyCommand.Parameters;
					_with6.Clear();
					_with6.Add("REF_NO_IN", CollectionNo).Direction = ParameterDirection.Input;
					_with6.Add("REF_FK_IN", JobPk).Direction = ParameterDirection.Input;
					_with6.Add("LOC_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
					_with6.Add("STATUS_IN", Col_Status).Direction = ParameterDirection.Input;
					_with6.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
					_with6.Add("DOCTYPE_IN", doctype).Direction = ParameterDirection.Input;
					objcmd.ExecuteNonQuery();
					//end
				} else {
					TRAN.Rollback();
					if (chkFlag == true) {
                        RollbackProtocolKey("REMCOLLECTIONS", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), CollectionNo, System.DateTime.Now);
                    }
				}
				return intSaveSucceeded;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				if (chkFlag == true) {
                    RollbackProtocolKey("REMCOLLECTIONS", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), CollectionNo, System.DateTime.Now);
                }
				TRAN.Rollback();
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
		public string Fetch_Col_Ref_No(Int32 ColPk)
		{
			WorkFlow objWF = new WorkFlow();
			string sqlstr = null;
			string ColRefNo = null;
			sqlstr = "select ctrn.INVOICE_REF_NR\t from REM_T_COLLECTIONS_TRN_TBL ctrn where ctrn.collections_tbl_fk='" + ColPk + "'";
			ColRefNo = objWF.ExecuteScaler(sqlstr);
			return ColRefNo;
		}
		#endregion

		#region " Enhance Search Function for Job Card "
		public string Fetch_Job_For_Collection(string strCond)
		{

			WorkFlow objWF = new WorkFlow();
			OracleCommand cmd = new OracleCommand();
			string strReturn = null;
			string[] arr = null;
			string strSearchIn = "";
			short intBizType = 0;
			short intProcess = 0;
			int intParty = 0;
			long intLocPk = 0;
			string strReq = null;
			arr = strCond.Split(Convert.ToChar("~"));
			strReq = arr[0];
			strSearchIn = arr[1];
			if (arr.Length > 2)
				intBizType = Convert.ToInt16(arr[2]);
			if (arr.Length > 3)
				intProcess = Convert.ToInt16(arr[3]);
			if (arr.Length > 4) {
				if (!string.IsNullOrEmpty(Convert.ToString(arr[4]))) {
					intParty = Convert.ToInt32(arr[4]);
				}
			}

			if (arr.Length > 5)
				intLocPk = Convert.ToInt64(arr[5]);

			try {
				objWF.OpenConnection();
				cmd.Connection = objWF.MyConnection;
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandText = objWF.MyUserName + ".EN_JOB_FOR_COLLECTION.GET_JOB_COLL";
				var _with7 = cmd.Parameters;
				_with7.Add("SEARCH_IN", getDefault(strSearchIn, "")).Direction = ParameterDirection.Input;
				_with7.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with7.Add("BUSINESS_TYPE_IN", intBizType).Direction = ParameterDirection.Input;
				_with7.Add("PROCESS_IN", intProcess).Direction = ParameterDirection.Input;
				_with7.Add("PARTY_IN", intParty).Direction = ParameterDirection.Input;
				_with7.Add("LOC_IN", intLocPk).Direction = ParameterDirection.Input;
				_with7.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				cmd.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				cmd.ExecuteNonQuery();
				strReturn = Convert.ToString(cmd.Parameters["RETURN_VALUE"].Value).Trim();
				return strReturn;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				cmd.Connection.Close();
			}
		}
		#endregion

		#region " Protocol Reference Int32"
		public string GenerateCollectionNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null)
		{
			string functionReturnValue = null;
			try {
				functionReturnValue = GenerateProtocolKey("REMCOLLECTIONS", nLocationId, nEmployeeId, DateTime.Now,"" ,"" , "", nCreatedBy, ObjWK);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
			return functionReturnValue;
		}
		#endregion

		#region "Get DataSet for Report"
		public DataSet getRepHeadDS(int colpk)
		{
			WorkFlow ObjWk = new WorkFlow();
			StringBuilder strSql = new StringBuilder();
			try {
				strSql.Append(" SELECT COL.PROCESS_TYPE PROTYPE, COL.BUSINESS_TYPE BIZTYPE, COL.COLLECTIONS_TBL_PK COLPK,");
				strSql.Append(" COL.COLLECTIONS_REF_NO COLREFNO, TO_CHAR(COL.COLLECTIONS_DATE,'" + dateFormat + "') COLDATE,");
				strSql.Append(" LOC.OFFICE_NAME CMPNM, LOC.ADDRESS_LINE1 CMPADD1, LOC.ADDRESS_LINE2 CMPADD2,");
				strSql.Append(" LOC.ADDRESS_LINE3 CMPADD3, LOC.CITY CMPCITY, LOC.ZIP CMPZIP, CNT.COUNTRY_NAME CMPCNT,");


				strSql.Append(" ('PHONE :'||LOC.TELE_PHONE_NO||' '||'FAX :'||LOC.FAX_NO) PHONE,");
				strSql.Append("  (select corp.home_page from corporate_mst_tbl corp where corp.corporate_mst_pk=loc.corporate_mst_fk) URL,");


				strSql.Append(" CUST.CUSTOMER_NAME CUSTNM, CURR.CURRENCY_ID CURRNM, SUM(COLTRN.RECD_AMOUNT_HDR_CURR) COLAMT");
				strSql.Append(" FROM COLLECTIONS_TBL COL, COLLECTIONS_TRN_TBL COLTRN, CUSTOMER_MST_TBL CUST, CURRENCY_TYPE_MST_TBL CURR,");
				strSql.Append(" COUNTRY_MST_TBL CNT, USER_MST_TBL USMST, LOCATION_MST_TBL LOC");
				strSql.Append(" WHERE COL.COLLECTIONS_TBL_PK = COLTRN.COLLECTIONS_TBL_FK(+) AND COL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK(+)");
				strSql.Append(" AND LOC.COUNTRY_MST_FK = CNT.COUNTRY_MST_PK(+) AND COL.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)");
				strSql.Append(" AND COL.CREATED_BY_FK = USMST.USER_MST_PK(+) AND USMST.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
				strSql.Append(" AND COL.COLLECTIONS_TBL_PK = '" + colpk + "'");
				strSql.Append(" GROUP BY loc.corporate_mst_fk,COL.PROCESS_TYPE, COL.BUSINESS_TYPE, COL.COLLECTIONS_TBL_PK, COL.COLLECTIONS_REF_NO,");
				strSql.Append(" COL.COLLECTIONS_DATE, LOC.OFFICE_NAME, LOC.ADDRESS_LINE1, LOC.ADDRESS_LINE2, LOC.ADDRESS_LINE3,");
				strSql.Append(" LOC.CITY, LOC.ZIP, CNT.COUNTRY_NAME, CUST.CUSTOMER_NAME, CURR.CURRENCY_ID,loc.tele_phone_no,loc.fax_no");

				return ObjWk.GetDataSet(strSql.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet getModDetDs(int colpk)
		{
			WorkFlow ObjWk = new WorkFlow();
			StringBuilder strSql = new StringBuilder();
			try {
				strSql.Append("SELECT DECODE(COLMOD.RECEIPT_MODE,1,'CHEQUE','CASH') \"Mode\", COLMOD.COLLECTIONS_TBL_FK COLMODFK, COLMOD.CHEQUE_NUMBER CHQNO,");
				strSql.Append(" TO_CHAR(COLMOD.CHEQUE_DATE,'" + dateFormat + "') CHQDT, CURR.CURRENCY_ID CURRNM,");
				strSql.Append(" round((COLMOD.RECD_AMOUNT / COLMOD.EXCHANGE_RATE),2) RECAMT, COLMOD.EXCHANGE_RATE ROE, COLMOD.RECD_AMOUNT LOCAMT,");
				strSql.Append("(select Bank_ID from Bank_Mst_Tbl where bank_mst_pk=COLMOD.BANK_MST_FK) Bank_Name");
				// Added By : ANand Reason : To Display Bank Field in Report
				strSql.Append(" FROM COLLECTIONS_MODE_TRN_TBL COLMOD, CURRENCY_TYPE_MST_TBL CURR");
				strSql.Append(" WHERE COLMOD.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)");
				strSql.Append(" AND COLMOD.COLLECTIONS_TBL_FK = " + colpk);

				return ObjWk.GetDataSet(strSql.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet getTransDetDs(int COLPK_IN, int CUR_IN)
		{
			WorkFlow ObjWk = new WorkFlow();
			StringBuilder strSql = new StringBuilder();

			try {
				strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'mm/dd/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT,");
				// DatFormat Added By Anand to display mm/dd/yyyy format
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE,");
				strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1  else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
				//Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
				strSql.Append(" round(nvl(inv.net_payable * ");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk," + CUR_IN + ",inv.invoice_date) from dual) - ");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
				strSql.Append(" 0),2)  receivable , ");
				strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt ");

				strSql.Append(" FROM INV_CUST_SEA_EXP_TBL INV, Inv_Cust_Trn_Sea_Exp_Tbl INVFRT,");
				strSql.Append(" CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN,  COLLECTIONS_TRN_TBL CTRN");
				strSql.Append(" WHERE INV.INV_CUST_SEA_EXP_PK = invfrt.inv_cust_sea_exp_fk AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
				strSql.Append(" AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN);
				strSql.Append(" AND CLN.PROCESS_TYPE = 1 AND CLN.BUSINESS_TYPE = 2");
				strSql.Append(" AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");

				strSql.Append(" UNION ");
				strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'mm/dd/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT, ");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE, ");
				strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
				//Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
				strSql.Append(" round(nvl(inv.net_receivable * ");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",inv.invoice_date) from dual) - ");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
				strSql.Append(" 0),2)  receivable ,");
				strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt");

				strSql.Append(" FROM CONSOL_INVOICE_TBL  INV, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN ");
				strSql.Append(" WHERE CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
				strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				strSql.Append(" AND CLN.PROCESS_TYPE = INV.PROCESS_TYPE AND CLN.BUSINESS_TYPE = INV.BUSINESS_TYPE");
				strSql.Append(" UNION ");
				strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'mm/dd/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT,");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE,");
				strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
				strSql.Append("(select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
				//Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
				strSql.Append(" round(nvl(inv.net_payable *");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",inv.invoice_date) from dual) - ");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
				strSql.Append(" 0),2)  receivable ,");
				strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt ");
				strSql.Append(" FROM INV_CUST_AIR_EXP_TBL INV, Inv_Cust_Trn_AIR_Exp_Tbl INVFRT, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN");
				strSql.Append(" WHERE INV.INV_CUST_AIR_EXP_PK = invfrt.inv_cust_AIR_exp_fk AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK ");
				strSql.Append(" AND CLN.PROCESS_TYPE = 1 AND CLN.BUSINESS_TYPE = 1");
				strSql.Append(" AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK  AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");

				strSql.Append(" UNION ");
				strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'mm/dd/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT,");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE,");
				strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
				strSql.Append("(select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
				//Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
				strSql.Append(" round(nvl(inv.net_receivable * ");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",inv.invoice_date) from dual) - ");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
				strSql.Append(" 0),2)  receivable ,");
				strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt");
				strSql.Append(" FROM CONSOL_INVOICE_TBL INV, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN");
				strSql.Append(" WHERE CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
				strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO ");
				strSql.Append(" UNION ");
				strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'mm/dd/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT,");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE,");
				strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
				//Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
				strSql.Append(" round(nvl(inv.net_payable * ");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",inv.invoice_date) from dual) - ");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
				strSql.Append(" 0),2)  receivable , ");
				strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt ");
				strSql.Append(" FROM INV_CUST_SEA_IMP_TBL INV, Inv_Cust_Trn_Sea_IMP_Tbl INVFRT, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN,");
				strSql.Append(" COLLECTIONS_TRN_TBL CTRN WHERE INV.INV_CUST_SEA_IMP_PK = invfrt.inv_cust_sea_IMP_fk");
				strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
				strSql.Append(" AND CLN.PROCESS_TYPE = 2 AND CLN.BUSINESS_TYPE = 2");
				strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");

				strSql.Append(" UNION ");
				strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'mm/dd/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT,");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE,");
				strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved, ");
				//Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
				strSql.Append(" round(nvl(inv.net_receivable * ");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",inv.invoice_date) from dual) - ");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
				strSql.Append(" 0),2)  receivable ,");
				strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt");
				strSql.Append(" FROM CONSOL_INVOICE_TBL INV, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN");
				strSql.Append(" WHERE  CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK  AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
				strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				strSql.Append(" AND CLN.PROCESS_TYPE = INV.PROCESS_TYPE AND CLN.BUSINESS_TYPE = INV.BUSINESS_TYPE");
				strSql.Append(" UNION ");
				strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'mm/dd/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT,");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE, ");
				strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved,");
				//Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
				strSql.Append(" round(nvl(inv.net_payable * ");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",inv.invoice_date) from dual) - ");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
				strSql.Append(" 0),2)  receivable ,");
				strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt ");
				strSql.Append(" FROM INV_CUST_AIR_IMP_TBL INV, Inv_Cust_Trn_AIR_IMP_Tbl INVFRT, CURRENCY_TYPE_MST_TBL CUMT,");
				strSql.Append(" COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN");
				strSql.Append(" WHERE INV.INV_CUST_AIR_IMP_PK = invfrt.inv_cust_AIR_IMP_fk AND CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK");
				strSql.Append(" AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				strSql.Append(" AND CLN.PROCESS_TYPE = 2 AND CLN.BUSINESS_TYPE = 1");

				strSql.Append(" UNION ");
				strSql.Append(" SELECT CTRN.COLLECTIONS_TBL_FK COLFK, INV.INVOICE_REF_NO, TO_CHAR(INV.INVOICE_DATE, 'mm/dd/yyyy') INVOICE_DATE, CUMT.CURRENCY_ID, round((CTRN.RECD_AMOUNT_HDR_CURR * (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual)),2) RECAMT,");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) ROE,");
				strSql.Append(" round((CTRN.RECD_AMOUNT_HDR_CURR) / (case when (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) = 0 then 1 else (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ", inv.invoice_date) from dual) end),2) AmtinLoc,");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO) recieved, ");
				//Added By : Anand 'Reason : To Display Received,Receivable and Curr Receipt in the report 'Date : 02/04/08
				strSql.Append(" round(nvl(inv.net_receivable* ");
				strSql.Append(" (select get_ex_rate(inv.currency_mst_fk, " + CUR_IN + ",inv.invoice_date) from dual) - ");
				strSql.Append(" (select sum(CRN.RECD_AMOUNT_HDR_CURR) from COLLECTIONS_TRN_TBL CRN where CRN.INVOICE_REF_NR = INV.INVOICE_REF_NO), ");
				strSql.Append(" 0),2)  receivable ,");
				strSql.Append(" nvl(CTRN.RECD_AMOUNT_HDR_CURR,0)  CurrReceipt");
				strSql.Append(" FROM CONSOL_INVOICE_TBL INV, CURRENCY_TYPE_MST_TBL CUMT, COLLECTIONS_TBL CLN, COLLECTIONS_TRN_TBL CTRN");
				strSql.Append(" WHERE CLN.COLLECTIONS_TBL_PK = CTRN.COLLECTIONS_TBL_FK AND INV.Currency_Mst_Fk = CUMT.CURRENCY_MST_PK");
				strSql.Append(" AND CLN.COLLECTIONS_TBL_PK = " + COLPK_IN + " AND CTRN.INVOICE_REF_NR = INV.INVOICE_REF_NO");
				strSql.Append(" AND CLN.PROCESS_TYPE = INV.PROCESS_TYPE AND CLN.BUSINESS_TYPE = INV.BUSINESS_TYPE");

				return ObjWk.GetDataSet(strSql.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion
		#region "Credit Limit"
		public double FetchCustCreditAmt(string CustName)
		{
			string Strsql = null;
			double CreditAmt = 0;
			WorkFlow ObjWF = new WorkFlow();
			try {
				Strsql = "select c.credit_limit from Customer_Mst_Tbl c where c.customer_name in('" + CustName + "')";
				CreditAmt = Convert.ToInt32(getDefault(ObjWF.ExecuteScaler(Strsql), 0));
				return CreditAmt;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public double FetchcolCustCreditAmt(string CustName)
		{
			string Strsql = null;
			double CreditAmt = 0;
			WorkFlow ObjWF = new WorkFlow();
			try {
				Strsql = "select c.credit_limit from Customer_Mst_Tbl c where c.customer_id in('" + CustName + "')";
				CreditAmt = Convert.ToInt32(getDefault(ObjWF.ExecuteScaler(Strsql), 0));
				return CreditAmt;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public void SaveCreditLimit(double NetAmt, string Customer, double CrLimitUsed, OracleTransaction TRAN)
		{
			WorkFlow objWK = new WorkFlow();
			OracleCommand cmd = new OracleCommand();
			string strSQL = null;
			double temp = 0;
			//temp = CrLimitUsed - NetAmt
			temp = NetAmt + CrLimitUsed;
			try {
				cmd.CommandType = CommandType.Text;
				cmd.Connection = TRAN.Connection;
				cmd.Transaction = TRAN;

				cmd.Parameters.Clear();
				strSQL = "update customer_mst_tbl a set a.credit_limit_used = " + temp;
				strSQL = strSQL + " where a.customer_name in ('" + Customer + "')";
				cmd.CommandText = strSQL;
				cmd.ExecuteNonQuery();
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}

		}
		public double ConvertToBaseCurrency(int intBaseCurrPk, System.DateTime intBaseDate, Int32 BaseCurrency = 0, Int32 extype = 1)
		{
			StringBuilder strSql = new StringBuilder();
			double RateOfExchange = 0;
			WorkFlow ObjWF = new WorkFlow();
			try {
				if (extype == 1) {
					strSql.Append("select  GET_EX_RATE(" + intBaseCurrPk + "," + BaseCurrency + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )) FROM DUAL" );
				} else {
					strSql.Append("select  GET_EX_RATE1(" + intBaseCurrPk + "," + BaseCurrency + ",TO_DATE('" + intBaseDate + "',DATEFORMAT )," + extype + ") FROM DUAL" );
				}
				RateOfExchange = Convert.ToDouble(ObjWF.ExecuteScaler(strSql.ToString()));
				return RateOfExchange;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}

		}
		#endregion

		public DataSet Fetch_Cust(string InvRefNo)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				strQuery.Append(" select cmt.customer_id from " );
				strQuery.Append(" REM_M_INVOICE_TBL cit ," );
				strQuery.Append(" customer_mst_tbl cmt " );
				strQuery.Append(" where " );
				strQuery.Append(" cit.customer_mst_fk=cmt.customer_mst_pk(+)" );
				strQuery.Append(" and cit.invoice_ref_no= '" + InvRefNo + "'");
				return objWF.GetDataSet(strQuery.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}


		}
		#region "Fetch Currency"

		public DataSet GetCurrPK(string InvRefno)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				strQuery.Append(" SELECT CURR.CURRENCY_MST_PK CURRPK ,CURR.CURRENCY_ID CURRID FROM REM_M_INVOICE_TBL CON,CURRENCY_TYPE_MST_TBL CURR WHERE ");
				strQuery.Append(" CURR.CURRENCY_MST_PK=CON.CURRENCY_MST_FK AND CON.INVOICE_REF_NO LIKE '" + InvRefno + "' ");
				return objWF.GetDataSet(strQuery.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		public DataSet FetchCurrency(Int16 CurrencyPK = 0, string CurrencyID = "", string CurrencyName = "", bool ActiveOnly = true, Int16 ExType = 1, string COLDate = "")
		{
			string strSQL = null;
			strSQL = " select ' ' CURRENCY_ID,";
			strSQL = strSQL + "' ' CURRENCY_NAME, ";
			strSQL = strSQL + "0 CURRENCY_MST_PK ";
			strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL ";
			strSQL = strSQL + " UNION ";
			strSQL = strSQL + " SELECT C.CURRENCY_ID, C.CURRENCY_NAME, C.CURRENCY_MST_PK ";
			strSQL = strSQL + " FROM CURRENCY_TYPE_MST_TBL C";
			strSQL = strSQL + " WHERE C.CURRENCY_MST_PK=" + HttpContext.Current.Session["CURRENCY_MST_PK"];
			strSQL = strSQL + " UNION ";
			strSQL = strSQL + "Select CMT.CURRENCY_ID, ";
			strSQL = strSQL + "CMT.CURRENCY_NAME,";
			strSQL = strSQL + "CMT.CURRENCY_MST_PK ";
			strSQL = strSQL + "from CURRENCY_TYPE_MST_TBL CMT , EXCHANGE_RATE_TRN EXC  Where 1=1 ";
			if (CurrencyPK > 0) {
				strSQL = strSQL + " And CMT.CURRENCY_MST_PK =" + Convert.ToString(CurrencyPK);
			}
			strSQL = strSQL + " AND EXC.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK ";
			strSQL = strSQL + " AND " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " <> CMT.CURRENCY_MST_PK";
			strSQL = strSQL + " AND EXC.EXCHANGE_RATE IS NOT NULL ";
			strSQL = strSQL + " AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK ";
			strSQL = strSQL + " AND EXC.VOYAGE_TRN_FK IS NULL ";
			if (string.IsNullOrEmpty(COLDate)) {
				strSQL = strSQL + " AND ROUND(SYSDATE-0.5) Between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
				//nvl(TO_DATE,NULL_DATE_FORMAT) "
			} else {
				strSQL = strSQL + " AND TO_DATE(' " + COLDate + "','" + dateFormat + "') between EXC.FROM_DATE and nvl(EXC.TO_DATE,NULL_DATE_FORMAT)";
				//nvl(TO_DATE,NULL_DATE_FORMAT) "
			}
			if (ActiveOnly) {
				strSQL = strSQL + " And CMT.Active_Flag = 1  ";
			}
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL);
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion
		#region " Fetch Exchange Rate Bassed On Exchange Rate Type -Hidden"
		public DataSet FetchExchTypeROE(Int64 baseCurrency, Int64 ExType = 1, string Coldate = "")
		{
			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				strBuilder.Append(" SELECT DISTINCT");
				strBuilder.Append(" CMT.CURRENCY_MST_PK,");
				strBuilder.Append(" CMT.CURRENCY_ID,");
				if (ExType == 3) {
					strBuilder.Append(" ROUND(GET_EX_RATE1( ");
					strBuilder.Append("CMT.CURRENCY_MST_PK, " + baseCurrency + ",ROUND(TO_DATE(' " + Coldate + "',dateFormat))," + ExType + "),6) AS ROE");
				} else {
					strBuilder.Append(" ROUND(GET_EX_RATE( ");
					strBuilder.Append("CMT.CURRENCY_MST_PK, " + baseCurrency + ",ROUND(TO_DATE(' " + Coldate + "',dateFormat))),6) AS ROE");
				}
				strBuilder.Append(" FROM");
				strBuilder.Append(" CURRENCY_TYPE_MST_TBL CMT , EXCHANGE_RATE_TRN EXC");
				strBuilder.Append(" WHERE");
				strBuilder.Append(" CMT.ACTIVE_FLAG = 1");
				strBuilder.Append(" AND EXC.CURRENCY_MST_FK=CMT.CURRENCY_MST_PK ");
				strBuilder.Append(" AND EXC.CURRENCY_MST_BASE_FK =" + HttpContext.Current.Session["CURRENCY_MST_PK"]);
				strBuilder.Append(" AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK");
				strBuilder.Append(" AND EXC.VOYAGE_TRN_FK IS NULL");
				return objWF.GetDataSet(strBuilder.ToString());
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
		#endregion
	}
}
