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
using System.Text;

namespace Quantum_QFOR
{
    public class Cls_Removal_PayablesEntry : CommonFeatures
	{
		#region "Private Variables"
			#endregion
		long _Payment_Tbl_Pk;

		#region "Property"
		public long PaymentMstFk {
			get { return _Payment_Tbl_Pk; }
		}
		#endregion

		public DataTable FetchHeaderInformation(long lPaymentMstFk)
		{
			WorkFlow objWF = new WorkFlow();
			try {
				var _with1 = objWF.MyCommand;
				_with1.Parameters.Add("PAYMENT_MST_FK_IN", lPaymentMstFk).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("HEADER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataTable("REM_PAYMENTS_TBL_PKG", "FETCH_HEADER_DETAILS");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyConnection.Close();
				objWF = null;
			}
		}
		public DataTable FetchPaymentDetails(long lPaymentMstFk, long lngLocalCurrency)
		{
			WorkFlow objWF = new WorkFlow();
			try {
				var _with2 = objWF.MyCommand;
				_with2.Parameters.Add("PAYMENT_MST_FK_IN", lPaymentMstFk).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("PAYMENT_CURR_FK_IN", lngLocalCurrency).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("PAY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataTable("REM_PAYMENTS_TBL_PKG", "FETCH_PAYMENT_DETAILS");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyConnection.Close();
				objWF = null;
			}
		}
		public DataTable FetchChequeDetails(long lPaymentMstFk)
		{
			WorkFlow objWF = new WorkFlow();
			try {
				var _with3 = objWF.MyCommand;
				_with3.Parameters.Add("PAYMENT_MST_FK_IN", lPaymentMstFk).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("CHEQUE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataTable("REM_PAYMENTS_TBL_PKG", "FETCH_CHEQUE_DETAILS");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyConnection.Close();
				objWF = null;
			}
		}
		#region " Enhance Search Function for Job Card "
		public string Fetch_Job_For_Payments(string strCond)
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
			//If arr.Length > 2 Then intBizType = CShort(arr(2))
			//If arr.Length > 3 Then intProcess = CShort(arr(3))
			//If arr.Length > 4 Then
			//    If CStr(arr(4)) <> "" Then
			//        intParty = CInt(arr(4))
			//    End If
			//End If

			//If arr.Length > 5 Then intLocPk = CLng(arr(5))
			if (arr.Length > 2) {
				if (!string.IsNullOrEmpty(Convert.ToString(arr[2]))) {
					intParty = Convert.ToInt32(arr[2]);
				}
			}

			if (arr.Length > 3)
				intLocPk = Convert.ToInt64(arr[3]);

			try {
				objWF.OpenConnection();
				cmd.Connection = objWF.MyConnection;
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandText = objWF.MyUserName + ".REM_PAYMENTS_TBL_PKG.GET_JOB_PAY";
				var _with4 = cmd.Parameters;
				_with4.Add("SEARCH_IN", getDefault(strSearchIn, "")).Direction = ParameterDirection.Input;
				_with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				//.Add("BUSINESS_TYPE_IN", intBizType).Direction = ParameterDirection.Input
				//.Add("PROCESS_IN", intProcess).Direction = ParameterDirection.Input
				_with4.Add("PARTY_IN", intParty).Direction = ParameterDirection.Input;
				_with4.Add("LOC_IN", intLocPk).Direction = ParameterDirection.Input;
				_with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

		public string FetchJobVendors(string strCond)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand selectCommand = new OracleCommand();
			string strReturn = null;
			Array arr = null;
			string strLOC_MST_IN = null;
			string strSERACH_IN = "";
			string strVendorTypeIN = "1";
			int intLocationfk = 0;
			string strReq = null;
			string strJobRefNr = null;
			var strNull = "";
            arr = strCond.Split('~');
			strReq = Convert.ToString(arr.GetValue(0));
			strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
				intLocationfk = Convert.ToInt32(arr.GetValue(2));
            //If arr.Length > 3 Then intBUSINESS_TYPE_IN = arr(3)
            if (arr.Length > 3)
				strVendorTypeIN = Convert.ToString(arr.GetValue(3));
            //If arr.Length > 5 Then intProcess = arr(5)
            if (arr.Length >= 4)
				strJobRefNr = Convert.ToString(arr.GetValue(4));
            try {
				objWF.OpenConnection();
				selectCommand.Connection = objWF.MyConnection;
				selectCommand.CommandType = CommandType.StoredProcedure;
				selectCommand.CommandText = objWF.MyUserName + ".REM_PAYMENTS_TBL_PKG.GETJOBVENDOR_COMMON";
				var _with5 = selectCommand.Parameters;
				_with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
				//.Add("BUSINESS_TYPE_IN", intBUSINESS_TYPE_IN).Direction = ParameterDirection.Input
				_with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
				_with5.Add("VENDOR_TYPE_IN", strVendorTypeIN).Direction = ParameterDirection.Input;
				_with5.Add("LOCATION_MST_FK_IN", intLocationfk).Direction = ParameterDirection.Input;
				//.Add("PROCESS_TYPE_IN", intProcess).Direction = ParameterDirection.Input
				_with5.Add("JOB_REF_NR_IN", (!string.IsNullOrEmpty(strJobRefNr) ? strJobRefNr : strNull)).Direction = ParameterDirection.Input;
				_with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				selectCommand.ExecuteNonQuery();
				strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
				return strReturn;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				selectCommand.Connection.Close();
			}
		}

		#endregion
		public DataTable FetchUnsettledInvoices(string strJob, long lngLocalCurrency, bool isJob)
		{
			WorkFlow objWF = new WorkFlow();
			try {
				if (isJob) {
					var _with6 = objWF.MyCommand;
					_with6.Parameters.Add("JOB_REF_NO_IN", getDefault(strJob, 0)).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("PAYMENT_CURR_FK_IN", lngLocalCurrency).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("ACC_PAYBLE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					return objWF.GetDataTable("REM_PAYMENTS_TBL_PKG", "FETCH_ACCOUNTS_PAYABLE_FRMJOB");
				} else {
					var _with7 = objWF.MyCommand;
					_with7.Parameters.Add("VOUCHER_NO_IN", getDefault(strJob, 0)).Direction = ParameterDirection.Input;
					_with7.Parameters.Add("PAYMENT_CURR_FK_IN", lngLocalCurrency).Direction = ParameterDirection.Input;
					_with7.Parameters.Add("ACC_PAYBLE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
					return objWF.GetDataTable("REM_PAYMENTS_TBL_PKG", "FETCH_ACCOUNTS_PAYABLE_VOUCHER");
				}
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyConnection.Close();
			}
		}

		public DataTable FetchUnsettledInvoicesforsupp(long lngSupplierPk, string strFromDate, string strToDate, long lngLocalCurrency)
		{
			WorkFlow objWF = new WorkFlow();
			try {
				var _with8 = objWF.MyCommand;
				_with8.Parameters.Add("VENDOR_MST_FK_IN", lngSupplierPk).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("FROM_DATE_IN", strFromDate).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("TO_DATE_IN", getDefault(strToDate, "")).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("PAYMENT_CURR_FK_IN", lngLocalCurrency).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("ACC_PAYBLE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataTable("REM_PAYMENTS_TBL_PKG", "FETCH_ACCOUNTS_PAYABLE");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyConnection.Close();
			}
		}


		#region "Save Data"
		//public ArrayList SaveData(DataSet dsAccPayables, Collection PaymentCollection, string OperationMode, System.Web.UI.HtmlControls.HtmlInputText txtRefNo, string status = "", string JobRefNr = "")
		//{

		//	WorkFlow objWf = new WorkFlow();
		//	OracleTransaction oraTran = null;
		//	string strResult = null;
		//	string strRefNo = null;
		//	bool bCanCommit = false;
		//	try {
		//		var _with9 = objWf;
		//		_with9.OpenConnection();
		//		oraTran = _with9.MyConnection.BeginTransaction();
		//		_with9.MyCommand.Connection = _with9.MyConnection;
		//		_with9.MyCommand.Transaction = oraTran;
		//		_with9.MyCommand.CommandType = CommandType.StoredProcedure;
		//		if (OperationMode == "Add") {
		//			strRefNo = GenerateProtocolKey("REM_PAYMENT", Convert.ToInt64(PaymentCollection["LOGGED_IN_LOC"]), Convert.ToInt64(PaymentCollection["EMP_PK"]), DateTime.Now, , , , Convert.ToInt64(PaymentCollection["LOGGED_IN_USER"]), objWf);
		//		}

		//		//"status" adding by thiyagarajan on 27/1/09:TrackNTrace Task:VEK Req.
		//		strResult = SavePayments(PaymentCollection, OperationMode, objWf, strRefNo, status, JobRefNr);
		//		if (strResult.ToLower() == "saved") {
		//			strResult = string.Empty;
		//			strResult = SavePayments_Trn(dsAccPayables.Tables["Payments_Trn"], OperationMode, objWf);
		//			if (strResult.ToLower() == "saved") {
		//				strResult = string.Empty;
		//				strResult = SavePayments_Mode(dsAccPayables.Tables["Payments_Mode"], OperationMode, objWf);
		//				if (strResult.ToLower() == "saved") {
		//					bCanCommit = true;
		//				} else {
		//					bCanCommit = false;
		//				}
		//			}
		//		}
		//		if (bCanCommit) {
		//			oraTran.Commit();

		//			txtRefNo.Value = strRefNo;
		//		} else {
		//			if (OperationMode == "Add") {
		//				RollbackProtocolKey("REM_PAYMENT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), strRefNo, System.DateTime.Now);
		//			}
		//			oraTran.Rollback();
		//		}
		//		arrMessage.Add(strResult);
		//	//'Manjunath  PTS ID:Sep-02  28/09/2011
		//	} catch (OracleException oraexp) {
		//		throw oraexp;
		//	} catch (Exception ex) {
		//		oraTran.Rollback();
		//		if (OperationMode == "Add") {
		//			RollbackProtocolKey("REM_PAYMENT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]) ,
  //                      strRefNo, System.DateTime.Now);
		//		}
		//		arrMessage.Add(ex.Message);
		//		return arrMessage;
		//	} finally {
		//		objWf.MyCommand.Cancel();
		//		objWf = null;
		//	}
		//	return arrMessage;
		//}
		
		private Int32 GetEnqPk(string JobRefNr)
		{
			WorkFlow objWf = new WorkFlow();
			System.Text.StringBuilder strBuild = new System.Text.StringBuilder();
			try {
				strBuild.Append(" SELECT j.job_card_enq_fk FROM REM_M_JOB_CARD_MST_TBL J WHERE J.JOB_CARD_REF LIKE '" + JobRefNr + "' ");
				return Convert.ToInt32(objWf.ExecuteScaler(strBuild.ToString()));
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		private string SavePayments_Trn(DataTable dtPayments_Trn, string OperationMode, WorkFlow objWf)
		{
			try {
				objWf.MyCommand.Parameters.Clear();
				int intRowAffected = 0;
				var _with13 = objWf.MyCommand;
				_with13.Parameters.Add("PAID_AMOUNT_HDR_CURR_IN", OracleDbType.Double, 10, "CURRENT_PAYMENT_IN_LOCAL_CURR").Direction = ParameterDirection.Input;
				_with13.Parameters["PAID_AMOUNT_HDR_CURR_IN"].SourceVersion = DataRowVersion.Current;
				if (OperationMode == "Add") {
					_with13.CommandText = objWf.MyUserName + ".REM_PAYMENTS_TBL_PKG.REM_PAYMENTS_TRN_TBL_INS";
					_with13.Parameters.Add("PAYMENTS_TBL_FK_IN", _Payment_Tbl_Pk).Direction = ParameterDirection.Input;
					_with13.Parameters.Add("INV_SUPPLIER_TBL_FK_IN", OracleDbType.Int32, 10, "INV_SUPPLIER_TBL_FK").Direction = ParameterDirection.Input;
					_with13.Parameters["INV_SUPPLIER_TBL_FK_IN"].SourceVersion = DataRowVersion.Current;
					_with13.Parameters.Add("EXISTING_PAID_AMT_HDR_CURR_IN", OracleDbType.Int32, 10, "EXISTING_PAID_AMT_HDR_CURR").Direction = ParameterDirection.Input;
					_with13.Parameters["EXISTING_PAID_AMT_HDR_CURR_IN"].SourceVersion = DataRowVersion.Current;
					_with13.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					var _with14 = objWf.MyDataAdapter;
					_with14.InsertCommand = objWf.MyCommand;
					//added by surya prasad
					_with14.InsertCommand.Transaction = objWf.MyCommand.Transaction;
					intRowAffected = _with14.Update(dtPayments_Trn);
				} else {
					_with13.CommandText = objWf.MyUserName + ".REM_PAYMENTS_TBL_PKG.REM_PAYMENTS_TRN_TBL_UPD";
					_with13.Parameters.Add("PAYMENTS_TRN_PK_IN", OracleDbType.Int32, 10, "SEL").Direction = ParameterDirection.Input;
					_with13.Parameters["PAYMENTS_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;
					_with13.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
					var _with15 = objWf.MyDataAdapter;
					_with15.UpdateCommand = objWf.MyCommand;
					//added by surya prasad
					_with15.UpdateCommand.Transaction = objWf.MyCommand.Transaction;
					intRowAffected = _with15.Update(dtPayments_Trn);
				}
				return "saved";
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		private string SavePayments_Mode(DataTable dtPayments_Mode, string OperationMode, WorkFlow objWf)
		{
			try {
				objWf.MyCommand.Parameters.Clear();
				int intRowAffected = 0;
				var _with16 = objWf.MyCommand;
				_with16.Parameters.Add("PAYMENT_MODE_IN", OracleDbType.Int32, 1, "PAYMENT_MODE").Direction = ParameterDirection.Input;
				_with16.Parameters["PAYMENT_MODE_IN"].SourceVersion = DataRowVersion.Current;
				_with16.Parameters.Add("CHEQUE_NUMBER_IN", OracleDbType.Int32, 10, "CHEQUE_NUMBER").Direction = ParameterDirection.Input;
				_with16.Parameters["CHEQUE_NUMBER_IN"].SourceVersion = DataRowVersion.Current;
				_with16.Parameters.Add("CHEQUE_DATE_IN", OracleDbType.Varchar2, 15, "CHEQUE_DATE").Direction = ParameterDirection.Input;
				_with16.Parameters["CHEQUE_DATE_IN"].SourceVersion = DataRowVersion.Current;
				_with16.Parameters.Add("BANK_MST_FK_IN", OracleDbType.Int32, 10, "BANK_MST_FK").Direction = ParameterDirection.Input;
				_with16.Parameters["BANK_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				_with16.Parameters.Add("PAID_AMOUNT_IN", OracleDbType.Double, 10, "PAID_AMOUNT").Direction = ParameterDirection.Input;
				_with16.Parameters["PAID_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
				_with16.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Double, 10, "EXCHANGE_RATE").Direction = ParameterDirection.Input;
				_with16.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;
				_with16.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
				_with16.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				if (OperationMode == "Add") {
					_with16.CommandText = objWf.MyUserName + ".REM_PAYMENTS_TBL_PKG.REM_PAYMENTS_MODE_TRN_TBL_INS";
					_with16.Parameters.Add("PAYMENTS_TBL_FK_IN", _Payment_Tbl_Pk).Direction = ParameterDirection.Input;
					_with16.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					var _with17 = objWf.MyDataAdapter;
					_with17.InsertCommand = objWf.MyCommand;
					intRowAffected = _with17.Update(dtPayments_Mode);
				} else {
					_with16.CommandText = objWf.MyUserName + ".REM_PAYMENTS_TBL_PKG.REM_PAYMENTS_MODE_TRN_TBL_UPD";
					_with16.Parameters.Add("PAYMENTS_MODE_TRN_PK_IN", OracleDbType.Int32, 10, "PAYMENTS_MODE_TRN_PK").Direction = ParameterDirection.Input;
					_with16.Parameters["PAYMENTS_MODE_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;
					_with16.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
					var _with18 = objWf.MyDataAdapter;
					_with18.UpdateCommand = objWf.MyCommand;
					intRowAffected = _with18.Update(dtPayments_Mode);
				}
				return "saved";
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

		#endregion

		#region "PK Value"
		private string AllMasterPKs(DataSet ds)
		{
			Int16 RowCnt = default(Int16);
			Int16 ln = default(Int16);
			System.Text.StringBuilder strBuild = new System.Text.StringBuilder();
			strBuild.Append("-1,");
			for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++) {
				strBuild.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["PK"]).Trim() + ",");
			}
			strBuild.Remove(strBuild.Length - 1, 1);
			return strBuild.ToString();
		}
		#endregion

		#region "ChildTable"
		public DataTable Fetchchildlist(string CONTSpotPKs = "", string REFNo = "", string InvNo = "", string VENDOR = "", string RefDate = "", string FromDate = "", string toDate = "", Int16 Active = 0, long lngUsrLocFk = 0)
		{

			System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
			string strsql = null;
			WorkFlow objWF = new WorkFlow();
			DataTable dt = null;
			int RowCnt = 0;
			int Rno = 0;
			int pk = 0;


			buildQuery.Append("  SELECT ROWNUM \"Sl.Nr.\", T.* ");
			buildQuery.Append(" FROM ");
			buildQuery.Append(" (SELECT P.PAYMENT_HDR_PK PK, ");
			buildQuery.Append(" I.INVOICE_REF_NO,");
			buildQuery.Append(" I.INVOICE_DATE, ");
			buildQuery.Append(" I.SUPPLIER_INV_NO,");
			buildQuery.Append(" I.SUPPLIER_INV_DT,");
			buildQuery.Append(" PTRN.PAID_AMOUNT_HDR_CURR,");
			buildQuery.Append(" CUR.CURRENCY_ID");
			buildQuery.Append("  FROM REM_M_PAYMENT_HDR_TBL P,vendor_contact_dtls VCD,vendor_type_mst_tbl vt,vendor_services_trn VST,");
			buildQuery.Append("  REM_T_PAYMENT_TRN_TBL PTRN,");
			buildQuery.Append(" CURRENCY_TYPE_MST_TBL CUR,");
			buildQuery.Append(" USER_MST_TBL          UMT,");
			buildQuery.Append(" REM_INV_SUPPLIER_TBL  I,");
			buildQuery.Append(" VENDOR_MST_TBL V ");
			buildQuery.Append(" WHERE");
			buildQuery.Append(" P.PAYMENT_HDR_PK = PTRN.PAYMENTS_HDR_FK ");
			buildQuery.Append(" AND CUR.CURRENCY_MST_PK = P.CURRENCY_MST_FK");
			buildQuery.Append(" AND V.VENDOR_MST_PK=P.VENDOR_MST_FK");
			buildQuery.Append(" AND P.CREATED_BY_FK = UMT.USER_MST_PK");
			buildQuery.Append("  AND PTRN.INV_SUPPLIER_TBL_FK = I.REM_INV_SUPPLIER_PK");
			//buildQuery.Append(" AND P.PROCESS_TYPE ='" & ProcessType & "'")
			//buildQuery.Append(" AND P.BUSINESS_TYPE ='" & bizType & "'")

			if (Active != 1) {
				buildQuery.Append(" AND P.APPROVED = " + (Active == 2 ? 1 : 2));
			}

			if (!string.IsNullOrEmpty(VENDOR)) {
				buildQuery.Append(" AND P.VENDOR_MST_FK = '" + VENDOR + "' ");
			}

			//buildQuery.Append(" AND UMT.DEFAULT_LOCATION_FK ='" & lngUsrLocFk & "'")

			if (!string.IsNullOrEmpty(REFNo)) {
				REFNo = REFNo.ToUpper();
				buildQuery.Append(" AND UPPER(P.PAYMENT_REF_NO) LIKE '%" + REFNo.ToUpper().Replace("'", "''") + "%'");
				// strCondition = strCondition & " AND UPPER(Freight_Element_ID) LIKE '" & Freight_ElementID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
			}

			if (((FromDate != null) & !string.IsNullOrEmpty(FromDate)) | ((toDate != null) & !string.IsNullOrEmpty(toDate))) {
				if (((FromDate != null) | !string.IsNullOrEmpty(FromDate)) & (toDate == null | !string.IsNullOrEmpty(toDate))) {
					buildQuery.Append(" AND I.SUPPLIER_INV_DT >=TO_DATE('" + FromDate + "' ,'" + dateFormat + "')");
				}

				if (((toDate != null) | !string.IsNullOrEmpty(toDate)) & (FromDate == null | !string.IsNullOrEmpty(FromDate))) {
					buildQuery.Append(" AND I.SUPPLIER_INV_DT <=TO_DATE('" + toDate + "' ,'" + dateFormat + "')");
				}

				if (((FromDate != null) | !string.IsNullOrEmpty(FromDate)) & ((toDate != null) | !string.IsNullOrEmpty(toDate))) {
					buildQuery.Append(" AND I.SUPPLIER_INV_DT BETWEEN TO_DATE('" + FromDate + "', '" + dateFormat + "') AND TO_DATE('" + toDate + "', '" + dateFormat + "')");
				}
			} else if (((RefDate != null) | !string.IsNullOrEmpty(RefDate))) {
				buildQuery.Append(" AND P.PAYMENT_DATE =TO_DATE('" + RefDate + "' ,'" + dateFormat + "')");
			}

			if (!string.IsNullOrEmpty(InvNo)) {
				buildQuery.Append(" AND UPPER(I.SUPPLIER_INV_NO) LIKE '%" + InvNo.Trim().ToUpper().Replace("'", "''") + "%' ");
			}

			if (CONTSpotPKs.Trim().Length > 0) {
				buildQuery.Append(" AND P.PAYMENT_HDR_PK  IN (" + CONTSpotPKs + ") ");
			}
			buildQuery.Append(" GROUP BY P.PAYMENT_HDR_PK ,I.INVOICE_REF_NO,I.INVOICE_DATE,I.SUPPLIER_INV_NO,I.SUPPLIER_INV_DT,PTRN.PAID_AMOUNT_HDR_CURR,CUR.CURRENCY_ID");
			buildQuery.Append(" ) T ");

			strsql = buildQuery.ToString();
			try {
				pk = -1;
				dt = objWF.GetDataTable(strsql);
				for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++) {
					if (Convert.ToInt32(dt.Rows[RowCnt]["PK"]) != pk) {
						pk = Convert.ToInt32(dt.Rows[RowCnt]["PK"]);
						Rno = 0;
					}
					Rno += 1;
					dt.Rows[RowCnt]["Sl.Nr."] = Rno;
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

		#region "Parent Table"
		public object FetchAll(string REFNo = "", string InvNo = "", string VENDOR = "", string RefDate = "", string FromDate = "", string toDate = "", Int16 Active = 0, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
		string SortType = " DESC ", long lngUsrLocFk = 0, Int32 flag = 0)
		{

			Int32 last = default(Int32);
			Int32 start = default(Int32);
			StringBuilder strSQLBuilder = new StringBuilder();
			StringBuilder strSQL = new StringBuilder();
			WorkFlow objWF = new WorkFlow();
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			StringBuilder strCount = new StringBuilder();
			strSQLBuilder.Append(" SELECT ");
			strSQLBuilder.Append(" P.PAYMENT_HDR_PK PK,");
			strSQLBuilder.Append(" V.VENDOR_ID,");
			strSQLBuilder.Append(" P.PAYMENT_REF_NO,");
			strSQLBuilder.Append(" P.PAYMENT_DATE,");
			strSQLBuilder.Append(" SUM(PTRN.PAID_AMOUNT_HDR_CURR) AMT,");
			strSQLBuilder.Append(" CUR.CURRENCY_ID,");
			strSQLBuilder.Append(" Decode(P.APPROVED,1,'Approved',2,'Rejected',0,'')");
			strSQLBuilder.Append(" FROM  REM_M_PAYMENT_HDR_TBL P,vendor_contact_dtls VCD,vendor_type_mst_tbl vt,vendor_services_trn VST,");
			strSQLBuilder.Append(" REM_T_PAYMENT_TRN_TBL PTRN,");
			strSQLBuilder.Append(" CURRENCY_TYPE_MST_TBL CUR,");
			strSQLBuilder.Append(" USER_MST_TBL          UMT,");
			strSQLBuilder.Append(" REM_INV_SUPPLIER_TBL  I,");
			strSQLBuilder.Append(" VENDOR_MST_TBL V");
			strSQLBuilder.Append(" WHERE ");
			strSQLBuilder.Append(" P.PAYMENT_HDR_PK = PTRN.PAYMENTS_HDR_FK ");
			strSQLBuilder.Append(" AND CUR.CURRENCY_MST_PK = P.CURRENCY_MST_FK");
			strSQLBuilder.Append(" AND P.CREATED_BY_FK = UMT.USER_MST_PK");
			strSQLBuilder.Append(" AND PTRN.INV_SUPPLIER_TBL_FK = I.REM_INV_SUPPLIER_PK");
			strSQLBuilder.Append(" AND V.VENDOR_MST_PK = P.VENDOR_MST_FK");
			//strSQLBuilder.Append(" AND P.PROCESS_TYPE ='" & ProcessType & "'")
			//strSQLBuilder.Append(" AND P.BUSINESS_TYPE ='" & bizType & "'")
			if (Active != 1) {
				strSQLBuilder.Append(" AND P.APPROVED = " + (Active == 2 ? 1 : 2));
			}
			if (flag == 0) {
				strSQLBuilder.Append(" AND 1=2 ");
			}
			if (!string.IsNullOrEmpty(VENDOR)) {
				strSQLBuilder.Append(" AND P.VENDOR_MST_FK = '" + VENDOR + "' ");
			}

			if (((FromDate != null) | !string.IsNullOrEmpty(FromDate)) | ((toDate != null) | !string.IsNullOrEmpty(toDate))) {
				if (((FromDate != null) | !string.IsNullOrEmpty(FromDate)) & (toDate == null | !string.IsNullOrEmpty(toDate))) {
					strSQLBuilder.Append(" AND I.SUPPLIER_INV_DT >=TO_DATE('" + FromDate + "' ,'" + dateFormat + "')");
				}

				if (((toDate != null) | !string.IsNullOrEmpty(toDate)) & (FromDate == null | !string.IsNullOrEmpty(FromDate))) {
					strSQLBuilder.Append(" AND I.SUPPLIER_INV_DT <=TO_DATE('" + toDate + "' ,'" + dateFormat + "')");
				}

				if (((FromDate != null) | !string.IsNullOrEmpty(FromDate)) & ((toDate != null) | !string.IsNullOrEmpty(toDate))) {
					strSQLBuilder.Append(" AND I.SUPPLIER_INV_DT BETWEEN TO_DATE('" + FromDate + "', '" + dateFormat + "') AND TO_DATE('" + toDate + "', '" + dateFormat + "')");
				}
			} else if (((RefDate != null) | !string.IsNullOrEmpty(RefDate))) {
				strSQLBuilder.Append(" AND P.PAYMENT_DATE =TO_DATE('" + RefDate + "' ,'" + dateFormat + "')");
			}

			//strSQLBuilder.Append(" AND UMT.DEFAULT_LOCATION_FK ='" & lngUsrLocFk & "'")
			if (!string.IsNullOrEmpty(REFNo)) {
				REFNo = REFNo.ToUpper();
				strSQLBuilder.Append(" AND UPPER(P.PAYMENT_REF_NO) LIKE '%" + REFNo.ToUpper().Replace("'", "''") + "%'");
				// strCondition = strCondition & " AND UPPER(Freight_Element_ID) LIKE '" & Freight_ElementID.ToUpper.Replace("'", "''") & "%'" & vbCrLf
			}
			if (!string.IsNullOrEmpty(InvNo)) {
				strSQLBuilder.Append(" AND UPPER(I.SUPPLIER_INV_NO) LIKE '%" + InvNo.Trim().ToUpper().Replace("'", "''") + "%' ");
			}
			strSQLBuilder.Append(" GROUP BY PAYMENT_HDR_PK,");
			strSQLBuilder.Append(" P.PAYMENT_HDR_PK,V.VENDOR_ID,P.PAYMENT_REF_NO,P.PAYMENT_DATE,CUR.CURRENCY_ID,P.APPROVED  ORDER BY " + SortColumn + "  " + SortType + "  ");
			strCount.Append(" SELECT COUNT(*)");
			strCount.Append(" FROM ");
			strCount.Append("(" + strSQLBuilder.ToString() + ")");

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

			strSQL.Append("SELECT QRY.* FROM ");
			strSQL.Append("(SELECT ROWNUM \"Sl.Nr.\", T.*  FROM ");
			strSQL.Append("  (" + strSQLBuilder.ToString() + " ");
			strSQL.Append("  ) T) QRY  WHERE \"Sl.Nr.\"  BETWEEN " + start + " AND " + last);
			string sql = null;
			sql = strSQL.ToString();

			DataSet DS = null;
			try {
				DS = objWF.GetDataSet(sql);
				DataRelation CONTRel = null;
				DS.Tables.Add(Fetchchildlist(AllMasterPKs(DS), REFNo, InvNo, VENDOR, RefDate, FromDate, toDate, Active, lngUsrLocFk));
				CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["PK"], DS.Tables[1].Columns["PK"], true);
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

	}
}
