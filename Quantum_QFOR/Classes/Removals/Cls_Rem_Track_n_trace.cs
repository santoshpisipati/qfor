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
using System.Web;
namespace Quantum_QFOR
{
    public class Cls_Rem_Track_n_trace : CommonFeatures
	{
		public DataSet FetchAllEnq(string partyfk, string plrfk, string pfdfk, string validfrom, string validto, Int32 ChkONLD, Int32 CurrentPage, Int32 TotalPage)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			DataSet dstbl = new DataSet();
			try {
				var _with1 = objWF.MyCommand.Parameters;
				//fetch all dts with enq while search
				_with1.Clear();
				_with1.Add("PARTY_FK_IN", getDefault(partyfk, "0")).Direction = ParameterDirection.Input;
				_with1.Add("PLR_FK_IN", getDefault(plrfk, "0")).Direction = ParameterDirection.Input;
				_with1.Add("PFD_FK_IN", getDefault(pfdfk, "0")).Direction = ParameterDirection.Input;

				_with1.Add("VALID_FROM_IN", getDefault(validfrom, "")).Direction = ParameterDirection.Input;
				_with1.Add("VALID_TO_IN", getDefault(validto, "")).Direction = ParameterDirection.Input;
				_with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;

				_with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
				_with1.Add("RECORDS_PER_PAGE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
				_with1.Add("POST_BACK_IN", ChkONLD).Direction = ParameterDirection.Input;
				_with1.Add("FETCHENQ", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dstbl.Tables.Add(objWF.GetDataTable("REM_TRACK_N_TRACE_PKG", "FETCH_DETAIS"));
				TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
				CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
				return dstbl;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public Int32 FetchEnqPk(string refno, Int32 doctype)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			Int32 Return_value = default(Int32);
			try {
				objWF.OpenConnection();
				objWF.MyCommand.Connection = objWF.MyConnection;
				objWF.MyCommand.CommandType = CommandType.StoredProcedure;
				objWF.MyCommand.CommandText = objWF.MyUserName + ".REM_TRACK_N_TRACE_PKG.FETCH_ENQ_PK";
				var _with2 = objWF.MyCommand.Parameters;
				//fetch enqpk for selected refno.
				_with2.Clear();
				_with2.Add("REF_NO", refno).Direction = ParameterDirection.Input;
				_with2.Add("DOCTYPE_IN", getDefault(doctype, 1)).Direction = ParameterDirection.Input;
				_with2.Add("RETURN_VALUE", Return_value).Direction = ParameterDirection.Output;
				objWF.MyCommand.ExecuteNonQuery();
				Return_value = Convert.ToInt32(objWF.MyCommand.Parameters["RETURN_VALUE"].Value);
				return Return_value;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyCommand.Cancel();
				objWF.MyConnection.Close();
			}
		}
		public DataSet FetchChild(Int32 EnqFk)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				var _with3 = objWF.MyCommand.Parameters;
				//fetch child rows for enq.
				_with3.Clear();
				_with3.Add("Enq_pk_in", getDefault(EnqFk, 0)).Direction = ParameterDirection.Input;
				_with3.Add("FETCHENQ", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataSet("REM_TRACK_N_TRACE_PKG", "FETCHCHILD");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchCustomer(Int32 EnqFk)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				var _with4 = objWF.MyCommand.Parameters;
				//fetch child rows for enq.
				_with4.Clear();
				_with4.Add("Enq_pk_in", getDefault(EnqFk, 0)).Direction = ParameterDirection.Input;
				_with4.Add("FETCHCUST", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataSet("REM_TRACK_N_TRACE_PKG", "FETCHCUSTOMER");
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		public Int32 SaveInTrackNTrace(string refno, Int32 refpk, string status, Int32 Doctype, OracleTransaction TRAN)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			Int32 Return_value = default(Int32);
			try {
				objWF.OpenConnection();
				objWF.MyCommand.Connection = objWF.MyConnection;
				objWF.MyCommand.Transaction = TRAN;
				objWF.MyCommand.CommandType = CommandType.StoredProcedure;
				objWF.MyCommand.CommandText = objWF.MyUserName + ".REM_TRACK_N_TRACE_PKG.REM_TRACK_N_TRACE_INS";
				var _with5 = objWF.MyCommand.Parameters;
				_with5.Clear();
				_with5.Add("REF_NO_IN", refno).Direction = ParameterDirection.Input;
				_with5.Add("REF_FK_IN", refpk).Direction = ParameterDirection.Input;
				_with5.Add("LOC_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
				_with5.Add("STATUS_IN", status).Direction = ParameterDirection.Input;
				_with5.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
				_with5.Add("DOCTYPE_IN", Doctype).Direction = ParameterDirection.Input;
				objWF.MyCommand.ExecuteNonQuery();
				TRAN.Commit();
				return Return_value;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyCommand.Cancel();
				objWF.MyConnection.Close();
			}
		}
	}
}
