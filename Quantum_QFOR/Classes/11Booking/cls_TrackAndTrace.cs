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
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_TrackAndTrace : CommonFeatures
	{
        /// <summary>
        /// Saves the track and trace.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="objWF">The object wf.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <param name="TRAN">The tran.</param>
        /// <returns></returns>
        public ArrayList SaveTrackAndTrace(int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, WorkFlow objWF, string flagInsUpd, long lngCreatedby, string PkStatus,
		OracleTransaction TRAN = null)
		{

			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			//As String = "J" _
			try {
				arrMessage.Clear();
				//If TRAN Is Nothing Then
				//Added By sivachandran for Single transaction Commit
				if ((TRAN != null)) {
					objWF.MyCommand.Connection = TRAN.Connection;
				}
				var _with1 = objWF.MyCommand;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
				//Added By sivachandran for Single transaction Commit
				if ((TRAN != null)) {
					_with1.Transaction = TRAN;
				}
				_with1.Parameters.Clear();
				_with1.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input;
				_with1.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
				_with1.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with1.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Saves the bb track and trace.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="objWF">The object wf.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <param name="TRAN">The tran.</param>
        /// <returns></returns>
        public ArrayList SaveBBTrackAndTrace(int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, WorkFlow objWF, string flagInsUpd, long lngCreatedby, string PkStatus,
		OracleTransaction TRAN = null)
		{

			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			//As String = "J" _
			try {
				arrMessage.Clear();
				//If TRAN Is Nothing Then
				//Added By sivachandran for Single transaction Commit
				if ((TRAN != null)) {
					objWF.MyCommand.Connection = TRAN.Connection;
				}
				var _with2 = objWF.MyCommand;
				_with2.CommandType = CommandType.StoredProcedure;
				_with2.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.BB_TRACK_N_TRACE_INS";
				//Added By sivachandran for Single transaction Commit
				if ((TRAN != null)) {
					_with2.Transaction = TRAN;
				}
				_with2.Parameters.Clear();
				_with2.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input;
				_with2.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
				_with2.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with2.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Saves the track and trace for inv.
        /// </summary>
        /// <param name="TRAN">The tran.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="objWF">The object wf.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <returns></returns>
        public ArrayList SaveTrackAndTraceForInv(OracleTransaction TRAN, int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, WorkFlow objWF, string flagInsUpd, long lngCreatedby,
		string PkStatus)
		{

			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			//As String = "J" _
			try {
				arrMessage.Clear();


				if ((objWF.MyCommand.Transaction == null)) {
				}

				var _with3 = objWF.MyCommand;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
				_with3.Transaction = TRAN;
				_with3.Parameters.Clear();
				_with3.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input;
				_with3.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
				_with3.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with3.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Saves the track and trace import job.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <returns></returns>
        public ArrayList SaveTrackAndTraceImportJob(int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, OracleTransaction TRAN, string flagInsUpd, long lngCreatedby, string PkStatus)
		{
			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			try {
				arrMessage.Clear();
				WorkFlow objWF = new WorkFlow();
				OracleCommand InsCmd = new OracleCommand();

				var _with4 = InsCmd;
				_with4.CommandType = CommandType.StoredProcedure;
				_with4.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
				_with4.Connection = TRAN.Connection;
				_with4.Transaction = TRAN;
				_with4.Parameters.Clear();
				_with4.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input;
				_with4.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
				_with4.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with4.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Saves the bb track and trace import job.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <returns></returns>
        public ArrayList SaveBBTrackAndTraceImportJob(int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, OracleTransaction TRAN, string flagInsUpd, long lngCreatedby, string PkStatus)
		{
			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			try {
				arrMessage.Clear();
				WorkFlow objWF = new WorkFlow();
				OracleCommand InsCmd = new OracleCommand();

				var _with5 = InsCmd;
				_with5.CommandType = CommandType.StoredProcedure;
				_with5.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.BB_TRACK_N_TRACE_INS";
				_with5.Connection = TRAN.Connection;
				_with5.Transaction = TRAN;
				_with5.Parameters.Clear();
				_with5.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
				_with5.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with5.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
				_with5.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
				_with5.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
				_with5.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
				_with5.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
				_with5.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
				_with5.Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input;
				_with5.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
				_with5.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with5.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Saves the track and trace import job ata.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <param name="ATADate">The ata date.</param>
        /// <returns></returns>
        public ArrayList SaveTrackAndTraceImportJobATA(int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, OracleTransaction TRAN, string flagInsUpd, long lngCreatedby, string PkStatus,
		System.DateTime ATADate)
		{
			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			try {
				arrMessage.Clear();
				WorkFlow objWF = new WorkFlow();
				OracleCommand InsCmd = new OracleCommand();

				if (ATADate != System.DateTime.Now) {
					var _with6 = InsCmd;
					_with6.CommandType = CommandType.StoredProcedure;
					_with6.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS_ATA";
					_with6.Connection = TRAN.Connection;
					_with6.Transaction = TRAN;
					_with6.Parameters.Clear();
					_with6.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("DATE_IN", ATADate).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input;
					_with6.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
					_with6.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
					_with6.ExecuteNonQuery();

				}
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

        /// <summary>
        /// Saves the track and trace export on ld upd.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <param name="strContData">The string cont data.</param>
        /// <returns></returns>
        public ArrayList SaveTrackAndTraceExportOnLDUpd(int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, OracleTransaction TRAN, string flagInsUpd, long lngCreatedby, string PkStatus,
		string strContData)
		{

			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			try {
				arrMessage.Clear();
				WorkFlow objWF = new WorkFlow();
				OracleCommand InsCmd = new OracleCommand();
				int intCnt = 0;
				var _with7 = InsCmd;
				_with7.CommandType = CommandType.StoredProcedure;
				_with7.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
				_with7.Connection = TRAN.Connection;
				_with7.Transaction = TRAN;
				_with7.Parameters.Clear();
				_with7.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("status_in", (string.IsNullOrEmpty(strContData) ? "" : strContData)).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("Container_Data_in", (string.IsNullOrEmpty(strContData) ? "" : strContData)).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
				_with7.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with7.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Saves the bb track and trace export on ld upd.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <param name="strContData">The string cont data.</param>
        /// <returns></returns>
        public ArrayList SaveBBTrackAndTraceExportOnLDUpd(int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, OracleTransaction TRAN, string flagInsUpd, long lngCreatedby, string PkStatus,
		string strContData)
		{

			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			try {
				arrMessage.Clear();
				WorkFlow objWF = new WorkFlow();
				OracleCommand InsCmd = new OracleCommand();
				int intCnt = 0;
				var _with8 = InsCmd;
				_with8.CommandType = CommandType.StoredProcedure;
				_with8.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.BB_TRACK_N_TRACE_INS";
				_with8.Connection = TRAN.Connection;
				_with8.Transaction = TRAN;
				_with8.Parameters.Clear();
				_with8.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("status_in", (string.IsNullOrEmpty(strContData) ? "" : strContData)).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("Container_Data_in", (string.IsNullOrEmpty(strContData) ? "" : strContData)).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
				_with8.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with8.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

        /// <summary>
        /// Saves the track and trace export on atd upd.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <param name="strContData">The string cont data.</param>
        /// <returns></returns>
        public ArrayList SaveTrackAndTraceExportOnATDUpd(int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, OracleTransaction TRAN, string flagInsUpd, long lngCreatedby, string PkStatus,
		string strContData)
		{

			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			try {
				arrMessage.Clear();
				WorkFlow objWF = new WorkFlow();
				OracleCommand InsCmd = new OracleCommand();
				int intCnt = 0;
				var _with9 = InsCmd;
				_with9.CommandType = CommandType.StoredProcedure;
				_with9.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
				_with9.Connection = TRAN.Connection;
				_with9.Transaction = TRAN;
				_with9.Parameters.Clear();
				_with9.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
				_with9.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with9.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
				//.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input
				_with9.Parameters.Add("status_in", (string.IsNullOrEmpty(strContData) ? "" : strContData)).Direction = ParameterDirection.Input;
				_with9.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
				_with9.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
				_with9.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
				_with9.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
				_with9.Parameters.Add("Container_Data_in", (string.IsNullOrEmpty(strContData) ? "" : strContData)).Direction = ParameterDirection.Input;
				_with9.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
				_with9.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with9.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Saves the bb track and trace export on atd upd.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <param name="strContData">The string cont data.</param>
        /// <returns></returns>
        public ArrayList SaveBBTrackAndTraceExportOnATDUpd(int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, OracleTransaction TRAN, string flagInsUpd, long lngCreatedby, string PkStatus,
		string strContData)
		{

			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			try {
				arrMessage.Clear();
				WorkFlow objWF = new WorkFlow();
				OracleCommand InsCmd = new OracleCommand();
				int intCnt = 0;
				var _with10 = InsCmd;
				_with10.CommandType = CommandType.StoredProcedure;
				_with10.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.BB_TRACK_N_TRACE_INS";
				_with10.Connection = TRAN.Connection;
				_with10.Transaction = TRAN;
				_with10.Parameters.Clear();
				_with10.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
				_with10.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with10.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
				//.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input
				_with10.Parameters.Add("status_in", (string.IsNullOrEmpty(strContData) ? "" : strContData)).Direction = ParameterDirection.Input;
				_with10.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
				_with10.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
				_with10.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
				_with10.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
				_with10.Parameters.Add("Container_Data_in", (string.IsNullOrEmpty(strContData) ? "" : strContData)).Direction = ParameterDirection.Input;
				_with10.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
				_with10.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with10.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Deletes the on save trace export on atdld upd.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <param name="strContData">The string cont data.</param>
        /// <returns></returns>
        public ArrayList DeleteOnSaveTraceExportOnATDLDUpd(int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, OracleTransaction TRAN, string flagInsUpd, long lngCreatedby, string PkStatus,
		string strContData)
		{

			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			try {
				arrMessage.Clear();
				WorkFlow objWF = new WorkFlow();
				OracleCommand InsCmd = new OracleCommand();
				int intCnt = 0;
				var _with11 = InsCmd;
				_with11.CommandType = CommandType.StoredProcedure;
				_with11.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
				_with11.Connection = TRAN.Connection;
				_with11.Transaction = TRAN;
				_with11.Parameters.Clear();
				_with11.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
				_with11.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with11.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
				_with11.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
				_with11.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
				_with11.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
				_with11.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
				_with11.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
				_with11.Parameters.Add("Container_Data_in", (string.IsNullOrEmpty(strContData) ? "" : strContData)).Direction = ParameterDirection.Input;
				_with11.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
				_with11.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with11.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #region "JOBCARD"
        /// <summary>
        /// Saves the track and trace jobcard.
        /// </summary>
        /// <param name="JOBCARD_PK">The jobcar d_ pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="objWF">The object wf.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <param name="TRAN">The tran.</param>
        /// <returns></returns>
        public ArrayList SaveTrackAndTraceJOBCARD(int JOBCARD_PK, int BizType, int Process, int Locationfk, WorkFlow objWF, string flagInsUpd, long lngCreatedby, string PkStatus, OracleTransaction TRAN = null)
		{

			Int32 retVal = default(Int32);
			Int32 RecAfct = default(Int32);
			try {
				arrMessage.Clear();
				if ((TRAN != null)) {
					objWF.MyCommand.Connection = TRAN.Connection;
				}
				var _with12 = objWF.MyCommand;
				_with12.Parameters.Clear();
				if ((TRAN != null)) {
					_with12.Transaction = TRAN;
				}
				_with12.CommandType = CommandType.StoredProcedure;
				if (BizType == 2 & Process == 1) {
					_with12.CommandText = objWF.MyUserName + ".JOB_CARD_SEA_PKG.TRACK_N_TRACE";
					_with12.Parameters.Add("JOB_CARD_SEA_EXP_PK_IN", JOBCARD_PK).Direction = ParameterDirection.Input;
				} else if (BizType == 2 & Process == 2) {
					_with12.CommandText = objWF.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.TRACK_N_TRACE";
					_with12.Parameters.Add("JOB_CARD_SEA_IMP_PK_IN", JOBCARD_PK).Direction = ParameterDirection.Input;
				} else if (BizType == 1 & Process == 1) {
					_with12.CommandText = objWF.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.TRACK_N_TRACE";
					_with12.Parameters.Add("JOB_CARD_AIR_EXP_PK_IN", JOBCARD_PK).Direction = ParameterDirection.Input;
				} else if (BizType == 1 & Process == 2) {
					_with12.CommandText = objWF.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.TRACK_N_TRACE";
					_with12.Parameters.Add("JOB_CARD_AIR_IMP_PK_IN", JOBCARD_PK).Direction = ParameterDirection.Input;
				}
				_with12.Parameters.Add("CREATED_BY_FK_IN", lngCreatedby).Direction = ParameterDirection.Input;
				_with12.Parameters.Add("LOCATION_MST_FK_IN", Locationfk).Direction = ParameterDirection.Input;
				_with12.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion


        #region " Fetch Track And Trace Detais"
        //Fetch Functionality for Track N Trace
        /// <summary>
        /// Fetches the details.
        /// </summary>
        /// <param name="strRefPk">The string reference pk.</param>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcess">The int process.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="ContUld">The cont uld.</param>
        /// <returns></returns>
        public DataSet FetchDetails(string strRefPk, int intBizType, int intProcess, int TotalPage = 0, int CurrentPage = 0, string ContUld = "")
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            string strSQL = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            // holds the last record slo of the page
            Int32 start = default(Int32);
            //holds the first record slo of the page

            WorkFlow objWF = new WorkFlow();
            strBuilder.Append(" SELECT TRK.JOB_CARD_FK REFPK, ");
            strBuilder.Append(" (TO_CHAR(TRK.CREATED_ON,'" + dateFormat + "')) ");
            strBuilder.Append(" \"Date\",(TO_CHAR(TRK.CREATED_ON,'HH24:Mi:ss')) \"Time\", ");
            strBuilder.Append(" LOC.LOCATION_ID \"Loc\",");
            strBuilder.Append(" LOC.OFFICE_NAME \"Office Name\",");
            strBuilder.Append(" TRK.DOC_REF_NO \"Reference Nr.\",");
            strBuilder.Append(" DECODE(TRK.BIZ_TYPE,1,'Air',2,'Sea',3,'Both') \"Biz\",");
            strBuilder.Append(" DECODE(TRK.PROCESS,1,'Exp',2,'Imp') \"Process\",");
            strBuilder.Append(" TRK.STATUS \"Status\", ");
            strBuilder.Append(" USR.USER_NAME \"User Name\",");
            strBuilder.Append(" DM.DOCUMENT_URL \"URL\", ");
            strBuilder.Append(" CUST.CUSTOMER_ID \"CUSTOMERID\",");
            strBuilder.Append(" CUST.CUSTOMER_NAME \"CUSTOMERNAME\",");
            strBuilder.Append(" decode(trk.process,1,'Shipper',2,'consignee') \"CUSTOMERTYPEID\",");
            strBuilder.Append(" FETCH_REFPK_TRACK_N_TRACE(TRK.DOC_REF_NO,dm.document_url_pk,TRK.BIZ_TYPE,TRK.PROCESS) \"URLREFPK\",");
            strBuilder.Append(" TRK.CREATED_ON");

            strBuilder.Append(" FROM TRACK_N_TRACE_TBL TRK,USER_MST_TBL USR,");
            strBuilder.Append(" LOCATION_MST_TBL LOC, ");

            if (intBizType == 1 & intProcess == 1)
            {
                strBuilder.Append(" BOOKING_MST_TBL BOOK,");
                strBuilder.Append(" JOB_CARD_TRN JC,");
            }
            else if (intBizType == 2 & intProcess == 1)
            {
                strBuilder.Append(" BOOKING_MST_TBL BOOK,");
                strBuilder.Append(" JOB_CARD_TRN JC,");
            }
            else if (intBizType == 1 & intProcess == 2)
            {
                strBuilder.Append(" JOB_CARD_TRN JC,");
            }
            else
            {
                strBuilder.Append(" JOB_CARD_TRN JC,");
            }

            strBuilder.Append(" DOCUMENT_URL_MST_TBL DM,CUSTOMER_MST_TBL CUST");
            strBuilder.Append(" WHERE TRK.CREATED_BY = USR.USER_MST_PK");
            strBuilder.Append(" AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK");


            if (intBizType == 1 & intProcess == 1)
            {
                strBuilder.Append(" AND JC.JOB_CARD_TRN_PK in (" + strRefPk + ") ");
                strBuilder.Append(" AND JC.BOOKING_MST_FK = BOOK.BOOKING_MST_PK");
                strBuilder.Append(" AND BOOK.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                strBuilder.Append("");
            }
            else if (intBizType == 2 & intProcess == 1)
            {
                strBuilder.Append(" AND JC.JOB_CARD_TRN_PK in (" + strRefPk + ") ");
                strBuilder.Append(" AND JC.BOOKING_MST_FK = BOOK.BOOKING_MST_PK");
                strBuilder.Append(" AND BOOK.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                strBuilder.Append("");
            }
            else if (intBizType == 1 & intProcess == 2)
            {
                strBuilder.Append(" AND JC.JOB_CARD_TRN_PK in (" + strRefPk + ") ");
                strBuilder.Append(" AND JC.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                strBuilder.Append("");
            }
            else
            {
                strBuilder.Append(" AND JC.JOB_CARD_TRN_PK in (" + strRefPk + ") ");
                strBuilder.Append(" AND JC.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                strBuilder.Append("");
            }
            strBuilder.Append(" AND TRK.DOCUMENT_URL_FK = DM.DOCUMENT_URL_PK(+) ");
            strBuilder.Append(" AND TRK.BIZ_TYPE = " + intBizType + "");
            strBuilder.Append(" AND TRK.PROCESS = " + intProcess + "");
            strBuilder.Append(" AND TRK.JOB_CARD_FK in (" + strRefPk + ")");
            strBuilder.Append(" ORDER BY TRK.CREATED_ON DESC, TRK.DOC_REF_NO DESC");

            //Added by Ajay 
            strSQL = "SELECT COUNT(*) FROM (";
            strSQL += strBuilder.ToString();
            strSQL += " ) ";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
            {
                CurrentPage = 1;
            }
            if (TotalRecords == 0)
            {
                CurrentPage = 0;
            }

            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;
            //Ended by Ajay

            return objWF.GetDataSet(strBuilder.ToString());
        }
        /// <summary>
        /// Fetches the details customer.
        /// </summary>
        /// <param name="strRefPk">The string reference pk.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcess">The int process.</param>
        /// <returns></returns>
        public DataSet FetchDetailsCust(string strRefPk, int CustPK, int intBizType, int intProcess)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strBuilder.Append(" SELECT TRK.JOB_CARD_FK REFPK, ");
            strBuilder.Append(" (TO_CHAR(TRK.CREATED_ON,'" + dateFormat + "')) ");
            strBuilder.Append(" \"Date\",(TO_CHAR(TRK.CREATED_ON,'HH24:Mi:ss')) \"Time\", ");
            strBuilder.Append(" LOC.LOCATION_ID \"Loc\",");
            strBuilder.Append(" LOC.OFFICE_NAME \"Office Name\",");
            strBuilder.Append(" TRK.DOC_REF_NO \"Reference Nr.\",");
            strBuilder.Append(" DECODE(TRK.BIZ_TYPE,1,'Air',2,'Sea',3,'Both') \"Biz\",");
            strBuilder.Append(" DECODE(TRK.PROCESS,1,'Exp',2,'Imp') \"Process\",");
            strBuilder.Append(" TRK.STATUS \"Status\", ");
            strBuilder.Append(" USR.USER_ID \"UserID\",");
            strBuilder.Append(" DM.DOCUMENT_URL \"URL\", ");
            strBuilder.Append(" CUST.CUSTOMER_ID \"CUSTOMERID\",");
            strBuilder.Append(" CUST.CUSTOMER_NAME \"CUSTOMERNAME\",");
            strBuilder.Append(" decode(trk.process,1,'Shipper',2,'consignee') \"CUSTOMERTYPEID\",");
            strBuilder.Append(" FETCH_REFPK_TRACK_N_TRACE(TRK.DOC_REF_NO,dm.document_url_pk,TRK.BIZ_TYPE,TRK.PROCESS) \"URLREFPK\",");
            strBuilder.Append(" TRK.CREATED_ON");

            strBuilder.Append(" FROM TRACK_N_TRACE_TBL TRK,USER_MST_TBL USR,");
            strBuilder.Append(" LOCATION_MST_TBL LOC, ");

            if (intBizType == 1 & intProcess == 1)
            {
                strBuilder.Append(" BOOKING_MST_TBL BOOK,");
                strBuilder.Append(" JOB_CARD_TRN JC,");
            }
            else if (intBizType == 2 & intProcess == 1)
            {
                strBuilder.Append(" BOOKING_MST_TBL BOOK,");
                strBuilder.Append(" JOB_CARD_TRN JC,");
            }
            else if (intBizType == 1 & intProcess == 2)
            {
                strBuilder.Append(" JOB_CARD_TRN JC,");
            }
            else
            {
                strBuilder.Append(" JOB_CARD_TRN JC,");
            }

            strBuilder.Append(" DOCUMENT_URL_MST_TBL DM,CUSTOMER_MST_TBL CUST");
            strBuilder.Append(" WHERE TRK.CREATED_BY = USR.USER_MST_PK");
            strBuilder.Append(" AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK");


            if (intBizType == 1 & intProcess == 1)
            {
                strBuilder.Append(" AND JC.JOB_CARD_MST_PK in (" + strRefPk + ") ");
                strBuilder.Append(" AND JC.BOOKING_MST_FK = BOOK.BOOKING_MST_PK");
                strBuilder.Append(" AND BOOK.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                strBuilder.Append("");
            }
            else if (intBizType == 2 & intProcess == 1)
            {
                strBuilder.Append(" AND JC.JOB_CARD_TRN_PK in (" + strRefPk + ") ");
                strBuilder.Append(" AND JC.BOOKING_MST_FK = BOOK.BOOKING_MST_PK");
                strBuilder.Append(" AND BOOK.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                strBuilder.Append("");
            }
            else if (intBizType == 1 & intProcess == 2)
            {
                strBuilder.Append(" AND JC.JOB_CARD_TRN_PK in (" + strRefPk + ") ");
                strBuilder.Append(" AND JC.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                strBuilder.Append("");
            }
            else
            {
                strBuilder.Append(" AND JC.JOB_CARD_TRN_PK in (" + strRefPk + ") ");
                strBuilder.Append(" AND JC.Cust_Customer_Mst_Fk = cust.customer_mst_pk");
                strBuilder.Append("");
            }
            strBuilder.Append(" AND TRK.DOCUMENT_URL_FK = DM.DOCUMENT_URL_PK ");
            strBuilder.Append(" AND TRK.BIZ_TYPE = " + intBizType + "");
            strBuilder.Append(" AND TRK.PROCESS = " + intProcess + "");
            strBuilder.Append(" AND TRK.JOB_CARD_FK in (" + strRefPk + ")");
            strBuilder.Append(" AND cust.customer_mst_pk = " + CustPK + "");
            strBuilder.Append(" ORDER BY TO_DATE(TRK.CREATED_ON, 'dd/MM/yyyy HH24:MI:SS') DESC, TRK.DOC_REF_NO ");

            return objWF.GetDataSet(strBuilder.ToString());
        }
        #endregion

        #region "Quotation Type"
        // Fetch Quotation Type
        /// <summary>
        /// Fetches the type of the quotation.
        /// </summary>
        /// <param name="qno">The qno.</param>
        /// <returns></returns>
        public int FetchQuotationType(string qno)
        {
            int qType = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL += "SELECT QAT.QUOTATION_TYPE QTYPE ";
            strSQL += "FROM QUOTATION_MST_TBL QAT";
            strSQL += "WHERE QAT.QUOTATION_REF_NO='" + qno + "'";
            qType = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
            return qType;

        }
        #endregion

        #region "GetLocation"
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
            try
            {
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
                while (dr.Read())
                {
                    strReturn += dr["LOCATION_MST_PK"] + "~$";
                }
                dr.Close();
                if (strReturn == "0~$")
                {
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
                    while (dr.Read())
                    {
                        strReturn += dr["LOCATION_MST_PK"] + "~$";
                    }
                    dr.Close();
                }

                strALL = strReturn;
                //1481~$1123~$'
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch All"
        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="BussinessType">Type of the bussiness.</param>
        /// <param name="Process">The process.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="Group">The group.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="POL">The pol.</param>
        /// <param name="POD">The pod.</param>
        /// <param name="ShippingLine">The shipping line.</param>
        /// <param name="VslVoy">The VSL voy.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="Sector">The sector.</param>
        /// <param name="ContUld">The cont uld.</param>
        /// <returns></returns>
        public DataSet FetchAll(string BussinessType, Int32 Process = 0, string FromDt = "", string ToDt = "", string Group = "", string CustomerID = "", string POL = "", string POD = "", string ShippingLine = "", string VslVoy = "",
        Int32 TotalPage = 0, Int32 CurrentPage = 0, string Sector = "", string ContUld = "")
        {
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder addStr = new StringBuilder();

            //add by latha for fetching location wise
            Int32 intLoc = default(Int32);
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            intLoc = (Int32)objPage.Session["LOGED_IN_LOC_FK"];
            if (BussinessType == "SI" | BussinessType == "AI")
            {
                addStr.Append(" AND ((UMT.DEFAULT_LOCATION_FK = " + intLoc);
                addStr.Append(" and JHDR.JC_AUTO_MANUAL = 0) OR ");
                addStr.Append(" (JHDR.PORT_MST_POD_FK IN ");
                addStr.Append(" (SELECT T.PORT_MST_FK ");
                addStr.Append(" FROM LOC_PORT_MAPPING_TRN T ");
                addStr.Append(" WHERE T.LOCATION_MST_FK =  " + intLoc + ")");
                addStr.Append(" and JHDR.JC_AUTO_MANUAL = 1))");
                addStr.Append(" AND JHDR.CREATED_BY_FK = UMT.USER_MST_PK ");
            }
            else
            {
                addStr.Append(" AND UMT.DEFAULT_LOCATION_FK = " + intLoc);
                addStr.Append(" AND JHDR.CREATED_BY_FK = UMT.USER_MST_PK ");
            }

            if (!string.IsNullOrEmpty(CustomerID) & CustomerID != "0")
            {
                addStr.Append(" AND  CMT.Customer_Mst_Pk in (" + CustomerID + ")");
            }

            if (!string.IsNullOrEmpty(ShippingLine))
            {
                if (BussinessType == "AE" | BussinessType == "AI")
                {
                    addStr.Append(" AND   AIR.AIRLINE_MST_PK=" + ShippingLine);
                }
                else if (BussinessType == "SI")
                {
                    addStr.Append(" AND  JHDR.CARRIER_MST_FK=" + ShippingLine);
                }
                else
                {
                    addStr.Append(" AND  BHDR.CARRIER_MST_FK=" + ShippingLine);
                }
            }

            if (!string.IsNullOrEmpty(VslVoy))
            {
                if (BussinessType == "SE" | BussinessType == "SI")
                {
                    //addStr.Append(" AND  (VVT.VESSEL_NAME || '/' || VTT.VOYAGE) in ('" + VslVoy + "')")
                    addStr.Append("  And VVT.VESSEL_ID = '" + VslVoy + "'");
                }
                else
                {
                    addStr.Append(" AND AIR.AIRLINE_ID in ('" + VslVoy + "')");
                }
            }

            if (!string.IsNullOrEmpty(Sector))
            {
                addStr.Append(Sector);
            }

            if (Group != "0")
            {
                addStr.Append(" AND JHDR.COMMODITY_GROUP_FK = " + Group);
            }
            //modified by thiyagaran on 16/12/08 
            if (!string.IsNullOrEmpty(Convert.ToString(getDefault(FromDt, ""))) & !string.IsNullOrEmpty(Convert.ToString(getDefault(ToDt, ""))))
            {
                addStr.Append(" AND to_char(JHDR.JOBCARD_DATE) BETWEEN TO_DATE('" + FromDt + "',DATEFORMAT) and to_date('" + ToDt + "',DATEFORMAT)");
            }
            else if (!string.IsNullOrEmpty(Convert.ToString(getDefault(FromDt, ""))))
            {
                addStr.Append(" AND to_char(JHDR.JOBCARD_DATE) >= TO_DATE('" + FromDt + "',DATEFORMAT) ");
            }
            else if (!string.IsNullOrEmpty(Convert.ToString(getDefault(ToDt, ""))))
            {
                addStr.Append(" AND to_char(JHDR.JOBCARD_DATE) <= TO_DATE('" + ToDt + "',DATEFORMAT)");
            }

            //'TotalRecords = GetTotalRecordCount(BussinessType, addStr.ToString)

            //'TotalPage = TotalRecords \ RecordsPerPage

            //'If TotalRecords Mod RecordsPerPage <> 0 Then
            //'    TotalPage += 1
            //'End If

            //'If CurrentPage > TotalPage Then
            //'    CurrentPage = 1
            //'End If

            //'If TotalRecords = 0 Then
            //'    CurrentPage = 0
            //'End If

            //'last = CurrentPage * RecordsPerPage
            //'start = (CurrentPage - 1) * RecordsPerPage + 1

            DataSet DS = null;
            WorkFlow objWF = new WorkFlow();
            //adding "ContUld" by thiyagarajan on 20/8/08 to filter the recs based container number also
            DS = objWF.GetDataSet(MainQuery(BussinessType, addStr.ToString(), start, last, ContUld));
            try
            {
                return DS;
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        #endregion

        #region "Main Query & Get total Record"
        /// <summary>
        /// Mains the query.
        /// </summary>
        /// <param name="bussinessType">Type of the bussiness.</param>
        /// <param name="addConditions">The add conditions.</param>
        /// <param name="startPage">The start page.</param>
        /// <param name="endPage">The end page.</param>
        /// <param name="CONTULD">The contuld.</param>
        /// <returns></returns>
        public string MainQuery(string bussinessType, string addConditions, Int32 startPage, Int32 endPage, string CONTULD = "")
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            if (bussinessType == "SE")
            {
                strQuery.Append("   SELECT MAIN.*" );
                strQuery.Append("   FROM (SELECT ROWNUM AS \"Sl. Nr.\", Q.* ");
                strQuery.Append("   FROM (SELECT * FROM  (SELECT DISTINCT JHDR.JOBCARD_REF_NO \"Document Nr.\"," );
                strQuery.Append("   CMT.CUSTOMER_NAME \"Cust.Name\"," );
                strQuery.Append("   POL.PORT_ID POL," );
                strQuery.Append("   POD.PORT_ID POD," );
                strQuery.Append("   OPMT.OPERATOR_ID \"Shipping Line\"," );
                strQuery.Append("   trim(VVT.VESSEL_ID) ||  decode(vtt.voyage,NULL,'', ' / ' || trim(VTT.Voyage)) \"Vsl/Voy\"" );

                strQuery.Append("   FROM JOB_CARD_TRN      JHDR," );
                strQuery.Append("   BOOKING_MST_TBL           BHDR," );
                strQuery.Append("   BOOKING_TRN        BTRN," );
                strQuery.Append("   CUSTOMER_MST_TBL          CMT," );
                strQuery.Append("   PORT_MST_TBL              POL," );
                strQuery.Append("   PORT_MST_TBL              POD," );
                strQuery.Append("   COMMODITY_GROUP_MST_TBL   COMM," );
                strQuery.Append("   operator_mst_tbl          opmt," );
                strQuery.Append("   VESSEL_VOYAGE_TBL         VVT," );
                //add by latha for fetching location wise
                strQuery.Append("   USER_MST_TBL     UMT," );
                strQuery.Append("   VESSEL_VOYAGE_TRN         VTT" );

                //implementing by thiyagarajan on 20/8/08 to filter the recs based container also
                if (CONTULD.Trim().Length > 0)
                {
                    strQuery.Append("   , JOB_TRN_CONT      JOBCONT " );
                }
                //end

                strQuery.Append("   WHERE BHDR.BOOKING_MST_PK = JHDR.BOOKING_MST_FK" );
                strQuery.Append("   AND BHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" );
                strQuery.Append("   AND BHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" );
                strQuery.Append("   AND BTRN.BOOKING_MST_FK(+) = BHDR.BOOKING_MST_PK" );
                strQuery.Append("   AND BHDR.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)" );
                strQuery.Append("   AND BHDR.CARRIER_MST_FK = OPMT.OPERATOR_MST_PK" );
                strQuery.Append("   AND JHDR.VOYAGE_TRN_FK = VTT.VOYAGE_TRN_PK(+)" );
                strQuery.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VTT.VESSEL_VOYAGE_TBL_FK" );
                strQuery.Append("   AND COMM.COMMODITY_GROUP_PK(+)=JHDR.COMMODITY_GROUP_FK " );
                strQuery.Append(addConditions);

                //implementing by thiyagarajan on 20/8/08 to filter the recs based container number also
                if (CONTULD.Trim().Length > 0)
                {
                    strQuery.Append("   AND JOBCONT.JOB_CARD_TRN_FK(+)=JHDR.JOB_CARD_TRN_PK " );
                    strQuery.Append("   AND JOBCONT.CONTAINER_NUMBER LIKE '%" + CONTULD.ToUpper() + "%' " );
                }
                //end

                strQuery.Append("   ORDER BY CMT.CUSTOMER_NAME DESC )");
                strQuery.Append("   ) Q) MAIN");
                //strQuery.Append("   WHERE ""Sl.Nr.""    BETWEEN " & startPage & " AND " & endPage & vbCrLf)

            }
            else if (bussinessType == "SI")
            {
                strQuery.Append("SELECT MAIN.*" );
                strQuery.Append("  FROM (SELECT ROWNUM \"Sl. Nr.\", Q.*" );
                strQuery.Append("          FROM (SELECT * FROM  (SELECT DISTINCT JHDR.JOBCARD_REF_NO \"Document Nr.\"," );
                strQuery.Append("                                CMT.CUSTOMER_NAME \"Cust.Name\"," );
                strQuery.Append("                                POL.PORT_ID POL," );
                strQuery.Append("                                POD.PORT_ID POD," );
                strQuery.Append("                                OPMT.OPERATOR_ID    \"Shipping Line\"," );
                strQuery.Append("                                trim(VVT.VESSEL_ID) ||  decode(vtt.voyage,NULL,'', ' / ' || trim(VTT.Voyage)) \"Vsl/Voy\"" );
                strQuery.Append("                  FROM ");
                strQuery.Append("                  JOB_CARD_TRN      JHDR," );
                strQuery.Append("                       CUSTOMER_MST_TBL          CMT," );
                strQuery.Append("                       PORT_MST_TBL              POL," );
                strQuery.Append("                       PORT_MST_TBL              POD," );
                strQuery.Append("                       COMMODITY_GROUP_MST_TBL   COMM," );
                strQuery.Append("                       OPERATOR_MST_TBL          OPMT," );
                strQuery.Append("                       VESSEL_VOYAGE_TBL         VVT," );
                //add by latha for fetching location wise
                strQuery.Append("   USER_MST_TBL     UMT," );
                strQuery.Append("                       VESSEL_VOYAGE_TRN         VTT" );

                //implementing by thiyagarajan on 20/8/08 to filter the recs based container number also
                if (CONTULD.Trim().Length > 0)
                {
                    strQuery.Append("   , JOB_TRN_CONT      JOBCONT " );
                }
                //end

                strQuery.Append("                 WHERE JHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" );
                strQuery.Append("                   AND JHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" );
                strQuery.Append("                   AND JHDR.CARRIER_MST_FK = OPMT.OPERATOR_MST_PK" );
                strQuery.Append("                   AND JHDR.VOYAGE_TRN_FK = VTT.VOYAGE_TRN_PK(+)" );
                strQuery.Append("                   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VTT.VESSEL_VOYAGE_TBL_FK" );
                strQuery.Append("                   AND JHDR.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)" );
                strQuery.Append("   AND COMM.COMMODITY_GROUP_PK(+)=JHDR.COMMODITY_GROUP_FK " );
                strQuery.Append(addConditions);

                //implementing by thiyagarajan on 20/8/08 to filter the recs based container number also
                if (CONTULD.Trim().Length > 0)
                {
                    strQuery.Append("   AND JOBCONT.JOB_CARD_TRN_FK(+)=JHDR.JOB_CARD_TRN_PK " );
                    strQuery.Append("   AND JOBCONT.CONTAINER_NUMBER LIKE '%" + CONTULD.ToUpper() + "%' " );
                }
                //end

                strQuery.Append("  ORDER BY CMT.CUSTOMER_NAME DESC )");
                strQuery.Append("   ) Q) MAIN");
                //strQuery.Append("   WHERE ""Sl.Nr.""    BETWEEN " & startPage & " AND " & endPage & vbCrLf)


            }
            else if (bussinessType == "AE")
            {
                strQuery.Append("   SELECT MAIN.*" );
                strQuery.Append("    FROM (SELECT ROWNUM AS \"Sl. Nr.\", Q.* ");
                strQuery.Append("   FROM (SELECT * FROM  (SELECT DISTINCT JHDR.JOBCARD_REF_NO \"Document Nr.\"," );
                strQuery.Append("                                CMT.CUSTOMER_NAME \"Cust.Name\"," );
                strQuery.Append("                                POL.PORT_ID POL," );
                strQuery.Append("                                POD.PORT_ID POD," );
                strQuery.Append("                                AIR.AIRLINE_ID \"Shipping Line\"," );
                strQuery.Append("                                JHDR.VOYAGE_FLIGHT_NO \"Vsl/Voy\"" );
                strQuery.Append("FROM");
                strQuery.Append("                       JOB_CARD_TRN JHDR," );
                strQuery.Append("                       BOOKING_MST_TBL      BHDR," );
                strQuery.Append("                       BOOKING_TRN      BTRN," );
                strQuery.Append("                       CUSTOMER_MST_TBL     CMT," );
                strQuery.Append("                       PORT_MST_TBL         POL," );
                strQuery.Append("                       PORT_MST_TBL         POD," );
                strQuery.Append("                       AIRLINE_MST_TBL      AIR," );
                //add by latha for fetching location wise
                strQuery.Append("   USER_MST_TBL     UMT," );
                // Added
                strQuery.Append("                       COMMODITY_GROUP_MST_TBL    COMM" );

                //implementing by thiyagarajan on 21/8/08 to filter the recs based container also
                if (CONTULD.Trim().Length > 0)
                {
                    strQuery.Append("   , JOB_TRN_CONT    JOBCONT " );
                }
                //end

                strQuery.Append("                 WHERE BHDR.BOOKING_MST_PK = JHDR.BOOKING_MST_FK" );
                strQuery.Append("                   AND BHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" );
                strQuery.Append("                   AND BHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" );
                strQuery.Append("                   AND BHDR.CARRIER_MST_FK = AIR.AIRLINE_MST_PK" );
                strQuery.Append("                   AND BTRN.BOOKING_MST_FK(+) = BHDR.BOOKING_MST_PK" );
                strQuery.Append("                   AND BHDR.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)" );
                strQuery.Append("                   AND COMM.COMMODITY_GROUP_PK(+)=JHDR.COMMODITY_GROUP_FK " );
                strQuery.Append(addConditions);

                //implementing by thiyagarajan on 21/8/08 to filter the recs based container number also
                if (CONTULD.Trim().Length > 0)
                {
                    strQuery.Append("   AND JOBCONT.JOB_CARD_TRN_FK(+)=JHDR.JOB_CARD_TRN_PK " );
                    strQuery.Append("   AND JOBCONT.ULD_NUMBER LIKE '%" + CONTULD.ToUpper() + "%' " );
                }
                //end

                strQuery.Append("   ORDER BY CMT.CUSTOMER_NAME DESC )");
                strQuery.Append("   ) Q) MAIN");
                //strQuery.Append("   WHERE ""Sl.Nr.""    BETWEEN " & startPage & " AND " & endPage & vbCrLf)


            }
            else if (bussinessType == "AI")
            {
                strQuery.Append("   SELECT MAIN.*" );
                strQuery.Append("    FROM (SELECT ROWNUM AS \"Sl. Nr.\", Q.* ");
                strQuery.Append("   FROM (SELECT * FROM  (SELECT DISTINCT JHDR.JOBCARD_REF_NO \"Document Nr.\"," );
                strQuery.Append("                                CMT.CUSTOMER_NAME     \"Cust.Name\"," );

                strQuery.Append("                                POL.PORT_ID         POL," );
                strQuery.Append("                                POD.PORT_ID         POD," );

                strQuery.Append("                                AIR.AIRLINE_ID \"Shipping Line\"," );
                strQuery.Append("                                JHDR.VOYAGE_FLIGHT_NO \"Vsl/Voy\"" );

                strQuery.Append("                  FROM JOB_CARD_TRN JHDR," );
                strQuery.Append("                       CUSTOMER_MST_TBL     CMT," );
                strQuery.Append("                       PORT_MST_TBL         POL," );
                strQuery.Append("                       PORT_MST_TBL         POD," );
                strQuery.Append("                       AIRLINE_MST_TBL      AIR," );
                //add by latha for fetching location wise
                strQuery.Append("   USER_MST_TBL     UMT," );
                strQuery.Append("                       COMMODITY_GROUP_MST_TBL    COMM" );
                //implementing by thiyagarajan on 21/8/08 to filter the recs based container also
                if (CONTULD.Trim().Length > 0)
                {
                    strQuery.Append("   , JOB_TRN_CONT    JOBCONT " );
                }
                //end
                strQuery.Append("                 WHERE JHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" );
                strQuery.Append("                   AND JHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" );
                strQuery.Append("                   AND JHDR.CARRIER_MST_FK = AIR.AIRLINE_MST_PK" );

                strQuery.Append("                   AND JHDR.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)" );
                strQuery.Append("                   AND COMM.COMMODITY_GROUP_PK(+) = JHDR.COMMODITY_GROUP_FK" );

                strQuery.Append(addConditions);
                //implementing by thiyagarajan on 21/8/08 to filter the recs based container number also
                if (CONTULD.Trim().Length > 0)
                {
                    strQuery.Append("   AND JOBCONT.JOB_CARD_TRN_FK(+) = JHDR.JOB_CARD_TRN_PK " );
                    strQuery.Append("   AND JOBCONT.ULD_NUMBER LIKE '%" + CONTULD.ToUpper() + "%' " );
                }
                //end
                strQuery.Append("  ORDER BY CMT.CUSTOMER_NAME DESC )");
                strQuery.Append("   ) Q) MAIN");
                //strQuery.Append("   WHERE ""Sl.Nr.""    BETWEEN " & startPage & " AND " & endPage & vbCrLf)


            }

            return strQuery.ToString();

        }

        /// <summary>
        /// Gets the total record count.
        /// </summary>
        /// <param name="bussinessType">Type of the bussiness.</param>
        /// <param name="strConditions">The string conditions.</param>
        /// <returns></returns>
        public int GetTotalRecordCount(string bussinessType, string strConditions)
        {

            StringBuilder strSqlRecordCound = new StringBuilder();

            Int32 totRecords = 0;
            WorkFlow objTotRecCount = new WorkFlow();
            if (bussinessType == "SE")
            {
                strSqlRecordCound.Append("select count(*)" );
                strSqlRecordCound.Append("   from (SELECT DISTINCT JHDR.JOBCARD_REF_NO ," );
                strSqlRecordCound.Append("   CMT.CUSTOMER_NAME ," );
                strSqlRecordCound.Append("   POL.PORT_ID POL," );
                strSqlRecordCound.Append("   POD.PORT_ID POD," );
                strSqlRecordCound.Append("   OPMT.OPERATOR_ID \"Shipping Line\"," );
                strSqlRecordCound.Append("   trim(VVT.VESSEL_ID) ||  decode(vtt.voyage,NULL,'', ' / ' || trim(VTT.Voyage)) \"Vsl/Voy\"" );
                strSqlRecordCound.Append("   FROM JOB_CARD_TRN      JHDR," );
                strSqlRecordCound.Append("   BOOKING_MST_TBL           BHDR," );
                strSqlRecordCound.Append("   BOOKING_TRN       BTRN," );
                strSqlRecordCound.Append("   CUSTOMER_MST_TBL          CMT," );
                strSqlRecordCound.Append("   PORT_MST_TBL              POL," );
                strSqlRecordCound.Append("   PORT_MST_TBL              POD," );
                strSqlRecordCound.Append("   COMMODITY_GROUP_MST_TBL   COMM," );
                strSqlRecordCound.Append("   operator_mst_tbl          opmt," );
                strSqlRecordCound.Append("   VESSEL_VOYAGE_TBL         VVT," );
                //add by latha for fetching location wise
                strSqlRecordCound.Append("   USER_MST_TBL     UMT," );
                strSqlRecordCound.Append("   VESSEL_VOYAGE_TRN         VTT" );
                strSqlRecordCound.Append("   WHERE BHDR.BOOKING_MST_PK = JHDR.BOOKING_MST_FK" );
                strSqlRecordCound.Append("   AND BHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" );
                strSqlRecordCound.Append("   AND BHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" );
                strSqlRecordCound.Append("   AND BTRN.BOOKING_MST_FK(+) = BHDR.BOOKING_MST_PK" );
                strSqlRecordCound.Append("   AND BHDR.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)" );
                strSqlRecordCound.Append("   AND BHDR.CARRIER_MST_FK = OPMT.OPERATOR_MST_PK" );
                strSqlRecordCound.Append("   AND JHDR.VOYAGE_TRN_FK = VTT.VOYAGE_TRN_PK(+)" );
                strSqlRecordCound.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VTT.VESSEL_VOYAGE_TBL_FK" );
                strSqlRecordCound.Append("   AND COMM.COMMODITY_GROUP_PK(+)=JHDR.COMMODITY_GROUP_FK " );
                strSqlRecordCound.Append(strConditions);
                strSqlRecordCound.Append("                 ORDER BY CMT.CUSTOMER_ID)" );

                totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSqlRecordCound.ToString()));


            }
            else if (bussinessType == "SI")
            {
                strSqlRecordCound.Append("select count(*)" );
                strSqlRecordCound.Append("  from (SELECT DISTINCT JHDR.JOBCARD_REF_NO," );
                strSqlRecordCound.Append("                        CMT.CUSTOMER_NAME," );
                strSqlRecordCound.Append("                        POL.PORT_ID POL," );
                strSqlRecordCound.Append("                        POD.PORT_ID POD," );
                strSqlRecordCound.Append("                                OPMT.OPERATOR_ID    \"Shipping Line\"," );
                strSqlRecordCound.Append("                                trim(VVT.VESSEL_ID) ||  decode(vtt.voyage,NULL,'', ' / ' || trim(VTT.Voyage)) \"Vsl/Voy\"" );
                strSqlRecordCound.Append("          FROM JOB_CARD_TRN    JHDR," );
                strSqlRecordCound.Append("               CUSTOMER_MST_TBL        CMT," );
                strSqlRecordCound.Append("               PORT_MST_TBL            POL," );
                strSqlRecordCound.Append("               PORT_MST_TBL            POD," );
                strSqlRecordCound.Append("               COMMODITY_GROUP_MST_TBL COMM," );
                strSqlRecordCound.Append("                       OPERATOR_MST_TBL          OPMT," );
                strSqlRecordCound.Append("                       VESSEL_VOYAGE_TBL         VVT," );
                //add by latha for fetching location wise
                strSqlRecordCound.Append("   USER_MST_TBL     UMT," );
                strSqlRecordCound.Append("                       VESSEL_VOYAGE_TRN         VTT" );
                strSqlRecordCound.Append("         WHERE JHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" );
                strSqlRecordCound.Append("           AND JHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" );
                strSqlRecordCound.Append("                   AND JHDR.CARRIER_MST_FK = OPMT.OPERATOR_MST_PK" );
                strSqlRecordCound.Append("                   AND JHDR.VOYAGE_TRN_FK = VTT.VOYAGE_TRN_PK(+)" );
                strSqlRecordCound.Append("                   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VTT.VESSEL_VOYAGE_TBL_FK" );
                strSqlRecordCound.Append("           AND JHDR.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)" );
                strSqlRecordCound.Append("           AND COMM.COMMODITY_GROUP_PK(+) = JHDR.COMMODITY_GROUP_FK  " );
                strSqlRecordCound.Append(strConditions);
                strSqlRecordCound.Append("         ORDER BY CMT.CUSTOMER_ID)" );

                totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSqlRecordCound.ToString()));


            }
            else if (bussinessType == "AE")
            {
                strSqlRecordCound.Append("SELECT COUNT(*)" );
                strSqlRecordCound.Append("  FROM (SELECT DISTINCT JHDR.JOBCARD_REF_NO ," );
                strSqlRecordCound.Append("                        CMT.CUSTOMER_NAME ," );
                strSqlRecordCound.Append("                        POL.PORT_ID AOO," );
                strSqlRecordCound.Append("                        POD.PORT_ID AOD," );
                strSqlRecordCound.Append("                        AIR.AIRLINE_ID \"Air Line\"," );
                strSqlRecordCound.Append("                        JHDR.VOYAGE_FLIGHT_NO \"Flight No.\"" );
                strSqlRecordCound.Append("          FROM " );
                strSqlRecordCound.Append("               JOB_CARD_TRN    JHDR," );
                strSqlRecordCound.Append("               BOOKING_MST_TBL         BHDR," );
                strSqlRecordCound.Append("               BOOKING_TRN         BTRN," );
                strSqlRecordCound.Append("               CUSTOMER_MST_TBL        CMT," );
                strSqlRecordCound.Append("               PORT_MST_TBL            POL," );
                strSqlRecordCound.Append("               PORT_MST_TBL            POD," );
                strSqlRecordCound.Append("               AIRLINE_MST_TBL         AIR," );
                //add by latha for fetching location wise
                strSqlRecordCound.Append("   USER_MST_TBL     UMT," );
                strSqlRecordCound.Append("               COMMODITY_GROUP_MST_TBL COMM" );
                strSqlRecordCound.Append("         WHERE BHDR.BOOKING_MST_PK = JHDR.BOOKING_MST_FK" );
                strSqlRecordCound.Append("           AND BHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" );
                strSqlRecordCound.Append("           AND BHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" );
                strSqlRecordCound.Append("           AND BHDR.CARRIER_MST_FK = AIR.AIRLINE_MST_PK" );
                strSqlRecordCound.Append("           AND BTRN.BOOKING_MST_FK(+) = BHDR.BOOKING_MST_PK" );
                strSqlRecordCound.Append("           AND BHDR.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)" );
                strSqlRecordCound.Append("           AND COMM.COMMODITY_GROUP_PK(+) = JHDR.COMMODITY_GROUP_FK   " );
                strSqlRecordCound.Append(strConditions);
                strSqlRecordCound.Append("         ORDER BY CMT.CUSTOMER_ID)" );

                totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSqlRecordCound.ToString()));


            }
            else if (bussinessType == "AI")
            {
                strSqlRecordCound.Append("select count(*) from (" );
                strSqlRecordCound.Append("SELECT DISTINCT JHDR.JOBCARD_REF_NO ," );
                strSqlRecordCound.Append("                CMT.CUSTOMER_NAME ," );
                strSqlRecordCound.Append("                POL.PORT_ID AOO," );
                strSqlRecordCound.Append("                POD.PORT_ID AOD," );
                strSqlRecordCound.Append("                AIR.AIRLINE_ID \"Air Line\"," );
                strSqlRecordCound.Append("                JHDR.VOYAGE_FLIGHT_NO \"Flight No.\"" );
                strSqlRecordCound.Append("  FROM JOB_CARD_TRN    JHDR," );
                strSqlRecordCound.Append("       CUSTOMER_MST_TBL        CMT," );
                strSqlRecordCound.Append("       PORT_MST_TBL            POL," );
                strSqlRecordCound.Append("       PORT_MST_TBL            POD," );
                strSqlRecordCound.Append("       AIRLINE_MST_TBL         AIR," );
                //add by latha for fetching location wise
                strSqlRecordCound.Append("   USER_MST_TBL     UMT," );
                strSqlRecordCound.Append("       COMMODITY_GROUP_MST_TBL COMM" );
                strSqlRecordCound.Append(" WHERE JHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" );
                strSqlRecordCound.Append("   AND JHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" );
                strSqlRecordCound.Append("   AND JHDR.CARRIER_MST_FK = AIR.AIRLINE_MST_PK" );
                strSqlRecordCound.Append("   AND JHDR.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)" );
                strSqlRecordCound.Append("   AND COMM.COMMODITY_GROUP_PK(+) = JHDR.COMMODITY_GROUP_FK   " );
                strSqlRecordCound.Append(strConditions);
                strSqlRecordCound.Append(" ORDER BY CMT.CUSTOMER_ID )" );

                totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSqlRecordCound.ToString()));

            }
            return totRecords;

        }
        #endregion

        #region " Fetch Job Card Fk's for Given Reference Number"

        //Fetch  Job Card Pks For Track And Trace Fetch
        /// <summary>
        /// Fetches the job PKS.
        /// </summary>
        /// <param name="strStatus">The string status.</param>
        /// <param name="process">The process.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="strRefNo">The string reference no.</param>
        /// <returns></returns>
        public DataSet FetchJobPks(string strStatus, int process, int BizType, string strRefNo = "0")
        {

            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;
                //.Add("REFNO_IN", strRefNo).Direction = ParameterDirection.Input
                _with1.Add("REFNO_IN", getDefault(strRefNo, DBNull.Value)).Direction = ParameterDirection.Input;
                _with1.Add("STATUS_IN", strStatus.ToUpper()).Direction = ParameterDirection.Input;
                _with1.Add("Proces", process).Direction = ParameterDirection.Input;
                _with1.Add("BizType", BizType).Direction = ParameterDirection.Input;
                _with1.Add("CurFetchData", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_TRACK_N_TRACE_PKG", "FETCH_TRACK_N_TRACE");
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Container Nr"
        //implementing by thiyagarajan on 20/8/08 to filter the recs based container number
        /// <summary>
        /// Fetches the cont uld nr.
        /// </summary>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="process">The process.</param>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="contRefNo">The cont reference no.</param>
        /// <returns></returns>
        public string FetchContUldNr(Int32 Biztype, int process, Int32 JobPk, string contRefNo = "")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            try
            {
                strsql.Append(" select JOBCONT.CONTAINER_NUMBER from  JOB_TRN_CONT JOBCONT ");
                strsql.Append(" WHERE JOBCONT.JOB_CARD_TRN_FK= " + JobPk + " AND JOBCONT.CONTAINER_NUMBER LIKE '%" + contRefNo.ToUpper() + "%' ");
                if (Biztype == 1)
                {
                    //strsql.Replace("SEA", "AIR")
                    strsql.Replace("CONTAINER_NUMBER", "ULD_NUMBER");
                }
                if (process == 2)
                {
                    //strsql.Replace("EXP", "IMP")
                }
                return objWF.ExecuteScaler(strsql.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Print Documents through Work Flow"
        /// <summary>
        /// Fetches the quotation pk.
        /// </summary>
        /// <param name="QNO">The qno.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public int FetchQuotationPK(string QNO, int BizType)
        {
            int Int_QPK = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (BizType == 1)
                {
                    strSQL += "SELECT QAT.QUOTATION_MST_PK QPK ";
                    strSQL += "FROM QUOTATION_MST_TBL QAT";
                    strSQL += "WHERE QAT.QUOTATION_REF_NO='" + QNO + "'";
                }
                else
                {
                    strSQL += "SELECT QAT.QUOTATION_MST_PK QTYPE ";
                    strSQL += "FROM QUOTATION_MST_TBL QAT";
                    strSQL += "WHERE QAT.QUOTATION_REF_NO='" + QNO + "'";
                }

                Int_QPK = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
                return Int_QPK;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        /// <summary>
        /// Fetches the booking pk.
        /// </summary>
        /// <param name="BNo">The b no.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public int FetchBookingPK(string BNo, int BizType, int process)
        {
            int Int_BookPK = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (BizType == 1)
                {
                    strSQL += "SELECT BAT.BOOKING_MST_PK BPK ";
                    strSQL += "FROM BOOKING_MST_TBL BAT";
                    strSQL += "WHERE BAT.BOOKING_REF_NO='" + BNo + "'";
                }
                else
                {
                    strSQL += "SELECT BAT.BOOKING_MST_PK BPK ";
                    strSQL += "FROM  BOOKING_MST_TBL BAT";
                    strSQL += "WHERE BAT.BOOKING_REF_NO='" + BNo + "'";
                }

                Int_BookPK = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
                return Int_BookPK;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the HBLPK.
        /// </summary>
        /// <param name="HBLNr">The HBL nr.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public int FetchHBLPK(string HBLNr, int BizType, int process)
        {
            int Int_HBLPK = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT H.HBL_EXP_TBL_PK HPK ";
                strSQL += "FROM HBL_EXP_TBL H";
                strSQL += "WHERE H.HBL_REF_NO='" + HBLNr + "'";
                Int_HBLPK = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
                return Int_HBLPK;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the hawbpk.
        /// </summary>
        /// <param name="HAWBNr">The hawb nr.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public int FetchHAWBPK(string HAWBNr, int BizType, int process)
        {
            int Int_HAWBPK = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT  H.HAWB_EXP_TBL_PK HPK ";
                strSQL += "FROM HAWB_EXP_TBL H";
                strSQL += "WHERE H.HAWB_REF_NO='" + HAWBNr + "'";
                Int_HAWBPK = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
                return Int_HAWBPK;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the MBLPK.
        /// </summary>
        /// <param name="MBLNo">The MBL no.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public int FetchMBLPK(string MBLNo, int BizType, int process)
        {
            int Int_MBLPK = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT M.MBL_EXP_TBL_PK MPK ";
                strSQL += "FROM MBL_EXP_TBL M";
                strSQL += "WHERE M.MBL_REF_NO='" + MBLNo + "'";
                Int_MBLPK = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
                return Int_MBLPK;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the discharge pk.
        /// </summary>
        /// <param name="DischargeNo">The discharge no.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public DataSet FetchDischargePK(string DischargeNo, int BizType, int process)
        {
            int Int_DischargePK = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (BizType == 2)
                {
                    strSQL += "SELECT DOD.DELIVERY_ORDER_DTL_PK";
                    strSQL += "FROM  DELIVERY_ORDER_MST_TBL D, JOB_CARD_TRN J , DELIVERY_ORDER_DTL_TBL DOD ";
                    strSQL += "WHERE D.DELIVERY_ORDER_REF_NO='" + DischargeNo + "'";
                    strSQL += "AND J.JOB_CARD_TRN_PK = D.JOB_CARD_MST_FK";
                    strSQL += "AND D.DELIVERY_ORDER_PK = DOD.DELIVERY_ORDER_FK";
                    return objWF.GetDataSet(strSQL.ToString());

                }
                else if (BizType == 1)
                {
                    strSQL += "SELECT  DOD.DELIVERY_ORDER_DTL_PK ";
                    strSQL += "FROM DELIVERY_ORDER_MST_TBL D, JOB_CARD_TRN JA,DELIVERY_ORDER_DTL_TBL DOD  ";
                    strSQL += "WHERE D.DELIVERY_ORDER_REF_NO='" + DischargeNo + "'";
                    strSQL += "AND JA.JOB_CARD_TRN_PK = D.JOB_CARD_AIR_MST_FK";
                    strSQL += "AND DOD.DELIVERY_ORDER_FK = D.DELIVERY_ORDER_PK";

                    return objWF.GetDataSet(strSQL.ToString());
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new DataSet();
        }
        /// <summary>
        /// Fetches the mawbpk.
        /// </summary>
        /// <param name="MAWBNo">The mawb no.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public int FetchMAWBPK(string MAWBNo, int BizType, int process)
        {
            int Int_MAWBPK = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT M.MAWB_EXP_TBL_PK MPK ";
                strSQL += "FROM MAWB_EXP_TBL M";
                strSQL += "WHERE M.MAWB_REF_NO='" + MAWBNo + "'";
                Int_MAWBPK = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
                return Int_MAWBPK;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the amt tx.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <returns></returns>
        public DataSet FetchAmtTx(int PK)
        {
            int AmtTax = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT SUM(CITT.AMT_IN_INV_CURR) INV_AMT, SUM(CITT.TAX_AMT) TAX_AMT ";
                strSQL += "FROM CONSOL_INVOICE_TBL CIT, CONSOL_INVOICE_TRN_TBL CITT";
                strSQL += "WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK";
                strSQL += "AND CIT.CONSOL_INVOICE_PK = " + PK;
                //AmtTax = CType(objWF.ExecuteScaler(strSQL.ToString()), Integer)
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the other det.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <returns></returns>
        public DataSet FetchOtherDet(int PK)
        {
            int otherdet = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT INVOICE_REF_NO,CIT.DISCOUNT_AMT,CIT.NET_RECEIVABLE,CIT.INVOICE_DATE,CIT.INV_UNIQUE_REF_NR,CITT.CUSTOMER_NAME,CIT.AIF, CIT.BANK_MST_FK ";
                strSQL += "FROM CONSOL_INVOICE_TBL CIT,CUSTOMER_MST_TBL CITT";
                strSQL += "WHERE CONSOL_INVOICE_PK = " + PK;
                strSQL += "and CIT.CUSTOMER_MST_FK =  CITT.CUSTOMER_MST_PK";

                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region "Function to check whether a user is an administrator or not"
        //Fetch Invoice Type
        /// <summary>
        /// Fetches the type of the invoice.
        /// </summary>
        /// <param name="InvNo">The inv no.</param>
        /// <returns></returns>
        public int FetchInvoiceType(string InvNo)
        {
            int InvType = 0;
            string strsQL = null;
            WorkFlow objwf = new WorkFlow();
            strsQL += "SELECT inv.inv_type ITYPE ";
            strsQL += "FROM consol_invoice_tbl inv";
            strsQL += "WHERE inv.invoice_ref_no='" + InvNo + "'";
            InvType = Convert.ToInt32(objwf.ExecuteScaler(strsQL.ToString()));
            return InvType;
        }

        /// <summary>
        /// Fetches the invoice pk.
        /// </summary>
        /// <param name="invno">The invno.</param>
        /// <param name="biztype">The biztype.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public int FetchInvoicePK(string invno, int biztype, int process)
        {
            int Int_invPK = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT i.CONSOL_INVOICE_PK iPK ";
                strSQL += "FROM consol_invoice_tbl i";
                strSQL += "WHERE i.INVOICE_REF_NO='" + invno + "'";
                strSQL += "AND i.BUSINESS_TYPE='" + biztype + "'";
                strSQL += "AND i.PROCESS_TYPE='" + process + "'";
                Int_invPK = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
                return Int_invPK;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the sup details.
        /// </summary>
        /// <param name="invno">The invno.</param>
        /// <param name="biztype">The biztype.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public DataSet FetchSupDetails(string invno, int biztype, int process)
        {
            int Int_invPK = 0;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT INV.INV_SUPPLIER_PK,");
                sb.Append("       INV.PROCESS_TYPE,");
                sb.Append("       INV.BUSINESS_TYPE,");
                sb.Append("       INVTRN.FLAG,");
                sb.Append("       INV.VENDOR_MST_FK,");
                sb.Append("       INV.CURRENCY_MST_FK,");
                sb.Append("       INV.INVOICE_DATE,");
                sb.Append("       INV.SUPPLIER_DUE_DT,");
                sb.Append("       INV.REMARKS");
                sb.Append("  FROM INV_SUPPLIER_TBL     INV,");
                sb.Append("       INV_SUPPLIER_TRN_TBL INVTRN,");
                sb.Append("       JOB_CARD_TRN JOB,");
                sb.Append("       JOB_TRN_COST JOB_COST");
                sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JOB_COST.JOB_CARD_TRN_FK");
                sb.Append("   AND INV.INV_SUPPLIER_PK = INVTRN.INV_SUPPLIER_TBL_FK");
                sb.Append("   AND INVTRN.JOB_TRN_EST_FK = JOB_COST.JOB_TRN_COST_PK");
                sb.Append("  AND INV.INVOICE_REF_NO = '" + invno + "'");
                if (biztype == 1)
                {
                    sb.Replace("SEA", "AIR");
                }
                if (process == 2)
                {
                    sb.Replace("EXP", "IMP");
                }
                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the pay details.
        /// </summary>
        /// <param name="PaymentNr">The payment nr.</param>
        /// <returns></returns>
        public DataSet FetchPayDetails(string PaymentNr)
        {
            int Int_invPK = 0;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT PMT.PAYMENT_TBL_PK,");
                sb.Append("       PMT.VENDOR_MST_FK,");
                sb.Append("       PMT.CURRENCY_MST_FK,");
                sb.Append("       PMT.PAYMENT_DATE,");
                sb.Append("       PMT.REMARKS ");
                sb.Append("  FROM PAYMENTS_TBL PMT");
                sb.Append(" WHERE PMT.PAYMENT_REF_NO ='" + PaymentNr + "'");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Gets the type of the cargo.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="biztype">The biztype.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public int GetCargoType(string JobPK, int biztype, int process)
        {
            int Int_invPK = 0;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (process == 1)
                {
                    sb.Append("SELECT BKG.CARGO_TYPE");
                    sb.Append("  FROM JOB_CARD_TRN JOB, BOOKING_MST_TBL BKG");
                    sb.Append(" WHERE BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK");
                    sb.Append("   AND JOB.JOB_CARD_TRN_PK = " + JobPK);
                }
                else
                {
                    if (biztype == 2)
                    {
                        sb.Append("SELECT JOB.CARGO_TYPE");
                        sb.Append("  FROM JOB_CARD_TRN JOB");
                        sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = " + JobPK);
                    }
                    else
                    {
                        sb.Append("SELECT 1 CARGO_TYPE");
                        sb.Append("  FROM JOB_CARD_TRN JOB");
                        sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = " + JobPK);
                    }
                }
                //If biztype = 1 Then
                //    sb.Replace("SEA", "AIR")
                //End If
                //If process = 2 Then
                //    sb.Replace("EXP", "IMP")
                //End If
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the invoice agent pk.
        /// </summary>
        /// <param name="invno">The invno.</param>
        /// <param name="biztype">The biztype.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public DataSet FetchInvoiceAgentPK(string invno, int biztype, int process)
        {
            int InvType = 0;
            string strsQL = null;
            WorkFlow objwf = new WorkFlow();
            if (biztype == 2)
            {
                if (process == 1)
                {
                    strsQL += "SELECT INV.INV_AGENT_PK  INVAGENTPK,";
                    strsQL += "INV.CB_DP_LOAD_AGENT  AGENTTYPE";
                    strsQL += "FROM INV_AGENT_TBL INV ";
                }
                else
                {
                    strsQL += "SELECT INV.INV_AGENT_PK  INVAGENTPK,";
                    strsQL += "INV.CB_DP_LOAD_AGENT  AGENTTYPE";
                    strsQL += "FROM INV_AGENT_TBL INV ";
                }
            }
            else
            {
                if (process == 1)
                {
                    strsQL += "SELECT INV.INV_AGENT_PK  INVAGENTPK,";
                    strsQL += "INV.CB_DP_LOAD_AGENT  AGENTTYPE";
                    strsQL += "FROM INV_AGENT_TBL INV ";
                }
                else
                {
                    strsQL += "SELECT INV.INV_AGENT_PK  INVAGENTPK,";
                    strsQL += "INV.CB_DP_LOAD_AGENT  AGENTTYPE";
                    strsQL += "FROM INV_AGENT_TBL INV ";
                }
            }
            strsQL += "WHERE INV.INVOICE_REF_NO='" + invno + "'";
            return (DataSet)objwf.GetDataSet(strsQL.ToString());
        }

        /// <summary>
        /// Fetches the credit note agent pk.
        /// </summary>
        /// <param name="CRNNr">The CRN nr.</param>
        /// <param name="biztype">The biztype.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public DataSet FetchCreditNoteAgentPK(string CRNNr, int biztype, int process)
        {
            int InvType = 0;
            string strsQL = null;
            WorkFlow objwf = new WorkFlow();
            if (biztype == 2)
            {
                if (process == 1)
                {
                    strsQL += "SELECT T.CR_AGENT_PK  CRNAGENTPK,";
                    strsQL += "T.CREDIT_NOTE_DATE CRDATE,";
                    strsQL += "T.CB_DP_LOAD_AGENT  AGENTTYPE";
                    strsQL += "FROM CR_AGENT_TBL T ";
                }
                else
                {
                    strsQL += "SELECT T.CR_AGENT_PK  CRNAGENTPK,";
                    strsQL += "T.CREDIT_NOTE_DATE CRDATE,";
                    strsQL += "T.CB_DP_LOAD_AGENT  AGENTTYPE";
                    strsQL += "FROM CR_AGENT_TBL T ";
                }
            }
            else
            {
                if (process == 1)
                {
                    strsQL += "SELECT T.CR_AGENT_PK  CRNAGENTPK,";
                    strsQL += "T.CREDIT_NOTE_DATE CRDATE,";
                    strsQL += "T.CB_DP_LOAD_AGENT  AGENTTYPE";
                    strsQL += "FROM CR_AGENT_TBL. T ";
                }
                else
                {
                    strsQL += "SELECT T.CR_AGENT_PK  CRNAGENTPK,";
                    strsQL += "T.CREDIT_NOTE_DATE CRDATE,";
                    strsQL += "T.CB_DP_LOAD_AGENT  AGENTTYPE";
                    strsQL += "FROM CR_AGENT_TBL T ";
                }
            }
            strsQL += "WHERE T.CREDIT_NOTE_REF_NO='" + CRNNr + "'";
            return (DataSet)objwf.GetDataSet(strsQL.ToString());
        }
        #endregion

        /// <summary>
        /// Fetches the collection pk.
        /// </summary>
        /// <param name="collno">The collno.</param>
        /// <param name="biztype">The biztype.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public int FetchCollectionPK(string collno, int biztype, int process)
        {
            int Int_collPK = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT C.COLLECTIONS_TBL_PK CPK ";
                strSQL += "FROM COLLECTIONS_TBL C";
                strSQL += "WHERE C.COLLECTIONS_REF_NO='" + collno + "'";
                strSQL += "AND C.BUSINESS_TYPE='" + biztype + "'";
                strSQL += "AND C.PROCESS_TYPE='" + process + "'";
                Int_collPK = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
                return Int_collPK;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the can pk.
        /// </summary>
        /// <param name="CanNo">The can no.</param>
        /// <param name="biztype">The biztype.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public int FetchCanPK(string CanNo, int biztype, int process)
        {
            int Int_jobCardfK = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += " SELECT C.JOB_CARD_FK FROM CAN_MST_TBL C";
                strSQL += " WHERE C.CAN_REF_NO ='" + CanNo + "'";
                Int_jobCardfK = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
                return Int_jobCardfK;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the jobpk.
        /// </summary>
        /// <param name="JobCardNo">The job card no.</param>
        /// <param name="biztype">The biztype.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public int FetchJOBPK(string JobCardNo, int biztype, int process)
        {
            int Int_jobCardfK = 0;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT J.JOB_CARD_TRN_PK  ";
                strSQL += "FROM JOB_CARD_TRN J";
                strSQL += "WHERE  J.JOBCARD_REF_NO='" + JobCardNo + "'";
                Int_jobCardfK = Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
                return Int_jobCardfK;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the quotation details.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <returns></returns>
        public DataSet FetchQuotationDetails(int PK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT CMT.CUSTOMER_NAME,";
                strSQL += "CMT.CUSTOMER_ID,";
                strSQL += "CMT.CUSTOMER_MST_PK,";
                strSQL += "Q.QUOTATION_REF_NO,";
                strSQL += "Q.QUOTATION_DATE,";
                strSQL += "(Q.QUOTATION_DATE + Q.VALID_FOR) QUOTE_TILLDATE,";
                strSQL += "Q.CARGO_TYPE,QDT.PORT_MST_PLR_FK,QDT.PORT_MST_PFD_FK ";
                strSQL += " FROM QUOTATION_MST_TBL Q, CUSTOMER_MST_TBL CMT,QUOTATION_DTL_TBL QDT";
                strSQL += " WHERE Q.QUOTATION_MST_PK = " + PK;
                strSQL += " AND CMT.CUSTOMER_MST_PK = Q.CUSTOMER_MST_FK ";
                strSQL += " AND QDT.QUOTATION_MST_FK = Q.QUOTATION_MST_PK ";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the delivery details.
        /// </summary>
        /// <param name="PKs">The p ks.</param>
        /// <returns></returns>
        public DataSet FetchDeliveryDetails(string PKs)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += " SELECT DISTINCT D.CARGO_TYPE, D.DELIVERY_ORDER_PK ";
                strSQL += " FROM DELIVERY_ORDER_MST_TBL D, DELIVERY_ORDER_DTL_TBL DOD ";
                strSQL += " WHERE DOD.DELIVERY_ORDER_DTL_PK IN (" + PKs + " )";

                strSQL += " AND D.DELIVERY_ORDER_PK = DOD.DELIVERY_ORDER_FK";

                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the HBL details.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <returns></returns>
        public DataSet FetchHBLDetails(int PK)
        {
            string strSql = null;
            WorkFlow objWf = new WorkFlow();
            try
            {
                strSql += "SELECT H.HBL_STATUS,H.CARGO_MOVE, JCSET.JOBCARD_REF_NO , ";
                strSql += "BST.CARGO_TYPE, H.PYMT_TYPE, H.IS_TO_ORDER";
                strSql += "FROM HBL_EXP_TBL H , JOB_CARD_TRN JCSET, BOOKING_MST_TBL BST";
                strSql += "WHERE (JCSET.JOB_CARD_TRN_PK = H.JOB_CARD_SEA_EXP_FK OR H.NEW_JOB_CARD_SEA_EXP_FK = JCSET.JOB_CARD_TRN_PK) ";
                strSql += "AND BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK";
                strSql += "AND H.HBL_EXP_TBL_PK = " + PK;
                return (DataSet)objWf.GetDataSet(strSql.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the booking sea details.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <returns></returns>
        public DataSet FetchBookingSeaDetails(int PK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT S.STATUS,S.BOOKING_REF_NO";
                strSQL += "FROM BOOKING_MST_TBL S";
                strSQL += "WHERE S.BOOKING_MST_PK = " + PK;

                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the booking air details.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <returns></returns>
        public DataSet FetchBookingAirDetails(int PK)
        {
            return FetchBookingSeaDetails(PK);
        }

        /// <summary>
        /// Fetches the quotation details air.
        /// </summary>
        /// <param name="PK">The pk.</param>
        /// <returns></returns>
        public DataSet FetchQuotationDetailsAir(int PK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT CMT.CUSTOMER_MST_PK,";
                strSQL += "CMT.CUSTOMER_NAME,";
                strSQL += "Q.QUOTATION_REF_NO,";
                strSQL += "Q.QUOTATION_DATE,";
                strSQL += "(Q.QUOTATION_DATE + Q.VALID_FOR) QUOTE_TILLDATE";
                strSQL += "FROM QUOTATION_MST_TBL Q, CUSTOMER_MST_TBL CMT";
                strSQL += "WHERE Q.QUOTATION_MST_PK = " + PK;
                strSQL += "AND CMT.CUSTOMER_MST_PK = Q.CUSTOMER_MST_FK";

                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        #endregion

        #region "Save Print Document"
        /// <summary>
        /// Fetches the storage.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchStorage()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='BOOKING (SEA)'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        /// <summary>
        /// Fetches the cr note to cb.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCrNoteToCB()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='AGENT CREDIT NOTE SEA EXPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the cr note to cb air exp.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCrNoteToCBAirExp()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='AGENT CREDIT NOTE AIR EXPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the inv details.
        /// </summary>
        /// <param name="biz">The biz.</param>
        /// <param name="process">The process.</param>
        /// <returns></returns>
        public DataSet FetchInvDetails(int biz, int process)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (biz == 1 & process == 1)
                {
                    strSQL += "SELECT P.APPLY_STORAGE";
                    strSQL += "FROM PROTOCOL_MST_TBL P";
                    strSQL += "WHERE PROTOCOL_NAME='CUSTOMER INVOICE AIR EXPORTS'";
                    return (DataSet)objWF.GetDataSet(strSQL.ToString());
                }
                else if (biz == 1 & process == 2)
                {
                    strSQL += "SELECT P.APPLY_STORAGE";
                    strSQL += "FROM PROTOCOL_MST_TBL P";
                    strSQL += "WHERE PROTOCOL_NAME='CUSTOMER INVOICE AIR IMPORTS'";
                    return (DataSet)objWF.GetDataSet(strSQL.ToString());
                }
                else if (biz == 2 & process == 1)
                {
                    strSQL += "SELECT P.APPLY_STORAGE";
                    strSQL += "FROM PROTOCOL_MST_TBL P";
                    strSQL += "WHERE PROTOCOL_NAME='CUSTOMER INVOICE SEA EXPORTS'";
                    return (DataSet)objWF.GetDataSet(strSQL.ToString());
                }
                else if (biz == 2 & process == 2)
                {
                    strSQL += "SELECT P.APPLY_STORAGE";
                    strSQL += "FROM PROTOCOL_MST_TBL P";
                    strSQL += "WHERE PROTOCOL_NAME='CUSTOMER INVOICE SEA IMPORTS'";
                    return (DataSet)objWF.GetDataSet(strSQL.ToString());
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the credit note to cb air imp.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCreditNoteToCBAirImp()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='AGENT CREDIT NOTE AIR IMPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the credit note to shipper.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCreditNoteToShipper()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='CUSTOMER CREDIT NOTE SEA EXPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the credit note to shipper air exp.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCreditNoteToShipperAirExp()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='CUSTOMER CREDIT NOTE AIR EXPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the credit note.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCreditNote()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='Credit Note'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the ex sea cn.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchExSeaCN()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='CUSTOMER CREDIT NOTE SEA EXPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the imp sea cn.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchImpSeaCN()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='CUSTOMER CREDIT NOTE SEA IMPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the ex air cn.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchExAirCN()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='CUSTOMER CREDIT NOTE AIR EXPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the imp air cn.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchImpAirCN()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='CUSTOMER CREDIT NOTE AIR IMPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the trans note air imp.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchTransNoteAirImp()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='TRANSPORT INSTRUCTION AIR IMPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the trans note air exp.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchTransNoteAirExp()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='TRANSPORT INSTRUCTION AIR EXPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the trans note imp.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchTransNoteImp()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='TRANSPORT INSTRUCTION SEA IMPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the trans note exp.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchTransNoteExp()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='TRANSPORT INSTRUCTION SEA EXPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the storage quote sea.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchStorageQuoteSea()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='QUOTATION (SEA)'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the storage MBL.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchStorageMBL()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='QUOTATION (SEA)'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the booking air.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchBookingAir()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='BOOKING (AIR)'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the storage col.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchStorageCol()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='COLLECTIONS'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the storage hawb.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchStorageHAWB()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='HAWB EXPORTS'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the quotation air.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchQuotationAir()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='QUOTATION (AIR)'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the storage HBL.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchStorageHBL()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='HBL'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the mawb.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchMAWB()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='HBL'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the MBL value.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchMblVal()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='HBL'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// Fetches the inv to cb agent sea exp.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchInvToCBAgentSeaExp()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='AGENT INVOICE SEA EXPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the inv to cb agent sea imp.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchInvToCBAgentSeaImp()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='AGENT INVOICE SEA IMPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        /// <summary>
        /// Fetches the inv to cb agent air exp.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchInvToCBAgentAirExp()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='AGENT INVOICE AIR EXPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the inv to cb agent air imp.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchInvToCBAgentAirImp()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='AGENT INVOICE AIR IMPORT'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the agent collections.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchAgentCollections()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='COLLECTIONS'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the HBL.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchHBL()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL += "SELECT P.APPLY_STORAGE";
                strSQL += "FROM PROTOCOL_MST_TBL P";
                strSQL += "WHERE PROTOCOL_NAME='HBL'";
                return (DataSet)objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the can.
        /// </summary>
        /// <param name="Biz">The biz.</param>
        /// <returns></returns>
        public DataSet FetchCAN(int Biz)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (Biz == 1 | Biz == 3)
                {
                    strSQL += "SELECT P.APPLY_STORAGE";
                    strSQL += "FROM PROTOCOL_MST_TBL P";
                    strSQL += "WHERE PROTOCOL_NAME='CAN (AIR)'";
                    return (DataSet)objWF.GetDataSet(strSQL.ToString());
                }
                else if (Biz == 2 | Biz == 3)
                {
                    strSQL += "SELECT P.APPLY_STORAGE";
                    strSQL += "FROM PROTOCOL_MST_TBL P";
                    strSQL += "WHERE PROTOCOL_NAME='CAN (SEA)'";
                    return (DataSet)objWF.GetDataSet(strSQL.ToString());
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the delivery order.
        /// </summary>
        /// <param name="DOval">The d oval.</param>
        /// <returns></returns>
        public DataSet FetchDeliveryOrder(int DOval)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (DOval == 2)
                {
                    strSQL += "SELECT P.APPLY_STORAGE";
                    strSQL += "FROM PROTOCOL_MST_TBL P";
                    strSQL += "WHERE PROTOCOL_NAME='DO (SEA)'";
                    return (DataSet)objWF.GetDataSet(strSQL.ToString());
                }
                else if (DOval == 1)
                {
                    strSQL += "SELECT P.APPLY_STORAGE";
                    strSQL += "FROM PROTOCOL_MST_TBL P";
                    strSQL += "WHERE PROTOCOL_NAME='DO (AIR)'";
                    return (DataSet)objWF.GetDataSet(strSQL.ToString());
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new DataSet();
        }
        #endregion

        #region "Function to check whether a user is an administrator or not"
        /// <summary>
        /// Determines whether this instance is administrator.
        /// </summary>
        /// <returns></returns>
        public int IsAdministrator()
        {
            string strSQL = null;
            Int16 Admin = default(Int16);
            WorkFlow objWF = new WorkFlow();

            strSQL = "SELECT COUNT(*) FROM USER_MST_TBL U,ROLE_MST_TBL R ";
            strSQL = strSQL + "  WHERE  U.ROLE_MST_FK=R.ROLE_MST_TBL_PK AND R.ROLE_ID = 'ADM' AND U.IS_ACTIVATED=1 ";
            strSQL = strSQL + " AND U.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"];
            try
            {
                Admin = Convert.ToInt16(objWF.ExecuteScaler(strSQL.ToString()));
                return Admin;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        /// <summary>
        /// Determines whether this instance is customer.
        /// </summary>
        /// <returns></returns>
        public int IsCustomer()
        {
            string strSQL = null;
            Int16 Admin = default(Int16);
            WorkFlow objWF = new WorkFlow();

            strSQL = " SELECT NVL(U.CUSTOMER_MST_FK,0) CUSTOMER_MST_FK FROM USER_MST_TBL U ";
            strSQL = strSQL + " WHERE U.USER_MST_PK = " + HttpContext.Current.Session["USER_PK"];
            try
            {
                Admin = Convert.ToInt16(objWF.ExecuteScaler(strSQL.ToString()));
                return Admin;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion

        #region "Fetch Tracking"
        /// <summary>
        /// Fetches the tracking.
        /// </summary>
        /// <param name="DOCREFNR">The docrefnr.</param>
        /// <param name="JOBPKS">The jobpks.</param>
        /// <param name="JOB_NR">The jo b_ nr.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchTracking(string DOCREFNR, string JOBPKS, string JOB_NR, int Process)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            try
            {
                if (DOCREFNR != "-1")
                {
                    sb.Append("SELECT *");
                    sb.Append("  FROM (SELECT DISTINCT TT.JOB_CARD_FK,");
                    sb.Append("                        BK.BOOKING_REF_NO,");
                    sb.Append("                        JE.JOBCARD_REF_NO JOB_CARD_NR,");
                    //sb.Append("                        TT.STATUS,")
                    sb.Append(" CASE WHEN JI.JOB_CARD_TRN_PK IS NOT NULL THEN");
                    sb.Append("                           GET_IMPORT_STATUS(JI.JOB_CARD_TRN_PK)");
                    sb.Append("                          ELSE GET_IMPORT_STATUS(TT.JOB_CARD_FK) END STATUS,");
                    //sb.Append("                        MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)) TT_DT,")
                    sb.Append("                        TO_CHAR(MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)),DATEFORMAT) TT_DT,");
                    sb.Append("                        MAX(TO_CHAR(TT.CREATED_ON, 'HH24:MI')) TT_TM,");
                    sb.Append("                        PORTPOL.PORT_ID POL,");
                    sb.Append("                        PORTPOD.PORT_ID POD,");
                    sb.Append("                        OP.OPERATOR_NAME CARRIER,");
                    sb.Append("                        (VM.VESSEL_ID || '/' || VT.VOYAGE) VSL_VOY,");
                    sb.Append("                        TO_CHAR(JE.ETD_DATE, DATEFORMAT) ETD_DATE,");
                    sb.Append("                        TO_CHAR(JE.ETA_DATE, DATEFORMAT) ETA_DATE,");
                    sb.Append("                        DECODE(BK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                    sb.Append("                        CG.COMMODITY_GROUP_CODE COMM_GRP,");
                    sb.Append("                        '' BTNCLK,");
                    sb.Append("                        NULL CREATED_ON");
                    sb.Append("          FROM TRACK_N_TRACE_TBL       TT,");
                    sb.Append("               JOB_CARD_TRN    JE,");
                    sb.Append("               JOB_CARD_TRN          JI,");
                    sb.Append("               BOOKING_MST_TBL         BK,");
                    sb.Append("               PORT_MST_TBL            PORTPOL,");
                    sb.Append("               PORT_MST_TBL            PORTPOD,");
                    sb.Append("               COMMODITY_GROUP_MST_TBL CG,");
                    sb.Append("               VESSEL_VOYAGE_TBL       VM,");
                    sb.Append("               VESSEL_VOYAGE_TRN       VT,");
                    sb.Append("               OPERATOR_MST_TBL        OP");
                    sb.Append("         WHERE JE.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND JE.JOBCARD_REF_NO = JI.JOBCARD_REF_NO(+) ");
                    sb.Append("           AND BK.BOOKING_MST_PK = JE.BOOKING_MST_FK");
                    sb.Append("           AND PORTPOL.PORT_MST_PK = BK.PORT_MST_POL_FK");
                    sb.Append("           AND PORTPOD.PORT_MST_PK = BK.PORT_MST_POD_FK");
                    sb.Append("           AND CG.COMMODITY_GROUP_PK = BK.COMMODITY_GROUP_FK");
                    sb.Append("           AND VM.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                    sb.Append("           AND VT.VOYAGE_TRN_PK(+) = JE.VOYAGE_TRN_FK");
                    sb.Append("           AND OP.OPERATOR_MST_PK(+) = VM.OPERATOR_MST_FK");
                    sb.Append("           AND JE.BUSINESS_TYPE = 2 ");
                    sb.Append("           AND TT.JOB_CARD_FK IN (" + JOBPKS + ")");
                    sb.Append("           AND JE.JOBCARD_REF_NO IN (" + JOB_NR + ")");
                    sb.Append("GROUP BY TT.JOB_CARD_FK,");
                    sb.Append("          BK.BOOKING_REF_NO,");
                    sb.Append("          JE.JOBCARD_REF_NO,");
                    sb.Append("          PORTPOL.PORT_ID,");
                    sb.Append("          PORTPOD.PORT_ID,");
                    sb.Append("          OP.OPERATOR_NAME,");
                    sb.Append("          VM.VESSEL_ID,");
                    sb.Append("          VT.VOYAGE,");
                    sb.Append("          JE.ETA_DATE,");
                    sb.Append("          JE.ETD_DATE,");
                    sb.Append("          BK.CARGO_TYPE,");
                    sb.Append("          CG.COMMODITY_GROUP_CODE,");
                    sb.Append("          JI.JOB_CARD_TRN_PK");
                    sb.Append("          )");
                    sb.Append(" UNION ");
                    sb.Append("SELECT *");
                    sb.Append("  FROM (SELECT DISTINCT TT.JOB_CARD_FK,");
                    sb.Append("                        BK.BOOKING_REF_NO,");
                    sb.Append("                        JE.JOBCARD_REF_NO JOB_CARD_NR,");
                    sb.Append("CASE WHEN JI.JOB_CARD_TRN_PK IS NOT NULL THEN");
                    sb.Append("                           GET_IMPORT_STATUS(JI.JOB_CARD_TRN_PK)");
                    sb.Append("                          ELSE GET_IMPORT_STATUS(TT.JOB_CARD_FK) END STATUS,");
                    //sb.Append("                        MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)) TT_DT,")
                    sb.Append("                        TO_CHAR(MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)),DATEFORMAT) TT_DT,");
                    sb.Append("                        MAX(TO_CHAR(TT.CREATED_ON, 'HH24:MI')) TT_TM,");
                    sb.Append("                        PORTPOL.PORT_ID POL,");
                    sb.Append("                        PORTPOD.PORT_ID POD,");
                    sb.Append("                        OP.AIRLINE_NAME CARRIER,");
                    sb.Append("                        JE.VOYAGE_FLIGHT_NO VSL_VOY,");
                    sb.Append("                        TO_CHAR(JE.ETD_DATE, DATEFORMAT) ETD_DATE,");
                    sb.Append("                        TO_CHAR(JE.ETA_DATE, DATEFORMAT) ETA_DATE,");
                    sb.Append("                        'AIR' CARGO_TYPE,");
                    sb.Append("                        CG.COMMODITY_GROUP_CODE COMM_GRP,");
                    sb.Append("                        '' BTNCLK,");
                    sb.Append("                        NULL CREATED_ON");
                    sb.Append("          FROM TRACK_N_TRACE_TBL       TT,");
                    sb.Append("               JOB_CARD_TRN    JE,");
                    sb.Append("               JOB_CARD_TRN    JI,");
                    sb.Append("               BOOKING_MST_TBL         BK,");
                    sb.Append("               PORT_MST_TBL            PORTPOL,");
                    sb.Append("               PORT_MST_TBL            PORTPOD,");
                    sb.Append("               COMMODITY_GROUP_MST_TBL CG,");
                    sb.Append("               AIRLINE_MST_TBL         OP");
                    sb.Append("         WHERE JE.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND JE.JOBCARD_REF_NO = JI.JOBCARD_REF_NO(+)");
                    sb.Append("           AND BK.BOOKING_MST_PK = JE.BOOKING_MST_FK");
                    sb.Append("           AND PORTPOL.PORT_MST_PK = BK.PORT_MST_POL_FK");
                    sb.Append("           AND PORTPOD.PORT_MST_PK = BK.PORT_MST_POD_FK");
                    sb.Append("           AND CG.COMMODITY_GROUP_PK = BK.COMMODITY_GROUP_FK");
                    sb.Append("           AND OP.AIRLINE_MST_PK(+) = BK.CARRIER_MST_FK");
                    sb.Append("           AND JE.BUSINESS_TYPE = 1 ");
                    sb.Append("           AND TT.JOB_CARD_FK IN (" + JOBPKS + ")");
                    sb.Append("           AND JE.JOBCARD_REF_NO IN (" + JOB_NR + ")");
                    sb.Append(" GROUP BY TT.JOB_CARD_FK,");
                    sb.Append("          BK.BOOKING_REF_NO,");
                    sb.Append("          JE.JOBCARD_REF_NO,");
                    sb.Append("          PORTPOL.PORT_ID,");
                    sb.Append("          PORTPOD.PORT_ID,");
                    sb.Append("          OP.AIRLINE_NAME,");
                    sb.Append("          JE.VOYAGE_FLIGHT_NO,");
                    sb.Append("          JE.ETD_DATE,");
                    sb.Append("          JE.ETA_DATE,");
                    sb.Append("          CG.COMMODITY_GROUP_CODE,");
                    sb.Append("          JI.JOB_CARD_TRN_PK");
                    sb.Append("          )");

                    sb.Append("          UNION ");
                    sb.Append("SELECT *");
                    sb.Append("  FROM (SELECT DISTINCT TT.JOB_CARD_FK,");
                    sb.Append("                        BK.BOOKING_REF_NO,");
                    sb.Append("                        JE.JOBCARD_REF_NO JOB_CARD_NR,");
                    sb.Append("                        GET_IMPORT_STATUS(TT.JOB_CARD_FK) STATUS,");
                    //sb.Append("                        MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)) TT_DT,")
                    sb.Append("                        TO_CHAR(MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)),DATEFORMAT) TT_DT,");
                    sb.Append("                        MAX(TO_CHAR(TT.CREATED_ON, 'HH24:MI')) TT_TM,");
                    sb.Append("                        PORTPOL.PORT_ID POL,");
                    sb.Append("                        PORTPOD.PORT_ID POD,");
                    sb.Append("                        OP.OPERATOR_NAME CARRIER,");
                    sb.Append("                        (VM.VESSEL_ID || '/' || VT.VOYAGE) VSL_VOY,");
                    sb.Append("                        TO_CHAR(JE.ETD_DATE, DATEFORMAT) ETD_DATE,");
                    sb.Append("                        TO_CHAR(JE.ETA_DATE, DATEFORMAT) ETA_DATE,");
                    sb.Append("                        DECODE(BK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                    sb.Append("                        CG.COMMODITY_GROUP_CODE COMM_GRP,");
                    sb.Append("                        '' BTNCLK,");
                    sb.Append("                        NULL CREATED_ON");
                    sb.Append("          FROM TRACK_N_TRACE_TBL       TT,");
                    sb.Append("               JOB_CARD_TRN    JE,");
                    sb.Append("               JOB_CARD_TRN          JI,");
                    sb.Append("               BOOKING_MST_TBL         BK,");
                    sb.Append("               PORT_MST_TBL            PORTPOL,");
                    sb.Append("               PORT_MST_TBL            PORTPOD,");
                    sb.Append("               COMMODITY_GROUP_MST_TBL CG,");
                    sb.Append("               VESSEL_VOYAGE_TBL       VM,");
                    sb.Append("               VESSEL_VOYAGE_TRN       VT,");
                    sb.Append("               OPERATOR_MST_TBL        OP");
                    sb.Append("         WHERE JI.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND JE.JOBCARD_REF_NO = JI.JOBCARD_REF_NO");
                    sb.Append("           AND BK.BOOKING_MST_PK = JE.BOOKING_MST_FK");
                    sb.Append("           AND PORTPOL.PORT_MST_PK = BK.PORT_MST_POL_FK");
                    sb.Append("           AND PORTPOD.PORT_MST_PK = BK.PORT_MST_POD_FK");
                    sb.Append("           AND CG.COMMODITY_GROUP_PK = BK.COMMODITY_GROUP_FK");
                    sb.Append("           AND VM.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                    sb.Append("           AND VT.VOYAGE_TRN_PK(+) = JE.VOYAGE_TRN_FK");
                    sb.Append("           AND OP.OPERATOR_MST_PK(+) = VM.OPERATOR_MST_FK");
                    sb.Append("           AND JI.JC_AUTO_MANUAL = 1 ");
                    sb.Append("           AND JE.BUSINESS_TYPE = 2 ");
                    sb.Append("           AND TT.JOB_CARD_FK IN (" + JOBPKS + ")");
                    sb.Append("           AND JE.JOBCARD_REF_NO IN (" + JOB_NR + ")");
                    sb.Append("         GROUP BY TT.JOB_CARD_FK,");
                    sb.Append("                  BK.BOOKING_REF_NO,");
                    sb.Append("                  JE.JOBCARD_REF_NO,");
                    sb.Append("                  PORTPOL.PORT_ID,");
                    sb.Append("                  PORTPOD.PORT_ID,");
                    sb.Append("                  OP.OPERATOR_NAME,");
                    sb.Append("                  VM.VESSEL_ID,");
                    sb.Append("                  VT.VOYAGE,");
                    sb.Append("                  JE.ETA_DATE,");
                    sb.Append("                  JE.ETD_DATE,");
                    sb.Append("                  BK.CARGO_TYPE,");
                    sb.Append("                  CG.COMMODITY_GROUP_CODE,");
                    sb.Append("                  JI.JOB_CARD_TRN_PK)");
                    sb.Append(" UNION ");
                    sb.Append("SELECT *");
                    sb.Append("  FROM (SELECT DISTINCT TT.JOB_CARD_FK,");
                    sb.Append("                        BK.BOOKING_REF_NO,");
                    sb.Append("                        JE.JOBCARD_REF_NO JOB_CARD_NR,");
                    sb.Append("                        CASE");
                    sb.Append("                          WHEN JI.JOB_CARD_TRN_PK IS NOT NULL THEN");
                    sb.Append("                           GET_IMPORT_STATUS(JI.JOB_CARD_TRN_PK)");
                    sb.Append("                          ELSE");
                    sb.Append("                           GET_IMPORT_STATUS(TT.JOB_CARD_FK)");
                    sb.Append("                        END STATUS,");
                    //sb.Append("                        MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)) TT_DT,")
                    sb.Append("                        TO_CHAR(MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)),DATEFORMAT) TT_DT,");
                    sb.Append("                        MAX(TO_CHAR(TT.CREATED_ON, 'HH24:MI')) TT_TM,");
                    sb.Append("                        PORTPOL.PORT_ID POL,");
                    sb.Append("                        PORTPOD.PORT_ID POD,");
                    sb.Append("                        OP.AIRLINE_NAME CARRIER,");
                    sb.Append("                        JE.VOYAGE_FLIGHT_NO VSL_VOY,");
                    sb.Append("                        TO_CHAR(JE.ETD_DATE, DATEFORMAT) ETD_DATE,");
                    sb.Append("                        TO_CHAR(JE.ETA_DATE, DATEFORMAT) ETA_DATE,");
                    sb.Append("                        'AIR' CARGO_TYPE,");
                    sb.Append("                        CG.COMMODITY_GROUP_CODE COMM_GRP,");
                    sb.Append("                        '' BTNCLK,");
                    sb.Append("                        NULL CREATED_ON");
                    sb.Append("          FROM TRACK_N_TRACE_TBL       TT,");
                    sb.Append("               JOB_CARD_TRN    JE,");
                    sb.Append("               JOB_CARD_TRN    JI,");
                    sb.Append("               BOOKING_MST_TBL         BK,");
                    sb.Append("               PORT_MST_TBL            PORTPOL,");
                    sb.Append("               PORT_MST_TBL            PORTPOD,");
                    sb.Append("               COMMODITY_GROUP_MST_TBL CG,");
                    sb.Append("               AIRLINE_MST_TBL         OP");
                    sb.Append("         WHERE JI.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND JE.JOBCARD_REF_NO = JI.JOBCARD_REF_NO");
                    sb.Append("           AND BK.BOOKING_MST_PK = JE.BOOKING_MST_FK");
                    sb.Append("           AND PORTPOL.PORT_MST_PK = BK.PORT_MST_POL_FK");
                    sb.Append("           AND PORTPOD.PORT_MST_PK = BK.PORT_MST_POD_FK");
                    sb.Append("           AND CG.COMMODITY_GROUP_PK = BK.COMMODITY_GROUP_FK");
                    sb.Append("           AND OP.AIRLINE_MST_PK(+) = BK.CARRIER_MST_FK");
                    sb.Append("           AND JI.JC_AUTO_MANUAL = 1 ");
                    sb.Append("           AND JE.BUSINESS_TYPE = 1 ");
                    sb.Append("           AND TT.JOB_CARD_FK IN (" + JOBPKS + ")");
                    sb.Append("           AND JE.JOBCARD_REF_NO IN (" + JOB_NR + ")");
                    sb.Append("         GROUP BY TT.JOB_CARD_FK,");
                    sb.Append("                  BK.BOOKING_REF_NO,");
                    sb.Append("                  JE.JOBCARD_REF_NO,");
                    sb.Append("                  PORTPOL.PORT_ID,");
                    sb.Append("                  PORTPOD.PORT_ID,");
                    sb.Append("                  OP.AIRLINE_NAME,");
                    sb.Append("                  JE.VOYAGE_FLIGHT_NO,");
                    sb.Append("                  JE.ETD_DATE,");
                    sb.Append("                  JE.ETA_DATE,");
                    sb.Append("                  CG.COMMODITY_GROUP_CODE,");
                    sb.Append("                  JI.JOB_CARD_TRN_PK)");
                    sb.Append(" UNION ");
                    sb.Append("SELECT DISTINCT TT.JOB_CARD_FK,");
                    sb.Append("                '' BOOKING_REF_NO,");
                    sb.Append("                JI.JOBCARD_REF_NO JOB_CARD_NR,");
                    sb.Append("                GET_IMPORT_STATUS(TT.JOB_CARD_FK) STATUS,");
                    //sb.Append("                MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)) TT_DT,")
                    sb.Append("                TO_CHAR(MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)),DATEFORMAT) TT_DT,");
                    sb.Append("                MAX(TO_CHAR(TT.CREATED_ON, 'HH24:MI')) TT_TM,");
                    sb.Append("                PORTPOL.PORT_ID POL,");
                    sb.Append("                PORTPOD.PORT_ID POD,");
                    sb.Append("                OP.OPERATOR_NAME CARRIER,");
                    sb.Append("                (VM.VESSEL_ID || '/' || VT.VOYAGE) VSL_VOY,");
                    sb.Append("                TO_CHAR(JI.ETD_DATE, DATEFORMAT) ETD_DATE,");
                    sb.Append("                TO_CHAR(JI.ETA_DATE, DATEFORMAT) ETA_DATE,");
                    sb.Append("                DECODE(JI.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                    sb.Append("                CG.COMMODITY_GROUP_CODE COMM_GRP,");
                    sb.Append("                '' BTNCLK,");
                    sb.Append("                NULL CREATED_ON");
                    sb.Append("  FROM TRACK_N_TRACE_TBL       TT,");
                    sb.Append("       JOB_CARD_TRN    JI,");
                    sb.Append("       PORT_MST_TBL            PORTPOL,");
                    sb.Append("       PORT_MST_TBL            PORTPOD,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CG,");
                    sb.Append("       VESSEL_VOYAGE_TBL       VM,");
                    sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                    sb.Append("       OPERATOR_MST_TBL        OP");
                    sb.Append(" WHERE JI.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("   AND PORTPOL.PORT_MST_PK = JI.PORT_MST_POL_FK");
                    sb.Append("   AND PORTPOD.PORT_MST_PK = JI.PORT_MST_POD_FK");
                    sb.Append("   AND CG.COMMODITY_GROUP_PK = JI.COMMODITY_GROUP_FK");
                    sb.Append("   AND VM.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                    sb.Append("   AND VT.VOYAGE_TRN_PK(+) = JI.VOYAGE_TRN_FK");
                    sb.Append("   AND OP.OPERATOR_MST_PK(+) = VM.OPERATOR_MST_FK");
                    sb.Append("   AND JI.JC_AUTO_MANUAL = 0 ");
                    sb.Append("   AND JI.BUSINESS_TYPE = 2 ");
                    sb.Append("   AND TT.JOB_CARD_FK IN (" + JOBPKS + ")");
                    sb.Append("           AND JI.JOBCARD_REF_NO IN (" + JOB_NR + ")");
                    sb.Append(" GROUP BY TT.JOB_CARD_FK,");
                    sb.Append("          JI.JOBCARD_REF_NO,");
                    sb.Append("          PORTPOL.PORT_ID,");
                    sb.Append("          PORTPOD.PORT_ID,");
                    sb.Append("          OP.OPERATOR_NAME,");
                    sb.Append("          VM.VESSEL_ID,");
                    sb.Append("          VT.VOYAGE,");
                    sb.Append("          JI.ETA_DATE,");
                    sb.Append("          JI.ETD_DATE,");
                    sb.Append("          JI.CARGO_TYPE,");
                    sb.Append("          CG.COMMODITY_GROUP_CODE,");
                    sb.Append("          JI.JOB_CARD_TRN_PK");
                    sb.Append(" UNION ");
                    sb.Append("SELECT DISTINCT TT.JOB_CARD_FK,");
                    sb.Append("                '' BOOKING_REF_NO,");
                    sb.Append("                JI.JOBCARD_REF_NO JOB_CARD_NR,");
                    sb.Append("                GET_IMPORT_STATUS(TT.JOB_CARD_FK) STATUS,");
                    //sb.Append("                MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)) TT_DT,")
                    sb.Append("                 TO_CHAR(MAX(TO_DATE(TT.CREATED_ON, DATEFORMAT)),DATEFORMAT) TT_DT,");
                    sb.Append("                MAX(TO_CHAR(TT.CREATED_ON, 'HH24:MI')) TT_TM,");
                    sb.Append("                PORTPOL.PORT_ID POL,");
                    sb.Append("                PORTPOD.PORT_ID POD,");
                    sb.Append("                OP.AIRLINE_NAME CARRIER,");
                    sb.Append("                JI.VOYAGE_FLIGHT_NO VSL_VOY,");
                    sb.Append("                TO_CHAR(JI.ETD_DATE, DATEFORMAT) ETD_DATE,");
                    sb.Append("                TO_CHAR(JI.ETA_DATE, DATEFORMAT) ETA_DATE,");
                    sb.Append("                'AIR' CARGO_TYPE,");
                    sb.Append("                CG.COMMODITY_GROUP_CODE COMM_GRP,");
                    sb.Append("                '' BTNCLK,");
                    sb.Append("                NULL CREATED_ON");
                    sb.Append("  FROM TRACK_N_TRACE_TBL       TT,");
                    sb.Append("       JOB_CARD_TRN    JI,");
                    sb.Append("       PORT_MST_TBL            PORTPOL,");
                    sb.Append("       PORT_MST_TBL            PORTPOD,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CG,");
                    sb.Append("       AIRLINE_MST_TBL         OP");
                    sb.Append(" WHERE JI.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("   AND PORTPOL.PORT_MST_PK = JI.PORT_MST_POL_FK");
                    sb.Append("   AND PORTPOD.PORT_MST_PK = JI.PORT_MST_POD_FK");
                    sb.Append("   AND CG.COMMODITY_GROUP_PK = JI.COMMODITY_GROUP_FK");
                    sb.Append("   AND OP.AIRLINE_MST_PK = JI.CARRIER_MST_FK");
                    sb.Append("   AND JI.JC_AUTO_MANUAL = 0 ");
                    sb.Append("   AND JI.BUSINESS_TYPE = 1 ");
                    sb.Append("   AND TT.JOB_CARD_FK IN (" + JOBPKS + ")");
                    sb.Append("           AND JI.JOBCARD_REF_NO IN (" + JOB_NR + ")");
                    sb.Append(" GROUP BY TT.JOB_CARD_FK,");
                    sb.Append("          JI.JOBCARD_REF_NO,");
                    sb.Append("          PORTPOL.PORT_ID,");
                    sb.Append("          PORTPOD.PORT_ID,");
                    sb.Append("          OP.AIRLINE_NAME,");
                    sb.Append("          JI.VOYAGE_FLIGHT_NO,");
                    sb.Append("          JI.ETA_DATE,");
                    sb.Append("          JI.ETD_DATE,");
                    sb.Append("          CG.COMMODITY_GROUP_CODE,");
                    sb.Append("          JI.JOB_CARD_TRN_PK");
                }
                else
                {
                    sb.Append("SELECT 0 JOB_CARD_FK,");
                    sb.Append("       '' BOOKING_REF_NO,");
                    sb.Append("       '' JOB_CARD_NR,");
                    sb.Append("       '' STATUS,");
                    sb.Append("       '' TT_DT,");
                    sb.Append("       '' TT_TM,");
                    sb.Append("       '' POL,");
                    sb.Append("       '' POD,");
                    sb.Append("       '' CARRIER,");
                    sb.Append("       '' VSL_VOY,");
                    sb.Append("       NULL ETD_DATE,");
                    sb.Append("       NULL ETA_DATE,");
                    sb.Append("       '' CARGO_TYPE,");
                    sb.Append("       '' COMM_GRP,");
                    sb.Append("       '' BTNCLK,NULL CREATED_ON ");
                    sb.Append("  FROM DUAL");
                    sb.Append(" WHERE 1 = 2");
                }

                return objWK.GetDataSet(sb.ToString());

            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        #endregion

        #region "Multiple Tracking"
        /// <summary>
        /// Fetches the multiple tracking.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="DocType">Type of the document.</param>
        /// <param name="DocNr">The document nr.</param>
        /// <param name="Process">The process.</param>
        /// <param name="DocTypeItem">The document type item.</param>
        /// <returns></returns>
        public DataSet FetchMultipleTracking(int BizType, string DocType, string DocNr, int Process, string DocTypeItem)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                if (!string.IsNullOrEmpty(DocNr))
                {
                    //sb.Append("SELECT T.JOB_CARD_FK JOB_CARD_PK,")
                    //sb.Append("       DECODE(T.BIZ_TYPE, 1, 'AIR', 2, 'SEA') BIZ_TYPE,")
                    //sb.Append("       '" & DocType & "' DOC_TYPE,")
                    //sb.Append("       T.DOC_REF_NO DOC_NR")
                    //sb.Append("  FROM TRACK_N_TRACE_TBL T")
                    //sb.Append(" WHERE T.DOC_REF_NO  = '" & DocNr & "' ")
                    if (Process == 1)
                    {
                        sb.Append("SELECT T.JOB_CARD_FK JOB_CARD_PK,");
                        sb.Append("       JE.JOBCARD_REF_NO,");
                        sb.Append("       DECODE(T.BIZ_TYPE, 1, 'AIR', 2, 'SEA') BIZ_TYPE,");
                        sb.Append("       '" + DocTypeItem + "' DOC_TYPE,");
                        sb.Append("       T.DOC_REF_NO DOC_NR");
                        sb.Append("  FROM TRACK_N_TRACE_TBL T, JOB_CARD_TRN JE");
                        if (DocTypeItem == "Container Nr")
                        {
                            sb.Append("   ,JOB_TRN_CONT JT ");
                        }
                        sb.Append(" WHERE JE.JOB_CARD_TRN_PK = T.JOB_CARD_FK");
                        sb.Append(" AND T.BIZ_TYPE = 2 ");
                        sb.Append(" AND T.PROCESS = 1 ");
                        if (DocTypeItem == "Container Nr")
                        {
                            sb.Append(" AND JT.JOB_CARD_TRN_FK = JE.JOB_CARD_TRN_PK");
                            sb.Append(" AND ROWNUM=1 ");
                            sb.Append("   AND JT.CONTAINER_NUMBER = '" + DocNr + "' ");
                        }
                        else
                        {
                            sb.Append("   AND T.DOC_REF_NO = '" + DocNr + "' ");
                        }
                        sb.Append(" UNION ");
                        sb.Append("SELECT T.JOB_CARD_FK JOB_CARD_PK,");
                        sb.Append("       JE.JOBCARD_REF_NO,");
                        sb.Append("       DECODE(T.BIZ_TYPE, 1, 'AIR', 2, 'SEA') BIZ_TYPE,");
                        sb.Append("       '" + DocTypeItem + "' DOC_TYPE,");
                        sb.Append("       T.DOC_REF_NO DOC_NR");
                        sb.Append("  FROM TRACK_N_TRACE_TBL T, JOB_CARD_TRN JE");
                        sb.Append(" WHERE JE.JOB_CARD_TRN_PK = T.JOB_CARD_FK");
                        sb.Append(" AND T.BIZ_TYPE = 1 ");
                        sb.Append(" AND T.PROCESS = 1 ");
                        sb.Append("   AND T.DOC_REF_NO = '" + DocNr + "' ");
                    }
                    else
                    {
                        sb.Append("SELECT T.JOB_CARD_FK JOB_CARD_PK,");
                        sb.Append("       JI.JOBCARD_REF_NO,");
                        sb.Append("       DECODE(T.BIZ_TYPE, 1, 'AIR', 2, 'SEA') BIZ_TYPE,");
                        sb.Append("       '" + DocTypeItem + "' DOC_TYPE,");
                        sb.Append("       T.DOC_REF_NO DOC_NR");
                        sb.Append("  FROM TRACK_N_TRACE_TBL T, JOB_CARD_TRN JI");
                        if (DocTypeItem == "Container Nr")
                        {
                            sb.Append(" ,JOB_TRN_CONT JT");
                        }
                        sb.Append(" WHERE JI.JOB_CARD_TRN_PK = T.JOB_CARD_FK");
                        sb.Append(" AND T.BIZ_TYPE = 2 ");
                        sb.Append(" AND T.PROCESS = 2 ");
                        if (DocTypeItem == "Container Nr")
                        {
                            sb.Append(" AND JT.JOB_CARD_TRN_FK=JI.JOB_CARD_TRN_PK ");
                            sb.Append("   AND JT.CONTAINER_NUMBER = '" + DocNr + "' ");
                            sb.Append(" AND ROWNUM = 1 ");
                        }
                        else
                        {
                            sb.Append("   AND T.DOC_REF_NO = '" + DocNr + "' ");
                        }
                        sb.Append(" UNION ");
                        sb.Append("SELECT T.JOB_CARD_FK JOB_CARD_PK,");
                        sb.Append("       JI.JOBCARD_REF_NO,");
                        sb.Append("       DECODE(T.BIZ_TYPE, 1, 'AIR', 2, 'SEA') BIZ_TYPE,");
                        sb.Append("       '" + DocTypeItem + "' DOC_TYPE,");
                        sb.Append("       T.DOC_REF_NO DOC_NR");
                        sb.Append("  FROM TRACK_N_TRACE_TBL T, JOB_CARD_TRN JI");
                        sb.Append(" WHERE JI.JOB_CARD_TRN_PK = T.JOB_CARD_FK");
                        sb.Append(" AND T.BIZ_TYPE = 1 ");
                        sb.Append(" AND T.PROCESS = 2 ");
                        sb.Append("   AND T.DOC_REF_NO = '" + DocNr + "' ");
                    }


                }
                else
                {
                    sb.Append("    SELECT   0 JOB_CARD_PK,'' JOBCARD_REF_NO , ");
                    sb.Append("       '' BIZ_TYPE,");
                    sb.Append("       '' DOC_TYPE,");
                    sb.Append("       '' DOC_NR");
                    sb.Append("  FROM DUAL");
                    sb.Append(" WHERE 1 = 2");
                }

                return objWK.GetDataSet(sb.ToString());

            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }
        #endregion

        #region "Fetch Tracking Gates"
        /// <summary>
        /// Fetches the gates tracking.
        /// </summary>
        /// <param name="JOBCARD_PK">The jobcar d_ pk.</param>
        /// <param name="JOBCARD_NR">The jobcar d_ nr.</param>
        /// <param name="CARGO_TYPE">Type of the carg o_.</param>
        /// <returns></returns>
        public DataSet FetchGatesTracking(int JOBCARD_PK, string JOBCARD_NR, string CARGO_TYPE)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                if (CARGO_TYPE != "AIR")
                {
                    sb.Append("SELECT *");
                    sb.Append("  FROM (SELECT DISTINCT TT.STATUS ACTION,");
                    sb.Append("                        TT.DOC_REF_NO,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DD/MM/YYYY') ACTION_DTTIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'HH24:MI') ACTION_TIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DAY') ACTION_DAY,");
                    sb.Append("                        UM.USER_ID,");
                    sb.Append("                        LM.LOCATION_NAME LOCATION,");
                    sb.Append("                        '' DTL,");
                    //sb.Append("                        TT.CREATED_ON,")
                    sb.Append("                 CASE WHEN TT.STATUS = 'Job Card' THEN");
                    sb.Append("                 TT.CREATED_ON + 30 / (24 * 60 * 60)");
                    sb.Append("                 ELSE TT.CREATED_ON END CREATED_ON,");
                    sb.Append("                        TT.JOB_CARD_FK,");
                    sb.Append("                        TT.BIZ_TYPE,");
                    sb.Append("                        TT.PROCESS,");
                    sb.Append("                        JC.JOBCARD_REF_NO JOB_CARD_NR ");
                    sb.Append("          FROM TRACK_N_TRACE_TBL    TT,");
                    sb.Append("               JOB_CARD_TRN JC,");
                    sb.Append("               USER_MST_TBL         UM,");
                    sb.Append("               LOCATION_MST_TBL     LM");
                    sb.Append("         WHERE UM.USER_MST_PK = TT.CREATED_BY");
                    sb.Append("           AND JC.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND LM.LOCATION_MST_PK = TT.LOCATION_FK");
                    sb.Append("           AND TT.BIZ_TYPE=2 ");
                    sb.Append("           AND TT.PROCESS=1 ");
                    sb.Append("           AND JC.JOBCARD_REF_NO = '" + JOBCARD_NR + "'");
                    sb.Append("        UNION ");
                    sb.Append("        SELECT DISTINCT TT.STATUS ACTION,");
                    sb.Append("                        TT.DOC_REF_NO,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DD/MM/YYYY') ACTION_DTTIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'HH24:MI') ACTION_TIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DAY') ACTION_DAY,");
                    sb.Append("                        UM.USER_ID,");
                    sb.Append("                        LM.LOCATION_NAME LOCATION,");
                    sb.Append("                        '' DTL,");
                    //sb.Append("                        TT.CREATED_ON,")
                    sb.Append("                 CASE WHEN TT.STATUS = 'Job Card' THEN");
                    sb.Append("                 TT.CREATED_ON + 30 / (24 * 60 * 60)");
                    sb.Append("                 ELSE TT.CREATED_ON END CREATED_ON,");
                    sb.Append("                        TT.JOB_CARD_FK,");
                    sb.Append("                        TT.BIZ_TYPE,");
                    sb.Append("                        TT.PROCESS,");
                    sb.Append("                        JC.JOBCARD_REF_NO JOB_CARD_NR ");
                    sb.Append("          FROM TRACK_N_TRACE_TBL    TT,");
                    sb.Append("               JOB_CARD_TRN JC,");
                    sb.Append("               USER_MST_TBL         UM,");
                    sb.Append("               LOCATION_MST_TBL     LM");
                    sb.Append("         WHERE UM.USER_MST_PK = TT.CREATED_BY");
                    sb.Append("           AND JC.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND TT.BIZ_TYPE=2 ");
                    sb.Append("           AND TT.PROCESS=2 ");
                    sb.Append("           AND LM.LOCATION_MST_PK = TT.LOCATION_FK");
                    sb.Append("           AND JC.JOBCARD_REF_NO = '" + JOBCARD_NR + "')");
                    //'For DTS:10071
                    //sb.Append("WHERE NOT REGEXP_LIKE(ACTION, 'Invoice')")
                    //sb.Append(" AND  NOT REGEXP_LIKE(ACTION, 'Collection')")
                    //sb.Append(" AND  NOT REGEXP_LIKE(ACTION, 'Credit')")
                    //'End
                    sb.Append(" ORDER BY CREATED_ON DESC");
                }
                else
                {
                    sb.Append("SELECT *");
                    sb.Append("  FROM (SELECT DISTINCT TT.STATUS ACTION,");
                    sb.Append("                        TT.DOC_REF_NO,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DD/MM/YYYY') ACTION_DTTIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'HH24:MI') ACTION_TIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DAY') ACTION_DAY,");
                    sb.Append("                        UM.USER_ID,");
                    sb.Append("                        LM.LOCATION_NAME LOCATION,");
                    sb.Append("                        '' DTL,");
                    //sb.Append("                        TT.CREATED_ON,")
                    sb.Append("                 CASE WHEN TT.STATUS = 'Job Card' THEN");
                    sb.Append("                 TT.CREATED_ON + 30 / (24 * 60 * 60)");
                    sb.Append("                 ELSE TT.CREATED_ON END CREATED_ON,");
                    sb.Append("                        TT.JOB_CARD_FK,");
                    sb.Append("                        TT.BIZ_TYPE,");
                    sb.Append("                        TT.PROCESS,");
                    sb.Append("                        JC.JOBCARD_REF_NO JOB_CARD_NR ");
                    sb.Append("          FROM TRACK_N_TRACE_TBL    TT,");
                    sb.Append("               JOB_CARD_TRN JC,");
                    sb.Append("               USER_MST_TBL         UM,");
                    sb.Append("               LOCATION_MST_TBL     LM");
                    sb.Append("         WHERE UM.USER_MST_PK = TT.CREATED_BY");
                    sb.Append("           AND JC.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND LM.LOCATION_MST_PK = TT.LOCATION_FK");
                    sb.Append("           AND TT.BIZ_TYPE=1 ");
                    sb.Append("           AND TT.PROCESS=1 ");
                    sb.Append("           AND JC.JOBCARD_REF_NO = '" + JOBCARD_NR + "'");
                    sb.Append("        UNION ");
                    sb.Append("        SELECT DISTINCT TT.STATUS ACTION,");
                    sb.Append("                        TT.DOC_REF_NO,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DD/MM/YYYY') ACTION_DTTIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'HH24:MI') ACTION_TIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DAY') ACTION_DAY,");
                    sb.Append("                        UM.USER_ID,");
                    sb.Append("                        LM.LOCATION_NAME LOCATION,");
                    sb.Append("                        '' DTL,");
                    //sb.Append("                        TT.CREATED_ON,")
                    sb.Append("                 CASE WHEN TT.STATUS = 'Job Card' THEN");
                    sb.Append("                 TT.CREATED_ON + 30 / (24 * 60 * 60)");
                    sb.Append("                 ELSE TT.CREATED_ON END CREATED_ON,");
                    sb.Append("                        TT.JOB_CARD_FK,");
                    sb.Append("                        TT.BIZ_TYPE,");
                    sb.Append("                        TT.PROCESS,");
                    sb.Append("                        JC.JOBCARD_REF_NO JOB_CARD_NR ");
                    sb.Append("          FROM TRACK_N_TRACE_TBL    TT,");
                    sb.Append("               JOB_CARD_TRN JC,");
                    sb.Append("               USER_MST_TBL         UM,");
                    sb.Append("               LOCATION_MST_TBL     LM");
                    sb.Append("         WHERE UM.USER_MST_PK = TT.CREATED_BY");
                    sb.Append("           AND JC.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND LM.LOCATION_MST_PK = TT.LOCATION_FK");
                    sb.Append("           AND TT.BIZ_TYPE=1 ");
                    sb.Append("           AND TT.PROCESS=2 ");
                    sb.Append("           AND JC.JOBCARD_REF_NO = '" + JOBCARD_NR + "')");
                    //'For DTS:10071
                    //sb.Append("WHERE NOT REGEXP_LIKE(ACTION, 'Invoice')")
                    //sb.Append(" AND  NOT REGEXP_LIKE(ACTION, 'Collection')")
                    //sb.Append(" AND  NOT REGEXP_LIKE(ACTION, 'Credit')")
                    //'End
                    sb.Append(" ORDER BY CREATED_ON DESC");
                }

                return objWK.GetDataSet(sb.ToString());

            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }
        #endregion

        #region "For Muliple Tracking"
        /// <summary>
        /// Fetches the tracking DTL.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="JobCard_Nr">The job card_ nr.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchTrackingDtl(int JobPK, string JobCard_Nr, int BizType, int Process)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                if (BizType == 2)
                {
                    sb.Append("SELECT *");
                    sb.Append("  FROM (SELECT DISTINCT TT.STATUS ACTION,");
                    sb.Append("                        TT.DOC_REF_NO,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DD/MM/YYYY') ACTION_DTTIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'HH24:MI') ACTION_TIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DAY') ACTION_DAY,");
                    sb.Append("                        UM.USER_ID,");
                    sb.Append("                        LM.LOCATION_NAME LOCATION,");
                    //sb.Append("                        TT.CREATED_ON")
                    sb.Append("                 CASE WHEN TT.STATUS = 'Job Card' THEN");
                    sb.Append("                 TT.CREATED_ON + 30 / (24 * 60 * 60)");
                    sb.Append("                 ELSE TT.CREATED_ON END CREATED_ON");
                    sb.Append("          FROM TRACK_N_TRACE_TBL    TT,");
                    sb.Append("               JOB_CARD_TRN JC,");
                    sb.Append("               USER_MST_TBL         UM,");
                    sb.Append("               LOCATION_MST_TBL     LM");
                    sb.Append("         WHERE UM.USER_MST_PK = TT.CREATED_BY");
                    sb.Append("           AND JC.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND LM.LOCATION_MST_PK = TT.LOCATION_FK");
                    sb.Append("           AND TT.BIZ_TYPE=2 ");
                    sb.Append("           AND TT.PROCESS=1 ");
                    //sb.Append("           AND JC.JOB_CARD_TRN_PK = " & JobPK)
                    sb.Append("           AND JC.JOBCARD_REF_NO = '" + JobCard_Nr + "'  ");
                    sb.Append("        UNION");
                    sb.Append("        SELECT DISTINCT TT.STATUS ACTION,");
                    sb.Append("                        TT.DOC_REF_NO,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DD/MM/YYYY') ACTION_DTTIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'HH24:MI') ACTION_TIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DAY') ACTION_DAY,");
                    sb.Append("                        UM.USER_ID,");
                    sb.Append("                        LM.LOCATION_NAME LOCATION,");
                    //sb.Append("                        TT.CREATED_ON")
                    sb.Append("                 CASE WHEN TT.STATUS = 'Job Card' THEN");
                    sb.Append("                 TT.CREATED_ON + 30 / (24 * 60 * 60)");
                    sb.Append("                 ELSE TT.CREATED_ON END CREATED_ON");
                    sb.Append("          FROM TRACK_N_TRACE_TBL    TT,");
                    sb.Append("               JOB_CARD_TRN JC,");
                    sb.Append("               USER_MST_TBL         UM,");
                    sb.Append("               LOCATION_MST_TBL     LM");
                    sb.Append("         WHERE UM.USER_MST_PK = TT.CREATED_BY");
                    sb.Append("           AND JC.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND LM.LOCATION_MST_PK = TT.LOCATION_FK");
                    sb.Append("           AND TT.BIZ_TYPE=2 ");
                    sb.Append("           AND TT.PROCESS=2 ");
                    //sb.Append("           AND JC.JOB_CARD_TRN_PK = " & JobPK)
                    sb.Append("           AND JC.JOBCARD_REF_NO = '" + JobCard_Nr + "' ) ");
                    //sb.Append(" ) WHERE NOT REGEXP_LIKE(ACTION, 'Invoice')")
                    //sb.Append(" AND  NOT REGEXP_LIKE(ACTION, 'Collection')")
                    //sb.Append(" AND  NOT REGEXP_LIKE(ACTION, 'Credit')")
                    sb.Append("  ORDER BY CREATED_ON DESC");
                }
                else
                {
                    sb.Append("SELECT *");
                    sb.Append("  FROM (SELECT DISTINCT TT.STATUS ACTION,");
                    sb.Append("                        TT.DOC_REF_NO,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DD/MM/YYYY') ACTION_DTTIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'HH24:MI') ACTION_TIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DAY') ACTION_DAY,");
                    sb.Append("                        UM.USER_ID,");
                    sb.Append("                        LM.LOCATION_NAME LOCATION,");
                    //sb.Append("                        TT.CREATED_ON")
                    sb.Append("                 CASE WHEN TT.STATUS = 'Job Card' THEN");
                    sb.Append("                 TT.CREATED_ON + 30 / (24 * 60 * 60)");
                    sb.Append("                 ELSE TT.CREATED_ON END CREATED_ON");
                    sb.Append("          FROM TRACK_N_TRACE_TBL    TT,");
                    sb.Append("               JOB_CARD_TRN JC,");
                    sb.Append("               USER_MST_TBL         UM,");
                    sb.Append("               LOCATION_MST_TBL     LM");
                    sb.Append("         WHERE UM.USER_MST_PK = TT.CREATED_BY");
                    sb.Append("           AND JC.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND LM.LOCATION_MST_PK = TT.LOCATION_FK");
                    sb.Append("           AND TT.BIZ_TYPE=1 ");
                    sb.Append("           AND TT.PROCESS=1 ");
                    //sb.Append("           AND JC.JOB_CARD_TRN_PK = " & JobPK)
                    sb.Append("           AND JC.JOBCARD_REF_NO = '" + JobCard_Nr + "'  ");
                    sb.Append("        UNION");
                    sb.Append("        SELECT DISTINCT TT.STATUS ACTION,");
                    sb.Append("                        TT.DOC_REF_NO,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DD/MM/YYYY') ACTION_DTTIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'HH24:MI') ACTION_TIME,");
                    sb.Append("                        TO_CHAR(TT.CREATED_ON, 'DAY') ACTION_DAY,");
                    sb.Append("                        UM.USER_ID,");
                    sb.Append("                        LM.LOCATION_NAME LOCATION,");
                    //sb.Append("                        TT.CREATED_ON")
                    sb.Append("                 CASE WHEN TT.STATUS = 'Job Card' THEN");
                    sb.Append("                 TT.CREATED_ON + 30 / (24 * 60 * 60)");
                    sb.Append("                 ELSE TT.CREATED_ON END CREATED_ON");
                    sb.Append("          FROM TRACK_N_TRACE_TBL    TT,");
                    sb.Append("               JOB_CARD_TRN JC,");
                    sb.Append("               USER_MST_TBL         UM,");
                    sb.Append("               LOCATION_MST_TBL     LM");
                    sb.Append("         WHERE UM.USER_MST_PK = TT.CREATED_BY");
                    sb.Append("           AND JC.JOB_CARD_TRN_PK = TT.JOB_CARD_FK");
                    sb.Append("           AND LM.LOCATION_MST_PK = TT.LOCATION_FK");
                    sb.Append("           AND TT.BIZ_TYPE=1 ");
                    sb.Append("           AND TT.PROCESS=2 ");
                    //sb.Append("           AND JC.JOB_CARD_TRN_PK = " & JobPK)
                    sb.Append("           AND JC.JOBCARD_REF_NO = '" + JobCard_Nr + "' )");
                    //sb.Append(" ) WHERE NOT REGEXP_LIKE(ACTION, 'Invoice')")
                    //sb.Append(" AND  NOT REGEXP_LIKE(ACTION, 'Collection')")
                    //sb.Append(" AND  NOT REGEXP_LIKE(ACTION, 'Credit')")
                    sb.Append("  ORDER BY CREATED_ON DESC");
                }

            }
            catch (Exception ex)
            {
            }

            return objWK.GetDataSet(sb.ToString());
        }
        /// <summary>
        /// Fetches the report header.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchReportHeader(int JobPK, int BizType, int Process)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            if (BizType == 2)
            {
                if (Process == 1)
                {
                    sb.Append("SELECT DISTINCT JC.JOBCARD_REF_NO, POL.PORT_ID      ORIGIN,");
                    sb.Append("       POD.PORT_ID      DESTINATION,");
                    sb.Append("       POO.PORT_NAME    POO,");
                    sb.Append("       PFD.PORT_NAME    PFD,");
                    sb.Append("       SH.CUSTOMER_NAME SHIPPER,");
                    sb.Append("       CN.CUSTOMER_NAME CONSIGNEE,");
                    sb.Append("       NT.CUSTOMER_NAME NOTIFY,");
                    sb.Append("       OP.OPERATOR_NAME,");
                    sb.Append("       VM.VESSEL_NAME,");
                    sb.Append("       VT.VOYAGE");
                    sb.Append("  FROM BOOKING_MST_TBL      BK,");
                    sb.Append("       JOB_CARD_TRN       JC,");
                    sb.Append("       PORT_MST_TBL         POL,");
                    sb.Append("       PORT_MST_TBL         POD,");
                    sb.Append("       PORT_MST_TBL         POO,");
                    sb.Append("       PORT_MST_TBL         PFD,");
                    sb.Append("       CUSTOMER_MST_TBL     SH,");
                    sb.Append("       CUSTOMER_MST_TBL     CN,");
                    sb.Append("       CUSTOMER_MST_TBL     NT,");
                    sb.Append("       VESSEL_VOYAGE_TRN    VT,");
                    sb.Append("       VESSEL_VOYAGE_TBL    VM,");
                    sb.Append("       OPERATOR_MST_TBL     OP");
                    sb.Append(" WHERE JC.BOOKING_MST_FK = BK.BOOKING_MST_PK");
                    sb.Append("   AND POL.PORT_MST_PK = BK.PORT_MST_POL_FK");
                    sb.Append("   AND POD.PORT_MST_PK = BK.PORT_MST_POD_FK");
                    sb.Append("   AND POO.PORT_MST_PK(+) = BK.POO_FK");
                    sb.Append("   AND PFD.PORT_MST_PK(+) = BK.PFD_FK");
                    sb.Append("   AND SH.CUSTOMER_MST_PK = JC.SHIPPER_CUST_MST_FK");
                    sb.Append("   AND CN.CUSTOMER_MST_PK(+) = JC.CONSIGNEE_CUST_MST_FK");
                    sb.Append("   AND NT.CUSTOMER_MST_PK(+) = JC.NOTIFY1_CUST_MST_FK");
                    sb.Append("   AND VT.VOYAGE_TRN_PK(+) = JC.VOYAGE_TRN_FK");
                    sb.Append("   AND VM.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                    sb.Append("   AND OP.OPERATOR_MST_PK(+) = VM.OPERATOR_MST_FK");
                    sb.Append("   AND JC.JOB_CARD_TRN_PK = " + JobPK);
                }
                else
                {
                    sb.Append("SELECT DISTINCT JC.JOBCARD_REF_NO,");
                    sb.Append("                POL.PORT_ID       ORIGIN,");
                    sb.Append("                POD.PORT_ID       DESTINATION,");
                    sb.Append("                POO.PORT_NAME     POO,");
                    sb.Append("                PFD.PORT_NAME     PFD,");
                    sb.Append("                SH.CUSTOMER_NAME  SHIPPER,");
                    sb.Append("                CN.CUSTOMER_NAME  CONSIGNEE,");
                    sb.Append("                NT.CUSTOMER_NAME  NOTIFY,");
                    sb.Append("                OP.OPERATOR_NAME,");
                    sb.Append("                VM.VESSEL_NAME,");
                    sb.Append("                VT.VOYAGE");
                    sb.Append("  FROM JOB_CARD_TRN JC,");
                    sb.Append("       PORT_MST_TBL         POL,");
                    sb.Append("       PORT_MST_TBL         POD,");
                    sb.Append("       PORT_MST_TBL         POO,");
                    sb.Append("       PORT_MST_TBL         PFD,");
                    sb.Append("       CUSTOMER_MST_TBL     SH,");
                    sb.Append("       CUSTOMER_MST_TBL     CN,");
                    sb.Append("       CUSTOMER_MST_TBL     NT,");
                    sb.Append("       VESSEL_VOYAGE_TRN    VT,");
                    sb.Append("       VESSEL_VOYAGE_TBL    VM,");
                    sb.Append("       OPERATOR_MST_TBL     OP");
                    sb.Append(" WHERE POL.PORT_MST_PK = JC.PORT_MST_POL_FK");
                    sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                    sb.Append("   AND POO.PORT_MST_PK(+) = JC.POO_FK");
                    sb.Append("   AND PFD.PORT_MST_PK(+) = JC.PFD_FK");
                    sb.Append("   AND SH.CUSTOMER_MST_PK = JC.SHIPPER_CUST_MST_FK");
                    sb.Append("   AND CN.CUSTOMER_MST_PK(+) = JC.CONSIGNEE_CUST_MST_FK");
                    sb.Append("   AND NT.CUSTOMER_MST_PK(+) = JC.NOTIFY1_CUST_MST_FK");
                    sb.Append("   AND VT.VOYAGE_TRN_PK(+) = JC.VOYAGE_TRN_FK");
                    sb.Append("   AND VM.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                    sb.Append("   AND OP.OPERATOR_MST_PK(+) = VM.OPERATOR_MST_FK");
                    sb.Append("   AND JC.JOB_CARD_TRN_PK = " + JobPK);
                }
            }
            else
            {
                if (Process == 1)
                {
                    sb.Append("SELECT DISTINCT JC.JOBCARD_REF_NO,");
                    sb.Append("                POL.PORT_ID ORIGIN,");
                    sb.Append("                POD.PORT_ID DESTINATION,");
                    sb.Append("                POO.PLACE_NAME POO,");
                    sb.Append("                PFD.PLACE_NAME PFD,");
                    sb.Append("                SH.CUSTOMER_NAME SHIPPER,");
                    sb.Append("                CN.CUSTOMER_NAME CONSIGNEE,");
                    sb.Append("                NT.CUSTOMER_NAME NOTIFY,");
                    sb.Append("                OP.AIRLINE_NAME OPERATOR_NAME,");
                    sb.Append("                '' VESSEL_NAME,");
                    sb.Append("                JC.VOYAGE_FLIGHT_NO VOYAGE");
                    sb.Append("  FROM BOOKING_MST_TBL      BK,");
                    sb.Append("       JOB_CARD_TRN        JC,");
                    sb.Append("       PORT_MST_TBL         POL,");
                    sb.Append("       PORT_MST_TBL         POD,");
                    sb.Append("       PLACE_MST_TBL        POO,");
                    sb.Append("       PLACE_MST_TBL        PFD,");
                    sb.Append("       CUSTOMER_MST_TBL     SH,");
                    sb.Append("       CUSTOMER_MST_TBL     CN,");
                    sb.Append("       CUSTOMER_MST_TBL     NT,");
                    sb.Append("       AIRLINE_MST_TBL      OP");
                    sb.Append(" WHERE JC.BOOKING_MST_FK = BK.BOOKING_MST_PK");
                    sb.Append("   AND POL.PORT_MST_PK = BK.PORT_MST_POL_FK");
                    sb.Append("   AND POD.PORT_MST_PK = BK.PORT_MST_POD_FK");
                    sb.Append("   AND POO.PLACE_PK(+) = BK.COL_PLACE_MST_FK");
                    sb.Append("   AND PFD.PLACE_PK(+) = BK.DEL_PLACE_MST_FK");
                    sb.Append("   AND SH.CUSTOMER_MST_PK = JC.SHIPPER_CUST_MST_FK");
                    sb.Append("   AND CN.CUSTOMER_MST_PK(+) = JC.CONSIGNEE_CUST_MST_FK");
                    sb.Append("   AND NT.CUSTOMER_MST_PK(+) = JC.NOTIFY1_CUST_MST_FK");
                    sb.Append("   AND OP.AIRLINE_MST_PK(+) = BK.CARRIER_MST_FK");
                    sb.Append("   AND JC.JOB_CARD_TRN_PK = " + JobPK);
                }
                else
                {
                    sb.Append("SELECT DISTINCT JC.JOBCARD_REF_NO,");
                    sb.Append("                POL.PORT_ID ORIGIN,");
                    sb.Append("                POD.PORT_ID DESTINATION,");
                    sb.Append("                '' POO,");
                    sb.Append("                PFD.PLACE_NAME PFD,");
                    sb.Append("                SH.CUSTOMER_NAME SHIPPER,");
                    sb.Append("                CN.CUSTOMER_NAME CONSIGNEE,");
                    sb.Append("                NT.CUSTOMER_NAME NOTIFY,");
                    sb.Append("                OP.AIRLINE_NAME OPERATOR_NAME,");
                    sb.Append("                '' VESSEL_NAME,");
                    sb.Append("                JC.VOYAGE_FLIGHT_NO VOYAGE");
                    sb.Append("  FROM JOB_CARD_TRN JC,");
                    sb.Append("       PORT_MST_TBL         POL,");
                    sb.Append("       PORT_MST_TBL         POD,");
                    sb.Append("       PLACE_MST_TBL        PFD,");
                    sb.Append("       CUSTOMER_MST_TBL     SH,");
                    sb.Append("       CUSTOMER_MST_TBL     CN,");
                    sb.Append("       CUSTOMER_MST_TBL     NT,");
                    sb.Append("       AIRLINE_MST_TBL      OP");
                    sb.Append(" WHERE POL.PORT_MST_PK = JC.PORT_MST_POL_FK");
                    sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                    sb.Append("   AND PFD.PLACE_PK(+) = JC.DEL_PLACE_MST_FK");
                    sb.Append("   AND SH.CUSTOMER_MST_PK = JC.SHIPPER_CUST_MST_FK");
                    sb.Append("   AND CN.CUSTOMER_MST_PK(+) = JC.CONSIGNEE_CUST_MST_FK");
                    sb.Append("   AND NT.CUSTOMER_MST_PK(+) = JC.NOTIFY1_CUST_MST_FK");
                    sb.Append("   AND OP.AIRLINE_MST_PK(+) = JC.CARRIER_MST_FK");
                    sb.Append("   AND JC.JOB_CARD_TRN_PK = " + JobPK);
                }
            }


            return objWK.GetDataSet(sb.ToString());
        }
        /// <summary>
        /// Fetches the multiple header commodity.
        /// </summary>
        /// <param name="DOCnR">The do cn r.</param>
        /// <returns></returns>
        public DataSet FetchMultipleHeaderCommodity(string DOCnR)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append("select DISTINCT QCDT.COMM_GROUP,");
            sb.Append("                QCDT.COMMODITY,");
            sb.Append("                QCDT.CONTAINER_NR,");
            sb.Append("                QCDT.QTY,");
            sb.Append("                QCDT.LENGTH,");
            sb.Append("                QCDT.VOLUME,");
            sb.Append("                QCDT.WIDTH,");
            sb.Append("                QCDT.HEIGHT,");
            sb.Append("                QCDT.PACK_TYPE,");
            sb.Append("                QCDT.NET_WT,");
            sb.Append("                QCDT.GROSS_WT,");
            sb.Append("                QCDT.TRACK_TRACE_FK");
            sb.Append("  FROM QWFA_CARGO_DETAILS_TBL QCDT, QWFA_TRACK_TRACE_DTL TNT");
            sb.Append(" WHERE TNT.TRACK_TRACE_PK = QCDT.TRACK_TRACE_FK");
            if (!string.IsNullOrEmpty(DOCnR))
            {
                sb.Append("  AND  TNT.TRACK_TRACE_PK IN (" + DOCnR + ") ");
            }
            return objWK.GetDataSet(sb.ToString());
        }
        #endregion

        #region "Send Mail with PDF"

        /// <summary>
        /// Fetches the document.
        /// </summary>
        /// <param name="documentId">The document identifier.</param>
        /// <returns></returns>
        public int FetchDocument(string documentId)
        {
            System.Text.StringBuilder strbldrSQL = new System.Text.StringBuilder();
            WorkFlow objWK = new WorkFlow();
            int DocPk = 0;
            OracleDataReader dr = null;
            strbldrSQL.Append(" SELECT NVL(DMT.DOCUMENT_MST_PK,0) DOCUMENT_MST_PK ");
            strbldrSQL.Append(" FROM DOCUMENT_MST_TBL DMT ");
            strbldrSQL.Append(" WHERE");
            strbldrSQL.Append(" DMT.DOCUMENT_ID='" + documentId + "'");
            try
            {
                DocPk = Convert.ToInt32(objWK.ExecuteScaler(strbldrSQL.ToString()));
                return DocPk;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        /// <summary>
        /// Fetches the document detail.
        /// </summary>
        /// <param name="documentId">The document identifier.</param>
        /// <returns></returns>
        public DataTable FetchDocumentDetail(string documentId)
        {
            System.Text.StringBuilder strbldrSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strbldrSQL.Append(" SELECT DOC.DOCUMENT_SUBJECT,DOC.ATTACHMENT_URL, ");
            strbldrSQL.Append(" DOC.DOCUMENT_HEADER H1,DOC.DOCUMENT_BODY H2,DOC.DOCUMENT_FOOTER H3,' ' MSG_BODY ");
            strbldrSQL.Append(" FROM DOCUMENT_MST_TBL DOC ");
            strbldrSQL.Append(" WHERE DOC.DOCUMENT_MST_PK=" + documentId + "");
            try
            {
                return objWF.GetDataTable(strbldrSQL.ToString());

            }
            catch (Exception exp)
            {
                throw exp;
            }
        }
        #endregion
    }
}