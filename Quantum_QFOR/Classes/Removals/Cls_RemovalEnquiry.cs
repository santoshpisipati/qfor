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


    public class Cls_RemovalEnquiry : CommonFeatures
	{
		public int intPKVal;

		public int hdnversion;
		//This function is called to generate the enquiry reference no.
		//Called for Enquiry on New Booking
		public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
		{
			return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
		}


		#region "Save Function"
		public ArrayList Save(Int64 enquirypk, string enquiryno, Int64 pk, Int32 movetype, string enqdt, string sertype, Int64 plrpk, Int64 pfdpk, double exceptedweight, string pickupadd1,
		string deliveryadd1, double exceptedvolume, string pickupadd2, string deliveryadd2, double exceceptedarea, string plzip, string pfzip, string piccity, string delcity, Int64 plrcountrypk,
		string enmvdt, Int64 pfdcountrpk, string enqdedt, Int64 chksurvay, Int64 chkwear, string note, string plrzip, string pfdzip, Int64 Version)
		{


			WorkFlow objWK = new WorkFlow();
			//adding by thiyagarajan on 22/1/09:TrackNTrace Task:VEK Req.
			Int32 Temp = default(Int32);
			Int32 Rowsaffected = 0;
			//end
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();

			long lngI = 0;
			Int32 RecAfct = default(Int32);
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			//Dim delCommand As New OracleClient.OracleCommand

			try {
				if (enquirypk == 0) {
					var _with1 = insCommand;
					_with1.Transaction = TRAN;
					_with1.Connection = objWK.MyConnection;
					_with1.CommandType = CommandType.StoredProcedure;
					_with1.CommandText = objWK.MyUserName + ".REM_ENQUIRY_PKG.REMOVAL_ENQUIRY_INS";
					var _with2 = _with1.Parameters;
					_with2.Clear();

					if (string.IsNullOrEmpty(enquiryno)) {
						_with2.Add("REMOVAL_ENQUIRY_REF_NO_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REMOVAL_ENQUIRY_REF_NO_IN", enquiryno).Direction = ParameterDirection.Input;

					}

					if (string.IsNullOrEmpty(enqdt)) {
						_with2.Add("REMOVAL_ENQUIRY_DATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REMOVAL_ENQUIRY_DATE_IN", enqdt).Direction = ParameterDirection.Input;

					}
					if (pk == 0) {
						_with2.Add("REMOVAL_ENQUIRY_PARTY_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REMOVAL_ENQUIRY_PARTY_FK_IN", pk).Direction = ParameterDirection.Input;

					}
					//If movetype = 0 Then
					//.Add("REMOVAL_ENQUIRY_MOVETYPE_IN", "").Direction = ParameterDirection.Input
					//Else
					_with2.Add("REMOVAL_ENQUIRY_MOVETYPE_IN", movetype).Direction = ParameterDirection.Input;

					// End If
					if (string.IsNullOrEmpty(sertype)) {
						_with2.Add("REM_M_ENQ_SERVICE_TYPE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_SERVICE_TYPE_IN", getDefault(sertype, "")).Direction = ParameterDirection.Input;

					}
					if (plrpk == 0) {
						_with2.Add("REM_M_ENQ_PLR_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PLR_FK_IN", plrpk).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(pickupadd1)) {
						_with2.Add("REM_M_ENQ_PLR_ADD1_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PLR_ADD1_IN", pickupadd1).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(pickupadd2)) {
						_with2.Add("REM_M_ENQ_PLR_ADD2_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PLR_ADD2_IN", pickupadd2).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(piccity)) {
						_with2.Add("REM_M_ENQ_PLR_CITY_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PLR_CITY_IN", piccity).Direction = ParameterDirection.Input;

					}
					if (plrcountrypk == 0) {
						_with2.Add("REM_M_ENQ_PLR_COUNTRY_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PLR_COUNTRY_FK_IN", plrcountrypk).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(enmvdt)) {
						_with2.Add("REM_M_ENQ_PLR_EMOVEDATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PLR_EMOVEDATE_IN", enmvdt).Direction = ParameterDirection.Input;

					}
					if (pfdpk == 0) {
						_with2.Add("REM_M_ENQ_PFD_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PFD_FK_IN", pfdpk).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(deliveryadd1)) {
						_with2.Add("REM_M_ENQ_PFD_ADD1_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PFD_ADD1_IN", deliveryadd1).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(deliveryadd2)) {
						_with2.Add("REM_M_ENQ_PFD_ADD2_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PFD_ADD2_IN", deliveryadd2).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(delcity)) {
						_with2.Add("REM_M_ENQ_PFD_CITY_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PFD_CITY_IN", delcity).Direction = ParameterDirection.Input;

					}
					if (pfdcountrpk == 0) {
						_with2.Add("REM_M_ENQ_PFD_COUNTRY_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PFD_COUNTRY_FK_IN", pfdcountrpk).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(enqdedt)) {
						_with2.Add("REM_M_ENQ_PFD_RDELDATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PFD_RDELDATE_IN", enqdedt).Direction = ParameterDirection.Input;

					}
					if (exceptedweight == 0) {
						_with2.Add("REM_M_ENQ_EXP_WT_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_EXP_WT_IN", exceptedweight).Direction = ParameterDirection.Input;

					}
					if (exceptedvolume == 0) {
						_with2.Add("REM_M_ENQ_EXP_VOL_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_EXP_VOL_IN", exceptedvolume).Direction = ParameterDirection.Input;

					}
					if (exceceptedarea == 0) {
						_with2.Add("REM_M_ENQ_EXP_AREA_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_EXP_AREA_IN", exceceptedarea).Direction = ParameterDirection.Input;

					}
					//If chksurvay = 0 Then
					//  .Add("REM_M_ENQ_SURVEY_REQ_IN", "").Direction = ParameterDirection.Input
					// Else
					_with2.Add("REM_M_ENQ_SURVEY_REQ_IN", chksurvay).Direction = ParameterDirection.Input;

					// End If
					//If chkwear = 0 Then
					//.Add("REM_M_ENQ_WAREHOUSE_REQ_IN", "").Direction = ParameterDirection.Input
					// Else
					_with2.Add("REM_M_ENQ_WAREHOUSE_REQ_IN", chkwear).Direction = ParameterDirection.Input;

					// End If
					if (string.IsNullOrEmpty(note)) {
						_with2.Add("REM_M_ENQ_NOTES_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_NOTES_IN", note).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(plrzip)) {
						_with2.Add("REM_M_ENQ_PLR_ZIP_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PLR_ZIP_IN", plrzip).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(pfdzip)) {
						_with2.Add("REM_M_ENQ_PFD_ZIP_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with2.Add("REM_M_ENQ_PFD_ZIP_IN", pfdzip).Direction = ParameterDirection.Input;

					}
					insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
					insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;
					//adding by thiyagarajan on 22/1/09:TrackNTrace Task:VEK Req.
					Rowsaffected = insCommand.ExecuteNonQuery();
					intPKVal = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
					//end
				} else {
					var _with3 = updCommand;
					_with3.Transaction = TRAN;
					_with3.Connection = objWK.MyConnection;
					_with3.CommandType = CommandType.StoredProcedure;
					_with3.CommandText = objWK.MyUserName + ".REM_ENQUIRY_PKG.REMOVAL_ENQUIRY_UPD";
					var _with4 = _with3.Parameters;
					_with4.Clear();

					if (enquirypk == 0) {
						_with4.Add("REMOVAL_ENQUIRY_PK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REMOVAL_ENQUIRY_PK_IN", enquirypk).Direction = ParameterDirection.Input;

					}

					if (string.IsNullOrEmpty(enquiryno)) {
						_with4.Add("REMOVAL_ENQUIRY_REF_NO_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REMOVAL_ENQUIRY_REF_NO_IN", enquiryno).Direction = ParameterDirection.Input;

					}

					if (string.IsNullOrEmpty(enqdt)) {
						_with4.Add("REMOVAL_ENQUIRY_DATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REMOVAL_ENQUIRY_DATE_IN", enqdt).Direction = ParameterDirection.Input;

					}
					if (pk == 0) {
						_with4.Add("REMOVAL_ENQUIRY_PARTY_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REMOVAL_ENQUIRY_PARTY_FK_IN", pk).Direction = ParameterDirection.Input;

					}
					//If movetype = 0 Then
					// .Add("REMOVAL_ENQUIRY_MOVETYPE_IN", "").Direction = ParameterDirection.Input
					// Else
					_with4.Add("REMOVAL_ENQUIRY_MOVETYPE_IN", movetype).Direction = ParameterDirection.Input;

					//  End If
					if (string.IsNullOrEmpty(sertype)) {
						_with4.Add("REM_M_ENQ_SERVICE_TYPE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_SERVICE_TYPE_IN", getDefault(sertype, "")).Direction = ParameterDirection.Input;

					}
					if (plrpk == 0) {
						_with4.Add("REM_M_ENQ_PLR_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PLR_FK_IN", plrpk).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(pickupadd1)) {
						_with4.Add("REM_M_ENQ_PLR_ADD1_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PLR_ADD1_IN", pickupadd1).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(pickupadd2)) {
						_with4.Add("REM_M_ENQ_PLR_ADD2_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PLR_ADD2_IN", pickupadd2).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(piccity)) {
						_with4.Add("REM_M_ENQ_PLR_CITY_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PLR_CITY_IN", piccity).Direction = ParameterDirection.Input;

					}
					if (plrcountrypk == 0) {
						_with4.Add("REM_M_ENQ_PLR_COUNTRY_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PLR_COUNTRY_FK_IN", plrcountrypk).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(enmvdt)) {
						_with4.Add("REM_M_ENQ_PLR_EMOVEDATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PLR_EMOVEDATE_IN", enmvdt).Direction = ParameterDirection.Input;

					}
					if (pfdpk == 0) {
						_with4.Add("REM_M_ENQ_PFD_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PFD_FK_IN", pfdpk).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(deliveryadd1)) {
						_with4.Add("REM_M_ENQ_PFD_ADD1_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PFD_ADD1_IN", deliveryadd1).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(deliveryadd2)) {
						_with4.Add("REM_M_ENQ_PFD_ADD2_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PFD_ADD2_IN", deliveryadd2).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(delcity)) {
						_with4.Add("REM_M_ENQ_PFD_CITY_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PFD_CITY_IN", delcity).Direction = ParameterDirection.Input;

					}
					if (pfdcountrpk == 0) {
						_with4.Add("REM_M_ENQ_PFD_COUNTRY_FK_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PFD_COUNTRY_FK_IN", pfdcountrpk).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(enqdedt)) {
						_with4.Add("REM_M_ENQ_PFD_RDELDATE_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PFD_RDELDATE_IN", enqdedt).Direction = ParameterDirection.Input;

					}
					if (exceptedweight == 0) {
						_with4.Add("REM_M_ENQ_EXP_WT_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_EXP_WT_IN", exceptedweight).Direction = ParameterDirection.Input;

					}
					if (exceptedvolume == 0) {
						_with4.Add("REM_M_ENQ_EXP_VOL_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_EXP_VOL_IN", exceptedvolume).Direction = ParameterDirection.Input;

					}
					if (exceceptedarea == 0) {
						_with4.Add("REM_M_ENQ_EXP_AREA_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_EXP_AREA_IN", exceceptedarea).Direction = ParameterDirection.Input;

					}
					//If chksurvay = 0 Then
					//    .Add("REM_M_ENQ_SURVEY_REQ_IN", "").Direction = ParameterDirection.Input
					//Else
					_with4.Add("REM_M_ENQ_SURVEY_REQ_IN", chksurvay).Direction = ParameterDirection.Input;

					//End If
					//If chkwear = 0 Then
					//    .Add("REM_M_ENQ_WAREHOUSE_REQ_IN", "").Direction = ParameterDirection.Input
					//Else
					_with4.Add("REM_M_ENQ_WAREHOUSE_REQ_IN", chkwear).Direction = ParameterDirection.Input;

					//End If
					if (string.IsNullOrEmpty(note)) {
						_with4.Add("REM_M_ENQ_NOTES_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_NOTES_IN", note).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(plrzip)) {
						_with4.Add("REM_M_ENQ_PLR_ZIP_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PLR_ZIP_IN", plrzip).Direction = ParameterDirection.Input;

					}
					if (string.IsNullOrEmpty(pfdzip)) {
						_with4.Add("REM_M_ENQ_PFD_ZIP_IN", "").Direction = ParameterDirection.Input;
					} else {
						_with4.Add("REM_M_ENQ_PFD_ZIP_IN", pfdzip).Direction = ParameterDirection.Input;

					}
					updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
					updCommand.Parameters.Add("VERSION_NO_IN", Version).Direction = ParameterDirection.Input;
					updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
					updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;
					//adding by thiyagarajan on 22/1/09:TrackNTrace Task:VEK Req.
					Rowsaffected = updCommand.ExecuteNonQuery();
					intPKVal = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
				}
				//adding by thiyagarajan on 22/1/09:TrackNTrace Task:VEK Req.
				if (Rowsaffected <= 0) {
					TRAN.Rollback();
					return arrMessage;
				} else {
					//chk whether it is already inserted or not
					if (enquirypk <= 0) {
						SaveInTrackNTrace(enquiryno, intPKVal, "Enquiry Generated", 1, insCommand, TRAN);
						//modified by suryaprasad for impl session management
					}
				}
				//end
				arrMessage.Add("All Data Saved Successfully");
				TRAN.Commit();
				return arrMessage;
			} catch (OracleException oraexp) {
				TRAN.Rollback();
				if (enquirypk == 0) {
                    RollbackProtocolKey("REMOVAL CREDIT NOTE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), enquiryno, System.DateTime.Now);
                }
				throw oraexp;
			} catch (Exception ex) {
				if (enquirypk == 0) {
                    RollbackProtocolKey("REMOVAL CREDIT NOTE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), enquiryno, System.DateTime.Now);
                }
				throw ex;
			} finally {
				objWK.CloseConnection();
				//added by surya prasad for implementing session management task
			}
		}
		//adding by thiyagarajan on 22/1/09:TrackNTrace Task:VEK Req.
		public Int32 SaveInTrackNTrace(string refno, Int32 refpk, string status, Int32 Doctype, OracleCommand InsCommand, OracleTransaction TRAN = null)
		{
			System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			Int32 Return_value = default(Int32);
			try {
				objWF.OpenConnection();
				objWF.MyConnection = TRAN.Connection;
				InsCommand.Connection = objWF.MyConnection;
				InsCommand.Transaction = TRAN;
				InsCommand.CommandType = CommandType.StoredProcedure;
				InsCommand.CommandText = objWF.MyUserName + ".REM_TRACK_N_TRACE_PKG.REM_TRACK_N_TRACE_INS";
				var _with5 = InsCommand.Parameters;
				_with5.Clear();
				_with5.Add("REF_NO_IN", refno).Direction = ParameterDirection.Input;
				_with5.Add("REF_FK_IN", refpk).Direction = ParameterDirection.Input;
				_with5.Add("LOC_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
				_with5.Add("STATUS_IN", status).Direction = ParameterDirection.Input;
				_with5.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
				_with5.Add("DOCTYPE_IN", Doctype).Direction = ParameterDirection.Input;
				Return_value = InsCommand.ExecuteNonQuery();
				return Return_value;
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		//end
		#endregion

		#region "Fetch"
		public DataSet FetchEnquiryLisitng(string Enquiry_Nr, long party_fk, string MoveType = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string blnSortAscending = "", Int32 flag = 0)
		{


			WorkFlow objWK = new WorkFlow();
			StringBuilder strQuery = new StringBuilder();
			StringBuilder strQry = new StringBuilder();
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			try {
				if (flag == 0) {
					strCondition += " AND 1=2";
				}
				if (Enquiry_Nr.Trim().Length > 0) {
					if (SearchType == "C") {
						strCondition = strCondition + " And upper(ENQMST.REM_M_ENQ_REF_NR) like '%" + Enquiry_Nr.ToUpper().Replace("'", "''").Trim() + "%' " ;
					} else {
						strCondition = strCondition + " And upper(ENQMST.REM_M_ENQ_REF_NR) like '" + Enquiry_Nr.ToUpper().Replace("'", "''").Trim() + "%' " ;
					}
				}
				if (party_fk > 0) {
					strCondition = strCondition + "AND  ENQMST.REM_M_ENQ_PARTY_FK = " + party_fk;
				}

				if (MoveType == "Domestic") {
					strCondition = strCondition + " and ENQMST.REM_M_ENQ_MOVE_TYPE = 0";
				} else if (MoveType == "European") {
					strCondition = strCondition + " and ENQMST.REM_M_ENQ_MOVE_TYPE = 1 ";
				} else if (MoveType == "Overseas") {
					strCondition = strCondition + " and ENQMST.REM_M_ENQ_MOVE_TYPE = 2";
				}
				strQuery.Append(" SELECT COUNT(*)   ");
				strQuery.Append(" FROM (SELECT ROWNUM SLNR ,");
				strQuery.Append("     QTY.REM_M_ENQ_MST_TBL_PK,");
				strQuery.Append("     QTY.REM_M_ENQ_REF_NR,");
				strQuery.Append("     TO_DATE(QTY.REM_M_ENQ_DATE,dateformat),");
				strQuery.Append("     QTY.CUSTOMER_NAME,");
				strQuery.Append("     QTY.MOVE_TYPE,");
				strQuery.Append("     QTY.DELFLAG FROM");
				strQuery.Append("     (SELECT ");
				strQuery.Append("     ENQMST.REM_M_ENQ_MST_TBL_PK,");
				strQuery.Append("     ENQMST.REM_M_ENQ_REF_NR,");
				strQuery.Append("     ENQMST.REM_M_ENQ_DATE,");
				strQuery.Append("     PTY.CUSTOMER_NAME,");
				strQuery.Append("     DECODE(ENQMST.REM_M_ENQ_MOVE_TYPE,0,'Domestic',1,'European',2,'Overseas') MOVE_TYPE,");
				strQuery.Append("     '' DELFLAG");
				strQuery.Append("     FROM");
				strQuery.Append("     REM_M_ENQUIRY_MST_TBL ENQMST,");
				strQuery.Append("     CUSTOMER_MST_TBL PTY");
				strQuery.Append("     WHERE PTY.CUSTOMER_MST_PK = ENQMST.REM_M_ENQ_PARTY_FK ");
				strQuery.Append(" " + strCondition + ")QTY)");

				TotalRecords = Convert.ToInt32(objWK.ExecuteScaler(strQuery.ToString()));
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

				strQry.Append(" select  * from ( ");
				strQry.Append(" SELECT ROWNUM SLNR ,");
				strQry.Append("  QTY.REM_M_ENQ_MST_TBL_PK,");
				strQry.Append("  QTY.REM_M_ENQ_REF_NR,");
				strQry.Append("  QTY.ENQUIRY_DT,");
				strQry.Append("  QTY.CUSTOMER_NAME,");
				strQry.Append("  QTY.MOVE_TYPE,");
				strQry.Append("  QTY.DELFLAG FROM");
				strQry.Append("  (SELECT ");
				strQry.Append("  ENQMST.REM_M_ENQ_MST_TBL_PK,");
				strQry.Append("  ENQMST.REM_M_ENQ_REF_NR,");
				//strQry.Append("  TO_DATE(ENQMST.REM_M_ENQ_DATE,dateformat) ENQUIRY_DT,")
				strQry.Append("  TO_CHAR(ENQMST.REM_M_ENQ_DATE,'' || DATEFORMAT || '') ENQUIRY_DT,");
				// strSQL &= vbCrLf & "TO_CHAR(ENQ.ENQUIRY_DATE,'' || DATEFORMAT || '') AS ""ENQDATE"","
				strQry.Append("  PTY.CUSTOMER_NAME,");
				strQry.Append("   DECODE(ENQMST.REM_M_ENQ_MOVE_TYPE,0,'Domestic',1,'European',2,'Overseas') MOVE_TYPE,");
				strQry.Append("      '' DELFLAG");
				strQry.Append("  FROM");
				strQry.Append("  REM_M_ENQUIRY_MST_TBL ENQMST,");
				strQry.Append("  CUSTOMER_MST_TBL PTY");
				strQry.Append("  WHERE PTY.CUSTOMER_MST_PK = ENQMST.REM_M_ENQ_PARTY_FK");
				//strQry.Append(strCondition)
				strQry.Append(" " + strCondition + "");
				//strQry.Append(" & strCondition & ")
				//If Not (strColumnName.Equals("SLNR") Or strColumnName = "") Then
				strQry.Append(" order by " + strColumnName + " " + blnSortAscending + ")QTY)");
				//End If
				strQry.Append(" WHERE SLNR  Between " + start + " and " + last + "");
				return (objWK.GetDataSet(strQry.ToString()));
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "fetch MoveType"
		public DataSet fetchMoveType()
		{
			WorkFlow objWK = new WorkFlow();
			StringBuilder strQuery = new StringBuilder();
			try {
				strQuery.Append("select 0 MOVE_TYPE_PK,'All' MOve_code from dual ");
				strQuery.Append("union ");
				strQuery.Append(" select 1 MOVE_TYPE_PK,'Domestic' Move_code from dual ");
				strQuery.Append(" union ");
				strQuery.Append(" select 2 MOVE_TYPE_PK,'European' Move_code from dual  ");
				strQuery.Append(" union ");
				strQuery.Append(" select 3 MOVE_TYPE_PK,'Overseas' Move_code from dual ");

				return (objWK.GetDataSet(strQuery.ToString()));
			//'Manjunath  PTS ID:Sep-02  28/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "GET DATA"
		public DataSet GETDATA(string PK = "")
		{
			try {
				string strSql = null;
				strSql = strSql + "SELECT ";
				strSql = strSql + "ENQMST.REM_M_ENQ_MST_TBL_PK,";
				strSql = strSql + "ENQMST.REM_M_ENQ_REF_NR,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PARTY_FK,";
				strSql = strSql + "PTY.CUSTOMER_ID,";
				strSql = strSql + "PTY.CUSTOMER_NAME,";
				strSql = strSql + "ENQMST.REM_M_ENQ_SERVICE_TYPE,";
				strSql = strSql + "ENQMST.REM_M_ENQ_DATE,";
				strSql = strSql + "ENQMST.REM_M_ENQ_MOVE_TYPE,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PLR_FK,";
				strSql = strSql + "PMT.PLACE_CODE PLR,";
				strSql = strSql + "PMT.PLACE_NAME,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PFD_FK,";
				strSql = strSql + "PMTT.PLACE_CODE PFD,";
				strSql = strSql + "PMTT.PLACE_NAME PLACENAME,";
				strSql = strSql + "ENQMST.REM_M_ENQ_EXP_WT,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PLR_ADD1,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PFD_ADD1,";
				strSql = strSql + "ENQMST.REM_M_ENQ_EXP_VOL,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PLR_ADD2,";
				strSql = strSql + "ENQMST.Rem_m_Enq_Pfd_Add1,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PFD_ADD2,";
				strSql = strSql + "ENQMST.REM_M_ENQ_EXP_AREA,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PLR_CITY,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PFD_CITY,";
				strSql = strSql + "ENQMST.REM_M_ENQ_SURVEY_REQ,";
				strSql = strSql + "ENQMST.REM_M_ENQ_WAREHOUSE_REQ,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PLR_COUNTRY_FK,";
				strSql = strSql + "CMT.COUNTRY_ID PLRCONTRY,";
				strSql = strSql + "CMT.COUNTRY_NAME,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PFD_COUNTRY_FK,";
				strSql = strSql + "CMTT.COUNTRY_ID PFDCOUNTRY,";
				strSql = strSql + "CMTT.COUNTRY_NAME PFCOUNTRYNAME,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PLR_EMOVEDATE,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PFD_RDELDATE,";
				strSql = strSql + "ENQMST.REM_M_ENQ_NOTES,";
				strSql = strSql + "ENQMST.Version,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PLR_ZIP,";
				strSql = strSql + "ENQMST.REM_M_ENQ_PFD_ZIP,";
				strSql = strSql + "ENQMST.Rem_m_Enq_Survey_Req,";
				strSql = strSql + "ENQMST.Rem_m_Enq_Warehouse_Req,";
				strSql = strSql + "ENQMST.REM_M_ENQ_REF_TO ";
				strSql = strSql + " FROM REM_M_ENQUIRY_MST_TBL ENQMST,";
				strSql = strSql + " PLACE_MST_TBL PMT,PLACE_MST_TBL PMTT,COUNTRY_MST_TBL CMT, COUNTRY_MST_TBL CMTT,CUSTOMER_MST_TBL PTY";
				strSql = strSql + " WHERE PMT.PLACE_PK=ENQMST.REM_M_ENQ_PLR_FK";
				strSql = strSql + " AND PMTT.PLACE_PK=ENQMST.REM_M_ENQ_PFD_FK";
				strSql = strSql + " AND CMT.COUNTRY_MST_PK=ENQMST.REM_M_ENQ_PLR_COUNTRY_FK";
				strSql = strSql + " AND CMTT.COUNTRY_MST_PK=ENQMST.REM_M_ENQ_PFD_COUNTRY_FK";
				strSql = strSql + " AND PTY.CUSTOMER_MST_PK=ENQMST.REM_M_ENQ_PARTY_FK";
				strSql = strSql + " AND ENQMST.REM_M_ENQ_MST_TBL_PK = '" + PK + "'";
				WorkFlow objWF = new WorkFlow();
				return objWF.GetDataSet(strSql);
			//'Manjunath  PTS ID:Sep-02  23/09/2011
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion


	}
}
