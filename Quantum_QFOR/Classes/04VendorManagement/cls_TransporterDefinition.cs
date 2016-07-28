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
namespace Quantum_QFOR
{
    public class cls_TransporterDefinition : CommonFeatures
	{

		#region "Fetch Function"
		public object Fetchpk(string TrnID)
		{
			string strSQL = null;
			strSQL = "Select cont.CONT_MAIN_TRANS_PK from CONT_MAIN_TRANS_TBL cont where cont.contract_no = '" + TrnID + "'";
			try {
				return (new WorkFlow()).ExecuteScaler(strSQL);
			} catch (Exception ex) {
				throw ex;
			}
		}
		public object FetchTransportpk(string TrnID)
		{
			string strSQL = null;
			strSQL = "SELECT C2.CONT_TRANS_FCL_PK FROM CONT_TRANS_FCL_TBL C2 WHERE C2.CONTRACT_NO = '" + TrnID + "'";
			try {
				return (new WorkFlow()).ExecuteScaler(strSQL);
			} catch (Exception ex) {
				throw ex;
			}
		}
		public DataSet FetchAll(string TranID = "", string TranName = "", string LocName = "", string LocID = "", int BType = 0, int CurrentBType = 0, bool ActiveOnly = true, string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0,
		Int32 TotalPage = 0, bool blnSortAscending = false)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();



			if (TranID.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and UPPER(tra.TRANSPORTER_ID) like '%" + TranID.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and UPPER(tra.TRANSPORTER_ID) like '" + TranID.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (TranName.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and UPPER(tra.TRANSPORTER_NAME) like '%" + TranName.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and UPPER(tra.TRANSPORTER_NAME) like '" + TranName.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (LocName.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and UPPER(loc.LOCATION_NAME) LIKE '%" + LocName.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and UPPER(loc.LOCATION_NAME) LIKE '" + LocName.ToUpper().Replace("'", "''") + "%'";
				}
			}

			if (LocID.Trim().Length > 0) {
				strCondition = strCondition + " and loc.LOCATION_ID = '" + LocID + "'";

			}
			if (ActiveOnly) {
				strCondition = strCondition + " and tra.ACTIVE_FLAG = 1";
			}
			if (BType != 3) {
				strCondition += " and (tra.BUSINESS_TYPE = " + BType + " OR tra.BUSINESS_TYPE = 3 )";
			}
			if (CurrentBType == 3 & BType == 3) {
				strCondition += " AND tra.BUSINESS_TYPE IN (1,2,3) ";
			} else if (CurrentBType == 3 & BType == 2) {
				strCondition += " AND tra.BUSINESS_TYPE IN (2,3) ";
			} else if (CurrentBType == 3 & BType == 1) {
				strCondition += " AND tra.BUSINESS_TYPE IN (1,3) ";
			} else {
				strCondition += " AND tra.BUSINESS_TYPE = " + CurrentBType + " ";
			}

			strSQL = "SELECT Count(*) ";
			strSQL = strSQL + " FROM LOCATION_MST_TBL loc,transporter_mst_tbl tra";
			strSQL = strSQL + " WHERE  tra.LOCATION_MST_FK =loc.LOCATION_MST_PK(+)  ";
			strSQL += strCondition;
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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
			strSQL = " SELECT * from (";
			strSQL = strSQL + "SELECT ROWNUM SR_NO,q.* FROM ";
			strSQL = strSQL + " (SELECT tra.TRANSPORTER_MST_PK, ";
			strSQL = strSQL + "tra.ACTIVE_FLAG,";
			strSQL = strSQL + " tra.TRANSPORTER_ID, ";
			strSQL = strSQL + " tra.TRANSPORTER_NAME,";
			strSQL = strSQL + "  LOC.LOCATION_NAME,";
			strSQL += " DECODE(tra.BUSINESS_TYPE,'1','Air','2','Sea','3','Both') BUSINESS_TYPE, ";
			strSQL = strSQL + " tra.VERSION_NO";
			strSQL = strSQL + " FROM LOCATION_MST_TBL loc,transporter_mst_tbl tra ";

			strSQL = strSQL + " WHERE  tra.LOCATION_MST_FK =loc.LOCATION_MST_PK(+)  ";
			strSQL = strSQL + strCondition;
			if (!strColumnName.Equals("SR_NO")) {
				strSQL += "order by " + strColumnName;
			}

			if (!blnSortAscending & !strColumnName.Equals("SR_NO")) {
				strSQL += " DESC";
			}
			strSQL = strSQL + ") q  )WHERE SR_NO  Between " + start + " and " + last;
			try {
				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "contract"
		public DataSet FetchContract(System.DateTime contractDate, System.DateTime validFrom, System.DateTime validTo, string TranID = "", string TranPk = "", string TranName = "", int zoneName = 0, string contractNo = "", int BType = 0, int CurrentBType = 0,
		bool ActiveOnly = true, bool ApproveOnly = true, string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false, long lngUsrLocFk = 0, Int32 flag = 0, Int16 Int_Wf_Status = 3)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strSQLCount = null;
			string strSQLMain = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			ApproveOnly = false;
			if (flag == 0) {
				strCondition = strCondition + " and 1=2 ";
			}
			if (contractDate != null) {
				strCondition = strCondition + " and TO_DATE(cont.CONTRACT_DATE) = TO_date('" + contractDate.Date + "','" + dateFormat + "')";
			}
			if (validTo != null & validFrom != null) {
				strCondition = strCondition + " AND ((cont.VALID_FROM >= TO_DATE('" + validFrom + "' , '" + dateFormat + "')) AND ";
				strCondition = strCondition + "    (nvl(cont.VALID_TO,TO_DATE('01/01/0001')) <= TO_DATE('" + validTo + "' , '" + dateFormat + "'))) ";
			} else if (validTo != null & validFrom == null) {
				strCondition = strCondition + "  AND ( ";
				strCondition = strCondition + "         cont.VALID_TO <= TO_DATE('" + validTo + "' , '" + dateFormat + "') ";
				strCondition = strCondition + "        OR cont.VALID_TO IS NULL ";
				strCondition = strCondition + "      ) ";
			} else if (validFrom != null & validTo == null) {
				strCondition = strCondition + "  AND ( ";
				strCondition = strCondition + "         cont.VALID_FROM >= TO_DATE('" + validFrom + "' , '" + dateFormat + "') ";
				strCondition = strCondition + "        OR cont.VALID_TO IS NULL ";
				strCondition = strCondition + "      ) ";
			}

			if (ActiveOnly == true) {
				strCondition = strCondition + " and cont.ACTIVE = 1";
			}

			if (contractNo.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and UPPER(cont.CONTRACT_NO) like '%" + contractNo.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and UPPER(cont.CONTRACT_NO) like '" + contractNo.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (TranID.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and UPPER(tra.VENDOR_ID) like '%" + TranID.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and UPPER(tra.VENDOR_ID) like '" + TranID.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (TranName.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and UPPER(tra.VENDOR_NAME) like '%" + TranName.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and UPPER(tra.VENDOR_NAME) like '" + TranName.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (zoneName != 0) {
				strCondition = strCondition + " and trn.CONT_MAIN_TRANS_FK=cont.CONT_MAIN_TRANS_PK";
				strCondition = strCondition + " and trn.TRANSPORTER_ZONES_FK = " + zoneName;
			}

			if (ApproveOnly) {
				strCondition = strCondition + " and cont.CONT_APPROVED = 1";
			}
			if (CurrentBType == 3) {
				strCondition += " AND cont.BUSINESS_TYPE IN (1,2) ";
			} else if (CurrentBType == 2) {
				strCondition += " AND cont.BUSINESS_TYPE IN (2) ";
			} else if (CurrentBType == 1) {
				strCondition += " AND cont.BUSINESS_TYPE IN (1) ";
			}


			strCondition += " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk;

			strSQL = " SELECT distinct cont.ACTIVE, ";
			strSQL = strSQL + "cont.CONT_MAIN_TRANS_PK,";
			strSQL = strSQL + " cont.CONTRACT_NO,";
			strSQL = strSQL + "tra.VENDOR_MST_PK,";
			strSQL = strSQL + " tra.VENDOR_ID, ";
			strSQL = strSQL + " tra.VENDOR_NAME,";
			strSQL = strSQL + " TO_DATE(cont.CONTRACT_DATE) CONTRACT_DATE,";
			strSQL = strSQL + " TO_DATE(cont.VALID_FROM),";
			strSQL = strSQL + " TO_DATE(cont.VALID_TO),";
			strSQL = strSQL + " cont.CONT_APPROVED CONT_STATUS,";
			strSQL += " DECODE(cont.BUSINESS_TYPE,'1','Air','2','Sea','3','Both') BUSINESS_TYPE, ";
			strSQL = strSQL + " DECODE(cont.BUSINESS_TYPE,'1','','2','LCL') Cargo,DECODE(CONT.WORKFLOW_STATUS,0,'Requested',1,'Approved',2,'Rejected')STATUS  ";
			strSQL = strSQL + " FROM vendor_mst_tbl tra,CONT_MAIN_TRANS_TBL cont,";
			strSQL = strSQL + " VENDOR_CONTACT_DTLS VCD,VENDOR_TYPE_MST_TBL VT, VENDOR_SERVICES_TRN VST, ";
			strSQL = strSQL + " CONT_TRN_TRANS trn, USER_MST_TBL UMT ";
			strSQL = strSQL + " WHERE tra.VENDOR_MST_PK=cont.TRANSPORTER_MST_FK  ";
			strSQL = strSQL + " AND TRN.CONT_MAIN_TRANS_FK=CONT.CONT_MAIN_TRANS_PK ";
			strSQL = strSQL + " AND TRA.VENDOR_MST_PK = VCD.VENDOR_MST_FK ";
			strSQL = strSQL + " AND VST.VENDOR_MST_FK = TRA.VENDOR_MST_PK ";
			strSQL = strSQL + " AND VST.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK ";
			strSQL = strSQL + " AND VT.VENDOR_TYPE_ID = 'TRANSPORTER' ";
			strSQL = strSQL + " AND cont.CREATED_BY_FK = UMT.USER_MST_PK ";

			if (Int_Wf_Status != 3) {
				strSQL = strSQL + " AND CONT.WORKFLOW_STATUS=" + Int_Wf_Status;
			}
			strSQL = strSQL + strCondition;



			//Dim last1 As Int32
			//Dim start1 As Int32
			//Dim TotalRecords1 As Int32
			string strSQL1 = null;
			string strCondition1 = null;
			//Dim strSQLCount1 As String
			//Dim strSQLMain1 As String

			if (flag == 0) {
				strCondition1 = strCondition1 + " and 1=2 ";
			}
			if (contractDate != null) {
				strCondition1 = strCondition1 + " and TO_DATE(cont.CONTRACT_DATE) = TO_date('" + contractDate.Date + "','" + dateFormat + "')";
			}
			if (validTo != null & validFrom != null) {
				strCondition1 += " AND ((TO_DATE('" + validTo + "' , '" + dateFormat + "') BETWEEN ";
				strCondition1 += "     cont.VALID_FROM AND cont.VALID_TO) OR ";
				strCondition1 += "     (TO_DATE('" + validFrom + "' , '" + dateFormat + "') BETWEEN ";
				strCondition1 += "     cont.VALID_FROM AND cont.VALID_TO) OR ";
				strCondition1 += "     (cont.VALID_TO IS NULL))";
			} else if (validTo != null & validFrom == null) {
				strCondition1 += "  AND ( ";
				strCondition1 += "         cont.VALID_TO <= TO_DATE('" + validTo + "' , '" + dateFormat + "') ";
				strCondition1 += "        OR cont.VALID_TO IS NULL ";
				strCondition1 += "      ) ";
			} else if (validFrom != null & validTo == null) {
				strCondition1 += "  AND ( ";
				strCondition1 += "         cont.VALID_FROM >= TO_DATE('" + validFrom + "' , '" + dateFormat + "') ";
				strCondition1 += "        OR cont.VALID_TO IS NULL ";
				strCondition1 += "      ) ";
			}
			if (ActiveOnly == true) {
				strCondition1 = strCondition1 + " and cont.ACTIVE = 1";
			}
			if (contractNo.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition1 = strCondition1 + " and UPPER(cont.CONTRACT_NO) like '%" + contractNo.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition1 = strCondition1 + " and UPPER(cont.CONTRACT_NO) like '" + contractNo.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (TranID.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition1 = strCondition1 + " and UPPER(tra.VENDOR_ID) like '%" + TranID.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition1 = strCondition1 + " and UPPER(tra.VENDOR_ID) like '" + TranID.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (TranName.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition1 = strCondition1 + " and UPPER(tra.VENDOR_NAME) like '%" + TranName.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition1 = strCondition1 + " and UPPER(tra.VENDOR_NAME) like '" + TranName.ToUpper().Replace("'", "''") + "%'";
				}
			}


			if (ApproveOnly) {
				strCondition1 = strCondition1 + " and CONT.CONT_STATUS = 1";

			}

			strCondition1 += "  AND VST.VENDOR_MST_FK=tra.VENDOR_MST_PK AND VST.VENDOR_TYPE_FK=VT.VENDOR_TYPE_PK AND tra.VENDOR_MST_PK=VCD.VENDOR_MST_FK ";
			strCondition1 += "   And vt.vendor_type_id = 'TRANSPORTER' ";

			//---------------------------------------------------------------------------
			strCondition1 += " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk;
			strCondition1 += " AND cont.CREATED_BY_FK = UMT.USER_MST_PK ";

			strSQL1 = "Union  ";
			strSQL1 = strSQL1 + " SELECT ACTIVE,CONT_TRANS_FCL_PK,CONTRACT_NO,VENDOR_MST_PK,VENDOR_ID,VENDOR_NAME,CONTRACT_DATE, VALID_FROM,VALID_TO,CONT_STATUS,BUSINESS_TYPE,Cargo,STATUS FROM ";
			strSQL1 = strSQL1 + " (SELECT distinct cont.ACTIVE, ";
			strSQL1 = strSQL1 + " cont.CONT_TRANS_FCL_PK,";
			strSQL1 = strSQL1 + " cont.CONTRACT_NO,";
			strSQL1 = strSQL1 + " tra.VENDOR_MST_PK,";
			strSQL1 = strSQL1 + " tra.VENDOR_ID, ";
			strSQL1 = strSQL1 + " tra.VENDOR_NAME,";
			strSQL1 = strSQL1 + " TO_DATE(cont.CONTRACT_DATE) CONTRACT_DATE,";
			strSQL1 = strSQL1 + " TO_DATE(cont.VALID_FROM) VALID_FROM,";
			strSQL1 = strSQL1 + " TO_DATE(cont.VALID_TO) VALID_TO,";
			strSQL1 = strSQL1 + " cont.CONT_STATUS,";
			strSQL1 += " 'SEA' BUSINESS_TYPE, ";
			strSQL1 = strSQL1 + " 'FCL' Cargo,DECODE(CONT.Cont_Status,0,'Requested',1,'Approved',2,'Rejected')STATUS  ";
			strSQL1 = strSQL1 + " FROM vendor_mst_tbl tra,CONT_TRANS_FCL_TBL cont,USER_MST_TBL UMT,vendor_contact_dtls VCD,vendor_type_mst_tbl vt,vendor_services_trn VST  ";
			strSQL1 = strSQL1 + " WHERE tra.VENDOR_MST_PK=cont.TRANSPORTER_MST_FK ";

			if (Int_Wf_Status != 3) {
				strSQL1 = strSQL1 + " AND cont.CONT_STATUS=" + Int_Wf_Status;
			}

			strSQL1 = strSQL1 + strCondition1;
			strSQL1 = strSQL1 + ") D ";

			if (CurrentBType == 2) {
				strSQL1 = strSQL1 + " WHERE D.CARGO='LCL' ";
			} else if (CurrentBType == 1) {
				strSQL1 = strSQL1 + " WHERE D.BUSINESS_TYPE='AIR' ";
			}


			strSQLCount = " select count(*) from (";
			strSQLCount += (strSQL + strSQL1).ToString() + ")";

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQLCount));
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

			strSQLMain = " SELECT * from (SELECT ROWNUM SR_NO, q.* FROM (";
			strSQLMain += (strSQL + strSQL1).ToString();
			strSQLMain += " )q order by CONTRACT_NO desc) WHERE SR_NO Between " + start + " and " + last;

			try {
				if (CurrentBType == 0) {
					return objWF.GetDataSet(strSQLMain);
				} else {
					return objWF.GetDataSet(strSQLMain);
				}

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		public DataSet FetchContractFcl(System.DateTime contractDate, System.DateTime validFrom, System.DateTime validTo, string TranID = "", string TranPk = "", string TranName = "", int zoneName = 0, string contractNo = "", int BType = 0, int CurrentBType = 0,
		bool ActiveOnly = true, bool ApproveOnly = true, string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, bool blnSortAscending = false, long lngUsrLocFk = 0, Int32 flag = 0, Int16 Int_Wf_Status = 3)
		{
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			ApproveOnly = false;
			if (flag == 0) {
				strCondition = strCondition + " and 1=2 ";
			}
			if (contractDate != null) {
				strCondition = strCondition + " and TO_DATE(cont.CONTRACT_DATE) = TO_date('" + contractDate.Date + "','" + dateFormat + "')";
			}

			if (validTo != null & validFrom != null) {
				//strCondition &= vbCrLf & " AND ((TO_DATE('" & validTo & "' , '" & dateFormat & "') BETWEEN "
				//strCondition &= vbCrLf & "     cont.VALID_FROM AND cont.VALID_TO) OR "
				//strCondition &= vbCrLf & "     (TO_DATE('" & validFrom & "' , '" & dateFormat & "') BETWEEN "
				//strCondition &= vbCrLf & "     cont.VALID_FROM AND cont.VALID_TO) OR "
				//strCondition &= vbCrLf & "     (cont.VALID_TO IS NULL))"
				strCondition = strCondition + " AND ((cont.VALID_FROM >= TO_DATE('" + validFrom + "' , '" + dateFormat + "')) AND ";
				strCondition = strCondition + "    (nvl(cont.VALID_TO,TO_DATE('01/01/0001')) <= TO_DATE('" + validTo + "' , '" + dateFormat + "'))) ";
			} else if (validTo != null & validFrom == null) {
				strCondition += "  AND ( ";
				strCondition += "         cont.VALID_TO <= TO_DATE('" + validTo + "' , '" + dateFormat + "') ";
				strCondition += "        OR cont.VALID_TO IS NULL ";
				strCondition += "      ) ";
			} else if (validFrom != null & validTo == null) {
				strCondition += "  AND ( ";
				strCondition += "         cont.VALID_FROM >= TO_DATE('" + validFrom + "' , '" + dateFormat + "') ";
				strCondition += "        OR cont.VALID_TO IS NULL ";
				strCondition += "      ) ";
			}
			if (ActiveOnly == true) {
				strCondition = strCondition + " and cont.ACTIVE = 1";
				//strCondition &= vbCrLf & " AND ( "
				//strCondition &= vbCrLf & "        cont.VALID_TO >= TO_DATE(SYSDATE , '" & dateFormat & "') "
				//strCondition &= vbCrLf & "       OR cont.VALID_TO IS NULL "
				//strCondition &= vbCrLf & "     ) "
			}


			if (contractNo.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and UPPER(cont.CONTRACT_NO) like '%" + contractNo.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and UPPER(cont.CONTRACT_NO) like '" + contractNo.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (TranID.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and UPPER(tra.VENDOR_ID) like '%" + TranID.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and UPPER(tra.VENDOR_ID) like '" + TranID.ToUpper().Replace("'", "''") + "%'";
				}
			}
			if (TranName.Trim().Length > 0) {
				if (SearchType == "C") {
					strCondition = strCondition + " and UPPER(tra.VENDOR_NAME) like '%" + TranName.ToUpper().Replace("'", "''") + "%'";
				} else if (SearchType == "S") {
					strCondition = strCondition + " and UPPER(tra.VENDOR_NAME) like '" + TranName.ToUpper().Replace("'", "''") + "%'";
				}
			}


			if (ApproveOnly) {
				strCondition = strCondition + " and CONT.CONT_STATUS = 1";
			}


			strCondition += "  AND VST.VENDOR_MST_FK=tra.VENDOR_MST_PK AND VST.VENDOR_TYPE_FK=VT.VENDOR_TYPE_PK AND tra.VENDOR_MST_PK=VCD.VENDOR_MST_FK ";
			strCondition += "   And vt.vendor_type_id = 'TRANSPORTER' And  VCD.ADM_LOCATION_MST_FK = " + lngUsrLocFk;

			strCondition += " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk;
			strCondition += " AND cont.CREATED_BY_FK = UMT.USER_MST_PK ";

			strSQL = "SELECT Count(distinct(cont.CONTRACT_NO)) ";
			strSQL = strSQL + " FROM vendor_mst_tbl tra,vendor_contact_dtls VCD,CONT_TRANS_FCL_TBL CONT,USER_MST_TBL UMT,vendor_type_mst_tbl vt,vendor_services_trn VST ";
			strSQL = strSQL + " WHERE tra.VENDOR_MST_PK=cont.TRANSPORTER_MST_FK  ";
			strSQL += strCondition;
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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
			strSQL = " SELECT * from (";
			strSQL = strSQL + "SELECT ROWNUM SR_NO,q.* FROM ";
			strSQL = strSQL + " (SELECT distinct cont.ACTIVE, ";
			strSQL = strSQL + "cont.CONT_TRANS_FCL_PK,";
			strSQL = strSQL + " cont.CONTRACT_NO,";
			strSQL = strSQL + "tra.VENDOR_MST_PK,";
			strSQL = strSQL + " tra.VENDOR_ID, ";
			strSQL = strSQL + " tra.VENDOR_NAME,";
			strSQL = strSQL + " TO_DATE(cont.CONTRACT_DATE) CONTRACT_DATE,";
			strSQL = strSQL + " TO_DATE(cont.VALID_FROM),";
			strSQL = strSQL + " TO_DATE(cont.VALID_TO),";
			strSQL = strSQL + " cont.CONT_STATUS,";
			strSQL += " 'SEA' BUSINESS_TYPE, ";
			strSQL = strSQL + " 'FCL' Cargo,DECODE(CONT.Cont_Status,0,'Requested',1,'Approved',2,'Rejected')STATUS  ";
			strSQL = strSQL + " FROM vendor_mst_tbl tra,CONT_TRANS_FCL_TBL cont,USER_MST_TBL UMT,vendor_contact_dtls VCD,vendor_type_mst_tbl vt,vendor_services_trn VST  ";
			strSQL = strSQL + " WHERE tra.VENDOR_MST_PK=cont.TRANSPORTER_MST_FK ";

			if (Int_Wf_Status != 3) {
				strSQL = strSQL + " AND cont.CONT_STATUS=" + Int_Wf_Status;
			}

			strSQL = strSQL + strCondition;
			if (!strColumnName.Equals("SR_NO")) {
				strSQL += "order by " + strColumnName;
			}

			if (blnSortAscending & !strColumnName.Equals("SR_NO")) {
				strSQL += " DESC";
			} else {
				strSQL += " ASC";
			}
			strSQL = strSQL + " ,cont.CONTRACT_NO desc";
			strSQL = strSQL + ") q  )WHERE SR_NO  Between " + start + " and " + last;


			try {
				return objWF.GetDataSet(strSQL);
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

