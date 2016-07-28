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
using System.Configuration;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class clsMSTJobCardSea : CommonFeatures
	{
		#region "Fetch Master JobCard Records"
		//Sreenivas on 05/02/2010. GetMSTJCExp functionality has been written below for fetch the Master JobCard Sea Export from the database
		public int GetMSTJCExp(string mstjcRefNr, int mstjcPk, int mjseActive, int locPk)
		{
			try {
				System.Text.StringBuilder strMSTJCQuery = new System.Text.StringBuilder(5000);
				strMSTJCQuery.Append(" select mjse. master_jc_sea_exp_pk, mjse.master_jc_ref_no");
				strMSTJCQuery.Append(" from master_jc_sea_exp_tbl mjse, user_mst_tbl umt");
				strMSTJCQuery.Append(" where mjse.master_jc_ref_no like '%" + mstjcRefNr + "%'");
				strMSTJCQuery.Append(" and mjse.created_by_fk = umt.user_mst_pk");
				strMSTJCQuery.Append(" and umt.default_location_fk=" + locPk);
				if (mjseActive == 1) {
					strMSTJCQuery.Append(" and mjse.master_jc_status = 1");
				}
				WorkFlow objWF = new WorkFlow();
				DataSet objMSTJCDS = new DataSet();
				objMSTJCDS = objWF.GetDataSet(strMSTJCQuery.ToString());
				if (objMSTJCDS.Tables[0].Rows.Count == 1) {
					mstjcPk = Convert.ToInt32(objMSTJCDS.Tables[0].Rows[0][0]);
				}
				return objMSTJCDS.Tables[0].Rows.Count;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Private Variables"
		private string _MSTJCRefNumber;
			#endregion
		Quantum_QFOR.cls_SeaBookingEntry objBookingSea = new Quantum_QFOR.cls_SeaBookingEntry();

		#region "Property"
		public string MSTJCRefNumber {
			get { return _MSTJCRefNumber; }
		}

		#endregion

		#region "Fetch Master Jobcard"
		public DataSet FetchAll(string MSTJCRefNo = "", bool ActiveOnly = true, string POLPk = "", string PODPk = "", string AgentPk = "", string LinePk = "", string SearchType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string POLID = "",
		string PODId = "", string POLName = "", string PODName = "", long lngUsrLocFk = 0, string strColumnName = "", bool blnSortAscending = false, Int32 flag = 0, string VesselName = "")
		{

			Int32 last = default(Int32);
			Int32 start = default(Int32);
			string strSQL = null;
			string strCondition = null;
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
			string SrOP = (SearchType == "C" ? "%" : "");
			if (flag == 0) {
				buildCondition.Append(" AND 1=2 ");
			}
			if (MSTJCRefNo.Length > 0) {
				buildCondition.Append(" AND UPPER(M.MASTER_JC_REF_NO) LIKE '" + SrOP + MSTJCRefNo.ToUpper().Replace("'", "''") + "%'");
			}

			if (ActiveOnly) {
				buildCondition.Append(" AND M.MASTER_JC_STATUS = 1 ");
				//Else
				//    buildCondition.Append(vbCrLf & " AND M.MASTER_JC_STATUS = 2 ")
			}

			if (POLPk.Length > 0) {
				buildCondition.Append(" AND M.PORT_MST_POL_FK = " + POLPk);
			}

			if (PODPk.Length > 0) {
				buildCondition.Append(" AND M.PORT_MST_POD_FK = " + PODPk);
			}

			if (AgentPk.Length > 0) {
				buildCondition.Append(" AND M.DP_AGENT_MST_FK = " + AgentPk);
			}

			if (LinePk.Length > 0) {
				buildCondition.Append(" AND M.OPERATOR_MST_FK = " + LinePk);
			}
			if (VesselName.Length > 0) {
				buildCondition.Append(" AND Upper(VST.VESSEL_NAME) LIKE '%" + VesselName.ToUpper().Replace("'", "''") + "%' ");
			}

			buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + "");
			buildCondition.Append("  AND M.CREATED_BY_FK = UMT.USER_MST_PK ");
			strCondition = buildCondition.ToString();

			buildQuery.Append(" SELECT ");
			buildQuery.Append(" COUNT(*) ");
			buildQuery.Append(" FROM MASTER_JC_SEA_EXP_TBL M, ");
			buildQuery.Append(" PORT_MST_TBL          POL, ");
			buildQuery.Append(" PORT_MST_TBL          POD, ");
			buildQuery.Append(" AGENT_MST_TBL         AMT, ");
			buildQuery.Append(" OPERATOR_MST_TBL      OMT,USER_MST_TBL UMT, ");
			//'
			buildQuery.Append("  VESSEL_VOYAGE_TRN     VVT, ");
			buildQuery.Append("VESSEL_VOYAGE_TBL     VST ");
			//'
			buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
			buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK AND AMT.AGENT_MST_PK(+) = M.DP_AGENT_MST_FK AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK ");
			buildQuery.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+) AND  M.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
			//'
			buildQuery.Append("               " + strCondition);

			strSQL = buildQuery.ToString();

			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

			buildQuery.Remove(0, buildQuery.Length);
			// buildQuery = ""

			buildQuery.Append(" Select * from ");
			buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
			buildQuery.Append("    ( SELECT ");
			buildQuery.Append(" M.OPERATOR_MST_FK OPERATORFK,");
			//buildQuery.Append(vbCrLf & " OMT.OPERATOR_NAME OPERATORNAME, ")
			buildQuery.Append(" M.MASTER_JC_REF_NO MSTJCREFNO, ");
			buildQuery.Append(" to_date(M.MASTER_JC_DATE,'DD/MM/RRRR') MSTJCDATE, ");
			buildQuery.Append(" OMT.OPERATOR_NAME OPERATORNAME, ");
			//'
			buildQuery.Append("(CASE");
			buildQuery.Append(" WHEN (NVL(VST.VESSEL_NAME, '') || '/' ||");
			buildQuery.Append(" NVL(VVT.VOYAGE, '') = '/') THEN");
			buildQuery.Append(" ''");
			buildQuery.Append(" ELSE");
			buildQuery.Append(" NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '')");
			buildQuery.Append(" END) AS VESVOYAGE,");
			//'
			buildQuery.Append(" M.PORT_MST_POL_FK POLFK, POL.PORT_NAME POLNAME, ");
			buildQuery.Append(" M.PORT_MST_POD_FK PODFK, POD.PORT_NAME PODNAME, ");
			buildQuery.Append(" M.POL_ETD, ");
			buildQuery.Append(" M.DP_AGENT_MST_FK AGENTFK,  AMT.AGENT_NAME AGENTNAME, ");
			buildQuery.Append(" M.MASTER_JC_SEA_EXP_PK MSTJCPK, ");

			buildQuery.Append(" CASE WHEN M.CARGO_TYPE =2 THEN (SELECT SUM(JCONT.CHARGEABLE_WEIGHT) ");
			buildQuery.Append("  FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
			buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
			buildQuery.Append(" AND JC.MASTER_JC_FK =M.MASTER_JC_SEA_EXP_PK GROUP BY M.MASTER_JC_SEA_EXP_PK) ");
			buildQuery.Append(" ELSE (SELECT SUM(JCONT.NET_WEIGHT) FROM JOB_TRN_CONT JCONT,JOB_CARD_TRN JC ");
			buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
			buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_EXP_PK ");
			buildQuery.Append(" GROUP BY M.MASTER_JC_SEA_EXP_PK) END CHARGEABLE_WEIGHT,  ");

			//buildQuery.Append(vbCrLf & " (SELECT SUM(JCONT.VOLUME_IN_CBM) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ")
			//buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
			//buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_EXP_PK ")
			//buildQuery.Append(vbCrLf & " GROUP BY M.MASTER_JC_SEA_EXP_PK) AS VOLUME_IN_CBM, ")

			buildQuery.Append(" (SELECT SUM(JCONT.GROSS_WEIGHT) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
			buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
			buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_EXP_PK ");
			buildQuery.Append(" GROUP BY M.MASTER_JC_SEA_EXP_PK) AS GROSS_WEIGHT, ");

			//Modified by Faheem
			//buildQuery.Append(vbCrLf & " (SELECT SUM(JCONT.CHARGEABLE_WEIGHT) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ")
			//buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
			//buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_EXP_PK  ")
			//buildQuery.Append(vbCrLf & " GROUP BY M.MASTER_JC_SEA_EXP_PK) AS CHARGEABLE_WEIGHT ")
			//buildQuery.Append(vbCrLf & " CASE WHEN M.CARGO_TYPE =2 THEN (SELECT SUM(JCONT.CHARGEABLE_WEIGHT) ")
			//buildQuery.Append(vbCrLf & "  FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ")
			//buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
			//buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_FK =M.MASTER_JC_SEA_EXP_PK GROUP BY M.MASTER_JC_SEA_EXP_PK) ")
			//buildQuery.Append(vbCrLf & " ELSE (SELECT SUM(JCONT.NET_WEIGHT) FROM JOB_TRN_CONT JCONT,JOB_CARD_TRN JC ")
			//buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
			//buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_EXP_PK ")
			//buildQuery.Append(vbCrLf & " GROUP BY M.MASTER_JC_SEA_EXP_PK) END CHARGEABLE_WEIGHT  ")

			buildQuery.Append(" (SELECT SUM(JCONT.VOLUME_IN_CBM) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
			buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
			buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_EXP_PK ");
			buildQuery.Append(" GROUP BY M.MASTER_JC_SEA_EXP_PK) AS VOLUME_IN_CBM ");

			//Ended by Faheem
			buildQuery.Append(" FROM MASTER_JC_SEA_EXP_TBL M, ");
			buildQuery.Append(" PORT_MST_TBL          POL, ");
			buildQuery.Append(" PORT_MST_TBL          POD, ");
			buildQuery.Append(" AGENT_MST_TBL         AMT, ");
			buildQuery.Append(" OPERATOR_MST_TBL      OMT,USER_MST_TBL UMT, ");
			//'
			buildQuery.Append("  VESSEL_VOYAGE_TRN     VVT,  ");
			buildQuery.Append(" VESSEL_VOYAGE_TBL     VST ");
			//'
			buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
			buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK AND AMT.AGENT_MST_PK(+) = M.DP_AGENT_MST_FK AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK ");
			buildQuery.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+) AND M.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK(+)");
			//'
			buildQuery.Append("               " + strCondition);
			//buildQuery.Append(vbCrLf & "      ORDER BY M.MASTER_JC_DATE DESC,M.MASTER_JC_REF_NO desc ")
			//Manoharan 18June2007: to Sort the columns as per the Header click
			if (string.IsNullOrEmpty(strColumnName.Trim())) {
				buildQuery.Append(" ORDER BY M.MASTER_JC_DATE DESC,M.MASTER_JC_REF_NO desc ");
			} else {
				buildQuery.Append(" ORDER BY " + strColumnName);
			}

			if (!blnSortAscending & !string.IsNullOrEmpty(strColumnName.Trim())) {
				buildQuery.Append(" DESC");
			}
			//end
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");
			buildQuery.Append("  where  ");
			buildQuery.Append("     SR_NO between " + start + " and " + last);

			buildQuery.Append(" Order By SR_NO ");
			strSQL = buildQuery.ToString();

			DataSet DS = null;

			try {
				DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), "", "", "", "", POLID, PODId, POLName, PODName, "", ""
                , "", "2"));
				DataRelation trfRel = new DataRelation("TariffRelation", DS.Tables[0].Columns["MSTJCPK"], DS.Tables[1].Columns["MASTER_JC_FK"], true);
				DS.Relations.Add(trfRel);
				return DS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

		private DataTable FetchChildFor(string MSTJCPKs = "", string jobrefNO = "", string bookingNo = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "",
		string consignee = "", string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, bool BOOKING = false)
		{


			System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
			string strCondition = "";
			string strSQL = "";
			string strTable = "JOB_CARD_TRN";

			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
			//CONDITION COMING FROM MASTRER FUNCTION

			if (process == "2") {
				buildCondition.Append("     BOOKING_MST_TBL BK, JOB_CARD_TRN JC,");
				buildCondition.Append("     HBL_EXP_TBL HBL, ");
				buildCondition.Append("     MBL_EXP_TBL MBL, ");
				buildCondition.Append("     AGENT_MST_TBL DPA, ");
			} else {
				buildCondition.Append(" JOB_CARD_TRN JC,");
			}
			buildCondition.Append("     CUSTOMER_MST_TBL SH,");
			buildCondition.Append("     CUSTOMER_MST_TBL CO,");
			buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
			buildCondition.Append("     AGENT_MST_TBL CBA, ");
			buildCondition.Append("     AGENT_MST_TBL CLA, MASTER_JC_SEA_EXP_TBL MJ, JOB_TRN_CONT JCONT ");

			buildCondition.Append("      where ");
			// JOIN CONDITION
			if (process == "2") {
				buildCondition.Append("      BK.BOOKING_MST_PK = JC.BOOKING_MST_FK (+)");
				buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_SEA_EXP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
				buildCondition.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
				buildCondition.Append("    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
				buildCondition.Append("    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
				buildCondition.Append("    AND  JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
				buildCondition.Append("   AND JC.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK (+)");
				buildCondition.Append("   AND JC.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK (+) AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
			} else {
				buildCondition.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
				buildCondition.Append("   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
				buildCondition.Append("   AND JC.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)");
				buildCondition.Append("   AND JC.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ");
			}

			if (jobrefNO.Length > 0) {
				buildCondition.Append(" AND UPPER(JOBCARD_REF_NO) LIKE '" + jobrefNO.ToUpper().Replace("'", "''") + "%'");

			}
			if (SearchFor > 0 & SearchFortime > 0) {
				int NO = -Convert.ToInt32(SearchFor);
				System.DateTime Time = default(System.DateTime);
				switch (SearchFortime) {
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
				//TO_DATE('12/27/2005','" & dateFormat & "')
				buildCondition.Append(" AND JOBCARD_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
			}

			if (process == "2") {
				if (bookingNo.Length > 0) {
					buildCondition.Append(" AND UPPER(BOOKING_REF_NO) LIKE '" + bookingNo.ToUpper().Replace("'", "''") + "%'");
				}
				if (SearchFor > 0 & SearchFortime > 0) {
					int NO = -Convert.ToInt32(SearchFor);
					System.DateTime Time = default(System.DateTime);
					switch (SearchFortime) {
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
					buildCondition.Append(" AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
				}
				buildCondition.Append(" AND BK.STATUS=2");
			}
			if (jcStatus.Length > 0) {
				buildCondition.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
			}
			if (Hbl.Trim().Length > 0) {
				buildCondition.Append(" AND UPPER(HBL_REF_NO) LIKE '" + Hbl.ToUpper().Replace("'", "''") + "%'");
			}
			if (polID.Length > 0) {
				buildCondition.Append("       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
			}
			if (polName.Length > 0) {
				buildCondition.Append("     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
			}
			// PORT OF DISCHARGE
			if (podId.Length > 0) {
				buildCondition.Append("     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
			}
			if (podName.Length > 0) {
				buildCondition.Append("     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
			}
			// CARGO TYPE
			if (cargoType.Length > 0) {
				if (process == "2") {
					buildCondition.Append("   AND BK.CARGO_TYPE = " + cargoType);
				} else {
					buildCondition.Append("  AND JC.CARGO_TYPE = " + cargoType);
				}
			}
			if (agent.Length > 0) {
				buildCondition.Append(" AND  (UPPER(CLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "' OR UPPER(CBA.AGENT_ID) = '" + " %" + agent.ToUpper().Replace("'", "''") + "')");
			}
			if (shipper.Length > 0) {
				buildCondition.Append(" AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
			}
			if (consignee.Length > 0) {
				buildCondition.Append(" AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
			}

			//===========================================================================================================================

			if (MSTJCPKs.Trim().Length > 0) {
				buildCondition.Append(" AND JC.MASTER_JC_FK IN (" + MSTJCPKs + ") ");
			}



			strCondition = buildCondition.ToString();

			buildQuery.Append(" Select * from ");
			buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
			buildQuery.Append("    ( Select " );
			//CUST_CUSTOMER_MST_FK
			buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
			//
			if (process == "2") {
				buildQuery.Append("       JOB_CARD_TRN_PK, ");
				buildQuery.Append("       BOOKING_MST_PK, ");
				buildQuery.Append("       BOOKING_REF_NO, ");
			} else {
				buildQuery.Append("       JOB_CARD_SEA_IMP_PK, ");
				buildQuery.Append("       PORT_MST_POL_FK, ");
				buildQuery.Append("       PORT_MST_POD_FK, ");
			}
			buildQuery.Append("       JOBCARD_REF_NO, ");
			buildQuery.Append("       HBL_REF_NO, ");
			buildQuery.Append("       MBL_REF_NO, JC.MASTER_JC_FK, ");
			//buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT ")
			//buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT,")
			//Added by Faheem
			buildQuery.Append("   CASE WHEN BK.CARGO_TYPE = 2 THEN SUM(JCONT.CHARGEABLE_WEIGHT) ");
			buildQuery.Append("   ELSE SUM(JCONT.NET_WEIGHT) END AS CHARGEABLE_WEIGHT ,");
			//End
			buildQuery.Append("        SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT,SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM");
			buildQuery.Append("      from ");

			buildQuery.Append(strCondition);
			//buildQuery.Append(vbCrLf & "      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HBL_REF_NO, MBL_REF_NO, JC.MASTER_JC_FK ORDER BY JOBCARD_REF_NO ASC ")
			buildQuery.Append("      GROUP BY BK.CARGO_TYPE,JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HBL_REF_NO, MBL_REF_NO, JC.MASTER_JC_FK,jobcard_date ORDER BY JOBCARD_REF_NO desc, jobcard_date desc ");
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");
			// AND (JOB.JOB_CARD_STATUS IS NULL OR JOB.JOB_CARD_STATUS = 1 )
			// band1_SRNO = 17       :   band1_RfqSpotFK = 18    :   band1_PortPOLFK = 19    :   band1_PortPOLID = 20
			// band1_PortPOLName = 21:   band1_PortPODFK = 22    :   band1_PortPODID = 23    :   band1_PortPODName = 24
			// band1_ValidFrom = 25  :   band1_ValidTo = 26

			strSQL = buildQuery.ToString();

			WorkFlow objWF = new WorkFlow();
			DataTable dt = null;
			try {
				dt = objWF.GetDataTable(strSQL);
				int RowCnt = 0;
				int Rno = 0;
				int pk = 0;
				pk = -1;
				for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++) {
					if (Convert.ToInt32(dt.Rows[RowCnt]["MASTER_JC_FK"]) != pk) {
						pk = Convert.ToInt32(dt.Rows[RowCnt]["MASTER_JC_FK"]);
						Rno = 0;
					}
					Rno += 1;
					dt.Rows[RowCnt]["SR_NO"] = Rno;
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

		private string AllMasterPKs(DataSet ds)
		{
			Int16 RowCnt = default(Int16);
			Int16 ln = default(Int16);
			System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
			strBuilder.Append("-1,");
			for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++) {
				strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["MSTJCPK"]).Trim() + ",");
			}
			strBuilder.Remove(strBuilder.Length - 1, 1);
			return strBuilder.ToString();
		}

		public DataSet FetchForEdit(long MSTJCPK)
		{

			DataSet DS = null;
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();
			System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();

			buildQuery.Append("   SELECT M.OPERATOR_MST_FK, OMT.OPERATOR_ID, ");
			buildQuery.Append(" OMT.OPERATOR_NAME, M.MASTER_JC_REF_NO, M.MASTER_JC_STATUS, ");
			buildQuery.Append(" M.MASTER_JC_DATE, M.MASTER_JC_CLOSED_ON, ");
			buildQuery.Append(" M.PORT_MST_POL_FK, POL.PORT_ID POLID, POL.PORT_NAME POLNAME, ");
			buildQuery.Append(" M.PORT_MST_POD_FK, POD.PORT_ID PODID, POD.PORT_NAME PODNAME, ");
			buildQuery.Append(" M.DP_AGENT_MST_FK, AMT.AGENT_ID, AMT.AGENT_NAME, ");
			buildQuery.Append(" M.MASTER_JC_SEA_EXP_PK, M.VERSION_NO, M.REMARKS, ");
			buildQuery.Append(" M.VOYAGE_TRN_FK,VES.VESSEL_NAME,VOY.VOYAGE, ");
			buildQuery.Append(" M.POL_ATD,M.COMMODITY_GROUP_FK,M.POL_ETD,M.CUT_OFF_DATE,VES.VESSEL_ID,M.RELEASE,M.CARGO_TYPE ");
			buildQuery.Append(" FROM MASTER_JC_SEA_EXP_TBL M, ");
			buildQuery.Append(" PORT_MST_TBL          POL,      PORT_MST_TBL          POD, ");
			buildQuery.Append(" AGENT_MST_TBL         AMT,      OPERATOR_MST_TBL      OMT, ");
			buildQuery.Append(" VESSEL_VOYAGE_TRN     VOY,      VESSEL_VOYAGE_TBL     VES ");
			buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
			buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK ");
			buildQuery.Append(" AND AMT.AGENT_MST_PK(+) = M.DP_AGENT_MST_FK ");
			buildQuery.Append(" AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK ");
			buildQuery.Append(" AND VES.VESSEL_VOYAGE_TBL_PK(+) = VOY.VESSEL_VOYAGE_TBL_FK ");
			buildQuery.Append(" AND M.VOYAGE_TRN_FK = VOY.VOYAGE_TRN_PK(+)");
			buildQuery.Append(" AND M.MASTER_JC_SEA_EXP_PK = " + MSTJCPK);
			strSQL = buildQuery.ToString();

			try {
				DS = objWF.GetDataSet(strSQL);
				return DS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

		public DataSet FetchGridData(string MSTJCPKs = "", string jobrefNO = "", string bookingNo = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "",
		string consignee = "", string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, bool BOOKING = false, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 lblCustPk = 0)
		{


			System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
			string strCondition = "";
			string strSQL = "";
			string strTable = "JOB_CARD_TRN";
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			Int32 last = default(Int32);
			Int32 start = default(Int32);

			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
			//CONDITION COMING FROM MASTRER FUNCTION

			if (process == "2") {
				buildCondition.Append("     BOOKING_MST_TBL BK, JOB_CARD_TRN JC,");
				buildCondition.Append("     HBL_EXP_TBL HBL, ");
				buildCondition.Append("     MBL_EXP_TBL MBL, ");
				buildCondition.Append("     AGENT_MST_TBL DPA, ");
			} else {
				buildCondition.Append(" JOB_CARD_TRN JC,");
			}
			buildCondition.Append("     CUSTOMER_MST_TBL SH,");
			buildCondition.Append("     CUSTOMER_MST_TBL CO,");
			buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
			buildCondition.Append("     AGENT_MST_TBL CBA, ");
			buildCondition.Append("     AGENT_MST_TBL CLA, MASTER_JC_SEA_EXP_TBL MJ, JOB_TRN_CONT JCONT  ");

			buildCondition.Append("      where ");
			// JOIN CONDITION
			if (process == "2") {
				buildCondition.Append("      BK.BOOKING_MST_PK = JC.BOOKING_MST_FK (+)");
				buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_SEA_EXP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
				buildCondition.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
				buildCondition.Append("    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
				buildCondition.Append("    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
				buildCondition.Append("    AND  JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
				buildCondition.Append("   AND JC.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK (+)");
				buildCondition.Append("   AND JC.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK (+) AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
			} else {
				buildCondition.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
				buildCondition.Append("   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
				buildCondition.Append("   AND JC.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)");
				buildCondition.Append("   AND JC.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ");
			}

			if (jobrefNO.Length > 0) {
				buildCondition.Append(" AND UPPER(JOBCARD_REF_NO) LIKE '" + jobrefNO.ToUpper().Replace("'", "''") + "%'");

			}
			if (SearchFor > 0 & SearchFortime > 0) {
				int NO = -Convert.ToInt32(SearchFor);
				System.DateTime Time = default(System.DateTime);
				switch (SearchFortime) {
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
				//TO_DATE('12/27/2005','" & dateFormat & "')
				buildCondition.Append(" AND JOBCARD_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
			}

			if (process == "2") {
				if (bookingNo.Length > 0) {
					buildCondition.Append(" AND UPPER(BOOKING_REF_NO) LIKE '" + bookingNo.ToUpper().Replace("'", "''") + "%'");
				}
				if (SearchFor > 0 & SearchFortime > 0) {
					int NO = -Convert.ToInt32(SearchFor);
					System.DateTime Time = default(System.DateTime);
					switch (SearchFortime) {
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
					buildCondition.Append(" AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
				}
				buildCondition.Append(" AND BK.STATUS IN (2,6)");
				//'CHANGED FOR THE DTS:
			}
			if (jcStatus.Length > 0) {
				buildCondition.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
			}
			if (Hbl.Trim().Length > 0) {
				buildCondition.Append(" AND UPPER(HBL_REF_NO) LIKE '" + Hbl.ToUpper().Replace("'", "''") + "%'");
			}
			if (polID.Length > 0) {
				buildCondition.Append("       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
			}
			if (polName.Length > 0) {
				buildCondition.Append("     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
			}
			// PORT OF DISCHARGE
			if (podId.Length > 0) {
				buildCondition.Append("     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
			}
			if (podName.Length > 0) {
				buildCondition.Append("     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
			}
			// CARGO TYPE
			if (cargoType.Length > 0) {
				if (process == "2") {
					buildCondition.Append("   AND BK.CARGO_TYPE = " + cargoType);
				} else {
					buildCondition.Append("  AND JC.CARGO_TYPE = " + cargoType);
				}
			}
			if (agent.Length > 0) {
				buildCondition.Append(" AND  (UPPER(CLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "' OR UPPER(CBA.AGENT_ID) = '" + " %" + agent.ToUpper().Replace("'", "''") + "')");
			}
			if (shipper.Length > 0) {
				buildCondition.Append(" AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
			}
			if (consignee.Length > 0) {
				buildCondition.Append(" AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
			}

			//===========================================================================================================================

			if (MSTJCPKs.Trim().Length > 0) {
				buildCondition.Append(" and JC.MASTER_JC_FK IN (" + MSTJCPKs + ") ");
			}



			strCondition = buildCondition.ToString();

			buildQuery.Append(" Select COUNT(*) from ");
			buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
			buildQuery.Append("    ( Select " );
			//CUST_CUSTOMER_MST_FK
			buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
			//
			if (process == "2") {
				buildQuery.Append("       JOB_CARD_TRN_PK, ");
				buildQuery.Append("       BOOKING_MST_PK, ");
				buildQuery.Append("       BOOKING_REF_NO, ");
			} else {
				buildQuery.Append("       JOB_CARD_SEA_IMP_PK, ");
				buildQuery.Append("       PORT_MST_POL_FK, ");
				buildQuery.Append("       PORT_MST_POD_FK, ");
			}
			buildQuery.Append("       JOBCARD_REF_NO, ");
			buildQuery.Append("       HBL_REF_NO, ");
			buildQuery.Append("       MBL_REF_NO, JC.MASTER_JC_FK MASTER_JC_SEA_EXP_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ");
			//buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT ")
			buildQuery.Append("      from ");

			buildQuery.Append(strCondition);
			buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HBL_REF_NO, MBL_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ORDER BY JOBCARD_REF_NO ASC ");
			//buildQuery.Append(vbCrLf & "      ORDER BY JOBCARD_REF_NO ASC ")
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");

			strSQL = buildQuery.ToString();

			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			TotalPage = TotalRecords / M_MasterPageSize;
			if (TotalRecords % M_MasterPageSize != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage)
				CurrentPage = 1;
			if (TotalRecords == 0)
				CurrentPage = 0;

			last = CurrentPage * M_MasterPageSize;
			start = (CurrentPage - 1) * M_MasterPageSize + 1;

			buildQuery.Remove(0, buildQuery.Length);
			// buildQuery = ""

			buildQuery.Append(" Select * from ");
			buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
			buildQuery.Append("    ( Select " );
			//CUST_CUSTOMER_MST_FK
			buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
			//
			if (process == "2") {
				buildQuery.Append("       JOB_CARD_TRN_PK, ");
				buildQuery.Append("       BOOKING_MST_PK, ");
				buildQuery.Append("       BOOKING_REF_NO, ");
			} else {
				buildQuery.Append("       JOB_CARD_SEA_IMP_PK, ");
				buildQuery.Append("       PORT_MST_POL_FK, ");
				buildQuery.Append("       PORT_MST_POD_FK, ");
			}
			buildQuery.Append("       JOBCARD_REF_NO, ");
			buildQuery.Append("       HBL_REF_NO, ");
			buildQuery.Append("       MBL_REF_NO, JC.MASTER_JC_FK MASTER_JC_SEA_EXP_FK, ");
			//Modified by Faheem
			//buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK, ")
			buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, ");
			buildQuery.Append(" CASE WHEN BK.CARGO_TYPE = 2 THEN SUM(JCONT.CHARGEABLE_WEIGHT) ");
			buildQuery.Append("  ELSE SUM(JCONT.NET_WEIGHT) END AS CHARGEABLE_WEIGHT, ");
			buildQuery.Append(" JC.VERSION_NO,'' Sel,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK,  ");
			//End
			buildQuery.Append("   (select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
			//Manoharan EFS:17/04/07
			buildQuery.Append("      from ");

			buildQuery.Append(strCondition);
			buildQuery.Append("      GROUP BY BK.CARGO_TYPE,JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HBL_REF_NO, MBL_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ORDER BY JOBCARD_REF_NO ASC ");
			//buildQuery.Append(vbCrLf & "      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HBL_REF_NO, MBL_REF_NO, JC.MASTER_JC_FK,JC.JOBCARD_DATE ORDER BY JOBCARD_DATE DESC,JOBCARD_REF_NO DESC ")
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");
			buildQuery.Append("  where  ");
			buildQuery.Append("     SR_NO between " + start + " and " + last);
			// AND (JOB.JOB_CARD_STATUS IS NULL OR JOB.JOB_CARD_STATUS = 1 )
			// band1_SRNO = 17       :   band1_RfqSpotFK = 18    :   band1_PortPOLFK = 19    :   band1_PortPOLID = 20
			// band1_PortPOLName = 21:   band1_PortPODFK = 22    :   band1_PortPODID = 23    :   band1_PortPODName = 24
			// band1_ValidFrom = 25  :   band1_ValidTo = 26

			strSQL = buildQuery.ToString();

			DataSet ds = null;
			try {
				ds = objWF.GetDataSet(strSQL);
				return ds;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Afetr Attach JC Fill Grid"
		//Added by rabbani reason USS Gap
		public DataSet FetchAttachGridData(string JCPKs = "", string jobrefNO = "", string bookingNo = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "",
		string consignee = "", string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, bool BOOKING = false, Int32 CurrentPage = 0, Int32 TotalPage = 0, string MSTJCPKs = "",
		Int16 Flag = 0, Int32 lblCustPk = 0)
		{


			System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder buildConditiontab = new System.Text.StringBuilder();
			System.Text.StringBuilder buildConditionCon = new System.Text.StringBuilder();
			System.Text.StringBuilder buildConditionConJCPK = new System.Text.StringBuilder();
			System.Text.StringBuilder buildConditionConAtt = new System.Text.StringBuilder();
			System.Text.StringBuilder buildConditiontab1 = new System.Text.StringBuilder();
			System.Text.StringBuilder buildConditionCon1 = new System.Text.StringBuilder();
			System.Text.StringBuilder buildConditionConMSTJCPK1 = new System.Text.StringBuilder();
			//Dim buildCondition3 As New System.Text.StringBuilder
			string strConditiontab = "";
			string strConditionCon = "";
			string strConditionConJCPK = "";
			string strConditionConAtt = "";
			string strConditiontab1 = "";
			string strConditionCon1 = "";
			string strConditionConMSTJCPK1 = "";
			//Dim strCondition3 As String = ""
			string strSQL = "";
			string strTable = "JOB_CARD_TRN";
			Int32 TotalRecords = default(Int32);
			WorkFlow objWF = new WorkFlow();
			Int32 last = default(Int32);
			Int32 start = default(Int32);

			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
			//CONDITION COMING FROM MASTRER FUNCTION

			if (process == "2") {
				buildConditiontab.Append("     BOOKING_MST_TBL BK, JOB_CARD_TRN JC,");
				buildConditiontab.Append("     HBL_EXP_TBL HBL, ");
				buildConditiontab.Append("     MBL_EXP_TBL MBL, ");
				buildConditiontab.Append("     AGENT_MST_TBL DPA, ");
			} else {
				buildConditiontab.Append(" JOB_CARD_TRN JC,");
			}
			buildConditiontab.Append("     CUSTOMER_MST_TBL SH,");
			buildConditiontab.Append("     CUSTOMER_MST_TBL CO,");
			buildConditiontab.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
			buildConditiontab.Append("     AGENT_MST_TBL CBA, ");
			buildConditiontab.Append("     AGENT_MST_TBL CLA, JOB_TRN_CONT JCONT  ");
			if (Flag == 1) {
				buildConditiontab1.Append("     ,MASTER_JC_SEA_EXP_TBL MJ ");
			}

			buildConditionCon.Append("      where ");
			// JOIN CONDITION
			if (process == "2") {
				buildConditionCon.Append("      BK.BOOKING_MST_PK = JC.BOOKING_MST_FK (+)");
				buildConditionCon.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				buildConditionCon.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				buildConditionConAtt.Append("    AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
				buildConditionConAtt.Append("   AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");
				if (Flag == 1) {
					buildConditionCon1.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_SEA_EXP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
					buildConditionCon1.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
				}

				buildConditionCon.Append("    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
				buildConditionCon.Append("    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
				buildConditionCon.Append("    AND  JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
				buildConditionCon.Append("   AND JC.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK (+)");
				buildConditionCon.Append("   AND JC.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK (+) AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
			} else {
				buildConditionCon.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				buildConditionCon.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				buildConditionCon.Append("   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
				buildConditionCon.Append("   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
				buildConditionCon.Append("   AND JC.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)");
				buildConditionCon.Append("   AND JC.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ");
			}

			if (jobrefNO.Length > 0) {
				buildConditionCon.Append(" AND UPPER(JOBCARD_REF_NO) LIKE '" + jobrefNO.ToUpper().Replace("'", "''") + "%'");

			}
			if (SearchFor > 0 & SearchFortime > 0) {
				int NO = -Convert.ToInt32(SearchFor);
				System.DateTime Time = default(System.DateTime);
				switch (SearchFortime) {
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
				//TO_DATE('12/27/2005','" & dateFormat & "')
				buildConditionCon.Append(" AND JOBCARD_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
			}

			if (process == "2") {
				if (bookingNo.Length > 0) {
					buildConditionCon.Append(" AND UPPER(BOOKING_REF_NO) LIKE '" + bookingNo.ToUpper().Replace("'", "''") + "%'");
				}
				if (SearchFor > 0 & SearchFortime > 0) {
					int NO = -Convert.ToInt32(SearchFor);
					System.DateTime Time = default(System.DateTime);
					switch (SearchFortime) {
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
					buildConditionCon.Append(" AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
				}
				buildConditionCon.Append(" AND BK.STATUS IN (2)");
			}
			if (jcStatus.Length > 0) {
				buildConditionCon.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
			}
			if (Hbl.Trim().Length > 0) {
				buildConditionCon.Append(" AND UPPER(HBL_REF_NO) LIKE '" + Hbl.ToUpper().Replace("'", "''") + "%'");
			}
			if (polID.Length > 0) {
				buildConditionCon.Append("       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
			}
			if (polName.Length > 0) {
				buildConditionCon.Append("     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
			}
			// PORT OF DISCHARGE
			if (podId.Length > 0) {
				buildConditionCon.Append("     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
			}
			if (podName.Length > 0) {
				buildConditionCon.Append("     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
			}
			// CARGO TYPE
			if (cargoType.Length > 0) {
				if (process == "2") {
					buildConditionCon.Append("   AND BK.CARGO_TYPE = " + cargoType);
				} else {
					buildConditionCon.Append("  AND JC.CARGO_TYPE = " + cargoType);
				}
			}
			if (agent.Length > 0) {
				buildConditionCon.Append(" AND  (UPPER(CLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "' OR UPPER(CBA.AGENT_ID) = '" + " %" + agent.ToUpper().Replace("'", "''") + "')");
			}
			if (shipper.Length > 0) {
				buildConditionCon.Append(" AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
			}
			if (consignee.Length > 0) {
				buildConditionCon.Append(" AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
			}

			//===========================================================================================================================

			if (JCPKs.Trim().Length > 0) {
				buildConditionConJCPK.Append(" and JC.JOB_CARD_TRN_PK IN (" + JCPKs + ") ");
			}
			if (Flag == 1) {
				if (MSTJCPKs.Trim().Length > 0) {
					buildConditionConMSTJCPK1.Append(" and JC.MASTER_JC_FK IN (" + MSTJCPKs + ") ");
				}
			}



			strConditiontab = buildConditiontab.ToString();
			strConditionCon = buildConditionCon.ToString();
			strConditionConAtt = buildConditionConAtt.ToString();
			strConditiontab1 = buildConditiontab1.ToString();
			strConditionCon1 = buildConditionCon1.ToString();
			strConditionConJCPK = buildConditionConJCPK.ToString();
			strConditionConMSTJCPK1 = buildConditionConMSTJCPK1.ToString();
			//strCondition3 = buildCondition3.ToString

			buildQuery.Append(" Select COUNT(*) from ");
			buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
			buildQuery.Append("    ( Select " );
			//CUST_CUSTOMER_MST_FK
			buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
			//
			if (process == "2") {
				buildQuery.Append("       JOB_CARD_TRN_PK, ");
				buildQuery.Append("       BOOKING_MST_PK, ");
				buildQuery.Append("       BOOKING_REF_NO, ");
			} else {
				buildQuery.Append("       JOB_CARD_SEA_IMP_PK, ");
				buildQuery.Append("       PORT_MST_POL_FK, ");
				buildQuery.Append("       PORT_MST_POD_FK, ");
			}
			buildQuery.Append("       JOBCARD_REF_NO, ");
			buildQuery.Append("       HBL_REF_NO, ");
			buildQuery.Append("       MBL_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK");
			//buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT ")
			buildQuery.Append("      from ");

			buildQuery.Append(strConditiontab);
			buildQuery.Append(strConditionCon);
			buildQuery.Append(strConditionConAtt);
			buildQuery.Append(strConditionConJCPK);
			buildQuery.Append("AND JC.MASTER_JC_FK IS NULL");


			if (Flag == 1) {
				buildQuery.Append("union");
				buildQuery.Append("     Select " );
				//CUST_CUSTOMER_MST_FK
				buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
				//
				if (process == "2") {
					buildQuery.Append("       JOB_CARD_TRN_PK, ");
					buildQuery.Append("       BOOKING_MST_PK, ");
					buildQuery.Append("       BOOKING_REF_NO, ");
				} else {
					buildQuery.Append("       JOB_CARD_SEA_IMP_PK, ");
					buildQuery.Append("       PORT_MST_POL_FK, ");
					buildQuery.Append("       PORT_MST_POD_FK, ");
				}
				buildQuery.Append("       JOBCARD_REF_NO, ");
				buildQuery.Append("       HBL_REF_NO, ");
				buildQuery.Append("       MBL_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK");
				buildQuery.Append("      from ");

				buildQuery.Append(strConditiontab);
				buildQuery.Append(strConditiontab1);
				buildQuery.Append(strConditionCon);
				buildQuery.Append(strConditionCon1);
				buildQuery.Append(strConditionConMSTJCPK1);
			}

			//buildQuery.Append(vbCrLf & "      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HBL_REF_NO, MBL_REF_NO, JC.MASTER_JC_FK ORDER BY JOBCARD_REF_NO ASC ")
			buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HBL_REF_NO, MBL_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ORDER BY JOBCARD_REF_NO ASC  ");
			//buildQuery.Append(vbCrLf & "      ORDER BY JOBCARD_REF_NO ASC ")
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");

			strSQL = buildQuery.ToString();

			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
			TotalPage = TotalRecords / M_MasterPageSize;
			if (TotalRecords % M_MasterPageSize != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage)
				CurrentPage = 1;
			if (TotalRecords == 0)
				CurrentPage = 0;

			last = CurrentPage * M_MasterPageSize;
			start = (CurrentPage - 1) * M_MasterPageSize + 1;

			buildQuery.Remove(0, buildQuery.Length);
			// buildQuery = ""

			buildQuery.Append(" Select * from ");
			buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
			buildQuery.Append("    ( Select " );
			//CUST_CUSTOMER_MST_FK
			buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
			//
			if (process == "2") {
				buildQuery.Append("       JOB_CARD_TRN_PK, ");
				buildQuery.Append("       BOOKING_MST_PK, ");
				buildQuery.Append("       BOOKING_REF_NO, ");
			} else {
				buildQuery.Append("       JOB_CARD_SEA_IMP_PK, ");
				buildQuery.Append("       PORT_MST_POL_FK, ");
				buildQuery.Append("       PORT_MST_POD_FK, ");
			}
			buildQuery.Append("       JOBCARD_REF_NO, ");
			buildQuery.Append("       HBL_REF_NO, ");
			buildQuery.Append("       MBL_REF_NO, JC.MASTER_JC_FK, ");
			//Modified by Faheem
			//buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK")
			if (Convert.ToInt32(cargoType) == 2) {
				buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK");
			} else {
				buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.NET_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK");
			}

			buildQuery.Append("   ,(select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
			//Manoharan EFS:17/04/07
			buildQuery.Append("      from ");

			buildQuery.Append(strConditiontab);
			buildQuery.Append(strConditionCon);
			buildQuery.Append(strConditionConAtt);
			buildQuery.Append(strConditionConJCPK);
			buildQuery.Append("AND JC.MASTER_JC_FK IS NULL");
			buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HBL_REF_NO, MBL_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ");
			if (Flag == 1) {
				buildQuery.Append("union");

				buildQuery.Append("     Select " );
				//CUST_CUSTOMER_MST_FK
				buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
				//
				if (process == "2") {
					buildQuery.Append("       JOB_CARD_TRN_PK, ");
					buildQuery.Append("       BOOKING_MST_PK, ");
					buildQuery.Append("       BOOKING_REF_NO, ");
				} else {
					buildQuery.Append("       JOB_CARD_SEA_IMP_PK, ");
					buildQuery.Append("       PORT_MST_POL_FK, ");
					buildQuery.Append("       PORT_MST_POD_FK, ");
				}
				buildQuery.Append("       JOBCARD_REF_NO, ");
				buildQuery.Append("       HBL_REF_NO, ");
				buildQuery.Append("       MBL_REF_NO, JC.MASTER_JC_FK, ");
				//Modified by Faheem
				//buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ")
				if (Convert.ToInt32(cargoType) == 2) {
					buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ");
				} else {
					buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.NET_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ");
				}
				buildQuery.Append("  , (select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
				//Manoharan EFS:17/04/07
				buildQuery.Append("      from ");
				buildQuery.Append(strConditiontab);
				buildQuery.Append(strConditiontab1);
				buildQuery.Append(strConditionCon);
				buildQuery.Append(strConditionCon1);
				buildQuery.Append(strConditionConMSTJCPK1);
			}
			buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HBL_REF_NO, MBL_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ORDER BY JOBCARD_REF_NO ASC  ");
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");
			buildQuery.Append("  where  ");
			buildQuery.Append("     SR_NO between " + start + " and " + last);
			// AND (JOB.JOB_CARD_STATUS IS NULL OR JOB.JOB_CARD_STATUS = 1 )
			// band1_SRNO = 17       :   band1_RfqSpotFK = 18    :   band1_PortPOLFK = 19    :   band1_PortPOLID = 20
			// band1_PortPOLName = 21:   band1_PortPODFK = 22    :   band1_PortPODID = 23    :   band1_PortPODName = 24
			// band1_ValidFrom = 25  :   band1_ValidTo = 26

			strSQL = buildQuery.ToString();

			DataSet ds = null;
			try {
				ds = objWF.GetDataSet(strSQL);
				return ds;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		//Ended by rabbani
		#endregion

		#region "FetchExistingJCPK"
		public DataSet FetchExistingJCPK(string MSTJCPK)
		{
			string strSQL = null;

			strSQL = "SELECT JOB.JOB_CARD_TRN_PK   from JOB_CARD_TRN JOB,MASTER_JC_SEA_EXP_TBL MST where JOB.MASTER_JC_FK=MST.MASTER_JC_SEA_EXP_PK(+) AND MST.MASTER_JC_SEA_EXP_PK=" + MSTJCPK;

			try {
				WorkFlow objWF = new WorkFlow();
				return objWF.GetDataSet(strSQL);
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region " Fetch Max Ref No."
		public string FetchRefNo(string strRFQNo)
		{
			try {
				string strSQL = null;
				strSQL = "SELECT NVL(MAX(JC.JOBCARD_REF_NO),0) FROM JOB_CARD_TRN JC" + "WHERE JC.JOBCARD_REF_NO LIKE '" + strRFQNo + "/%' " + "ORDER BY JC.JOBCARD_REF_NO";
				return (new WorkFlow()).ExecuteScaler(strSQL);
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Delete in PIA"
		public string Delete_PIA(long MSTJobCardPK, OracleTransaction TRANSAC = null)
		{
			//Public Function Delete_PIA(ByRef MSTJobCardPK As Long) 
			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;
			OracleCommand ObjCommand = new OracleCommand();
			string strSql = null;
			int recAfct = 0;
			try {
				if ((TRANSAC != null)) {
					objWK.OpenConnection();
					objWK.MyConnection = TRANSAC.Connection;
					strSql = " DELETE FROM JOB_TRN_PIA P WHERE P.MJC_TRN_PIA_FK =" + MSTJobCardPK;
					var _with1 = ObjCommand;
					_with1.Connection = objWK.MyConnection;
					_with1.CommandType = CommandType.Text;
					_with1.CommandText = strSql;
					_with1.Transaction = TRANSAC;
					_with1.Parameters.Clear();
					recAfct = _with1.ExecuteNonQuery();
					if (recAfct > 0) {
						return "1";
						//TRAN.Commit()
					} else {
						return "0";
					}
				} else {
					objWK.OpenConnection();
					TRAN = objWK.MyConnection.BeginTransaction();

					//strSql = " DELETE FROM JOB_TRN_PIA P WHERE P.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK "
					//strSql &= " FROM JOB_CARD_TRN J WHERE J.MASTER_JC_FK =" & MSTJobCardPK & ") AND P.MJC_TRN_PIA_FK = " & MSTJobCardPK
					strSql = " DELETE FROM JOB_TRN_PIA P WHERE P.MJC_TRN_PIA_FK =" + MSTJobCardPK;
					var _with2 = ObjCommand;
					_with2.Connection = objWK.MyConnection;
					_with2.CommandType = CommandType.Text;
					_with2.CommandText = strSql;
					_with2.Transaction = TRAN;
					_with2.Parameters.Clear();
					recAfct = _with2.ExecuteNonQuery();
					if (recAfct > 0) {
						TRAN.Commit();
					} else {
						TRAN.Rollback();
					}
				}

			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
            return "";
		}
		#endregion

		#region "Delete in COST"
		public string Delete_COST(long MSTJobCardPK, OracleTransaction TRANSAC = null)
		{
			//Public Function Delete_PIA(ByRef MSTJobCardPK As Long) 
			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;
			OracleCommand ObjCommand = new OracleCommand();
			string strSql = null;
			int recAfct = 0;
			try {
				if ((TRANSAC != null)) {
					objWK.OpenConnection();
					objWK.MyConnection = TRANSAC.Connection;
					strSql = " DELETE FROM JOB_TRN_COST P WHERE P.MJC_TRN_COST_FK =" + MSTJobCardPK;
					var _with3 = ObjCommand;
					_with3.Connection = objWK.MyConnection;
					_with3.CommandType = CommandType.Text;
					_with3.CommandText = strSql;
					_with3.Transaction = TRANSAC;
					_with3.Parameters.Clear();
					recAfct = _with3.ExecuteNonQuery();
					if (recAfct > 0) {
						return "1";
						//TRAN.Commit()
					} else {
						return "0";
					}
				} else {
					objWK.OpenConnection();
					TRAN = objWK.MyConnection.BeginTransaction();

					//strSql = " DELETE FROM JOB_TRN_PIA P WHERE P.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK "
					//strSql &= " FROM JOB_CARD_TRN J WHERE J.MASTER_JC_FK =" & MSTJobCardPK & ") AND P.MJC_TRN_PIA_FK = " & MSTJobCardPK
					strSql = " DELETE FROM JOB_TRN_COST P WHERE P.MJC_TRN_COST_FK =" + MSTJobCardPK;
					var _with4 = ObjCommand;
					_with4.Connection = objWK.MyConnection;
					_with4.CommandType = CommandType.Text;
					_with4.CommandText = strSql;
					_with4.Transaction = TRAN;
					_with4.Parameters.Clear();
					recAfct = _with4.ExecuteNonQuery();
					if (recAfct > 0) {
						TRAN.Commit();
					} else {
						TRAN.Rollback();
					}
				}
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
            return "";
		}
		#endregion

		#region "Save Function"
		public ArrayList Save(DataSet M_DataSet, bool isEdting, string MSTJCRefNo, string Location, string EmpPk, long MSTJobCardPK, DataSet GridDS, Int32 Attach = 0, Int32 Detach = 0, string JCRefNo = "0",
		string strVoyagepk = "", string Grid_Job_Ref_Nr = "0", string sid = "", string polid = "", string podid = "")
		{

			WorkFlow objWK = new WorkFlow();

			OracleTransaction TRAN = null;
			//Dim TRAN1 As OracleTransaction
			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();
			OracleCommand SelectCommand = new OracleCommand();
			//SelectCommand.Transaction = TRAN
			objWK.MyCommand.Transaction = TRAN;

			bool chkflag = false;
			int intPKVal = 0;
			long lngI = 0;
			string MSTJCRefNum = null;
			Int32 RecAfct = default(Int32);
			//Modified by Faheem
			if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["MASTER_JC_CLOSED_ON"].ToString())) {
				try {
					//Delete_PIA(MSTJobCardPK, TRAN)
					Delete_COST(MSTJobCardPK, TRAN);
				} catch (Exception ex) {
					TRAN.Rollback();
					throw ex;
				}
			}

			//end
			//objWK.OpenConnection()
			//TRAN1 = objWK.MyConnection.BeginTransaction()
			//objWK.MyCommand.Transaction = TRAN1
			//objWK.MyCommand.Connection = objWK.MyConnection
			string VesselId = "";

			if ((M_DataSet.Tables[0].Rows[0]["VESSEL_ID"] == null) == false) {
				VesselId = Convert.ToString(getDefault(M_DataSet.Tables[0].Rows[0]["VESSEL_ID"], ""));
			}

			string VesselName = "";
			if ((M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"] == null) == false) {
				VesselName = Convert.ToString(getDefault(M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"], ""));
			}

			string Voyage = "";
			if ((M_DataSet.Tables[0].Rows[0]["VOYAGE"] == null) == false) {
				Voyage = Convert.ToString(getDefault(M_DataSet.Tables[0].Rows[0]["VOYAGE"], ""));
			}


            if ((string.IsNullOrEmpty(strVoyagepk) || strVoyagepk == "0") & !string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["VESSEL_ID"].ToString()))
            {
                strVoyagepk = "0";
                objBookingSea.ConfigurationPK = M_Configuration_PK;
                objBookingSea.CREATED_BY = M_CREATED_BY_FK;
                if (!string.IsNullOrEmpty(VesselId) & !string.IsNullOrEmpty(VesselName) & !string.IsNullOrEmpty(Voyage))
                {
                    arrMessage = objBookingSea.SaveVesselMaster(Convert.ToInt64(strVoyagepk),
                        M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"].ToString(), Convert.ToInt64(M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"]), M_DataSet.Tables[0].Rows[0]["VESSEL_ID"].ToString(), M_DataSet.Tables[0].Rows[0]["VOYAGE"].ToString(), objWK.MyCommand, Convert.ToInt64(M_DataSet.Tables[0].Rows[0]["PORT_MST_POL_FK"]),
                        M_DataSet.Tables[0].Rows[0]["PORT_MST_POD_FK"].ToString(), DateTime.MinValue, Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["POL_ETD"]), Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CUT_OFF_DATE"]), DateTime.MinValue, Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["POL_ATD"]), DateTime.MinValue);
                    M_DataSet.Tables[0].Rows[0]["VOYAGE_TRN_FK"] = strVoyagepk;
                    if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                    {
                        //TRAN1.Rollback()
                        return arrMessage;
                    }
                    else
                    {
                        arrMessage.Clear();
                    }
                }
            }

			if (string.IsNullOrEmpty(MSTJCRefNo)) {
                MSTJCRefNum = GenerateProtocolKey("MASTER JC SEA EXPORT", Convert.ToInt32(Location), Convert.ToInt32(EmpPk), DateTime.Now, "", "", polid, CREATED_BY, new WorkFlow(), sid,
                podid);
				if (MSTJCRefNum == "Protocol Not Defined.") {
					arrMessage.Add("Protocol Not Defined.");
					return arrMessage;
				} else if (MSTJCRefNum.Length > 20) {
					arrMessage.Add("Protocol should be less than 20 Characters");
					return arrMessage;
				}
			} else {
				MSTJCRefNum = MSTJCRefNo;
			}

			//objWK.OpenConnection()
			//TRAN = objWK.MyConnection.BeginTransaction()
			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();
			OracleCommand attCommand = new OracleCommand();
			//Dim SelectCommand As New OracleClient.OracleCommand
			//SelectCommand.Transaction = TRAN

			try {
				DataTable DtTbl = new DataTable();
				DataRow DtRw = null;
				int i = 0;


				var _with5 = insCommand;
				_with5.Connection = objWK.MyConnection;
				_with5.CommandType = CommandType.StoredProcedure;
				_with5.CommandText = objWK.MyUserName + ".MASTER_JC_SEA_EXP_TBL_PKG.MASTER_JC_SEA_EXP_TBL_INS";
				var _with6 = _with5.Parameters;

				insCommand.Parameters.Add("MASTER_JC_REF_NO_IN", MSTJCRefNum).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("MASTER_JC_DATE_IN", OracleDbType.Date, 20, "MASTER_JC_DATE").Direction = ParameterDirection.Input;
				insCommand.Parameters["MASTER_JC_DATE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("MASTER_JC_STATUS_IN", OracleDbType.Int32, 1, "MASTER_JC_STATUS").Direction = ParameterDirection.Input;
				insCommand.Parameters["MASTER_JC_STATUS_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("MASTER_JC_CLOSED_ON_IN", OracleDbType.Date, 20, "MASTER_JC_CLOSED_ON").Direction = ParameterDirection.Input;
				insCommand.Parameters["MASTER_JC_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("PORT_MST_POL_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POL_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("PORT_MST_POD_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POD_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("DP_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "DP_AGENT_MST_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 500, "REMARKS").Direction = ParameterDirection.Input;
				insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(CREATED_BY)).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 10, "OPERATOR_MST_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("VOYAGE_TRN_FK_IN", OracleDbType.Int32, 10, "VOYAGE_TRN_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["VOYAGE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("POL_ATD_IN", OracleDbType.Date, 20, "POL_ATD").Direction = ParameterDirection.Input;
				insCommand.Parameters["POL_ATD_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("POL_ETD_IN", OracleDbType.Date, 20, "POL_ETD").Direction = ParameterDirection.Input;
				insCommand.Parameters["POL_ETD_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CUT_OFF_DATE_IN", OracleDbType.Date, 20, "CUT_OFF_DATE").Direction = ParameterDirection.Input;
				insCommand.Parameters["CUT_OFF_DATE_IN"].SourceVersion = DataRowVersion.Current;
				//Added by Faheem
				insCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
				insCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;
				//End
				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_SEA_EXP_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;



				var _with7 = updCommand;
				_with7.Connection = objWK.MyConnection;
				_with7.CommandType = CommandType.StoredProcedure;
				_with7.CommandText = objWK.MyUserName + ".MASTER_JC_SEA_EXP_TBL_PKG.MASTER_JC_SEA_EXP_TBL_UPD";
				var _with8 = _with7.Parameters;

				updCommand.Parameters.Add("MASTER_JC_SEA_EXP_PK_IN", OracleDbType.Int32, 10, "MASTER_JC_SEA_EXP_PK").Direction = ParameterDirection.Input;
				updCommand.Parameters["MASTER_JC_SEA_EXP_PK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("MASTER_JC_REF_NO_IN", MSTJCRefNum).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("MASTER_JC_DATE_IN", OracleDbType.Date, 20, "MASTER_JC_DATE").Direction = ParameterDirection.Input;
				updCommand.Parameters["MASTER_JC_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("MASTER_JC_STATUS_IN", OracleDbType.Int32, 1, "MASTER_JC_STATUS").Direction = ParameterDirection.Input;
				updCommand.Parameters["MASTER_JC_STATUS_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("MASTER_JC_CLOSED_ON_IN", OracleDbType.Date, 20, "MASTER_JC_CLOSED_ON").Direction = ParameterDirection.Input;
				updCommand.Parameters["MASTER_JC_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("PORT_MST_POL_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POL_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("PORT_MST_POD_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POD_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("DP_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "DP_AGENT_MST_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 500, "REMARKS").Direction = ParameterDirection.Input;
				updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(LAST_MODIFIED_BY)).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 10, "OPERATOR_MST_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("VOYAGE_TRN_FK_IN", OracleDbType.Int32, 10, "VOYAGE_TRN_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["VOYAGE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("POL_ATD_IN", OracleDbType.Date, 20, "POL_ATD").Direction = ParameterDirection.Input;
				updCommand.Parameters["POL_ATD_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;


				updCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("POL_ETD_IN", OracleDbType.Date, 20, "POL_ETD").Direction = ParameterDirection.Input;
				updCommand.Parameters["POL_ETD_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("CUT_OFF_DATE_IN", OracleDbType.Date, 20, "CUT_OFF_DATE").Direction = ParameterDirection.Input;
				updCommand.Parameters["CUT_OFF_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_SEA_EXP_PK").Direction = ParameterDirection.Output;
				updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                
				var _with9 = objWK.MyDataAdapter;

				_with9.InsertCommand = insCommand;
				_with9.InsertCommand.Transaction = TRAN;

				_with9.UpdateCommand = updCommand;
				_with9.UpdateCommand.Transaction = TRAN;
				//ADDED BY SURYA PRASAD for implementing protocol rollback on 18-feb-2008
				if ((M_DataSet.GetChanges(DataRowState.Added) != null)) {
					chkflag = true;
				} else {
					chkflag = false;
				}
				//end
				RecAfct = _with9.Update(M_DataSet);

				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					//ADDED BY SURYA PRASAD for implementing protocol rollback on 18-feb-2008
					if (chkflag) {
						RollbackProtocolKey("MASTER JC SEA EXPORT", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
					}
					return arrMessage;
				} else {
					attCommand.Transaction = TRAN;
					if (Attach == 1 | Detach == 2) {
						arrMessage = AttachSave(M_DataSet, GridDS, insCommand, objWK.MyUserName, Detach, JCRefNo, Grid_Job_Ref_Nr, Attach);
						if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0)) {
							TRAN.Rollback();
							return arrMessage;
						} else {
							arrMessage.Clear();
						}
					}
					if (isEdting == false) {
						MSTJobCardPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
					}
					TRAN.Commit();
					//Modified by Faheem as to apportion the cost only when MJC is closed
					if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["MASTER_JC_CLOSED_ON"].ToString())) {
						OracleCommand ObjCommand = new OracleCommand();
						var _with10 = ObjCommand;
						_with10.Connection = objWK.MyConnection;
						_with10.CommandType = CommandType.StoredProcedure;
						//'Modified By Koteshwari on 7/5/2011
						//.CommandText = objWK.MyUserName & ".JC_COST_PIA_CALCULATION_PKG.JC_SEA_EXP_COST_PIA_CALC"
						_with10.CommandText = objWK.MyUserName + ".JC_COST_CALCULATION_PKG.JC_SEA_EXP_COST_CALC";
						//End
						var _with11 = _with10.Parameters;
						_with11.Add("MASTER_JC_SEA_EXP_PK_IN", MSTJobCardPK).Direction = ParameterDirection.Input;
						_with11.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
						_with10.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
						_with10.ExecuteNonQuery();
					}
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
					//End
				}

			} catch (OracleException oraexp) {
				//ADDED BY SURYA PRASAD for implementing protocol rollback on 18-feb-2008
				if (chkflag) {
					RollbackProtocolKey("MASTER JC SEA EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
				}
				throw oraexp;
			} catch (Exception ex) {
				//ADDED BY SURYA PRASAD for implementing protocol rollback on 18-feb-2008
				if (chkflag) {
					RollbackProtocolKey("MASTER JC SEA EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
				}
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
		//Added by rabbani reason USS Gap
		private ArrayList AttachSave(DataSet M_DataSet, DataSet GridDS, OracleCommand attCommand, string user, Int32 Detach = 0, string JCRefNo = "0", string Grid_Job_Ref_Nr = "0", Int32 Attach = 0)
		{
			int i = 0;
			int m = 0;
			string str1 = null;
			string str2 = null;
			Array arr = null;
			string strRefNr = null;

			try {
				for (i = 0; i <= GridDS.Tables[0].Rows.Count - 1; i++) {
					strRefNr = "";
					attCommand.Parameters.Clear();
					var _with12 = attCommand;
					_with12.CommandType = CommandType.StoredProcedure;
					_with12.CommandText = user + ".MASTER_JC_SEA_EXP_TBL_PKG.JOB_CARD_SEA_EXP_TBL_UPD";
					var _with13 = _with12.Parameters;
					str1 = Convert.ToString(GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]);
					str2 = Convert.ToString(str1.IndexOf("/"));
					if (Convert.ToInt32(str2) >= 0) {
						_with13.Add("JOB_CARD_SEA_EXP_PK_IN", GridDS.Tables[0].Rows[i]["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;
						_with13.Add("BOOKING_SEA_FK_IN", GridDS.Tables[0].Rows[i]["BOOKING_MST_PK"]).Direction = ParameterDirection.Input;
						//Added by Faheem
						if (GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"].ToString().IndexOf(">") == -1) {
							strRefNr = Convert.ToString(GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]);
						} else {
							strRefNr = GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"].ToString().Substring(GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"].ToString().IndexOf(">") + 1, (GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"].ToString().IndexOf("<", 2) - GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"].ToString().IndexOf(">")) - 1);
						}
						_with13.Add("JOBCARD_REF_NO_IN", strRefNr).Direction = ParameterDirection.Input;
						//End
						//.Add("JOBCARD_REF_NO_IN", GridDS.Tables(0).Rows(i).Item("JOBCARD_REF_NO")).Direction = ParameterDirection.Input
						_with13.Add("OPERATOR_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"]).Direction = ParameterDirection.Input;
						_with13.Add("DP_AGENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["DP_AGENT_MST_FK"]).Direction = ParameterDirection.Input;
						_with13.Add("VESSEL_NAME_IN", M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"]).Direction = ParameterDirection.Input;
						_with13.Add("VOYAGE_IN", M_DataSet.Tables[0].Rows[0]["VOYAGE"]).Direction = ParameterDirection.Input;
						_with13.Add("VOYAGE_FK_IN", M_DataSet.Tables[0].Rows[0]["VOYAGE_TRN_FK"]).Direction = ParameterDirection.Input;
						_with13.Add("DEPARTURE_DATE_IN", M_DataSet.Tables[0].Rows[0]["POL_ATD"]).Direction = ParameterDirection.Input;
						_with13.Add("MASTER_JC_SEA_EXP_FK_IN", M_DataSet.Tables[0].Rows[0]["MASTER_JC_SEA_EXP_PK"]).Direction = ParameterDirection.Input;
						_with13.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(LAST_MODIFIED_BY)).Direction = ParameterDirection.Input;
						_with13.Add("VERSION_NO_IN", GridDS.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
						_with13.Add("CONFIG_PK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;
						_with13.Add("ETD_DATE_IN", M_DataSet.Tables[0].Rows[0]["POL_ETD"]).Direction = ParameterDirection.Input;
						_with13.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
						attCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
					} else {
						_with13.Add("JOB_CARD_SEA_EXP_PK_IN", GridDS.Tables[0].Rows[i]["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;
						_with13.Add("BOOKING_SEA_FK_IN", GridDS.Tables[0].Rows[i]["BOOKING_MST_PK"]).Direction = ParameterDirection.Input;
						//Added by Faheem
						if (GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"].ToString().IndexOf(">") == -1) {
							strRefNr = Convert.ToString(GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]);
						} else {
							strRefNr = GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"].ToString().Substring(GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"].ToString().IndexOf(">") + 1, (GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"].ToString().IndexOf("<", 2) - GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"].ToString().IndexOf(">")) - 1);
						}
						_with13.Add("JOBCARD_REF_NO_IN", strRefNr).Direction = ParameterDirection.Input;
						//End
						//.Add("JOBCARD_REF_NO_IN", GridDS.Tables(0).Rows(i).Item("JOBCARD_REF_NO")).Direction = ParameterDirection.Input
						_with13.Add("OPERATOR_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"]).Direction = ParameterDirection.Input;
						_with13.Add("DP_AGENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["DP_AGENT_MST_FK"]).Direction = ParameterDirection.Input;
						_with13.Add("VESSEL_NAME_IN", M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"]).Direction = ParameterDirection.Input;
						_with13.Add("VOYAGE_IN", M_DataSet.Tables[0].Rows[0]["VOYAGE"]).Direction = ParameterDirection.Input;
						_with13.Add("VOYAGE_FK_IN", M_DataSet.Tables[0].Rows[0]["VOYAGE_TRN_FK"]).Direction = ParameterDirection.Input;
						_with13.Add("DEPARTURE_DATE_IN", M_DataSet.Tables[0].Rows[0]["POL_ATD"]).Direction = ParameterDirection.Input;

						//If Detach = 2 Then
						if (object.ReferenceEquals(GridDS.Tables[0].Rows[i]["MASTER_JC_SEA_EXP_FK"], "")) {
							_with13.Add("MASTER_JC_SEA_EXP_FK_IN", "").Direction = ParameterDirection.Input;
						} else {
							_with13.Add("MASTER_JC_SEA_EXP_FK_IN", GridDS.Tables[0].Rows[i]["MASTER_JC_FK"]).Direction = ParameterDirection.Input;
						}

						//Else
						//.Add("MASTER_JC_SEA_EXP_FK_IN", M_DataSet.Tables(0).Rows(0).Item("MASTER_JC_SEA_EXP_PK")).Direction = ParameterDirection.Input

						//End If

						_with13.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(LAST_MODIFIED_BY)).Direction = ParameterDirection.Input;
						_with13.Add("VERSION_NO_IN", GridDS.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
						_with13.Add("CONFIG_PK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;
						_with13.Add("ETD_DATE_IN", M_DataSet.Tables[0].Rows[0]["POL_ETD"]).Direction = ParameterDirection.Input;
						_with13.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
						attCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
					}

					_with12.ExecuteNonQuery();
					//Update Track and Trace : Amit
					if (Attach == 1) {
						str1 = Convert.ToString(GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]);
						//str2 = str1.IndexOf("/")
						//If str2 >= 0 Then ' Attach
						if (!string.IsNullOrEmpty(Grid_Job_Ref_Nr)) {
							arr = Grid_Job_Ref_Nr.Split(',');
							for (m = 0; m <= Grid_Job_Ref_Nr.Length - 1; m++) {
								if (i == m) {
									//UpdateTrackandTrace(arr(m), str1, attCommand)
									UpdateTrackandTrace(Convert.ToString(arr.GetValue(m)), strRefNr, attCommand);
								}
							}
						}
						//End If
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
		//End Rabbani
		public string SaveATDETD(Int32 MSTJobPk, string POLETD, string POLATD)
		{
			WorkFlow objWK = new WorkFlow();

			objWK.OpenConnection();

			string strSQL = "";
			string strSQL1 = "";
			System.DateTime ETDDate = default(System.DateTime);
			System.DateTime ATDDate = default(System.DateTime);


			if (!string.IsNullOrEmpty(POLETD) & !string.IsNullOrEmpty(POLATD)) {
				POLETD = Convert.ToDateTime(POLETD).Day + "\\" + Convert.ToDateTime(POLETD).Month + "\\" + Convert.ToDateTime(POLETD).Year;
				POLATD = Convert.ToDateTime(POLATD).Day + "\\" + Convert.ToDateTime(POLATD).Month + "\\" + Convert.ToDateTime(POLATD).Year;

				strSQL = "update MASTER_JC_SEA_EXP_TBL t";
				strSQL = strSQL + " set t.POL_ATD= to_date('" + POLATD + "','" + dateFormat + "')";
				strSQL = strSQL + ",t.POL_ETD=to_date('" + POLETD + "','" + dateFormat + "')";
				strSQL = strSQL + " where t.MASTER_JC_SEA_EXP_PK='" + MSTJobPk + "'";

				strSQL1 = "update JOB_CARD_TRN jc";
				strSQL1 = strSQL1 + " set jc.ETD_DATE=to_date('" + POLETD + "','" + dateFormat + "')";
				strSQL1 = strSQL1 + ", jc.DEPARTURE_DATE= to_date('" + POLATD + "','" + dateFormat + "')";
				strSQL1 = strSQL1 + " where jc.MASTER_JC_FK='" + MSTJobPk + "'";


			} else {
				if (!string.IsNullOrEmpty(POLATD)) {
					POLATD = Convert.ToDateTime(POLATD).Day + "\\" + Convert.ToDateTime(POLATD).Month + "\\" + Convert.ToDateTime(POLATD).Year;
					strSQL = "update MASTER_JC_SEA_EXP_TBL t";
					strSQL = strSQL + " set t.POL_ATD=to_date('" + POLATD + "','" + dateFormat + "')";
					strSQL = strSQL + " where t.MASTER_JC_SEA_EXP_PK='" + MSTJobPk + "'";

					strSQL1 = "update JOB_CARD_TRN jc";
					strSQL1 = strSQL1 + " set jc.DEPARTURE_DATE= to_date('" + POLATD + "','" + dateFormat + "')";
					strSQL1 = strSQL1 + " where jc.MASTER_JC_FK='" + MSTJobPk + "'";
				}

				if (!string.IsNullOrEmpty(POLETD)) {
					POLETD = Convert.ToDateTime(POLETD).Day + "\\" + Convert.ToDateTime(POLETD).Month + "\\" + Convert.ToDateTime(POLETD).Year;
					strSQL = "update MASTER_JC_SEA_EXP_TBL t";
					strSQL = strSQL + " set t.POL_ETD= to_date('" + POLETD + "','" + dateFormat + "')";
					strSQL = strSQL + " where t.MASTER_JC_SEA_EXP_PK='" + MSTJobPk + "'";

					strSQL1 = "update JOB_CARD_TRN jc";
					strSQL1 = strSQL1 + " set jc.ETD_DATE=to_date('" + POLETD + "','" + dateFormat + "')";
					strSQL1 = strSQL1 + " where jc.MASTER_JC_FK='" + MSTJobPk + "'";
				}
			}


			try {
				if (!string.IsNullOrEmpty(strSQL) & !string.IsNullOrEmpty(strSQL1)) {
					objWK.ExecuteScaler(strSQL.ToString());
					return objWK.ExecuteScaler(strSQL1.ToString());
				}
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
            return "";
		}
		#endregion

		#region "Update Track and Trace "
		//Public Function UpdateTrackandTrace(ByVal NewJobRef As String, _
		//                                    ByVal PREV_JOB_REF As String) As String
		public string UpdateTrackandTrace(string NewJobRef, string PREV_JOB_REF, OracleCommand updCommand = null)
		{
			string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "update track_n_trace_tbl t";
			strSQL = strSQL + "set t.doc_ref_no= '" + PREV_JOB_REF + "'";
			strSQL = strSQL + "where t.doc_ref_no= '" + NewJobRef + "'";
			//With updCommand
			//    .CommandType = CommandType.Text
			//    .CommandText = strSQL
			//End With
			//modifying by thiyagarajan on 24/3/09
			try {
				return Convert.ToString(objWK.ExecuteCommands(strSQL.ToString()));
				//Return updCommand.ExecuteNonQuery()
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch ATD"
		public string FetchATD(string MJCPK)
		{
			string strSQL = null;
			strSQL = "select mm.pol_atd" + "from master_jc_sea_exp_tbl mm" + "where mm.master_jc_sea_exp_pk=" + MJCPK;
			try {
				return (new WorkFlow()).ExecuteScaler(strSQL);
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch VesselVoyage"
		public DataSet FetchVesselVoyage(string VoyagePK = "0")
		{
			string strSQL = null;

			strSQL = "SELECT V.VESSEL_ID, V.VESSEL_NAME, VVT.VOYAGE, VVT.POL_ETD" + "FROM VESSEL_VOYAGE_TBL V, VESSEL_VOYAGE_TRN VVT" + "WHERE V.VESSEL_VOYAGE_TBL_PK =VVT.VESSEL_VOYAGE_TBL_FK" + "AND VVT.VOYAGE_TRN_PK =" + VoyagePK;

			try {
				WorkFlow objWF = new WorkFlow();
				return objWF.GetDataSet(strSQL);
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch MasterJobFK"
		public string FetchMSTFK(string JCPK)
		{
            WorkFlow objWK = new WorkFlow();
            string strSQL = null;
			string MSTJobFK = "0";
			strSQL = "select v.MASTER_JC_FK" + "from JOB_CARD_TRN v" + "where v.JOB_CARD_TRN_PK=" + JCPK;
			try {
				MSTJobFK = Convert.ToString(getDefault((objWK).ExecuteScaler(strSQL), 0));
				return MSTJobFK;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "MJC Release Save Function"

		public ArrayList ReleaseMJC(int MSTJobCardPK, DataSet GridDS, string MSTJobCardNr)
		{

			try {
				WorkFlow objWK = new WorkFlow();
				Int16 Execute = default(Int16);
				string RefNrs = "";
				objWK.OpenConnection();
				OracleTransaction TRAN = null;
				TRAN = objWK.MyConnection.BeginTransaction();
				objWK.MyCommand.Transaction = TRAN;
				try {
					//Modified By Koteshwari on 5.7.2011
					//Delete_PIA(MSTJobCardPK, TRAN)
					Delete_COST(MSTJobCardPK, TRAN);
					//End
				} catch (Exception ex) {
					TRAN.Rollback();
					throw ex;
				}
				var _with14 = objWK.MyCommand;
				_with14.CommandText = objWK.MyUserName + ".MASTER_JC_SEA_EXP_TBL_PKG.AUTO_CREATE_MJC_SEA_IMP";
				_with14.Connection = objWK.MyConnection;
				_with14.CommandType = CommandType.StoredProcedure;
				_with14.Transaction = TRAN;
				var _with15 = _with14.Parameters;
				_with15.Add("MJC_EXP_TBL_PK_IN", MSTJobCardPK).Direction = ParameterDirection.Input;
				_with15.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_SEA_IMP_PK").Direction = ParameterDirection.Output;
				try {
					Execute = Convert.ToInt16(_with14.ExecuteNonQuery());
				} catch (OracleException oraexp) {
					TRAN.Rollback();
					throw oraexp;
				}
				string MJImpPK = null;

				MJImpPK = Convert.ToString(_with14.Parameters["RETURN_VALUE"].Value);
				_with14.CommandText = objWK.MyUserName + ".MASTER_JC_SEA_EXP_TBL_PKG.AUTO_CREATE_JOB_CARD_SEA_IMP";
				DataRow dr = null;
				_with14.Connection = objWK.MyConnection;
				_with14.CommandType = CommandType.StoredProcedure;
				try {
					foreach (DataRow dr_loopVariable in GridDS.Tables[0].Rows) {
						dr = dr_loopVariable;
						var _with16 = _with14.Parameters;
						_with16.Clear();
						_with16.Add("HBL_EXP_TBL_PK_IN", dr["HBL_HAWB_FK"]);
						_with16.Add("HBL_REF_NO_IN", dr["HBL_REF_NO"]);
						_with16.Add("JOB_CARD_SEA_EXP_FK_IN", dr["JOB_CARD_TRN_PK"]);
						_with16.Add("MJ_SEA_IMP_FK_IN", MJImpPK);
						_with16.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_SEA_IMP_PK").Direction = ParameterDirection.Output;
						Execute = Convert.ToInt16(_with14.ExecuteNonQuery());
						if (string.IsNullOrEmpty(RefNrs)) {
							RefNrs = "'" + dr["HBL_REF_NO"] + "'";
						} else {
							RefNrs = RefNrs + ",'" + dr["HBL_REF_NO"] + "'";
						}
					}
					//Modified by Faheem as to apportion the cost only when MJC is closed
					//.CommandText = objWK.MyUserName & ".JC_COST_PIA_CALCULATION_PKG.JC_SEA_EXP_COST_PIA_CALC"
					_with14.CommandText = objWK.MyUserName + ".JC_COST_CALCULATION_PKG.JC_SEA_EXP_COST_CALC";
					_with14.Connection = objWK.MyConnection;
					_with14.CommandType = CommandType.StoredProcedure;
					_with14.Transaction = TRAN;
					var _with17 = _with14.Parameters;
					_with17.Clear();
					_with17.Add("MASTER_JC_SEA_EXP_PK_IN", MSTJobCardPK).Direction = ParameterDirection.Input;
					_with17.Add("RETURN_VALUE", OracleDbType.Varchar2, 250, "RETURN_VALUE").Direction = ParameterDirection.Output;
					//.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
					_with14.ExecuteNonQuery();
					//arrMessage.Add("All Data Saved Successfully")
					//Return arrMessage
					//End
				} catch (OracleException oraexp) {
					TRAN.Rollback();
					arrMessage.Add(oraexp.Message);
					return arrMessage;
					throw oraexp;
				}
				if (Execute > 0) {
					TRAN.Commit();
					//Push to financial system if realtime is selected
					string JCPKs = "0";
					JCPKs = GetImportJCPKs(RefNrs);
					if (JCPKs != "0") {
						Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
						ArrayList schDtls = null;
						bool errGen = false;
						if (objSch.GetSchedulerPushType() == true) {
							//QFSIService.serFinApp objPush = new QFSIService.serFinApp();
							//try {
							//	schDtls = objSch.FetchSchDtls();
							//	//'Used to Fetch the Sch Dtls
							//	objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), , , JCPKs);
							//	if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
							//		objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
							//	}
							//} catch (Exception ex) {
							//	if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
							//		objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
							//	}
							//}
						}
					}
					//*****************************************************************
					arrMessage.Add("All Data Saved Successfully");
					return arrMessage;
				} else {
					TRAN.Rollback();
				}
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
				return arrMessage;
			}
            return new ArrayList();

		}
		public ArrayList ApportionSeaExpCost(int MSTJobCardPK)
		{
			WorkFlow objWK = new WorkFlow();

			OracleTransaction TRAN = null;
			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();
			OracleCommand SelectCommand = new OracleCommand();
			objWK.MyCommand.Transaction = TRAN;
			Int32 RecAfct = default(Int32);

			try {
				//Delete_PIA(MSTJobCardPK, TRAN)
				Delete_COST(MSTJobCardPK, TRAN);
			} catch (Exception ex) {
				TRAN.Rollback();
				throw ex;
			}

			try {
				OracleCommand ObjCommand = new OracleCommand();
				var _with18 = ObjCommand;
				_with18.Connection = objWK.MyConnection;
				_with18.CommandType = CommandType.StoredProcedure;
				//Modified By Koteshwari on 7/5/2011
				//.CommandText = objWK.MyUserName & ".JC_COST_PIA_CALCULATION_PKG.JC_SEA_EXP_COST_PIA_CALC"
				_with18.CommandText = objWK.MyUserName + ".JC_COST_CALCULATION_PKG.JC_SEA_EXP_COST_CALC";
				//End
				var _with19 = _with18.Parameters;
				_with19.Add("MASTER_JC_SEA_EXP_PK_IN", MSTJobCardPK).Direction = ParameterDirection.Input;
				_with19.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				_with18.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				_with18.ExecuteNonQuery();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
				//End
			} catch (OracleException oraexp) {
				throw oraexp;
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWK.CloseConnection();
			}

		}

		private ArrayList SaveImportJobs(int MJImpPK, DataSet GridDS, OracleCommand attCommand, string user)
		{
			int i = 0;
			Array arr = null;
			string strRefNr = null;
			try {
				for (i = 0; i <= GridDS.Tables[0].Rows.Count - 1; i++) {
					attCommand.Parameters.Clear();
					var _with20 = attCommand;
					_with20.CommandType = CommandType.StoredProcedure;
					_with20.CommandText = user + ".MASTER_JC_SEA_EXP_TBL_PKG.AUTO_CREATE_JOB_CARD_SEA_IMP";
					var _with21 = _with20.Parameters;
					_with21.Add("HBL_EXP_TBL_PK_IN", GridDS.Tables[0].Rows[i]["HBL_HAWB_FK"]).Direction = ParameterDirection.Input;
					_with21.Add("HBL_REF_NO_IN", GridDS.Tables[0].Rows[i]["HBL_REF_NO"]).Direction = ParameterDirection.Input;
					//.Add("HBL_DATE_IN", GridDS.Tables(0).Rows(0).Item("POL_ATD")).Direction = ParameterDirection.Input
					_with21.Add("JOB_CARD_SEA_EXP_FK_IN", GridDS.Tables[0].Rows[0]["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;
					_with21.Add("MJ_SEA_IMP_FK_IN", Convert.ToInt64(MJImpPK)).Direction = ParameterDirection.Input;
					_with21.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					attCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
					_with20.ExecuteNonQuery();
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
		#endregion

		#region "Get Import Job Card Pks "
		public string GetImportJCPKs(string HBLNr)
		{
			StringBuilder strSQL = new StringBuilder();
			WorkFlow objWF = new WorkFlow();
			DataSet ds = null;
			string strJCPks = "";
			try {
				strSQL.Append(" SELECT 0 AS JCPK FROM DUAL");
				strSQL.Append(" UNION");
				strSQL.Append(" SELECT JCSIT.JOB_CARD_TRN_PK AS JCPK FROM JOB_CARD_TRN JCSIT");
				strSQL.Append(" WHERE JCSIT.HBL_HAWB_REF_NO in (" + HBLNr + ") AND JCSIT.JC_AUTO_MANUAL=1");
				ds = objWF.GetDataSet(strSQL.ToString());
				if (ds.Tables[0].Rows.Count > 0) {
					for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++) {
						if (string.IsNullOrEmpty(strJCPks)) {
							strJCPks = ds.Tables[0].Rows[i]["JCPK"].ToString();
						} else {
							strJCPks += "," + ds.Tables[0].Rows[i]["JCPK"].ToString();
						}
					}
				} else {
					strJCPks = "0";
				}
				return strJCPks;
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
