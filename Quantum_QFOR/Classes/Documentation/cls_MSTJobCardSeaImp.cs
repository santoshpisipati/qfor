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
    public class clsMSTJobCardSeaImp : CommonFeatures
	{
		#region "Fetch Master Jobcard Sea Import"
		public int GetMJCSICount(string mjcsiRefNr, int mjcsiPk, int mjsiActive, long locPk)
		{
			try {
				System.Text.StringBuilder strMJCSIQuery = new System.Text.StringBuilder(5000);
				strMJCSIQuery.Append(" select mjsi.master_jc_sea_imp_pk, mjsi.master_jc_ref_no");
				strMJCSIQuery.Append(" from  master_jc_sea_imp_tbl mjsi , user_mst_tbl umt");
				strMJCSIQuery.Append(" where mjsi.master_jc_ref_no like '%" + mjcsiRefNr + "%'");
				strMJCSIQuery.Append(" and mjsi.created_by_fk = umt.user_mst_pk");
				strMJCSIQuery.Append(" and umt.default_location_fk=" + locPk);
				if (mjsiActive == 1) {
					strMJCSIQuery.Append(" and mjsi.master_jc_status = 1");
				}
				WorkFlow objWF = new WorkFlow();
				DataSet objMJCSIDS = new DataSet();
				objMJCSIDS = objWF.GetDataSet(strMJCSIQuery.ToString());
                if (objMJCSIDS.Tables[0].Rows.Count > 0)
                {
                    mjcsiPk = Convert.ToInt32(objMJCSIDS.Tables[0].Rows[0][0]);
                }
				return objMJCSIDS.Tables[0].Rows.Count;
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
		string PODId = "", string POLName = "", string PODName = "", long lngUsrLocFk = 0, Int32 flag = 0, int IsAdmin = 0)
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
			//If BlankGrid = 0 Then
			//    buildCondition.Append(vbCrLf & " AND 1=2 ")
			//End If
			if (flag == 0) {
				buildCondition.Append(" AND 1=2 ");
			}
			if (MSTJCRefNo.Length > 0) {
				buildCondition.Append(" AND UPPER(M.MASTER_JC_REF_NO) LIKE '" + SrOP + MSTJCRefNo.ToUpper().Replace("'", "''") + "%'");
			}

			if (ActiveOnly == true) {
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
				buildCondition.Append(" AND M.LOAD_AGENT_MST_FK = " + AgentPk);
			}

			if (LinePk.Length > 0) {
				buildCondition.Append(" AND M.OPERATOR_MST_FK = " + LinePk);
			}
			//Modified by Faheem
			//buildCondition.Append(vbCrLf & " AND UMT.DEFAULT_LOCATION_FK = " & lngUsrLocFk & "")
			//buildCondition.Append(vbCrLf & "  AND M.CREATED_BY_FK = UMT.USER_MST_PK ")
			if (IsAdmin == 0) {
				buildCondition.Append(" AND POD.LOCATION_MST_FK = " + lngUsrLocFk + "");
			}

			strCondition = buildCondition.ToString();

			buildQuery.Append(" SELECT ");
			buildQuery.Append(" COUNT(*) ");
			buildQuery.Append(" FROM MASTER_JC_SEA_IMP_TBL M, ");
			buildQuery.Append(" PORT_MST_TBL          POL, ");
			buildQuery.Append(" PORT_MST_TBL          POD, ");
			buildQuery.Append(" AGENT_MST_TBL         AMT, ");
			//'
			buildQuery.Append("  VESSEL_VOYAGE_TRN     VVT, ");
			buildQuery.Append("   VESSEL_VOYAGE_TBL     VST, ");
			//'
			//buildQuery.Append(vbCrLf & " OPERATOR_MST_TBL      OMT,USER_MST_TBL UMT ")
			buildQuery.Append(" OPERATOR_MST_TBL      OMT ");
			buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
			buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK ");
			buildQuery.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+) AND M.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK(+) ");
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
			buildQuery.Append(" to_date(M.MASTER_JC_DATE,'" + dateFormat + "') MSTJCDATE, ");
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
			buildQuery.Append("  VVT.POL_ETD, ");
			//'
			buildQuery.Append(" M.LOAD_AGENT_MST_FK AGENTFK,  AMT.AGENT_NAME AGENTNAME, ");
			buildQuery.Append(" M.MASTER_JC_SEA_IMP_PK MSTJCPK, ");

			buildQuery.Append(" CASE WHEN M.CARGO_TYPE =2 THEN (SELECT SUM(JCONT.CHARGEABLE_WEIGHT) ");
			buildQuery.Append("  FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
			buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
			buildQuery.Append(" AND JC.MASTER_JC_FK =M.MASTER_JC_SEA_IMP_PK GROUP BY M.MASTER_JC_SEA_IMP_PK) ");
			buildQuery.Append(" ELSE (SELECT SUM(JCONT.NET_WEIGHT) FROM JOB_TRN_CONT JCONT,JOB_CARD_TRN JC ");
			buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
			buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK ");
			buildQuery.Append(" GROUP BY M.MASTER_JC_SEA_IMP_PK) END CHARGEABLE_WEIGHT,  ");

			//buildQuery.Append(vbCrLf & " (SELECT SUM(JCONT.VOLUME_IN_CBM) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ")
			//buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
			//buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK ")
			//buildQuery.Append(vbCrLf & " GROUP BY M.MASTER_JC_SEA_IMP_PK) AS VOLUME_IN_CBM, ")

			buildQuery.Append(" (SELECT SUM(JCONT.GROSS_WEIGHT) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
			buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
			buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK ");
			buildQuery.Append(" GROUP BY M.MASTER_JC_SEA_IMP_PK) AS GROSS_WEIGHT, ");
			//Modified by Faheem
			//buildQuery.Append(vbCrLf & " (SELECT SUM(JCONT.CHARGEABLE_WEIGHT) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ")
			//buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
			//buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK  ")
			//buildQuery.Append(vbCrLf & " GROUP BY M.MASTER_JC_SEA_IMP_PK) AS CHARGEABLE_WEIGHT")
			//buildQuery.Append(vbCrLf & " CASE WHEN M.CARGO_TYPE =2 THEN (SELECT SUM(JCONT.CHARGEABLE_WEIGHT) ")
			//buildQuery.Append(vbCrLf & "  FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ")
			//buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
			//buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_FK =M.MASTER_JC_SEA_IMP_PK GROUP BY M.MASTER_JC_SEA_IMP_PK) ")
			//buildQuery.Append(vbCrLf & " ELSE (SELECT SUM(JCONT.NET_WEIGHT) FROM JOB_TRN_CONT JCONT,JOB_CARD_TRN JC ")
			//buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
			//buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK ")
			//buildQuery.Append(vbCrLf & " GROUP BY M.MASTER_JC_SEA_IMP_PK) END CHARGEABLE_WEIGHT  ")

			buildQuery.Append(" (SELECT SUM(JCONT.VOLUME_IN_CBM) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
			buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
			buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK ");
			buildQuery.Append(" GROUP BY M.MASTER_JC_SEA_IMP_PK) AS VOLUME_IN_CBM, ");
			buildQuery.Append("  '' SEL,M.CONSOLE  ");
			//Ended by Faheem

			buildQuery.Append(" FROM MASTER_JC_SEA_IMP_TBL M, ");
			buildQuery.Append(" PORT_MST_TBL          POL, ");
			buildQuery.Append(" PORT_MST_TBL          POD, ");
			buildQuery.Append(" AGENT_MST_TBL         AMT, ");
			//'
			buildQuery.Append("  VESSEL_VOYAGE_TRN     VVT, ");
			buildQuery.Append("   VESSEL_VOYAGE_TBL     VST, ");
			//'
			//buildQuery.Append(vbCrLf & " OPERATOR_MST_TBL      OMT,USER_MST_TBL UMT ")
			buildQuery.Append(" OPERATOR_MST_TBL      OMT ");
			buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
			buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK ");
			buildQuery.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+) AND M.VOYAGE_TRN_FK= VVT.VOYAGE_TRN_PK(+) ");
			//'
			buildQuery.Append("               " + strCondition);
			buildQuery.Append("      ORDER BY M.MASTER_JC_DATE DESC,M.MASTER_JC_REF_NO desc ");
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");
			buildQuery.Append("  where  ");
			buildQuery.Append("     SR_NO between " + start + " and " + last);
			strSQL = buildQuery.ToString();


			DataSet DS = null;

			try {
				DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), "", "", "", POLID, PODId, POLName, 
                    PODName, "", "", "", "", "", 0, 2));
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

		private DataTable FetchChildFor(string MSTJCPKs = "", string jobrefNO = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "", string consignee = "",
		string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0)
		{


			System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
			System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
			string strCondition = "";
			string strSQL = "";
			string strTable = "JOB_CARD_TRN";

			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
			//CONDITION COMING FROM MASTRER FUNCTION

			if (process == "2") {
				buildCondition.Append("     JOB_CARD_TRN JC,");
				buildCondition.Append("     AGENT_MST_TBL POLA, ");
			} else {
				buildCondition.Append(" JOB_CARD_TRN JC,");
			}
			buildCondition.Append("     CUSTOMER_MST_TBL SH,");
			buildCondition.Append("     CUSTOMER_MST_TBL CO,");
			buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
			buildCondition.Append("     AGENT_MST_TBL CBA, ");
			buildCondition.Append("     AGENT_MST_TBL CLA, MASTER_JC_SEA_IMP_TBL MJ, JOB_TRN_CONT JCONT ");

			buildCondition.Append("      where ");
			// JOIN CONDITION
			if (process == "2") {
				buildCondition.Append("    JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_SEA_IMP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
				buildCondition.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
				buildCondition.Append("    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
				buildCondition.Append("    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
				buildCondition.Append("    AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+) ");
				buildCondition.Append("   AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
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

			if (jcStatus.Length > 0) {
				buildCondition.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
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
				if (process != "2") {
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
			} else {
				buildQuery.Append("       JOB_CARD_TRN_PK, ");
				buildQuery.Append("       PORT_MST_POL_FK, ");
				buildQuery.Append("       PORT_MST_POD_FK, ");
			}
			buildQuery.Append("       JC.JOBCARD_REF_NO, ");
			buildQuery.Append("       JC.HBL_HAWB_REF_NO, ");
			buildQuery.Append("       JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK, ");

			//buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT,")
			//buildQuery.Append(vbCrLf & "   SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT ,")
			buildQuery.Append("   CASE WHEN JC.CARGO_TYPE = 2 THEN SUM(JCONT.CHARGEABLE_WEIGHT)  ELSE  SUM(JCONT.NET_WEIGHT) END AS CHARGEABLE_WEIGHT ,");
			buildQuery.Append("      SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, ");
			buildQuery.Append("  JC.CONSOLE  ");
			buildQuery.Append("      from ");

			buildQuery.Append(strCondition);
			buildQuery.Append("      GROUP BY JC.CARGO_TYPE, JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK,JOBCARD_REF_NO, HBL_HAWB_REF_NO, MBL_MAWB_REF_NO, JC.MASTER_JC_FK,jobcard_date,JC.CONSOLE  ORDER BY jobcard_date desc, JOBCARD_REF_NO desc ");
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");
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
			buildQuery.Append(" M.LOAD_AGENT_MST_FK, AMT.AGENT_ID, AMT.AGENT_NAME, ");
			buildQuery.Append(" M.MASTER_JC_SEA_IMP_PK, M.VERSION_NO, M.REMARKS, ");
			buildQuery.Append(" M.VOYAGE_TRN_FK,VES.VESSEL_NAME,VOY.VOYAGE, ");
			buildQuery.Append(" M.POD_ATA,M.COMMODITY_GROUP_FK,M.POD_ETA,VES.VESSEL_ID,M.CARGO_TYPE ");
			buildQuery.Append(" FROM MASTER_JC_SEA_IMP_TBL M, ");
			buildQuery.Append(" PORT_MST_TBL          POL,      PORT_MST_TBL          POD, ");
			buildQuery.Append(" AGENT_MST_TBL         AMT,      OPERATOR_MST_TBL      OMT, ");
			buildQuery.Append(" VESSEL_VOYAGE_TRN     VOY,      VESSEL_VOYAGE_TBL     VES ");
			buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
			buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK ");
			buildQuery.Append(" AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK ");
			buildQuery.Append(" AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK ");
			buildQuery.Append(" AND VES.VESSEL_VOYAGE_TBL_PK(+) = VOY.VESSEL_VOYAGE_TBL_FK ");
			buildQuery.Append(" AND M.VOYAGE_TRN_FK = VOY.VOYAGE_TRN_PK(+)");
			buildQuery.Append(" AND M.MASTER_JC_SEA_IMP_PK = " + MSTJCPK);
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

		public DataSet FetchGridData(string MSTJCPKs = "", string jobrefNO = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "", string consignee = "",
		string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
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
				buildCondition.Append("     JOB_CARD_TRN JC,");
				buildCondition.Append("     AGENT_MST_TBL POLA, ");
			} else {
				buildCondition.Append(" JOB_CARD_TRN JC,");
			}
			buildCondition.Append("     CUSTOMER_MST_TBL SH,");
			buildCondition.Append("     CUSTOMER_MST_TBL CO,");
			buildCondition.Append("     CUSTOMER_MST_TBL CU,");
			buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
			buildCondition.Append("     AGENT_MST_TBL CBA, ");
			buildCondition.Append("     AGENT_MST_TBL CLA, MASTER_JC_SEA_IMP_TBL MJ, JOB_TRN_CONT JCONT  ");

			buildCondition.Append("      where ");
			// JOIN CONDITION
			if (process == "2") {
				buildCondition.Append("   JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.CUST_CUSTOMER_MST_FK = CU.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_SEA_IMP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
				buildCondition.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
				buildCondition.Append("   AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
				buildCondition.Append("   AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
				buildCondition.Append("   AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+) ");
				buildCondition.Append("   AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
			} else {
				buildCondition.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				buildCondition.Append("   AND JC.CUST_CUSTOMER_MST_FK = CU.CUSTOMER_MST_PK(+)");
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

			if (jcStatus.Length > 0) {
				buildCondition.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
			}
			if (Hbl.Trim().Length > 0) {
				buildCondition.Append(" AND UPPER(HBL_HAWB_REF_NO) LIKE '" + Hbl.ToUpper().Replace("'", "''") + "%'");
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
				if (process != "2") {
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
			buildQuery.Append("    ( Select DISTINCT" );
			//CUST_CUSTOMER_MST_FK
			buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
			//
			if (process == "2") {
				buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
				buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
				buildQuery.Append("       CU.CUSTOMER_NAME, ");
			} else {
				buildQuery.Append("       JOB_CARD_TRN_PK, ");
				buildQuery.Append("       PORT_MST_POL_FK, ");
				buildQuery.Append("       PORT_MST_POD_FK, ");
			}
			buildQuery.Append("       JC.JOBCARD_REF_NO, ");
			buildQuery.Append("       JC.HBL_HAWB_REF_NO, ");
			buildQuery.Append("       JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO ");
			//buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT ")
			buildQuery.Append("      from ");

			buildQuery.Append(strCondition);
			//buildQuery.Append(vbCrLf & "      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_SEA_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HBL_HAWB_REF_NO, MBL_MAWB_REF_NO, JC.MASTER_JC_FK ORDER BY JOBCARD_REF_NO ASC ")
			buildQuery.Append("      ORDER BY JOBCARD_REF_NO ASC ");
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
				buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
				buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
				buildQuery.Append("       CU.CUSTOMER_NAME, ");
			} else {
				buildQuery.Append("       JOB_CARD_TRN_PK, ");
				buildQuery.Append("       PORT_MST_POL_FK, ");
				buildQuery.Append("       PORT_MST_POD_FK, ");
			}
			buildQuery.Append("       JC.JOBCARD_REF_NO, ");
			buildQuery.Append("       JC.HBL_HAWB_REF_NO, ");
			buildQuery.Append("       JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK, ");
			buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, ");
			buildQuery.Append("       CASE WHEN JC.CARGO_TYPE=2 THEN SUM(JCONT.CHARGEABLE_WEIGHT) ");
			buildQuery.Append("       ELSE SUM(JCONT.NET_WEIGHT) END AS CHARGEABLE_WEIGHT, ");
			buildQuery.Append("   JC.VERSION_NO,'' Sel   ");


			buildQuery.Append("  , (select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
			buildQuery.Append("      from ");

			buildQuery.Append(strCondition);
			buildQuery.Append("      GROUP BY JC.CARGO_TYPE,JC.SHIPPER_CUST_MST_FK, JC.JOB_CARD_TRN_PK,JC.CUST_CUSTOMER_MST_FK,CU.CUSTOMER_NAME,JC.JOBCARD_REF_NO, JC.HBL_HAWB_REF_NO, JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO ORDER BY JOBCARD_REF_NO ASC ");
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");
			buildQuery.Append("  where  ");
			buildQuery.Append("     SR_NO between " + start + " and " + last);

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
		public DataSet FetchAttachGridData(string JCPKs = "", string jobrefNO = "", string bookingNo = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "",
		string consignee = "", string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, bool BOOKING = false, Int32 CurrentPage = 0, Int32 TotalPage = 0, string MSTJCPKs = "",
		Int16 Flag = 0)
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
				buildConditiontab.Append("     JOB_CARD_TRN JC,");
				buildConditiontab.Append("     AGENT_MST_TBL POLA, ");
			} else {
				buildConditiontab.Append(" JOB_CARD_TRN JC,");
			}
			//'
			buildConditiontab.Append("     CUSTOMER_MST_TBL SH,");
			buildConditiontab.Append("     CUSTOMER_MST_TBL CO,");
			buildConditiontab.Append("     CUSTOMER_MST_TBL CU,");
			buildConditiontab.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
			buildConditiontab.Append("     AGENT_MST_TBL CBA, ");
			buildConditiontab.Append("     AGENT_MST_TBL CLA, JOB_TRN_CONT JCONT  ");

			if (Flag == 1) {
				buildConditiontab1.Append("     ,MASTER_JC_SEA_IMP_TBL MJ ");
			}

			buildConditionCon.Append("      where ");
			// JOIN CONDITION

			if (process == "2") {
				buildConditionCon.Append("   JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				buildConditionCon.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				buildConditionCon.Append("   AND JC.CUST_CUSTOMER_MST_FK = CU.CUSTOMER_MST_PK(+)");
				if (Flag == 1) {
					buildConditionCon1.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_SEA_IMP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
					buildConditionCon1.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
				}
				buildConditionCon.Append("   AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
				buildConditionCon.Append("   AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
				buildConditionCon.Append("   AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+) ");
				buildConditionCon.Append("   AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
			} else {
				buildConditionCon.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				buildConditionCon.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				buildConditionCon.Append("   AND JC.CUST_CUSTOMER_MST_FK = CU.CUSTOMER_MST_PK(+)");
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

			if (jcStatus.Length > 0) {
				buildConditionCon.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
			}
			if (Hbl.Trim().Length > 0) {
				buildConditionCon.Append(" AND UPPER(HBL_HAWB_REF_NO) LIKE '" + Hbl.ToUpper().Replace("'", "''") + "%'");
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
				buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
				buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
				buildQuery.Append("       CU.CUSTOMER_NAME, ");
			} else {
				buildQuery.Append("       JOB_CARD_TRN_PK, ");
				buildQuery.Append("       PORT_MST_POL_FK, ");
				buildQuery.Append("       PORT_MST_POD_FK, ");
			}
			buildQuery.Append("       JC.JOBCARD_REF_NO, ");
			buildQuery.Append("       JC.HBL_HAWB_REF_NO, ");
			buildQuery.Append("       JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO ");
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
					buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
					buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
					buildQuery.Append("       CU.CUSTOMER_NAME, ");
				} else {
					buildQuery.Append("       JOB_CARD_TRN_PK, ");
					buildQuery.Append("       PORT_MST_POL_FK, ");
					buildQuery.Append("       PORT_MST_POD_FK, ");
				}
				buildQuery.Append("       JC.JOBCARD_REF_NO, ");
				buildQuery.Append("       JC.HBL_HAWB_REF_NO, ");
				buildQuery.Append("       JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO ");
				buildQuery.Append("      from ");

				buildQuery.Append(strConditiontab);
				buildQuery.Append(strConditiontab1);
				buildQuery.Append(strConditionCon);
				buildQuery.Append(strConditionCon1);
				buildQuery.Append(strConditionConMSTJCPK1);
			}

			//buildQuery.Append(vbCrLf & "      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_SEA_EXP_PK, BOOKING_SEA_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HBL_HAWB_REF_NO, MBL_MAWB_REF_NO, JC.MASTER_JC_FK ORDER BY JOBCARD_REF_NO ASC ")
			buildQuery.Append("      ORDER BY JOBCARD_REF_NO ASC ");
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
				buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
				buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
				buildQuery.Append("       CU.CUSTOMER_NAME, ");
			} else {
				buildQuery.Append("       JOB_CARD_TRN_PK, ");
				buildQuery.Append("       PORT_MST_POL_FK, ");
				buildQuery.Append("       PORT_MST_POD_FK, ");
			}
			buildQuery.Append("       JC.JOBCARD_REF_NO, ");
			buildQuery.Append("       JC.HBL_HAWB_REF_NO, ");
			buildQuery.Append("       JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK, ");
			buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel");
			buildQuery.Append("  , (select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
			buildQuery.Append("      from ");

			buildQuery.Append(strConditiontab);
			buildQuery.Append(strConditionCon);
			buildQuery.Append(strConditionConAtt);
			buildQuery.Append(strConditionConJCPK);
			buildQuery.Append("AND JC.MASTER_JC_FK IS NULL");
			//buildQuery.Append(vbCrLf & "      GROUP BY JC.SHIPPER_CUST_MST_FK, JC.JOB_CARD_TRN_PK,JC.CUST_CUSTOMER_MST_FK,CU.CUSTOMER_NAME,JC.JOBCARD_REF_NO, JC.HBL_HAWB_REF_NO, JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO ")
			if (Flag == 1) {
				buildQuery.Append("union");

				buildQuery.Append("     Select " );
				//CUST_CUSTOMER_MST_FK
				buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
				//
				if (process == "2") {
					buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
					buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
					buildQuery.Append("       CU.CUSTOMER_NAME, ");
				} else {
					buildQuery.Append("       JOB_CARD_TRN_PK, ");
					buildQuery.Append("       PORT_MST_POL_FK, ");
					buildQuery.Append("       PORT_MST_POD_FK, ");
				}
				buildQuery.Append("       JC.JOBCARD_REF_NO, ");
				buildQuery.Append("       JC.HBL_HAWB_REF_NO, ");
				buildQuery.Append("       JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK, ");
				buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel");
				buildQuery.Append("  , (select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
				buildQuery.Append("      from ");
				buildQuery.Append(strConditiontab);
				buildQuery.Append(strConditiontab1);
				buildQuery.Append(strConditionCon);
				buildQuery.Append(strConditionCon1);
				buildQuery.Append(strConditionConMSTJCPK1);
			}

			buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JC.JOB_CARD_TRN_PK,JC.CUST_CUSTOMER_MST_FK,CU.CUSTOMER_NAME,JC.JOBCARD_REF_NO, JC.HBL_HAWB_REF_NO, JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO ORDER BY JOBCARD_REF_NO ASC ");
			buildQuery.Append("    ) q ");
			buildQuery.Append("  )   ");
			buildQuery.Append("  where  ");
			buildQuery.Append("     SR_NO between " + start + " and " + last);

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

		#region "FetchExistingJCPK"
		public DataSet FetchExistingJCPK(string MSTJCPK)
		{
			string strSQL = null;

			strSQL = "SELECT JOB.JOB_CARD_TRN_PK  from JOB_CARD_TRN JOB,MASTER_JC_SEA_IMP_TBL MST where JOB.MASTER_JC_FK=MST.MASTER_JC_SEA_IMP_PK(+) AND MST.MASTER_JC_SEA_IMP_PK=" + MSTJCPK;

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
			objBookingSea.ConfigurationPK = M_Configuration_PK;
			objBookingSea.CREATED_BY = M_CREATED_BY_FK;
            
			int intPKVal = 0;
			long lngI = 0;
			string MSTJCRefNum = null;
			Int32 RecAfct = default(Int32);
			bool chkflag = false;
			//objWK.OpenConnection()
			//TRAN1 = objWK.MyConnection.BeginTransaction()
			//objWK.MyCommand.Transaction = TRAN1
			//objWK.MyCommand.Connection = objWK.MyConnection
			//Dim VesselId As String = M_DataSet.Tables(0).Rows(0).Item("VESSEL_ID")
			//Dim VesselName As String = M_DataSet.Tables(0).Rows(0).Item("VESSEL_NAME")
			//Dim Voyage As String = M_DataSet.Tables(0).Rows(0).Item("VOYAGE")

			if ((string.IsNullOrEmpty(strVoyagepk) || strVoyagepk == "0") & !string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["VESSEL_ID"].ToString())) {
				strVoyagepk = "0";
                arrMessage = objBookingSea.SaveVesselMaster(Convert.ToInt64(strVoyagepk),
                       M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"].ToString(), Convert.ToInt64(M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"]), M_DataSet.Tables[0].Rows[0]["VESSEL_ID"].ToString(), M_DataSet.Tables[0].Rows[0]["VOYAGE"].ToString(), objWK.MyCommand, Convert.ToInt64(M_DataSet.Tables[0].Rows[0]["PORT_MST_POL_FK"]),
                       M_DataSet.Tables[0].Rows[0]["PORT_MST_POD_FK"].ToString(), DateTime.MinValue, Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["POD_ETA"]), DateTime.MinValue, DateTime.MinValue, Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["POD_ATA"]), DateTime.MinValue);
                M_DataSet.Tables[0].Rows[0]["VOYAGE_TRN_FK"] = strVoyagepk;
				if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0)) {
					//TRAN1.Rollback()
					return arrMessage;
				} else {
					arrMessage.Clear();
				}
				//End If
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
			attCommand.Transaction = TRAN;
			try {
				DataTable DtTbl = new DataTable();
				DataRow DtRw = null;
				int i = 0;


				var _with1 = insCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".MASTER_JC_SEA_IMP_TBL_PKG.MASTER_JC_SEA_IMP_TBL_INS";
				var _with2 = _with1.Parameters;

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

				insCommand.Parameters.Add("LOAD_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "LOAD_AGENT_MST_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["LOAD_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 500, "REMARKS").Direction = ParameterDirection.Input;
				insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(CREATED_BY)).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 10, "OPERATOR_MST_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("VOYAGE_TRN_FK_IN", OracleDbType.Int32, 10, "VOYAGE_TRN_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["VOYAGE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("POD_ATA_IN", OracleDbType.Date, 20, "POD_ATA").Direction = ParameterDirection.Input;
				insCommand.Parameters["POD_ATA_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("POD_ETA_IN", OracleDbType.Date, 20, "POD_ETA").Direction = ParameterDirection.Input;
				insCommand.Parameters["POD_ETA_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
				insCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_SEA_IMP_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;



				var _with3 = updCommand;
				_with3.Connection = objWK.MyConnection;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWK.MyUserName + ".MASTER_JC_SEA_IMP_TBL_PKG.MASTER_JC_SEA_IMP_TBL_UPD";
				var _with4 = _with3.Parameters;

				updCommand.Parameters.Add("MASTER_JC_SEA_IMP_PK_IN", OracleDbType.Int32, 10, "MASTER_JC_SEA_IMP_PK").Direction = ParameterDirection.Input;
				updCommand.Parameters["MASTER_JC_SEA_IMP_PK_IN"].SourceVersion = DataRowVersion.Current;

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

				updCommand.Parameters.Add("LOAD_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "LOAD_AGENT_MST_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["LOAD_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 500, "REMARKS").Direction = ParameterDirection.Input;
				updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(LAST_MODIFIED_BY)).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 10, "OPERATOR_MST_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("VOYAGE_TRN_FK_IN", OracleDbType.Int32, 10, "VOYAGE_TRN_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["VOYAGE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("POD_ATA_IN", OracleDbType.Date, 20, "POD_ATA").Direction = ParameterDirection.Input;
				updCommand.Parameters["POD_ATA_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
				updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("POD_ETA_IN", OracleDbType.Date, 20, "POD_ETA").Direction = ParameterDirection.Input;
				updCommand.Parameters["POD_ETA_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_SEA_IMP_PK").Direction = ParameterDirection.Output;
				updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with5 = objWK.MyDataAdapter;

				_with5.InsertCommand = insCommand;
				_with5.InsertCommand.Transaction = TRAN;

				_with5.UpdateCommand = updCommand;
				_with5.UpdateCommand.Transaction = TRAN;
				if ((M_DataSet.GetChanges(DataRowState.Added) != null)) {
					chkflag = true;
				} else {
					chkflag = false;
				}
				RecAfct = _with5.Update(M_DataSet);

				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					if (chkflag) {
                        RollbackProtocolKey("MASTER JC SEA EXPORT", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
						//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
					}
					return arrMessage;
				} else {
					if (Attach == 1 | Detach == 2) {
						arrMessage = AttachSave(M_DataSet, GridDS, insCommand, objWK.MyUserName, Detach, JCRefNo, Grid_Job_Ref_Nr, Attach);
						if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0)) {
							TRAN.Rollback();
							if (chkflag) {
                                RollbackProtocolKey("MASTER JC SEA EXPORT", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                            }
							return arrMessage;
						} else {
							arrMessage.Clear();
						}
					}

					if (isEdting == false) {
						MSTJobCardPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
					}
					//Modified by Faheem as to apportion the cost only when MJC is closed
					if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["MASTER_JC_CLOSED_ON"].ToString())) {
						OracleCommand ObjCommand = new OracleCommand();
						var _with6 = ObjCommand;
						//.Connection = objWK.MyConnection
						_with6.Transaction = TRAN;
						_with6.Connection = TRAN.Connection;
						_with6.CommandType = CommandType.StoredProcedure;
						//.CommandText = objWK.MyUserName & ".JC_COST_PIA_CALCULATION_PKG.JC_SEA_IMP_COST_PIA_CALC"
						_with6.CommandText = objWK.MyUserName + ".JC_COST_CALCULATION_PKG.JC_SEA_IMP_COST_CALC";
						var _with7 = _with6.Parameters;
						_with7.Add("MASTER_JC_SEA_IMP_PK_IN", MSTJobCardPK).Direction = ParameterDirection.Input;
						_with7.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
						_with6.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
						_with6.ExecuteNonQuery();
					}
					//End
					arrMessage.Add("All Data Saved Successfully");
					TRAN.Commit();
					return arrMessage;
				}

			} catch (OracleException oraexp) {
				if (chkflag) {
                    RollbackProtocolKey("MASTER JC SEA EXPORT", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
				throw oraexp;
			} catch (Exception ex) {
				if (chkflag) {
                    RollbackProtocolKey("MASTER JC SEA EXPORT", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
				throw ex;
			} finally {
				objWK.CloseConnection();
				//Added by sivachandran - To close the connection after Transaction
			}
		}

		private ArrayList AttachSave(DataSet M_DataSet, DataSet GridDS, OracleCommand attCommand, string user, Int32 Detach = 0, string JCRefNo = "0", string Grid_Job_Ref_Nr = "0", Int32 Attach = 0)
		{
			int i = 0;
			int m = 0;
			string str1 = null;
			string str2 = null;
			Array arr = null;

			try {
				for (i = 0; i <= GridDS.Tables[0].Rows.Count - 1; i++) {
					attCommand.Parameters.Clear();
					var _with8 = attCommand;
					_with8.CommandType = CommandType.StoredProcedure;
					_with8.CommandText = user + ".MASTER_JC_SEA_IMP_TBL_PKG.JOB_CARD_SEA_IMP_TBL_UPD";
					var _with9 = _with8.Parameters;
					str1 = Convert.ToString(GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]);
					str2 = Convert.ToString(str1.IndexOf("/"));
					if (Convert.ToInt32(str2) >= 0) {
						_with9.Add("JOB_CARD_SEA_IMP_PK_IN", GridDS.Tables[0].Rows[i]["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;
						_with9.Add("JOBCARD_REF_NO_IN", GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]).Direction = ParameterDirection.Input;
						_with9.Add("OPERATOR_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"]).Direction = ParameterDirection.Input;
						_with9.Add("LOAD_AGENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["LOAD_AGENT_MST_FK"]).Direction = ParameterDirection.Input;
						_with9.Add("VESSEL_NAME_IN", M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"]).Direction = ParameterDirection.Input;
						_with9.Add("VOYAGE_IN", M_DataSet.Tables[0].Rows[0]["VOYAGE"]).Direction = ParameterDirection.Input;
						_with9.Add("VOYAGE_FK_IN", M_DataSet.Tables[0].Rows[0]["VOYAGE_TRN_FK"]).Direction = ParameterDirection.Input;
						_with9.Add("ARRIVAL_DATE_IN", M_DataSet.Tables[0].Rows[0]["POD_ATA"]).Direction = ParameterDirection.Input;
						_with9.Add("MASTER_JC_SEA_IMP_FK_IN", M_DataSet.Tables[0].Rows[0]["MASTER_JC_SEA_IMP_PK"]).Direction = ParameterDirection.Input;
						_with9.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(LAST_MODIFIED_BY)).Direction = ParameterDirection.Input;
						_with9.Add("VERSION_NO_IN", GridDS.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
						_with9.Add("CONFIG_PK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;
						_with9.Add("ETA_DATE_IN", M_DataSet.Tables[0].Rows[0]["POD_ETA"]).Direction = ParameterDirection.Input;
						_with9.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
						attCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
					} else {
						_with9.Add("JOB_CARD_SEA_IMP_PK_IN", GridDS.Tables[0].Rows[i]["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;
						_with9.Add("JOBCARD_REF_NO_IN", GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]).Direction = ParameterDirection.Input;
						_with9.Add("OPERATOR_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"]).Direction = ParameterDirection.Input;
						_with9.Add("LOAD_AGENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["LOAD_AGENT_MST_FK"]).Direction = ParameterDirection.Input;
						_with9.Add("VESSEL_NAME_IN", M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"]).Direction = ParameterDirection.Input;
						_with9.Add("VOYAGE_IN", M_DataSet.Tables[0].Rows[0]["VOYAGE"]).Direction = ParameterDirection.Input;
						_with9.Add("VOYAGE_FK_IN", M_DataSet.Tables[0].Rows[0]["VOYAGE_TRN_FK"]).Direction = ParameterDirection.Input;
						_with9.Add("ARRIVAL_DATE_IN", M_DataSet.Tables[0].Rows[0]["POD_ATA"]).Direction = ParameterDirection.Input;

						//If Detach = 2 Then
						if (object.ReferenceEquals(GridDS.Tables[0].Rows[i]["MASTER_JC_FK"], "")) {
							_with9.Add("MASTER_JC_SEA_IMP_FK_IN", "").Direction = ParameterDirection.Input;
						} else {
							_with9.Add("MASTER_JC_SEA_IMP_FK_IN", GridDS.Tables[0].Rows[i]["MASTER_JC_FK"]).Direction = ParameterDirection.Input;
						}

						//Else
						//.Add("MASTER_JC_SEA_IMP_FK_IN", M_DataSet.Tables(0).Rows(0).Item("MASTER_JC_SEA_EXP_PK")).Direction = ParameterDirection.Input

						//End If

						_with9.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(LAST_MODIFIED_BY)).Direction = ParameterDirection.Input;
						_with9.Add("VERSION_NO_IN", GridDS.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
						_with9.Add("CONFIG_PK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;
						_with9.Add("ETA_DATE_IN", M_DataSet.Tables[0].Rows[0]["POD_ETA"]).Direction = ParameterDirection.Input;
						_with9.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
						attCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
					}

					_with8.ExecuteNonQuery();
					//Update Track and Trace
					if (Attach == 1) {
						str1 = Convert.ToString(GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]);
						//str2 = str1.IndexOf("/")
						//If str2 >= 0 Then ' Attach
						if (!string.IsNullOrEmpty(Grid_Job_Ref_Nr)) {
							arr = Grid_Job_Ref_Nr.Split(',');
							for (m = 0; m <= Grid_Job_Ref_Nr.Length - 1; m++) {
								if (i == m) {
									UpdateTrackandTrace(Convert.ToString(arr.GetValue(m)), str1);
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
		//End 
		#endregion

		#region "Update Track and Trace "
		public string UpdateTrackandTrace(string NewJobRef, string PREV_JOB_REF)
		{
			string strSQL = null;
			strSQL = "update track_n_trace_tbl t";
			strSQL = strSQL + "set t.doc_ref_no= '" + PREV_JOB_REF + "'";
			strSQL = strSQL + "where t.doc_ref_no= '" + NewJobRef + "'";
			try {
				return (new WorkFlow()).ExecuteScaler(strSQL.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
		#endregion

		#region "Fetch Previous Job Card No"
		public string FetchPreviousJCrefNr(int JobCardPk)
		{
			string strSQL = null;

			strSQL = " SELECT JCT.JOBCARD_PREV_REF_NO" + " FROM JOB_CARD_TRN JCT" + "WHERE" + " JCT.JOB_CARD_TRN_PK =" + JobCardPk;
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
		public DataSet FetchVesselVoyage(string VoyagePK)
		{
			string strSQL = null;

			strSQL = "SELECT V.VESSEL_ID, V.VESSEL_NAME, VVT.VOYAGE" + "FROM VESSEL_VOYAGE_TBL V, VESSEL_VOYAGE_TRN VVT" + "WHERE V.VESSEL_VOYAGE_TBL_PK =VVT.VESSEL_VOYAGE_TBL_FK" + "AND VVT.VOYAGE_TRN_PK =" + VoyagePK;

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

		#region "De-Console MasterJobCard"

		public ArrayList DeConsoleMJCImp(Int64 MJCIMPPK)
		{
			OracleCommand Cmd = new OracleCommand();
			WorkFlow objWK = new WorkFlow();
			int RAF = 0;
			objWK.OpenConnection();
			string SRet_Value = null;
			arrMessage.Clear();
			try {
				var _with10 = Cmd;
				_with10.Parameters.Clear();
				_with10.Connection = objWK.MyConnection;
				_with10.CommandType = CommandType.StoredProcedure;
				_with10.CommandText = objWK.MyUserName + ".MASTER_JC_SEA_EXP_TBL_PKG.MJC_IMP_DECONSOLE";
				var _with11 = _with10.Parameters;
				_with11.Add("MJC_SEA_IMP_PK_IN", MJCIMPPK).Direction = ParameterDirection.Input;
				_with11.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, SRet_Value).Direction = ParameterDirection.Output;
				RAF = Cmd.ExecuteNonQuery();

			} catch (OracleException oraexp) {
				if (oraexp.ErrorCode == 20999) {
					arrMessage.Add("20999");
				} else {
					arrMessage.Add(oraexp.Message);
				}
			} catch (Exception ex) {
				throw ex;
			}
			if (arrMessage.Count > 0) {
				return arrMessage;
			} else {
				arrMessage.Add("Saved");
				return arrMessage;
			}
		}

		#endregion

		#region "Function to check whether a user is an administrator or not"
		public int IsAdministrator(string strUserID)
		{
			string strSQL = null;
			Int16 Admin = default(Int16);
			WorkFlow objWF = new WorkFlow();
			strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
			strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
			strSQL = strSQL + "AND U.USER_MST_PK = " + HttpContext.Current.Session["USER_PK"];
			try {
				Admin = Convert.ToInt16(objWF.ExecuteScaler(strSQL.ToString()));
				if (Admin == 1) {
					return 1;
				} else {
					return 0;
				}

			} catch (OracleException sqlExp) {
				throw sqlExp;
			} catch (Exception exp) {
				throw exp;
			}

		}
		#endregion

	}
}

