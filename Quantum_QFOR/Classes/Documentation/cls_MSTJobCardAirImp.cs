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
    public class clsMSTJobCardAirImp : CommonFeatures
    {
        #region "Fetch Master Job Card Air Import List"

        public int GetMJAICount(string mjaiRefNr, int mjaiPk, int mjaiActive, long locPk)
        {
            try
            {
                System.Text.StringBuilder strMJAIQuery = new System.Text.StringBuilder(5000);
                strMJAIQuery.Append(" select mjai.master_jc_air_imp_pk, mjai.master_jc_ref_no");
                strMJAIQuery.Append(" from master_jc_air_imp_tbl mjai, user_mst_tbl umt");
                strMJAIQuery.Append(" where mjai.master_jc_ref_no like '%" + mjaiRefNr + "%'");
                strMJAIQuery.Append(" and mjai.created_by_fk = umt.user_mst_pk");
                strMJAIQuery.Append(" and umt.default_location_fk=" + locPk);
                if (mjaiActive == 1)
                {
                    strMJAIQuery.Append(" and mjai.master_jc_status = 1");
                }
                WorkFlow objWF = new WorkFlow();
                DataSet objmjaiDS = new DataSet();
                objmjaiDS = objWF.GetDataSet(strMJAIQuery.ToString());
                if (objmjaiDS.Tables[0].Rows.Count == 1)
                {
                    mjaiPk = Convert.ToInt32(objmjaiDS.Tables[0].Rows[0][0]);
                }
                return objmjaiDS.Tables[0].Rows.Count;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Master Job Card Air Import List"

        private string _MSTJCRefNumber;

        #region "Property"

        public string MSTJCRefNumber
        {
            get { return _MSTJCRefNumber; }
        }

        #endregion "Property"

        #region "Fetch Master Jobcard"

        public DataSet FetchAll(string MSTJCRefNo = "", bool ActiveOnly = true, string POLPk = "", string PODPk = "", string AgentPk = "", string SearchType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string POLID = "", string PODId = "",
        string POLName = "", string PODName = "", long lngUsrLocFk = 0, Int32 flag = 0, int IsAdmin = 0)
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
            if (MSTJCRefNo.Length > 0)
            {
                buildCondition.Append(" AND UPPER(M.MASTER_JC_REF_NO) LIKE '" + SrOP + MSTJCRefNo.ToUpper().Replace("'", "''") + "%'");
            }
            //If BlankGrid = 0 Then
            //    buildCondition.Append(vbCrLf & " AND 1=2 ")
            //End If
            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }
            if (ActiveOnly == true)
            {
                buildCondition.Append(" AND M.MASTER_JC_STATUS = 1 ");
                //Else
                //    buildCondition.Append(vbCrLf & " AND M.MASTER_JC_STATUS = 2 ")
            }

            if (POLPk.Length > 0)
            {
                buildCondition.Append(" AND M.PORT_MST_POL_FK = " + POLPk);
            }

            if (PODPk.Length > 0)
            {
                buildCondition.Append(" AND M.PORT_MST_POD_FK = " + PODPk);
            }

            if (AgentPk.Length > 0)
            {
                buildCondition.Append(" AND M.LOAD_AGENT_MST_FK = " + AgentPk);
            }
            //buildCondition.Append(vbCrLf & " AND UMT.DEFAULT_LOCATION_FK = " & lngUsrLocFk & "")
            //buildCondition.Append(vbCrLf & "  AND M.CREATED_BY_FK = UMT.USER_MST_PK ")
            if (IsAdmin == 0)
            {
                buildCondition.Append(" AND POD.LOCATION_MST_FK = " + lngUsrLocFk + "");
            }
            strCondition = buildCondition.ToString();

            buildQuery.Append(" SELECT ");
            buildQuery.Append(" COUNT(*) ");
            buildQuery.Append(" FROM MASTER_JC_AIR_IMP_TBL M, ");
            buildQuery.Append(" PORT_MST_TBL          POL, ");
            buildQuery.Append(" PORT_MST_TBL          POD, ");
            buildQuery.Append("  AIRLINE_MST_TBL        ART, ");
            //'
            buildQuery.Append(" AGENT_MST_TBL         AMT ");
            //buildQuery.Append(vbCrLf & " USER_MST_TBL         UMT ")
            buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
            buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK ");
            buildQuery.Append(" AND ART.AIRLINE_MST_PK(+)=M.AIRLINE_MST_FK ");
            //'
            buildQuery.Append("    " + strCondition);

            strSQL = buildQuery.ToString();

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
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
            buildQuery.Append(" M.MASTER_JC_REF_NO MSTJCREFNO, ");
            buildQuery.Append(" MASTER_JC_DATE MSTJCDATE, ");
            buildQuery.Append(" ART.AIRLINE_NAME, ");
            ///
            buildQuery.Append(" M.PORT_MST_POL_FK POLFK, POL.PORT_ID POLNAME, ");
            buildQuery.Append(" M.PORT_MST_POD_FK PODFK, POD.PORT_ID PODNAME, ");
            buildQuery.Append(" M.AOD_ETA ETD, ");
            ///
            buildQuery.Append(" M.LOAD_AGENT_MST_FK AGENTFK,  AMT.AGENT_NAME AGENTNAME, ");
            buildQuery.Append(" M.MASTER_JC_AIR_IMP_PK MSTJCPK, ");

            buildQuery.Append(" (SELECT SUM(JCONT.CHARGEABLE_WEIGHT) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
            buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
            buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK  ");
            buildQuery.Append(" GROUP BY M.MASTER_JC_AIR_IMP_PK) AS CHARGEABLE_WEIGHT,");

            //buildQuery.Append(vbCrLf & " (SELECT SUM(JCONT.VOLUME_IN_CBM) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ")
            //buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
            //buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK ")
            //buildQuery.Append(vbCrLf & " GROUP BY M.MASTER_JC_AIR_IMP_PK) AS VOLUME_IN_CBM, ")

            buildQuery.Append(" (SELECT SUM(JCONT.GROSS_WEIGHT) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
            buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
            buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK ");
            buildQuery.Append(" GROUP BY M.MASTER_JC_AIR_IMP_PK) AS GROSS_WEIGHT, ");

            //buildQuery.Append(vbCrLf & " (SELECT SUM(JCONT.CHARGEABLE_WEIGHT) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ")
            //buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
            //buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK  ")
            //buildQuery.Append(vbCrLf & " GROUP BY M.MASTER_JC_AIR_IMP_PK) AS CHARGEABLE_WEIGHT,'' SEL,M.CONSOLE ")

            buildQuery.Append(" (SELECT SUM(JCONT.VOLUME_IN_CBM) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
            buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
            buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK ");
            buildQuery.Append(" GROUP BY M.MASTER_JC_AIR_IMP_PK) AS VOLUME_IN_CBM,'' SEL,M.CONSOLE ");

            buildQuery.Append(" FROM MASTER_JC_AIR_IMP_TBL M, ");
            buildQuery.Append(" PORT_MST_TBL          POL, ");
            buildQuery.Append(" PORT_MST_TBL          POD, ");
            buildQuery.Append("  AIRLINE_MST_TBL        ART ,");
            ///
            buildQuery.Append(" AGENT_MST_TBL         AMT ");
            //buildQuery.Append(vbCrLf & " USER_MST_TBL         UMT ")
            buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
            buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK ");
            buildQuery.Append(" AND ART.AIRLINE_MST_PK(+)=M.AIRLINE_MST_FK ");
            ///
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("      ORDER BY M.MASTER_JC_DATE DESC,M.MASTER_JC_REF_NO desc ");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
            buildQuery.Append("  where  ");
            buildQuery.Append("     SR_NO between " + start + " and " + last);
            strSQL = buildQuery.ToString();

            DataSet DS = null;

            try
            {
                DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), "", "", "", "", POLID, PODId, POLName, PODName, "", ""

                , "", "2"));
                DataRelation trfRel = new DataRelation("TariffRelation", DS.Tables[0].Columns["MSTJCPK"], DS.Tables[1].Columns["MASTER_JC_FK"], true);
                DS.Relations.Add(trfRel);
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

        private DataTable FetchChildFor(string CONTSpotPKs = "", string jobrefNO = "", string bookingNo = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "",
        string consignee = "", string agent = "", string process = "", double SearchFor = 0, Int32 SearchFortime = 0, bool BOOKING = false)
        {
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string strCondition = "";
            string strSQL = "";
            string strTable = "JOB_CARD_TRN";

            //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            //CONDITION COMING FROM MASTRER FUNCTION

            if (process == "2")
            {
                buildCondition.Append("     JOB_CARD_TRN JC,");
                buildCondition.Append("     AGENT_MST_TBL POLA, ");
            }
            else
            {
                buildCondition.Append(" JOB_CARD_TRN JC,");
            }
            buildCondition.Append("     CUSTOMER_MST_TBL SH,");
            buildCondition.Append("     CUSTOMER_MST_TBL CO,");
            buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            buildCondition.Append("     AGENT_MST_TBL CBA, ");
            buildCondition.Append("     AGENT_MST_TBL CLA, MASTER_JC_AIR_IMP_TBL MJ, JOB_TRN_CONT JCONT ");

            buildCondition.Append("      where ");
            // JOIN CONDITION
            if (process == "2")
            {
                buildCondition.Append("   JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_AIR_IMP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildCondition.Append("    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
                buildCondition.Append("    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
                buildCondition.Append("    AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+) ");
                buildCondition.Append("    AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
                buildCondition.Append("    AND JC.PROCESS_TYPE = 2 ");
            }
            else
            {
                buildCondition.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append("   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildCondition.Append("   AND JC.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)");
                buildCondition.Append("   AND JC.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ");
            }
            buildCondition.Append("    AND JC.PROCESS_TYPE = " + process);
            if (jobrefNO.Length > 0)
            {
                buildCondition.Append(" AND UPPER(JOBCARD_REF_NO) LIKE '" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
            }
            if (SearchFor > 0 & SearchFortime > 0)
            {
                int NO = -Convert.ToInt32(SearchFor);
                System.DateTime Time = default(System.DateTime);
                switch (SearchFortime)
                {
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

            if (jcStatus.Length > 0)
            {
                buildCondition.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
            }
            if (Hbl.Trim().Length > 0)
            {
                buildCondition.Append(" AND UPPER(HBL_HAWB_REF_NO) LIKE '" + Hbl.ToUpper().Replace("'", "''") + "%'");
            }
            if (polID.Length > 0)
            {
                buildCondition.Append("       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
            }
            if (polName.Length > 0)
            {
                buildCondition.Append("     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
            }
            // PORT OF DISCHARGE
            if (podId.Length > 0)
            {
                buildCondition.Append("     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
            }
            if (podName.Length > 0)
            {
                buildCondition.Append("     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
            }
            // CARGO TYPE

            if (agent.Length > 0)
            {
                buildCondition.Append(" AND  (UPPER(CLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "' OR UPPER(CBA.AGENT_ID) = '" + " %" + agent.ToUpper().Replace("'", "''") + "')");
            }
            if (shipper.Length > 0)
            {
                buildCondition.Append(" AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
            }
            if (consignee.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
            }

            if (CONTSpotPKs.Trim().Length > 0)
            {
                buildCondition.Append(" and JC.MASTER_JC_FK in (" + CONTSpotPKs + ") ");
            }

            strCondition = buildCondition.ToString();

            buildQuery.Append(" Select * from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select ");
            //CUST_CUSTOMER_MST_FK
            buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
            //
            if (process == "2")
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
            }
            else
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       PORT_MST_POL_FK, ");
                buildQuery.Append("       PORT_MST_POD_FK, ");
            }
            buildQuery.Append("       JC.JOBCARD_REF_NO, ");
            buildQuery.Append("       JC.MBL_MAWB_REF_NO MAWB_REF_NO, ");
            buildQuery.Append("       JC.HBL_HAWB_REF_NO HAWB_REF_NO, JC.MASTER_JC_FK, ");
            //buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.CONSOLE ")
            buildQuery.Append("       SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT,SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, JC.CONSOLE ");
            buildQuery.Append("      from ");
            buildQuery.Append(strCondition);
            buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, JOBCARD_REF_NO, HBL_HAWB_REF_NO, MBL_MAWB_REF_NO, JC.MASTER_JC_FK,JC.JOBCARD_DATE,JC.CONSOLE ORDER BY JOBCARD_DATE DESC,JOBCARD_REF_NO DESC ");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
            strSQL = buildQuery.ToString();

            WorkFlow objWF = new WorkFlow();
            DataTable dt = null;
            try
            {
                dt = objWF.GetDataTable(strSQL);
                int RowCnt = 0;
                int Rno = 0;
                int pk = 0;
                pk = -1;
                for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++)
                {
                    if (Convert.ToInt32(dt.Rows[RowCnt]["MASTER_JC_FK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["MASTER_JC_FK"]);
                        Rno = 0;
                    }
                    Rno += 1;
                    dt.Rows[RowCnt]["SR_NO"] = Rno;
                }
                return dt;
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

        private string AllMasterPKs(DataSet ds)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            strBuilder.Append("-1,");
            for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
            {
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

            buildQuery.Append("   SELECT ");
            buildQuery.Append(" M.MASTER_JC_REF_NO, M.MASTER_JC_STATUS, ");
            buildQuery.Append(" M.MASTER_JC_DATE, M.MASTER_JC_CLOSED_ON, ");
            buildQuery.Append(" M.PORT_MST_POL_FK, POL.PORT_ID POLID, POL.PORT_NAME POLNAME, ");
            buildQuery.Append(" M.PORT_MST_POD_FK, POD.PORT_ID PODID, POD.PORT_NAME PODNAME, ");
            buildQuery.Append(" M.LOAD_AGENT_MST_FK, AMT.AGENT_ID, AMT.AGENT_NAME, ");
            buildQuery.Append(" M.MASTER_JC_AIR_IMP_PK, M.VERSION_NO, M.REMARKS, ");
            buildQuery.Append(" M.AOD_ATA,M.FLIGHT_NO,M.AIRLINE_MST_FK,AIR.AIRLINE_ID,AIR.AIRLINE_NAME,M.COMMODITY_GROUP_FK,M.AOD_ETA,M.VOYAGE_TRN_FK ");
            buildQuery.Append(" FROM MASTER_JC_AIR_IMP_TBL M, ");
            buildQuery.Append(" PORT_MST_TBL          POL,      PORT_MST_TBL          POD, ");
            buildQuery.Append(" AGENT_MST_TBL         AMT,AIRLINE_MST_TBL AIR ");
            buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
            buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK ");
            buildQuery.Append(" AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK ");
            buildQuery.Append(" AND M.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK(+) ");
            buildQuery.Append(" AND M.MASTER_JC_AIR_IMP_PK = " + MSTJCPK);
            strSQL = buildQuery.ToString();

            try
            {
                DS = objWF.GetDataSet(strSQL);
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

        public DataSet FetchGridData(string CONTSpotPKs = "", string jobrefNO = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "", string consignee = "",
        string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string strCondition = "";
            string strSQL = "";
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strTable = "JOB_CARD_TRN";

            //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            //CONDITION COMING FROM MASTRER FUNCTION

            if (process == "2")
            {
                buildCondition.Append("     JOB_CARD_TRN JC,");
                buildCondition.Append("     AGENT_MST_TBL POLA, ");
            }
            else
            {
                buildCondition.Append(" JOB_CARD_TRN JC,");
            }
            buildCondition.Append("     CUSTOMER_MST_TBL SH,");
            buildCondition.Append("     CUSTOMER_MST_TBL CO,");
            buildCondition.Append("     CUSTOMER_MST_TBL CU,");
            buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            buildCondition.Append("     AGENT_MST_TBL CBA, ");
            buildCondition.Append("     AGENT_MST_TBL CLA, MASTER_JC_AIR_IMP_TBL MJ, JOB_TRN_CONT JCONT ");

            buildCondition.Append("      where ");
            // JOIN CONDITION
            if (process == "2")
            {
                buildCondition.Append("   JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.CUST_CUSTOMER_MST_FK = CU.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_AIR_IMP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildCondition.Append("    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
                buildCondition.Append("    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
                buildCondition.Append("    AND  JC.POL_AGENT_MST_FK= POLA.AGENT_MST_PK(+) ");
                buildCondition.Append("   AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
            }
            else
            {
                buildCondition.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.CUST_CUSTOMER_MST_FK = CU.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append("   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildCondition.Append("   AND JC.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)");
                buildCondition.Append("   AND JC.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ");
            }

            if (jobrefNO.Length > 0)
            {
                buildCondition.Append(" AND UPPER(JOBCARD_REF_NO) LIKE '" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
            }
            if (SearchFor > 0 & SearchFortime > 0)
            {
                int NO = -Convert.ToInt32(SearchFor);
                System.DateTime Time = default(System.DateTime);
                switch (SearchFortime)
                {
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

            if (jcStatus.Length > 0)
            {
                buildCondition.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
            }
            if (Hbl.Trim().Length > 0)
            {
                buildCondition.Append(" AND UPPER(HBL_HAWB_REF_NO) LIKE '" + Hbl.ToUpper().Replace("'", "''") + "%'");
            }
            if (polID.Length > 0)
            {
                buildCondition.Append("       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
            }
            if (polName.Length > 0)
            {
                buildCondition.Append("     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
            }
            // PORT OF DISCHARGE
            if (podId.Length > 0)
            {
                buildCondition.Append("     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
            }
            if (podName.Length > 0)
            {
                buildCondition.Append("     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
            }
            // CARGO TYPE

            if (agent.Length > 0)
            {
                buildCondition.Append(" AND  (UPPER(CLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "' OR UPPER(CBA.AGENT_ID) = '" + " %" + agent.ToUpper().Replace("'", "''") + "')");
            }
            if (shipper.Length > 0)
            {
                buildCondition.Append(" AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
            }
            if (consignee.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
            }

            if (CONTSpotPKs.Trim().Length > 0)
            {
                buildCondition.Append(" and JC.MASTER_JC_FK in (" + CONTSpotPKs + ") ");
            }

            strCondition = buildCondition.ToString();

            buildQuery.Append("  Select COUNT(*) from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select ");
            //CUST_CUSTOMER_MST_FK
            buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
            //
            if (process == "2")
            {
                buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
                buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
                buildQuery.Append("       CU.CUSTOMER_NAME, ");
            }
            else
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       PORT_MST_POL_FK, ");
                buildQuery.Append("       PORT_MST_POD_FK, ");
            }
            buildQuery.Append("       JOBCARD_REF_NO, ");
            buildQuery.Append("       JC.HBL_HAWB_REF_NO HAWB_REF_NO, ");
            buildQuery.Append("       JC.MBL_MAWB_REF_NO MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO");
            buildQuery.Append("      from ");
            buildQuery.Append(strCondition);
            buildQuery.Append("      ORDER BY JOBCARD_REF_NO ASC ");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");

            strSQL = buildQuery.ToString();

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
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
            buildQuery.Append("    ( Select ");
            //CUST_CUSTOMER_MST_FK
            buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
            //
            if (process == "2")
            {
                buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
                buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
                buildQuery.Append("       CU.CUSTOMER_NAME, ");
            }
            else
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       PORT_MST_POL_FK, ");
                buildQuery.Append("       PORT_MST_POD_FK, ");
            }
            buildQuery.Append("       JC.JOBCARD_REF_NO, ");
            buildQuery.Append("       JC.HBL_HAWB_REF_NO HAWB_REF_NO, ");
            buildQuery.Append("       JC.MBL_MAWB_REF_NO MAWB_REF_NO, JC.MASTER_JC_FK, ");
            buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel");
            buildQuery.Append("      , (select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
            buildQuery.Append("      from ");
            buildQuery.Append(strCondition);
            buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JC.JOB_CARD_TRN_PK,JC.CUST_CUSTOMER_MST_FK,CU.CUSTOMER_NAME, JC.JOBCARD_REF_NO, JC.HBL_HAWB_REF_NO, JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO ORDER BY JOBCARD_REF_NO ASC ");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
            buildQuery.Append("  where  ");
            buildQuery.Append("     SR_NO between " + start + " and " + last);
            strSQL = buildQuery.ToString();

            DataSet ds = null;
            try
            {
                ds = objWF.GetDataSet(strSQL);
                return ds;
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

        #endregion "Fetch Master Jobcard"

        #region "Fetch Afetr Attach JC Fill Grid"

        public DataSet FetchAttachGridData(string JCPKs = "", string jobrefNO = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "", string consignee = "",
        string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, string MSTJCPK = "", Int16 Flag = 0)
        {
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildConditiontab = new System.Text.StringBuilder();
            System.Text.StringBuilder buildConditionCon = new System.Text.StringBuilder();
            System.Text.StringBuilder buildConditionConJCPK = new System.Text.StringBuilder();
            System.Text.StringBuilder buildConditionConAtt = new System.Text.StringBuilder();
            System.Text.StringBuilder buildConditiontab1 = new System.Text.StringBuilder();
            System.Text.StringBuilder buildConditionCon1 = new System.Text.StringBuilder();
            System.Text.StringBuilder buildConditionConMSTJCPK1 = new System.Text.StringBuilder();
            string strConditiontab = "";
            string strConditionCon = "";
            string strConditionConJCPK = "";
            string strConditionConAtt = "";
            string strConditiontab1 = "";
            string strConditionCon1 = "";
            string strConditionConMSTJCPK1 = "";
            string strSQL = "";
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strTable = "JOB_CARD_TRN";

            //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            //CONDITION COMING FROM MASTRER FUNCTION

            if (process == "2")
            {
                buildConditiontab.Append("     JOB_CARD_TRN JC,");
                buildConditiontab.Append("     AGENT_MST_TBL POLA, ");
            }
            else
            {
                buildConditiontab.Append(" JOB_CARD_TRN JC,");
            }
            buildConditiontab.Append("     CUSTOMER_MST_TBL SH,");
            buildConditiontab.Append("     CUSTOMER_MST_TBL CO,");
            buildConditiontab.Append("     CUSTOMER_MST_TBL CU,");
            buildConditiontab.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            buildConditiontab.Append("     AGENT_MST_TBL CBA, ");
            buildConditiontab.Append("     AGENT_MST_TBL CLA,JOB_TRN_CONT JCONT ");

            if (Flag == 1)
            {
                buildConditiontab1.Append("     ,MASTER_JC_AIR_IMP_TBL MJ ");
            }

            buildConditionCon.Append("      where ");
            // JOIN CONDITION
            if (process == "2")
            {
                buildConditionCon.Append("    JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildConditionCon.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildConditionCon.Append("   AND JC.CUST_CUSTOMER_MST_FK = CU.CUSTOMER_MST_PK(+)");
                buildConditionCon.Append("    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
                buildConditionCon.Append("    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
                buildConditionCon.Append("   AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+) ");
                buildConditionCon.Append("  AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                if (Flag == 1)
                {
                    buildConditionCon1.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_AIR_IMP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                    buildConditionCon1.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                }
            }
            else
            {
                buildConditionCon.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildConditionCon.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildConditionCon.Append("   AND JC.CUST_CUSTOMER_MST_FK = CU.CUSTOMER_MST_PK(+)");
                buildConditionCon.Append("   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildConditionCon.Append("   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildConditionCon.Append("   AND JC.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)");
                buildConditionCon.Append("   AND JC.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ");
            }

            if (jobrefNO.Length > 0)
            {
                buildConditionCon.Append(" AND UPPER(JOBCARD_REF_NO) LIKE '" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
            }
            if (SearchFor > 0 & SearchFortime > 0)
            {
                int NO = -Convert.ToInt32(SearchFor);
                System.DateTime Time = default(System.DateTime);
                switch (SearchFortime)
                {
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

            if (jcStatus.Length > 0)
            {
                buildConditionCon.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
            }
            if (Hbl.Trim().Length > 0)
            {
                buildConditionCon.Append(" AND UPPER(HBL_HAWB_REF_NO) LIKE '" + Hbl.ToUpper().Replace("'", "''") + "%'");
            }
            if (polID.Length > 0)
            {
                buildConditionCon.Append("       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
            }
            if (polName.Length > 0)
            {
                buildConditionCon.Append("     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
            }
            // PORT OF DISCHARGE
            if (podId.Length > 0)
            {
                buildConditionCon.Append("     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
            }
            if (podName.Length > 0)
            {
                buildConditionCon.Append("     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
            }
            // CARGO TYPE

            if (agent.Length > 0)
            {
                buildConditionCon.Append(" AND  (UPPER(CLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "' OR UPPER(CBA.AGENT_ID) = '" + " %" + agent.ToUpper().Replace("'", "''") + "')");
            }
            if (shipper.Length > 0)
            {
                buildConditionCon.Append(" AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
            }
            if (consignee.Length > 0)
            {
                buildConditionCon.Append(" AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
            }

            if (JCPKs.Trim().Length > 0)
            {
                buildConditionConJCPK.Append(" and JC.JOB_CARD_TRN_PK IN (" + JCPKs + ") ");
            }

            if (MSTJCPK.Trim().Length > 0)
            {
                buildConditionConMSTJCPK1.Append(" and JC.MASTER_JC_FK in (" + MSTJCPK + ") ");
            }

            strConditiontab = buildConditiontab.ToString();
            strConditionCon = buildConditionCon.ToString();
            strConditionConAtt = buildConditionConAtt.ToString();
            strConditiontab1 = buildConditiontab1.ToString();
            strConditionCon1 = buildConditionCon1.ToString();
            strConditionConJCPK = buildConditionConJCPK.ToString();
            strConditionConMSTJCPK1 = buildConditionConMSTJCPK1.ToString();

            buildQuery.Append("  Select COUNT(*) from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select ");
            buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
            //
            if (process == "2")
            {
                buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
                buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
                buildQuery.Append("       CU.CUSTOMER_NAME, ");
            }
            else
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       PORT_MST_POL_FK, ");
                buildQuery.Append("       PORT_MST_POD_FK, ");
            }
            buildQuery.Append("       JC.JOBCARD_REF_NO, ");
            buildQuery.Append("       JC.HBL_HAWB_REF_NO HAWB_REF_NO, ");
            buildQuery.Append("       JC.MBL_MAWB_REF_NO MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO");
            buildQuery.Append("      from ");
            buildQuery.Append(strConditiontab);
            buildQuery.Append(strConditionCon);
            buildQuery.Append(strConditionConAtt);
            buildQuery.Append(strConditionConJCPK);
            buildQuery.Append("AND JC.MASTER_JC_FK IS NULL");

            if (Flag == 1)
            {
                buildQuery.Append("union");
                buildQuery.Append("     Select ");
                buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
                //
                if (process == "2")
                {
                    buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
                    buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
                    buildQuery.Append("       CU.CUSTOMER_NAME, ");
                }
                else
                {
                    buildQuery.Append("       JOB_CARD_TRN_PK, ");
                    buildQuery.Append("       PORT_MST_POL_FK, ");
                    buildQuery.Append("       PORT_MST_POD_FK, ");
                }
                buildQuery.Append("       JC.JOBCARD_REF_NO, ");
                buildQuery.Append("       JC.HBL_HAWB_REF_NO HAWB_REF_NO, ");
                buildQuery.Append("       JC.MBL_MAWB_REF_NO MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO");
                buildQuery.Append("      from ");
                buildQuery.Append(strConditiontab);
                buildQuery.Append(strConditiontab1);
                buildQuery.Append(strConditionCon);
                buildQuery.Append(strConditionCon1);
                buildQuery.Append(strConditionConMSTJCPK1);
            }
            buildQuery.Append("      ORDER BY JOBCARD_REF_NO ASC ");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");

            strSQL = buildQuery.ToString();

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            buildQuery.Remove(0, buildQuery.Length);

            buildQuery.Append(" Select * from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select ");
            buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
            //
            if (process == "2")
            {
                buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
                buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
                buildQuery.Append("       CU.CUSTOMER_NAME, ");
            }
            else
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       PORT_MST_POL_FK, ");
                buildQuery.Append("       PORT_MST_POD_FK, ");
            }
            buildQuery.Append("       JC.JOBCARD_REF_NO, ");
            buildQuery.Append("       JC.HBL_HAWB_REF_NO HAWB_REF_NO, ");
            buildQuery.Append("       JC.MBL_MAWB_REF_NO MAWB_REF_NO, JC.MASTER_JC_FK, ");
            buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel");
            buildQuery.Append("        , (select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
            buildQuery.Append("      from ");
            buildQuery.Append(strConditiontab);
            buildQuery.Append(strConditionCon);
            buildQuery.Append(strConditionConAtt);
            buildQuery.Append(strConditionConJCPK);
            buildQuery.Append("AND JC.MASTER_JC_FK IS NULL");
            //buildQuery.Append(vbCrLf & "      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_AIR_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HAWB_REF_NO, MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO ")
            buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JC.JOB_CARD_TRN_PK,JC.CUST_CUSTOMER_MST_FK,CU.CUSTOMER_NAME, JC.JOBCARD_REF_NO, JC.HBL_HAWB_REF_NO, JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO ");

            if (Flag == 1)
            {
                buildQuery.Append("union");
                buildQuery.Append("       Select ");
                buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
                //
                if (process == "2")
                {
                    buildQuery.Append("       JC.JOB_CARD_TRN_PK, ");
                    buildQuery.Append("       JC.CUST_CUSTOMER_MST_FK, ");
                    buildQuery.Append("       CU.CUSTOMER_NAME, ");
                }
                else
                {
                    buildQuery.Append("       JOB_CARD_TRN_PK, ");
                    buildQuery.Append("       PORT_MST_POL_FK, ");
                    buildQuery.Append("       PORT_MST_POD_FK, ");
                }
                buildQuery.Append("       JC.JOBCARD_REF_NO, ");
                buildQuery.Append("       JC.HBL_HAWB_REF_NO HAWB_REF_NO, ");
                buildQuery.Append("       JC.MBL_MAWB_REF_NO MAWB_REF_NO, JC.MASTER_JC_FK, ");
                buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel");
                buildQuery.Append("        , (select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
                buildQuery.Append("      from ");
                buildQuery.Append(strConditiontab);
                buildQuery.Append(strConditiontab1);
                buildQuery.Append(strConditionCon);
                buildQuery.Append(strConditionCon1);
                buildQuery.Append(strConditionConMSTJCPK1);
            }
            buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JC.JOB_CARD_TRN_PK,JC.CUST_CUSTOMER_MST_FK,CU.CUSTOMER_NAME, JC.JOBCARD_REF_NO, JC.HBL_HAWB_REF_NO, JC.MBL_MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO ORDER BY JOBCARD_REF_NO ASC ");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
            buildQuery.Append("  where  ");
            buildQuery.Append("     SR_NO between " + start + " and " + last);
            strSQL = buildQuery.ToString();

            DataSet ds = null;
            try
            {
                ds = objWF.GetDataSet(strSQL);
                return ds;
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

        #endregion "Fetch Afetr Attach JC Fill Grid"

        #region " Fetch Max Ref No."

        public string FetchRefNo(string strRFQNo)
        {
            try
            {
                string strSQL = null;
                strSQL = "SELECT NVL(MAX(JC.JOBCARD_REF_NO),0) FROM JOB_CARD_TRN JC" + "WHERE JC.JOBCARD_REF_NO LIKE '" + strRFQNo + "/%' " + "ORDER BY JC.JOBCARD_REF_NO";
                return (new WorkFlow()).ExecuteScaler(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Fetch Max Ref No."

        #region "Fetch Previous Job Card No"

        public string FetchPreviousJCrefNr(int JobCardPk)
        {
            string strSQL = null;

            strSQL = " SELECT JCT.JOBCARD_PREV_REF_NO" + " FROM JOB_CARD_TRN JCT" + "WHERE" + " JCT.JOB_CARD_TRN_PK =" + JobCardPk;
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Previous Job Card No"

        #region "Save Function"

        public ArrayList Save(DataSet M_DataSet, bool isEdting, string MSTJCRefNo, string Location, string EmpPk, long MSTJobCardPK, DataSet GridDS, Int32 Attach = 0, Int32 Detach = 0, string JCRefNo = "0",
        string Grid_Job_Ref_Nr = "0", string sid = "", string polid = "", string podid = "", long AirlinetrnPk = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            int intPKVal = 0;
            long lngI = 0;
            string MSTJCRefNum = null;
            Int32 RecAfct = default(Int32);
            string chkflag = null;
            if (string.IsNullOrEmpty(MSTJCRefNo))
            {
                MSTJCRefNum = GenerateProtocolKey("MASTER JC AIR IMPORT", Convert.ToInt64(Location), Convert.ToInt32(EmpPk), DateTime.Now, "", "", polid, CREATED_BY, new WorkFlow(), sid,
                podid);
                if (MSTJCRefNum == "Protocol Not Defined.")
                {
                    arrMessage.Add("Protocol Not Defined.");
                    return arrMessage;
                }
                else if (MSTJCRefNum.Length > 20)
                {
                    arrMessage.Add("Protocol Should Be Less Than 20 Characters");
                    return arrMessage;
                }
            }
            else
            {
                MSTJCRefNum = MSTJCRefNo;
            }

            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand attCommand = new OracleCommand();
            attCommand.Transaction = TRAN;

            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;

                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".MASTER_JC_AIR_IMP_TBL_PKG.MASTER_JC_AIR_IMP_TBL_INS";
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

                insCommand.Parameters.Add("AOD_ATA_IN", OracleDbType.Date, 20, "AOD_ATA").Direction = ParameterDirection.Input;
                insCommand.Parameters["AOD_ATA_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 25, "FLIGHT_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("AIRLINE_MST_FK_IN", OracleDbType.Int32, 10, "AIRLINE_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["AIRLINE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(CREATED_BY)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("AOD_ETA_IN", OracleDbType.Date, 20, "AOD_ETA").Direction = ParameterDirection.Input;
                insCommand.Parameters["AOD_ETA_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VOYAGE_TRN_FK_IN", Convert.ToInt64(AirlinetrnPk)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_AIR_IMP_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".MASTER_JC_AIR_IMP_TBL_PKG.MASTER_JC_AIR_IMP_TBL_UPD";
                var _with4 = _with3.Parameters;

                updCommand.Parameters.Add("MASTER_JC_AIR_IMP_PK_IN", OracleDbType.Int32, 10, "MASTER_JC_AIR_IMP_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MASTER_JC_AIR_IMP_PK_IN"].SourceVersion = DataRowVersion.Current;

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

                updCommand.Parameters.Add("AOD_ATA_IN", OracleDbType.Date, 20, "AOD_ATA").Direction = ParameterDirection.Input;
                updCommand.Parameters["AOD_ATA_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 25, "FLIGHT_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AIRLINE_MST_FK_IN", OracleDbType.Int32, 10, "AIRLINE_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["AIRLINE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(LAST_MODIFIED_BY)).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("AOD_ETA_IN", OracleDbType.Date, 20, "AOD_ETA").Direction = ParameterDirection.Input;
                updCommand.Parameters["AOD_ETA_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("VOYAGE_TRN_FK_IN", Convert.ToInt64(AirlinetrnPk)).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_AIR_IMP_PK").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = objWK.MyDataAdapter;

                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;

                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;
                if ((M_DataSet.GetChanges(DataRowState.Added) != null))
                {
                    chkflag = "1";
                }
                else
                {
                    chkflag = "0";
                }
                RecAfct = _with5.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    if (chkflag!="0")
                    {
                        RollbackProtocolKey("MASTER JC AIR IMPORT", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    }
                    return arrMessage;
                }
                else
                {
                    if (Attach == 1 | Detach == 2)
                    {
                        arrMessage = AttachSave(M_DataSet, GridDS, insCommand, objWK.MyUserName, Detach, JCRefNo, Grid_Job_Ref_Nr, Attach);
                        if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0))
                        {
                            TRAN.Rollback();
                            if (chkflag!="0")
                            {
                                RollbackProtocolKey("MASTER JC AIR IMPORT", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                            }
                            return arrMessage;
                        }
                        else
                        {
                            arrMessage.Clear();
                        }
                    }
                    if (isEdting == false)
                    {
                        MSTJobCardPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    }
                    //Modified by Faheem as to apportion the cost only when MJC is closed
                    if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["MASTER_JC_CLOSED_ON"].ToString()))
                    {
                        OracleCommand ObjCommand = new OracleCommand();
                        var _with6 = ObjCommand;
                        _with6.Connection = objWK.MyConnection;
                        _with6.CommandType = CommandType.StoredProcedure;
                        ObjCommand.Transaction = TRAN;
                        _with6.CommandText = objWK.MyUserName + ".JC_COST_PIA_CALCULATION_PKG.JC_AIR_IMP_COST_PIA_CALC";
                        var _with7 = _with6.Parameters;
                        _with7.Add("MASTER_JC_AIR_IMP_PK_IN", MSTJobCardPK).Direction = ParameterDirection.Input;
                        _with7.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with6.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        _with6.ExecuteNonQuery();
                    }
                    //End
                    arrMessage.Add("All Data Saved Successfully");
                    TRAN.Commit();
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                if (chkflag!="0")
                {
                    RollbackProtocolKey("MASTER JC AIR IMPORT", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                throw oraexp;
            }
            catch (Exception ex)
            {
                if (chkflag!="0")
                {
                    RollbackProtocolKey("MASTER JC AIR IMPORT", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                throw ex;
            }
            finally
            {
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

            try
            {
                for (i = 0; i <= GridDS.Tables[0].Rows.Count - 1; i++)
                {
                    attCommand.Parameters.Clear();
                    var _with8 = attCommand;
                    _with8.CommandType = CommandType.StoredProcedure;
                    _with8.CommandText = user + ".MASTER_JC_AIR_IMP_TBL_PKG.JOB_CARD_AIR_IMP_TBL_UPD";
                    var _with9 = _with8.Parameters;
                    str1 = Convert.ToString(GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]);
                    str2 = Convert.ToString(str1.IndexOf("/"));
                    if (Convert.ToInt32(str2) >= 0)
                    {
                        _with9.Add("JOB_CARD_AIR_IMP_PK_IN", GridDS.Tables[0].Rows[i]["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;
                        _with9.Add("JOBCARD_REF_NO_IN", GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]).Direction = ParameterDirection.Input;
                        _with9.Add("AIRLINE_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["AIRLINE_MST_FK"]).Direction = ParameterDirection.Input;
                        _with9.Add("LOAD_AGENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["LOAD_AGENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with9.Add("FLIGHT_NO_IN", M_DataSet.Tables[0].Rows[0]["FLIGHT_NO"]).Direction = ParameterDirection.Input;
                        _with9.Add("ARRIVAL_DATE_IN", M_DataSet.Tables[0].Rows[0]["AOD_ATA"]).Direction = ParameterDirection.Input;
                        _with9.Add("MASTER_JC_AIR_IMP_FK_IN", M_DataSet.Tables[0].Rows[0]["MASTER_JC_AIR_IMP_PK"]).Direction = ParameterDirection.Input;
                        _with9.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(LAST_MODIFIED_BY)).Direction = ParameterDirection.Input;
                        _with9.Add("VERSION_NO_IN", GridDS.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
                        _with9.Add("CONFIG_PK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;
                        _with9.Add("ETA_DATE_IN", M_DataSet.Tables[0].Rows[0]["AOD_ETA"]).Direction = ParameterDirection.Input;
                        _with9.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        attCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        _with9.Add("JOB_CARD_AIR_IMP_PK_IN", GridDS.Tables[0].Rows[i]["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;
                        _with9.Add("JOBCARD_REF_NO_IN", GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]).Direction = ParameterDirection.Input;
                        _with9.Add("AIRLINE_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["AIRLINE_MST_FK"]).Direction = ParameterDirection.Input;
                        _with9.Add("LOAD_AGENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["LOAD_AGENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with9.Add("FLIGHT_NO_IN", M_DataSet.Tables[0].Rows[0]["FLIGHT_NO"]).Direction = ParameterDirection.Input;
                        _with9.Add("ARRIVAL_DATE_IN", M_DataSet.Tables[0].Rows[0]["AOD_ATA"]).Direction = ParameterDirection.Input;

                        if (object.ReferenceEquals(GridDS.Tables[0].Rows[i]["MASTER_JC_FK"], ""))
                        {
                            _with9.Add("MASTER_JC_AIR_IMP_FK_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Add("MASTER_JC_AIR_IMP_FK_IN", GridDS.Tables[0].Rows[i]["MASTER_JC_FK"]).Direction = ParameterDirection.Input;
                        }

                        _with9.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(LAST_MODIFIED_BY)).Direction = ParameterDirection.Input;
                        _with9.Add("VERSION_NO_IN", GridDS.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
                        _with9.Add("CONFIG_PK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;
                        _with9.Add("ETA_DATE_IN", M_DataSet.Tables[0].Rows[0]["AOD_ETA"]).Direction = ParameterDirection.Input;
                        _with9.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        attCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    }
                    _with8.ExecuteNonQuery();
                    //Update Track and Trace
                    if (Attach == 1)
                    {
                        str1 = Convert.ToString(GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]);
                        //str2 = str1.IndexOf("/")
                        //If str2 >= 0 Then ' Attach
                        if (!string.IsNullOrEmpty(Grid_Job_Ref_Nr))
                        {
                            arr = Grid_Job_Ref_Nr.Split(',');
                            for (m = 0; m <= Grid_Job_Ref_Nr.Length - 1; m++)
                            {
                                if (i == m)
                                {
                                    UpdateTrackandTrace(Convert.ToString(arr.GetValue(m)), str1);
                                }
                            }
                        }
                        //End If
                    }
                }
                arrMessage.Add("Saved");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                arrMessage.Add(oraexp.Message);

                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        //End Rabbani

        #endregion "Save Function"

        #region "FetchExistingJCPK"

        //Added by rabbani reason USS Gap
        public DataSet FetchExistingJCPK(string MSTJCPK)
        {
            string strSQL = null;

            strSQL = "SELECT JOB.JOB_CARD_TRN_PK  JOB_CARD_AIR_IMP_PK from JOB_CARD_TRN JOB,MASTER_JC_AIR_IMP_TBL MST where JOB.MASTER_JC_FK=MST.MASTER_JC_AIR_IMP_PK(+) AND MST.MASTER_JC_AIR_IMP_PK=" + MSTJCPK;

            try
            {
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Ended by rabbani

        #endregion "FetchExistingJCPK"

        #region "Enhance Search Function"

        public string FetchMasterJobCardAirCON(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strReq = null;
            string POL = null;
            string POD = null;
            string operatorPK = null;

            var strNull = "";

            arr = strCond.Split('~');

            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            POL = Convert.ToString(arr.GetValue(2));
            POD = Convert.ToString(arr.GetValue(3));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_MASTER_PKG.GET_MASTERJOBCARDAIR";
                var _with10 = selectCommand.Parameters;
                _with10.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with10.Add("LOOKUP_VALUE_IN", (!string.IsNullOrEmpty(strReq) ? strReq : strNull)).Direction = ParameterDirection.Input;
                _with10.Add("POL_IN", (!string.IsNullOrEmpty(POL) ? POL : strNull)).Direction = ParameterDirection.Input;
                _with10.Add("POD_IN", (!string.IsNullOrEmpty(POD) ? POD : strNull)).Direction = ParameterDirection.Input;
                _with10.Add("LOC_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                //Snigdharani - 15/12/2008
                _with10.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
                selectCommand.Connection.Close();
            }
        }

        #endregion "Enhance Search Function"

        #region "Fetch ATD"

        public string FetchATD(string MJCPK)
        {
            string strSQL = null;
            strSQL = "select mm.aoo_atd" + "from MASTER_JC_AIR_IMP_TBL mm" + "where mm.MASTER_JC_AIR_IMP_PK=" + MJCPK;
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch ATD"

        #region "Update Track and Trace "

        public string UpdateTrackandTrace(string NewJobRef, string PREV_JOB_REF)
        {
            string strSQL = null;
            strSQL = "update track_n_trace_tbl t";
            strSQL = strSQL + "set t.doc_ref_no= '" + PREV_JOB_REF + "'";
            strSQL = strSQL + "where t.doc_ref_no= '" + NewJobRef + "'";
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Update Track and Trace "

        #region "De-Console MasterJobCard"

        public ArrayList DeConsoleMJCImp(Int64 MJCIMPPK)
        {
            OracleCommand Cmd = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            int RAF = 0;
            objWK.OpenConnection();
            string SRet_Value = null;
            arrMessage.Clear();
            try
            {
                var _with11 = Cmd;
                _with11.Parameters.Clear();
                _with11.Connection = objWK.MyConnection;
                _with11.CommandType = CommandType.StoredProcedure;
                _with11.CommandText = objWK.MyUserName + ".MASTER_JC_AIR_EXP_TBL_PKG.MJC_IMP_DECONSOLE";
                var _with12 = _with11.Parameters;
                _with12.Add("MJC_AIR_IMP_PK_IN", MJCIMPPK).Direction = ParameterDirection.Input;
                _with12.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, SRet_Value).Direction = ParameterDirection.Output;
                RAF = Cmd.ExecuteNonQuery();
            }
            catch (OracleException oraexp)
            {
                if (oraexp.ErrorCode == 20999)
                {
                    arrMessage.Add("20999");
                }
                else
                {
                    arrMessage.Add(oraexp.Message);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            if (arrMessage.Count > 0)
            {
                return arrMessage;
            }
            else
            {
                arrMessage.Add("Saved");
                return arrMessage;
            }
        }

        #endregion "De-Console MasterJobCard"
    }
}