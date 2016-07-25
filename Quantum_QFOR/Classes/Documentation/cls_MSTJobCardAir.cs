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
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_MSTJobCardAir : CommonFeatures
    {
        #region "Private Variables"
        #endregion
        private string _MSTJCRefNumber;

        #region "Property"
        public string MSTJCRefNumber
        {
            get { return _MSTJCRefNumber; }
        }
        #endregion

        #region "Fetch Master Jobcard"
        public DataSet FetchAll(string MSTJCRefNo = "", bool ActiveOnly = true, string POLPk = "", string PODPk = "", string AgentPk = "", string SearchType = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string POLID = "", string PODId = "",
        string POLName = "", string PODName = "", long lngUsrLocFk = 0, string strColumnName = "", bool blnSortAscending = false, Int32 flag = 0)
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
            if (flag == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }
            if (ActiveOnly)
            {
                buildCondition.Append(" AND M.MASTER_JC_STATUS = 1 ");
                //Else
                //    buildCondition.Append(vbCrLf & " AND M.MASTER_JC_STATUS = 2 ")
            }

            if (POLPk.Length > 0)
            {
                buildCondition.Append(" AND M.PORT_MST_POL_FK = " + Convert.ToInt32(POLPk));
            }

            if (PODPk.Length > 0)
            {
                buildCondition.Append(" AND M.PORT_MST_POD_FK = " + Convert.ToInt32(PODPk));
            }

            if (AgentPk.Length > 0)
            {
                buildCondition.Append(" AND M.DP_AGENT_MST_FK = " + Convert.ToInt32(AgentPk));
            }
            buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + "");
            buildCondition.Append("  AND M.CREATED_BY_FK = UMT.USER_MST_PK ");
            strCondition = buildCondition.ToString();

            buildQuery.Append(" SELECT ");
            buildQuery.Append(" COUNT(*) ");
            buildQuery.Append(" FROM MASTER_JC_AIR_EXP_TBL M, ");
            buildQuery.Append(" PORT_MST_TBL          POL, ");
            buildQuery.Append(" PORT_MST_TBL          POD, ");
            buildQuery.Append(" AGENT_MST_TBL         AMT, ");
            buildQuery.Append("  AIRLINE_MST_TBL        ART, ");
            //'
            buildQuery.Append(" USER_MST_TBL         UMT ");
            buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
            buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK AND AMT.AGENT_MST_PK(+) = M.DP_AGENT_MST_FK ");
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
            buildQuery.Append(" to_date(M.MASTER_JC_DATE,'DD/MM/RRRR') MSTJCDATE, ");
            buildQuery.Append(" ART.AIRLINE_NAME, ");
            ///
            buildQuery.Append(" M.PORT_MST_POL_FK POLFK, POL.PORT_ID POLNAME, ");
            buildQuery.Append(" M.PORT_MST_POD_FK PODFK, POD.PORT_ID PODNAME, ");
            buildQuery.Append(" M.AOO_ATD ETD, ");
            ///
            buildQuery.Append(" M.DP_AGENT_MST_FK AGENTFK,  AMT.AGENT_NAME AGENTNAME, ");
            buildQuery.Append(" M.MASTER_JC_AIR_EXP_PK MSTJCPK, ");

            buildQuery.Append(" (SELECT SUM(JCONT.CHARGEABLE_WEIGHT) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
            buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
            buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_EXP_PK  ");
            buildQuery.Append(" GROUP BY M.MASTER_JC_AIR_EXP_PK) AS CHARGEABLE_WEIGHT, ");

            //buildQuery.Append(vbCrLf & " (SELECT SUM(JCONT.VOLUME_IN_CBM) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ")
            //buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
            //buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_AIR_EXP_FK = M.MASTER_JC_AIR_EXP_PK ")
            //buildQuery.Append(vbCrLf & " GROUP BY M.MASTER_JC_AIR_EXP_PK) AS VOLUME_IN_CBM, ")

            buildQuery.Append(" (SELECT SUM(JCONT.GROSS_WEIGHT) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
            buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
            buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_EXP_PK ");
            buildQuery.Append(" GROUP BY M.MASTER_JC_AIR_EXP_PK) AS GROSS_WEIGHT, ");

            //buildQuery.Append(vbCrLf & " (SELECT SUM(JCONT.CHARGEABLE_WEIGHT) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ")
            //buildQuery.Append(vbCrLf & " WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ")
            //buildQuery.Append(vbCrLf & " AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_EXP_PK  ")
            //buildQuery.Append(vbCrLf & " GROUP BY M.MASTER_JC_AIR_EXP_PK) AS CHARGEABLE_WEIGHT ")

            buildQuery.Append(" (SELECT SUM(JCONT.VOLUME_IN_CBM) FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC ");
            buildQuery.Append(" WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
            buildQuery.Append(" AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_EXP_PK ");
            buildQuery.Append(" GROUP BY M.MASTER_JC_AIR_EXP_PK) AS VOLUME_IN_CBM ");

            buildQuery.Append(" FROM MASTER_JC_AIR_EXP_TBL M, ");
            buildQuery.Append(" PORT_MST_TBL          POL, ");
            buildQuery.Append(" PORT_MST_TBL          POD, ");
            buildQuery.Append(" AGENT_MST_TBL         AMT, ");
            buildQuery.Append("  AIRLINE_MST_TBL        ART ,");
            ///
            buildQuery.Append(" USER_MST_TBL         UMT ");
            buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
            buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK AND AMT.AGENT_MST_PK(+) = M.DP_AGENT_MST_FK ");
            buildQuery.Append(" AND ART.AIRLINE_MST_PK(+)=M.AIRLINE_MST_FK ");
            ///
            buildQuery.Append("               " + strCondition);
            //buildQuery.Append(vbCrLf & "      ORDER BY M.MASTER_JC_DATE DESC,M.MASTER_JC_REF_NO desc ")
            //Manoharan 18June2007: to Sort the columns as per the Header click
            if (string.IsNullOrEmpty(strColumnName.Trim()))
            {
                buildQuery.Append(" ORDER BY M.MASTER_JC_DATE DESC,M.MASTER_JC_REF_NO desc ");
            }
            else
            {
                buildQuery.Append(" ORDER BY " + strColumnName);
            }

            if (!blnSortAscending & !string.IsNullOrEmpty(strColumnName.Trim()))
            {
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

            try
            {
                DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), "", "", "", "", POLID, PODId, POLName, PODName, "", "", "", "2"));
                DataRelation trfRel = new DataRelation("TariffRelation", DS.Tables[0].Columns["MSTJCPK"], DS.Tables[1].Columns["MASTER_JC_AIR_EXP_FK"], true);
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
                buildCondition.Append("     BOOKING_MST_TBL BK, JOB_CARD_TRN JC,");
                buildCondition.Append("     HAWB_EXP_TBL HAWB, ");
                buildCondition.Append("     MAWB_EXP_TBL MAWB, ");
                buildCondition.Append("     AGENT_MST_TBL DPA, ");
            }
            else
            {
                buildCondition.Append(" JOB_CARD_TRN JC,");
            }
            buildCondition.Append("     CUSTOMER_MST_TBL SH,");
            buildCondition.Append("     CUSTOMER_MST_TBL CO,");
            buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            buildCondition.Append("     AGENT_MST_TBL CBA, ");
            buildCondition.Append("     AGENT_MST_TBL CLA, MASTER_JC_AIR_EXP_TBL MJ, JOB_TRN_CONT JCONT ");

            buildCondition.Append("      where ");
            // JOIN CONDITION
            if (process == "2")
            {
                buildCondition.Append("      BK.BOOKING_MST_PK = JC.BOOKING_MST_FK (+)");
                buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_AIR_EXP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildCondition.Append("    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
                buildCondition.Append("    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
                buildCondition.Append("    AND  JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
                buildCondition.Append("   AND JC.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK (+)");
                buildCondition.Append("   AND JC.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK (+) AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ");
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

            if (jobrefNO.Length > 0)
            {
                buildCondition.Append(" AND UPPER(JOBCARD_REF_NO) LIKE '" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
                //If jcStatus = "1" Then
                //    buildCondition.Append(vbCrLf & " AND JOB_CARD_STATUS=" & jcStatus & ")")
                //End If
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

            if (process == "2")
            {
                if (bookingNo.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(BOOKING_REF_NO) LIKE '" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                    buildCondition.Append(" AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
                }
                buildCondition.Append(" AND BK.STATUS=2");
            }
            if (jcStatus.Length > 0)
            {
                buildCondition.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
            }
            if (Hbl.Trim().Length > 0)
            {
                buildCondition.Append(" AND UPPER(HAWB_REF_NO) LIKE '" + Hbl.ToUpper().Replace("'", "''") + "%'");
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
                buildQuery.Append("       BOOKING_MST_PK, ");
                buildQuery.Append("       BOOKING_REF_NO, ");
            }
            else
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       PORT_MST_POL_FK, ");
                buildQuery.Append("       PORT_MST_POD_FK, ");
            }
            buildQuery.Append("       JOBCARD_REF_NO, ");
            buildQuery.Append("       HAWB_REF_NO, ");
            buildQuery.Append("       MAWB_REF_NO, JC.MASTER_JC_FK MASTER_JC_AIR_EXP_FK, ");
            //buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT ")
            buildQuery.Append("      SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,  SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT,SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM  ");
            buildQuery.Append("      from ");
            buildQuery.Append(strCondition);
            //buildQuery.Append(vbCrLf & "      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HAWB_REF_NO, MAWB_REF_NO, JC.MASTER_JC_AIR_EXP_FK ORDER BY JOBCARD_REF_NO ASC ")
            buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HAWB_REF_NO, MAWB_REF_NO, JC.MASTER_JC_FK,JC.JOBCARD_DATE ORDER BY JOBCARD_DATE DESC,JOBCARD_REF_NO DESC ");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
            // AND (JOB.JOB_CARD_STATUS IS NULL OR JOB.JOB_CARD_STATUS = 1 )
            // band1_SRNO = 17       :   band1_RfqSpotFK = 18    :   band1_PortPOLFK = 19    :   band1_PortPOLID = 20
            // band1_PortPOLName = 21:   band1_PortPODFK = 22    :   band1_PortPODID = 23    :   band1_PortPODName = 24
            // band1_ValidFrom = 25  :   band1_ValidTo = 26

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
                    if (Convert.ToInt32(dt.Rows[RowCnt]["MASTER_JC_AIR_EXP_FK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["MASTER_JC_AIR_EXP_FK"]);
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
            try
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
            catch (Exception ex)
            {
                throw ex;
            }
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
            buildQuery.Append(" M.DP_AGENT_MST_FK, AMT.AGENT_ID, AMT.AGENT_NAME, ");
            buildQuery.Append(" M.MASTER_JC_AIR_EXP_PK, M.VERSION_NO, M.REMARKS, ");
            buildQuery.Append(" M.AOO_ATD,M.FLIGHT_NO,M.AIRLINE_MST_FK,AIR.AIRLINE_ID,AIR.AIRLINE_NAME,M.COMMODITY_GROUP_FK,M.AOO_ETD");
            buildQuery.Append(" FROM MASTER_JC_AIR_EXP_TBL M, ");
            buildQuery.Append(" PORT_MST_TBL          POL,      PORT_MST_TBL          POD, ");
            buildQuery.Append(" AGENT_MST_TBL         AMT,AIRLINE_MST_TBL AIR ");
            buildQuery.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK ");
            buildQuery.Append(" AND POD.PORT_MST_PK = M.PORT_MST_POD_FK ");
            buildQuery.Append(" AND AMT.AGENT_MST_PK(+) = M.DP_AGENT_MST_FK ");
            buildQuery.Append(" AND M.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK(+) ");
            buildQuery.Append(" AND M.MASTER_JC_AIR_EXP_PK = " + MSTJCPK);
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

        public DataSet FetchGridData(string CONTSpotPKs = "", string jobrefNO = "", string bookingNo = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "",
        string consignee = "", string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, bool BOOKING = false, Int32 CurrentPage = 0, Int32 TotalPage = 0)
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
                buildCondition.Append("     BOOKING_MST_TBL BK, JOB_CARD_TRN JC,");
                buildCondition.Append("     HAWB_EXP_TBL HAWB, ");
                buildCondition.Append("     MAWB_EXP_TBL MAWB, ");
                buildCondition.Append("     AGENT_MST_TBL DPA, ");
            }
            else
            {
                buildCondition.Append(" JOB_CARD_TRN JC,");
            }
            buildCondition.Append("     CUSTOMER_MST_TBL SH,");
            buildCondition.Append("     CUSTOMER_MST_TBL CO,");
            buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            buildCondition.Append("     AGENT_MST_TBL CBA, ");
            buildCondition.Append("     AGENT_MST_TBL CLA, MASTER_JC_AIR_EXP_TBL MJ, JOB_TRN_CONT JCONT");


            buildCondition.Append("      where ");
            // JOIN CONDITION
            if (process == "2")
            {
                buildCondition.Append("      BK.BOOKING_MST_PK = JC.BOOKING_MST_FK (+)");
                buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_AIR_EXP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildCondition.Append("    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
                buildCondition.Append("    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
                buildCondition.Append("    AND  JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
                buildCondition.Append("   AND JC.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK (+)");
                buildCondition.Append("   AND JC.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK (+) AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
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

            if (jobrefNO.Length > 0)
            {
                buildCondition.Append(" AND UPPER(JOBCARD_REF_NO) LIKE '" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
                //If jcStatus = "1" Then
                //    buildCondition.Append(vbCrLf & " AND JOB_CARD_STATUS=" & jcStatus & ")")
                //End If
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

            if (process == "2")
            {
                if (bookingNo.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(BOOKING_REF_NO) LIKE '" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                    buildCondition.Append(" AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
                }
                buildCondition.Append(" AND BK.STATUS=2");
            }
            if (jcStatus.Length > 0)
            {
                buildCondition.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
            }
            if (Hbl.Trim().Length > 0)
            {
                buildCondition.Append(" AND UPPER(HAWB_REF_NO) LIKE '" + Hbl.ToUpper().Replace("'", "''") + "%'");
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
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       BOOKING_MST_PK, ");
                buildQuery.Append("       BOOKING_REF_NO, ");
            }
            else
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       PORT_MST_POL_FK, ");
                buildQuery.Append("       PORT_MST_POD_FK, ");
            }
            buildQuery.Append("       JOBCARD_REF_NO, ");
            buildQuery.Append("       HAWB_REF_NO, ");
            buildQuery.Append("       MAWB_REF_NO, JC.MASTER_JC_FK MASTER_JC_AIR_EXP_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK");
            //buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT ")
            buildQuery.Append("      from ");
            buildQuery.Append(strCondition);
            //buildQuery.Append(vbCrLf & "      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HAWB_REF_NO, MAWB_REF_NO, JC.MASTER_JC_AIR_EXP_FK ORDER BY JOBCARD_REF_NO ASC ")
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
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       BOOKING_MST_PK, ");
                buildQuery.Append("       BOOKING_REF_NO, ");
            }
            else
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       PORT_MST_POL_FK, ");
                buildQuery.Append("       PORT_MST_POD_FK, ");
            }
            buildQuery.Append("       JOBCARD_REF_NO, ");
            buildQuery.Append("       HAWB_REF_NO, ");
            buildQuery.Append("       MAWB_REF_NO, JC.MASTER_JC_FK MASTER_JC_AIR_EXP_FK, ");
            buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT,  SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel,");
            buildQuery.Append("      JC.MBL_MAWB_FK,JC.HBL_HAWB_FK, (select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
            buildQuery.Append("      from ");
            buildQuery.Append(strCondition);
            buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HAWB_REF_NO, MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ORDER BY JOBCARD_REF_NO ASC ");
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
        #endregion

        #region "Fetch Afetr Attach JC Fill Grid"
        public DataSet FetchAttachGridData(string JCPKs = "", string jobrefNO = "", string bookingNo = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "",
        string consignee = "", string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, bool BOOKING = false, Int32 CurrentPage = 0, Int32 TotalPage = 0, string MSTJCPK = "",
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
                buildConditiontab.Append("     BOOKING_MST_TBL BK, JOB_CARD_TRN JC,");
                buildConditiontab.Append("     HAWB_EXP_TBL HAWB, ");
                buildConditiontab.Append("     MAWB_EXP_TBL MAWB, ");
                buildConditiontab.Append("     AGENT_MST_TBL DPA, ");
            }
            else
            {
                buildConditiontab.Append(" JOB_CARD_TRN JC,");
            }
            buildConditiontab.Append("     CUSTOMER_MST_TBL SH,");
            buildConditiontab.Append("     CUSTOMER_MST_TBL CO,");
            buildConditiontab.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            buildConditiontab.Append("     AGENT_MST_TBL CBA, ");
            buildConditiontab.Append("     AGENT_MST_TBL CLA,JOB_TRN_CONT JCONT ");

            if (Flag == 1)
            {
                buildConditiontab1.Append("     ,MASTER_JC_AIR_EXP_TBL MJ ");
            }

            buildConditionCon.Append("      where ");
            // JOIN CONDITION
            if (process == "2")
            {
                buildConditionCon.Append("      BK.BOOKING_MST_PK = JC.BOOKING_MST_FK (+)");
                buildConditionCon.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildConditionCon.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildConditionCon.Append("    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)");
                buildConditionCon.Append("    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ");
                buildConditionCon.Append("    AND  JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
                buildConditionCon.Append("   AND JC.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK (+)");
                buildConditionCon.Append("   AND JC.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK (+) AND JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                if (Flag == 1)
                {
                    buildConditionCon1.Append("   AND JC.MASTER_JC_FK = MJ.MASTER_JC_AIR_EXP_PK AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                    buildConditionCon1.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                }
            }
            else
            {
                buildConditionCon.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildConditionCon.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildConditionCon.Append("   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildConditionCon.Append("   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildConditionCon.Append("   AND JC.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)");
                buildConditionCon.Append("   AND JC.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ");
            }

            if (jobrefNO.Length > 0)
            {
                buildConditionCon.Append(" AND UPPER(JOBCARD_REF_NO) LIKE '" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
                //If jcStatus = "1" Then
                //    buildCondition.Append(vbCrLf & " AND JOB_CARD_STATUS=" & jcStatus & ")")
                //End If
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

            if (process == "2")
            {
                if (bookingNo.Length > 0)
                {
                    buildConditionCon.Append(" AND UPPER(BOOKING_REF_NO) LIKE '" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                    buildConditionCon.Append(" AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
                }
                buildConditionCon.Append(" AND BK.STATUS=2");
            }
            if (jcStatus.Length > 0)
            {
                buildConditionCon.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
            }
            if (Hbl.Trim().Length > 0)
            {
                buildConditionCon.Append(" AND UPPER(HAWB_REF_NO) LIKE '" + Hbl.ToUpper().Replace("'", "''") + "%'");
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
                //buildConditionConMSTJCPK1.Append(vbCrLf & " and JC.JOB_CARD_TRN_PK IN (" & JCPKs & ") ")
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
            //CUST_CUSTOMER_MST_FK
            buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
            //
            if (process == "2")
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       BOOKING_MST_PK, ");
                buildQuery.Append("       BOOKING_REF_NO, ");
            }
            else
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       PORT_MST_POL_FK, ");
                buildQuery.Append("       PORT_MST_POD_FK, ");
            }
            buildQuery.Append("       JOBCARD_REF_NO, ");
            buildQuery.Append("       HAWB_REF_NO, ");
            buildQuery.Append("       MAWB_REF_NO, JC.MASTER_JC_FK MASTER_JC_AIR_EXP_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ");
            //buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT ")
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
                //CUST_CUSTOMER_MST_FK
                buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
                //
                if (process == "2")
                {
                    buildQuery.Append("       JOB_CARD_TRN_PK, ");
                    buildQuery.Append("       BOOKING_MST_PK, ");
                    buildQuery.Append("       BOOKING_REF_NO, ");
                }
                else
                {
                    buildQuery.Append("       JOB_CARD_TRN_PK, ");
                    buildQuery.Append("       PORT_MST_POL_FK, ");
                    buildQuery.Append("       PORT_MST_POD_FK, ");
                }
                buildQuery.Append("       JOBCARD_REF_NO, ");
                buildQuery.Append("       HAWB_REF_NO, ");
                buildQuery.Append("       MAWB_REF_NO, JC.MASTER_JC_FK MASTER_JC_AIR_EXP_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ");
                //buildQuery.Append(vbCrLf & "       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT ")
                buildQuery.Append("      from ");
                buildQuery.Append(strConditiontab);
                buildQuery.Append(strConditiontab1);
                buildQuery.Append(strConditionCon);
                buildQuery.Append(strConditionCon1);
                buildQuery.Append(strConditionConMSTJCPK1);

            }
            //buildQuery.Append(vbCrLf & "      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HAWB_REF_NO, MAWB_REF_NO, JC.MASTER_JC_AIR_EXP_FK ORDER BY JOBCARD_REF_NO ASC ")
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
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       BOOKING_MST_PK, ");
                buildQuery.Append("       BOOKING_REF_NO, ");
            }
            else
            {
                buildQuery.Append("       JOB_CARD_TRN_PK, ");
                buildQuery.Append("       PORT_MST_POL_FK, ");
                buildQuery.Append("       PORT_MST_POD_FK, ");
            }
            buildQuery.Append("       JOBCARD_REF_NO, ");
            buildQuery.Append("       HAWB_REF_NO, ");
            buildQuery.Append("       MAWB_REF_NO, JC.MASTER_JC_FK MASTER_JC_AIR_EXP_FK, ");
            buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ");
            buildQuery.Append("        , (select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
            //Manoharan EFS:17/04/07
            buildQuery.Append("      from ");
            buildQuery.Append(strConditiontab);
            buildQuery.Append(strConditionCon);
            buildQuery.Append(strConditionConAtt);
            buildQuery.Append(strConditionConJCPK);
            buildQuery.Append("AND JC.MASTER_JC_FK IS NULL");
            buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HAWB_REF_NO, MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ");

            if (Flag == 1)
            {
                buildQuery.Append("union");
                buildQuery.Append("       Select ");
                //CUST_CUSTOMER_MST_FK
                buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
                //
                if (process == "2")
                {
                    buildQuery.Append("       JOB_CARD_TRN_PK, ");
                    buildQuery.Append("       BOOKING_MST_PK, ");
                    buildQuery.Append("       BOOKING_REF_NO, ");
                }
                else
                {
                    buildQuery.Append("       JOB_CARD_TRN_PK, ");
                    buildQuery.Append("       PORT_MST_POL_FK, ");
                    buildQuery.Append("       PORT_MST_POD_FK, ");
                }
                buildQuery.Append("       JOBCARD_REF_NO, ");
                buildQuery.Append("       HAWB_REF_NO, ");
                buildQuery.Append("       MAWB_REF_NO, JC.MASTER_JC_FK MASTER_JC_AIR_EXP_FK, ");
                buildQuery.Append("       SUM(JCONT.VOLUME_IN_CBM) AS VOLUME_IN_CBM, SUM(JCONT.GROSS_WEIGHT) AS GROSS_WEIGHT, SUM(JCONT.CHARGEABLE_WEIGHT) AS CHARGEABLE_WEIGHT,JC.VERSION_NO,'' Sel,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ");
                buildQuery.Append("        , (select count(JPIA.INV_SUPPLIER_FK) from JOB_TRN_PIA JPIA where JPIA.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
                //Manoharan EFS:17/04/07
                buildQuery.Append("      from ");
                buildQuery.Append(strConditiontab);
                buildQuery.Append(strConditiontab1);
                buildQuery.Append(strConditionCon);
                buildQuery.Append(strConditionCon1);
                buildQuery.Append(strConditionConMSTJCPK1);

            }

            buildQuery.Append("      GROUP BY JC.SHIPPER_CUST_MST_FK, JOB_CARD_TRN_PK, BOOKING_MST_PK, BOOKING_REF_NO, JOBCARD_REF_NO, HAWB_REF_NO, MAWB_REF_NO, JC.MASTER_JC_FK,JC.VERSION_NO,JC.MBL_MAWB_FK,JC.HBL_HAWB_FK ORDER BY JOBCARD_REF_NO ASC ");
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
        #endregion

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
        #endregion

        #region "Delete in COST"
        //Public Function Delete_PIA(ByRef MSTJobCardPK As Long, Optional ByVal TRANSAC As OracleTransaction = Nothing)
        public void Delete_COST(long MSTJobCardPK, OracleTransaction TRANSAC = null)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            OracleCommand ObjCommand = new OracleCommand();
            string strSql = null;
            int recAfct = 0;

            try
            {


                //strSql = " DELETE FROM JOB_TRN_PIA P WHERE P.JOB_CARD_TRN_FK IN (SELECT J.JOB_CARD_TRN_PK "
                //strSql &= " FROM JOB_CARD_TRN J WHERE J.MASTER_JC_AIR_EXP_FK =" & MSTJobCardPK & ")"

                if ((TRANSAC != null))
                {
                    objWK.OpenConnection();
                    objWK.MyConnection = TRANSAC.Connection;
                    //Modified By Koteshwari on 7/5/2011
                    //strSql = " DELETE FROM JOB_TRN_PIA P WHERE P.MJC_TRN_AIR_EXP_PIA_FK =" & MSTJobCardPK
                    strSql = " DELETE FROM JOB_TRN_COST P WHERE P.MJC_TRN_COST_FK =" + MSTJobCardPK;
                    var _with1 = ObjCommand;
                    _with1.Connection = objWK.MyConnection;
                    _with1.CommandType = CommandType.Text;
                    _with1.CommandText = strSql;
                    _with1.Transaction = TRANSAC;
                    _with1.Parameters.Clear();
                    _with1.ExecuteNonQuery();
                }
                else
                {
                    objWK.OpenConnection();
                    TRAN = objWK.MyConnection.BeginTransaction();
                    //strSql = " DELETE FROM JOB_TRN_PIA P WHERE P.MJC_TRN_AIR_EXP_PIA_FK =" & MSTJobCardPK
                    strSql = " DELETE FROM JOB_TRN_COST P WHERE P.MJC_TRN_COST_FK =" + MSTJobCardPK;

                    var _with2 = ObjCommand;
                    _with2.Connection = objWK.MyConnection;
                    _with2.CommandType = CommandType.Text;
                    _with2.CommandText = strSql;
                    _with2.Transaction = TRAN;
                    _with2.Parameters.Clear();
                    recAfct = _with2.ExecuteNonQuery();

                    if (recAfct > 0)
                    {
                        TRAN.Commit();
                    }
                    else
                    {
                        TRAN.Rollback();
                    }
                }

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
        #endregion

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
        #endregion

        #region "Save Function"
        public ArrayList Save(DataSet M_DataSet, bool isEdting, string MSTJCRefNo, string Location, string EmpPk, long MSTJobCardPK, DataSet GridDS, Int32 Attach = 0, Int32 Detach = 0, string JCRefNo = "0",
        string strVoyagepk = "", string Grid_Job_Ref_Nr = "0", string sid = "", string polid = "", string podid = "", long Airline_trn_pk = 0)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
            TRAN = objWK.MyConnection.BeginTransaction();

            int intPKVal = 0;
            long lngI = 0;
            string MSTJCRefNum = null;
            Int32 RecAfct = default(Int32);
            bool chkflag = false;
            //Modified by Faheem
            if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["MASTER_JC_CLOSED_ON"].ToString()))
            {
                try
                {
                    //'Commented and Added By Koteshwari on 7/5/2011 
                    //Delete_PIA(MSTJobCardPK, TRAN)
                    Delete_COST(MSTJobCardPK, TRAN);
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();
                    throw ex;
                }
            }

            //end
            if (string.IsNullOrEmpty(MSTJCRefNo))
            {
                MSTJCRefNum = GenerateProtocolKey("MASTER JC AIR EXPORT", Convert.ToInt64(Location), Convert.ToInt64(EmpPk), DateTime.Now, "", "", polid, CREATED_BY, new WorkFlow(), sid,
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


                var _with3 = insCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".MASTER_JC_AIR_EXP_TBL_PKG.MASTER_JC_AIR_EXP_TBL_INS";
                var _with4 = _with3.Parameters;

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

                insCommand.Parameters.Add("AOO_ATD_IN", OracleDbType.Date, 20, "AOO_ATD").Direction = ParameterDirection.Input;
                insCommand.Parameters["AOO_ATD_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 25, "FLIGHT_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("AIRLINE_MST_FK_IN", OracleDbType.Int32, 10, "AIRLINE_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["AIRLINE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(CREATED_BY)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("AOO_ETD_IN", OracleDbType.Date, 20, "AOO_ETD").Direction = ParameterDirection.Input;
                insCommand.Parameters["AOO_ETD_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VOYAGE_TRN_FK_IN", Convert.ToInt64(Airline_trn_pk)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_AIR_EXP_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;



                var _with5 = updCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".MASTER_JC_AIR_EXP_TBL_PKG.MASTER_JC_AIR_EXP_TBL_UPD";
                var _with6 = _with5.Parameters;

                updCommand.Parameters.Add("MASTER_JC_AIR_EXP_PK_IN", OracleDbType.Int32, 10, "MASTER_JC_AIR_EXP_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MASTER_JC_AIR_EXP_PK_IN"].SourceVersion = DataRowVersion.Current;

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

                updCommand.Parameters.Add("AOO_ATD_IN", OracleDbType.Date, 20, "AOO_ATD").Direction = ParameterDirection.Input;
                updCommand.Parameters["AOO_ATD_IN"].SourceVersion = DataRowVersion.Current;

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

                updCommand.Parameters.Add("AOO_ETD_IN", OracleDbType.Date, 20, "AOO_ETD").Direction = ParameterDirection.Input;
                updCommand.Parameters["AOO_ETD_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("VOYAGE_TRN_FK_IN", Convert.ToInt64(Airline_trn_pk)).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_AIR_EXP_PK").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with7 = objWK.MyDataAdapter;

                _with7.InsertCommand = insCommand;
                _with7.InsertCommand.Transaction = TRAN;

                _with7.UpdateCommand = updCommand;
                _with7.UpdateCommand.Transaction = TRAN;

                //ADDED BY SURYA PRASAD for implementing protocol rollback on 18-feb-2008
                if ((M_DataSet.GetChanges(DataRowState.Added) != null))
                {
                    chkflag = true;
                }
                else
                {
                    chkflag = false;
                }
                //end

                RecAfct = _with7.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    //ADDED BY SURYA PRASAD for implementing protocol rollback on 18-feb-2008
                    if (chkflag)
                    {
                        RollbackProtocolKey("MASTER JC AIR EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                        chkflag = false;
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
                            //ADDED BY SURYA PRASAD for implementing protocol rollback on 18-feb-2008
                            if (chkflag)
                            {
                                RollbackProtocolKey("MASTER JC AIR EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                                chkflag = false;
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
                    TRAN.Commit();
                    //Modified by Faheem as to apportion the cost only when MJC is closed
                    if (!string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["MASTER_JC_CLOSED_ON"].ToString()))
                    {
                        OracleCommand ObjCommand = new OracleCommand();
                        var _with8 = ObjCommand;
                        _with8.Connection = objWK.MyConnection;
                        _with8.CommandType = CommandType.StoredProcedure;
                        //'Commented and AddED by Koteshwari on 7/5/2011
                        //.CommandText = objWK.MyUserName & ".JC_COST_PIA_CALCULATION_PKG.JC_AIR_EXP_COST_PIA_CALC"
                        _with8.CommandText = objWK.MyUserName + ".JC_COST_CALCULATION_PKG.JC_AIR_EXP_COST_CALC";
                        var _with9 = _with8.Parameters;
                        _with9.Add("MASTER_JC_AIR_EXP_PK_IN", MSTJobCardPK).Direction = ParameterDirection.Input;
                        _with9.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with8.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        _with8.ExecuteNonQuery();
                    }
                    //End
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }

            }
            catch (OracleException oraexp)
            {
                //ADDED BY SURYA PRASAD for implementing protocol rollback on 18-feb-2008
                if (chkflag)
                {
                    RollbackProtocolKey("MASTER JC AIR EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                    chkflag = false;
                }
                throw oraexp;
            }
            catch (Exception ex)
            {
                //ADDED BY SURYA PRASAD for implementing protocol rollback on 18-feb-2008
                if (chkflag)
                {
                    RollbackProtocolKey("MASTER JC AIR EXPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNum, System.DateTime.Now);
                    chkflag = false;
                }
                throw ex;
            }
            finally
            {
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


            try
            {
                for (i = 0; i <= GridDS.Tables[0].Rows.Count - 1; i++)
                {
                    attCommand.Parameters.Clear();
                    var _with10 = attCommand;
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = user + ".MASTER_JC_AIR_EXP_TBL_PKG.JOB_CARD_AIR_EXP_TBL_UPD";
                    var _with11 = _with10.Parameters;
                    str1 = Convert.ToString(GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]);
                    str2 = Convert.ToString(str1.IndexOf("/"));
                    if (str2.Length >= 0)
                    {
                        _with11.Add("JOB_CARD_AIR_EXP_PK_IN", GridDS.Tables[0].Rows[i]["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;
                        _with11.Add("BOOKING_AIR_FK_IN", GridDS.Tables[0].Rows[i]["BOOKING_MST_PK"]).Direction = ParameterDirection.Input;
                        _with11.Add("JOBCARD_REF_NO_IN", GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]).Direction = ParameterDirection.Input;
                        _with11.Add("AIRLINE_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["AIRLINE_MST_FK"]).Direction = ParameterDirection.Input;
                        _with11.Add("DP_AGENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["DP_AGENT_MST_FK"]).Direction = ParameterDirection.Input;
                        //.Add("VESSEL_NAME_IN", M_DataSet.Tables(0).Rows(0).Item("VESSEL_NAME")).Direction = ParameterDirection.Input
                        //.Add("VOYAGE_IN", M_DataSet.Tables(0).Rows(0).Item("VOYAGE")).Direction = ParameterDirection.Input
                        _with11.Add("FLIGHT_NO_IN", M_DataSet.Tables[0].Rows[0]["FLIGHT_NO"]).Direction = ParameterDirection.Input;
                        _with11.Add("DEPARTURE_DATE_IN", M_DataSet.Tables[0].Rows[0]["AOO_ATD"]).Direction = ParameterDirection.Input;
                        _with11.Add("MASTER_JC_AIR_EXP_FK_IN", M_DataSet.Tables[0].Rows[0]["MASTER_JC_AIR_EXP_PK"]).Direction = ParameterDirection.Input;
                        _with11.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(LAST_MODIFIED_BY)).Direction = ParameterDirection.Input;
                        _with11.Add("VERSION_NO_IN", GridDS.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
                        _with11.Add("CONFIG_PK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;
                        _with11.Add("ETD_DATE_IN", M_DataSet.Tables[0].Rows[0]["AOO_ETD"]).Direction = ParameterDirection.Input;
                        _with11.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        attCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        _with11.Add("JOB_CARD_AIR_EXP_PK_IN", GridDS.Tables[0].Rows[i]["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;
                        _with11.Add("BOOKING_AIR_FK_IN", GridDS.Tables[0].Rows[i]["BOOKING_MST_PK"]).Direction = ParameterDirection.Input;
                        _with11.Add("JOBCARD_REF_NO_IN", GridDS.Tables[0].Rows[i]["JOBCARD_REF_NO"]).Direction = ParameterDirection.Input;
                        _with11.Add("AIRLINE_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["AIRLINE_MST_FK"]).Direction = ParameterDirection.Input;
                        _with11.Add("DP_AGENT_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["DP_AGENT_MST_FK"]).Direction = ParameterDirection.Input;
                        //.Add("VESSEL_NAME_IN", M_DataSet.Tables(0).Rows(0).Item("VESSEL_NAME")).Direction = ParameterDirection.Input
                        //.Add("VOYAGE_IN", M_DataSet.Tables(0).Rows(0).Item("VOYAGE")).Direction = ParameterDirection.Input
                        _with11.Add("FLIGHT_NO_IN", M_DataSet.Tables[0].Rows[0]["FLIGHT_NO"]).Direction = ParameterDirection.Input;
                        _with11.Add("DEPARTURE_DATE_IN", M_DataSet.Tables[0].Rows[0]["AOO_ATD"]).Direction = ParameterDirection.Input;

                        //If Detach = 2 Then
                        if (object.ReferenceEquals(GridDS.Tables[0].Rows[i]["MASTER_JC_AIR_EXP_FK"], DBNull.Value))
                        {
                            _with11.Add("MASTER_JC_AIR_EXP_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with11.Add("MASTER_JC_AIR_EXP_FK_IN", GridDS.Tables[0].Rows[i]["MASTER_JC_AIR_EXP_FK"]).Direction = ParameterDirection.Input;
                        }

                        //Else
                        //.Add("MASTER_JC_AIR_EXP_FK_IN", M_DataSet.Tables(0).Rows(0).Item("MASTER_JC_AIR_EXP_PK")).Direction = ParameterDirection.Input

                        //End If

                        _with11.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(LAST_MODIFIED_BY)).Direction = ParameterDirection.Input;
                        _with11.Add("VERSION_NO_IN", GridDS.Tables[0].Rows[i]["VERSION_NO"]).Direction = ParameterDirection.Input;
                        _with11.Add("CONFIG_PK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;
                        _with11.Add("ETD_DATE_IN", M_DataSet.Tables[0].Rows[0]["AOO_ETD"]).Direction = ParameterDirection.Input;
                        _with11.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        attCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    }
                    _with10.ExecuteNonQuery();
                    //Update Track and Trace : Amit
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
        #endregion

        #region "FetchExistingJCPK"
        //Added by rabbani reason USS Gap
        public DataSet FetchExistingJCPK(string MSTJCPK)
        {
            string strSQL = null;

            strSQL = "SELECT JOB.JOB_CARD_TRN_PK  from JOB_CARD_TRN JOB,MASTER_JC_AIR_EXP_TBL MST where JOB.MASTER_JC_FK=MST.MASTER_JC_AIR_EXP_PK(+) AND MST.MASTER_JC_AIR_EXP_PK=" + MSTJCPK;

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
        #endregion

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


            dynamic strNull = DBNull.Value;

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
                var _with12 = selectCommand.Parameters;
                _with12.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with12.Add("LOOKUP_VALUE_IN", (!string.IsNullOrEmpty(strReq) ? strReq : strNull)).Direction = ParameterDirection.Input;
                _with12.Add("POL_IN", (!string.IsNullOrEmpty(POL) ? POL : strNull)).Direction = ParameterDirection.Input;
                _with12.Add("POD_IN", (!string.IsNullOrEmpty(POD) ? POD : strNull)).Direction = ParameterDirection.Input;
                _with12.Add("LOC_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                //Snigdharani - 15/12/2008
                _with12.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }
        #endregion

        #region "Fetch ATD"
        public string FetchATD(string MJCPK)
        {
            string strSQL = null;
            strSQL = "select mm.aoo_atd" + "from master_jc_air_exp_tbl mm" + "where mm.master_jc_air_exp_pk=" + MJCPK;
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
        #endregion
        //Sreenivas

        #region " Fetch Mater JobCard Air Export Records"
        public int GetMJCAE(string mjcaeRefNr, int mjcaePk, int mjaeActive, long locPk)
        {
            try
            {
                System.Text.StringBuilder strMJCAEQuery = new System.Text.StringBuilder(5000);

                strMJCAEQuery.Append(" select mjae.master_jc_air_exp_pk, mjae.master_jc_ref_no");
                strMJCAEQuery.Append(" from master_jc_air_exp_tbl mjae, user_mst_tbl umt");
                strMJCAEQuery.Append(" where mjae.master_jc_ref_no like '%" + mjcaeRefNr + "%'");
                strMJCAEQuery.Append(" and mjae.created_by_fk = umt.user_mst_pk");
                strMJCAEQuery.Append(" and umt.default_location_fk=" + locPk);
                if (mjaeActive == 1)
                {
                    strMJCAEQuery.Append(" and mjae.master_jc_status =1");
                }
                WorkFlow objWF = new WorkFlow();
                DataSet objMJCAEDS = new DataSet();
                objMJCAEDS = objWF.GetDataSet(strMJCAEQuery.ToString());
                if (objMJCAEDS.Tables[0].Rows.Count > 0)
                {
                    mjcaePk = Convert.ToInt32(objMJCAEDS.Tables[0].Rows[0][0]);
                }
                return objMJCAEDS.Tables[0].Rows.Count;
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
        #endregion

        #region "MJC Release Save Function"

        public ArrayList ReleaseMJC(int MSTJobCardPK, DataSet GridDS, string MSTJobCardNr)
        {

            try
            {
                WorkFlow objWK = new WorkFlow();
                Int16 Execute = default(Int16);
                string RefNrs = null;
                objWK.OpenConnection();
                OracleTransaction TRAN = default(OracleTransaction);
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;

                var _with13 = objWK.MyCommand;
                _with13.CommandText = objWK.MyUserName + ".MASTER_JC_AIR_EXP_TBL_PKG.AUTO_CREATE_MJC_AIR_IMP";
                _with13.Connection = objWK.MyConnection;
                _with13.CommandType = CommandType.StoredProcedure;
                _with13.Transaction = TRAN;
                var _with14 = _with13.Parameters;
                _with14.Add("MJC_EXP_TBL_PK_IN", MSTJobCardPK).Direction = ParameterDirection.Input;
                _with14.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_AIR_IMP_PK").Direction = ParameterDirection.Output;
                try
                {
                    Execute = Convert.ToInt16(_with13.ExecuteNonQuery());
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                    throw oraexp;
                }
                string MJImpPK = null;
                MJImpPK = Convert.ToString(_with13.Parameters["RETURN_VALUE"].Value);
                _with13.CommandText = objWK.MyUserName + ".MASTER_JC_AIR_EXP_TBL_PKG.AUTO_CREATE_JOB_CARD_AIR_IMP";
                DataRow dr = null;
                _with13.Connection = objWK.MyConnection;
                _with13.CommandType = CommandType.StoredProcedure;
                try
                {
                    foreach (DataRow dr_loopVariable in GridDS.Tables[0].Rows)
                    {
                        dr = dr_loopVariable;
                        var _with15 = _with13.Parameters;
                        _with15.Clear();
                        _with15.Add("HAWB_EXP_TBL_PK_IN", dr["HBL_HAWB_FK"]);
                        _with15.Add("HAWB_REF_NO_IN", dr["HAWB_REF_NO"]);
                        _with15.Add("JOB_CARD_AIR_EXP_FK_IN", dr["JOB_CARD_TRN_PK"]);
                        _with15.Add("MJ_SEA_IMP_FK_IN", MJImpPK);
                        _with15.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_SEA_IMP_PK").Direction = ParameterDirection.Output;
                        Execute = Convert.ToInt16(_with13.ExecuteNonQuery());
                        if (string.IsNullOrEmpty(RefNrs))
                        {
                            RefNrs = "'" + dr["HAWB_REF_NO"] + "'";
                        }
                        else
                        {
                            RefNrs = RefNrs + ",'" + dr["HAWB_REF_NO"] + "'";
                        }
                    }
                    //Added by Faheem as to apportion the cost only when MJC is closed
                    //Modified By Koteshwari on 7/5/2011
                    //.CommandText = objWK.MyUserName & ".JC_COST_PIA_CALCULATION_PKG.JC_AIR_EXP_COST_PIA_CALC"
                    _with13.CommandText = objWK.MyUserName + ".JC_COST_CALCULATION_PKG.JC_AIR_EXP_COST_CALC";
                    //End
                    _with13.Connection = objWK.MyConnection;
                    _with13.CommandType = CommandType.StoredProcedure;
                    _with13.Transaction = TRAN;
                    var _with16 = _with13.Parameters;
                    _with16.Clear();
                    _with16.Add("MASTER_JC_AIR_EXP_PK_IN", MSTJobCardPK).Direction = ParameterDirection.Input;
                    _with16.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with13.ExecuteNonQuery();
                    //End
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                    throw oraexp;
                }
                if (Execute > 0)
                {
                    TRAN.Commit();
                    //Push to financial system if realtime is selected
                    string JCPKs = "0";
                    JCPKs = GetImportJCPKs(RefNrs);
                    if (JCPKs != "0")
                    {
                        cls_Scheduler objSch = new cls_Scheduler();
                        ArrayList schDtls = null;
                        bool errGen = false;
                        if (objSch.GetSchedulerPushType() == true)
                        {
                            //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                            //try
                            //{
                            //    schDtls = objSch.FetchSchDtls();
                            //    //'Used to Fetch the Sch Dtls
                            //    objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), , , JCPKs);
                            //    if (ConfigurationManager.AppSettings["EVENTVIEWER"])
                            //    {
                            //        objPush.EventViewer(1, 1, Session("USER_PK"));
                            //    }
                            //}
                            //catch (Exception ex)
                            //{
                            //    if (ConfigurationManager.AppSettings["EVENTVIEWER"])
                            //    {
                            //        objPush.EventViewer(1, 2, Session("USER_PK"));
                            //    }
                            //}
                        }
                    }
                    //*****************************************************************

                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                    //If arrMessage.Count >= 1 Then
                    //    TRAN.Commit()
                    //    arrMessage.Add("All Data Saved Successfully")
                    //    Return arrMessage
                    //Else
                    //    TRAN.Rollback()
                    //End If
                }
                else
                {
                    TRAN.Rollback();
                }

            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            return new ArrayList();
        }

        private ArrayList SaveImportJobs(int MJImpPK, DataSet GridDS, OracleCommand attCommand, string user)
        {
            int i = 0;
            Array arr = null;
            string strRefNr = null;
            try
            {
                for (i = 0; i <= GridDS.Tables[0].Rows.Count - 1; i++)
                {
                    attCommand.Parameters.Clear();
                    var _with17 = attCommand;
                    _with17.CommandType = CommandType.StoredProcedure;
                    _with17.CommandText = user + ".MASTER_JC_SEA_EXP_TBL_PKG.AUTO_CREATE_JOB_CARD_SEA_IMP";
                    var _with18 = _with17.Parameters;
                    _with18.Add("HBL_EXP_TBL_PK_IN", GridDS.Tables[0].Rows[i]["HBL_EXP_TBL_FK"]).Direction = ParameterDirection.Input;
                    _with18.Add("HBL_REF_NO_IN", GridDS.Tables[0].Rows[i]["HBL_REF_NO"]).Direction = ParameterDirection.Input;
                    //.Add("HBL_DATE_IN", GridDS.Tables(0).Rows(0).Item("POL_ATD")).Direction = ParameterDirection.Input
                    _with18.Add("JOB_CARD_SEA_EXP_FK_IN", GridDS.Tables[0].Rows[0]["JOB_CARD_SEA_EXP_PK"]).Direction = ParameterDirection.Input;
                    _with18.Add("MJ_SEA_IMP_FK_IN", Convert.ToInt64(MJImpPK)).Direction = ParameterDirection.Input;
                    _with18.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    attCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    _with17.ExecuteNonQuery();
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
        #endregion
        //Ended by Faheem
        #region "Get Import Job Card Pks "
        public string GetImportJCPKs(string HBLNr)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataSet ds = null;
            string strJCPks = "";
            try
            {
                strSQL.Append(" SELECT 0 AS JCPK FROM DUAL");
                strSQL.Append(" UNION");
                strSQL.Append(" SELECT JCAIT.JOB_CARD_TRN_PK AS JCPK FROM JOB_CARD_TRN JCAIT");
                strSQL.Append(" WHERE JCAIT.HBL_HAWB_REF_NO in (" + HBLNr + ") AND JCAIT.JC_AUTO_MANUAL=1");


                ds = objWF.GetDataSet(strSQL.ToString());
                if (ds.Tables[0].Rows.Count > 0)
                {
                    for (int i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                    {
                        if (string.IsNullOrEmpty(strJCPks))
                        {
                            strJCPks = ds.Tables[0].Rows[i]["JCPK"].ToString();
                        }
                        else
                        {
                            strJCPks += "," + ds.Tables[0].Rows[i]["JCPK"].ToString();
                        }
                    }
                }
                else
                {
                    strJCPks = "0";
                }
                return strJCPks;

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
    }
}