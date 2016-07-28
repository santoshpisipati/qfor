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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsJobCardSearchAir : CommonFeatures
    {
        #region " Fetch all "

        /// <summary>
        /// Fors the fetch entry.
        /// </summary>
        /// <param name="ref">The reference.</param>
        /// <param name="JOBpk">The jo BPK.</param>
        /// <param name="BookingPK">The booking pk.</param>
        /// <param name="hblRef">The HBL reference.</param>
        /// <param name="proc">The proc.</param>
        /// <param name="Loc">The loc.</param>
        /// <returns></returns>
        public int ForFetchEntry(string @ref, string JOBpk = "", string BookingPK = "", string hblRef = "", string proc = "EXP", string Loc = "0")
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            DataSet ds = null;
            if (proc == "EXP")
            {
                strQuery.Append("SELECT J.JOB_CARD_AIR_EXP_PK,");
                strQuery.Append("       J.BOOKING_AIR_FK,");
                strQuery.Append("       H.HAWB_REF_NO, UMT.DEFAULT_LOCATION_FK,");
                strQuery.Append("       LOC.LOCATION_NAME");
                strQuery.Append("  FROM JOB_CARD_AIR_EXP_TBL J,");
                strQuery.Append("       HAWB_EXP_TBL          H,");
                strQuery.Append("       USER_MST_TBL         UMT,");
                strQuery.Append("       LOCATION_MST_TBL     LOC");
                strQuery.Append(" WHERE J.HAWB_EXP_TBL_FK = H.HAWB_EXP_TBL_PK(+)");
                strQuery.Append("   AND UPPER(J.JOBCARD_REF_NO) LIKE '%" + @ref.ToUpper() + "%'");
                strQuery.Append("   AND J.CREATED_BY_FK = UMT.USER_MST_PK");
                strQuery.Append("   AND LOC.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK");
                strQuery.Append("   AND LOC.LOCATION_MST_PK = " + Loc);
            }
            else
            {
                strQuery.Append("SELECT J.JOB_CARD_AIR_IMP_PK,j.jobcard_ref_no,");
                strQuery.Append("       J.HAWB_REF_NO, UMT.DEFAULT_LOCATION_FK,");
                strQuery.Append("       LOC.LOCATION_NAME");
                strQuery.Append("  FROM JOB_CARD_AIR_IMP_TBL J,");
                strQuery.Append("       USER_MST_TBL         UMT,");
                strQuery.Append("       LOCATION_MST_TBL     LOC");
                strQuery.Append(" WHERE UPPER(J.JOBCARD_REF_NO) LIKE '%" + @ref.ToUpper() + "%'");
                strQuery.Append("   AND J.CREATED_BY_FK = UMT.USER_MST_PK");
                strQuery.Append("  AND J.CONSOLE=1 AND LOC.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK");
                strQuery.Append(" AND (UMT.DEFAULT_LOCATION_FK =" + Loc);
                strQuery.Append(" OR J.PORT_MST_POD_FK  ");
                strQuery.Append(" IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + Loc + "))  ");
            }
            try
            {
                ds = (new WorkFlow()).GetDataSet(strQuery.ToString());
                if (ds.Tables[0].Rows.Count == 1)
                {
                    JOBpk = getDefault(ds.Tables[0].Rows[0][0], "").ToString();
                    BookingPK = getDefault(ds.Tables[0].Rows[0][1], "").ToString();
                    hblRef = getDefault(ds.Tables[0].Rows[0][2], "").ToString();
                    if (Loc != getDefault(ds.Tables[0].Rows[0][3], "") & proc == "EXP")
                    {
                        Loc = getDefault(ds.Tables[0].Rows[0][4], "").ToString();
                        return -1;
                    }
                }

                return ds.Tables[0].Rows.Count;
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

        /// <summary>
        /// Fetches all export.
        /// </summary>
        /// <param name="jobrefNO">The jobref no.</param>
        /// <param name="bookingNo">The booking no.</param>
        /// <param name="HblNo">The HBL no.</param>
        /// <param name="polID">The pol identifier.</param>
        /// <param name="podId">The pod identifier.</param>
        /// <param name="polName">Name of the pol.</param>
        /// <param name="podName">Name of the pod.</param>
        /// <param name="jcStatus">The jc status.</param>
        /// <param name="shipper">The shipper.</param>
        /// <param name="consignee">The consignee.</param>
        /// <param name="agent">The agent.</param>
        /// <param name="process">The process.</param>
        /// <param name="SearchFor">The search for.</param>
        /// <param name="SearchFortime">The search fortime.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="CurrBizType">Type of the curr biz.</param>
        /// <param name="BOOKING">if set to <c>true</c> [booking].</param>
        /// <param name="Mawb">The mawb.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="ULDNR">The uldnr.</param>
        /// <param name="jctype">The jctype.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet fetchAllExport(string jobrefNO = "", string bookingNo = "", string HblNo = "", string polID = "", string podId = "", string polName = "", string podName = "", string jcStatus = "", string shipper = "", string consignee = "",
        string agent = "", string process = "", double SearchFor = 0, Int32 SearchFortime = 0, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string CurrBizType = "3", bool BOOKING = false,
        string Mawb = "", long lngUsrLocFk = 0, string ULDNR = "", int jctype = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            if (BOOKING == false)
            {
                if (process == "2")
                {
                    buildCondition.Append("     BOOKING_AIR_TBL BK, JOB_CARD_AIR_EXP_TBL JC,");
                    buildCondition.Append("     HAWB_EXP_TBL HAWB, ");
                    buildCondition.Append("     MAWB_EXP_TBL MAWB, ");
                    //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL DPA, ")
                    buildCondition.Append("     job_trn_air_exp_cont CONT ,");
                    //added Manivannan
                }
                else
                {
                    buildCondition.Append(" JOB_CARD_AIR_IMP_TBL JC,");
                    //buildCondition.Append(vbCrLf & "     job_trn_air_imp_cont CONT, ") 'added Manivannan
                }
                buildCondition.Append("     CUSTOMER_MST_TBL SH,");
                buildCondition.Append("     CUSTOMER_MST_TBL CO,");
                buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
                if (process == "2")
                {
                    buildCondition.Append("     AGENT_MST_TBL DPA, ");
                }
                else
                {
                    buildCondition.Append("     AGENT_MST_TBL POLA, ");
                }
                //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL CBA, ")
                //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL CLA,")
                buildCondition.Append("     USER_MST_TBL UMT");

                buildCondition.Append("      where ");
                if (process == "2")
                {
                    buildCondition.Append("  BK.BOOKING_AIR_PK = JC.BOOKING_AIR_FK (+)");
                    buildCondition.Append("  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK");
                    buildCondition.Append("  AND CONT.job_card_air_exp_fk=JC.job_card_air_exp_pk");
                    //added Manivannan
                    buildCondition.Append("  AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                    buildCondition.Append("  AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                    buildCondition.Append("  AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                    //gopi on 15032007  EQA no: 2082
                    //buildCondition.Append(vbCrLf & "  AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)")
                    //buildCondition.Append(vbCrLf & "  AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ")
                    buildCondition.Append("  AND JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
                    //ended
                    buildCondition.Append("  AND JC.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK (+)");
                    buildCondition.Append("  AND JC.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK (+)");
                    buildCondition.Append("  AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
                    buildCondition.Append(" AND JC.CREATED_BY_FK = UMT.USER_MST_PK ");
                }
                else
                {
                    buildCondition.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                    buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK");
                    //buildCondition.Append(vbCrLf & "  AND CONT.job_card_air_imp_fk=JC.job_card_air_imp_pk") 'added Manivannan
                    buildCondition.Append("   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                    buildCondition.Append("   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                    //gopi on 15032007  EQA no: 2082
                    //buildCondition.Append(vbCrLf & "   AND JC.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)")
                    //buildCondition.Append(vbCrLf & "   AND JC.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ")
                    buildCondition.Append("   AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+) ");
                    //ended
                    buildCondition.Append(" AND ((UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " and JC.JC_AUTO_MANUAL = 0 ) ");
                    buildCondition.Append("  OR (JC.PORT_MST_POD_FK ");
                    buildCondition.Append(" IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + lngUsrLocFk + ") and JC.JC_AUTO_MANUAL = 1)) ");
                    buildCondition.Append(" AND jc.CREATED_BY_FK = UMT.USER_MST_PK ");
                    if (jctype != 2)
                    {
                        buildCondition.Append("AND  JC.JC_AUTO_MANUAL=" + jctype);
                    }
                }
                //buildCondition.Append(vbCrLf & " AND UMT.DEFAULT_LOCATION_FK = " & lngUsrLocFk & " ")
                //buildCondition.Append(vbCrLf & " AND JC.CREATED_BY_FK = UMT.USER_MST_PK ")
                if (jobrefNO.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(JOBCARD_REF_NO) LIKE '%" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
                    //If jcStatus = "1" Then
                    //    buildCondition.Append(vbCrLf & " AND JOB_CARD_STATUS=" & jcStatus & ")")
                    //End If
                }
                if (flag == 0)
                {
                    buildCondition.Append(" AND 1=2 ");
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
                        buildCondition.Append(" AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                    buildCondition.Append(" AND BK.STATUS IN (2,6) ");
                }
                if (jcStatus.Length > 0)
                {
                    buildCondition.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
                }
                if (HblNo.Trim().Length > 0)
                {
                    buildCondition.Append(" AND UPPER(HAWB_REF_NO) LIKE '%" + HblNo.ToUpper().Replace("'", "''") + "%'");
                }
                if (Mawb.Trim().Length > 0)
                {
                    buildCondition.Append(" AND UPPER(MAWB_REF_NO) LIKE '%" + Mawb.ToUpper().Replace("'", "''") + "%'");
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
                //ULDNR
                if (ULDNR.Length > 0)
                {
                    buildCondition.Append("     AND UPPER(CONT.ULD_NUMBER) LIKE '" + ULDNR.ToUpper().Replace("'", "''") + "' ");
                }
                //'Palette Size
                //If palettesize.Length > 0 Then
                //    buildCondition.Append(vbCrLf & "     AND UPPER(CONT.PALETTE_SIZE) LIKE '" & palettesize.ToUpper.Replace("'", "''") & "' ")
                //End If
                // CARGO TYPE

                if (agent.Length > 0)
                {
                    if (process == "2")
                    {
                        buildCondition.Append(" AND  UPPER(DPA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                    }
                    else
                    {
                        buildCondition.Append(" AND  UPPER(POLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                    }
                    //buildCondition.Append(vbCrLf & " AND  (UPPER(CLA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "' OR UPPER(CBA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "')")
                }

                if (shipper.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
                }
                if (consignee.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
                }
            }
            else
            {
                //for booking fetching only for job card creation

                buildCondition.Append("     BOOKING_AIR_TBL  BK,  ");
                buildCondition.Append("     CUSTOMER_MST_TBL SH,");
                buildCondition.Append("     CUSTOMER_MST_TBL CO,");
                buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
                buildCondition.Append("     AGENT_MST_TBL CBA, ");
                buildCondition.Append("     AGENT_MST_TBL CLA, ");
                buildCondition.Append("     USER_MST_TBL UMT ");

                buildCondition.Append("      where ");
                // JOIN CONDITION
                buildCondition.Append("   BK.CONS_CUSTOMER_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND BK.CUST_CUSTOMER_MST_FK = SH.CUSTOMER_MST_PK");
                buildCondition.Append("   AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append("   AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildCondition.Append("   AND BK.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)");
                buildCondition.Append("   AND BK.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ");
                buildCondition.Append(" AND BK.BOOKING_AIR_PK NOT IN(SELECT JC.BOOKING_AIR_FK FROM JOB_CARD_AIR_EXP_TBL JC) ");

                buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
                buildCondition.Append(" AND JC.CREATED_BY_FK = UMT.USER_MST_PK ");

                if (bookingNo.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                buildCondition.Append(" AND BK.STATUS IN (2,6) ");

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
                    if (process == "2")
                    {
                        buildCondition.Append(" AND  UPPER(DPA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                    }
                    else
                    {
                        buildCondition.Append(" AND  UPPER(POLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                    }
                    //buildCondition.Append(vbCrLf & " AND  (UPPER(CLA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "' OR UPPER(CBA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "')")
                }

                if (shipper.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
                }
                if (consignee.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
                }
            }

            strCondition = buildCondition.ToString();
            if (process == "2")
            {
                buildQuery.Append(" Select count(distinct SH.CUSTOMER_MST_PK) ");
            }
            else
            {
                buildQuery.Append(" Select count(distinct CO.CUSTOMER_MST_PK) ");
            }
            buildQuery.Append("      from ");
            buildQuery.Append("               " + strCondition);
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
            buildQuery.Append("    ( Select distinct ");
            if (process == "2")
            {
                buildQuery.Append("       SH.CUSTOMER_MST_PK, ");
                buildQuery.Append("       SH.CUSTOMER_ID, ");
                buildQuery.Append("       SH.CUSTOMER_NAME ");
            }
            else
            {
                buildQuery.Append("       CO.CUSTOMER_MST_PK, ");
                buildQuery.Append("       CO.CUSTOMER_ID, ");
                buildQuery.Append("       CO.CUSTOMER_NAME ");
            }

            buildQuery.Append("      from ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("     Order By " + SortColumn + SortType);
            buildQuery.Append("   ) q ");
            buildQuery.Append("  )   ");
            buildQuery.Append("  where  ");
            buildQuery.Append("     SR_NO between " + start + " and " + last);

            strSQL = buildQuery.ToString();

            DataSet DS = null;

            try
            {
                DS = objWF.GetDataSet(strSQL);
                //FetchChildForBooking
                if (BOOKING)
                {
                    DS.Tables.Add(FetchChildForBooking(AllMasterPKs(DS), jobrefNO, bookingNo, jcStatus, HblNo, polID, podId, polName, podName, shipper,
                    consignee, agent, process, Convert.ToString(SearchFor), SearchFortime, jctype));
                    DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CUSTOMER_MST_PK"], DS.Tables[1].Columns["CUST_CUSTOMER_MST_FK"], true);
                    DS.Relations.Add(CONTRel);
                }
                else
                {
                    DataRelation CONTRel = null;
                    DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), jobrefNO, bookingNo, jcStatus, HblNo, polID, podId, polName, podName, shipper,
                    consignee, agent, process, SearchFor, SearchFortime, true, Mawb, lngUsrLocFk, ULDNR, jctype));
                    //Dim CONTRel As New DataRelation("CONTRelation", _
                    //                                                DS.Tables(0).Columns("CUSTOMER_MST_PK"), _
                    //                                                DS.Tables(1).Columns("SHIPPER_CUST_MST_FK"), True)

                    if (process == "2")
                    {
                        CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CUSTOMER_MST_PK"], DS.Tables[1].Columns["SHIPPER_CUST_MST_FK"], true);
                        //CONSIGNEE_CUST_MST_FK
                    }
                    else
                    {
                        CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CUSTOMER_MST_PK"], DS.Tables[1].Columns["CONSIGNEE_CUST_MST_FK"], true);
                    }
                    DS.Relations.Add(CONTRel);
                }

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

        /// <summary>
        /// Alls the master p ks.
        /// </summary>
        /// <param name="ds">The ds.</param>
        /// <returns></returns>
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
                    strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["CUSTOMER_MST_PK"]).Trim() + ",");
                }
                strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
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

        #endregion " Fetch all "

        #region " Fetch Childs "

        /// <summary>
        /// Fetches the child for.
        /// </summary>
        /// <param name="CONTSpotPKs">The cont spot p ks.</param>
        /// <param name="jobrefNO">The jobref no.</param>
        /// <param name="bookingNo">The booking no.</param>
        /// <param name="jcStatus">The jc status.</param>
        /// <param name="Hbl">The HBL.</param>
        /// <param name="polID">The pol identifier.</param>
        /// <param name="podId">The pod identifier.</param>
        /// <param name="polName">Name of the pol.</param>
        /// <param name="podName">Name of the pod.</param>
        /// <param name="shipper">The shipper.</param>
        /// <param name="consignee">The consignee.</param>
        /// <param name="agent">The agent.</param>
        /// <param name="process">The process.</param>
        /// <param name="SearchFor">The search for.</param>
        /// <param name="SearchFortime">The search fortime.</param>
        /// <param name="BOOKING">if set to <c>true</c> [booking].</param>
        /// <param name="Mawb">The mawb.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="ULDNR">The uldnr.</param>
        /// <param name="jctype">The jctype.</param>
        /// <returns></returns>
        private DataTable FetchChildFor(string CONTSpotPKs = "", string jobrefNO = "", string bookingNo = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "",
        string consignee = "", string agent = "", string process = "", double SearchFor = 0, Int32 SearchFortime = 0, bool BOOKING = false, string Mawb = "", long lngUsrLocFk = 0, string ULDNR = "", int jctype = 0)
        {
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string strCondition = "";
            string strSQL = "";
            string strTable = "JOB_CARD_AIR_EXP_TBL";

            //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            //CONDITION COMING FROM MASTRER FUNCTION

            if (process == "2")
            {
                buildCondition.Append("     BOOKING_AIR_TBL BK, JOB_CARD_AIR_EXP_TBL JC,");
                buildCondition.Append("     HAWB_EXP_TBL HAWB, ");
                buildCondition.Append("     MAWB_EXP_TBL MAWB, ");
                //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL DPA, ")
                buildCondition.Append("     job_trn_air_exp_cont CONT ,");
                //added Manivannan
            }
            else
            {
                buildCondition.Append(" JOB_CARD_AIR_IMP_TBL JC,");
                //buildCondition.Append(vbCrLf & "     job_trn_air_imp_cont CONT ,") 'added Manivannan
            }
            buildCondition.Append("     CUSTOMER_MST_TBL SH,");
            buildCondition.Append("     CUSTOMER_MST_TBL CO,");
            buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");

            if (process == "2")
            {
                buildCondition.Append("     AGENT_MST_TBL DPA, ");
            }
            else
            {
                buildCondition.Append("     AGENT_MST_TBL POLA, ");
            }
            // added by gopi
            //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL CBA, ")
            //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL CLA ,")
            buildCondition.Append("     USER_MST_TBL UMT ");

            buildCondition.Append("      where ");
            // JOIN CONDITION
            if (process == "2")
            {
                buildCondition.Append("      BK.BOOKING_AIR_PK = JC.BOOKING_AIR_FK (+)");
                buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append("  AND CONT.job_card_air_exp_fk=JC.job_card_air_exp_pk");
                //added Manivannan
                buildCondition.Append("   AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                //buildCondition.Append(vbCrLf & "    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)")
                //buildCondition.Append(vbCrLf & "    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ")
                //added by gopi
                buildCondition.Append("   AND JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
                buildCondition.Append("   AND JC.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK (+)");
                buildCondition.Append("   AND JC.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK (+)");
                buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
                buildCondition.Append(" AND JC.CREATED_BY_FK = UMT.USER_MST_PK ");
            }
            else
            {
                buildCondition.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append("   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                //buildCondition.Append(vbCrLf & "  AND CONT.job_card_air_imp_fk=JC.job_card_air_imp_pk") 'added Manivannan
                //buildCondition.Append(vbCrLf & "   AND JC.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)") 'commented by gopi
                //buildCondition.Append(vbCrLf & "   AND JC.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ")
                buildCondition.Append("   AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)");
                buildCondition.Append(" AND ((UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " and JC.JC_AUTO_MANUAL = 0 ) ");
                buildCondition.Append("  OR (JC.PORT_MST_POD_FK ");
                buildCondition.Append(" IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + lngUsrLocFk + ") and JC.JC_AUTO_MANUAL = 1)) ");
                buildCondition.Append(" AND jc.CREATED_BY_FK = UMT.USER_MST_PK ");
                if (jctype != 2)
                {
                    buildCondition.Append("AND JC.JC_AUTO_MANUAL =" + jctype);
                }
            }

            //buildCondition.Append(vbCrLf & " AND UMT.DEFAULT_LOCATION_FK = " & lngUsrLocFk & " ")
            //buildCondition.Append(vbCrLf & " AND JC.CREATED_BY_FK = UMT.USER_MST_PK ")

            if (jobrefNO.Length > 0)
            {
                buildCondition.Append(" AND UPPER(JOBCARD_REF_NO) LIKE '%" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
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
                    buildCondition.Append(" AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                buildCondition.Append(" AND BK.STATUS IN (2,6)");
            }
            if (jcStatus.Length > 0)
            {
                buildCondition.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
            }
            if (Hbl.Trim().Length > 0)
            {
                buildCondition.Append(" AND UPPER(HAWB_REF_NO) LIKE '%" + Hbl.ToUpper().Replace("'", "''") + "%'");
            }
            if (Mawb.Trim().Length > 0)
            {
                buildCondition.Append(" AND UPPER(MAWB_REF_NO) LIKE '%" + Mawb.ToUpper().Replace("'", "''") + "%'");
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
            //ULDNR
            if (ULDNR.Length > 0)
            {
                buildCondition.Append("     AND UPPER(CONT.ULD_NUMBER) LIKE '" + ULDNR.ToUpper().Replace("'", "''") + "' ");
            }
            //'Palette Size
            //If palettesize.Length > 0 Then
            //    buildCondition.Append(vbCrLf & "     AND UPPER(CONT.PALETTE_SIZE) LIKE '" & palettesize.ToUpper.Replace("'", "''") & "' ")
            //End If
            // CARGO TYPE

            if (agent.Length > 0)
            {
                if (process == "2")
                {
                    buildCondition.Append(" AND  UPPER(DPA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                }
                else
                {
                    buildCondition.Append(" AND  UPPER(POLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                }
                //buildCondition.Append(vbCrLf & " AND  (UPPER(CLA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "' OR UPPER(CBA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "')")
            }

            if (shipper.Length > 0)
            {
                buildCondition.Append(" AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
            }
            if (consignee.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
            }

            //===========================================================================================================================
            //If process <> "2" Then
            //    strTable = "JOB_CARD_AIR_IMP_TBL"
            //Else
            //    buildCondition.Append(vbCrLf & " AND BOOK.STATUS=2")
            //End If

            //If CONTSpotPKs.Trim.Length > 0 Then
            //    buildCondition.Append(vbCrLf & " and JC.SHIPPER_CUST_MST_FK in (" & CONTSpotPKs & ") ")
            //End If
            if (process == "2")
            {
                buildCondition.Append(" and JC.SHIPPER_CUST_MST_FK in (" + CONTSpotPKs + ") ");
            }
            else
            {
                buildCondition.Append(" and JC.CONSIGNEE_CUST_MST_FK in (" + CONTSpotPKs + ") ");
            }
            //If jcStatus.Length > 0 Then
            //    'strTable = "(select * from " & strTable & " where JOB_CARD_STATUS = " & jcStatus & ")"
            //    buildCondition.Append(vbCrLf & "AND (JOB.JOB_CARD_STATUS IS NULL OR JOB.JOB_CARD_STATUS =" & jcStatus & ")")

            //End If

            //If Hbl.Trim.Length > 0 Then
            //    buildCondition.Append(vbCrLf & " AND UPPER(HBL_REF_NO) LIKE '" & "%" & Hbl.ToUpper.Replace("'", "''") & "%'")
            //End If
            //If cargoType.Length > 0 Then
            //    If process <> "2" Then
            //        buildCondition.Append(vbCrLf & " AND CARGO_TYPE=" & cargoType)
            //    Else
            //        buildCondition.Append(vbCrLf & " AND BOOK.CARGO_TYPE=" & cargoType)
            //    End If
            //End If

            strCondition = buildCondition.ToString();

            buildQuery.Append(" Select * from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select distinct ");
            //CUST_CUSTOMER_MST_FK

            if (process == "2")
            {
                buildQuery.Append("       JC.SHIPPER_CUST_MST_FK, ");
                //
                buildQuery.Append("       JOB_CARD_AIR_EXP_PK, ");
                buildQuery.Append("       BOOKING_AIR_PK, ");
                buildQuery.Append("       BOOKING_REF_NO, ");
            }
            else
            {
                buildQuery.Append("       JC.CONSIGNEE_CUST_MST_FK, ");
                buildQuery.Append("       JOB_CARD_AIR_IMP_PK, ");
                buildQuery.Append("       PORT_MST_POL_FK, ");
                buildQuery.Append("       PORT_MST_POD_FK, ");
            }
            buildQuery.Append("       JOBCARD_REF_NO, ");
            buildQuery.Append("       HAWB_REF_NO, ");
            buildQuery.Append("       MAWB_REF_NO,JOBCARD_DATE");
            buildQuery.Append("      from ");
            //BOOKING_AIR_TBL
            //HBL_EXP_TBL
            //COMMENTED============FOR CHECKING
            //If process = "2" Then
            //    buildQuery.Append(vbCrLf & strTable & " JOB, ")
            //    buildQuery.Append(vbCrLf & "       HBL_EXP_TBL HBL, ")
            //    buildQuery.Append(vbCrLf & "       MAWB_EXP_TBL MAWB,")
            //    buildQuery.Append(vbCrLf & "       BOOKING_AIR_TBL BOOK")
            //    buildQuery.Append(vbCrLf & "      where BOOK.BOOKING_AIR_PK=JOB.BOOKING_AIR_FK(+)") ' JOIN CONDITION
            //    buildQuery.Append(vbCrLf & "     AND JOB.HBL_EXP_TBL_FK=HBL.HBL_EXP_TBL_PK(+)")
            //    buildQuery.Append(vbCrLf & "     AND JOB.MAWB_EXP_TBL_FK=MAWB.MAWB_EXP_TBL_PK(+)")
            //Else
            //    buildQuery.Append(vbCrLf & strTable & " JOB ")
            //    buildQuery.Append(vbCrLf & "      where 1=1 ")
            //End If
            //COMMENTED END
            buildQuery.Append(strCondition);
            if (process == "2")
            {
                buildQuery.Append("      ORDER BY JOB_CARD_AIR_EXP_PK DESC,JOBCARD_REF_NO DESC  ");
            }
            else
            {
                buildQuery.Append("      ORDER BY JOB_CARD_AIR_IMP_PK DESC,JOBCARD_REF_NO DESC  ");
            }
            //buildQuery.Append(vbCrLf & "      ORDER BY JOBCARD_DATE DESC,JOBCARD_REF_NO DESC ")
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
                string strCol = (process == "2" ? "SHIPPER_CUST_MST_FK" : "CONSIGNEE_CUST_MST_FK");
                dt = objWF.GetDataTable(strSQL);
                int RowCnt = 0;
                int Rno = 0;
                int pk = 0;
                pk = -1;
                for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++)
                {
                    if (Convert.ToInt32(dt.Rows[RowCnt][strCol]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt][strCol]);
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

        //FETCH CHILD OF BOOKING
        /// <summary>
        /// Fetches the child for booking.
        /// </summary>
        /// <param name="CONTSpotPKs">The cont spot p ks.</param>
        /// <param name="jobrefNO">The jobref no.</param>
        /// <param name="bookingNo">The booking no.</param>
        /// <param name="jcStatus">The jc status.</param>
        /// <param name="Hbl">The HBL.</param>
        /// <param name="polID">The pol identifier.</param>
        /// <param name="podId">The pod identifier.</param>
        /// <param name="polName">Name of the pol.</param>
        /// <param name="podName">Name of the pod.</param>
        /// <param name="shipper">The shipper.</param>
        /// <param name="consignee">The consignee.</param>
        /// <param name="agent">The agent.</param>
        /// <param name="process">The process.</param>
        /// <param name="cargoType">Type of the cargo.</param>
        /// <param name="SearchFor">The search for.</param>
        /// <param name="SearchFortime">The search fortime.</param>
        /// <param name="BOOKING">if set to <c>true</c> [booking].</param>
        /// <returns></returns>
        private DataTable FetchChildForBooking(string CONTSpotPKs = "", string jobrefNO = "", string bookingNo = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "",
        string consignee = "", string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, bool BOOKING = false)
        {
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string strCondition = "";
            string strSQL = "";
            string strTable = "JOB_CARD_AIR_EXP_TBL";

            //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            //CONDITION COMING FROM MASTRER FUNCTION

            buildCondition.Append("     BOOKING_AIR_TBL  BK,  ");
            buildCondition.Append("     CUSTOMER_MST_TBL SH,");
            buildCondition.Append("     CUSTOMER_MST_TBL CO,");
            buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            //added by gopi
            if (process == "2")
            {
                buildCondition.Append("     AGENT_MST_TBL DPA, ");
            }
            else
            {
                buildCondition.Append("     AGENT_MST_TBL POLA, ");
            }
            //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL CBA, ")
            //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL CLA ")
            buildCondition.Append("      where ");
            // JOIN CONDITION
            buildCondition.Append("   BK.CONS_CUSTOMER_MST_FK = CO.CUSTOMER_MST_PK(+)");
            buildCondition.Append("   AND BK.CUST_CUSTOMER_MST_FK = SH.CUSTOMER_MST_PK(+)");
            buildCondition.Append("   AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            buildCondition.Append("   AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");

            buildCondition.Append("   AND BK.dp_agent_mst_fk  = DPA.AGENT_MST_PK (+)");
            //buildCondition.Append(vbCrLf & "   AND BK.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)")
            //buildCondition.Append(vbCrLf & "   AND BK.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ")

            if (bookingNo.Length > 0)
            {
                buildCondition.Append(" AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
            buildCondition.Append(" AND BK.STATUS IN (2,6)");

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
                if (process == "2")
                {
                    buildCondition.Append(" AND  UPPER(DPA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                }
                else
                {
                    buildCondition.Append(" AND  UPPER(POLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                }
                //buildCondition.Append(vbCrLf & " AND  (UPPER(CLA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "' OR UPPER(CBA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "')")
            }

            if (shipper.Length > 0)
            {
                buildCondition.Append(" AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
            }
            if (consignee.Length > 0)
            {
                buildCondition.Append(" AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
            }

            //===========================================================================================================================
            //If process <> "2" Then
            //    strTable = "JOB_CARD_SEA_IMP_TBL"
            //Else
            //    buildCondition.Append(vbCrLf & " AND BOOK.STATUS=2")
            //End If

            if (CONTSpotPKs.Trim().Length > 0)
            {
                buildCondition.Append(" and CUST_CUSTOMER_MST_FK in (" + CONTSpotPKs + ") ");
            }
            // AND BK.BOOKING_SEA_PK NOT IN
            //       (SELECT JC.BOOKING_SEA_FK FROM JOB_CARD_SEA_EXP_TBL JC)

            buildCondition.Append(" AND BK.BOOKING_AIR_PK NOT IN(SELECT JC.BOOKING_AIR_FK FROM JOB_CARD_AIR_EXP_TBL JC) ");

            //If jcStatus.Length > 0 Then
            //    'strTable = "(select * from " & strTable & " where JOB_CARD_STATUS = " & jcStatus & ")"
            //    buildCondition.Append(vbCrLf & "AND (JOB.JOB_CARD_STATUS IS NULL OR JOB.JOB_CARD_STATUS =" & jcStatus & ")")

            //End If

            //If Hbl.Trim.Length > 0 Then
            //    buildCondition.Append(vbCrLf & " AND UPPER(HBL_REF_NO) LIKE '" & "%" & Hbl.ToUpper.Replace("'", "''") & "%'")
            //End If
            //If cargoType.Length > 0 Then
            //    If process <> "2" Then
            //        buildCondition.Append(vbCrLf & " AND CARGO_TYPE=" & cargoType)
            //    Else
            //        buildCondition.Append(vbCrLf & " AND BOOK.CARGO_TYPE=" & cargoType)
            //    End If
            //End If

            strCondition = buildCondition.ToString();

            buildQuery.Append("     Select '' SR_NO,");
            //CUST_CUSTOMER_MST_FK
            buildQuery.Append("         BK.CUST_CUSTOMER_MST_FK, ");
            //
            buildQuery.Append("      '' JOB_CARD_AIR_EXP_PK, ");
            buildQuery.Append("       BOOKING_AIR_PK, ");
            buildQuery.Append("       BOOKING_REF_NO, ");
            buildQuery.Append("      '' JOBCARD_REF_NO, ");
            buildQuery.Append("      '' HAWB_REF_NO, ");
            buildQuery.Append("      '' MAWB_REF_NO ");
            buildQuery.Append("      from ");
            //BOOKING_AIR_TBL
            //HBL_EXP_TBL
            //COMMENTED============FOR CHECKING
            //If process = "2" Then
            //    buildQuery.Append(vbCrLf & strTable & " JOB, ")
            //    buildQuery.Append(vbCrLf & "       HBL_EXP_TBL HBL, ")
            //    buildQuery.Append(vbCrLf & "       MAWB_EXP_TBL MAWB,")
            //    buildQuery.Append(vbCrLf & "       BOOKING_AIR_TBL BOOK")
            //    buildQuery.Append(vbCrLf & "      where BOOK.BOOKING_AIR_PK=JOB.BOOKING_AIR_FK(+)") ' JOIN CONDITION
            //    buildQuery.Append(vbCrLf & "     AND JOB.HBL_EXP_TBL_FK=HBL.HBL_EXP_TBL_PK(+)")
            //    buildQuery.Append(vbCrLf & "     AND JOB.MAWB_EXP_TBL_FK=MAWB.MAWB_EXP_TBL_PK(+)")
            //Else
            //    buildQuery.Append(vbCrLf & strTable & " JOB ")
            //    buildQuery.Append(vbCrLf & "      where 1=1 ")
            //End If
            //COMMENTED END
            buildQuery.Append(strCondition);
            buildQuery.Append("     ORDER BY JOBCARD_DATE DESC,JOBCARD_REF_NO DESC ");

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
                    if (Convert.ToInt32(dt.Rows[RowCnt]["CUST_CUSTOMER_MST_FK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["CUST_CUSTOMER_MST_FK"]);
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

        #endregion " Fetch Childs "

        #region " Enhance Search Functions "

        /// <summary>
        /// Fetches for shipper and consignee.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForShipperAndConsignee(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strCATEGORY_IN = "";
            //Dim strSEARCH_CS_ID_IN As String = ""
            string strLOC_MST_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strCATEGORY_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            //If arr.Length > 4 Then strLOC_MST_IN = arr(4)

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_CATEGORY_COMMON";
                var _with1 = SCM.Parameters;
                _with1.Add("CATEGORY_IN", ifDBNull(strCATEGORY_IN)).Direction = ParameterDirection.Input;
                _with1.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }

        /// <summary>
        /// Fetches for agent.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForAgent(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_AGENT_PKG.GETAGENT_COMMON";
                var _with2 = SCM.Parameters;
                _with2.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                SCM.Connection.Close();
            }
        }

        /// <summary>
        /// Fetches the active job card.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchActiveJobCard(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_ACTIVE_JOB_REF_COMMON";
                var _with3 = SCM.Parameters;
                _with3.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
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
                SCM.Connection.Close();
            }
        }

        #region " Supporting Function "

        /// <summary>
        /// Ifs the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        /// <summary>
        /// Removes the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, ""))
            {
                return "";
            }
            return col;
        }

        #endregion " Supporting Function "

        #endregion " Enhance Search Functions "

        #region "Fetch Job Card For Listing Screen as per the new Requirement"

        /// <summary>
        /// FN_s the fetch listing new.
        /// </summary>
        /// <param name="jobrefNO">The jobref no.</param>
        /// <param name="bookingNo">The booking no.</param>
        /// <param name="HblNo">The HBL no.</param>
        /// <param name="polID">The pol identifier.</param>
        /// <param name="podId">The pod identifier.</param>
        /// <param name="polName">Name of the pol.</param>
        /// <param name="podName">Name of the pod.</param>
        /// <param name="jcStatus">The jc status.</param>
        /// <param name="shipper">The shipper.</param>
        /// <param name="consignee">The consignee.</param>
        /// <param name="agent">The agent.</param>
        /// <param name="process">The process.</param>
        /// <param name="SearchFor">The search for.</param>
        /// <param name="SearchFortime">The search fortime.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="CurrBizType">Type of the curr biz.</param>
        /// <param name="BOOKING">if set to <c>true</c> [booking].</param>
        /// <param name="Mawb">The mawb.</param>
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="ULDNR">The uldnr.</param>
        /// <param name="jctype">The jctype.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="PONr">The po nr.</param>
        /// <param name="IsNominated">if set to <c>true</c> [is nominated].</param>
        /// <param name="SalesExecMstFk">The sales execute MST fk.</param>
        /// <param name="OtherStatus">The other status.</param>
        /// <returns></returns>
        public DataSet fn_FetchListingNew(string jobrefNO = "", string bookingNo = "", string HblNo = "", string polID = "", string podId = "", string polName = "", string podName = "", string jcStatus = "", string shipper = "", string consignee = "",
        string agent = "", string process = "", double SearchFor = 0, Int32 SearchFortime = 0, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string CurrBizType = "3", bool BOOKING = false,
        string Mawb = "", long lngUsrLocFk = 0, string ULDNR = "", int jctype = 0, Int32 flag = 0, string PONr = "", bool IsNominated = false, int SalesExecMstFk = 0, int OtherStatus = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            if (BOOKING == false)
            {
                //EXP
                if (process == "2")
                {
                    buildCondition.Append("     BOOKING_AIR_TBL BK, JOB_CARD_AIR_EXP_TBL JC,");
                    buildCondition.Append("     HAWB_EXP_TBL HAWB, ");
                    buildCondition.Append("     MAWB_EXP_TBL MAWB, ");
                    buildCondition.Append("     job_trn_air_exp_cont CONT ,");
                }
                else
                {
                    buildCondition.Append("     BOOKING_AIR_TBL BK, JOB_CARD_AIR_EXP_TBL JAE,");
                    buildCondition.Append("     JOB_CARD_AIR_IMP_TBL JC,");
                    buildCondition.Append("     CUSTOMER_MST_TBL CMT,");
                }
                buildCondition.Append("     CUSTOMER_MST_TBL SH,");
                buildCondition.Append("     CUSTOMER_MST_TBL CO,");
                buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
                if (process == "2")
                {
                    buildCondition.Append("     AGENT_MST_TBL DPA, ");
                }
                else
                {
                    buildCondition.Append("     AGENT_MST_TBL POLA, ");
                }
                buildCondition.Append("     EMPLOYEE_MST_TBL EMP,");
                buildCondition.Append("     EMPLOYEE_MST_TBL DEF_EXEC,");
                buildCondition.Append("      AIRLINE_MST_TBL       ART,");
                buildCondition.Append("     USER_MST_TBL UMT");
                buildCondition.Append("      where ");
                if (process == "2")
                {
                    buildCondition.Append("  BK.BOOKING_AIR_PK = JC.BOOKING_AIR_FK (+)");
                    buildCondition.Append("  AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK");
                    buildCondition.Append("  AND CONT.job_card_air_exp_fk=JC.job_card_air_exp_pk");
                    buildCondition.Append("  AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                    buildCondition.Append("  AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                    buildCondition.Append("  AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                    buildCondition.Append("  AND JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
                    buildCondition.Append("  AND ART.AIRLINE_MST_PK(+)=BK.AIRLINE_MST_FK ");
                    ///
                    buildCondition.Append("  AND JC.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK (+)");
                    buildCondition.Append("  AND JC.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK (+)");
                    buildCondition.Append("  AND (UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
                    buildCondition.Append("        OR POL.LOCATION_MST_FK = " + lngUsrLocFk + ") ");
                    buildCondition.Append(" AND JC.CREATED_BY_FK = UMT.USER_MST_PK ");
                    buildCondition.Append("   AND SH.REP_EMP_MST_FK = DEF_EXEC.EMPLOYEE_MST_PK(+)");
                }
                else
                {
                    buildCondition.Append("   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");

                    buildCondition.Append("   AND JC.JOBCARD_REF_NO = JAE.JOBCARD_REF_NO(+)");
                    buildCondition.Append("   AND JAE.BOOKING_AIR_FK = BK.BOOKING_AIR_PK(+)");

                    buildCondition.Append("   AND JC.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    buildCondition.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK");
                    buildCondition.Append("   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                    buildCondition.Append("   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                    buildCondition.Append("  AND ART.AIRLINE_MST_PK(+)=JC.AIRLINE_MST_FK ");
                    ///
                    buildCondition.Append("   AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+) ");
                    buildCondition.Append(" AND ((UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " and JC.JC_AUTO_MANUAL = 0 ) ");
                    buildCondition.Append("  OR (JC.PORT_MST_POD_FK ");
                    buildCondition.Append(" IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + lngUsrLocFk + ") and JC.JC_AUTO_MANUAL = 1)) ");
                    buildCondition.Append(" AND jc.CREATED_BY_FK = UMT.USER_MST_PK ");
                    if (!string.IsNullOrEmpty(PONr))
                    {
                        buildCondition.Append(" AND UPPER(JC.PO_NUMBER) LIKE '%" + PONr.ToUpper().Replace("'", "''") + "%'");
                    }
                    if (jctype != 2)
                    {
                        buildCondition.Append("AND  JC.JC_AUTO_MANUAL=" + jctype);
                    }
                    buildCondition.Append(" AND JC.CONSOLE = 1 ");
                    buildCondition.Append("   AND CO.REP_EMP_MST_FK = DEF_EXEC.EMPLOYEE_MST_PK(+)");
                }
                buildCondition.Append("   AND JC.EXECUTIVE_MST_FK = EMP.EMPLOYEE_MST_PK(+)");
                if (jobrefNO.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(JC.JOBCARD_REF_NO) LIKE '%" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
                }
                if (flag == 0)
                {
                    buildCondition.Append(" AND 1=2 ");
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
                    buildCondition.Append(" AND JOBCARD_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
                }
                if (process == "2")
                {
                    if (bookingNo.Length > 0)
                    {
                        buildCondition.Append(" AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                    buildCondition.Append(" AND BK.STATUS IN (2,6, DECODE(JC.WIN_XML_GEN, 1, 3, -1)) ");
                }
                if (jcStatus.Length > 0)
                {
                    buildCondition.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
                }
                if (HblNo.Trim().Length > 0)
                {
                    buildCondition.Append(" AND UPPER(HAWB_REF_NO) LIKE '%" + HblNo.ToUpper().Replace("'", "''") + "%'");
                }
                if (Mawb.Trim().Length > 0)
                {
                    buildCondition.Append(" AND UPPER(MAWB_REF_NO) LIKE '%" + Mawb.ToUpper().Replace("'", "''") + "%'");
                }
                if (polID.Length > 0)
                {
                    buildCondition.Append("       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
                }
                if (polName.Length > 0)
                {
                    buildCondition.Append("     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
                }
                if (podId.Length > 0)
                {
                    buildCondition.Append("     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
                }
                if (podName.Length > 0)
                {
                    buildCondition.Append("     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
                }
                if (ULDNR.Length > 0)
                {
                    buildCondition.Append("     AND UPPER(CONT.ULD_NUMBER) LIKE '" + ULDNR.ToUpper().Replace("'", "''") + "' ");
                }
                if (agent.Length > 0)
                {
                    if (process == "2")
                    {
                        //buildCondition.Append(vbCrLf & " AND  UPPER(DPA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "'")
                        buildCondition.Append("  AND DPA.AGENT_MST_PK = " + agent);
                    }
                    else
                    {
                        //buildCondition.Append(vbCrLf & " AND  UPPER(POLA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "'")
                        buildCondition.Append("  AND POLA.AGENT_MST_PK = " + agent);
                    }
                }
                if (shipper.Length > 0)
                {
                    // buildCondition.Append(vbCrLf & " AND UPPER(SH.CUSTOMER_ID) LIKE '" & shipper.ToUpper.Replace("'", "''") & "'")
                    buildCondition.Append("  AND SH.CUSTOMER_MST_PK = " + shipper);
                }
                if (consignee.Length > 0)
                {
                    //buildCondition.Append(vbCrLf & " AND UPPER(CO.CUSTOMER_ID) LIKE '" & consignee.ToUpper.Replace("'", "''") & "'")
                    buildCondition.Append("  AND CO.CUSTOMER_MST_PK = " + consignee);
                }
            }
            else
            {
                buildCondition.Append("     BOOKING_AIR_TBL  BK,  ");
                buildCondition.Append("     CUSTOMER_MST_TBL SH,");
                buildCondition.Append("     CUSTOMER_MST_TBL CO,");
                buildCondition.Append("     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
                buildCondition.Append("     AGENT_MST_TBL CBA, ");
                buildCondition.Append("     AGENT_MST_TBL CLA, ");
                buildCondition.Append("      AIRLINE_MST_TBL   ART, ");
                //'
                buildCondition.Append("     USER_MST_TBL UMT ");
                buildCondition.Append("      where ");
                buildCondition.Append("   BK.CONS_CUSTOMER_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append("   AND BK.CUST_CUSTOMER_MST_FK = SH.CUSTOMER_MST_PK");
                buildCondition.Append("   AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append("   AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildCondition.Append("   AND BK.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)");
                buildCondition.Append("   AND BK.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ");
                buildCondition.Append("    AND ART.AIRLINE_MST_PK(+)=JC.AIRLINE_MST_FK ");
                ///
                buildCondition.Append(" AND BK.BOOKING_AIR_PK NOT IN(SELECT JC.BOOKING_AIR_FK FROM JOB_CARD_AIR_EXP_TBL JC) ");
                buildCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
                buildCondition.Append(" AND JC.CREATED_BY_FK = UMT.USER_MST_PK ");
                if (bookingNo.Length > 0)
                {
                    buildCondition.Append(" AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                buildCondition.Append(" AND BK.STATUS IN (2,6) ");
                if (polID.Length > 0)
                {
                    buildCondition.Append("       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
                }
                if (polName.Length > 0)
                {
                    buildCondition.Append("     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
                }
                if (podId.Length > 0)
                {
                    buildCondition.Append("     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
                }
                if (podName.Length > 0)
                {
                    buildCondition.Append("     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
                }
                if (agent.Length > 0)
                {
                    if (process == "2")
                    {
                        //buildCondition.Append(vbCrLf & " AND  UPPER(DPA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "'")
                        buildCondition.Append("  AND DPA.AGENT_MST_PK = " + agent);
                    }
                    else
                    {
                        //buildCondition.Append(vbCrLf & " AND  UPPER(POLA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "'")
                        buildCondition.Append("  AND POLA.AGENT_MST_PK = " + agent);
                    }
                }
                if (shipper.Length > 0)
                {
                    //buildCondition.Append(vbCrLf & " AND UPPER(SH.CUSTOMER_ID) LIKE '" & shipper.ToUpper.Replace("'", "''") & "'")
                    buildCondition.Append("  AND SH.CUSTOMER_MST_PK = " + shipper);
                }
                if (consignee.Length > 0)
                {
                    //buildCondition.Append(vbCrLf & " AND UPPER(CO.CUSTOMER_ID) LIKE '" & consignee.ToUpper.Replace("'", "''") & "'")
                    buildCondition.Append("  AND CO.CUSTOMER_MST_PK = " + consignee);
                }
            }
            //If process = 2 Then 'export
            //    If IsNominated Then
            //        buildCondition.Append(vbCrLf & "   AND JC.CHK_NOMINATED = 1")
            //    ElseIf SalesExecMstFk > 0 Then
            //        buildCondition.Append(vbCrLf & "   AND JC.EXECUTIVE_MST_FK = " & SalesExecMstFk)
            //    End If
            //Else
            //    If IsNominated Then
            //        buildCondition.Append(vbCrLf & "   AND JC.JC_AUTO_MANUAL = 1 AND JC.CHK_CSR=0")
            //    ElseIf SalesExecMstFk > 0 Then
            //        buildCondition.Append(vbCrLf & "   AND JC.EXECUTIVE_MST_FK = " & SalesExecMstFk)
            //    End If
            //End If
            //export
            if (Convert.ToInt32(process) == 2 & IsNominated)
            {
                buildCondition.Append("   AND JC.CHK_NOMINATED = 1");
            }
            if (SalesExecMstFk > 0)
            {
                buildCondition.Append("   AND (JC.EXECUTIVE_MST_FK = " + SalesExecMstFk);
                buildCondition.Append("    OR DEF_EXEC.EMPLOYEE_MST_PK = " + SalesExecMstFk + ")");
            }

            if (OtherStatus == 1)
            {
                buildCondition.Append("   AND jc.win_xml_gen = 1");
                //'Generated
            }
            else if (OtherStatus == 2)
            {
                buildCondition.Append("   AND jc.win_xml_gen = 0");
                //'Not Generate
            }
            else if (OtherStatus == 3)
            {
                buildCondition.Append("   AND jc.win_xml_status = 1");
                //'Active
            }
            else if (OtherStatus == 4)
            {
                buildCondition.Append("   AND jc.win_xml_status = 2");
                //'Completed
            }
            else if (OtherStatus == 5)
            {
                buildCondition.Append("   AND jc.win_xml_status = 3");
                //'Cancelled
            }
            else if (OtherStatus == 6)
            {
                buildCondition.Append("   AND jc.win_xml_status = 0");
                //'NA
            }
            else if (OtherStatus == 7)
            {
                buildCondition.Append("   AND jc.win_ack_status = 0");
                //' NR
            }
            else if (OtherStatus == 8)
            {
                buildCondition.Append("   AND jc.win_ack_status = 1");
                //' Ack not received
            }
            else if (OtherStatus == 9)
            {
                buildCondition.Append("   AND jc.win_ack_status = 2");
                //' Success
            }
            else if (OtherStatus == 10)
            {
                buildCondition.Append("   AND jc.win_ack_status = 3");
                //' Failure
            }
            else if (OtherStatus == 11)
            {
                buildCondition.Append("   AND jc.win_ack_status = 4");
                //' Warning
            }

            strCondition = buildCondition.ToString();
            if (process == "2")
            {
                buildQuery.Append(" Select count(distinct JC.JOB_CARD_AIR_EXP_PK) ");
            }
            else
            {
                buildQuery.Append(" Select count(distinct JC.JOB_CARD_AIR_IMP_PK) ");
            }
            buildQuery.Append("      from ");
            buildQuery.Append("               " + strCondition);
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
            buildQuery.Append(" Select * from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select distinct ");
            if (process == "2")
            {
                buildQuery.Append("       JC.JOB_CARD_AIR_EXP_PK JCPK, ");
                buildQuery.Append("       JC.JOBCARD_REF_NO JCREFNR, ");
                buildQuery.Append("        JC.JOBCARD_DATE JCDATE, ");
                ///
                buildQuery.Append("       BK.BOOKING_AIR_PK BKGPK, ");
                buildQuery.Append("       BK.BOOKING_REF_NO BKGREFNR, ");
                buildQuery.Append("       SH.CUSTOMER_MST_PK, ");
                buildQuery.Append("       SH.CUSTOMER_ID, ");
                buildQuery.Append("       SH.CUSTOMER_NAME, ");

                buildQuery.Append("        ART.AIRLINE_NAME, ");
                buildQuery.Append("        POL.PORT_ID POL, ");
                buildQuery.Append("         POD.PORT_ID POD, ");
                buildQuery.Append("        JC.ETD_DATE ETD, ");
                buildQuery.Append("        JC.ETA_DATE ETA, ");

                buildQuery.Append("       HAWB.HAWB_REF_NO HBLREFNR, ");
                buildQuery.Append("       MAWB.MAWB_REF_NO MBLREFNR, ");
                buildQuery.Append("       NVL(EMP.EMPLOYEE_NAME,NVL(DEF_EXEC.EMPLOYEE_NAME,'CSR')) SALES_EXEC, ");
                buildQuery.Append("       DECODE(JC.JOB_CARD_STATUS, '1','Open','2','Close') JCSTATUS,");
                //'
                buildQuery.Append("       Decode(jc.win_xml_gen, 0, 'Not Generated', 1,'Generated') XMLGen, ");
                buildQuery.Append("       Decode(jc.win_xml_status, 0, 'NA', 1, 'Active', 2, 'Completed', 3, 'Cancelled')  XMLStatus, ");
                buildQuery.Append("       Decode(jc.win_ack_status, 0, 'NR', 1, 'Pending for Ack', 2, 'Success', 3, 'Failure',4,'Warning') AckStatus, ");
                buildQuery.Append("       '0' SEL ");
            }
            else
            {
                buildQuery.Append("       JC.JOB_CARD_AIR_IMP_PK JCPK, ");
                buildQuery.Append("       JC.JOBCARD_REF_NO JCREFNR, ");
                buildQuery.Append("        JC.JOBCARD_DATE JCDATE, ");
                ///
                buildQuery.Append("       BK.BOOKING_AIR_PK BKGPK, ");
                buildQuery.Append("       BK.BOOKING_REF_NO BKGREFNR, ");
                buildQuery.Append("       CO.CUSTOMER_MST_PK, ");
                buildQuery.Append("       CO.CUSTOMER_ID, ");
                buildQuery.Append("       CMT.CUSTOMER_NAME, ");

                buildQuery.Append("        ART.AIRLINE_NAME, ");
                buildQuery.Append("        POL.PORT_ID POL, ");
                buildQuery.Append("         POD.PORT_ID POD, ");
                buildQuery.Append("        JC.ETD_DATE ETD, ");
                buildQuery.Append("        JC.ETA_DATE ETA, ");

                buildQuery.Append("       JC.HAWB_REF_NO HBLREFNR, ");
                buildQuery.Append("       JC.MAWB_REF_NO MBLREFNR, ");
                buildQuery.Append("       NVL(EMP.EMPLOYEE_NAME,NVL(DEF_EXEC.EMPLOYEE_NAME,'CSR')) SALES_EXEC, ");
                buildQuery.Append("       DECODE(JC.JOB_CARD_STATUS, '1','Open','2','Close') JCSTATUS,");
                //'
                buildQuery.Append("       Decode(jc.win_xml_gen, 0, 'Not Generated', 1,'Generated') XMLGen, ");
                buildQuery.Append("       Decode(jc.win_xml_status, 0, 'NA', 1, 'Active', 2, 'Completed', 3, 'Cancelled')  XMLStatus, ");
                buildQuery.Append("       Decode(jc.win_ack_status, 0, 'NR', 1, 'Pending for Ack', 2, 'Success', 3, 'Failure',4,'Warning') AckStatus, ");
                buildQuery.Append("       '0' SEL ");
            }

            buildQuery.Append("      from ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("     Order By " + SortColumn + SortType);
            buildQuery.Append("   ) q ");
            buildQuery.Append("  )   ");
            buildQuery.Append("  where  ");
            buildQuery.Append("     SR_NO between " + start + " and " + last);
            strSQL = buildQuery.ToString();
            DataSet DS = null;
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

        #endregion "Fetch Job Card For Listing Screen as per the new Requirement"

        #region "FetchXML JOBdData"

        /// <summary>
        /// Fetches the xmljo bdata.
        /// </summary>
        /// <param name="JobXmlpk">The job XMLPK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchXMLJOBdata(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with4.Add("BIZTYPE_IN", 1).Direction = ParameterDirection.Input;
                _with4.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with4.Add("LOG_LOC_FK", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with4.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FETCH_XMLJOBDATA");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FetchXML JOBdData"

        #region "FetchXML Cargo Data"

        /// <summary>
        /// Fetches the XML cargodata.
        /// </summary>
        /// <param name="JobXmlpk">The job XMLPK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchXMLCargodata(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();

            try
            {
                var _with5 = objWF.MyCommand.Parameters;
                _with5.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with5.Add("BIZTYPE_IN", 1).Direction = ParameterDirection.Input;
                _with5.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with5.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FETCH_XMLCARGODATA");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches the xmlhazardous data.
        /// </summary>
        /// <param name="JobXmlpk">The job XMLPK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchXMLHAZARDOUSData(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();

            try
            {
                var _with6 = objWF.MyCommand.Parameters;
                _with6.Add("CONT_PK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with6.Add("BIZTYPE_IN", 1).Direction = ParameterDirection.Input;
                _with6.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with6.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FETCH_XMLHAZ_DTL");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches the XML commoditydata.
        /// </summary>
        /// <param name="JobXmlpk">The job XMLPK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchXMLCommoditydata(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();

            try
            {
                var _with7 = objWF.MyCommand.Parameters;
                _with7.Add("CONT_PK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with7.Add("BIZTYPE_IN", 1).Direction = ParameterDirection.Input;
                _with7.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with7.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FETCH_XMLCOMMODITY_DTL");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FetchXML Cargo Data"

        #region "FetchPickUpDropDetails"

        /// <summary>
        /// Fetches the XML pick up.
        /// </summary>
        /// <param name="JobXmlpk">The job XMLPK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchXMLPickUp(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with8 = objWF.MyCommand.Parameters;
                _with8.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with8.Add("BIZTYPE_IN", 1).Direction = ParameterDirection.Input;
                _with8.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with8.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FETCH_XML_PICKUP");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches the XML drop.
        /// </summary>
        /// <param name="JobXmlpk">The job XMLPK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchXMLDrop(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with9 = objWF.MyCommand.Parameters;
                _with9.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with9.Add("BIZTYPE_IN", 1).Direction = ParameterDirection.Input;
                _with9.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with9.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FETCH_XML_DROP");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FetchPickUpDropDetails"

        #region "Get Document List"

        /// <summary>
        /// Fetches the XML document.
        /// </summary>
        /// <param name="JobXmlpk">The job XMLPK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchXMLDocument(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with10 = objWF.MyCommand.Parameters;
                _with10.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with10.Add("BIZTYPE_IN", 1).Direction = ParameterDirection.Input;
                _with10.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with10.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FECTCH_XMLDOCUMENT");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Get Document List"

        #region "Get Activity List"

        /// <summary>
        /// Fetches the XML activity.
        /// </summary>
        /// <param name="JobXmlpk">The job XMLPK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchXMLActivity(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with11 = objWF.MyCommand.Parameters;
                _with11.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with11.Add("BIZ_TYPE_IN", 1).Direction = ParameterDirection.Input;
                _with11.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with11.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FETCH_XMLACTIVITIES");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Get Activity List"

        #region "FetchXML IMPSEAAIR"

        /// <summary>
        /// Fetches the XML imp sea air.
        /// </summary>
        /// <param name="JobXmlpk">The job XMLPK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchXMLImpSeaAir(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with12 = objWF.MyCommand.Parameters;
                _with12.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with12.Add("BIZTYPE_IN", 1).Direction = ParameterDirection.Input;
                _with12.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with12.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FETCH_XMLIMP_SEAAIR");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FetchXML IMPSEAAIR"

        #region "FETCHXML TRANSHIPMENT"

        /// <summary>
        /// Fetchxmltranshipments the specified job XMLPK.
        /// </summary>
        /// <param name="JobXmlpk">The job XMLPK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FETCHXMLTRANSHIPMENT(string JobXmlpk = "0", int BizType = 0, int ProcessType = 1)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with13 = objWF.MyCommand.Parameters;
                _with13.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with13.Add("BIZTYPE_IN", 1).Direction = ParameterDirection.Input;
                _with13.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with13.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FETCH_XML_TRANSHIPMENT");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FETCHXML TRANSHIPMENT"

        #region "Get Operator PK"

        /// <summary>
        /// Gets the operator pk.
        /// </summary>
        /// <param name="OperId">The oper identifier.</param>
        /// <returns></returns>
        public object GetOperatorPK(string OperId = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT A.AIRLINE_MST_PK FROM AIRLINE_MST_TBL A WHERE A.AIRLINE_NAME= '" + OperId + "'");
                return (objWF.GetDataSet(sb.ToString()));
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

        #endregion "Get Operator PK"
    }
}