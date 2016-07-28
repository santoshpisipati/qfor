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

using Microsoft.VisualBasic;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class cls_JobCardSearch : CommonFeatures
    {
        private WorkFlow objWF = new WorkFlow();
        private DataSet MainDS = new DataSet();
        private OracleDataAdapter DA = new OracleDataAdapter();
        private DataSet dsBkg = new DataSet();
        private DataSet ds = new DataSet();

        private clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();

        #region "fetch all "

        public int ForFetchEntry(string @ref, string JOBpk = "", string BookingPK = "", string hblRef = "", string proc = "EXP", string Loc = "0", int CargoType = 1)
        {
            string strSQL = null;
            DataSet ds = null;
            if (proc == "EXP")
            {
                strSQL = "SELECT j.job_card_sea_exp_pk,j.booking_sea_fk,h.hbl_ref_no, " + " UMT.DEFAULT_LOCATION_FK, " + " LOC.LOCATION_NAME FROM JOB_CARD_SEA_EXP_TBL j,hbl_exp_tbl h," + " BOOKING_SEA_TBL B, USER_MST_TBL UMT, LOCATION_MST_TBL  LOC" + " WHERE j.hbl_exp_tbl_fk=h.hbl_exp_tbl_pk(+) AND UPPER(j.JOBCARD_REF_NO) LIKE '%" + @ref.ToUpper() + "%'" + " AND J.CREATED_BY_FK = UMT.USER_MST_PK AND LOC.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK AND J.BOOKING_SEA_FK = B.BOOKING_SEA_PK " + " AND B.CARGO_TYPE = " + CargoType + " AND LOC.LOCATION_MST_PK = " + Loc;
            }
            else
            {
                strSQL = "SELECT j.job_card_sea_imp_pk,j.jobcard_ref_no,j.hbl_ref_no, " + " UMT.DEFAULT_LOCATION_FK, " + " LOC.LOCATION_NAME " + " FROM JOB_CARD_SEA_IMP_TBL j,USER_MST_TBL UMT, LOCATION_MST_TBL  LOC" + " WHERE UPPER(j.JOBCARD_REF_NO) LIKE '%" + @ref.ToUpper() + "%'" + " AND J.CONSOLE=1 AND LOC.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK  " + " AND ( (select p.location_mst_fk from port_mst_tbl p where p.port_mst_pk=j.port_mst_pod_fk)=" + Loc + " OR J.PORT_MST_POD_FK IN" + " (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + Loc + "))  " + "  AND UMT.USER_MST_PK=J.CREATED_BY_FK";
            }
            try
            {
                ds = (new WorkFlow()).GetDataSet(strSQL);
                if (ds.Tables[0].Rows.Count == 1)
                {
                    JOBpk = Convert.ToString(getDefault(ds.Tables[0].Rows[0][0], ""));
                    BookingPK = Convert.ToString(getDefault(ds.Tables[0].Rows[0][1], ""));
                    hblRef = Convert.ToString(getDefault(ds.Tables[0].Rows[0][2], ""));
                    if (Loc != getDefault(ds.Tables[0].Rows[0][3], "") & proc == "EXP")
                    {
                        Loc = Convert.ToString(getDefault(ds.Tables[0].Rows[0][4], ""));
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

        public DataSet fetchAllExport(string jobrefNO = "", string bookingNo = "", string HblNo = "", string polID = "", string podId = "", string polName = "", string podName = "", string jcStatus = "", string shipper = "", string consignee = "",
        string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string CurrBizType = "3",
        bool BOOKING = false, string Mbl = "", long lngUsrLocFk = 0, string containerno = "", int jctype = 0, Int32 flag = 0, string hdnPlrpk = "", string hdnPfdpk = "", string hdnSLpk = "", string hdnVslpk = "",
        string UcrNr = "", string Commpk = "0", bool flgXBkg = false, bool flgCL = false)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            StringBuilder buildQuery = new StringBuilder();
            StringBuilder buildCondition = new StringBuilder();
            if (BOOKING == false)
            {
                //2exp 1-imp
                if (process == "2")
                {
                    buildCondition.Append( "     BOOKING_SEA_TBL BK, JOB_CARD_SEA_EXP_TBL JC,");
                    buildCondition.Append( "     HBL_EXP_TBL HBL, ");
                    buildCondition.Append( "     MBL_EXP_TBL MBL, ");
                    //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL DPA, ")
                    buildCondition.Append( "     JOB_TRN_SEA_EXP_CONT cont, ");
                    //added manivannan
                }
                else
                {
                    buildCondition.Append( " JOB_CARD_SEA_IMP_TBL JC,");
                    buildCondition.Append( "     JOB_TRN_SEA_IMP_CONT cont, ");
                    //added manivannan
                }
                buildCondition.Append( "     CUSTOMER_MST_TBL SH,");
                buildCondition.Append( "     CUSTOMER_MST_TBL CO,");
                buildCondition.Append( "     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
                if (process == "2")
                {
                    buildCondition.Append( "     AGENT_MST_TBL DPA, ");
                }
                else
                {
                    buildCondition.Append( "     AGENT_MST_TBL POLA, ");
                }
                //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL CLA,USER_MST_TBL UMT ")
                buildCondition.Append( "     USER_MST_TBL UMT ");
                buildCondition.Append( "      where ");

                if (process == "2")
                {
                    buildCondition.Append( "      BK.BOOKING_SEA_PK = JC.BOOKING_SEA_FK (+)");
                    buildCondition.Append( "   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK");
                    buildCondition.Append( "   AND cont.job_card_sea_exp_fk=jc.job_card_sea_exp_pk");
                    //added manivannan
                    buildCondition.Append( "   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                    buildCondition.Append( "   AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                    buildCondition.Append( "   AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                    //gopi on 15032007  EQA no: 2082
                    //buildCondition.Append(vbCrLf & "    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)")
                    //buildCondition.Append(vbCrLf & "    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ")
                    buildCondition.Append( "    AND  JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
                    //ended
                    buildCondition.Append( "   AND JC.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK (+)");
                    buildCondition.Append( "   AND JC.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK (+)");
                    buildCondition.Append( " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
                    buildCondition.Append( " AND JC.CREATED_BY_FK = UMT.USER_MST_PK ");
                }
                else
                {
                    buildCondition.Append( "   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                    buildCondition.Append( "   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK");
                    buildCondition.Append( "   AND jc.job_card_sea_imp_pk = cont.job_card_sea_imp_fk (+)");
                    //added manivannan
                    buildCondition.Append( "   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                    buildCondition.Append( "   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                    //gopi on 15032007  EQA no: 2082
                    //buildCondition.Append(vbCrLf & "   AND JC.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)")
                    //buildCondition.Append(vbCrLf & "   AND JC.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ")
                    buildCondition.Append( "   AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+) ");
                    //ended
                    //If jctype <> 2 Then
                    //    buildCondition.Append(vbCrLf & "AND  JC.JC_AUTO_MANUAL=" & jctype)
                    //End If
                    buildCondition.Append( " AND ((UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " and JC.JC_AUTO_MANUAL = 0) ");
                    buildCondition.Append( "  OR (JC.PORT_MST_POD_FK ");
                    buildCondition.Append( " IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + lngUsrLocFk + ")  and JC.JC_AUTO_MANUAL = 1)) ");
                    buildCondition.Append( " AND jc.CREATED_BY_FK = UMT.USER_MST_PK ");
                    if (jctype != 2)
                    {
                        buildCondition.Append( "AND  JC.JC_AUTO_MANUAL=" + jctype);
                    }
                }

                if (jobrefNO.Length > 0)
                {
                    buildCondition.Append( " AND UPPER(JOBCARD_REF_NO) LIKE '%" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
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
                    buildCondition.Append( " AND JOBCARD_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
                }

                if (process == "2")
                {
                    if (bookingNo.Length > 0)
                    {
                        buildCondition.Append( " AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                        buildCondition.Append( " AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
                    }
                    buildCondition.Append( " AND BK.STATUS in (2,5,6) ");
                }
                if (jcStatus.Length > 0)
                {
                    buildCondition.Append( " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
                }
                if (HblNo.Trim().Length > 0)
                {
                    buildCondition.Append( " AND UPPER(HBL_REF_NO) LIKE '%" + HblNo.ToUpper().Replace("'", "''") + "%'");
                }
                if (polID.Length > 0)
                {
                    buildCondition.Append( "       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
                }
                if (polName.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
                }
                // PORT OF DISCHARGE
                if (podId.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
                }
                if (podName.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
                }
                //Manoharan 19May09: New controls introduced in the Header part for the Search Criteria. CR-Q-FOR-QBSO-MAY-032
                //For export
                if (Convert.ToInt32(process) == 2)
                {
                    if (!string.IsNullOrEmpty(hdnPlrpk))
                    {
                        buildCondition.Append( " AND BK.COL_PLACE_MST_FK = " + hdnPlrpk);
                    }
                    if (!string.IsNullOrEmpty(hdnPfdpk))
                    {
                        buildCondition.Append( "  AND BK.DEL_PLACE_MST_FK = " + hdnPfdpk);
                    }
                    if (!string.IsNullOrEmpty(hdnSLpk))
                    {
                        buildCondition.Append( "  AND BK.OPERATOR_MST_FK = " + hdnSLpk);
                    }
                    //For import
                }
                else
                {
                    if (!string.IsNullOrEmpty(hdnPfdpk))
                    {
                        buildCondition.Append( "  AND JC.DEL_PLACE_MST_FK = " + hdnPfdpk);
                    }
                    if (!string.IsNullOrEmpty(hdnSLpk))
                    {
                        buildCondition.Append( "  AND JC.OPERATOR_MST_FK = " + hdnSLpk);
                    }
                }
                if (!string.IsNullOrEmpty(hdnVslpk))
                {
                    buildCondition.Append( "  AND JC.VOYAGE_TRN_FK = " + hdnVslpk);
                }
                if (!string.IsNullOrEmpty(UcrNr))
                {
                    buildCondition.Append( "  AND JC.UCR_NO = '" + UcrNr + "'");
                }
                if (Convert.ToInt32(Commpk) != 0)
                {
                    buildCondition.Append( "  AND JC.COMMODITY_GROUP_FK = " + Commpk);
                }
                if (flgXBkg)
                {
                    buildCondition.Append( "  AND JC.CB_AGENT_MST_FK IS NOT NULL");
                }
                if (flgCL)
                {
                    buildCondition.Append( "  AND JC.CL_AGENT_MST_FK IS NOT NULL");
                }
                //End 'Manoharan 19May09: New controls introduced in the Header part for the Search Criteria. CR-Q-FOR-QBSO-MAY-032
                //container no by manivannan
                if (containerno.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(CONT.CONTAINER_NUMBER) LIKE '" + containerno.ToUpper().Replace("'", "''") + "' ");
                }
                // CARGO TYPE
                if (cargoType.Length > 0)
                {
                    if (process == "2")
                    {
                        buildCondition.Append( "   AND BK.CARGO_TYPE = " + cargoType);
                    }
                    else
                    {
                        buildCondition.Append( "  AND JC.CARGO_TYPE = " + cargoType);
                    }
                }
                if (agent.Length > 0)
                {
                    if (process == "2")
                    {
                        buildCondition.Append( " AND  UPPER(DPA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                    }
                    else
                    {
                        buildCondition.Append( " AND  UPPER(POLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                    }
                    //buildCondition.Append(vbCrLf & " AND  (UPPER(CLA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "' OR UPPER(CBA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "')")
                }
                if (shipper.Length > 0)
                {
                    buildCondition.Append( " AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
                }
                if (consignee.Length > 0)
                {
                    buildCondition.Append( " AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
                }
                if (Mbl.Trim().Length > 0)
                {
                    buildCondition.Append( " AND UPPER(MBL_REF_NO) LIKE '%" + Mbl.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else
            {
                //for booking fetching only for job card creation

                buildCondition.Append( "     BOOKING_SEA_TBL  BK,  ");
                buildCondition.Append( "     CUSTOMER_MST_TBL SH,");
                buildCondition.Append( "     CUSTOMER_MST_TBL CO,");
                buildCondition.Append( "     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
                buildCondition.Append( "     AGENT_MST_TBL CBA, ");
                buildCondition.Append( "     AGENT_MST_TBL CLA, ");

                buildCondition.Append( "     USER_MST_TBL UMT ");

                buildCondition.Append( "      where ");
                buildCondition.Append( "   BK.CONS_CUSTOMER_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append( "   AND BK.CUST_CUSTOMER_MST_FK = SH.CUSTOMER_MST_PK");
                buildCondition.Append( "   AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append( "   AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildCondition.Append( "   AND BK.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)");
                buildCondition.Append( "   AND BK.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ");
                buildCondition.Append( " AND BK.BOOKING_SEA_PK NOT IN(SELECT JC.BOOKING_SEA_FK FROM JOB_CARD_SEA_EXP_TBL JC) ");

                buildCondition.Append( " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
                buildCondition.Append( " AND BK.CREATED_BY_FK = UMT.USER_MST_PK ");

                if (bookingNo.Length > 0)
                {
                    buildCondition.Append( " AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                    buildCondition.Append( " AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
                }
                buildCondition.Append( " AND BK.STATUS in (2,5,6) ");

                if (polID.Length > 0)
                {
                    buildCondition.Append( "       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
                }
                if (polName.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
                }
                // PORT OF DISCHARGE
                if (podId.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
                }
                if (podName.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
                }
                //Manoharan 19May09: New controls introduced in the Header part for the Search Criteria. CR-Q-FOR-QBSO-MAY-032
                if (hdnPlrpk != "0")
                {
                    buildCondition.Append( " AND BK.COL_PLACE_MST_FK = " + hdnPlrpk);
                }
                if (hdnPfdpk != "0")
                {
                    buildCondition.Append( "  AND BK.DEL_PLACE_MST_FK = " + hdnPfdpk);
                }
                if (hdnSLpk != "0")
                {
                    buildCondition.Append( "  AND BK.OPERATOR_MST_FK = " + hdnSLpk);
                }
                if (hdnVslpk != "0")
                {
                    buildCondition.Append( "  AND JC.VOYAGE_TRN_FK = " + hdnVslpk);
                }
                if (UcrNr != "0")
                {
                    buildCondition.Append( "  AND JC.UCR_NO = '" + UcrNr + "'");
                }
                if (Convert.ToInt32(Commpk )!= 0)
                {
                    buildCondition.Append( "  AND JC.COMMODITY_GROUP_FK = " + Commpk);
                }
                if (flgXBkg)
                {
                    buildCondition.Append( "  AND JC.CB_AGENT_MST_FK IS NOT NULL");
                }
                if (flgCL)
                {
                    buildCondition.Append( "  AND JC.CL_AGENT_MST_FK IS NOT NULL");
                }
                //End 'Manoharan 19May09: New controls introduced in the Header part for the Search Criteria. CR-Q-FOR-QBSO-MAY-032
                // CARGO TYPE
                if (cargoType.Length > 0)
                {
                    buildCondition.Append( "   AND BK.CARGO_TYPE = " + cargoType);
                }
                if (agent.Length > 0)
                {
                    if (process == "2")
                    {
                        buildCondition.Append( " AND  UPPER(DPA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                    }
                    else
                    {
                        buildCondition.Append( " AND  UPPER(POLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                    }
                    //buildCondition.Append(vbCrLf & " AND  (UPPER(CLA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "' OR UPPER(CBA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "')")
                }
                if (shipper.Length > 0)
                {
                    buildCondition.Append( " AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
                }
                if (consignee.Length > 0)
                {
                    buildCondition.Append( " AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
                }
            }
            if (flag == 0)
            {
                buildCondition.Append( " AND 1=2 ");
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
            buildQuery.Append( "      from ");
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
            buildQuery.Append( "  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append( "    ( Select distinct " );
            if (process == "2")
            {
                buildQuery.Append( "       SH.CUSTOMER_MST_PK, ");
                buildQuery.Append( "       SH.CUSTOMER_ID, ");
                buildQuery.Append( "       SH.CUSTOMER_NAME ");
            }
            else
            {
                //'buildQuery.Append(vbCrLf & "      JC.JC_AUTO_MANUAL, ")
                buildQuery.Append( "       CO.CUSTOMER_MST_PK, ");
                buildQuery.Append( "       CO.CUSTOMER_ID, ");
                buildQuery.Append( "       CO.CUSTOMER_NAME ");
            }

            buildQuery.Append( "      from ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append( "     Order By " + SortColumn + SortType);
            buildQuery.Append( "   ) q ");
            buildQuery.Append( "  )   ");
            buildQuery.Append( "  where  ");
            buildQuery.Append( "     SR_NO between " + start + " and " + last);

            strSQL = buildQuery.ToString();

            DataSet DS = null;

            try
            {
                DS = objWF.GetDataSet(strSQL);
                //FetchChildForBooking
                if (BOOKING)
                {
                    DS.Tables.Add(FetchChildForBooking(AllMasterPKs(DS), jobrefNO, bookingNo, jcStatus, HblNo, polID, podId, polName, podName, shipper,
                    consignee, agent, process, cargoType, SearchFor, SearchFortime,false , containerno, jctype));
                    DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["CUSTOMER_MST_PK"], DS.Tables[1].Columns["CUST_CUSTOMER_MST_FK"], true);
                    DS.Relations.Add(CONTRel);
                }
                else
                {
                    DataRelation CONTRel = null;
                    DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), jobrefNO, bookingNo, jcStatus, HblNo, polID, podId, polName, podName, shipper,
                    consignee, agent, process, cargoType, SearchFor, SearchFortime, false, Mbl, lngUsrLocFk, containerno,
                    jctype, hdnPlrpk, hdnPfdpk, hdnSLpk, hdnVslpk, UcrNr, Commpk, flgXBkg, flgCL));
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

        private string AllMasterPKs(DataSet ds)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                Int16 ln = default(Int16);
                StringBuilder strBuilder = new StringBuilder();
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

        #endregion "fetch all "

        #region " Fetch Childs "

        private DataTable FetchChildFor(string CONTSpotPKs = "", string jobrefNO = "", string bookingNo = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "",
        string consignee = "", string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, bool BOOKING = false, string Mbl = "", long lngUsrLocFk = 0, string containerno = "",
        int jctype = 0, string hdnPlrpk = "", string hdnPfdpk = "", string hdnSLpk = "", string hdnVslpk = "", string UcrNr = "", string Commpk = "", bool flgXBkg = false, bool flgCL = false)
        {
            StringBuilder buildQuery = new StringBuilder();
            StringBuilder buildCondition = new StringBuilder();
            string strCondition = "";
            string strSQL = "";
            string strTable = "JOB_CARD_SEA_EXP_TBL";

            //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            //CONDITION COMING FROM MASTRER FUNCTION

            if (process == "2")
            {
                buildCondition.Append( "     BOOKING_SEA_TBL BK, JOB_CARD_SEA_EXP_TBL JC,");
                buildCondition.Append( "     HBL_EXP_TBL HBL, ");
                buildCondition.Append( "     MBL_EXP_TBL MBL, ");
                //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL DPA, ")
                buildCondition.Append( "     JOB_TRN_SEA_EXP_CONT cont, ");
                //added by manivannan
            }
            else
            {
                buildCondition.Append( " JOB_CARD_SEA_IMP_TBL JC,");
                buildCondition.Append( "     JOB_TRN_SEA_IMP_CONT cont, ");
                //added by manivannan
            }
            buildCondition.Append( "     CUSTOMER_MST_TBL SH,");
            buildCondition.Append( "     CUSTOMER_MST_TBL CO,");
            buildCondition.Append( "     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            if (process == "2")
            {
                buildCondition.Append( "     AGENT_MST_TBL DPA, ");
            }
            else
            {
                buildCondition.Append( "     AGENT_MST_TBL POLA, ");
            }
            // added by gopi
            //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL CBA, ")
            //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL CLA, ")
            buildCondition.Append( "     USER_MST_TBL UMT ");
            buildCondition.Append( "      where ");
            // JOIN CONDITION
            if (process == "2")
            {
                buildCondition.Append( "      BK.BOOKING_SEA_PK = JC.BOOKING_SEA_FK (+)");
                buildCondition.Append( "   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildCondition.Append( "   AND cont.job_card_sea_exp_fk=jc.job_card_sea_exp_pk");
                //added by manivannan
                buildCondition.Append( "   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append( "   AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append( "   AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                //buildCondition.Append(vbCrLf & "    AND JC.CL_AGENT_MST_FK= CLA.AGENT_MST_PK(+)")
                //buildCondition.Append(vbCrLf & "    AND JC.CB_AGENT_MST_FK= CBA.AGENT_MST_PK(+) ")
                buildCondition.Append( "    AND  JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
                buildCondition.Append( "   AND JC.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK (+)");
                buildCondition.Append( "   AND JC.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK (+)");
                buildCondition.Append( " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
                buildCondition.Append( " AND JC.CREATED_BY_FK = UMT.USER_MST_PK ");
            }
            else
            {
                //buildCondition.Append()
                buildCondition.Append( "   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append( "   AND jc.job_card_sea_imp_pk = cont.job_card_sea_imp_fk (+)");
                //added by manivannan
                buildCondition.Append( "   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                buildCondition.Append( "   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append( "   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                //buildCondition.Append(vbCrLf & "   AND JC.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)") 'commented by gopi
                //buildCondition.Append(vbCrLf & "   AND JC.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ")
                buildCondition.Append( "   AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)");
                buildCondition.Append( " AND ((UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " and JC.JC_AUTO_MANUAL = 0 )");
                buildCondition.Append( "  OR (JC.PORT_MST_POD_FK ");
                buildCondition.Append( " IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + lngUsrLocFk + ") and JC.JC_AUTO_MANUAL = 1)) ");
                buildCondition.Append( " AND jc.CREATED_BY_FK = UMT.USER_MST_PK ");
                if (jctype != 2)
                {
                    buildCondition.Append( "AND JC.JC_AUTO_MANUAL =" + jctype);
                }
            }

            if (jobrefNO.Length > 0)
            {
                buildCondition.Append( " AND UPPER(JOBCARD_REF_NO) LIKE '%" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
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
                buildCondition.Append( " AND JOBCARD_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
            }

            if (process == "2")
            {
                if (bookingNo.Length > 0)
                {
                    buildCondition.Append( " AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                    buildCondition.Append( " AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
                }
                buildCondition.Append( " AND BK.STATUS in (2,5,6) ");
            }
            if (jcStatus.Length > 0)
            {
                buildCondition.Append( " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
            }
            if (Hbl.Trim().Length > 0)
            {
                buildCondition.Append( " AND UPPER(HBL_REF_NO) LIKE '%" + Hbl.ToUpper().Replace("'", "''") + "%'");
            }
            if (Mbl.Trim().Length > 0)
            {
                buildCondition.Append( " AND UPPER(MBL_REF_NO) LIKE '%" + Mbl.ToUpper().Replace("'", "''") + "%'");
            }
            if (polID.Length > 0)
            {
                buildCondition.Append( "       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
            }
            if (polName.Length > 0)
            {
                buildCondition.Append( "     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
            }
            // PORT OF DISCHARGE
            if (podId.Length > 0)
            {
                buildCondition.Append( "     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
            }
            if (podName.Length > 0)
            {
                buildCondition.Append( "     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
            }
            //Manoharan 19May09: New controls introduced in the Header part for the Search Criteria. CR-Q-FOR-QBSO-MAY-032
            //For export
            if (Convert.ToInt32(process) == 2)
            {
                if (!string.IsNullOrEmpty(hdnPlrpk))
                {
                    buildCondition.Append( " AND BK.COL_PLACE_MST_FK = " + hdnPlrpk);
                }
                if (!string.IsNullOrEmpty(hdnPfdpk))
                {
                    buildCondition.Append( "  AND BK.DEL_PLACE_MST_FK = " + hdnPfdpk);
                }
                if (!string.IsNullOrEmpty(hdnSLpk))
                {
                    buildCondition.Append( "  AND BK.OPERATOR_MST_FK = " + hdnSLpk);
                }
                //For import
            }
            else
            {
                if (!string.IsNullOrEmpty(hdnPfdpk))
                {
                    buildCondition.Append( "  AND JC.DEL_PLACE_MST_FK = " + hdnPfdpk);
                }
                if (!string.IsNullOrEmpty(hdnSLpk))
                {
                    buildCondition.Append( "  AND JC.OPERATOR_MST_FK = " + hdnSLpk);
                }
            }
            if (!string.IsNullOrEmpty(hdnVslpk))
            {
                buildCondition.Append( "  AND JC.VOYAGE_TRN_FK = " + hdnVslpk);
            }
            if (!string.IsNullOrEmpty(UcrNr))
            {
                buildCondition.Append( "  AND JC.UCR_NO = '" + UcrNr + "'");
            }
            if (Convert.ToInt32(Commpk) != 0)
            {
                buildCondition.Append( "  AND JC.COMMODITY_GROUP_FK = " + Commpk);
            }
            if (flgXBkg)
            {
                buildCondition.Append( "  AND JC.CB_AGENT_MST_FK IS NOT NULL");
            }
            if (flgCL)
            {
                buildCondition.Append( "  AND JC.CL_AGENT_MST_FK IS NOT NULL");
            }
            //End 'Manoharan 19May09: New controls introduced in the Header part for the Search Criteria. CR-Q-FOR-QBSO-MAY-032

            //container no
            if (containerno.Length > 0)
            {
                buildCondition.Append( "     AND UPPER(CONT.CONTAINER_NUMBER) LIKE '" + containerno.ToUpper().Replace("'", "''") + "' ");
            }
            // CARGO TYPE
            if (cargoType.Length > 0)
            {
                if (process == "2")
                {
                    buildCondition.Append( "   AND BK.CARGO_TYPE = " + cargoType);
                }
                else
                {
                    buildCondition.Append( "  AND JC.CARGO_TYPE = " + cargoType);
                }
            }
            if (agent.Length > 0)
            {
                if (process == "2")
                {
                    buildCondition.Append( " AND  UPPER(DPA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                }
                else
                {
                    buildCondition.Append( " AND  UPPER(POLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                }
                //buildCondition.Append(vbCrLf & " AND  (UPPER(CLA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "' OR UPPER(CBA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "')")
            }
            if (shipper.Length > 0)
            {
                buildCondition.Append( " AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
            }
            if (consignee.Length > 0)
            {
                buildCondition.Append( " AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
            }

            //===========================================================================================================================

            if (CONTSpotPKs.Trim().Length > 0)
            {
                if (process == "2")
                {
                    buildCondition.Append( " and JC.SHIPPER_CUST_MST_FK in (" + CONTSpotPKs + ") ");
                }
                else
                {
                    buildCondition.Append( " and JC.CONSIGNEE_CUST_MST_FK in (" + CONTSpotPKs + ") ");
                }
            }

            //buildCondition.Append(vbCrLf & " AND UMT.DEFAULT_LOCATION_FK = " & lngUsrLocFk & " ")
            //buildCondition.Append(vbCrLf & " AND JC.CREATED_BY_FK = UMT.USER_MST_PK ")

            strCondition = buildCondition.ToString();

            buildQuery.Append( " Select * from ");
            buildQuery.Append( "  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append( "    ( Select distinct " );
            //CUST_CUSTOMER_MST_FK
            if (process == "2")
            {
                buildQuery.Append( "       JC.SHIPPER_CUST_MST_FK, ");
                buildQuery.Append( "       JOB_CARD_SEA_EXP_PK, ");
                buildQuery.Append( "       BOOKING_SEA_PK, ");
                buildQuery.Append( "       BOOKING_REF_NO, ");
            }
            else
            {
                buildQuery.Append( "       JC.CONSIGNEE_CUST_MST_FK, ");
                buildQuery.Append( "       JOB_CARD_SEA_IMP_PK, ");
                //CONSIGNEE_CUST_MST_FK
                buildQuery.Append( "       PORT_MST_POL_FK, ");
                buildQuery.Append( "       PORT_MST_POD_FK, ");
            }
            buildQuery.Append( "       JOBCARD_REF_NO, ");
            buildQuery.Append( "       HBL_REF_NO, ");
            buildQuery.Append( "       MBL_REF_NO,JOBCARD_DATE ");
            //modified by manivannan
            buildQuery.Append( "      from ");

            buildQuery.Append( strCondition);
            if (process == "2")
            {
                buildQuery.Append( "      ORDER BY JOB_CARD_SEA_EXP_PK DESC,JOBCARD_REF_NO DESC  ");
            }
            else
            {
                buildQuery.Append( "      ORDER BY JOB_CARD_SEA_IMP_PK DESC,JOBCARD_REF_NO DESC  ");
            }
            //buildQuery.Append(vbCrLf & "      ORDER BY JOBCARD_DATE DESC,JOBCARD_REF_NO DESC  ")
            buildQuery.Append( "    ) q ");
            buildQuery.Append( "  )   ");
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
                string strCol = (process == "2" ? "SHIPPER_CUST_MST_FK" : "CONSIGNEE_CUST_MST_FK");
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
        private DataTable FetchChildForBooking(string CONTSpotPKs = "", string jobrefNO = "", string bookingNo = "", string jcStatus = "", string Hbl = "", string polID = "", string podId = "", string polName = "", string podName = "", string shipper = "",
        string consignee = "", string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, bool BOOKING = false, string containerno = "", int jctype = 0)
        {
            StringBuilder buildQuery = new StringBuilder();
            StringBuilder buildCondition = new StringBuilder();
            string strCondition = "";
            string strSQL = "";
            string strTable = "JOB_CARD_SEA_EXP_TBL";

            //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            //CONDITION COMING FROM MASTRER FUNCTION

            buildCondition.Append( "     BOOKING_SEA_TBL  BK,  ");
            buildCondition.Append( "     CUSTOMER_MST_TBL SH,");
            buildCondition.Append( "     CUSTOMER_MST_TBL CO,");
            buildCondition.Append( "     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
            //added by gopi
            if (process == "2")
            {
                buildCondition.Append( "     AGENT_MST_TBL DPA, ");
            }
            else
            {
                buildCondition.Append( "     AGENT_MST_TBL POLA, ");
            }

            //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL CBA, ")
            //buildCondition.Append(vbCrLf & "     AGENT_MST_TBL CLA ")

            buildCondition.Append( "      where ");
            // JOIN CONDITION
            buildCondition.Append( "   BK.CONS_CUSTOMER_MST_FK = CO.CUSTOMER_MST_PK(+)");
            buildCondition.Append( "   AND BK.CUST_CUSTOMER_MST_FK = SH.CUSTOMER_MST_PK(+)");
            buildCondition.Append( "   AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
            buildCondition.Append( "   AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");

            buildCondition.Append( "   AND BK.dp_agent_mst_fk  = DPA.AGENT_MST_PK (+)");
            //buildCondition.Append(vbCrLf & "   AND BK.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)")
            //buildCondition.Append(vbCrLf & "   AND BK.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ")

            if (bookingNo.Length > 0)
            {
                buildCondition.Append( " AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                buildCondition.Append( " AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
            }
            buildCondition.Append( " AND BK.STATUS in (2,5,6) ");

            if (polID.Length > 0)
            {
                buildCondition.Append( "       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
            }
            if (polName.Length > 0)
            {
                buildCondition.Append( "     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
            }
            // PORT OF DISCHARGE
            if (podId.Length > 0)
            {
                buildCondition.Append( "     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
            }
            if (podName.Length > 0)
            {
                buildCondition.Append( "     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
            }
            // CARGO TYPE
            if (cargoType.Length > 0)
            {
                buildCondition.Append( "   AND BK.CARGO_TYPE = " + cargoType);
            }
            if (agent.Length > 0)
            {
                if (process == "2")
                {
                    buildCondition.Append( " AND  UPPER(DPA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                }
                else
                {
                    buildCondition.Append( " AND  UPPER(POLA.AGENT_ID) = '" + agent.ToUpper().Replace("'", "''") + "'");
                }
                //buildCondition.Append(vbCrLf & " AND  (UPPER(CLA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "' OR UPPER(CBA.AGENT_ID) = '" & agent.ToUpper.Replace("'", "''") & "')")
            }

            if (shipper.Length > 0)
            {
                buildCondition.Append( " AND UPPER(SH.CUSTOMER_ID) LIKE '" + shipper.ToUpper().Replace("'", "''") + "'");
            }
            if (consignee.Length > 0)
            {
                buildCondition.Append( " AND UPPER(CO.CUSTOMER_ID) LIKE '" + consignee.ToUpper().Replace("'", "''") + "'");
            }

            //===========================================================================================================================

            if (CONTSpotPKs.Trim().Length > 0)
            {
                buildCondition.Append( " and CUST_CUSTOMER_MST_FK in (" + CONTSpotPKs + ") ");
            }

            buildCondition.Append( " AND BK.BOOKING_SEA_PK NOT IN(SELECT JC.BOOKING_SEA_FK FROM JOB_CARD_SEA_EXP_TBL JC) ");

            strCondition = buildCondition.ToString();

            buildQuery.Append( "     Select '' SR_NO," );
            //CUST_CUSTOMER_MST_FK
            buildQuery.Append( "         BK.CUST_CUSTOMER_MST_FK, ");
            //
            buildQuery.Append( "      '' JOB_CARD_SEA_EXP_PK, ");
            buildQuery.Append( "       BOOKING_SEA_PK, ");
            buildQuery.Append( "       BOOKING_REF_NO, ");
            buildQuery.Append( "      '' JOBCARD_REF_NO, ");
            buildQuery.Append( "      '' HBL_REF_NO, ");
            buildQuery.Append( "      '' MBL_REF_NO, ");
            buildQuery.Append( "      '' HBL_EXP_TBL_PK ");

            buildQuery.Append( "      from ");

            buildQuery.Append( strCondition);
            buildQuery.Append( "      Order By BOOKING_REF_NO ASC ");

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

        public string FetchForShipperAndConsignee(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strCATEGORY_IN = "";
            string businessType = "";
            string Import = "";
            string Consignee = "0";
            string strLOC_MST_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strCATEGORY_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                businessType = Convert.ToString(arr.GetValue(4));
            //new condition added by vimlesh kumar for checking consignee
            //in place of location we need to pass pod pk
            //this condition gives the consignee of pod location.
            if (arr.Length > 5)
                Consignee = "1";
            //added by gopi in import side we need show all shippper
            if (arr.Length > 6)
                Import = Convert.ToString(arr.GetValue(6));

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
                _with1.Add("CONSIGNEE_IN", Consignee).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with1.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        //FetchForShipperComm : Amit
        public string FetchForShipperComm(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strCATEGORY_IN = "";
            string businessType = "";
            string Import = "";
            string Consignee = "0";
            string strLOC_MST_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strCATEGORY_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                businessType = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Consignee = "1";
            if (arr.Length > 6)
                Import = Convert.ToString(arr.GetValue(6));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_CATEGORY_COM";
                var _with2 = SCM.Parameters;
                _with2.Add("CATEGORY_IN", ifDBNull(strCATEGORY_IN)).Direction = ParameterDirection.Input;
                _with2.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("CONSIGNEE_IN", Consignee).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with2.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        // By Amit
        public string FetchConsigneeLookUp(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strCATEGORY_IN = "";
            string businessType = "";
            string Import = "";
            int Vendor = 0;
            string Consignee = "0";
            string strLOC_MST_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strCATEGORY_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                businessType = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                Consignee = "1";
            if (arr.Length > 6)
                Import = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                Vendor = Convert.ToInt32(arr.GetValue(7));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_CATEGORY_LOOKUP";
                var _with3 = SCM.Parameters;
                _with3.Add("CATEGORY_IN", ifDBNull(strCATEGORY_IN)).Direction = ParameterDirection.Input;
                _with3.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("CONSIGNEE_IN", Consignee).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with3.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input;
                _with3.Add("VENDOR_IN", Vendor).Direction = ParameterDirection.Input;
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

        //IMRAN :16th Feb 2006
        public string FetchForSeaImportJobRef(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_ACTIVE_JOB_REF_IMP_COMMON";
                var _with4 = SCM.Parameters;
                _with4.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        public string FetchForAgent(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            string businessType = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            businessType = Convert.ToString(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_AGENT_PKG.GETAGENT_COMMON";
                var _with5 = SCM.Parameters;
                _with5.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with5.Add("BUSINESS_TYPE_IN", OracleDbType.NVarchar2, 2, businessType).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        public string FetchForJobRef(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            //Business Type to identify the user belongs to AIR/SEA
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_JOB_REF_COMMON";
                var _with6 = SCM.Parameters;
                _with6.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with6.Add("LOCATION_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        // Written By : Rijesh
        // Date       : 08-Jun -2006
        // For Transporter Note Print Screen
        public string FetchJobRefForTransNote(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            //Business Type to identify the user belongs to AIR/SEA
            //If arr.Length > 3 Then strBusiType = arr(3)
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_FOR_TRANSPORT_NOTE_PKG.GET_JOBCARDNO";
                var _with7 = SCM.Parameters;
                _with7.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with7.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with7.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with7.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        public string FetchForImportJobRef(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            //Business Type to identify the user belongs to AIR/SEA
            if (arr.Length > 3)
                strBusiType = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_IMPORT_JOB_REF_NO_PKG.GET_JOB_REF_COMMON";
                var _with8 = SCM.Parameters;
                _with8.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with8.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with8.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with8.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        public string FetchForTransImportJobRef(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            //Business Type to identify the user belongs to AIR/SEA
            if (arr.Length > 3)
                strBusiType = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_IMPORT_JOB_REF_NO_PKG.GET_JOB_REF_TRNSPORTER_IMP";
                var _with9 = SCM.Parameters;
                _with9.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with9.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with9.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with9.Add("RETURN_VALUE", OracleDbType.NVarchar2, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        public string FetchActiveJobCard(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strReq = null;
            string strLoc = null;
            strLoc = loc;
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
                var _with10 = SCM.Parameters;
                _with10.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with10.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with10.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with10.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with10.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        public string FetchActiveJobCardHBLMBL(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strReq = null;
            string strAgentType = null;
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strAgentType = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_ACT_JOB_FOR_EXP_INV_HBLMBL";
                var _with11 = SCM.Parameters;
                _with11.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with11.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with11.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with11.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with11.Add("AGENTTYPE_IN", strAgentType).Direction = ParameterDirection.Input;
                _with11.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        public string FetchActiveJobCardHBLMBLNew(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strReq = null;
            string strAgentType = null;
            string strProcessType = null;
            string strAgentID = null;
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strAgentType = Convert.ToString(arr.GetValue(3));
            strProcessType = Convert.ToString(arr.GetValue(4));
            // strAgentID = arr(5)
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_ACT_JOB_FOR_EXP_INV_NEW";
                var _with12 = SCM.Parameters;
                _with12.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with12.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with12.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with12.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with12.Add("AGENTTYPE_IN", strAgentType).Direction = ParameterDirection.Input;
                _with12.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
                // .Add("AGENT_TYPE_IN", strAgentID).Direction = ParameterDirection.Input
                //.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output
                //SCM.Parameters("RETURN_VALUE").SourceVersion = DataRowVersion.Current
                _with12.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
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

        // Written By : Amit
        // Date       : 14-May -2007
        // For Shipper & Consignee Search
        public string FetchCustomerLocation(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strCATEGORY_IN = "";
            string businessType = "";
            string Import = "";
            string Consignee = "0";
            string strLOC_MST_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strCATEGORY_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                businessType = Convert.ToString(arr.GetValue(4));
            //in place of location we need to pass pod pk
            //this condition gives the consignee of pod location.
            //If arr.Length > 5 Then Consignee = "1"
            //import side we need show all shippper
            //If arr.Length > 6 Then Import = arr(6)

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_LOCATION";
                var _with13 = SCM.Parameters;
                _with13.Add("CATEGORY_IN", ifDBNull(strCATEGORY_IN)).Direction = ParameterDirection.Input;
                _with13.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with13.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with13.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("CONSIGNEE_IN", Consignee).Direction = ParameterDirection.Input
                _with13.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                _with13.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, DBNull.Value))
            {
                return "";
            }
            return col;
        }

        #endregion " Supporting Function "

        #endregion " Enhance Search Functions "

        #region "Fetch Export Header Details"

        public DataSet FetchSeaExpHeaderDocment(int JobcardPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "select JobSeaExp.Job_Card_Sea_Exp_Pk Jobpk,";
            strSQL +=  "JobSeaExp.Jobcard_Ref_No RefNo,";
            strSQL +=  "hbl.hbl_ref_no HbNo,";
            strSQL +=  "mbl.mbl_ref_no MbNo,";
            strSQL +=  "JobSeaExp.Ucr_No UCRNO,";
            strSQL +=  "JobSeaExp.Vessel_Name VesFlight,";
            strSQL +=  "JobSeaExp.Voyage Voyage,";
            strSQL +=  "CustMstShipper.Customer_Name Shipper,";
            strSQL +=  "CustShipperDtls.Adm_Address_1 ShiAddress1,";
            strSQL +=  "CustShipperDtls.Adm_Address_2 ShiAddress2,";
            strSQL +=  "CustShipperDtls.Adm_Address_3 ShiAddress3,";
            strSQL +=  "CustShipperDtls.Adm_City ShiCity,";
            strSQL +=  "CustMstConsignee.Customer_Name Consignee,";
            strSQL +=  "CustConsigneeDtls.Adm_Address_1 ConsiAddress1,";
            strSQL +=  "CustConsigneeDtls.Adm_Address_2 ConsiAddress2,";
            strSQL +=  "CustConsigneeDtls.Adm_Address_3 ConsiAddress3,";
            strSQL +=  "CustConsigneeDtls.Adm_City ConsiCity,";
            strSQL +=  "AgentMst.Agent_Name,";
            strSQL +=  "AgentDtls.Adm_Address_1 AgtAddress1,";
            strSQL +=  "AgentDtls.Adm_Address_2 AgtAddress2,";
            strSQL +=  "AgentDtls.Adm_Address_3 AgtAddress3,";
            strSQL +=  "AgentDtls.Adm_City AgtCity,";
            strSQL +=  "POL.PORT_NAME POL,";
            strSQL +=  "POD.PORT_NAME POD,";
            strSQL +=  "Pmt.PLACE_NAME PLD,";
            strSQL +=  "hbl.goods_description GoodsDesc";
            strSQL +=  "from job_card_sea_exp_tbl JobSeaExp,";
            strSQL +=  "hbl_exp_tbl hbl,";
            strSQL +=  "mbl_exp_tbl mbl,";
            strSQL +=  "Customer_Mst_Tbl CustMstShipper,";
            strSQL +=  "Customer_Mst_Tbl CustMstConsignee,";
            strSQL +=  "Agent_Mst_Tbl AgentMst,";
            strSQL +=  "Booking_Sea_Tbl BkgSea,";
            strSQL +=  "Port_Mst_Tbl POL,";
            strSQL +=  "Port_Mst_Tbl POD,";
            strSQL +=  "Place_Mst_Tbl PMT,";
            strSQL +=  "Customer_Contact_Dtls CustShipperDtls,";
            strSQL +=  "Customer_Contact_Dtls CustConsigneeDtls,";
            strSQL +=  "Agent_Contact_Dtls AgentDtls";
            strSQL +=  "where JobSeaExp.Shipper_Cust_Mst_Fk = CustMstShipper.Customer_Mst_Pk(+)";
            strSQL +=  "and JobSeaExp.Consignee_Cust_Mst_Fk=CustMstConsignee.Customer_Mst_Pk(+)";
            strSQL +=  "and JobSeaExp.Dp_Agent_Mst_Fk=AgentMst.Agent_Mst_Pk(+)";
            strSQL +=  "and POL.PORT_MST_PK(+)=BkgSea.Port_Mst_Pol_Fk";
            strSQL +=  "and POD.PORT_MST_PK(+)=BkgSea.Port_Mst_Pod_Fk";
            strSQL +=  "and PMT.PLACE_PK(+)=BkgSea.Del_Place_Mst_Fk";
            strSQL +=  "and CustMstShipper.Customer_Mst_Pk=CustShipperDtls.Customer_Mst_Fk(+)";
            strSQL +=  "and CustMstConsignee.Customer_Mst_Pk=CustConsigneeDtls.Customer_Mst_Fk(+)";
            strSQL +=  "and AgentMst.Agent_Mst_Pk=AgentDtls.Agent_Mst_Fk(+)";
            strSQL +=  "and JobSeaExp.Booking_Sea_Fk=BkgSea.Booking_Sea_Pk(+)";
            strSQL +=  " and hbl.job_card_sea_exp_fk(+)=JobSeaExp.Job_Card_Sea_Exp_Pk";
            strSQL +=  "and hbl.hbl_exp_tbl_pk(+)=JobSeaExp.Hbl_Exp_Tbl_Fk";
            strSQL +=  "and mbl.mbl_exp_tbl_pk(+)=JobSeaExp.Mbl_Exp_Tbl_Fk";
            strSQL +=  "and JobSeaExp.Job_Card_Sea_Exp_Pk=" + JobcardPK;

            try
            {
                return (objWF.GetDataSet(strSQL));
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

        #endregion "Fetch Export Header Details"

        #region "Fetch Import Header Details"

        public DataSet FetchSeaImpHeaderDocment(int JobcardPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "SELECT JSI.JOB_CARD_TRN_PK JOBPK,";
            strSQL +=  "JSI.JOBCARD_REF_NO JOBREFNO,";
            strSQL +=  "'' BKGPK,";
            strSQL +=  "'' BKGREFNO,";
            strSQL +=  "'' BKGDATE,";
            strSQL +=  "(CASE WHEN JSI.VOYAGE_FLIGHT_NO IS NOT NULL THEN";
            strSQL +=  "JSI.VESSEL_NAME || '-' || JSI.VOYAGE_FLIGHT_NO";
            strSQL +=  "ELSE";
            strSQL +=  "JSI.VESSEL_NAME END) VESFLIGHT,";
            strSQL +=  " JSI.HBL_HAWB_REF_NO HBLREFNO,";
            strSQL +=  "JSI.MBL_MAWB_REF_NO MBLREFNO,";
            strSQL +=  "JSI.MARKS_NUMBERS MARKS,";
            strSQL +=  "JSI.GOODS_DESCRIPTION GOODS,";
            strSQL +=  "JSI.CARGO_TYPE CARGO_TYPE,";
            strSQL +=  "JSI.UCR_NO UCRNO,";
            strSQL +=  "JSI.CLEARANCE_ADDRESS CLEARANCEPOINT,";
            strSQL +=  "TO_CHAR(JSI.ETD_DATE,'" + dateFormat + "') ETD,";
            strSQL +=  "JSI.SHIPPER_CUST_MST_FK,";
            strSQL +=  "CMST.CUSTOMER_NAME SHIPNAME,";
            strSQL +=  "'' SHIPREFNO,";
            strSQL +=  "CDTLS.ADM_ADDRESS_1 SHIPADD1,";
            strSQL +=  "CDTLS.ADM_ADDRESS_2 SHIPADD2,";
            strSQL +=  "CDTLS.ADM_ADDRESS_3 SHIPADD3,";
            strSQL +=  "CDTLS.ADM_CITY SHIPCITY,";
            strSQL +=  "CDTLS.ADM_ZIP_CODE SHIPZIP,";
            strSQL +=  "CDTLS.ADM_EMAIL_ID AS SHIPEMAIL,";
            strSQL +=  "CDTLS.ADM_PHONE_NO_1 SHIPPHONE,";
            strSQL +=  "CDTLS.ADM_FAX_NO SHIPFAX,";
            strSQL +=  "SHICOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
            strSQL +=  "CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,";
            strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGADD1,";
            strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGADD2,";
            strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGADD3,";
            strSQL +=  "CONSIGNEEDTLS.ADM_CITY CONSIGCITY,";
            strSQL +=  "CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGZIP,";
            strSQL +=  "CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGEMAIL,";
            strSQL +=  "CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGPHONE,";
            strSQL +=  "CONSIGNEEDTLS.ADM_FAX_NO CONSIGFAX,";
            strSQL +=  "CONSIGCOUNTRY.COUNTRY_NAME CONSICOUNTRY,";
            strSQL +=  "POL.PORT_NAME POLNAME,";
            strSQL +=  "POD.PORT_NAME PODNAME,";
            strSQL +=  "PLD.PLACE_NAME DELNAME, ";
            strSQL +=  "DBAMST.AGENT_MST_PK DBAGENTPK,";
            strSQL +=  "DBAMST.AGENT_NAME DBAGENTNAME,";
            strSQL +=  "DBADTLS.ADM_ADDRESS_1 DBAGENTADD1,";
            strSQL +=  "DBADTLS.ADM_ADDRESS_2 DBAGENTADD2,";
            strSQL +=  "DBADTLS.ADM_ADDRESS_3 DBAGENTADD3,";
            strSQL +=  "DBADTLS.ADM_CITY DBAGENTCITY,";
            strSQL +=  "DBADTLS.ADM_ZIP_CODE DBAGENTZIP,";
            strSQL +=  "DBADTLS.ADM_EMAIL_ID DBAGENTEMAIL,";
            strSQL +=  "DBADTLS.ADM_PHONE_NO_1 DBAGENTPHONE,";
            strSQL +=  "DBADTLS.ADM_FAX_NO DBAGENTFAX,";
            strSQL +=  "DBCOUNTRY.COUNTRY_NAME DBCOUNTRY,";
            strSQL +=  "STMST.INCO_CODE TERMS,";
            strSQL +=  "NVL(JSI.INSURANCE_AMT, 0) INSURANCE,";
            strSQL +=  "JSI.PYMT_TYPE,";
            strSQL +=  "CGMST.commodity_group_desc COMMCODE,";
            strSQL +=  "TO_CHAR(JSI.ETA_DATE,'" + dateFormat + "') ETA,";
            strSQL +=  "SUM(JTAEC.GROSS_WEIGHT) GROSS,";
            strSQL +=  "SUM(JTAEC.CHARGEABLE_WEIGHT) CHARWT,";
            strSQL +=  "SUM(JTAEC.NET_WEIGHT) NETWT,";
            strSQL +=  "SUM(JTAEC.VOLUME_IN_CBM) VOLUME";
            strSQL +=  "FROM JOB_CARD_TRN JSI,";
            //strSQL &= vbCrLf & "JOB_TRN_SEA_IMP_TP   JTAEP,"
            strSQL +=  "JOB_TRN_CONT JTAEC,";
            strSQL +=  "CUSTOMER_MST_TBL      CMST,";
            strSQL +=  "CUSTOMER_CONTACT_DTLS CDTLS,";
            strSQL +=  "CUSTOMER_MST_TBL      CONSIGNEE,";
            strSQL +=  "CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,";
            strSQL +=  "COUNTRY_MST_TBL       SHICOUNTRY,";
            strSQL +=  "COUNTRY_MST_TBL       CONSIGCOUNTRY,";
            strSQL +=  "PORT_MST_TBL          POL,";
            strSQL +=  "PORT_MST_TBL          POD,";
            strSQL +=  "PLACE_MST_TBL         PLD,";
            strSQL +=  "AGENT_MST_TBL           DBAMST,";
            strSQL +=  "AGENT_CONTACT_DTLS      DBADTLS,";
            strSQL +=  "COUNTRY_MST_TBL         DBCOUNTRY,";
            strSQL +=  "SHIPPING_TERMS_MST_TBL  STMST,";
            strSQL +=  "COMMODITY_GROUP_MST_TBL CGMST";
            strSQL +=  "WHERE";
            strSQL +=  "JSI.JOB_CARD_TRN_PK IN(" + JobcardPK + " )";
            //strSQL &= vbCrLf & "AND JTAEP.JOB_CARD_TRN_FK(+) = JSI.JOB_CARD_TRN_PK "
            //strSQL &= vbCrLf & "AND  nvl(JTAEP.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_SEA_IMP_TP JTT WHERE JTT.JOB_CARD_TRN_FK=JTAEP.JOB_CARD_TRN_FK)"
            strSQL +=  "AND JTAEC.JOB_CARD_TRN_FK(+) = JSI.JOB_CARD_TRN_PK";
            strSQL +=  "AND CMST.CUSTOMER_MST_PK(+) = JSI.SHIPPER_CUST_MST_FK";
            strSQL +=  "AND CDTLS.CUSTOMER_MST_FK(+) = CMST.CUSTOMER_MST_PK";
            strSQL +=  "AND CONSIGNEE.CUSTOMER_MST_PK(+) = JSI.CONSIGNEE_CUST_MST_FK";
            strSQL +=  "AND CONSIGNEEDTLS.CUSTOMER_MST_FK(+) = CONSIGNEE.CUSTOMER_MST_PK";
            strSQL +=  "AND JSI.PORT_MST_POL_FK = POL.PORT_MST_PK(+)";
            strSQL +=  "AND JSI.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
            strSQL +=  "AND JSI.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)";
            strSQL +=  "AND JSI.POL_AGENT_MST_FK = DBAMST.AGENT_MST_PK(+)";
            strSQL +=  "AND DBAMST.AGENT_MST_PK = DBADTLS.AGENT_MST_FK(+)";
            strSQL +=  "AND DBCOUNTRY.COUNTRY_MST_PK(+) = DBADTLS.ADM_COUNTRY_MST_FK";
            strSQL +=  "AND CONSIGCOUNTRY.COUNTRY_MST_PK(+) = CONSIGNEE.COUNTRY_MST_FK";
            strSQL +=  "AND SHICOUNTRY.COUNTRY_MST_PK(+) = CMST.COUNTRY_MST_FK";
            strSQL +=  "AND STMST.SHIPPING_TERMS_MST_PK(+) = JSI.SHIPPING_TERMS_MST_FK";
            strSQL +=  "AND JSI.COMMODITY_GROUP_FK = CGMST.COMMODITY_GROUP_PK(+)";
            strSQL +=  "GROUP BY JSI.JOB_CARD_TRN_PK,";
            strSQL +=  "JSI.JOBCARD_REF_NO,  ";
            strSQL +=  "(CASE WHEN JSI.VOYAGE_FLIGHT_NO IS NOT NULL THEN";
            strSQL +=  "JSI.VESSEL_NAME || '-' || JSI.VOYAGE_FLIGHT_NO";
            strSQL +=  "ELSE";
            strSQL +=  "JSI.VESSEL_NAME END),";
            strSQL +=  "JSI.HBL_HAWB_REF_NO,";
            strSQL +=  "JSI.MBL_MAWB_REF_NO,";
            strSQL +=  "JSI.MARKS_NUMBERS,";
            strSQL +=  "JSI.GOODS_DESCRIPTION,";
            strSQL +=  "JSI.UCR_NO, ";
            strSQL +=  "JSI.CLEARANCE_ADDRESS,";
            strSQL +=  "JSI.ETD_DATE,";
            strSQL +=  "JSI.SHIPPER_CUST_MST_FK,";
            strSQL +=  "CMST.CUSTOMER_NAME,";
            strSQL +=  "CDTLS.ADM_ADDRESS_1,";
            strSQL +=  "CDTLS.ADM_ADDRESS_2,";
            strSQL +=  "CDTLS.ADM_ADDRESS_3,";
            strSQL +=  "CDTLS.ADM_CITY,";
            strSQL +=  "CDTLS.ADM_ZIP_CODE,";
            strSQL +=  "CDTLS.ADM_EMAIL_ID,";
            strSQL +=  "CDTLS.ADM_PHONE_NO_1,";
            strSQL +=  "CDTLS.ADM_FAX_NO,";
            strSQL +=  "SHICOUNTRY.COUNTRY_NAME,";
            strSQL +=  "CONSIGNEE.CUSTOMER_NAME,";
            strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_1,";
            strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_2,";
            strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_3,";
            strSQL +=  "CONSIGNEEDTLS.ADM_CITY,";
            strSQL +=  "CONSIGNEEDTLS.ADM_ZIP_CODE,";
            strSQL +=  "CONSIGNEEDTLS.ADM_EMAIL_ID,";
            strSQL +=  "CONSIGNEEDTLS.ADM_PHONE_NO_1,";
            strSQL +=  "CONSIGNEEDTLS.ADM_FAX_NO,";
            strSQL +=  "CONSIGCOUNTRY.COUNTRY_NAME,";
            strSQL +=  "POL.PORT_NAME,";
            strSQL +=  "POD.PORT_NAME,";
            strSQL +=  "PLD.PLACE_NAME,";
            strSQL +=  "JSI.CARGO_TYPE,";
            strSQL +=  "DBAMST.AGENT_MST_PK,";
            strSQL +=  "DBAMST.AGENT_NAME,";
            strSQL +=  "DBADTLS.ADM_ADDRESS_1,";
            strSQL +=  "DBADTLS.ADM_ADDRESS_2,";
            strSQL +=  "DBADTLS.ADM_ADDRESS_3,";
            strSQL +=  "DBADTLS.ADM_CITY,";
            strSQL +=  "DBADTLS.ADM_ZIP_CODE,";
            strSQL +=  "DBADTLS.ADM_EMAIL_ID,";
            strSQL +=  "DBADTLS.ADM_PHONE_NO_1,";
            strSQL +=  "DBADTLS.ADM_FAX_NO,";
            strSQL +=  "DBCOUNTRY.COUNTRY_NAME,";
            strSQL +=  "STMST.INCO_CODE,";
            strSQL +=  "NVL(JSI.INSURANCE_AMT, 0),";
            strSQL +=  "JSI.PYMT_TYPE,";
            strSQL +=  "CGMST.commodity_group_desc,JSI.ETA_DATE";
            try
            {
                return (objWF.GetDataSet(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataSet FetchSeaImpContainers(string JobRefPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT JSI.JOB_CARD_TRN_FK JOBPK ,JSI.CONTAINER_NUMBER CONTAINER FROM JOB_TRN_CONT JSI ";
            Strsql +=  "WHERE JSI.JOB_CARD_TRN_FK IN(" + JobRefPK + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Import Header Details"

        #region "fetch Address Details"

        public DataSet FetchAddressDetails(int LocPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "select CorMst.Corporate_Name,LocMst.address_line1,";
            strSQL +=  "LocMst.address_line2,";
            strSQL +=  "LocMst.address_line3, ";
            strSQL +=  "LocMst.zip, ";
            strSQL +=  "LocMst.city,";
            strSQL +=  "CountryMst.Country_Name,";
            strSQL +=  "LocMst.tele_phone_no TeleNo";
            strSQL +=  "from Location_mst_tbl LocMst,";
            strSQL +=  "country_mst_tbl CountryMst,corporate_mst_tbl CorMst";
            strSQL +=  "where LocMst.location_mst_pk = " + LocPK;
            strSQL +=  "AND CountryMst.Country_Mst_Pk=LocMst.country_mst_fk";
            try
            {
                return (objWF.GetDataSet(strSQL));
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

        #endregion "fetch Address Details"

        #region "Fetch Export Acknowledgement Details"

        public DataSet FetchSeaAcknowledgement(string JOBPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL = "SELECT JSE.JOB_CARD_TRN_PK JOBPK,";
                strSQL +=  "JSE.JOBCARD_REF_NO JOBREFNO,";
                strSQL +=  "BST.BOOKING_MST_PK BKGPK,";
                strSQL +=  "BST.BOOKING_REF_NO BKGREFNO,";
                strSQL +=  "BST.BOOKING_DATE BKGDATE,";
                strSQL +=  "(CASE WHEN JSE.VOYAGE_FLIGHT_NO IS NOT NULL THEN";
                strSQL +=  "JSE.VESSEL_NAME ||'-' || JSE.VOYAGE_FLIGHT_NO";
                strSQL +=  " ELSE";
                strSQL +=  " JSE.VESSEL_NAME END ) VESFLIGHT,";
                strSQL +=  "  HBL.HBL_REF_NO HBLREFNO,";
                strSQL +=  "  MBL.MBL_REF_NO  MBLREFNO,";
                strSQL +=  "JSE.MARKS_NUMBERS MARKS,";
                strSQL +=  " JSE.GOODS_DESCRIPTION GOODS,";
                strSQL +=  " BST.CARGO_TYPE ,";
                strSQL +=  "JSE.UCR_NO UCRNO,";
                strSQL +=  "'' CLEARANCEPOINT,";
                strSQL +=  "TO_CHAR(JSE.ETD_DATE,'" + dateFormat + "') ETD,";
                strSQL +=  "JSE.SHIPPER_CUST_MST_FK,";
                strSQL +=  "CMST.CUSTOMER_NAME SHIPNAME,";
                strSQL +=  "BST.CUSTOMER_REF_NO SHIPREFNO,";
                strSQL +=  "CDTLS.ADM_ADDRESS_1 SHIPADD1,";
                strSQL +=  "CDTLS.ADM_ADDRESS_2 SHIPADD2,";
                strSQL +=  "CDTLS.ADM_ADDRESS_3 SHIPADD3,";
                strSQL +=  "CDTLS.ADM_CITY SHIPCITY,";
                strSQL +=  "CDTLS.ADM_ZIP_CODE SHIPZIP,";
                strSQL +=  "CDTLS.ADM_EMAIL_ID AS SHIPEMAIL,";
                strSQL +=  "CDTLS.ADM_PHONE_NO_1 SHIPPHONE,";
                strSQL +=  "CDTLS.ADM_FAX_NO SHIPFAX,";
                strSQL +=  "SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
                strSQL +=  "CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,";
                strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGADD1,";
                strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGADD2,";
                strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGADD3,";
                strSQL +=  "CONSIGNEEDTLS.ADM_CITY CONSIGCITY,";
                strSQL +=  "CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGZIP,";
                strSQL +=  "CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGEMAIL,";
                strSQL +=  "CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGPHONE,";
                strSQL +=  "CONSIGNEEDTLS.ADM_FAX_NO CONSIGFAX,";
                strSQL +=  " CONSIGCOUNTRY.COUNTRY_NAME CONSICOUNTRY,";
                strSQL +=  "POL.PORT_NAME POLNAME,";
                strSQL +=  "POD.PORT_NAME PODNAME,";
                strSQL +=  "PLD.PLACE_NAME DELNAME,";
                strSQL +=  "COLPLD.PLACE_NAME COLNAME,";
                strSQL +=  "DBAMST.AGENT_MST_PK DBAGENTPK,";
                strSQL +=  "DBAMST.AGENT_NAME  DBAGENTNAME,";
                strSQL +=  "DBADTLS.ADM_ADDRESS_1  DBAGENTADD1,";
                strSQL +=  "DBADTLS.ADM_ADDRESS_2  DBAGENTADD2,";
                strSQL +=  "DBADTLS.ADM_ADDRESS_3  DBAGENTADD3,";
                strSQL +=  "DBADTLS.ADM_CITY  DBAGENTCITY,";
                strSQL +=  "DBADTLS.ADM_ZIP_CODE DBAGENTZIP,";
                strSQL +=  "DBADTLS.ADM_EMAIL_ID DBAGENTEMAIL,";
                strSQL +=  "DBADTLS.ADM_PHONE_NO_1 DBAGENTPHONE,";
                strSQL +=  "DBADTLS.ADM_FAX_NO DBAGENTFAX,";
                strSQL +=  "DBCOUNTRY.COUNTRY_NAME DBCOUNTRY,";
                strSQL +=  "STMST.INCO_CODE TERMS,";
                strSQL +=  "NVL(JSE.INSURANCE_AMT,0) INSURANCE,";
                strSQL +=  "JSE.PYMT_TYPE ,";
                strSQL +=  "CGMST.commodity_group_desc COMMCODE,";
                strSQL +=  "TO_CHAR(JSE.ETA_DATE,'" + dateFormat + "') ETA,";
                strSQL +=  "SUM(JTSEC.GROSS_WEIGHT) GROSS,";
                strSQL +=  "SUM(JTSEC.CHARGEABLE_WEIGHT) CHARWT,";
                strSQL +=  "SUM(JTSEC.NET_WEIGHT) NETWT,";
                strSQL +=  "SUM(JTSEC.VOLUME_IN_CBM) VOLUME";
                strSQL +=  "FROM   JOB_CARD_TRN JSE,";
                strSQL +=  "JOB_TRN_CONT JTSEC,";
                strSQL +=  "BOOKING_MST_TBL BST,";
                strSQL +=  "CUSTOMER_MST_TBL CMST,";
                strSQL +=  "CUSTOMER_MST_TBL CONSIGNEE,";
                strSQL +=  "CUSTOMER_CONTACT_DTLS CDTLS,";
                strSQL +=  "CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,";
                strSQL +=  "COUNTRY_MST_TBL SHIPCOUNTRY,";
                strSQL +=  "COUNTRY_MST_TBL CONSIGCOUNTRY,";
                strSQL +=  "PORT_MST_TBL POL,";
                strSQL +=  "PORT_MST_TBL POD,";
                strSQL +=  "PLACE_MST_TBL PLD,";
                strSQL +=  "PLACE_MST_TBL COLPLD,";
                strSQL +=  "AGENT_MST_TBL DBAMST,";
                strSQL +=  "AGENT_CONTACT_DTLS DBADTLS,";
                strSQL +=  "COUNTRY_MST_TBL DBCOUNTRY,";
                strSQL +=  "SHIPPING_TERMS_MST_TBL STMST,";
                strSQL +=  " COMMODITY_GROUP_MST_TBL CGMST,";
                strSQL +=  "HBL_EXP_TBL HBL,";
                strSQL +=  "MBL_EXP_TBL MBL";

                strSQL +=  "WHERE JSE.JOB_CARD_TRN_PK IN( " + JOBPK + " )";
                strSQL +=  " AND JTSEC.JOB_CARD_TRN_FK(+)=JSE.JOB_CARD_TRN_PK";
                strSQL +=  "AND JSE.HBL_HAWB_FK=HBL.HBL_EXP_TBL_PK(+)";
                strSQL +=  "AND JSE.MBL_MAWB_FK=MBL.MBL_EXP_TBL_PK(+)";
                strSQL +=  "AND   CMST.CUSTOMER_MST_PK(+)=JSE.SHIPPER_CUST_MST_FK";
                strSQL +=  "AND   CONSIGNEE.CUSTOMER_MST_PK(+)=JSE.CONSIGNEE_CUST_MST_FK";
                strSQL +=  "AND   CDTLS.CUSTOMER_MST_FK(+)=CMST.CUSTOMER_MST_PK";
                strSQL +=  "AND CONSIGNEE.CUSTOMER_MST_PK=CONSIGNEEDTLS.CUSTOMER_MST_FK(+)";
                strSQL +=  "AND SHIPCOUNTRY.COUNTRY_MST_PK(+)=CDTLS.ADM_COUNTRY_MST_FK";
                strSQL +=  "AND CONSIGCOUNTRY.COUNTRY_MST_PK(+)=CONSIGNEEDTLS.ADM_COUNTRY_MST_FK";
                strSQL +=  "AND   JSE.BOOKING_MST_FK=BST.BOOKING_MST_PK(+)";
                strSQL +=  "AND   BST.PORT_MST_POL_FK=POL.PORT_MST_PK(+)";
                strSQL +=  "AND   BST.PORT_MST_POD_FK=POD.PORT_MST_PK(+)";
                strSQL +=  "AND   BST.DEL_PLACE_MST_FK=PLD.PLACE_PK(+)";
                strSQL +=  "AND   BST.COL_PLACE_MST_FK=COLPLD.PLACE_PK(+)";
                strSQL +=  "AND   JSE.DP_AGENT_MST_FK=DBAMST.AGENT_MST_PK(+)";
                strSQL +=  "AND   DBAMST.AGENT_MST_PK=DBADTLS.AGENT_MST_FK(+)";
                strSQL +=  "AND DBCOUNTRY.COUNTRY_MST_PK(+)=DBADTLS.ADM_COUNTRY_MST_FK";
                strSQL +=  "AND  STMST.SHIPPING_TERMS_MST_PK(+)=JSE.SHIPPING_TERMS_MST_FK";
                strSQL +=  "AND  JSE.COMMODITY_GROUP_FK=CGMST.COMMODITY_GROUP_PK(+)";
                strSQL +=  "GROUP BY";
                strSQL +=  "JSE.JOB_CARD_TRN_PK ,";
                strSQL +=  "JSE.JOBCARD_REF_NO ,";
                strSQL +=  "BST.BOOKING_MST_PK ,";
                strSQL +=  "BST.BOOKING_REF_NO ,";
                strSQL +=  "BST.BOOKING_DATE ,";
                strSQL +=  "(CASE WHEN JSE.VOYAGE_FLIGHT_NO IS NOT NULL THEN";
                strSQL +=  "JSE.VESSEL_NAME ||'-' || JSE.VOYAGE_FLIGHT_NO";
                strSQL +=  " ELSE";
                strSQL +=  " JSE.VESSEL_NAME END ),";
                strSQL +=  " HBL.HBL_REF_NO ,";
                strSQL +=  "MBL.MBL_REF_NO ,";

                strSQL +=  "JSE.MARKS_NUMBERS,";
                strSQL +=  "JSE.GOODS_DESCRIPTION,";
                strSQL +=  "JSE.UCR_NO ,";
                strSQL +=  "BST.CARGO_TYPE,";
                strSQL +=  "JSE.ETD_DATE ,";

                strSQL +=  "JSE.SHIPPER_CUST_MST_FK,";
                strSQL +=  "CMST.CUSTOMER_NAME ,";
                strSQL +=  "BST.CUSTOMER_REF_NO ,";
                strSQL +=  "CDTLS.ADM_ADDRESS_1,";
                strSQL +=  "CDTLS.ADM_ADDRESS_2 ,";
                strSQL +=  "CDTLS.ADM_ADDRESS_3 ,";
                strSQL +=  "CDTLS.ADM_CITY ,";
                strSQL +=  "CDTLS.ADM_ZIP_CODE,";
                strSQL +=  "CDTLS.ADM_EMAIL_ID,";
                strSQL +=  "CDTLS.ADM_PHONE_NO_1 ,";
                strSQL +=  "CDTLS.ADM_FAX_NO ,";
                strSQL +=  "SHIPCOUNTRY.COUNTRY_NAME ,";
                strSQL +=  "CONSIGNEE.CUSTOMER_NAME ,";
                strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_1 ,";
                strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_2,";
                strSQL +=  "CONSIGNEEDTLS.ADM_ADDRESS_3 ,";
                strSQL +=  "CONSIGNEEDTLS.ADM_CITY,";
                strSQL +=  "CONSIGNEEDTLS.ADM_ZIP_CODE,";
                strSQL +=  "CONSIGNEEDTLS.ADM_EMAIL_ID,";
                strSQL +=  "CONSIGNEEDTLS.ADM_PHONE_NO_1,";
                strSQL +=  "CONSIGNEEDTLS.ADM_FAX_NO ,";
                strSQL +=  "CONSIGCOUNTRY.COUNTRY_NAME ,";
                strSQL +=  "POL.PORT_NAME ,";
                strSQL +=  "POD.PORT_NAME ,";
                strSQL +=  "PLD.PLACE_NAME,";
                strSQL +=  "COLPLD.PLACE_NAME,";
                strSQL +=  "DBAMST.AGENT_MST_PK ,";
                strSQL +=  "DBAMST.AGENT_NAME ,";
                strSQL +=  "DBADTLS.ADM_ADDRESS_1,";
                strSQL +=  "DBADTLS.ADM_ADDRESS_2  ,";
                strSQL +=  "DBADTLS.ADM_ADDRESS_3 ,";
                strSQL +=  "DBADTLS.ADM_CITY ,";
                strSQL +=  "DBADTLS.ADM_ZIP_CODE ,";
                strSQL +=  "DBADTLS.ADM_EMAIL_ID,";
                strSQL +=  "DBADTLS.ADM_PHONE_NO_1 ,";
                strSQL +=  "DBADTLS.ADM_FAX_NO,";
                strSQL +=  "DBCOUNTRY.COUNTRY_NAME,";
                strSQL +=  "STMST.INCO_CODE,";
                strSQL +=  " NVL(JSE.INSURANCE_AMT,0),";
                strSQL +=  " JSE.PYMT_TYPE ,";
                strSQL +=  " CGMST.commodity_group_desc,JSE.ETA_DATE ";

                return (objWF.GetDataSet(strSQL));
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

        public DataSet FetchSeaContainers(string JobRefPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT JSE.JOB_CARD_TRN_FK JOBPK ,JSE.CONTAINER_NUMBER CONTAINER FROM JOB_TRN_CONT JSE ";
            Strsql +=  "WHERE JSE.JOB_CARD_TRN_FK IN(" + JobRefPK + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
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

        public DataTable FetchBookingPk(string JobRefPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "  select bkg.booking_sea_pk , bkg.status ";
            Strsql += " from booking_sea_tbl bkg,job_card_sea_exp_tbl jc ";
            Strsql += " where bkg.booking_sea_pk = jc.booking_sea_fk ";
            Strsql += " and jc.job_card_sea_exp_pk = " + JobRefPK + " ";
            try
            {
                return Objwk.GetDataTable(Strsql);
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

        #endregion "Fetch Export Acknowledgement Details"

        #region "Movement Loading List Details"

        public DataSet FetchMovementListing(int JOBPK)
        {
            string Strsql = null;
            WorkFlow ObjWF = new WorkFlow();
            Strsql = "SELECT JSE.JOB_CARD_TRN_PK JOBPK," +  "JSE.JOBCARD_REF_NO JOBREFNO," +  "(CASE WHEN JSE.VOYAGE_FLIGHT_NO IS NOT NULL THEN" +  "JSE.VESSEL_NAME || '/' || JSE.VOYAGE_FLIGHT_NO" +  "ELSE" +  "JSE.VESSEL_NAME END) VES_FLIGHT," +  "TO_CHAR(JSE.ETA_DATE,dateFormat)ETA," +  "TO_CHAR(JSE.ETD_DATE,dateFormat) ETD," +  "DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL') CONTAINER," +  "AMST.AGENT_NAME AGENTNAME," +  "ADTLS.ADM_ADDRESS_1 DPADDRESS1," +  "ADTLS.ADM_ADDRESS_2 DPADDRESS2," +  "ADTLS.ADM_ADDRESS_3 DPADDRESS3," +  "ADTLS.ADM_CITY DPCITY," +  "ADTLS.ADM_ZIP_CODE DPZIP," +  "ADTLS.ADM_PHONE_NO_1 DPPHONE," +  "ADTLS.ADM_FAX_NO DPFAX," +  "ADTLS.ADM_EMAIL_ID DPEMAIL," +  "ADCOUNTRY.COUNTRY_NAME DPCOUNTRY," +  "POL.PORT_NAME POLNAME," +  "POD.PORT_NAME PODNAME," +  "SHIPPER.CUSTOMER_NAME SHIPPER," +  "JSE.MARKS_NUMBERS MARKS," +  "JSE.GOODS_DESCRIPTION GOODS," +  "SUM(JTSEC.PACK_COUNT) PACKCOUNT," +  "SUM(JTSEC.VOLUME_IN_CBM) VOLUME," +  "SUM(JTSEC.GROSS_WEIGHT) GROSSWT," +  "SUM(JTSEC.NET_WEIGHT) NETWT," +  "SUM(JTSEC.COMMODITY_MST_FK) CHRWT" +  "FROM JOB_CARD_TRN JSE," +  "JOB_TRN_CONT JTSEC," +  "CUSTOMER_MST_TBL  SHIPPER," +  "BOOKING_MST_TBL      BST," +  "PORT_MST_TBL         POL," +  "PORT_MST_TBL         POD," +  "AGENT_MST_TBL        AMST," +  "AGENT_CONTACT_DTLS   ADTLS," +  "COUNTRY_MST_TBL ADCOUNTRY" +  "WHERE JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)" +  "AND JTSEC.JOB_CARD_TRN_FK(+) = JSE.JOB_CARD_TRN_PK" +  "AND JSE.BOOKING_MST_FK = BST.BOOKING_MST_PK" +  "AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK" +  "AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK" +  "AND JSE.DP_AGENT_MST_FK = AMST.AGENT_MST_PK(+)" +  "AND AMST.AGENT_MST_PK = ADTLS.AGENT_MST_FK(+)" +  "AND ADCOUNTRY.COUNTRY_MST_PK(+) = ADTLS.ADM_COUNTRY_MST_FK" +  "AND JSE.JOB_CARD_TRN_PK =" + JOBPK +  "GROUP BY" +  "JSE.JOB_CARD_TRN_PK ," +  "JSE.JOBCARD_REF_NO," +  "(CASE WHEN JSE.VOYAGE_FLIGHT_NO IS NOT NULL THEN" +  " JSE.VESSEL_NAME || '/' || JSE.VOYAGE_FLIGHT_NO " +  "ELSE" +  "JSE.VESSEL_NAME END)," +  "JSE.ETD_DATE," +  "DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL') ," +  "AMST.AGENT_NAME ," +  "ADTLS.ADM_ADDRESS_1," +  "ADTLS.ADM_ADDRESS_2 ," +  "ADTLS.ADM_ADDRESS_3 ," +  "ADTLS.ADM_CITY ," +  "ADTLS.ADM_ZIP_CODE," +  "ADTLS.ADM_PHONE_NO_1 ," +  "ADTLS.ADM_FAX_NO ," +  "ADTLS.ADM_EMAIL_ID ," +  "ADCOUNTRY.COUNTRY_NAME," +  "POL.PORT_NAME," +  "POD.PORT_NAME ," +  "SHIPPER.CUSTOMER_NAME ," +  "JSE.MARKS_NUMBERS," +  "JSE.GOODS_DESCRIPTION,JSE.ETA_DATE";
            try
            {
                return (ObjWF.GetDataSet(Strsql));
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

        #endregion "Movement Loading List Details"

        #region "Fetch Job Card For Listing Screen as per the new Requirement"

        public DataSet fn_FetchListingNew(string jobrefNO = "", string bookingNo = "", string HblNo = "", string polID = "", string podId = "", string polName = "", string podName = "", string jcStatus = "", string shipper = "", string consignee = "",
        string agent = "", string process = "", string cargoType = "", double SearchFor = 0, Int32 SearchFortime = 0, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string CurrBizType = "3",
        bool BOOKING = false, string Mbl = "", long lngUsrLocFk = 0, string containerno = "", int jctype = 0, Int32 flag = 0, string hdnPlrpk = "", string hdnPfdpk = "", string hdnSLpk = "", string hdnVslpk = "",
        string UcrNr = "", string Commpk = "0", bool flgXBkg = false, bool flgCL = false, string VesselName = "", string PONumber = "", bool IsNominated = false, int SalesExecMstFk = 0, int OtherStatus = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            StringBuilder buildQuery = new StringBuilder();
            StringBuilder buildCondition = new StringBuilder();
            if (BOOKING == false)
            {
                //2exp 1-imp
                if (process == "2")
                {
                    buildCondition.Append( "     BOOKING_SEA_TBL BK, JOB_CARD_SEA_EXP_TBL JC,");
                    buildCondition.Append( "     HBL_EXP_TBL HBL, ");
                    buildCondition.Append( "     MBL_EXP_TBL MBL, ");
                    buildCondition.Append( "     JOB_TRN_SEA_EXP_CONT cont, ");
                }
                else
                {
                    buildCondition.Append( "     BOOKING_SEA_TBL BK, JOB_CARD_SEA_EXP_TBL JSE,");
                    buildCondition.Append( "     JOB_CARD_SEA_IMP_TBL JC,");
                    buildCondition.Append( "     JOB_TRN_SEA_IMP_CONT cont, ");
                }

                buildCondition.Append( "     CUSTOMER_MST_TBL SH,");

                if (process == "2")
                {
                    buildCondition.Append( "     CUSTOMER_MST_TBL CO,");
                }

                buildCondition.Append( "     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
                if (process == "2")
                {
                    buildCondition.Append( "     AGENT_MST_TBL DPA, ");
                }
                else
                {
                    buildCondition.Append( "     AGENT_MST_TBL POLA, ");
                }

                buildCondition.Append( "     EMPLOYEE_MST_TBL EMP,");
                if (process == "2")
                {
                    buildCondition.Append( "     EMPLOYEE_MST_TBL DEF_EXEC,");
                }
                buildCondition.Append( "     OPERATOR_MST_TBL     OMT, ");
                buildCondition.Append( "     VESSEL_VOYAGE_TRN    VVT, ");
                buildCondition.Append( "    VESSEL_VOYAGE_TBL    VST, ");

                buildCondition.Append( "     USER_MST_TBL UMT ");
                buildCondition.Append( "      where ");
                //exp
                if (process == "2")
                {
                    buildCondition.Append( "   BK.BOOKING_SEA_PK = JC.BOOKING_SEA_FK (+)");
                    buildCondition.Append( "   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK");
                    buildCondition.Append( "   AND cont.job_card_sea_exp_fk=jc.job_card_sea_exp_pk");
                    //added manivannan
                    buildCondition.Append( "   AND JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                    buildCondition.Append( "   AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                    buildCondition.Append( "   AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                    buildCondition.Append( "    AND  JC.DP_AGENT_MST_FK= DPA.AGENT_MST_PK(+) ");
                    buildCondition.Append( "   AND JC.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK (+)");
                    buildCondition.Append( "   AND JC.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK (+)");
                    ///
                    buildCondition.Append( "    AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+) ");
                    buildCondition.Append( "   AND JC.VOYAGE_TRN_FK=VVT.VOYAGE_TRN_PK(+)");
                    buildCondition.Append( "    AND OMT.OPERATOR_MST_PK(+) = BK.OPERATOR_MST_FK");
                    ///
                    buildCondition.Append( " AND (UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
                    buildCondition.Append( "        OR POL.LOCATION_MST_FK = " + lngUsrLocFk + ") ");

                    buildCondition.Append( " AND JC.CREATED_BY_FK = UMT.USER_MST_PK ");
                    buildCondition.Append( "   AND SH.REP_EMP_MST_FK = DEF_EXEC.EMPLOYEE_MST_PK(+)");
                }
                else
                {
                    // buildCondition.Append(vbCrLf & "   JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)")

                    //buildCondition.Append(vbCrLf & "   AND JC.JOBCARD_REF_NO=JSE.JOBCARD_REF_NO(+)")
                    buildCondition.Append( "    JC.JOBCARD_REF_NO=JSE.JOBCARD_REF_NO(+)");
                    buildCondition.Append( "   AND JSE.BOOKING_SEA_FK=BK.BOOKING_SEA_PK(+)");

                    buildCondition.Append( "   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+) ");
                    buildCondition.Append( "   AND jc.job_card_sea_imp_pk = cont.job_card_sea_imp_fk (+)");
                    //added manivannan
                    buildCondition.Append( "   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                    buildCondition.Append( "   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                    buildCondition.Append( "   AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+) ");
                    ///
                    buildCondition.Append( "    AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+) ");
                    buildCondition.Append( "   AND JC.VOYAGE_TRN_FK=VVT.VOYAGE_TRN_PK(+)");
                    buildCondition.Append( "    AND OMT.OPERATOR_MST_PK(+) = JC.OPERATOR_MST_FK");
                    ///
                    buildCondition.Append( " AND ((UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " and JC.JC_AUTO_MANUAL = 0) ");
                    buildCondition.Append( "  OR (JC.PORT_MST_POD_FK ");
                    buildCondition.Append( " IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " + lngUsrLocFk + ")  and JC.JC_AUTO_MANUAL = 1)) ");
                    buildCondition.Append( " AND jc.CREATED_BY_FK = UMT.USER_MST_PK ");
                    //Added by Faheem
                    buildCondition.Append( " AND JC.CONSOLE = 1 ");
                    //End
                    if (!string.IsNullOrEmpty(PONumber))
                    {
                        buildCondition.Append( " AND UPPER(JC.PO_NUMBER) LIKE '%" + PONumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    if (jctype != 2)
                    {
                        buildCondition.Append( "AND  JC.JC_AUTO_MANUAL=" + jctype);
                    }
                    //buildCondition.Append(vbCrLf & "   AND CO.REP_EMP_MST_FK = DEF_EXEC.EMPLOYEE_MST_PK(+)")
                }
                buildCondition.Append( "   AND JC.EXECUTIVE_MST_FK = EMP.EMPLOYEE_MST_PK(+)");

                if (jobrefNO.Length > 0)
                {
                    buildCondition.Append( " AND UPPER(JC.JOBCARD_REF_NO) LIKE '%" + jobrefNO.ToUpper().Replace("'", "''") + "%'");
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
                    buildCondition.Append( " AND JOBCARD_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
                }
                if (process == "2")
                {
                    if (bookingNo.Length > 0)
                    {
                        buildCondition.Append( " AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                        buildCondition.Append( " AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
                    }
                    buildCondition.Append( " AND BK.STATUS in (2,5,6, DECODE(JC.WIN_XML_GEN, 1, 3, -1)) ");
                }
                if (jcStatus.Length > 0)
                {
                    buildCondition.Append( " AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS =" + jcStatus + ")");
                }
                if (HblNo.Trim().Length > 0)
                {
                    buildCondition.Append( " AND UPPER(HBL_REF_NO) LIKE '%" + HblNo.ToUpper().Replace("'", "''") + "%'");
                }
                if (polID.Length > 0)
                {
                    buildCondition.Append( "       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
                }
                if (polName.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
                }
                if (podId.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
                }
                if (podName.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
                }
                //'
                if (VesselName.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(VST.VESSEL_NAME) = '" + VesselName.ToUpper().Replace("'", "''") + "' ");
                }
                //For export
                if (Convert.ToInt32(process) == 2)
                {
                    if (!string.IsNullOrEmpty(hdnPlrpk))
                    {
                        buildCondition.Append( " AND BK.COL_PLACE_MST_FK = " + hdnPlrpk);
                    }
                    if (!string.IsNullOrEmpty(hdnPfdpk))
                    {
                        buildCondition.Append( "  AND BK.DEL_PLACE_MST_FK = " + hdnPfdpk);
                    }
                    if (!string.IsNullOrEmpty(hdnSLpk))
                    {
                        buildCondition.Append( "  AND BK.OPERATOR_MST_FK = " + hdnSLpk);
                    }
                    //For import
                }
                else
                {
                    if (!string.IsNullOrEmpty(hdnPfdpk))
                    {
                        buildCondition.Append( "  AND JC.DEL_PLACE_MST_FK = " + hdnPfdpk);
                    }
                    if (!string.IsNullOrEmpty(hdnSLpk))
                    {
                        buildCondition.Append( "  AND JC.OPERATOR_MST_FK = " + hdnSLpk);
                    }
                }
                if (!string.IsNullOrEmpty(hdnVslpk))
                {
                    buildCondition.Append( "  AND JC.VOYAGE_TRN_FK = " + hdnVslpk);
                }
                if (!string.IsNullOrEmpty(UcrNr))
                {
                    buildCondition.Append( "  AND JC.UCR_NO = '" + UcrNr + "'");
                }
                if (Convert.ToInt32(Commpk) != 0)
                {
                    buildCondition.Append( "  AND JC.COMMODITY_GROUP_FK = " + Commpk);
                }
                if (flgXBkg)
                {
                    buildCondition.Append( "  AND JC.CB_AGENT_MST_FK IS NOT NULL");
                }
                if (flgCL)
                {
                    buildCondition.Append( "  AND JC.CL_AGENT_MST_FK IS NOT NULL");
                }
                if (containerno.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(CONT.CONTAINER_NUMBER) LIKE '" + containerno.ToUpper().Replace("'", "''") + "' ");
                }
                ///Modified By Koteshwari on 2/3/2011
                //If cargoType.Length > 0 Then
                if (Convert.ToInt32(cargoType) > 0)
                {
                    if (process == "2")
                    {
                        buildCondition.Append( "   AND BK.CARGO_TYPE = " + cargoType);
                    }
                    else
                    {
                        buildCondition.Append( "  AND JC.CARGO_TYPE = " + cargoType);
                    }
                }
                if (agent.Length > 0)
                {
                    if (process == "2")
                    {
                        //buildCondition.Append(vbCrLf & " AND  UPPER(DPA.AGENT_NAME) = '" & agent.ToUpper.Replace("'", "''") & "'")
                        buildCondition.Append( "  AND DPA.AGENT_MST_PK = " + agent);
                    }
                    else
                    {
                        //buildCondition.Append(vbCrLf & " AND  UPPER(POLA.AGENT_NAME) = '" & agent.ToUpper.Replace("'", "''") & "'")
                        buildCondition.Append( "  AND POLA.AGENT_MST_PK = " + agent);
                    }
                }
                if (shipper.Length > 0)
                {
                    // buildCondition.Append(vbCrLf & " AND UPPER(SH.CUSTOMER_NAME) LIKE '" & shipper.ToUpper.Replace("'", "''") & "'")
                    buildCondition.Append( "  AND SH.CUSTOMER_MST_PK = " + shipper);
                }
                if (consignee.Length > 0)
                {
                    //buildCondition.Append(vbCrLf & " AND UPPER(CO.CUSTOMER_NAME) LIKE '" & consignee.ToUpper.Replace("'", "''") & "'")
                    buildCondition.Append( "  AND CO.CUSTOMER_MST_PK = " + consignee);
                }
                if (Mbl.Trim().Length > 0)
                {
                    buildCondition.Append( " AND UPPER(MBL_REF_NO) LIKE '%" + Mbl.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else
            {
                buildCondition.Append( "     BOOKING_SEA_TBL  BK,  ");
                buildCondition.Append( "     CUSTOMER_MST_TBL SH,");
                buildCondition.Append( "     CUSTOMER_MST_TBL CO,");
                buildCondition.Append( "     PORT_MST_TBL POL,PORT_MST_TBL POD, ");
                buildCondition.Append( "     AGENT_MST_TBL CBA, ");
                buildCondition.Append( "     AGENT_MST_TBL CLA, ");
                //'
                buildCondition.Append( "     OPERATOR_MST_TBL     OMT, ");
                buildCondition.Append( "      VESSEL_VOYAGE_TRN    VVT, ");
                buildCondition.Append( "    VESSEL_VOYAGE_TBL    VST, ");
                //'
                buildCondition.Append( "     USER_MST_TBL UMT ");
                buildCondition.Append( "      where ");
                buildCondition.Append( "   BK.CONS_CUSTOMER_MST_FK = CO.CUSTOMER_MST_PK(+)");
                buildCondition.Append( "   AND BK.CUST_CUSTOMER_MST_FK = SH.CUSTOMER_MST_PK");
                buildCondition.Append( "   AND BK.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                buildCondition.Append( "   AND BK.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                buildCondition.Append( "   AND BK.CB_AGENT_MST_FK = CBA.AGENT_MST_PK (+)");
                buildCondition.Append( "   AND BK.CL_AGENT_MST_FK = CLA.AGENT_MST_PK (+) ");
                ///
                buildCondition.Append( "    AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+) ");
                buildCondition.Append( "   AND JC.VOYAGE_TRN_FK=VVT.VOYAGE_TRN_PK(+)");
                buildCondition.Append( "    AND OMT.OPERATOR_MST_PK(+) = JC.OPERATOR_MST_FK");
                ///
                buildCondition.Append( " AND BK.BOOKING_SEA_PK NOT IN(SELECT JC.BOOKING_SEA_FK FROM JOB_CARD_SEA_EXP_TBL JC) ");
                buildCondition.Append( " AND UMT.DEFAULT_LOCATION_FK = " + lngUsrLocFk + " ");
                buildCondition.Append( " AND BK.CREATED_BY_FK = UMT.USER_MST_PK ");
                if (bookingNo.Length > 0)
                {
                    buildCondition.Append( " AND UPPER(BOOKING_REF_NO) LIKE '%" + bookingNo.ToUpper().Replace("'", "''") + "%'");
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
                    buildCondition.Append( " AND BOOKING_DATE BETWEEN TO_DATE('" + Time + "','" + dateFormat + "') AND TO_DATE('" + DateTime.Today + "','" + dateFormat + "')");
                }
                buildCondition.Append( " AND BK.STATUS in (2,5,6) ");
                if (polID.Length > 0)
                {
                    buildCondition.Append( "       AND UPPER(POL.PORT_ID) = '" + polID.ToUpper().Replace("'", "''") + "'");
                }
                if (polName.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POL.PORT_NAME) = '" + polName.ToUpper().Replace("'", "''") + "' ");
                }
                if (podId.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POD.PORT_ID) LIKE '" + podId.ToUpper().Replace("'", "''") + "'");
                }
                if (podName.Length > 0)
                {
                    buildCondition.Append( "     AND UPPER(POD.PORT_NAME) LIKE '" + podName.ToUpper().Replace("'", "''") + "' ");
                }
                if (hdnPlrpk != "0")
                {
                    buildCondition.Append( " AND BK.COL_PLACE_MST_FK = " + hdnPlrpk);
                }
                if (hdnPfdpk != "0")
                {
                    buildCondition.Append( "  AND BK.DEL_PLACE_MST_FK = " + hdnPfdpk);
                }
                if (hdnSLpk != "0")
                {
                    buildCondition.Append( "  AND BK.OPERATOR_MST_FK = " + hdnSLpk);
                }
                if (hdnVslpk != "0")
                {
                    buildCondition.Append( "  AND JC.VOYAGE_TRN_FK = " + hdnVslpk);
                }
                if (UcrNr != "0")
                {
                    buildCondition.Append( "  AND JC.UCR_NO = '" + UcrNr + "'");
                }
                if (Convert.ToInt32(Commpk) != 0)
                {
                    buildCondition.Append( "  AND JC.COMMODITY_GROUP_FK = " + Commpk);
                }
                if (flgXBkg)
                {
                    buildCondition.Append( "  AND JC.CB_AGENT_MST_FK IS NOT NULL");
                }
                if (flgCL)
                {
                    buildCondition.Append( "  AND JC.CL_AGENT_MST_FK IS NOT NULL");
                }
                ///Modified By Koteshwari on 2/3/2011
                //If cargoType.Length > 0 Then
                if (Convert.ToInt32(cargoType) > 0)
                {
                    buildCondition.Append( "   AND BK.CARGO_TYPE = " + cargoType);
                }
                if (agent.Length > 0)
                {
                    if (process == "2")
                    {
                        //buildCondition.Append(vbCrLf & " AND  UPPER(DPA.AGENT_NAME) = '" & agent.ToUpper.Replace("'", "''") & "'")
                        buildCondition.Append( "  AND DPA.AGENT_MST_PK = " + agent);
                    }
                    else
                    {
                        //buildCondition.Append(vbCrLf & " AND  UPPER(POLA.AGENT_NAME) = '" & agent.ToUpper.Replace("'", "''") & "'")
                        buildCondition.Append( "  AND POLA.AGENT_MST_PK = " + agent);
                    }
                }
                if (shipper.Length > 0)
                {
                    //buildCondition.Append(vbCrLf & " AND UPPER(SH.CUSTOMER_NAME) LIKE '" & shipper.ToUpper.Replace("'", "''") & "'")
                    buildCondition.Append( "  AND SH.CUSTOMER_MST_PK = " + shipper);
                }
                if (consignee.Length > 0)
                {
                    //buildCondition.Append(vbCrLf & " AND UPPER(CO.CUSTOMER_NAME) LIKE '" & consignee.ToUpper.Replace("'", "''") & "'")
                    buildCondition.Append( "  AND CO.CUSTOMER_MST_PK = " + consignee);
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
                buildCondition.Append( "   AND JC.CHK_NOMINATED = 1");
            }
            if (SalesExecMstFk > 0)
            {
                buildCondition.Append( "   AND (JC.EXECUTIVE_MST_FK = " + SalesExecMstFk);
                buildCondition.Append( "    OR DEF_EXEC.EMPLOYEE_MST_PK=" + SalesExecMstFk + ")");
            }

            if (OtherStatus == 1)
            {
                buildCondition.Append( "   AND jc.win_xml_gen = 1");
                //'Generated
            }
            else if (OtherStatus == 2)
            {
                buildCondition.Append( "   AND jc.win_xml_gen = 0");
                //'Not Generate
            }
            else if (OtherStatus == 3)
            {
                buildCondition.Append( "   AND jc.win_xml_status = 1");
                //'Active
            }
            else if (OtherStatus == 4)
            {
                buildCondition.Append( "   AND jc.win_xml_status = 2");
                //'Completed
            }
            else if (OtherStatus == 5)
            {
                buildCondition.Append( "   AND jc.win_xml_status = 3");
                //'Cancelled
            }
            else if (OtherStatus == 6)
            {
                buildCondition.Append( "   AND jc.win_xml_status = 0");
                //'NA
            }
            else if (OtherStatus == 7)
            {
                buildCondition.Append( "   AND jc.win_ack_status = 0");
                //' NR
            }
            else if (OtherStatus == 8)
            {
                buildCondition.Append( "   AND jc.win_ack_status = 1");
                //' Ack not received
            }
            else if (OtherStatus == 9)
            {
                buildCondition.Append( "   AND jc.win_ack_status = 2");
                //' Success
            }
            else if (OtherStatus == 10)
            {
                buildCondition.Append( "   AND jc.win_ack_status = 3");
                //' Failure
            }
            else if (OtherStatus == 11)
            {
                buildCondition.Append( "   AND jc.win_ack_status = 4");
                //' Warning
            }

            if (flag == 0)
            {
                buildCondition.Append( " AND 1=2 ");
            }
            strCondition = buildCondition.ToString();

            buildQuery.Append( "     Select distinct " );
            if (process == "2")
            {
                buildQuery.Append( "       JC.JOB_CARD_SEA_EXP_PK JCPK, ");
                buildQuery.Append( "       JC.JOBCARD_REF_NO JCREFNR, ");
                buildQuery.Append( "       JC.JOBCARD_DATE JCDATE, ");
                ///'''
                buildQuery.Append( "       BK.BOOKING_SEA_PK BKGPK, ");
                buildQuery.Append( "       BK.BOOKING_REF_NO BKGREFNR, ");
                buildQuery.Append( "       SH.CUSTOMER_MST_PK, ");
                buildQuery.Append( "       SH.CUSTOMER_ID, ");
                buildQuery.Append( "       SH.CUSTOMER_NAME, ");
                ///
                buildQuery.Append( "        OMT.OPERATOR_ID SLID, ");
                buildQuery.Append( "        (CASE ");
                buildQuery.Append( "       WHEN (NVL(VST.VESSEL_NAME, '') || '/' || ");
                buildQuery.Append( "        NVL(VVT.VOYAGE, '') = '/') THEN ");
                buildQuery.Append( "        '' ");
                buildQuery.Append( "          ELSE ");
                buildQuery.Append( "         NVL(VST.VESSEL_NAME, '') || '/' || ");
                buildQuery.Append( "         NVL(VVT.VOYAGE, '') ");
                buildQuery.Append( "          END) AS VESVOYAGE, ");
                buildQuery.Append( "         POL.PORT_ID POL, ");
                buildQuery.Append( "        POD.PORT_ID POD, ");
                buildQuery.Append( "        VVT.POL_ETD ETD, ");
                buildQuery.Append( "        VVT.POD_ETA ETA, ");
                ///
                //buildQuery.Append(vbCrLf & "       HBL.HBL_REF_NO HBLREFNR, ")
                //buildQuery.Append(vbCrLf & "       MBL.MBL_REF_NO MBLREFNR, ")
                //'Added By Koteshwari on 2/3/2011
                buildQuery.Append( "       DECODE(BK.CARGO_TYPE, '1','FCL','2','LCL','4','BBC' ) CARGO_TYPE,");
                //'End
                buildQuery.Append( "       HBL.HBL_REF_NO HBLREFNR, ");
                buildQuery.Append( "       MBL.MBL_REF_NO MBLREFNR, ");
                buildQuery.Append( "       NVL(EMP.EMPLOYEE_NAME,NVL(DEF_EXEC.EMPLOYEE_NAME,'CSR')) SALES_EXEC, ");
                buildQuery.Append( "       DECODE(JC.JOB_CARD_STATUS, '1','Open','2','Close') JCSTATUS,");
                //'
                buildQuery.Append( "       Decode(jc.win_xml_gen, 0, 'Not Gen.', 1,'Generated') XMLSTATUS, ");
                buildQuery.Append( "       Decode(jc.win_xml_status, 0, 'NA', 1, 'Active', 2, 'Completed', 3, 'Cancelled')  WINSTATUS, ");
                buildQuery.Append( "       Decode(jc.win_ack_status, 0, 'NR', 1, 'Pending for Ack', 2, 'Success', 3, 'Failure',4,'Warning') SUCFAIL, ");
                buildQuery.Append( "       '' SEL ");
            }
            else
            {
                buildQuery.Append( "       JC.JOB_CARD_SEA_IMP_PK JCPK, ");
                buildQuery.Append( "       JC.JOBCARD_REF_NO JCREFNR, ");
                buildQuery.Append( "       JC.JOBCARD_DATE JCDATE, ");
                ///'''
                buildQuery.Append( "       BK.BOOKING_SEA_PK BKGPK, ");
                buildQuery.Append( "       BK.BOOKING_REF_NO BKGREFNR, ");
                //buildQuery.Append(vbCrLf & "       CO.CUSTOMER_MST_PK, ")
                //buildQuery.Append(vbCrLf & "       CO.CUSTOMER_ID, ")
                //buildQuery.Append(vbCrLf & "       CO.CUSTOMER_NAME, ")
                //'Vasava:PTS WIN_007B if JC is created with Temp customer it is not listing
                buildQuery.Append( "  NVL((SELECT CMT.CUSTOMER_MST_PK  FROM CUSTOMER_MST_TBL CMT");
                buildQuery.Append( "  WHERE CMT.CUSTOMER_MST_PK =JC.CUST_CUSTOMER_MST_FK),");
                buildQuery.Append( "  (SELECT TMT.CUSTOMER_MST_PK FROM TEMP_CUSTOMER_TBL TMT ");
                buildQuery.Append( "  WHERE TMT.CUSTOMER_MST_PK = JC.CUST_CUSTOMER_MST_FK)) CUSTOMER_MST_PK,");
                buildQuery.Append( "  NVL((SELECT CMT.CUSTOMER_ID  FROM CUSTOMER_MST_TBL CMT");
                buildQuery.Append( "  WHERE CMT.CUSTOMER_MST_PK =JC.CUST_CUSTOMER_MST_FK),");
                buildQuery.Append( "  (SELECT TMT.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL TMT ");
                buildQuery.Append( "  WHERE TMT.CUSTOMER_MST_PK = JC.CUST_CUSTOMER_MST_FK)) CUSTOMER_ID,");
                buildQuery.Append( "  NVL((SELECT CMT.CUSTOMER_NAME  FROM CUSTOMER_MST_TBL CMT");
                buildQuery.Append( "  WHERE CMT.CUSTOMER_MST_PK =JC.CUST_CUSTOMER_MST_FK),");
                buildQuery.Append( "  (SELECT TMT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TMT ");
                buildQuery.Append( "  WHERE TMT.CUSTOMER_MST_PK = JC.CUST_CUSTOMER_MST_FK)) CUSTOMER_NAME,");
                ///
                buildQuery.Append( "        OMT.OPERATOR_ID SLID, ");
                buildQuery.Append( "        (CASE ");
                buildQuery.Append( "       WHEN (NVL(VST.VESSEL_NAME, '') || '/' || ");
                buildQuery.Append( "        NVL(VVT.VOYAGE, '') = '/') THEN ");
                buildQuery.Append( "        '' ");
                buildQuery.Append( "          ELSE ");
                buildQuery.Append( "         NVL(VST.VESSEL_NAME, '') || '/' || ");
                buildQuery.Append( "         NVL(VVT.VOYAGE, '') ");
                buildQuery.Append( "          END) AS VESVOYAGE, ");
                buildQuery.Append( "         POL.PORT_ID POL, ");
                buildQuery.Append( "        POD.PORT_ID POD, ");
                //'If Record is Saved from WIN Vessel Voyage is Optional
                buildQuery.Append( "        CASE WHEN JC.WIN_UNIQ_REF_ID IS NOT NULL THEN JC.ETD_DATE ELSE VVT.POL_ETD END ETD,");
                buildQuery.Append( "        CASE WHEN JC.WIN_UNIQ_REF_ID IS NOT NULL THEN JC.ETA_DATE ELSE VVT.POD_ETA END ETA,");
                //buildQuery.Append(vbCrLf & "        VVT.POL_ETD ETD, ")
                //buildQuery.Append(vbCrLf & "        VVT.POD_ETA ETA, ")
                ///
                //'Added By Koteshwari on 2/3/2011
                //'COMENTED BY SUBHRANSU buildQuery.Append(vbCrLf & "       ''CARGO_TYPE,")
                buildQuery.Append( "       DECODE(JC.CARGO_TYPE, '1','FCL','2','LCL','4','BBC' ) CARGO_TYPE,");
                //'End
                buildQuery.Append( "       JC.HBL_REF_NO HBLREFNR, ");
                buildQuery.Append( "       JC.MBL_REF_NO MBLREFNR, ");
                //buildQuery.Append(vbCrLf & "       NVL(EMP.EMPLOYEE_NAME,NVL(DEF_EXEC.EMPLOYEE_NAME,'CSR')) SALES_EXEC, ")
                buildQuery.Append( "       NVL(EMP.EMPLOYEE_NAME,(SELECT NVL(DEF_EXEC.EMPLOYEE_NAME, 'CSR') ");
                buildQuery.Append( "       FROM EMPLOYEE_MST_TBL DEF_EXEC,CUSTOMER_MST_TBL CMT ");
                buildQuery.Append( "       WHERE DEF_EXEC.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK ");
                buildQuery.Append( "       AND CMT.CUSTOMER_MST_PK = JC.CUST_CUSTOMER_MST_FK)) SALES_EXEC,");
                buildQuery.Append( "       DECODE(JC.JOB_CARD_STATUS, '1','Open','2','Close') JCSTATUS,");
                //' ''''
                buildQuery.Append( "       Decode(jc.win_xml_gen, 0, 'Not Gen.', 1,'Generated') XMLSTATUS, ");
                buildQuery.Append( "       Decode(jc.win_xml_status, 0, 'NA', 1, 'Active', 2, 'Completed', 3, 'Cancelled')  WINSTATUS, ");
                buildQuery.Append( "       Decode(jc.win_ack_status, 0, 'NR', 1, 'Pending for Ack', 2, 'Success', 3, 'Failure',4,'Warning') SUCFAIL, ");
                buildQuery.Append( "       '0' SEL ");
            }
            buildQuery.Append( "      from ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append( "     Order By " + SortColumn + SortType);

            StringBuilder strCount = new StringBuilder();
            strSQL = buildQuery.ToString();
            strCount.Append(" SELECT COUNT(*)  from  ");
            strCount.Append((" (" + buildQuery.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
            strCount.Remove(0, strCount.Length);

            StringBuilder sqlstr2 = new StringBuilder();
            sqlstr2.Append(" Select * from ");
            sqlstr2.Append( "  ( Select ROWNUM SR_NO, q.* from ");
            sqlstr2.Append("  (" + buildQuery.ToString() + " ");
            sqlstr2.Append("  ) q )  WHERE \"SR_NO\"  BETWEEN " + start + " AND " + last + "");
            //buildQuery.Append(vbCrLf & "   ) q ")
            //buildQuery.Append(vbCrLf & "  )   ")
            //buildQuery.Append(vbCrLf & "  where  ")
            //buildQuery.Append(vbCrLf & "     SR_NO between " & start & " and " & last)
            //strSQL = buildQuery.ToString
            strSQL = sqlstr2.ToString();
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

        #region "Getting Grid Details"

        public DataSet FetchGridDetails(string Doctype = "", string Cargotype = "", string bizType = "", string lblDocPK = "", string lblCarrierPK = "", string POLPK = "", string PODPK = "", string FromDate = "", string ToDate = "", Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 flag = 0, int Process = 0, int EDIStatus = 0, string VslName = "", string VoyageNr = "", Int32 ChkONLD = 0)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            string strCondition1 = null;
            string strCondition2 = null;
            string strCondition3 = null;
            string strCondition4 = null;

            if (ChkONLD == 0)
            {
                strCondition = strCondition + "  AND 1=2";
                strCondition1 = strCondition1 + "  AND 1=2";
                strCondition2 = strCondition2 + "  AND 1=2";
                strCondition3 = strCondition3 + "  AND 1=2";
            }
            //If flag = 0 Then
            //    strCondition = strCondition & "  AND 1=2"
            //    strCondition1 = strCondition1 & "  AND 1=2"
            //    strCondition2 = strCondition2 & "  AND 1=2"
            //    strCondition3 = strCondition3 & "  AND 1=2"
            //End If
            if (Convert.ToInt32(lblDocPK) > 0)
            {
                strCondition = strCondition + " AND BST.BOOKING_SEA_PK=" + lblDocPK;
                strCondition1 = strCondition1 + " AND HBL.HBL_EXP_TBL_PK=" + lblDocPK;
                strCondition2 = strCondition2 + " AND INV.CONSOL_INVOICE_PK=" + lblDocPK;
                strCondition3 = strCondition3 + " AND MET.MBL_EXP_TBL_PK=" + lblDocPK;
            }
            if (Convert.ToInt32(lblCarrierPK) > 0)
            {
                strCondition = strCondition + " AND BST.OPERATOR_MST_FK=" + lblCarrierPK;
                strCondition1 = strCondition1 + " AND BOOK.OPERATOR_MST_FK=" + lblCarrierPK;
                //Export
                if (Process == 1)
                {
                    strCondition2 = strCondition2 + " AND BKG.OPERATOR_MST_FK=" + lblCarrierPK;
                    //Import
                }
                else if (Process == 2)
                {
                    strCondition2 = strCondition2 + " AND JOB.OPERATOR_MST_FK=" + lblCarrierPK;
                }
                strCondition3 = strCondition3 + " AND MET.OPERATOR_MST_FK=" + lblCarrierPK;
            }
            if (Convert.ToInt32(POLPK) > 0)
            {
                strCondition = strCondition + "  AND PMTL.PORT_MST_PK= " + POLPK;
                strCondition1 = strCondition1 + "  AND PO.PORT_MST_PK= " + POLPK;
                strCondition2 = strCondition2 + "  AND POL.PORT_MST_PK= " + POLPK;
                strCondition3 = strCondition3 + "  AND POL.PORT_MST_PK= " + POLPK;
            }
            if (Convert.ToInt32(PODPK) > 0)
            {
                strCondition = strCondition + " AND PMTD.PORT_MST_PK = " + PODPK;
                strCondition1 = strCondition1 + " AND PO1.PORT_MST_PK = " + PODPK;
                strCondition2 = strCondition2 + " AND POD.PORT_MST_PK = " + PODPK;
                strCondition3 = strCondition3 + " AND POD.PORT_MST_PK = " + PODPK;
            }
            if (bizType == "2")
            {
                if (Convert.ToInt32(Cargotype) != 0)
                {
                    strCondition = strCondition + " AND BST.CARGO_TYPE=" + Cargotype;
                    strCondition1 = strCondition1 + " AND BOOK.CARGO_TYPE=" + Cargotype;
                    //Export
                    if (Process == 1)
                    {
                        strCondition2 = strCondition2 + " AND BKG.CARGO_TYPE=" + Cargotype;
                        //Import
                    }
                    else if (Process == 2)
                    {
                        strCondition2 = strCondition2 + " AND JOB.CARGO_TYPE=" + Cargotype;
                    }
                    strCondition3 = strCondition3 + " AND MET.CARGO_TYPE=" + Cargotype;
                }
            }
            if ((FromDate != null))
            {
                if (!string.IsNullOrEmpty(FromDate))
                {
                    strCondition = strCondition + " AND BST.BOOKING_DATE >= TO_DATE('" + FromDate + "',dateformat) ";
                    strCondition1 = strCondition1 + " AND HBL.HBL_DATE >= TO_DATE('" + FromDate + "',dateformat) ";
                    strCondition2 = strCondition2 + " AND INV.INVOICE_DATE >= TO_DATE('" + FromDate + "',dateformat) ";
                    strCondition3 = strCondition3 + " AND MET.MBL_DATE >= TO_DATE('" + FromDate + "',dateformat) ";
                }
            }
            if ((ToDate != null))
            {
                if (!string.IsNullOrEmpty(ToDate))
                {
                    strCondition = strCondition + " AND BST.BOOKING_DATE <= TO_DATE('" + ToDate + "',dateformat) ";
                    strCondition1 = strCondition1 + " AND HBL.HBL_DATE <= TO_DATE('" + ToDate + "',dateformat) ";
                    strCondition2 = strCondition2 + " AND INV.INVOICE_DATE <= TO_DATE('" + ToDate + "',dateformat) ";
                    strCondition3 = strCondition3 + " AND MET.MBL_DATE <= TO_DATE('" + ToDate + "',dateformat) ";
                }
            }
            //If Not ((FromDate Is Nothing Or FromDate = "") And (ToDate Is Nothing Or ToDate = "")) Then
            //    strCondition = strCondition & " AND BST.BOOKING_DATE BETWEEN TO_DATE('" & FromDate & "', DATEFORMAT) AND TO_DATE('" & ToDate & "', DATEFORMAT)"
            //    strCondition1 = strCondition1 & " AND HBL.HBL_DATE BETWEEN TO_DATE('" & FromDate & "', DATEFORMAT) AND TO_DATE('" & ToDate & "', DATEFORMAT)"
            //    strCondition2 = strCondition2 & " AND INV.INVOICE_DATE BETWEEN TO_DATE('" & FromDate & "', DATEFORMAT) AND TO_DATE('" & ToDate & "', DATEFORMAT)"
            //    strCondition3 = strCondition3 & " AND MET.MBL_DATE BETWEEN TO_DATE('" & FromDate & "', DATEFORMAT) AND TO_DATE('" & ToDate & "', DATEFORMAT)"
            //ElseIf (FromDate <> "") And (IsNothing(ToDate) Or ToDate = "") Then
            //    strCondition = strCondition & " AND BST.BOOKING_DATE >= TO_DATE('" & FromDate & "',dateformat) "
            //    strCondition1 = strCondition1 & " AND HBL.HBL_DATE >= TO_DATE('" & FromDate & "',dateformat) "
            //    strCondition2 = strCondition2 & " AND NV.INVOICE_DATE >= TO_DATE('" & FromDate & "',dateformat) "
            //    strCondition3 = strCondition3 & " AND MET.MBL_DATE >= TO_DATE('" & FromDate & "',dateformat) "
            //ElseIf (ToDate <> "") And (IsNothing(FromDate) Or FromDate = "") Then
            //    strCondition = strCondition & " AND BST.BOOKING_DATE <= TO_DATE('" & ToDate & "',dateformat) "
            //    strCondition1 = strCondition1 & " AND HBL.HBL_DATE <= TO_DATE('" & ToDate & "',dateformat) "
            //    strCondition2 = strCondition2 & " AND NV.INVOICE_DATE <= TO_DATE('" & ToDate & "',dateformat) "
            //    strCondition3 = strCondition3 & " AND MET.MBL_DATE <= TO_DATE('" & ToDate & "',dateformat) "
            //End If
            if (EDIStatus != 2)
            {
                strCondition = strCondition + " AND BST.EDI_STATUS = " + EDIStatus + " ";
                //strCondition1 = strCondition1 & " AND HBL.HBL_DATE <= TO_DATE('" & ToDate & "',dateformat) "
                strCondition2 = strCondition2 + " AND INV.EDI_STATUS = " + EDIStatus + " ";
                strCondition3 = strCondition3 + " AND MET.EDI_STATUS = " + EDIStatus + " ";
            }
            VslName = VslName.Trim().ToUpper();
            if (!string.IsNullOrEmpty(VslName.Trim()))
            {
                strCondition += " AND UPPER(BST.VESSEL_NAME) LIKE '%" + VslName + "%'";
                //strCondition1 &= " "
                strCondition2 += " AND UPPER(JOB.VESSEL_NAME) LIKE '%" + VslName + "%'";
                strCondition3 += " AND UPPER(VOY.VESSEL_NAME) LIKE '%" + VslName + "%'";
            }
            VslName = VoyageNr.Trim().ToUpper();
            if (!string.IsNullOrEmpty(VoyageNr.Trim()))
            {
                strCondition += " AND UPPER(BST.VOYAGE) LIKE '%" + VoyageNr + "%'";
                //strCondition1 &= " "
                strCondition2 += " AND UPPER(JOB.VOYAGE) LIKE '%" + VoyageNr + "%'";
                strCondition3 += " AND UPPER(VOY.VOYAGE) LIKE '%" + VoyageNr + "%'";
            }
            //'BOOKING
            if (Convert.ToInt32(Doctype) == 1)
            {
                sb.Append(GetGridBookingSeaQuery(strCondition));
                //'INVOICE
            }
            else if (Convert.ToInt32(Doctype) == 2)
            {
                sb.Append(GetInvoiceSeaQuery(strCondition2, Process));
                //'CARGO/FREIGHT MANIFEST
            }
            else if (Convert.ToInt32(Doctype) == 3 | Convert.ToInt32(Doctype) == 4)
            {
                sb.Append(GetCargoFreightManifestSeaQuery(strCondition3));
            }
            StringBuilder strCount = new StringBuilder();
            strSQL = sb.ToString();
            strCount.Append(" SELECT COUNT(*)  from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
            strCount.Remove(0, strCount.Length);

            StringBuilder sqlstr2 = new StringBuilder();
            sqlstr2.Append(" Select * from ");
            sqlstr2.Append( "  ( Select ROWNUM SR_NO, q.* from ");
            sqlstr2.Append("  (" + sb.ToString() + " ");
            sqlstr2.Append("  ) q )  WHERE \"SR_NO\"  BETWEEN " + start + " AND " + last + "");
            strSQL = sqlstr2.ToString();
            try
            {
                return objWF.GetDataSet(strSQL);
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

        public string GetGridBookingSeaQuery(string Condition = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("  SELECT DISTINCT BST.BOOKING_SEA_PK PK,");
            sb.Append("       BST.BOOKING_REF_NO DOCUMENTNR,");
            sb.Append("       BST.BOOKING_DATE DOCDATE,");
            sb.Append("       PMTL.PORT_ID AS POL,");
            sb.Append("       PMTD.PORT_ID AS POD,");
            sb.Append("       OPR.OPERATOR_NAME CARRIER,");
            sb.Append(" (CASE");
            sb.Append(" WHEN (NVL(BST.VESSEL_NAME, '') || '/' ||");
            sb.Append(" NVL(BST.VOYAGE, '') = '/') THEN");
            sb.Append(" ''");
            sb.Append(" ELSE");
            sb.Append(" NVL(BST.VESSEL_NAME, '') || '/' || NVL(BST.VOYAGE, '')");
            sb.Append(" END) AS VESVOYAGE,");
            sb.Append("       CMT.CUSTOMER_NAME SHIPPER,");
            sb.Append("       CST.CUSTOMER_NAME CONSIGNEE,");
            sb.Append("       OPR.OPERATOR_NAME,");
            sb.Append("     DECODE(BST.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE,");
            sb.Append("       CGT.COMMODITY_GROUP_CODE,");
            sb.Append("       DECODE(BST.EDI_STATUS,0,'NG',1,'TM') EDI_STATUS,");
            sb.Append("       '' SELFLAG,");
            sb.Append("      CMT.CUSTOMER_MST_PK");
            sb.Append("  FROM BOOKING_SEA_TBL  BST,");
            sb.Append("       USER_MST_TBL      UMT,");
            sb.Append("       CUSTOMER_MST_TBL CMT,");
            sb.Append("       CUSTOMER_MST_TBL CST,");
            sb.Append("       PORT_MST_TBL     PMTL,");
            sb.Append("       PORT_MST_TBL     PMTD,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGT,");
            sb.Append("       OPERATOR_MST_TBL OPR");
            sb.Append(" WHERE BST.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            sb.Append("   AND BST.CONS_CUSTOMER_MST_FK = CST.CUSTOMER_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POL_FK = PMTL.PORT_MST_PK");
            sb.Append("   AND BST.PORT_MST_POD_FK = PMTD.PORT_MST_PK");
            sb.Append("   AND CGT.COMMODITY_GROUP_PK=BST.COMMODITY_GROUP_FK");
            sb.Append("   AND OPR.OPERATOR_MST_PK=BST.OPERATOR_MST_FK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND BST.STATUS <> 3 ");
            sb.Append(" " + Condition + "");
            sb.Append("     order by BOOKING_DATE DESC, BOOKING_REF_NO DESC");
            return sb.ToString();
        }

        public string GetHBLQuery(string Condition = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append(" SELECT DISTINCT HBL.HBL_EXP_TBL_PK BOOKING_SEA_PK,");
            sb.Append("   HBL.HBL_REF_NO BOOKING_REF_NO,");
            sb.Append("   HBL.HBL_DATE BOOKING_DATE,");
            sb.Append("   PO.PORT_ID         AS POL,");
            sb.Append("   PO1.PORT_ID        AS POD,");
            sb.Append("   OPR.OPERATOR_NAME CARRIER,");
            sb.Append("   (CASE");
            sb.Append("   WHEN (NVL(V.VESSEL_NAME, '') || '/' ||");
            sb.Append("   NVL(VVT.VOYAGE, '') = '/') THEN");
            sb.Append("    ''");
            sb.Append("   ELSE");
            sb.Append("   NVL(V.VESSEL_NAME, '') || '/' ||");
            sb.Append("   NVL(VVT.VOYAGE, '')");
            sb.Append("   END) AS VESVOYAGE,");
            sb.Append("   CMT.CUSTOMER_ID SHIPPER,");
            sb.Append("   CST.CUSTOMER_ID CONSIGNEE,");
            sb.Append("   OPR.OPERATOR_ID,");
            sb.Append("   DECODE(BOOK.CARGO_TYPE,");
            sb.Append("   '1',");
            sb.Append("   'FCL',");
            sb.Append("   '2',");
            sb.Append("   'LCL',");
            sb.Append("   '4',");
            sb.Append("   'BBC') CARGO_TYPE,");
            sb.Append("   CGT.COMMODITY_GROUP_CODE,");
            sb.Append("   '' SELFLAG,");
            sb.Append("  CMT.CUSTOMER_MST_PK");
            sb.Append("   FROM HBL_EXP_TBL          HBL,");
            sb.Append("   JOB_CARD_SEA_EXP_TBL JOB,");
            sb.Append("   CUSTOMER_MST_TBL     CMT,");
            sb.Append("   CUSTOMER_MST_TBL     CST,");
            sb.Append("   BOOKING_SEA_TBL      BOOK,");
            sb.Append("   PORT_MST_TBL         PO,");
            sb.Append("   OPERATOR_MST_TBL     OPR,");
            sb.Append("   PORT_MST_TBL         PO1,");
            sb.Append("   USER_MST_TBL         UMT,");
            sb.Append("   VESSEL_VOYAGE_TBL    V,");
            sb.Append("   VESSEL_VOYAGE_TRN    VVT,");
            sb.Append("   COMMODITY_GROUP_MST_TBL CGT");
            sb.Append("   WHERE OPR.OPERATOR_MST_PK = BOOK.OPERATOR_MST_FK");
            sb.Append("   AND CMT.CUSTOMER_MST_PK = JOB.SHIPPER_CUST_MST_FK");
            sb.Append("   AND CST.CUSTOMER_MST_PK=JOB.CONSIGNEE_CUST_MST_FK");
            sb.Append("   AND JOB.BOOKING_SEA_FK = BOOK.BOOKING_SEA_PK");
            sb.Append("   AND BOOK.PORT_MST_POL_FK = PO.PORT_MST_PK");
            sb.Append("   AND BOOK.PORT_MST_POD_FK = PO1.PORT_MST_PK");
            sb.Append("   AND HBL.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_SEA_EXP_PK");
            sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK(+)");
            sb.Append("   AND HBL.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
            sb.Append("   AND JOB.COMMODITY_GROUP_FK=CGT.COMMODITY_GROUP_PK");
            sb.Append("   AND HBL.HBL_STATUS = 1");
            sb.Append("   AND HBL.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append(" " + Condition + "");
            sb.Append("    order by HBL_DATE DESC, HBL_REF_NO DESC");
            return sb.ToString();
        }

        public string GetInvoiceSeaQuery(string Condition = "", int Process = 0)
        {
            StringBuilder sb = new StringBuilder(5000);
            //IMPORT
            if (Process == 2)
            {
                sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK PK,");
                sb.Append("       INV.INVOICE_REF_NO DOCUMENTNR,");
                sb.Append("       INV.INVOICE_DATE DOCDATE,");
                sb.Append("       POL.PORT_ID POL,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       OPR.OPERATOR_NAME CARRIER,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append("       CMT.CUSTOMER_ID SHIPPER,");
                sb.Append("       CST.CUSTOMER_ID CONSIGNEE,");
                sb.Append("       OPR.OPERATOR_ID OPERATOR_NAME,");
                sb.Append("       DECODE(JOB.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                sb.Append("       CGM.COMMODITY_GROUP_CODE, DECODE(INV.EDI_STATUS,0,'NG',1,'TM') EDI_STATUS,");
                sb.Append("       '' SELFLAG,");
                sb.Append("       CMT.CUSTOMER_MST_PK");
                sb.Append("  FROM CONSOL_INVOICE_TBL      INV,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL  INVTRN,");
                sb.Append("       JOB_CARD_SEA_IMP_TBL    JOB,");
                //sb.Append("       BOOKING_SEA_TBL         BKG,")
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       CUSTOMER_MST_TBL        CST,");
                sb.Append("       OPERATOR_MST_TBL        OPR,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGM,");
                sb.Append("       USER_MST_TBL      UMT ");
                sb.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                sb.Append("   AND JOB.JOB_CARD_SEA_IMP_PK = INVTRN.JOB_CARD_FK");
                //sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK")
                sb.Append("   AND POL.PORT_MST_PK = JOB.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JOB.PORT_MST_POD_FK");
                sb.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CST.CUSTOMER_MST_PK");
                sb.Append("   AND OPR.OPERATOR_MST_PK = JOB.OPERATOR_MST_FK");
                sb.Append("   AND CGM.COMMODITY_GROUP_PK = JOB.COMMODITY_GROUP_FK");
                sb.Append("   AND INV.PROCESS_TYPE=2");
                sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
                sb.Append("   AND POD.LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
                sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append(" " + Condition + "");
                sb.Append("     order by INVOICE_DATE DESC, INVOICE_REF_NO DESC");
                //EXPORT
            }
            else
            {
                sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK PK,");
                sb.Append("       INV.INVOICE_REF_NO DOCUMENTNR,");
                sb.Append("       INV.INVOICE_DATE DOCDATE,");
                sb.Append("       POL.PORT_ID POL,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       OPR.OPERATOR_NAME CARRIER,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append("       CMT.CUSTOMER_ID SHIPPER,");
                sb.Append("       CST.CUSTOMER_ID CONSIGNEE,");
                sb.Append("       OPR.OPERATOR_ID,");
                sb.Append("       DECODE(BKG.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                sb.Append("       CGM.COMMODITY_GROUP_CODE,DECODE(INV.EDI_STATUS,0,'NG',1,'TM') EDI_STATUS,");
                sb.Append("       '' SELFLAG,");
                sb.Append("       CMT.CUSTOMER_MST_PK");
                sb.Append("  FROM CONSOL_INVOICE_TBL      INV,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL  INVTRN,");
                sb.Append("       JOB_CARD_SEA_EXP_TBL    JOB,");
                sb.Append("       BOOKING_SEA_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       CUSTOMER_MST_TBL        CST,");
                sb.Append("       OPERATOR_MST_TBL        OPR,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGM,");
                sb.Append("       USER_MST_TBL      UMT ");
                sb.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                sb.Append("   AND JOB.JOB_CARD_SEA_EXP_PK = INVTRN.JOB_CARD_FK");
                sb.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                sb.Append("   AND POL.PORT_MST_PK = BKG.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = BKG.PORT_MST_POD_FK");
                sb.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CST.CUSTOMER_MST_PK");
                sb.Append("   AND OPR.OPERATOR_MST_PK = BKG.OPERATOR_MST_FK");
                sb.Append("   AND CGM.COMMODITY_GROUP_PK = JOB.COMMODITY_GROUP_FK");
                sb.Append("   AND INV.PROCESS_TYPE=1");
                sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
                sb.Append("   AND POL.LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
                sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append(" " + Condition + "");
                sb.Append("     order by INVOICE_DATE DESC, INVOICE_REF_NO DESC");
            }
            return sb.ToString();
        }

        public string GetCargoFreightManifestSeaQuery(string Condition = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT MET.MBL_EXP_TBL_PK PK,");
            sb.Append("       MET.MBL_REF_NO DOCUMENTNR,");
            sb.Append("       MET.MBL_DATE DOCDATE,");
            sb.Append("       POL.PORT_ID POL,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       OMT.OPERATOR_NAME CARRIER,");
            sb.Append(" (CASE");
            sb.Append(" WHEN (NVL(VOY.VESSEL_NAME, '') || '/' ||");
            sb.Append(" NVL(VVT.VOYAGE, '') = '/') THEN");
            sb.Append(" ''");
            sb.Append(" ELSE");
            sb.Append(" NVL(VOY.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '')");
            sb.Append(" END) AS VESVOYAGE,");
            //sb.Append("       (VOY.VESSEL_NAME || ' / ' || VVT.VOYAGE) VESSEL_VOY,")
            sb.Append("       MET.SHIPPER_NAME SHIPPER,");
            sb.Append("       MET.CONSIGNEE_NAME CONSIGNEE,");
            sb.Append("       OMT.OPERATOR_NAME,");
            sb.Append("       DECODE(MET.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE,");
            sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
            sb.Append("       DECODE(MET.EDI_STATUS,0,'NG',1,'TM') EDI_STATUS,");
            sb.Append("       '' SELFLAG,");
            sb.Append("       CMT.CUSTOMER_MST_PK CUSTOMER_MST_PK");
            sb.Append("  FROM MBL_EXP_TBL       MET,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL   JSE,");
            sb.Append("       USER_MST_TBL      UMT,");
            sb.Append("       CUSTOMER_MST_TBL  CMT,");
            sb.Append("       PORT_MST_TBL      POL,");
            sb.Append("       PORT_MST_TBL      POD,");
            sb.Append("       OPERATOR_MST_TBL  OMT,");
            sb.Append("       VESSEL_VOYAGE_TRN VVT,");
            sb.Append("       VESSEL_VOYAGE_TBL VOY, ");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
            sb.Append(" WHERE UMT.USER_MST_PK = MET.CREATED_BY_FK");
            sb.Append("   AND JSE.MBL_EXP_TBL_FK = MET.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
            sb.Append("   AND POL.PORT_MST_PK = MET.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = MET.PORT_MST_POD_FK");
            sb.Append("   AND OMT.OPERATOR_MST_PK = MET.OPERATOR_MST_FK");
            sb.Append("   AND VVT.VOYAGE_TRN_PK(+) = MET.VOYAGE_TRN_FK");
            sb.Append("   AND VOY.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND CGMT.COMMODITY_GROUP_PK = MET.COMMODITY_GROUP_FK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MET.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND MET.MBL_REF_NO IS NOT NULL ");
            sb.Append("  " + Condition + " ");
            sb.Append("     order by MBL_DATE DESC, MBL_REF_NO DESC");
            sb.Append("");

            return sb.ToString();
        }

        public DataSet FetchGridDetailsAir(string Doctype = "", string Cargotype = "", string bizType = "", string lblDocPK = "", string lblCarrierPK = "", string POLPK = "", string PODPK = "", string FromDate = "", string ToDate = "", Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 flag = 0, int Process = 0, int EDIStatus = 0, string VslVoy = "", Int32 ChkONLD = 0)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            string strCondition1 = null;
            string strCondition2 = null;
            string strCondition3 = null;

            if (ChkONLD == 0)
            {
                strCondition = strCondition + "  AND 1=2";
                strCondition1 = strCondition1 + "  AND 1=2";
                strCondition2 = strCondition2 + "  AND 1=2";
                strCondition3 = strCondition3 + "  AND 1=2";
            }
            //If flag = 0 Then
            //    strCondition = strCondition & "  AND 1=2"
            //    strCondition1 = strCondition1 & "  AND 1=2"
            //    strCondition2 = strCondition2 & "  AND 1=2"
            //    strCondition3 = strCondition3 & "  AND 1=2"
            //End If
            if (Convert.ToInt32(lblDocPK )> 0)
            {
                strCondition = strCondition + " AND BRT.BOOKING_AIR_PK=" + lblDocPK;
                strCondition1 = strCondition1 + " AND HAWB.HAWB_EXP_TBL_PK=" + lblDocPK;
                strCondition2 = strCondition2 + " AND INV.CONSOL_INVOICE_PK=" + lblDocPK;
                strCondition3 = strCondition3 + " AND MET.MAWB_EXP_TBL_PK=" + lblDocPK;
            }
            if (Convert.ToInt32(lblCarrierPK) > 0)
            {
                strCondition = strCondition + " AND BRT.AIRLINE_MST_FK=" + lblCarrierPK;
                strCondition1 = strCondition1 + " AND BOOK.AIRLINE_MST_FK=" + lblCarrierPK;
                //Export
                if (Process == 1)
                {
                    strCondition2 = strCondition2 + " AND BKG.AIRLINE_MST_FK=" + lblCarrierPK;
                    //Import
                }
                else if (Process == 2)
                {
                    strCondition2 = strCondition2 + " AND JOB.AIRLINE_MST_FK=" + lblCarrierPK;
                }
                strCondition3 = strCondition3 + " AND MET.AIRLINE_MST_FK=" + lblCarrierPK;
            }
            if (Convert.ToInt32(POLPK) > 0)
            {
                strCondition = strCondition + "  AND PMTL.PORT_MST_PK= " + POLPK;
                strCondition1 = strCondition1 + "  AND PO.PORT_MST_PK= " + POLPK;
                strCondition2 = strCondition2 + "  AND POL.PORT_MST_PK= " + POLPK;
                strCondition3 = strCondition3 + "  AND POL.PORT_MST_PK= " + POLPK;
            }
            if (Convert.ToInt32(PODPK) >0)
            {
                strCondition = strCondition + " AND PMTD.PORT_MST_PK = " + PODPK;
                strCondition1 = strCondition1 + " AND PO1.PORT_MST_PK = " + PODPK;
                strCondition2 = strCondition2 + " AND POD.PORT_MST_PK = " + PODPK;
                strCondition3 = strCondition3 + " AND POD.PORT_MST_PK = " + PODPK;
            }
            if ((FromDate != null))
            {
                if (!string.IsNullOrEmpty(FromDate))
                {
                    strCondition = strCondition + " AND BRT.BOOKING_DATE >= TO_DATE('" + FromDate + "',dateformat) ";
                    strCondition1 = strCondition1 + " AND HAWB.HAWB_DATE >= TO_DATE('" + FromDate + "',dateformat) ";
                    strCondition2 = strCondition2 + " AND INV.INVOICE_DATE >= TO_DATE('" + FromDate + "',dateformat) ";
                    strCondition3 = strCondition3 + " AND MET.MAWB_DATE >= TO_DATE('" + FromDate + "',dateformat) ";
                }
            }
            if ((ToDate != null))
            {
                if (!string.IsNullOrEmpty(ToDate))
                {
                    strCondition = strCondition + " AND BRT.BOOKING_DATE <= TO_DATE('" + ToDate + "',dateformat) ";
                    strCondition1 = strCondition1 + " AND HAWB.HAWB_DATE <= TO_DATE('" + ToDate + "',dateformat) ";
                    strCondition2 = strCondition2 + " AND INV.INVOICE_DATE <= TO_DATE('" + ToDate + "',dateformat) ";
                    strCondition3 = strCondition3 + " AND MET.MAWB_DATE <= TO_DATE('" + ToDate + "',dateformat) ";
                }
            }
            //If Not ((FromDate Is Nothing Or FromDate = "") And (ToDate Is Nothing Or ToDate = "")) Then
            //    strCondition = strCondition & " AND BRT.BOOKING_DATE BETWEEN TO_DATE('" & FromDate & "', DATEFORMAT) AND TO_DATE('" & ToDate & "', DATEFORMAT)"
            //    strCondition1 = strCondition1 & " AND HAWB.HAWB_DATE BETWEEN TO_DATE('" & FromDate & "', DATEFORMAT) AND TO_DATE('" & ToDate & "', DATEFORMAT)"
            //    strCondition2 = strCondition2 & " AND INV.INVOICE_DATE BETWEEN TO_DATE('" & FromDate & "', DATEFORMAT) AND TO_DATE('" & ToDate & "', DATEFORMAT)"
            //    strCondition3 = strCondition3 & " AND MET.MAWB_DATE BETWEEN TO_DATE('" & FromDate & "', DATEFORMAT) AND TO_DATE('" & ToDate & "', DATEFORMAT)"
            //ElseIf (FromDate <> "") And (IsNothing(ToDate) Or ToDate = "") Then
            //    strCondition = strCondition & " AND BRT.BOOKING_DATE >= TO_DATE('" & FromDate & "',dateformat) "
            //    strCondition1 = strCondition1 & " AND HAWB.HAWB_DATE >= TO_DATE('" & FromDate & "',dateformat) "
            //    strCondition2 = strCondition2 & " AND NV.INVOICE_DATE >= TO_DATE('" & FromDate & "',dateformat) "
            //    strCondition3 = strCondition3 & " AND MET.MAWB_DATE >= TO_DATE('" & FromDate & "',dateformat) "
            //ElseIf (ToDate <> "") And (IsNothing(FromDate) Or FromDate = "") Then
            //    strCondition = strCondition & " AND BRT.BOOKING_DATE <= TO_DATE('" & ToDate & "',dateformat) "
            //    strCondition1 = strCondition1 & " AND HAWB.HAWB_DATE <= TO_DATE('" & ToDate & "',dateformat) "
            //    strCondition2 = strCondition2 & " AND NV.INVOICE_DATE <= TO_DATE('" & ToDate & "',dateformat) "
            //    strCondition3 = strCondition3 & " AND MET.MAWB_DATE <= TO_DATE('" & ToDate & "',dateformat) "
            //End If
            if (EDIStatus != 2)
            {
                strCondition = strCondition + " AND BRT.EDI_STATUS = " + EDIStatus + " ";
                //strCondition1 = strCondition1 & " AND HBL.HBL_DATE <= TO_DATE('" & ToDate & "',dateformat) "
                strCondition2 = strCondition2 + " AND INV.EDI_STATUS = " + EDIStatus + " ";
                strCondition3 = strCondition3 + " AND MET.EDI_STATUS = " + EDIStatus + " ";
            }
            VslVoy = VslVoy.Trim().ToUpper();
            if (!string.IsNullOrEmpty(VslVoy.Trim()))
            {
                strCondition += " AND UPPER(BRT.FLIGHT_NO) LIKE '%" + VslVoy + "%'";
                //strCondition1 &= " "
                strCondition2 += " AND UPPER(JOB.FLIGHT_NO) LIKE '%" + VslVoy + "%'";
                strCondition3 += " AND UPPER(JSE.FLIGHT_NO) LIKE '%" + VslVoy + "%'";
            }
            //'BOOKING
            if (Convert.ToInt32(Doctype) == 1)
            {
                sb.Append(GetGridBookingAirQuery(strCondition));
                //'INVOICE
            }
            else if (Convert.ToInt32(Doctype )== 2)
            {
                sb.Append(GetInvoiceAirQuery(strCondition2, Process));
                //'CARGO/FREIGHT MANIFEST
            }
            else if (Convert.ToInt32(Doctype) == 3 | Convert.ToInt32(Doctype) == 4)
            {
                sb.Append(GetGridCargoFreightManifestAirQuery(strCondition3));
            }
            StringBuilder strCount = new StringBuilder();
            strSQL = sb.ToString();
            strCount.Append(" SELECT COUNT(*)  from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
            strCount.Remove(0, strCount.Length);

            StringBuilder sqlstr2 = new StringBuilder();
            sqlstr2.Append(" Select * from ");
            sqlstr2.Append( "  ( Select ROWNUM SR_NO, q.* from ");
            sqlstr2.Append("  (" + sb.ToString() + " ");
            sqlstr2.Append("  ) q )  WHERE \"SR_NO\"  BETWEEN " + start + " AND " + last + "");
            strSQL = sqlstr2.ToString();
            try
            {
                return objWF.GetDataSet(strSQL);
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

        public string GetGridBookingAirQuery(string Condition = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("  SELECT DISTINCT BRT.BOOKING_AIR_PK PK,");
            sb.Append("       BRT.BOOKING_REF_NO DOCUMENTNR,");
            sb.Append("       BRT.BOOKING_DATE DOCDATE,");
            sb.Append("       PMTL.PORT_ID AS POL,");
            sb.Append("       PMTD.PORT_ID AS POD,");
            sb.Append("       AMT.AIRLINE_NAME CARRIER,");
            sb.Append("       BRT.FLIGHT_NO VESVOYAGE,");
            sb.Append("       CMT.CUSTOMER_NAME SHIPPER,");
            sb.Append("       CST.CUSTOMER_NAME CONSIGNEE,");
            sb.Append("       AMT.AIRLINE_ID,");
            sb.Append("     DECODE(BRT.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE,");
            sb.Append("       CGT.COMMODITY_GROUP_CODE,");
            sb.Append("       DECODE(BRT.EDI_STATUS,0,'NG',1,'TM') EDI_STATUS,");
            sb.Append("       '' SELFLAG,");
            sb.Append("      CMT.CUSTOMER_MST_PK");
            sb.Append("  FROM BOOKING_AIR_TBL  BRT,");
            sb.Append("       USER_MST_TBL      UMT,");
            sb.Append("       CUSTOMER_MST_TBL CMT,");
            sb.Append("       CUSTOMER_MST_TBL CST,");
            sb.Append("       PORT_MST_TBL     PMTL,");
            sb.Append("       PORT_MST_TBL     PMTD,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGT,");
            sb.Append("       AIRLINE_MST_TBL         AMT");
            sb.Append(" WHERE BRT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            sb.Append("   AND BRT.CONS_CUSTOMER_MST_FK = CST.CUSTOMER_MST_PK(+)");
            sb.Append("   AND BRT.PORT_MST_POL_FK = PMTL.PORT_MST_PK");
            sb.Append("   AND BRT.PORT_MST_POD_FK = PMTD.PORT_MST_PK");
            sb.Append("   AND CGT.COMMODITY_GROUP_PK=BRT.COMMODITY_GROUP_FK");
            sb.Append("   AND AMT.AIRLINE_MST_PK=BRT.AIRLINE_MST_FK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND BRT.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND BRT.STATUS <> 3 ");
            sb.Append(" " + Condition + "");
            sb.Append("     order by BOOKING_DATE DESC, BOOKING_REF_NO DESC");
            return sb.ToString();
        }

        public string GetHAWBQuery(string Condition = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append(" SELECT DISTINCT HAWB.HAWB_EXP_TBL_PK BOOKING_SEA_PK,");
            sb.Append("   HAWB.HAWB_REF_NO BOOKING_REF_NO,");
            sb.Append("   HAWB.HAWB_DATE BOOKING_DATE,");
            sb.Append("   PO.PORT_ID         AS POL,");
            sb.Append("   PO1.PORT_ID        AS POD,");
            sb.Append("   AMT.AIRLINE_NAME CARRIER,");
            sb.Append("   BOOK.FLIGHT_NO AS VESVOYAGE,");
            sb.Append("   CMT.CUSTOMER_ID SHIPPER,");
            sb.Append("   CST.CUSTOMER_ID CONSIGNEE,");
            sb.Append("   AMT.AIRLINE_ID,");
            sb.Append("   DECODE(BOOK.CARGO_TYPE,");
            sb.Append("   '1',");
            sb.Append("   'FCL',");
            sb.Append("   '2',");
            sb.Append("   'LCL',");
            sb.Append("   '4',");
            sb.Append("   'BBC') CARGO_TYPE,");
            sb.Append("   CGT.COMMODITY_GROUP_CODE,");
            sb.Append("   '' SELFLAG,");
            sb.Append("  CMT.CUSTOMER_MST_PK");
            sb.Append("   FROM HAWB_EXP_TBL          HAWB,");
            sb.Append("   JOB_CARD_AIR_EXP_TBL JOB,");
            sb.Append("   CUSTOMER_MST_TBL     CMT,");
            sb.Append("   CUSTOMER_MST_TBL     CST,");
            sb.Append("   BOOKING_AIR_TBL      BOOK,");
            sb.Append("   PORT_MST_TBL         PO,");
            sb.Append("   AIRLINE_MST_TBL      AMT,");
            sb.Append("   PORT_MST_TBL         PO1,");
            sb.Append("   USER_MST_TBL         UMT,");
            sb.Append("   COMMODITY_GROUP_MST_TBL CGT");
            sb.Append("   WHERE AMT.AIRLINE_MST_PK=BOOK.AIRLINE_MST_FK");
            sb.Append("   AND CMT.CUSTOMER_MST_PK = JOB.SHIPPER_CUST_MST_FK");
            sb.Append("   AND CST.CUSTOMER_MST_PK=JOB.CONSIGNEE_CUST_MST_FK");
            sb.Append("   AND JOB.BOOKING_AIR_FK = BOOK.BOOKING_AIR_PK");
            sb.Append("   AND BOOK.PORT_MST_POL_FK = PO.PORT_MST_PK");
            sb.Append("   AND BOOK.PORT_MST_POD_FK = PO1.PORT_MST_PK");
            sb.Append("   AND HAWB.JOB_CARD_AIR_EXP_FK = JOB.JOB_CARD_AIR_EXP_PK");
            sb.Append("   AND JOB.COMMODITY_GROUP_FK=CGT.COMMODITY_GROUP_PK");
            sb.Append("   AND HAWB.HAWB_STATUS = 1");
            sb.Append("   AND HAWB.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append(" " + Condition + "");
            sb.Append("    order by HAWB_DATE DESC, HAWB_REF_NO DESC");
            return sb.ToString();
        }

        public string GetInvoiceAirQuery(string Condition = "", int Process = 0)
        {
            StringBuilder sb = new StringBuilder(5000);
            //IMPORT
            if (Process == 2)
            {
                sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK PK,");
                sb.Append("       INV.INVOICE_REF_NO DOCUMENTNR,");
                sb.Append("       INV.INVOICE_DATE DOCDATE,");
                sb.Append("       POL.PORT_ID POL,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       AMT.AIRLINE_NAME CARRIER,");
                sb.Append("       JOB.FLIGHT_NO AS VESVOYAGE,");
                sb.Append("       CMT.CUSTOMER_ID SHIPPER,");
                sb.Append("       CST.CUSTOMER_ID CONSIGNEE,");
                sb.Append("       AMT.AIRLINE_ID,");
                sb.Append("       '' CARGO_TYPE,");
                sb.Append("       CGM.COMMODITY_GROUP_CODE,DECODE(INV.EDI_STATUS,0,'NG',1,'TM') EDI_STATUS,");
                sb.Append("       '' SELFLAG,");
                sb.Append("       CMT.CUSTOMER_MST_PK");
                sb.Append("  FROM CONSOL_INVOICE_TBL      INV,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL  INVTRN,");
                sb.Append("       JOB_CARD_AIR_IMP_TBL    JOB,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       CUSTOMER_MST_TBL        CST,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGM,");
                sb.Append("       USER_MST_TBL      UMT ");
                sb.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                sb.Append("   AND JOB.JOB_CARD_AIR_IMP_PK = INVTRN.JOB_CARD_FK");
                sb.Append("   AND POL.PORT_MST_PK = JOB.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JOB.PORT_MST_POD_FK");
                sb.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CST.CUSTOMER_MST_PK");
                sb.Append("   AND AMT.AIRLINE_MST_PK = JOB.AIRLINE_MST_FK");
                sb.Append("   AND CGM.COMMODITY_GROUP_PK = JOB.COMMODITY_GROUP_FK");
                sb.Append("   AND INV.PROCESS_TYPE=2");
                sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
                sb.Append("   AND POD.LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
                sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append(" " + Condition + "");
                sb.Append("     order by INVOICE_DATE DESC, INVOICE_REF_NO DESC");
                //EXPORT
            }
            else
            {
                sb.Append("SELECT DISTINCT INV.CONSOL_INVOICE_PK PK,");
                sb.Append("       INV.INVOICE_REF_NO DOCUMENTNR,");
                sb.Append("       INV.INVOICE_DATE DOCDATE,");
                sb.Append("       POL.PORT_ID POL,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       AMT.AIRLINE_NAME CARRIER,");
                sb.Append("       JOB.FLIGHT_NO AS VESVOYAGE,");
                sb.Append("       CMT.CUSTOMER_ID SHIPPER,");
                sb.Append("       CST.CUSTOMER_ID CONSIGNEE,");
                sb.Append("       AMT.AIRLINE_ID,");
                sb.Append("       DECODE(BKG.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                sb.Append("       CGM.COMMODITY_GROUP_CODE,DECODE(INV.EDI_STATUS,0,'NG',1,'TM') EDI_STATUS,");
                sb.Append("       '' SELFLAG,");
                sb.Append("       CMT.CUSTOMER_MST_PK");
                sb.Append("  FROM CONSOL_INVOICE_TBL      INV,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL  INVTRN,");
                sb.Append("       JOB_CARD_AIR_EXP_TBL    JOB,");
                sb.Append("       BOOKING_AIR_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       CUSTOMER_MST_TBL        CST,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGM,");
                sb.Append("       USER_MST_TBL      UMT ");
                sb.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = INVTRN.JOB_CARD_FK");
                sb.Append("   AND JOB.BOOKING_AIR_FK = BKG.BOOKING_AIR_PK");
                sb.Append("   AND POL.PORT_MST_PK = BKG.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = BKG.PORT_MST_POD_FK");
                sb.Append("   AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CST.CUSTOMER_MST_PK");
                sb.Append("   AND AMT.AIRLINE_MST_PK = BKG.AIRLINE_MST_FK");
                sb.Append("   AND CGM.COMMODITY_GROUP_PK = JOB.COMMODITY_GROUP_FK");
                sb.Append("   AND INV.PROCESS_TYPE=1");
                sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
                sb.Append("   AND POL.LOCATION_MST_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
                sb.Append("   AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append(" " + Condition + "");
                sb.Append("     order by INVOICE_DATE DESC, INVOICE_REF_NO DESC");
            }
            return sb.ToString();
        }

        public string GetGridCargoFreightManifestAirQuery(string Condition = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("       SELECT DISTINCT MET.MAWB_EXP_TBL_PK PK,");
            sb.Append("       MET.MAWB_REF_NO DOCUMENTNR,");
            sb.Append("       MET.MAWB_DATE DOCDATE,");
            sb.Append("       POL.PORT_ID POL,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       AMT.AIRLINE_NAME CARRIER,");
            //sb.Append("       '' VESVOYAGE1,")
            sb.Append("       JSE.FLIGHT_NO VESVOYAGE,");
            sb.Append("       MET.SHIPPER_NAME SHIPPER,");
            sb.Append("       MET.CONSIGNEE_NAME CONSIGNEE,");
            sb.Append("       AMT.AIRLINE_ID OPERATOR_NAME,");
            sb.Append("       '' CARGO_TYPE,");
            sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMODITY_GROUP_CODE,");
            sb.Append("       DECODE(MET.EDI_STATUS,0,'NG',1,'TM') EDI_STATUS,");
            sb.Append("       '' SELFLAG,");
            sb.Append("       CMT.CUSTOMER_MST_PK CUSTOMER_MST_PK");
            sb.Append("  FROM MAWB_EXP_TBL      MET,");
            sb.Append("       JOB_CARD_AIR_EXP_TBL   JSE,");
            sb.Append("       USER_MST_TBL      UMT,");
            sb.Append("       CUSTOMER_MST_TBL  CMT,");
            sb.Append("       PORT_MST_TBL      POL,");
            sb.Append("       PORT_MST_TBL      POD,");
            sb.Append("       AIRLINE_MST_TBL   AMT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
            sb.Append(" WHERE UMT.USER_MST_PK = MET.CREATED_BY_FK");
            sb.Append("   AND JSE.MAWB_EXP_TBL_FK = MET.MAWB_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
            sb.Append("   AND POL.PORT_MST_PK = MET.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = MET.PORT_MST_POD_FK");
            sb.Append("   AND AMT.AIRLINE_MST_PK = MET.AIRLINE_MST_FK");
            sb.Append("   AND CGMT.COMMODITY_GROUP_PK = MET.COMMODITY_GROUP_FK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MET.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND MET.MAWB_REF_NO IS NOT NULL");
            sb.Append(" " + Condition + "");
            sb.Append("     order by MAWB_DATE DESC, MAWB_REF_NO DESC");
            sb.Append("       ");

            return sb.ToString();
        }

        #endregion "Getting Grid Details"

        #region "Get Jobcar Details for invoice"

        public DataSet GetInvJobDetails1(string InvPks, int Biz, int Process, string CargoType = "")
        {
            string Biz_Process = "";
            Biz_Process = "SELECT DISTINCT ";
            //Sea
            if (Biz == 2)
            {
                Biz_Process = " 'SEA' BIZ_TYPE, ";
                //Air
            }
            else if (Biz == 1)
            {
                Biz_Process = " 'AIR' BIZ_TYPE, ";
            }
            //Export
            if (Process == 1)
            {
                Biz_Process += " 'EXPORT' PROCESS_TYPE, ";
                //Import
            }
            else if (Process == 2)
            {
                Biz_Process += " 'IMPORT' PROCESS_TYPE, ";
            }
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT DISTINCT " + Biz_Process + " HDR.INVOICE_REF_NO, ");
            sb.Append("                TO_CHAR(HDR.INVOICE_DATE,DATEFORMAT) INVOICE_DATE,");
            sb.Append("                TO_CHAR(HDR.INVOICE_DUE_DATE,DATEFORMAT) INVOICE_DUE_DATE,");
            sb.Append("                UPPER(NVL('''' || HDR.INV_UNIQUE_REF_NR,' ')) BANK_REF_NR,");
            sb.Append("                (SELECT CMT.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK=HDR.CUSTOMER_MST_FK) CUSTOMER_CODE,");
            sb.Append("                (SELECT CMT.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK=HDR.CUSTOMER_MST_FK) CUSTOMER_NAME,");
            sb.Append("                (SELECT CTM.CURRENCY_ID FROM CURRENCY_TYPE_MST_TBL CTM WHERE CTM.CURRENCY_MST_PK=HDR.CURRENCY_MST_FK) INV_CURR,");
            sb.Append("                DECODE(HDR.CHK_INVOICE,0,'Pending',1,'Approved') STATUS,");
            sb.Append("                NVL(JOB.JOBCARD_REF_NO,' ') JOBCARD_REF_NO,");
            sb.Append("                TO_CHAR(JOB.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
            sb.Append("                FMT.FREIGHT_ELEMENT_ID ELEMENT_CODE,");
            sb.Append("                FMT.FREIGHT_ELEMENT_NAME ELEMENT_NAME,");
            sb.Append("                DECODE(JOB.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE,");
            sb.Append("                NVL(CUMT.CURRENCY_ID,' ') CURRENCY,");
            //sb.Append("                NVL(TRN.ELEMENT_AMT,0.0) AS FREIGHT_AMOUNT,")
            sb.Append("                NVL(TRN.ELEMENT_AMT,0.0) AS AMOUNT,");
            sb.Append("                GET_EX_RATE(TRN.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",JOB.JOBCARD_DATE) ROE,");
            sb.Append("                NVL(TRN.AMT_IN_INV_CURR,0) AS INV_AMOUNT,");
            sb.Append("                (CASE WHEN TRN.VAT_CODE = '0' THEN '' ELSE TRN.VAT_CODE END) VAT_CODE,");
            sb.Append("                TRN.TAX_PCNT AS VAT_PERCENT,");
            sb.Append("                NVL(TRN.TAX_AMT,0) AS VAT_AMOUNT,");
            //sb.Append("                NVL(TRN.TOT_AMT,0) AS TOTAL_JOB_AMOUNT,")
            sb.Append("                NVL(TRN.TOT_AMT,0) AS TOTAL_AMOUNT,");
            //sb.Append("                CASE WHEN TRN.FRT_OTH_ELEMENT_FK IS NOT NULL THEN ")
            //sb.Append("                (CASE WHEN TRN.FRT_OTH_ELEMENT_FK < 3 THEN ")
            //sb.Append("                DECODE(TRN.FRT_OTH_ELEMENT_FK,")
            //sb.Append("                       1,")
            //sb.Append("                       'COST',")
            //sb.Append("                       2,")
            //sb.Append("                       'FREIGHT') ELSE 'OTHER' END) ELSE 'OTHER' END AS TYPE,")
            //If Biz = 2 Then
            //    If Process = 1 Then
            //        sb.Append("            CASE WHEN BST.CARGO_TYPE=1 THEN ")
            //    ElseIf Process = 2 Then
            //        sb.Append("            CASE WHEN JOB.CARGO_TYPE=1 THEN ")
            //    End If
            //    sb.Append("                (CASE")
            //    sb.Append("                  WHEN JOBFRT.CONTAINER_TYPE_MST_FK IS NOT NULL THEN")
            //    sb.Append("                   CON.CONTAINER_TYPE_MST_ID")
            //    sb.Append("                  ELSE")
            //    sb.Append("                   ' '")
            //    sb.Append("                END) ")
            //    If Process = 1 Then
            //        sb.Append("                WHEN BST.CARGO_TYPE=2 THEN (SELECT DIMEN.DIMENTION_ID FROM DIMENTION_UNIT_MST_TBL DIMEN WHERE DIMEN.DIMENTION_UNIT_MST_PK=JOBFRT.BASIS)")
            //    ElseIf Process = 2 Then
            //        sb.Append("                WHEN JOB.CARGO_TYPE=2 THEN (SELECT DIMEN.DIMENTION_ID FROM DIMENTION_UNIT_MST_TBL DIMEN WHERE DIMEN.DIMENTION_UNIT_MST_PK=JOBFRT.BASIS)")
            //    End If
            //    sb.Append("                ELSE 'BB'  END UNIT,")
            //Else
            //    sb.Append("                ' ' UNIT,")
            //End If
            //sb.Append("                ' ' FRT_OTH_ELEMENT,")
            //sb.Append("                ROUND((CASE NVL(TRN.ELEMENT_AMT,0) ")
            //sb.Append("                        WHEN 0 THEN")
            //sb.Append("                         1")
            //sb.Append("                        ELSE")
            //sb.Append("                         NVL(TRN.AMT_IN_INV_CURR,0) / TRN.ELEMENT_AMT")
            //sb.Append("                      END),")
            //sb.Append("                      6) AS EXCHANGE_RATE,")
            sb.Append("                (SELECT SUM(NVL(TRN1.AMT_IN_INV_CURR,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS INV_TOTAL,");
            sb.Append("                (SELECT SUM(NVL(TRN1.TAX_AMT,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS VAT_TOTAL,");
            sb.Append("                (SELECT SUM(NVL(TRN1.TOT_AMT,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS TOTAL,");
            sb.Append("                NVL(HDR.DISCOUNT_AMT,0) DISCOUNT_AMT, ");
            sb.Append("                (SELECT SUM(NVL(TRN1.TOT_AMT,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS NET_REC,");
            sb.Append("                NVL(HDR.REMARKS,' ') REMARKS ");
            sb.Append("  FROM CONSOL_INVOICE_TRN_TBL  TRN,");
            sb.Append("       CONSOL_INVOICE_TBL      HDR,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
            if (Biz == 2)
            {
                sb.Append("       CONTAINER_TYPE_MST_TBL  CON,");
                if (Process == 1)
                {
                    sb.Append("       JOB_TRN_SEA_EXP_FD      JOBFRT,");
                    sb.Append("       JOB_CARD_SEA_EXP_TBL    JOB,");
                    sb.Append("       BOOKING_SEA_TBL         BST,");
                }
                else if (Process == 2)
                {
                    sb.Append("       JOB_TRN_SEA_IMP_FD      JOBFRT,");
                    sb.Append("       JOB_CARD_SEA_IMP_TBL    JOB,");
                }
            }
            else if (Biz == 1)
            {
                if (Process == 1)
                {
                    sb.Append("       JOB_TRN_AIR_EXP_FD      JOBFRT,");
                    sb.Append("       JOB_CARD_AIR_EXP_TBL    JOB,");
                }
                else if (Process == 2)
                {
                    sb.Append("       JOB_TRN_AIR_IMP_FD      JOBFRT,");
                    sb.Append("       JOB_CARD_AIR_IMP_TBL    JOB,");
                }
            }
            sb.Append("       PARAMETERS_TBL          PAR");
            sb.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK");
            sb.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            if (Biz == 2)
            {
                if (Process == 1)
                {
                    sb.Append("   AND BST.BOOKING_SEA_PK=JOB.BOOKING_SEA_FK ");
                    sb.Append("   AND JOB.JOB_CARD_SEA_EXP_PK = TRN.JOB_CARD_FK");
                    sb.Append("   AND TRN.JOB_CARD_FK = JOBFRT.JOB_CARD_SEA_EXP_FK");
                }
                else if (Process == 2)
                {
                    sb.Append("   AND JOB.JOB_CARD_SEA_IMP_PK = TRN.JOB_CARD_FK");
                    sb.Append("   AND TRN.JOB_CARD_FK = JOBFRT.JOB_CARD_SEA_IMP_FK");
                }
                sb.Append("   AND (JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
                sb.Append("       JOBFRT.CONTAINER_TYPE_MST_FK IS NULL)");
            }
            else if (Biz == 1)
            {
                if (Process == 1)
                {
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = TRN.JOB_CARD_FK");
                    sb.Append("   AND TRN.JOB_CARD_FK = JOBFRT.JOB_CARD_AIR_EXP_FK");
                }
                else if (Process == 2)
                {
                    sb.Append("   AND JOB.JOB_CARD_AIR_IMP_PK = TRN.JOB_CARD_FK");
                    sb.Append("   AND TRN.JOB_CARD_FK = JOBFRT.JOB_CARD_AIR_IMP_FK");
                }
            }
            sb.Append("   AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK = TRN.CONSOL_INVOICE_TRN_PK");
            sb.Append("   AND JOBFRT.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK");
            sb.Append("   AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+)");
            sb.Append("   AND HDR.CONSOL_INVOICE_PK IN (" + InvPks + ")");

            sb.Append(" UNION ");

            sb.Append("SELECT " + Biz_Process + " HDR.INVOICE_REF_NO,");
            sb.Append("       TO_CHAR(HDR.INVOICE_DATE,DATEFORMAT) INVOICE_DATE,");
            sb.Append("       TO_CHAR(HDR.INVOICE_DUE_DATE,DATEFORMAT) INVOICE_DUE_DATE,");
            sb.Append("       UPPER(NVL('''' || HDR.INV_UNIQUE_REF_NR,' ')) BANK_REF_NR,");
            sb.Append("       (SELECT CMT.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK=HDR.CUSTOMER_MST_FK) CUSTOMER_CODE,");
            sb.Append("       (SELECT CMT.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK=HDR.CUSTOMER_MST_FK) CUSTOMER_NAME,");
            sb.Append("       (SELECT CTM.CURRENCY_ID FROM CURRENCY_TYPE_MST_TBL CTM WHERE CTM.CURRENCY_MST_PK=HDR.CURRENCY_MST_FK) INV_CURR,");
            sb.Append("       DECODE(HDR.CHK_INVOICE,0,'Pending',1,'Approved') STATUS,");
            sb.Append("       NVL(JOB.JOBCARD_REF_NO,' ') JOBCARD_REF_NO,");
            sb.Append("       TO_CHAR(JOB.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
            sb.Append("       FMT.FREIGHT_ELEMENT_ID ELEMENT_CODE,");
            sb.Append("       FMT.FREIGHT_ELEMENT_NAME ELEMENT_NAME,");
            sb.Append("       DECODE(JOB.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE,");
            sb.Append("       NVL(CUMT.CURRENCY_ID,' ') CURRENCY,");
            sb.Append("       NVL(TRN.ELEMENT_AMT,0.0) AS AMOUNT,");
            sb.Append("                GET_EX_RATE(TRN.CURRENCY_MST_FK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",JOB.JOBCARD_DATE) ROE,");
            sb.Append("       NVL(TRN.AMT_IN_INV_CURR,0) AS INV_AMOUNT,");
            sb.Append("       NVL(TRN.VAT_CODE,' ') AS VAT_CODE,");
            sb.Append("       TRN.TAX_PCNT AS VAT_PERCENT,");
            sb.Append("       NVL(TRN.TAX_AMT,0) AS VAT_AMOUNT,");
            sb.Append("       NVL(TRN.TOT_AMT,0) AS TOTAL_AMOUNT,");
            //sb.Append("       CASE WHEN TRN.FRT_OTH_ELEMENT_FK IS NOT NULL THEN ")
            //sb.Append("       (CASE WHEN TRN.FRT_OTH_ELEMENT_FK < 3 THEN ")
            //sb.Append("       DECODE(TRN.FRT_OTH_ELEMENT_FK, 1, 'COST', 2, 'FREIGHT') ELSE 'OTHER' END) ELSE 'OTHER' END AS TYPE,")
            //sb.Append("       'Oth.Chrg' AS UNIT,")
            //sb.Append("       NVL(UPPER(TRN.FRT_OTH_ELEMENT),' ') FRT_OTH_ELEMENT,")
            //sb.Append("       ROUND((CASE NVL(TRN.ELEMENT_AMT,0)")
            //sb.Append("               WHEN 0 THEN")
            //sb.Append("                1")
            //sb.Append("               ELSE")
            //sb.Append("                NVL(TRN.AMT_IN_INV_CURR,0) / TRN.ELEMENT_AMT")
            //sb.Append("             END),")
            //sb.Append("             6) AS EXCHANGE_RATE,")
            sb.Append("                (SELECT SUM(NVL(TRN1.AMT_IN_INV_CURR,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS INV_TOTAL,");
            sb.Append("                (SELECT SUM(NVL(TRN1.TAX_AMT,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS VAT_TOTAL,");
            sb.Append("                (SELECT SUM(NVL(TRN1.TOT_AMT,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS TOTAL,");
            sb.Append("       NVL(HDR.DISCOUNT_AMT,0) DISCOUNT_AMT, ");
            sb.Append("                (SELECT SUM(NVL(TRN1.TOT_AMT,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS NET_REC,");
            sb.Append("       NVL(HDR.REMARKS,' ') REMARKS ");
            sb.Append("  FROM CONSOL_INVOICE_TRN_TBL   TRN,");
            sb.Append("       CONSOL_INVOICE_TBL       HDR,");
            sb.Append("       CURRENCY_TYPE_MST_TBL    CUMT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL  FMT,");
            if (Biz == 2)
            {
                if (Process == 1)
                {
                    sb.Append("       JOB_TRN_SEA_EXP_OTH_CHRG JOBOTH,");
                    sb.Append("       JOB_CARD_SEA_EXP_TBL     JOB,");
                }
                else if (Process == 2)
                {
                    sb.Append("       JOB_TRN_SEA_IMP_OTH_CHRG JOBOTH,");
                    sb.Append("       JOB_CARD_SEA_IMP_TBL     JOB,");
                }
            }
            else if (Biz == 1)
            {
                if (Process == 1)
                {
                    sb.Append("       JOB_TRN_AIR_EXP_OTH_CHRG JOBOTH,");
                    sb.Append("       JOB_CARD_AIR_EXP_TBL     JOB,");
                }
                else if (Process == 2)
                {
                    sb.Append("       JOB_TRN_AIR_IMP_OTH_CHRG JOBOTH,");
                    sb.Append("       JOB_CARD_AIR_IMP_TBL     JOB,");
                }
            }
            sb.Append("       PARAMETERS_TBL           PAR");
            sb.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK");
            sb.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            sb.Append("   AND JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
            if (Biz == 2)
            {
                if (Process == 1)
                {
                    sb.Append("   AND JOB.JOB_CARD_SEA_EXP_PK = TRN.JOB_CARD_FK");
                    sb.Append("   AND TRN.JOB_CARD_FK = JOBOTH.JOB_CARD_SEA_EXP_FK");
                }
                else if (Process == 2)
                {
                    sb.Append("   AND JOB.JOB_CARD_SEA_IMP_PK = TRN.JOB_CARD_FK");
                    sb.Append("   AND TRN.JOB_CARD_FK = JOBOTH.JOB_CARD_SEA_IMP_FK");
                }
            }
            else if (Biz == 1)
            {
                if (Process == 1)
                {
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = TRN.JOB_CARD_FK");
                    sb.Append("   AND TRN.JOB_CARD_FK = JOBOTH.JOB_CARD_AIR_EXP_FK");
                }
                else if (Process == 2)
                {
                    sb.Append("   AND JOB.JOB_CARD_AIR_IMP_PK = TRN.JOB_CARD_FK");
                    sb.Append("   AND TRN.JOB_CARD_FK = JOBOTH.JOB_CARD_AIR_IMP_FK");
                }
            }
            sb.Append("   AND JOBOTH.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK");
            sb.Append("   AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+)");
            sb.Append("   AND HDR.CONSOL_INVOICE_PK IN (" + InvPks + ")");
            sb.Append("");
            DataSet dsJob = new DataSet();
            dsJob = objWF.GetDataSet(sb.ToString());
            try
            {
                foreach (DataRow _row in dsJob.Tables[0].Rows)
                {
                    foreach (DataColumn col in dsJob.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return dsJob;
        }

        public DataSet GetInvJobDetails(string InvPks, int Biz, int Process, string CargoType = "")
        {
            DataSet dsInvoiceDetails = new DataSet();
            dsInvoiceDetails.Tables.Add();
            DataSet dsJob = new DataSet();
            dsJob = GetInvJobDetailsForXML(InvPks, Biz, Process);
            DataTable InvHeader = new DataTable();
            InvHeader = FetchInvoiceHdr(InvPks, Biz, Process);
            //Fetching Columns for Invoice freight details
            foreach (DataColumn col in InvHeader.Columns)
            {
                string ColName = col.ColumnName.ToUpper();
                try
                {
                    if (ColName == "CONSOL_INVOICE_PK")
                    {
                    }
                    else
                    {
                        dsInvoiceDetails.Tables[0].Columns.Add(col.ColumnName, col.DataType);
                    }
                }
                catch (Exception ex)
                {
                }
            }
            DataTable InvDt = new DataTable();
            InvDt = FetchInvoiceData(InvPks, Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]), Convert.ToInt16(Biz), Convert.ToInt16(Process));
            //Fetching Columns for Invoice freight details
            foreach (DataColumn col in InvDt.Columns)
            {
                string ColName = col.ColumnName.ToUpper();
                try
                {
                    //
                    if (ColName == "PK" | ColName == "JOBCARD_FK" | ColName == "ELEMENT_FK" | ColName == "CURRENCY_MST_FK" | ColName == "REMARKS" | ColName == "MODE" | ColName == "CHK" | ColName == "FRT_BOF_FK" | ColName == "CONSOL_INVOICE_FK" | ColName == "CURR" | ColName == "FREIGHT_OR_OTH" | ColName == "TYPE")
                    {
                    }
                    else
                    {
                        dsInvoiceDetails.Tables[0].Columns.Add(col.ColumnName, col.DataType);
                    }
                }
                catch (Exception ex)
                {
                }
            }
            DataTable InvAmtDet = new DataTable();
            InvAmtDet = InvoiceAmountSummary(InvPks);
            //Fetching overall amount summary
            foreach (DataColumn col in InvAmtDet.Columns)
            {
                string ColName = col.ColumnName.ToUpper();
                try
                {
                    if (ColName == "CONSOL_INVOICE_PK")
                    {
                    }
                    else
                    {
                        dsInvoiceDetails.Tables[0].Columns.Add(col.ColumnName, col.DataType);
                    }
                }
                catch (Exception ex)
                {
                }
            }
            //---------------------------------------------------------------------
            foreach (DataRow drInvHdr in InvHeader.Rows)
            {
                int _invPk = Convert.ToInt32(drInvHdr["CONSOL_INVOICE_PK"]);
                InvDt = FetchInvoiceData(Convert.ToString(_invPk), Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]), Convert.ToInt16(Biz), Convert.ToInt16(Process));
                InvAmtDet = InvoiceAmountSummary(Convert.ToString(_invPk));
                foreach (DataRow drInvFrtHdr in InvDt.Rows)
                {
                    foreach (DataRow drInvFooter in InvAmtDet.Rows)
                    {
                        DataRow drInvDt = null;
                        drInvDt = dsInvoiceDetails.Tables[0].NewRow();
                        foreach (DataColumn dcInvHdr in InvHeader.Columns)
                        {
                            try
                            {
                                drInvDt[dcInvHdr.ColumnName] = drInvHdr[dcInvHdr.ColumnName];
                            }
                            catch (Exception ex)
                            {
                            }
                        }
                        foreach (DataColumn dcInvFrt in InvDt.Columns)
                        {
                            try
                            {
                                drInvDt[dcInvFrt.ColumnName] = drInvFrtHdr[dcInvFrt.ColumnName];
                            }
                            catch (Exception ex)
                            {
                            }
                        }
                        foreach (DataColumn dcInvFooter in InvAmtDet.Columns)
                        {
                            try
                            {
                                drInvDt[dcInvFooter.ColumnName] = drInvFooter[dcInvFooter.ColumnName];
                            }
                            catch (Exception ex)
                            {
                            }
                        }
                        //drInvDt("BANK_REF_NR") = "'" & drInvHdr("BANK_REF_NR")
                        dsInvoiceDetails.Tables[0].Rows.Add(drInvDt);
                    }
                }
            }
            return dsInvoiceDetails;
        }

        public DataSet GetInvJobDetailsForXML(string InvPks, int Biz, int Process, string CargoType = "")
        {
            //Dim Biz_Process As String = ""
            DataSet dsJobDetails = new DataSet();
            DataTable InvDetails = new DataTable();
            DataTable JobDetails = new DataTable();
            DataTable FreightDetails = new DataTable();
            DataTable FooterDetails = new DataTable();
            string JobcardPks = "";
            string AmountFormat = "#0.00";
            //Biz_Process = "SELECT DISTINCT "
            //If Biz = 2 Then 'Sea
            //    Biz_Process = " 'SEA' BIZ_TYPE, "
            //ElseIf Biz = 1 Then 'Air
            //    Biz_Process = " 'AIR' BIZ_TYPE, "
            //End If
            //If Process = 1 Then 'Export
            //    Biz_Process &= " 'EXPORT' PROCESS_TYPE, "
            //ElseIf Process = 2 Then 'Import
            //    Biz_Process &= " 'IMPORT' PROCESS_TYPE, "
            //End If
            StringBuilder sb = new StringBuilder(5000);
            //Get Invoice Header
            InvDetails = FetchInvoiceHdr(InvPks, Biz, Process);
            //-------------------------------------------------------------------------
            sb = new StringBuilder();
            sb.Append("SELECT DISTINCT HDR.CONSOL_INVOICE_PK ");
            sb.Append("  FROM CONSOL_INVOICE_TBL      HDR ");
            sb.Append(" WHERE HDR.CONSOL_INVOICE_PK IN (" + InvPks + ")");

            DataTable InvoiceFrtHDR = new DataTable();
            InvoiceFrtHDR = objWF.GetDataTable(sb.ToString());
            InvoiceFrtHDR.Columns["CONSOL_INVOICE_PK"].ColumnMapping = MappingType.Hidden;

            DataTable InvoiceFrtDetails = new DataTable();
            InvoiceFrtDetails = FetchInvoiceData(InvPks, Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]), Convert.ToInt16(Biz), Convert.ToInt16(Process));
            InvoiceFrtDetails.Columns["TYPE"].ColumnMapping = MappingType.Hidden;
            //-------------------------------------------------------------------------

            //Footer Details
            FooterDetails = InvoiceAmountSummary(InvPks);
            //-------------------------------------------------------------------------

            dsJobDetails.Tables.Clear();
            dsJobDetails.Relations.Clear();
            dsJobDetails.Tables.Add(InvDetails);
            dsJobDetails.Tables[0].TableName = "Invoice_Header";
            //Uncomment the below code to display the jobcard details
            //************************************************************************************
            //Dim _counter As Integer = 0
            //For Each _inv As DataRow In InvDetails.Rows
            //    Dim _invPk As Integer = _inv("CONSOL_INVOICE_PK")
            //    Dim InvRefNo As String = _inv("INVOICE_REF_NO")
            //    JobDetails = GetJobDetails(_invPk, Biz, Process)
            //    JobDetails.Columns("CONSOL_INVOICE_FK").ColumnMapping = MappingType.Hidden
            //    JobDetails.Columns("JOB_CARD_PK").ColumnMapping = MappingType.Hidden
            //    '-------------------------------------------------
            //    Dim UniqueNameJob As String = "JobDetails_" & InvRefNo
            //    Dim UniqueNameJobFreight As String = "JobFrtDetails_" & InvRefNo

            //    dsJobDetails.Tables.Add(JobDetails)
            //    dsJobDetails.Tables(dsJobDetails.Tables.Count - 1).TableName = UniqueNameJob
            //    'Creating relation between invoice-header and job details tables
            //    Dim _rel As New DataRelation("InvJob" & _counter, dsJobDetails.Tables("Invoice_Header").Columns("CONSOL_INVOICE_PK"), dsJobDetails.Tables(dsJobDetails.Tables.Count - 1).Columns("CONSOL_INVOICE_FK"))
            //    _rel.Nested = True
            //    dsJobDetails.Tables("Invoice_Header").ChildRelations.Add(_rel)

            //    JobcardPks = GetJobPksOfInvoice(_invPk, Biz, Process)
            //    FreightDetails = FetchConsolidatable(JobcardPks, Biz, Process)
            //    'For Each _row As DataRow In FreightDetails.Rows
            //    '    _row("FREIGHT_AMT") = Format(AmountFormat, IIf(IsDBNull(_row("FREIGHT_AMT")), 0, _row("FREIGHT_AMT")))
            //    '    _row("INV_AMT") = Format(AmountFormat, IIf(IsDBNull(_row("INV_AMT")), 0, _row("INV_AMT")))
            //    'Next
            //    FreightDetails.AcceptChanges()
            //    dsJobDetails.Tables.Add(FreightDetails)
            //    dsJobDetails.Tables(dsJobDetails.Tables.Count - 1).TableName = UniqueNameJobFreight
            //    'Creating relation between job details and job freight details tables
            //    Dim _rel1 As New DataRelation("JobFreight" & _counter, dsJobDetails.Tables(UniqueNameJob).Columns("JOBCARD_REF_NO"), dsJobDetails.Tables(UniqueNameJobFreight).Columns("JOBCARD_REF_NO"))
            //    _rel1.Nested = True
            //    dsJobDetails.Tables(UniqueNameJob).ChildRelations.Add(_rel1)
            //    _counter += 1
            //Next
            //InvDetails.AcceptChanges()
            //************************************************************************************
            dsJobDetails.Tables.Add(InvoiceFrtDetails);
            dsJobDetails.Tables[dsJobDetails.Tables.Count - 1].TableName = "Inv_Frt_Details";

            dsJobDetails.Tables.Add(FooterDetails);
            dsJobDetails.Tables[dsJobDetails.Tables.Count - 1].TableName = "Footer_Details";

            dsJobDetails.Tables.Add(InvoiceFrtHDR);
            dsJobDetails.Tables[dsJobDetails.Tables.Count - 1].TableName = "Invoice_Details";

            DataRelation rel2 = new DataRelation("InvFrt", dsJobDetails.Tables["Invoice_Header"].Columns["CONSOL_INVOICE_PK"], dsJobDetails.Tables["Invoice_Details"].Columns["CONSOL_INVOICE_PK"]);
            DataRelation rel3 = new DataRelation("InvFooter", dsJobDetails.Tables["Invoice_Header"].Columns["CONSOL_INVOICE_PK"], dsJobDetails.Tables["Footer_Details"].Columns["CONSOL_INVOICE_PK"]);
            DataRelation rel4 = new DataRelation("InvFrtHdr", dsJobDetails.Tables["Invoice_Details"].Columns["CONSOL_INVOICE_PK"], dsJobDetails.Tables["Inv_Frt_Details"].Columns["CONSOL_INVOICE_FK"]);
            rel2.Nested = true;
            rel3.Nested = true;
            rel4.Nested = true;
            dsJobDetails.Tables["Invoice_Header"].ChildRelations.Add(rel2);
            dsJobDetails.Tables["Invoice_Header"].ChildRelations.Add(rel3);
            dsJobDetails.Tables["Invoice_Details"].ChildRelations.Add(rel4);
            dsJobDetails.DataSetName = "INVOICE_EDI";
            dsJobDetails.AcceptChanges();
            return dsJobDetails;
        }

        public DataTable FetchInvoiceHdr(string InvPks, int BIZ, int Process)
        {
            StringBuilder sb = new StringBuilder();
            string Biz_Process = "";
            Biz_Process = "SELECT DISTINCT ";
            //Sea
            if (BIZ == 2)
            {
                Biz_Process = " 'SEA' BIZ_TYPE, ";
                //Air
            }
            else if (BIZ == 1)
            {
                Biz_Process = " 'AIR' BIZ_TYPE, ";
            }
            //Export
            if (Process == 1)
            {
                Biz_Process += " 'EXPORT' PROCESS_TYPE, ";
                //Import
            }
            else if (Process == 2)
            {
                Biz_Process += " 'IMPORT' PROCESS_TYPE, ";
            }
            //Get Invoice Details
            sb.Append("SELECT DISTINCT " + Biz_Process + " HDR.CONSOL_INVOICE_PK, HDR.INVOICE_REF_NO, ");
            sb.Append("                TO_CHAR(HDR.INVOICE_DATE,DATEFORMAT) INVOICE_DATE,");
            sb.Append("                TO_CHAR(HDR.INVOICE_DUE_DATE,DATEFORMAT) INVOICE_DUE_DATE,");
            sb.Append("                TO_CHAR(NVL(HDR.INV_UNIQUE_REF_NR,' ')) BANK_REF_NR,");
            sb.Append("                (SELECT CMT.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK=HDR.CUSTOMER_MST_FK) CUSTOMER_CODE,");
            sb.Append("                (SELECT CMT.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK=HDR.CUSTOMER_MST_FK) CUSTOMER_NAME,");
            sb.Append("                (SELECT CTM.CURRENCY_ID FROM CURRENCY_TYPE_MST_TBL CTM WHERE CTM.CURRENCY_MST_PK=HDR.CURRENCY_MST_FK) INV_CURR,");
            //sb.Append("                NVL(CUMT.CURRENCY_ID,' ') CURRENCY,")
            sb.Append("                DECODE(HDR.CHK_INVOICE,0,'Pending',1,'Approved') STATUS");
            sb.Append("  FROM CONSOL_INVOICE_TBL      HDR,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CUMT ");
            sb.Append(" WHERE HDR.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            sb.Append("   AND HDR.CONSOL_INVOICE_PK IN (" + InvPks + ")");
            //sb.Append(" UNION ")
            //sb.Append("SELECT " & Biz_Process & " HDR.CONSOL_INVOICE_PK, HDR.INVOICE_REF_NO,")
            //sb.Append("       TO_CHAR(HDR.INVOICE_DATE,DATEFORMAT) INVOICE_DATE,")
            //sb.Append("       TO_CHAR(HDR.INVOICE_DUE_DATE,DATEFORMAT) INVOICE_DUE_DATE,")
            //sb.Append("       UPPER(NVL(HDR.INV_UNIQUE_REF_NR,' ')) BANK_REF_NR,")
            //sb.Append("       (SELECT CMT.CUSTOMER_ID FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK=HDR.CUSTOMER_MST_FK) CUSTOMER_CODE,")
            //sb.Append("       (SELECT CMT.CUSTOMER_NAME FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK=HDR.CUSTOMER_MST_FK) CUSTOMER_NAME,")
            //sb.Append("       (SELECT CTM.CURRENCY_ID FROM CURRENCY_TYPE_MST_TBL CTM WHERE CTM.CURRENCY_MST_PK=HDR.CURRENCY_MST_FK) INV_CURR,")
            //'sb.Append("       NVL(CUMT.CURRENCY_ID,' ') CURRENCY,")
            //sb.Append("       DECODE(HDR.CHK_INVOICE,0,'Pending',1,'Approved') STATUS")
            //sb.Append("  FROM CONSOL_INVOICE_TBL       HDR,")
            //sb.Append("       CURRENCY_TYPE_MST_TBL    CUMT ")
            //sb.Append(" WHERE HDR.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK")
            //sb.Append("   AND HDR.CONSOL_INVOICE_PK IN (" & InvPks & ")")
            DataTable InvDetails = new DataTable();
            InvDetails = objWF.GetDataTable(sb.ToString());
            foreach (DataRow _inv in InvDetails.Rows)
            {
                int _invPk = Convert.ToInt32(_inv["CONSOL_INVOICE_PK"]);
                string InvRefNo = Convert.ToString(_inv["INVOICE_REF_NO"]);
                DataTable dtHdr = new DataTable();
                dtHdr = objConsInv.FetchHeader(_invPk).Tables[0];
                var _with14 = dtHdr.Rows[0];
                if (Convert.ToInt32(string.IsNullOrEmpty(_with14["CRCUS"].ToString()) ? 0 : Convert.ToInt32(_with14["CRCUS"].ToString())) == 1)
                {
                    _inv["INVOICE_DUE_DATE"] = DateAndTime.DateAdd(DateInterval.Day, (string.IsNullOrEmpty(_with14["CRDAYS"].ToString()) ? 0 : Convert.ToInt32(_with14["CRDAYS"].ToString())) + 15, Convert.ToDateTime(_with14["CDATE"]));
                }
                else
                {
                    _inv["INVOICE_DUE_DATE"] = DateAndTime.DateAdd(DateInterval.Day, 0 + 15, Convert.ToDateTime(_with14["CDATE"])).ToString(dateFormat);
                }
            }
            InvDetails.AcceptChanges();
            InvDetails.Columns["CONSOL_INVOICE_PK"].ColumnMapping = MappingType.Hidden;
            return InvDetails;
        }

        public DataTable InvoiceAmountSummary(string InvPks)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT DISTINCT HDR.CONSOL_INVOICE_PK, ");
            sb.Append("                NVL(HDR.REMARKS,' ') REMARKS, ");
            sb.Append("                (SELECT SUM(NVL(TRN1.AMT_IN_INV_CURR,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS INV_TOTAL,");
            sb.Append("                (SELECT SUM(NVL(TRN1.TAX_AMT,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS VAT_TOTAL,");
            sb.Append("                (SELECT SUM(NVL(TRN1.TOT_AMT,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS TOTAL,");
            sb.Append("                NVL(HDR.DISCOUNT_AMT,0) DISCOUNT_AMT, ");
            sb.Append("                (SELECT SUM(NVL(TRN1.TOT_AMT,0))");
            sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1");
            sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS NET_REC");
            sb.Append("  FROM CONSOL_INVOICE_TBL      HDR,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CUMT ");
            sb.Append(" WHERE HDR.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            sb.Append("   AND HDR.CONSOL_INVOICE_PK IN (" + InvPks + ")");
            //sb.Append(" UNION ")
            //sb.Append("SELECT HDR.CONSOL_INVOICE_PK,")
            //sb.Append("       NVL(HDR.REMARKS,' ') REMARKS, ")
            //sb.Append("       (SELECT SUM(NVL(TRN1.AMT_IN_INV_CURR,0))")
            //sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1")
            //sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS INV_TOTAL,")
            //sb.Append("       (SELECT SUM(NVL(TRN1.TAX_AMT,0))")
            //sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1")
            //sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS VAT_TOTAL,")
            //sb.Append("       (SELECT SUM(NVL(TRN1.TOT_AMT,0))")
            //sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1")
            //sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS TOTAL,")
            //sb.Append("       NVL(HDR.DISCOUNT_AMT,0) DISCOUNT_AMT, ")
            //sb.Append("                (SELECT SUM(NVL(TRN1.TOT_AMT,0))")
            //sb.Append("                   FROM CONSOL_INVOICE_TRN_TBL TRN1")
            //sb.Append("                   WHERE TRN1.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK) AS NET_REC")
            //sb.Append("  FROM CONSOL_INVOICE_TBL       HDR,")
            //sb.Append("       CURRENCY_TYPE_MST_TBL    CUMT ")
            //sb.Append(" WHERE HDR.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK")
            //sb.Append("   AND HDR.CONSOL_INVOICE_PK IN (" & InvPks & ")")
            DataTable FooterDetails = new DataTable();
            FooterDetails = objWF.GetDataTable(sb.ToString());
            FooterDetails.Columns["CONSOL_INVOICE_PK"].ColumnMapping = MappingType.Hidden;
            FooterDetails.AcceptChanges();
            return FooterDetails;
        }

        public DataTable GetTblWithStringCols(DataTable TBL)
        {
            DataTable dtNew = new DataTable();
            foreach (DataRow invRow in TBL.Rows)
            {
                if (dtNew.Columns.Count == 0)
                {
                    foreach (DataColumn col in TBL.Columns)
                    {
                        dtNew.Columns.Add(col.ColumnName, typeof(System.String));
                    }
                }
                DataRow drNew = dtNew.NewRow();
                foreach (DataColumn col in TBL.Columns)
                {
                    drNew[col.ColumnName] = invRow[col.ColumnName];
                }
                dtNew.Rows.Add(drNew);
            }
            return dtNew;
        }

        public DataTable GetJobDetails(string InvPks, int BIZ, int Process)
        {
            //Job Details
            StringBuilder sb = new StringBuilder();
            DataTable JobDetails = new DataTable();
            sb.Append(" SELECT DISTINCT TRN.CONSOL_INVOICE_FK, ");
            if (BIZ == 2 & Process == 1)
            {
                sb.Append(" JOB.JOB_CARD_SEA_EXP_PK JOB_CARD_PK, ");
            }
            else if (BIZ == 2 & Process == 2)
            {
                sb.Append(" JOB.JOB_CARD_SEA_IMP_PK JOB_CARD_PK, ");
            }
            else if (BIZ == 1 & Process == 1)
            {
                sb.Append(" JOB.JOB_CARD_AIR_EXP_PK JOB_CARD_PK, ");
            }
            else if (BIZ == 1 & Process == 2)
            {
                sb.Append(" JOB.JOB_CARD_AIR_IMP_PK JOB_CARD_PK, ");
            }
            sb.Append("                NVL(JOB.JOBCARD_REF_NO,' ') JOBCARD_REF_NO,");
            sb.Append("                TO_CHAR(JOB.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE");
            sb.Append("  FROM ");
            if (BIZ == 2)
            {
                if (Process == 1)
                {
                    sb.Append("       JOB_CARD_SEA_EXP_TBL    JOB,");
                }
                else if (Process == 2)
                {
                    sb.Append("       JOB_CARD_SEA_IMP_TBL    JOB,");
                }
            }
            else if (BIZ == 1)
            {
                if (Process == 1)
                {
                    sb.Append("       JOB_CARD_AIR_EXP_TBL    JOB,");
                }
                else if (Process == 2)
                {
                    sb.Append("       JOB_CARD_AIR_IMP_TBL    JOB,");
                }
            }
            sb.Append("       CONSOL_INVOICE_TRN_TBL  TRN ");
            //sb.Append("       CONSOL_INVOICE_TBL      HDR")
            sb.Append(" WHERE 1=1 ");
            //TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK
            if (BIZ == 2 & Process == 1)
            {
                sb.Append("   AND JOB.JOB_CARD_SEA_EXP_PK = TRN.JOB_CARD_FK");
            }
            else if (BIZ == 2 & Process == 2)
            {
                sb.Append("   AND JOB.JOB_CARD_SEA_IMP_PK = TRN.JOB_CARD_FK");
            }
            else if (BIZ == 1 & Process == 1)
            {
                sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = TRN.JOB_CARD_FK");
            }
            else if (BIZ == 1 & Process == 2)
            {
                sb.Append("   AND JOB.JOB_CARD_AIR_IMP_PK = TRN.JOB_CARD_FK");
            }
            sb.Append("   AND TRN.CONSOL_INVOICE_FK IN (" + InvPks + ")");
            //----------------------------------------------------------
            JobDetails = objWF.GetDataTable(sb.ToString());
            JobDetails.Columns["JOB_CARD_PK"].ColumnMapping = MappingType.Hidden;
            JobDetails.Columns["CONSOL_INVOICE_FK"].ColumnMapping = MappingType.Hidden;
            return JobDetails;
        }

        public string GetJobPksOfInvoice(string InvPks, int BIZ, int Process)
        {
            //Job Details
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT DISTINCT ");
            if (BIZ == 2 & Process == 1)
            {
                sb.Append(" JOB.JOB_CARD_SEA_EXP_PK JOB_CARD_PK, ");
            }
            else if (BIZ == 2 & Process == 2)
            {
                sb.Append(" JOB.JOB_CARD_SEA_IMP_PK JOB_CARD_PK, ");
            }
            else if (BIZ == 1 & Process == 1)
            {
                sb.Append(" JOB.JOB_CARD_AIR_EXP_PK JOB_CARD_PK, ");
            }
            else if (BIZ == 1 & Process == 2)
            {
                sb.Append(" JOB.JOB_CARD_AIR_IMP_PK JOB_CARD_PK, ");
            }
            sb.Append("                NVL(JOB.JOBCARD_REF_NO,' ') JOBCARD_REF_NO");
            sb.Append("  FROM ");
            if (BIZ == 2 & Process == 1)
            {
                sb.Append("       JOB_CARD_SEA_EXP_TBL    JOB,");
            }
            else if (BIZ == 2 & Process == 2)
            {
                sb.Append("       JOB_CARD_SEA_IMP_TBL    JOB,");
            }
            else if (BIZ == 1 & Process == 1)
            {
                sb.Append("       JOB_CARD_AIR_EXP_TBL    JOB,");
            }
            else if (BIZ == 1 & Process == 2)
            {
                sb.Append("       JOB_CARD_AIR_IMP_TBL    JOB,");
            }
            sb.Append("       CONSOL_INVOICE_TRN_TBL  TRN");
            sb.Append(" WHERE 1=1 ");
            if (BIZ == 2 & Process == 1)
            {
                sb.Append("   AND JOB.JOB_CARD_SEA_EXP_PK = TRN.JOB_CARD_FK");
            }
            else if (BIZ == 2 & Process == 2)
            {
                sb.Append("   AND JOB.JOB_CARD_SEA_IMP_PK = TRN.JOB_CARD_FK");
            }
            else if (BIZ == 1 & Process == 1)
            {
                sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = TRN.JOB_CARD_FK");
            }
            else if (BIZ == 1 & Process == 2)
            {
                sb.Append("   AND JOB.JOB_CARD_AIR_IMP_PK = TRN.JOB_CARD_FK");
            }
            sb.Append("   AND TRN.CONSOL_INVOICE_FK IN (" + InvPks + ")");
            DataTable JobDet = new DataTable();
            JobDet = objWF.GetDataTable(sb.ToString());
            string JobPks = "";
            foreach (DataRow row in JobDet.Rows)
            {
                JobPks += row["JOB_CARD_PK"] + ",";
            }
            if (!string.IsNullOrEmpty(JobPks))
            {
                JobPks = JobPks.Substring(0, JobPks.Length - 1);
            }
            return JobPks;
        }

        public DataTable FetchConsolidatable(string strJobPks, short BizType, short Process)
        {
            StringBuilder strBuilder = new StringBuilder();
            //Dim objWk As New WorkFlow
            if (BizType == 2 & Process == 1)
            {
                strBuilder.Append(" SELECT JOBCARD_REF_NO, JOBCARD_DATE, UNIT, JOB_TRN_SEA_EXP_FD_PK, JOBFK, FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PAYMENT_TYPE, CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK, FRT_BOF_FK FROM ( ");
                strBuilder.Append(" select JOBCARD_REF_NO ,JOBCARD_DATE,unit,JOB_TRN_SEA_EXP_FD_PK,JOBFK,FREIGHT_ELEMENT_MST_FK,");
                strBuilder.Append(" CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PAYMENT_TYPE,CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK,FRT_BOF_FK, PREFERENCE from ");
                strBuilder.Append(" ( SELECT distinct JOBFRT.FREIGHT_ELEMENT_MST_FK, ");
                strBuilder.Append(" JOB.JOBCARD_REF_NO,");
                strBuilder.Append(" JOB.JOBCARD_DATE,");
                strBuilder.Append(" ( case when jobfrt.container_type_mst_fk is not null then ");
                strBuilder.Append(" con.container_type_mst_id Else '1' end) UNIT,");
                strBuilder.Append(" JOBFRT.JOB_TRN_SEA_EXP_FD_PK,");
                strBuilder.Append(" JOBFRT.JOB_CARD_SEA_EXP_FK JOBFK,");
                strBuilder.Append(" JOBFRT.CURRENCY_MST_FK,");
                strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
                strBuilder.Append(" DECODE(JOBFRT.FREIGHT_TYPE,1,'Prepaid',2,'Collect') AS PAYMENT_TYPE,");
                strBuilder.Append(" CUMT.CURRENCY_ID,");
                strBuilder.Append(" JOBFRT.FREIGHT_AMT,");
                //Invoice Amount:
                strBuilder.Append(" (CASE ");
                //        If No Invoice then Invoice Amount is Null
                strBuilder.Append(" WHEN (JOBFRT.invoice_sea_tbl_fk IS NULL ");
                strBuilder.Append("   AND JOBFRT.INV_AGENT_TRN_SEA_EXP_FK IS NULL");
                //  If Edit = False Then
                strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ");
                // End' If
                strBuilder.Append(" ) THEN NULL ");
                //        If Consol. Invoice exists Fetch Invoice Amount from its Transaction
                strBuilder.Append(" WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT distinct ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2)  FROM CONSOL_INVOICE_TRN_TBL TRN ");
                strBuilder.Append("   WHERE TRN.FRT_OTH_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.FRT_OTH_ELEMENT=1  ");
                strBuilder.Append("   and TRN.CONSOL_INVOICE_TRN_PK=JOBFRT.CONSOL_INVOICE_TRN_FK ");
                strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_SEA_EXP_PK)");

                //       If Customer Invoice exists Fetch Invoice Amount from its Transacion
                strBuilder.Append(" WHEN JOBFRT.INVOICE_SEA_TBL_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_SEA_EXP_TBL TRN ");
                strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=2  ");
                strBuilder.Append("   AND TRN.INV_CUST_TRN_SEA_EXP_PK=JOBFRT.INVOICE_SEA_TBL_FK) ");

                //       If Agent Invoice exists Fetch Invoice Amount from its Transaction
                strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN");
                strBuilder.Append("    WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("    AND TRN.COST_FRT_ELEMENT=2 ");
                strBuilder.Append("    AND TRN.INV_AGENT_TRN_PK=JOBFRT.INV_AGENT_TRN_SEA_EXP_FK) END) INV_AMT,");

                strBuilder.Append(" (CASE WHEN (JOBFRT.INVOICE_SEA_TBL_FK IS NULL ");
                strBuilder.Append("            AND JOBFRT.INV_AGENT_TRN_SEA_EXP_FK IS NULL");
                strBuilder.Append("            AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL)");
                strBuilder.Append("            THEN 'False' ELSE 'True' END) CHK,PAR.FRT_BOF_FK,FMT.PREFERENCE");

                strBuilder.Append(" FROM ");
                strBuilder.Append(" JOB_CARD_SEA_EXP_TBL JOB, ");
                strBuilder.Append(" JOB_TRN_SEA_EXP_FD JOBFRT,");
                strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,CONTAINER_TYPE_MST_TBL CON,PARAMETERS_TBL PAR");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
                strBuilder.Append(" AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
                strBuilder.Append(" AND JOB.JOB_CARD_SEA_EXP_PK=JOBFRT.JOB_CARD_SEA_EXP_FK AND ( JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK or JOBFRT.CONTAINER_TYPE_MST_FK is null ) AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) ");
                //JOBFRT.FREIGHT_TYPE()
                //If Edit = False Then
                //    strBuilder.Append(" AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ")
                //End If

                strBuilder.Append(" AND JOBFRT.FREIGHT_TYPE=1 ");

                strBuilder.Append(" AND JOBFRT.JOB_CARD_SEA_EXP_FK IN (" + strJobPks + ")");
                //strBuilder.Append(" AND JOBFRT.frtpayer_cust_mst_fk in ( " & CustPk & ") ORDER BY unit,fmt.preference)")
                strBuilder.Append(" ORDER BY unit,fmt.preference)");
                strBuilder.Append(" UNION");
                //Other(Charges)
                strBuilder.Append("  SELECT ");
                strBuilder.Append(" JOB.JOBCARD_REF_NO, ");
                strBuilder.Append(" JOB.JOBCARD_DATE,'Oth.Chrg' UNIT,");
                strBuilder.Append(" JOBOTH.JOB_TRN_SEA_EXP_OTH_PK,");
                strBuilder.Append(" JOBOTH.JOB_CARD_SEA_EXP_FK JOBFK,");
                strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK,");
                strBuilder.Append(" JOBOTH.CURRENCY_MST_FK,");
                strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
                strBuilder.Append(" 'Prepaid' AS PAYMENT_TYPE,");
                strBuilder.Append(" CUMT.CURRENCY_ID,");
                strBuilder.Append(" JOBOTH.AMOUNT,");

                strBuilder.Append(" (CASE ");
                //If No Invoice then Invoice Amount is null
                strBuilder.Append(" WHEN (JOBOTH.INV_CUST_TRN_SEA_EXP_FK IS NULL ");
                strBuilder.Append("    AND JOBOTH.INV_AGENT_TRN_SEA_EXP_FK IS NULL ");
                //If Edit = False Then
                //    strBuilder.Append("   AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL ")
                //End If
                //strBuilder.Append(" ) THEN NULL ")
                strBuilder.Append("    AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL )THEN NULL ");

                //If Consolidated Invoice exists then : Inv Amount -> Select from Consol. Inv. Transaction table
                strBuilder.Append(" WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2)  FROM CONSOL_INVOICE_TRN_TBL TRN ");
                strBuilder.Append("    WHERE TRN.FRT_OTH_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("    AND TRN.FRT_OTH_ELEMENT=2  ");
                strBuilder.Append("   and TRN.CONSOL_INVOICE_TRN_PK=JOBOTH.CONSOL_INVOICE_TRN_FK ");
                strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_SEA_EXP_PK)");

                //If Customer Invoice exists then : Inv Amount -> Select from Customer. Inv. Transaction table
                strBuilder.Append(" WHEN JOBOTH.Inv_Cust_Trn_Sea_Exp_Fk IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_SEA_EXP_TBL TRN ");
                strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=3  ");
                strBuilder.Append("   AND TRN.INV_CUST_TRN_SEA_EXP_PK=JOBOTH.Inv_Cust_Trn_Sea_Exp_Fk) ");

                //If Customer Invoice exists then : Inv Amount -> Select from Customer. Inv. Transaction table
                strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN");
                strBuilder.Append("      WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("      AND TRN.COST_FRT_ELEMENT=3 ");
                strBuilder.Append("      AND TRN.INV_AGENT_TRN_PK=JOBOTH.INV_AGENT_TRN_SEA_EXP_FK) END) INV_AMT,");

                strBuilder.Append("(CASE WHEN (JOBOTH.INV_CUST_TRN_SEA_EXP_FK IS NULL ");
                strBuilder.Append(" AND JOBOTH.INV_AGENT_TRN_SEA_EXP_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL)");
                strBuilder.Append(" THEN 'False' ELSE 'True' END) CHK, PAR.FRT_BOF_FK, FMT.PREFERENCE ");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" JOB_CARD_SEA_EXP_TBL JOB,");
                strBuilder.Append(" JOB_TRN_SEA_EXP_OTH_CHRG JOBOTH,");
                strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT, PARAMETERS_TBL PAR");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
                strBuilder.Append(" AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
                strBuilder.Append(" AND JOB.JOB_CARD_SEA_EXP_PK= JOBOTH.JOB_CARD_SEA_EXP_FK");
                // added by jitendra
                strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) AND JOBOTH.Freight_Type=1");
                strBuilder.Append(" AND JOBOTH.JOB_CARD_SEA_EXP_FK IN(" + strJobPks + ")");
                //strBuilder.Append("   AND JOBOTH.frtpayer_cust_mst_fk in ( " & CustPk & ") ORDER BY unit, preference)")
                strBuilder.Append("   ORDER BY unit, preference)");
                //Air Export
            }
            else if (BizType == 1 & Process == 1)
            {
                strBuilder.Append(" SELECT JOBCARD_REF_NO, JOBCARD_DATE, UNIT, JOB_TRN_AIR_EXP_FD_PK, JOBFK, FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PAYMENT_TYPE, CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK, FRT_AFC_FK FROM ( ");
                strBuilder.Append(" SELECT ");
                strBuilder.Append(" JOB.JOBCARD_REF_NO,");
                strBuilder.Append(" JOB.JOBCARD_DATE, upper(JOBFRT.QUANTITY) UNIT,");
                strBuilder.Append(" JOBFRT.JOB_TRN_AIR_EXP_FD_PK,");
                strBuilder.Append(" JOBFRT.JOB_CARD_AIR_EXP_FK JOBFK,");

                strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK,");
                strBuilder.Append(" JOBFRT.CURRENCY_MST_FK,");
                strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
                strBuilder.Append(" DECODE(JOBFRT.FREIGHT_TYPE,1,'Prepaid',2,'Collect') AS PAYMENT_TYPE,");
                strBuilder.Append(" CUMT.CURRENCY_ID,");
                strBuilder.Append(" JOBFRT.FREIGHT_AMT,");

                //Invoice Amount:
                strBuilder.Append(" (CASE ");

                //        If No Invoice then Invoice Amount is Null
                strBuilder.Append(" WHEN (JOBFRT.INV_CUST_TRN_AIR_EXP_FK IS NULL ");
                strBuilder.Append("   AND JOBFRT.INV_AGENT_TRN_AIR_EXP_FK IS NULL");
                strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ) THEN NULL ");
                //If Edit = False Then
                //    strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ")
                //End If
                //strBuilder.Append(" ) THEN NULL ")

                //        If Consol. Invoice exists Fetch Invoice Amount from its Transaction
                strBuilder.Append(" WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT sum(ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2))  FROM CONSOL_INVOICE_TRN_TBL TRN ");
                strBuilder.Append("   WHERE TRN.FRT_OTH_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.FRT_OTH_ELEMENT=1  ");
                strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_AIR_EXP_PK)");

                //       If Customer Invoice exists Fetch Invoice Amount from its Transacion
                strBuilder.Append(" WHEN JOBFRT.INV_CUST_TRN_AIR_EXP_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_EXP_TBL TRN ");
                strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=2  ");
                strBuilder.Append("   AND TRN.INV_CUST_TRN_AIR_EXP_PK=JOBFRT.INV_CUST_TRN_AIR_EXP_FK) ");

                //       If Agent Invoice exists Fetch Invoice Amount from its Transaction
                strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN");
                strBuilder.Append("    WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("    AND TRN.COST_FRT_ELEMENT=2 ");
                strBuilder.Append("    AND TRN.INV_AGENT_TRN_PK=JOBFRT.INV_AGENT_TRN_AIR_EXP_FK) END) INV_AMT,");

                strBuilder.Append(" (CASE WHEN (JOBFRT.INV_CUST_TRN_AIR_EXP_FK IS NULL ");
                strBuilder.Append("            AND JOBFRT.INV_AGENT_TRN_AIR_EXP_FK IS NULL");
                strBuilder.Append("            AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL)");
                //If Edit = False Then
                //    strBuilder.Append(" AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ")
                //End If
                //strBuilder.Append(" )")

                strBuilder.Append("            THEN 'False' ELSE 'True' END) CHK, PAR.FRT_AFC_FK, FMT.PREFERENCE ");

                strBuilder.Append(" FROM ");
                strBuilder.Append(" JOB_CARD_AIR_EXP_TBL JOB, ");
                strBuilder.Append(" JOB_TRN_AIR_EXP_FD JOBFRT,");
                strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,PARAMETERS_TBL PAR");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
                strBuilder.Append(" AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
                strBuilder.Append(" AND JOB.JOB_CARD_AIR_EXP_PK=JOBFRT.JOB_CARD_AIR_EXP_FK");
                strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_AFC_FK(+)");

                //strBuilder.Append(" AND JOBFRT.FREIGHT_TYPE=1 ")
                //strBuilder.Append(" AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ")
                strBuilder.Append(" AND JOBFRT.JOB_CARD_AIR_EXP_FK IN (" + strJobPks + ")");
                //strBuilder.Append("   AND JOBFRT.frtpayer_cust_mst_fk in ( " & CustPk & ") ")
                strBuilder.Append(" UNION");
                //Other(Charges)
                strBuilder.Append("  SELECT ");
                strBuilder.Append(" JOB.JOBCARD_REF_NO, ");
                strBuilder.Append(" JOB.JOBCARD_DATE,'Oth.Chrg' UNIT, ");
                strBuilder.Append(" JOBOTH.JOB_TRN_AIR_EXP_OTH_PK,");
                strBuilder.Append(" JOBOTH.JOB_CARD_AIR_EXP_FK JOBFK,");
                strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK,");
                strBuilder.Append(" JOBOTH.CURRENCY_MST_FK,");
                strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
                strBuilder.Append(" 'Prepaid' AS PAYMENT_TYPE,");
                strBuilder.Append(" CUMT.CURRENCY_ID,");
                strBuilder.Append(" JOBOTH.AMOUNT,");

                strBuilder.Append(" (CASE ");
                //If No Invoice then Invoice Amount is null
                strBuilder.Append(" WHEN (JOBOTH.INV_CUST_TRN_AIR_EXP_FK IS NULL ");
                strBuilder.Append("    AND JOBOTH.INV_AGENT_TRN_AIR_EXP_FK IS NULL ");
                //If Edit = False Then
                //    strBuilder.Append("   AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL ")
                //End If
                strBuilder.Append("    AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL  ");
                strBuilder.Append(" ) THEN NULL ");
                // strBuilder.Append("    AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL THEN NULL ")

                //If Consolidated Invoice exists then : Inv Amount -> Select from Consol. Inv. Transaction table
                strBuilder.Append(" WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2)  FROM CONSOL_INVOICE_TRN_TBL TRN ");
                strBuilder.Append("    WHERE TRN.FRT_OTH_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("    AND TRN.FRT_OTH_ELEMENT=2  ");
                strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_AIR_EXP_PK)");

                //If Customer Invoice exists then : Inv Amount -> Select from Customer. Inv. Transaction table
                strBuilder.Append(" WHEN JOBOTH.Inv_Cust_Trn_AIR_Exp_Fk IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_EXP_TBL TRN ");
                strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=3  ");
                strBuilder.Append("   AND TRN.INV_CUST_TRN_AIR_EXP_PK=JOBOTH.Inv_Cust_Trn_AIR_Exp_Fk) ");

                //If Customer Invoice exists then : Inv Amount -> Select from Customer. Inv. Transaction table
                strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN");
                strBuilder.Append("      WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("      AND TRN.COST_FRT_ELEMENT=3 ");
                strBuilder.Append("      AND TRN.INV_AGENT_TRN_PK=JOBOTH.INV_AGENT_TRN_AIR_EXP_FK) END) INV_AMT,");

                strBuilder.Append("(CASE WHEN (JOBOTH.INV_CUST_TRN_AIR_EXP_FK IS NULL ");
                strBuilder.Append(" AND JOBOTH.INV_AGENT_TRN_AIR_EXP_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL)");
                strBuilder.Append(" THEN 'False' ELSE 'True' END) CHK, PAR.FRT_AFC_FK, FMT.PREFERENCE ");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" JOB_CARD_AIR_EXP_TBL JOB,");
                strBuilder.Append(" JOB_TRN_AIR_EXP_OTH_CHRG JOBOTH,");
                strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,PARAMETERS_TBL PAR");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
                strBuilder.Append(" AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
                strBuilder.Append(" AND JOB.JOB_CARD_AIR_EXP_PK= JOBOTH.JOB_CARD_AIR_EXP_FK");
                strBuilder.Append(" AND JOBOTH.JOB_CARD_AIR_EXP_FK IN(" + strJobPks + ")");
                strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_AFC_FK(+)");
                //strBuilder.Append("   AND JOBOTH.frtpayer_cust_mst_fk in ( " & CustPk & ")")
                strBuilder.Append(" AND JOBOTH.Freight_Type=1 ORDER BY unit,preference) ");
                //imports
            }
            else if (BizType == 2 & Process == 2)
            {
                strBuilder.Append(" SELECT JOBCARD_REF_NO, JOBCARD_DATE, UNIT, JOB_TRN_SEA_IMP_FD_PK, JOBFK, FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PAYMENT_TYPE, CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK, FRT_BOF_FK FROM ( ");
                strBuilder.Append(" select JOBCARD_REF_NO ,JOBCARD_DATE,unit,JOB_TRN_SEA_IMP_FD_PK,JOBFK,FREIGHT_ELEMENT_MST_FK,");
                strBuilder.Append(" CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PAYMENT_TYPE,CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK,FRT_BOF_FK, PREFERENCE from ");
                strBuilder.Append(" ( SELECT distinct JOBFRT.FREIGHT_ELEMENT_MST_FK, ");
                strBuilder.Append(" JOB.JOBCARD_REF_NO,");
                strBuilder.Append(" JOB.JOBCARD_DATE,");
                strBuilder.Append(" ( case when jobfrt.container_type_mst_fk is not null then ");
                strBuilder.Append(" con.container_type_mst_id Else '1' end) UNIT,");

                strBuilder.Append(" JOBFRT.JOB_TRN_SEA_IMP_FD_PK,");
                strBuilder.Append(" JOBFRT.JOB_CARD_SEA_IMP_FK JOBFK,");

                //strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK,")
                strBuilder.Append(" JOBFRT.CURRENCY_MST_FK,");
                strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
                strBuilder.Append(" DECODE(JOBFRT.FREIGHT_TYPE,1,'Prepaid',2,'Collect') AS PAYMENT_TYPE,");
                strBuilder.Append(" CUMT.CURRENCY_ID,");
                strBuilder.Append(" JOBFRT.FREIGHT_AMT,");

                //Invoice Amount:
                strBuilder.Append(" (CASE ");

                //        If No Invoice then Invoice Amount is Null
                strBuilder.Append(" WHEN (JOBFRT.INVOICE_SEA_TBL_FK IS NULL ");
                strBuilder.Append("   AND JOBFRT.INV_AGENT_TRN_SEA_IMP_FK IS NULL");
                strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ) THEN NULL ");
                //If Edit = False Then
                //    strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ")
                //End If
                //strBuilder.Append(" ) THEN NULL ")

                //        If Consol. Invoice exists Fetch Invoice Amount from its Transaction
                strBuilder.Append(" WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2)  FROM CONSOL_INVOICE_TRN_TBL TRN ");
                strBuilder.Append("   WHERE TRN.FRT_OTH_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.FRT_OTH_ELEMENT=1  ");
                strBuilder.Append("   and TRN.CONSOL_INVOICE_TRN_PK=JOBFRT.CONSOL_INVOICE_TRN_FK ");
                strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_sea_imp_PK)");

                //       If Customer Invoice exists Fetch Invoice Amount from its Transacion
                strBuilder.Append(" WHEN JOBFRT.INVOICE_SEA_TBL_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_SEA_IMP_TBL TRN ");
                strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=2  ");
                strBuilder.Append("   AND TRN.INV_CUST_TRN_SEA_IMP_PK=JOBFRT.INVOICE_SEA_TBL_FK) ");

                //       If Agent Invoice exists Fetch Invoice Amount from its Transaction
                strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN");
                strBuilder.Append("    WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("    AND TRN.COST_FRT_ELEMENT=2 ");
                strBuilder.Append("    AND TRN.INV_AGENT_TRN_PK=JOBFRT.INV_AGENT_TRN_SEA_IMP_FK) END) INV_AMT,");

                strBuilder.Append(" (CASE WHEN (JOBFRT.INVOICE_SEA_TBL_FK IS NULL ");
                strBuilder.Append("            AND JOBFRT.INV_AGENT_TRN_SEA_IMP_FK IS NULL");
                strBuilder.Append("            AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL)");
                //If Edit = False Then
                //    strBuilder.Append(" AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ")
                //End If
                //strBuilder.Append(" )")

                strBuilder.Append("            THEN 'False' ELSE 'True' END) CHK, PAR.FRT_BOF_FK , FMT.PREFERENCE ");

                strBuilder.Append(" FROM ");
                strBuilder.Append(" JOB_CARD_SEA_IMP_TBL JOB, ");
                strBuilder.Append(" JOB_TRN_SEA_IMP_FD JOBFRT,");
                strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,CONTAINER_TYPE_MST_TBL CON,PARAMETERS_TBL PAR");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
                strBuilder.Append(" AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
                strBuilder.Append(" AND JOB.JOB_CARD_SEA_IMP_PK=JOBFRT.JOB_CARD_SEA_IMP_FK AND ( JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK or JOBFRT.CONTAINER_TYPE_MST_FK is null ) ");
                //strBuilder.Append(" AND JOBFRT.FREIGHT_TYPE=2 ")
                //strBuilder.Append(" AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ")
                strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) AND JOBFRT.JOB_CARD_SEA_IMP_FK IN (" + strJobPks + ")");
                // added by jitendra on 22/05/07
                //strBuilder.Append("   AND JOBFRT.frtpayer_cust_mst_fk in ( " & CustPk & ") ORDER BY unit,preference)")
                strBuilder.Append("   ORDER BY unit,preference)");
                strBuilder.Append(" UNION");
                //Other(Charges)
                strBuilder.Append("  SELECT ");
                strBuilder.Append(" JOB.JOBCARD_REF_NO, ");
                strBuilder.Append(" JOB.JOBCARD_DATE,'Oth.Chrg' UNIT,");
                strBuilder.Append(" JOBOTH.JOB_TRN_SEA_IMP_OTH_PK,");
                strBuilder.Append(" JOBOTH.JOB_CARD_SEA_IMP_FK JOBFK,");
                strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK,");
                strBuilder.Append(" JOBOTH.CURRENCY_MST_FK,");
                strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
                strBuilder.Append(" 'Collect' AS PAYMENT_TYPE,");
                strBuilder.Append(" CUMT.CURRENCY_ID,");
                strBuilder.Append(" JOBOTH.AMOUNT,");

                strBuilder.Append(" (CASE ");
                //If No Invoice then Invoice Amount is null
                strBuilder.Append(" WHEN (JOBOTH.INV_CUST_TRN_SEA_IMP_FK IS NULL ");
                strBuilder.Append("    AND JOBOTH.INV_AGENT_TRN_SEA_IMP_FK IS NULL ");
                //If Edit = False Then
                //    strBuilder.Append("   AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL ")
                //End If
                //strBuilder.Append(" ) THEN NULL ")
                strBuilder.Append("    AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL ) THEN NULL ");

                //If Consolidated Invoice exists then : Inv Amount -> Select from Consol. Inv. Transaction table
                strBuilder.Append(" WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2)  FROM CONSOL_INVOICE_TRN_TBL TRN ");
                strBuilder.Append("    WHERE TRN.FRT_OTH_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("    AND TRN.FRT_OTH_ELEMENT=2  ");
                strBuilder.Append("   and TRN.CONSOL_INVOICE_TRN_PK=JOBOTH.CONSOL_INVOICE_TRN_FK ");
                strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_sea_imp_PK)");

                //If Customer Invoice exists then : Inv Amount -> Select from Customer. Inv. Transaction table
                strBuilder.Append(" WHEN JOBOTH.Inv_Cust_Trn_Sea_IMP_Fk IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_SEA_IMP_TBL TRN ");
                strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=3  ");
                strBuilder.Append("   AND TRN.INV_CUST_TRN_SEA_IMP_PK=JOBOTH.Inv_Cust_Trn_Sea_IMP_Fk) ");

                //If Customer Invoice exists then : Inv Amount -> Select from Customer. Inv. Transaction table
                strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN");
                strBuilder.Append("      WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("      AND TRN.COST_FRT_ELEMENT=3 ");
                strBuilder.Append("      AND TRN.INV_AGENT_TRN_PK=JOBOTH.INV_AGENT_TRN_SEA_IMP_FK) END) INV_AMT,");

                strBuilder.Append("(CASE WHEN (JOBOTH.INV_CUST_TRN_SEA_IMP_FK IS NULL ");
                strBuilder.Append(" AND JOBOTH.INV_AGENT_TRN_SEA_IMP_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL)");
                strBuilder.Append(" THEN 'False' ELSE 'True' END) CHK,PAR.FRT_BOF_FK , FMT.PREFERENCE");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" JOB_CARD_SEA_IMP_TBL JOB,");
                strBuilder.Append(" JOB_TRN_SEA_IMP_OTH_CHRG JOBOTH,");
                strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,PARAMETERS_TBL PAR");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
                strBuilder.Append(" AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
                strBuilder.Append(" AND JOB.JOB_CARD_SEA_IMP_PK= JOBOTH.JOB_CARD_SEA_IMP_FK");
                strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) AND JOBOTH.JOB_CARD_SEA_IMP_FK IN(" + strJobPks + ")");
                //strBuilder.Append(" AND JOBOTH.frtpayer_cust_mst_fk in ( " & CustPk & ")")
                strBuilder.Append(" AND JOBOTH.Freight_Type=2 ORDER BY unit,preference) ");
            }
            else if (BizType == 1 & Process == 2)
            {
                strBuilder.Append(" SELECT JOBCARD_REF_NO, JOBCARD_DATE, UNIT, JOB_TRN_AIR_IMP_FD_PK, JOBFK, FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PAYMENT_TYPE, CURRENCY_ID,FREIGHT_AMT, INV_AMT, CHK, FRT_AFC_FK FROM ( ");
                strBuilder.Append(" SELECT ");
                strBuilder.Append(" JOB.JOBCARD_REF_NO,");
                strBuilder.Append(" JOB.JOBCARD_DATE, upper(JOBFRT.QUANTITY) UNIT,");
                strBuilder.Append(" JOBFRT.JOB_TRN_AIR_IMP_FD_PK,");
                strBuilder.Append(" JOBFRT.JOB_CARD_AIR_IMP_FK JOBFK,");

                strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK,");
                strBuilder.Append(" JOBFRT.CURRENCY_MST_FK,");
                strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
                strBuilder.Append(" DECODE(JOBFRT.FREIGHT_TYPE,1,'Prepaid',2,'Collect') AS PAYMENT_TYPE,");
                strBuilder.Append(" CUMT.CURRENCY_ID,");
                strBuilder.Append(" JOBFRT.FREIGHT_AMT,");

                //Invoice Amount:
                strBuilder.Append(" (CASE ");

                //If No Invoice then Invoice Amount is Null
                strBuilder.Append(" WHEN (JOBFRT.INV_CUST_TRN_AIR_IMP_FK IS NULL ");
                strBuilder.Append("   AND JOBFRT.INV_AGENT_TRN_AIR_IMP_FK IS NULL");
                strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ) THEN NULL ");
                //If Edit = False Then
                //    strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ")
                //End If
                //strBuilder.Append(" ) THEN NULL ")

                //        If Consol. Invoice exists Fetch Invoice Amount from its Transaction
                strBuilder.Append(" WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT SUM(ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2))  FROM CONSOL_INVOICE_TRN_TBL TRN ");
                strBuilder.Append("   WHERE TRN.FRT_OTH_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.FRT_OTH_ELEMENT=1  ");
                strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_air_imp_PK)");

                //       If Customer Invoice exists Fetch Invoice Amount from its Transacion
                strBuilder.Append(" WHEN JOBFRT.INV_CUST_TRN_AIR_IMP_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_IMP_TBL TRN ");
                strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=2  ");
                strBuilder.Append("   AND TRN.INV_CUST_TRN_AIR_IMP_PK=JOBFRT.INV_CUST_TRN_AIR_IMP_FK) ");

                //       If Agent Invoice exists Fetch Invoice Amount from its Transaction
                strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN");
                strBuilder.Append("    WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("    AND TRN.COST_FRT_ELEMENT=2 ");
                strBuilder.Append("    AND TRN.INV_AGENT_TRN_PK=JOBFRT.INV_AGENT_TRN_AIR_IMP_FK) END) INV_AMT,");

                strBuilder.Append(" (CASE WHEN (JOBFRT.INV_CUST_TRN_AIR_IMP_FK IS NULL ");
                strBuilder.Append("            AND JOBFRT.INV_AGENT_TRN_AIR_IMP_FK IS NULL");
                strBuilder.Append("            AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL)");
                //If Edit = False Then
                //    strBuilder.Append(" AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ")
                //End If
                //strBuilder.Append(" )")

                strBuilder.Append("            THEN 'False' ELSE 'True' END) CHK, PAR.FRT_AFC_FK , FMT.PREFERENCE");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" JOB_CARD_AIR_IMP_TBL JOB, ");
                strBuilder.Append(" JOB_TRN_AIR_IMP_FD JOBFRT,");
                strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,PARAMETERS_TBL PAR");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
                strBuilder.Append(" AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
                strBuilder.Append(" AND JOB.JOB_CARD_AIR_IMP_PK=JOBFRT.JOB_CARD_AIR_IMP_FK");
                strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                //strBuilder.Append(" AND JOBFRT.FREIGHT_TYPE=2 ")
                //strBuilder.Append(" AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ")
                strBuilder.Append(" AND JOBFRT.JOB_CARD_AIR_IMP_FK IN (" + strJobPks + ")");
                // added by jitendra on 22/05/07
                //strBuilder.Append("   AND JOBFRT.frtpayer_cust_mst_fk in ( " & CustPk & ") ")
                strBuilder.Append(" UNION");
                //Other(Charges)
                strBuilder.Append("  SELECT ");
                strBuilder.Append(" JOB.JOBCARD_REF_NO, ");
                strBuilder.Append(" JOB.JOBCARD_DATE,'Oth.Chrg' UNIT ,");
                strBuilder.Append(" JOBOTH.JOB_TRN_AIR_IMP_OTH_PK,");
                strBuilder.Append(" JOBOTH.JOB_CARD_AIR_IMP_FK JOBFK,");
                strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK,");
                strBuilder.Append(" JOBOTH.CURRENCY_MST_FK,");
                strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
                strBuilder.Append(" 'Collect' AS PAYMENT_TYPE,");
                strBuilder.Append(" CUMT.CURRENCY_ID,");
                strBuilder.Append(" JOBOTH.AMOUNT,");

                strBuilder.Append(" (CASE ");
                //If No Invoice then Invoice Amount is null
                strBuilder.Append(" WHEN (JOBOTH.INV_CUST_TRN_AIR_IMP_FK IS NULL ");
                strBuilder.Append("    AND JOBOTH.INV_AGENT_TRN_AIR_IMP_FK IS NULL ");
                //If Edit = False Then
                //    strBuilder.Append("   AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL ")
                //End If
                strBuilder.Append("   AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL ");
                strBuilder.Append(" ) THEN NULL ");
                //strBuilder.Append("    AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL THEN NULL ")

                //If Consolidated Invoice exists then : Inv Amount -> Select from Consol. Inv. Transaction table
                strBuilder.Append(" WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT SUM(ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2))  FROM CONSOL_INVOICE_TRN_TBL TRN ");
                strBuilder.Append("    WHERE TRN.FRT_OTH_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("    AND TRN.FRT_OTH_ELEMENT=2  ");
                strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_air_imp_PK)");

                //If Customer Invoice exists then : Inv Amount -> Select from Customer. Inv. Transaction table
                strBuilder.Append(" WHEN JOBOTH.Inv_Cust_Trn_AIR_IMP_Fk IS NOT NULL THEN ");
                strBuilder.Append("   (SELECT TRN.TOT_AMT FROM INV_CUST_TRN_AIR_IMP_TBL TRN ");
                strBuilder.Append("   WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("   AND TRN.COST_FRT_ELEMENT=3  ");
                strBuilder.Append("   AND TRN.INV_CUST_TRN_AIR_IMP_PK=JOBOTH.Inv_Cust_Trn_AIR_IMP_Fk) ");

                //If Customer Invoice exists then : Inv Amount -> Select from Customer. Inv. Transaction table
                strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN");
                strBuilder.Append("      WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
                strBuilder.Append("      AND TRN.COST_FRT_ELEMENT=3 ");
                strBuilder.Append("      AND TRN.INV_AGENT_TRN_PK=JOBOTH.INV_AGENT_TRN_AIR_IMP_FK) END) INV_AMT,");

                strBuilder.Append("(CASE WHEN (JOBOTH.INV_CUST_TRN_AIR_IMP_FK IS NULL ");
                strBuilder.Append(" AND JOBOTH.INV_AGENT_TRN_AIR_IMP_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL)");
                strBuilder.Append(" THEN 'False' ELSE 'True' END) CHK ,PAR.FRT_AFC_FK , FMT.PREFERENCE");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" JOB_CARD_AIR_IMP_TBL JOB,");
                strBuilder.Append(" JOB_TRN_AIR_IMP_OTH_CHRG JOBOTH,");
                strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
                strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,PARAMETERS_TBL PAR");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
                strBuilder.Append(" AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
                strBuilder.Append(" AND JOB.JOB_CARD_AIR_IMP_PK= JOBOTH.JOB_CARD_AIR_IMP_FK");
                strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                strBuilder.Append(" AND JOBOTH.Freight_Type=2 ");
                strBuilder.Append(" AND JOBOTH.JOB_CARD_AIR_IMP_FK IN(" + strJobPks + ")");
                //strBuilder.Append("   AND JOBOTH.frtpayer_cust_mst_fk in ( " & CustPk & ") ORDER BY unit,preference )")
                strBuilder.Append("   ORDER BY unit,preference )");
            }
            try
            {
                DataTable DT = new DataTable();
                DT = objWF.GetDataTable(strBuilder.ToString());
                DT.Columns["JOBCARD_REF_NO"].ColumnMapping = MappingType.Hidden;
                DT.Columns["JOBCARD_DATE"].ColumnMapping = MappingType.Hidden;
                if (BizType == 2 & Process == 1)
                {
                    DT.Columns["JOB_TRN_SEA_EXP_FD_PK"].ColumnMapping = MappingType.Hidden;
                }
                else if (BizType == 2 & Process == 2)
                {
                    DT.Columns["JOB_TRN_SEA_IMP_FD_PK"].ColumnMapping = MappingType.Hidden;
                }
                else if (BizType == 1 & Process == 1)
                {
                    DT.Columns["JOB_TRN_AIR_EXP_FD_PK"].ColumnMapping = MappingType.Hidden;
                }
                else if (BizType == 1 & Process == 2)
                {
                    DT.Columns["JOB_TRN_AIR_IMP_FD_PK"].ColumnMapping = MappingType.Hidden;
                }
                DT.Columns["JOBFK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["FREIGHT_ELEMENT_MST_FK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["CURRENCY_MST_FK"].ColumnMapping = MappingType.Hidden;
                DT.Columns[DT.Columns.Count - 1].ColumnMapping = MappingType.Hidden;
                //FRT_BOF_FK/FRT_AFC_FK
                DT.Columns[DT.Columns.Count - 2].ColumnMapping = MappingType.Hidden;
                //CHK
                return DT;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable FetchInvoiceData(string InvPks, int nBaseCurrPK, short BizType, short Process)
        {
            StringBuilder strQuery = new StringBuilder();
            string strsql = null;
            string vatcode = null;
            Int32 rowcunt = 0;
            Int32 Contpk = 0;
            string strJobPks = "";
            DataTable DT = new DataTable();
            DataTable dt1 = new DataTable();
            string frtType = Convert.ToString((Process == 1 ? "1" : "2"));
            foreach (string intInvPk in InvPks.Split(','))
            {
                strQuery = new StringBuilder();
                strJobPks = GetJobPksOfInvoice(intInvPk, BizType, Process);
                //-----------------------------------------------------------
                //    '1->Type        2->fdpk            3->jobcardfk    4->freightfk    5->currencyfk
                //    '6->frieghtname 7->ElementSearch   8->Currency Id  9->Curr-Search  10->Frei. Amt.
                //    '11->Exchange Rate 12->Inv Amt     13->Tax Percent 14->Tax Amount  15->Total Amt
                //    '16->Remarks     17->Mode          18->Chk
                if (BizType == 2 & Process == 2 & Convert.ToInt32(intInvPk) > 0)
                {
                    rowcunt = objConsInv.FetchDetFrt(Convert.ToInt32(intInvPk));
                    if (rowcunt > 0)
                    {
                        Contpk = objConsInv.FetchDetContPk(Convert.ToInt32(strJobPks), Convert.ToInt32(intInvPk));
                        if (Contpk == 0)
                        {
                            strQuery.Append("SELECT 0 PK,J.JOBCARD_REF_NO Jobcard_ref_no , '' unit , TRN.CONSOL_INVOICE_TRN_PK PK,J.JOB_CARD_SEA_IMP_PK jobcard_fk, ");
                            strQuery.Append(" TRN.FRT_OTH_ELEMENT freight_or_oth ,TRN.FRT_OTH_ELEMENT_FK element_fk,TRN.CURRENCY_MST_FK currency_mst_fk,TRN.FRT_DESC element_name, ");
                            strQuery.Append(" '' AS ELEMENT,cur.currency_id,  '' AS CURR, TRN.ELEMENT_AMT AS AMOUNT, ROUND((CASE TRN.ELEMENT_AMT ");
                            strQuery.Append(" WHEN 0 THEN 1 ELSE  TRN.AMT_IN_INV_CURR /TRN.ELEMENT_AMT END),6) AS EXCHANGE_RATE,       TRN.AMT_IN_INV_CURR AS INV_AMOUNT, ");
                            strQuery.Append(" TRN.VAT_CODE AS VAT_CODE,TRN.TAX_PCNT AS VAT_PERCENT,TRN.TAX_AMT AS VAT_AMOUNT,TRN.TOT_AMT AS TOTAL_AMOUNT,TRN.REMARKS, ");
                            strQuery.Append(" 'EDIT' AS MODE1,'TRUE' AS CHK,''FRT_BOF_FK FROM CONSOL_INVOICE_TBL MAS,CONSOL_INVOICE_TRN_TBL TRN,JOB_CARD_SEA_IMP_TBL J,currency_type_mst_tbl cur ");
                            strQuery.Append(" where cur.currency_mst_pk=trn.currency_mst_fk and MAS.CONSOL_INVOICE_PK=" + intInvPk + " AND TRN.CONSOL_INVOICE_FK=MAS.CONSOL_INVOICE_PK AND trn.job_card_fk=j.job_card_sea_imp_pk ");

                            dt1 = objWF.GetDataTable(strQuery.ToString());
                            if (DT.Columns.Count == 0)
                            {
                                DT = dt1;
                            }
                            else
                            {
                                foreach (DataRow row in dt1.Rows)
                                {
                                    DataRow dr = null;
                                    dr = DT.NewRow();
                                    foreach (DataColumn col in dt1.Columns)
                                    {
                                        dr[col.ColumnName] = row[col.ColumnName];
                                    }
                                    DT.Rows.Add(dr);
                                }
                            }
                        }
                    }
                }
            }
            if (DT.Rows.Count > 0)
            {
                DT.Columns["CONSOL_INVOICE_FK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["PK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["JOBCARD_FK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["ELEMENT_FK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["CURRENCY_MST_FK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["MODE"].ColumnMapping = MappingType.Hidden;
                DT.Columns["CHK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["FRT_BOF_FK"].ColumnMapping = MappingType.Hidden;
                return DT;
            }
            foreach (string intInvPk in InvPks.Split(','))
            {
                strQuery = new StringBuilder();
                strQuery.Append(" select Q.* from ( ");
                strQuery.Append(" SELECT " + intInvPk + " CONSOL_INVOICE_FK,TYPE, JOBCARD_REF_NO, UNIT, PK, JOBCARD_FK, FREIGHT_OR_OTH, ELEMENT_FK, CURRENCY_MST_FK, ELEMENT_NAME, ");
                strQuery.Append(" ELEMENT, CURRENCY_ID, CURR, AMOUNT,  EXCHANGE_RATE, INV_AMOUNT, VAT_CODE, VAT_PERCENT, VAT_AMOUNT, TOTAL_AMOUNT, ");
                strQuery.Append(" REMARKS, MODE1 AS \"MODE\", CHK, FRT_BOF_FK FROM ( ");
                strQuery.Append(" select type,jobcard_ref_no,unit,pk,jobcard_fk,freight_or_oth,element_fk,currency_mst_fk,element_name,element,currency_id, ");
                strQuery.Append(" curr,amount,exchange_rate,inv_amount,vat_code,vat_percent VAT_PERCENT,vat_amount VAT_AMOUNT,total_amount,remarks,mode1 as \"MODE1\",chk,FRT_BOF_FK, PREFERENCE from ( ");
                if (BizType == 1)
                {
                    strQuery.Append(" SELECT  DECODE(TRN.FRT_OTH_ELEMENT_FK, 1, 'COST', 2, 'FREIGHT', 3, 'OTHER') AS TYPE," );
                }
                else
                {
                    strQuery.Append(" SELECT  distinct TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK, DECODE(TRN.FRT_OTH_ELEMENT_FK, 1, 'COST', 2, 'FREIGHT', 3, 'OTHER') AS TYPE," );
                }
                strQuery.Append("     JOB.JOBCARD_REF_NO," );
                if (BizType == 1)
                {
                    strQuery.Append("       CON.CONTAINER_TYPE_MST_ID AS UNIT, " );
                }
                else
                {
                    strQuery.Append("  (case when jobfrt.container_type_mst_fk is not null then  con.container_type_mst_id  else '1' end) UNIT, " );
                }
                strQuery.Append(" TRN.CONSOL_INVOICE_TRN_PK AS PK," );
                strQuery.Append("       TRN.JOB_CARD_FK AS JOBCARD_FK," );
                strQuery.Append("       '' FREIGHT_OR_OTH," );

                if (BizType == 1)
                {
                    strQuery.Append("       TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK," );
                }
                strQuery.Append("       TRN.CURRENCY_MST_FK," );
                // strQuery.Append("       (SELECT FREIGHT_ELEMENT_NAME" & vbCrLf)
                //  strQuery.Append("          FROM FREIGHT_ELEMENT_MST_TBL F" & vbCrLf)
                // strQuery.Append("         WHERE F.FREIGHT_ELEMENT_MST_PK = TRN.FRT_OTH_ELEMENT_FK) AS ELEMENT_NAME," & vbCrLf)
                //strQuery.Append("    FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME," & vbCrLf) 'Added By jitendra
                //strQuery.Append("       FMT.FREIGHT_ELEMENT_ID AS ELEMENT," & vbCrLf)
                strQuery.Append("TRN.FRT_DESC AS ELEMENT_NAME,");
                strQuery.Append("                       CASE (SELECT FT.FREIGHT_ELEMENT_ID");
                strQuery.Append("                           FROM FREIGHT_ELEMENT_MST_TBL FT");
                strQuery.Append("                          WHERE UPPER(FT.FREIGHT_ELEMENT_NAME) =");
                strQuery.Append("                                UPPER(TRN.FRT_DESC))");
                strQuery.Append("                         WHEN NULL THEN");
                strQuery.Append("                          FMT.FREIGHT_ELEMENT_ID");
                strQuery.Append("                         WHEN '' THEN");
                strQuery.Append("                          FMT.FREIGHT_ELEMENT_ID");
                strQuery.Append("                         ELSE");
                strQuery.Append("                          (SELECT FT.FREIGHT_ELEMENT_ID");
                strQuery.Append("                             FROM FREIGHT_ELEMENT_MST_TBL FT");
                strQuery.Append("                            WHERE UPPER(FT.FREIGHT_ELEMENT_NAME) =");
                strQuery.Append("                                  UPPER(TRN.FRT_DESC))");
                strQuery.Append("                       END AS ELEMENT,");

                strQuery.Append("       CUMT.CURRENCY_ID," );
                strQuery.Append("       '' AS CURR," );
                strQuery.Append("       TRN.ELEMENT_AMT AS AMOUNT," );
                strQuery.Append("       ROUND((CASE TRN.ELEMENT_AMT" );
                strQuery.Append("               WHEN 0 THEN" );
                strQuery.Append("1" );
                strQuery.Append("               ELSE" );
                strQuery.Append("                TRN.AMT_IN_INV_CURR / TRN.ELEMENT_AMT" );
                strQuery.Append("             END)," );
                strQuery.Append("             6) AS EXCHANGE_RATE," );
                strQuery.Append("       TRN.AMT_IN_INV_CURR AS INV_AMOUNT," );
                //strQuery.Append("       TRN.VAT_CODE AS VAT_CODE," & vbCrLf) 'Added by Venkata
                strQuery.Append("  (CASE " );
                strQuery.Append("  WHEN TRN.VAT_CODE = '0' THEN " );
                strQuery.Append("    '' " );
                strQuery.Append("   ELSE" );
                strQuery.Append("   TRN.VAT_CODE " );
                strQuery.Append("  END) VAT_CODE," );
                strQuery.Append("       TRN.TAX_PCNT AS VAT_PERCENT," );
                strQuery.Append("       TRN.TAX_AMT AS VAT_AMOUNT," );
                strQuery.Append("       TRN.TOT_AMT AS TOTAL_AMOUNT," );
                strQuery.Append("       TRN.REMARKS," );
                strQuery.Append("       'EDIT' AS \"MODE1\"," );
                strQuery.Append("       'TRUE' AS CHK,PAR.FRT_BOF_FK, FMT.PREFERENCE " );
                strQuery.Append("  FROM CONSOL_INVOICE_TRN_TBL TRN," );
                strQuery.Append("       CONSOL_INVOICE_TBL     HDR," );
                strQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT,freight_element_mst_tbl fmt,JOB_TRN_SEA_EXP_FD JOBFRT,CONTAINER_TYPE_MST_TBL CON,PARAMETERS_TBL PAR, " );
                strQuery.Append("        JOB_CARD_SEA_EXP_TBL JOB" );
                strQuery.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK" );
                strQuery.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK" );
                strQuery.Append("   AND JOB.JOB_CARD_SEA_EXP_PK=TRN.JOB_CARD_FK" );
                strQuery.Append("   AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK ");
                //strQuery.Append("   AND TRN.FRT_OTH_ELEMENT_FK=FMT.FREIGHT_ELEMENT_MST_PK ")
                strQuery.Append("   and jobfrt.consol_invoice_trn_fk = trn.consol_invoice_trn_pk " );
                //Added By Prakash Chandra on 26/06/2008
                strQuery.Append(" AND TRN.JOB_CARD_FK  = JOBFRT.JOB_CARD_SEA_EXP_FK " );
                if (BizType == 1)
                {
                    strQuery.Append(" AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK " );
                }
                else
                {
                    strQuery.Append("  AND ( JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK or JOBFRT.CONTAINER_TYPE_MST_FK is null ) ");
                }
                if (rowcunt <= 0)
                {
                    strQuery.Append(" AND JOBFRT.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK " );
                }
                else
                {
                    if (Contpk > 0)
                    {
                        strQuery.Append("  and jobfrt.job_trn_sea_imp_fd_pk in ( " + Contpk + " )");
                    }
                }
                strQuery.Append(" AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+) " );
                strQuery.Append("  AND HDR.CONSOL_INVOICE_PK = " + intInvPk + " ORDER BY FMT.PREFERENCE ) ");
                strQuery.Append(" UNION ");

                //To display othercharges in the grid
                strQuery.Append("  SELECT DECODE(TRN.FRT_OTH_ELEMENT_FK, 1, 'COST', 2, 'FREIGHT', 3, 'OTHER') AS TYPE," );
                strQuery.Append("       JOB.JOBCARD_REF_NO," );
                strQuery.Append("       'Oth.Chrg' AS UNIT, TRN.CONSOL_INVOICE_TRN_PK AS PK," );
                strQuery.Append("       TRN.JOB_CARD_FK AS JOBCARD_FK," );
                //strQuery.Append("       '' FREIGHT_OR_OTH," & vbCrLf)
                strQuery.Append("      upper(TRN.FRT_OTH_ELEMENT),");
                strQuery.Append("       TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK," );
                strQuery.Append("       TRN.CURRENCY_MST_FK," );
                //strQuery.Append("      FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME," & vbCrLf)
                //strQuery.Append("       FMT.FREIGHT_ELEMENT_ID AS ELEMENT," & vbCrLf)
                strQuery.Append("       TRN.FRT_DESC AS ELEMENT_NAME,");
                strQuery.Append("                       CASE (SELECT FT.FREIGHT_ELEMENT_ID");
                strQuery.Append("                           FROM FREIGHT_ELEMENT_MST_TBL FT");
                strQuery.Append("                          WHERE UPPER(FT.FREIGHT_ELEMENT_NAME) =");
                strQuery.Append("                                UPPER(TRN.FRT_DESC))");
                strQuery.Append("                         WHEN NULL THEN");
                strQuery.Append("                          FMT.FREIGHT_ELEMENT_ID");
                strQuery.Append("                         WHEN '' THEN");
                strQuery.Append("                          FMT.FREIGHT_ELEMENT_ID");
                strQuery.Append("                         ELSE");
                strQuery.Append("                          (SELECT FT.FREIGHT_ELEMENT_ID");
                strQuery.Append("                             FROM FREIGHT_ELEMENT_MST_TBL FT");
                strQuery.Append("                            WHERE UPPER(FT.FREIGHT_ELEMENT_NAME) =");
                strQuery.Append("                                  UPPER(TRN.FRT_DESC))");
                strQuery.Append("                       END AS ELEMENT,");
                strQuery.Append("       CUMT.CURRENCY_ID," );
                strQuery.Append("       '' AS CURR," );
                strQuery.Append("       TRN.ELEMENT_AMT AS AMOUNT," );
                strQuery.Append("       ROUND((CASE TRN.ELEMENT_AMT" );
                strQuery.Append("               WHEN 0 THEN" );
                strQuery.Append("1" );
                strQuery.Append("               ELSE" );
                strQuery.Append("                TRN.AMT_IN_INV_CURR / TRN.ELEMENT_AMT" );
                strQuery.Append("             END)," );
                strQuery.Append("             6) AS EXCHANGE_RATE," );
                strQuery.Append("       TRN.AMT_IN_INV_CURR AS INV_AMOUNT," );
                strQuery.Append("       TRN.VAT_CODE AS VAT_CODE," );
                strQuery.Append("       TRN.TAX_PCNT AS VAT_PERCENT," );
                strQuery.Append("       TRN.TAX_AMT AS VAT_AMOUNT," );
                strQuery.Append("       TRN.TOT_AMT AS TOTAL_AMOUNT," );
                strQuery.Append("       TRN.REMARKS," );
                strQuery.Append("       'EDIT' AS \"MODE1\"," );
                strQuery.Append("       'TRUE' AS CHK,PAR.FRT_BOF_FK, FMT.PREFERENCE AS PREFERENCE " );
                strQuery.Append("  FROM CONSOL_INVOICE_TRN_TBL TRN," );
                strQuery.Append("       CONSOL_INVOICE_TBL     HDR," );
                strQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT, FREIGHT_ELEMENT_MST_TBL  FMT, JOB_TRN_SEA_EXP_oth_chrg JOBoth,PARAMETERS_TBL PAR, " );
                strQuery.Append("        JOB_CARD_SEA_EXP_TBL JOB" );
                strQuery.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK" );
                strQuery.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK" );
                strQuery.Append("   AND JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK " );
                strQuery.Append("   AND TRN.FRT_OTH_ELEMENT_FK=FMT.FREIGHT_ELEMENT_MST_PK " );
                strQuery.Append("   AND JOB.JOB_CARD_SEA_EXP_PK=TRN.JOB_CARD_FK" );
                strQuery.Append(" AND TRN.JOB_CARD_FK  = JOBoth.JOB_CARD_SEA_EXP_FK " );
                //strQuery.Append(" AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK " & vbCrLf)
                if (rowcunt <= 0)
                {
                    strQuery.Append(" AND JOBoth.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK " );
                }
                else
                {
                    if (Contpk > 0)
                    {
                        strQuery.Append("  and JOBoth.job_trn_sea_imp_oth_pk in (" + Contpk + " ) ");
                    }
                }
                //end
                strQuery.Append(" AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+) " );
                strQuery.Append("   AND HDR.CONSOL_INVOICE_PK = " + intInvPk + "");
                //strQuery.Append("   ORDER BY PAR.FRT_BOF_FK ASC " & vbCrLf)
                strQuery.Append("  ORDER BY unit, PREFERENCE )) Q ");
                //strQuery.Append("  ORDER BY PREFERENCE )) Q order by FRT_BOF_FK ")
                ///'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                //If BizType = 1 And Process = 1 Then 'Sea Export
                //    strQuery.Replace("SEA", "AIR")
                //    strQuery.Replace("INVOICE_AIR_TBL_FK", "INV_CUST_TRN_AIR_EXP_FK")
                //Air import
                if (BizType == 1 & Process == 2)
                {
                    strQuery.Replace("SEA", "AIR");
                    strQuery.Replace("INVOICE_AIR_TBL_FK", "INV_CUST_TRN_AIR_IMP_FK");
                    strQuery.Replace("EXP", "IMP");
                    strQuery.Replace("CON.CONTAINER_TYPE_MST_ID", "upper(JOBFRT.QUANTITY)");
                    strQuery.Replace("FRT_BOF_FK", "FRT_AFC_FK");
                    strQuery.Replace("CONTAINER_TYPE_MST_TBL CON,", " ");
                    strQuery.Replace("AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK", " ");
                    //strQuery.Replace("TRN.FRT_OTH_ELEMENT_FK", "JOBFRT.FREIGHT_ELEMENT_MST_FK")
                    //air export
                }
                else if (BizType == 1 & Process == 1)
                {
                    strQuery.Replace("SEA", "AIR");
                    strQuery.Replace("INVOICE_AIR_TBL_FK", "INV_CUST_TRN_AIR_EXP_FK");
                    strQuery.Replace("IMP", "EXP");
                    strQuery.Replace("CON.CONTAINER_TYPE_MST_ID", "upper(JOBFRT.QUANTITY)");
                    strQuery.Replace("FRT_BOF_FK", "FRT_AFC_FK");
                    strQuery.Replace("CONTAINER_TYPE_MST_TBL CON,", " ");
                    strQuery.Replace("AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK", " ");
                    // ImportSea
                }
                else if (BizType == 2 & Process == 2)
                {
                    strQuery.Replace("EXP", "IMP");
                }
                dt1 = objWF.GetDataTable(strQuery.ToString());
                if (DT.Columns.Count == 0)
                {
                    DT = dt1;
                }
                else
                {
                    foreach (DataRow row in dt1.Rows)
                    {
                        DataRow dr = null;
                        dr = DT.NewRow();
                        foreach (DataColumn col in dt1.Columns)
                        {
                            dr[col.ColumnName] = row[col.ColumnName];
                        }
                        DT.Rows.Add(dr);
                    }
                }
            }
            try
            {
                DT.Columns["CONSOL_INVOICE_FK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["PK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["JOBCARD_FK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["ELEMENT_FK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["CURRENCY_MST_FK"].ColumnMapping = MappingType.Hidden;
                DT.Columns["MODE"].ColumnMapping = MappingType.Hidden;
                DT.Columns[DT.Columns.Count - 2].ColumnMapping = MappingType.Hidden;
                //CHK
                DT.Columns[DT.Columns.Count - 1].ColumnMapping = MappingType.Hidden;
                //FRT_BOF_FK/FRT_AFC_FK
                return DT;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void updateEDIStatus(string strPK, string strBizType = "", string strDocType = "")
        {
            string strSql = "";
            WorkFlow objWF = new WorkFlow();

            try
            {
                if (Convert.ToInt32(strDocType) == 1)
                {
                    if (Convert.ToInt32(strBizType) == 2)
                    {
                        strSql = " UPDATE BOOKING_SEA_TBL SET EDI_STATUS = 1 WHERE BOOKING_SEA_PK IN (" + strPK + ") AND NVL(EDI_STATUS,0) = 0 ";
                        objWF.ExecuteCommands(strSql);
                    }
                    else if (Convert.ToInt32(strBizType) == 1)
                    {
                        strSql = " UPDATE BOOKING_AIR_TBL SET EDI_STATUS = 1 WHERE BOOKING_AIR_PK IN (" + strPK + ") AND NVL(EDI_STATUS,0) = 0 ";
                        objWF.ExecuteCommands(strSql);
                    }
                }
                else if (Convert.ToInt32(strDocType) == 3 | Convert.ToInt32(strDocType) == 4)
                {
                    if (Convert.ToInt32(strBizType) == 2)
                    {
                        strSql = " UPDATE MBL_EXP_TBL SET EDI_STATUS = 1 WHERE MBL_EXP_TBL_PK IN (" + strPK + ") AND NVL(EDI_STATUS,0) = 0 ";
                        objWF.ExecuteCommands(strSql);
                    }
                    else if (Convert.ToInt32(strBizType) == 1)
                    {
                        strSql = " UPDATE MAWB_EXP_TBL SET EDI_STATUS = 1 WHERE MAWB_EXP_TBL_PK IN (" + strPK + ") AND NVL(EDI_STATUS,0) = 0 ";
                        objWF.ExecuteCommands(strSql);
                    }
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string FetchAgentMailId(string AgentName)
        {
            string strSql = "";
            WorkFlow objWF = new WorkFlow();
            string MailTo = "";
            string ds = "";

            try
            {
                strSql = " SELECT TRIM(NVL(AC.adm_email_id, ' ')) FROM Agent_Mst_Tbl AM, agent_contact_dtls AC WHERE AM.AGENT_MST_PK = AC.AGENT_MST_FK(+) AND AM.active_flag = 1 AND TRIM(AM.agent_name) = TRIM('" + AgentName + "') ";
                MailTo = objWF.ExecuteScaler(strSql);

                if (string.IsNullOrEmpty(MailTo))
                {
                    strSql = "select custdtl.adm_email_id from customer_contact_dtls custdtl ,";
                    strSql += " customer_mst_tbl cust ";
                    strSql += "where ";
                    strSql += "cust.customer_mst_pk = custdtl.customer_mst_fk ";
                    strSql += "and cust.CUSTOMER_NAME = '" + AgentName + "' ";

                    MailTo = objWF.ExecuteScaler(strSql);
                }

                return MailTo;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Get Jobcar Details for invoice"

        #region "Generate Booking"

        public DataSet GenerateBooking(string BkgPks, int Biz, string CargoType = "", string Currency = "", int inc = 0, string ExportType = "")
        {
            //Air
            if (Biz == 1)
            {
                //XML
                if (Convert.ToInt32(ExportType) == 2)
                {
                    if (inc == 1)
                    {
                        dsBkg = GetExportAirBooking(BkgPks);
                        dsBkg = GetExportBookingAirHeaderQuery(BkgPks, Currency);
                    }
                    else if (inc == 2)
                    {
                        dsBkg = GetExportBookingAirAddressQuery(BkgPks, Currency);
                    }
                    else if (inc == 3)
                    {
                        dsBkg = GetExportBookingAirReferenceDtlsQuery(BkgPks, Currency);
                    }
                    else if (inc == 4)
                    {
                        dsBkg = GetExportBookingAirCommodityQuery(BkgPks, Currency);
                    }
                    else if (inc == 5)
                    {
                        dsBkg = GetExportBookingAirOthChrgQuery(BkgPks, Currency);
                    }
                    else if (inc == 6)
                    {
                        dsBkg = GetExportBookingAirFrtElementsQuery(BkgPks, Currency);
                    }
                    else if (inc == 7)
                    {
                        dsBkg = GetExportBookingAirFooterQuery(BkgPks, Currency);
                    }
                }
                else
                {
                    dsBkg = GetExportBookingAirQuery(BkgPks, Currency);
                }
                //Sea
            }
            else
            {
                //FCL
                if (Convert.ToInt32(CargoType) == 2)
                {
                    //XML
                    if (Convert.ToInt32(ExportType) == 2)
                    {
                        if (inc == 1)
                        {
                            dsBkg = GetExportSeaBooking(BkgPks);
                            dsBkg = GetExportBookingSeaHeaderFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 2)
                        {
                            dsBkg = GetExportBookingSeaOthChrgFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 3)
                        {
                            dsBkg = GetExportBookingSeaAddressFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 4)
                        {
                            dsBkg = GetExportBookingSeaReferenceDtlsFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 5)
                        {
                            dsBkg = GetExportBookingSeaCargoDtls1FCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 6)
                        {
                            dsBkg = GetExportBookingSeaCmdtyFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 7)
                        {
                            dsBkg = GetExportBookingSeaCmdtyDetailsFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 8)
                        {
                            dsBkg = GetExportBookingSeaFrtElementsFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 9)
                        {
                            dsBkg = GetExportBookingSeaFooterFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 10)
                        {
                            dsBkg = GetExportBookingSeaClauseFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 11)
                        {
                            dsBkg = GetExportBookingSeaCargoDtls2FCLQuery(BkgPks, Currency);
                        }
                    }
                    else
                    {
                        dsBkg = GetExportBookingSeaFCLQuery(BkgPks, Currency);
                    }
                    //LCL
                }
                else if (Convert.ToInt32(CargoType) == 3)
                {
                    //XML
                    if (Convert.ToInt32(ExportType) == 2)
                    {
                        if (inc == 1)
                        {
                            dsBkg = GetExportSeaBooking(BkgPks);
                            dsBkg = GetExportBookingSeaHeaderLCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 2)
                        {
                            dsBkg = GetExportBookingSeaOthChrgFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 3)
                        {
                            dsBkg = GetExportBookingSeaAddressFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 4)
                        {
                            dsBkg = GetExportBookingSeaReferenceDtlsLCLQuery(BkgPks, Currency);
                            //ElseIf inc = 6 Then
                            //    dsBkg = GetExportBookingSeaCmdtyFCLQuery(BkgPks, Currency)
                        }
                        else if (inc == 7)
                        {
                            dsBkg = GetExportBookingSeaCmdtyDetailsLCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 8)
                        {
                            dsBkg = GetExportBookingSeaFrtElementsFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 9)
                        {
                            dsBkg = GetExportBookingSeaFooterFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 10)
                        {
                            dsBkg = GetExportBookingSeaClauseFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 11)
                        {
                            dsBkg = GetExportBookingSeaCargoDtls2FCLQuery(BkgPks, Currency);
                        }
                    }
                    else
                    {
                        dsBkg = GetExportBookingSeaLCLQuery(BkgPks, Currency);
                    }
                    //BBC
                }
                else if (Convert.ToInt32(CargoType) == 4)
                {
                    //XML
                    if (Convert.ToInt32(ExportType) == 2)
                    {
                        if (inc == 1)
                        {
                            dsBkg = GetExportSeaBooking(BkgPks);
                            dsBkg = GetExportBookingSeaHeaderBBCQuery(BkgPks, Currency);
                        }
                        else if (inc == 2)
                        {
                            dsBkg = GetExportBookingSeaOthChrgFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 3)
                        {
                            dsBkg = GetExportBookingSeaAddressFCLQuery(BkgPks, Currency);
                        }
                        else if (inc == 4)
                        {
                            dsBkg = GetExportBookingSeaCommodityBBCQuery(BkgPks, Currency);
                        }
                        else if (inc == 6)
                        {
                            dsBkg = GetExportBookingSeaReferenceDtlsBBCQuery(BkgPks, Currency);
                            //ElseIf inc = 7 Then
                            //    dsBkg = GetExportBookingSeaCmdtyDetailsLCLQuery(BkgPks, Currency)
                        }
                        else if (inc == 8)
                        {
                            dsBkg = GetExportBookingSeaFrtElementsBBCQuery(BkgPks, Currency);
                        }
                        else if (inc == 9)
                        {
                            dsBkg = GetExportBookingSeaFooterBBCQuery(BkgPks, Currency);
                        }
                        else if (inc == 10)
                        {
                            dsBkg = GetExportBookingSeaClauseFCLQuery(BkgPks, Currency);
                            //ElseIf inc = 11 Then
                            //    dsBkg = GetExportBookingSeaCargoDtls2FCLQuery(BkgPks, Currency)
                        }
                    }
                    else
                    {
                        dsBkg = GetExportBookingSeaBBCQuery(BkgPks, Currency);
                    }
                }
            }
            try
            {
                return dsBkg;
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

        #region "FCL"

        public DataSet GetExportBookingSeaFCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT");
            sb.Append("       BTFD.PREFERENCE, BST.BOOKING_REF_NO,");
            sb.Append("       TO_CHAR(BST.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
            sb.Append("       DECODE(BST.STATUS,");
            sb.Append("              1,");
            sb.Append("              'Provisional',");
            sb.Append("              2,");
            sb.Append("              'Confirmed',");
            sb.Append("              3,");
            sb.Append("              'Cancelled',");
            sb.Append("              4,");
            sb.Append("              'E-Booking',");
            sb.Append("              5,");
            sb.Append("              'Shipped',");
            sb.Append("              6,");
            sb.Append("              'Shipped') STATUS,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1, BTSF.TRANS_REF_NO) QUOTE_REF_NR,");
            sb.Append("       TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
            sb.Append("       DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            sb.Append("       POL.PORT_ID POL,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       SHP.CUSTOMER_ID SHIPPER,");
            sb.Append("       CONS.CUSTOMER_ID CONSIGNEE,");
            sb.Append("       BST.CUSTOMER_REF_NO CUSTOMER_REF_NO,");
            sb.Append("       BST.CREDIT_DAYS CREDIT_DAYS,");
            sb.Append("       BST.CREDIT_LIMIT CREDIT_LIMIT,");
            sb.Append("       '" + Currency + "' CREDIT_LIMIT_CURRENCY,");
            sb.Append("       DPAT.AGENT_ID DP_AGENT,");
            sb.Append("       CBAT.AGENT_ID XBKG_AGENT,");
            sb.Append("       CLAT.AGENT_ID COLOAD_AGENT,");
            sb.Append("       CASE");
            sb.Append("         WHEN BST.CB_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END X_BKG,");
            sb.Append("       CASE");
            sb.Append("         WHEN BST.CL_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END CO_LOADING,");
            sb.Append("       CMMT.CARGO_MOVE_CODE CARGO_MOVE_CODE,");
            sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMODITY_GROUP_CODE,");
            sb.Append("       '" + Currency + "' CURRENCY,");
            sb.Append("       STMT.INCO_CODE INCO_TERMS,");
            sb.Append("       DECODE(BST.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAY_TYPE,");
            //sb.Append("       PTMT.PACK_TYPE_ID PACK_TYPE,")
            sb.Append("       BST.PACK_COUNT PACK_COUNT,");
            sb.Append("       BST.GROSS_WEIGHT GROSS_WEIGHT,");
            sb.Append("       BST.NET_WEIGHT NET_WEIGHT,");
            sb.Append("       BST.VOLUME_IN_CBM VOLUME,");
            sb.Append("       BKGOTH.FREIGHT_ELEMENT_ID OTH_FREIGHT_ELEMENT,");
            sb.Append("       BKGOTH.CURRENCY_ID OTH_CURRENCY,");
            sb.Append("       BKGOTH.AMOUNT OTH_AMOUNT,");
            sb.Append("       DECODE(BKGOTH.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') OTH_PYMT_TYPE,");
            sb.Append("       BST.COL_ADDRESS COLLECTION_ADDRESS,");
            sb.Append("       BST.DEL_ADDRESS DELIVERY_ADDRESS,");
            sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMODITY_GROUP,");
            sb.Append("       '' COMMODITY_ID,");
            sb.Append("       '' COMMODITY_NAME,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Operator Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') REFERENCE_TYPE,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1, BTSF.TRANS_REF_NO) REF_NR,");
            sb.Append("       OMT.OPERATOR_ID LINE,");
            sb.Append("       ' ' PACK,");
            sb.Append("       CONTTYPE.CONTAINER_TYPE_MST_ID TYPE,");
            //sb.Append("       BTSF.QUANTITY BOXES,")
            sb.Append("       TO_CHAR(BTSF.NO_OF_BOXES) BOXES,");
            sb.Append("       BTSF.ALL_IN_TARIFF BOOKING_RATE,");
            sb.Append("       BTSF.ALL_IN_TARIFF TOTAL_RATE,");
            sb.Append("       BKGCOMM.P1 QTY,");
            sb.Append("       BKGCOMM.N1 NET_WT,");
            sb.Append("       BKGCOMM.G1 GROSS_WT,");
            sb.Append("       BKGCOMM.V1 VOLUME,");
            sb.Append("       BKGCOMM.REMARK REMARKS,");
            sb.Append("       BKGCOMM.COMMODITY_ID COMMODITY,");
            sb.Append("       BKGCOMM.PACK_TYPE_ID PACK_TYPE,");
            sb.Append("       BKGCOMM.P2 QTY,");
            sb.Append("       BKGCOMM.N2 NET_WT,");
            sb.Append("       BKGCOMM.G2 GROSS_WT,");
            sb.Append("       BKGCOMM.V1 VOLUME,");
            sb.Append("       BTFD.FREIGHT_ELEMENT_ID ELEMENT,");
            sb.Append("       BTFD.CHARGE_BASIS,");
            sb.Append("       BTFD.CURRENCY_ID CURR,");
            sb.Append("       '' VALUE,");
            sb.Append("       BTFD.TARIFF_RATE BKG_RATE,");
            sb.Append("       '' EXRATE,");
            sb.Append("       BTFD.MIN_RATE AMOUNT,");
            //sb.Append("       BTFD.PYMT_TYPE PAYMENT_TYPE,")
            sb.Append("       DECODE(BTFD.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE,");
            sb.Append("       0 FRIEGHT_CHARGES,");
            sb.Append("       0 OTH_FREIGHT_CHARGES,");
            sb.Append("       0 TOTAL_FREIGHT_CHARGES,");
            sb.Append("       VAT.VESSEL_ID VSL_CODE,");
            sb.Append("       VAT.VESSEL_NAME VSL_NAME,");
            sb.Append("       TO_CHAR('''' || BST.VOYAGE) VOY,");
            sb.Append("       TO_CHAR(BST.ETD_DATE, DATEFORMAT || ' HH24:MI') ETD_POL,");
            sb.Append("       TO_CHAR(BST.CUT_OFF_DATE, DATEFORMAT || ' HH24:MI') CUT_OFF,");
            sb.Append("       TO_CHAR(BST.ETA_DATE, DATEFORMAT || ' HH24:MI') ETA_POD,");
            sb.Append("       NVL(BST.LINE_BKG_NO,' ') LINE_BKG_NO,");
            sb.Append("       '' CLAUSE,");
            sb.Append("       CASE WHEN JCSET.MARKS_NUMBERS IS NOT NULL ");
            sb.Append("         OR JCSET.GOODS_DESCRIPTION IS NOT NULL THEN 'YES'");
            sb.Append("         ELSE");
            sb.Append("           'NO' END CARGO_DETAIL,");
            sb.Append("       JCSET.MARKS_NUMBERS MARKS_NUMBERS,");
            sb.Append("       JCSET.GOODS_DESCRIPTION GOODS_DESCRIPTION");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       PORT_MST_TBL POL,");
            sb.Append("       PORT_MST_TBL POD,");
            sb.Append("       CUSTOMER_MST_TBL SHP,");
            sb.Append("       CUSTOMER_MST_TBL CONS,");
            sb.Append("       AGENT_MST_TBL DPAT,");
            sb.Append("       AGENT_MST_TBL CBAT,");
            sb.Append("       AGENT_MST_TBL CLAT,");
            sb.Append("       CARGO_MOVE_MST_TBL CMMT,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMT,");
            sb.Append("       PACK_TYPE_MST_TBL PTMT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("       OPERATOR_MST_TBL OMT,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CONTTYPE,");
            sb.Append("       VESSEL_VOYAGE_TBL VAT,");
            sb.Append("       VESSEL_VOYAGE_TRN VVT,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL JCSET,");
            sb.Append("       (SELECT BTOTH.BOOKING_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTOTH.AMOUNT,");
            sb.Append("               BTOTH.FREIGHT_TYPE");
            sb.Append("          FROM BOOKING_TRN_SEA_OTH_CHRG BTOTH,");
            sb.Append("               BOOKING_SEA_TBL          BST,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE BTOTH.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("           AND BTOTH.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("           AND BTOTH.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK) BKGOTH,");
            sb.Append("       (SELECT BTCD.BOOKING_TRN_SEA_FK,");
            sb.Append("               BTCD.PACK_COUNT         P1,");
            sb.Append("               BTCD.NET_WEIGHT         N1,");
            sb.Append("               BTCD.GROSS_WEIGHT       G1,");
            sb.Append("               BTCD.REMARK,");
            sb.Append("               COMM.COMMODITY_ID,");
            sb.Append("               PACK.PACK_TYPE_ID,");
            sb.Append("               BCD.PACK_COUNT          P2,");
            sb.Append("               BCD.NET_WEIGHT          N2,");
            sb.Append("               BCD.GROSS_WEIGHT        G2,");
            sb.Append("               BCD.VOLUME_IN_CBM       V1");
            sb.Append("          FROM BOOKING_TRN_CARGO_DTL BTCD,");
            sb.Append("               BOOKING_COMMODITY_DTL BCD,");
            sb.Append("               COMMODITY_MST_TBL     COMM,");
            sb.Append("               PACK_TYPE_MST_TBL     PACK");
            sb.Append("         WHERE BTCD.BOOKING_TRN_CARGO_PK = BCD.BOOKING_CARGO_DTL_FK");
            sb.Append("           AND COMM.COMMODITY_MST_PK = BCD.COMMODITY_MST_FK");
            sb.Append("           AND PACK.PACK_TYPE_MST_PK = BCD.PACK_TYPE_FK) BKGCOMM,");
            sb.Append("       (SELECT BTFD.BOOKING_TRN_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               DECODE(FEMT.CHARGE_BASIS,");
            sb.Append("                      1,");
            sb.Append("                      '%',");
            sb.Append("                      2,");
            sb.Append("                      'Flat',");
            sb.Append("                      3,");
            sb.Append("                      'Kgs',");
            sb.Append("                      4,");
            sb.Append("                      'Unit') CHARGE_BASIS,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTFD.TARIFF_RATE,");
            sb.Append("               BTFD.MIN_RATE,");
            sb.Append("               BTFD.PYMT_TYPE,");
            sb.Append("               FEMT.PREFERENCE");
            sb.Append("          FROM BOOKING_TRN_SEA_FRT_DTLS BTFD,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE FEMT.FREIGHT_ELEMENT_MST_PK = BTFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           AND CTMT.CURRENCY_MST_PK = BTFD.CURRENCY_MST_FK) BTFD");
            sb.Append(" WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
            sb.Append("   AND SHP.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
            sb.Append("   AND CONS.CUSTOMER_MST_PK(+) = BST.CONS_CUSTOMER_MST_FK");
            sb.Append("   AND DPAT.AGENT_MST_PK(+) = BST.DP_AGENT_MST_FK");
            sb.Append("   AND CBAT.AGENT_MST_PK(+) = BST.CB_AGENT_MST_FK");
            sb.Append("   AND CLAT.AGENT_MST_PK(+) = BST.CL_AGENT_MST_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = BST.CARGO_MOVE_FK");
            sb.Append("   AND STMT.SHIPPING_TERMS_MST_PK(+) = BST.SHIPPING_TERMS_MST_FK");
            sb.Append("   AND PTMT.PACK_TYPE_MST_PK(+) = BST.PACK_TYP_MST_FK");
            sb.Append("   AND BST.BOOKING_SEA_PK = BKGOTH.BOOKING_SEA_FK(+)");
            sb.Append("   AND CGMT.COMMODITY_GROUP_PK = BST.COMMODITY_GROUP_FK");
            sb.Append("   AND OMT.OPERATOR_MST_PK = BST.OPERATOR_MST_FK");
            sb.Append("   AND CONTTYPE.CONTAINER_TYPE_MST_PK = BTSF.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND BKGCOMM.BOOKING_TRN_SEA_FK(+) = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("   AND BTFD.BOOKING_TRN_SEA_FK(+) = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("   AND BST.VESSEL_VOYAGE_FK = VAT.VESSEL_VOYAGE_TBL_PK(+)");
            sb.Append("   AND VAT.VESSEL_VOYAGE_TBL_PK = VVT.VESSEL_VOYAGE_TBL_FK(+)");
            sb.Append("   AND BST.BOOKING_SEA_PK = JCSET.BOOKING_SEA_FK(+)");
            sb.Append("   ORDER BY CONTTYPE.CONTAINER_TYPE_MST_ID, BTFD.PREFERENCE");
            sb.Append("");

            DataSet dsBkg = new DataSet();
            dsBkg = objWF.GetDataSet(sb.ToString());

            try
            {
                foreach (DataRow _row in dsBkg.Tables[0].Rows)
                {
                    foreach (DataColumn col in dsBkg.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return dsBkg;
        }

        public DataSet GetExportSeaBooking(string BkgPks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT BST.BOOKING_SEA_PK,BST.BOOKING_REF_NO ");
            sb.Append("  FROM BOOKING_SEA_TBL BST ");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "BOOKING");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaHeaderFCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT BST.BOOKING_SEA_PK,");
            sb.Append("       TO_CHAR(BST.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
            sb.Append("       DECODE(BST.STATUS,");
            sb.Append("              1,");
            sb.Append("              'Provisional',");
            sb.Append("              2,");
            sb.Append("              'Confirmed',");
            sb.Append("              3,");
            sb.Append("              'Cancelled',");
            sb.Append("              4,");
            sb.Append("              'E-Booking',");
            sb.Append("              5,");
            sb.Append("              'Shipped',");
            sb.Append("              6,");
            sb.Append("              'Shipped') STATUS,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1, BTSF.TRANS_REF_NO) QUOTE_REF_NR,");
            sb.Append("       TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
            sb.Append("       DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            sb.Append("       POL.PORT_ID POL,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       SHP.CUSTOMER_ID SHIPPER,");
            sb.Append("       CONS.CUSTOMER_ID CONSIGNEE,");
            sb.Append("       BST.CUSTOMER_REF_NO CUSTOMER_REF_NO,");
            sb.Append("       TO_CHAR(BST.CREDIT_DAYS) CREDIT_DAYS,");
            sb.Append("       TO_CHAR(BST.CREDIT_LIMIT) CREDIT_LIMIT,");
            sb.Append("       '" + Currency + "' CREDIT_LIMIT_CURRENCY,");
            sb.Append("       DPAT.AGENT_ID DP_AGENT,");
            sb.Append("       CBAT.AGENT_ID XBKG_AGENT,");
            sb.Append("       CLAT.AGENT_ID COLOAD_AGENT,");
            sb.Append("       CASE");
            sb.Append("         WHEN BST.CB_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END X_BKG,");
            sb.Append("       CASE");
            sb.Append("         WHEN BST.CL_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END CO_LOADING,");
            sb.Append("       CMMT.CARGO_MOVE_CODE CARGO_MOVE_CODE,");
            sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMODITY_GROUP_CODE,");
            sb.Append("       '" + Currency + "' CURRENCY,");
            sb.Append("       STMT.INCO_CODE INCO_TERMS,");
            sb.Append("       DECODE(BST.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAY_TYPE,");
            sb.Append("       '' PACK_TYPE,");
            sb.Append("       TO_CHAR(BST.PACK_COUNT) PACK_COUNT,");
            sb.Append("       TO_CHAR(BST.GROSS_WEIGHT) GROSS_WEIGHT,");
            sb.Append("       TO_CHAR(BST.NET_WEIGHT) NET_WEIGHT,");
            sb.Append("       TO_CHAR(BST.VOLUME_IN_CBM) VOLUME ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       PORT_MST_TBL POL,");
            sb.Append("       PORT_MST_TBL POD,");
            sb.Append("       CUSTOMER_MST_TBL SHP,");
            sb.Append("       CUSTOMER_MST_TBL CONS,");
            sb.Append("       AGENT_MST_TBL DPAT,");
            sb.Append("       AGENT_MST_TBL CBAT,");
            sb.Append("       AGENT_MST_TBL CLAT,");
            sb.Append("       CARGO_MOVE_MST_TBL CMMT,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CONTTYPE,");
            sb.Append("       VESSEL_VOYAGE_TBL VAT,");
            sb.Append("       VESSEL_VOYAGE_TRN VVT,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL JCSET,");
            sb.Append("       (SELECT BTOTH.BOOKING_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTOTH.AMOUNT,");
            sb.Append("               BTOTH.FREIGHT_TYPE");
            sb.Append("          FROM BOOKING_TRN_SEA_OTH_CHRG BTOTH,");
            sb.Append("               BOOKING_SEA_TBL          BST,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE BTOTH.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("           AND BTOTH.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("           AND BTOTH.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK) BKGOTH,");
            sb.Append("       (SELECT BTCD.BOOKING_TRN_SEA_FK,");
            sb.Append("               BTCD.PACK_COUNT         P1,");
            sb.Append("               BTCD.NET_WEIGHT         N1,");
            sb.Append("               BTCD.GROSS_WEIGHT       G1,");
            sb.Append("               BTCD.REMARK,");
            sb.Append("               COMM.COMMODITY_ID,");
            sb.Append("               PACK.PACK_TYPE_ID,");
            sb.Append("               BCD.PACK_COUNT          P2,");
            sb.Append("               BCD.NET_WEIGHT          N2,");
            sb.Append("               BCD.GROSS_WEIGHT        G2,");
            sb.Append("               BCD.VOLUME_IN_CBM       V1");
            sb.Append("          FROM BOOKING_TRN_CARGO_DTL BTCD,");
            sb.Append("               BOOKING_COMMODITY_DTL BCD,");
            sb.Append("               COMMODITY_MST_TBL     COMM,");
            sb.Append("               PACK_TYPE_MST_TBL     PACK");
            sb.Append("         WHERE BTCD.BOOKING_TRN_CARGO_PK = BCD.BOOKING_CARGO_DTL_FK");
            sb.Append("           AND COMM.COMMODITY_MST_PK = BCD.COMMODITY_MST_FK");
            sb.Append("           AND PACK.PACK_TYPE_MST_PK = BCD.PACK_TYPE_FK) BKGCOMM,");
            sb.Append("       (SELECT BTFD.BOOKING_TRN_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               DECODE(FEMT.CHARGE_BASIS,");
            sb.Append("                      1,");
            sb.Append("                      '%',");
            sb.Append("                      2,");
            sb.Append("                      'Flat',");
            sb.Append("                      3,");
            sb.Append("                      'Kgs',");
            sb.Append("                      4,");
            sb.Append("                      'Unit') CHARGE_BASIS,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTFD.TARIFF_RATE,");
            sb.Append("               BTFD.MIN_RATE,");
            sb.Append("               BTFD.PYMT_TYPE");
            sb.Append("          FROM BOOKING_TRN_SEA_FRT_DTLS BTFD,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE FEMT.FREIGHT_ELEMENT_MST_PK = BTFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           AND CTMT.CURRENCY_MST_PK = BTFD.CURRENCY_MST_FK) BTFD");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
            sb.Append("   AND SHP.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
            sb.Append("   AND CONS.CUSTOMER_MST_PK(+) = BST.CONS_CUSTOMER_MST_FK");
            sb.Append("   AND DPAT.AGENT_MST_PK(+) = BST.DP_AGENT_MST_FK");
            sb.Append("   AND CBAT.AGENT_MST_PK(+) = BST.CB_AGENT_MST_FK");
            sb.Append("   AND CLAT.AGENT_MST_PK(+) = BST.CL_AGENT_MST_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = BST.CARGO_MOVE_FK");
            sb.Append("   AND STMT.SHIPPING_TERMS_MST_PK(+) = BST.SHIPPING_TERMS_MST_FK");
            sb.Append("   AND BST.BOOKING_SEA_PK = BKGOTH.BOOKING_SEA_FK(+)");
            sb.Append("   AND CGMT.COMMODITY_GROUP_PK = BST.COMMODITY_GROUP_FK");
            sb.Append("   AND CONTTYPE.CONTAINER_TYPE_MST_PK = BTSF.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND BKGCOMM.BOOKING_TRN_SEA_FK(+) = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("   AND BTFD.BOOKING_TRN_SEA_FK(+) = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("   AND BST.VESSEL_VOYAGE_FK = VVT.voyage_trn_pk(+)");
            sb.Append("   AND VAT.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND BST.BOOKING_SEA_PK = JCSET.BOOKING_SEA_FK(+)");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "HEADER");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaOthChrgFCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_SEA_PK,");
            sb.Append("       BKGOTH.FREIGHT_ELEMENT_ID OTH_FREIGHT_ELEMENT,");
            sb.Append("       BKGOTH.CURRENCY_ID OTH_CURRENCY,");
            sb.Append("       TO_CHAR(BKGOTH.AMOUNT) OTH_AMOUNT,");
            sb.Append("       DECODE(BKGOTH.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') OTH_PYMT_TYPE ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       (SELECT BTOTH.BOOKING_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTOTH.AMOUNT,");
            sb.Append("               BTOTH.FREIGHT_TYPE");
            sb.Append("          FROM BOOKING_TRN_SEA_OTH_CHRG BTOTH,");
            sb.Append("               BOOKING_SEA_TBL          BST,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE BTOTH.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("           AND BTOTH.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("           AND BTOTH.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK) BKGOTH ");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = BKGOTH.BOOKING_SEA_FK ");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "OTH_CHARGES");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaAddressFCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_SEA_PK,");
            sb.Append("       BST.COL_ADDRESS COLLECTION_ADDRESS,");
            sb.Append("       BST.DEL_ADDRESS DELIVERY_ADDRESS ");
            sb.Append("  FROM BOOKING_SEA_TBL BST ");
            sb.Append("  WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("     AND (BST.COL_ADDRESS IS NOT NULL OR BST.DEL_ADDRESS IS NOT NULL)  ");
            sb.Append(" ");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "ADDRESS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaReferenceDtlsFCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_SEA_PK,BTSF.BOOKING_TRN_SEA_PK,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Operator Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') REFERENCE_TYPE,");
            sb.Append("       BTSF.TRANS_REF_NO REFERENCE_NR,");
            sb.Append("       OMT.OPERATOR_ID LINE,");
            sb.Append("       CONTTYPE.CONTAINER_TYPE_MST_ID TYPE,");
            sb.Append("       TO_CHAR(BTSF.NO_OF_BOXES) BOXES ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       OPERATOR_MST_TBL OMT,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CONTTYPE ");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND OMT.OPERATOR_MST_PK = BST.OPERATOR_MST_FK");
            sb.Append("   AND CONTTYPE.CONTAINER_TYPE_MST_PK = BTSF.CONTAINER_TYPE_MST_FK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "REFERENCE_DETAILS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaCargoDtls1FCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_SEA_PK,BTSF.BOOKING_TRN_SEA_PK,");
            sb.Append("       CONTTYPE.CONTAINER_TYPE_MST_ID CONTR_TYPE,");
            sb.Append("       TO_CHAR(BKGCOMM.P1) QTY,");
            sb.Append("       TO_CHAR(BKGCOMM.N1) NET_WT,");
            sb.Append("       TO_CHAR(BKGCOMM.G1) GROSS_WT,");
            sb.Append("       TO_CHAR(BKGCOMM.V1) VOLUME,");
            sb.Append("       BKGCOMM.REMARK REMARKS, ");
            sb.Append("       TO_CHAR(BTSF.ALL_IN_TARIFF) BOOKING_RATE,");
            sb.Append("       TO_CHAR(FETCH_FCL_LCL_TOTALBKGFRT(BTSF.BOOKING_TRN_SEA_PK,1," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)) AS TOTAL_RATE,");
            sb.Append("       BKGCOMM.BOOKING_TRN_CARGO_PK ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CONTTYPE,");
            sb.Append("       (SELECT BTCD.BOOKING_TRN_SEA_FK,");
            sb.Append("               BTCD.BOOKING_TRN_CARGO_PK,");
            sb.Append("               BTCD.PACK_COUNT         P1,");
            sb.Append("               BTCD.NET_WEIGHT         N1,");
            sb.Append("               BTCD.GROSS_WEIGHT       G1,");
            sb.Append("               BTCD.REMARK,");
            sb.Append("               COMM.COMMODITY_ID,");
            sb.Append("               PACK.PACK_TYPE_ID,");
            sb.Append("               BCD.PACK_COUNT          P2,");
            sb.Append("               BCD.NET_WEIGHT          N2,");
            sb.Append("               BCD.GROSS_WEIGHT        G2,");
            sb.Append("               BCD.VOLUME_IN_CBM       V1");
            sb.Append("          FROM BOOKING_TRN_CARGO_DTL BTCD,");
            sb.Append("               BOOKING_COMMODITY_DTL BCD,");
            sb.Append("               COMMODITY_MST_TBL     COMM,");
            sb.Append("               PACK_TYPE_MST_TBL     PACK");
            sb.Append("         WHERE BTCD.BOOKING_TRN_CARGO_PK = BCD.BOOKING_CARGO_DTL_FK");
            sb.Append("           AND COMM.COMMODITY_MST_PK = BCD.COMMODITY_MST_FK");
            sb.Append("           AND PACK.PACK_TYPE_MST_PK = BCD.PACK_TYPE_FK) BKGCOMM ");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND CONTTYPE.CONTAINER_TYPE_MST_PK = BTSF.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND BKGCOMM.BOOKING_TRN_SEA_FK = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append(" ");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "CARGO_DETAILS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaCmdtyFCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append(" SELECT BST.BOOKING_SEA_PK,");
            sb.Append("       BKGCOMM.COMMODITY_ID COMMODITY,");
            sb.Append("       BKGCOMM.BOOKING_TRN_CARGO_PK ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CONTTYPE,");
            sb.Append("       (SELECT BTCD.BOOKING_TRN_SEA_FK,");
            sb.Append("               BTCD.BOOKING_TRN_CARGO_PK,");
            sb.Append("               BTCD.PACK_COUNT         P1,");
            sb.Append("               BTCD.NET_WEIGHT         N1,");
            sb.Append("               BTCD.GROSS_WEIGHT       G1,");
            sb.Append("               BTCD.REMARK,");
            sb.Append("               COMM.COMMODITY_ID,");
            sb.Append("               PACK.PACK_TYPE_ID,");
            sb.Append("               BCD.PACK_COUNT          P2,");
            sb.Append("               BCD.NET_WEIGHT          N2,");
            sb.Append("               BCD.GROSS_WEIGHT        G2,");
            sb.Append("               BCD.VOLUME_IN_CBM       V1");
            sb.Append("          FROM BOOKING_TRN_CARGO_DTL BTCD,");
            sb.Append("               BOOKING_COMMODITY_DTL BCD,");
            sb.Append("               COMMODITY_MST_TBL     COMM,");
            sb.Append("               PACK_TYPE_MST_TBL     PACK");
            sb.Append("         WHERE BTCD.BOOKING_TRN_CARGO_PK = BCD.BOOKING_CARGO_DTL_FK");
            sb.Append("           AND COMM.COMMODITY_MST_PK = BCD.COMMODITY_MST_FK");
            sb.Append("           AND PACK.PACK_TYPE_MST_PK = BCD.PACK_TYPE_FK) BKGCOMM ");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND CONTTYPE.CONTAINER_TYPE_MST_PK = BTSF.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND BKGCOMM.BOOKING_TRN_SEA_FK = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "COMMODITY");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaCmdtyDetailsFCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_SEA_PK,");
            sb.Append("       BKGCOMM.COMMODITY_ID COMMODITY,");
            sb.Append("       BKGCOMM.PACK_TYPE_ID PACK_TYPE,");
            sb.Append("       TO_CHAR(BKGCOMM.P2) QTY,");
            sb.Append("       TO_CHAR(BKGCOMM.N2) NET_WT,");
            sb.Append("       TO_CHAR(BKGCOMM.G2) GROSS_WT,");
            sb.Append("       TO_CHAR(BKGCOMM.V1) VOLUME, ");
            sb.Append("       BKGCOMM.BOOKING_TRN_CARGO_PK ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CONTTYPE,");
            sb.Append("       (SELECT BTCD.BOOKING_TRN_SEA_FK,");
            sb.Append("               BTCD.BOOKING_TRN_CARGO_PK,");
            sb.Append("               BTCD.PACK_COUNT         P1,");
            sb.Append("               BTCD.NET_WEIGHT         N1,");
            sb.Append("               BTCD.GROSS_WEIGHT       G1,");
            sb.Append("               BTCD.REMARK,");
            sb.Append("               COMM.COMMODITY_ID,");
            sb.Append("               PACK.PACK_TYPE_ID,");
            sb.Append("               BCD.PACK_COUNT          P2,");
            sb.Append("               BCD.NET_WEIGHT          N2,");
            sb.Append("               BCD.GROSS_WEIGHT        G2,");
            sb.Append("               BCD.VOLUME_IN_CBM       V1");
            sb.Append("          FROM BOOKING_TRN_CARGO_DTL BTCD,");
            sb.Append("               BOOKING_COMMODITY_DTL BCD,");
            sb.Append("               COMMODITY_MST_TBL     COMM,");
            sb.Append("               PACK_TYPE_MST_TBL     PACK");
            sb.Append("         WHERE BTCD.BOOKING_TRN_CARGO_PK = BCD.BOOKING_CARGO_DTL_FK");
            sb.Append("           AND COMM.COMMODITY_MST_PK = BCD.COMMODITY_MST_FK");
            sb.Append("           AND PACK.PACK_TYPE_MST_PK = BCD.PACK_TYPE_FK) BKGCOMM ");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND CONTTYPE.CONTAINER_TYPE_MST_PK = BTSF.CONTAINER_TYPE_MST_FK");
            sb.Append("   AND BKGCOMM.BOOKING_TRN_SEA_FK = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "COMMODITY_DETAILS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaFrtElementsFCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_SEA_PK,BTSF.BOOKING_TRN_SEA_PK,");
            sb.Append("       BTFD.FREIGHT_ELEMENT_ID FREIGHT_ELEMENT,");
            sb.Append("       BTFD.CHARGE_BASIS CHARGE_BASIS,");
            sb.Append("       BTFD.CURRENCY_ID CURRENCY_ID,");
            sb.Append("       TO_CHAR(BTFD.TARIFF_RATE) BKG_RATE,");
            sb.Append("       TO_CHAR(BTFD.MIN_RATE) AMOUNT,");
            sb.Append("       DECODE(BTFD.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       (SELECT BTFD.BOOKING_TRN_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               DECODE(FEMT.CHARGE_BASIS,");
            sb.Append("                      1,");
            sb.Append("                      '%',");
            sb.Append("                      2,");
            sb.Append("                      'Flat',");
            sb.Append("                      3,");
            sb.Append("                      'Kgs',");
            sb.Append("                      4,");
            sb.Append("                      'Unit') CHARGE_BASIS,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTFD.TARIFF_RATE,");
            sb.Append("               BTFD.MIN_RATE,");
            sb.Append("               BTFD.PYMT_TYPE ");
            sb.Append("          FROM BOOKING_TRN_SEA_FRT_DTLS BTFD,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE FEMT.FREIGHT_ELEMENT_MST_PK = BTFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           AND CTMT.CURRENCY_MST_PK = BTFD.CURRENCY_MST_FK) BTFD");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND BTFD.BOOKING_TRN_SEA_FK = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "FREIGHT_ELEMENTS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaFooterFCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_SEA_PK,");
            sb.Append("       VAT.VESSEL_ID VESSEL_CODE,");
            sb.Append("       VAT.VESSEL_NAME VESSEL_NAME,");
            sb.Append("       BST.VOYAGE VOYAGE,");
            sb.Append("       TO_CHAR(BST.ETD_DATE, DATEFORMAT || ' HH24:MI') ETD_POL,");
            sb.Append("       TO_CHAR(BST.CUT_OFF_DATE, DATEFORMAT || ' HH24:MI') CUT_OFF,");
            sb.Append("       TO_CHAR(BST.ETA_DATE, DATEFORMAT || ' HH24:MI') ETA_POD ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       VESSEL_VOYAGE_TBL VAT,");
            sb.Append("       VESSEL_VOYAGE_TRN VVT ");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.VESSEL_VOYAGE_FK = VVT.voyage_trn_pk(+)");
            sb.Append("   AND VAT.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "FOOTER");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaClauseFCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT BST.BOOKING_SEA_PK,");
            sb.Append("                       HBL.REFERENCE_NR CLAUSE_REFERENCE_NR ");
            sb.Append("                   FROM HBL_BL_CLAUSE_TBL HBL,");
            sb.Append("                       BL_CLAUSE_TBL     BLMST,");
            sb.Append("                       BOOKING_SEA_TBL   BST ");
            sb.Append("                 WHERE BST.BOOKING_SEA_PK = HBL.HBL_EXP_TBL_FK");
            sb.Append("                   AND BLMST.BL_CLAUSE_PK(+) = HBL.BL_CLAUSE_FK");
            sb.Append("                   AND HBL.CLAUSE_TYPE_FLAG = 3");
            sb.Append("                   AND TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "CLAUSE");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaCargoDtls2FCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_SEA_PK,");
            sb.Append("       JCSET.MARKS_NUMBERS,");
            sb.Append("       JCSET.GOODS_DESCRIPTION ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL JCSET ");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = JCSET.BOOKING_SEA_FK ");
            sb.Append("   AND (JCSET.MARKS_NUMBERS IS NOT NULL OR JCSET.GOODS_DESCRIPTION IS NOT NULL) ");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "CARGO_DETAIL");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        #endregion "FCL"

        #region "LCL"

        public DataSet GetExportBookingSeaLCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT BTFD.PREFERENCE, BST.BOOKING_REF_NO,");
            sb.Append("       TO_CHAR(BST.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
            sb.Append("       DECODE(BST.STATUS,");
            sb.Append("              1,");
            sb.Append("              'Provisional',");
            sb.Append("              2,");
            sb.Append("              'Confirmed',");
            sb.Append("              3,");
            sb.Append("              'Cancelled',");
            sb.Append("              4,");
            sb.Append("              'E-Booking',");
            sb.Append("              5,");
            sb.Append("              'Shipped',");
            sb.Append("              6,");
            sb.Append("              'Shipped') STATUS,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1, BTSF.TRANS_REF_NO) QUOTE_REF_NR,");
            sb.Append("       TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
            sb.Append("       DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            sb.Append("       POL.PORT_ID POL,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       SHP.CUSTOMER_ID SHIPPER,");
            sb.Append("       CONS.CUSTOMER_ID CONSIGNEE,");
            sb.Append("       BST.CUSTOMER_REF_NO,");
            sb.Append("       BST.CREDIT_DAYS,");
            sb.Append("       BST.CREDIT_LIMIT,");
            sb.Append("       '" + Currency + "' CREDIT_LIMIT_CURRENCY,");
            sb.Append("       DPAT.AGENT_ID DP_AGENT,");
            sb.Append("       CBAT.AGENT_ID XBKG_AGENT,");
            sb.Append("       CLAT.AGENT_ID COLOAD_AGENT,");
            sb.Append("       CASE");
            sb.Append("         WHEN BST.CB_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END X_BKG,");
            sb.Append("       CASE");
            sb.Append("         WHEN BST.CL_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END CO_LOADING,");
            sb.Append("       CMMT.CARGO_MOVE_CODE,");
            sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
            sb.Append("       '" + Currency + "' CURRENCY,");
            sb.Append("       STMT.INCO_CODE INCO_TERMS,");
            sb.Append("       DECODE(BST.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAY_TYPE,");
            sb.Append("       PTMT.PACK_TYPE_ID PACK_TYPE,");
            sb.Append("       BST.PACK_COUNT,");
            sb.Append("       BST.GROSS_WEIGHT,");
            sb.Append("       BST.CHARGEABLE_WEIGHT NET_WEIGHT,");
            sb.Append("       BST.VOLUME_IN_CBM VOLUME,");
            sb.Append("       BKGOTH.FREIGHT_ELEMENT_ID OTH_FREIGHT_ELEMENT,");
            sb.Append("       BKGOTH.CURRENCY_ID OTH_CURRENCY,");
            sb.Append("       BKGOTH.AMOUNT OTH_AMOUNT,");
            sb.Append("       DECODE(BKGOTH.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') OTH_PYMT_TYPE,");
            sb.Append("       BST.COL_ADDRESS COLLECTION_ADDRESS,");
            sb.Append("       BST.DEL_ADDRESS DELIVERY_ADDRESS,");
            sb.Append("       '' COMMODITY_GROUP,");
            sb.Append("       '' COMMODITY_ID,");
            sb.Append("       '' COMMODITY_NAME,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Operator Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') REFERENCE_TYPE,");
            sb.Append("       BTSF.TRANS_REF_NO REF_NR,");
            sb.Append("       OMT.OPERATOR_ID LINE,");
            sb.Append("       '' PACK,");
            sb.Append("       BASIS.DIMENTION_ID TYPE,");
            sb.Append("       BTSF.QUANTITY BOXES,");
            sb.Append("       BTSF.ALL_IN_TARIFF BOOKING_RATE,");
            //sb.Append("       BTSF.BUYING_RATE TOTAL_RATE,")
            sb.Append("       FETCH_FCL_LCL_TOTALBKGFRT(BTSF.BOOKING_TRN_SEA_PK,1," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)AS TOTAL_RATE, ");
            sb.Append("       BST.PACK_COUNT QTY,");
            sb.Append("       '' NET_WT,");
            sb.Append("       '' GROSS_WT,");
            sb.Append("       '' VOLUME,");
            sb.Append("       '' REMARKS,");
            sb.Append("       (SELECT C.COMMODITY_ID");
            sb.Append("         FROM COMMODITY_MST_TBL C");
            sb.Append("         WHERE C.COMMODITY_MST_PK IN");
            sb.Append("       (SELECT *");
            sb.Append("          FROM TABLE (SELECT FN_SPLIT(B.COMMODITY_MST_FKS)");
            sb.Append("                        FROM BOOKING_SEA_TBL BST,");
            sb.Append("                        BOOKING_TRN_SEA_FCL_LCL B ");
            sb.Append("                       WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("                       AND BST.BOOKING_SEA_PK = B.BOOKING_SEA_FK))) COMMODITY,");
            sb.Append("       '' PACK_TYPE,");
            sb.Append("       '' QTY,");
            sb.Append("       '' NET_WT,");
            sb.Append("       '' VOLUME,");
            sb.Append("       BTFD.FREIGHT_ELEMENT_ID ELEMENT,");
            sb.Append("       BTFD.CHARGE_BASIS,");
            sb.Append("       BTFD.CURRENCY_ID CURR,");
            sb.Append("       '' VALUE,");
            sb.Append("       BTFD.TARIFF_RATE BKG_RATE,");
            sb.Append("       '' EXRATE,");
            sb.Append("       BTFD.MIN_RATE AMOUNT,");
            sb.Append("       DECODE(BTFD.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE,");
            sb.Append("       0 FRIEGHT_CHARGES,");
            sb.Append("       0 OTH_FREIGHT_CHARGES,");
            sb.Append("       0 TOTAL_FREIGHT_CHARGES,");
            sb.Append("       VAT.VESSEL_ID VSL_CODE,");
            sb.Append("       VAT.VESSEL_NAME VSL_NAME,");
            sb.Append("       TO_CHAR('''' || BST.VOYAGE) VOY,");
            sb.Append("       TO_CHAR(BST.ETD_DATE, DATEFORMAT || ' HH24:MI') ETD_POL,");
            sb.Append("       TO_CHAR(BST.CUT_OFF_DATE, DATEFORMAT || ' HH24:MI') CUT_OFF,");
            sb.Append("       TO_CHAR(BST.ETA_DATE, DATEFORMAT || ' HH24:MI') ETA_POD,");
            sb.Append("       BST.LINE_BKG_NO,");
            sb.Append("       '' CLAUSE,");
            sb.Append("       CASE");
            sb.Append("         WHEN JCSET.MARKS_NUMBERS IS NOT NULL OR");
            sb.Append("              JCSET.GOODS_DESCRIPTION IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END CARGO_DETAIL,");
            sb.Append("       JCSET.MARKS_NUMBERS,");
            sb.Append("       JCSET.GOODS_DESCRIPTION");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       PORT_MST_TBL POL,");
            sb.Append("       PORT_MST_TBL POD,");
            sb.Append("       CUSTOMER_MST_TBL SHP,");
            sb.Append("       CUSTOMER_MST_TBL CONS,");
            sb.Append("       AGENT_MST_TBL DPAT,");
            sb.Append("       AGENT_MST_TBL CBAT,");
            sb.Append("       AGENT_MST_TBL CLAT,");
            sb.Append("       CARGO_MOVE_MST_TBL CMMT,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMT,");
            sb.Append("       PACK_TYPE_MST_TBL PTMT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("       OPERATOR_MST_TBL OMT,");
            sb.Append("       DIMENTION_UNIT_MST_TBL BASIS,");
            sb.Append("       VESSEL_VOYAGE_TBL VAT,");
            sb.Append("       VESSEL_VOYAGE_TRN VVT,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL JCSET,");
            sb.Append("       (SELECT BTOTH.BOOKING_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTOTH.AMOUNT,");
            sb.Append("               BTOTH.FREIGHT_TYPE");
            sb.Append("          FROM BOOKING_TRN_SEA_OTH_CHRG BTOTH,");
            sb.Append("               BOOKING_SEA_TBL          BST,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE BTOTH.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("           AND BTOTH.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("           AND BTOTH.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK) BKGOTH,");
            sb.Append("       (SELECT BTFD.BOOKING_TRN_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               DECODE(FEMT.CHARGE_BASIS,");
            sb.Append("                      1,");
            sb.Append("                      '%',");
            sb.Append("                      2,");
            sb.Append("                      'Flat',");
            sb.Append("                      3,");
            sb.Append("                      'Kgs',");
            sb.Append("                      4,");
            sb.Append("                      'Unit') CHARGE_BASIS,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTFD.TARIFF_RATE,");
            sb.Append("               BTFD.MIN_RATE,");
            sb.Append("               BTFD.PYMT_TYPE,");
            sb.Append("               FEMT.PREFERENCE");
            sb.Append("          FROM BOOKING_TRN_SEA_FRT_DTLS BTFD,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE FEMT.FREIGHT_ELEMENT_MST_PK = BTFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           AND CTMT.CURRENCY_MST_PK = BTFD.CURRENCY_MST_FK) BTFD");
            sb.Append(" WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append(" AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
            sb.Append("   AND SHP.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
            sb.Append("   AND CONS.CUSTOMER_MST_PK(+) = BST.CONS_CUSTOMER_MST_FK");
            sb.Append("   AND DPAT.AGENT_MST_PK(+) = BST.DP_AGENT_MST_FK");
            sb.Append("   AND CBAT.AGENT_MST_PK(+) = BST.CB_AGENT_MST_FK");
            sb.Append("   AND CLAT.AGENT_MST_PK(+) = BST.CL_AGENT_MST_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = BST.CARGO_MOVE_FK");
            sb.Append("   AND STMT.SHIPPING_TERMS_MST_PK(+) = BST.SHIPPING_TERMS_MST_FK");
            sb.Append("   AND PTMT.PACK_TYPE_MST_PK(+) = BST.PACK_TYP_MST_FK");
            sb.Append("   AND BST.BOOKING_SEA_PK = BKGOTH.BOOKING_SEA_FK(+)");
            sb.Append("   AND CGMT.COMMODITY_GROUP_PK = BST.COMMODITY_GROUP_FK");
            sb.Append("   AND OMT.OPERATOR_MST_PK = BST.OPERATOR_MST_FK");
            sb.Append("   AND BASIS.DIMENTION_UNIT_MST_PK = BTSF.BASIS");
            sb.Append("   AND BTFD.BOOKING_TRN_SEA_FK(+) = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("   AND BST.VESSEL_VOYAGE_FK = VVT.voyage_trn_pk(+)");
            sb.Append("   AND VAT.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND BST.BOOKING_SEA_PK = JCSET.BOOKING_SEA_FK(+)");
            sb.Append("   ORDER BY BTFD.PREFERENCE");
            sb.Append("");

            DataSet dsBkg = new DataSet();
            dsBkg = objWF.GetDataSet(sb.ToString());

            try
            {
                foreach (DataRow _row in dsBkg.Tables[0].Rows)
                {
                    foreach (DataColumn col in dsBkg.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return dsBkg;
        }

        public DataSet GetExportBookingSeaHeaderLCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT BST.BOOKING_SEA_PK,");
            sb.Append("       TO_CHAR(BST.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
            sb.Append("       DECODE(BST.STATUS,");
            sb.Append("              1,");
            sb.Append("              'Provisional',");
            sb.Append("              2,");
            sb.Append("              'Confirmed',");
            sb.Append("              3,");
            sb.Append("              'Cancelled',");
            sb.Append("              4,");
            sb.Append("              'E-Booking',");
            sb.Append("              5,");
            sb.Append("              'Shipped',");
            sb.Append("              6,");
            sb.Append("              'Shipped') STATUS,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1, BTSF.TRANS_REF_NO) QUOTE_REF_NR,");
            sb.Append("       TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
            sb.Append("       DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            sb.Append("       POL.PORT_ID POL,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       SHP.CUSTOMER_ID SHIPPER,");
            sb.Append("       CONS.CUSTOMER_ID CONSIGNEE,");
            sb.Append("       BST.CUSTOMER_REF_NO,");
            sb.Append("       BST.CREDIT_DAYS,");
            sb.Append("       BST.CREDIT_LIMIT,");
            sb.Append("       '" + Currency + "' CREDIT_LIMIT_CURRENCY,");
            sb.Append("       DPAT.AGENT_ID DP_AGENT,");
            sb.Append("       CBAT.AGENT_ID XBKG_AGENT,");
            sb.Append("       CLAT.AGENT_ID COLOAD_AGENT,");
            sb.Append("       CASE");
            sb.Append("         WHEN BST.CB_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END X_BKG,");
            sb.Append("       CASE");
            sb.Append("         WHEN BST.CL_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END CO_LOADING,");
            sb.Append("       CMMT.CARGO_MOVE_CODE,");
            sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
            sb.Append("       '" + Currency + "' CURRENCY,");
            sb.Append("       STMT.INCO_CODE INCO_TERMS,");
            sb.Append("       DECODE(BST.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAY_TYPE,");
            sb.Append("       PTMT.PACK_TYPE_ID PACK_TYPE,");
            sb.Append("       BST.PACK_COUNT,");
            sb.Append("       BST.GROSS_WEIGHT,");
            sb.Append("       BST.CHARGEABLE_WEIGHT NET_WEIGHT,");
            sb.Append("       BST.VOLUME_IN_CBM VOLUME ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       PORT_MST_TBL POL,");
            sb.Append("       PORT_MST_TBL POD,");
            sb.Append("       CUSTOMER_MST_TBL SHP,");
            sb.Append("       CUSTOMER_MST_TBL CONS,");
            sb.Append("       AGENT_MST_TBL DPAT,");
            sb.Append("       AGENT_MST_TBL CBAT,");
            sb.Append("       AGENT_MST_TBL CLAT,");
            sb.Append("       CARGO_MOVE_MST_TBL CMMT,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMT,");
            sb.Append("       PACK_TYPE_MST_TBL PTMT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT ");
            sb.Append(" WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append(" AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
            sb.Append("   AND SHP.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
            sb.Append("   AND CONS.CUSTOMER_MST_PK(+) = BST.CONS_CUSTOMER_MST_FK");
            sb.Append("   AND DPAT.AGENT_MST_PK(+) = BST.DP_AGENT_MST_FK");
            sb.Append("   AND CBAT.AGENT_MST_PK(+) = BST.CB_AGENT_MST_FK");
            sb.Append("   AND CLAT.AGENT_MST_PK(+) = BST.CL_AGENT_MST_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = BST.CARGO_MOVE_FK");
            sb.Append("   AND STMT.SHIPPING_TERMS_MST_PK(+) = BST.SHIPPING_TERMS_MST_FK");
            sb.Append("   AND PTMT.PACK_TYPE_MST_PK(+) = BST.PACK_TYP_MST_FK");
            sb.Append("   AND CGMT.COMMODITY_GROUP_PK = BST.COMMODITY_GROUP_FK");
            sb.Append("");

            //'
            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "HEADER");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaReferenceDtlsLCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_SEA_PK,BTSF.BOOKING_TRN_SEA_PK,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Operator Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') REFERENCE_TYPE,");
            sb.Append("       BTSF.TRANS_REF_NO REFERENCE_NR,");
            sb.Append("       OMT.OPERATOR_ID LINE,");
            sb.Append("       BASIS.DIMENTION_ID TYPE,");
            sb.Append("       BTSF.QUANTITY BOXES,");
            sb.Append("       BST.PACK_COUNT QTY,");
            sb.Append("       BTSF.ALL_IN_TARIFF BOOKING_RATE,");
            sb.Append("       FETCH_FCL_LCL_TOTALBKGFRT(BTSF.BOOKING_TRN_SEA_PK,1," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)AS TOTAL_RATE ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       OPERATOR_MST_TBL OMT,");
            sb.Append("       DIMENTION_UNIT_MST_TBL BASIS ");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND OMT.OPERATOR_MST_PK = BST.OPERATOR_MST_FK");
            sb.Append("   AND BASIS.DIMENTION_UNIT_MST_PK = BTSF.BASIS");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "REFERENCE_DETAILS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        //Public Function GetExportBookingSeaCmdtyLCLQuery(Optional ByVal BkgPks As String = "", Optional ByVal Currency As String = "") As DataSet
        //    Dim sb As New StringBuilder(5000)
        //    sb.Append(" SELECT ")
        //    sb.Append("       BKGCOMM.COMMODITY_ID COMMODITY,")
        //    sb.Append("       BKGCOMM.BOOKING_TRN_CARGO_PK ")
        //    sb.Append("  FROM BOOKING_SEA_TBL BST,")
        //    sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,")
        //    sb.Append("       CONTAINER_TYPE_MST_TBL CONTTYPE,")
        //    sb.Append("       (SELECT BTCD.BOOKING_TRN_SEA_FK,")
        //    sb.Append("               BTCD.BOOKING_TRN_CARGO_PK,")
        //    sb.Append("               BTCD.PACK_COUNT         P1,")
        //    sb.Append("               BTCD.NET_WEIGHT         N1,")
        //    sb.Append("               BTCD.GROSS_WEIGHT       G1,")
        //    sb.Append("               BTCD.REMARK,")
        //    sb.Append("               COMM.COMMODITY_ID,")
        //    sb.Append("               PACK.PACK_TYPE_ID,")
        //    sb.Append("               BCD.PACK_COUNT          P2,")
        //    sb.Append("               BCD.NET_WEIGHT          N2,")
        //    sb.Append("               BCD.GROSS_WEIGHT        G2,")
        //    sb.Append("               BCD.VOLUME_IN_CBM       V1")
        //    sb.Append("          FROM BOOKING_TRN_CARGO_DTL BTCD,")
        //    sb.Append("               BOOKING_COMMODITY_DTL BCD,")
        //    sb.Append("               COMMODITY_MST_TBL     COMM,")
        //    sb.Append("               PACK_TYPE_MST_TBL     PACK")
        //    sb.Append("         WHERE BTCD.BOOKING_TRN_CARGO_PK = BCD.BOOKING_CARGO_DTL_FK")
        //    sb.Append("           AND COMM.COMMODITY_MST_PK = BCD.COMMODITY_MST_FK")
        //    sb.Append("           AND PACK.PACK_TYPE_MST_PK = BCD.PACK_TYPE_FK) BKGCOMM ")
        //    sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" & BkgPks & ")")
        //    sb.Append("   AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK")
        //    sb.Append("   AND CONTTYPE.CONTAINER_TYPE_MST_PK = BTSF.CONTAINER_TYPE_MST_FK")
        //    sb.Append("   AND BKGCOMM.BOOKING_TRN_SEA_FK = BTSF.BOOKING_TRN_SEA_PK")

        //    DA = objWF.GetDataAdapter(sb.ToString)
        //    DA.Fill(MainDS, "COMMODITY")
        //    Try
        //        For Each _row As DataRow In MainDS.Tables(0).Rows
        //            For Each col As DataColumn In MainDS.Tables(0).Columns
        //                If IsNothing(_row(col.ColumnName)) Or IsDBNull(_row(col.ColumnName)) Then
        //                    Try
        //                        _row(col.ColumnName) = " "
        //                    Catch ex As Exception
        //                        _row(col.ColumnName) = 0
        //                    End Try
        //                End If
        //            Next
        //        Next
        //    Catch ex As Exception
        //    End Try

        //    Return MainDS
        //End Function
        public DataSet GetExportBookingSeaCmdtyDetailsLCLQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT COMM.COMMODITY_ID,TRN.BOOKING_TRN_SEA_PK FROM");
            sb.Append("  BOOKING_SEA_TBL BST,");
            sb.Append("  BOOKING_TRN_SEA_FCL_LCL TRN,");
            sb.Append("  COMMODITY_MST_TBL COMM");
            sb.Append("  WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("  AND BST.BOOKING_SEA_PK = TRN.BOOKING_SEA_FK AND");
            sb.Append("  COMM.COMMODITY_MST_PK IN");
            sb.Append("       (SELECT *");
            sb.Append("          FROM TABLE (SELECT FN_SPLIT(B.COMMODITY_MST_FKS)");
            sb.Append("                        FROM BOOKING_SEA_TBL BST,");
            sb.Append("                        BOOKING_TRN_SEA_FCL_LCL B ");
            sb.Append("                       WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("                       AND BST.BOOKING_SEA_PK = B.BOOKING_SEA_FK))");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "COMMODITY_DETAILS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        #endregion "LCL"

        #region "BBC"

        public DataSet GetExportBookingSeaBBCQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT BTFD.PREFERENCE, BST.BOOKING_REF_NO,");
            sb.Append("       TO_CHAR(BST.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
            sb.Append("                DECODE(BST.STATUS,");
            sb.Append("                       1,");
            sb.Append("                       'Provisional',");
            sb.Append("                       2,");
            sb.Append("                       'Confirmed',");
            sb.Append("                       3,");
            sb.Append("                       'Cancelled',");
            sb.Append("                       4,");
            sb.Append("                       'E-Booking',");
            sb.Append("                       5,");
            sb.Append("                       'Shipped',");
            sb.Append("                       6,");
            sb.Append("                       'Shipped') STATUS,");
            sb.Append("                DECODE(BTSF.TRANS_REFERED_FROM, 1, BTSF.TRANS_REF_NO) QUOTE_REF_NR,");
            sb.Append("                TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
            sb.Append("                DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            sb.Append("                POL.PORT_ID POL,");
            sb.Append("                POD.PORT_ID POD,");
            sb.Append("                SHP.CUSTOMER_ID SHIPPER,");
            sb.Append("                CONS.CUSTOMER_ID CONSIGNEE,");
            sb.Append("                BST.CUSTOMER_REF_NO,");
            sb.Append("                BST.CREDIT_DAYS,");
            sb.Append("                BST.CREDIT_LIMIT,");
            sb.Append("                '" + Currency + "' CREDIT_LIMIT_CURRENCY,");
            sb.Append("                DPAT.AGENT_ID DP_AGENT,");
            sb.Append("                CBAT.AGENT_ID XBKG_AGENT,");
            sb.Append("                CLAT.AGENT_ID COLOAD_AGENT,");
            sb.Append("                CASE");
            sb.Append("                  WHEN BST.CB_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("                   'YES'");
            sb.Append("                  ELSE");
            sb.Append("                   'NO'");
            sb.Append("                END X_BKG,");
            sb.Append("                CASE");
            sb.Append("                  WHEN BST.CL_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("                   'YES'");
            sb.Append("                  ELSE");
            sb.Append("                   'NO'");
            sb.Append("                END CO_LOADING,");
            sb.Append("                CMMT.CARGO_MOVE_CODE,");
            sb.Append("                '' COMMODITY_GROUP_CODE,");
            sb.Append("                '" + Currency + "' CURRENCY,");
            sb.Append("                STMT.INCO_CODE INCO_TERMS,");
            sb.Append("                DECODE(BST.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAY_TYPE,");
            sb.Append("                PTMT.PACK_TYPE_ID PACK_TYPE,");
            sb.Append("                '' PACK_COUNT,");
            sb.Append("                '' GROSS_WEIGHT,");
            sb.Append("                '' NET_WEIGHT,");
            sb.Append("                '' VOLUME,");
            sb.Append("                BKGOTH.FREIGHT_ELEMENT_ID OTH_FREIGHT_ELEMENT,");
            sb.Append("                BKGOTH.CURRENCY_ID OTH_CURRENCY,");
            sb.Append("                BKGOTH.AMOUNT OTH_AMOUNT,");
            sb.Append("                DECODE(BKGOTH.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') OTH_PYMT_TYPE,");
            sb.Append("                BST.COL_ADDRESS COLLECTION_ADDRESS,");
            sb.Append("                BST.DEL_ADDRESS DELIVERY_ADDRESS,");
            sb.Append("                CGMT.COMMODITY_GROUP_CODE COMMODITY_GROUP,");
            sb.Append("                COMM.COMMODITY_ID,");
            sb.Append("                COMM.COMMODITY_NAME,");
            sb.Append("                DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Operator Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') REFERENCE_TYPE,");
            sb.Append("                BTSF.TRANS_REF_NO REF_NR,");
            sb.Append("                OMT.OPERATOR_ID LINE,");
            sb.Append("                PTMT.PACK_TYPE_ID PACK,");
            sb.Append("                BASIS.DIMENTION_ID TYPE,");
            sb.Append("                '' BOXES,");
            //sb.Append("                BTSF.ALL_IN_TARIFF BOOKING_RATE, ") '----MODIFICATION REQUIRED
            //sb.Append("                BTSF.BUYING_RATE TOTAL_RATE, ") '----MODIFICATION REQUIRED
            sb.Append("                 ROUND(FETCH_TOTALBKGFRT(BTSF.BOOKING_TRN_SEA_PK, 2),2) AS BOOKING_RATE, ");
            sb.Append("                 ROUND(FETCH_TOTALBKGFRT(BTSF.BOOKING_TRN_SEA_PK, 1),2) AS TOTAL_RATE, ");
            //sb.Append("                FETCH_FCL_LCL_TOTALBKGFRT(BTSF.BOOKING_TRN_SEA_PK,1," & Session("CURRENCY_MST_PK") & ",2)AS TOTAL_RATE, ")
            sb.Append("                BTSF.QUANTITY QTY,");
            sb.Append("                '' NET_WT,");
            sb.Append("                BTSF.WEIGHT_MT GROSS_WT,");
            sb.Append("                BTSF.VOLUME_CBM VOLUME,");
            sb.Append("                '' REMARKS,");
            sb.Append("                COMM.COMMODITY_NAME COMMODITY,");
            sb.Append("                '' PACK_TYPE,");
            sb.Append("                '' QTY,");
            sb.Append("                '' NET_WT,");
            sb.Append("                '' VOLUME,");
            sb.Append("                ");
            sb.Append("                BTFD.FREIGHT_ELEMENT_ID ELEMENT,");
            sb.Append("                BTFD.CHARGE_BASIS,");
            sb.Append("                BTFD.CURRENCY_ID CURR,");
            sb.Append("                BTFD.MIN_RATE VALUE,");
            sb.Append("                BTFD.TARIFF_RATE BKG_RATE,");
            sb.Append("                BTFD.EXCHANGE_RATE EXRATE,");
            sb.Append("                BTFD.TARIFF_RATE * BTFD.EXCHANGE_RATE AMOUNT,");
            sb.Append("                DECODE(BTFD.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE,");
            sb.Append("                SUM(NVL(BTFD.TARIFF_RATE,0) * NVL(BTFD.EXCHANGE_RATE,0)) FRIEGHT_CHARGES,");
            sb.Append("                SUM(NVL(BKGOTH.AMOUNT,0)) OTH_FREIGHT_CHARGES,");
            sb.Append("                SUM(NVL(BTFD.TARIFF_RATE,0) * NVL(BTFD.EXCHANGE_RATE,1)) +");
            sb.Append("                SUM(NVL(BKGOTH.AMOUNT,0)) TOTAL_FREIGHT_CHARGES,");
            sb.Append("                VAT.VESSEL_ID VSL_CODE,");
            sb.Append("                VAT.VESSEL_NAME VSL_NAME,");
            sb.Append("                TO_CHAR('''' || BST.VOYAGE) VOY,");
            sb.Append("                TO_CHAR(BST.ETD_DATE, DATEFORMAT || ' HH24:MI') ETD_POL,");
            sb.Append("                TO_CHAR(BST.CUT_OFF_DATE, DATEFORMAT || ' HH24:MI') CUT_OFF,");
            sb.Append("                TO_CHAR(BST.ETA_DATE, DATEFORMAT || ' HH24:MI') ETA_POD,");
            sb.Append("                BST.LINE_BKG_NO,");
            sb.Append("                '' CLAUSE,");
            sb.Append("                CASE");
            sb.Append("                  WHEN JCSET.MARKS_NUMBERS IS NOT NULL OR");
            sb.Append("                       JCSET.GOODS_DESCRIPTION IS NOT NULL THEN");
            sb.Append("                   'YES'");
            sb.Append("                  ELSE");
            sb.Append("                   'NO'");
            sb.Append("                END CARGO_DETAIL,");
            sb.Append("                JCSET.MARKS_NUMBERS,");
            sb.Append("                JCSET.GOODS_DESCRIPTION");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       PORT_MST_TBL POL,");
            sb.Append("       PORT_MST_TBL POD,");
            sb.Append("       CUSTOMER_MST_TBL SHP,");
            sb.Append("       CUSTOMER_MST_TBL CONS,");
            sb.Append("       AGENT_MST_TBL DPAT,");
            sb.Append("       AGENT_MST_TBL CBAT,");
            sb.Append("       AGENT_MST_TBL CLAT,");
            sb.Append("       CARGO_MOVE_MST_TBL CMMT,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMT,");
            sb.Append("       PACK_TYPE_MST_TBL PTMT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("       COMMODITY_MST_TBL COMM,");
            sb.Append("       OPERATOR_MST_TBL OMT,");
            sb.Append("       DIMENTION_UNIT_MST_TBL BASIS,");
            sb.Append("       VESSEL_VOYAGE_TBL VAT,");
            sb.Append("       VESSEL_VOYAGE_TRN VVT,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL JCSET,");
            sb.Append("       (SELECT BTOTH.BOOKING_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTOTH.AMOUNT,");
            sb.Append("               BTOTH.FREIGHT_TYPE");
            sb.Append("          FROM BOOKING_TRN_SEA_OTH_CHRG BTOTH,");
            sb.Append("               BOOKING_SEA_TBL          BST,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE BTOTH.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("           AND BTOTH.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("           AND BTOTH.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK) BKGOTH,");
            sb.Append("       ");
            sb.Append("       (SELECT BTFD.BOOKING_TRN_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               DECODE(FEMT.CHARGE_BASIS,");
            sb.Append("                      1,");
            sb.Append("                      '%',");
            sb.Append("                      2,");
            sb.Append("                      'Flat',");
            sb.Append("                      3,");
            sb.Append("                      'Kgs',");
            sb.Append("                      4,");
            sb.Append("                      'Unit') CHARGE_BASIS,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTFD.TARIFF_RATE,");
            sb.Append("               BTFD.EXCHANGE_RATE,");
            sb.Append("               BTFD.MIN_RATE,");
            sb.Append("               BTFD.PYMT_TYPE,");
            sb.Append("               FEMT.PREFERENCE");
            sb.Append("          FROM BOOKING_TRN_SEA_FRT_DTLS BTFD,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE FEMT.FREIGHT_ELEMENT_MST_PK = BTFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           AND CTMT.CURRENCY_MST_PK = BTFD.CURRENCY_MST_FK) BTFD");
            sb.Append("         WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("         AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
            sb.Append("   AND SHP.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
            sb.Append("   AND CONS.CUSTOMER_MST_PK(+) = BST.CONS_CUSTOMER_MST_FK");
            sb.Append("   AND DPAT.AGENT_MST_PK(+) = BST.DP_AGENT_MST_FK");
            sb.Append("   AND CBAT.AGENT_MST_PK(+) = BST.CB_AGENT_MST_FK");
            sb.Append("   AND CLAT.AGENT_MST_PK(+) = BST.CL_AGENT_MST_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = BST.CARGO_MOVE_FK");
            sb.Append("   AND STMT.SHIPPING_TERMS_MST_PK(+) = BST.SHIPPING_TERMS_MST_FK");
            sb.Append("   AND PTMT.PACK_TYPE_MST_PK(+) = BTSF.PACK_TYPE_FK");
            sb.Append("   AND BST.BOOKING_SEA_PK = BKGOTH.BOOKING_SEA_FK(+)");
            sb.Append("   AND CGMT.COMMODITY_GROUP_PK = BST.COMMODITY_GROUP_FK");
            sb.Append("   AND OMT.OPERATOR_MST_PK = BST.OPERATOR_MST_FK");
            sb.Append("   AND BASIS.DIMENTION_UNIT_MST_PK = BTSF.BASIS");
            sb.Append("   AND BTFD.BOOKING_TRN_SEA_FK(+) = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("   AND BST.VESSEL_VOYAGE_FK = VVT.voyage_trn_pk(+)");
            sb.Append("   AND VAT.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND BST.BOOKING_SEA_PK = JCSET.BOOKING_SEA_FK(+)");
            sb.Append("   AND BTSF.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK");
            sb.Append("     GROUP BY   BTFD.PREFERENCE, BST.BOOKING_REF_NO,");
            sb.Append("                BST.BOOKING_DATE,");
            sb.Append("                BST.STATUS,");
            sb.Append("                       ");
            sb.Append("                BTSF.TRANS_REFERED_FROM, ");
            sb.Append("                BST.SHIPMENT_DATE,");
            sb.Append("                BST.CARGO_TYPE,");
            sb.Append("                POL.PORT_ID ,");
            sb.Append("                POD.PORT_ID ,");
            sb.Append("                SHP.CUSTOMER_ID ,");
            sb.Append("                CONS.CUSTOMER_ID ,");
            sb.Append("                BST.CUSTOMER_REF_NO,");
            sb.Append("                BST.CREDIT_DAYS,");
            sb.Append("                BST.CREDIT_LIMIT,");
            sb.Append("                DPAT.AGENT_ID ,");
            sb.Append("                CBAT.AGENT_ID ,");
            sb.Append("                CLAT.AGENT_ID ,");
            sb.Append("                BST.CB_AGENT_MST_FK ,");
            sb.Append("                BST.CL_AGENT_MST_FK ,");
            sb.Append("                CMMT.CARGO_MOVE_CODE,");
            sb.Append("                STMT.INCO_CODE ,");
            sb.Append("                BST.PYMT_TYPE, ");
            sb.Append("                PTMT.PACK_TYPE_ID ,");
            sb.Append("                BKGOTH.FREIGHT_ELEMENT_ID ,");
            sb.Append("                BKGOTH.CURRENCY_ID ,");
            sb.Append("                BKGOTH.AMOUNT ,");
            sb.Append("                BKGOTH.FREIGHT_TYPE, ");
            sb.Append("                BST.COL_ADDRESS ,");
            sb.Append("                BST.DEL_ADDRESS ,");
            sb.Append("                CGMT.COMMODITY_GROUP_CODE ,");
            sb.Append("                COMM.COMMODITY_ID,");
            sb.Append("                COMM.COMMODITY_NAME,");
            sb.Append("                BTSF.TRANS_REFERED_FROM, ");
            sb.Append("                BTSF.TRANS_REFERED_FROM, ");
            sb.Append("                OMT.OPERATOR_ID ,");
            sb.Append("                PTMT.PACK_TYPE_ID ,");
            sb.Append("                BASIS.DIMENTION_ID ,");
            sb.Append("                BTSF.ALL_IN_TARIFF , ");
            //----MODIFICATION REQUIRED
            //sb.Append("                BTSF.BUYING_RATE , ") '----MODIFICATION REQUIRED
            sb.Append("                BTSF.QUANTITY ,");
            sb.Append("                BTSF.WEIGHT_MT ,");
            sb.Append("                BTSF.VOLUME_CBM ,");
            sb.Append("                COMM.COMMODITY_NAME ,");
            sb.Append("                BTFD.FREIGHT_ELEMENT_ID ,");
            sb.Append("                BTFD.CHARGE_BASIS,");
            sb.Append("                BTFD.CURRENCY_ID ,");
            sb.Append("                BTFD.MIN_RATE ,");
            sb.Append("                BTFD.TARIFF_RATE ,");
            sb.Append("                BTFD.EXCHANGE_RATE ,");
            sb.Append("                BTFD.PYMT_TYPE ,");
            sb.Append("                VAT.VESSEL_ID ,");
            sb.Append("                VAT.VESSEL_NAME ,");
            sb.Append("                BST.VOYAGE ,");
            sb.Append("                BST.ETD_DATE ,");
            sb.Append("                BST.CUT_OFF_DATE ,");
            sb.Append("                BST.ETA_DATE ,");
            sb.Append("                BST.LINE_BKG_NO,");
            sb.Append("                JCSET.MARKS_NUMBERS,");
            sb.Append("                JCSET.GOODS_DESCRIPTION,");
            sb.Append("                BTSF.TRANS_REF_NO,");
            sb.Append("                BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("     ORDER BY COMM.COMMODITY_ID, BTFD.PREFERENCE ");
            sb.Append(" ");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "CARGO_DETAILS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaHeaderBBCQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT BST.BOOKING_SEA_PK,");
            sb.Append("       TO_CHAR(BST.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
            sb.Append("       DECODE(BST.STATUS,");
            sb.Append("              1,");
            sb.Append("              'Provisional',");
            sb.Append("              2,");
            sb.Append("              'Confirmed',");
            sb.Append("              3,");
            sb.Append("              'Cancelled',");
            sb.Append("              4,");
            sb.Append("              'E-Booking',");
            sb.Append("              5,");
            sb.Append("              'Shipped',");
            sb.Append("              6,");
            sb.Append("              'Shipped') STATUS,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1, BTSF.TRANS_REF_NO) QUOTE_REF_NR,");
            sb.Append("       TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
            sb.Append("       DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            sb.Append("       POL.PORT_ID POL,");
            sb.Append("       POD.PORT_ID POD,");
            sb.Append("       SHP.CUSTOMER_ID SHIPPER,");
            sb.Append("       CONS.CUSTOMER_ID CONSIGNEE,");
            sb.Append("       BST.CUSTOMER_REF_NO,");
            sb.Append("       BST.CREDIT_DAYS,");
            sb.Append("       BST.CREDIT_LIMIT,");
            sb.Append("       '" + Currency + "' CREDIT_LIMIT_CURRENCY,");
            sb.Append("       DPAT.AGENT_ID DP_AGENT,");
            sb.Append("       CBAT.AGENT_ID XBKG_AGENT,");
            sb.Append("       CLAT.AGENT_ID COLOAD_AGENT,");
            sb.Append("       CASE");
            sb.Append("         WHEN BST.CB_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END X_BKG,");
            sb.Append("       CASE");
            sb.Append("         WHEN BST.CL_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END CO_LOADING,");
            sb.Append("       CMMT.CARGO_MOVE_CODE,");
            sb.Append("       '" + Currency + "' CURRENCY,");
            sb.Append("       STMT.INCO_CODE INCO_TERMS,");
            sb.Append("       DECODE(BST.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAY_TYPE ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       PORT_MST_TBL POL,");
            sb.Append("       PORT_MST_TBL POD,");
            sb.Append("       CUSTOMER_MST_TBL SHP,");
            sb.Append("       CUSTOMER_MST_TBL CONS,");
            sb.Append("       AGENT_MST_TBL DPAT,");
            sb.Append("       AGENT_MST_TBL CBAT,");
            sb.Append("       AGENT_MST_TBL CLAT,");
            sb.Append("       CARGO_MOVE_MST_TBL CMMT,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMT ");
            sb.Append(" WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append(" AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
            sb.Append("   AND SHP.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
            sb.Append("   AND CONS.CUSTOMER_MST_PK(+) = BST.CONS_CUSTOMER_MST_FK");
            sb.Append("   AND DPAT.AGENT_MST_PK(+) = BST.DP_AGENT_MST_FK");
            sb.Append("   AND CBAT.AGENT_MST_PK(+) = BST.CB_AGENT_MST_FK");
            sb.Append("   AND CLAT.AGENT_MST_PK(+) = BST.CL_AGENT_MST_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = BST.CARGO_MOVE_FK");
            sb.Append("   AND STMT.SHIPPING_TERMS_MST_PK(+) = BST.SHIPPING_TERMS_MST_FK");
            sb.Append("");

            //'
            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "HEADER");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName].ToString() == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaCommodityBBCQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("select bst.BOOKING_SEA_PK,trn.booking_trn_sea_pk,cgmt.commodity_group_code COMMODITY_GROUP, cmt.COMMODITY_ID, cmt.COMMODITY_NAME ");
            sb.Append("FROM BOOKING_SEA_TBL bst,BOOKING_TRN_SEA_FCL_LCL trn , commodity_mst_tbl cmt, commodity_group_mst_tbl cgmt");
            sb.Append(" WHERE TO_CHAR(bst.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append(" AND bst.booking_sea_pk = trn.booking_sea_fk");
            sb.Append(" AND trn.commodity_mst_fk = cmt.commodity_mst_pk");
            sb.Append(" AND cgmt.commodity_group_pk = cmt.commodity_group_fk");
            sb.Append("");

            //'
            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "COMMODITY_DETAILS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaReferenceDtlsBBCQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_SEA_PK,BTSF.BOOKING_TRN_SEA_PK,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Operator Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') REFERENCE_TYPE,");
            sb.Append("       BTSF.TRANS_REF_NO REFERENCE_NR,");
            sb.Append("       OMT.OPERATOR_ID LINE,");
            sb.Append("       COMM.COMMODITY_NAME COMMODITY,");
            sb.Append("       PTMT.PACK_TYPE_ID PACK,");
            sb.Append("       BASIS.DIMENTION_ID TYPE,");
            sb.Append("       BTSF.QUANTITY BOXES,");
            sb.Append("       BST.PACK_COUNT QTY,");
            sb.Append("       BTSF.WEIGHT_MT GROSS_WT,");
            sb.Append("       BTSF.VOLUME_CBM VOLUME,");
            sb.Append("       ROUND(FETCH_TOTALBKGFRT(BTSF.BOOKING_TRN_SEA_PK, 2),2) AS BOOKING_RATE, ");
            sb.Append("       ROUND(FETCH_TOTALBKGFRT(BTSF.BOOKING_TRN_SEA_PK, 1),2) AS TOTAL_RATE ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       OPERATOR_MST_TBL OMT,");
            sb.Append("       COMMODITY_MST_TBL COMM,");
            sb.Append("       PACK_TYPE_MST_TBL PTMT,");
            sb.Append("       DIMENTION_UNIT_MST_TBL BASIS ");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND OMT.OPERATOR_MST_PK = BST.OPERATOR_MST_FK");
            sb.Append("   AND BTSF.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK");
            sb.Append("   AND PTMT.PACK_TYPE_MST_PK = BTSF.PACK_TYPE_FK");
            sb.Append("   AND BASIS.DIMENTION_UNIT_MST_PK = BTSF.BASIS");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "REFERENCE_DETAILS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaFrtElementsBBCQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BTSF.BOOKING_TRN_SEA_PK,");
            sb.Append("       BTFD.FREIGHT_ELEMENT_ID FREIGHT_ELEMENT,");
            sb.Append("       BTFD.CHARGE_BASIS,");
            sb.Append("       BTFD.CURRENCY_ID CURRENCY_ID,");
            sb.Append("       BTFD.TARIFF_RATE BKG_RATE,");
            sb.Append("       BTFD.TARIFF_RATE * BTFD.EXCHANGE_RATE AMOUNT,");
            sb.Append("       DECODE(BTFD.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       (SELECT BTFD.BOOKING_TRN_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               DECODE(FEMT.CHARGE_BASIS,");
            sb.Append("                      1,");
            sb.Append("                      '%',");
            sb.Append("                      2,");
            sb.Append("                      'Flat',");
            sb.Append("                      3,");
            sb.Append("                      'Kgs',");
            sb.Append("                      4,");
            sb.Append("                      'Unit') CHARGE_BASIS,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTFD.TARIFF_RATE,");
            sb.Append("               BTFD.EXCHANGE_RATE,");
            sb.Append("               BTFD.MIN_RATE,");
            sb.Append("               BTFD.PYMT_TYPE ");
            sb.Append("          FROM BOOKING_TRN_SEA_FRT_DTLS BTFD,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE FEMT.FREIGHT_ELEMENT_MST_PK = BTFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           AND CTMT.CURRENCY_MST_PK = BTFD.CURRENCY_MST_FK) BTFD");
            sb.Append("   WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("   AND BTFD.BOOKING_TRN_SEA_FK = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "FREIGHT_ELEMENTS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingSeaFooterBBCQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BST.BOOKING_SEA_PK,");
            sb.Append("SUM(NVL(BTFD.TARIFF_RATE,0) * NVL(BTFD.EXCHANGE_RATE,0)) FRIEGHT_CHARGES,");
            sb.Append("       SUM(NVL(BKGOTH.AMOUNT,0)) OTH_FREIGHT_CHARGES,");
            sb.Append("       SUM(NVL(BTFD.TARIFF_RATE,0) * NVL(BTFD.EXCHANGE_RATE,1)) +");
            sb.Append("       SUM(NVL(BKGOTH.AMOUNT,0)) TOTAL_FREIGHT_CHARGES,");
            sb.Append("       VAT.VESSEL_ID VESSEL_CODE,");
            sb.Append("       VAT.VESSEL_NAME VESSEL_NAME,");
            sb.Append("       BST.VOYAGE VOYAGE,");
            sb.Append("       TO_CHAR(BST.ETD_DATE, DATEFORMAT || ' HH24:MI') ETD_POL,");
            sb.Append("       TO_CHAR(BST.CUT_OFF_DATE, DATEFORMAT || ' HH24:MI') CUT_OFF,");
            sb.Append("       TO_CHAR(BST.ETA_DATE, DATEFORMAT || ' HH24:MI') ETA_POD ");
            sb.Append("  FROM BOOKING_SEA_TBL BST,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BTSF,");
            sb.Append("       (SELECT BTOTH.BOOKING_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTOTH.AMOUNT,");
            sb.Append("               BTOTH.FREIGHT_TYPE");
            sb.Append("          FROM BOOKING_TRN_SEA_OTH_CHRG BTOTH,");
            sb.Append("               BOOKING_SEA_TBL          BST,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE BTOTH.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("           AND BTOTH.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("           AND BTOTH.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK) BKGOTH,");
            sb.Append("       (SELECT BTFD.BOOKING_TRN_SEA_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               DECODE(FEMT.CHARGE_BASIS,");
            sb.Append("                      1,");
            sb.Append("                      '%',");
            sb.Append("                      2,");
            sb.Append("                      'Flat',");
            sb.Append("                      3,");
            sb.Append("                      'Kgs',");
            sb.Append("                      4,");
            sb.Append("                      'Unit') CHARGE_BASIS,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTFD.TARIFF_RATE,");
            sb.Append("               BTFD.EXCHANGE_RATE,");
            sb.Append("               BTFD.MIN_RATE,");
            sb.Append("               BTFD.PYMT_TYPE");
            sb.Append("          FROM BOOKING_TRN_SEA_FRT_DTLS BTFD,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE FEMT.FREIGHT_ELEMENT_MST_PK = BTFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           AND CTMT.CURRENCY_MST_PK = BTFD.CURRENCY_MST_FK) BTFD,");
            sb.Append("       VESSEL_VOYAGE_TBL VAT,");
            sb.Append("       VESSEL_VOYAGE_TRN VVT ");
            sb.Append("  WHERE TO_CHAR(BST.BOOKING_SEA_PK) IN (" + BkgPks + ")");
            sb.Append("     AND BST.BOOKING_SEA_PK = BTSF.BOOKING_SEA_FK");
            sb.Append("     AND BST.BOOKING_SEA_PK = BKGOTH.BOOKING_SEA_FK(+)");
            sb.Append("     AND BTFD.BOOKING_TRN_SEA_FK(+) = BTSF.BOOKING_TRN_SEA_PK");
            sb.Append("     AND BST.VESSEL_VOYAGE_FK = VVT.voyage_trn_pk(+)");
            sb.Append("     AND VAT.VESSEL_VOYAGE_TBL_PK(+) = VVT.VESSEL_VOYAGE_TBL_FK");
            sb.Append("     GROUP BY BST.BOOKING_SEA_PK,");
            sb.Append("                VAT.VESSEL_ID,");
            sb.Append("                VAT.VESSEL_NAME, ");
            sb.Append("                BST.VOYAGE,");
            sb.Append("                BST.ETD_DATE,");
            sb.Append("                BST.CUT_OFF_DATE,");
            sb.Append("                BST.ETA_DATE ");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "FOOTER");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        #endregion "BBC"

        #region "AIR"

        public DataSet GetExportAirBooking(string BkgPks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT BAT.BOOKING_AIR_PK,BAT.BOOKING_REF_NO ");
            sb.Append("  FROM BOOKING_AIR_TBL BAT ");
            sb.Append("   WHERE TO_CHAR(BAT.BOOKING_AIR_PK) IN (" + BkgPks + ")");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "BOOKING");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingAirQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT BAT.BOOKING_REF_NO,");
            sb.Append("       TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
            sb.Append("       DECODE(BAT.STATUS,");
            sb.Append("              1,");
            sb.Append("              'Provisional',");
            sb.Append("              2,");
            sb.Append("              'Confirmed',");
            sb.Append("              3,");
            sb.Append("              'Cancelled',");
            sb.Append("              4,");
            sb.Append("              'E-Booking',");
            sb.Append("              5,");
            sb.Append("              'Shipped',");
            sb.Append("              6,");
            sb.Append("              'Shipped') STATUS,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1, BTSF.TRANS_REF_NO) QUOTE_REF_NR,");
            sb.Append("       TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
            sb.Append("       '' MAWB_NR,");
            sb.Append("       POL.PORT_ID AOO,");
            sb.Append("       POD.PORT_ID AOD,");
            sb.Append("       SHP.CUSTOMER_ID SHIPPER,");
            sb.Append("       CONS.CUSTOMER_ID CONSIGNEE,");
            sb.Append("       BAT.CUSTOMER_REF_NO,");
            sb.Append("       BAT.CREDIT_DAYS,");
            sb.Append("       BAT.CREDIT_LIMIT,");
            sb.Append("       '" + Currency + "' CREDIT_LIMIT_CURRENCY,");
            sb.Append("       DPAT.AGENT_ID DP_AGENT,");
            sb.Append("       CBAT.AGENT_ID XBKG_AGENT,");
            sb.Append("       CLAT.AGENT_ID COLOAD_AGENT,");
            sb.Append("       CASE");
            sb.Append("         WHEN BAT.CB_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END X_BKG,");
            sb.Append("       CASE");
            sb.Append("         WHEN BAT.CL_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END CO_LOADING,");
            sb.Append("       CSMT.CUSTOMS_STATUS_CODE, ");
            sb.Append("       CMMT.CARGO_MOVE_CODE,");
            sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMODITY_GROUP,");
            sb.Append("       STMT.INCO_CODE INCO_TERMS,");
            sb.Append("       DECODE(BAT.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAY_TYPE,");
            sb.Append("       CASE WHEN BAT.CARGO_TYPE=1 THEN");
            sb.Append("         'YES'");
            sb.Append("       ELSE ");
            sb.Append("         'NO'");
            sb.Append("         END KGS,");
            sb.Append("       CASE WHEN BAT.CARGO_TYPE=2 THEN");
            sb.Append("         'YES'");
            sb.Append("       ELSE ");
            sb.Append("         'NO'");
            sb.Append("         END ULD,");
            //sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Operator Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') REFERENCE_TYPE,")
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Cust Contract',4,'Air Tariff',5,'Gen Tariff',6,'SRR',7,'Manual') REFERENCE_TYPE,");
            sb.Append("       '" + Currency + "' CURRENCY,");
            sb.Append("       BAT.COL_ADDRESS COLLECTION_ADDRESS,");
            sb.Append("       BAT.DEL_ADDRESS DELIVERY_ADDRESS,");
            sb.Append("       PTMT.PACK_TYPE_ID PACK_TYPE,");
            sb.Append("       BAT.PACK_COUNT,");
            sb.Append("       BAT.GROSS_WEIGHT ACTUAL_WT,");
            sb.Append("       BAT.VOLUME_WEIGHT,");
            sb.Append("       BAT.CHARGEABLE_WEIGHT,");
            sb.Append("       BAT.NO_OF_BOXES ULD_CNT,");
            sb.Append("       BAT.VOLUME_IN_CBM VOLUME,");
            sb.Append("       BAT.DENSITY,");
            //sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Operator Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') REFERENCE_TYPE,")
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Cust Contract',4,'Air Tariff',5,'Gen Tariff',6,'SRR',7,'Manual') REFERENCE_TYPE,");
            sb.Append("       BTSF.TRANS_REF_NO REF_NR,");
            sb.Append("       OMT.AIRLINE_ID AIRLINE,   ");
            sb.Append("       CONTTYPE.BREAKPOINT_ID SLAB,");
            sb.Append("       (SELECT C.COMMODITY_ID");
            sb.Append("  FROM COMMODITY_MST_TBL C");
            sb.Append(" WHERE C.COMMODITY_MST_PK IN");
            sb.Append("       (SELECT *");
            sb.Append("          FROM TABLE (SELECT FN_SPLIT(TRN.COMMODITY_MST_FKS)");
            sb.Append("                        FROM BOOKING_AIR_TBL B,");
            sb.Append("                             BOOKING_TRN_AIR TRN ");
            sb.Append("                       WHERE TO_CHAR(B.BOOKING_AIR_PK) IN (" + BkgPks + ")");
            sb.Append("                       AND B.BOOKING_AIR_PK = TRN.BOOKING_AIR_FK))) COMMODITY,");
            sb.Append("       BTSF.ALL_IN_TARIFF BKG_AMNT,");
            sb.Append("       BKGOTH.FREIGHT_ELEMENT_ID OTH_FRT_ELEMENT,");
            sb.Append("       BKGOTH.CURRENCY_ID OTH_CURR,");
            sb.Append("       BKGOTH.CHARGE_BASIS,");
            sb.Append("       BKGOTH.AMOUNT OTH_AMT,");
            sb.Append("       DECODE(BKGOTH.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') OTH_PYMT_TYPE, ");
            sb.Append("       BTFD.FREIGHT_ELEMENT_ID FRT_ELEMENT,");
            sb.Append("       BTFD.CURRENCY_ID CURR,");
            sb.Append("       BTFD.CHARGE_BASIS,");
            sb.Append("       BTFD.BASIS_RATE RATE,");
            sb.Append("       BTFD.TARIFF_RATE BKG_AMT,");
            sb.Append("       DECODE(BTFD.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE,");
            sb.Append("       OMT.AIRLINE_ID,");
            sb.Append("       BAT.FLIGHT_NO,");
            sb.Append("       TO_CHAR(BAT.ETD_DATE, DATEFORMAT) ETD_AOO,");
            sb.Append("       TO_CHAR(BAT.CUT_OFF_DATE, DATEFORMAT) CUT_OFF,");
            sb.Append("       TO_CHAR(BAT.ETA_DATE, DATEFORMAT) ETA_AOD,");
            sb.Append("       BAT.LINE_BKG_NO,");
            sb.Append("       CASE WHEN JCSET.MARKS_NUMBERS IS NOT NULL ");
            sb.Append("         OR JCSET.GOODS_DESCRIPTION IS NOT NULL THEN 'YES'");
            sb.Append("         ELSE");
            sb.Append("           'NO' END OTHER_DETAILS,");
            sb.Append("       JCSET.MARKS_NUMBERS,");
            sb.Append("       JCSET.GOODS_DESCRIPTION");
            sb.Append("  FROM BOOKING_AIR_TBL BAT,");
            sb.Append("       BOOKING_TRN_AIR BTSF,");
            sb.Append("       PORT_MST_TBL POL,");
            sb.Append("       PORT_MST_TBL POD,");
            sb.Append("       CUSTOMER_MST_TBL SHP,");
            sb.Append("       CUSTOMER_MST_TBL CONS,");
            sb.Append("       AGENT_MST_TBL DPAT,");
            sb.Append("       AGENT_MST_TBL CBAT,");
            sb.Append("       AGENT_MST_TBL CLAT,");
            sb.Append("       CARGO_MOVE_MST_TBL CMMT,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMT,");
            sb.Append("       CUSTOMS_STATUS_MST_TBL CSMT,");
            sb.Append("       PACK_TYPE_MST_TBL PTMT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("       AIRLINE_MST_TBL OMT,");
            sb.Append("       AIRFREIGHT_SLABS_TBL CONTTYPE,");
            sb.Append("       JOB_CARD_AIR_EXP_TBL JCSET,");
            sb.Append("       (SELECT BTOTH.BOOKING_TRN_AIR_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               BTOTH.CHARGE_BASIS,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTOTH.AMOUNT,");
            sb.Append("               BTOTH.FREIGHT_TYPE");
            sb.Append("          FROM BOOKING_TRN_AIR_OTH_CHRG BTOTH,");
            sb.Append("               BOOKING_AIR_TBL          BAT,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE BTOTH.BOOKING_TRN_AIR_FK = BAT.BOOKING_AIR_PK");
            sb.Append("           AND BTOTH.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("           AND BTOTH.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK) BKGOTH,");
            sb.Append("     ");
            sb.Append("       (SELECT BTFD.BOOKING_TRN_AIR_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               DECODE(FEMT.CHARGE_BASIS,");
            sb.Append("                      1,");
            sb.Append("                      '%',");
            sb.Append("                      2,");
            sb.Append("                      'Flat',");
            sb.Append("                      3,");
            sb.Append("                      'Kgs',");
            sb.Append("                      4,");
            sb.Append("                      'Unit') CHARGE_BASIS,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTFD.TARIFF_RATE,");
            sb.Append("               BTFD.BASIS_RATE,");
            sb.Append("               BTFD.PYMT_TYPE");
            sb.Append("          FROM BOOKING_TRN_AIR_FRT_DTLS BTFD,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE FEMT.FREIGHT_ELEMENT_MST_PK = BTFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           AND CTMT.CURRENCY_MST_PK = BTFD.CURRENCY_MST_FK) BTFD ");
            sb.Append(" WHERE TO_CHAR(BAT.BOOKING_AIR_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BAT.BOOKING_AIR_PK = BTSF.BOOKING_AIR_FK");
            sb.Append("   AND POL.PORT_MST_PK = BAT.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = BAT.PORT_MST_POD_FK");
            sb.Append("   AND SHP.CUSTOMER_MST_PK = BAT.CUST_CUSTOMER_MST_FK");
            sb.Append("   AND CONS.CUSTOMER_MST_PK(+) = BAT.CONS_CUSTOMER_MST_FK");
            sb.Append("   AND DPAT.AGENT_MST_PK(+) = BAT.DP_AGENT_MST_FK");
            sb.Append("   AND CBAT.AGENT_MST_PK(+) = BAT.CB_AGENT_MST_FK");
            sb.Append("   AND CLAT.AGENT_MST_PK(+) = BAT.CL_AGENT_MST_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = BAT.CARGO_MOVE_FK");
            sb.Append("   AND CSMT.CUSTOMS_CODE_MST_PK(+) = BAT.CUSTOMS_CODE_MST_FK");
            sb.Append("   AND STMT.SHIPPING_TERMS_MST_PK(+) = BAT.SHIPPING_TERMS_MST_FK");
            sb.Append("   AND PTMT.PACK_TYPE_MST_PK(+) = BAT.PACK_TYPE_MST_FK");
            sb.Append("   AND BTSF.BOOKING_TRN_AIR_PK = BKGOTH.BOOKING_TRN_AIR_FK(+)");
            sb.Append("   AND CGMT.COMMODITY_GROUP_PK = BAT.COMMODITY_GROUP_FK");
            sb.Append("   AND OMT.AIRLINE_MST_PK(+) = BAT.AIRLINE_MST_FK");
            sb.Append("   AND CONTTYPE.AIRFREIGHT_SLABS_TBL_PK(+) = BTSF.BASIS");
            sb.Append("   AND BTFD.BOOKING_TRN_AIR_FK(+) = BTSF.BOOKING_TRN_AIR_PK");
            sb.Append("   AND BAT.BOOKING_AIR_PK = JCSET.BOOKING_AIR_FK(+)");
            sb.Append("");

            DataSet dsBkg = new DataSet();
            dsBkg = objWF.GetDataSet(sb.ToString());

            try
            {
                foreach (DataRow _row in dsBkg.Tables[0].Rows)
                {
                    foreach (DataColumn col in dsBkg.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return dsBkg;
        }

        public DataSet GetExportBookingAirHeaderQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT BAT.BOOKING_AIR_PK,");
            sb.Append("       TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
            sb.Append("       DECODE(BAT.STATUS,");
            sb.Append("              1,");
            sb.Append("              'Provisional',");
            sb.Append("              2,");
            sb.Append("              'Confirmed',");
            sb.Append("              3,");
            sb.Append("              'Cancelled',");
            sb.Append("              4,");
            sb.Append("              'E-Booking',");
            sb.Append("              5,");
            sb.Append("              'Shipped',");
            sb.Append("              6,");
            sb.Append("              'Shipped') STATUS,");
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1, BTSF.TRANS_REF_NO) QUOTE_REF_NR,");
            sb.Append("       TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
            sb.Append("       '' MAWB_NR,");
            sb.Append("       POL.PORT_ID AOO,");
            sb.Append("       POD.PORT_ID AOD,");
            sb.Append("       SHP.CUSTOMER_ID SHIPPER,");
            sb.Append("       CONS.CUSTOMER_ID CONSIGNEE,");
            sb.Append("       BAT.CUSTOMER_REF_NO,");
            sb.Append("       BAT.CREDIT_DAYS,");
            sb.Append("       BAT.CREDIT_LIMIT,");
            sb.Append("       '" + Currency + "' CREDIT_LIMIT_CURRENCY,");
            sb.Append("       DPAT.AGENT_ID DP_AGENT,");
            sb.Append("       CBAT.AGENT_ID XBKG_AGENT,");
            sb.Append("       CLAT.AGENT_ID COLOAD_AGENT,");
            sb.Append("       CASE");
            sb.Append("         WHEN BAT.CB_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END X_BKG,");
            sb.Append("       CASE");
            sb.Append("         WHEN BAT.CL_AGENT_MST_FK IS NOT NULL THEN");
            sb.Append("          'YES'");
            sb.Append("         ELSE");
            sb.Append("          'NO'");
            sb.Append("       END CO_LOADING,");
            sb.Append("       CSMT.CUSTOMS_STATUS_CODE, ");
            sb.Append("       CMMT.CARGO_MOVE_CODE,");
            sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMODITY_GROUP,");
            sb.Append("       STMT.INCO_CODE INCO_TERMS,");
            sb.Append("       DECODE(BAT.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAY_TYPE,");
            sb.Append("       CASE WHEN BAT.CARGO_TYPE=1 THEN");
            sb.Append("         'YES'");
            sb.Append("       ELSE ");
            sb.Append("         'NO'");
            sb.Append("         END KGS,");
            sb.Append("       CASE WHEN BAT.CARGO_TYPE=2 THEN");
            sb.Append("         'YES'");
            sb.Append("       ELSE ");
            sb.Append("         'NO'");
            sb.Append("         END ULD,");
            sb.Append("       PTMT.PACK_TYPE_ID PACK_TYPE,");
            sb.Append("       BAT.PACK_COUNT,");
            sb.Append("       BAT.GROSS_WEIGHT ACTUAL_WT,");
            sb.Append("       BAT.VOLUME_WEIGHT,");
            sb.Append("       BAT.CHARGEABLE_WEIGHT,");
            sb.Append("       BAT.NO_OF_BOXES ULD_CNT,");
            sb.Append("       BAT.VOLUME_IN_CBM VOLUME,");
            sb.Append("       BAT.DENSITY,");
            sb.Append("       '" + Currency + "' CURRENCY ");
            sb.Append("  FROM BOOKING_AIR_TBL BAT,");
            sb.Append("       BOOKING_TRN_AIR BTSF,");
            sb.Append("       PORT_MST_TBL POL,");
            sb.Append("       PORT_MST_TBL POD,");
            sb.Append("       CUSTOMER_MST_TBL SHP,");
            sb.Append("       CUSTOMER_MST_TBL CONS,");
            sb.Append("       AGENT_MST_TBL DPAT,");
            sb.Append("       AGENT_MST_TBL CBAT,");
            sb.Append("       AGENT_MST_TBL CLAT,");
            sb.Append("       CARGO_MOVE_MST_TBL CMMT,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMT,");
            sb.Append("       CUSTOMS_STATUS_MST_TBL CSMT,");
            sb.Append("       PACK_TYPE_MST_TBL PTMT,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT ");
            sb.Append(" WHERE TO_CHAR(BAT.BOOKING_AIR_PK) IN (" + BkgPks + ")");
            sb.Append(" AND BAT.BOOKING_AIR_PK = BTSF.BOOKING_AIR_FK");
            sb.Append("   AND POL.PORT_MST_PK = BAT.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = BAT.PORT_MST_POD_FK");
            sb.Append("   AND SHP.CUSTOMER_MST_PK = BAT.CUST_CUSTOMER_MST_FK");
            sb.Append("   AND CONS.CUSTOMER_MST_PK(+) = BAT.CONS_CUSTOMER_MST_FK");
            sb.Append("   AND DPAT.AGENT_MST_PK(+) = BAT.DP_AGENT_MST_FK");
            sb.Append("   AND CBAT.AGENT_MST_PK(+) = BAT.CB_AGENT_MST_FK");
            sb.Append("   AND CLAT.AGENT_MST_PK(+) = BAT.CL_AGENT_MST_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = BAT.CARGO_MOVE_FK");
            sb.Append("   AND CSMT.CUSTOMS_CODE_MST_PK(+) = BAT.CUSTOMS_CODE_MST_FK");
            sb.Append("   AND STMT.SHIPPING_TERMS_MST_PK(+) = BAT.SHIPPING_TERMS_MST_FK");
            sb.Append("   AND PTMT.PACK_TYPE_MST_PK(+) = BAT.PACK_TYPE_MST_FK");
            sb.Append("   AND CGMT.COMMODITY_GROUP_PK = BAT.COMMODITY_GROUP_FK");
            sb.Append("");

            //'
            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "HEADER");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingAirAddressQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BAT.BOOKING_AIR_PK,");
            sb.Append("       BAT.COL_ADDRESS COLLECTION_ADDRESS,");
            sb.Append("       BAT.DEL_ADDRESS DELIVERY_ADDRESS ");
            sb.Append("  FROM BOOKING_AIR_TBL BAT ");
            sb.Append("   WHERE TO_CHAR(BAT.BOOKING_AIR_PK) IN (" + BkgPks + ")");
            sb.Append(" AND (BAT.COL_ADDRESS IS NOT NULL OR BAT.DEL_ADDRESS IS NOT NULL) ");
            sb.Append(" ");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "ADDRESS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingAirReferenceDtlsQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BAT.BOOKING_AIR_PK,BTSF.BOOKING_TRN_AIR_PK,");
            //sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Operator Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') REFERENCE_TYPE,")
            sb.Append("       DECODE(BTSF.TRANS_REFERED_FROM, 1,'Quotation',2,'Spot Rate',3,'Cust Contract',4,'Air Tariff',5,'Gen Tariff',6,'SRR',7,'Manual') REFERENCE_TYPE,");
            sb.Append("       BTSF.TRANS_REF_NO REF_NR,");
            sb.Append("       OMT.AIRLINE_ID AIRLINE,   ");
            sb.Append("       CONTTYPE.BREAKPOINT_ID SLAB,");
            sb.Append("       BTSF.ALL_IN_TARIFF BKG_AMNT ");
            sb.Append("  FROM BOOKING_AIR_TBL BAT,");
            sb.Append("       BOOKING_TRN_AIR BTSF,");
            sb.Append("       AIRLINE_MST_TBL OMT,");
            sb.Append("       AIRFREIGHT_SLABS_TBL CONTTYPE ");
            sb.Append(" WHERE TO_CHAR(BAT.BOOKING_AIR_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BAT.BOOKING_AIR_PK = BTSF.BOOKING_AIR_FK");
            sb.Append("   AND OMT.AIRLINE_MST_PK = BAT.AIRLINE_MST_FK");
            sb.Append("   AND CONTTYPE.AIRFREIGHT_SLABS_TBL_PK(+) = BTSF.BASIS");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "REFERENCE_DETAILS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingAirCommodityQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT COMM.COMMODITY_ID,COMM.COMMODITY_NAME,TRN.BOOKING_TRN_AIR_PK FROM");
            sb.Append("  BOOKING_AIR_TBL BAT,");
            sb.Append("  BOOKING_TRN_AIR TRN,");
            sb.Append("  COMMODITY_MST_TBL COMM");
            sb.Append("  WHERE TO_CHAR(BAT.BOOKING_AIR_PK) IN (" + BkgPks + ")");
            sb.Append("  AND BAT.BOOKING_AIR_PK = TRN.BOOKING_AIR_FK AND");
            sb.Append("  COMM.COMMODITY_MST_PK IN");
            sb.Append("       (SELECT *");
            sb.Append("          FROM TABLE (SELECT FN_SPLIT(TRN.COMMODITY_MST_FKS)");
            sb.Append("                        FROM BOOKING_AIR_TBL B,");
            sb.Append("                             BOOKING_TRN_AIR TRN ");
            sb.Append("                       WHERE TO_CHAR(B.BOOKING_AIR_PK) IN (" + BkgPks + ")");
            sb.Append("                       AND B.BOOKING_AIR_PK = TRN.BOOKING_AIR_FK))");
            sb.Append("");

            //'
            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "COMMODITY");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingAirOthChrgQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BTSF.BOOKING_TRN_AIR_PK,");
            sb.Append("       BKGOTH.FREIGHT_ELEMENT_ID OTH_FREIGHT_ELEMENT,");
            sb.Append("       BKGOTH.CURRENCY_ID OTH_CURRENCY,");
            sb.Append("       BKGOTH.CHARGE_BASIS,");
            sb.Append("       BKGOTH.CHARGE_BASIS RATE,");
            sb.Append("       BKGOTH.AMOUNT OTH_AMOUNT,");
            sb.Append("       DECODE(BKGOTH.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') OTH_PYMT_TYPE ");
            sb.Append("  FROM BOOKING_AIR_TBL BAT,");
            sb.Append("       BOOKING_TRN_AIR BTSF,");
            sb.Append("       (SELECT BTOTH.BOOKING_TRN_AIR_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               BTOTH.CHARGE_BASIS,");
            sb.Append("               BTOTH.BASIS_RATE,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTOTH.AMOUNT,");
            sb.Append("               BTOTH.FREIGHT_TYPE");
            sb.Append("          FROM BOOKING_TRN_AIR_OTH_CHRG BTOTH,");
            sb.Append("               BOOKING_AIR_TBL          BAT,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE BTOTH.BOOKING_TRN_AIR_FK = BAT.BOOKING_AIR_PK");
            sb.Append("           AND BTOTH.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("           AND BTOTH.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK) BKGOTH ");
            sb.Append(" WHERE TO_CHAR(BAT.BOOKING_AIR_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BAT.BOOKING_AIR_PK = BTSF.BOOKING_AIR_FK");
            sb.Append("   AND BTSF.BOOKING_TRN_AIR_PK = BKGOTH.BOOKING_TRN_AIR_FK ");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "OTHER_CHARGES");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingAirFrtElementsQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BTSF.BOOKING_TRN_AIR_PK,");
            sb.Append("       BTFD.FREIGHT_ELEMENT_ID FRT_ELEMENT,");
            sb.Append("       BTFD.CURRENCY_ID CURR,");
            sb.Append("       BTFD.CHARGE_BASIS,");
            sb.Append("       BTFD.BASIS_RATE RATE,");
            sb.Append("       BTFD.TARIFF_RATE BKG_AMT,");
            sb.Append("       DECODE(BTFD.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENT_TYPE ");
            sb.Append("  FROM BOOKING_AIR_TBL BAT,");
            sb.Append("       BOOKING_TRN_AIR BTSF,");
            sb.Append("       (SELECT BTFD.BOOKING_TRN_AIR_FK,");
            sb.Append("               FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("               DECODE(FEMT.CHARGE_BASIS,");
            sb.Append("                      1,");
            sb.Append("                      '%',");
            sb.Append("                      2,");
            sb.Append("                      'Flat',");
            sb.Append("                      3,");
            sb.Append("                      'Kgs',");
            sb.Append("                      4,");
            sb.Append("                      'Unit') CHARGE_BASIS,");
            sb.Append("               CTMT.CURRENCY_ID,");
            sb.Append("               BTFD.TARIFF_RATE,");
            sb.Append("               BTFD.BASIS_RATE,");
            sb.Append("               BTFD.PYMT_TYPE");
            sb.Append("          FROM BOOKING_TRN_AIR_FRT_DTLS BTFD,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("               CURRENCY_TYPE_MST_TBL    CTMT");
            sb.Append("         WHERE FEMT.FREIGHT_ELEMENT_MST_PK = BTFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           AND CTMT.CURRENCY_MST_PK = BTFD.CURRENCY_MST_FK) BTFD");
            sb.Append(" WHERE TO_CHAR(BAT.BOOKING_AIR_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BAT.BOOKING_AIR_PK = BTSF.BOOKING_AIR_FK");
            sb.Append("   AND BTFD.BOOKING_TRN_AIR_FK = BTSF.BOOKING_TRN_AIR_PK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "FREIGHT_ELEMENTS");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        public DataSet GetExportBookingAirFooterQuery(string BkgPks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT BAT.BOOKING_AIR_PK,");
            sb.Append("       OMT.AIRLINE_ID,");
            sb.Append("       BAT.FLIGHT_NO,");
            sb.Append("       TO_CHAR(BAT.ETD_DATE, DATEFORMAT) ETD_AOO,");
            sb.Append("       TO_CHAR(BAT.CUT_OFF_DATE, DATEFORMAT) CUT_OFF,");
            sb.Append("       TO_CHAR(BAT.ETA_DATE, DATEFORMAT) ETA_AOD,");
            sb.Append("       BAT.LINE_BKG_NO ");
            sb.Append("  FROM BOOKING_AIR_TBL BAT,");
            sb.Append("       BOOKING_TRN_AIR BTSF,");
            sb.Append("       AIRLINE_MST_TBL OMT ");
            sb.Append(" WHERE TO_CHAR(BAT.BOOKING_AIR_PK) IN (" + BkgPks + ")");
            sb.Append("   AND BAT.BOOKING_AIR_PK = BTSF.BOOKING_AIR_FK");
            sb.Append("   AND OMT.AIRLINE_MST_PK(+) = BAT.AIRLINE_MST_FK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "FOOTER");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return MainDS;
        }

        #endregion "AIR"

        #endregion "Generate Booking"

        #region "Generate Cargo Manifest"

        public DataSet GenerateCargoManifest(string Pks, int Biz, string CargoType = "", string Currency = "", int inc = 0, string ExportType = "", string DocType = "")
        {
            //Air
            if (Biz == 1)
            {
                //XML
                if (Convert.ToInt32(ExportType) == 2)
                {
                    if (inc == 1)
                    {
                        ds = GetExportAirCargoManifestMBLDetail(Pks);
                        ds = GetExportAirCargoManifestMBLHeader(Pks);
                    }
                    else if (inc == 2)
                    {
                        ds = GetExportAirCargoManifestHBLDetail(Pks);
                    }
                    else if (inc == 3)
                    {
                        ds = GetExportAirCargoManifestShipConsDetail(Pks);
                    }
                    else if (inc == 4)
                    {
                        ds = GetExportAirCargoManifestContCargoDetail(Pks);
                    }
                    else if (inc == 5)
                    {
                        ds = GetExportAirCargoManifestFreightDetail(Pks);
                    }
                }
                else
                {
                    if (Convert.ToInt32(DocType) == 3)
                    {
                        ds = GetExportCargoManifestAirQuery(Pks, Currency);
                    }
                    else if (Convert.ToInt32(DocType) == 4)
                    {
                        ds = GetExportFreightManifestAirQuery(Pks, Currency);
                    }
                }
                //Sea
            }
            else
            {
                //XML
                if (Convert.ToInt32(ExportType) == 2)
                {
                    if (inc == 1)
                    {
                        ds = GetExportSeaCargoManifestMBLDetail(Pks);
                        ds = GetExportSeaCargoManifestMBLHeader(Pks);
                    }
                    else if (inc == 2)
                    {
                        ds = GetExportSeaCargoManifestHBLDetail(Pks);
                    }
                    else if (inc == 3)
                    {
                        ds = GetExportSeaCargoManifestShipConsDetail(Pks);
                    }
                    else if (inc == 4)
                    {
                        ds = GetExportSeaCargoManifestContCargoDetail(Pks);
                    }
                    else if (inc == 5)
                    {
                        ds = GetExportSeaCargoManifestFreightDetail(Pks);
                    }
                }
                else
                {
                    if (Convert.ToInt32(DocType) == 3)
                    {
                        ds = GetExportCargoManifestSeaQuery(Pks, Currency);
                    }
                    else if (Convert.ToInt32(DocType) == 4)
                    {
                        ds = GetExportFreightManifestSeaQuery(Pks, Currency);
                    }
                }
            }
            try
            {
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

        #region " CARGO/FREIGHT MANIFEST SEA "

        public DataSet GetExportCargoManifestSeaQuery(string Pks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT 'SEA' BIZ_TYPE,");
            //sb.Append("               -------LOCATION---")
            sb.Append("                LMT.LOCATION_NAME,");
            sb.Append("                LMT.ADDRESS_LINE1,");
            sb.Append("                LMT.ADDRESS_LINE2,");
            sb.Append("                LMT.ADDRESS_LINE3,");
            sb.Append("                LMT.CITY,");
            sb.Append("                LMT.ZIP POST_CODE,");
            sb.Append("                COUNTRY.COUNTRY_NAME,");
            sb.Append("                LMT.TELE_PHONE_NO,");
            sb.Append("                LMT.FAX_NO,");
            sb.Append("                LMT.E_MAIL_ID,");
            //sb.Append("                ------LOCATION----")
            sb.Append("                'CM' MANIFEST_TYPE,");
            //sb.Append("                ----AGENT--------")
            sb.Append("                AMT.AGENT_NAME POD_AGENT_NAME,");
            sb.Append("                ACD.ADM_ADDRESS_1 POD_AGENT_ADDRESS1,");
            sb.Append("                ACD.ADM_ADDRESS_2 POD_AGENT_ADDRESS2,");
            sb.Append("                ACD.ADM_ADDRESS_3 POD_AGENT_ADDRESS3,");
            sb.Append("                ACD.COR_CITY POD_CITY,");
            sb.Append("                ACD.ADM_ZIP_CODE POD_POST_CODE,");
            //sb.Append("                ACD.ADM_ZIP_CODE,")
            sb.Append("                ACMT.COUNTRY_NAME POD_COUNTRY_NAME,");
            //sb.Append("                -----AGENT------")
            sb.Append("                OMT.OPERATOR_ID CARRIER,");
            sb.Append("                VVT.VESSEL_NAME,");
            sb.Append("                VOY.VOYAGE,");
            sb.Append("                MBL.MBL_REF_NO,");
            sb.Append("                JSE.JOBCARD_REF_NO,");
            sb.Append("                DECODE(MBL.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE,");
            sb.Append("                POLMST.PORT_ID POL,");
            sb.Append("                PODMST.PORT_ID POD,");
            sb.Append("                CMMT.CARGO_MOVE_CODE MOVE_CODE,");
            sb.Append("                TO_CHAR(MBL.MBL_DATE, DATEFORMAT) MBL_DATE,");
            sb.Append("                DECODE(MBL.PYMT_TYPE,1,'Prepaid',2,'Collect') PAYMENT_TYPE,");
            sb.Append("                HBL.HBL_REF_NO,");
            //sb.Append("                STMST.INCO_CODE TERMS,")
            //sb.Append("                -----SHIPPER CONSIGNEE---")
            sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPER_NAME,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_1 SHIPPER_ADDRESS1,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_2 SHIPPER_ADDRESS2,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_3 SHIPPER_ADDRESS3,");
            sb.Append("                SHIPPERDTLS.ADM_CITY SHIPPER_CITY,");
            sb.Append("                SHIPPERDTLS.ADM_ZIP_CODE SHIPPER_POST_CODE,");
            sb.Append("                SHIPPERCNT.COUNTRY_NAME SHIPPER_COUNTRY,");
            sb.Append("                SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPER_PHONE,");
            sb.Append("                SHIPPERDTLS.ADM_FAX_NO SHIPPER_FAX,");
            //sb.Append("                SHIPPERDTLS.ADM_EMAIL_ID SHIPPER_EMAIL,")
            sb.Append("                CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEE_ADDRESS1,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEE_ADDRESS2,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEE_ADDRESS3,");
            sb.Append("                CONSIGNEEDTLS.ADM_CITY CONSIGNEE_CITY,");
            sb.Append("                CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEE_POST_CODE,");
            sb.Append("                CONSIGNEECNT.COUNTRY_NAME CONSIGNEE_COUNTRY,");
            sb.Append("                CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEE_PHONE,");
            sb.Append("                CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEE_FAX,");
            //sb.Append("                CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEE_EMAIL,")
            //sb.Append("                ----")
            sb.Append("                PFD.PORT_NAME PFD,");
            sb.Append("                JTSEC.SEAL_NUMBER,");
            sb.Append("                JTSEC.CONTAINER_NUMBER,");
            sb.Append("                JSE.MARKS_NUMBERS MARKS_NUMBER,");
            sb.Append("                PY.PACK_TYPE_DESC PACK_TYPE,");
            sb.Append("                JTSEC.PACK_COUNT COUNT,");
            sb.Append("                JTSEC.GROSS_WEIGHT WEIGHT,");
            sb.Append("                JTSEC.VOLUME_IN_CBM VOLUME,");
            sb.Append("                JSE.GOODS_DESCRIPTION GOODS_DESC,");
            sb.Append("                ");
            //sb.Append("                ---SPECIAL REQU---")
            sb.Append("                IPACK.PACK_TYPE_ID HAZ_OUTERPACK_TYPE,");
            sb.Append("                OPACK.PACK_TYPE_ID HAZ_INNERPACK_TYPE,");
            sb.Append("                JTSESR.IMDG_CLASS_CODE HAZ_IMDG_CLASS,");
            sb.Append("                JTSESR.UN_NO HAZ_UN_NR,");
            sb.Append("                JTSESR.FLASH_PNT_TEMP FLASH_POINT,");
            sb.Append("                JTSESR.MIN_TEMP,");
            sb.Append("                DECODE(JTSESR.MIN_TEMP_UOM,0,'C',1,'F') MINCF,");
            sb.Append("                JTSESR.MAX_TEMP,");
            sb.Append("                DECODE(JTSESR.MAX_TEMP_UOM,0,'C',1,'F') MAXCF,");
            sb.Append("                JTSESR.EMS_NUMBER EMS_MFAG_NR,");
            sb.Append("                JTSESR.HUMIDITY_FACTOR,");
            sb.Append("                DECODE(JTSESR.IS_PERSHIABLE_GOODS,1,'YES',0,'NO') ISPERISHABLE");
            sb.Append("                ");
            sb.Append("                ");
            sb.Append(" FROM MBL_EXP_TBL            MBL,");
            sb.Append("       PORT_MST_TBL           POLMST,");
            sb.Append("       PORT_MST_TBL           PODMST,");
            sb.Append("       HBL_EXP_TBL            HBL,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL   JSE,");
            sb.Append("       BOOKING_SEA_TBL        BST,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMST,");
            sb.Append("       JOB_TRN_SEA_EXP_CONT   JTSEC,");
            sb.Append("       JOB_TRN_SEA_EXP_SPL_REQ JTSESR,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTMST,");
            sb.Append("       CUSTOMER_MST_TBL       SHIPPER,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  SHIPPERDTLS,");
            sb.Append("       COUNTRY_MST_TBL        SHIPPERCNT,");
            sb.Append("       CUSTOMER_MST_TBL       CONSIGNEE,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  CONSIGNEEDTLS,");
            sb.Append("       COUNTRY_MST_TBL        CONSIGNEECNT,");
            sb.Append("       PORT_MST_TBL           PFD,");
            sb.Append("       PACK_TYPE_MST_TBL      PY,");
            sb.Append("       CARGO_MOVE_MST_TBL     CMMT,");
            sb.Append("       ");
            //sb.Append("       ----")
            sb.Append("       USER_MST_TBL           UMT,");
            sb.Append("       LOCATION_MST_TBL       LMT,");
            sb.Append("       COUNTRY_MST_TBL        COUNTRY,");
            //sb.Append("       ----")
            sb.Append("       AGENT_MST_TBL          AMT,");
            sb.Append("       AGENT_CONTACT_DTLS     ACD,");
            sb.Append("       COUNTRY_MST_TBL        ACMT,");
            //sb.Append("       -----")
            sb.Append("       OPERATOR_MST_TBL       OMT,");
            sb.Append("       VESSEL_VOYAGE_TBL      VVT,");
            sb.Append("       VESSEL_VOYAGE_TRN      VOY,");
            //sb.Append("       ---sr--")
            sb.Append("       PACK_TYPE_MST_TBL     IPACK,");
            sb.Append("       PACK_TYPE_MST_TBL     OPACK");
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MBL.MBL_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("   AND JSE.JOB_CARD_SEA_EXP_PK = JTSEC.JOB_CARD_SEA_EXP_FK(+)");
            sb.Append("   AND JSE.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)");
            sb.Append("   AND JSE.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK(+)");
            sb.Append("   AND BST.PFD_FK = PFD.PORT_MST_PK(+)");
            sb.Append("   AND JTSEC.PACK_TYPE_MST_FK = PY.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPING_TERMS_MST_FK = STMST.SHIPPING_TERMS_MST_PK(+)");
            sb.Append("   AND JTSEC.CONTAINER_TYPE_MST_FK = CTMST.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)");
            sb.Append("   AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)");
            //sb.Append("   ------")
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MBL.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
            sb.Append("   AND LMT.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK");
            sb.Append("   AND MBL.AGENT_NAME = AMT.AGENT_NAME(+)");
            sb.Append("   AND AMT.AGENT_MST_PK = ACD.AGENT_MST_FK(+)");
            sb.Append("   AND ACMT.COUNTRY_MST_PK(+) = ACD.ADM_COUNTRY_MST_FK");
            sb.Append("   AND OMT.OPERATOR_MST_PK = MBL.OPERATOR_MST_FK");
            sb.Append("   AND VOY.VOYAGE_TRN_PK(+)  = MBL.VOYAGE_TRN_FK");
            sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VOY.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = MBL.CARGO_MOVE_FK");
            //sb.Append("   ---")
            sb.Append("   AND JTSEC.JOB_TRN_SEA_EXP_CONT_PK = JTSESR.JOB_TRN_SEA_EXP_CONT_FK(+)");
            sb.Append("   AND IPACK.PACK_TYPE_MST_PK(+)  = JTSESR.INNER_PACK_TYPE_MST_FK ");
            sb.Append("   AND OPACK.PACK_TYPE_MST_PK(+) = JTSESR.OUTER_PACK_TYPE_MST_FK");
            sb.Append("   ORDER BY TO_CHAR(MBL.MBL_DATE, DATEFORMAT) DESC ");
            sb.Append("");

            DataSet ds = new DataSet();
            ds = objWF.GetDataSet(sb.ToString());

            try
            {
                foreach (DataRow _row in ds.Tables[0].Rows)
                {
                    foreach (DataColumn col in ds.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return ds;
        }

        public DataSet GetExportFreightManifestSeaQuery(string Pks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT ");
            //sb.Append("               -------LOCATION---")
            sb.Append("                LMT.LOCATION_NAME,");
            sb.Append("                LMT.ADDRESS_LINE1,");
            sb.Append("                LMT.ADDRESS_LINE2,");
            sb.Append("                LMT.ADDRESS_LINE3,");
            sb.Append("                LMT.CITY,");
            sb.Append("                LMT.ZIP POST_CODE,");
            sb.Append("                COUNTRY.COUNTRY_NAME,");
            sb.Append("                LMT.TELE_PHONE_NO,");
            sb.Append("                LMT.FAX_NO,");
            sb.Append("                LMT.E_MAIL_ID,");
            //sb.Append("                ------LOCATION----")
            sb.Append("                'CM' MANIFEST_TYPE,");
            sb.Append("                OMT.OPERATOR_ID CARRIER,");
            sb.Append("                VVT.VESSEL_NAME,");
            sb.Append("                VOY.VOYAGE,");
            //--Nationality of ship
            //--POR
            sb.Append("                POLMST.PORT_ID POL,");
            sb.Append("                PODMST.PORT_ID POD,");
            sb.Append("                PFD.PORT_NAME PFD,");
            //--Sail Dt
            sb.Append("                MBL.MBL_REF_NO,");
            sb.Append("                TO_CHAR(MBL.MBL_DATE, DATEFORMAT) MBL_DATE,");
            sb.Append("                HBL.HBL_REF_NO,");
            sb.Append("                TO_CHAR(HBL.HBL_DATE, DATEFORMAT) HBL_DATE,");
            sb.Append("                DECODE(MBL.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE,");
            //sb.Append("                -----SHIPPER CONSIGNEE---")
            sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPER_NAME,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_1 SHIPPER_ADDRESS1,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_2 SHIPPER_ADDRESS2,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_3 SHIPPER_ADDRESS3,");
            sb.Append("                SHIPPERDTLS.ADM_CITY SHIPPER_CITY,");
            sb.Append("                SHIPPERDTLS.ADM_ZIP_CODE SHIPPER_POST_CODE,");
            sb.Append("                SHIPPERCNT.COUNTRY_NAME SHIPPER_COUNTRY,");
            sb.Append("                SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPER_PHONE,");
            sb.Append("                SHIPPERDTLS.ADM_FAX_NO SHIPPER_FAX,");
            sb.Append("                SHIPPERDTLS.ADM_EMAIL_ID SHIPPER_EMAIL,");
            sb.Append("                CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEE_ADDRESS1,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEE_ADDRESS2,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEE_ADDRESS3,");
            sb.Append("                CONSIGNEEDTLS.ADM_CITY CONSIGNEE_CITY,");
            sb.Append("                CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEE_POST_CODE,");
            sb.Append("                CONSIGNEECNT.COUNTRY_NAME CONSIGNEE_COUNTRY,");
            sb.Append("                CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEE_PHONE,");
            sb.Append("                CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEE_FAX,");
            sb.Append("                CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEE_EMAIL,");
            //sb.Append("                ----")
            sb.Append("                JSE.MARKS_NUMBERS MARKS_NUMBER,");
            sb.Append("                JTSEC.PACK_COUNT NO_OF_PACKAGES,");
            sb.Append("                PY.PACK_TYPE_DESC PACK_TYPE,");
            sb.Append("                JTSEC.CONTAINER_NUMBER,");
            sb.Append("                JTSEC.SEAL_NUMBER,");
            sb.Append("                JSE.GOODS_DESCRIPTION GOODS_DESC,");
            sb.Append("                JTSEC.GROSS_WEIGHT WEIGHT,");
            sb.Append("                JTSEC.VOLUME_IN_CBM VOLUME,");
            //--Clause
            sb.Append("                ");
            //sb.Append("                ---SPECIAL REQU---")
            sb.Append("                IPACK.PACK_TYPE_ID HAZ_OUTERPACK_TYPE,");
            sb.Append("                OPACK.PACK_TYPE_ID HAZ_INNERPACK_TYPE,");
            sb.Append("                JTSESR.IMDG_CLASS_CODE HAZ_IMDG_CLASS,");
            sb.Append("                JTSESR.UN_NO HAZ_UN_NR,");
            sb.Append("                JTSESR.FLASH_PNT_TEMP FLASH_POINT,");
            sb.Append("                JTSESR.MIN_TEMP,");
            sb.Append("                DECODE(JTSESR.MIN_TEMP_UOM,0,'C',1,'F') MINCF,");
            sb.Append("                JTSESR.MAX_TEMP,");
            sb.Append("                DECODE(JTSESR.MAX_TEMP_UOM,0,'C',1,'F') MAXCF,");
            sb.Append("                JTSESR.EMS_NUMBER EMS_MFAG_NR,");
            sb.Append("                JTSESR.HUMIDITY_FACTOR,");
            sb.Append("                DECODE(JTSESR.IS_PERSHIABLE_GOODS,1,'YES',0,'NO') ISPERISHABLE,");
            sb.Append("                FEMT.FREIGHT_ELEMENT_ID FREIGHT_TERMS,");
            sb.Append("                FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT,");
            sb.Append("                DECODE(JTSEF.FREIGHT_TYPE,1,'Prepaid',2,'Collect') PAYMENT_TYPE,");
            sb.Append("                JTSEF.FREIGHT_AMT,");
            sb.Append("                CURR.CURRENCY_ID CURRENCY,");
            sb.Append("                ");
            sb.Append("                JTSEC.PACK_COUNT MANIFEST_PKG,");
            sb.Append("                JTSEC.GROSS_WEIGHT MANIFEST_WT,");
            sb.Append("                JTSEC.VOLUME_IN_CBM MANIFEST_CBM,");
            sb.Append("                ");
            sb.Append("                JTSEC.PACK_COUNT LOADED_PKG,");
            sb.Append("                JTSEC.GROSS_WEIGHT LOADED_WT,");
            sb.Append("                JTSEC.VOLUME_IN_CBM LOADED_CBM");
            sb.Append("                ");
            sb.Append("                ");
            sb.Append(" FROM MBL_EXP_TBL            MBL,");
            sb.Append("       PORT_MST_TBL           POLMST,");
            sb.Append("       PORT_MST_TBL           PODMST,");
            sb.Append("       HBL_EXP_TBL            HBL,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL   JSE,");
            sb.Append("       BOOKING_SEA_TBL        BST,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMST,");
            sb.Append("       JOB_TRN_SEA_EXP_CONT   JTSEC,");
            sb.Append("       JOB_TRN_SEA_EXP_SPL_REQ JTSESR,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTMST,");
            sb.Append("       CUSTOMER_MST_TBL       SHIPPER,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  SHIPPERDTLS,");
            sb.Append("       COUNTRY_MST_TBL        SHIPPERCNT,");
            sb.Append("       CUSTOMER_MST_TBL       CONSIGNEE,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  CONSIGNEEDTLS,");
            sb.Append("       COUNTRY_MST_TBL        CONSIGNEECNT,");
            sb.Append("       PORT_MST_TBL           PFD,");
            sb.Append("       PACK_TYPE_MST_TBL      PY,");
            sb.Append("       CARGO_MOVE_MST_TBL     CMMT,");
            sb.Append("       ");
            //sb.Append("       ----")
            sb.Append("       USER_MST_TBL           UMT,");
            sb.Append("       LOCATION_MST_TBL       LMT,");
            sb.Append("       COUNTRY_MST_TBL        COUNTRY,");
            //sb.Append("       ----")
            sb.Append("       AGENT_MST_TBL          AMT,");
            sb.Append("       AGENT_CONTACT_DTLS     ACD,");
            sb.Append("       COUNTRY_MST_TBL        ACMT,");
            //sb.Append("       -----")
            sb.Append("       OPERATOR_MST_TBL       OMT,");
            sb.Append("       VESSEL_VOYAGE_TBL      VVT,");
            sb.Append("       VESSEL_VOYAGE_TRN      VOY,");
            //sb.Append("       ---sr--")
            sb.Append("       PACK_TYPE_MST_TBL     IPACK,");
            sb.Append("       PACK_TYPE_MST_TBL     OPACK,");
            //sb.Append("       --------")
            sb.Append("       JOB_TRN_SEA_EXP_FD    JTSEF,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL    CURR");
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MBL.MBL_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("   AND JSE.JOB_CARD_SEA_EXP_PK = JTSEC.JOB_CARD_SEA_EXP_FK(+)");
            sb.Append("   AND JSE.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)");
            sb.Append("   AND JSE.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK(+)");
            sb.Append("   AND BST.PFD_FK = PFD.PORT_MST_PK(+)");
            sb.Append("   AND JTSEC.PACK_TYPE_MST_FK = PY.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPING_TERMS_MST_FK = STMST.SHIPPING_TERMS_MST_PK(+)");
            sb.Append("   AND JTSEC.CONTAINER_TYPE_MST_FK = CTMST.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)");
            sb.Append("   AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)");
            //sb.Append("   ------")
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MBL.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
            sb.Append("   AND LMT.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK");
            sb.Append("   AND MBL.AGENT_NAME = AMT.AGENT_NAME(+)");
            sb.Append("   AND AMT.AGENT_MST_PK = ACD.AGENT_MST_FK(+)");
            sb.Append("   AND ACMT.COUNTRY_MST_PK(+) = ACD.ADM_COUNTRY_MST_FK");
            sb.Append("   AND OMT.OPERATOR_MST_PK = MBL.OPERATOR_MST_FK");
            sb.Append("   AND VOY.VOYAGE_TRN_PK(+)  = MBL.VOYAGE_TRN_FK");
            sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VOY.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = MBL.CARGO_MOVE_FK");
            //sb.Append("   ---")
            sb.Append("   AND JTSEC.JOB_TRN_SEA_EXP_CONT_PK = JTSESR.JOB_TRN_SEA_EXP_CONT_FK(+)");
            sb.Append("   AND IPACK.PACK_TYPE_MST_PK(+)  = JTSESR.INNER_PACK_TYPE_MST_FK ");
            sb.Append("   AND OPACK.PACK_TYPE_MST_PK(+) = JTSESR.OUTER_PACK_TYPE_MST_FK");
            //sb.Append("   ---------")
            sb.Append("   AND JSE.JOB_CARD_SEA_EXP_PK  = JTSEF.JOB_CARD_SEA_EXP_FK(+)");
            sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK(+) = JTSEF.FREIGHT_ELEMENT_MST_FK");
            sb.Append("   AND CURR.CURRENCY_MST_PK = JTSEF.CURRENCY_MST_FK");
            sb.Append("   ORDER BY TO_CHAR(MBL.MBL_DATE, DATEFORMAT) DESC ");
            sb.Append("");

            DataSet ds = new DataSet();
            ds = objWF.GetDataSet(sb.ToString());

            try
            {
                foreach (DataRow _row in ds.Tables[0].Rows)
                {
                    foreach (DataColumn col in ds.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return ds;
        }

        public DataSet GetExportSeaCargoManifestHeader()
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT ");
            //sb.Append("               -------LOCATION---")
            sb.Append("                LMT.LOCATION_NAME,");
            sb.Append("                LMT.ADDRESS_LINE1,");
            sb.Append("                LMT.ADDRESS_LINE2,");
            sb.Append("                LMT.ADDRESS_LINE3,");
            sb.Append("                LMT.CITY,");
            sb.Append("                LMT.ZIP POST_CODE,");
            sb.Append("                COUNTRY.COUNTRY_NAME,");
            sb.Append("                LMT.TELE_PHONE_NO,");
            sb.Append("                LMT.FAX_NO,");
            sb.Append("                LMT.E_MAIL_ID ");
            sb.Append(" FROM MBL_EXP_TBL            MBL,");
            sb.Append("       USER_MST_TBL           UMT,");
            sb.Append("       LOCATION_MST_TBL       LMT,");
            sb.Append("       COUNTRY_MST_TBL        COUNTRY ");
            sb.Append("       ");
            //sb.Append(" WHERE TO_CHAR(MBL.MBL_EXP_TBL_PK) IN (" & Pks & ")")
            sb.Append("   WHERE UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MBL.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
            sb.Append("   AND LMT.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "HEADER");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportSeaCargoManifestMBLDetail(string Pks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT MBL.MBL_EXP_TBL_PK ");
            sb.Append(" FROM MBL_EXP_TBL  MBL ");
            sb.Append(" WHERE TO_CHAR(MBL.MBL_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "MBL_DETAIL");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportSeaCargoManifestMBLHeader(string Pks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT MBL.MBL_EXP_TBL_PK,'SEA' BIZ_TYPE,");
            sb.Append("                'CM' MANIFEST_TYPE,");
            //sb.Append("                ----AGENT--------")
            sb.Append("                AMT.AGENT_NAME POD_AGENT_NAME,");
            sb.Append("                ACD.ADM_ADDRESS_1 POD_AGENT_ADDRESS1,");
            sb.Append("                ACD.ADM_ADDRESS_2 POD_AGENT_ADDRESS2,");
            sb.Append("                ACD.ADM_ADDRESS_3 POD_AGENT_ADDRESS3,");
            sb.Append("                ACD.COR_CITY POD_CITY,");
            sb.Append("                ACD.ADM_ZIP_CODE POD_POST_CODE,");
            sb.Append("                ACMT.COUNTRY_NAME POD_COUNTRY_NAME,");
            //sb.Append("                -----AGENT------")
            sb.Append("                OMT.OPERATOR_ID CARRIER,");
            sb.Append("                VVT.VESSEL_NAME,");
            sb.Append("                VOY.VOYAGE,");
            sb.Append("                MBL.MBL_REF_NO,");
            sb.Append("                JSE.JOBCARD_REF_NO,");
            sb.Append("                DECODE(MBL.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGO_TYPE,");
            sb.Append("                POLMST.PORT_ID POL,");
            sb.Append("                PODMST.PORT_ID POD,");
            sb.Append("                PFD.PORT_NAME PFD,");
            sb.Append("                CMMT.CARGO_MOVE_CODE MOVE_CODE,");
            sb.Append("                TO_CHAR(MBL.MBL_DATE, DATEFORMAT) MBL_DATE,");
            sb.Append("                DECODE(MBL.PYMT_TYPE,1,'Prepaid',2,'Collect') PAYMENT_TYPE,");
            sb.Append("                HBL.HBL_REF_NO ");
            sb.Append(" FROM MBL_EXP_TBL            MBL,");
            sb.Append("       PORT_MST_TBL           POLMST,");
            sb.Append("       PORT_MST_TBL           PODMST,");
            sb.Append("       HBL_EXP_TBL            HBL,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL   JSE,");
            sb.Append("       BOOKING_SEA_TBL        BST,");
            sb.Append("       PORT_MST_TBL           PFD,");
            sb.Append("       CARGO_MOVE_MST_TBL     CMMT,");
            sb.Append("       ");
            //sb.Append("       ----")
            sb.Append("       USER_MST_TBL           UMT,");
            //sb.Append("       ----")
            sb.Append("       AGENT_MST_TBL          AMT,");
            sb.Append("       AGENT_CONTACT_DTLS     ACD,");
            sb.Append("       COUNTRY_MST_TBL        ACMT,");
            //sb.Append("       -----")
            sb.Append("       OPERATOR_MST_TBL       OMT,");
            sb.Append("       VESSEL_VOYAGE_TBL      VVT,");
            sb.Append("       VESSEL_VOYAGE_TRN      VOY ");
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MBL.MBL_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("   AND JSE.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)");
            sb.Append("   AND JSE.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK(+)");
            sb.Append("   AND BST.PFD_FK = PFD.PORT_MST_PK(+)");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MBL.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND MBL.AGENT_NAME = AMT.AGENT_NAME(+)");
            sb.Append("   AND AMT.AGENT_MST_PK = ACD.AGENT_MST_FK(+)");
            sb.Append("   AND ACMT.COUNTRY_MST_PK(+) = ACD.ADM_COUNTRY_MST_FK");
            sb.Append("   AND OMT.OPERATOR_MST_PK = MBL.OPERATOR_MST_FK");
            sb.Append("   AND VOY.VOYAGE_TRN_PK(+)  = MBL.VOYAGE_TRN_FK");
            sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VOY.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = MBL.CARGO_MOVE_FK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "MBL_HEADER");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportSeaCargoManifestHBLDetail(string Pks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT MBL.MBL_EXP_TBL_PK,HBL.HBL_EXP_TBL_PK,HBL.HBL_REF_NO");
            sb.Append(" FROM MBL_EXP_TBL  MBL,");
            sb.Append("       HBL_EXP_TBL            HBL,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL   JSE,");
            sb.Append("       BOOKING_SEA_TBL        BST ");
            sb.Append(" WHERE TO_CHAR(MBL.MBL_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("   AND JSE.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK ");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "HBL_DETAIL");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportSeaCargoManifestShipConsDetail(string Pks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT MBL.MBL_EXP_TBL_PK,HBL.HBL_EXP_TBL_PK,");
            sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPER_NAME,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_1 SHIPPER_ADDRESS1,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_2 SHIPPER_ADDRESS2,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_3 SHIPPER_ADDRESS3,");
            sb.Append("                SHIPPERDTLS.ADM_CITY SHIPPER_CITY,");
            sb.Append("                SHIPPERDTLS.ADM_ZIP_CODE SHIPPER_POST_CODE,");
            sb.Append("                SHIPPERCNT.COUNTRY_NAME SHIPPER_COUNTRY,");
            sb.Append("                SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPER_PHONE,");
            sb.Append("                SHIPPERDTLS.ADM_FAX_NO SHIPPER_FAX,");
            //sb.Append("                SHIPPERDTLS.ADM_EMAIL_ID SHIPPER_EMAIL,")
            sb.Append("                CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEE_ADDRESS1,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEE_ADDRESS2,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEE_ADDRESS3,");
            sb.Append("                CONSIGNEEDTLS.ADM_CITY CONSIGNEE_CITY,");
            sb.Append("                CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEE_POST_CODE,");
            sb.Append("                CONSIGNEECNT.COUNTRY_NAME CONSIGNEE_COUNTRY,");
            sb.Append("                CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEE_PHONE,");
            sb.Append("                CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEE_FAX ");
            //sb.Append("                CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEE_EMAIL,")
            sb.Append(" FROM MBL_EXP_TBL            MBL,");
            sb.Append("       HBL_EXP_TBL            HBL,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL   JSE,");
            sb.Append("       BOOKING_SEA_TBL        BST,");
            sb.Append("       CUSTOMER_MST_TBL       SHIPPER,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  SHIPPERDTLS,");
            sb.Append("       COUNTRY_MST_TBL        SHIPPERCNT,");
            sb.Append("       CUSTOMER_MST_TBL       CONSIGNEE,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  CONSIGNEEDTLS,");
            sb.Append("       COUNTRY_MST_TBL        CONSIGNEECNT,");
            sb.Append("       ");
            //sb.Append("       ----")
            sb.Append("       USER_MST_TBL           UMT ");
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MBL.MBL_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("   AND JSE.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK ");
            sb.Append("   AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)");
            sb.Append("   AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)");
            //sb.Append("   ------")
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MBL.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "SHIPPER_CONSIGNEE_DETAIL");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportSeaCargoManifestContCargoDetail(string Pks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT MBL.MBL_EXP_TBL_PK,HBL.HBL_EXP_TBL_PK,");
            sb.Append("                JTSEC.CONTAINER_NUMBER,");
            sb.Append("                JTSEC.SEAL_NUMBER,");
            sb.Append("                JSE.MARKS_NUMBERS MARKS_NUMBER,");
            sb.Append("                PY.PACK_TYPE_DESC PACK_TYPE,");
            sb.Append("                JTSEC.PACK_COUNT COUNT,");
            sb.Append("                JTSEC.GROSS_WEIGHT WEIGHT,");
            sb.Append("                JTSEC.VOLUME_IN_CBM VOLUME,");
            sb.Append("                JSE.GOODS_DESCRIPTION GOODS_DESC,");
            sb.Append("                ");
            //sb.Append("                ---SPECIAL REQU---")
            sb.Append("                IPACK.PACK_TYPE_ID HAZ_OUTERPACK_TYPE,");
            sb.Append("                OPACK.PACK_TYPE_ID HAZ_INNERPACK_TYPE,");
            sb.Append("                JTSESR.IMDG_CLASS_CODE HAZ_IMDG_CLASS,");
            sb.Append("                JTSESR.UN_NO HAZ_UN_NR,");
            sb.Append("                JTSESR.FLASH_PNT_TEMP FLASH_POINT,");
            sb.Append("                JTSESR.MIN_TEMP,");
            sb.Append("                DECODE(JTSESR.MIN_TEMP_UOM,0,'C',1,'F') MINCF,");
            sb.Append("                JTSESR.MAX_TEMP,");
            sb.Append("                DECODE(JTSESR.MAX_TEMP_UOM,0,'C',1,'F') MAXCF,");
            sb.Append("                JTSESR.EMS_NUMBER EMS_MFAG_NR,");
            sb.Append("                JTSESR.HUMIDITY_FACTOR,");
            sb.Append("                DECODE(JTSESR.IS_PERSHIABLE_GOODS,1,'YES',0,'NO') ISPERISHABLE");
            sb.Append("                ");
            sb.Append("                ");
            sb.Append(" FROM MBL_EXP_TBL            MBL,");
            sb.Append("       HBL_EXP_TBL            HBL,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL   JSE,");
            sb.Append("       BOOKING_SEA_TBL        BST,");
            sb.Append("       JOB_TRN_SEA_EXP_CONT   JTSEC,");
            sb.Append("       JOB_TRN_SEA_EXP_SPL_REQ JTSESR,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTMST,");
            sb.Append("       PACK_TYPE_MST_TBL      PY,");
            sb.Append("       USER_MST_TBL           UMT,");
            //sb.Append("       ---sr--")
            sb.Append("       PACK_TYPE_MST_TBL     IPACK,");
            sb.Append("       PACK_TYPE_MST_TBL     OPACK");
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MBL.MBL_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("   AND JSE.JOB_CARD_SEA_EXP_PK = JTSEC.JOB_CARD_SEA_EXP_FK(+)");
            sb.Append("   AND JSE.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK ");
            sb.Append("   AND JTSEC.PACK_TYPE_MST_FK = PY.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND JTSEC.CONTAINER_TYPE_MST_FK = CTMST.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MBL.CREATED_BY_FK = UMT.USER_MST_PK");
            //sb.Append("   ---")
            sb.Append("   AND JTSEC.JOB_TRN_SEA_EXP_CONT_PK = JTSESR.JOB_TRN_SEA_EXP_CONT_FK(+)");
            sb.Append("   AND IPACK.PACK_TYPE_MST_PK(+)  = JTSESR.INNER_PACK_TYPE_MST_FK ");
            sb.Append("   AND OPACK.PACK_TYPE_MST_PK(+) = JTSESR.OUTER_PACK_TYPE_MST_FK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "CONTAINER_CARGO_DETAIL");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportSeaCargoManifestFreightDetail(string Pks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT MBL.MBL_EXP_TBL_PK,HBL.HBL_EXP_TBL_PK,");
            sb.Append("                FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("                FEMT.FREIGHT_ELEMENT_NAME,");
            sb.Append("                DECODE(JTSEF.FREIGHT_TYPE,1,'Prepaid',2,'Collect') PAYMENT_TYPE,");
            sb.Append("                JTSEF.FREIGHT_AMT,");
            sb.Append("                CURR.CURRENCY_ID CURRENCY ");
            sb.Append("                ");
            sb.Append("                ");
            sb.Append(" FROM MBL_EXP_TBL            MBL,");
            sb.Append("       HBL_EXP_TBL            HBL,");
            sb.Append("       JOB_CARD_SEA_EXP_TBL   JSE,");
            sb.Append("       BOOKING_SEA_TBL        BST,");
            sb.Append("       JOB_TRN_SEA_EXP_FD    JTSEF,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL    CURR ");
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MBL.MBL_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
            sb.Append("   AND JSE.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK ");
            sb.Append("   AND JSE.JOB_CARD_SEA_EXP_PK  = JTSEF.JOB_CARD_SEA_EXP_FK(+)");
            sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK = JTSEF.FREIGHT_ELEMENT_MST_FK");
            sb.Append("   AND CURR.CURRENCY_MST_PK = JTSEF.CURRENCY_MST_FK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "FREIGHT_DETAIL");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        #endregion " CARGO/FREIGHT MANIFEST SEA "

        #region " CARGO/FREIGHT MANIFEST AIR "

        public DataSet GetExportCargoManifestAirQuery(string Pks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT 'AIR' BIZ_TYPE,");
            //sb.Append("               -------LOCATION---")
            sb.Append("                LMT.LOCATION_NAME,");
            sb.Append("                LMT.ADDRESS_LINE1,");
            sb.Append("                LMT.ADDRESS_LINE2,");
            sb.Append("                LMT.ADDRESS_LINE3,");
            sb.Append("                LMT.CITY,");
            sb.Append("                LMT.ZIP,");
            sb.Append("                COUNTRY.COUNTRY_NAME,");
            sb.Append("                LMT.TELE_PHONE_NO,");
            sb.Append("                LMT.FAX_NO,");
            sb.Append("                LMT.E_MAIL_ID,");
            //sb.Append("                ------LOCATION----")
            sb.Append("                'CM' MANIFEST_TYPE,");
            //sb.Append("                ----AGENT--------")
            sb.Append("                AMT.AGENT_NAME POD_AGENT_NAME,");
            sb.Append("                ACD.ADM_ADDRESS_1 POD_AGENT_ADDRESS1,");
            sb.Append("                ACD.ADM_ADDRESS_2 POD_AGENT_ADDRESS2,");
            sb.Append("                ACD.ADM_ADDRESS_3 POD_AGENT_ADDRESS3,");
            sb.Append("                ACD.COR_CITY POD_CITY,");
            sb.Append("                ACD.ADM_ZIP_CODE POD_POST_CODE,");
            //sb.Append("                ACD.ADM_ZIP_CODE,")
            sb.Append("                ACMT.COUNTRY_NAME POD_COUNTRY_NAME,");
            //sb.Append("                -----AGENT------")
            sb.Append("                AIRMT.AIRLINE_ID AIRLINE,");
            sb.Append("                AIRMT.AIRLINE_NAME,");
            sb.Append("                JSE.FLIGHT_NO,");
            sb.Append("                MAWB.MAWB_REF_NO,");
            sb.Append("                JSE.JOBCARD_REF_NO,");
            //sb.Append("                '' CARGO_TYPE,")
            sb.Append("                POLMST.PORT_ID AOO,");
            sb.Append("                PODMST.PORT_ID AOD,");
            sb.Append("                CMMT.CARGO_MOVE_CODE MOVE_CODE,");
            //sb.Append("                STMST.INCO_CODE TERMS,")
            sb.Append("                TO_CHAR(MAWB.MAWB_DATE, DATEFORMAT) MAWB_DATE,");
            sb.Append("                DECODE(MAWB.PYMT_TYPE,1,'Prepaid',2,'Collect') PAYMENT_TYPE,");
            sb.Append("                HAWB.HAWB_REF_NO,");
            //sb.Append("                -----SHIPPER CONSIGNEE---")
            sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPER_NAME,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_1 SHIPPER_ADDRESS1,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_2 SHIPPER_ADDRESS2,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_3 SHIPPER_ADDRESS3,");
            sb.Append("                SHIPPERDTLS.ADM_CITY SHIPPER_CITY,");
            sb.Append("                SHIPPERDTLS.ADM_ZIP_CODE SHIPPER_POST_CODE,");
            sb.Append("                SHIPPERCNT.COUNTRY_NAME SHIPPER_COUNTRY,");
            sb.Append("                SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPER_PHONE,");
            sb.Append("                SHIPPERDTLS.ADM_FAX_NO SHIPPER_FAX,");
            //sb.Append("                SHIPPERDTLS.ADM_EMAIL_ID SHIPPER_EMAIL,")
            sb.Append("                CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEE_ADDRESS1,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEE_ADDRESS2,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEE_ADDRESS3,");
            sb.Append("                CONSIGNEEDTLS.ADM_CITY CONSIGNEE_CITY,");
            sb.Append("                CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEE_POST_CODE,");
            sb.Append("                CONSIGNEECNT.COUNTRY_NAME CONSIGNEE_COUNTRY,");
            sb.Append("                CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEE_PHONE,");
            sb.Append("                CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEE_FAX,");
            //sb.Append("                CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEE_EMAIL,")
            //sb.Append("                ----")
            sb.Append("                DELPMST.PLACE_NAME PFD,");
            //sb.Append("                '' SEAL_NUMBER,")
            //sb.Append("                '' CONTAINER_NUMBER,")
            sb.Append("                JSE.MARKS_NUMBERS MARKS_NUMBER,");
            sb.Append("                PY.PACK_TYPE_DESC PACK_TYPE,");
            sb.Append("                JTSEC.PACK_COUNT COUNT,");
            sb.Append("                JTSEC.GROSS_WEIGHT WEIGHT,");
            sb.Append("                JTSEC.VOLUME_IN_CBM VOLUME,");
            sb.Append("                JSE.GOODS_DESCRIPTION GOODS_DESC,");
            sb.Append("                ");
            //sb.Append("                ---SPECIAL REQU---")
            sb.Append("                JTAHSR.HAZ_OUTERPACK_TYPE,");
            sb.Append("                JTAHSR.HAZ_INNERPACK_TYPE,");
            sb.Append("                JTAHSR.IMDG_CLASS_CODE HAZ_IMDG_CLASS,");
            sb.Append("                JTAHSR.UN_NO HAZ_UN_NR,");
            sb.Append("                JTAHSR.FLASH_PNT_TEMP FLASH_POINT,");
            sb.Append("                JTARSR.MIN_TEMP,");
            sb.Append("                DECODE(JTARSR.MIN_TEMP_UOM, 0, 'C', 1, 'F') MINCF,");
            sb.Append("                JTARSR.MAX_TEMP,");
            sb.Append("                DECODE(JTARSR.MAX_TEMP_UOM, 0, 'C', 1, 'F') MAXCF,");
            sb.Append("                JTAHSR.EMS_NUMBER EMS_MFAG_NR,");
            sb.Append("                JTARSR.HUMIDITY_FACTOR,");
            sb.Append("                DECODE(JTARSR.IS_PERISHABLE_GOODS, 1, 'YES', 0, 'NO') ISPERISHABLE");
            sb.Append("                ");
            sb.Append("                ");
            sb.Append(" FROM  MAWB_EXP_TBL            MAWB,");
            sb.Append("       PORT_MST_TBL           POLMST,");
            sb.Append("       PORT_MST_TBL           PODMST,");
            sb.Append("       HAWB_EXP_TBL            HAWB,");
            sb.Append("       JOB_CARD_AIR_EXP_TBL   JSE,");
            sb.Append("       BOOKING_AIR_TBL        BST,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMST,");
            sb.Append("       JOB_TRN_AIR_EXP_CONT   JTSEC,");
            sb.Append("       (SELECT JTAHSR.BOOKING_AIR_FK,");
            sb.Append("               OPACK.PACK_TYPE_ID HAZ_OUTERPACK_TYPE,");
            sb.Append("               IPACK.PACK_TYPE_ID HAZ_INNERPACK_TYPE,");
            sb.Append("               JTAHSR.IMDG_CLASS_CODE,");
            sb.Append("               JTAHSR.UN_NO,");
            sb.Append("               JTAHSR.FLASH_PNT_TEMP,");
            sb.Append("               JTAHSR.EMS_NUMBER ");
            sb.Append("          FROM BKG_TRN_AIR_HAZ_SPL_REQ JTAHSR,");
            sb.Append("               BOOKING_AIR_TBL BAT,");
            sb.Append("               COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("               PACK_TYPE_MST_TBL     IPACK,");
            sb.Append("               PACK_TYPE_MST_TBL     OPACK");
            sb.Append("         WHERE CGMT.COMMODITY_GROUP_CODE = 'HAZARDOUS' ");
            sb.Append("           AND CGMT.COMMODITY_GROUP_PK = BAT.COMMODITY_GROUP_FK");
            sb.Append("           AND BAT.BOOKING_AIR_PK = JTAHSR.BOOKING_AIR_FK");
            sb.Append("           AND IPACK.PACK_TYPE_MST_PK(+) = JTAHSR.INNER_PACK_TYPE_MST_FK");
            sb.Append("           AND OPACK.PACK_TYPE_MST_PK(+) = JTAHSR.OUTER_PACK_TYPE_MST_FK) JTAHSR,");
            sb.Append("       (SELECT JTARSR.BOOKING_AIR_FK,");
            sb.Append("               JTARSR.MIN_TEMP,");
            sb.Append("               JTARSR.MAX_TEMP,");
            sb.Append("               JTARSR.MIN_TEMP_UOM,");
            sb.Append("               JTARSR.MAX_TEMP_UOM,");
            sb.Append("               JTARSR.HUMIDITY_FACTOR,");
            sb.Append("               JTARSR.IS_PERISHABLE_GOODS ");
            sb.Append("          FROM BKG_TRN_AIR_REF_SPL_REQ JTARSR,");
            sb.Append("               BOOKING_AIR_TBL BAT,");
            sb.Append("               COMMODITY_GROUP_MST_TBL CGMT ");
            sb.Append("         WHERE CGMT.COMMODITY_GROUP_CODE = 'REEFER' ");
            sb.Append("           AND CGMT.COMMODITY_GROUP_PK = BAT.COMMODITY_GROUP_FK");
            sb.Append("           AND BAT.BOOKING_AIR_PK = JTARSR.BOOKING_AIR_FK) JTARSR,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTMST,");
            sb.Append("       CUSTOMER_MST_TBL       SHIPPER,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  SHIPPERDTLS,");
            sb.Append("       COUNTRY_MST_TBL        SHIPPERCNT,");
            sb.Append("       CUSTOMER_MST_TBL       CONSIGNEE,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  CONSIGNEEDTLS,");
            sb.Append("       COUNTRY_MST_TBL        CONSIGNEECNT,");
            sb.Append("       PORT_MST_TBL           PFD,");
            sb.Append("       PACK_TYPE_MST_TBL      PY,");
            sb.Append("       CARGO_MOVE_MST_TBL     CMMT,");
            sb.Append("       PLACE_MST_TBL          DELPMST,");
            //sb.Append("       ----")
            sb.Append("       USER_MST_TBL           UMT,");
            sb.Append("       LOCATION_MST_TBL       LMT,");
            sb.Append("       COUNTRY_MST_TBL        COUNTRY,");
            //sb.Append("       ----")
            sb.Append("       AGENT_MST_TBL          AMT,");
            sb.Append("       AGENT_CONTACT_DTLS     ACD,");
            sb.Append("       COUNTRY_MST_TBL        ACMT,");
            //sb.Append("       -----")
            sb.Append("       AIRLINE_MST_TBL       AIRMT, ");
            //sb.Append("        ---sr--")
            sb.Append("       PACK_TYPE_MST_TBL     IPACK,");
            sb.Append("       PACK_TYPE_MST_TBL     OPACK ");
            //sb.Append("       --------")
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MAWB.MAWB_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_AIR_FK = BST.BOOKING_AIR_PK");
            sb.Append("   AND JSE.JOB_CARD_AIR_EXP_PK = JTSEC.JOB_CARD_AIR_EXP_FK(+)");
            sb.Append("   AND BST.BOOKING_AIR_PK = JTAHSR.BOOKING_AIR_FK(+)");
            sb.Append("   AND BST.BOOKING_AIR_PK = JTARSR.BOOKING_AIR_FK(+)");
            sb.Append("   AND JSE.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK(+)");
            sb.Append("   AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)");
            sb.Append("   AND JSE.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+)");
            sb.Append("   AND JTSEC.PACK_TYPE_MST_FK = PY.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPING_TERMS_MST_FK = STMST.SHIPPING_TERMS_MST_PK(+)");
            sb.Append("   AND JTSEC.CONTAINER_TYPE_MST_FK = CTMST.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)");
            sb.Append("   AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)");
            //sb.Append("   ------")
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MAWB.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
            sb.Append("   AND LMT.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK");
            sb.Append("   AND MAWB.AGENT_NAME = AMT.AGENT_NAME(+)");
            sb.Append("   AND AMT.AGENT_MST_PK = ACD.AGENT_MST_FK(+)");
            sb.Append("   AND ACMT.COUNTRY_MST_PK(+) = ACD.ADM_COUNTRY_MST_FK");
            sb.Append("   AND AIRMT.AIRLINE_MST_PK = MAWB.AIRLINE_MST_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = MAWB.CARGO_MOVE_FK");
            //sb.Append("   ---")
            sb.Append("   AND BST.DEL_PLACE_MST_FK = DELPMST.PLACE_PK(+) ");
            //'sb.Append("   ---------")
            sb.Append("   ORDER BY TO_CHAR(MAWB.MAWB_DATE, DATEFORMAT) DESC ");
            sb.Append("");

            DataSet dsBkg = new DataSet();
            dsBkg = objWF.GetDataSet(sb.ToString());

            try
            {
                foreach (DataRow _row in dsBkg.Tables[0].Rows)
                {
                    foreach (DataColumn col in dsBkg.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return dsBkg;
        }

        public DataSet GetExportFreightManifestAirQuery(string Pks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT ");
            //sb.Append("               -------LOCATION---")
            sb.Append("                LMT.LOCATION_NAME,");
            sb.Append("                LMT.ADDRESS_LINE1,");
            sb.Append("                LMT.ADDRESS_LINE2,");
            sb.Append("                LMT.ADDRESS_LINE3,");
            sb.Append("                LMT.CITY,");
            sb.Append("                LMT.ZIP,");
            sb.Append("                COUNTRY.COUNTRY_NAME,");
            sb.Append("                LMT.TELE_PHONE_NO,");
            sb.Append("                LMT.FAX_NO,");
            sb.Append("                LMT.E_MAIL_ID,");
            //sb.Append("                ------LOCATION----")
            sb.Append("                'CM' MANIFEST_TYPE,");
            sb.Append("                AIRMT.AIRLINE_ID AIRLINE,");
            //sb.Append("                AIRMT.AIRLINE_NAME,")
            sb.Append("                JSE.FLIGHT_NO,");
            sb.Append("                POLMST.PORT_ID AOO,");
            sb.Append("                PODMST.PORT_ID AOD,");
            sb.Append("                TO_CHAR(MAWB.MAWB_DATE, DATEFORMAT) MAWB_DATE,");
            //--Consolidator
            //--DeConsolidator
            //--AWB Type
            //--AWBN
            sb.Append("                JTSEC.PACK_COUNT NO_OF_PIECES,");
            sb.Append("                JTSEC.GROSS_WEIGHT WEIGHT,");
            //--No oF HBL's
            //sb.Append("                -----SHIPPER CONSIGNEE---")
            sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPER_NAME,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_1 SHIPPER_ADDRESS1,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_2 SHIPPER_ADDRESS2,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_3 SHIPPER_ADDRESS3,");
            sb.Append("                SHIPPERDTLS.ADM_CITY SHIPPER_CITY,");
            sb.Append("                SHIPPERDTLS.ADM_ZIP_CODE SHIPPER_POST_CODE,");
            sb.Append("                SHIPPERCNT.COUNTRY_NAME SHIPPER_COUNTRY,");
            sb.Append("                SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPER_PHONE,");
            sb.Append("                SHIPPERDTLS.ADM_FAX_NO SHIPPER_FAX,");
            sb.Append("                SHIPPERDTLS.ADM_EMAIL_ID SHIPPER_EMAIL,");
            sb.Append("                CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEE_ADDRESS1,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEE_ADDRESS2,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEE_ADDRESS3,");
            sb.Append("                CONSIGNEEDTLS.ADM_CITY CONSIGNEE_CITY,");
            sb.Append("                CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEE_POST_CODE,");
            sb.Append("                CONSIGNEECNT.COUNTRY_NAME CONSIGNEE_COUNTRY,");
            sb.Append("                CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEE_PHONE,");
            sb.Append("                CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEE_FAX,");
            sb.Append("                CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEE_EMAIL,");
            //--Nature Of Goods
            //sb.Append("                ---SPECIAL REQU---")
            sb.Append("                JTAHSR.HAZ_OUTERPACK_TYPE,");
            sb.Append("                JTAHSR.HAZ_INNERPACK_TYPE,");
            sb.Append("                JTAHSR.IMDG_CLASS_CODE HAZ_IMDG_CLASS,");
            sb.Append("                JTAHSR.UN_NO HAZ_UN_NR,");
            sb.Append("                JTAHSR.FLASH_PNT_TEMP FLASH_POINT,");
            sb.Append("                JTARSR.MIN_TEMP,");
            sb.Append("                DECODE(JTARSR.MIN_TEMP_UOM, 0, 'C', 1, 'F') MINCF,");
            sb.Append("                JTARSR.MAX_TEMP,");
            sb.Append("                DECODE(JTARSR.MAX_TEMP_UOM, 0, 'C', 1, 'F') MAXCF,");
            sb.Append("                JTAHSR.EMS_NUMBER EMS_MFAG_NR,");
            sb.Append("                JTARSR.HUMIDITY_FACTOR,");
            sb.Append("                DECODE(JTARSR.IS_PERISHABLE_GOODS, 1, 'YES', 0, 'NO') ISPERISHABLE,");
            //sb.Append("                ---SPECIAL REQU---")
            sb.Append("                ");
            sb.Append("                FEMT.FREIGHT_ELEMENT_ID FREIGHT_TERMS,");
            sb.Append("                FEMT.FREIGHT_ELEMENT_NAME FREIGHT_ELEMENT,");
            sb.Append("                DECODE(JTSEF.FREIGHT_TYPE,1,'Prepaid',2,'Collect') PAYMENT_TYPE,");
            sb.Append("                JTSEF.FREIGHT_AMT,");
            sb.Append("                CURR.CURRENCY_ID CURRENCY");
            sb.Append("                ");
            sb.Append("                ");
            sb.Append("                ");
            sb.Append(" FROM  MAWB_EXP_TBL            MAWB,");
            sb.Append("       PORT_MST_TBL           POLMST,");
            sb.Append("       PORT_MST_TBL           PODMST,");
            sb.Append("       HAWB_EXP_TBL            HAWB,");
            sb.Append("       JOB_CARD_AIR_EXP_TBL   JSE,");
            sb.Append("       BOOKING_AIR_TBL        BST,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMST,");
            sb.Append("       JOB_TRN_AIR_EXP_CONT   JTSEC,");
            sb.Append("       (SELECT JTAHSR.BOOKING_AIR_FK,");
            sb.Append("               OPACK.PACK_TYPE_ID HAZ_OUTERPACK_TYPE,");
            sb.Append("               IPACK.PACK_TYPE_ID HAZ_INNERPACK_TYPE,");
            sb.Append("               JTAHSR.IMDG_CLASS_CODE,");
            sb.Append("               JTAHSR.UN_NO,");
            sb.Append("               JTAHSR.FLASH_PNT_TEMP,");
            sb.Append("               JTAHSR.EMS_NUMBER ");
            sb.Append("          FROM BKG_TRN_AIR_HAZ_SPL_REQ JTAHSR,");
            sb.Append("               BOOKING_AIR_TBL BAT,");
            sb.Append("               COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("               PACK_TYPE_MST_TBL     IPACK,");
            sb.Append("               PACK_TYPE_MST_TBL     OPACK");
            sb.Append("         WHERE CGMT.COMMODITY_GROUP_CODE = 'HAZARDOUS' ");
            sb.Append("           AND CGMT.COMMODITY_GROUP_PK = BAT.COMMODITY_GROUP_FK");
            sb.Append("           AND BAT.BOOKING_AIR_PK = JTAHSR.BOOKING_AIR_FK");
            sb.Append("           AND IPACK.PACK_TYPE_MST_PK(+) = JTAHSR.INNER_PACK_TYPE_MST_FK");
            sb.Append("           AND OPACK.PACK_TYPE_MST_PK(+) = JTAHSR.OUTER_PACK_TYPE_MST_FK) JTAHSR,");
            sb.Append("       (SELECT JTARSR.BOOKING_AIR_FK,");
            sb.Append("               JTARSR.MIN_TEMP,");
            sb.Append("               JTARSR.MAX_TEMP,");
            sb.Append("               JTARSR.MIN_TEMP_UOM,");
            sb.Append("               JTARSR.MAX_TEMP_UOM,");
            sb.Append("               JTARSR.HUMIDITY_FACTOR,");
            sb.Append("               JTARSR.IS_PERISHABLE_GOODS ");
            sb.Append("          FROM BKG_TRN_AIR_REF_SPL_REQ JTARSR,");
            sb.Append("               BOOKING_AIR_TBL BAT,");
            sb.Append("               COMMODITY_GROUP_MST_TBL CGMT ");
            sb.Append("         WHERE CGMT.COMMODITY_GROUP_CODE = 'REEFER' ");
            sb.Append("           AND CGMT.COMMODITY_GROUP_PK = BAT.COMMODITY_GROUP_FK");
            sb.Append("           AND BAT.BOOKING_AIR_PK = JTARSR.BOOKING_AIR_FK) JTARSR,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTMST,");
            sb.Append("       CUSTOMER_MST_TBL       SHIPPER,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  SHIPPERDTLS,");
            sb.Append("       COUNTRY_MST_TBL        SHIPPERCNT,");
            sb.Append("       CUSTOMER_MST_TBL       CONSIGNEE,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  CONSIGNEEDTLS,");
            sb.Append("       COUNTRY_MST_TBL        CONSIGNEECNT,");
            sb.Append("       PORT_MST_TBL           PFD,");
            sb.Append("       PACK_TYPE_MST_TBL      PY,");
            sb.Append("       CARGO_MOVE_MST_TBL     CMMT,");
            sb.Append("       PLACE_MST_TBL          DELPMST,");
            //sb.Append("       ----")
            sb.Append("       USER_MST_TBL           UMT,");
            sb.Append("       LOCATION_MST_TBL       LMT,");
            sb.Append("       COUNTRY_MST_TBL        COUNTRY,");
            //sb.Append("       ----")
            sb.Append("       AGENT_MST_TBL          AMT,");
            sb.Append("       AGENT_CONTACT_DTLS     ACD,");
            sb.Append("       COUNTRY_MST_TBL        ACMT,");
            //sb.Append("       -----")
            sb.Append("       AIRLINE_MST_TBL       AIRMT, ");
            //sb.Append("        ---sr--")
            sb.Append("       PACK_TYPE_MST_TBL     IPACK,");
            sb.Append("       PACK_TYPE_MST_TBL     OPACK,");
            //sb.Append("       --------")
            sb.Append("       JOB_TRN_AIR_EXP_FD    JTSEF,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL    CURR");
            //sb.Append("       --------")
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MAWB.MAWB_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_AIR_FK = BST.BOOKING_AIR_PK");
            sb.Append("   AND JSE.JOB_CARD_AIR_EXP_PK = JTSEC.JOB_CARD_AIR_EXP_FK(+)");
            sb.Append("   AND BST.BOOKING_AIR_PK = JTAHSR.BOOKING_AIR_FK(+)");
            sb.Append("   AND BST.BOOKING_AIR_PK = JTARSR.BOOKING_AIR_FK(+)");
            sb.Append("   AND JSE.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK(+)");
            sb.Append("   AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)");
            sb.Append("   AND JSE.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+)");
            sb.Append("   AND JTSEC.PACK_TYPE_MST_FK = PY.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPING_TERMS_MST_FK = STMST.SHIPPING_TERMS_MST_PK(+)");
            sb.Append("   AND JTSEC.CONTAINER_TYPE_MST_FK = CTMST.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)");
            sb.Append("   AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)");
            //sb.Append("   ------")
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MAWB.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
            sb.Append("   AND LMT.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK");
            sb.Append("   AND MAWB.AGENT_NAME = AMT.AGENT_NAME(+)");
            sb.Append("   AND AMT.AGENT_MST_PK = ACD.AGENT_MST_FK(+)");
            sb.Append("   AND ACMT.COUNTRY_MST_PK(+) = ACD.ADM_COUNTRY_MST_FK");
            sb.Append("   AND AIRMT.AIRLINE_MST_PK = MAWB.AIRLINE_MST_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = MAWB.CARGO_MOVE_FK");
            //sb.Append("   ---")
            sb.Append("   AND BST.DEL_PLACE_MST_FK = DELPMST.PLACE_PK(+) ");
            //sb.Append("   ---------")
            sb.Append("   AND JSE.JOB_CARD_AIR_EXP_PK  = JTSEF.JOB_CARD_AIR_EXP_FK(+)");
            sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK(+) = JTSEF.FREIGHT_ELEMENT_MST_FK");
            sb.Append("   AND CURR.CURRENCY_MST_PK = JTSEF.CURRENCY_MST_FK");
            //'sb.Append("   ---------")
            sb.Append("   ORDER BY TO_CHAR(MAWB.MAWB_DATE, DATEFORMAT) DESC ");
            sb.Append("");

            DataSet dsBkg = new DataSet();
            dsBkg = objWF.GetDataSet(sb.ToString());

            try
            {
                foreach (DataRow _row in dsBkg.Tables[0].Rows)
                {
                    foreach (DataColumn col in dsBkg.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return dsBkg;
        }

        public DataSet GetExportAirCargoManifestHeader()
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT ");
            //sb.Append("               -------LOCATION---")
            sb.Append("                LMT.LOCATION_NAME,");
            sb.Append("                LMT.ADDRESS_LINE1,");
            sb.Append("                LMT.ADDRESS_LINE2,");
            sb.Append("                LMT.ADDRESS_LINE3,");
            sb.Append("                LMT.CITY,");
            sb.Append("                LMT.ZIP,");
            sb.Append("                COUNTRY.COUNTRY_NAME,");
            sb.Append("                LMT.TELE_PHONE_NO,");
            sb.Append("                LMT.FAX_NO,");
            sb.Append("                LMT.E_MAIL_ID ");
            sb.Append(" FROM  MAWB_EXP_TBL           MAWB,");
            sb.Append("       USER_MST_TBL           UMT,");
            sb.Append("       LOCATION_MST_TBL       LMT,");
            sb.Append("       COUNTRY_MST_TBL        COUNTRY ");
            sb.Append("       ");
            //sb.Append(" WHERE TO_CHAR(MBL.MBL_EXP_TBL_PK) IN (" & Pks & ")")
            sb.Append("   WHERE UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MAWB.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
            sb.Append("   AND LMT.COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "HEADER");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportAirCargoManifestMBLDetail(string Pks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            ///'
            sb.Append("SELECT MAWB.MAWB_EXP_TBL_PK ");
            sb.Append(" FROM  MAWB_EXP_TBL MAWB ");
            sb.Append(" WHERE TO_CHAR(MAWB.MAWB_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("");
            ///'

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "MBL_DETAIL");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportAirCargoManifestMBLHeader(string Pks = "", string Currency = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT MAWB.MAWB_EXP_TBL_PK,'AIR' BIZ_TYPE,");
            sb.Append("                'CM' MANIFEST_TYPE,");
            //sb.Append("                ----AGENT--------")
            sb.Append("                AMT.AGENT_NAME POD_AGENT_NAME,");
            sb.Append("                ACD.ADM_ADDRESS_1 POD_AGENT_ADDRESS1,");
            sb.Append("                ACD.ADM_ADDRESS_2 POD_AGENT_ADDRESS2,");
            sb.Append("                ACD.ADM_ADDRESS_3 POD_AGENT_ADDRESS3,");
            sb.Append("                ACD.COR_CITY POD_CITY,");
            sb.Append("                ACD.ADM_ZIP_CODE POD_POST_CODE,");
            sb.Append("                ACMT.COUNTRY_NAME POD_COUNTRY_NAME,");
            //sb.Append("                -----AGENT------")
            sb.Append("                AIRMT.AIRLINE_ID AIRLINE,");
            sb.Append("                AIRMT.AIRLINE_NAME,");
            sb.Append("                JSE.FLIGHT_NO,");
            sb.Append("                MAWB.MAWB_REF_NO,");
            sb.Append("                JSE.JOBCARD_REF_NO,");
            sb.Append("                POLMST.PORT_ID AOO,");
            sb.Append("                PODMST.PORT_ID AOD,");
            sb.Append("                DELPMST.PLACE_NAME PFD,");
            sb.Append("                CMMT.CARGO_MOVE_CODE MOVE_CODE,");
            sb.Append("                TO_CHAR(MAWB.MAWB_DATE, DATEFORMAT) MAWB_DATE,");
            sb.Append("                DECODE(MAWB.PYMT_TYPE,1,'Prepaid',2,'Collect') PAYMENT_TYPE,");
            sb.Append("                HAWB.HAWB_REF_NO ");
            sb.Append(" FROM  MAWB_EXP_TBL            MAWB,");
            sb.Append("       PORT_MST_TBL           POLMST,");
            sb.Append("       PORT_MST_TBL           PODMST,");
            sb.Append("       HAWB_EXP_TBL            HAWB,");
            sb.Append("       JOB_CARD_AIR_EXP_TBL   JSE,");
            sb.Append("       BOOKING_AIR_TBL        BST,");
            sb.Append("       PORT_MST_TBL           PFD,");
            sb.Append("       CARGO_MOVE_MST_TBL     CMMT,");
            sb.Append("       PLACE_MST_TBL          DELPMST,");
            sb.Append("       ");
            //sb.Append("       ----")
            sb.Append("       USER_MST_TBL           UMT,");
            //sb.Append("       ----")
            sb.Append("       AGENT_MST_TBL          AMT,");
            sb.Append("       AGENT_CONTACT_DTLS     ACD,");
            sb.Append("       COUNTRY_MST_TBL        ACMT,");
            //sb.Append("       -----")
            sb.Append("       AIRLINE_MST_TBL       AIRMT ");
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MAWB.MAWB_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_AIR_FK = BST.BOOKING_AIR_PK");
            sb.Append("   AND JSE.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK(+)");
            sb.Append("   AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)");
            sb.Append("   AND JSE.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+)");
            sb.Append("   AND BST.DEL_PLACE_MST_FK = DELPMST.PLACE_PK(+) ");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MAWB.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND MAWB.AGENT_NAME = AMT.AGENT_NAME(+)");
            sb.Append("   AND AMT.AGENT_MST_PK = ACD.AGENT_MST_FK(+)");
            sb.Append("   AND ACMT.COUNTRY_MST_PK(+) = ACD.ADM_COUNTRY_MST_FK");
            sb.Append("   AND AIRMT.AIRLINE_MST_PK = MAWB.AIRLINE_MST_FK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = MAWB.CARGO_MOVE_FK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "MBL_HEADER");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportAirCargoManifestHBLDetail(string Pks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT MAWB.MAWB_EXP_TBL_PK,HAWB.HAWB_EXP_TBL_PK,HAWB.HAWB_REF_NO ");
            sb.Append(" FROM  MAWB_EXP_TBL           MAWB,");
            sb.Append("       HAWB_EXP_TBL           HAWB,");
            sb.Append("       JOB_CARD_AIR_EXP_TBL   JSE,");
            sb.Append("       BOOKING_AIR_TBL        BST ");
            sb.Append(" WHERE TO_CHAR(MAWB.MAWB_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_AIR_FK = BST.BOOKING_AIR_PK");
            sb.Append("   AND JSE.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK ");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "HBL_DETAIL");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportAirCargoManifestShipConsDetail(string Pks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT MAWB.MAWB_EXP_TBL_PK,HAWB.HAWB_EXP_TBL_PK,");
            sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPER_NAME,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_1 SHIPPER_ADDRESS1,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_2 SHIPPER_ADDRESS2,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_3 SHIPPER_ADDRESS3,");
            sb.Append("                SHIPPERDTLS.ADM_CITY SHIPPER_CITY,");
            sb.Append("                SHIPPERDTLS.ADM_ZIP_CODE SHIPPER_POST_CODE,");
            sb.Append("                SHIPPERCNT.COUNTRY_NAME SHIPPER_COUNTRY,");
            sb.Append("                SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPER_PHONE,");
            sb.Append("                SHIPPERDTLS.ADM_FAX_NO SHIPPER_FAX,");
            //sb.Append("                SHIPPERDTLS.ADM_EMAIL_ID SHIPPER_EMAIL,")
            sb.Append("                CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEE_ADDRESS1,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEE_ADDRESS2,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEE_ADDRESS3,");
            sb.Append("                CONSIGNEEDTLS.ADM_CITY CONSIGNEE_CITY,");
            sb.Append("                CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEE_POST_CODE,");
            sb.Append("                CONSIGNEECNT.COUNTRY_NAME CONSIGNEE_COUNTRY,");
            sb.Append("                CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEE_PHONE,");
            sb.Append("                CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEE_FAX ");
            //sb.Append("                CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEE_EMAIL,")
            sb.Append(" FROM  MAWB_EXP_TBL            MAWB,");
            sb.Append("       HAWB_EXP_TBL            HAWB,");
            sb.Append("       JOB_CARD_AIR_EXP_TBL   JSE,");
            sb.Append("       BOOKING_AIR_TBL        BST,");
            sb.Append("       CUSTOMER_MST_TBL       SHIPPER,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  SHIPPERDTLS,");
            sb.Append("       COUNTRY_MST_TBL        SHIPPERCNT,");
            sb.Append("       CUSTOMER_MST_TBL       CONSIGNEE,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  CONSIGNEEDTLS,");
            sb.Append("       COUNTRY_MST_TBL        CONSIGNEECNT,");
            sb.Append("       ");
            //sb.Append("       ----")
            sb.Append("       USER_MST_TBL           UMT ");
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MAWB.MAWB_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_AIR_FK = BST.BOOKING_AIR_PK");
            sb.Append("   AND JSE.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+) ");
            sb.Append("   AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)");
            sb.Append("   AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)");
            //sb.Append("   ------")
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MAWB.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "SHIPPER_CONSIGNEE_DETAIL");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportAirCargoManifestContCargoDetail(string Pks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT MAWB.MAWB_EXP_TBL_PK,HAWB.HAWB_EXP_TBL_PK,");
            sb.Append("                JSE.MARKS_NUMBERS MARKS_NUMBER,");
            sb.Append("                PY.PACK_TYPE_DESC PACK_TYPE,");
            sb.Append("                JTSEC.PACK_COUNT COUNT,");
            sb.Append("                JTSEC.GROSS_WEIGHT WEIGHT,");
            sb.Append("                JTSEC.VOLUME_IN_CBM VOLUME,");
            sb.Append("                JSE.GOODS_DESCRIPTION GOODS_DESC,");
            sb.Append("                ");
            //sb.Append("                ---SPECIAL REQU---")
            sb.Append("                JTAHSR.HAZ_OUTERPACK_TYPE,");
            sb.Append("                JTAHSR.HAZ_INNERPACK_TYPE,");
            sb.Append("                JTAHSR.IMDG_CLASS_CODE HAZ_IMDG_CLASS,");
            sb.Append("                JTAHSR.UN_NO HAZ_UN_NR,");
            sb.Append("                JTAHSR.FLASH_PNT_TEMP FLASH_POINT,");
            sb.Append("                JTARSR.MIN_TEMP,");
            sb.Append("                DECODE(JTARSR.MIN_TEMP_UOM, 0, 'C', 1, 'F') MINCF,");
            sb.Append("                JTARSR.MAX_TEMP,");
            sb.Append("                DECODE(JTARSR.MAX_TEMP_UOM, 0, 'C', 1, 'F') MAXCF,");
            sb.Append("                JTAHSR.EMS_NUMBER EMS_MFAG_NR,");
            sb.Append("                JTARSR.HUMIDITY_FACTOR,");
            sb.Append("                DECODE(JTARSR.IS_PERISHABLE_GOODS, 1, 'YES', 0, 'NO') ISPERISHABLE");
            sb.Append("                ");
            sb.Append("                ");
            sb.Append(" FROM  MAWB_EXP_TBL            MAWB,");
            sb.Append("       HAWB_EXP_TBL            HAWB,");
            sb.Append("       JOB_CARD_AIR_EXP_TBL   JSE,");
            sb.Append("       BOOKING_AIR_TBL        BST,");
            sb.Append("       JOB_TRN_AIR_EXP_CONT   JTSEC,");
            sb.Append("       (SELECT JTAHSR.BOOKING_AIR_FK,");
            sb.Append("               OPACK.PACK_TYPE_ID HAZ_OUTERPACK_TYPE,");
            sb.Append("               IPACK.PACK_TYPE_ID HAZ_INNERPACK_TYPE,");
            sb.Append("               JTAHSR.IMDG_CLASS_CODE,");
            sb.Append("               JTAHSR.UN_NO,");
            sb.Append("               JTAHSR.FLASH_PNT_TEMP,");
            sb.Append("               JTAHSR.EMS_NUMBER ");
            sb.Append("          FROM BKG_TRN_AIR_HAZ_SPL_REQ JTAHSR,");
            sb.Append("               BOOKING_AIR_TBL BAT,");
            sb.Append("               COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("               PACK_TYPE_MST_TBL     IPACK,");
            sb.Append("               PACK_TYPE_MST_TBL     OPACK");
            sb.Append("         WHERE CGMT.COMMODITY_GROUP_CODE = 'HAZARDOUS' ");
            sb.Append("           AND CGMT.COMMODITY_GROUP_PK = BAT.COMMODITY_GROUP_FK");
            sb.Append("           AND BAT.BOOKING_AIR_PK = JTAHSR.BOOKING_AIR_FK");
            sb.Append("           AND IPACK.PACK_TYPE_MST_PK(+) = JTAHSR.INNER_PACK_TYPE_MST_FK");
            sb.Append("           AND OPACK.PACK_TYPE_MST_PK(+) = JTAHSR.OUTER_PACK_TYPE_MST_FK) JTAHSR,");
            sb.Append("       (SELECT JTARSR.BOOKING_AIR_FK,");
            sb.Append("               JTARSR.MIN_TEMP,");
            sb.Append("               JTARSR.MAX_TEMP,");
            sb.Append("               JTARSR.MIN_TEMP_UOM,");
            sb.Append("               JTARSR.MAX_TEMP_UOM,");
            sb.Append("               JTARSR.HUMIDITY_FACTOR,");
            sb.Append("               JTARSR.IS_PERISHABLE_GOODS ");
            sb.Append("          FROM BKG_TRN_AIR_REF_SPL_REQ JTARSR,");
            sb.Append("               BOOKING_AIR_TBL BAT,");
            sb.Append("               COMMODITY_GROUP_MST_TBL CGMT ");
            sb.Append("         WHERE CGMT.COMMODITY_GROUP_CODE = 'REEFER' ");
            sb.Append("           AND CGMT.COMMODITY_GROUP_PK = BAT.COMMODITY_GROUP_FK");
            sb.Append("           AND BAT.BOOKING_AIR_PK = JTARSR.BOOKING_AIR_FK) JTARSR,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTMST,");
            sb.Append("       PACK_TYPE_MST_TBL      PY,");
            sb.Append("       USER_MST_TBL           UMT,");
            //sb.Append("       ---sr--")
            sb.Append("       PACK_TYPE_MST_TBL     IPACK,");
            sb.Append("       PACK_TYPE_MST_TBL     OPACK");
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MAWB.MAWB_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_AIR_FK = BST.BOOKING_AIR_PK");
            sb.Append("   AND JSE.JOB_CARD_AIR_EXP_PK = JTSEC.JOB_CARD_AIR_EXP_FK(+)");
            sb.Append("   AND JSE.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+) ");
            sb.Append("   AND JTSEC.PACK_TYPE_MST_FK = PY.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND JTSEC.CONTAINER_TYPE_MST_FK = CTMST.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " ");
            sb.Append("   AND MAWB.CREATED_BY_FK = UMT.USER_MST_PK");
            //sb.Append("   ---")
            sb.Append("   AND BST.BOOKING_AIR_PK = JTAHSR.BOOKING_AIR_FK(+)");
            sb.Append("   AND BST.BOOKING_AIR_PK = JTARSR.BOOKING_AIR_FK(+)");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "CONTAINER_CARGO_DETAIL");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        public DataSet GetExportAirCargoManifestFreightDetail(string Pks = "")
        {
            StringBuilder sb = new StringBuilder(5000);

            sb.Append("SELECT DISTINCT MAWB.MAWB_EXP_TBL_PK,HAWB.HAWB_EXP_TBL_PK,");
            sb.Append("                FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("                FEMT.FREIGHT_ELEMENT_NAME,");
            sb.Append("                DECODE(JTSEF.FREIGHT_TYPE,1,'Prepaid',2,'Collect') PAYMENT_TYPE,");
            sb.Append("                JTSEF.FREIGHT_AMT,");
            sb.Append("                CURR.CURRENCY_ID CURRENCY ");
            sb.Append("                ");
            sb.Append("                ");
            sb.Append(" FROM MAWB_EXP_TBL            MAWB,");
            sb.Append("       HAWB_EXP_TBL           HAWB,");
            sb.Append("       JOB_CARD_AIR_EXP_TBL   JSE,");
            sb.Append("       BOOKING_AIR_TBL        BST,");
            sb.Append("       JOB_TRN_AIR_EXP_FD    JTSEF,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL    CURR");
            sb.Append("       ");
            sb.Append(" WHERE TO_CHAR(MAWB.MAWB_EXP_TBL_PK) IN (" + Pks + ")");
            sb.Append("   AND JSE.BOOKING_AIR_FK = BST.BOOKING_AIR_PK");
            sb.Append("   AND JSE.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK ");
            sb.Append("   AND JSE.JOB_CARD_AIR_EXP_PK  = JTSEF.JOB_CARD_AIR_EXP_FK(+)");
            sb.Append("   AND FEMT.FREIGHT_ELEMENT_MST_PK(+) = JTSEF.FREIGHT_ELEMENT_MST_FK");
            sb.Append("   AND CURR.CURRENCY_MST_PK = JTSEF.CURRENCY_MST_FK");
            sb.Append("");

            DA = objWF.GetDataAdapter(sb.ToString());
            DA.Fill(MainDS, "FREIGHT_DETAIL");
            try
            {
                foreach (DataRow _row in MainDS.Tables[0].Rows)
                {
                    foreach (DataColumn col in MainDS.Tables[0].Columns)
                    {
                        if ((_row[col.ColumnName] == null) | string.IsNullOrEmpty(_row[col.ColumnName].ToString()))
                        {
                            try
                            {
                                _row[col.ColumnName] = " ";
                            }
                            catch (Exception ex)
                            {
                                _row[col.ColumnName] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return MainDS;
        }

        #endregion " CARGO/FREIGHT MANIFEST AIR "

        #endregion "Generate Cargo Manifest"

        #region "Save Invoice Status"

        public string SaveInvStatus(string InvPks)
        {
            StringBuilder sb = new StringBuilder(5000);
            try
            {
                sb.Append("UPDATE CONSOL_INVOICE_TBL CON ");
                sb.Append("SET CON.EDI_STATUS=1 ");
                sb.Append("WHERE CON.CONSOL_INVOICE_PK IN (" + InvPks + ")");
                objWF.ExecuteCommands(sb.ToString());
                return "Status changed to transmitted";
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }

        #endregion "Save Invoice Status"

        #region "CAN Count"

        public DataSet FetchCANCount(int JobPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT * FROM CAN_MST_TBL C WHERE C.JOB_CARD_FK=" + JobPK);
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "CAN Count"

        #region "Fetch Grid Details"

        public DataSet FecthGridDetails(string Jobpk, string BizType, string ProcessType, string CargoType)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                //'Sea
                if (Convert.ToInt32(BizType) == 2)
                {
                    //'Export
                    if (Convert.ToInt32(ProcessType) == 1)
                    {
                        if (Convert.ToInt32(CargoType) == 1)
                        {
                            var _with15 = objWF.MyCommand.Parameters;
                            _with15.Add("JOB_CARD_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                            _with15.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            _with15.Add("GETSUM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            return objWF.GetDataSet("FETCH_JOBCARD_PROFITABILITY", "FETCH_GRIDDETAILS");
                        }
                        else if (Convert.ToInt32(CargoType) == 2)
                        {
                            var _with17 = objWF.MyCommand.Parameters;
                            _with17.Add("JOB_CARD_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                            _with17.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            _with17.Add("GETSUM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            return objWF.GetDataSet("FETCH_JOBCARD_PROFITABILITY", "FETCH_LCLGRIDDETAILS");
                        }
                        else
                        {
                            var _with16 = objWF.MyCommand.Parameters;
                            _with16.Add("JOB_CARD_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                            _with16.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            _with16.Add("GETSUM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            return objWF.GetDataSet("FETCH_JOBCARD_PROFITABILITY", "FETCH_BBCGRIDDETAILS");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(CargoType) == 1)
                        {
                            var _with18 = objWF.MyCommand.Parameters;
                            _with18.Add("JOB_CARD_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                            _with18.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            _with18.Add("GETSUM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            return objWF.GetDataSet("FETCH_JOBCARD_PROFITABILITY", "FETCH_IMPFCLDETAILS");
                        }
                        else if (Convert.ToInt32(CargoType) == 2)
                        {
                            var _with20 = objWF.MyCommand.Parameters;
                            _with20.Add("JOB_CARD_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                            _with20.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            _with20.Add("GETSUM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            return objWF.GetDataSet("FETCH_JOBCARD_PROFITABILITY", "FETCH_IMPLCLGRIDDETAILS");
                        }
                        else
                        {
                            var _with19 = objWF.MyCommand.Parameters;
                            _with19.Add("JOB_CARD_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                            _with19.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            _with19.Add("GETSUM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                            return objWF.GetDataSet("FETCH_JOBCARD_PROFITABILITY", "FETCH_IMPBBCGRIDDETAILS");
                        }
                    }
                    //'Air
                }
                else
                {
                    //'Export
                    if (Convert.ToInt32(ProcessType) == 1)
                    {
                        var _with21 = objWF.MyCommand.Parameters;
                        _with21.Add("JOB_CARD_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                        _with21.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                        _with21.Add("GETSUM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                        return objWF.GetDataSet("FETCH_JOBCARD_PROFITABILITY", "FETCH_AIRDETAILS");
                    }
                    else
                    {
                        var _with22 = objWF.MyCommand.Parameters;
                        _with22.Add("JOB_CARD_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                        _with22.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                        _with22.Add("GETSUM_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                        return objWF.GetDataSet("FETCH_JOBCARD_PROFITABILITY", "FETCH_IMPAIRDETAILS");
                    }
                }
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

        #endregion "Fetch Grid Details"

        #region "Fetch Receivables Header"

        public DataSet FetchRecHeader()
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            sb.Append(" SELECT ");
            sb.Append("       '' Receivable,");
            sb.Append("       '' Payable");
            sb.Append("       FROM DUAL WHERE 1 <> 1 ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Fetch Receivables Header"

        #region "Fetch Summary Header"

        public DataSet FetchSummHeader()
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            sb.Append(" SELECT ");
            sb.Append("       '' CurSum");
            sb.Append("       FROM DUAL WHERE 1 <> 1 ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Fetch Summary Header"

        #region "Fetch Header Details"

        public DataSet GetHeaderDetails(string JOBPK, string BizType, string ProcType)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT JOB.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(JOB.JOBCARD_DATE,DATEFORMAT)JOBCARD_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME   SHIPPER,");
                sb.Append("       CONSG.CUSTOMER_NAME CONSIGNEE,");
                if (Convert.ToInt32(BizType) == 2)
                {
                    sb.Append("       OPR.OPERATOR_NAME,");
                }
                else
                {
                    sb.Append("       ART.AIRLINE_NAME OPERATOR_NAME,");
                }
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       JOB.SHIPPER_CUST_MST_FK");
                sb.Append("  FROM JOB_CARD_SEA_EXP_TBL JOB,");
                sb.Append("       BOOKING_SEA_TBL      BOOK,");
                sb.Append("       CUSTOMER_MST_TBL     CMT,");
                sb.Append("       CUSTOMER_MST_TBL     CONSG,");

                if (Convert.ToInt32(BizType) == 2)
                {
                    sb.Append("       OPERATOR_MST_TBL     OPR,");
                }
                else if (Convert.ToInt32(BizType) == 1)
                {
                    sb.Append("       AIRLINE_MST_TBL     ART,");
                }

                sb.Append("       AGENT_MST_TBL        AMT");
                sb.Append(" WHERE JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CONSG.CUSTOMER_MST_PK(+)");
                if (Convert.ToInt32(ProcType) == 1)
                {
                    sb.Append("   AND JOB.BOOKING_SEA_FK = BOOK.BOOKING_SEA_PK");
                }
                if (Convert.ToInt32(BizType) == 2 & Convert.ToInt32(ProcType) == 1)
                {
                    sb.Append("   AND BOOK.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK");
                }
                else if (Convert.ToInt32(BizType) == 1 & Convert.ToInt32(ProcType) == 1)
                {
                    sb.Append("   AND BOOK.AIRLINE_MST_FK = ART.AIRLINE_MST_PK");
                }
                else if (Convert.ToInt32(BizType) == 2 & Convert.ToInt32(ProcType) == 2)
                {
                    sb.Append("   AND JOB.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK");
                }
                else
                {
                    sb.Append("   AND JOB.AIRLINE_MST_FK = ART.AIRLINE_MST_PK");
                }

                if (Convert.ToInt32(ProcType) == 1)
                {
                    sb.Append("   AND JOB.DP_AGENT_MST_FK = AMT.AGENT_MST_PK(+)");
                }
                else
                {
                    sb.Append("   AND JOB.POL_AGENT_MST_FK = AMT.AGENT_MST_PK(+)");
                }
                sb.Append("   AND JOB.JOB_CARD_SEA_EXP_PK = " + JOBPK);
                if (Convert.ToInt32(BizType) == 1)
                {
                    sb.Replace("SEA", "AIR");
                }
                if (Convert.ToInt32(ProcType) == 2)
                {
                    sb.Replace("EXP", "IMP");
                }
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Fetch Header Details"

        #region "Get Contract PK"

        public DataSet GetContractPK(string ContractNr, string BizType)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (Convert.ToInt32(BizType) == 2)
                {
                    sb.Append(" SELECT CMAIN.CONT_MAIN_SEA_PK,");
                    sb.Append("        CMAIN.CARGO_TYPE,");
                    sb.Append("        CMAIN.ACTIVE,");
                    sb.Append("        CMAIN.CONT_APPROVED");
                    sb.Append("  FROM CONT_MAIN_SEA_TBL CMAIN");
                    sb.Append("  WHERE CMAIN.CONTRACT_NO = '" + ContractNr + "' ");
                }
                else
                {
                    sb.Append(" SELECT CMAIN.CONT_MAIN_AIR_PK,");
                    sb.Append("        CMAIN.ACTIVE,");
                    sb.Append("        CMAIN.CONT_APPROVED");
                    sb.Append("  FROM CONT_MAIN_AIR_TBL CMAIN");
                    sb.Append("  WHERE CMAIN.CONTRACT_NO = '" + ContractNr + "' ");
                }

                return objWF.GetDataSet(sb.ToString());
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

        public DataSet GetTranPK(string From, string ContractNr, string BizType)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (Convert.ToInt32(BizType) == 2)
                {
                    if (Convert.ToInt32(From) == 1)
                    {
                        sb.Append(" SELECT Q.QUOTATION_SEA_PK");
                        sb.Append("  FROM QUOTATION_SEA_TBL Q");
                        sb.Append("  WHERE  Q.QUOTATION_REF_NO = '" + ContractNr + "' ");
                        //Customer Contract
                    }
                    else if (Convert.ToInt32(From) == 3)
                    {
                        sb.Append(" SELECT CMAIN.CONT_CUST_SEA_PK, CMAIN.CUSTOMER_MST_FK,CMAIN.CARGO_TYPE");
                        sb.Append("  FROM CONT_CUST_SEA_TBL CMAIN");
                        sb.Append("  WHERE  CMAIN.CONT_REF_NO= '" + ContractNr + "' ");
                        //SRR
                    }
                    else if (Convert.ToInt32(From) == 5)
                    {
                        sb.Append(" SELECT CMAIN.SRR_SEA_PK, CMAIN.STATUS,CMAIN.CARGO_TYPE");
                        sb.Append("  FROM SRR_SEA_TBL CMAIN");
                        sb.Append("  WHERE  CMAIN.SRR_REF_NO= '" + ContractNr + "' ");
                    }
                }
                else
                {
                    if (Convert.ToInt32(From) == 1)
                    {
                        sb.Append(" SELECT Q.QUOTATION_AIR_PK,Q.QUOTATION_TYPE");
                        sb.Append("  FROM QUOTATION_AIR_TBL Q");
                        sb.Append("  WHERE  Q.QUOTATION_REF_NO = '" + ContractNr + "' ");
                    }
                    else if (Convert.ToInt32(From) == 3)
                    {
                        sb.Append(" SELECT CMAIN.CONT_CUST_AIR_PK, CMAIN.CUSTOMER_MST_FK");
                        sb.Append("  FROM CONT_CUST_AIR_TBL CMAIN");
                        sb.Append("  WHERE  CMAIN.CONT_REF_NO= '" + ContractNr + "' ");
                        //SRR
                    }
                    else if (Convert.ToInt32(From) == 5)
                    {
                        sb.Append(" SELECT CMAIN.SRR_AIR_PK, CMAIN.SRR_APPROVED");
                        sb.Append("  FROM SRR_AIR_TBL CMAIN");
                        sb.Append("  WHERE  CMAIN.SRR_REF_NO= '" + ContractNr + "' ");
                    }
                }
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Get Contract PK"

        #region "JobCard Search"

        public string FETCH_JOB_REF_PAYREQ(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            string strProcess = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            //Business Type to identify the user belongs to AIR/SEA
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strProcess = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_JOB_REF_PAYREQ";
                var _with23 = SCM.Parameters;
                _with23.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with23.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with23.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with23.Add("LOCATION_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input;
                _with23.Add("PROCESS_TYPE_IN", strProcess).Direction = ParameterDirection.Input;
                _with23.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
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

        #endregion "JobCard Search"

        #region "FetchXML JOBdData"

        public DataSet FetchXMLJOBdata(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with24 = objWF.MyCommand.Parameters;
                _with24.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with24.Add("BIZTYPE_IN", 2).Direction = ParameterDirection.Input;
                _with24.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with24.Add("LOG_LOC_FK", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with24.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FETCH_XMLJOBDATA_SEA");
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

        public DataSet FetchXMLCargodata(string JobXmlpk = "0", int BizType = 1, int ProcessType = 1, string CaroType = "1")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();

            try
            {
                var _with25 = objWF.MyCommand.Parameters;
                _with25.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with25.Add("BIZTYPE_IN", 2).Direction = ParameterDirection.Input;
                _with25.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with25.Add("CARGO_TYPE_IN", CaroType).Direction = ParameterDirection.Input;
                _with25.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_WINXML_PKG", "FETCH_XMLCARGODATA_SEA");
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

        public DataSet FetchXMLHAZARDOUSData(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();

            try
            {
                var _with26 = objWF.MyCommand.Parameters;
                _with26.Add("CONT_PK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with26.Add("BIZTYPE_IN", 2).Direction = ParameterDirection.Input;
                _with26.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with26.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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

        public DataSet FetchXMLCommoditydata(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();

            try
            {
                var _with27 = objWF.MyCommand.Parameters;
                _with27.Add("CONT_PK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with27.Add("BIZTYPE_IN", 2).Direction = ParameterDirection.Input;
                _with27.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with27.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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

        public DataSet FetchXMLPickUp(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with28 = objWF.MyCommand.Parameters;
                _with28.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with28.Add("BIZTYPE_IN", 2).Direction = ParameterDirection.Input;
                _with28.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with28.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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

        public DataSet FetchXMLDrop(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with29 = objWF.MyCommand.Parameters;
                _with29.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with29.Add("BIZTYPE_IN", 2).Direction = ParameterDirection.Input;
                _with29.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with29.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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

        public DataSet FetchXMLDocument(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with30 = objWF.MyCommand.Parameters;
                _with30.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with30.Add("BIZTYPE_IN", 2).Direction = ParameterDirection.Input;
                _with30.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with30.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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

        public DataSet FetchXMLActivity(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with31 = objWF.MyCommand.Parameters;
                _with31.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with31.Add("BIZ_TYPE_IN", 2).Direction = ParameterDirection.Input;
                _with31.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with31.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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

        public DataSet FetchXMLImpSeaAir(string JobXmlpk = "0", int BizType = 0, int ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with32 = objWF.MyCommand.Parameters;
                _with32.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with32.Add("BIZTYPE_IN", 2).Direction = ParameterDirection.Input;
                _with32.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with32.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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

        public DataSet FETCHXMLTRANSHIPMENT(string JobXmlpk = "0", int BizType = 0, int ProcessType = 1)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with33 = objWF.MyCommand.Parameters;
                _with33.Add("JOBPK_IN", JobXmlpk).Direction = ParameterDirection.Input;
                _with33.Add("BIZTYPE_IN", 2).Direction = ParameterDirection.Input;
                _with33.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with33.Add("GETDS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
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
    }
}