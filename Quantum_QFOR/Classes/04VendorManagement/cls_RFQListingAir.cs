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
    public class clsRFQListingAir : CommonFeatures
    {
        private Int32 m_last;
        private Int32 m_start;

        #region "Fetch Main"

        private DataSet DSMain = new DataSet();

        public DataSet FetchAll(string AirlineID = "", string AirlineName = "", string RFQNO = "", string RFQDATE = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string ValidFrom = "", string ValidTo = "",
        bool ActiveOnly = true, string SearchType = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, int ChkONLD = 0, string SortType = " ASC ", string CurrBizType = "3")
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
            if (AirlineID.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AIRLINE_ID) LIKE '" + SrOP + AirlineID.ToUpper().Replace("'", "''") + "%'");
            }
            if (AirlineName.Length > 0)
            {
                buildCondition.Append(" AND UPPER(AIRLINE_NAME) LIKE '" + SrOP + AirlineName.ToUpper().Replace("'", "''") + "%'");
            }
            if (RFQNO.Length > 0)
            {
                buildCondition.Append(" AND UPPER(RFQ_REF_NO) LIKE '" + SrOP + RFQNO.ToUpper().Replace("'", "''") + "%'");
            }
            if (RFQDATE.Length > 0)
            {
                buildCondition.Append(" AND RFQ_DATE = TO_DATE('" + RFQDATE + "' , '" + dateFormat + "') ");
            }

            if (POLID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + POLID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM RFQ_TRN_AIR_LCL ");
                buildCondition.Append("           WHERE RFQ_MAIN_AIR_FK = main.RFQ_MAIN_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }

            if (POLName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + POLName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POL_FK FROM RFQ_TRN_AIR_LCL ");
                buildCondition.Append("           WHERE RFQ_MAIN_AIR_FK = main.RFQ_MAIN_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODID.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_ID FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_ID) LIKE '" + SrOP + PODID.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM RFQ_TRN_AIR_LCL ");
                buildCondition.Append("           WHERE RFQ_MAIN_AIR_FK = main.RFQ_MAIN_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }
            if (PODName.Length > 0)
            {
                buildCondition.Append(" AND EXISTS ");
                buildCondition.Append("     ( SELECT PORT_NAME FROM PORT_MST_TBL ");
                buildCondition.Append("       WHERE UPPER(PORT_NAME) LIKE '" + SrOP + PODName.ToUpper().Replace("'", "''") + "%' ");
                buildCondition.Append("       and PORT_MST_PK IN ");
                buildCondition.Append("         ( SELECT PORT_MST_POD_FK FROM RFQ_TRN_AIR_LCL ");
                buildCondition.Append("           WHERE RFQ_MAIN_AIR_FK = main.RFQ_MAIN_AIR_PK ");
                buildCondition.Append("         ) ");
                buildCondition.Append("     ) ");
            }

            if (ActiveOnly == true)
            {
                buildCondition.Append(" AND MAIN.ACTIVE = 1 ");
                buildCondition.Append(" AND ( ");
                buildCondition.Append("        MAIN.VALID_TO >= TO_DATE(SYSDATE , '" + dateFormat + "') ");
                buildCondition.Append("       OR MAIN.VALID_TO IS NULL ");
                buildCondition.Append("     ) ");
            }

            //'Goutam : When From and To date are selected, any contract valid b/w selected period should list in the listing screen.
            if (!string.IsNullOrEmpty(ValidFrom) & string.IsNullOrEmpty(ValidTo))
            {
                buildCondition.Append(" AND TO_DATE('" + ValidFrom + "', '" + dateFormat + "') BETWEEN TO_DATE(MAIN.VALID_FROM, '" + dateFormat + "') AND TO_DATE(NVL(MAIN.VALID_TO, SYSDATE), '" + dateFormat + "') ");
            }

            if (!string.IsNullOrEmpty(ValidTo) & string.IsNullOrEmpty(ValidFrom))
            {
                buildCondition.Append(" AND TO_DATE('" + ValidTo + "', '" + dateFormat + "') BETWEEN TO_DATE(MAIN.VALID_FROM, '" + dateFormat + "') AND TO_DATE(NVL(MAIN.VALID_TO, SYSDATE), '" + dateFormat + "') ");
            }

            if (!string.IsNullOrEmpty(ValidFrom) & !string.IsNullOrEmpty(ValidTo))
            {
                buildCondition.Append(" AND (TO_DATE(MAIN.VALID_FROM,'" + dateFormat + "') BETWEEN TO_DATE('" + ValidFrom.Trim() + "','" + dateFormat + "') AND TO_DATE('" + ValidTo.Trim() + "','" + dateFormat + "')  ");
                buildCondition.Append(" OR TO_DATE(NVL(MAIN.VALID_TO, SYSDATE) ,'" + dateFormat + "') BETWEEN TO_DATE('" + ValidFrom.Trim() + "','" + dateFormat + "') AND TO_DATE('" + ValidTo.Trim() + "','" + dateFormat + "'))  ");
            }

            //If ValidTo.Length > 0 And Not ValidFrom.Length > 0 Then
            //    buildCondition.Append(vbCrLf & " AND ")
            //    buildCondition.Append(vbCrLf & "        ((TO_DATE('" & ValidTo & "' , '" & dateFormat & "') ")
            //    buildCondition.Append(vbCrLf & "       between main.valid_from and main.valid_to) OR main.VALID_TO IS NULL ")
            //    buildCondition.Append(vbCrLf & "     ) ")

            //ElseIf ValidFrom.Length > 0 And Not ValidTo.Length > 0 Then
            //    buildCondition.Append(vbCrLf & " AND ")
            //    buildCondition.Append(vbCrLf & "        (TO_DATE('" & ValidFrom & "' , '" & dateFormat & "') ")
            //    buildCondition.Append(vbCrLf & "       between main.valid_from and main.valid_to ")
            //    buildCondition.Append(vbCrLf & "     ) ")
            //ElseIf ValidFrom.Length > 0 And ValidTo.Length > 0 Then
            //    buildCondition.Append("     AND ((TO_DATE('" & ValidTo & "' , '" & dateFormat & "') between")
            //    buildCondition.Append("     main.valid_from and main.valid_to) OR")
            //    buildCondition.Append("     main.VALID_TO IS NULL")
            //    buildCondition.Append("     OR (TO_DATE('" & ValidFrom & "' , '" & dateFormat & "') between")
            //    buildCondition.Append("     main.valid_from and main.valid_to ))")
            //End If
            //'End Goutam

            buildCondition.Append("     AND MAIN.CREATED_BY_FK IN");
            buildCondition.Append("     (SELECT UMT.USER_MST_PK");
            buildCondition.Append("     FROM USER_MST_TBL UMT");
            buildCondition.Append("     WHERE UMT.DEFAULT_LOCATION_FK =");
            buildCondition.Append("     (SELECT U.DEFAULT_LOCATION_FK");
            buildCondition.Append("     FROM USER_MST_TBL U");
            buildCondition.Append("     WHERE U.USER_MST_PK =" + HttpContext.Current.Session["USER_PK"] + "))");
            //End Snigdharani
            if (ActiveOnly == true)
            {
                buildCondition.Append(" AND main.ACTIVE = 1 ");
            }

            if (ChkONLD == 0)
            {
                buildCondition.Append(" AND 1=2 ");
            }

            strCondition = buildCondition.ToString();

            buildQuery.Append(" Select count(*) ");
            buildQuery.Append("      from ");
            buildQuery.Append("       RFQ_MAIN_AIR_TBL main, ");
            buildQuery.Append("       AIRLINE_MST_TBL ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       main.AIRLINE_MST_FK = AIRLINE_MST_PK ");
            buildQuery.Append("      " + strCondition);

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
            buildQuery.Append("    ( Select ");
            buildQuery.Append(" NVL(main.ACTIVE, 0) ACTIVE,");
            buildQuery.Append(" RFQ_REF_NO, ");
            buildQuery.Append(" to_date(RFQ_DATE,'" + dateFormat + "')RFQ_DATE, ");
            buildQuery.Append(" AIRLINE_NAME, ");
            buildQuery.Append(" RFQ_MAIN_AIR_PK, ");
            buildQuery.Append(" to_date(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append(" to_date(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
            buildQuery.Append(" 0 POL, ");
            buildQuery.Append(" 0 POD ");
            buildQuery.Append("      from ");
            buildQuery.Append("       RFQ_MAIN_AIR_TBL main, ");
            buildQuery.Append("       AIRLINE_MST_TBL ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       main.AIRLINE_MST_FK = AIRLINE_MST_PK ");
            buildQuery.Append("               " + strCondition);
            //buildQuery.Append(vbCrLf & "     Order By " & SortColumn & "," & SortType)
            buildQuery.Append("     Order By " + SortColumn + " DESC ");
            buildQuery.Append("    ) q ");
            buildQuery.Append("  )   ");
            buildQuery.Append("  where  ");
            buildQuery.Append("     SR_NO between " + start + " and " + last);
            strSQL = buildQuery.ToString();

            DataSet DS = null;

            try
            {
                DS = objWF.GetDataSet(strSQL);
                DS.Tables.Add(FetchChildFor(AllMasterPKs(DS), POLID, POLName, PODID, PODName, SrOP));
                DataRelation trfRel = new DataRelation("TariffRelation", DS.Tables[0].Columns["RFQ_MAIN_AIR_PK"], DS.Tables[1].Columns["RFQ_MAIN_AIR_FK"], true);
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

        private DataTable FetchChildFor(string RFQPKS = "", string POLID = "", string POLName = "", string PODID = "", string PODName = "", string SrOp = "%")
        {
            System.Text.StringBuilder buildQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder buildCondition = new System.Text.StringBuilder();
            string strCondition = "";
            string strSQL = "";

            if (RFQPKS.Trim().Length > 0)
            {
                buildCondition.Append(" and RFQ_MAIN_AIR_FK in (" + RFQPKS + ") ");
            }
            if (POLID.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(PORTPOL.PORT_ID) LIKE '" + SrOp + POLID.ToUpper().Replace("'", "''") + "%'");
            }
            if (POLName.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(PORTPOL.PORT_NAME) LIKE '" + SrOp + POLName.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODID.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(PORTPOD.PORT_ID) LIKE '" + SrOp + PODID.ToUpper().Replace("'", "''") + "%'");
            }
            if (PODName.Trim().Length > 0)
            {
                buildCondition.Append(" and UPPER(PORTPOD.PORT_NAME) LIKE '" + SrOp + PODName.ToUpper().Replace("'", "''") + "%'");
            }

            strCondition = buildCondition.ToString();

            buildQuery.Append(" Select * from ");
            buildQuery.Append("  ( Select ROWNUM SR_NO, q.* from ");
            buildQuery.Append("    ( Select DISTINCT ");
            buildQuery.Append("       RFQ_MAIN_AIR_FK, ");
            buildQuery.Append("       PORT_MST_POL_FK, ");
            buildQuery.Append("       PORTPOL.PORT_NAME PORT_POL_NAME, ");
            buildQuery.Append("       PORTPOL.PORT_ID PORT_POL_ID, ");
            buildQuery.Append("       PORTPOD.PORT_NAME PORT_POD_NAME, ");
            buildQuery.Append("       to_Char(VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            buildQuery.Append("       to_Char(VALID_TO, '" + dateFormat + "') VALID_TO, ");
            buildQuery.Append("       PORT_MST_POD_FK, ");
            buildQuery.Append("       PORTPOD.PORT_ID PORT_POD_ID ");
            buildQuery.Append("      from ");
            buildQuery.Append("       RFQ_TRN_AIR_LCL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOL, ");
            buildQuery.Append("       PORT_MST_TBL PORTPOD ");
            buildQuery.Append("      where ");
            // JOIN CONDITION
            buildQuery.Append("       PORT_MST_POL_FK = PORTPOL.PORT_MST_PK AND ");
            buildQuery.Append("       PORT_MST_POD_FK = PORTPOD.PORT_MST_PK ");
            buildQuery.Append("               " + strCondition);
            buildQuery.Append("      Order By RFQ_MAIN_AIR_FK, PORTPOL.PORT_NAME, PORTPOD.PORT_NAME ASC ");
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
                    if (Convert.ToInt32(dt.Rows[RowCnt]["RFQ_MAIN_AIR_FK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["RFQ_MAIN_AIR_FK"]);
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
                    strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["RFQ_MAIN_AIR_PK"]).Trim() + ",");
                }
                strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Main"

        #region "Make child Data Table"

        public DataTable MakeChildDataTable(string strSQl)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                DataTable dtChild = null;
                dtChild = objWF.GetDataTable(strSQl);
                return dtChild;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Make child Data Table"

        #region "Select Airline Fk when passing Airline ID"

        public int SelectAirlineFk(string AirlineId)
        {
            try
            {
                string strSql = null;
                int AirlineFk = 0;
                WorkFlow objWF = new WorkFlow();
                strSql = "SELECT AIRLINE_MST_PK FROM AIRLINE_MST_TBL WHERE AIRLINE_ID = '" + AirlineId + "' ";
                AirlineFk = Convert.ToInt32(objWF.ExecuteScaler(strSql));
                return AirlineFk;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Select Airline Fk when passing Airline ID"

        #region "Select Port fk when passing port ID"

        public int SelectPortFk(string Portid)
        {
            try
            {
                string strSql = null;
                int PortFk = 0;
                WorkFlow objWF = new WorkFlow();
                strSql = "SELECT PORT_MST_PK FROM PORT_MST_TBL WHERE PORT_ID = '" + Portid + "' ";
                PortFk = Convert.ToInt32(objWF.ExecuteScaler(strSql));
                return PortFk;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Select Port fk when passing port ID"

        #region "Select TradeFk when passing Trade ID"

        public int SelectTradeFk(string Tradeid)
        {
            try
            {
                string strSql = null;
                int TradeFk = 0;
                WorkFlow objWF = new WorkFlow();
                strSql = "SELECT TRADE_MST_PK FROM TRADE_MST_TBL WHERE TRADE_ID = '" + Tradeid + "' ";
                TradeFk = Convert.ToInt32(objWF.ExecuteScaler(strSql));
                return TradeFk;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Select TradeFk when passing Trade ID"
    }
}