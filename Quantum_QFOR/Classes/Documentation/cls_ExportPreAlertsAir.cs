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
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_ExportPreAlertsAir : CommonFeatures
    {
        #region "Fetch All"

        /// <summary>
        /// Funs the fetch all.
        /// </summary>
        /// <param name="lngAirlinePK">The LNG airline pk.</param>
        /// <param name="strFlightNo">The string flight no.</param>
        /// <param name="strPaletteSize">Size of the string palette.</param>
        /// <param name="lngAOOPK">The LNG aoopk.</param>
        /// <param name="lngAODPK">The LNG aodpk.</param>
        /// <param name="lngHAWBPK">The LNG hawbpk.</param>
        /// <param name="lngMAWBPK">The LNG mawbpk.</param>
        /// <param name="strFromDate">The string from date.</param>
        /// <param name="strToDate">The string to date.</param>
        /// <param name="lngDPAgentPK">The LNG dp agent pk.</param>
        /// <param name="strSearchType">Type of the string search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="blnSortAscending">The BLN sort ascending.</param>
        /// <param name="coload">if set to <c>true</c> [coload].</param>
        /// <param name="flag">The flag.</param>
        /// <param name="LocPk">The loc pk.</param>
        /// <returns></returns>
        public DataSet funFetchAll(long lngAirlinePK = 0, string strFlightNo = "", string strPaletteSize = "", long lngAOOPK = 0, long lngAODPK = 0, long lngHAWBPK = 0, long lngMAWBPK = 0, string strFromDate = "", string strToDate = "", long lngDPAgentPK = 0,
        string strSearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, string blnSortAscending = "", bool coload = false, Int32 flag = 0, Int32 LocPk = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = "";
            StringBuilder strBuilder = new StringBuilder();
            StringBuilder strCondition = new StringBuilder();
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition.Append(" AND 1=2 ");
            }
            if (!(lngAirlinePK == 0))
            {
                strCondition.Append("  AND B.CARRIER_MST_FK= " + lngAirlinePK);
            }
            if (strFlightNo.Trim().Length > 0)
            {
                strCondition.Append("  AND UPPER(H.FLIGHT_NO) LIKE '" + strFlightNo.ToUpper().Replace("'", "''") + "%'");
            }
            if (strPaletteSize.Trim().Length > 0)
            {
                strCondition.Append("  AND UPPER(JTRN.PALETTE_SIZE) LIKE '" + strPaletteSize.ToUpper().Replace("'", "''") + "%'");
            }
            if (!(lngAOOPK == 0))
            {
                strCondition.Append("  AND B.PORT_MST_POL_FK= " + lngAOOPK);
            }
            if (!(lngAODPK == 0))
            {
                strCondition.Append("  AND B.PORT_MST_POD_FK= " + lngAODPK);
            }
            if (!(lngHAWBPK == 0))
            {
                strCondition.Append("  AND H.HAWB_EXP_TBL_PK= " + lngHAWBPK);
            }
            if (!(lngMAWBPK == 0))
            {
                strCondition.Append("  AND M.MAWB_EXP_TBL_PK= " + lngMAWBPK);
            }
            if (!(lngDPAgentPK == 0))
            {
                strCondition.Append("  AND H.DP_AGENT_MST_FK= " + lngDPAgentPK);
            }
            if (strFromDate.Trim().Length > 0 & strToDate.Trim().Length > 0)
            {
                strCondition.Append(" AND H.hawb_date between to_date('" + System.String.Format("{0:" + dateFormat + "}", strFromDate) + "',dateformat)  and to_date('" + System.String.Format("{0:" + dateFormat + "}", strToDate) + "',dateformat)");
            }
            else if (strFromDate.Trim().Length > 0)
            {
                strCondition.Append(" AND to_date('" + System.String.Format("{0:" + dateFormat + "}", strFromDate) + "',dateformat) <= H.hawb_date ");
            }
            else if (strToDate.Trim().Length > 0)
            {
                strCondition.Append(" AND to_date('" + System.String.Format("{0:" + dateFormat + "}", strToDate) + "',dateformat) >= H.hawb_date ");
            }
            if (coload == true)
            {
                strCondition.Append(" AND J.CL_AGENT_MST_FK IS NOT NULL  ");
            }
            strCondition.Append(" AND J.BUSINESS_TYPE = 1 ");
            strCondition.Append(" AND J.PROCESS_TYPE = 1 ");

            strBuilder.Append(" SELECT DISTINCT COUNT(*) ");
            strBuilder.Append(" FROM HAWB_EXP_TBL    H, ");
            strBuilder.Append(" JOB_CARD_TRN         J, ");
            strBuilder.Append(" MAWB_EXP_TBL         M, ");
            strBuilder.Append(" CUSTOMER_MST_TBL     C, ");
            strBuilder.Append(" BOOKING_MST_TBL      B, ");
            strBuilder.Append(" PORT_MST_TBL         P, ");
            strBuilder.Append(" AIRLINE_MST_TBL      A, ");
            strBuilder.Append(" AGENT_MST_TBL        AMT, ");
            strBuilder.Append(" PORT_MST_TBL P1 ");
            if (strPaletteSize.Trim().Length > 0)
            {
                strBuilder.Append(" , JOB_TRN_CONT JTRN ");
            }
            strBuilder.Append(" WHERE A.AIRLINE_MST_PK = B.CARRIER_MST_FK ");
            strBuilder.Append(" AND H.DP_AGENT_MST_FK=AMT.AGENT_MST_PK(+) ");
            strBuilder.Append(" AND J.MBL_MAWB_FK=M.MAWB_EXP_TBL_PK(+) ");
            strBuilder.Append(" AND C.CUSTOMER_MST_PK(+) = J.SHIPPER_CUST_MST_FK ");
            strBuilder.Append(" AND J.BOOKING_MST_FK = B.BOOKING_MST_PK ");
            strBuilder.Append(" AND B.PORT_MST_POL_FK = P.PORT_MST_PK ");
            strBuilder.Append(" AND B.PORT_MST_POD_FK = P1.PORT_MST_PK ");
            strBuilder.Append(" AND H.JOB_CARD_AIR_EXP_FK = J.JOB_CARD_TRN_PK ");
            strBuilder.Append(" AND p.location_mst_fk =" + LocPk);
            if (strPaletteSize.Trim().Length > 0)
            {
                strBuilder.Append(" AND JTRN.JOB_CARD_TRN_FK=J.JOB_CARD_TRN_PK  ");
            }

            strSQL += strBuilder.ToString() + strCondition.ToString();

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

            strBuilder.Remove(0, strBuilder.ToString().Length);

            strBuilder.Append(" SELECT * ");
            strBuilder.Append(" from (SELECT ROWNUM SR_NO, q.* FROM (SELECT A.AIRLINE_ID, ");
            strBuilder.Append(" H.FLIGHT_NO, ");
            strBuilder.Append(" P.PORT_ID AS AOO, ");
            strBuilder.Append(" P1.PORT_ID AS AOD, ");
            strBuilder.Append(" H.HAWB_EXP_TBL_PK, ");
            strBuilder.Append(" H.HAWB_REF_NO, ");
            strBuilder.Append(" H.HAWB_DATE, ");
            strBuilder.Append(" M.MAWB_REF_NO, ");
            strBuilder.Append(" AMT.AGENT_NAME AGENT_ID, ");
            strBuilder.Append(" C.CUSTOMER_ID, ");
            strBuilder.Append(" DECODE(H.HAWB_STATUS, 0, 'Draft', 1, 'Released', 2, 'Confirmed', 3, 'Cancelled') STATUS, ");
            strBuilder.Append(" DECODE(B.EDI_STATUS,'0','NG','1','TM') EDI_STATUS, ");
            strBuilder.Append(" 'false' AS SEL ");
            strBuilder.Append(" FROM HAWB_EXP_TBL         H, ");
            strBuilder.Append(" JOB_CARD_TRN J, ");
            strBuilder.Append(" MAWB_EXP_TBL         M, ");
            strBuilder.Append(" CUSTOMER_MST_TBL     C, ");
            strBuilder.Append(" BOOKING_MST_TBL      B, ");
            strBuilder.Append(" PORT_MST_TBL         P, ");
            strBuilder.Append(" AIRLINE_MST_TBL      A, ");
            strBuilder.Append(" AGENT_MST_TBL        AMT, ");
            strBuilder.Append(" PORT_MST_TBL P1 ");
            if (strPaletteSize.Trim().Length > 0)
            {
                strBuilder.Append(" , JOB_TRN_CONT JTRN ");
            }
            strBuilder.Append(" WHERE A.AIRLINE_MST_PK = B.CARRIER_MST_FK ");
            strBuilder.Append(" AND H.DP_AGENT_MST_FK=AMT.AGENT_MST_PK(+) ");
            strBuilder.Append(" AND J.MBL_MAWB_FK=M.MAWB_EXP_TBL_PK(+) ");
            strBuilder.Append(" AND C.CUSTOMER_MST_PK(+) = J.SHIPPER_CUST_MST_FK ");
            strBuilder.Append(" AND J.BOOKING_MST_FK = B.BOOKING_MST_PK ");
            strBuilder.Append(" AND B.PORT_MST_POL_FK = P.PORT_MST_PK ");
            strBuilder.Append(" AND B.PORT_MST_POD_FK = P1.PORT_MST_PK ");
            strBuilder.Append(" AND H.JOB_CARD_AIR_EXP_FK = J.JOB_CARD_TRN_PK ");
            strBuilder.Append(" AND p.location_mst_fk =" + LocPk);
            if (strPaletteSize.Trim().Length > 0)
            {
                strBuilder.Append(" AND JTRN.JOB_CARD_TRN_FK=J.JOB_CARD_TRN_PK  ");
            }

            strSQL = null;
            strSQL += strBuilder.ToString() + strCondition.ToString();

            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by  H.HAWB_DATE DESC," + strColumnName;
            }

            if (!blnSortAscending.Equals("ASC") & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }

            strSQL += " )q) WHERE SR_NO  Between " + start + " and " + last;

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

        #endregion "Fetch All"

        #region "GetHAWBPk"

        /// <summary>
        /// Gets the hawb pk.
        /// </summary>
        /// <param name="MablRefNo">The mabl reference no.</param>
        /// <returns></returns>
        public object GetHAWBPk(string MablRefNo = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT HAWB.HAWB_EXP_TBL_PK");
            sb.Append("  FROM HAWB_EXP_TBL HAWB, MAWB_EXP_TBL MAWB, JOB_CARD_TRN JOB");
            sb.Append(" WHERE JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
            sb.Append("   And JOB.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK");
            sb.Append("  AND JOB.BUSINESS_TYPE = 1 ");
            sb.Append("   And MAWB.MAWB_REF_NO = '" + MablRefNo + "'");
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

        #endregion "GetHAWBPk"

        #region "Fetch HAWB Details"

        /// <summary>
        /// Fetches the export pre alert air.
        /// </summary>
        /// <param name="strHAWBPK">The string hawbpk.</param>
        /// <param name="strDate">The string date.</param>
        /// <param name="strPaletteSize">Size of the string palette.</param>
        /// <returns></returns>
        public DataSet FetchExportPreAlertAir(string strHAWBPK, string strDate, string strPaletteSize = "")
        {
            DataSet expPreAlertsDS = new DataSet("ExportPreAlertsAir");
            WorkFlow objwf = new WorkFlow();
            StringBuilder strBuilder = new StringBuilder();

            try
            {
                strBuilder.Remove(0, strBuilder.Length);
                MakeFileHeaderString(strBuilder, strHAWBPK);
                OracleDataAdapter PreAlertsAirDA = new OracleDataAdapter(strBuilder.ToString(), objwf.MyConnection);
                PreAlertsAirDA.Fill(expPreAlertsDS, "PreAlertsAir");
                var _with1 = expPreAlertsDS.Tables["PreAlertsAir"];
                _with1.Columns.Add("FILENAME");
                _with1.Columns.Add("TYPE");
                _with1.Columns.Add("DATE_OF_CREATION");
                _with1.Rows[0]["FILENAME"] = "EXPORTPREALERTSAIR";
                _with1.Rows[0]["TYPE"] = "XML";
                _with1.Rows[0]["DATE_OF_CREATION"] = strDate;

                strBuilder.Remove(0, strBuilder.Length);
                MakeAirlineFlightString(strBuilder, strHAWBPK);
                OracleDataAdapter AFDetailsDA = new OracleDataAdapter(strBuilder.ToString(), objwf.MyConnection);
                AFDetailsDA.Fill(expPreAlertsDS, "AirlineFlight");

                strBuilder.Remove(0, strBuilder.Length);
                MakePortPairString(strBuilder, strHAWBPK);
                OracleDataAdapter PortPairDetailsDA = new OracleDataAdapter(strBuilder.ToString(), objwf.MyConnection);
                PortPairDetailsDA.Fill(expPreAlertsDS, "PortPair");

                strBuilder.Remove(0, strBuilder.Length);
                MakeGeneralString(strBuilder, strHAWBPK);
                OracleDataAdapter HAWBDetailsDA = new OracleDataAdapter(strBuilder.ToString(), objwf.MyConnection);
                HAWBDetailsDA.Fill(expPreAlertsDS, "HAWB");

                strBuilder.Remove(0, strBuilder.Length);
                MakeContainerString(strBuilder, strHAWBPK, strPaletteSize);
                OracleDataAdapter ContainerDetailsDA = new OracleDataAdapter(strBuilder.ToString(), objwf.MyConnection);
                ContainerDetailsDA.Fill(expPreAlertsDS, "Palette");

                strBuilder.Remove(0, strBuilder.Length);
                MakeFreightString(strBuilder, strHAWBPK, strPaletteSize);
                OracleDataAdapter FreightDetailsDA = new OracleDataAdapter(strBuilder.ToString(), objwf.MyConnection);
                FreightDetailsDA.Fill(expPreAlertsDS, "FreightDetails");

                strBuilder.Remove(0, strBuilder.Length);
                MakePortsString(strBuilder, strHAWBPK);
                OracleDataAdapter TSPortsDetailsDA = new OracleDataAdapter(strBuilder.ToString(), objwf.MyConnection);
                TSPortsDetailsDA.Fill(expPreAlertsDS, "TSPortsDetails");
                //'
                strBuilder.Remove(0, strBuilder.Length);
                subMakeOtherString(strBuilder, strHAWBPK);
                OracleDataAdapter TSOtherDetailsDA = new OracleDataAdapter(strBuilder.ToString(), objwf.MyConnection);
                TSOtherDetailsDA.Fill(expPreAlertsDS, "Other_Details");
                expPreAlertsDS.Tables["Other_Details"].Columns["SALES_EXEC_FK"].ColumnMapping = MappingType.Hidden;

                if (expPreAlertsDS.Tables.Count > 0)
                {
                    DataRelation relAirlineFlight_PortPair = new DataRelation("relAirlineFlightPortPair", new DataColumn[] {
                        expPreAlertsDS.Tables["AirlineFlight"].Columns["AIRLINE_ID"],
                        expPreAlertsDS.Tables["AirlineFlight"].Columns["FLIGHT_NO"]
                    }, new DataColumn[] {
                        expPreAlertsDS.Tables["PortPair"].Columns["AIRLINE_ID"],
                        expPreAlertsDS.Tables["PortPair"].Columns["FLIGHT_NO"]
                    });
                    DataRelation relPortPair_HAWB = new DataRelation("relPortPairHAWB", new DataColumn[] {
                        expPreAlertsDS.Tables["PortPair"].Columns["AIRLINE_ID"],
                        expPreAlertsDS.Tables["PortPair"].Columns["FLIGHT_NO"],
                        expPreAlertsDS.Tables["PortPair"].Columns["AOO_ID"],
                        expPreAlertsDS.Tables["PortPair"].Columns["AOD_ID"]
                    }, new DataColumn[] {
                        expPreAlertsDS.Tables["HAWB"].Columns["AIRLINE_ID"],
                        expPreAlertsDS.Tables["HAWB"].Columns["FLIGHT_NO"],
                        expPreAlertsDS.Tables["HAWB"].Columns["AOO_ID"],
                        expPreAlertsDS.Tables["HAWB"].Columns["AOD_ID"]
                    });
                    DataRelation relHAWB_Palette = new DataRelation("relHAWBPalette", new DataColumn[] {
                        expPreAlertsDS.Tables["HAWB"].Columns["HAWB_REF_NO"],
                        expPreAlertsDS.Tables["HAWB"].Columns["JOBCARD_REF_NO"]
                    }, new DataColumn[] {
                        expPreAlertsDS.Tables["Palette"].Columns["HAWB_REF_NO"],
                        expPreAlertsDS.Tables["Palette"].Columns["JOBCARD_REF_NO"]
                    });
                    DataRelation relHAWB_Freight = new DataRelation("relHAWBFreight", new DataColumn[] {
                        expPreAlertsDS.Tables["HAWB"].Columns["HAWB_REF_NO"],
                        expPreAlertsDS.Tables["HAWB"].Columns["JOBCARD_REF_NO"]
                    }, new DataColumn[] {
                        expPreAlertsDS.Tables["FreightDetails"].Columns["HAWB_REF_NO"],
                        expPreAlertsDS.Tables["FreightDetails"].Columns["JOBCARD_REF_NO"]
                    });
                    DataRelation relHAWB_TSPorts = new DataRelation("relHAWBTSPorts", new DataColumn[] {
                        expPreAlertsDS.Tables["HAWB"].Columns["HAWB_REF_NO"],
                        expPreAlertsDS.Tables["HAWB"].Columns["JOBCARD_REF_NO"]
                    }, new DataColumn[] {
                        expPreAlertsDS.Tables["TSPortsDetails"].Columns["HAWB_REF_NO"],
                        expPreAlertsDS.Tables["TSPortsDetails"].Columns["JOBCARD_REF_NO"]
                    });
                    DataRelation relHAWB_Others = new DataRelation("relBLOthers", new DataColumn[] { expPreAlertsDS.Tables["HAWB"].Columns["HAWB_REF_NO"] }, new DataColumn[] { expPreAlertsDS.Tables["Other_Details"].Columns["HAWB_REF_NO"] });

                    relAirlineFlight_PortPair.Nested = true;
                    relPortPair_HAWB.Nested = true;
                    relHAWB_Palette.Nested = true;
                    relHAWB_Freight.Nested = true;
                    relHAWB_TSPorts.Nested = true;
                    relHAWB_Others.Nested = true;

                    expPreAlertsDS.Relations.Add(relAirlineFlight_PortPair);
                    expPreAlertsDS.Relations.Add(relPortPair_HAWB);
                    expPreAlertsDS.Relations.Add(relHAWB_Palette);
                    expPreAlertsDS.Relations.Add(relHAWB_Freight);
                    expPreAlertsDS.Relations.Add(relHAWB_TSPorts);
                    expPreAlertsDS.Relations.Add(relHAWB_Others);
                }

                return expPreAlertsDS;
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

        #endregion "Fetch HAWB Details"

        #region "MakeFileHeaderString"

        /// <summary>
        /// Makes the file header string.
        /// </summary>
        /// <param name="strBuilder">The string builder.</param>
        /// <param name="strHAWBPK">The string hawbpk.</param>
        private void MakeFileHeaderString(StringBuilder strBuilder, string strHAWBPK)
        {
            try
            {
                strBuilder.Append(" SELECT (SELECT SUM(COUNT(DISTINCT JHDR.VOYAGE_FLIGHT_NO)) ");
                strBuilder.Append(" FROM JOB_CARD_TRN JHDR ");
                strBuilder.Append(" WHERE JHDR.HBL_HAWB_FK IN (" + strHAWBPK + ") ");
                strBuilder.Append(" AND JHDR.BUSINESS_TYPE = 1 ");
                strBuilder.Append(" GROUP BY JHDR.VOYAGE_FLIGHT_NO) AS NO_OF_AIRLINE_FLIGHTNO, ");
                strBuilder.Append(" (select count(jobn.HBL_HAWB_FK) ");
                strBuilder.Append(" from JOB_CARD_TRN jobn ");
                strBuilder.Append(" where jobn.HBL_HAWB_FK IN (" + strHAWBPK + ") AND jobn.BUSINESS_TYPE = 1 ) AS NO_OF_HAWB, ");
                strBuilder.Append(" (select SUM(count(jobc.job_trn_cont_pk)) ");
                strBuilder.Append(" from job_trn_cont jobc, JOB_CARD_TRN jhdr ");
                strBuilder.Append(" where(jobc.Job_Card_Trn_FK = jhdr.Job_Card_Trn_Pk) ");
                strBuilder.Append(" and jhdr.HBL_HAWB_FK IN (" + strHAWBPK + ")");
                strBuilder.Append(" AND jhdr.BUSINESS_TYPE = 1 ");
                strBuilder.Append(" GROUP BY JHDR.VOYAGE_FLIGHT_NO) AS NO_OF_PALETTES ");
                strBuilder.Append(" FROM DUAL ");
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

        #endregion "MakeFileHeaderString"

        #region "MakeAirlineFlightString"

        /// <summary>
        /// Makes the airline flight string.
        /// </summary>
        /// <param name="strBuilder">The string builder.</param>
        /// <param name="strHAWBPK">The string hawbpk.</param>
        private void MakeAirlineFlightString(StringBuilder strBuilder, string strHAWBPK)
        {
            try
            {
                strBuilder.Append(" Select DISTINCT ");
                strBuilder.Append(" AMT.AIRLINE_ID          AIRLINE_ID, ");
                strBuilder.Append(" HAWBHDR.FLIGHT_NO       FLIGHT_NO ");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" HAWB_EXP_TBL            HAWBHDR, ");
                strBuilder.Append(" JOB_CARD_TRN            JCHDR, ");
                strBuilder.Append(" BOOKING_MST_TBL         BHDR, ");
                strBuilder.Append(" AIRLINE_MST_TBL AMT ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append(" HAWBHDR.JOB_CARD_AIR_EXP_FK = JCHDR.JOB_CARD_TRN_PK ");
                strBuilder.Append(" AND JCHDR.BOOKING_MST_FK=BHDR.BOOKING_MST_PK ");
                strBuilder.Append(" AND BHDR.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+) ");
                strBuilder.Append(" AND JCHDR.BUSINESS_TYPE = 1 ");
                strBuilder.Append(" AND HAWBHDR.HAWB_EXP_TBL_PK IN (" + strHAWBPK + ")");
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

        #endregion "MakeAirlineFlightString"

        #region "MakePortPairString"

        /// <summary>
        /// Makes the port pair string.
        /// </summary>
        /// <param name="strBuilder">The string builder.</param>
        /// <param name="strHAWBPK">The string hawbpk.</param>
        private void MakePortPairString(StringBuilder strBuilder, string strHAWBPK)
        {
            try
            {
                strBuilder.Append(" Select DISTINCT ");
                strBuilder.Append(" POL.PORT_ID             AOO_ID, ");
                strBuilder.Append(" POL.port_name           AOO_NAME, ");
                strBuilder.Append(" POD.port_id             AOD_ID, ");
                strBuilder.Append(" pod.port_name           AOD_NAME, ");
                strBuilder.Append(" AMT.AIRLINE_ID          AIRLINE_ID, ");
                strBuilder.Append(" HAWBHDR.FLIGHT_NO       FLIGHT_NO ");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" HAWB_EXP_TBL            HAWBHDR, ");
                strBuilder.Append(" JOB_CARD_TRN            JCHDR, ");
                strBuilder.Append(" BOOKING_MST_TBL         BHDR, ");
                strBuilder.Append(" port_mst_tbl            POD, ");
                strBuilder.Append(" port_mst_tbl            POL, ");
                strBuilder.Append(" AIRLINE_MST_TBL AMT ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append(" HAWBHDR.JOB_CARD_AIR_EXP_FK = JCHDR.JOB_CARD_TRN_PK ");
                strBuilder.Append(" AND JCHDR.BOOKING_MST_FK=BHDR.BOOKING_MST_PK ");
                strBuilder.Append(" AND BHDR.PORT_MST_POL_FK= pol.port_mst_pk ");
                strBuilder.Append(" AND BHDR.PORT_MST_POD_FK= pod.port_mst_pk ");
                strBuilder.Append(" AND BHDR.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+) ");
                strBuilder.Append(" AND JCHDR.BUSINESS_TYPE = 1 ");
                strBuilder.Append(" AND HAWBHDR.HAWB_EXP_TBL_PK IN (" + strHAWBPK + ")");
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

        #endregion "MakePortPairString"

        #region "MakeGeneralString"

        /// <summary>
        /// Makes the general string.
        /// </summary>
        /// <param name="strBuilder">The string builder.</param>
        /// <param name="strHAWBPK">The string hawbpk.</param>
        private void MakeGeneralString(StringBuilder strBuilder, string strHAWBPK)
        {
            try
            {
                strBuilder.Append(" SELECT ");
                strBuilder.Append(" JCHDR.JOBCARD_REF_NO       JOBCARD_REF_NO, ");
                strBuilder.Append(" HAWBHDR.HAWB_REF_NO        HAWB_REF_NO, ");
                strBuilder.Append(" BHDR.BOOKING_REF_NO        BOOKING_REF_NO, ");
                strBuilder.Append(" MAWBHDR.MAWB_REF_NO        MAWB_REF_NO, ");
                strBuilder.Append(" cust.customer_name         CUSTOMER_NAME, ");
                strBuilder.Append(" col_place.place_name       COLLECTION_PLACE, ");
                strBuilder.Append(" pol.port_id                AOO_ID, ");
                strBuilder.Append(" pol.port_name              AOO_NAME, ");
                strBuilder.Append(" pod.port_id                AOD_ID, ");
                strBuilder.Append(" pod.port_name              AOD_NAME, ");
                strBuilder.Append(" del_place.place_name       DELIVERY_PLACE, ");
                strBuilder.Append(" DECODE(JCHDR.JOB_CARD_STATUS,'1','Open','2','Closed') JOB_CARD_STATUS, ");
                strBuilder.Append(" DECODE(HAWBHDR.HAWB_STATUS,'0','Not Released','1','Released') HAWB_STATUS, ");
                strBuilder.Append(" to_char(JCHDR.JOB_CARD_CLOSED_ON,'" + dateFormat + "')  JOB_CARD_CLOSED_ON, ");
                strBuilder.Append(" AMT.AIRLINE_ID             AIRLINE_ID, ");
                strBuilder.Append(" AMT.AIRLINE_NAME           AIRLINE_NAME, ");
                strBuilder.Append(" HAWBHDR.FLIGHT_NO          FLIGHT_NO, ");
                strBuilder.Append(" to_char(HAWBHDR.ETA_DATE,'" + dateFormat + "') ETA, ");
                strBuilder.Append(" to_char(HAWBHDR.ETD_DATE,'" + dateFormat + "') ETD, ");
                strBuilder.Append(" to_char(HAWBHDR.ARRIVAL_DATE, '" + dateFormat + "') ARRIVAL_DATE, ");
                strBuilder.Append(" to_char(HAWBHDR.DEPARTURE_DATE,'" + dateFormat + "') DEPARTURE_DATE, ");
                strBuilder.Append(" HAWBHDR.SEC_AIRLINE_NAME   SECOND_AIRLINE_NAME, ");
                strBuilder.Append(" HAWBHDR.SEC_FLIGHT_NO      SECOND_FLIGHTNO, ");
                strBuilder.Append(" to_char(HAWBHDR.SEC_ETA_DATE,'" + dateFormat + "') SECOND_ETA, ");
                strBuilder.Append(" to_char(HAWBHDR.SEC_ETD_DATE,'" + dateFormat + "') SECOND_ETD, ");
                strBuilder.Append(" shipper.customer_id        SHIPPER_ID, ");
                strBuilder.Append(" shipper.customer_name      SHIPPER_NAME, ");
                strBuilder.Append(" consignee.customer_id      CONSIGNEE_ID, ");
                strBuilder.Append(" consignee.customer_name    CONSIGNEE_NAME, ");
                strBuilder.Append(" notify1.customer_id        NOTIFY_PARTY1_ID, ");
                strBuilder.Append(" notify1.customer_name      NOTIFY_PARTY1_NAME, ");
                strBuilder.Append(" notify2.customer_id        NOTIFY_PARTY2_ID, ");
                strBuilder.Append(" notify2.customer_name      NOTIFY_PARTY2_NAME, ");
                strBuilder.Append(" cbagnt.agent_id            CB_AGENT_ID, ");
                strBuilder.Append(" cbagnt.agent_name          CB_AGENT_NAME, ");
                strBuilder.Append(" dpagnt.agent_id            DP_AGENT_ID, ");
                strBuilder.Append(" dpagnt.agent_name          DP_AGENT_NAME, ");
                strBuilder.Append(" clagnt.agent_id            CL_AGENT_ID, ");
                strBuilder.Append(" clagnt.agent_name          CL_AGENT_NAME, ");
                strBuilder.Append(" JCHDR.REMARKS              JC_REMARKS, ");
                strBuilder.Append(" to_char(JCHDR.JOBCARD_DATE,'" + dateFormat + "') JOBCARD_DATE, ");
                strBuilder.Append(" JCHDR.UCR_NO               UCR_NO, ");
                strBuilder.Append(" to_char(HAWBHDR.HAWB_DATE,'" + dateFormat + "') HAWB_DATE, ");
                strBuilder.Append(" DECODE(JCHDR.PYMT_TYPE,'1','PrePaid','2','Collect') PAYMENT_TYPE, ");
                strBuilder.Append(" JCHDR.INSURANCE_AMT        INSURANCE_AMOUNT, ");
                strBuilder.Append(" JCHDR.INSURANCE_CURRENCY   INSURANCE_CURRENCY, ");
                strBuilder.Append(" comm.commodity_group_desc  COMMODITY_GROUP_DESC, ");
                strBuilder.Append(" depot.transporter_id       DEPOT_ID, ");
                strBuilder.Append(" depot.transporter_name     DEPOT_NAME, ");
                strBuilder.Append(" carrier.transporter_id     CARRIER_ID, ");
                strBuilder.Append(" carrier.transporter_name   CARRIER_NAME, ");
                strBuilder.Append(" country.country_id         COUNTRY_ID, ");
                strBuilder.Append(" country.country_name       COUNTRY_NAME, ");
                strBuilder.Append(" JCHDR.DA_NUMBER DA_NUMBER, ");
                strBuilder.Append(" DECODE(JCHDR.LC_SHIPMENT,0,'NO',1,'YES')LC_SHIPMENT ");
                strBuilder.Append(" FROM ");

                strBuilder.Append(" HAWB_EXP_TBL            HAWBHDR, ");
                strBuilder.Append(" MAWB_EXP_TBL            MAWBHDR, ");
                strBuilder.Append(" JOB_CARD_TRN            JCHDR, ");
                strBuilder.Append(" BOOKING_MST_TBL         BHDR, ");
                strBuilder.Append(" port_mst_tbl            POD, ");
                strBuilder.Append(" port_mst_tbl            POL, ");
                strBuilder.Append(" customer_mst_tbl        cust, ");
                strBuilder.Append(" customer_mst_tbl        consignee, ");
                strBuilder.Append(" customer_mst_tbl        shipper, ");
                strBuilder.Append(" customer_mst_tbl        notify1, ");
                strBuilder.Append(" customer_mst_tbl        notify2, ");
                strBuilder.Append(" place_mst_tbl           col_place, ");
                strBuilder.Append(" place_mst_tbl           del_place, ");
                strBuilder.Append(" agent_mst_tbl           clagnt, ");
                strBuilder.Append(" agent_mst_tbl           dpagnt, ");
                strBuilder.Append(" agent_mst_tbl           cbagnt, ");
                strBuilder.Append(" commodity_group_mst_tbl comm, ");
                strBuilder.Append(" transporter_mst_tbl     depot, ");
                strBuilder.Append(" transporter_mst_tbl     carrier, ");
                strBuilder.Append(" AIRLINE_MST_TBL         AMT, ");
                strBuilder.Append(" country_mst_tbl country ");

                strBuilder.Append(" WHERE ");

                strBuilder.Append(" HAWBHDR.JOB_CARD_AIR_EXP_FK = JCHDR.JOB_CARD_TRN_PK ");
                strBuilder.Append(" AND JCHDR.BOOKING_MST_FK=BHDR.BOOKING_MST_PK ");
                strBuilder.Append(" AND HAWBHDR.MAWB_EXP_TBL_FK=MAWBHDR.MAWB_EXP_TBL_PK(+) ");
                strBuilder.Append(" AND BHDR.PORT_MST_POL_FK= pol.port_mst_pk ");
                strBuilder.Append(" AND BHDR.PORT_MST_POD_FK= pod.port_mst_pk ");
                strBuilder.Append(" AND BHDR.COL_PLACE_MST_FK= col_place.place_pk(+) ");
                strBuilder.Append(" AND BHDR.DEL_PLACE_MST_FK= del_place.place_pk(+) ");
                strBuilder.Append(" AND BHDR.CUST_CUSTOMER_MST_FK= cust.customer_mst_pk(+) ");
                strBuilder.Append(" AND BHDR.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+) ");
                strBuilder.Append(" AND HAWBHDR.SHIPPER_CUST_MST_FK = shipper.customer_mst_pk(+) ");
                strBuilder.Append(" AND HAWBHDR.CONSIGNEE_CUST_MST_FK = consignee.customer_mst_pk(+) ");
                strBuilder.Append(" AND HAWBHDR.NOTIFY1_CUST_MST_FK = notify1.customer_mst_pk(+) ");
                strBuilder.Append(" AND HAWBHDR.NOTIFY2_CUST_MST_FK = notify2.customer_mst_pk(+) ");
                strBuilder.Append(" AND HAWBHDR.CL_AGENT_MST_FK= clagnt.agent_mst_pk(+) ");
                strBuilder.Append(" AND HAWBHDR.CB_AGENT_MST_FK= cbagnt.agent_mst_pk(+) ");
                strBuilder.Append(" AND HAWBHDR.DP_AGENT_MST_FK= dpagnt.agent_mst_pk(+) ");
                strBuilder.Append(" AND JCHDR.COMMODITY_GROUP_FK = comm.commodity_group_pk(+) ");
                strBuilder.Append(" AND JCHDR.TRANSPORTER_DEPOT_FK = depot.transporter_mst_pk(+) ");
                strBuilder.Append(" AND JCHDR.TRANSPORTER_CARRIER_FK = carrier.transporter_mst_pk(+) ");
                strBuilder.Append(" AND JCHDR.COUNTRY_ORIGIN_FK = country.country_mst_pk(+) ");
                strBuilder.Append(" AND JCHDR.BUSINESS_TYPE = 1 ");
                strBuilder.Append(" AND HAWBHDR.HAWB_EXP_TBL_PK IN (" + strHAWBPK + ")");
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

        #endregion "MakeGeneralString"

        #region "MakeGeneralString"

        /// <summary>
        /// Makes the container string.
        /// </summary>
        /// <param name="strbuilder">The strbuilder.</param>
        /// <param name="strHAWBPK">The string hawbpk.</param>
        /// <param name="strPaletteSize">Size of the string palette.</param>
        private void MakeContainerString(StringBuilder strbuilder, string strHAWBPK, string strPaletteSize = "")
        {
            try
            {
                strbuilder.Append(" SELECT HAWBHDR.HAWB_REF_NO  HAWB_REF_NO, ");
                strbuilder.Append(" JCHDR.JOBCARD_REF_NO        JOBCARD_REF_NO, ");
                strbuilder.Append(" JCTRN.PALETTE_SIZE          PALETTE_SIZE, ");
                strbuilder.Append(" JCTRN.VOLUME_IN_CBM         VOLUME_IN_CBM, ");
                strbuilder.Append(" JCTRN.GROSS_WEIGHT          GROSS_WEIGHT, ");
                strbuilder.Append(" JCTRN.CHARGEABLE_WEIGHT     CHARGEABLE_WEIGHT, ");
                strbuilder.Append(" PACK.PACK_TYPE_ID           PACK_ID, ");
                strbuilder.Append(" JCTRN.PACK_COUNT            PACK_COUNT, ");
                strbuilder.Append(" COMM.COMMODITY_NAME         COMMODITY, ");
                strbuilder.Append(" to_char(JCTRN.LOAD_DATE, '" + dateFormat + "') LOAD_DATE ");

                strbuilder.Append(" FROM ");

                strbuilder.Append(" HAWB_EXP_TBL                HAWBHDR, ");
                strbuilder.Append(" JOB_CARD_TRN                JCHDR, ");
                strbuilder.Append(" job_trn_cont                JCTRN, ");
                strbuilder.Append(" pack_type_mst_tbl           pack, ");
                strbuilder.Append(" commodity_mst_tbl           comm, ");
                strbuilder.Append(" BOOKING_MST_TBL             BAT, ");
                strbuilder.Append(" port_mst_tbl                pod, ");
                strbuilder.Append(" port_mst_tbl                pol ");

                strbuilder.Append(" WHERE ");

                strbuilder.Append(" HAWBHDR.JOB_CARD_AIR_EXP_FK = JCHDR.JOB_CARD_TRN_PK ");
                strbuilder.Append(" AND JCTRN.JOB_CARD_TRN_FK=JCHDR.JOB_CARD_TRN_PK ");
                strbuilder.Append(" AND JCTRN.PACK_TYPE_MST_FK = PACK.PACK_TYPE_MST_PK(+) ");
                strbuilder.Append(" AND JCTRN.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK(+) ");
                strbuilder.Append(" AND JCHDR.BOOKING_MST_FK=BAT.BOOKING_MST_PK ");
                strbuilder.Append(" AND BAT.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                strbuilder.Append(" AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                strbuilder.Append(" AND JCHDR.BUSINESS_TYPE = 1 ");
                strbuilder.Append(" AND HAWBHDR.HAWB_EXP_TBL_PK IN (" + strHAWBPK + ")");
                if (strPaletteSize.Length > 0)
                {
                    strbuilder.Append(" AND JCTRN.PALETTE_SIZE LIKE '" + strPaletteSize.ToUpper().Replace("'", "''") + "%'");
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

        #endregion "MakeGeneralString"

        #region "MakeFreightString"

        /// <summary>
        /// Makes the freight string.
        /// </summary>
        /// <param name="strbuilder">The strbuilder.</param>
        /// <param name="strHAWBPK">The string hawbpk.</param>
        /// <param name="strPaletteSize">Size of the string palette.</param>
        private void MakeFreightString(StringBuilder strbuilder, string strHAWBPK, string strPaletteSize = "")
        {
            try
            {
                strbuilder.Append(" SELECT HAWBHDR.HAWB_REF_NO  HAWB_REF_NO, ");
                strbuilder.Append(" JCHDR.JOBCARD_REF_NO        JOBCARD_REF_NO, ");
                strbuilder.Append(" FEMT.FREIGHT_ELEMENT_ID     FREIGHT_ELEMENT_ID, ");
                strbuilder.Append(" FEMT.FREIGHT_ELEMENT_NAME   FREIGHT_ELEMENT_NAME, ");
                strbuilder.Append(" DECODE(JCFRT.BASIS,'1','%','2','Flat','3','Kgs') BASIS, ");
                strbuilder.Append(" JCFRT.QUANTITY              QUANTITY, ");
                strbuilder.Append(" DECODE(JCFRT.FREIGHT_TYPE, 1, 'PrePaid', 2, 'Collect') FREIGHT_TYPE, ");
                strbuilder.Append(" JCFRT.FREIGHT_AMT           FREIGHT_AMT, ");
                strbuilder.Append(" CTMT.CURRENCY_ID            CURRENCY_ID, ");
                strbuilder.Append(" JCFRT.EXCHANGE_RATE         ROE ");

                strbuilder.Append(" FROM ");

                strbuilder.Append(" HAWB_EXP_TBL HAWBHDR, ");
                strbuilder.Append(" JOB_CARD_TRN                JCHDR, ");
                strbuilder.Append(" JOB_TRN_FD                  JCFRT, ");
                strbuilder.Append(" CURRENCY_TYPE_MST_TBL       CTMT, ");
                strbuilder.Append(" freight_element_mst_tbl     FEMT, ");
                strbuilder.Append(" BOOKING_MST_TBL             BAT, ");
                strbuilder.Append(" port_mst_tbl                pol, ");
                strbuilder.Append(" port_mst_tbl pod ");

                strbuilder.Append(" WHERE ");

                strbuilder.Append(" HAWBHDR.JOB_CARD_AIR_EXP_FK = JCHDR.JOB_CARD_TRN_PK ");
                strbuilder.Append(" AND JCHDR.BOOKING_MST_FK = BAT.BOOKING_MST_PK ");
                strbuilder.Append(" AND JCFRT.JOB_CARD_TRN_FK = JCHDR.JOB_CARD_TRN_PK ");
                strbuilder.Append(" AND JCFRT.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK(+) ");
                strbuilder.Append(" AND JCFRT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+) ");
                strbuilder.Append(" AND BAT.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                strbuilder.Append(" AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                strbuilder.Append(" AND JCHDR.BUSINESS_TYPE = 1 ");
                strbuilder.Append(" AND HAWBHDR.HAWB_EXP_TBL_PK IN (" + strHAWBPK + ")");
                //If strPaletteSize.Length > 0 Then
                //    strbuilder.Append(" AND JCTRN.PALETTE_SIZE LIKE '" & strPaletteSize.ToUpper.Replace("'", "''") & "%'")
                //End If
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

        #endregion "MakeFreightString"

        #region "MakePortsString"

        /// <summary>
        /// Makes the ports string.
        /// </summary>
        /// <param name="strbuilder">The strbuilder.</param>
        /// <param name="strHAWBPK">The string hawbpk.</param>
        /// <param name="strPaletteSize">Size of the string palette.</param>
        private void MakePortsString(StringBuilder strbuilder, string strHAWBPK, string strPaletteSize = "")
        {
            try
            {
                strbuilder.Append(" SELECT HAWBHDR.HAWB_REF_NO  HAWB_REF_NO, ");
                strbuilder.Append(" JCHDR.JOBCARD_REF_NO        JOBCARD_REF_NO, ");
                strbuilder.Append(" JCTP.TRANSHIPMENT_NO        TRANSHIPMENT_NO, ");
                strbuilder.Append(" PORT.PORT_ID                PORT_ID, ");
                strbuilder.Append(" PORT.PORT_NAME              PORT_NAME, ");
                strbuilder.Append(" AMT.AIRLINE_ID              AIRLINE_ID, ");
                strbuilder.Append(" AMT.AIRLINE_NAME            AIRLINE_NAME, ");
                strbuilder.Append(" JCHDR.VOYAGE_FLIGHT_NO      FLIGHT_NO, ");
                strbuilder.Append(" TO_CHAR(JCTP.eta_date, '" + dateFormat + "') ETA_DATE, ");
                strbuilder.Append(" TO_CHAR(JCTP.etd_date, '" + dateFormat + "') ETD_DATE ");

                strbuilder.Append(" FROM ");

                strbuilder.Append(" HAWB_EXP_TBL                HAWBHDR, ");
                strbuilder.Append(" JOB_CARD_TRN                JCHDR, ");
                strbuilder.Append(" JOB_TRN_AIR_EXP_TP          JCTP, ");
                strbuilder.Append(" port_mst_tbl                port, ");
                strbuilder.Append(" BOOKING_MST_TBL             BAT, ");
                strbuilder.Append(" AIRLINE_MST_TBL             AMT ");

                strbuilder.Append(" WHERE ");

                strbuilder.Append(" HAWBHDR.JOB_CARD_AIR_EXP_FK = JCHDR.JOB_CARD_TRN_PK ");
                strbuilder.Append(" AND JCTP.JOB_CARD_AIR_EXP_FK = JCHDR.JOB_CARD_TRN_PK ");
                strbuilder.Append(" AND JCTP.PORT_MST_FK = PORT.PORT_MST_PK ");
                strbuilder.Append(" AND JCHDR.BOOKING_MST_FK = BAT.BOOKING_MST_PK ");
                strbuilder.Append(" AND BAT.CARRIER_MST_FK = AMT.AIRLINE_MST_PK ");
                strbuilder.Append(" AND JCHDR.BUSINESS_TYPE = 1 ");
                strbuilder.Append(" AND HAWBHDR.HAWB_EXP_TBL_PK IN (" + strHAWBPK + ")");
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

        #endregion "MakePortsString"

        #region "subMakeOtherString"

        /// <summary>
        /// Subs the make other string.
        /// </summary>
        /// <param name="strSql">The string SQL.</param>
        /// <param name="strHAWBPK">The string hawbpk.</param>
        private void subMakeOtherString(StringBuilder strSql, string strHAWBPK)
        {
            try
            {
                strSql.Append(" SELECT HAWB.HAWB_REF_NO  HAWB_REF_NO,");
                strSql.Append("       HAWB.LC_NUMBER LC_NR,");
                strSql.Append("       HAWB.LC_DATE LC_Date,");
                strSql.Append("       HAWB.LC_EXPIRES_ON LC_Expires_On,");
                strSql.Append("       HAWB.CONSIGNEE_ADDRESS Consignemnt_Bank,");
                strSql.Append("       HAWB.LETTER_OF_CREDIT Remarks,");
                strSql.Append("       NVL(EMP.EMPLOYEE_MST_PK,NVL(SHP_SE.EMPLOYEE_MST_PK,0)) SALES_EXEC_FK,");
                strSql.Append("       NVL(EMP.EMPLOYEE_NAME,NVL(SHP_SE.EMPLOYEE_NAME,' ')) SALES_EXECUTIVE ");
                strSql.Append("  FROM JOB_CARD_TRN JOB, HAWB_EXP_TBL HAWB,");
                strSql.Append("    customer_mst_tbl shipper, ");
                strSql.Append("    EMPLOYEE_MST_TBL        EMP, ");
                strSql.Append("    EMPLOYEE_MST_TBL        SHP_SE ");
                //SHIPPER SALES PERSON
                strSql.Append(" WHERE JOB.JOB_CARD_TRN_PK = HAWB.JOB_CARD_AIR_EXP_FK");
                strSql.Append("   AND HAWB.HAWB_EXP_TBL_PK IN (" + strHAWBPK + ")");
                strSql.Append("   AND JOB.SHIPPER_CUST_MST_FK =  shipper.customer_mst_pk(+) ");
                strSql.Append("   AND shipper.REP_EMP_MST_FK=SHP_SE.EMPLOYEE_MST_PK(+) ");
                strSql.Append("   AND JOB.BUSINESS_TYPE = 1 ");
                strSql.Append("   AND JOB.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) ");
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

        #endregion "subMakeOtherString"

        #region "Enhance Search"

        /// <summary>
        /// Fetches the flight no.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchFlightNo(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strReq = null;
            string strairline = "";
            string strSearch = "";
            string strLoc = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            //Look up L or E
            strSearch = Convert.ToString(arr.GetValue(1));
            //Search Value
            strLoc = Convert.ToString(arr.GetValue(2));
            //Location not used
            if (arr.Length > 3)
                strairline = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_DOCS_PKG.GET_FLIGHTNO_HAWB";
                var _with2 = SCM.Parameters;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("SEARCH_IN", ifDBNull(strSearch)).Direction = ParameterDirection.Input;
                _with2.Add("AIRLINE_IN", ifDBNull(strairline)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", ifDBNull(strLoc)).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        /// Fetches the size of the palette.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchPaletteSize(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strReq = null;
            string strSearch = "";
            string strLoc = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            //Look up L or E
            strSearch = Convert.ToString(arr.GetValue(1));
            //Search Value
            strLoc = Convert.ToString(arr.GetValue(2));
            //Location not used

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_DOCS_PKG.GET_PALETTESIZE_JC";
                var _with3 = SCM.Parameters;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("SEARCH_IN", ifDBNull(strSearch)).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion "Enhance Search"

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

        #region " Update EDI STATUS "

        /// <summary>
        /// Updates the ed istatus.
        /// </summary>
        /// <param name="strHAWBPK">The string hawbpk.</param>
        public void updateEDIstatus(string strHAWBPK)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            try
            {
                sb.Append("UPDATE BOOKING_MST_TBL B");
                sb.Append("   SET B.EDI_STATUS = 1");
                sb.Append(" WHERE B.BOOKING_MST_PK IN");
                sb.Append("       (SELECT J.BOOKING_MST_FK");
                sb.Append("          FROM JOB_CARD_TRN J, HAWB_EXP_TBL H");
                sb.Append("         WHERE J.HBL_HAWB_FK = H.HAWB_EXP_TBL_PK");
                sb.Append("           AND J.BUSINESS_TYPE = 1 ");
                sb.Append("           AND H.HAWB_EXP_TBL_PK IN (" + strHAWBPK + "))");
                sb.Append("");
                objWF.ExecuteCommands(sb.ToString());
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

        #endregion " Update EDI STATUS "
    }
}