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
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsExportPreAlertsSEA : CommonFeatures
    {
        #region "Fetch All"

        /// <summary>
        /// Funs the fetch all.
        /// </summary>
        /// <param name="lngVoyagePK">The LNG voyage pk.</param>
        /// <param name="strVoyageNo">The string voyage no.</param>
        /// <param name="strContainerNo">The string container no.</param>
        /// <param name="lngPOLPK">The LNG polpk.</param>
        /// <param name="lngPODPK">The LNG podpk.</param>
        /// <param name="strHBLRefNo">The string HBL reference no.</param>
        /// <param name="strMBLRefNo">The string MBL reference no.</param>
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
        public DataSet funFetchAll(long lngVoyagePK = 0, string strVoyageNo = "", string strContainerNo = "", long lngPOLPK = 0, long lngPODPK = 0, string strHBLRefNo = "", string strMBLRefNo = "", string strFromDate = "", string strToDate = "", long lngDPAgentPK = 0,
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
            if (!(lngVoyagePK == 0))
            {
                strCondition.Append("  AND  h.voyage_trn_fk= " + lngVoyagePK);
            }
            if (strVoyageNo.Trim().Length > 0)
            {
                strCondition.Append("  AND UPPER(VTRN.VOYAGE) LIKE '" + strVoyageNo.ToUpper().Replace("'", "''") + "%'");
            }
            if (strContainerNo.Trim().Length > 0)
            {
                strCondition.Append("  AND UPPER(JTRN.CONTAINER_NUMBER) LIKE '" + strContainerNo.ToUpper().Replace("'", "''") + "%'");
            }
            if (!(lngPOLPK == 0))
            {
                strCondition.Append("  AND B.PORT_MST_POL_FK= " + lngPOLPK);
            }
            if (!(lngPODPK == 0))
            {
                strCondition.Append("  AND B.PORT_MST_POD_FK= " + lngPODPK);
            }
            if (strHBLRefNo.Trim().Length > 0)
            {
                strCondition.Append("  AND upper(H.hbl_ref_no) like '" + strHBLRefNo.ToUpper().Replace("'", "''") + "%'");
            }
            if (strMBLRefNo.Trim().Length > 0)
            {
                strCondition.Append("  AND upper(M.MBL_REF_NO) like '" + strMBLRefNo.ToUpper().Replace("'", "''") + "%'");
            }
            if (!(lngDPAgentPK == 0))
            {
                strCondition.Append("  AND H.DP_AGENT_MST_FK= " + lngDPAgentPK);
            }
            if (strFromDate.Trim().Length > 0 & strToDate.Trim().Length > 0)
            {
                strCondition.Append(" AND H.HBL_DATE between to_date('" + System.String.Format("{0:" + dateFormat + "}", strFromDate) + "',dateformat)  and to_date('" + System.String.Format("{0:" + dateFormat + "}", strToDate) + "',dateformat)");
            }
            else if (strFromDate.Trim().Length > 0)
            {
                strCondition.Append(" AND to_date('" + System.String.Format("{0:" + dateFormat + "}", strFromDate) + "',dateformat) <= H.HBL_DATE ");
            }
            else if (strToDate.Trim().Length > 0)
            {
                strCondition.Append(" AND to_date('" + System.String.Format("{0:" + dateFormat + "}", strToDate) + "',dateformat) >= H.HBL_DATE ");
            }

            if (coload == true)
            {
                strCondition.Append("  AND J.CL_AGENT_MST_FK IS NOT NULL ");
            }
            strCondition.Append("  AND J.BUSINESS_TYPE = 2 ");
            strCondition.Append("  AND J.PROCESS_TYPE = 1 ");

            //strBuilder.Append(" SELECT DISTINCT COUNT(*) " & vbCrLf)
            //strBuilder.Append(" FROM HBL_EXP_TBL H,JOB_CARD_TRN J, " & vbCrLf)
            //strBuilder.Append(" BOOKING_MST_TBL B, ")
            //strBuilder.Append(" MBL_EXP_TBL M, ")
            //strBuilder.Append(" VESSEL_VOYAGE_TBL V, VESSEL_VOYAGE_TRN VTRN ")
            //If strContainerNo.Trim.Length > 0 Then
            //    strBuilder.Append(" , JOB_TRN_CONT JTRN " & vbCrLf)
            //End If
            //strBuilder.Append(" WHERE" & vbCrLf)
            //strBuilder.Append(" H.JOB_CARD_SEA_EXP_FK = J.JOB_CARD_TRN_PK ")
            //strBuilder.Append(" AND J.BOOKING_MST_FK=B.BOOKING_MST_PK")
            //strBuilder.Append(" AND J.MBL_MAWB_FK=M.MBL_EXP_TBL_PK(+)")
            //strBuilder.Append(" AND J.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK")
            //strBuilder.Append(" AND VTRN.VESSEL_VOYAGE_TBL_FK=V.VESSEL_VOYAGE_TBL_PK")
            //If strContainerNo.Trim.Length > 0 Then
            //    strBuilder.Append(" AND JTRN.JOB_CARD_TRN_FK=J.JOB_CARD_TRN_PK  ")
            //End If

            //strSQL &= vbCrLf & strBuilder.ToString() & strCondition.ToString()

            strBuilder.Append(" SELECT V.VESSEL_ID, ");
            strBuilder.Append(" VTRN.VOYAGE, ");
            strBuilder.Append(" POL.PORT_ID AS POL, ");
            strBuilder.Append(" POD.PORT_ID AS POD, ");
            strBuilder.Append(" H.HBL_EXP_TBL_PK, ");
            strBuilder.Append(" H.HBL_REF_NO, ");
            strBuilder.Append(" H.HBL_DATE, ");
            strBuilder.Append(" M.MBL_REF_NO, ");
            strBuilder.Append(" AMT.AGENT_NAME AGENT_ID, ");
            strBuilder.Append(" C.CUSTOMER_ID, ");
            strBuilder.Append(" DECODE(H.HBL_STATUS,'0','Draft','1','Confirmed','2','Released') STATUS, ");
            strBuilder.Append(" DECODE(B.EDI_STATUS,'0','NG','1','TM') EDI_STATUS, ");
            strBuilder.Append(" 'false' AS SEL ");
            strBuilder.Append(" FROM ");
            strBuilder.Append(" HBL_EXP_TBL          H, ");
            strBuilder.Append(" JOB_CARD_TRN         J, ");
            strBuilder.Append(" MBL_EXP_TBL          M, ");
            strBuilder.Append(" CUSTOMER_MST_TBL     C, ");
            strBuilder.Append(" BOOKING_MST_TBL      B, ");
            strBuilder.Append(" PORT_MST_TBL         POL, ");
            strBuilder.Append(" VESSEL_VOYAGE_TBL    V, ");
            strBuilder.Append(" VESSEL_VOYAGE_TRN    VTRN, ");
            strBuilder.Append(" AGENT_MST_TBL        AMT, ");
            strBuilder.Append(" PORT_MST_TBL         POD, ");
            strBuilder.Append(" OPERATOR_MST_TBL OMT ");
            if (strContainerNo.Trim().Length > 0)
            {
                strBuilder.Append(" , JOB_TRN_CONT JTRN ");
            }
            strBuilder.Append(" WHERE H.JOB_CARD_SEA_EXP_FK = J.JOB_CARD_TRN_PK ");
            strBuilder.Append(" AND J.BOOKING_MST_FK=B.BOOKING_MST_PK ");
            strBuilder.Append(" AND J.MBL_MAWB_FK=M.MBL_EXP_TBL_PK(+) ");
            strBuilder.Append(" AND J.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK ");
            strBuilder.Append(" AND VTRN.VESSEL_VOYAGE_TBL_FK=V.VESSEL_VOYAGE_TBL_PK ");
            strBuilder.Append(" AND V.OPERATOR_MST_FK=OMT.OPERATOR_MST_PK ");
            strBuilder.Append(" AND J.DP_AGENT_MST_FK=AMT.AGENT_MST_PK(+) ");
            strBuilder.Append(" AND J.SHIPPER_CUST_MST_FK=C.CUSTOMER_MST_PK(+) ");
            strBuilder.Append(" AND B.PORT_MST_POL_FK=POL.PORT_MST_PK ");
            strBuilder.Append(" AND B.PORT_MST_POD_FK=POD.PORT_MST_PK ");
            strBuilder.Append(" AND POL.location_mst_fk =" + LocPk);
            if (strContainerNo.Trim().Length > 0)
            {
                strBuilder.Append(" AND JTRN.JOB_CARD_TRN_FK=J.JOB_CARD_TRN_PK  ");
            }
            strBuilder.Append(strCondition);

            strSQL = " select count(*) from (";
            strSQL += strBuilder.ToString() + ")";

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

            strSQL += "order by " + strColumnName;

            strBuilder.Append(" order by NVL(H.HBL_DATE,'01/01/0001') desc ");
            strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
            strSQL += strBuilder.ToString();
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

        #region "GetHBLPk"

        /// <summary>
        /// Gets the HBL pk.
        /// </summary>
        /// <param name="MblRefNo">The MBL reference no.</param>
        /// <returns></returns>
        public object GetHBLPk(string MblRefNo = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT HBL.HBL_EXP_TBL_PK");
            sb.Append("  FROM HBL_EXP_TBL HBL, MBL_EXP_TBL MBL, JOB_CARD_TRN JOB");
            sb.Append(" WHERE JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
            sb.Append("   And JOB.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK");
            sb.Append("   AND JOB.BUSINESS_TYPE = 2 ");
            sb.Append("   And MBL.MBL_REF_NO = '" + MblRefNo + "'");
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

        #endregion "GetHBLPk"

        #region "getbookingpk"

        /// <summary>
        /// Getbookingpks the specified HBLPK.
        /// </summary>
        /// <param name="HBLPK">The HBLPK.</param>
        /// <returns></returns>
        public object getbookingpk(string HBLPK = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("select B.BOOKING_MST_PK BOOKING_SEA_PK");
            sb.Append("  from BOOKING_MST_TBL B, JOB_CARD_TRN J, HBL_EXP_TBL H");
            sb.Append(" where J.BOOKING_MST_FK = B.BOOKING_MST_PK");
            sb.Append("   AND H.JOB_CARD_SEA_EXP_FK = J.JOB_CARD_TRN_PK");
            sb.Append("   AND J.BUSINESS_TYPE = 2 ");
            sb.Append("   AND H.HBL_EXP_TBL_PK IN (" + HBLPK + ") ");
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

        #endregion "getbookingpk"

        #region "Fetch HBL Details"

        /// <summary>
        /// Fetches the export pre alert sea.
        /// </summary>
        /// <param name="strHBLPk">The string HBL pk.</param>
        /// <param name="strDate">The string date.</param>
        /// <param name="strCntNr">The string count nr.</param>
        /// <returns></returns>
        public DataSet FetchExportPreAlertSEA(string strHBLPk, string strDate, string strCntNr = "")
        {
            StringBuilder strSql = new StringBuilder();
            DataSet expPreAlertsDS = new DataSet("ExportPreAlertsSEA");
            WorkFlow objwf = new WorkFlow();
            StringBuilder strCondition = new StringBuilder();

            try
            {
                strSql.Remove(0, strSql.Length);
                MakeFileHeaderString(strSql, strHBLPk);
                OracleDataAdapter PreAlertsSeaDA = new OracleDataAdapter(strSql.ToString(), objwf.MyConnection);
                PreAlertsSeaDA.Fill(expPreAlertsDS, "PreAlertsSea");
                var _with1 = expPreAlertsDS.Tables["PreAlertsSea"];
                _with1.Columns.Add("FILENAME");
                _with1.Columns.Add("TYPE");
                _with1.Columns.Add("DATE_OF_CREATION");
                _with1.Rows[0]["FILENAME"] = "EXPORTPREALERTSSEA";
                _with1.Rows[0]["TYPE"] = "XML";
                _with1.Rows[0]["DATE_OF_CREATION"] = strDate;

                strSql.Remove(0, strSql.ToString().Length);
                MakeVesselVoyageString(strSql, strHBLPk);
                OracleDataAdapter VVDetailsDA = new OracleDataAdapter(strSql.ToString(), objwf.MyConnection);

                strSql.Remove(0, strSql.Length);
                MakePortPairString(strSql, strHBLPk);
                OracleDataAdapter PortPairDetailsDA = new OracleDataAdapter(strSql.ToString(), objwf.MyConnection);

                strSql.Remove(0, strSql.ToString().Length);
                subMakeHeaderString(strSql, strHBLPk);
                OracleDataAdapter HBLDetailsDA = new OracleDataAdapter(strSql.ToString(), objwf.MyConnection);

                strSql.Remove(0, strSql.ToString().Length);
                subMakeContString(strSql, strHBLPk);
                OracleDataAdapter ContainerDetailsDA = new OracleDataAdapter(strSql.ToString(), objwf.MyConnection);

                strSql.Remove(0, strSql.ToString().Length);
                subMakeFrtString(strSql, strHBLPk);
                OracleDataAdapter FreightDetailsDA = new OracleDataAdapter(strSql.ToString(), objwf.MyConnection);

                strSql.Remove(0, strSql.ToString().Length);
                subMakeTPortString(strSql, strHBLPk);
                OracleDataAdapter TSPortsDetailsDA = new OracleDataAdapter(strSql.ToString(), objwf.MyConnection);
                //'
                strSql.Remove(0, strSql.ToString().Length);
                subMakeOtherString(strSql, strHBLPk);
                OracleDataAdapter TSOtherDetailsDA = new OracleDataAdapter(strSql.ToString(), objwf.MyConnection);
                //'
                VVDetailsDA.Fill(expPreAlertsDS, "VesselVoyage");
                PortPairDetailsDA.Fill(expPreAlertsDS, "PortPair");
                HBLDetailsDA.Fill(expPreAlertsDS, "HBL");
                ContainerDetailsDA.Fill(expPreAlertsDS, "Container");
                FreightDetailsDA.Fill(expPreAlertsDS, "FreightDetails");
                TSPortsDetailsDA.Fill(expPreAlertsDS, "TSPortsDetails");
                TSOtherDetailsDA.Fill(expPreAlertsDS, "Other_Details");
                expPreAlertsDS.Tables["Other_Details"].Columns["SALES_EXEC_FK"].ColumnMapping = MappingType.Hidden;
                //----------------------------------Set The Relation between data tables-------------------------------

                if (expPreAlertsDS.Tables.Count > 0)
                {
                    DataRelation relVesselVoyage_PortPair = new DataRelation("relVesselVoyagePortPair", new DataColumn[] {
                        expPreAlertsDS.Tables["VesselVoyage"].Columns["VESSEL_ID"],
                        expPreAlertsDS.Tables["VesselVoyage"].Columns["VOYAGE"]
                    }, new DataColumn[] {
                        expPreAlertsDS.Tables["PortPair"].Columns["VESSEL_ID"],
                        expPreAlertsDS.Tables["PortPair"].Columns["VOYAGE"]
                    });
                    DataRelation relPortPair_HBL = new DataRelation("relPortPairHBL", new DataColumn[] {
                        expPreAlertsDS.Tables["PortPair"].Columns["VESSEL_ID"],
                        expPreAlertsDS.Tables["PortPair"].Columns["VOYAGE"],
                        expPreAlertsDS.Tables["PortPair"].Columns["POL"],
                        expPreAlertsDS.Tables["PortPair"].Columns["POD"]
                    }, new DataColumn[] {
                        expPreAlertsDS.Tables["HBL"].Columns["VESSEL_ID"],
                        expPreAlertsDS.Tables["HBL"].Columns["VOYAGE"],
                        expPreAlertsDS.Tables["HBL"].Columns["POL"],
                        expPreAlertsDS.Tables["HBL"].Columns["POD"]
                    });
                    DataRelation relHBL_Container = new DataRelation("relBLContainer", new DataColumn[] { expPreAlertsDS.Tables["HBL"].Columns["HBL_REF_NO"] }, new DataColumn[] { expPreAlertsDS.Tables["Container"].Columns["HBL_REF_NO"] });
                    DataRelation relHBL_Freight = new DataRelation("relBLFreight", new DataColumn[] { expPreAlertsDS.Tables["HBL"].Columns["HBL_REF_NO"] }, new DataColumn[] { expPreAlertsDS.Tables["FreightDetails"].Columns["HBL_REF_NO"] });
                    DataRelation relHBL_TSPorts = new DataRelation("relBLTSPorts", new DataColumn[] { expPreAlertsDS.Tables["HBL"].Columns["HBL_REF_NO"] }, new DataColumn[] { expPreAlertsDS.Tables["TSPortsDetails"].Columns["HBL_REF_NO"] });
                    DataRelation relHBL_Others = new DataRelation("relBLOthers", new DataColumn[] { expPreAlertsDS.Tables["HBL"].Columns["HBL_REF_NO"] }, new DataColumn[] { expPreAlertsDS.Tables["Other_Details"].Columns["HBL_REF_NO"] });

                    relVesselVoyage_PortPair.Nested = true;
                    relPortPair_HBL.Nested = true;
                    relHBL_Container.Nested = true;
                    relHBL_Freight.Nested = true;
                    relHBL_TSPorts.Nested = true;
                    relHBL_Others.Nested = true;
                    //------------------------Add the relation to dataset---------------------------------------
                    expPreAlertsDS.Relations.Add(relVesselVoyage_PortPair);
                    expPreAlertsDS.Relations.Add(relPortPair_HBL);
                    expPreAlertsDS.Relations.Add(relHBL_Container);
                    expPreAlertsDS.Relations.Add(relHBL_Freight);
                    expPreAlertsDS.Relations.Add(relHBL_TSPorts);
                    expPreAlertsDS.Relations.Add(relHBL_Others);
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

        #endregion "Fetch HBL Details"

        #region "MakeFileHeaderString"

        /// <summary>
        /// Makes the file header string.
        /// </summary>
        /// <param name="strBuilder">The string builder.</param>
        /// <param name="strHBLPK">The string HBLPK.</param>
        private void MakeFileHeaderString(StringBuilder strBuilder, string strHBLPK)
        {
            try
            {
                strBuilder.Append(" SELECT (SELECT SUM(COUNT(DISTINCT VVVT.Voyage)) ");
                strBuilder.Append(" FROM JOB_CARD_TRN JHDR, ");
                strBuilder.Append(" HBL_EXP_TBL HET, ");
                strBuilder.Append(" VESSEL_VOYAGE_TBL VVT, ");
                strBuilder.Append(" VESSEL_VOYAGE_TRN VVVT ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append(" JHDR.HBL_HAWB_FK = HET.HBL_EXP_TBL_PK ");
                strBuilder.Append(" AND JHDR.VOYAGE_TRN_FK = VVVT.VOYAGE_TRN_PK AND JHDR.BUSINESS_TYPE = 2  ");
                strBuilder.Append(" AND VVVT.VESSEL_VOYAGE_TBL_FK=VVT.VESSEL_VOYAGE_TBL_PK ");
                strBuilder.Append(" AND JHDR.HBL_HAWB_FK IN (" + strHBLPK + ")");
                strBuilder.Append(" GROUP BY VVVT.VOYAGE) AS NO_OF_VESSEL_VOYAGE, ");
                strBuilder.Append(" (select count(jobn.HBL_HAWB_FK) ");
                strBuilder.Append(" from JOB_CARD_TRN jobn ");
                strBuilder.Append(" where jobn.HBL_HAWB_FK IN (" + strHBLPK + " ) AND jobn.BUSINESS_TYPE = 2 ) AS NO_OF_HBL, ");
                strBuilder.Append(" (select SUM(count(jobc.Job_Trn_Cont_Pk)) ");
                strBuilder.Append(" from JOB_TRN_CONT jobc, JOB_CARD_TRN jhdr ");
                strBuilder.Append(" where(jobc.Job_Card_Trn_Fk = jhdr.job_card_trn_pk) ");
                strBuilder.Append(" and jhdr.Hbl_Hawb_Fk IN (" + strHBLPK + ") AND jhdr.BUSINESS_TYPE = 2 ");
                strBuilder.Append(" GROUP BY JHDR.VOYAGE_TRN_FK) AS NO_OF_CONTAINERS");
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

        #region "MakeVesselVoyageString"

        /// <summary>
        /// Makes the vessel voyage string.
        /// </summary>
        /// <param name="strBuilder">The string builder.</param>
        /// <param name="strHBLPk">The string HBL pk.</param>
        private void MakeVesselVoyageString(StringBuilder strBuilder, string strHBLPk)
        {
            try
            {
                strBuilder.Append(" Select DISTINCT ");
                strBuilder.Append(" VVT.VESSEL_ID VESSEL_ID, ");
                strBuilder.Append(" VVVT.VOYAGE VOYAGE ");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" HBL_EXP_TBL          HBLHDR, ");
                strBuilder.Append(" JOB_CARD_TRN         JCHDR, ");
                strBuilder.Append(" VESSEL_VOYAGE_TBL    VVT, ");
                strBuilder.Append(" VESSEL_VOYAGE_TRN    VVVT ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append(" HBLHDR.JOB_CARD_SEA_EXP_FK=JCHDR.JOB_CARD_TRN_PK ");
                strBuilder.Append("  AND JCHDR.VOYAGE_TRN_FK = VVVT.VOYAGE_TRN_PK ");
                strBuilder.Append(" AND VVVT.VESSEL_VOYAGE_TBL_FK=VVT.VESSEL_VOYAGE_TBL_PK ");
                strBuilder.Append(" AND JCHDR.BUSINESS_TYPE = 2  ");
                strBuilder.Append(" AND HBLHDR.HBL_EXP_TBL_PK IN (" + strHBLPk + ")");
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

        #endregion "MakeVesselVoyageString"

        #region "MakeVesselVoyageString"

        /// <summary>
        /// Makes the port pair string.
        /// </summary>
        /// <param name="strBuilder">The string builder.</param>
        /// <param name="strHBLPk">The string HBL pk.</param>
        private void MakePortPairString(StringBuilder strBuilder, string strHBLPk)
        {
            try
            {
                strBuilder.Append(" Select DISTINCT ");
                strBuilder.Append(" POL.PORT_ID             POL, ");
                strBuilder.Append(" POL.port_name           POL_NAME, ");
                strBuilder.Append(" POD.port_id             POD, ");
                strBuilder.Append(" pod.port_name           POD_NAME, ");
                strBuilder.Append(" VVT.VESSEL_ID           VESSEL_ID, ");
                strBuilder.Append(" VVVT.VOYAGE VOYAGE ");
                strBuilder.Append(" FROM ");
                strBuilder.Append(" HBL_EXP_TBL          HBLHDR, ");
                strBuilder.Append(" JOB_CARD_TRN         JCHDR, ");
                strBuilder.Append(" BOOKING_MST_TBL      BHDR, ");
                strBuilder.Append(" PORT_MST_TBL         POL, ");
                strBuilder.Append(" PORT_MST_TBL         POD, ");
                strBuilder.Append(" VESSEL_VOYAGE_TBL    VVT, ");
                strBuilder.Append(" VESSEL_VOYAGE_TRN VVVT ");
                strBuilder.Append(" WHERE ");
                strBuilder.Append(" HBLHDR.JOB_CARD_SEA_EXP_FK = JCHDR.JOB_CARD_TRN_pK ");
                strBuilder.Append(" AND JCHDR.BOOKING_MST_FK= BHDR.BOOKING_MST_PK ");
                strBuilder.Append(" AND BHDR.PORT_MST_POL_FK= POL.PORT_MST_PK ");
                strBuilder.Append(" AND BHDR.PORT_MST_POD_FK= POD.PORT_MST_PK ");
                strBuilder.Append("  AND JCHDR.VOYAGE_TRN_FK = VVVT.VOYAGE_TRN_PK ");
                strBuilder.Append(" AND VVVT.VESSEL_VOYAGE_TBL_FK=VVT.VESSEL_VOYAGE_TBL_PK ");
                strBuilder.Append(" AND JCHDR.BUSINESS_TYPE = 2  ");
                strBuilder.Append(" AND HBLHDR.HBL_EXP_TBL_PK IN  (" + strHBLPk + ")");
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

        #endregion "MakeVesselVoyageString"

        #region "subMakeHeaderString"

        /// <summary>
        /// Subs the make header string.
        /// </summary>
        /// <param name="strSql">The string SQL.</param>
        /// <param name="strHBLPk">The string HBL pk.</param>
        private void subMakeHeaderString(StringBuilder strSql, string strHBLPk)
        {
            try
            {
                strSql.Append("SELECT ");
                strSql.Append("job_exp.jobcard_ref_no JOBCARD_REF_NO, ");
                strSql.Append("exp_bl.hbl_ref_no HBL_REF_NO, ");
                strSql.Append("bst.booking_ref_no BOOKING_REF_NO, ");
                strSql.Append("DECODE(bst.cargo_type,'1','FCL','2','LCL','4','BBC') CARGO_TYPE, ");
                strSql.Append("cust.customer_name CUSTOMER_NAME, ");
                strSql.Append("col_place.place_name COLLECTION_PLACE, ");
                strSql.Append("POL.PORT_ID POL, ");
                strSql.Append("POD.port_ID POD, ");
                strSql.Append("pol.port_name POL_NAME, ");
                strSql.Append("pod.port_name POD_NAME, ");
                strSql.Append("del_place.place_name DELIVERY_PLACE, ");
                strSql.Append("DECODE(job_card_status,'1','OPEN','CLOSE') JOB_CARD_STATUS, ");
                strSql.Append("DECODE(exp_bl.hbl_status,'2','RELEASED') BL_STATUS, ");
                strSql.Append("TO_CHAR(JOB_EXP.JOB_CARD_CLOSED_ON, DATEFORMAT) JOB_CARD_CLOSED_ON, ");
                strSql.Append("omt.operator_id OPERATOR_ID, ");
                strSql.Append("omt.operator_name OPERATOR_NAME, ");
                strSql.Append("VVT.VESSEL_ID VESSEL_ID, ");
                strSql.Append("VVT.VESSEL_NAME VESSEL_NAME, ");
                strSql.Append("VVVT.VOYAGE VOYAGE, ");
                strSql.Append("TO_CHAR(exp_bl.eta_date, DATEFORMAT) ETA, ");
                strSql.Append("TO_CHAR(exp_bl.etd_date, DATEFORMAT) ETD, ");
                strSql.Append("TO_CHAR(exp_bl.arrival_date, DATEFORMAT) ARRIVAL_DATE, ");
                strSql.Append("TO_CHAR(exp_bl.departure_date,DATEFORMAT) DEPARTURE_DATE, ");
                strSql.Append("exp_bl.sec_vessel_name SECOND_VESSEL_NAME, ");
                strSql.Append("exp_bl.sec_voyage SECOND_VOYAGE, ");
                strSql.Append("TO_CHAR(exp_bl.sec_eta_date,DATEFORMAT) SECOND_ETA, ");
                strSql.Append("TO_CHAR(exp_bl.sec_etd_date,DATEFORMAT) SECOND_ETD, ");
                strSql.Append("shipper.customer_id SHIPPER_ID, ");
                strSql.Append("shipper.customer_name SHIPPER_NAME, ");
                strSql.Append("consignee.customer_id CONSIGNEE_ID, ");
                strSql.Append("consignee.customer_name CONSIGNEE_NAME, ");
                strSql.Append("notify1.customer_id NOTIFY_PARTY1_ID, ");
                strSql.Append("notify1.customer_name NOTIFY_PARTY1_NAME, ");
                strSql.Append("notify2.customer_id NOTIFY_PARTY2_ID, ");
                strSql.Append("notify2.customer_name  NOTIFY_PARTY2_NAME, ");
                strSql.Append("cbagnt.agent_id  CB_AGENT_ID, ");
                strSql.Append("cbagnt.agent_name CB_AGENT_NAME, ");
                strSql.Append("dpagnt.agent_id DP_AGENT_ID, ");
                strSql.Append("dpagnt.agent_name DP_AGENT_NAME, ");
                strSql.Append("clagnt.agent_id CL_AGENT_ID, ");
                strSql.Append("clagnt.agent_name CL_AGENT_NAME, ");
                strSql.Append("exp_bl.remarks BL_REMARKS, ");
                strSql.Append("TO_CHAR(job_exp.jobcard_date,DATEFORMAT) JOBCARD_DATE, ");
                strSql.Append("job_exp.ucr_no UCR_NO, ");
                strSql.Append("TO_CHAR(exp_bl.hbl_date,DATEFORMAT) BL_DATE, ");
                strSql.Append("DECODE(job_exp.pymt_type,'1','PREPAID','COLLECT') PAYMENT_TYPE, ");
                strSql.Append("job_exp.insurance_amt INSURANCE_AMOUNT, ");
                strSql.Append("job_exp.insurance_currency INSURANCE_CURRENCY, ");
                strSql.Append("comm.commodity_group_desc COMMODITY_GROUP_DESC, ");
                strSql.Append("depot.transporter_id  DEPOT_ID, ");
                strSql.Append("depot.transporter_name DEPOT_NAME, ");
                strSql.Append("carrier.transporter_id CARRIER_ID, ");
                strSql.Append("carrier.transporter_name CARRIER_NAME, ");
                strSql.Append("country.country_id COUNTRY_ID, ");
                strSql.Append("country.country_name COUNTRY_NAME, ");
                strSql.Append("job_exp.da_number DA_NUMBER,");
                strSql.Append(" DECODE(job_exp.LC_SHIPMENT,0,'NO',1,'YES') LC_SHIPMENT ");
                strSql.Append(" FROM ");
                strSql.Append(" hbl_exp_tbl exp_bl, ");
                strSql.Append(" JOB_CARD_TRN job_exp, ");
                strSql.Append(" BOOKING_MST_TBL bst, ");
                strSql.Append(" port_mst_tbl POD, ");
                strSql.Append(" port_mst_tbl POL, ");
                strSql.Append(" customer_mst_tbl cust, ");
                strSql.Append(" customer_mst_tbl consignee, ");
                strSql.Append(" customer_mst_tbl shipper, ");
                strSql.Append(" customer_mst_tbl notify1, ");
                strSql.Append(" customer_mst_tbl notify2, ");
                strSql.Append(" place_mst_tbl col_place, ");
                strSql.Append(" place_mst_tbl del_place, ");
                strSql.Append(" operator_mst_tbl omt, ");
                strSql.Append(" agent_mst_tbl clagnt, ");
                strSql.Append(" agent_mst_tbl dpagnt,  ");
                strSql.Append(" agent_mst_tbl cbagnt, ");
                strSql.Append(" commodity_group_mst_tbl comm, ");
                strSql.Append(" transporter_mst_tbl  depot, ");
                strSql.Append(" transporter_mst_tbl  carrier, ");
                strSql.Append(" country_mst_tbl country, ");
                strSql.Append(" VESSEL_VOYAGE_TBL VVT, ");
                strSql.Append(" VESSEL_VOYAGE_TRN VVVT, ");
                strSql.Append("    EMPLOYEE_MST_TBL        EMP, ");
                strSql.Append("    EMPLOYEE_MST_TBL        SHP_SE ");
                strSql.Append(" WHERE ");
                strSql.Append(" exp_bl.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK ");
                strSql.Append(" AND job_exp.BOOKING_MST_FK =  bst.BOOKING_MST_PK ");
                strSql.Append(" AND bst.port_mst_pol_fk    =  pol.port_mst_pk ");
                strSql.Append(" AND bst.port_mst_pod_fk    =  pod.port_mst_pk ");
                strSql.Append(" AND bst.col_place_mst_fk   =  col_place.place_pk(+) ");
                strSql.Append(" AND bst.del_place_mst_fk =  del_place.place_pk(+) ");
                strSql.Append(" AND bst.cust_customer_mst_fk =  cust.customer_mst_pk(+) ");
                strSql.Append(" AND bst.CARRIER_MST_FK = omt.operator_mst_pk ");
                strSql.Append(" AND exp_bl.shipper_cust_mst_fk =  shipper.customer_mst_pk(+) ");
                strSql.Append(" AND exp_bl.consignee_cust_mst_fk =  consignee.customer_mst_pk(+) ");
                strSql.Append(" AND exp_bl.notify1_cust_mst_fk =  notify1.customer_mst_pk(+) ");
                strSql.Append(" AND exp_bl.notify2_cust_mst_fk = notify2.customer_mst_pk(+) ");
                strSql.Append(" AND exp_bl.cl_agent_mst_fk =  clagnt.agent_mst_pk(+) ");
                strSql.Append(" AND exp_bl.cb_agent_mst_fk =  cbagnt.agent_mst_pk(+) ");
                strSql.Append(" AND exp_bl.dp_agent_mst_fk =  dpagnt.agent_mst_pk(+) ");
                strSql.Append(" AND job_exp.commodity_group_fk =  comm.commodity_group_pk(+) ");
                strSql.Append(" AND job_exp.transporter_depot_fk =  depot.transporter_mst_pk(+) ");
                strSql.Append(" AND job_exp.transporter_carrier_fk = carrier.transporter_mst_pk(+) ");
                strSql.Append(" AND job_exp.country_origin_fk  =  country.country_mst_pk(+) ");
                strSql.Append("    AND shipper.REP_EMP_MST_FK=SHP_SE.EMPLOYEE_MST_PK(+) ");
                strSql.Append("    AND JOB_EXP.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) ");
                strSql.Append("AND JOB_EXP.VOYAGE_TRN_FK = VVVT.VOYAGE_TRN_PK ");
                strSql.Append("AND VVVT.VESSEL_VOYAGE_TBL_FK=VVT.VESSEL_VOYAGE_TBL_PK ");
                strSql.Append(" AND job_exp.BUSINESS_TYPE = 2  ");
                strSql.Append("    AND exp_bl.hbl_exp_tbl_pk  IN (" + strHBLPk + ")");
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

        #endregion "subMakeHeaderString"

        #region "subMakeContString"

        /// <summary>
        /// Subs the make cont string.
        /// </summary>
        /// <param name="strSql">The string SQL.</param>
        /// <param name="strHBLPk">The string HBL pk.</param>
        private void subMakeContString(StringBuilder strSql, string strHBLPk)
        {
            try
            {
                strSql.Append("SELECT ");
                strSql.Append("exp_bl.hbl_ref_no HBL_REF_NO, ");
                strSql.Append("job_trn_cont.container_number CONTAINER_NUMBER, ");
                strSql.Append("cont.container_type_mst_id CONTAINER_TYPE, ");
                strSql.Append("job_trn_cont.seal_number SEAL_NUMBER, ");
                strSql.Append("job_trn_cont.volume_in_cbm VOLUME_IN_CBM, ");
                strSql.Append("job_trn_cont.gross_weight GROSS_WEIGHT, ");
                strSql.Append("job_trn_cont.net_weight NET_WEIGHT, ");
                strSql.Append("job_trn_cont.chargeable_weight CHARGEABLE_WEIGHT, ");
                strSql.Append("pack.pack_type_id PACK_TYPE, ");
                strSql.Append("job_trn_cont.pack_count PACK_COUNT, ");
                strSql.Append("comm.Commodity_Name COMMODITY, ");
                strSql.Append("job_trn_cont.LOAD_DATE LOAD_DATE ");
                strSql.Append("FROM ");
                strSql.Append("HBL_EXP_TBL exp_bl, ");
                strSql.Append("JOB_TRN_CONT job_trn_cont, ");
                strSql.Append("pack_type_mst_tbl pack, ");
                strSql.Append("commodity_mst_tbl comm, ");
                strSql.Append("container_type_mst_tbl cont, ");
                strSql.Append(" JOB_CARD_TRN job_exp, ");
                strSql.Append(" BOOKING_MST_TBL bst, ");
                strSql.Append("port_mst_tbl pod, ");
                strSql.Append("port_mst_tbl pol ");
                strSql.Append("WHERE ");
                strSql.Append("exp_bl.job_card_sea_exp_fk = job_exp.JOB_CARD_TRN_PK ");
                strSql.Append("AND exp_bl.job_card_sea_exp_fk = job_trn_cont.JOB_CARD_TRN_FK ");
                strSql.Append("AND job_trn_cont.pack_type_mst_fk = pack.pack_type_mst_pk(+) ");
                strSql.Append("AND job_trn_cont.container_type_mst_fk = cont.container_type_mst_pk(+) ");
                strSql.Append("AND job_trn_cont.commodity_mst_fk = comm.commodity_mst_pk(+) ");
                strSql.Append("AND job_trn_cont.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK ");
                strSql.Append("AND job_exp.BOOKING_MST_FK = bst.BOOKING_MST_PK ");
                strSql.Append("AND bst.port_mst_pod_fk = pod.port_mst_pk ");
                strSql.Append("AND bst.port_mst_pol_fk = pol.port_mst_pk ");
                strSql.Append(" AND job_exp.BUSINESS_TYPE = 2  ");
                strSql.Append("AND exp_bl.hbl_exp_tbl_pk IN (" + strHBLPk + ")");
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

        #endregion "subMakeContString"

        #region "subMakeFrtString"

        /// <summary>
        /// Subs the make FRT string.
        /// </summary>
        /// <param name="strSql">The string SQL.</param>
        /// <param name="strHBLPk">The string HBL pk.</param>
        private void subMakeFrtString(StringBuilder strSql, string strHBLPk)
        {
            try
            {
                strSql.Append("SELECT ");
                strSql.Append("HBLHDR.HBL_REF_NO HBL_REF_NO, ");
                strSql.Append("CONT.CONTAINER_TYPE_MST_ID, ");
                strSql.Append("FRT.freight_element_id FREIGHT_ELEMENT_ID, ");
                strSql.Append("FRT.freight_element_name FREIGHT_ELEMENT_NAME, ");
                strSql.Append("UOM.DIMENTION_ID DIMENTION_ID, ");
                strSql.Append("JFRT.quantity QUANTITY, ");
                strSql.Append("DECODE(JFRT.freight_type,1,'PREPAID',2,'COLLECT') FREIGHT_TYPE, ");
                strSql.Append("JFRT.freight_amt FREIGHT_AMOUNT, ");
                strSql.Append("CURR.CURRENCY_ID CURRENCY_ID, ");
                strSql.Append("CURR.CURRENCY_NAME CURRENCY_NAME, ");
                strSql.Append("JFRT.exchange_rate AS RATE_OF_EXCHANGE ");
                strSql.Append("FROM ");
                strSql.Append("HBL_EXP_TBL HBLHDR, ");
                strSql.Append("JOB_CARD_TRN JHDR, ");
                strSql.Append("JOB_TRN_FD JFRT, ");
                strSql.Append("container_type_mst_tbl CONT, ");
                strSql.Append("currency_type_mst_tbl CURR, ");
                strSql.Append("freight_element_mst_tbl FRT, ");
                strSql.Append("DIMENTION_UNIT_MST_TBL UOM ");
                strSql.Append("WHERE ");
                strSql.Append("HBLHDR.JOB_CARD_SEA_EXP_FK = JHDR.JOB_CARD_TRN_PK ");
                strSql.Append("AND JFRT.JOB_CARD_TRN_FK=JHDR.JOB_CARD_TRN_PK ");
                strSql.Append("AND JFRT.CONTAINER_TYPE_MST_FK=CONT.CONTAINER_TYPE_MST_PK(+) ");
                strSql.Append("AND JFRT.CURRENCY_MST_FK=CURR.CURRENCY_MST_PK ");
                strSql.Append("AND JFRT.FREIGHT_ELEMENT_MST_FK=FRT.FREIGHT_ELEMENT_MST_PK ");
                strSql.Append("AND JFRT.BASIS=UOM.DIMENTION_UNIT_MST_PK(+) ");
                strSql.Append(" AND JHDR.BUSINESS_TYPE = 2  ");
                strSql.Append(" AND HBLHDR.hbl_exp_tbl_pk  IN (" + strHBLPk + ")");
                strSql.Append(" ORDER BY cont.container_type_mst_id, frt.freight_element_id ");
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

        #endregion "subMakeFrtString"

        #region "subMakeTPortString"

        /// <summary>
        /// Subs the make t port string.
        /// </summary>
        /// <param name="strSql">The string SQL.</param>
        /// <param name="strHBLPk">The string HBL pk.</param>
        private void subMakeTPortString(StringBuilder strSql, string strHBLPk)
        {
            try
            {
                strSql.Append("SELECT ");
                strSql.Append("    exp_bl.hbl_ref_no HBL_REF_NO, ");
                strSql.Append("    job_trn_tp.transhipment_no TRANSHIPMENT_NO, ");
                strSql.Append("    port.port_id PORT_ID, ");
                strSql.Append("    port.port_name PORT_NAME, ");
                strSql.Append("    job_trn_tp.vessel_name VESSEL_NAME, ");
                strSql.Append("    job_trn_tp.voyage VOYAGE, ");
                strSql.Append("    TO_CHAR(job_trn_tp.eta_date,'" + dateFormat + "') ETA_DATE, ");
                strSql.Append("    TO_CHAR(job_trn_tp.etd_date,'" + dateFormat + "') ETD_DATE ");
                strSql.Append("    FROM ");
                strSql.Append("    HBL_EXP_TBL exp_bl, ");
                strSql.Append("    job_trn_sea_exp_tp  job_trn_tp, ");
                strSql.Append("    port_mst_tbl port, ");
                strSql.Append("    JOB_CARD_TRN job_exp, ");
                strSql.Append("    BOOKING_MST_TBL bst, ");
                strSql.Append("    port_mst_tbl pod, ");
                strSql.Append("    port_mst_tbl pol ");
                strSql.Append(",   JOB_TRN_CONT job_trn_cont ");
                strSql.Append("    WHERE ");
                strSql.Append("    exp_bl.job_card_sea_exp_fk = job_exp.job_card_trn_pk ");
                strSql.Append("    AND job_trn_tp.job_card_sea_exp_fk = job_exp.job_card_trn_pk ");
                strSql.Append("    AND job_trn_tp.port_mst_fk = port.port_mst_pk ");
                strSql.Append("    AND job_exp.Booking_Mst_FK = bst.Booking_Mst_Pk ");
                strSql.Append("    AND bst.port_mst_pod_fk = pod.port_mst_pk ");
                strSql.Append("    AND bst.port_mst_pol_fk = pol.port_mst_pk ");
                strSql.Append(" AND job_exp.BUSINESS_TYPE = 2  ");
                strSql.Append("    AND exp_bl.hbl_exp_tbl_pk  IN (" + strHBLPk + ")");
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

        #endregion "subMakeTPortString"

        #region "subMakeOtherString"

        /// <summary>
        /// Subs the make other string.
        /// </summary>
        /// <param name="strSql">The string SQL.</param>
        /// <param name="strHBLPk">The string HBL pk.</param>
        private void subMakeOtherString(StringBuilder strSql, string strHBLPk)
        {
            try
            {
                strSql.Append(" SELECT HBL.HBL_REF_NO,");
                strSql.Append("        HBL.LC_NUMBER LC_NR,");
                strSql.Append("        HBL.LC_DATE LC_Date,");
                strSql.Append("        HBL.LC_EXPIRES_ON LC_Expires_On,");
                strSql.Append("        HBL.CONSIGNEE_ADDRESS Consignemnt_Bank,");
                strSql.Append("        HBL.LETTER_OF_CREDIT Remarks,");
                strSql.Append("        NVL(EMP.EMPLOYEE_MST_PK,NVL(SHP_SE.EMPLOYEE_MST_PK,0)) SALES_EXEC_FK,");
                strSql.Append("        NVL(EMP.EMPLOYEE_NAME,NVL(SHP_SE.EMPLOYEE_NAME,' ')) SALES_EXECUTIVE ");
                strSql.Append("  FROM JOB_CARD_TRN JOB, HBL_EXP_TBL HBL,");
                strSql.Append("    customer_mst_tbl shipper, ");
                strSql.Append("    EMPLOYEE_MST_TBL        EMP, ");
                strSql.Append("    EMPLOYEE_MST_TBL        SHP_SE ");
                strSql.Append(" WHERE JOB.JOB_CARD_TRN_PK = HBL.JOB_CARD_SEA_EXP_FK");
                strSql.Append("   AND HBL.HBL_EXP_TBL_PK IN (" + strHBLPk + ")");
                strSql.Append("   AND JOB.SHIPPER_CUST_MST_FK =  shipper.customer_mst_pk(+) ");
                strSql.Append("   AND shipper.REP_EMP_MST_FK=SHP_SE.EMPLOYEE_MST_PK(+) ");
                strSql.Append("   AND JOB.BUSINESS_TYPE = 2  ");
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

        #region "Enhance Search VesVoy"

        /// <summary>
        /// Fetches the voyage for bl.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchVoyageForBL(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = null;
            string strBizType = null;
            string strJobPK = null;
            string strReq = null;

            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strVES = Convert.ToString(arr.GetValue(1));
            strVOY = Convert.ToString(arr.GetValue(2));
            strBizType = Convert.ToString(arr.GetValue(3));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.GET_VES_VOY_BL";

                var _with2 = selectCommand.Parameters;
                _with2.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with2.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
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

        #endregion "Enhance Search VesVoy"
    }
}