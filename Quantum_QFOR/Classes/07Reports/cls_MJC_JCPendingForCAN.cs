#region "Comments"

//'***************************************************************************************************************
//'*  Company Name:
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By  :    Santosh on 31-May-16
//'*  Module/Project Leader   :    Santosh Pisipati
//'*  Description :
//'*  Module/Form/Class Name  :
//'*  Configuration ID        :
//'***************************************************************************************************************
//'*  Revision History
//'***************************************************************************************************************
//'*  Modified DateTime(DD-MON-YYYY)  Modified By     Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.DataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class ClsMJC_JCPendingForCAN : CommonFeatures
    {
        #region "Sea Grid Function For CAN"

        /// <summary>
        /// Fetches the sea grid.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <returns></returns>
        public DataSet FetchSeaGrid(Int32 LocFk = 0, Int32 CustPK = 0, string VslName = "", string FromDt = "", string ToDt = "", string ETADt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 ExportExcel = 0,
        Int16 CargoType = 0, string Voyage = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            DataSet DS = null;

            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }

                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND JC.JOBCARD_DATE  BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND JC.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND JC.JOBCARD_DATE  <= TO_DATE('" + ToDt + "',dateformat) ";
                }

                if (!string.IsNullOrEmpty(ETADt))
                {
                    strCondition = strCondition + " And TO_DATE( JC.ETA_DATE) = TO_DATE('" + ETADt + "',dateformat)";
                }

                //If VslName <> "" ThenRaghu
                //    strCondition = strCondition & " And VST.VESSEL_NAME = '" & VslName & "'"
                //End If

                //If Voyage <> "" Then
                //    strCondition = strCondition & " And VVT.VOYAGE = '" & Voyage & "'"
                //End If
                ///''''
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition = strCondition + "  AND VVT.VOYAGE_TRN_PK  = '" + Voyage + "'";
                }

                if (CustPK > 0)
                {
                    strCondition = strCondition + " And JC.CONSIGNEE_CUST_MST_FK = " + CustPK + "";
                }

                if (CargoType > 0)
                {
                    strCondition = strCondition + " and JC.CARGO_TYPE=" + CargoType + "";
                }

                sb.Append("SELECT M.MASTER_JC_SEA_IMP_PK MSTJCPK,");
                sb.Append("       JC.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       JC.JOBCARD_REF_NO JOBREFNO,");
                sb.Append("       TO_DATE(JC.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("       M.MASTER_JC_REF_NO MSTJCREFNO,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       OMT.OPERATOR_NAME OPRANAME,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append("       JC.PORT_MST_POL_FK POLFK,");
                sb.Append("       POL.PORT_NAME POLNAME,");
                sb.Append("       JC.ETA_DATE ETA,");
                sb.Append("       JC.POL_AGENT_MST_FK AGENTFK,");
                sb.Append("       AMT.AGENT_NAME AGENTNAME,");
                sb.Append("       DECODE(JC.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMGRPCODE,");
                sb.Append("       CASE");
                sb.Append("         WHEN M.CARGO_TYPE = 2 THEN");
                sb.Append("          (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
                sb.Append("         ELSE");
                sb.Append("          (SELECT SUM(JCONT.NET_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
                sb.Append("       END CHARGEABLE_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS VOLUME_IN_CBM,");
                sb.Append("        JC.CONSOLE JCON,");
                sb.Append("       M.CONSOLE MCON,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_SEA_IMP_TBL   M,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VVT,");
                sb.Append("       VESSEL_VOYAGE_TBL       VST,");
                sb.Append("       OPERATOR_MST_TBL        OMT,");
                sb.Append("       JOB_CARD_TRN    JC,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       CAN_MST_TBL             CAN");
                sb.Append(" WHERE POL.PORT_MST_PK = JC.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = JC.POL_AGENT_MST_FK");
                sb.Append("   AND OMT.OPERATOR_MST_PK(+) = JC.CARRIER_MST_FK");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND JC.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK(+)");
                sb.Append("   AND JC.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JC.JOB_CARD_STATUS = 1");
                sb.Append("   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                sb.Append("   AND CAN.JOB_CARD_FK IS   NULL");
                sb.Append("   AND VVT.VOYAGE_TRN_PK IS NOT NULL");
                sb.Append("   AND JC.BUSINESS_TYPE = 2");
                sb.Append("   AND JC.PROCESS_TYPE = 2");
                sb.Append("   AND POD.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition);

                DS = objWF.GetDataSet("SELECT * FROM ( " + sb.ToString() + " )");
                TotalRecords = (Int32)DS.Tables[0].Rows.Count;

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

                sb.Append(" ORDER BY TO_DATE(JOBCARD_DATE) DESC, JC.JOBCARD_REF_NO DESC ");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();

                if (ExportExcel == 0)
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " )q ) ";
                }
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Sea Grid Function For CAN"

        #region "Air Grid Function For CAN"

        /// <summary>
        /// Fetches the air grid.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="AirlineName">Name of the airline.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <param name="FlightNr">The flight nr.</param>
        /// <returns></returns>
        public DataSet FetchAirGrid(Int32 LocFk = 0, Int32 CustPK = 0, string AirlineName = "", string FromDt = "", string ToDt = "", string ETADt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 ExportExcel = 0,
        string FlightNr = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            DataSet DS = null;
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }

                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND JC.JOBCARD_DATE  BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND JC.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND JC.JOBCARD_DATE  <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    strCondition = strCondition + " And TO_DATE( JC.ETA_DATE) = TO_DATE('" + ETADt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(AirlineName))
                {
                    strCondition = strCondition + " And ART.AIRLINE_NAME = '" + AirlineName + "'";
                }
                if (!string.IsNullOrEmpty(FlightNr))
                {
                    strCondition = strCondition + " And JC.VOYAGE_FLIGHT_NO = '" + FlightNr + "'";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And JC.CONSIGNEE_CUST_MST_FK = " + CustPK + "";
                }
                sb.Append("SELECT M.MASTER_JC_AIR_IMP_PK MSTJCPK,");
                sb.Append("       JC.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       JC.JOBCARD_REF_NO JOBREFNO,");
                sb.Append("       TO_DATE(JC.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("       M.MASTER_JC_REF_NO MSTJCREFNO,");
                sb.Append("       'AIR' BIZTYPE,");
                sb.Append("       ART.AIRLINE_NAME OPRANAME,");
                sb.Append("       JC.VOYAGE_FLIGHT_NO VESVOYAGE,");
                sb.Append("       JC.PORT_MST_POL_FK POLFK,");
                sb.Append("       POL.PORT_ID POLNAME,");
                sb.Append("       JC.ETA_DATE ETA,");
                sb.Append("       JC.POL_AGENT_MST_FK AGENTFK,");
                sb.Append("       AMT.AGENT_NAME AGENTNAME,");
                sb.Append("       '' CARGO_TYPE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMGRPCODE,");
                sb.Append("       (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS CHARGEABLE_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ) AS VOLUME_IN_CBM,");
                sb.Append("        JC.CONSOLE JCON,");
                sb.Append("        M.CONSOLE MCON,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_AIR_IMP_TBL   M,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       AIRLINE_MST_TBL         ART,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       JOB_CARD_TRN    JC,");
                sb.Append("       CAN_MST_TBL             CAN,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = JC.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = JC.POL_AGENT_MST_FK");
                sb.Append("   AND ART.AIRLINE_MST_PK(+) = JC.CARRIER_MST_FK");
                sb.Append("   AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK(+)");
                sb.Append("   AND JC.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JC.JOB_CARD_STATUS = 1");
                sb.Append("   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                sb.Append("   AND CAN.JOB_CARD_FK IS NULL");
                sb.Append("   AND ART.AIRLINE_MST_PK IS NOT NULL");
                sb.Append("   AND JC.BUSINESS_TYPE = 1 ");
                sb.Append("   AND JC.PROCESS_TYPE = 2");
                sb.Append("   AND POD.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                sb.Append(strCondition);

                DS = objWF.GetDataSet("SELECT * FROM ( " + sb.ToString() + " )");
                TotalRecords = (Int32)DS.Tables[0].Rows.Count;

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

                sb.Append(" order by TO_DATE(JOBCARD_DATE) DESC, JC.JOBCARD_REF_NO DESC ");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();

                if (ExportExcel == 0)
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " )q ) ";
                }
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Air Grid Function For CAN"

        #region "Sea Print Function For CAN"

        /// <summary>
        /// Fetches the sea report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <returns></returns>
        public DataSet FetchSeaReport(Int32 LocFk = 0, Int32 CustPK = 0, string VslName = "", string FromDt = "", string ToDt = "", string ETADt = "", Int16 CargoType = 0, string Voyage = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT M.MASTER_JC_SEA_IMP_PK MSTJCPK,");
                sb.Append("       JC.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       JC.JOBCARD_REF_NO JOBREFNO,");
                sb.Append("       TO_CHAR(JC.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("       M.MASTER_JC_REF_NO MSTJCREFNO,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       OMT.OPERATOR_NAME OPRANAME,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append("       JC.PORT_MST_POL_FK POLFK,");
                sb.Append("       POL.PORT_NAME POLNAME,");
                sb.Append("       JC.ETA_DATE ETA,");
                sb.Append("       JC.POL_AGENT_MST_FK AGENTFK,");
                sb.Append("       AMT.AGENT_NAME AGENTNAME,");
                sb.Append("       DECODE(JC.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMGRPCODE,");
                sb.Append("       CASE");
                sb.Append("         WHEN M.CARGO_TYPE = 2 THEN");
                sb.Append("          (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
                sb.Append("         ELSE");
                sb.Append("          (SELECT SUM(JCONT.NET_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
                sb.Append("       END CHARGEABLE_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS VOLUME_IN_CBM,");
                sb.Append("       JC.CONSOLE,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_SEA_IMP_TBL   M,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VVT,");
                sb.Append("       VESSEL_VOYAGE_TBL       VST,");
                sb.Append("       OPERATOR_MST_TBL        OMT,");
                sb.Append("       JOB_CARD_TRN    JC,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       CAN_MST_TBL             CAN");
                sb.Append(" WHERE POL.PORT_MST_PK = JC.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = JC.POL_AGENT_MST_FK");
                sb.Append("   AND OMT.OPERATOR_MST_PK(+) = JC.CARRIER_MST_FK");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND JC.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK(+)");
                sb.Append("   AND JC.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JC.JOB_CARD_STATUS = 1");
                sb.Append("   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                sb.Append("   AND CAN.JOB_CARD_FK IS   NULL");
                sb.Append("   AND VVT.VOYAGE_TRN_PK IS NOT NULL");
                sb.Append("   AND JC.BUSINESS_TYPE = 2 ");
                sb.Append("   AND JC.PROCESS_TYPE = 2");
                sb.Append("   AND POD.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append(" AND JC.JOBCARD_DATE  BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" AND JC.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append("  AND JC.JOBCARD_DATE  <= TO_DATE('" + ToDt + "',dateformat) ");
                }

                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE( JC.ETA_DATE) = TO_DATE('" + ETADt + "',dateformat)");
                }

                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And VST.VESSEL_NAME = '" + VslName + "'");
                }
                if (CustPK > 0)
                {
                    sb.Append("  And JC.CONSIGNEE_CUST_MST_FK = " + CustPK + "");
                }

                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append("  And VVT.VOYAGE = '" + Voyage + "'");
                }

                if (CargoType > 0)
                {
                    sb.Append(" and JC.CARGO_TYPE=" + CargoType + "");
                }

                sb.Append(" order by TO_DATE(JOBCARD_DATE) DESC, JC.JOBCARD_REF_NO DESC ");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Sea Print Function For CAN"

        #region "Air Print Function For CAN"

        /// <summary>
        /// Fetches the air report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="AirlineName">Name of the airline.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="FlightNr">The flight nr.</param>
        /// <returns></returns>
        public DataSet FetchAirReport(Int32 LocFk = 0, Int32 CustPK = 0, string AirlineName = "", string FromDt = "", string ToDt = "", string ETADt = "", string FlightNr = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT M.MASTER_JC_AIR_IMP_PK MSTJCPK,");
                sb.Append("       JC.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       JC.JOBCARD_REF_NO JOBREFNO,");
                sb.Append("       TO_CHAR(JC.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("       M.MASTER_JC_REF_NO MSTJCREFNO,");
                sb.Append("       ART.AIRLINE_NAME OPRANAME,");
                sb.Append("       '' VESVOYAGE,");
                sb.Append("       JC.PORT_MST_POL_FK POLFK,");
                sb.Append("       POL.PORT_ID POLNAME,");
                sb.Append("       JC.ETA_DATE ETA,");
                sb.Append("       JC.POL_AGENT_MST_FK AGENTFK,");
                sb.Append("       AMT.AGENT_NAME AGENTNAME,");
                sb.Append("       '' CARGO_TYPE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMGRPCODE,");
                sb.Append("       (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS CHARGEABLE_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ) AS VOLUME_IN_CBM,");
                sb.Append("       JC.CONSOLE,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_AIR_IMP_TBL   M,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       AIRLINE_MST_TBL         ART,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       JOB_CARD_TRN    JC,");
                sb.Append("       CAN_MST_TBL             CAN,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = JC.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = JC.POL_AGENT_MST_FK");
                sb.Append("   AND ART.AIRLINE_MST_PK(+) = JC.CARRIER_MST_FK");
                sb.Append("   AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK(+)");
                sb.Append("   AND JC.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JC.JOB_CARD_STATUS = 1");
                sb.Append("   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                sb.Append("   AND CAN.JOB_CARD_FK IS NULL");
                sb.Append("   AND ART.AIRLINE_MST_PK IS NOT NULL");
                sb.Append("   AND JC.BUSINESS_TYPE = 1 ");
                sb.Append("   AND JC.PROCESS_TYPE = 2");
                sb.Append("   AND POD.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append("  AND JC.JOBCARD_DATE  BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" AND JC.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" AND JC.JOBCARD_DATE  <= TO_DATE('" + ToDt + "',dateformat) ");
                }

                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE( JC.ETA_DATE) = TO_DATE('" + ETADt + "',dateformat)");
                }

                if (!string.IsNullOrEmpty(AirlineName))
                {
                    sb.Append(" And VST.VESSEL_NAME = '" + AirlineName + "'");
                }

                if (!string.IsNullOrEmpty(FlightNr))
                {
                    sb.Append(" And JC.VOYAGE_FLIGHT_NO = '" + FlightNr + "'");
                }

                if (CustPK > 0)
                {
                    sb.Append("  And JC.CONSIGNEE_CUST_MST_FK = " + CustPK + "");
                }

                sb.Append(" order by TO_DATE(JOBCARD_DATE) DESC, JC.JOBCARD_REF_NO DESC ");

                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Air Print Function For CAN"

        #region "Both Grid Function for CAN"

        /// <summary>
        /// Fetches the both grid.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="VoyageFlightNum">The voyage flight number.</param>
        /// <returns></returns>
        public DataSet FetchBothGrid(Int32 LocFk = 0, Int32 CustPK = 0, string VslName = "", string FromDt = "", string ToDt = "", string ETADt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 ExportExcel = 0,
        Int16 CargoType = 0, string Voyage = "", string VoyageFlightNum = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strSeaCondition = null;
            string strAirCondition = null;
            Int32 TotalRecords = default(Int32);
            DataSet DS = null;

            try
            {
                if (flag == 0)
                {
                    strSeaCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strSeaCondition = strSeaCondition + " AND JC.JOBCARD_DATE  BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strSeaCondition = strSeaCondition + " AND JC.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strSeaCondition = strSeaCondition + " AND JC.JOBCARD_DATE  <= TO_DATE('" + ToDt + "',dateformat) ";
                }

                if (!string.IsNullOrEmpty(ETADt))
                {
                    strSeaCondition = strSeaCondition + " And TO_DATE( JC.ETA_DATE) = TO_DATE('" + ETADt + "',dateformat)";
                }

                //If VslName <> "" Then Raghu
                //    strSeaCondition = strSeaCondition & " And VST.VESSEL_NAME = '" & VslName & "'"
                //End If

                //If Voyage <> "" Then
                //    strSeaCondition = strSeaCondition & " And VVT.VOYAGE = '" & Voyage & "'"
                //End If

                if (!string.IsNullOrEmpty(Voyage))
                {
                    strSeaCondition = strSeaCondition + "  AND VVT.VOYAGE_TRN_PK  = '" + Voyage + "'";
                }

                if (CustPK > 0)
                {
                    strSeaCondition = strSeaCondition + " And JC.CONSIGNEE_CUST_MST_FK = " + CustPK + "";
                }

                if (CargoType > 0)
                {
                    strSeaCondition = strSeaCondition + " and JC.CARGO_TYPE=" + CargoType + "";
                }
                //Sea
                sb.Append("SELECT * FROM (");
                sb.Append("SELECT M.MASTER_JC_SEA_IMP_PK MSTJCPK,");
                sb.Append("       JC.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       JC.JOBCARD_REF_NO JOBREFNO,");
                sb.Append("       TO_DATE(JC.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("       M.MASTER_JC_REF_NO MSTJCREFNO,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       OMT.OPERATOR_NAME OPRANAME,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append("       JC.PORT_MST_POL_FK POLFK,");
                sb.Append("       POL.PORT_NAME POLNAME,");
                sb.Append("       JC.ETA_DATE ETA,");
                sb.Append("       JC.POL_AGENT_MST_FK AGENTFK,");
                sb.Append("       AMT.AGENT_NAME AGENTNAME,");
                sb.Append("       DECODE(JC.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMGRPCODE,");
                sb.Append("       CASE");
                sb.Append("         WHEN M.CARGO_TYPE = 2 THEN");
                sb.Append("          (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
                sb.Append("         ELSE");
                sb.Append("          (SELECT SUM(JCONT.NET_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
                sb.Append("       END CHARGEABLE_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS VOLUME_IN_CBM,");
                sb.Append("        JC.CONSOLE JCON,");
                sb.Append("       M.CONSOLE MCON,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_SEA_IMP_TBL   M,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VVT,");
                sb.Append("       VESSEL_VOYAGE_TBL       VST,");
                sb.Append("       OPERATOR_MST_TBL        OMT,");
                sb.Append("       JOB_CARD_TRN    JC,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       CAN_MST_TBL             CAN");
                sb.Append(" WHERE POL.PORT_MST_PK = JC.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = JC.POL_AGENT_MST_FK");
                sb.Append("   AND OMT.OPERATOR_MST_PK(+) = JC.CARRIER_MST_FK");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND JC.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK(+)");
                sb.Append("   AND JC.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JC.JOB_CARD_STATUS = 1");
                sb.Append("   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                sb.Append("   AND CAN.JOB_CARD_FK IS   NULL");
                sb.Append("   AND VVT.VOYAGE_TRN_PK IS NOT NULL");
                sb.Append("   AND JC.BUSINESS_TYPE = 2");
                sb.Append("   AND JC.PROCESS_TYPE = 2");
                sb.Append("   AND POD.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strSeaCondition);

                if (flag == 0)
                {
                    strAirCondition += " AND 1=2";
                }

                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strAirCondition = strAirCondition + " AND JC.JOBCARD_DATE  BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strAirCondition = strAirCondition + " AND JC.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strAirCondition = strAirCondition + " AND JC.JOBCARD_DATE  <= TO_DATE('" + ToDt + "',dateformat) ";
                }

                if (!string.IsNullOrEmpty(ETADt))
                {
                    strAirCondition = strAirCondition + " And TO_DATE( JC.ETA_DATE) = TO_DATE('" + ETADt + "',dateformat)";
                }

                if (!string.IsNullOrEmpty(VslName))
                {
                    strAirCondition = strAirCondition + " And ART.AIRLINE_NAME = '" + VslName + "'";
                }

                if (!string.IsNullOrEmpty(VoyageFlightNum))
                {
                    strAirCondition = strAirCondition + " And JC.VOYAGE_FLIGHT_NO = '" + VoyageFlightNum + "'";
                }

                if (CustPK > 0)
                {
                    strAirCondition = strAirCondition + " And JC.CONSIGNEE_CUST_MST_FK = " + CustPK + "";
                }
                //AIR
                sb.Append(" UNION ");
                sb.Append("SELECT M.MASTER_JC_AIR_IMP_PK MSTJCPK,");
                sb.Append("       JC.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       JC.JOBCARD_REF_NO JOBREFNO,");
                sb.Append("       TO_DATE(JC.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("       M.MASTER_JC_REF_NO MSTJCREFNO,");
                sb.Append("       'AIR' BIZTYPE,");
                sb.Append("       ART.AIRLINE_NAME OPRANAME,");
                sb.Append("       JC.VOYAGE_FLIGHT_NO VESVOYAGE,");
                sb.Append("       JC.PORT_MST_POL_FK POLFK,");
                sb.Append("       POL.PORT_ID POLNAME,");
                sb.Append("       JC.ETA_DATE ETA,");
                sb.Append("       JC.POL_AGENT_MST_FK AGENTFK,");
                sb.Append("       AMT.AGENT_NAME AGENTNAME,");
                sb.Append("       '' CARGO_TYPE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMGRPCODE,");
                sb.Append("       (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS CHARGEABLE_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ) AS VOLUME_IN_CBM,");
                sb.Append("        JC.CONSOLE JCON,");
                sb.Append("        M.CONSOLE MCON,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_AIR_IMP_TBL   M,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       AIRLINE_MST_TBL         ART,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       JOB_CARD_TRN    JC,");
                sb.Append("       CAN_MST_TBL             CAN,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = JC.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = JC.POL_AGENT_MST_FK");
                sb.Append("   AND ART.AIRLINE_MST_PK(+) = JC.CARRIER_MST_FK");
                sb.Append("   AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK(+)");
                sb.Append("   AND JC.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JC.JOB_CARD_STATUS = 1");
                sb.Append("   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                sb.Append("   AND CAN.JOB_CARD_FK IS NULL");
                sb.Append("   AND ART.AIRLINE_MST_PK IS NOT NULL");
                sb.Append("   AND JC.BUSINESS_TYPE = 1");
                sb.Append("   AND JC.PROCESS_TYPE = 2");
                sb.Append("   AND POD.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strAirCondition);
                sb.Append(" ) ORDER BY TO_DATE(JOBCARD_DATE) DESC, JOBREFNO DESC ");

                DS = objWF.GetDataSet(sb.ToString());
                TotalRecords = (Int32)DS.Tables[0].Rows.Count;

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

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();

                if (ExportExcel == 0)
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " )q ) ";
                }
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Both Grid Function for CAN"

        #region "Both Print Function for CAN"

        /// <summary>
        /// Fetches the both report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <returns></returns>
        public DataSet FetchBothReport(Int32 LocFk = 0, Int32 CustPK = 0, string VslName = "", string FromDt = "", string ToDt = "", string ETADt = "", Int16 CargoType = 0, string Voyage = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT * FROM (");
                sb.Append("SELECT M.MASTER_JC_SEA_IMP_PK MSTJCPK,");
                sb.Append("       JC.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       JC.JOBCARD_REF_NO JOBREFNO,");
                sb.Append("       TO_CHAR(JC.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("       M.MASTER_JC_REF_NO MSTJCREFNO,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       OMT.OPERATOR_NAME OPRANAME,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append("       JC.PORT_MST_POL_FK POLFK,");
                sb.Append("       POL.PORT_NAME POLNAME,");
                sb.Append("       JC.ETA_DATE ETA,");
                sb.Append("       JC.POL_AGENT_MST_FK AGENTFK,");
                sb.Append("       AMT.AGENT_NAME AGENTNAME,");
                sb.Append("       DECODE(JC.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMGRPCODE,");
                sb.Append("       CASE");
                sb.Append("         WHEN M.CARGO_TYPE = 2 THEN");
                sb.Append("          (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
                sb.Append("         ELSE");
                sb.Append("          (SELECT SUM(JCONT.NET_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK)");
                sb.Append("       END CHARGEABLE_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS VOLUME_IN_CBM,");
                sb.Append("       JC.CONSOLE,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_SEA_IMP_TBL   M,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VVT,");
                sb.Append("       VESSEL_VOYAGE_TBL       VST,");
                sb.Append("       OPERATOR_MST_TBL        OMT,");
                sb.Append("       JOB_CARD_TRN    JC,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       CAN_MST_TBL             CAN");
                sb.Append(" WHERE POL.PORT_MST_PK = JC.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = JC.POL_AGENT_MST_FK");
                sb.Append("   AND OMT.OPERATOR_MST_PK(+) = JC.CARRIER_MST_FK");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND JC.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK(+)");
                sb.Append("   AND JC.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JC.JOB_CARD_STATUS = 1");
                sb.Append("   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                sb.Append("   AND JC.BUSINESS_TYPE = 2 ");
                sb.Append("   AND JC.PROCESS_TYPE = 2");
                sb.Append("   AND CAN.JOB_CARD_FK IS   NULL");
                sb.Append("   AND VVT.VOYAGE_TRN_PK IS NOT NULL");
                sb.Append("   AND POD.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append(" AND JC.JOBCARD_DATE  BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" AND JC.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append("  AND JC.JOBCARD_DATE  <= TO_DATE('" + ToDt + "',dateformat) ");
                }

                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE( JC.ETA_DATE) = TO_DATE('" + ETADt + "',dateformat)");
                }

                //If VslName <> "" Then
                //    sb.Append(" And VST.VESSEL_NAME = '" & VslName & "'")
                //End If
                if (CustPK > 0)
                {
                    sb.Append("  And JC.CONSIGNEE_CUST_MST_FK = " + CustPK + "");
                }

                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append("  And VVT.VOYAGE = '" + Voyage + "'");
                }

                if (CargoType > 0)
                {
                    sb.Append(" and JC.CARGO_TYPE=" + CargoType + "");
                }

                sb.Append(" UNION ");

                sb.Append("SELECT M.MASTER_JC_AIR_IMP_PK MSTJCPK,");
                sb.Append("       JC.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       JC.JOBCARD_REF_NO JOBREFNO,");
                sb.Append("       TO_CHAR(JC.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("       M.MASTER_JC_REF_NO MSTJCREFNO,");
                sb.Append("       'AIR' BIZTYPE,");
                sb.Append("       ART.AIRLINE_NAME OPRANAME,");
                sb.Append("       JC.VOYAGE_FLIGHT_NO VESVOYAGE,");
                sb.Append("       JC.PORT_MST_POL_FK POLFK,");
                sb.Append("       POL.PORT_ID POLNAME,");
                sb.Append("       JC.ETA_DATE ETA,");
                sb.Append("       JC.POL_AGENT_MST_FK AGENTFK,");
                sb.Append("       AMT.AGENT_NAME AGENTNAME,");
                sb.Append("       '' CARGO_TYPE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE COMMGRPCODE,");
                sb.Append("       (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK) AS CHARGEABLE_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK ) AS VOLUME_IN_CBM,");
                sb.Append("       JC.CONSOLE,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_AIR_IMP_TBL   M,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       AIRLINE_MST_TBL         ART,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       JOB_CARD_TRN    JC,");
                sb.Append("       CAN_MST_TBL             CAN,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = JC.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JC.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = JC.POL_AGENT_MST_FK");
                sb.Append("   AND ART.AIRLINE_MST_PK(+) = JC.CARRIER_MST_FK");
                sb.Append("   AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK(+)");
                sb.Append("   AND JC.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JC.JOB_CARD_STATUS = 1");
                sb.Append("   AND CAN.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                sb.Append("   AND CAN.JOB_CARD_FK IS NULL");
                sb.Append("   AND ART.AIRLINE_MST_PK IS NOT NULL");
                sb.Append("   AND JC.BUSINESS_TYPE = 1 ");
                sb.Append("   AND JC.PROCESS_TYPE = 2");
                sb.Append("   AND POD.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append(" AND JC.JOBCARD_DATE  BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" AND JC.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" AND JC.JOBCARD_DATE  <= TO_DATE('" + ToDt + "',dateformat) ");
                }

                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE( JC.ETA_DATE) = TO_DATE('" + ETADt + "',dateformat)");
                }

                //If VslName <> "" Then
                //    sb.Append("  And ART.AIRLINE_NAME = '" & VslName & "'")
                //End If

                if (CustPK > 0)
                {
                    sb.Append("  And JC.CONSIGNEE_CUST_MST_FK = " + CustPK + "");
                }

                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append(" And JC.VOYAGE_FLIGHT_NO = '" + Voyage + "'");
                }

                sb.Append(" )order by TO_DATE(JOBCARD_DATE) DESC, JOBREFNO DESC ");

                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Both Print Function for CAN"
    }
}