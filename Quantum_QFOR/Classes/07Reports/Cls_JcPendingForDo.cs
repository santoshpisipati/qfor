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
    public class Cls_JcPendingForDo : CommonFeatures
    {
        #region "Seagrid"

        /// <summary>
        /// Fetchseagrids the specified total page.
        /// </summary>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public object Fetchseagrid(Int32 TotalPage = 0, Int32 CurrentPage = 0, Int32 LocFk = 0, string FromDt = "", string ToDt = "", string ETDDt = "", string CustName = "", string VslName = "", Int32 ExportExcel = 0, Int32 flag = 0,
        int CargoType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }

                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And JS.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And JS.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JS.Arrival_Date) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CUST.CUSTOMER_NAME = '" + CustName + "'";
                }
                //If VslName <> "" Then
                //    strCondition = strCondition & " And JS.VESSEL_NAME = '" & VslName & "'"
                //End If
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And JS.VOYAGE_TRN_FK = '" + VslName + "'";
                }
                if (CargoType != 0)
                {
                    strCondition = strCondition + " And JS.CARGO_TYPE = " + CargoType + " ";
                }

                sb.Append("SELECT * FROM (SELECT DISTINCT JS.JOB_CARD_TRN_PK AS \"HIDDEN\",");
                sb.Append("                JS.JOBCARD_REF_NO AS \"JOB CARD REF NO\",");
                sb.Append("                'Sea' AS \"BIZ_TYPE\",");
                sb.Append("                OPR.OPERATOR_NAME LINE,");
                sb.Append("                CASE WHEN JS.VESSEL_NAME IS NULL THEN");
                sb.Append("                        JS.VESSEL_NAME");
                sb.Append("                 ELSE");
                sb.Append("                   (JS.VESSEL_NAME || '/' || JS.Voyage_Flight_No)");
                sb.Append("                  END AS VOYAGE,");
                sb.Append("                POL.PORT_ID AS \"POL\",");
                sb.Append("                TO_CHAR(JS.Arrival_Date, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                AGNT.AGENT_NAME AS \"POL Agent\",");
                sb.Append("                DECODE(JS.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC')AS \"cargo type\",");
                sb.Append("                COMMGRP.COMMODITY_GROUP_CODE AS \"Commodity Group\",");
                sb.Append("                SUM(NVL(JSCONT.NET_WEIGHT, 0)) AS \"Net Wt\",");
                sb.Append("                SUM(NVL(JSCONT.GROSS_WEIGHT, 0)) AS \"Gross Wt\",");
                sb.Append("                SUM(NVL(JSCONT.VOLUME_IN_CBM, 0)) AS \"Volume\"");
                sb.Append("  FROM JOB_CARD_TRN    JS,");
                // sb.Append("       USER_MST_TBL            UMT,")
                sb.Append("       CUSTOMER_MST_TBL        CUST,");
                sb.Append("       JOB_TRN_CONT      JSCONT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMMGRP,");
                sb.Append("       Operator_Mst_Tbl       OPR,");
                sb.Append("       AGENT_MST_TBL           AGNT");
                sb.Append(" WHERE JS.HBL_HAWB_REF_NO IS NOT NULL");
                sb.Append("   AND JS.JOB_CARD_TRN_PK = JSCONT.JOB_CARD_TRN_FK");
                sb.Append("   AND JS.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND JS.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JS.COMMODITY_GROUP_FK = COMMGRP.COMMODITY_GROUP_PK");
                sb.Append("   AND JS.POL_AGENT_MST_FK = AGNT.AGENT_MST_PK(+)");
                sb.Append("   AND JS.Carrier_Mst_Fk=OPR.OPERATOR_MST_PK  AND JS.BUSINESS_TYPE=2 ");
                // sb.Append("   AND ((UMT.DEFAULT_LOCATION_FK=1301 AND JS.JC_AUTO_MANUAL = 0) OR")
                sb.Append("       AND POD.location_mst_fk  =" + LocFk + "");
                sb.Append("   AND CUST.CUSTOMER_MST_PK = JS.CONSIGNEE_CUST_MST_FK(+)");
                sb.Append("   AND JSCONT.JOB_TRN_CONT_PK NOT IN");
                sb.Append("       (SELECT DODTL.JOB_CARD_SEA_IMP_CONT_FK");
                sb.Append("          FROM DELIVERY_ORDER_MST_TBL DO,");
                sb.Append("               DELIVERY_ORDER_DTL_TBL DODTL,");
                sb.Append("               JOB_CARD_TRN   JS");
                sb.Append("         WHERE DO.DELIVERY_ORDER_PK = DODTL.DELIVERY_ORDER_FK");
                sb.Append("           AND DO.JOB_CARD_MST_FK = JS.JOB_CARD_TRN_PK");
                sb.Append("           AND do.biz_type=2)");
                sb.Append(strCondition);
                sb.Append(" GROUP BY JS.JOB_CARD_TRN_PK,");
                sb.Append("          JS.JOBCARD_REF_NO,");
                sb.Append("          JS.VESSEL_NAME,");
                sb.Append("          JS.Voyage_Flight_No,");
                sb.Append("          POL.PORT_ID,");
                sb.Append("          JS.Arrival_Date,");
                sb.Append("          AGNT.AGENT_NAME,");
                sb.Append("          JS.CARGO_TYPE,");
                sb.Append("          OPR.OPERATOR_NAME,");
                sb.Append("          COMMGRP.COMMODITY_GROUP_CODE)");
                sb.Append(" ORDER BY  NVL(ETA_DATE,'01/01/0001') DESC");

                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";

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

                //sb.Append(" ORDER BY NVL(ETA_DATE,'01/12/2011') DESC")
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
            catch (OracleException oraExp)
            {
                throw oraExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Seagrid"

        #region "AIR grid"

        /// <summary>
        /// Fetches the air grid.
        /// </summary>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public object FetchAirGrid(Int32 TotalPage = 0, Int32 CurrentPage = 0, Int32 LocFk = 0, string FromDt = "", string ToDt = "", string ETDDt = "", string CustName = "", string VslName = "", Int32 ExportExcel = 0, Int32 flag = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And JA.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And JA.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JA.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CUST.CUSTOMER_NAME = '" + CustName + "'";
                }
                //If VslName <> "" Then
                //    strCondition = strCondition & " And AIR.AIRLINE_NAME = '" & VslName & "'"
                //End If
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And JA.VOYAGE_FLIGHT_NO = '" + VslName + "'";
                }
                sb.Append("SELECT * FROM (SELECT DISTINCT JA.JOB_CARD_TRN_PK AS \"HIDDEN\",");
                sb.Append("                JA.JOBCARD_REF_NO AS \"JOB CARD REF NO\",");
                sb.Append("                'Air' AS \"BIZ_TYPE\",");
                sb.Append("                AIR.AIRLINE_NAME LINE,");
                sb.Append("                ''  AS \"VOYAGE\",");
                sb.Append("                AOO.PORT_ID AS \"POL\",");
                sb.Append("                TO_CHAR(JA.ETD_DATE, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                 AGNT.AGENT_NAME AS  \"POL Agent\",");
                sb.Append("                DECODE(JA.Cargo_Type, '1', 'KGS', '2', 'ULD') AS \"cargo type\",");
                sb.Append("                COMMGRP.COMMODITY_GROUP_CODE AS \"Commodity Group\",");
                sb.Append("                SUM(NVL(JACONT.Chargeable_Weight, 0)) AS \"Net Wt\",");
                sb.Append("                SUM(NVL(JACONT.GROSS_WEIGHT, 0)) AS \"Gross Wt\",");
                sb.Append("                SUM(NVL(JACONT.VOLUME_IN_CBM, 0)) AS \"Volume\"");
                sb.Append("  FROM JOB_CARD_TRN    JA,");
                //sb.Append("       USER_MST_TBL            UMT,")
                // sb.Append("       PORT_MST_TBL            PMT,")
                sb.Append("       CUSTOMER_MST_TBL        CUST,");
                sb.Append("       JOB_TRN_CONT    JACONT,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       AIRLINE_MST_TBL         AIR,");
                sb.Append("       AGENT_MST_TBL           AGNT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMMGRP");
                sb.Append(" WHERE JA.HBL_HAWB_REF_NO IS NOT NULL");
                sb.Append("   AND JA.JOB_CARD_TRN_PK = JACONT.JOB_CARD_TRN_FK");
                sb.Append("   AND AOO.PORT_MST_PK = JA.PORT_MST_POL_FK");
                sb.Append("   AND AOD.PORT_MST_PK = JA.PORT_MST_POD_FK");
                sb.Append("   AND AIR.AIRLINE_MST_PK = JA.CARRIER_MST_FK(+) AND JA.BUSINESS_TYPE=1 ");
                sb.Append("   AND JA.CONSIGNEE_CUST_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("   AND JA.Pol_Agent_Mst_Fk = AGNT.AGENT_MST_PK(+)");
                sb.Append("   AND COMMGRP.COMMODITY_GROUP_PK = JA.Commodity_Group_Fk");
                //sb.Append("   AND ((UMT.DEFAULT_LOCATION_FK = 1301 AND JA.JC_AUTO_MANUAL = 0) OR")
                //sb.Append("       (AOD.LOCATION_MST_FK = 1301 AND JA.JC_AUTO_MANUAL = 1))")
                sb.Append("   AND AOD.LOCATION_MST_FK = " + LocFk + "");

                sb.Append("   AND JA.JOB_CARD_TRN_PK = JACONT.JOB_CARD_TRN_FK");
                //sb.Append("   AND JA.CREATED_BY_FK = UMT.USER_MST_PK(+)")
                sb.Append("   AND JACONT.JOB_TRN_CONT_PK NOT IN");
                sb.Append("       (SELECT DODTL.JOB_CARD_AIR_IMP_CONT_FK");
                sb.Append("          FROM DELIVERY_ORDER_MST_TBL DO,");
                sb.Append("               DELIVERY_ORDER_DTL_TBL DODTL,");
                sb.Append("               JOB_CARD_TRN   JA");
                sb.Append("         WHERE DO.DELIVERY_ORDER_PK = DODTL.DELIVERY_ORDER_FK");
                sb.Append("           AND DO.JOB_CARD_AIR_MST_FK = JA.JOB_CARD_TRN_PK)");
                sb.Append(strCondition);
                sb.Append(" Group By JA.JOB_CARD_TRN_PK,");
                sb.Append("          JA.JOBCARD_REF_NO,");
                sb.Append("          AIR.AIRLINE_NAME,");
                sb.Append("          AOO.PORT_ID,");
                sb.Append("          JA.ETD_DATE,");
                sb.Append("          JA.Cargo_Type,");
                sb.Append("          COMMGRP.COMMODITY_GROUP_CODE,");
                sb.Append("             AGNT.AGENT_NAME)");
                sb.Append(" ORDER BY NVL(ETA_DATE,'01/01/0001') DESC");
                sb.Append("");

                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";

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
                //sb.Append(" ORDER BY JA.JOBCARD_REF_NO DESC")
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (ExportExcel == 0)
                {
                    strSQL += " )q )  WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "AIR grid"

        #region "AIR SEA Both grid"

        //Sushama for CR-Jan06_PTS_ List Of Job card Pending For DO
        /// <summary>
        /// Fetches the air sea grid.
        /// </summary>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public object FetchAirSeaGrid(Int32 TotalPage = 0, Int32 CurrentPage = 0, Int32 LocFk = 0, string FromDt = "", string ToDt = "", string ETDDt = "", string CustName = "", string VslName = "", Int32 ExportExcel = 0, Int32 flag = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And J.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And J.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }

                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CUST.CUSTOMER_NAME = '" + CustName + "'";
                }

                sb.Append("SELECT HIDDEN,\"JOB CARD REF NO\",\"BIZ_TYPE\",LINE,VOYAGE,POL,ETA_DATE,\"POL Agent\",\"cargo type\",\"Commodity Group\",\"Net Wt\",\"Gross Wt\",\"Volume\" FROM (SELECT DISTINCT J.JOB_CARD_TRN_PK AS \"HIDDEN\",");
                sb.Append("                J.JOBCARD_REF_NO AS \"JOB CARD REF NO\",");
                sb.Append("                J.JOBCARD_DATE,");
                sb.Append("                'Air' AS \"BIZ_TYPE\",");
                sb.Append("                AIR.AIRLINE_NAME LINE,");
                sb.Append("                ''  AS \"VOYAGE\",");
                sb.Append("                AOO.PORT_ID AS \"POL\",");
                sb.Append("                TO_CHAR(J.ETD_DATE, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                 AGNT.AGENT_NAME AS  \"POL Agent\",");
                //sb.Append("                 ''  AS ""cargo type"",")
                sb.Append("                DECODE(J.Cargo_Type, '1', 'KGS', '2', 'ULD')\"cargo type\",");
                sb.Append("                COMMGRP.COMMODITY_GROUP_CODE AS \"Commodity Group\",");
                sb.Append("                SUM(NVL(JACONT.Chargeable_Weight, 0)) AS \"Net Wt\",");
                sb.Append("                SUM(NVL(JACONT.GROSS_WEIGHT, 0)) AS \"Gross Wt\",");
                sb.Append("                SUM(NVL(JACONT.VOLUME_IN_CBM, 0)) AS \"Volume\"");
                sb.Append("  FROM JOB_CARD_TRN    J,");
                //sb.Append("       USER_MST_TBL            UMT,")
                // sb.Append("       PORT_MST_TBL            PMT,")
                sb.Append("       CUSTOMER_MST_TBL        CUST,");
                sb.Append("       JOB_TRN_CONT          JACONT,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       AIRLINE_MST_TBL         AIR,");
                sb.Append("       AGENT_MST_TBL           AGNT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMMGRP");
                sb.Append(" WHERE J.HBL_HAWB_REF_NO IS NOT NULL");
                sb.Append("   AND J.JOB_CARD_TRN_PK = JACONT.JOB_CARD_TRN_FK");
                sb.Append("   AND AOO.PORT_MST_PK = J.PORT_MST_POL_FK");
                sb.Append("   AND AOD.PORT_MST_PK = J.PORT_MST_POD_FK");
                sb.Append("   AND AIR.AIRLINE_MST_PK = J.CARRIER_MST_FK(+)  AND J.BUSINESS_TYPE=1");
                sb.Append("   AND J.CONSIGNEE_CUST_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("   AND J.Pol_Agent_Mst_Fk = AGNT.AGENT_MST_PK(+)");
                sb.Append("   AND COMMGRP.COMMODITY_GROUP_PK = J.Commodity_Group_Fk");
                //sb.Append("   AND ((UMT.DEFAULT_LOCATION_FK = 1301 AND J.JC_AUTO_MANUAL = 0) OR")
                //sb.Append("       (AOD.LOCATION_MST_FK = 1301 AND J.JC_AUTO_MANUAL = 1))")
                sb.Append("   AND AOD.LOCATION_MST_FK = " + LocFk + "");

                sb.Append("   AND J.JOB_CARD_TRN_PK = JACONT.JOB_CARD_TRN_FK");
                //sb.Append("   AND J.CREATED_BY_FK = UMT.USER_MST_PK(+)")
                sb.Append("   AND JACONT.JOB_TRN_CONT_PK NOT IN");
                sb.Append("       (SELECT DODTL.JOB_CARD_AIR_IMP_CONT_FK");
                sb.Append("          FROM DELIVERY_ORDER_MST_TBL DO,");
                sb.Append("               DELIVERY_ORDER_DTL_TBL DODTL,");
                sb.Append("               JOB_CARD_TRN   JA");
                sb.Append("         WHERE DO.DELIVERY_ORDER_PK = DODTL.DELIVERY_ORDER_FK");
                sb.Append("           AND DO.JOB_CARD_AIR_MST_FK = J.JOB_CARD_TRN_PK)");
                sb.Append(strCondition);
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append("   AND AIR.AIRLINE_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(J.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                sb.Append(" Group By J.JOB_CARD_TRN_PK,");
                sb.Append("          J.JOBCARD_DATE,");
                sb.Append("          J.JOBCARD_REF_NO,");
                sb.Append("          J.JOBCARD_DATE,");
                sb.Append("          AIR.AIRLINE_NAME,");
                sb.Append("          AOO.PORT_ID,");
                sb.Append("          J.ETD_DATE,");
                sb.Append("          J.CARGO_TYPE,");
                sb.Append("          COMMGRP.COMMODITY_GROUP_CODE,");
                sb.Append("             AGNT.AGENT_NAME");
                //sb.Append(" ORDER BY J.JOBCARD_REF_NO DESC")
                sb.Append(" UNION ");
                sb.Append(" SELECT DISTINCT J.JOB_CARD_TRN_PK AS \"HIDDEN\",");
                sb.Append("                J.JOBCARD_REF_NO AS \"JOB CARD REF NO\",");
                sb.Append("                J.JOBCARD_DATE,");
                sb.Append("                'Sea' AS \"BIZ_TYPE\",");
                sb.Append("                OPR.OPERATOR_NAME LINE,");
                sb.Append("                CASE WHEN J.VESSEL_NAME IS NULL THEN");
                sb.Append("                        J.VESSEL_NAME");
                sb.Append("                 ELSE");
                sb.Append("                   (J.VESSEL_NAME || '/' || J.VOYAGE_FLIGHT_NO)");
                sb.Append("                  END AS VOYAGE,");
                sb.Append("                POL.PORT_ID AS \"POL\",");
                sb.Append("                TO_CHAR(J.Arrival_Date, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                AGNT.AGENT_NAME AS \"POL Agent\",");
                sb.Append("                DECODE(J.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC')cargotype,");
                sb.Append("                COMMGRP.COMMODITY_GROUP_CODE AS \"Commodity Group\",");
                sb.Append("                SUM(NVL(JSCONT.NET_WEIGHT, 0)) AS \"Net Wt\",");
                sb.Append("                SUM(NVL(JSCONT.GROSS_WEIGHT, 0)) AS \"Gross Wt\",");
                sb.Append("                SUM(NVL(JSCONT.VOLUME_IN_CBM, 0)) AS \"Volume\"");
                sb.Append("  FROM JOB_CARD_TRN    J,");
                sb.Append("       CUSTOMER_MST_TBL        CUST,");
                sb.Append("       JOB_TRN_CONT          JSCONT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMMGRP,");
                sb.Append("       Operator_Mst_Tbl       OPR,");
                sb.Append("       AGENT_MST_TBL           AGNT");
                sb.Append(" WHERE J.HBL_HAWB_REF_NO IS NOT NULL");
                sb.Append("   AND J.JOB_CARD_TRN_PK = JSCONT.JOB_CARD_TRN_FK  ");
                sb.Append("   AND J.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND J.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND J.COMMODITY_GROUP_FK = COMMGRP.COMMODITY_GROUP_PK");
                sb.Append("   AND J.POL_AGENT_MST_FK = AGNT.AGENT_MST_PK(+)");
                sb.Append("   AND J.CARRIER_MST_FK=OPR.OPERATOR_MST_PK  AND J.BUSINESS_TYPE=2");
                // sb.Append("   AND ((UMT.DEFAULT_LOCATION_FK=1301 AND J.JC_AUTO_MANUAL = 0) OR")
                sb.Append("       AND POD.location_mst_fk  =" + LocFk + "");
                sb.Append("   AND CUST.CUSTOMER_MST_PK = J.CONSIGNEE_CUST_MST_FK(+)");
                sb.Append("   AND JSCONT.JOB_TRN_CONT_PK NOT IN");
                sb.Append("       (SELECT DODTL.JOB_CARD_SEA_IMP_CONT_FK");
                sb.Append("          FROM DELIVERY_ORDER_MST_TBL DO,");
                sb.Append("               DELIVERY_ORDER_DTL_TBL DODTL,");
                sb.Append("               JOB_CARD_TRN   JS");
                sb.Append("         WHERE DO.DELIVERY_ORDER_PK = DODTL.DELIVERY_ORDER_FK");
                sb.Append("           AND DO.JOB_CARD_MST_FK = J.JOB_CARD_TRN_PK");
                sb.Append("           AND do.biz_type=2)");
                sb.Append(strCondition);
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append("   And J.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(J.Arrival_Date) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                sb.Append(" GROUP BY J.JOB_CARD_TRN_PK,");
                sb.Append("          J.JOBCARD_DATE,");
                sb.Append("          J.JOBCARD_REF_NO,");
                sb.Append("          J.VESSEL_NAME,");
                sb.Append("          J.VOYAGE_FLIGHT_NO,");
                sb.Append("          POL.PORT_ID,");
                sb.Append("          J.Arrival_Date,");
                sb.Append("          AGNT.AGENT_NAME,");
                sb.Append("          J.CARGO_TYPE,");
                sb.Append("          OPR.OPERATOR_NAME,");
                sb.Append("          COMMGRP.COMMODITY_GROUP_CODE) SQ ORDER BY NVL(SQ.ETA_DATE,'01/01/0001') DESC");

                strSQL = " select count(*) from (";
                strSQL += sb.ToString() + ")";

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
                //sb.Append(" ORDER BY JOBCARD_REF_NO DESC")
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (ExportExcel == 0)
                {
                    strSQL += " )q )  WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "AIR SEA Both grid"

        #region "Sea Print Function For Pending "

        /// <summary>
        /// Fetches the sea report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="cargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchSeaReport(Int32 LocFk = 0, string CustName = "", string VslName = "", string FromDt = "", string ToDt = "", string ETDDt = "", int cargoType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT DISTINCT JS.JOB_CARD_TRN_PK,");
                sb.Append("                JS.JOBCARD_REF_NO,");
                sb.Append("                'Sea' AS \"BIZ_TYPE\",");
                sb.Append("                JS.JOBCARD_DATE,");
                sb.Append("                OPR.OPERATOR_NAME,");
                sb.Append("                CASE WHEN JS.VESSEL_NAME IS NULL THEN");
                sb.Append("                        JS.VESSEL_NAME");
                sb.Append("                 ELSE");
                sb.Append("                   (JS.VESSEL_NAME || '/' || JS.VOYAGE_FLIGHT_NO)");
                sb.Append("                  END AS VOYAGE,");
                sb.Append("                POL.PORT_ID POL,");
                sb.Append("                TO_DATE(JS.Arrival_Date, DATEFORMAT) ETA_DATE,");
                sb.Append("                AGNT.AGENT_NAME,");
                sb.Append("                DECODE(JS.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') cargotype,");
                sb.Append("                COMMGRP.COMMODITY_GROUP_CODE,");
                sb.Append("                SUM(NVL(JSCONT.NET_WEIGHT, 0)) AS \"Net Wt\",");
                sb.Append("                SUM(NVL(JSCONT.GROSS_WEIGHT, 0)) AS \"Gross Wt\",");
                sb.Append("                SUM(NVL(JSCONT.VOLUME_IN_CBM, 0)) AS \"Volume\"");
                sb.Append("  FROM JOB_CARD_TRN    JS,");
                sb.Append("       CUSTOMER_MST_TBL        CUST,");
                sb.Append("       JOB_TRN_CONT          JSCONT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       Operator_Mst_Tbl         OPR, ");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMMGRP,");
                sb.Append("       AGENT_MST_TBL           AGNT");
                sb.Append(" WHERE JS.HBL_HAWB_REF_NO IS NOT NULL");
                sb.Append("   AND JS.JOB_CARD_TRN_PK = JSCONT.JOB_CARD_TRN_FK");
                sb.Append("   AND JS.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND JS.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JS.COMMODITY_GROUP_FK = COMMGRP.COMMODITY_GROUP_PK");
                sb.Append("   AND JS.POL_AGENT_MST_FK = AGNT.AGENT_MST_PK(+)");
                sb.Append("   AND JS.CARRIER_MST_FK=OPR.Operator_Mst_Pk  AND JS.BUSINESS_TYPE=2");
                sb.Append("   AND POD.location_mst_fk = " + LocFk + "");
                sb.Append("   AND CUST.CUSTOMER_MST_PK = JS.CONSIGNEE_CUST_MST_FK(+)");
                sb.Append("   AND JSCONT.JOB_TRN_CONT_PK NOT IN");
                sb.Append("       (SELECT DODTL.JOB_CARD_SEA_IMP_CONT_FK");
                sb.Append("          FROM DELIVERY_ORDER_MST_TBL DO,");
                sb.Append("               DELIVERY_ORDER_DTL_TBL DODTL,");
                sb.Append("               JOB_CARD_TRN   JS");
                sb.Append("         WHERE DO.DELIVERY_ORDER_PK = DODTL.DELIVERY_ORDER_FK");
                sb.Append("           AND DO.JOB_CARD_MST_FK = JS.JOB_CARD_TRN_PK");
                sb.Append("          AND  do.biz_type=2)");
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CUST.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And JS.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE(JS.JOBCARD_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE(JS.JOBCARD_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JS.Arrival_Date) = TO_DATE('" + ETDDt + "',dateformat) ");
                }
                if (cargoType != 0)
                {
                    sb.Append(" And JS.CARGO_TYPE = " + cargoType + " ");
                }
                sb.Append(" GROUP BY JS.JOB_CARD_TRN_PK,");
                sb.Append("          JS.JOBCARD_REF_NO,");
                sb.Append("          JS.JOBCARD_DATE,");
                sb.Append("          OPR.OPERATOR_NAME,");
                sb.Append("          JS.VESSEL_NAME,");
                sb.Append("          JS.VOYAGE_FLIGHT_NO,");
                sb.Append("          POL.PORT_ID,");
                sb.Append("          JS.Arrival_Date,");
                sb.Append("          AGNT.AGENT_NAME,");
                sb.Append("          JS.CARGO_TYPE,");
                sb.Append("          COMMGRP.COMMODITY_GROUP_CODE");
                sb.Append(" ORDER BY NVL(ETA_DATE,'01/01/1900') DESC");
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

        #endregion "Sea Print Function For Pending "

        #region "Air Report Function For MJC Pendind for deconsolidation"

        /// <summary>
        /// Fetches the air report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <returns></returns>
        public DataSet FetchAirReport(Int32 LocFk = 0, string CustName = "", string VslName = "", string FromDt = "", string ToDt = "", string ETADt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT DISTINCT JA.JOB_CARD_TRN_PK,");
                sb.Append("                JA.JOBCARD_REF_NO,");
                sb.Append("                'Air' AS \"BIZ_TYPE\",");
                sb.Append("                JA.JOBCARD_DATE,");
                sb.Append("                AIR.AIRLINE_NAME,");
                sb.Append("                AOO.PORT_ID AOO,");
                sb.Append("                TO_CHAR(JA.ETD_DATE, DATEFORMAT) AOD_ETD,");
                sb.Append("                AGNT.AGENT_NAME,");
                sb.Append("                DECODE(JA.Cargo_Type, '1', 'KGS', '2', 'ULD') cargotype,");
                sb.Append("                COMMGRP.COMMODITY_GROUP_CODE,");
                sb.Append("                SUM(NVL(JACONT.Chargeable_Weight, 0)) AS \"Net Wt\",");
                sb.Append("                SUM(NVL(JACONT.GROSS_WEIGHT, 0)) AS \"Gross Wt\",");
                sb.Append("                SUM(NVL(JACONT.VOLUME_IN_CBM, 0)) AS \"Volume\"");
                sb.Append("  FROM JOB_CARD_TRN    JA,");
                sb.Append("       USER_MST_TBL            UMT,");
                //sb.Append("       PORT_MST_TBL            PMT,")
                sb.Append("       CUSTOMER_MST_TBL        CUST,");
                sb.Append("       JOB_TRN_CONT          JACONT,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       AIRLINE_MST_TBL         AIR,");
                sb.Append("       AGENT_MST_TBL           AGNT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMMGRP");
                sb.Append(" WHERE JA.HBL_HAWB_REF_NO IS NOT NULL");
                sb.Append("   AND JA.JOB_CARD_TRN_PK = JACONT.JOB_CARD_TRN_FK");
                sb.Append("   AND AOO.PORT_MST_PK = JA.PORT_MST_POL_FK");
                sb.Append("   AND AOD.PORT_MST_PK = JA.PORT_MST_POD_FK");
                sb.Append("   AND AIR.AIRLINE_MST_PK = JA.CARRIER_MST_FK(+) AND JA.BUSINESS_TYPE=1 ");
                sb.Append("   AND JA.CONSIGNEE_CUST_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("   AND JA.Pol_Agent_Mst_Fk = AGNT.AGENT_MST_PK(+)");
                sb.Append("   AND COMMGRP.COMMODITY_GROUP_PK = JA.Commodity_Group_Fk");
                //sb.Append("   AND ((UMT.DEFAULT_LOCATION_FK = 1301 AND JA.JC_AUTO_MANUAL = 0) OR")
                //sb.Append("       (AOD.LOCATION_MST_FK = 1301 AND JA.JC_AUTO_MANUAL = 1))")
                sb.Append("   AND AOD.LOCATION_MST_FK = " + LocFk + "");
                sb.Append("   AND JA.JOB_CARD_TRN_PK = JACONT.JOB_CARD_TRN_FK");
                sb.Append("   AND JA.CREATED_BY_FK = UMT.USER_MST_PK(+)");
                sb.Append("   AND JACONT.JOB_TRN_CONT_PK NOT IN");
                sb.Append("       (SELECT DODTL.JOB_CARD_AIR_IMP_CONT_FK");
                sb.Append("          FROM DELIVERY_ORDER_MST_TBL DO,");
                sb.Append("               DELIVERY_ORDER_DTL_TBL DODTL,");
                sb.Append("               JOB_CARD_TRN   JA");
                sb.Append("         WHERE DO.DELIVERY_ORDER_PK = DODTL.DELIVERY_ORDER_FK");
                sb.Append("           AND DO.JOB_CARD_AIR_MST_FK = JA.JOB_CARD_TRN_PK)");
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And AIR.AIRLINE_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE( JA.JOBCARD_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE( JA.JOBCARD_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(JA.ETD_DATE, DATEFORMAT) = TO_DATE('" + ETADt + "',dateformat) ");
                }
                sb.Append(" Group By JA.JOB_CARD_TRN_PK,");
                sb.Append("          JA.JOBCARD_REF_NO,");
                sb.Append("           JA.JOBCARD_DATE,");
                sb.Append("          AIR.AIRLINE_NAME,");
                sb.Append("          AOO.PORT_ID,");
                sb.Append("          JA.ETD_DATE,");
                sb.Append("          JA.Cargo_Type, ");
                sb.Append("          AGNT.AGENT_NAME,");
                sb.Append("          COMMGRP.COMMODITY_GROUP_CODE");

                //sb.Append(" ORDER BY JA.JOBCARD_REF_NO DESC")
                sb.Append(" ORDER BY NVL(AOD_ETD,'01/01/0001') DESC");

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

        #endregion "Air Report Function For MJC Pendind for deconsolidation"

        #region "Air Sea Both Report Function"

        /// <summary>
        /// Fetches the air sea both report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <returns></returns>
        public DataSet FetchAirSeaBothReport(Int32 LocFk = 0, string CustName = "", string VslName = "", string FromDt = "", string ToDt = "", string ETADt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT * FROM (SELECT DISTINCT JA.JOB_CARD_TRN_PK,");
                sb.Append("                JA.JOBCARD_REF_NO,");
                sb.Append("                'Air' AS \"BIZ_TYPE\",");
                sb.Append("                JA.JOBCARD_DATE,");
                sb.Append("                AIR.AIRLINE_NAME,");
                sb.Append("                AOO.PORT_ID AOO,");
                //sb.Append("                TO_DATE(JA.ETD_DATE, DATEFORMAT) ETA_DATE,")
                //sb.Append("                JA.ETD_DATE ETA_DATE,")
                sb.Append("                TO_CHAR(JA.ETD_DATE, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                AGNT.AGENT_NAME,");
                sb.Append("                COMMGRP.COMMODITY_GROUP_CODE,");
                sb.Append("                SUM(NVL(JACONT.Chargeable_Weight, 0)) AS \"Net Wt\",");
                sb.Append("                SUM(NVL(JACONT.GROSS_WEIGHT, 0)) AS \"Gross Wt\",");
                sb.Append("                SUM(NVL(JACONT.VOLUME_IN_CBM, 0)) AS \"Volume\"");
                sb.Append("  FROM JOB_CARD_TRN    JA,");
                sb.Append("       USER_MST_TBL            UMT,");
                //sb.Append("       PORT_MST_TBL            PMT,")
                sb.Append("       CUSTOMER_MST_TBL        CUST,");
                sb.Append("       JOB_TRN_CONT          JACONT,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       AIRLINE_MST_TBL         AIR,");
                sb.Append("       AGENT_MST_TBL           AGNT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMMGRP");
                sb.Append(" WHERE JA.HBL_HAWB_REF_NO IS NOT NULL");
                sb.Append("   AND JA.JOB_CARD_TRN_PK = JACONT.JOB_CARD_TRN_FK");
                sb.Append("   AND AOO.PORT_MST_PK = JA.PORT_MST_POL_FK");
                sb.Append("   AND AOD.PORT_MST_PK = JA.PORT_MST_POD_FK");
                sb.Append("   AND AIR.AIRLINE_MST_PK = JA.CARRIER_MST_FK(+)  AND JA.BUSINESS_TYPE=1");
                sb.Append("   AND JA.CONSIGNEE_CUST_MST_FK = CUST.CUSTOMER_MST_PK");
                sb.Append("   AND JA.Pol_Agent_Mst_Fk = AGNT.AGENT_MST_PK(+)");
                sb.Append("   AND COMMGRP.COMMODITY_GROUP_PK = JA.Commodity_Group_Fk");
                //sb.Append("   AND ((UMT.DEFAULT_LOCATION_FK = 1301 AND JA.JC_AUTO_MANUAL = 0) OR")
                //sb.Append("       (AOD.LOCATION_MST_FK = 1301 AND JA.JC_AUTO_MANUAL = 1))")
                sb.Append("   AND AOD.LOCATION_MST_FK = " + LocFk + "");
                sb.Append("   AND JA.JOB_CARD_TRN_PK = JACONT.JOB_CARD_TRN_FK");
                sb.Append("   AND JA.CREATED_BY_FK = UMT.USER_MST_PK(+)");
                sb.Append("   AND JACONT.JOB_TRN_CONT_PK NOT IN");
                sb.Append("       (SELECT DODTL.JOB_CARD_AIR_IMP_CONT_FK");
                sb.Append("          FROM DELIVERY_ORDER_MST_TBL DO,");
                sb.Append("               DELIVERY_ORDER_DTL_TBL DODTL,");
                sb.Append("               JOB_CARD_TRN   JA");
                sb.Append("         WHERE DO.DELIVERY_ORDER_PK = DODTL.DELIVERY_ORDER_FK");
                sb.Append("           AND DO.JOB_CARD_AIR_MST_FK = JA.JOB_CARD_TRN_PK)");
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CUST.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And AIR.AIRLINE_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE( JA.JOBCARD_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE( JA.JOBCARD_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(JA.ETD_DATE, DATEFORMAT) = TO_DATE('" + ETADt + "',dateformat) ");
                }
                sb.Append(" Group By JA.JOB_CARD_TRN_PK,");
                sb.Append("          JA.JOBCARD_REF_NO,");
                sb.Append("           JA.JOBCARD_DATE,");
                sb.Append("          AIR.AIRLINE_NAME,");
                sb.Append("          AOO.PORT_ID,");
                sb.Append("          JA.ETD_DATE,");
                sb.Append("          AGNT.AGENT_NAME,");
                sb.Append("          COMMGRP.COMMODITY_GROUP_CODE");

                //sb.Append(" ORDER BY JA.JOBCARD_REF_NO DESC")
                //'
                sb.Append(" UNION ");
                //'
                sb.Append(" SELECT DISTINCT JS.JOB_CARD_TRN_PK,");
                sb.Append("                JS.JOBCARD_REF_NO,");
                sb.Append("                'Sea' AS \"BIZ_TYPE\",");
                sb.Append("                JS.JOBCARD_DATE,");
                sb.Append("                OPR.OPERATOR_NAME,");
                //sb.Append("                CASE WHEN JS.VESSEL_NAME IS NULL THEN")
                //sb.Append("                        JS.VESSEL_NAME")
                //sb.Append("                 ELSE")
                //sb.Append("                   (JS.VESSEL_NAME || '/' || JS.VOYAGE)")
                //sb.Append("                  END AS VESSEL_FLIGHT,")
                sb.Append("                POL.PORT_ID POL,");
                //sb.Append("                JS.Arrival_Date ETA_DATE,")
                sb.Append("                TO_CHAR(JS.Arrival_Date, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                AGNT.AGENT_NAME,");
                // sb.Append("                DECODE(JS.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') cargotype,")
                sb.Append("                COMMGRP.COMMODITY_GROUP_CODE,");
                sb.Append("                SUM(NVL(JSCONT.NET_WEIGHT, 0)) AS \"Net Wt\",");
                sb.Append("                SUM(NVL(JSCONT.GROSS_WEIGHT, 0)) AS \"Gross Wt\",");
                sb.Append("                SUM(NVL(JSCONT.VOLUME_IN_CBM, 0)) AS \"Volume\"");
                sb.Append("  FROM JOB_CARD_TRN    JS,");
                sb.Append("       CUSTOMER_MST_TBL        CUST,");
                sb.Append("       JOB_TRN_CONT           JSCONT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       Operator_Mst_Tbl         OPR, ");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMMGRP,");
                sb.Append("       AGENT_MST_TBL           AGNT");
                sb.Append(" WHERE JS.HBL_HAWB_REF_NO IS NOT NULL");
                sb.Append("   AND JS.JOB_CARD_TRN_PK = JSCONT.JOB_CARD_TRN_FK");
                sb.Append("   AND JS.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND JS.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JS.COMMODITY_GROUP_FK = COMMGRP.COMMODITY_GROUP_PK");
                sb.Append("   AND JS.POL_AGENT_MST_FK = AGNT.AGENT_MST_PK(+)");
                sb.Append("   AND JS.CARRIER_MST_FK=OPR.OPERATOR_MST_PK AND JS.BUSINESS_TYPE=2");
                sb.Append("   AND POD.location_mst_fk = " + LocFk + "");
                sb.Append("   AND CUST.CUSTOMER_MST_PK = JS.CONSIGNEE_CUST_MST_FK(+)");
                sb.Append("   AND JSCONT.JOB_TRN_CONT_PK NOT IN");
                sb.Append("       (SELECT DODTL.JOB_CARD_SEA_IMP_CONT_FK");
                sb.Append("          FROM DELIVERY_ORDER_MST_TBL DO,");
                sb.Append("               DELIVERY_ORDER_DTL_TBL DODTL,");
                sb.Append("               JOB_CARD_TRN   JS");
                sb.Append("         WHERE DO.DELIVERY_ORDER_PK = DODTL.DELIVERY_ORDER_FK");
                sb.Append("           AND DO.JOB_CARD_MST_FK = JS.JOB_CARD_TRN_PK");
                sb.Append("          AND  do.biz_type=2)");
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CUST.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And JS.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE(JS.JOBCARD_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE(JS.JOBCARD_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(JS.Arrival_Date) = TO_DATE('" + ETADt + "',dateformat) ");
                }
                sb.Append(" GROUP BY JS.JOB_CARD_TRN_PK,");
                sb.Append("          JS.JOBCARD_REF_NO,");
                sb.Append("          JS.JOBCARD_DATE,");
                sb.Append("          OPR.OPERATOR_NAME,");
                sb.Append("          JS.VESSEL_NAME,");
                sb.Append("          JS.VOYAGE_FLIGHT_NO,");
                sb.Append("          POL.PORT_ID,");
                sb.Append("         JS.Arrival_Date,");
                sb.Append("          AGNT.AGENT_NAME,");
                sb.Append("          JS.CARGO_TYPE,");
                sb.Append("          COMMGRP.COMMODITY_GROUP_CODE )");
                sb.Append(" ORDER BY NVL(ETA_DATE,'01/01/0001') DESC");
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

        #endregion "Air Sea Both Report Function"

        //------JC Pending for HBL Allocation---

        #region "JobCard Pending For HBL Allocation Grid Function"

        /// <summary>
        /// Fetches the j cfor HBL.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="expExcel">The exp excel.</param>
        /// <returns></returns>
        public DataSet FetchJCforHBL(Int32 Business = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 expExcel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (Business == 2)
                {
                    sb.Append("SELECT BST.BOOKING_MST_PK BOOKING_PK,");
                    sb.Append("       BST.BOOKING_REF_NO,");
                    sb.Append("       JCSET.JOB_CARD_TRN_PK JOBCARD_PK,");
                    sb.Append("       JCSET.JOBCARD_REF_NO,");
                    sb.Append("       JCSET.JOBCARD_DATE JOBCARD_DATE,");
                    sb.Append("       TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       POL.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN VT.VOYAGE IS NULL THEN");
                    sb.Append("          VVT.VESSEL_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                    sb.Append("       END VOYAGE,");
                    sb.Append("       TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                    sb.Append("       TO_CHAR(JCSET.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                    sb.Append("       DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       CGMT.COMMODITY_GROUP_CODE");
                    sb.Append("  FROM JOB_CARD_TRN    JCSET,");
                    sb.Append("       BOOKING_MST_TBL         BST,");
                    sb.Append("       CUSTOMER_MST_TBL        CMT,");
                    sb.Append("       PORT_MST_TBL            POL,");
                    sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                    sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                    sb.Append(" WHERE BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK");
                    sb.Append("   AND CMT.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                    sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                    sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                    sb.Append("   AND VT.VOYAGE_TRN_PK(+) = JCSET.VOYAGE_TRN_FK AND BST.BUSINESS_TYPE=2");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK = JCSET.COMMODITY_GROUP_FK");
                    sb.Append("   AND JCSET.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND POL.LOCATION_MST_FK =  " + LocFk + "");
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strCondition = strCondition + " And JCSET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strCondition = strCondition + " And JCSET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(ETDDt))
                    {
                        strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(CustName))
                    {
                        strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                    }
                    if (!string.IsNullOrEmpty(VslName))
                    {
                        sb.Append(" And VVT.VESSEL_NAME = '" + VslName + "'");
                    }
                }
                else
                {
                    sb.Append("SELECT BAT.BOOKING_MST_PK BOOKING_PK,");
                    sb.Append("       BAT.BOOKING_REF_NO,");
                    sb.Append("       JCAET.JOB_CARD_TRN_PK JOBCARD_PK,");
                    sb.Append("       JCAET.JOBCARD_REF_NO,");
                    sb.Append("       JCAET.JOBCARD_DATE JOBCARD_DATE,");
                    sb.Append("       TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       AOO.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN JCAET.VOYAGE_FLIGHT_NO IS NULL THEN");
                    sb.Append("          AMT.AIRLINE_ID");
                    sb.Append("         ELSE");
                    sb.Append("          (AMT.AIRLINE_ID || '/' || JCAET.VOYAGE_FLIGHT_NO)");
                    sb.Append("       END VOYAGE,");
                    sb.Append("       TO_CHAR(JCAET.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                    sb.Append("       TO_CHAR(JCAET.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                    sb.Append("       '' CARGO_TYPE,");
                    sb.Append("       CGMT.COMMODITY_GROUP_CODE");
                    sb.Append("  FROM JOB_CARD_TRN   JCAET,");
                    sb.Append("       BOOKING_MST_TBL         BAT,");
                    sb.Append("       CUSTOMER_MST_TBL        CMT,");
                    sb.Append("       PORT_MST_TBL            AOO,");
                    sb.Append("       AIRLINE_MST_TBL         AMT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                    sb.Append(" WHERE BAT.BOOKING_MST_PK = JCAET.BOOKING_MST_FK");
                    sb.Append("   AND CMT.CUSTOMER_MST_PK = JCAET.SHIPPER_CUST_MST_FK");
                    sb.Append("   AND AOO.PORT_MST_PK = BAT.PORT_MST_POL_FK");
                    sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK BAT.BUSINESS_TYPE=1");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK = JCAET.COMMODITY_GROUP_FK");
                    sb.Append("   AND JCAET.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND AOO.LOCATION_MST_FK = " + LocFk + "");
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strCondition = strCondition + " And JCAET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strCondition = strCondition + " And JCAET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(ETDDt))
                    {
                        strCondition = strCondition + " And TO_DATE(JCAET.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(CustName))
                    {
                        strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                    }
                    if (!string.IsNullOrEmpty(VslName))
                    {
                        sb.Append(" And AMT.AIRLINE_NAME = '" + VslName + "'");
                    }
                }
                sb.Append(strCondition);

                strSQL = " select count(QRY.BOOKING_PK) from (";
                strSQL += sb.ToString() + ")QRY";

                //TotalRecords = CType(objWF.ExecuteScaler(strSQL), Int32)
                TotalRecords = (Int32)objWF.GetDataSet(strSQL).Tables[0].Rows[0][0];
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

                sb.Append(" order by TO_DATE(JOBCARD_DATE) desc ");
                strSQL = " SELECT * FROM (SELECT ROWNUM SLNO, q.* FROM(";
                strSQL += sb.ToString();
                if (expExcel == 0)
                {
                    strSQL += " )q ) WHERE SLNO Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " )q )";
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

        #endregion "JobCard Pending For HBL Allocation Grid Function"

        #region "JobCard Pending For HBL Allocation Print Function"

        /// <summary>
        /// Fetches the job card pending for HBL print.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchJobCardPendingForHBLPrint(Int32 Business = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (Business == 2)
                {
                    sb.Append("SELECT BST.BOOKING_MST_PK BOOKING_PK,");
                    sb.Append("       BST.BOOKING_REF_NO,");
                    sb.Append("       JCSET.JOB_CARD_SEA_EXP_PK JOBCARD_PK,");
                    sb.Append("       JCSET.JOBCARD_REF_NO,");
                    sb.Append("       JCSET.JOBCARD_DATE JOBCARD_DATE,");
                    sb.Append("       TO_DATE(BST.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       POL.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN VT.VOYAGE IS NULL THEN");
                    sb.Append("          VVT.VESSEL_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                    sb.Append("       END VOYAGE,");
                    sb.Append("       TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                    sb.Append("       TO_CHAR(JCSET.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                    sb.Append("       DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       CGMT.COMMODITY_GROUP_CODE");
                    sb.Append("  FROM JOB_CARD_TRN    JCSET,");
                    sb.Append("       BOOKING_MST_TBL         BST,");
                    sb.Append("       CUSTOMER_MST_TBL        CMT,");
                    sb.Append("       PORT_MST_TBL            POL,");
                    sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                    sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                    sb.Append(" WHERE BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK");
                    sb.Append("   AND CMT.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                    sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                    sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                    sb.Append("   AND VT.VOYAGE_TRN_PK(+) = JCSET.VOYAGE_TRN_FK");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK = JCSET.COMMODITY_GROUP_FK");
                    sb.Append("   AND JCSET.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND POL.LOCATION_MST_FK =  " + LocFk + "");
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        sb.Append("   And JCSET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        sb.Append("   And JCSET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(ETDDt))
                    {
                        sb.Append("   And TO_DATE(JCSET.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(CustName))
                    {
                        sb.Append("   And CMT.CUSTOMER_NAME = '" + CustName + "'");
                    }
                    if (!string.IsNullOrEmpty(VslName))
                    {
                        sb.Append(" And VVT.VESSEL_NAME = '" + VslName + "'");
                    }
                }
                else
                {
                    sb.Append("SELECT BAT.BOOKING_MST_PK BOOKING_PK,");
                    sb.Append("       BAT.BOOKING_REF_NO,");
                    sb.Append("       JCAET.JOB_CARD_TRN_PK JOBCARD_PK,");
                    sb.Append("       JCAET.JOBCARD_REF_NO,");
                    sb.Append("       JCAET.JOBCARD_DATE JOBCARD_DATE,");
                    sb.Append("       TO_DATE(BAT.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       AOO.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN JCAET.VOYAGE_FLIGHT_NO IS NULL THEN");
                    sb.Append("          AMT.AIRLINE_ID");
                    sb.Append("         ELSE");
                    sb.Append("          (AMT.AIRLINE_ID || '/' || JCAET.VOYAGE_FLIGHT_NO)");
                    sb.Append("       END VOYAGE,");
                    sb.Append("       TO_CHAR(JCAET.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                    sb.Append("       TO_CHAR(JCAET.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                    sb.Append("       '' CARGO_TYPE,");
                    sb.Append("       CGMT.COMMODITY_GROUP_CODE");
                    sb.Append("  FROM JOB_CARD_TRN    JCAET,");
                    sb.Append("       BOOKING_MST_TBL         BAT,");
                    sb.Append("       CUSTOMER_MST_TBL        CMT,");
                    sb.Append("       PORT_MST_TBL            AOO,");
                    sb.Append("       AIRLINE_MST_TBL         AMT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                    sb.Append(" WHERE BAT.BOOKING_MST_PK = JCAET.BOOKING_MST_FK");
                    sb.Append("   AND CMT.CUSTOMER_MST_PK = JCAET.SHIPPER_CUST_MST_FK");
                    sb.Append("   AND AOO.PORT_MST_PK = BAT.PORT_MST_POL_FK");
                    sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK AND BAT.BUSINESS_TYPE=1");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK = JCAET.COMMODITY_GROUP_FK");
                    sb.Append("   AND JCAET.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND AOO.LOCATION_MST_FK = " + LocFk + "");
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        sb.Append("   And JCAET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        sb.Append("   And JCAET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(ETDDt))
                    {
                        sb.Append("   And TO_DATE(JCAET.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(CustName))
                    {
                        sb.Append("   And CMT.CUSTOMER_NAME = '" + CustName + "'");
                    }
                    if (!string.IsNullOrEmpty(VslName))
                    {
                        sb.Append(" And AMT.AIRLINE_NAME = '" + VslName + "'");
                    }
                }

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

        #endregion "JobCard Pending For HBL Allocation Print Function"
    }
}