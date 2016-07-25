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
    public class Cls_PendingBkgForStuffing : CommonFeatures
    {
        #region "Fetch Stuffing"

        /// <summary>
        /// Fetches the stuffing.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <returns></returns>
        public DataSet FetchStuffing(Int32 LocFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 Export = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = "";
            Int32 TotalRecords = default(Int32);
            try
            {
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And JOB.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition = strCondition + " And JOB.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And BKG.SHIPMENT_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And BKG.SHIPMENT_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JOB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }

                sb.Append(" SELECT DISTINCT BKG.BOOKING_MST_PK,");
                sb.Append(" BKG.BOOKING_REF_NO,");
                sb.Append(" JOB.JOB_CARD_TRN_PK,");
                sb.Append(" JOB.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(JOB.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                sb.Append("       TO_DATE(BKG.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                sb.Append(" CMT.CUSTOMER_NAME,");
                sb.Append(" POD.PORT_ID, ");
                sb.Append("  CASE WHEN JOB.VESSEL_NAME IS NULL THEN ");
                sb.Append(" JOB.VESSEL_NAME");
                sb.Append(" ELSE JOB.VESSEL_NAME || '/' || JOB.VOYAGE_FLIGHT_NO ");
                sb.Append(" END \"VESSEL/VOYAGE\", ");

                sb.Append(" JOB.ETD_DATE ETD, ");
                sb.Append(" JOB.ETA_DATE ETA, ");
                sb.Append(" DECODE(BKG.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 3, 'BBC') Cargo_Type, ");
                sb.Append(" CONT.CONTAINER_TYPE_MST_ID, ");
                sb.Append(" JOB_CONT.CONTAINER_NUMBER, ");
                sb.Append(" CGP.COMMODITY_GROUP_CODE, ");
                sb.Append(" JOB.STUFF_LOC ");

                sb.Append(" FROM BOOKING_MST_TBL     BKG,");
                sb.Append(" JOB_CARD_TRN             JOB,");
                sb.Append(" CUSTOMER_MST_TBL        CMT,");
                sb.Append(" PORT_MST_TBL            POL,");
                sb.Append(" PORT_MST_TBL            POD,");
                sb.Append(" CONTAINER_TYPE_MST_TBL  CONT,");
                sb.Append(" JOB_TRN_CONT    JOB_CONT,");
                sb.Append(" COMMODITY_GROUP_MST_TBL CGP ");

                sb.Append(" WHERE BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK ");
                sb.Append(" AND BKG.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
                sb.Append(" AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                sb.Append(" AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                sb.Append(" AND CONT.CONTAINER_TYPE_MST_PK = JOB_CONT.CONTAINER_TYPE_MST_FK ");
                sb.Append(" AND JOB.JOB_CARD_TRN_PK = JOB_CONT.JOB_CARD_TRN_FK  ");
                sb.Append(" AND JOB.COMMODITY_GROUP_FK = CGP.COMMODITY_GROUP_PK ");
                sb.Append(" AND BKG.STATUS <> 3  ");
                sb.Append(" AND POL.LOCATION_MST_FK = " + LocFk);
                sb.Append(" AND BKG.CARGO_TYPE IN (1,2) ");
                sb.Append(" AND JOB.STF_DATE IS NULL ");
                sb.Append(" AND JOB.STF = 0 ");
                sb.Append(strCondition);

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

                sb.Append("  ORDER BY TO_DATE(JOBCARD_DATE) DESC, JOB.JOBCARD_REF_NO DESC ");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (Export == 1)
                {
                    strSQL += " )q ) ";
                }
                else
                {
                    strSQL += " )q ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Fetch Stuffing"

        #region "Fetch Report"

        /// <summary>
        /// Fetches the stuffing report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchStuffingReport(Int32 LocFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            try
            {
                sb.Append(" SELECT DISTINCT BKG.BOOKING_MST_PK,");
                sb.Append(" BKG.BOOKING_REF_NO,");
                sb.Append(" JOB.JOB_CARD_TRN_PK,");
                sb.Append(" JOB.JOBCARD_REF_NO,");
                sb.Append(" TO_CHAR(JOB.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("  TO_CHAR(BKG.SHIPMENT_DATE,DATEFORMAT) SHIPMENT_DATE,");
                sb.Append(" CMT.CUSTOMER_NAME,");
                sb.Append(" POD.PORT_ID, ");
                sb.Append("  CASE WHEN JOB.VOYAGE_FLIGHT_NO IS NULL THEN ");
                sb.Append(" JOB.VESSEL_NAME");
                sb.Append(" ELSE JOB.VESSEL_NAME || '/' || JOB.VOYAGE_FLIGHT_NO ");
                sb.Append(" END \"VESSEL/VOYAGE\", ");
                sb.Append(" JOB.ETD_DATE ETD, ");
                sb.Append(" JOB.ETA_DATE ETA, ");
                sb.Append(" DECODE(BKG.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 3, 'BBC') Cargo_Type, ");
                sb.Append(" CONT.CONTAINER_TYPE_MST_ID, ");
                sb.Append(" JOB_CONT.CONTAINER_NUMBER, ");
                sb.Append(" CGP.COMMODITY_GROUP_CODE, ");
                sb.Append(" JOB.STUFF_LOC ");

                sb.Append(" FROM BOOKING_MST_TBL     BKG,");
                sb.Append(" JOB_CARD_TRN             JOB,");
                sb.Append(" CUSTOMER_MST_TBL        CMT,");
                sb.Append(" PORT_MST_TBL            POL,");
                sb.Append(" PORT_MST_TBL            POD,");
                sb.Append(" CONTAINER_TYPE_MST_TBL  CONT,");
                sb.Append(" JOB_TRN_CONT            JOB_CONT,");
                sb.Append(" COMMODITY_GROUP_MST_TBL CGP ");

                sb.Append(" WHERE BKG.BOOKING_MST_PK = JOB.BOOKING_MST_FK ");
                sb.Append(" AND BKG.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
                sb.Append(" AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK ");
                sb.Append(" AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK ");
                sb.Append(" AND CONT.CONTAINER_TYPE_MST_PK = JOB_CONT.CONTAINER_TYPE_MST_FK ");
                sb.Append(" AND JOB.JOB_CARD_TRN_PK = JOB_CONT.JOB_CARD_TRN_FK  ");
                sb.Append(" AND JOB.COMMODITY_GROUP_FK = CGP.COMMODITY_GROUP_PK ");
                sb.Append(" AND BKG.STATUS <> 3  ");
                sb.Append(" AND POL.LOCATION_MST_FK = " + LocFk);
                sb.Append(" AND BKG.CARGO_TYPE IN (1,2) ");
                sb.Append(" AND JOB.STF_DATE IS NULL ");
                sb.Append(" AND JOB.STF = 0 ");

                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And JOB.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append("  And JOB.VOYAGE_FLIGHT_NO = '" + Voyage + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And BKG.SHIPMENT_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And BKG.SHIPMENT_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JOB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat) ");
                }
                sb.Append(" ORDER BY TO_DATE(JOBCARD_DATE) DESC, JOB.JOBCARD_REF_NO DESC ");

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

        #endregion "Fetch Report"
    }
}