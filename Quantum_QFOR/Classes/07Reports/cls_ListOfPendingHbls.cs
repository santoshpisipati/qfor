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
    public class cls_ListOfPendingHbls : CommonFeatures
    {
        #region "Function For Fetching Draft HBL"

        /// <summary>
        /// Fetches the sea data.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchSeaData(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 Export = 0, Int32 CargoType = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And JOB_EXP.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition = strCondition + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }
                if (CargoType > 0)
                {
                    strCondition = strCondition + " AND BOOK.CARGO_TYPE  = " + CargoType + "";
                }

                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       HBL.HBL_DATE SHIP_DATE,");
                sb.Append("       'Sea' BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append("  HBL.ETD_DATE ETD, ");
                sb.Append("  HBL.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE, ");
                sb.Append("       2 BIZTYPE ");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND JOB_EXP.BUSINESS_TYPE=2 ");
                sb.Append("   AND HBL.HBL_STATUS = 0");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
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
                sb.Append(" order by HBL.HBL_DATE desc, HBL.HBL_REF_NO desc ");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (Export == 1)
                {
                    strSQL += " )q )";
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

        #endregion "Function For Fetching Draft HBL"

        #region "Function For Fetching Draft HAWB"

        /// <summary>
        /// Fetches the air data.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="Flightno">The flightno.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <returns></returns>
        public DataSet FetchAirData(Int32 CustPK = 0, Int32 LocFk = 0, string Airline = "", string Flightno = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 Export = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    strCondition = strCondition + " And AMT.AIRLINE_NAME = '" + Airline + "'";
                }
                if (!string.IsNullOrEmpty(Flightno))
                {
                    strCondition = strCondition + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Flightno + "'";
                }
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'Air' BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (AMT.AIRLINE_NAME || '/' || JOB_EXP.VOYAGE_FLIGHT_NO)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" HAWB.ETD_DATE ETD, ");
                sb.Append(" HAWB.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE, ");
                sb.Append("       1 BIZTYPE ");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND JOB_EXP.BUSINESS_TYPE=1 ");
                sb.Append("   AND HAWB.HAWB_STATUS = 0");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
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
                sb.Append(" order by HAWB.HAWB_DATE desc, HAWB.HAWB_REF_NO desc ");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (Export == 1)
                {
                    strSQL += " )q )";
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

        #endregion "Function For Fetching Draft HAWB"

        #region "Function For Fetching Draft HBL/HAWB"

        /// <summary>
        /// Fetches the sea air data.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchSeaAirData(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 Export = 0, Int32 CargoType = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            string strCondition1 = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And JOB_EXP.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition = strCondition + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }

                if (Convert.ToString(CargoType) != "0")
                {
                    strCondition = strCondition + " AND BOOK.CARGO_TYPE = " + CargoType + "";
                }
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'Sea' BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" HBL.ETD_DATE ETD, ");
                sb.Append(" HBL.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE, ");
                sb.Append("       2 BIZTYPE ");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND JOB_EXP.BUSINESS_TYPE=2 ");
                sb.Append("   AND HBL.HBL_STATUS = 0");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition);

                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition1 = strCondition1 + " AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition1 = strCondition1 + " AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition1 = strCondition1 + " AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition1 = strCondition1 + " And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition1 = strCondition1 + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition1 = strCondition1 + " And AMT.AIRLINE_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition1 = strCondition1 + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }
                sb.Append(" UNION ");
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'Air' BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (AMT.AIRLINE_NAME || '/' || JOB_EXP.VOYAGE_FLIGHT_NO)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" HAWB.ETD_DATE ETD, ");
                sb.Append(" HAWB.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE, ");
                sb.Append("       1 BIZTYPE ");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND JOB_EXP.BUSINESS_TYPE=1 ");
                sb.Append("   AND HAWB.HAWB_STATUS = 0");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition1);

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

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, QRY.* FROM(SELECT Q.* FROM (";
                strSQL += sb.ToString();
                if (Export == 1)
                {
                    strSQL += " )q order by Q.SHIP_DATE desc, Q.SHIP_REF_NR desc)QRY) ";
                }
                else
                {
                    strSQL += " )q order by Q.SHIP_DATE desc, Q.SHIP_REF_NR desc)QRY) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Function For Fetching Draft HBL/HAWB"

        #region "Function For Printing Draft HBL"

        /// <summary>
        /// Fetches the sea report.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchSeaReport(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'Sea' BIZ_TYPE, ");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HBL.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HBL.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 0");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append(" AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append("AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And JOB_EXP.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append(" And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'");
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

        #endregion "Function For Printing Draft HBL"

        #region "Function For Printing Draft HAWB"

        /// <summary>
        /// Fetches the air report.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="FlightNo">The flight no.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchAirReport(Int32 CustPK = 0, Int32 LocFk = 0, string Airline = "", string FlightNo = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (AMT.AIRLINE_NAME || '/' || JOB_EXP.VOYAGE_FLIGHT_NO)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HAWB.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HAWB.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 0");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append("  AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append("  AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    sb.Append("  And AMT.AIRLINE_NAME = '" + Airline + "'");
                }
                if (!string.IsNullOrEmpty(FlightNo))
                {
                    sb.Append("  And JOB_EXP.VOYAGE_FLIGHT_NO = '" + FlightNo + "'");
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

        #endregion "Function For Printing Draft HAWB"

        #region "Function For Printing Draft HBL/HAWB"

        /// <summary>
        /// Fetches the sea air report.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchSeaAirReport(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT * FROM ( ");
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'Sea' BIZ_TYPE, ");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HBL.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HBL.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 0");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append(" AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append("AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And JOB_EXP.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append(" And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'");
                }
                sb.Append(" UNION ");
                sb.Append(" SELECT JOB_EXP.JOB_CARD_TRN_PK, ");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK, ");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR, ");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat) SHIP_DATE,");
                sb.Append("       'Air' BIZ_TYPE, ");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (AMT.AIRLINE_NAME || '/' || JOB_EXP.VOYAGE_FLIGHT_NO)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HAWB.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HAWB.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 0");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append("  AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append("  AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append("  And AMT.AIRLINE_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append("  And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'");
                }
                sb.Append(" ) Q order by Q.SHIP_DATE desc, Q.SHIP_REF_NR desc ");
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

        #endregion "Function For Printing Draft HBL/HAWB"

        #region "Function For Fetching Confirmed HBL"

        /// <summary>
        /// Fetches the HBL sea data.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslVoyPK">The VSL voy pk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchHBLSeaData(Int32 CustPK = 0, Int32 LocFk = 0, Int32 VslVoyPK = 0, string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 Export = 0,
        Int32 CargoType = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (VslVoyPK > 0)
                {
                    strCondition = strCondition + " AND JOB_EXP.VOYAGE_TRN_FK  = " + VslVoyPK + "";
                }
                if (CargoType > 0)
                {
                    strCondition = strCondition + " AND BOOK.CARGO_TYPE  = " + CargoType + "";
                }
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       HBL.HBL_DATE SHIP_DATE,");
                sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 2) BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" HBL.ETD_DATE ETD, ");
                sb.Append(" HBL.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND JOB_EXP.BUSINESS_TYPE=2 ");
                sb.Append("   AND HBL.HBL_STATUS = 1");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
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
                sb.Append(" order by NVL(HBL.ETD_DATE,'01/01/001') desc ");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (Export == 1)
                {
                    strSQL += " )q )";
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

        #endregion "Function For Fetching Confirmed HBL"

        #region "Function For Fetching Confirmed HAWB"

        /// <summary>
        /// Fetches the hawb air data.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <returns></returns>
        public DataSet FetchHAWBAirData(Int32 CustPK = 0, Int32 LocFk = 0, string Airline = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 Export = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    strCondition = strCondition + " And AMT.AIRLINE_NAME = '" + Airline + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition = strCondition + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       HAWB.HAWB_DATE SHIP_DATE,");
                sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 1) BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (BOOK.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" HAWB.ETD_DATE ETD, ");
                sb.Append(" HAWB.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND JOB_EXP.BUSINESS_TYPE=1 ");
                sb.Append("   AND HAWB.HAWB_STATUS = 2");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
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
                sb.Append(" order by NVL(HAWB.ETD_DATE,'01/01/001') desc ");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (Export == 1)
                {
                    strSQL += " )q )";
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

        #endregion "Function For Fetching Confirmed HAWB"

        #region "Function For Printing Confirmed HBL"

        /// <summary>
        /// Fetches the HBL sea report.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslVoyPK">The VSL voy pk.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchHBLSeaReport(Int32 CustPK = 0, Int32 LocFk = 0, Int32 VslVoyPK = 0, string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 CargoType = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       'Sea' AS BIZ_TYPE,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HBL.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HBL.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 1");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append(" AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append("AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (VslVoyPK > 0)
                {
                    sb.Append("  AND JOB_EXP.VOYAGE_TRN_FK  = " + VslVoyPK + "");
                }
                if (CargoType > 0)
                {
                    sb.Append("   AND BOOK.CARGO_TYPE  = " + CargoType + "");
                }
                sb.Append(" order by HBL.HBL_DATE desc, HBL.HBL_REF_NO desc ");
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

        #endregion "Function For Printing Confirmed HBL"

        #region "Function For Fetching Confirmed HAWB"

        /// <summary>
        /// Fetches the hawb air report.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchHAWBAirReport(Int32 CustPK = 0, Int32 LocFk = 0, string Airline = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       'Air' AS BIZ_TYPE,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HAWB.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HAWB.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 2");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append("  AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append("  AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    sb.Append("  And AMT.AIRLINE_NAME = '" + Airline + "'");
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append(" And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'");
                }
                sb.Append(" order by HAWB.HAWB_DATE desc, HAWB.HAWB_REF_NO desc ");
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

        #endregion "Function For Fetching Confirmed HAWB"

        #region "Function For Fetching Released HBL"

        /// <summary>
        /// Fetches the sea data released.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="Cargo_type">The cargo_type.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <returns></returns>
        public DataSet FetchSeaDataReleased(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string Cargo_type = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 Export = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And JOB_EXP.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition = strCondition + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }
                if (Cargo_type != "0")
                {
                    strCondition = strCondition + " AND BOOK.CARGO_TYPE = " + Cargo_type + "";
                }
                strCondition = strCondition + " AND JOB_EXP.BUSINESS_TYPE = 2 ";
                strCondition = strCondition + " AND JOB_EXP.PROCESS_TYPE = 1 ";
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'Sea' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" HBL.ETD_DATE ETD, ");
                sb.Append(" HBL.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 2");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
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
                sb.Append(" order by HBL.HBL_DATE desc, HBL.HBL_REF_NO desc ");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (Export == 1)
                {
                    strSQL += " )q )";
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

        #endregion "Function For Fetching Released HBL"

        #region "Function For Fetching Released HAWB"

        /// <summary>
        /// Fetches the air data released.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="Flightno">The flightno.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <returns></returns>
        public DataSet FetchAirDataReleased(Int32 CustPK = 0, Int32 LocFk = 0, string Airline = "", string Flightno = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 Export = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    strCondition = strCondition + " And AMT.AIRLINE_NAME = '" + Airline + "'";
                }
                if (!string.IsNullOrEmpty(Flightno))
                {
                    strCondition = strCondition + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Flightno + "'";
                }
                strCondition = strCondition + " AND JOB_EXP.BUSINESS_TYPE = 1";
                strCondition = strCondition + " AND JOB_EXP.PROCESS_TYPE = 1 ";
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'Air' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" HAWB.ETD_DATE ETD, ");
                sb.Append(" HAWB.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 1");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
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
                sb.Append(" order by HAWB.HAWB_DATE desc, HAWB.HAWB_REF_NO desc ");
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (Export == 1)
                {
                    strSQL += " )q )";
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

        #endregion "Function For Fetching Released HAWB"

        #region "Function For Fetching Released HBL"

        /// <summary>
        /// Fetches all data released.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="Cargo_type">The cargo_type.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <returns></returns>
        public DataSet FetchAllDataReleased(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string Cargo_type = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 Export = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            string strCondition1 = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And JOB_EXP.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition = strCondition + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }
                strCondition = strCondition + " AND JOB_EXP.BUSINESS_TYPE = 2";
                strCondition = strCondition + " AND JOB_EXP.PROCESS_TYPE = 1 ";
                if (Cargo_type != "0")
                {
                    strCondition = strCondition + " AND BOOK.CARGO_TYPE = " + Cargo_type + "";
                }
                if (flag == 0)
                {
                    strCondition1 += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition1 = strCondition1 + " AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition1 = strCondition1 + " AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition1 = strCondition1 + " AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition1 = strCondition1 + " And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition1 = strCondition1 + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition1 = strCondition1 + " And AMT.AIRLINE_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition1 = strCondition1 + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }
                strCondition1 = strCondition1 + " AND JOB_EXP.BUSINESS_TYPE = 1";
                strCondition1 = strCondition1 + " AND JOB_EXP.PROCESS_TYPE = 1 ";
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'Sea' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" HBL.ETD_DATE ETD, ");
                sb.Append(" HBL.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 2");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition);

                sb.Append(" UNION ");
                sb.Append(" SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'Air' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" HAWB.ETD_DATE ETD, ");
                sb.Append(" HAWB.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 1");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition1);

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
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, QRY.* FROM(SELECT Q.* FROM(";
                strSQL += sb.ToString();
                if (Export == 1)
                {
                    strSQL += " )q ORDER BY Q.SHIP_DATE DESC, Q.SHIP_REF_NR DESC)";
                }
                else
                {
                    strSQL += " )q ORDER BY Q.SHIP_DATE DESC, Q.SHIP_REF_NR DESC )QRY) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Function For Fetching Released HBL"

        #region "Function For Printing Released HBL"

        /// <summary>
        /// Fetches the sea report released.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchSeaReportReleased(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       'Sea' BIZTYPE,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HBL.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HBL.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 2");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append(" AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append("AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And JOB_EXP.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append(" And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'");
                }
                sb.Append(" AND JOB_EXP.BUSINESS_TYPE = 2 ");
                sb.Append(" AND JOB_EXP.PROCESS_TYPE = 1 ");
                sb.Append(" order by HBL.HBL_DATE desc, HBL.HBL_REF_NO desc ");
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

        #endregion "Function For Printing Released HBL"

        #region "Function For Printing Released HAWB"

        /// <summary>
        /// Fetches the air report released.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="FlightNo">The flight no.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchAirReportReleased(Int32 CustPK = 0, Int32 LocFk = 0, string Airline = "", string FlightNo = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HAWB.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HAWB.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 1");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append("  AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append("  AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    sb.Append("  And AMT.AIRLINE_NAME = '" + Airline + "'");
                }
                if (!string.IsNullOrEmpty(FlightNo))
                {
                    sb.Append("  And JOB_EXP.VOYAGE_FLIGHT_NO = '" + FlightNo + "'");
                }
                sb.Append(" AND JOB_EXP.BUSINESS_TYPE = 1 ");
                sb.Append(" AND JOB_EXP.PROCESS_TYPE = 1 ");
                sb.Append(" order by HAWB.HAWB_DATE desc, HAWB.HAWB_REF_NO desc ");
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

        #endregion "Function For Printing Released HAWB"

        #region "For Fetching DropDown Values From DataBase"

        /// <summary>
        /// Fetches the drop down values.
        /// </summary>
        /// <param name="Flag">The flag.</param>
        /// <param name="ConfigID">The configuration identifier.</param>
        /// <returns></returns>
        public static DataSet FetchDropDownValues(string Flag = "", string ConfigID = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string ErrorMessage = null;
            sb.Append("SELECT T.DD_VALUE, T.DD_ID");
            sb.Append("  FROM QFOR_DROP_DOWN_TBL T");
            sb.Append(" WHERE T.DD_FLAG = '" + Flag + "'");
            sb.Append(" AND T.CONFIG_ID  = '" + ConfigID + "'");
            sb.Append("    ORDER BY T.DD_VALUE");
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

        #endregion "For Fetching DropDown Values From DataBase"

        #region "Function For Printing Released HBL"

        /// <summary>
        /// Fetches all report released.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchAllReportReleased(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append(" SELECT * FROM ( ");
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       'Sea' BIZTYPE, ");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HBL.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HBL.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 2");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append(" AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append("AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And JOB_EXP.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append(" And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'");
                }
                sb.Append(" AND JOB_EXP.PROCESS_TYPE = 1 ");
                //'
                sb.Append(" UNION ");
                sb.Append(" SELECT JOB_EXP.JOB_CARD_TRN_PK JOB_CARD_TRN_PK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBL_EXP_TBL_PK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       'Air' BIZTYPE, ");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HAWB.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HAWB.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 1");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append("  AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append("  AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append("  And AMT.AIRLINE_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append("  And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'");
                }
                sb.Append(" ) Q ORDER BY Q.SHIP_DATE DESC, Q.SHIP_REF_NR DESC ");

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

        #endregion "Function For Printing Released HBL"

        //' Cancelled HBL's PTS Jan-009

        #region "Function For Fetching Cancelled HBL"

        /// <summary>
        /// Fetches the sea data cancelled.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchSeaDataCancelled(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 Export = 0, Int16 CargoType = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And JOB_EXP.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition = strCondition + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }

                if (CargoType > 0)
                {
                    strCondition = strCondition + " and BOOK.CARGO_TYPE=" + CargoType + "";
                }
                strCondition = strCondition + " AND JOB_EXP.BUSINESS_TYPE = 2";
                strCondition = strCondition + " AND JOB_EXP.PROCESS_TYPE = 1 ";
                sb.Append(" SELECT DISTINCT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" HBL.ETA_DATE ETA, ");
                sb.Append(" HBL.ETD_DATE ETD, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("  UNION ");
                sb.Append(" SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" HBL.ETA_DATE ETA, ");
                sb.Append(" HBL.ETD_DATE ETD, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND HBL.NEW_JOB_CARD_SEA_EXP_FK = JOB_EXP.JOB_CARD_TRN_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
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
                string tempStr = null;
                tempStr = " SELECT * FROM (";
                tempStr += " SELECT ROWNUM SR_NO,q.* FROM (  ";
                strSQL = tempStr + sb.ToString() + " order by SHIP_DATE desc, SHIP_REF_NR desc ) q  )";
                if (Export == 0)
                {
                    strSQL += "  WHERE SR_NO  Between " + start + " and " + last;
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

        #endregion "Function For Fetching Cancelled HBL"

        #region "Function For Fetching Cancelled HAWB"

        /// <summary>
        /// Fetches the air data cancelled.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="Flightno">The flightno.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <returns></returns>
        public DataSet FetchAirDataCancelled(Int32 CustPK = 0, Int32 LocFk = 0, string Airline = "", string Flightno = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 Export = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition = strCondition + " AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    strCondition = strCondition + " And AMT.AIRLINE_NAME = '" + Airline + "'";
                }
                if (!string.IsNullOrEmpty(Flightno))
                {
                    strCondition = strCondition + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Flightno + "'";
                }
                strCondition = strCondition + " AND JOB_EXP.BUSINESS_TYPE = 1";
                strCondition = strCondition + " AND JOB_EXP.PROCESS_TYPE = 1 ";

                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'AIR' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" HAWB.ETA_DATE ETA, ");
                sb.Append(" HAWB.ETD_DATE ETD, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("  UNION ");
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'AIR' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" HAWB.ETA_DATE ETA, ");
                sb.Append(" HAWB.ETD_DATE ETD, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append(" AND HAWB.NEW_JOB_CARD_AIR_EXP_FK = JOB_EXP.JOB_CARD_TRN_PK ");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
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
                string tempStr = null;
                tempStr = " SELECT * FROM (";
                tempStr += " SELECT ROWNUM SR_NO,q.* FROM (  ";
                strSQL = tempStr + sb.ToString() + " order by SHIP_DATE desc, SHIP_REF_NR desc ) q  )";
                if (Export == 0)
                {
                    strSQL += "  WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Function For Fetching Cancelled HAWB"

        #region "Function For Fetching Cancelled HBL/HAWB"

        /// <summary>
        /// Fetches the both data cancelled.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchBothDataCancelled(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 Export = 0, Int16 CargoType = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strSeaCondition = null;
            string strAirCondition = null;
            Int32 TotalRecords = default(Int32);

            try
            {
                if (flag == 0)
                {
                    strSeaCondition += " AND 1=2";
                }
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strSeaCondition = strSeaCondition + " AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strSeaCondition = strSeaCondition + " AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strSeaCondition = strSeaCondition + " AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strSeaCondition = strSeaCondition + " And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strSeaCondition = strSeaCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strSeaCondition = strSeaCondition + " And JOB_EXP.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strSeaCondition = strSeaCondition + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }

                if (CargoType > 0)
                {
                    strSeaCondition = strSeaCondition + " and BOOK.CARGO_TYPE=" + CargoType + "";
                }
                strSeaCondition = strSeaCondition + " AND JOB_EXP.BUSINESS_TYPE = 2 ";
                strSeaCondition = strSeaCondition + "  AND JOB_EXP.PROCESS_TYPE = 1 ";
                sb.Append(" SELECT DISTINCT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" HBL.ETA_DATE ETA, ");
                sb.Append(" HBL.ETD_DATE ETD, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strSeaCondition);
                sb.Append("  UNION ");
                sb.Append(" SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" HBL.ETA_DATE ETA, ");
                sb.Append(" HBL.ETD_DATE ETD, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND HBL.NEW_JOB_CARD_SEA_EXP_FK = JOB_EXP.JOB_CARD_TRN_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
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
                    strAirCondition = strAirCondition + " AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strAirCondition = strAirCondition + " AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strAirCondition = strAirCondition + " AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strAirCondition = strAirCondition + " And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strAirCondition = strAirCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strAirCondition = strAirCondition + " And AMT.AIRLINE_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strAirCondition = strAirCondition + " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }
                strAirCondition = strAirCondition + "  AND JOB_EXP.BUSINESS_TYPE = 1";
                strAirCondition = strAirCondition + "   AND JOB_EXP.PROCESS_TYPE = 1 ";
                sb.Append("  UNION ");
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'AIR' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" HAWB.ETA_DATE ETA, ");
                sb.Append(" HAWB.ETD_DATE ETD, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strAirCondition);
                sb.Append("  UNION ");
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'AIR' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" HAWB.ETA_DATE ETA, ");
                sb.Append(" HAWB.ETD_DATE ETD, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append(" AND HAWB.NEW_JOB_CARD_AIR_EXP_FK = JOB_EXP.JOB_CARD_TRN_PK ");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strAirCondition);

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
                string tempStr = null;
                tempStr = " SELECT * FROM (";
                tempStr += " SELECT ROWNUM SR_NO,q.* FROM (  ";
                strSQL = tempStr + sb.ToString() + " order by SHIP_DATE desc, SHIP_REF_NR desc ) q  )";

                if (Export == 0)
                {
                    strSQL += "  WHERE SR_NO  Between " + start + " and " + last;
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

        #endregion "Function For Fetching Cancelled HBL/HAWB"

        #region "Function For Printing Cancelled HBL"

        /// <summary>
        /// Fetches the sea report cancelled.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchSeaReportCancelled(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int16 CargoType = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            try
            {
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition += "  AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition += "  AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition += "  AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition += "  And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition += "  And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition += "  And JOB_EXP.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition += "  And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }

                if (CargoType > 0)
                {
                    strCondition = strCondition + " and BOOK.CARGO_TYPE=" + CargoType + "";
                }
                strCondition = strCondition + " AND JOB_EXP.BUSINESS_TYPE = 2";
                strCondition = strCondition + " AND JOB_EXP.PROCESS_TYPE = 1 ";
                sb.Append("SELECT * FROM (");
                sb.Append(" SELECT DISTINCT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HBL.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HBL.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition);
                sb.Append("  UNION ");

                sb.Append("SELECT DISTINCT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HBL.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HBL.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND HBL.NEW_JOB_CARD_SEA_EXP_FK = JOB_EXP.JOB_CARD_TRN_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition);

                sb.Append(" )order by to_date(SHIP_DATE) desc, SHIP_REF_NR desc ");
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

        #endregion "Function For Printing Cancelled HBL"

        #region "Function For Printing Cancelled HAWB"

        /// <summary>
        /// Fetches the air report cancelled.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="FlightNo">The flight no.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchAirReportCancelled(Int32 CustPK = 0, Int32 LocFk = 0, string Airline = "", string FlightNo = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strCondition = null;
            try
            {
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strCondition += " AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strCondition += " AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strCondition += " AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition += " And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition += " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    strCondition += " And AMT.AIRLINE_NAME = '" + Airline + "'";
                }
                if (!string.IsNullOrEmpty(FlightNo))
                {
                    strCondition += " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + FlightNo + "'";
                }
                strCondition = strCondition + " AND JOB_EXP.BUSINESS_TYPE = 1";
                strCondition = strCondition + " AND JOB_EXP.PROCESS_TYPE = 1 ";
                sb.Append("SELECT * FROM (");
                sb.Append(" SELECT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HAWB.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HAWB.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition);

                sb.Append("  UNION ");
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HAWB.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HAWB.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append(" AND HAWB.NEW_JOB_CARD_AIR_EXP_FK = JOB_EXP.JOB_CARD_TRN_PK ");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition);

                sb.Append(" )order by to_date(SHIP_DATE) desc, SHIP_REF_NR desc ");
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

        #endregion "Function For Printing Cancelled HAWB"

        #region "Function For Printing Cancelled HBL/HAWB"

        /// <summary>
        /// Fetches the both report cancelled.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchBothReportCancelled(Int32 CustPK = 0, Int32 LocFk = 0, string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int16 CargoType = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSeaCondition = null;
            string strAirCondition = null;
            try
            {
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strSeaCondition += "  AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strSeaCondition += "  AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strSeaCondition += "  AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strSeaCondition += "  And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strSeaCondition += "  And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strSeaCondition += "  And JOB_EXP.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strSeaCondition += "  And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }
                if (CargoType > 0)
                {
                    strSeaCondition = strSeaCondition + " and BOOK.CARGO_TYPE=" + CargoType + "";
                }
                strSeaCondition = strSeaCondition + " AND JOB_EXP.BUSINESS_TYPE = 2 ";
                strSeaCondition = strSeaCondition + "  AND JOB_EXP.PROCESS_TYPE = 1 ";
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt))) & !((ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    strAirCondition += " AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)";
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    strAirCondition += " AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ";
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    strAirCondition += " AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strAirCondition += " And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strAirCondition += " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strAirCondition += " And AMT.AIRLINE_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strAirCondition += " And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }
                strAirCondition = strAirCondition + " AND JOB_EXP.BUSINESS_TYPE = 1 ";
                strAirCondition = strAirCondition + "  AND JOB_EXP.PROCESS_TYPE = 1 ";
                sb.Append("SELECT * FROM (");
                sb.Append(" SELECT DISTINCT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HBL.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HBL.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strSeaCondition);

                sb.Append("  UNION ");

                sb.Append("SELECT DISTINCT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'SEA' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HBL.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HBL.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND HBL.NEW_JOB_CARD_SEA_EXP_FK = JOB_EXP.JOB_CARD_TRN_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strSeaCondition);

                sb.Append("  UNION ");
                sb.Append(" SELECT JOB_EXP.JOB_CARD_TRN_PK JOB_CARD_TRN_PK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBL_EXP_TBL_PK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'AIR' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HAWB.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HAWB.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strAirCondition);

                sb.Append("  UNION ");
                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOB_CARD_TRN_PK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBL_EXP_TBL_PK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'AIR' BIZTYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" TO_CHAR(HAWB.ETD_DATE,DATETIMEFORMAT24) ETD, ");
                sb.Append(" TO_CHAR(HAWB.ETA_DATE,DATETIMEFORMAT24) ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append(" AND HAWB.NEW_JOB_CARD_AIR_EXP_FK = JOB_EXP.JOB_CARD_TRN_PK ");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 3");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strAirCondition);

                sb.Append(" )order by to_date(SHIP_DATE) desc, SHIP_REF_NR desc ");
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

        #endregion "Function For Printing Cancelled HBL/HAWB"

        #region "Function For Fetching Confirmed HAWB/HBL for Search"

        /// <summary>
        /// Fetches the hawb air HBL sea data.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="VslVoyPK">The VSL voy pk.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Export">The export.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchHAWBAirHBLSeaData(Int32 CustPK = 0, Int32 LocFk = 0, Int32 VslVoyPK = 0, string Airline = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 Export = 0, Int32 CargoType = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            Int32 last = 0;
            Int32 start = 0;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }

                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(H.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (CustPK > 0)
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK = " + CustPK + "";
                }

                sb.Append("SELECT * FROM(SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       H.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       H.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       H.HAWB_DATE SHIP_DATE,");
                sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 1) BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (BOOK.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                sb.Append(" H.ETD_DATE ETD, ");
                sb.Append(" H.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            H,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append(" AND H.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append("  AND H.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append("  AND H.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (VslVoyPK > 0)
                {
                    sb.Append("  AND AMT.AIRLINE_MST_PK = " + VslVoyPK + "");
                }
                //If Airline <> "" Then
                //    sb.Append("  AND AMT.AIRLINE_NAME = '" & Airline & "'")
                //End If
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append("  AND UPPER(JOB_EXP.VOYAGE_FLIGHT_NO) = '" + Voyage.Trim().ToUpper() + "'");
                }
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = H.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND JOB_EXP.BUSINESS_TYPE=1 ");
                sb.Append("   AND H.HAWB_STATUS = 2");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append(strCondition);

                sb.Append(" UNION ");

                sb.Append("SELECT JOB_EXP.JOB_CARD_TRN_PK JOBCARDPK,");
                sb.Append("       H.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       H.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                //sb.Append("       H.HBL_DATE SHIP_DATE,")
                sb.Append("       H.HBL_DATE SHIP_DATE,");
                sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 2) BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" H.ETD_DATE ETD, ");
                sb.Append(" H.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             H,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append("  AND H.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append("  AND H.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append("  AND H.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (VslVoyPK > 0)
                {
                    sb.Append("  AND JOB_EXP.VOYAGE_TRN_FK  = " + VslVoyPK + "");
                }
                if (CargoType > 0)
                {
                    sb.Append("  AND BOOK.CARGO_TYPE = " + CargoType + "");
                }
                sb.Append(strCondition);
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = H.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND JOB_EXP.BUSINESS_TYPE=2 ");
                sb.Append("   AND H.HBL_STATUS = 1");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")) order by NVL(ETD,'01/01/0001') desc");

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
                // sb.Append(" order by H.HAWB_DATE desc, H.HAWB_REF_NO desc ")
                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
                strSQL += sb.ToString();
                if (Export == 1)
                {
                    strSQL += " )q )";
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

        #endregion "Function For Fetching Confirmed HAWB/HBL for Search"

        #region "Function For Printing Confirmed HBL/HAWB"

        /// <summary>
        /// Fetches the HBL sea hawb air report.
        /// </summary>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="VslVoyPK">The VSL voy pk.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchHBLSeaHAWBAirReport(Int32 CustPK = 0, Int32 LocFk = 0, string Airline = "", Int32 VslVoyPK = 0, string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 CargoType = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT * FROM (SELECT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HBL.HBL_EXP_TBL_PK HBLPK,");
                sb.Append("       HBL.HBL_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HBL.HBL_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'Sea' AS BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_EXP.VESSEL_NAME, '') || '/' || NVL(JOB_EXP.VOYAGE_FLIGHT_NO, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append(" HBL.ETD_DATE ETD, ");
                sb.Append(" HBL.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HBL_EXP_TBL             HBL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND HBL.HBL_STATUS = 1");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append(" AND HBL.HBL_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append(" AND HBL.HBL_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append("AND HBL.HBL_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(HBL.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (VslVoyPK > 0)
                {
                    sb.Append("  AND JOB_EXP.VOYAGE_TRN_FK  = " + VslVoyPK + "");
                }
                if (CargoType > 0)
                {
                    sb.Append("   AND BOOK.CARGO_TYPE  = " + CargoType + "");
                }
                //sb.Append(" order by HBL.HBL_DATE desc, HBL.HBL_REF_NO desc ")
                sb.Append(" UNION SELECT JOB_EXP.JOB_CARD_TRN_PK,");
                sb.Append("       HAWB.HAWB_EXP_TBL_PK HBLPK,");
                sb.Append("       HAWB.HAWB_REF_NO SHIP_REF_NR,");
                sb.Append("       BOOK.BOOKING_REF_NO,");
                sb.Append("       JOB_EXP.JOBCARD_REF_NO,");
                sb.Append("       TO_DATE(HAWB.HAWB_DATE, dateformat)SHIP_DATE,");
                sb.Append("       'Air' AS BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE");
                sb.Append("         WHEN JOB_EXP.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append("          (JOB_EXP.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("         ELSE");
                sb.Append("          AMT.AIRLINE_NAME");
                sb.Append("       END AS VESVOYAGE,");
                //sb.Append("       HAWB.ETD_DATE ETD,")
                //sb.Append("       HAWB.ETA_DATE ETA,")
                sb.Append(" HAWB.ETD_DATE ETD, ");
                sb.Append(" HAWB.ETA_DATE ETA, ");
                sb.Append("       DECODE(BOOK.CARGO_TYPE, 1, 'KGS', 2, 'ULD') CARGOTYPE,");
                sb.Append("       COMM.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BOOK,");
                sb.Append("       JOB_CARD_TRN    JOB_EXP,");
                sb.Append("       HAWB_EXP_TBL            HAWB,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COMM");
                sb.Append(" WHERE BOOK.BOOKING_MST_PK = JOB_EXP.BOOKING_MST_FK");
                sb.Append("   AND JOB_EXP.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK");
                sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB_EXP.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND JOB_EXP.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                sb.Append("   AND BOOK.CARRIER_MST_FK = AMT.AIRLINE_MST_PK");
                sb.Append("   AND HAWB.HAWB_STATUS = 2");
                sb.Append("   AND POL.LOCATION_MST_FK =");
                sb.Append("       (SELECT L.LOCATION_MST_PK");
                sb.Append("          FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!((FromDt == null | string.IsNullOrEmpty(FromDt)) & (ToDt == null | string.IsNullOrEmpty(ToDt))))
                {
                    sb.Append("  AND HAWB.HAWB_DATE BETWEEN TO_DATE('" + FromDt + "', DATEFORMAT) AND TO_DATE('" + ToDt + "', DATEFORMAT)");
                }
                else if ((!string.IsNullOrEmpty(FromDt)) & ((ToDt == null) | string.IsNullOrEmpty(ToDt)))
                {
                    sb.Append("  AND HAWB.HAWB_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                else if ((!string.IsNullOrEmpty(ToDt)) & ((FromDt == null) | string.IsNullOrEmpty(FromDt)))
                {
                    sb.Append(" AND HAWB.HAWB_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(HAWB.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (CustPK > 0)
                {
                    sb.Append(" And CMT.CUSTOMER_MST_PK = " + CustPK + "");
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    sb.Append("  And AMT.AIRLINE_NAME = '" + Airline + "'");
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append(" And JOB_EXP.VOYAGE_FLIGHT_NO = '" + Voyage + "'");
                }
                sb.Append(" ) Q order by NVL(Q.ETD,'01/01/0001') desc ");
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

        #endregion "Function For Printing Confirmed HBL/HAWB"
    }
}