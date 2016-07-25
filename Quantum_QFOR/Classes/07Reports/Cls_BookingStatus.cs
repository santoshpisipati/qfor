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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_BookingStatus : CommonFeatures
    {
        #region "Sea Grid Function For Provisional Bookings"

        /// <summary>
        /// Fetches the sea grid.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="VoyNr">The voy nr.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <returns></returns>
        public DataSet FetchSeaGrid(Int32 BookingPk = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string VoyNr = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 ExportExcel = 0, Int32 CargoType = 0, Int32 CommGrp = 0)
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
                if (BookingPk > 0)
                {
                    strCondition = strCondition + " And BKG.BOOKING_MST_PK = " + BookingPk;
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And BKG.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And BKG.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(BKG.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }

                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + "  And BKG.VESSEL_NAME = '" + VslName + "'";
                    if (!string.IsNullOrEmpty(VoyNr))
                    {
                        strCondition = strCondition + "  And BKG.VOYAGE_FLIGHT_NO = '" + VoyNr + "'";
                    }
                    else
                    {
                        strCondition = strCondition + "  And BKG.VOYAGE_FLIGHT_NO is null";
                    }
                }
                if (CargoType != 0)
                {
                    strCondition = strCondition + " And BKG.CARGO_TYPE = " + CargoType + "";
                }
                if (CommGrp != 0)
                {
                    strCondition = strCondition + " And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "";
                }
                sb.Append("SELECT BKG.BOOKING_MST_PK BOOKINGPK,");
                sb.Append("       BKG.BOOKING_REF_NO,");
                sb.Append("       TO_DATE(BKG.BOOKING_DATE, 'DD/MM/YYYY') BOOKING_DATE,");
                sb.Append("       TO_DATE(BKG.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                sb.Append("       'Sea' AS BIZ_TYPE,");
                sb.Append("       DECODE(BKG.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGO_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POL.PORT_ID POL,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE WHEN BKG.VESSEL_NAME IS NULL THEN");
                sb.Append("       BKG.VESSEL_NAME ");
                sb.Append("       ELSE");
                sb.Append("        (BKG.VESSEL_NAME || '/' || BKG.VOYAGE_FLIGHT_NO)");
                sb.Append("       END AS \"VESSEL/VOYAGE\",");
                sb.Append("       BKG.ETD_DATE ETD_DATE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       '' SEL");
                sb.Append("  FROM BOOKING_MST_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK(+) = BKG.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK(+) = BKG.PORT_MST_POD_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK(+) = BKG.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BKG.COMMODITY_GROUP_FK");
                sb.Append("   AND BKG.STATUS = 1 ");
                sb.Append("       AND BKG.BUSINESS_TYPE = 2 ");
                // sb.Append("   AND BKG.IS_EBOOKING = 0 ")
                sb.Append("  AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");

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

                sb.Append(" order by NVL(BKG.BOOKING_DATE,'01/01/0001') desc ");
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

        #endregion "Sea Grid Function For Provisional Bookings"

        #region "Fetch Air Sea Function for Search Grid"

        /// <summary>
        /// Fetches the air sea grid.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="VoyNr">The voy nr.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <returns></returns>
        public DataSet FetchAirSeaGrid(Int32 BookingPk = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string VoyNr = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 ExportExcel = 0, Int32 CargoType = 0, Int32 CommGrp = 0)
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
                if (BookingPk > 0)
                {
                    strCondition = strCondition + " And BKG.BOOKING_MST_PK = " + BookingPk;
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And BKG.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And BKG.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(BKG.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (CargoType != 0)
                {
                    strCondition = strCondition + " And BKG.CARGO_TYPE = " + CargoType + "";
                }
                if (CommGrp != 0)
                {
                    strCondition = strCondition + " And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "";
                }
                sb.Append("SELECT * FROM (SELECT BKG.BOOKING_MST_PK BOOKINGPK,");
                sb.Append("       BKG.BOOKING_REF_NO,");
                sb.Append("       TO_DATE(BKG.BOOKING_DATE, 'DD/MM/YYYY') BOOKING_DATE,");
                sb.Append("       TO_DATE(BKG.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                sb.Append("       'Sea' AS BIZ_TYPE,");
                sb.Append("       DECODE(BKG.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGO_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POL.PORT_ID POL,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE WHEN BKG.VESSEL_NAME IS NULL THEN");
                sb.Append("       BKG.VESSEL_NAME ");
                sb.Append("       ELSE");
                sb.Append("        (BKG.VESSEL_NAME || '/' || BKG.VOYAGE_FLIGHT_NO)");
                sb.Append("       END AS \"VESSEL/VOYAGE\",");
                sb.Append("       BKG.ETD_DATE ETD_DATE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       '' SEL");
                sb.Append("       FROM BOOKING_MST_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append("       WHERE POL.PORT_MST_PK(+) = BKG.PORT_MST_POL_FK");
                sb.Append("       AND POD.PORT_MST_PK(+) = BKG.PORT_MST_POD_FK");
                sb.Append("       AND CMT.CUSTOMER_MST_PK(+) = BKG.CUST_CUSTOMER_MST_FK");
                sb.Append("       AND CGMT.COMMODITY_GROUP_PK(+) = BKG.COMMODITY_GROUP_FK");
                sb.Append("       AND BKG.STATUS = 1 ");
                sb.Append("       AND BKG.BUSINESS_TYPE = 2 ");
                //  sb.Append("       AND BKG.IS_EBOOKING = 0 ")
                sb.Append("  AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And BKG.VESSEL_NAME = '" + VslName + "'");
                    if (!string.IsNullOrEmpty(VoyNr))
                    {
                        sb.Append(" And BKG.VOYAGE_FLIGHT_NO = '" + VoyNr + "'");
                    }
                    else
                    {
                        sb.Append(" And BKG.VOYAGE_FLIGHT_NO is null");
                    }
                }
                sb.Append(strCondition);
                sb.Append("   UNION ");
                sb.Append("SELECT BKG.BOOKING_MST_PK BOOKINGPK,");
                sb.Append("       BKG.BOOKING_REF_NO,");
                sb.Append("       TO_DATE(BKG.BOOKING_DATE, 'DD/MM/YYYY') BOOKING_DATE,");
                sb.Append("       TO_DATE(BKG.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                sb.Append("       'Air' AS BIZ_TYPE,");
                sb.Append("       '' AS CARGO_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       AOO.PORT_ID POL,");
                sb.Append("       AOD.PORT_ID POD,");
                sb.Append(" CASE WHEN BKG.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append(" (AMT.AIRLINE_NAME || '/' || BKG.VOYAGE_FLIGHT_NO)");
                sb.Append("  ELSE");
                sb.Append("   AMT.AIRLINE_NAME");
                sb.Append("  END AS \"VESSEL/VOYAGE\",");
                sb.Append("       BKG.ETD_DATE ETD_DATE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       '' SEL");
                sb.Append("  FROM BOOKING_MST_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE AOO.PORT_MST_PK(+) = BKG.PORT_MST_POL_FK");
                sb.Append("   AND AOD.PORT_MST_PK(+) = BKG.PORT_MST_POD_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK(+) = BKG.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BKG.CARRIER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BKG.COMMODITY_GROUP_FK");
                sb.Append("   AND BKG.STATUS = 1");
                sb.Append("       AND BKG.BUSINESS_TYPE =1 ");
                //  sb.Append("   AND BKG.IS_EBOOKING = 0")
                sb.Append("  AND AOO.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And AMT.AIRLINE_NAME = '" + VslName + "'");
                    if (!string.IsNullOrEmpty(VoyNr))
                    {
                        sb.Append(" AND BKG.VOYAGE_FLIGHT_NO = '" + VoyNr + "'");
                    }
                    else
                    {
                        sb.Append(" AND BKG.VOYAGE_FLIGHT_NO is null");
                    }
                }
                sb.Append(strCondition);
                sb.Append(" ) SQ order by NVL(SQ.BOOKING_DATE,'01/01/0001') desc ");

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, q.* FROM(";
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

        #endregion "Fetch Air Sea Function for Search Grid"

        #region "Fetch Sea Function"

        /// <summary>
        /// Fetches the specified BKG pk.
        /// </summary>
        /// <param name="BkgPk">The BKG pk.</param>
        /// <returns></returns>
        public DataSet Fetch(Int32 BkgPk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT BKG.BOOKING_MST_PK,");
            sb.Append("       BKG.PORT_MST_POD_FK,");
            sb.Append("       BKG.CUST_CUSTOMER_MST_FK,");
            sb.Append("       BKG.CONS_CUSTOMER_MST_FK, ");
            sb.Append("       BKG.VESSEL_VOYAGE_FK    ");
            sb.Append("  FROM BOOKING_MST_TBL   BKG");
            sb.Append("  WHERE BKG.BOOKING_MST_PK IN (" + BkgPk + ")");
            try
            {
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

        #endregion "Fetch Sea Function"

        #region "Booking Update"

        /// <summary>
        /// Bookings the update.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <returns></returns>
        public ArrayList BookingUpdate(DataSet dsMain)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            Int16 exe = default(Int16);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            objWK.MyCommand.Parameters.Clear();
            try
            {
                var _with1 = objWK.MyCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BKG_JOB_CARD_SEA_EXP_TBL_INS";

                _with1.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["BOOKING_MST_PK"])).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("LOCATION_MST_FK_IN", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"])).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("POD_FK_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("SHIPPER_MST_FK_IN", Convert.ToInt32(dsMain.Tables[0].Rows[0]["CUST_CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("CONSIGNEE_MST_FK_IN", (string.IsNullOrEmpty(dsMain.Tables[0].Rows[0]["CONS_CUSTOMER_MST_FK"].ToString()) ? "" : dsMain.Tables[0].Rows[0]["CONS_CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("VOYAGE_PK_IN", (string.IsNullOrEmpty(dsMain.Tables[0].Rows[0]["VESSEL_VOYAGE_FK"].ToString()) ? "" : dsMain.Tables[0].Rows[0]["VESSEL_VOYAGE_FK"])).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "BOOKING_MST_PK").Direction = ParameterDirection.Output;
                _with1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                exe = Convert.ToInt16(objWK.MyCommand.ExecuteNonQuery());
                if (exe > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
            return arrMessage;
        }

        #endregion "Booking Update"

        #region "Sea Print Function For Provisional Bookings"

        /// <summary>
        /// Fetches the sea report.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <returns></returns>
        public DataSet FetchSeaReport(Int32 BookingPk = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 CargoType = 0, Int32 CommGrp = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT BKG.BOOKING_MST_PK,");
                sb.Append("       BKG.BOOKING_REF_NO,");
                sb.Append("       BKG.BOOKING_DATE,");
                sb.Append("       'Sea' AS BIZ_TYPE,");
                sb.Append("       BKG.SHIPMENT_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POL.PORT_ID POL,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE WHEN BKG.VESSEL_NAME IS NULL THEN");
                sb.Append("       BKG.VESSEL_NAME ");
                sb.Append("       ELSE");
                sb.Append("        (BKG.VESSEL_NAME || '/' || BKG.VOYAGE_FLIGHT_NO)");
                sb.Append("       END AS \"VESSEL/VOYAGE\",");
                sb.Append("       TO_CHAR(BKG.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       DECODE(BKG.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGO_TYPE,");
                sb.Append("       '' SEL");
                sb.Append("  FROM BOOKING_MST_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK(+) = BKG.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK(+) = BKG.PORT_MST_POD_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK(+) = BKG.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BKG.COMMODITY_GROUP_FK");
                sb.Append("   AND BKG.STATUS = 1 ");
                sb.Append("   AND BKG.IS_EBOOKING = 0 ");
                sb.Append("  AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (CargoType != 0)
                {
                    sb.Append(" And BKG.CARGO_TYPE = " + CargoType + "");
                }
                if (CommGrp != 0)
                {
                    sb.Append(" And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "");
                }
                if (BookingPk > 0)
                {
                    sb.Append(" And BKG.BOOKING_MST_PK = " + BookingPk);
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And BKG.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And BKG.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And BKG.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(BKG.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat) ");
                }
                sb.Append(" order by BKG.BOOKING_DATE desc, BKG.BOOKING_REF_NO desc ");
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

        #endregion "Sea Print Function For Provisional Bookings"

        #region "Air Grid Function For Provisional Bookings"

        /// <summary>
        /// Fetches the air grid.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="VoyNr">The voy nr.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <returns></returns>
        public DataSet FetchAirGrid(Int32 BookingPk = 0, Int32 LocFk = 0, string CustName = "", string Airline = "", string VoyNr = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 ExportExcel = 0, Int32 CommGrp = 0)
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
                if (BookingPk > 0)
                {
                    strCondition = strCondition + " And BKG.BOOKING_MST_PK = " + BookingPk;
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And BKG.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And BKG.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(BKG.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }

                if (!string.IsNullOrEmpty(Airline))
                {
                    strCondition = strCondition + "  And AMT.AIRLINE_NAME = '" + Airline + "'";
                    if (!string.IsNullOrEmpty(VoyNr))
                    {
                        strCondition = strCondition + " And  BKG.VOYAGE_FLIGHT_NO = '" + VoyNr + "'";
                    }
                    else
                    {
                        strCondition = strCondition + " And BKG.VOYAGE_FLIGHT_NO is null";
                    }
                }
                if (CommGrp != 0)
                {
                    strCondition = strCondition + " And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "";
                }
                sb.Append(" SELECT BKG.BOOKING_MST_PK BOOKINGPK,");
                sb.Append("       BKG.BOOKING_REF_NO,");
                sb.Append("       TO_DATE(BKG.BOOKING_DATE, 'DD/MM/YYYY') BOOKING_DATE,");
                sb.Append("       TO_DATE(BKG.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                sb.Append("       'Air' AS BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       AOO.PORT_ID POL,");
                sb.Append("       AOD.PORT_ID POD,");
                sb.Append(" CASE WHEN BKG.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append(" (AMT.AIRLINE_NAME || '/' || BKG.VOYAGE_FLIGHT_NO)");
                sb.Append("  ELSE");
                sb.Append("   AMT.AIRLINE_NAME");
                sb.Append("  END AS \"VESSEL/VOYAGE\",");
                sb.Append("       BKG.ETD_DATE ETD_DATE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       '' SEL");
                sb.Append("  FROM BOOKING_MST_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE AOO.PORT_MST_PK(+) = BKG.PORT_MST_POL_FK");
                sb.Append("   AND AOD.PORT_MST_PK(+) = BKG.PORT_MST_POD_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK(+) = BKG.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BKG.CARRIER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BKG.COMMODITY_GROUP_FK");
                sb.Append("   AND BKG.STATUS = 1");
                sb.Append("       AND BKG.BUSINESS_TYPE = 1 ");
                // sb.Append("   AND BKG.IS_EBOOKING = 0")
                sb.Append("  AND AOO.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
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

                sb.Append(" order by NVL(BKG.BOOKING_DATE,'01/01/0001') desc ");
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

        #endregion "Air Grid Function For Provisional Bookings"

        #region "Air Sea Both Print Function For Provisional Bookings"

        /// <summary>
        /// Fetches the air sea both report.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <returns></returns>
        public DataSet FetchAirSeaBothReport(Int32 BookingPk = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 CargoType = 0, Int32 CommGrp = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT * FROM (SELECT BKG.BOOKING_MST_PK,");
                sb.Append("       BKG.BOOKING_REF_NO,");
                sb.Append("       BKG.BOOKING_DATE,");
                sb.Append("       BKG.SHIPMENT_DATE,");
                sb.Append("       'Sea' AS BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POL.PORT_ID POL,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE WHEN BKG.VESSEL_NAME IS NULL THEN");
                sb.Append("       BKG.VESSEL_NAME ");
                sb.Append("       ELSE");
                sb.Append("        (BKG.VESSEL_NAME || '/' || BKG.VOYAGE_FLIGHT_NO)");
                sb.Append("       END AS \"VESSEL/VOYAGE\",");
                sb.Append("       BKG.ETD_DATE ETD_DATE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       DECODE(BKG.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGO_TYPE,");
                sb.Append("       '' SEL");
                sb.Append("  FROM BOOKING_MST_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK(+) = BKG.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK(+) = BKG.PORT_MST_POD_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK(+) = BKG.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BKG.COMMODITY_GROUP_FK");
                sb.Append("   AND BKG.STATUS = 1 ");
                sb.Append("   AND BKG.IS_EBOOKING = 0 ");
                sb.Append("  AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (CargoType != 0)
                {
                    sb.Append(" And BKG.CARGO_TYPE = " + CargoType + "");
                }
                if (CommGrp != 0)
                {
                    sb.Append(" And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "");
                }
                if (BookingPk > 0)
                {
                    sb.Append(" And BKG.BOOKING_MST_PK = " + BookingPk);
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And BKG.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And BKG.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And BKG.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(BKG.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat) ");
                }
                sb.Append(" UNION ");
                sb.Append(" SELECT BKG.BOOKING_MST_PK,");
                sb.Append("       BKG.BOOKING_REF_NO,");
                sb.Append("       BKG.BOOKING_DATE,");
                sb.Append("       BKG.SHIPMENT_DATE,");
                sb.Append("       'Air' AS BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       AOO.PORT_ID AOO,");
                sb.Append("       AOD.PORT_ID AOD,");
                sb.Append(" CASE WHEN BKG.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append(" (BKG.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("  ELSE");
                sb.Append("   AMT.AIRLINE_NAME");
                sb.Append("  END AS \"FLIGHT_NO/AIRLINE_NAME\",");
                sb.Append("       BKG.ETD_DATE ETD_DATE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       '' CARGO_TYPE,");
                sb.Append("       '' SEL");
                sb.Append("  FROM BOOKING_MST_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE AOO.PORT_MST_PK(+) = BKG.PORT_MST_POL_FK");
                sb.Append("   AND AOD.PORT_MST_PK(+) = BKG.PORT_MST_POD_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK(+) = BKG.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BKG.CARRIER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BKG.COMMODITY_GROUP_FK");
                sb.Append("   AND BKG.STATUS = 1");
                sb.Append("   AND BKG.IS_EBOOKING = 0");
                sb.Append("  AND AOO.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (CommGrp != 0)
                {
                    sb.Append(" And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "");
                }
                if (BookingPk > 0)
                {
                    sb.Append(" And BKG.BOOKING_MST_PK = " + BookingPk);
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And AMT.AIRLINE_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And BKG.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And BKG.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(BKG.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat) ");
                }
                sb.Append(" ) SQ order by NVL(SQ.ETD_DATE,'01/01/0001') DESC ");
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

        #endregion "Air Sea Both Print Function For Provisional Bookings"

        #region "Air Print Function For Provisional Bookings"

        /// <summary>
        /// Fetches the air report.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <returns></returns>
        public DataSet FetchAirReport(Int32 BookingPk = 0, Int32 LocFk = 0, string CustName = "", string Airline = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 CommGrp = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT BKG.BOOKING_MST_PK,");
                sb.Append("       BKG.BOOKING_REF_NO,");
                sb.Append("       BKG.BOOKING_DATE,");
                sb.Append("       BKG.SHIPMENT_DATE,");
                sb.Append("       'Air' AS BIZ_TYPE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       AOO.PORT_ID AOO,");
                sb.Append("       AOD.PORT_ID AOD,");
                sb.Append(" CASE WHEN BKG.VOYAGE_FLIGHT_NO IS NOT NULL THEN");
                sb.Append(" (BKG.VOYAGE_FLIGHT_NO || '/' || AMT.AIRLINE_NAME)");
                sb.Append("  ELSE");
                sb.Append("   AMT.AIRLINE_NAME");
                sb.Append("  END AS \"FLIGHT_NO/AIRLINE_NAME\",");
                sb.Append("       BKG.ETD_DATE ETD_DATE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       '' SEL");
                sb.Append("  FROM BOOKING_MST_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE AOO.PORT_MST_PK(+) = BKG.PORT_MST_POL_FK");
                sb.Append("   AND AOD.PORT_MST_PK(+) = BKG.PORT_MST_POD_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK(+) = BKG.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BKG.CARRIER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BKG.COMMODITY_GROUP_FK");
                sb.Append("   AND BKG.STATUS = 1");
                sb.Append("   AND BKG.IS_EBOOKING = 0");
                sb.Append("  AND AOO.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (CommGrp != 0)
                {
                    sb.Append("     And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "");
                }
                if (BookingPk > 0)
                {
                    sb.Append(" And BKG.BOOKING_MST_PK = " + BookingPk);
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    sb.Append(" And AMT.AIRLINE_NAME = '" + Airline + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And BKG.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And BKG.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(BKG.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat) ");
                }
                sb.Append(" order by BKG.ETD_DATE desc ");
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

        #endregion "Air Print Function For Provisional Bookings"

        #region "Sea Grid Function For Pending Bookings for Load Confirm"

        /// <summary>
        /// Fetches the load sea grid.
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
        /// <param name="ExportExcel">The export excel.</param>
        /// <returns></returns>
        public DataSet FetchLoadSeaGrid(Int32 LocFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 ExportExcel = 0)
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
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.JOBCARD_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE, DATEFORMAT ) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And VVT.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition = strCondition + " And VT.VOYAGE = '" + Voyage + "'";
                }

                sb.Append("SELECT DISTINCT BKG.BOOKING_MST_PK,");
                sb.Append("       BKG.BOOKING_REF_NO,");
                sb.Append("       JCSET.JOB_CARD_TRN_PK,");
                sb.Append("       JCSET.JOBCARD_REF_NO, ");
                sb.Append("       TO_DATE(BKG.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE WHEN VVT.VESSEL_NAME IS NULL THEN");
                sb.Append("         VVT.VESSEL_NAME");
                sb.Append("        ELSE");
                sb.Append("          (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("        END AS \"VESSEL_FLIGHT\",");
                sb.Append("      JCSET.ETD_DATE ETD_DATE,");
                sb.Append("      JCSET.ETA_DATE ETA_DATE,");
                sb.Append("      DECODE(BKG.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("      (SELECT rowtocol('(SELECT CTMT.CONTAINER_TYPE_MST_ID FROM CONTAINER_TYPE_MST_TBL CTMT");
                sb.Append("      WHERE CTMT.CONTAINER_TYPE_MST_PK IN (SELECT JCONT.CONTAINER_TYPE_MST_FK FROM JOB_TRN_CONT    JCONT");
                sb.Append("      WHERE JCONT.JOB_CARD_TRN_FK =' ||  JCSET.JOB_CARD_TRN_PK || '))') FROM DUAL) CONTAINER_TYPE_MST_ID,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       JCSET.JOBCARD_DATE ");
                sb.Append("  FROM BOOKING_MST_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("        VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       JOB_TRN_CONT    JTSEC");
                sb.Append(" WHERE POL.PORT_MST_PK(+) = BKG.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK(+) = BKG.PORT_MST_POD_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK(+) = BKG.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BKG.COMMODITY_GROUP_FK");
                sb.Append("   AND JCSET.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                sb.Append("   AND JCSET.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND JCSET.JOB_CARD_TRN_PK=JTSEC.JOB_CARD_TRN_FK");
                sb.Append("   AND JTSEC.LOAD_DATE IS NULL");
                sb.Append("   AND BKG.STATUS <> 3");
                sb.Append("  AND JCSET.BUSINESS_TYPE = 2");
                sb.Append("  AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
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

                sb.Append("    ORDER BY   TO_DATE(SHIPMENT_DATE) DESC, JCSET.JOBCARD_REF_NO DESC");

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

        #endregion "Sea Grid Function For Pending Bookings for Load Confirm"

        #region "Sea Print Function For Pending Bookings for Load Confirm"

        /// <summary>
        /// Fetches the load sea report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchLoadSeaReport(Int32 LocFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT DISTINCT BKG.BOOKING_MST_PK,");
                sb.Append("       BKG.BOOKING_REF_NO,");
                sb.Append("       JCSET.JOB_CARD_TRN_PK,");
                sb.Append("       JCSET.JOBCARD_REF_NO, ");
                sb.Append("       TO_CHAR(BKG.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                sb.Append("       CMT.CUSTOMER_NAME,");
                sb.Append("       POD.PORT_ID POD,");
                sb.Append("       CASE WHEN VVT.VESSEL_NAME IS NULL THEN");
                sb.Append("         VVT.VESSEL_NAME");
                sb.Append("        ELSE");
                sb.Append("          (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("        END AS \"VESSEL_FLIGHT\",");
                sb.Append("      TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                sb.Append("      TO_CHAR(JCSET.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("      DECODE(BKG.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("      (SELECT rowtocol('(SELECT CTMT.CONTAINER_TYPE_MST_ID FROM CONTAINER_TYPE_MST_TBL CTMT");
                sb.Append("      WHERE CTMT.CONTAINER_TYPE_MST_PK IN (SELECT JCONT.CONTAINER_TYPE_MST_FK FROM JOB_TRN_CONT    JCONT");
                sb.Append("      WHERE JCONT.JOB_CARD_TRN_FK =' ||  JCSET.JOB_CARD_TRN_PK || '))') FROM DUAL) CONTAINER_TYPE_MST_ID,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       JCSET.JOBCARD_DATE ");
                sb.Append("  FROM BOOKING_MST_TBL         BKG,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("        VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       JOB_TRN_CONT    JTSEC");
                sb.Append(" WHERE POL.PORT_MST_PK(+) = BKG.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK(+) = BKG.PORT_MST_POD_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK(+) = BKG.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = BKG.COMMODITY_GROUP_FK");
                sb.Append("   AND JCSET.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                sb.Append("   AND JCSET.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND JCSET.JOB_CARD_TRN_PK=JTSEC.JOB_CARD_TRN_FK");
                sb.Append("   AND JTSEC.LOAD_DATE IS NULL");
                sb.Append("   AND JCSET.BUSINESS_TYPE=2");
                sb.Append("  AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("         WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And VVT.VESSEL_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    sb.Append(" And VT.VOYAGE = '" + Voyage + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat) ");
                }
                sb.Append("    ORDER BY  JCSET.JOBCARD_DATE DESC, JCSET.JOBCARD_REF_NO DESC");
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

        #endregion "Sea Print Function For Pending Bookings for Load Confirm"

        #region "Sea Grid Function For MJC Pendind for deconsolidation"

        /// <summary>
        /// Fetches the MJC sea grid.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="Cargo_type">The cargo_type.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="POLAgentPK">The pol agent pk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <returns></returns>
        public DataSet FetchMJCSeaGrid(Int32 LocFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETADt = "", string Cargo_type = "", string CommGrp = "", int POLPK = 0,
        int POLAgentPK = 0, Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 ExportExcel = 0)
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
                    strCondition = strCondition + " And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    strCondition = strCondition + " And TO_DATE(M.POD_ETA, DATEFORMAT) = TO_DATE('" + ETADt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And VST.VESSEL_NAME = '" + VslName + "'";
                    if (!string.IsNullOrEmpty(Voyage))
                    {
                        strCondition = strCondition + " And VVT.VOYAGE = '" + Voyage + "'";
                    }
                    else
                    {
                        strCondition = strCondition + " And VVT.VOYAGE is null ";
                    }
                }
                if (Cargo_type != "0")
                {
                    strCondition = strCondition + " AND M.CARGO_TYPE =" + Cargo_type;
                }
                if (CommGrp != "0")
                {
                    strCondition = strCondition + " AND CGMT.COMMODITY_GROUP_PK =" + CommGrp;
                }
                if (POLPK > 0)
                {
                    strCondition = strCondition + " AND POL.PORT_MST_PK =" + POLPK;
                }
                if (POLAgentPK > 0)
                {
                    strCondition = strCondition + " AND AMT.AGENT_MST_PK =" + POLAgentPK;
                }

                sb.Append("SELECT M.MASTER_JC_SEA_IMP_PK JBCARD_PK,");
                sb.Append("        M.MASTER_JC_REF_NO,");
                sb.Append("       TO_DATE(M.MASTER_JC_DATE, 'DD/MM/YYYY') MSTJCDATE,");
                sb.Append("       'Sea' BIZTYPE,");
                sb.Append("       DECODE(M.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGO_TYPE,");
                sb.Append("       OMT.OPERATOR_NAME OPERATORNAME,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append("       POL.PORT_ID,");
                sb.Append("       M.POD_ETA POD_ETA,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       CASE");
                sb.Append("         WHEN M.CARGO_TYPE = 2 THEN");
                sb.Append("          (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("              AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("            GROUP BY M.MASTER_JC_SEA_IMP_PK)");
                sb.Append("         ELSE");
                sb.Append("          (SELECT SUM(JCONT.NET_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("              AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("            GROUP BY M.MASTER_JC_SEA_IMP_PK)");
                sb.Append("       END NET_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_SEA_IMP_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_SEA_IMP_PK) AS VOLUME_IN_CBM,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_SEA_IMP_TBL M,");
                sb.Append("       PORT_MST_TBL          POL,");
                sb.Append("       PORT_MST_TBL          POD,");
                sb.Append("       AGENT_MST_TBL         AMT,");
                sb.Append("       VESSEL_VOYAGE_TRN     VVT,");
                sb.Append("       VESSEL_VOYAGE_TBL     VST,");
                sb.Append("       OPERATOR_MST_TBL      OMT,");
                sb.Append("       JOB_CARD_TRN  JCIT,");
                sb.Append("      CUSTOMER_MST_TBL       CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = M.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK");
                sb.Append("   AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND M.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JCIT.MASTER_JC_FK=M.MASTER_JC_SEA_IMP_PK");
                sb.Append("   AND JCIT.CONSIGNEE_CUST_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                sb.Append("   AND M.COMMODITY_GROUP_FK =CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JCIT.BUSINESS_TYPE=2 ");
                sb.Append("   AND JCIT.PROCESS_TYPE=2 ");
                sb.Append("   AND M.MASTER_JC_STATUS = 1");
                sb.Append("  AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");

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

                sb.Append("  GROUP BY M.MASTER_JC_SEA_IMP_PK,");
                sb.Append("         M.MASTER_JC_REF_NO,");
                sb.Append("         M.MASTER_JC_DATE,");
                sb.Append("         OMT.OPERATOR_NAME,");
                sb.Append("         VST.VESSEL_NAME,");
                sb.Append("         VVT.VOYAGE, ");
                sb.Append("         POL.PORT_ID,");
                sb.Append("         M.POD_ETA,");
                sb.Append("         AMT.AGENT_NAME,");
                sb.Append("         M.CARGO_TYPE,");
                sb.Append("         CGMT.COMMODITY_GROUP_CODE");

                sb.Append(" ORDER BY M.MASTER_JC_DATE DESC, M.MASTER_JC_REF_NO DESC");
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

        #endregion "Sea Grid Function For MJC Pendind for deconsolidation"

        #region "AIR Grid Function For MJC Pendind for deconsolidation"

        /// <summary>
        /// Fetches the MJC air grid.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="Cargo_type">The cargo_type.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="POLAgentPK">The pol agent pk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <returns></returns>
        public DataSet FetchMJCAirGrid(Int32 LocFk = 0, string CustName = "", string Airline = "", string FromDt = "", string ToDt = "", string ETADt = "", string Cargo_type = "", string CommGrp = "", int POLPK = 0, int POLAgentPK = 0,
        Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 ExportExcel = 0)
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
                    strCondition = strCondition + " And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    strCondition = strCondition + " And TO_DATE(M.AOD_ETA, DATEFORMAT) = TO_DATE('" + ETADt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    strCondition = strCondition + " And ART.AIRLINE_NAME = '" + Airline + "'";
                }
                if (CommGrp != "0")
                {
                    strCondition = strCondition + " AND CGMT.COMMODITY_GROUP_PK =" + CommGrp;
                }
                if (POLPK > 0)
                {
                    strCondition = strCondition + " AND POL.PORT_MST_PK =" + POLPK;
                }
                if (POLAgentPK > 0)
                {
                    strCondition = strCondition + " AND AMT.AGENT_MST_PK =" + POLAgentPK;
                }
                sb.Append("SELECT M.MASTER_JC_AIR_IMP_PK JBCARD_PK,");
                sb.Append("       M.MASTER_JC_REF_NO,");
                sb.Append("       TO_DATE(M.MASTER_JC_DATE, 'DD/MM/YYYY') MSTJCDATE,");
                sb.Append("       'Air' BIZTYPE,");
                sb.Append("       '' CARGO_TYPE,");
                sb.Append("       ART.AIRLINE_NAME OPERATORNAME, ");
                sb.Append("       JCAI.VOYAGE_FLIGHT_NO VESVOYAGE,");
                sb.Append("       POL.PORT_ID,");
                sb.Append("       M.AOD_ETA POD_ETA,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS NET_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS VOLUME_IN_CBM,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_AIR_IMP_TBL M,");
                sb.Append("       PORT_MST_TBL          POL,");
                sb.Append("       PORT_MST_TBL          POD,");
                sb.Append("       AIRLINE_MST_TBL       ART,");
                sb.Append("       AGENT_MST_TBL         AMT,");
                sb.Append("       JOB_CARD_TRN  JCAI,");
                sb.Append("        CUSTOMER_MST_TBL     CMT,");
                sb.Append("        COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = M.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK");
                sb.Append("   AND ART.AIRLINE_MST_PK(+) = M.AIRLINE_MST_FK");
                sb.Append("   AND JCAI.MASTER_JC_FK=M.MASTER_JC_AIR_IMP_PK");
                sb.Append("   AND JCAI.CONSIGNEE_CUST_MST_FK=CMT.CUSTOMER_MST_PK(+)");
                sb.Append("   AND M.COMMODITY_GROUP_FK =CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JCAI.BUSINESS_TYPE=1 ");
                sb.Append("   AND JCAI.PROCESS_TYPE=2 ");
                sb.Append("   AND M.MASTER_JC_STATUS = 1");
                sb.Append("  AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");

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

                sb.Append("  GROUP BY M.MASTER_JC_AIR_IMP_PK,");
                sb.Append("         M.MASTER_JC_REF_NO,");
                sb.Append("         M.MASTER_JC_DATE,");
                sb.Append("         ART.AIRLINE_NAME,");
                sb.Append("         POL.PORT_ID,");
                sb.Append("         M.AOD_ETA,");
                sb.Append("         AMT.AGENT_NAME,");
                sb.Append("         CGMT.COMMODITY_GROUP_CODE,JCAI.VOYAGE_FLIGHT_NO");
                sb.Append(" ORDER BY M.MASTER_JC_DATE DESC, M.MASTER_JC_REF_NO DESC");
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

        #endregion "AIR Grid Function For MJC Pendind for deconsolidation"

        #region "Sea Grid Function For MJC Pendind for deconsolidation"

        /// <summary>
        /// Fetches the MJC all grid.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="Cargo_type">The cargo_type.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="POLAgentPK">The pol agent pk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ExportExcel">The export excel.</param>
        /// <returns></returns>
        public DataSet FetchMJCAllGrid(Int32 LocFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETADt = "", string Cargo_type = "", string CommGrp = "", int POLPK = 0,
        int POLAgentPK = 0, Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 ExportExcel = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strCondition = null;
            string strCondition1 = null;
            Int32 TotalRecords = default(Int32);
            try
            {
                if (flag == 0)
                {
                    strCondition += " AND 1=2";
                }

                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    strCondition = strCondition + " And TO_DATE(M.POD_ETA, DATEFORMAT) = TO_DATE('" + ETADt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And VST.VESSEL_NAME = '" + VslName + "'";
                    if (!string.IsNullOrEmpty(Voyage))
                    {
                        strCondition = strCondition + " And VVT.VOYAGE = '" + Voyage + "'";
                    }
                    else
                    {
                        strCondition = strCondition + " And VVT.VOYAGE is null ";
                    }
                }
                if (Cargo_type != "0")
                {
                    strCondition = strCondition + " AND M.CARGO_TYPE =" + Cargo_type;
                }
                if (CommGrp != "0")
                {
                    strCondition = strCondition + " AND CGMT.COMMODITY_GROUP_PK =" + CommGrp;
                }
                if (POLPK > 0)
                {
                    strCondition = strCondition + " AND POL.PORT_MST_PK =" + POLPK;
                }
                if (POLAgentPK > 0)
                {
                    strCondition = strCondition + " AND AMT.AGENT_MST_PK =" + POLAgentPK;
                }

                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition1 = strCondition1 + " And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition1 = strCondition1 + " And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    strCondition1 = strCondition1 + " And TO_DATE(M.AOD_ETA, DATEFORMAT) = TO_DATE('" + ETADt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition1 = strCondition1 + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition1 = strCondition1 + " And ART.AIRLINE_NAME = '" + VslName + "'";
                }
                if (CommGrp != "0")
                {
                    strCondition1 = strCondition1 + " AND CGMT.COMMODITY_GROUP_PK =" + CommGrp;
                }
                if (POLPK > 0)
                {
                    strCondition1 = strCondition1 + " AND POL.PORT_MST_PK =" + POLPK;
                }
                if (POLAgentPK > 0)
                {
                    strCondition1 = strCondition1 + " AND AMT.AGENT_MST_PK =" + POLAgentPK;
                }

                sb.Append("SELECT M.MASTER_JC_SEA_IMP_PK JBCARD_PK,");
                sb.Append("        M.MASTER_JC_REF_NO,");
                sb.Append("       TO_DATE(M.MASTER_JC_DATE, 'DD/MM/YYYY') MSTJCDATE,");
                sb.Append("       'Sea' BIZTYPE,");
                sb.Append("      DECODE(M.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGO_TYPE, ");
                sb.Append("       OMT.OPERATOR_NAME OPERATORNAME,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append("       POL.PORT_ID,");
                sb.Append("       M.POD_ETA POD_ETA,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       CASE");
                sb.Append("         WHEN M.CARGO_TYPE = 2 THEN");
                sb.Append("          (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("              AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("            GROUP BY M.MASTER_JC_SEA_IMP_PK)");
                sb.Append("         ELSE");
                sb.Append("          (SELECT SUM(JCONT.NET_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("              AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("            GROUP BY M.MASTER_JC_SEA_IMP_PK)");
                sb.Append("       END NET_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_SEA_IMP_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_SEA_IMP_PK) AS VOLUME_IN_CBM,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_SEA_IMP_TBL M,");
                sb.Append("       PORT_MST_TBL          POL,");
                sb.Append("       PORT_MST_TBL          POD,");
                sb.Append("       AGENT_MST_TBL         AMT,");
                sb.Append("       VESSEL_VOYAGE_TRN     VVT,");
                sb.Append("       VESSEL_VOYAGE_TBL     VST,");
                sb.Append("       OPERATOR_MST_TBL      OMT,");
                sb.Append("       JOB_CARD_TRN  JCIT,");
                sb.Append("      CUSTOMER_MST_TBL       CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = M.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK");
                sb.Append("   AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND M.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JCIT.MASTER_JC_FK=M.MASTER_JC_SEA_IMP_PK");
                sb.Append("   AND JCIT.CONSIGNEE_CUST_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                sb.Append("   AND M.COMMODITY_GROUP_FK =CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JCIT.BUSINESS_TYPE=2 ");
                sb.Append("   AND JCIT.PROCESS_TYPE=2 ");
                sb.Append("   AND M.MASTER_JC_STATUS = 1");
                sb.Append("  AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                sb.Append(strCondition);
                sb.Append("  GROUP BY M.MASTER_JC_SEA_IMP_PK,");
                sb.Append("         M.MASTER_JC_REF_NO,");
                sb.Append("         M.MASTER_JC_DATE,");
                sb.Append("         OMT.OPERATOR_NAME,");
                sb.Append("         VST.VESSEL_NAME,");
                sb.Append("         VVT.VOYAGE, ");
                sb.Append("         POL.PORT_ID,");
                sb.Append("         M.POD_ETA,");
                sb.Append("         AMT.AGENT_NAME,");
                sb.Append("         M.CARGO_TYPE,");
                sb.Append("         CGMT.COMMODITY_GROUP_CODE");

                sb.Append(" UNION ");
                sb.Append("SELECT M.MASTER_JC_AIR_IMP_PK JBCARD_PK,");
                sb.Append("       M.MASTER_JC_REF_NO,");
                sb.Append("       TO_DATE(M.MASTER_JC_DATE, 'DD/MM/YYYY') MSTJCDATE,");
                sb.Append("       'Air' BIZTYPE,");
                sb.Append("       '' CARGO_TYPE,");
                sb.Append("       ART.AIRLINE_NAME OPERATORNAME,");
                sb.Append("       JCAI.VOYAGE_FLIGHT_NO VESVOYAGE,");
                sb.Append("       POL.PORT_ID,");
                sb.Append("       M.AOD_ETA POD_ETA,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS NET_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS VOLUME_IN_CBM,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_AIR_IMP_TBL M,");
                sb.Append("       PORT_MST_TBL          POL,");
                sb.Append("       PORT_MST_TBL          POD,");
                sb.Append("       AIRLINE_MST_TBL       ART,");
                sb.Append("       AGENT_MST_TBL         AMT,");
                sb.Append("       JOB_CARD_TRN  JCAI,");
                sb.Append("        CUSTOMER_MST_TBL     CMT,");
                sb.Append("        COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = M.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK");
                sb.Append("   AND ART.AIRLINE_MST_PK(+) = M.AIRLINE_MST_FK");
                sb.Append("   AND JCAI.MASTER_JC_FK=M.MASTER_JC_AIR_IMP_PK");
                sb.Append("   AND JCAI.CONSIGNEE_CUST_MST_FK=CMT.CUSTOMER_MST_PK(+)");
                sb.Append("   AND M.COMMODITY_GROUP_FK =CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND JCAI.BUSINESS_TYPE=1 ");
                sb.Append("   AND JCAI.PROCESS_TYPE=2 ");
                sb.Append("   AND M.MASTER_JC_STATUS = 1");
                sb.Append("  AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                sb.Append(strCondition1);
                sb.Append("  GROUP BY M.MASTER_JC_AIR_IMP_PK,");
                sb.Append("         M.MASTER_JC_REF_NO,");
                sb.Append("         M.MASTER_JC_DATE,");
                sb.Append("         ART.AIRLINE_NAME,");
                sb.Append("         POL.PORT_ID,");
                sb.Append("         M.AOD_ETA,");
                sb.Append("         AMT.AGENT_NAME,");
                sb.Append("         CGMT.COMMODITY_GROUP_CODE,JCAI.VOYAGE_FLIGHT_NO");

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

                strSQL = " SELECT * FROM (SELECT ROWNUM SR_NO, QRY.* FROM(SELECT Q.* FROM( ";
                strSQL += sb.ToString();

                if (ExportExcel == 0)
                {
                    strSQL += " )q ORDER BY Q.MSTJCDATE DESC,Q.MASTER_JC_REF_NO DESC)QRY ) WHERE SR_NO Between " + start + " and " + last;
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

        #endregion "Sea Grid Function For MJC Pendind for deconsolidation"

        #region "Sea Report Function For MJC Pendind for deconsolidation"

        /// <summary>
        /// Fetches the MJC sea report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="Cargo_type">The cargo_type.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="POLAgentPK">The pol agent pk.</param>
        /// <returns></returns>
        public DataSet FetchMJCSeaReport(Int32 LocFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETADt = "", string Cargo_type = "", string CommGrp = "", int POLPK = 0,
        int POLAgentPK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT M.MASTER_JC_SEA_IMP_PK,");
                sb.Append("        M.MASTER_JC_REF_NO,");
                sb.Append("       TO_DATE(M.MASTER_JC_DATE, 'DD/MM/YYYY') MSTJCDATE,");
                sb.Append("       OMT.OPERATOR_NAME OPERATORNAME,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append("       POL.PORT_ID,");
                sb.Append("       TO_CHAR( M.POD_ETA, DATETIMEFORMAT24) POD_ETA,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("      DECODE(M.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGO_TYPE, ");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       CASE");
                sb.Append("         WHEN M.CARGO_TYPE = 2 THEN");
                sb.Append("          (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("              AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("            GROUP BY M.MASTER_JC_SEA_IMP_PK)");
                sb.Append("         ELSE");
                sb.Append("          (SELECT SUM(JCONT.NET_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("              AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("            GROUP BY M.MASTER_JC_SEA_IMP_PK)");
                sb.Append("       END NET_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_SEA_IMP_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_SEA_IMP_PK) AS VOLUME_IN_CBM,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_SEA_IMP_TBL M,");
                sb.Append("       PORT_MST_TBL          POL,");
                sb.Append("       PORT_MST_TBL          POD,");
                sb.Append("       AGENT_MST_TBL         AMT,");
                sb.Append("       VESSEL_VOYAGE_TRN     VVT,");
                sb.Append("       VESSEL_VOYAGE_TBL     VST,");
                sb.Append("       OPERATOR_MST_TBL      OMT,");
                sb.Append("       JOB_CARD_TRN  JCIT,");
                sb.Append("      CUSTOMER_MST_TBL       CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = M.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK");
                sb.Append("   AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND M.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JCIT.MASTER_JC_FK=M.MASTER_JC_SEA_IMP_PK");
                sb.Append("   AND JCIT.CONSIGNEE_CUST_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                sb.Append("   AND M.COMMODITY_GROUP_FK =CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND M.MASTER_JC_STATUS = 1");
                sb.Append("  AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME= '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And VST.VESSEL_NAME = '" + VslName + "'");
                    if (!string.IsNullOrEmpty(Voyage))
                    {
                        sb.Append(" And VVT.VOYAGE = '" + Voyage + "'");
                    }
                    else
                    {
                        sb.Append(" And VVT.VOYAGE is null");
                    }
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(M.POD_ETA, DATEFORMAT) = TO_DATE('" + ETADt + "',dateformat) ");
                }
                sb.Append("  GROUP BY M.MASTER_JC_SEA_IMP_PK,");
                sb.Append("         M.MASTER_JC_REF_NO,");
                sb.Append("         M.MASTER_JC_DATE,");
                sb.Append("         OMT.OPERATOR_NAME,");
                sb.Append("         VST.VESSEL_NAME,");
                sb.Append("         VVT.VOYAGE, ");
                sb.Append("         POL.PORT_ID,");
                sb.Append("         M.POD_ETA,");
                sb.Append("         AMT.AGENT_NAME,");
                sb.Append("         M.CARGO_TYPE,");
                sb.Append("         CGMT.COMMODITY_GROUP_CODE");
                sb.Append(" ORDER BY M.MASTER_JC_DATE DESC, M.MASTER_JC_REF_NO DESC");
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

        #endregion "Sea Report Function For MJC Pendind for deconsolidation"

        #region "Air Report Function For MJC Pendind for deconsolidation"

        /// <summary>
        /// Fetches the MJC air report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="Cargo_type">The cargo_type.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="POLAgentPK">The pol agent pk.</param>
        /// <returns></returns>
        public DataSet FetchMJCAirReport(Int32 LocFk = 0, string CustName = "", string Airline = "", string FromDt = "", string ToDt = "", string ETADt = "", string Cargo_type = "", string CommGrp = "", int POLPK = 0, int POLAgentPK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append("SELECT M.MASTER_JC_AIR_IMP_PK,");
                sb.Append("         M.MASTER_JC_REF_NO,");
                sb.Append("       TO_DATE(M.MASTER_JC_DATE, 'DD/MM/YYYY') MSTJCDATE,");
                sb.Append("       ART.AIRLINE_NAME, ");
                sb.Append("       POL.PORT_ID,");
                sb.Append("       TO_CHAR(M.AOD_ETA, DATETIMEFORMAT24) AOD_ETA,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS CHARGEABLE_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS VOLUME_IN_CBM,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_AIR_IMP_TBL M,");
                sb.Append("       PORT_MST_TBL          POL,");
                sb.Append("       PORT_MST_TBL          POD,");
                sb.Append("       AIRLINE_MST_TBL       ART,");
                sb.Append("       AGENT_MST_TBL         AMT,");
                sb.Append("       JOB_CARD_TRN  JCAI,");
                sb.Append("        CUSTOMER_MST_TBL     CMT,");
                sb.Append("        COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = M.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK");
                sb.Append("   AND ART.AIRLINE_MST_PK(+) = M.AIRLINE_MST_FK");
                sb.Append("   AND JCAI.MASTER_JC_FK=M.MASTER_JC_AIR_IMP_PK");
                sb.Append("   AND JCAI.CONSIGNEE_CUST_MST_FK=CMT.CUSTOMER_MST_PK(+)");
                sb.Append("   AND M.COMMODITY_GROUP_FK =CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND M.MASTER_JC_STATUS = 1");
                sb.Append("  AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(Airline))
                {
                    sb.Append(" And ART.AIRLINE_NAME = '" + Airline + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(M.AOD_ETA, DATEFORMAT) = TO_DATE('" + ETADt + "',dateformat) ");
                }
                sb.Append("  GROUP BY M.MASTER_JC_AIR_IMP_PK,");
                sb.Append("         M.MASTER_JC_REF_NO,");
                sb.Append("         M.MASTER_JC_DATE,");
                sb.Append("         ART.AIRLINE_NAME,");
                sb.Append("         POL.PORT_ID,");
                sb.Append("         M.AOD_ETA,");
                sb.Append("         AMT.AGENT_NAME,");
                sb.Append("         CGMT.COMMODITY_GROUP_CODE");
                sb.Append(" ORDER BY M.MASTER_JC_DATE DESC, M.MASTER_JC_REF_NO DESC");
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

        #region "Sea Report Function For MJC Pendind for deconsolidation"

        /// <summary>
        /// Fetches the MJC all report.
        /// </summary>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETADt">The eta dt.</param>
        /// <param name="Cargo_type">The cargo_type.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="POLAgentPK">The pol agent pk.</param>
        /// <returns></returns>
        public DataSet FetchMJCAllReport(Int32 LocFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETADt = "", string Cargo_type = "", string CommGrp = "", int POLPK = 0,
        int POLAgentPK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            try
            {
                sb.Append(" SELECT * FROM ( ");
                sb.Append("SELECT M.MASTER_JC_SEA_IMP_PK,");
                sb.Append("        M.MASTER_JC_REF_NO,");
                sb.Append("       TO_DATE(M.MASTER_JC_DATE, 'DD/MM/YYYY') MSTJCDATE,");
                sb.Append("       'Sea' BIZTYPE, ");
                sb.Append("      DECODE(M.CARGO_TYPE, '1','FCL','2','LCL','4','BBC') CARGO_TYPE, ");
                sb.Append("       OMT.OPERATOR_NAME OPERATORNAME,");
                sb.Append("       (CASE");
                sb.Append("         WHEN (NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '') = '/') THEN");
                sb.Append("          ''");
                sb.Append("         ELSE");
                sb.Append("          NVL(VST.VESSEL_NAME, '') || '/' || NVL(VVT.VOYAGE, '')");
                sb.Append("       END) AS VESVOYAGE,");
                sb.Append("       POL.PORT_ID,");
                sb.Append("       TO_CHAR( M.POD_ETA, DATETIMEFORMAT24) POD_ETA,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       CASE");
                sb.Append("         WHEN M.CARGO_TYPE = 2 THEN");
                sb.Append("          (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("              AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("            GROUP BY M.MASTER_JC_SEA_IMP_PK)");
                sb.Append("         ELSE");
                sb.Append("          (SELECT SUM(JCONT.NET_WEIGHT)");
                sb.Append("             FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("            WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("              AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("            GROUP BY M.MASTER_JC_SEA_IMP_PK)");
                sb.Append("       END NET_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_SEA_IMP_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_SEA_IMP_PK) AS VOLUME_IN_CBM,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_SEA_IMP_TBL M,");
                sb.Append("       PORT_MST_TBL          POL,");
                sb.Append("       PORT_MST_TBL          POD,");
                sb.Append("       AGENT_MST_TBL         AMT,");
                sb.Append("       VESSEL_VOYAGE_TRN     VVT,");
                sb.Append("       VESSEL_VOYAGE_TBL     VST,");
                sb.Append("       OPERATOR_MST_TBL      OMT,");
                sb.Append("       JOB_CARD_TRN  JCIT,");
                sb.Append("      CUSTOMER_MST_TBL       CMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = M.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK");
                sb.Append("   AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND M.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JCIT.MASTER_JC_FK=M.MASTER_JC_SEA_IMP_PK");
                sb.Append("   AND JCIT.CONSIGNEE_CUST_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                sb.Append("   AND M.COMMODITY_GROUP_FK =CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND M.MASTER_JC_STATUS = 1");
                sb.Append("  AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME= '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And VST.VESSEL_NAME = '" + VslName + "'");
                    if (!string.IsNullOrEmpty(Voyage))
                    {
                        sb.Append(" And VVT.VOYAGE = '" + Voyage + "'");
                    }
                    else
                    {
                        sb.Append(" And VVT.VOYAGE is null");
                    }
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(M.POD_ETA, DATEFORMAT) = TO_DATE('" + ETADt + "',dateformat) ");
                }
                sb.Append("  GROUP BY M.MASTER_JC_SEA_IMP_PK,");
                sb.Append("         M.MASTER_JC_REF_NO,");
                sb.Append("         M.MASTER_JC_DATE,");
                sb.Append("         OMT.OPERATOR_NAME,");
                sb.Append("         VST.VESSEL_NAME,");
                sb.Append("         VVT.VOYAGE, ");
                sb.Append("         POL.PORT_ID,");
                sb.Append("         M.POD_ETA,");
                sb.Append("         AMT.AGENT_NAME,");
                sb.Append("         M.CARGO_TYPE,");
                sb.Append("         CGMT.COMMODITY_GROUP_CODE");

                sb.Append(" UNION ");

                sb.Append("SELECT M.MASTER_JC_AIR_IMP_PK,");
                sb.Append("         M.MASTER_JC_REF_NO,");
                sb.Append("       TO_DATE(M.MASTER_JC_DATE, 'DD/MM/YYYY') MSTJCDATE,");
                sb.Append("       'Air' BIZTYPE,");
                sb.Append("       '' CARGO_TYPE,");
                sb.Append("       ART.AIRLINE_NAME OPERATORNAME, ");
                sb.Append("       JCAI.VOYAGE_FLIGHT_NO AS VESVOYAGE, ");
                sb.Append("       POL.PORT_ID,");
                sb.Append("       TO_CHAR(M.AOD_ETA, DATETIMEFORMAT24) POD_ETA,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS NET_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
                sb.Append("         GROUP BY M.MASTER_JC_AIR_IMP_PK) AS VOLUME_IN_CBM,");
                sb.Append("       '' SEL");
                sb.Append("  FROM MASTER_JC_AIR_IMP_TBL M,");
                sb.Append("       PORT_MST_TBL          POL,");
                sb.Append("       PORT_MST_TBL          POD,");
                sb.Append("       AIRLINE_MST_TBL       ART,");
                sb.Append("       AGENT_MST_TBL         AMT,");
                sb.Append("       JOB_CARD_TRN  JCAI,");
                sb.Append("        CUSTOMER_MST_TBL     CMT,");
                sb.Append("        COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = M.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK");
                sb.Append("   AND ART.AIRLINE_MST_PK(+) = M.AIRLINE_MST_FK");
                sb.Append("   AND JCAI.MASTER_JC_FK=M.MASTER_JC_AIR_IMP_PK");
                sb.Append("   AND JCAI.CONSIGNEE_CUST_MST_FK=CMT.CUSTOMER_MST_PK(+)");
                sb.Append("   AND M.COMMODITY_GROUP_FK =CGMT.COMMODITY_GROUP_PK(+)");
                sb.Append("   AND M.MASTER_JC_STATUS = 1");
                sb.Append("  AND POD.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append(" And ART.AIRLINE_NAME = '" + VslName + "'");
                }
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE(M.MASTER_JC_DATE, DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat) ");
                }
                if (!string.IsNullOrEmpty(ETADt))
                {
                    sb.Append(" And TO_DATE(M.AOD_ETA, DATEFORMAT) = TO_DATE('" + ETADt + "',dateformat) ");
                }
                sb.Append("  GROUP BY M.MASTER_JC_AIR_IMP_PK,");
                sb.Append("         M.MASTER_JC_REF_NO,");
                sb.Append("         M.MASTER_JC_DATE,");
                sb.Append("         ART.AIRLINE_NAME,");
                sb.Append("         POL.PORT_ID,");
                sb.Append("         M.AOD_ETA,");
                sb.Append("         AMT.AGENT_NAME,JCAI.VOYAGE_FLIGHT_NO,");
                sb.Append("         CGMT.COMMODITY_GROUP_CODE");
                sb.Append(" ) Q ORDER BY Q.MSTJCDATE DESC, Q.MASTER_JC_REF_NO DESC ");

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

        #endregion "Sea Report Function For MJC Pendind for deconsolidation"

        #region "Container Allocation Grid Function"

        /// <summary>
        /// Fetches the container details.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustomerMstPk">The customer MST pk.</param>
        /// <param name="VslVoyTrnPk">The VSL voy TRN pk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="expExcel">The exp excel.</param>
        /// <param name="VoyFlightNr">The voy flight nr.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetails(Int32 BookingPk = 0, Int32 LocFk = 0, string CustomerMstPk = "", string VslVoyTrnPk = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
        Int32 expExcel = 0, string VoyFlightNr = "")
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
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And BST.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And BST.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustomerMstPk.Trim()) & CustomerMstPk.Trim() != "0")
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_MST_PK IN (" + CustomerMstPk + ")";
                }
                if (!string.IsNullOrEmpty(VslVoyTrnPk.Trim()) & VslVoyTrnPk.Trim() != "0")
                {
                    strCondition = strCondition + " And JCSET.VOYAGE_TRN_FK IN (" + VslVoyTrnPk.Trim() + ")";
                }
                if (!string.IsNullOrEmpty(VoyFlightNr.Trim()) & VoyFlightNr.Trim() != "0")
                {
                    strCondition = strCondition + " And (UPPER(VT.VOYAGE)='" + VoyFlightNr.Trim().ToUpper() + "' OR UPPER(JCSET.VOYAGE_FLIGHT_NO) = '" + VoyFlightNr.Trim().ToUpper() + "'";
                }
                sb.Append("SELECT DISTINCT BST.BOOKING_MST_PK,");
                sb.Append("                BST.BOOKING_REF_NO,");
                sb.Append("                JCSET.JOB_CARD_TRN_PK,");
                sb.Append("                JCSET.JOBCARD_REF_NO,");
                sb.Append("                TO_DATE(BST.SHIPMENT_DATE,DATEFORMAT) SHIPMENT_DATE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                POL.PORT_ID POL,");
                sb.Append("                POD.PORT_ID POD,");
                sb.Append("                ");
                sb.Append("                CASE");
                sb.Append("                  WHEN VVT.VESSEL_NAME IS NULL THEN");
                sb.Append("                   VVT.VESSEL_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("                END AS \"VESSEL_VOYAGE\",");
                sb.Append("                TO_DATE(JCSET.ETD_DATE,DATETIMEFORMAT24) ETD_DATE,");
                sb.Append("                TO_DATE(JCSET.ETA_DATE,DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK ");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = jcset.shipper_cust_mst_fk");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("   AND JCSET.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND BST.BUSINESS_TYPE=2 ");
                sb.Append("   AND BST.CARGO_TYPE IN (1, 2)");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSET.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK(+) = JCSET.JOB_CARD_TRN_PK");
                sb.Append("   AND JCONT.CONTAINER_NUMBER IS NULL");
                sb.Append("   AND POL.LOCATION_MST_FK = " + LocFk + "");
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

                sb.Append(" order by JCSET.JOB_CARD_TRN_PK desc, TO_DATE(SHIPMENT_DATE) DESC ");
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

        #endregion "Container Allocation Grid Function"

        #region "Container Allocation Print"

        /// <summary>
        /// Fetches the container report.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchContainerReport(Int32 BookingPk = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT BST.BOOKING_MST_PK,");
                sb.Append("                BST.BOOKING_REF_NO,");
                sb.Append("                JCSET.JOB_CARD_TRN_PK,");
                sb.Append("                JCSET.JOBCARD_REF_NO,");
                sb.Append("                TO_DATE(BST.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                POL.PORT_ID POL,");
                sb.Append("                POD.PORT_ID POD,");
                sb.Append("                ");
                sb.Append("                CASE");
                sb.Append("                  WHEN VVT.VESSEL_NAME IS NULL THEN");
                sb.Append("                   VVT.VESSEL_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("                END AS \"VESSEL_VOYAGE\",");
                sb.Append("                JCSET.ETD_DATE,");
                sb.Append("                JCSET.ETA_DATE,");
                sb.Append("                DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK ");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("   AND JCSET.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND BST.CARGO_TYPE IN (1, 2)");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSET.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK(+) = JCSET.JOB_CARD_TRN_PK");
                sb.Append("   AND JCONT.CONTAINER_NUMBER IS NULL");
                sb.Append("   AND POL.LOCATION_MST_FK = " + LocFk + "");

                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append("  And BST.BOOKING_DATE >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append("  And BST.BOOKING_DATE <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append("  And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append("  And BST.VESSEL_NAME = '" + VslName + "'");
                }
                sb.Append(" order by BST.BOOKING_REF_NO desc ");
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

        #endregion "Container Allocation Print"

        #region "Booking (Cargo) Not Recieved in Warehouse Grid Function"

        /// <summary>
        /// Fetches the BKG warehouse.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="Process">The process.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustomerMstPks">The customer MST PKS.</param>
        /// <param name="VslVoyTrnPks">The VSL voy TRN PKS.</param>
        /// <param name="VoyName">Name of the voy.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="expExcel">The exp excel.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchBkgWarehouse(Int32 Business = 0, Int32 Process = 0, Int32 LocFk = 0, string CustomerMstPks = "", string VslVoyTrnPks = "", string VoyName = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0,
        Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 expExcel = 0, Int32 CargoType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            Int32 last = 0;
            Int32 start = 0;
            string strSQL = null;
            string strSQL1 = null;
            string strCondition = null;
            string strCondition1 = null;
            string strCondition2 = null;
            string strCondition3 = null;
            string strCondition4 = null;
            Int32 TotalRecords = default(Int32);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                //-------------------------SEA EXP--------------------------
                sb.Append("SELECT DISTINCT BST.BOOKING_MST_PK BOOKING_PK,");
                sb.Append("                BST.BOOKING_REF_NO,");
                sb.Append("                JCSET.JOB_CARD_TRN_PK JOBCARD_PK,");
                sb.Append("                JCSET.JOBCARD_REF_NO,");
                sb.Append("                TO_DATE(JCSET.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                sb.Append("                TO_DATE(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=2 AND DD.DD_FLAG='BIZ_TYPE') BIZ_TYPE, ");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=1 AND DD.DD_FLAG='PROCESS_TYPE') PROCESS_TYPE,");
                sb.Append("                DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                POD.PORT_ID,");
                sb.Append("                CASE");
                sb.Append("                  WHEN VT.VOYAGE IS NULL THEN");
                sb.Append("                   VVT.VESSEL_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("                END AS \"VESSEL_FLIGHT\",");
                sb.Append("                JCSET.ETD_DATE ETD_DATE,");
                sb.Append("                JCSET.ETA_DATE ETA_DATE,");
                sb.Append("                SUM(JCONT.PACK_COUNT) PACK_COUNT,");
                sb.Append("                SUM(JCONT.NET_WEIGHT) NET_CHARGE_WEIGHT,");
                sb.Append("                SUM(JCONT.VOLUME_IN_CBM) VOLUME_IN_CBM, ");
                sb.Append("                2 BIZTYPE, ");
                sb.Append("                1 PROCESSTYPE ");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK");
                //sb.Append("   AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK")
                sb.Append("   AND CMT.CUSTOMER_MST_PK = JCSET.CUST_CUSTOMER_MST_FK ");

                sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("   AND BST.STATUS <> 3");
                sb.Append("   and jcset.business_type=2 AND JCSET.PROCESS_TYPE=1 ");
                sb.Append("   AND JCSET.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSET.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("   AND POL.LOCATION_MST_FK = " + LocFk + "");
                //sb.Append("   AND JCSET.TRANSPORTER_CARRIER_FK IS NULL")
                sb.Append("   AND JCSET.TRANSPORTER_DEPOT_FK IS NULL ");
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE, 'DD/MM/YYYY') >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE(JCSET.JOBCARD_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(JCSET.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustomerMstPks.Trim()) & CustomerMstPks.Trim() != "0")
                {
                    sb.Append(" And JCSET.CUST_CUSTOMER_MST_FK IN (" + CustomerMstPks + ") ");
                }
                if (!string.IsNullOrEmpty(VslVoyTrnPks.Trim()) & VslVoyTrnPks.Trim() != "0")
                {
                    sb.Append(" And VT.VOYAGE_TRN_PK IN (" + VslVoyTrnPks.Trim() + ")");
                }
                if (!string.IsNullOrEmpty(VoyName.Trim()) & VoyName.Trim() != "0")
                {
                    sb.Append(" And (UPPER(VT.VOYAGE)='" + VoyName.Trim().ToUpper() + "' OR UPPER(JCSET.VOYAGE_FLIGHT_NO) = '" + VoyName.Trim().ToUpper() + "')");
                }

                if (CargoType > 0)
                {
                    sb.Append(" And BST.CARGO_TYPE = " + CargoType + "");
                }
                sb.Append(" GROUP BY BST.BOOKING_MST_PK,");
                sb.Append("                          BST.BOOKING_REF_NO,");
                sb.Append("                          JCSET.JOB_CARD_TRN_PK,");
                sb.Append("                          JCSET.JOBCARD_REF_NO,");
                sb.Append("                          JCSET.JOBCARD_DATE,");
                sb.Append("                          BST.SHIPMENT_DATE,");
                sb.Append("                          CMT.CUSTOMER_NAME,");
                sb.Append("                          POD.PORT_ID,");
                sb.Append("                          BST.CARGO_TYPE,");
                sb.Append("                          VT.VOYAGE,");
                sb.Append("                          VVT.VESSEL_NAME,");
                sb.Append("                          JCSET.ETD_DATE,");
                sb.Append("                          JCSET.ETA_DATE,");
                sb.Append("                          CGMT.COMMODITY_GROUP_CODE ");

                sb.Append("                          UNION ");
                //-------------------------SEA IMP--------------------------
                sb.Append(" SELECT DISTINCT 0 BOOKING_PK,");
                sb.Append("                '' BOOKING_REF_NO,");
                sb.Append("                JCSIT.JOB_CARD_TRN_PK JOBCARD_PK,");
                sb.Append("                JCSIT.JOBCARD_REF_NO,");
                sb.Append("                TO_DATE(JCSIT.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE, ");
                //sb.Append("                '' SHIPMENT_DATE,")
                sb.Append("                TO_DATE(NULL) SHIPMENT_DATE,");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=2 AND DD.DD_FLAG='BIZ_TYPE') BIZ_TYPE, ");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=1 AND DD.DD_FLAG='PROCESS_TYPE') PROCESS_TYPE,");
                sb.Append("                DECODE(JCSIT.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                POL.PORT_ID,");
                sb.Append("                CASE");
                sb.Append("                  WHEN VT.VOYAGE IS NULL THEN");
                sb.Append("                   VVT.VESSEL_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("                END AS \"VESSEL_FLIGHT\",");
                sb.Append("                JCSIT.ETD_DATE ETD_DATE,");
                sb.Append("                JCSIT.ETA_DATE ETA_DATE,");
                sb.Append("                SUM(JCONT.PACK_COUNT) PACK_COUNT,");
                sb.Append("                SUM(JCONT.NET_WEIGHT) NET_CHARGE_WEIGHT,");
                sb.Append("                SUM(JCONT.VOLUME_IN_CBM) VOLUME_IN_CBM, ");
                sb.Append("                2 BIZTYPE, ");
                sb.Append("                2 PROCESSTYPE ");
                sb.Append("  FROM JOB_CARD_TRN    JCSIT,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append("");
                sb.Append(" WHERE CMT.CUSTOMER_MST_PK = JCSIT.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK = JCSIT.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JCSIT.PORT_MST_POD_FK");
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSIT.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCSIT.JOB_CARD_TRN_PK");
                sb.Append("   AND JCSIT.JOB_CARD_STATUS <> 2");
                sb.Append("   and jcsit.business_type=2 AND JCSIT.PROCESS_TYPE=2 ");
                sb.Append("   AND POD.LOCATION_MST_FK = " + LocFk + "");
                sb.Append("   AND JCSIT.TRANSPORTER_DEPOT_FK IS NULL");
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition1 = strCondition1 + " And TO_DATE(JCSIT.JOBCARD_DATE, 'DD/MM/YYYY') >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition1 = strCondition1 + " And TO_DATE(JCSIT.JOBCARD_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition1 = strCondition1 + " And TO_DATE(JCSIT.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustomerMstPks.Trim()) & CustomerMstPks.Trim() != "0")
                {
                    strCondition1 = strCondition1 + " And JCSIT.CUST_CUSTOMER_MST_FK IN (" + CustomerMstPks + ") ";
                }
                if (!string.IsNullOrEmpty(VslVoyTrnPks.Trim()) & VslVoyTrnPks.Trim() != "0")
                {
                    strCondition1 = strCondition1 + " And VT.VOYAGE_TRN_PK IN (" + VslVoyTrnPks.Trim() + ")";
                }
                if (!string.IsNullOrEmpty(VoyName.Trim()) & VoyName.Trim() != "0")
                {
                    sb.Append(" And (UPPER(VT.VOYAGE)='" + VoyName.Trim().ToUpper() + "' OR UPPER(JCSIT.VOYAGE_FLIGHT_NO) = '" + VoyName.Trim().ToUpper() + "')");
                }
                //If VoyName <> "" Then
                //    strCondition1 = strCondition1 & " And VT.VOYAGE = '" & VoyName & "'"
                //End If
                if (CargoType > 0)
                {
                    strCondition1 = strCondition1 + " And JCSIT.CARGO_TYPE = " + CargoType + "";
                }
                sb.Append(strCondition1);
                sb.Append(" GROUP BY ");
                sb.Append("                          JCSIT.JOB_CARD_TRN_PK,");
                sb.Append("                          JCSIT.JOBCARD_REF_NO,");
                sb.Append("                          JCSIT.JOBCARD_DATE,");
                sb.Append("                          CMT.CUSTOMER_NAME,");
                sb.Append("                          POL.PORT_ID,");
                sb.Append("                          JCSIT.CARGO_TYPE,");
                sb.Append("                          VT.VOYAGE,");
                sb.Append("                          VVT.VESSEL_NAME,");
                sb.Append("                          JCSIT.ETD_DATE,");
                sb.Append("                          JCSIT.ETA_DATE,");
                sb.Append("                          CGMT.COMMODITY_GROUP_CODE ");

                sb.Append("                          UNION ");
                //-------------------------AIR EXP--------------------------
                sb.Append("SELECT DISTINCT BAT.BOOKING_MST_PK BOOKING_PK,");
                sb.Append("                BAT.BOOKING_REF_NO,");
                sb.Append("                JCAET.JOB_CARD_TRN_PK JOBCARD_PK,");
                sb.Append("                JCAET.JOBCARD_REF_NO,");
                sb.Append("                TO_DATE(JCAET.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE, ");
                sb.Append("                TO_DATE(BAT.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=1 AND DD.DD_FLAG='BIZ_TYPE') BIZ_TYPE, ");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=1 AND DD.DD_FLAG='PROCESS_TYPE') PROCESS_TYPE,");
                sb.Append("                DECODE(BAT.CARGO_TYPE, '1', 'KGS', '2', 'ULD') CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                AOD.PORT_ID,");
                sb.Append("                CASE");
                sb.Append("                  WHEN JCAET.VOYAGE_FLIGHT_NO IS NULL THEN");
                sb.Append("                   AMT.AIRLINE_ID");
                sb.Append("                  ELSE");
                sb.Append("                   (AMT.AIRLINE_ID || '/' || JCAET.VOYAGE_FLIGHT_NO)");
                sb.Append("                END AS \"VESSEL_FLIGHT\",");
                sb.Append("                JCAET.ETD_DATE ETD_DATE,");
                sb.Append("                JCAET.ETA_DATE ETA_DATE,");
                sb.Append("                SUM(JCONT.PACK_COUNT) PACK_COUNT,");
                sb.Append("                SUM(JCONT.CHARGEABLE_WEIGHT) NET_CHARGE_WEIGHT,");
                sb.Append("                SUM(JCONT.VOLUME_IN_CBM) VOLUME_IN_CBM, ");
                sb.Append("                1 BIZTYPE, ");
                sb.Append("                1 PROCESSTYPE ");
                sb.Append("  FROM BOOKING_MST_TBL         BAT,");
                sb.Append("       JOB_CARD_TRN    JCAET,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append("");
                sb.Append(" WHERE BAT.BOOKING_MST_PK = JCAET.BOOKING_MST_FK");
                //sb.Append("   AND CMT.CUSTOMER_MST_PK = BAT.CUST_CUSTOMER_MST_FK")
                sb.Append("   AND CMT.CUSTOMER_MST_PK = JCAET.CUST_CUSTOMER_MST_FK ");
                sb.Append("   AND AOD.PORT_MST_PK = BAT.PORT_MST_POD_FK");
                sb.Append("   AND AOO.PORT_MST_PK = BAT.PORT_MST_POL_FK");
                sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCAET.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCAET.JOB_CARD_TRN_PK");
                sb.Append("    and jcaet.business_type=1 AND JCAET.PROCESS_TYPE=1");
                sb.Append("   AND BAT.STATUS <> 3");
                sb.Append("   AND AOO.LOCATION_MST_FK = " + LocFk + "");
                sb.Append("   AND JCAET.TRANSPORTER_DEPOT_FK IS NULL");
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition2 = strCondition2 + " And TO_DATE(JCAET.JOBCARD_DATE, 'DD/MM/YYYY') >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition2 = strCondition2 + " And TO_DATE(JCAET.JOBCARD_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition2 = strCondition2 + " And TO_DATE(JCAET.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustomerMstPks.Trim()) & CustomerMstPks.Trim() != "0")
                {
                    strCondition2 = strCondition2 + " And JCAET.CUST_CUSTOMER_MST_FK IN (" + CustomerMstPks + ") ";
                }
                if (!string.IsNullOrEmpty(VslVoyTrnPks.Trim()) & VslVoyTrnPks.Trim() != "0")
                {
                    strCondition2 = strCondition2 + " And AMT.AIRLINE_MST_PK IN (" + VslVoyTrnPks.Trim() + ")";
                }
                if (!string.IsNullOrEmpty(VoyName))
                {
                    strCondition2 = strCondition2 + " And UPPER(JCAET.VOYAGE_FLIGHT_NO) = '" + VoyName.Trim().ToUpper() + "'";
                }
                sb.Append(strCondition2);
                sb.Append(" GROUP BY BAT.BOOKING_MST_PK, BAT.BOOKING_REF_NO,");
                sb.Append("                          JCAET.JOB_CARD_TRN_PK,");
                sb.Append("                          JCAET.JOBCARD_REF_NO,");
                sb.Append("                          JCAET.JOBCARD_DATE,");
                sb.Append("                          CMT.CUSTOMER_NAME, ");
                sb.Append("                          AOD.PORT_ID,  ");
                sb.Append("                          BAT.SHIPMENT_DATE,");
                sb.Append("                          JCAET.VOYAGE_FLIGHT_NO,");
                sb.Append("                          AMT.AIRLINE_ID, BAT.CARGO_TYPE, ");
                sb.Append("                          JCAET.ETD_DATE,");
                sb.Append("                          JCAET.ETA_DATE,");
                sb.Append("                          CGMT.COMMODITY_GROUP_CODE ");

                sb.Append("                          UNION ");
                //-------------------------AIR IMP--------------------------
                sb.Append("SELECT DISTINCT 0 BOOKING_PK,");
                sb.Append("                '' BOOKING_REF_NO,");
                sb.Append("                JCAIT.JOB_CARD_TRN_PK JOBCARD_PK,");
                sb.Append("                JCAIT.JOBCARD_REF_NO,");
                sb.Append("                TO_DATE(JCAIT.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                //sb.Append("                '' SHIPMENT_DATE,")
                sb.Append("                TO_DATE(NULL) SHIPMENT_DATE,");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=1 AND DD.DD_FLAG='BIZ_TYPE') BIZ_TYPE, ");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=2 AND DD.DD_FLAG='PROCESS_TYPE') PROCESS_TYPE,");
                sb.Append("                DECODE(JCAIT.CARGO_TYPE, '1', 'KGS', '2', 'ULD') CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                AOO.PORT_ID,");
                sb.Append("                CASE");
                sb.Append("                  WHEN JCAIT.VOYAGE_FLIGHT_NO IS NULL THEN");
                sb.Append("                   AMT.AIRLINE_ID");
                sb.Append("                  ELSE");
                sb.Append("                   (AMT.AIRLINE_ID || '/' || JCAIT.VOYAGE_FLIGHT_NO)");
                sb.Append("                END AS \"VESSEL_FLIGHT\",");
                sb.Append("                JCAIT.ETD_DATE ETD_DATE,");
                sb.Append("                JCAIT.ETA_DATE ETA_DATE,");
                sb.Append("                SUM(JCONT.PACK_COUNT) PACK_COUNT,");
                sb.Append("                SUM(JCONT.CHARGEABLE_WEIGHT) NET_CHARGE_WEIGHT,");
                sb.Append("                SUM(JCONT.VOLUME_IN_CBM) VOLUME_IN_CBM, ");
                sb.Append("                1 BIZTYPE, ");
                sb.Append("                2 PROCESSTYPE ");
                sb.Append("  FROM JOB_CARD_TRN    JCAIT,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE CMT.CUSTOMER_MST_PK = JCAIT.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND AOO.PORT_MST_PK = JCAIT.PORT_MST_POL_FK");
                sb.Append("   AND AOD.PORT_MST_PK = JCAIT.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AIRLINE_MST_PK(+) = JCAIT.CARRIER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK = JCAIT.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCAIT.JOB_CARD_TRN_PK");
                sb.Append("   AND JCAIT.JOB_CARD_STATUS <> 2");
                sb.Append("   and jcait.business_type=1 AND JCAIT.PROCESS_TYPE=2 ");
                sb.Append("   AND AOD.LOCATION_MST_FK = " + LocFk + "");
                sb.Append("   AND JCAIT.TRANSPORTER_DEPOT_FK IS NULL");
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition3 = strCondition3 + " And TO_DATE(JCAIT.JOBCARD_DATE, 'DD/MM/YYYY') >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition3 = strCondition3 + " And TO_DATE(JCAIT.JOBCARD_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition3 = strCondition3 + " And TO_DATE(JCAIT.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustomerMstPks.Trim()) & CustomerMstPks.Trim() != "0")
                {
                    strCondition3 = strCondition3 + " And JCAIT.CUST_CUSTOMER_MST_FK IN (" + CustomerMstPks + ") ";
                }
                if (!string.IsNullOrEmpty(VslVoyTrnPks.Trim()) & VslVoyTrnPks.Trim() != "0")
                {
                    strCondition3 = strCondition3 + " And AMT.AIRLINE_MST_PK IN (" + VslVoyTrnPks.Trim() + ")";
                }
                if (!string.IsNullOrEmpty(VoyName))
                {
                    strCondition3 = strCondition3 + " And UPPER(JCAIT.VOYAGE_FLIGHT_NO) = '" + VoyName.Trim().ToUpper() + "'";
                }
                sb.Append(strCondition3);
                sb.Append(" GROUP BY ");
                sb.Append("                          JCAIT.JOB_CARD_TRN_PK,");
                sb.Append("                          JCAIT.JOBCARD_REF_NO,");
                sb.Append("                          JCAIT.JOBCARD_DATE, JCAIT.CARGO_TYPE, ");
                sb.Append("                          CMT.CUSTOMER_NAME, ");
                sb.Append("                          AOO.PORT_ID,  ");
                sb.Append("                          JCAIT.VOYAGE_FLIGHT_NO,");
                sb.Append("                          AMT.AIRLINE_ID,");
                sb.Append("                          JCAIT.ETD_DATE,");
                sb.Append("                          JCAIT.ETA_DATE,");
                sb.Append("                          CGMT.COMMODITY_GROUP_CODE ");

                strSQL = " SELECT * FROM (SELECT ROWNUM SLNO, q.* FROM( SELECT * FROM (";
                strSQL += sb.ToString();
                if (Business == 0 & Process == 0)
                {
                    strCondition4 += "  ";
                }
                else if (Business == 0 & Process == 1)
                {
                    strCondition4 += " AND Q.PROCESSTYPE=1 ";
                }
                else if (Business == 0 & Process == 2)
                {
                    strCondition4 += " AND Q.PROCESSTYPE=2 ";
                }
                else if (Business == 1 & Process == 0)
                {
                    strCondition4 += " AND Q.BIZTYPE=1  ";
                }
                else if (Business == 1 & Process == 1)
                {
                    strCondition4 += " AND Q.BIZTYPE=1 AND Q.PROCESSTYPE=1 ";
                }
                else if (Business == 1 & Process == 2)
                {
                    strCondition4 += " AND Q.BIZTYPE=1 AND Q.PROCESSTYPE=2 ";
                }
                else if (Business == 2 & Process == 0)
                {
                    strCondition4 += " AND Q.BIZTYPE=2  ";
                }
                else if (Business == 2 & Process == 1)
                {
                    strCondition4 += " AND Q.BIZTYPE=2 AND Q.PROCESSTYPE=1 ";
                }
                else if (Business == 2 & Process == 2)
                {
                    strCondition4 += " AND Q.BIZTYPE=2 AND Q.PROCESSTYPE=2 ";
                }
                else
                {
                    strCondition4 += " AND Q.BIZTYPE=2 AND Q.PROCESSTYPE=1 ";
                }

                if (expExcel == 0)
                {
                    strSQL1 = " SELECT COUNT(*) FROM (SELECT ROWNUM SLNO, q.* FROM( SELECT * FROM (";
                    strSQL1 += sb.ToString();
                    strSQL1 += " ) ORDER BY to_date(JOBCARD_DATE,dateformat) DESC )q WHERE 1=1" + strCondition4 + " ) ";

                    TotalRecords = (Int32)objWF.GetDataSet(strSQL1).Tables[0].Rows[0][0];
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
                    strSQL += " ) ORDER BY to_date(JOBCARD_DATE,dateformat) DESC )q WHERE 1=1" + strCondition4 + " ) WHERE SLNO Between " + start + " and " + last;
                }
                else
                {
                    strSQL += " ) ORDER BY to_date(JOBCARD_DATE,dateformat) DESC )q WHERE 1=1" + strCondition4 + " ) ";
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

        #endregion "Booking (Cargo) Not Recieved in Warehouse Grid Function"

        #region "Booking (Cargo) Not Recieved in Warehouse Print Function"

        /// <summary>
        /// Fetches the BKG warehouse print.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="Process">The process.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchBkgWarehousePrint(Int32 Business = 0, Int32 Process = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 CargoType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            string strCondition = null;
            string strCondition4 = null;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT BST.BOOKING_MST_PK BOOKING_PK,");
                sb.Append("                BST.BOOKING_REF_NO,");
                sb.Append("                JCSET.JOB_CARD_TRN_PK JOBCARD_PK,");
                sb.Append("                JCSET.JOBCARD_REF_NO,");
                sb.Append("                TO_CHAR(JCSET.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                sb.Append("                TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=2 AND DD.DD_FLAG='BIZ_TYPE') BIZ_TYPE, ");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=1 AND DD.DD_FLAG='PROCESS_TYPE') PROCESS_TYPE,");
                sb.Append("                DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                POD.PORT_ID,");
                sb.Append("                CASE");
                sb.Append("                  WHEN VT.VOYAGE IS NULL THEN");
                sb.Append("                   VVT.VESSEL_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("                END AS \"VESSEL_FLIGHT\",");
                sb.Append("                TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                sb.Append("                TO_CHAR(JCSET.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                SUM(JCONT.PACK_COUNT) PACK_COUNT,");
                sb.Append("                SUM(JCONT.NET_WEIGHT) NET_CHARGE_WEIGHT,");
                sb.Append("                SUM(JCONT.VOLUME_IN_CBM) VOLUME_IN_CBM,");
                sb.Append("                2 BIZTYPE, ");
                sb.Append("                1 PROCESSTYPE ");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("   AND JCSET.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSET.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("   AND BST.STATUS <> 3");
                sb.Append("   AND POL.LOCATION_MST_FK = " + LocFk + "");
                sb.Append("   AND JCSET.TRANSPORTER_CARRIER_FK IS NULL");
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append("   And TO_DATE(JCSET.JOBCARD_DATE, 'DD/MM/YYYY') >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append("   And TO_DATE(JCSET.JOBCARD_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("   And TO_DATE(JCSET.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append("  And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append("  And VVT.VESSEL_NAME = '" + VslName + "'");
                }
                if (CargoType > 0)
                {
                    sb.Append(" And BST.CARGO_TYPE = " + CargoType + "");
                }
                sb.Append(" GROUP BY BST.BOOKING_MST_PK,");
                sb.Append("                          BST.BOOKING_REF_NO,");
                sb.Append("                          JCSET.JOB_CARD_TRN_PK,");
                sb.Append("                          JCSET.JOBCARD_REF_NO,");
                sb.Append("                          JCSET.JOBCARD_DATE,");
                sb.Append("                          BST.SHIPMENT_DATE,");
                sb.Append("                          CMT.CUSTOMER_NAME,");
                sb.Append("                          POD.PORT_ID,");
                sb.Append("                          BST.CARGO_TYPE,");
                sb.Append("                          VT.VOYAGE,");
                sb.Append("                          VVT.VESSEL_NAME,");
                sb.Append("                          JCSET.ETD_DATE,");
                sb.Append("                          JCSET.ETA_DATE,");
                sb.Append("                          CGMT.COMMODITY_GROUP_CODE ");
                sb.Append("                          UNION ");
                sb.Append("SELECT DISTINCT 0 BOOKING_PK,");
                sb.Append("                '' BOOKING_REF_NO,");
                sb.Append("                JCSIT.JOB_CARD_TRN_PK JOBCARD_PK,");
                sb.Append("                JCSIT.JOBCARD_REF_NO,");
                sb.Append("                TO_CHAR(JCSIT.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("                '' SHIPMENT_DATE,");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=2 AND DD.DD_FLAG='BIZ_TYPE') BIZ_TYPE, ");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=2 AND DD.DD_FLAG='PROCESS_TYPE') PROCESS_TYPE,");
                sb.Append("                DECODE(JCSIT.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                POL.PORT_ID,");
                sb.Append("                CASE");
                sb.Append("                  WHEN VT.VOYAGE IS NULL THEN");
                sb.Append("                   VVT.VESSEL_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("                END AS \"VESSEL_FLIGHT\",");
                sb.Append("                TO_CHAR(JCSIT.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                sb.Append("                TO_CHAR(JCSIT.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                SUM(JCONT.PACK_COUNT) PACK_COUNT,");
                sb.Append("                SUM(JCONT.NET_WEIGHT) NET_CHARGE_WEIGHT,");
                sb.Append("                SUM(JCONT.VOLUME_IN_CBM) VOLUME_IN_CBM,");
                sb.Append("                2 BIZTYPE, ");
                sb.Append("                2 PROCESSTYPE ");
                sb.Append("  FROM JOB_CARD_TRN    JCSIT,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append("");
                sb.Append(" WHERE CMT.CUSTOMER_MST_PK = JCSIT.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK = JCSIT.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = JCSIT.PORT_MST_POD_FK");
                sb.Append("   AND JCSIT.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSIT.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCSIT.JOB_CARD_TRN_PK");
                sb.Append("   AND JCSIT.JOB_CARD_STATUS <> 2");
                sb.Append("   AND POD.LOCATION_MST_FK = " + LocFk + "");
                sb.Append("   AND JCSIT.TRANSPORTER_DEPOT_FK IS NULL");
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append("  And TO_DATE(JCSIT.JOBCARD_DATE, 'DD/MM/YYYY') >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append("  And TO_DATE(JCSIT.JOBCARD_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(JCSIT.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append("  And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append("  And VVT.VESSEL_NAME = '" + VslName + "'");
                }
                if (CargoType > 0)
                {
                    sb.Append(" And JCSIT.CARGO_TYPE = " + CargoType + "");
                }
                sb.Append(" GROUP BY ");
                sb.Append("                          JCSIT.JOB_CARD_TRN_PK,");
                sb.Append("                          JCSIT.JOBCARD_REF_NO,");
                sb.Append("                          JCSIT.JOBCARD_DATE,");
                sb.Append("                          CMT.CUSTOMER_NAME,");
                sb.Append("                          POL.PORT_ID,");
                sb.Append("                          JCSIT.CARGO_TYPE,");
                sb.Append("                          VT.VOYAGE,");
                sb.Append("                          VVT.VESSEL_NAME,");
                sb.Append("                          JCSIT.ETD_DATE,");
                sb.Append("                          JCSIT.ETA_DATE,");
                sb.Append("                          CGMT.COMMODITY_GROUP_CODE ");
                sb.Append("                          UNION ");
                sb.Append(" SELECT DISTINCT BAT.BOOKING_MST_PK BOOKING_PK,");
                sb.Append("                BAT.BOOKING_REF_NO,");
                sb.Append("                JCAET.JOB_CARD_TRN_PK JOBCARD_PK,");
                sb.Append("                JCAET.JOBCARD_REF_NO,");
                sb.Append("                TO_CHAR(JCAET.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("                TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=1 AND DD.DD_FLAG='BIZ_TYPE') BIZ_TYPE, ");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=1 AND DD.DD_FLAG='PROCESS_TYPE') PROCESS_TYPE,");
                sb.Append("                '' CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                AOD.PORT_ID,");
                sb.Append("                CASE");
                sb.Append("                  WHEN JCAET.VOYAGE_FLIGHT_NO IS NULL THEN");
                sb.Append("                   AMT.AIRLINE_ID");
                sb.Append("                  ELSE");
                sb.Append("                   (AMT.AIRLINE_ID || '/' || JCAET.VOYAGE_FLIGHT_NO)");
                sb.Append("                END AS \"VESSEL_FLIGHT\",");
                sb.Append("                TO_CHAR(JCAET.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                sb.Append("                TO_CHAR(JCAET.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                SUM(JCONT.PACK_COUNT) PACK_COUNT,");
                sb.Append("                SUM(JCONT.CHARGEABLE_WEIGHT) NET_CHARGE_WEIGHT,");
                sb.Append("                SUM(JCONT.VOLUME_IN_CBM) VOLUME_IN_CBM,");
                sb.Append("                1 BIZTYPE, ");
                sb.Append("                1 PROCESSTYPE ");
                sb.Append("  FROM BOOKING_MST_TBL         BAT,");
                sb.Append("       JOB_CARD_TRN    JCAET,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append("");
                sb.Append(" WHERE BAT.BOOKING_MST_PK = JCAET.BOOKING_MST_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = BAT.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND AOD.PORT_MST_PK = BAT.PORT_MST_POD_FK");
                sb.Append("   AND AOO.PORT_MST_PK = BAT.PORT_MST_POL_FK");
                sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCAET.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCAET.JOB_CARD_TRN_PK");
                sb.Append("   AND BAT.STATUS <> 3");
                sb.Append("   AND AOO.LOCATION_MST_FK = " + LocFk + "");
                sb.Append("   AND JCAET.TRANSPORTER_DEPOT_FK IS NULL");
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append("  And TO_DATE(JCAET.JOBCARD_DATE, 'DD/MM/YYYY') >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append("  And TO_DATE(JCAET.JOBCARD_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(JCAET.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append("  And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append("  And AMT.AIRLINE_NAME = '" + VslName + "'");
                }
                sb.Append(" GROUP BY BAT.BOOKING_MST_PK, BAT.BOOKING_REF_NO,");
                sb.Append("                          JCAET.JOB_CARD_TRN_PK,");
                sb.Append("                          JCAET.JOBCARD_REF_NO,");
                sb.Append("                          JCAET.JOBCARD_DATE,");
                sb.Append("                          CMT.CUSTOMER_NAME, ");
                sb.Append("                          AOD.PORT_ID,  ");
                sb.Append("                          BAT.SHIPMENT_DATE,");
                sb.Append("                          JCAET.VOYAGE_FLIGHT_NO,");
                sb.Append("                          AMT.AIRLINE_ID,");
                sb.Append("                          JCAET.ETD_DATE,");
                sb.Append("                          JCAET.ETA_DATE,");
                sb.Append("                          CGMT.COMMODITY_GROUP_CODE ");
                sb.Append("                          UNION ");
                sb.Append(" SELECT DISTINCT 0 BOOKING_PK,");
                sb.Append("                '' BOOKING_REF_NO,");
                sb.Append("                JCAIT.JOB_CARD_TRN_PK JOBCARD_PK,");
                sb.Append("                JCAIT.JOBCARD_REF_NO,");
                sb.Append("                TO_CHAR(JCAIT.JOBCARD_DATE,DATEFORMAT) JOBCARD_DATE,");
                sb.Append("                '' SHIPMENT_DATE,");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=1 AND DD.DD_FLAG='BIZ_TYPE') BIZ_TYPE, ");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.CONFIG_ID='QFOR4332' ");
                sb.Append("                AND DD.DD_VALUE=2 AND DD.DD_FLAG='PROCESS_TYPE') PROCESS_TYPE, ");
                sb.Append("                '' CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                AOO.PORT_ID,");
                sb.Append("                CASE");
                sb.Append("                  WHEN JCAIT.VOYAGE_FLIGHT_NO IS NULL THEN");
                sb.Append("                   AMT.AIRLINE_ID");
                sb.Append("                  ELSE");
                sb.Append("                   (AMT.AIRLINE_ID || '/' || JCAIT.VOYAGE_FLIGHT_NO)");
                sb.Append("                END AS \"VESSEL_FLIGHT\",");
                sb.Append("                TO_CHAR(JCAIT.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                sb.Append("                TO_CHAR(JCAIT.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                SUM(JCONT.PACK_COUNT) PACK_COUNT,");
                sb.Append("                SUM(JCONT.CHARGEABLE_WEIGHT) NET_CHARGE_WEIGHT,");
                sb.Append("                SUM(JCONT.VOLUME_IN_CBM) VOLUME_IN_CBM, ");
                sb.Append("                1 BIZTYPE, ");
                sb.Append("                2 PROCESSTYPE ");
                sb.Append("  FROM JOB_CARD_TRN    JCAIT,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE CMT.CUSTOMER_MST_PK = JCAIT.CUST_CUSTOMER_MST_FK");
                sb.Append("   AND AOO.PORT_MST_PK = JCAIT.PORT_MST_POL_FK");
                sb.Append("   AND AOD.PORT_MST_PK = JCAIT.PORT_MST_POD_FK");
                sb.Append("   AND AMT.AIRLINE_MST_PK(+) = JCAIT.CARRIER_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCAIT.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCAIT.JOB_CARD_TRN_PK");
                sb.Append("   AND JCAIT.JOB_CARD_STATUS <> 2");
                sb.Append("   AND AOD.LOCATION_MST_FK = " + LocFk + "");
                sb.Append("   AND JCAIT.TRANSPORTER_DEPOT_FK IS NULL");
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append("  And TO_DATE(JCAIT.JOBCARD_DATE, 'DD/MM/YYYY') >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append("  And TO_DATE(JCAIT.JOBCARD_DATE, 'DD/MM/YYYY') <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(JCAIT.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append("  And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append("  And AMT.AIRLINE_NAME = '" + VslName + "'");
                }
                sb.Append(" GROUP BY ");
                sb.Append("                          JCAIT.JOB_CARD_TRN_PK,");
                sb.Append("                          JCAIT.JOBCARD_REF_NO,");
                sb.Append("                          JCAIT.JOBCARD_DATE,");
                sb.Append("                          CMT.CUSTOMER_NAME, ");
                sb.Append("                          AOO.PORT_ID,  ");
                sb.Append("                          JCAIT.VOYAGE_FLIGHT_NO,");
                sb.Append("                          AMT.AIRLINE_ID,");
                sb.Append("                          JCAIT.ETD_DATE,");
                sb.Append("                          JCAIT.ETA_DATE,");
                sb.Append("                          CGMT.COMMODITY_GROUP_CODE ");

                strSQL = " SELECT * FROM (SELECT ROWNUM SLNO, q.* FROM( SELECT * FROM (";
                strSQL += sb.ToString();
                if (Business == 0 & Process == 0)
                {
                    strCondition4 += "  ";
                }
                else if (Business == 0 & Process == 1)
                {
                    strCondition4 += " AND Q.PROCESSTYPE=1 ";
                }
                else if (Business == 0 & Process == 2)
                {
                    strCondition4 += " AND Q.PROCESSTYPE=2 ";
                }
                else if (Business == 1 & Process == 0)
                {
                    strCondition4 += " AND Q.BIZTYPE=1  ";
                }
                else if (Business == 1 & Process == 1)
                {
                    strCondition4 += " AND Q.BIZTYPE=1 AND Q.PROCESSTYPE=1 ";
                }
                else if (Business == 1 & Process == 2)
                {
                    strCondition4 += " AND Q.BIZTYPE=1 AND Q.PROCESSTYPE=2 ";
                }
                else if (Business == 2 & Process == 0)
                {
                    strCondition4 += " AND Q.BIZTYPE=2  ";
                }
                else if (Business == 2 & Process == 1)
                {
                    strCondition4 += " AND Q.BIZTYPE=2 AND Q.PROCESSTYPE=1 ";
                }
                else if (Business == 2 & Process == 2)
                {
                    strCondition4 += " AND Q.BIZTYPE=2 AND Q.PROCESSTYPE=2 ";
                }
                else
                {
                    strCondition4 += " AND Q.BIZTYPE=2 AND Q.PROCESSTYPE=1 ";
                }

                strSQL += " ) ORDER BY to_date(JOBCARD_DATE,dateformat) DESC )q WHERE 1=1" + strCondition4 + " ) ";

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

        #endregion "Booking (Cargo) Not Recieved in Warehouse Print Function"

        #region "MJC For B/L or MAWB Allocation Grid Function"

        /// <summary>
        /// Fetches the mj cfor bl allocation.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="expExcel">The exp excel.</param>
        /// <returns></returns>
        public DataSet FetchMJCforBLAllocation(Int32 Business = 0, Int32 CargoType = 0, Int32 LocFk = 0, string CustName = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0,
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
                sb.Append("SELECT DISTINCT MJSET.MASTER_JC_SEA_EXP_PK MASTER_JC_EXP_PK,");
                sb.Append("                MJSET.MASTER_JC_REF_NO,");
                sb.Append("                MJSET.MASTER_JC_DATE MASTER_JC_DATE,");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 2) BIZ_TYPE,");
                sb.Append("                OMT.OPERATOR_NAME OPERATOR_AIRLINE_NAME,");
                sb.Append("                POD.PORT_ID,");
                sb.Append("                MJSET.POL_ETD ETD,");
                sb.Append("                AMT.AGENT_NAME,");
                sb.Append("                DECODE(MJSET.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                (SELECT SUM(JCONT.NET_WEIGHT)");
                sb.Append("                   FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("                  WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("                    AND JC.MASTER_JC_FK = MJSET.MASTER_JC_SEA_EXP_PK");
                sb.Append("                  ) AS NET_CHARGE_WEIGHT,");
                sb.Append("                (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("                   FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("                  WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("                    AND JC.MASTER_JC_FK = MJSET.MASTER_JC_SEA_EXP_PK");
                sb.Append("                  ) AS GROSS_WEIGHT,");
                sb.Append("                (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("                   FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("                  WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("                    AND JC.MASTER_JC_FK = MJSET.MASTER_JC_SEA_EXP_PK");
                sb.Append("                  ) AS VOLUME_IN_CBM, ");
                sb.Append("                  2 BIZ_TYPE_VALUE ");
                sb.Append("  FROM MASTER_JC_SEA_EXP_TBL   MJSET,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       OPERATOR_MST_TBL        OMT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT");
                sb.Append(" WHERE OMT.OPERATOR_MST_PK = MJSET.OPERATOR_MST_FK");
                sb.Append("   AND POD.PORT_MST_PK = MJSET.PORT_MST_POD_FK");
                sb.Append("   AND POL.PORT_MST_PK = MJSET.PORT_MST_POL_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = MJSET.DP_AGENT_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = MJSET.COMMODITY_GROUP_FK");
                sb.Append("   AND MJSET.MASTER_JC_SEA_EXP_PK = JCSET.MASTER_JC_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("   AND JCSET.MBL_MAWB_FK IS NULL");
                sb.Append("   AND POL.LOCATION_MST_FK =  " + LocFk + "");

                strCondition = "";
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And TO_DATE(MJSET.MASTER_JC_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And TO_DATE(MJSET.MASTER_JC_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(MJSET.POL_ETD, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (Business == 1)
                {
                    strCondition = strCondition + " And 1 = 2 ";
                }
                if (CargoType != 0)
                {
                    strCondition = strCondition + " And MJSET.CARGO_TYPE = " + CargoType;
                }
                sb.Append(strCondition);

                sb.Append(" UNION SELECT DISTINCT MJAET.MASTER_JC_AIR_EXP_PK MASTER_JC_EXP_PK,");
                sb.Append("       MJAET.MASTER_JC_REF_NO,");
                sb.Append("       MJAET.MASTER_JC_DATE MASTER_JC_DATE,");
                sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 1) BIZ_TYPE,");
                sb.Append("       AMT.AIRLINE_NAME OPERATOR_AIRLINE_NAME,");
                sb.Append("       AOD.PORT_ID,");
                sb.Append("       MJAET.AOO_ETD ETD,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       '' CARGO_TYPE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = MJAET.MASTER_JC_AIR_EXP_PK");
                sb.Append("         ) AS NET_CHARGE_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = MJAET.MASTER_JC_AIR_EXP_PK");
                sb.Append("         ) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = MJAET.MASTER_JC_AIR_EXP_PK");
                sb.Append("         ) AS VOLUME_IN_CBM,");
                sb.Append("        1 BIZ_TYPE_VALUE ");
                sb.Append("  FROM MASTER_JC_AIR_EXP_TBL   MJAET,");
                sb.Append("       JOB_CARD_TRN    JCAET,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT");
                sb.Append(" WHERE AMT.AIRLINE_MST_PK = MJAET.AIRLINE_MST_FK ");
                sb.Append("   AND AOD.PORT_MST_PK = MJAET.PORT_MST_POD_FK");
                sb.Append("   AND AOO.PORT_MST_PK = MJAET.PORT_MST_POL_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = MJAET.DP_AGENT_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = MJAET.COMMODITY_GROUP_FK");
                sb.Append("   AND MJAET.MASTER_JC_AIR_EXP_PK = JCAET.MASTER_JC_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = JCAET.SHIPPER_CUST_MST_FK");
                sb.Append("   AND AOO.LOCATION_MST_FK = " + LocFk + "");
                sb.Append("   AND JCAET.MBL_MAWB_FK IS NULL");

                strCondition = "";
                if (!string.IsNullOrEmpty(FromDt))
                {
                    strCondition = strCondition + " And TO_DATE(MJAET.MASTER_JC_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    strCondition = strCondition + " And TO_DATE(MJAET.MASTER_JC_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    strCondition = strCondition + " And TO_DATE(MJAET.AOO_ETD, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (Business == 2)
                {
                    strCondition = strCondition + " And 1 = 2 ";
                }
                sb.Append(strCondition);

                strSQL = " SELECT COUNT(QRY.MASTER_JC_EXP_PK) FROM (";
                strSQL += sb.ToString() + ")QRY";

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

                sb.Append(" ORDER BY MASTER_JC_DATE DESC, MASTER_JC_REF_NO DESC ");
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

        #endregion "MJC For B/L or MAWB Allocation Grid Function"

        #region "MJC For B/L or MAWB Allocation Print Function"

        /// <summary>
        /// Fetches the mj cfor bl allocation print.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchMJCforBLAllocationPrint(Int32 Business = 0, Int32 CargoType = 0, string CustName = "", Int32 LocFk = 0, string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0)
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
                sb.Append("SELECT DISTINCT MJSET.MASTER_JC_SEA_EXP_PK MASTER_JC_EXP_PK,");
                sb.Append("                MJSET.MASTER_JC_REF_NO,");
                sb.Append("                TO_DATE(MJSET.MASTER_JC_DATE, 'DD/MM/YYYY') MASTER_JC_DATE,");
                sb.Append("                (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 2) BIZ_TYPE,");
                sb.Append("                OMT.OPERATOR_NAME OPERATOR_AIRLINE_NAME,");
                sb.Append("                POD.PORT_ID,");
                sb.Append("                MJSET.POL_ETD ETD,");
                sb.Append("                AMT.AGENT_NAME,");
                sb.Append("                DECODE(MJSET.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("                (SELECT SUM(JCONT.NET_WEIGHT)");
                sb.Append("                   FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("                  WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("                    AND JC.MASTER_JC_FK = MJSET.MASTER_JC_SEA_EXP_PK");
                sb.Append("                  GROUP BY MJSET.MASTER_JC_SEA_EXP_PK) AS NET_CHARGE_WEIGHT,");
                sb.Append("                (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("                   FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("                  WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("                    AND JC.MASTER_JC_FK = MJSET.MASTER_JC_SEA_EXP_PK");
                sb.Append("                  GROUP BY MJSET.MASTER_JC_SEA_EXP_PK) AS GROSS_WEIGHT,");
                sb.Append("                (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("                   FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("                  WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("                    AND JC.MASTER_JC_FK = MJSET.MASTER_JC_SEA_EXP_PK");
                sb.Append("                  GROUP BY MJSET.MASTER_JC_SEA_EXP_PK) AS VOLUME_IN_CBM");
                sb.Append("  FROM MASTER_JC_SEA_EXP_TBL   MJSET,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       OPERATOR_MST_TBL        OMT,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT");
                sb.Append(" WHERE OMT.OPERATOR_MST_PK = MJSET.OPERATOR_MST_FK");
                sb.Append("   AND POD.PORT_MST_PK = MJSET.PORT_MST_POD_FK");
                sb.Append("   AND POL.PORT_MST_PK = MJSET.PORT_MST_POL_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = MJSET.DP_AGENT_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = MJSET.COMMODITY_GROUP_FK");
                sb.Append("   AND MJSET.MASTER_JC_SEA_EXP_PK = JCSET.MASTER_JC_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("   AND JCSET.MBL_MAWB_FK IS NULL");
                sb.Append("   AND POL.LOCATION_MST_FK =  " + LocFk + "");
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE(MJSET.MASTER_JC_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE(MJSET.MASTER_JC_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(MJSET.POL_ETD, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (Business == 1)
                {
                    sb.Append(" And 1 = 2 ");
                }
                if (CargoType != 0)
                {
                    sb.Append(" And MJSET.CARGO_TYPE = " + CargoType);
                }

                sb.Append(" UNION SELECT DISTINCT MJAET.MASTER_JC_AIR_EXP_PK MASTER_JC_EXP_PK,");
                sb.Append("       MJAET.MASTER_JC_REF_NO,");
                sb.Append("       TO_DATE(MJAET.MASTER_JC_DATE, 'DD/MM/YYYY') MASTER_JC_DATE,");
                sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 1) BIZ_TYPE,");
                sb.Append("       AMT.AIRLINE_NAME OPERATOR_AIRLINE_NAME,");
                sb.Append("       AOD.PORT_ID,");
                sb.Append("       MJAET.AOO_ETD ETD,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       '' CARGO_TYPE,");
                sb.Append("       CGMT.COMMODITY_GROUP_CODE,");
                sb.Append("       (SELECT SUM(JCONT.CHARGEABLE_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = MJAET.MASTER_JC_AIR_EXP_PK");
                sb.Append("         ) AS NET_CHARGE_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.GROSS_WEIGHT)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = MJAET.MASTER_JC_AIR_EXP_PK");
                sb.Append("         ) AS GROSS_WEIGHT,");
                sb.Append("       (SELECT SUM(JCONT.VOLUME_IN_CBM)");
                sb.Append("          FROM JOB_TRN_CONT JCONT, JOB_CARD_TRN JC");
                sb.Append("         WHERE JCONT.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK");
                sb.Append("           AND JC.MASTER_JC_FK = MJAET.MASTER_JC_AIR_EXP_PK");
                sb.Append("         ) AS VOLUME_IN_CBM");
                sb.Append("  FROM MASTER_JC_AIR_EXP_TBL   MJAET,");
                sb.Append("       JOB_CARD_TRN    JCAET,");
                sb.Append("       AIRLINE_MST_TBL         AMT,");
                sb.Append("       PORT_MST_TBL            AOO,");
                sb.Append("       PORT_MST_TBL            AOD,");
                sb.Append("       AGENT_MST_TBL           AMT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT");
                sb.Append(" WHERE AMT.AIRLINE_MST_PK = MJAET.AIRLINE_MST_FK");
                sb.Append("   AND AOD.PORT_MST_PK = MJAET.PORT_MST_POD_FK");
                sb.Append("   AND AOO.PORT_MST_PK = MJAET.PORT_MST_POL_FK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = MJAET.DP_AGENT_MST_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = MJAET.COMMODITY_GROUP_FK");
                sb.Append("   AND MJAET.MASTER_JC_AIR_EXP_PK = JCAET.MASTER_JC_FK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = JCAET.SHIPPER_CUST_MST_FK");
                sb.Append("   AND AOO.LOCATION_MST_FK = " + LocFk + "");
                sb.Append("   AND JCAET.MBL_MAWB_FK IS NULL");
                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append(" And TO_DATE(MJAET.MASTER_JC_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append(" And TO_DATE(MJAET.MASTER_JC_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append(" And TO_DATE(MJAET.POL_ETD, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append(" And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (Business == 2)
                {
                    sb.Append(" And 1 = 2 ");
                }
                sb.Append(" ORDER BY MASTER_JC_DATE DESC, MASTER_JC_REF_NO DESC ");

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

        #endregion "MJC For B/L or MAWB Allocation Print Function"

        #region "Booking Pending for Container Survey Function"

        /// <summary>
        /// Fetches the survey details.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Excel">The excel.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchSurveyDetails(Int32 BookingPk = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string Voyage = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 Excel = 0,
        Int32 CurrentPage = 0, Int32 TotalPage = 0)
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
                    strCondition = strCondition + " And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)";
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    strCondition = strCondition + " And JCSET.VESSEL_NAME = '" + VslName + "'";
                }
                if (!string.IsNullOrEmpty(Voyage))
                {
                    strCondition = strCondition + " And JCSET.VOYAGE_FLIGHT_NO = '" + Voyage + "'";
                }

                sb.Append("SELECT DISTINCT BST.BOOKING_MST_PK,");
                sb.Append("                BST.BOOKING_REF_NO,");
                sb.Append("                JCSET.JOB_CARD_TRN_PK,");
                sb.Append("                JCSET.JOBCARD_REF_NO,");
                sb.Append("                JCSET.JOBCARD_DATE,");
                sb.Append("                TO_CHAR(BST.SHIPMENT_DATE,DATEFORMAT) SHIPMENT_DATE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                POD.PORT_ID,");
                sb.Append("                ");
                sb.Append("                CASE");
                sb.Append("                  WHEN VVT.VESSEL_NAME IS NULL THEN");
                sb.Append("                   VVT.VESSEL_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("                END AS \"VESSEL_VOYAGE\",");
                sb.Append("                TO_CHAR(JCSET.ETD_DATE,DATETIMEFORMAT24) ETD_DATE,");
                sb.Append("                TO_CHAR(JCSET.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
                sb.Append("                DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CTMT.CONTAINER_TYPE_MST_ID,");
                sb.Append("                JCONT.CONTAINER_NUMBER,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK ");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("   AND JCSET.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSET.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("   AND JCSET.SURVEY_REF_NR IS  NULL");
                sb.Append("   AND JCSET.SURVEY_DATE IS  NULL");
                sb.Append("   AND JCSET.SURVEYOR_FK IS  NULL");
                sb.Append("   AND JCSET.SURVEY_COMPLETED = 0");
                sb.Append("   AND BST.CARGO_TYPE in (1,2)");
                sb.Append("   AND BST.STATUS <> 3");
                sb.Append("   AND JCONT.CONTAINER_NUMBER IS NOT NULL");
                sb.Append("  AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");
                sb.Append("");
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
                sb.Append(" ORDER BY BST.BOOKING_MST_PK DESC");

                strSQL = " SELECT * FROM (SELECT ROWNUM SLNO, q.* FROM(";
                strSQL += sb.ToString();

                if (Excel == 0)
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

        #endregion "Booking Pending for Container Survey Function"



        #region "Container Survey Print"

        /// <summary>
        /// Fetches the container survey report.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <returns></returns>
        public DataSet FetchContainerSurveyReport(Int32 BookingPk = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string FromDt = "", string ToDt = "", string ETDDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT DISTINCT BST.BOOKING_MST_PK,");
                sb.Append("                BST.BOOKING_REF_NO,");
                sb.Append("                JCSET.JOB_CARD_TRN_PK,");
                sb.Append("                JCSET.JOBCARD_REF_NO,");
                sb.Append("                JCSET.JOBCARD_DATE,");
                sb.Append("                TO_CHAR(BST.SHIPMENT_DATE,DATEFORMAT) SHIPMENT_DATE,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                POD.PORT_ID,");
                sb.Append("                ");
                sb.Append("                CASE");
                sb.Append("                  WHEN VVT.VESSEL_NAME IS NULL THEN");
                sb.Append("                   VVT.VESSEL_NAME");
                sb.Append("                  ELSE");
                sb.Append("                   (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                sb.Append("                END AS \"VESSEL_VOYAGE\",");
                sb.Append("                JCSET.ETD_DATE,");
                sb.Append("                JCSET.ETA_DATE,");
                sb.Append("                DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                sb.Append("                CTMT.CONTAINER_TYPE_MST_ID,");
                sb.Append("                JCONT.CONTAINER_NUMBER,");
                sb.Append("                CGMT.COMMODITY_GROUP_CODE");
                sb.Append("  FROM BOOKING_MST_TBL         BST,");
                sb.Append("       JOB_CARD_TRN    JCSET,");
                sb.Append("       JOB_TRN_CONT    JCONT,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
                sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                sb.Append(" WHERE BST.BOOKING_MST_PK = JCSET.BOOKING_MST_FK ");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = JCSET.SHIPPER_CUST_MST_FK");
                sb.Append("   AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                sb.Append("   AND JCSET.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSET.COMMODITY_GROUP_FK");
                sb.Append("   AND JCONT.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND JCONT.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK");
                sb.Append("   AND JCSET.SURVEY_REF_NR IS  NULL");
                sb.Append("   AND JCSET.SURVEY_DATE IS  NULL");
                sb.Append("   AND JCSET.SURVEYOR_FK IS  NULL");
                sb.Append("   AND JCSET.SURVEY_COMPLETED = 0");
                sb.Append("   AND BST.CARGO_TYPE in (1,2)");
                sb.Append("   AND JCONT.CONTAINER_NUMBER IS NOT NULL");
                sb.Append("  AND POL.LOCATION_MST_FK = ");
                sb.Append(" (SELECT L.LOCATION_MST_PK");
                sb.Append("  FROM LOCATION_MST_TBL L");
                sb.Append("   WHERE L.LOCATION_MST_PK = " + LocFk + ")");

                if (!string.IsNullOrEmpty(FromDt))
                {
                    sb.Append("  And JCSET.JOBCARD_DATE >= TO_DATE('" + FromDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ToDt))
                {
                    sb.Append("  And JCSET.JOBCARD_DATE <= TO_DATE('" + ToDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(ETDDt))
                {
                    sb.Append("  And TO_DATE(JCSET.ETD_DATE) = TO_DATE('" + ETDDt + "',dateformat)");
                }
                if (!string.IsNullOrEmpty(CustName))
                {
                    sb.Append("  And CMT.CUSTOMER_NAME = '" + CustName + "'");
                }
                if (!string.IsNullOrEmpty(VslName))
                {
                    sb.Append("  And BST.VESSEL_NAME = '" + VslName + "'");
                }
                sb.Append(" ORDER BY JCSET.JOBCARD_DATE DESC, JCSET.JOBCARD_REF_NO DESC");
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

        #endregion "Container Survey Print"

        #region "JobCard Pending For HBL Allocation Grid Function"

        /// <summary>
        /// Fetches the j cfor HBL.
        /// </summary>
        /// <param name="Business">The business.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustName">Name of the customer.</param>
        /// <param name="VslName">Name of the VSL.</param>
        /// <param name="VoyNr">The voy nr.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="expExcel">The exp excel.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <returns></returns>
        public DataSet FetchJCforHBL(Int32 Business = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string VoyNr = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 flag = 0, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 expExcel = 0, Int32 CargoType = 0, Int32 CommGrp = 0)
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
                    sb.Append("       JC.JOB_CARD_TRN_PK JOBCARD_PK,");
                    sb.Append("       JC.JOBCARD_REF_NO,");
                    sb.Append("       TO_DATE(JC.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                    sb.Append("       TO_DATE(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                    sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 2) BIZ_TYPE,");
                    sb.Append("       DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       POL.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN VT.VOYAGE IS NULL THEN");
                    sb.Append("          VVT.VESSEL_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                    sb.Append("       END VESSEL_FLIGHT,");
                    sb.Append("       JC.ETD_DATE ETD_DATE,");
                    sb.Append("       JC.ETA_DATE ETA_DATE,");
                    sb.Append("       CGMT.COMMODITY_GROUP_CODE");
                    sb.Append("  FROM JOB_CARD_TRN    JC,");
                    sb.Append("       BOOKING_MST_TBL         BST,");
                    sb.Append("       CUSTOMER_MST_TBL        CMT,");
                    sb.Append("       PORT_MST_TBL            POL,");
                    sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                    sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                    sb.Append(" WHERE BST.BOOKING_MST_PK = JC.BOOKING_MST_FK");
                    sb.Append("   AND CMT.CUSTOMER_MST_PK = JC.SHIPPER_CUST_MST_FK");
                    sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                    sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                    sb.Append("   AND VT.VOYAGE_TRN_PK(+) = JC.VOYAGE_TRN_FK");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JC.COMMODITY_GROUP_FK");
                    sb.Append("   AND JC.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND BST.STATUS <> 3");
                    sb.Append("   AND POL.LOCATION_MST_FK =  " + LocFk + "");
                    sb.Append("   AND JC.PROCESS_TYPE =1");
                    sb.Append("   AND JC.BUSINESS_TYPE = " + Business);
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strCondition = strCondition + " And TO_DATE(JC.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strCondition = strCondition + " And TO_DATE(JC.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(ETDDt))
                    {
                        strCondition = strCondition + " And TO_DATE(JC.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(CustName))
                    {
                        strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                    }
                    if (CargoType != 0)
                    {
                        strCondition = strCondition + " And BST.CARGO_TYPE = " + CargoType + "";
                    }
                    if (CommGrp != 0)
                    {
                        strCondition = strCondition + " And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "";
                    }
                    sb.Append(strCondition);
                    if (!string.IsNullOrEmpty(VslName))
                    {
                        sb.Append(" And VVT.VESSEL_NAME = '" + VslName + "'");
                        if (!string.IsNullOrEmpty(VoyNr))
                        {
                            sb.Append(" And VT.VOYAGE = '" + VoyNr + "'");
                        }
                        else
                        {
                            sb.Append(" And VT.VOYAGE is null");
                        }
                    }
                    sb.Append(" ORDER BY NVL(JC.ETD_DATE,'01/01/0001') DESC");
                }
                else if (Business == 1)
                {
                    sb.Append("SELECT BAT.BOOKING_MST_PK BOOKING_PK,");
                    sb.Append("       BAT.BOOKING_REF_NO,");
                    sb.Append("       JCAET.JOB_CARD_TRN_PK JOBCARD_PK,");
                    sb.Append("       JCAET.JOBCARD_REF_NO,");
                    sb.Append("       TO_DATE(JCAET.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                    sb.Append("       TO_DATE(BAT.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                    sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 1) BIZ_TYPE,");
                    sb.Append("       DECODE(JCAET.CARGO_TYPE, '1', 'KGS', '2', 'ULD') CARGO_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       AOO.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN JCAET.VOYAGE_FLIGHT_NO IS NULL THEN");
                    sb.Append("          AMT.AIRLINE_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          (AMT.AIRLINE_NAME || '/' || JCAET.VOYAGE_FLIGHT_NO)");
                    sb.Append("       END VESSEL_FLIGHT,");
                    sb.Append("       JCAET.ETD_DATE ETD_DATE,");
                    sb.Append("       JCAET.ETA_DATE ETA_DATE,");
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
                    sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCAET.COMMODITY_GROUP_FK");
                    sb.Append("   AND JCAET.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND BAT.STATUS <> 3");
                    sb.Append("   AND AOO.LOCATION_MST_FK = " + LocFk + "");
                    sb.Append("   AND JCAET.PROCESS_TYPE =1");
                    sb.Append("   AND JCAET.BUSINESS_TYPE = " + Business);
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strCondition = strCondition + " And TO_DATE(JCAET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strCondition = strCondition + " And TO_DATE(JCAET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(ETDDt))
                    {
                        strCondition = strCondition + " And TO_DATE(JCAET.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(CustName))
                    {
                        strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                    }
                    if (CommGrp != 0)
                    {
                        strCondition = strCondition + " And CGMT.COMMODITY_GROUP_PK = '" + CommGrp + "'";
                    }
                    sb.Append(strCondition);
                    if (!string.IsNullOrEmpty(VslName))
                    {
                        sb.Append(" And AMT.AIRLINE_NAME = '" + VslName + "'");
                        if (!string.IsNullOrEmpty(VoyNr))
                        {
                            sb.Append(" And JCAET.VOYAGE_FLIGHT_NO = '" + VoyNr + "'");
                        }
                        else
                        {
                            sb.Append(" And JCAET.VOYAGE_FLIGHT_NO is null");
                        }
                    }
                    sb.Append(" ORDER BY NVL(JCAET.ETD_DATE,'01/01/0001') DESC");
                }
                else
                {
                    strCondition = "";
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strCondition = strCondition + " And TO_DATE(JC.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strCondition = strCondition + " And TO_DATE(JC.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(ETDDt))
                    {
                        strCondition = strCondition + " And TO_DATE(JC.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)";
                    }
                    if (!string.IsNullOrEmpty(CustName))
                    {
                        strCondition = strCondition + " And CMT.CUSTOMER_NAME = '" + CustName + "'";
                    }
                    if (CommGrp != 0)
                    {
                        strCondition = strCondition + " And CGMT.COMMODITY_GROUP_PK = '" + CommGrp + "'";
                    }
                    sb.Append("   SELECT * FROM (SELECT BAT.BOOKING_MST_PK BOOKING_PK,");
                    sb.Append("       BAT.BOOKING_REF_NO,");
                    sb.Append("       JC.JOB_CARD_TRN_PK JOBCARD_PK,");
                    sb.Append("       JC.JOBCARD_REF_NO,");
                    sb.Append("       TO_DATE(JC.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                    sb.Append("       TO_DATE(BAT.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                    sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 1) BIZ_TYPE,");
                    sb.Append("       DECODE(BAT.CARGO_TYPE, '1', 'KGS', '2', 'ULD') CARGO_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       AOO.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN JC.VOYAGE_FLIGHT_NO IS NULL THEN");
                    sb.Append("          AMT.AIRLINE_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          (AMT.AIRLINE_NAME || '/' || JC.VOYAGE_FLIGHT_NO)");
                    sb.Append("       END VESSEL_FLIGHT,");
                    sb.Append("       JC.ETD_DATE ETD_DATE,");
                    sb.Append("       JC.ETA_DATE ETA_DATE,");
                    sb.Append("       CGMT.COMMODITY_GROUP_CODE");
                    sb.Append("  FROM JOB_CARD_TRN    JC,");
                    sb.Append("       BOOKING_MST_TBL         BAT,");
                    sb.Append("       CUSTOMER_MST_TBL        CMT,");
                    sb.Append("       PORT_MST_TBL            AOO,");
                    sb.Append("       AIRLINE_MST_TBL         AMT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                    sb.Append(" WHERE BAT.BOOKING_MST_PK = JC.BOOKING_MST_FK");
                    sb.Append("   AND CMT.CUSTOMER_MST_PK = JC.SHIPPER_CUST_MST_FK");
                    sb.Append("   AND AOO.PORT_MST_PK = BAT.PORT_MST_POL_FK");
                    sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JC.COMMODITY_GROUP_FK");
                    sb.Append("   AND JC.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND JC.PROCESS_TYPE =1");
                    //If Business > 0 Then
                    //    sb.Append("   AND JC.BUSINESS_TYPE = " & Business)
                    //End If
                    sb.Append("   AND JC.BUSINESS_TYPE = 1");
                    sb.Append(strCondition);
                    sb.Append("   AND AOO.LOCATION_MST_FK = " + LocFk + "");
                    if (!string.IsNullOrEmpty(VslName))
                    {
                        sb.Append(" And AMT.AIRLINE_NAME = '" + VslName + "'");
                        if (!string.IsNullOrEmpty(VoyNr))
                        {
                            sb.Append(" And JC.VOYAGE_FLIGHT_NO = '" + VoyNr + "'");
                        }
                        else
                        {
                            sb.Append(" And JC.VOYAGE_FLIGHT_NO is null");
                        }
                    }
                    sb.Append("   AND BAT.STATUS <> 3");
                    sb.Append(" UNION ");
                    sb.Append("SELECT BST.BOOKING_MST_PK BOOKING_PK,");
                    sb.Append("       BST.BOOKING_REF_NO,");
                    sb.Append("       JC.JOB_CARD_TRN_PK JOBCARD_PK,");
                    sb.Append("       JC.JOBCARD_REF_NO,");
                    sb.Append("       TO_DATE(JC.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                    sb.Append("       TO_DATE(BST.SHIPMENT_DATE, DATEFORMAT) SHIPMENT_DATE,");
                    sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 2) BIZ_TYPE,");
                    sb.Append("       DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       POL.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN VT.VOYAGE IS NULL THEN");
                    sb.Append("          VVT.VESSEL_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                    sb.Append("       END VESSEL_FLIGHT,");
                    sb.Append("       JC.ETD_DATE ETD_DATE,");
                    sb.Append("       JC.ETA_DATE ETA_DATE,");
                    sb.Append("       CGMT.COMMODITY_GROUP_CODE");
                    sb.Append("  FROM JOB_CARD_TRN    JC,");
                    sb.Append("       BOOKING_MST_TBL         BST,");
                    sb.Append("       CUSTOMER_MST_TBL        CMT,");
                    sb.Append("       PORT_MST_TBL            POL,");
                    sb.Append("       VESSEL_VOYAGE_TBL       VVT,");
                    sb.Append("       VESSEL_VOYAGE_TRN       VT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CGMT");
                    sb.Append(" WHERE BST.BOOKING_MST_PK = JC.BOOKING_MST_FK");
                    sb.Append("   AND CMT.CUSTOMER_MST_PK = JC.SHIPPER_CUST_MST_FK");
                    sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                    sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
                    sb.Append("   AND VT.VOYAGE_TRN_PK(+) = JC.VOYAGE_TRN_FK");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JC.COMMODITY_GROUP_FK");
                    sb.Append("   AND JC.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND JC.PROCESS_TYPE =1");
                    sb.Append("   AND JC.BUSINESS_TYPE = 2");
                    sb.Append("   AND BST.STATUS <> 3");
                    sb.Append(strCondition);
                    if (!string.IsNullOrEmpty(VslName))
                    {
                        sb.Append(" And VVT.VESSEL_NAME = '" + VslName + "'");
                        if (!string.IsNullOrEmpty(VoyNr))
                        {
                            sb.Append(" And VT.VOYAGE = '" + VoyNr + "'");
                        }
                        else
                        {
                            sb.Append(" And VT.VOYAGE is null");
                        }
                    }
                    sb.Append("   AND POL.LOCATION_MST_FK =  " + LocFk + "");
                    sb.Append(") order by NVL(ETD_DATE,'01/01/0001') DESC");
                }

                strSQL = " select count(QRY.BOOKING_PK) from (";
                strSQL += sb.ToString() + ")QRY";

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
        /// <param name="VoyNr">The voy nr.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="CommGrp">The comm GRP.</param>
        /// <returns></returns>
        public DataSet FetchJobCardPendingForHBLPrint(Int32 Business = 0, Int32 LocFk = 0, string CustName = "", string VslName = "", string VoyNr = "", string FromDt = "", string ToDt = "", string ETDDt = "", Int32 CargoType = 0, Int32 CommGrp = 0)
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
                    sb.Append("       TO_DATE(BST.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                    sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 2) BIZ_TYPE,");
                    sb.Append("       DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       POL.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN VT.VOYAGE IS NULL THEN");
                    sb.Append("          VVT.VESSEL_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                    sb.Append("       END VESSEL_FLIGHT,");
                    sb.Append("       TO_CHAR(JCSET.ETD_DATE, DATETIMEFORMAT24) ETD_DATE,");
                    sb.Append("       TO_CHAR(JCSET.ETA_DATE, DATETIMEFORMAT24) ETA_DATE,");
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
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSET.COMMODITY_GROUP_FK");
                    sb.Append("   AND JCSET.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND BST.STATUS <> 3  ");
                    sb.Append("   AND POL.LOCATION_MST_FK =  " + LocFk + "");
                    sb.Append("   AND JCSET.PROCESS_TYPE =1");
                    sb.Append("   AND JCSET.BUSINESS_TYPE = " + Business);
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        sb.Append("   And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        sb.Append("   And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(ETDDt))
                    {
                        sb.Append("   And TO_DATE(JCSET.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(CustName))
                    {
                        sb.Append("   And CMT.CUSTOMER_NAME = '" + CustName + "'");
                    }
                    if (CargoType != 0)
                    {
                        strCondition = strCondition + " And BST.CARGO_TYPE = " + CargoType + "";
                    }
                    if (CommGrp != 0)
                    {
                        strCondition = strCondition + " And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "";
                    }
                    if (!string.IsNullOrEmpty(VslName))
                    {
                        sb.Append(" And VVT.VESSEL_NAME = '" + VslName + "'");
                        if (!string.IsNullOrEmpty(VoyNr))
                        {
                            sb.Append(" And VT.VOYAGE = '" + VoyNr + "'");
                        }
                    }
                    sb.Append(" order by NVL(JCSET.ETD_DATE,'01/01/0001') desc ");
                }
                else if (Business == 1)
                {
                    sb.Append("SELECT BAT.BOOKING_MST_PK BOOKING_PK,");
                    sb.Append("       BAT.BOOKING_REF_NO,");
                    sb.Append("       JCAET.JOB_CARD_TRN_PK JOBCARD_PK,");
                    sb.Append("       JCAET.JOBCARD_REF_NO,");
                    sb.Append("       JCAET.JOBCARD_DATE JOBCARD_DATE,");
                    sb.Append("       BAT.SHIPMENT_DATE SHIPMENT_DATE,");
                    sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 1) BIZ_TYPE,");
                    sb.Append("       DECODE(BAT.CARGO_TYPE, '1', 'KGS', '2', 'ULD') CARGO_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       AOO.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN JCAET.VOYAGE_FLIGHT_NO IS NULL THEN");
                    sb.Append("          AMT.AIRLINE_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          (AMT.AIRLINE_NAME || '/' || JCAET.VOYAGE_FLIGHT_NO)");
                    sb.Append("       END VESSEL_FLIGHT,");
                    sb.Append("       JCAET.ETD_DATE ETD_DATE,");
                    sb.Append("       JCAET.ETA_DATE ETA_DATE,");
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
                    sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCAET.COMMODITY_GROUP_FK");
                    sb.Append("   AND JCAET.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND AOO.LOCATION_MST_FK = " + LocFk + "");
                    sb.Append("   AND BAT.STATUS <> 3  ");
                    sb.Append("   AND JCAET.PROCESS_TYPE =1");
                    sb.Append("   AND JCAET.BUSINESS_TYPE = " + Business);
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        sb.Append("   And TO_DATE(JCAET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        sb.Append("   And TO_DATE(JCAET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(ETDDt))
                    {
                        sb.Append("   And TO_DATE(JCAET.ETD_DATE, 'DD/MM/YYYY') = TO_DATE('" + ETDDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(CustName))
                    {
                        sb.Append("   And CMT.CUSTOMER_NAME = '" + CustName + "'");
                    }
                    if (CommGrp != 0)
                    {
                        strCondition = strCondition + " And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "";
                    }

                    if (!string.IsNullOrEmpty(VslName))
                    {
                        sb.Append(" And AMT.AIRLINE_NAME = '" + VslName + "'");
                        if (!string.IsNullOrEmpty(VoyNr))
                        {
                            sb.Append(" And JCAET.VOYAGE_FLIGHT_NO = '" + VoyNr + "'");
                        }
                    }
                    sb.Append(" order by NVL(JCAET.ETD_DATE,'01/01/0001') desc ");
                }
                else
                {
                    sb.Append("SELECT * FROM (SELECT BAT.BOOKING_MST_PK BOOKING_PK,");
                    sb.Append("       BAT.BOOKING_REF_NO,");
                    sb.Append("       JCAET.JOB_CARD_TRN_PK JOBCARD_PK,");
                    sb.Append("       JCAET.JOBCARD_REF_NO,");
                    sb.Append("       JCAET.JOBCARD_DATE JOBCARD_DATE,");
                    sb.Append("       TO_DATE(BAT.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                    sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 1) BIZ_TYPE,");
                    sb.Append("       DECODE(BAT.CARGO_TYPE, '1', 'KGS', '2', 'ULD') CARGO_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       AOO.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN JCAET.VOYAGE_FLIGHT_NO IS NULL THEN");
                    sb.Append("          AMT.AIRLINE_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          (AMT.AIRLINE_NAME || '/' || JCAET.VOYAGE_FLIGHT_NO)");
                    sb.Append("       END VESSEL_FLIGHT,");
                    sb.Append("       JCAET.ETD_DATE ETD_DATE,");
                    sb.Append("       JCAET.ETA_DATE ETA_DATE,");
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
                    sb.Append("   AND AMT.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCAET.COMMODITY_GROUP_FK");
                    sb.Append("   AND JCAET.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND AOO.LOCATION_MST_FK = " + LocFk + "");
                    sb.Append("   AND BAT.STATUS <> 3  ");
                    sb.Append("   AND JCAET.PROCESS_TYPE =1");
                    //If Business = 0 Then
                    sb.Append("   AND JCAET.BUSINESS_TYPE = 1");
                    //End If
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        sb.Append("   And TO_DATE(JCAET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        sb.Append("   And TO_DATE(JCAET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
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
                        if (!string.IsNullOrEmpty(VoyNr))
                        {
                            sb.Append(" And JCAET.VOYAGE_FLIGHT_NO = '" + VoyNr + "'");
                        }
                    }
                    if (CommGrp != 0)
                    {
                        sb.Append(" And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "");
                    }

                    sb.Append(" UNION ");
                    sb.Append(" SELECT BST.BOOKING_MST_PK BOOKING_PK,");
                    sb.Append("       BST.BOOKING_REF_NO,");
                    sb.Append("       JCSET.JOB_CARD_TRN_PK JOBCARD_PK,");
                    sb.Append("       JCSET.JOBCARD_REF_NO,");
                    sb.Append("       JCSET.JOBCARD_DATE JOBCARD_DATE,");
                    sb.Append("       TO_DATE(BST.SHIPMENT_DATE, 'DD/MM/YYYY') SHIPMENT_DATE,");
                    sb.Append("       (SELECT DD.DD_ID FROM QFOR_DROP_DOWN_TBL DD WHERE DD.DD_FLAG = 'BIZ_TYPE' AND DD.CONFIG_ID = 'QFORCOMMON' AND DD.DD_VALUE = 2) BIZ_TYPE,");
                    sb.Append("       DECODE(BST.CARGO_TYPE, '1', 'FCL', '2', 'LCL', '4', 'BBC') CARGO_TYPE,");
                    sb.Append("       CMT.CUSTOMER_NAME,");
                    sb.Append("       POL.PORT_ID,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN VT.VOYAGE IS NULL THEN");
                    sb.Append("          VVT.VESSEL_NAME");
                    sb.Append("         ELSE");
                    sb.Append("          (VVT.VESSEL_NAME || '/' || VT.VOYAGE)");
                    sb.Append("       END VESSEL_FLIGHT,");
                    sb.Append("       JCSET.ETD_DATE ETD_DATE,");
                    sb.Append("       JCSET.ETA_DATE ETA_DATE,");
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
                    sb.Append("   AND CGMT.COMMODITY_GROUP_PK(+) = JCSET.COMMODITY_GROUP_FK");
                    sb.Append("   AND JCSET.HBL_HAWB_FK IS NULL");
                    sb.Append("   AND POL.LOCATION_MST_FK =  " + LocFk + "");
                    sb.Append("   AND BST.STATUS <> 3  ");
                    sb.Append("   AND JCSET.PROCESS_TYPE =1");
                    //If Business = 0 Then
                    sb.Append("   AND JCSET.BUSINESS_TYPE = 2");
                    //End If
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        sb.Append("   And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        sb.Append("   And TO_DATE(JCSET.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',dateformat)");
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
                        if (!string.IsNullOrEmpty(VoyNr))
                        {
                            sb.Append(" And VT.VOYAGE = '" + VoyNr + "'");
                        }
                    }
                    if (CommGrp != 0)
                    {
                        sb.Append("  And CGMT.COMMODITY_GROUP_PK = " + CommGrp + "");
                    }
                    sb.Append(") order by NVL(ETD_DATE,'01/01/0001') desc ");
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

        #region "JC Pending for Customs Clearance Function"

        /// <summary>
        /// Fetches the customs details.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="CustPK">The customer pk.</param>
        /// <param name="VoyTrnPk">The voy TRN pk.</param>
        /// <param name="FlightNr">The flight nr.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="ETDDt">The etd dt.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="CommGrpPk">The comm GRP pk.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <param name="Status">The status.</param>
        /// <param name="CHAPK">The chapk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Excel">The excel.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchCustomsDetails(string JCPK = "", string LocFk = "", string CustPK = "", string VoyTrnPk = "", string FlightNr = "", string FromDt = "", string ToDt = "", string ETDDt = "", int BizType = 0, int Process = 0,
        int CargoType = 0, string CommGrpPk = "", int JobType = 0, int Status = 0, string CHAPK = "", Int32 flag = 0, Int32 Excel = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();

            try
            {
                var _with2 = objWF.MyCommand.Parameters;
                _with2.Add("JCPK_IN", (string.IsNullOrEmpty(JCPK) ? "" : JCPK)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_PK_IN", LocFk).Direction = ParameterDirection.Input;
                _with2.Add("CUSTOMER_PK_IN", (string.IsNullOrEmpty(CustPK) ? "" : CustPK)).Direction = ParameterDirection.Input;
                _with2.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
                _with2.Add("TODATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with2.Add("VOY_TRN_PK_IN", (string.IsNullOrEmpty(VoyTrnPk) ? "" : VoyTrnPk)).Direction = ParameterDirection.Input;
                _with2.Add("FLIGHT_NR_IN", (string.IsNullOrEmpty(FlightNr) ? "" : FlightNr)).Direction = ParameterDirection.Input;
                _with2.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with2.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with2.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with2.Add("STATUS_IN", Status).Direction = ParameterDirection.Input;
                _with2.Add("CHAPK_IN", (string.IsNullOrEmpty(CHAPK) ? "" : CHAPK)).Direction = ParameterDirection.Input;
                _with2.Add("COMMODITY_GROUP_PK_IN", (string.IsNullOrEmpty(CommGrpPk) ? "" : CommGrpPk)).Direction = ParameterDirection.Input;
                _with2.Add("ETD_IN", (string.IsNullOrEmpty(ETDDt) ? "" : ETDDt)).Direction = ParameterDirection.Input;
                _with2.Add("ISREPORT_DATA_IN", Excel).Direction = ParameterDirection.Input;
                _with2.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                DS = objWF.GetDataSet("FETCH_BKG_CUSTOMS_CLR_PKG", "FETCH_BKG_CUSTOMS_CLR_CBJC");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
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

        #endregion "JC Pending for Customs Clearance Function"
    }
}