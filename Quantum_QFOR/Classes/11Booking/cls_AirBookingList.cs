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
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_AirBookingList : CommonFeatures
    {
        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="Shipmentdate">The shipmentdate.</param>
        /// <param name="Commodityfk">The commodityfk.</param>
        /// <param name="BookingType">Type of the booking.</param>
        /// <param name="BookingPK">The booking pk.</param>
        /// <param name="CustomerPK">The customer pk.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="intCargoType">Type of the int cargo.</param>
        /// <param name="intXBkg">The int x BKG.</param>
        /// <param name="intStatus">The int status.</param>
        /// <param name="strSearchType">Type of the string search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="blnSortAscending">The BLN sort ascending.</param>
        /// <param name="lngUserLocFk">The LNG user loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string Shipmentdate = "", string Commodityfk = "0", string BookingType = "0", string BookingPK = "", string CustomerPK = "", string POLPK = "", string PODPK = "", Int16 intCargoType = 0, Int16 intXBkg = 0, string intStatus = "",
        string strSearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, string blnSortAscending = "", long lngUserLocFk = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition = strCondition + " and 1=2 ";
            }

            if (!(Shipmentdate == null))
            {
                strCondition += "      AND BAT.SHIPMENT_DATE = TO_DATE('" + Shipmentdate + "','" + dateFormat + "')   ";
            }

            if (Convert.ToInt32(Commodityfk) > 0)
            {
                strCondition = strCondition + " AND BAT.Commodity_Group_Fk=" + Commodityfk;
            }

            if (BookingType != "0")
            {
                strCondition = strCondition + " AND BTA.TRANS_REFERED_FROM=" + BookingType;
            }

            if (BookingPK.Trim().Length > 0)
            {
                strCondition = strCondition + " And BAT.BOOKING_AIR_PK = " + BookingPK;
            }
            if (CustomerPK.Trim().Length > 0)
            {
                strCondition = strCondition + " And BAT.CUST_CUSTOMER_MST_FK = " + CustomerPK;
            }
            if (POLPK.Trim().Length > 0)
            {
                strCondition = strCondition + " And BAT.PORT_MST_POL_FK = " + POLPK;
            }
            if (PODPK.Trim().Length > 0)
            {
                strCondition = strCondition + " And BAT.PORT_MST_POD_FK = " + PODPK;
            }
            if (intXBkg == 1)
            {
                strCondition = strCondition + " And BAT.CB_AGENT_MST_FK IS NOT NULL";
            }

            if (intStatus == "-1")
            {
                strCondition = strCondition + " AND BAT.FROM_FLAG=1";
                strCondition = strCondition + " AND BAT.STATUS<>-1";
            }
            else
            {
                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                {
                    if (Convert.ToInt32(intStatus) > 0)
                    { strCondition = strCondition + " AND BAT.IS_EBOOKING=1"; }
                    else
                    {
                        if (Convert.ToInt32(intStatus) > 0) { strCondition = strCondition + " AND BAT.STATUS=" + intStatus; }
                        strCondition = strCondition + " AND BAT.IS_EBOOKING=0";
                    }
                }
                else
                {
                    strCondition = strCondition + " AND BAT.STATUS=" + intStatus;
                }
            }
            strCondition = strCondition + " AND PMTL.LOCATION_MST_FK = " + lngUserLocFk + " ";

            strCondition = strCondition + " AND BAT.CREATED_BY_FK = UMT.USER_MST_PK ";
            strSQL = "SELECT Count(*) from BOOKING_AIR_TBL BAT,USER_MST_TBL UMT,PORT_MST_TBL PMTL,BOOKING_TRN_AIR BTA where 1=1 AND PMTL.PORT_MST_PK=BAT.PORT_MST_POL_FK";
            strSQL += " AND BTA.BOOKING_AIR_FK=BAT.BOOKING_AIR_PK(+)";
            strSQL += strCondition;
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

            strSQL = " select  * from (";
            strSQL += " SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT ";
            strSQL += " BAT.BOOKING_AIR_PK,";
            strSQL += " UPPER(BAT.BOOKING_REF_NO),";
            strSQL += " BAT.BOOKING_DATE,";
            strSQL += " CMT.CUSTOMER_ID,";
            strSQL += " UPPER(JCAET.JOBCARD_REF_NO),";
            strSQL += " PMTL.PORT_ID AS POL,";
            strSQL += " PMTD.PORT_ID AS POD,";
            strSQL += " BAT.SHIPMENT_DATE AS SHIPMETDATE,";
            strSQL += " CGMT.COMMODITY_GROUP_CODE AS COMMGROUP,";
            strSQL += " DECODE(BTA.TRANS_REFERED_FROM,1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Airline Tariff',5,'Gen Tariff',6,'SRR',7,'Manual') AS BOOKINGTYPE,";
            ///
            strSQL += " AMTCB.AGENT_ID AS CBAGENT,";
            strSQL += " AMTCL.AGENT_ID AS CLAGENT,";

            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                if (Convert.ToInt32(intStatus) == 4)
                {
                    strSQL += " DECODE(BAT.STATUS,-1,'Nominated',1,'Pending',2,'Confirm',3,'Rejected',6,'Shipped')STATUS,";
                }
                else
                {
                    strSQL += " DECODE(BAT.STATUS,-1,'Nominated',1,'Provisional',2,'Confirm',3,'Cancelled',6,'Shipped')STATUS,";
                }
            }
            else
            {
                strSQL += " DECODE(BAT.STATUS,-1,'Nominated',1,'Provisional',2,'Confirm',3,'Cancelled',6,'Shipped')STATUS,";
            }
            strSQL += " JCAET.JOB_CARD_AIR_EXP_PK";
            strSQL += " FROM BOOKING_AIR_TBL BAT,";

            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                strSQL += " V_ALL_CUSTOMER CMT,";
            }
            else
            {
                strSQL += " CUSTOMER_MST_TBL CMT,";
            }
            //'
            strSQL += " COMMODITY_GROUP_MST_TBL CGMT,";
            strSQL += " BOOKING_TRN_AIR          BTA,";
            //'
            strSQL += " PORT_MST_TBL PMTL, PORT_MST_TBL PMTD,USER_MST_TBL UMT,";
            strSQL += " AGENT_MST_TBL AMTCB, AGENT_MST_TBL AMTCL , JOB_CARD_AIR_EXP_TBL JCAET ";
            strSQL += " WHERE ";
            strSQL += " BAT.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK(+)";
            strSQL += " AND BAT.PORT_MST_POL_FK=PMTL.PORT_MST_PK(+)";
            strSQL += " AND BAT.PORT_MST_POD_FK=PMTD.PORT_MST_PK(+)";
            strSQL += " AND BAT.CB_AGENT_MST_FK=AMTCB.AGENT_MST_PK(+)";
            strSQL += " AND BAT.CL_AGENT_MST_FK=AMTCL.AGENT_MST_PK(+)";
            strSQL += " AND BAT.BOOKING_AIR_PK=JCAET.BOOKING_AIR_FK (+)";
            //'
            strSQL += " AND CGMT.COMMODITY_GROUP_PK(+) = BAT.COMMODITY_GROUP_FK ";
            strSQL += " AND BTA.BOOKING_AIR_FK(+)=BAT.BOOKING_AIR_PK";
            //'
            strSQL += strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }
            if (!blnSortAscending.Equals("ASC") & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL += " ,BOOKING_REF_NO DESC";
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

        #endregion "Fetch Function"

        #region "Fetch Nominations Function"

        /// <summary>
        /// Fetches the nominations.
        /// </summary>
        /// <param name="Shipmentdate">The shipmentdate.</param>
        /// <param name="Commodityfk">The commodityfk.</param>
        /// <param name="BookingType">Type of the booking.</param>
        /// <param name="BookingPK">The booking pk.</param>
        /// <param name="CustomerPK">The customer pk.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="intCargoType">Type of the int cargo.</param>
        /// <param name="intXBkg">The int x BKG.</param>
        /// <param name="intStatus">The int status.</param>
        /// <param name="strSearchType">Type of the string search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="blnSortAscending">The BLN sort ascending.</param>
        /// <param name="lngUserLocFk">The LNG user loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="EXECUTIVE_FK">The executiv e_ fk.</param>
        /// <param name="PO_NUMBER">The p o_ number.</param>
        /// <returns></returns>
        public DataSet FetchNominations(string Shipmentdate = "", string Commodityfk = "0", string BookingType = "0", string BookingPK = "", string CustomerPK = "", string POLPK = "", string PODPK = "", Int16 intCargoType = 0, Int16 intXBkg = 0, string intStatus = "",
        string strSearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, string blnSortAscending = "", long lngUserLocFk = 0, Int32 flag = 0, int EXECUTIVE_FK = 0, string PO_NUMBER = "")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition = strCondition + " and 1=2 ";
            }

            if (!(Shipmentdate == null))
            {
                strCondition += "      AND BAT.SHIPMENT_DATE = TO_DATE('" + Shipmentdate + "','" + dateFormat + "')   ";
            }

            if (Convert.ToInt32(Commodityfk) > 0)
            {
                strCondition = strCondition + " AND BAT.Commodity_Group_Fk=" + Commodityfk;
            }

            if (BookingType != "0")
            {
                strCondition = strCondition + " AND BTA.TRANS_REFERED_FROM=" + BookingType;
            }
            try
            {
                if (Convert.ToInt32(BookingPK) > 0)
                {
                    strCondition = strCondition + " And BAT.BOOKING_AIR_PK = " + BookingPK;
                }
            }
            catch (Exception ex)
            {
            }
            try
            {
                if (Convert.ToInt32(CustomerPK) > 0)
                {
                    strCondition = strCondition + " And BAT.CUST_CUSTOMER_MST_FK = " + CustomerPK;
                }
            }
            catch (Exception ex)
            {
            }
            try
            {
                if (Convert.ToInt32(POLPK) > 0)
                {
                    strCondition = strCondition + " And BAT.PORT_MST_POL_FK = " + POLPK;
                }
            }
            catch (Exception ex)
            {
            }
            try
            {
                if (Convert.ToInt32(PODPK) > 0)
                {
                    strCondition = strCondition + " And BAT.PORT_MST_POD_FK = " + PODPK;
                }
            }
            catch (Exception ex)
            {
            }

            if (intXBkg == 1)
            {
                strCondition = strCondition + " And BAT.CB_AGENT_MST_FK IS NOT NULL";
            }
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                if (Convert.ToInt32(intStatus) == 4)
                {
                    strCondition = strCondition + " AND BAT.IS_EBOOKING=1";
                }
                else
                {
                    if (Convert.ToInt32(intStatus) != 7)
                    {
                        strCondition = strCondition + " AND BAT.STATUS=" + intStatus;
                    }
                    strCondition = strCondition + " AND BAT.IS_EBOOKING=0";
                }
            }
            else
            {
                strCondition = strCondition + " AND BAT.STATUS=" + intStatus;
            }
            strCondition = strCondition + " AND PMTD.LOCATION_MST_FK = " + lngUserLocFk + " ";

            strCondition = strCondition + " AND NVL(BAT.FROM_FLAG,0) = 1";

            if (EXECUTIVE_FK > 0)
            {
                strCondition = strCondition + " AND NVL(EMP.EMPLOYEE_MST_PK,0) = " + EXECUTIVE_FK;
            }
            if (!string.IsNullOrEmpty(PO_NUMBER.Trim()))
            {
                strCondition = strCondition + " AND BAT.PO_NUMBER LIKE '%" + PO_NUMBER.Trim().ToUpper() + "%'";
            }
            strCondition = strCondition + " AND BAT.CREATED_BY_FK = UMT.USER_MST_PK ";
            strSQL = "SELECT Count(*) FROM BOOKING_AIR_TBL BAT,";
            strSQL += " USER_MST_TBL UMT,";
            strSQL += " PORT_MST_TBL PMTL,PORT_MST_TBL PMTD,";
            strSQL += " BOOKING_TRN_AIR BTA ";
            strSQL += " WHERE BAT.PORT_MST_POL_FK=PMTL.PORT_MST_PK(+)";
            strSQL += " AND BAT.PORT_MST_POD_FK=PMTD.PORT_MST_PK(+)";
            strSQL += " AND BTA.BOOKING_AIR_FK=BAT.BOOKING_AIR_PK(+)";
            strSQL += strCondition;
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

            strSQL = " select  * from (";
            strSQL += " SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT ";
            strSQL += " BAT.BOOKING_AIR_PK PK,";
            strSQL += " UPPER(BAT.NOMINATION_REF_NO) BOOKING_REF_NO,";
            strSQL += " TO_CHAR(BAT.BOOKING_DATE,DATEFORMAT) BOOKING_DATE,";
            strSQL += " CMT.CUSTOMER_NAME,";
            strSQL += " JCAET.JOB_CARD_AIR_EXP_PK,";
            strSQL += " UPPER(JCAET.JOBCARD_REF_NO) JOBCARD_REF_NO,";
            strSQL += " PMTL.PORT_ID AS POL,";
            strSQL += " PMTD.PORT_ID AS POD,";
            strSQL += " TO_CHAR(BAT.SHIPMENT_DATE,DATEFORMAT) AS SHIPMENTDATE,";
            strSQL += " BAT.PO_NUMBER,";
            strSQL += " TO_CHAR(BAT.PO_DATE,DATEFORMAT) PO_DATE, ";
            strSQL += " CGMT.COMMODITY_GROUP_CODE AS COMMGROUP,";
            strSQL += " DECODE(BTA.TRANS_REFERED_FROM,1,'Quotation',2,'Spot Rate',3,'Customer Contract',4,'Airline Tariff',5,'Gen Tariff',6,'SRR',7,'Manual') AS BOOKINGTYPE,";
            strSQL += " EMP.EMPLOYEE_MST_PK EXECUTIVE_FK,";
            strSQL += " EMP.EMPLOYEE_ID EXECUTIVE_ID,";
            strSQL += " EMP.EMPLOYEE_NAME EXECUTIVE_NAME,";
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                if (Convert.ToInt32(intStatus) == 4)
                {
                    strSQL += " DECODE(BAT.STATUS,-1,'Nominated',1,'Pending',2,'Confirm',3,'Rejected',6,'Shipped')STATUS ";
                }
                else
                {
                    strSQL += " DECODE(BAT.STATUS,-1,'Nominated',1,'Provisional',2,'Confirm',3,'Cancelled',6,'Shipped')STATUS ";
                }
            }
            else
            {
                strSQL += " DECODE(BAT.STATUS,-1,'Nominated',1,'Provisional',2,'Confirm',3,'Cancelled',6,'Shipped')STATUS ";
            }
            strSQL += " FROM BOOKING_AIR_TBL BAT,";
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                strSQL += " V_ALL_CUSTOMER CMT,";
                strSQL += " CUSTOMER_MST_TBL CUST,";
            }
            else
            {
                strSQL += " CUSTOMER_MST_TBL CMT,";
            }
            strSQL += " EMPLOYEE_MST_TBL  EMP,";
            strSQL += " COMMODITY_GROUP_MST_TBL CGMT,";
            strSQL += " BOOKING_TRN_AIR          BTA,";
            strSQL += " PORT_MST_TBL PMTL, PORT_MST_TBL PMTD,USER_MST_TBL UMT,";
            strSQL += " AGENT_MST_TBL AMTCB, AGENT_MST_TBL AMTCL , JOB_CARD_AIR_EXP_TBL JCAET ";
            strSQL += " WHERE ";
            strSQL += " BAT.CUST_CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK(+)";
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                strSQL += " AND CMT.CUSTOMER_MST_PK = CUST.CUSTOMER_MST_PK(+) ";
                strSQL += " AND CUST.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
            }
            else
            {
                strSQL += " AND CMT.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+) ";
            }
            strSQL += " AND BAT.PORT_MST_POL_FK=PMTL.PORT_MST_PK(+)";
            strSQL += " AND BAT.PORT_MST_POD_FK=PMTD.PORT_MST_PK(+)";
            strSQL += " AND BAT.CB_AGENT_MST_FK=AMTCB.AGENT_MST_PK(+)";
            strSQL += " AND BAT.CL_AGENT_MST_FK=AMTCL.AGENT_MST_PK(+)";
            strSQL += " AND BAT.BOOKING_AIR_PK=JCAET.BOOKING_AIR_FK (+)";
            strSQL += " AND CGMT.COMMODITY_GROUP_PK(+) = BAT.COMMODITY_GROUP_FK ";
            strSQL += " AND BTA.BOOKING_AIR_FK(+)=BAT.BOOKING_AIR_PK";
            strSQL += strCondition;
            if (!strColumnName.Equals("SR_NO"))
            {
                strSQL += "order by " + strColumnName;
            }
            if (!blnSortAscending.Equals("ASC") & !strColumnName.Equals("SR_NO"))
            {
                strSQL += " DESC";
            }
            strSQL += " ,BOOKING_REF_NO DESC";
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

        #endregion "Fetch Nominations Function"

        #region " Enhance Search Functions "

        /// <summary>
        /// Fetches the booking no.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchBookingNo(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBusinessType = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_BOOKING_NO_COMMON";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        #region "Get Booking Count"

        /// <summary>
        /// Gets the booking count.
        /// </summary>
        /// <param name="BkgRefNr">The BKG reference nr.</param>
        /// <param name="BkgPk">The BKG pk.</param>
        /// <returns></returns>
        public int GetBookingCount(string BkgRefNr, long BkgPk)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(1000);
                sb.Append(" SELECT BAT.BOOKING_AIR_PK, BAT.BOOKING_REF_NO");
                sb.Append(" FROM BOOKING_AIR_TBL BAT");
                sb.Append(" WHERE BAT.BOOKING_REF_NO LIKE '%" + BkgRefNr + "%'");
                DataSet ds = new DataSet();
                ds = (new WorkFlow()).GetDataSet(sb.ToString());

                if (ds.Tables[0].Rows.Count == 1)
                {
                    BkgRefNr = ds.Tables[0].Rows[0][1].ToString();
                    BkgPk = Convert.ToInt32(ds.Tables[0].Rows[0][0]);
                }
                return ds.Tables[0].Rows.Count;
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

        #endregion "Get Booking Count"
    }
}