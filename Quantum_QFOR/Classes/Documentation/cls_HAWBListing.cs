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

//Modified by Mani.Sureshkumar
namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsHAWBListing : CommonFeatures
    {
        #region "Fetch HAWB Count"

        /// <summary>
        /// Gets the hawb count.
        /// </summary>
        /// <param name="HAWBRefNr">The hawb reference nr.</param>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <returns></returns>
        public int GetHAWBCount(string HAWBRefNr, int HAWBPk)
        {
            try
            {
                System.Text.StringBuilder strHAWBQuery = new System.Text.StringBuilder(5000);
                strHAWBQuery.Append("select hwb.hawb_exp_tbl_pk from hawb_exp_tbl hwb where hwb.hawb_ref_no like '%" + HAWBRefNr + "%'");
                WorkFlow objWF = new WorkFlow();
                DataSet objHAWBDS = null;
                objHAWBDS = objWF.GetDataSet(strHAWBQuery.ToString());
                if (objHAWBDS.Tables[0].Rows.Count == 1)
                {
                    HAWBPk = Convert.ToInt32(objHAWBDS.Tables[0].Rows[0][0]);
                }
                return objHAWBDS.Tables[0].Rows.Count;
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
        }

        #endregion "Fetch HAWB Count"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="HAWBRefNo">The hawb reference no.</param>
        /// <param name="Shipperid">The shipperid.</param>
        /// <param name="POLID">The polid.</param>
        /// <param name="PODID">The podid.</param>
        /// <param name="POLname">The po lname.</param>
        /// <param name="PODname">The po dname.</param>
        /// <param name="HAWBdate">The haw bdate.</param>
        /// <param name="Airline">The airline.</param>
        /// <param name="Flight">The flight.</param>
        /// <param name="Status">The status.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="Etddate">The etddate.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAll(string HAWBRefNo = "", string Shipperid = "", string POLID = "", string PODID = "", string POLname = "", string PODname = "", string HAWBdate = "", string Airline = "", string Flight = "", string Status = "",
        string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", long usrLocFK = 0, string Etddate = "", Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            //**********Condition part*********
            if (flag == 0)
            {
                strCondition += " AND 1=2 ";
            }
            if (HAWBRefNo.Length > 0)
            {
                strCondition += " AND UPPER(H.HAWB_REF_NO) LIKE '%" + HAWBRefNo.ToUpper().Replace("'", "''") + "%'";
            }
            if (Shipperid.Length > 0)
            {
                strCondition += " AND C.CUSTOMER_ID LIKE '%" + Shipperid.ToUpper().Replace("'", "''") + "%'";
            }
            if (HAWBdate.Length > 0)
            {
                strCondition += " AND TO_DATE(H.HAWB_DATE,DATEFORMAT) = TO_DATE('" + HAWBdate + "',DATEFORMAT)";
            }
            if (Etddate.Length > 0)
            {
                strCondition += " AND TO_DATE(H.ETD_DATE,DATEFORMAT) = TO_DATE('" + Etddate + "',DATEFORMAT)";
            }
            if (Airline.Length > 0)
            {
                strCondition += "  AND Upper(A.AIRLINE_ID) LIKE '%" + Airline.ToUpper().Replace("'", "''") + "%'";
            }
            if (Flight.Length > 0)
            {
                strCondition += " AND Upper(H.FLIGHT_NO) LIKE '%" + Flight.ToUpper().Replace("'", "''") + "%'";
            }
            if (Convert.ToInt32(Status) != 4)
            {
                if (Status.Length > 0)
                {
                    strCondition += " AND H.HAWB_STATUS =" + Status;
                }
            }
            if (PODID.Length > 0)
            {
                strCondition += " AND B.PORT_MST_POD_FK IN ";
                strCondition += " ( SELECT P.PORT_MST_PK  ";
                strCondition += " FROM PORT_MST_TBL P ";
                strCondition += " WHERE ";
                strCondition += " P.PORT_ID LIKE '%" + PODID.ToUpper().Replace("' ", "''") + "%') ";
            }
            if (POLID.Length > 0)
            {
                strCondition += " AND B.PORT_MST_POL_FK IN ";
                strCondition += " (SELECT  P.PORT_MST_PK";
                strCondition += "   FROM PORT_MST_TBL P ";
                strCondition += "  WHERE ";
                strCondition += "  P.PORT_ID LIKE '%" + POLID.ToUpper().Replace("' ", "''") + "%') ";
            }
            strCondition += " AND J.BUSINESS_TYPE = 1 ";
            strCondition += " AND (H.JOB_CARD_AIR_EXP_FK = J.JOB_CARD_TRN_PK OR H.HAWB_EXP_TBL_PK=J.HBL_HAWB_FK OR H.NEW_JOB_CARD_AIR_EXP_FK = J.JOB_CARD_TRN_PK) ";
            //*****'End Condition****
            strSQL += " SELECT DISTINCT COUNT(*) ";
            strSQL += " FROM";
            strSQL += " HAWB_EXP_TBL H,";
            strSQL += " JOB_CARD_TRN J,";
            strSQL += " CUSTOMER_MST_TBL C,";
            strSQL += " BOOKING_MST_TBL B, ";
            strSQL += " PORT_MST_TBL P,";
            strSQL += " AIRLINE_MST_TBL A,";
            strSQL += " PORT_MST_TBL P1, ";
            strSQL += " USER_MST_TBL UMT ";
            strSQL += " WHERE";
            strSQL += " A.AIRLINE_MST_PK = B.CARRIER_MST_FK ";
            strSQL += " AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + "";
            strSQL += " AND H.CREATED_BY_FK = UMT.USER_MST_PK";
            strSQL += " AND C.CUSTOMER_MST_PK=J.SHIPPER_CUST_MST_FK ";
            strSQL += " AND J.BOOKING_MST_FK=B.BOOKING_MST_PK ";
            strSQL += " AND B.PORT_MST_POL_FK=P.PORT_MST_PK ";
            strSQL += " AND B.PORT_MST_POD_FK=P1.PORT_MST_PK  ";
            //strSQL &= " AND H.JOB_CARD_AIR_EXP_FK = J.JOB_CARD_TRN_PK "
            strSQL += strCondition;

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

            strSQL = "";

            strSQL += " SELECT * FROM ( SELECT  ROWNUM SR_NO, Q.* FROM ";
            strSQL += " (SELECT * FROM ";
            strSQL += " (SELECT DISTINCT H.HAWB_EXP_TBL_PK, ";
            strSQL += " H.HAWB_REF_NO, ";
            strSQL += " H.HAWB_DATE, ";
            strSQL += " C.CUSTOMER_ID,";
            strSQL += " P.PORT_ID AS POL,";
            strSQL += " P1.PORT_ID AS POD, ";
            strSQL += " A.AIRLINE_ID, ";
            strSQL += " H.FLIGHT_NO, ";
            strSQL += " H.ETD_DATE, ";
            strSQL += " DECODE(H.HAWB_STATUS, '0','Draft','1','Released','2','Confirmed','3','Cancelled','4','All' ) STATUS,'' SEL ";
            strSQL += " FROM HAWB_EXP_TBL H ,";
            strSQL += " JOB_CARD_TRN J, ";
            strSQL += " CUSTOMER_MST_TBL C,";
            strSQL += " BOOKING_MST_TBL B, ";
            strSQL += " PORT_MST_TBL P,";
            strSQL += " AIRLINE_MST_TBL A,";
            strSQL += " PORT_MST_TBL P1, ";
            strSQL += " USER_MST_TBL UMT ";
            strSQL += " WHERE ";
            strSQL += " A.AIRLINE_MST_PK = B.CARRIER_MST_FK ";
            strSQL += " AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + "";
            strSQL += " AND H.CREATED_BY_FK = UMT.USER_MST_PK";
            strSQL += " AND C.CUSTOMER_MST_PK(+)=J.SHIPPER_CUST_MST_FK ";
            strSQL += " AND J.BOOKING_MST_FK=B.BOOKING_MST_PK";
            strSQL += " AND B.PORT_MST_POL_FK=P.PORT_MST_PK ";
            strSQL += " AND B.PORT_MST_POD_FK=P1.PORT_MST_PK  ";

            strSQL += strCondition;
            //'Added by Faheem for Cancelled HAWB
            //strSQL &= " UNION "
            //strSQL &= " SELECT DISTINCT H.HAWB_EXP_TBL_PK, "
            //strSQL &= " H.HAWB_REF_NO, "
            //strSQL &= " H.HAWB_DATE, "
            //strSQL &= " C.CUSTOMER_ID,"
            //strSQL &= " P.PORT_ID AS POL,"
            //strSQL &= " P1.PORT_ID AS POD, "
            //strSQL &= " A.AIRLINE_ID, "
            //strSQL &= " H.FLIGHT_NO, "
            //strSQL &= "  H.ETD_DATE, "
            //'strSQL &= " DECODE(H.HAWB_STATUS, '0','Draft','1','Released','2','Confirmed','3','Cancelled' ) STATUS,'' SEL "
            //strSQL &= " DECODE(H.HAWB_STATUS, '0','Draft','1','Released','2','Confirmed','3','Cancelled','4','All' ) STATUS,'' SEL "
            //strSQL &= " FROM HAWB_EXP_TBL H ,"
            //strSQL &= " JOB_CARD_TRN J, "
            //strSQL &= " CUSTOMER_MST_TBL C,"
            //strSQL &= " BOOKING_MST_TBL B, "
            //strSQL &= " PORT_MST_TBL P,"
            //strSQL &= " AIRLINE_MST_TBL A,"
            //strSQL &= " PORT_MST_TBL P1, "
            //strSQL &= " USER_MST_TBL UMT "
            //strSQL &= " WHERE "
            //strSQL &= " A.AIRLINE_MST_PK = B.CARRIER_MST_FK "
            //strSQL &= " AND UMT.DEFAULT_LOCATION_FK = " & usrLocFK & ""
            //strSQL &= " AND H.CREATED_BY_FK = UMT.USER_MST_PK"
            //strSQL &= " AND C.CUSTOMER_MST_PK(+)=J.SHIPPER_CUST_MST_FK "
            //strSQL &= " AND J.BOOKING_MST_FK=B.BOOKING_MST_PK"
            //strSQL &= " AND B.PORT_MST_POL_FK=P.PORT_MST_PK "
            //strSQL &= " AND B.PORT_MST_POD_FK=P1.PORT_MST_PK  "
            //strSQL &= " AND H.NEW_JOB_CARD_AIR_EXP_FK = J.JOB_CARD_TRN_PK "
            //'End
            //strSQL &= strCondition
            strSQL += " )  ORDER BY HAWB_DATE DESC,HAWB_REF_NO DESC )Q)";
            //strSQL &= " ORDER BY H.HAWB_DATE DESC, H.HAWB_REF_NO DESC "
            strSQL += " WHERE SR_NO  Between " + start + " and " + last;

            DataSet DS = null;
            DS = objWF.GetDataSet(strSQL);
            try
            {
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
    }
}