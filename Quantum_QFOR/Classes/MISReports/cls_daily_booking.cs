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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_daily_booking : CommonFeatures
    {
        /// <summary>
        /// The m_last
        /// </summary>
        private Int32 m_last;

        /// <summary>
        /// The m_start
        /// </summary>
        private Int32 m_start;

        #region "Fetch Function"

        /// <summary>
        /// The ds main
        /// </summary>
        private DataSet DSMain = new DataSet();

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="CustomerPK">The customer pk.</param>
        /// <param name="BookingPK">The booking pk.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="lngUserLocFk">The LNG user loc fk.</param>
        /// <param name="intStatus">The int status.</param>
        /// <param name="VslVoyFK">The VSL voy fk.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="strSearchType">Type of the string search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="blnSortAscending">The BLN sort ascending.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="bookingno">The bookingno.</param>
        /// <param name="CountyPK">The county pk.</param>
        /// <param name="CurrPK">The curr pk.</param>
        /// <param name="ExecFK">The execute fk.</param>
        /// <returns></returns>
        public DataSet FetchAll(string CustomerPK = "", string BookingPK = "", string POLPK = "", string PODPK = "", string FromDate = "", string ToDate = "", string BizType = "", long lngUserLocFk = 0, string intStatus = "", string VslVoyFK = "",
        Int16 CargoType = 0, string strSearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, string blnSortAscending = "", Int32 flag = 0, string bookingno = "", long CountyPK = 0,
        long CurrPK = 0, long ExecFK = 0)
        {
            //Int32 last = "0";
            //Int32 start = "0";
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            DataSet dsGrid = new DataSet();
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("FROM_DT_IN", getDefault(Convert.ToDateTime(FromDate), "")).Direction = ParameterDirection.Input;
                if (!string.IsNullOrEmpty(ToDate))
                {
                    _with1.Add("TO_DT_IN", getDefault(Convert.ToDateTime(ToDate), "")).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with1.Add("TO_DT_IN", "").Direction = ParameterDirection.Input;
                }
                _with1.Add("CUSTOMER_MST_FK_IN", getDefault(CustomerPK, "")).Direction = ParameterDirection.Input;
                _with1.Add("BOOKING_MST_FK_IN", getDefault(BookingPK, "")).Direction = ParameterDirection.Input;
                _with1.Add("VSL_VOY_FK_IN", getDefault(VslVoyFK, "")).Direction = ParameterDirection.Input;
                _with1.Add("POL_FK_IN", getDefault(POLPK, "")).Direction = ParameterDirection.Input;
                _with1.Add("POD_FK_IN", getDefault(PODPK, "")).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                //'
                _with1.Add("STATUS_IN", intStatus).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", getDefault(lngUserLocFk, "")).Direction = ParameterDirection.Input;
                _with1.Add("COUNTRY_MST_FK_IN", getDefault(CountyPK, "")).Direction = ParameterDirection.Input;
                _with1.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with1.Add("FROM_CURR_FK_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with1.Add("TO_CURR_FK_IN", CurrPK).Direction = ParameterDirection.Input;
                _with1.Add("EXECUTIVE_FK_IN", ExecFK).Direction = ParameterDirection.Input;
                _with1.Add("CARPORATE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("COUNTRY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("BIZTYPE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("VSLVOY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("BKG_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsGrid = objWF.GetDataSet("FETCH_DAILY_BOOKING_RPT_PKG", "FETCH_DAILY_BKG_RPT");
                TotalRecords = dsGrid.Tables[0].Rows.Count;
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
                //last = CurrentPage * RecordsPerPage;
                //start = (CurrentPage - 1) * RecordsPerPage + 1;

                DataRelation drCorporate = null;
                DataRelation drCountry = null;
                DataRelation drLocation = null;
                DataRelation drBizType = null;
                DataRelation drVslVoy = null;
                DataRelation drBkg = null;

                drCorporate = new DataRelation("CORPORATE", dsGrid.Tables[0].Columns["COUNTRY_MST_FK"], dsGrid.Tables[1].Columns["COUNTRY_MST_FK"]);

                drCountry = new DataRelation("COUNTRY", new DataColumn[] {
                    dsGrid.Tables[1].Columns["COUNTRY_MST_FK"],
                    dsGrid.Tables[1].Columns["LOCATION_MST_FK"]
                }, new DataColumn[] {
                    dsGrid.Tables[2].Columns["COUNTRY_MST_FK"],
                    dsGrid.Tables[2].Columns["LOCATION_MST_FK"]
                });

                drLocation = new DataRelation("LOCATION", new DataColumn[] {
                    dsGrid.Tables[2].Columns["COUNTRY_MST_FK"],
                    dsGrid.Tables[2].Columns["LOCATION_MST_FK"],
                    dsGrid.Tables[2].Columns["BUSINESS_TYPE"]
                }, new DataColumn[] {
                    dsGrid.Tables[3].Columns["COUNTRY_MST_FK"],
                    dsGrid.Tables[3].Columns["LOCATION_MST_FK"],
                    dsGrid.Tables[3].Columns["BUSINESS_TYPE"]
                });

                drBizType = new DataRelation("BIZTYPE", new DataColumn[] {
                    dsGrid.Tables[3].Columns["COUNTRY_MST_FK"],
                    dsGrid.Tables[3].Columns["LOCATION_MST_FK"],
                    dsGrid.Tables[3].Columns["BUSINESS_TYPE"]
                }, new DataColumn[] {
                    dsGrid.Tables[4].Columns["COUNTRY_MST_FK"],
                    dsGrid.Tables[4].Columns["LOCATION_MST_FK"],
                    dsGrid.Tables[4].Columns["BUSINESS_TYPE"]
                });
                drVslVoy = new DataRelation("VSLSVOY", new DataColumn[] {
                    dsGrid.Tables[4].Columns["COUNTRY_MST_FK"],
                    dsGrid.Tables[4].Columns["LOCATION_MST_FK"],
                    dsGrid.Tables[4].Columns["BUSINESS_TYPE"],
                    dsGrid.Tables[4].Columns["VSLVOYFK"],
                    dsGrid.Tables[4].Columns["POL_NAME"],
                    dsGrid.Tables[4].Columns["POD_NAME"]
                }, new DataColumn[] {
                    dsGrid.Tables[5].Columns["COUNTRY_MST_FK"],
                    dsGrid.Tables[5].Columns["LOCATION_MST_FK"],
                    dsGrid.Tables[5].Columns["BUSINESS_TYPE"],
                    dsGrid.Tables[5].Columns["VSLVOYFK"],
                    dsGrid.Tables[5].Columns["POL_NAME"],
                    dsGrid.Tables[5].Columns["POD_NAME"]
                });

                drCorporate.Nested = true;
                drCountry.Nested = true;
                drLocation.Nested = true;
                drBizType.Nested = true;
                drVslVoy.Nested = true;
                dsGrid.Relations.Add(drCorporate);
                dsGrid.Relations.Add(drCountry);
                dsGrid.Relations.Add(drLocation);
                dsGrid.Relations.Add(drBizType);
                dsGrid.Relations.Add(drVslVoy);
                return dsGrid;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            //Try
            //    'If BizType contains non-numeric value then insert a default numeric value, 0
            //    Try
            //        BizType = CInt(BizType)
            //    Catch ex As Exception
            //        BizType = 0
            //    End Try
            //    '-----------------------------------------------------------------------------
            //    If flag = 0 Then
            //        strCondition = strCondition & vbCrLf & " and 1=2 "
            //    End If
            //    Try
            //        BookingPK = CInt(BookingPK)
            //    Catch ex As Exception
            //        BookingPK = 0
            //    End Try
            //    If BookingPK <> "0" Then
            //        strCondition = strCondition & " And BKG.BOOKING_MST_PK = " & BookingPK & vbCrLf
            //    End If

            //    If CustomerPK.Trim.Length > 0 Then
            //        strCondition = strCondition & " And CUST.CUSTOMER_MST_PK = " & CustomerPK & vbCrLf
            //    End If

            //    If POLPK.Trim.Length > 0 Then
            //        strCondition = strCondition & " And POL.PORT_MST_PK = " & POLPK & vbCrLf
            //    End If

            //    If PODPK.Trim.Length > 0 Then
            //        strCondition = strCondition & " And POD.PORT_MST_PK = " & PODPK & vbCrLf
            //    End If
            //    If BizType = 2 Then
            //        If CargoType <> 0 Then
            //            strCondition = strCondition & " AND BKG.CARGO_TYPE=" & CargoType & vbCrLf
            //        End If
            //    ElseIf BizType = 3 Or BizType = 0 Then
            //        If CargoType <> 0 Then
            //            strCondition = strCondition & " AND BKG.CARGO_TYPE=" & CargoType & vbCrLf
            //        End If
            //    End If
            //    If VslVoyFK.Trim.Length > 0 Then
            //        If BizType = 2 Then
            //            strCondition = strCondition & " AND BKG.Vessel_Voyage_Fk =" & VslVoyFK & vbCrLf
            //        ElseIf BizType = 1 Then
            //            strCondition = strCondition & " AND BKG.CARRIER_MST_FK=" & VslVoyFK & vbCrLf
            //        ElseIf BizType = 3 Or BizType = 0 Then
            //            strCondition = strCondition & " AND BKG.Vessel_Voyage_Fk =" & VslVoyFK & vbCrLf
            //        End If
            //    End If
            //    If intStatus <> "0" Then
            //        strCondition = strCondition & " AND BKG.STATUS=" & intStatus & vbCrLf
            //    End If
            //    If lngUserLocFk > "0" Then
            //        strCondition = strCondition & " AND LOC.LOCATION_MST_PK=" & lngUserLocFk & vbCrLf
            //    End If
            //    If Not ((FromDate Is Nothing Or FromDate = "") And (ToDate Is Nothing Or ToDate = "")) Then
            //        strCondition = strCondition & " AND BKG.BOOKING_DATE BETWEEN TO_DATE('" & FromDate & "', DATEFORMAT) AND TO_DATE('" & ToDate & "', DATEFORMAT)"
            //    ElseIf (Not IsNothing(FromDate) Or FromDate <> "") And (IsNothing(ToDate) Or ToDate = "") Then
            //        strCondition = strCondition & " AND BKG.BOOKING_DATE >= TO_DATE('" & FromDate & "',dateformat) "
            //    ElseIf (Not IsNothing(ToDate) Or ToDate <> "") And (IsNothing(FromDate) Or FromDate = "") Then
            //        strCondition = strCondition & " AND BKG.BOOKING_DATE <= TO_DATE('" & ToDate & "',dateformat) "
            //    End If
            //    If bookingno <> "" Then
            //        strCondition = strCondition & " AND BKG.BOOKING_REF_NO='" & bookingno & "'" & vbCrLf
            //    End If
            //    '*************
            //    strSQL = " SELECT  "
            //    strSQL &= vbCrLf & " BKG.BOOKING_MST_PK BOOKING,"
            //    strSQL &= vbCrLf & " BKG.BOOKING_REF_NO BOOKING_NO1,"
            //    strSQL &= vbCrLf & " BKG.BOOKING_REF_NO BOOKING_NO,"
            //    strSQL &= vbCrLf & "TO_DATE(BKG.Booking_Date, 'DD/MM/YYYY') BOOKING_DT,"
            //    strSQL &= vbCrLf & " CUST.CUSTOMER_NAME CUST_ID,"
            //    strSQL &= vbCrLf & " POL.PORT_NAME POL,"
            //    strSQL &= vbCrLf & " POD.PORT_NAME POD,"
            //    strSQL &= vbCrLf & " CASE WHEN BKG.BUSINESS_TYPE = 1 THEN AMT.AIRLINE_NAME ELSE "
            //    strSQL &= vbCrLf & "(case when BKG.VESSEL_NAME is not null and BKG.VOYAGE_FLIGHT_NO is not null then(BKG.VESSEL_NAME ||'/'|| BKG.VOYAGE_FLIGHT_NO)"
            //    strSQL &= vbCrLf & "Else '' end) END ID,"
            //    strSQL &= vbCrLf & " COM.COMMODITY_GROUP_CODE COMMODITY_CODE,"
            //    strSQL &= vbCrLf & " DECODE(BKG.STATUS,1,'Active',2,'Confirm',3,'Cancelled',6,'Shipped')Status"
            //    strSQL &= vbCrLf & " FROM CUSTOMER_MST_TBL     CUST,"
            //    strSQL &= vbCrLf & " PORT_MST_TBL         POL,"
            //    strSQL &= vbCrLf & " PORT_MST_TBL         POD,"
            //    strSQL &= vbCrLf & " BOOKING_MST_TBL      BKG,"
            //    strSQL &= vbCrLf & " LOC_PORT_MAPPING_TRN LOC_P,"
            //    strSQL &= vbCrLf & " LOCATION_MST_TBL     LOC,"
            //    strSQL &= vbCrLf & " USER_MST_TBL         USR,"
            //    strSQL &= vbCrLf & " COMMODITY_GROUP_MST_TBL COM,"
            //    strSQL &= vbCrLf & " AIRLINE_MST_TBL AMT "
            //    strSQL &= vbCrLf & " WHERE"
            //    strSQL &= vbCrLf & "  BKG.COMMODITY_GROUP_FK = COM.COMMODITY_GROUP_PK"
            //    strSQL &= vbCrLf & " AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK"
            //    strSQL &= vbCrLf & " AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK"
            //    strSQL &= vbCrLf & " AND BKG.CUST_CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK"
            //    strSQL &= vbCrLf & " AND LOC_P.PORT_MST_FK = POL.PORT_MST_PK"
            //    strSQL &= vbCrLf & " AND LOC_P.LOCATION_MST_FK = LOC.LOCATION_MST_PK"
            //    strSQL &= vbCrLf & " AND BKG.CREATED_BY_FK = USR.USER_MST_PK"
            //    strSQL &= vbCrLf & " AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK"
            //    strSQL &= vbCrLf & " AND BKG.CARRIER_MST_FK(+) = AMT.AIRLINE_MST_PK "
            //    If BizType <> 0 AndAlso BizType <> 3 Then
            //        strSQL &= vbCrLf & " AND BKG.BUSINESS_TYPE =" & BizType
            //    End If
            //    strSQL &= vbCrLf & strCondition
            //    '*************
            //    Dim strCount As New System.Text.StringBuilder
            //    strCount.Append(" SELECT COUNT(*) FROM( ")
            //    strCount.Append(strSQL.ToString)
            //    strCount.Append(" )")

            //    TotalRecords = CType(objWF.ExecuteScaler(strCount.ToString), Int32)
            //    TotalPage = TotalRecords \ RecordsPerPage
            //    If TotalRecords Mod RecordsPerPage <> 0 Then
            //        TotalPage += 1
            //    End If
            //    If CurrentPage > TotalPage Then
            //        CurrentPage = 1
            //    End If
            //    If TotalRecords = 0 Then
            //        CurrentPage = 0
            //    End If
            //    last = CurrentPage * RecordsPerPage
            //    start = (CurrentPage - 1) * RecordsPerPage + 1

            //    Dim strMain As New System.Text.StringBuilder

            //    strMain.Append("select * from ( ")
            //    strMain.Append("SELECT ROWNUM SR_NO,q.* FROM( ")
            //    strMain.Append(strSQL.ToString)
            //    If BizType = 3 Or BizType = 0 Then
            //        strMain.Append(" )q   order by  BOOKING_DT DESC ) WHERE SR_NO  Between " & start & " and " & last)
            //    Else
            //        strMain.Append(" ORDER BY BOOKING_DT  DESC")
            //        strMain.Append(" )q) WHERE SR_NO  Between " & start & " and " & last)
            //    End If
            //    Return objWF.GetDataSet(strMain.ToString)

            //Catch sqlExp As OracleException
            //    ErrorMessage = sqlExp.Message
            //    Throw sqlExp
            //Catch exp As Exception
            //    ErrorMessage = exp.Message
            //    Throw exp
            //End Try
        }

        #endregion "Fetch Function"

        #region "Get Function"

        /// <summary>
        /// Gets all.
        /// </summary>
        /// <param name="CustomerPK">The customer pk.</param>
        /// <param name="BookingPK">The booking pk.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="lngUserLocFk">The LNG user loc fk.</param>
        /// <param name="intStatus">The int status.</param>
        /// <param name="VslVoyFK">The VSL voy fk.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="strSearchType">Type of the string search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortCol">The sort col.</param>
        /// <param name="blnSortAscending">The BLN sort ascending.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet GetAll(string CustomerPK = "", string BookingPK = "", string POLPK = "", string PODPK = "", string FromDate = "", string ToDate = "", string BizType = "", string lngUserLocFk = "", string intStatus = "", string VslVoyFK = "",
        Int16 CargoType = 0, string strSearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 0, string blnSortAscending = "", Int32 flag = 0)
        {
            //Int32 last = "0";
            //Int32 start = "0";
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (flag == 0)
                {
                    strCondition = strCondition + " and 1=2 ";
                }
                //if (BookingPK > "0")
                //{
                //    strCondition = strCondition + " And BKG.BOOKING_MST_PK = " + BookingPK;
                //}

                if (CustomerPK.Trim().Length > 0)
                {
                    strCondition = strCondition + " And CUST.CUSTOMER_MST_PK = " + CustomerPK;
                }

                if (POLPK.Trim().Length > 0)
                {
                    strCondition = strCondition + " And POL.PORT_MST_PK = " + POLPK;
                }

                if (PODPK.Trim().Length > 0)
                {
                    strCondition = strCondition + " And POD.PORT_MST_PK = " + PODPK;
                }
                if (BizType == "2")
                {
                    if (CargoType != 0)
                    {
                        strCondition = strCondition + " AND BKG.CARGO_TYPE=" + CargoType;
                    }
                }
                else if (BizType == "3")
                {
                    if (CargoType != 0)
                    {
                        strCondition = strCondition + " AND BKG.CARGO_TYPE=" + CargoType;
                    }
                }
                if (VslVoyFK.Trim().Length > 0)
                {
                    if (BizType == "2")
                    {
                        strCondition = strCondition + " AND BKG.Vessel_Voyage_Fk =" + VslVoyFK;
                    }
                    else if (BizType == "1")
                    {
                        strCondition = strCondition + " AND BKG.CARRIER_MST_FK=" + VslVoyFK;
                    }
                    else
                    {
                        strCondition = strCondition + " AND BKG.Vessel_Voyage_Fk =" + VslVoyFK;
                    }
                }
                if (intStatus != "0")
                {
                    strCondition = strCondition + " AND BKG.STATUS=" + intStatus;
                }
                //if (lngUserLocFk > "0")
                //{
                //    strCondition = strCondition + " AND LOC.LOCATION_MST_PK=" + lngUserLocFk;
                //}
                if (!((FromDate == null | string.IsNullOrEmpty(FromDate)) & (ToDate == null | string.IsNullOrEmpty(ToDate))))
                {
                    strCondition = strCondition + " AND BKG.BOOKING_DATE BETWEEN TO_DATE('" + FromDate + "', DATEFORMAT) AND TO_DATE('" + ToDate + "', DATEFORMAT)";
                }
                else if (((FromDate != null) | !string.IsNullOrEmpty(FromDate)) & ((ToDate == null) | string.IsNullOrEmpty(ToDate)))
                {
                    strCondition = strCondition + " AND BKG.BOOKING_DATE >= TO_DATE('" + FromDate + "',dateformat) ";
                }
                else if (((ToDate != null) | !string.IsNullOrEmpty(ToDate)) & ((FromDate == null) | string.IsNullOrEmpty(FromDate)))
                {
                    strCondition = strCondition + " AND BKG.BOOKING_DATE >= TO_DATE('" + ToDate + "',dateformat) ";
                }
                //************
                strSQL = "SELECT ROWNUM \"SR_NO\", T.*";
                strSQL += " From (SELECT  ";
                strSQL += " BKG.BOOKING_MST_PK BOOKING,";
                strSQL += " BKG.BOOKING_REF_NO BOOKING_NO1,";
                strSQL += " BKG.BOOKING_REF_NO BOOKING_NO ,";
                strSQL += "  TO_DATE(BKG.Booking_Date, 'DD/MM/YYYY') BOOKING_DT,";
                strSQL += " CUST.CUSTOMER_NAME CUST_ID,";
                strSQL += " LOC.LOCATION_NAME LOCATION_NAM, ";
                strSQL += " POL.PORT_NAME POL,";
                strSQL += " POD.PORT_NAME POD,";
                strSQL += "  CASE WHEN BKG.BUSINESS_TYPE = 1 THEN AMT.AIRLINE_NAME ELSE ";
                strSQL += "(case when BKG.VESSEL_NAME is not null and BKG.VOYAGE_FLIGHT_NO is not null then(BKG.VESSEL_NAME ||'/'|| BKG.VOYAGE_FLIGHT_NO)";
                strSQL += "Else '' end) END ID,";
                strSQL += " COM.COMMODITY_GROUP_CODE COMMODITY_CODE,";
                strSQL += " DECODE(BKG.STATUS,1,'Active',2,'Confirm',3,'Cancelled',6,'Shipped')Status ";
                strSQL += " FROM CUSTOMER_MST_TBL     CUST,";
                strSQL += " PORT_MST_TBL         POL,";
                strSQL += " PORT_MST_TBL         POD,";
                strSQL += " BOOKING_MST_TBL      BKG,";
                strSQL += " LOC_PORT_MAPPING_TRN LOC_P,";
                strSQL += " LOCATION_MST_TBL     LOC,";
                strSQL += " USER_MST_TBL         USR,";
                strSQL += " COMMODITY_GROUP_MST_TBL COM,";
                strSQL += " AIRLINE_MST_TBL         AMT ";
                strSQL += " WHERE";
                strSQL += "  BKG.COMMODITY_GROUP_FK = COM.COMMODITY_GROUP_PK";
                strSQL += " AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK";
                strSQL += " AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK";
                strSQL += " AND BKG.CUST_CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK";
                strSQL += " AND LOC_P.PORT_MST_FK = POL.PORT_MST_PK";
                strSQL += " AND LOC_P.LOCATION_MST_FK = LOC.LOCATION_MST_PK";
                strSQL += " AND BKG.CREATED_BY_FK = USR.USER_MST_PK";
                strSQL += " AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK";
                strSQL += " AND BKG.CARRIER_MST_FK(+) = AMT.AIRLINE_MST_PK ";
                if (BizType != "0" && BizType != "3")
                {
                    strSQL += " AND BKG.BUSINESS_TYPE =" + BizType;
                }
                strSQL += strCondition;
                strSQL += "  ORDER BY BOOKING_DT DESC)T ";
                //************
                System.Text.StringBuilder strMain = new System.Text.StringBuilder();
                strMain.Append(strSQL.ToString());
                return objWF.GetDataSet(strMain.ToString());
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

        #endregion "Get Function"

        #region "Fetch Location of User Login"

        /// <summary>
        /// Fetches the location.
        /// </summary>
        /// <param name="Loc">The loc.</param>
        /// <returns></returns>
        public DataSet FetchLocation(long Loc)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
            StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID,");
            StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK");
            StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST");
            StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND L.LOCATION_MST_PK = " + Loc + "");

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
                //'Manjunath  PTS ID:Sep-02  23/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Location of User Login"

        #region "Fetch Buiseness Type"

        /// <summary>
        /// Gets the type of the biz.
        /// </summary>
        /// <returns></returns>
        public object GetBizType()
        {
            OracleCommand objCommand = new OracleCommand();
            string strSQL = null;
            WorkFlow objWk = new WorkFlow();
            try
            {
                strSQL = "select umt.business_type from user_mst_tbl umt where umt.user_mst_pk =" + HttpContext.Current.Session["USER_PK"];
                objWk.OpenConnection();
                var _with2 = objCommand;
                _with2.Connection = objWk.MyConnection;
                _with2.CommandType = CommandType.Text;
                _with2.CommandText = strSQL;
                return (_with2.ExecuteScalar());
                //'Manjunath  PTS ID:Sep-02  23/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            objWk.MyConnection.Close();
        }

        #endregion "Fetch Buiseness Type"
    }
}