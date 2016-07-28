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

using Oracle.ManagedDataAccess.Client;
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
    public class clsQuotationReport : CommonFeatures
    {
        /// <summary>
        /// The _ pk value
        /// </summary>
        private long _PkValue;

        /// <summary>
        /// The _ static_ col
        /// </summary>
        private int _Static_Col;

        /// <summary>
        /// The _ col_ incr
        /// </summary>
        private int _Col_Incr;

        /// <summary>
        /// The _ from date
        /// </summary>
        private string _FromDate = "";

        /// <summary>
        /// The _ todate
        /// </summary>
        private string _Todate = "";

        /// <summary>
        /// The _ use_ extra_ cols
        /// </summary>
        private bool _Use_Extra_Cols;

        /// <summary>
        /// The _ air line_ tariff_ cols
        /// </summary>
        private const int _AirLine_Tariff_Cols = 7;

        #region "GetCollectionData"

        /// <summary>
        /// Gets the ves voy report.
        /// </summary>
        /// <param name="Vessel">The vessel.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="customer">The customer.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="LocId">The loc identifier.</param>
        /// <param name="ShippingLine">The shipping line.</param>
        /// <param name="POL">The pol.</param>
        /// <param name="POD">The pod.</param>
        /// <param name="CommdtyGrp">The commdty GRP.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="strInd">The string ind.</param>
        /// <returns></returns>
        public DataSet GetVesVoyReport(string Vessel = "", string Voyage = "", string customer = "", Int32 CargoType = 1, string LocId = "", Int32 ShippingLine = 1, Int32 POL = 1, Int32 POD = 1, Int32 CommdtyGrp = 1, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, string strInd = "")
        {
            WorkFlow objWF = new WorkFlow();
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            DataSet ds = new DataSet();

            try
            {
                //'Added By Koteshwari on 21/3/2011 for Break Bulk Implementation
                if (CargoType == 4)
                {
                    objWF.MyCommand.Parameters.Clear();
                    var _with1 = objWF.MyCommand.Parameters;
                    _with1.Add("Vessel", getDefault(Vessel, "")).Direction = ParameterDirection.Input;
                    _with1.Add("Voyage12", getDefault(Voyage, "")).Direction = ParameterDirection.Input;
                    _with1.Add("CUSTOMER", getDefault(customer, "")).Direction = ParameterDirection.Input;
                    _with1.Add("VslVoy", getDefault(CargoType, "")).Direction = ParameterDirection.Input;
                    _with1.Add("LocId", getDefault(LocId, "")).Direction = ParameterDirection.Input;
                    _with1.Add("CURRENCY_IN", HttpContext.Current.Session["currency_mst_pk"]).Direction = ParameterDirection.Input;
                    //'------adding by Thirumoorthy on 02/11/11 for PTS ID : AUG-025
                    _with1.Add("OPERATOR_MST_PK_IN", getDefault(ShippingLine, "")).Direction = ParameterDirection.Input;
                    _with1.Add("PORT_MST_POL_PK_IN", getDefault(POL, "")).Direction = ParameterDirection.Input;
                    _with1.Add("PORT_MST_POD_PK_IN", getDefault(POD, "")).Direction = ParameterDirection.Input;
                    _with1.Add("COMMODITY_GROUP_PK_IN", getDefault(CommdtyGrp, "")).Direction = ParameterDirection.Input;
                    //'------End
                    _with1.Add("VesVoy_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                    _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                    _with1.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.InputOutput;
                    //'---------Added by Thirumoorthy  PTS Id : AUG-025  On 31/10/2011
                    ds = objWF.GetDataSet("FETCH_TOPCUSTOMER", "FETCH_VSLVOY_BBDETAILS");
                    if (string.IsNullOrEmpty(strInd))
                    {
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
                    }
                    //If strInd = "" Then
                    //    TotalRecords = ds.Tables(0).Rows.Count
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
                    //End If

                    return ds;
                    //'------End
                }
                else
                {
                    objWF.MyCommand.Parameters.Clear();
                    var _with2 = objWF.MyCommand.Parameters;
                    _with2.Add("Vessel", getDefault(Vessel, "")).Direction = ParameterDirection.Input;
                    _with2.Add("Voyage12", getDefault(Voyage, "")).Direction = ParameterDirection.Input;
                    _with2.Add("CUSTOMER", getDefault(customer, "")).Direction = ParameterDirection.Input;
                    _with2.Add("VslVoy", getDefault(CargoType, "")).Direction = ParameterDirection.Input;
                    _with2.Add("LocId", getDefault(LocId, "")).Direction = ParameterDirection.Input;
                    //adding by thiyagarajan on 16/12/08 for location base currency task
                    _with2.Add("CURRENCY_IN", HttpContext.Current.Session["currency_mst_pk"]).Direction = ParameterDirection.Input;
                    //'------adding by Thirumoorthy on 02/11/11 for PTS ID : AUG-025
                    _with2.Add("OPERATOR_MST_PK_IN", getDefault(ShippingLine, "")).Direction = ParameterDirection.Input;
                    _with2.Add("PORT_MST_POL_PK_IN", getDefault(POL, "")).Direction = ParameterDirection.Input;
                    _with2.Add("PORT_MST_POD_PK_IN", getDefault(POD, "")).Direction = ParameterDirection.Input;
                    _with2.Add("COMMODITY_GROUP_PK_IN", getDefault(CommdtyGrp, "")).Direction = ParameterDirection.Input;
                    //'------End
                    _with2.Add("VesVoy_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                    _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                    _with2.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.InputOutput;

                    //'---------Added by Thirumoorthy  PTS Id : AUG-025  On 31/10/2011
                    ds = objWF.GetDataSet("FETCH_TOPCUSTOMER", "FETCH_VSLVOY");
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

                    return ds;
                    //'------End
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "GetCollectionData"

        #region "GetQuotationData"

        /// <summary>
        /// Fetches the quotation.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="customer">The customer.</param>
        /// <param name="polfk">The polfk.</param>
        /// <param name="podfk">The podfk.</param>
        /// <param name="commodityfk">The commodityfk.</param>
        /// <param name="cargo">The cargo.</param>
        /// <param name="Sector">The sector.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet FetchQuotation(int BizType = 0, string fromDate = "", string toDate = "", int customer = 0, int polfk = 0, int podfk = 0, int commodityfk = 0, string cargo = "", string Sector = "", Int32 flag = 0,
        Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            Int32 last = 0;
            Int32 start = 0;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSQL = new StringBuilder();
            StringBuilder strSQL1 = new StringBuilder();
            StringBuilder strCondition = new StringBuilder();
            string condition = null;
            Int32 intLoc = default(Int32);
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            try
            {
                if (flag == 0)
                {
                    strCondition.Append(" AND 1=2  ");
                }
                intLoc = (Int32)HttpContext.Current.Session["LOGED_IN_LOC_FK"];
                if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate))))
                {
                    strCondition.Append(" AND q.quotation_date BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat) ");
                }
                else if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
                {
                    strCondition.Append(" AND q.quotation_date >= TO_DATE('" + fromDate + "',dateformat) ");
                }
                else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
                {
                    strCondition.Append(" AND q.quotation_date >= TO_DATE('" + toDate + "',dateformat) ");
                }
                if (polfk != 0)
                {
                    strCondition.Append(" AND qtrn.port_mst_pol_fk = " + polfk);
                }
                if (podfk != 0)
                {
                    strCondition.Append(" AND qtrn.port_mst_pod_fk = " + podfk);
                }
                if (commodityfk != 0)
                {
                    if (BizType == 2)
                    {
                        strCondition.Append(" AND qtrn.commodity_group_fk = " + commodityfk);
                    }
                    else if (BizType == 1)
                    {
                        strCondition.Append(" AND q.commodity_group_mst_fk = " + commodityfk);
                    }
                }
                if (customer != 0)
                {
                    strCondition.Append(" AND q.customer_mst_fk = " + customer);
                }
                if (!string.IsNullOrEmpty(Sector))
                {
                    strCondition.Append(Sector);
                }
                strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + intLoc);
                strCondition.Append(" AND q.CREATED_BY_FK = UMT.USER_MST_PK ");

                condition = strCondition.ToString();

                // SEA
                if (BizType == 2)
                {
                    strSQL.Append(" select distinct CUST_SHIP.CUSTOMER_NAME, ");
                    strSQL.Append("  Q.QUOTATION_MST_PK QUOTPK,  ");
                    strSQL.Append("  Q.QUOTATION_REF_NO,  ");
                    strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" BKG.BOOKING_REF_NO ");
                    strSQL.Append(" END BOOKING_REF_NO, ");
                    strSQL.Append(" CUST_SHIP.CUSTOMER_NAME SHIPPER_NAME, ");
                    strSQL.Append(" CUST_CONS.CUSTOMER_NAME CONSIGNEE_NAME, ");
                    strSQL.Append(" POL.PORT_NAME POL, ");
                    strSQL.Append(" POD.PORT_NAME POD, ");
                    strSQL.Append(" OPR.OPERATOR_NAME AIRLINE_NAME, ");
                    strSQL.Append(" 'SEA' BIZTYPE, ");
                    strSQL.Append(" DECODE(Q.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE, ");
                    strSQL.Append(" COM.COMMODITY_GROUP_DESC, ");
                    //strSQL.Append(vbCrLf & " DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled') AS BKG_STATUS, ")
                    strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled' ) ");
                    strSQL.Append(" END BKG_STATUS, ");
                    strSQL.Append(" DECODE(Q.STATUS, 1, 'Active', 2, 'Confirm', 4, 'Used') AS STATUS, ");
                    //'Added By George
                    strSQL.Append(" Q.QUOTATION_DATE, ");
                    strSQL.Append(" 0 QUOTATION_TYPE ");
                    //'
                    strSQL.Append(" from QUOTATION_MST_TBL  q, ");
                    strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                    strSQL.Append(" PORT_MST_TBL            pOL, ");
                    strSQL.Append("  PORT_MST_TBL            POD, ");
                    strSQL.Append(" BOOKING_TRN         btrn, ");
                    strSQL.Append(" booking_mst_tbl         bkg, ");
                    strSQL.Append(" customer_mst_tbl        cust_ship, ");
                    strSQL.Append(" customer_mst_tbl        cust_cons, ");
                    strSQL.Append("       USER_MST_TBL UMT,");
                    strSQL.Append(" commodity_group_mst_tbl com,operator_mst_tbl opr ");

                    strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                    strSQL.Append("    AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                    strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                    strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk(+) ");
                    strSQL.Append(" and btrn.trans_ref_no(+) = q.quotation_ref_no ");
                    strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                    strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+) ");
                    strSQL.Append(" and qtrn.commodity_group_fk = com.commodity_group_pk(+) AND opr.operator_mst_pk(+)=qtrn.CARRIER_MST_FK");

                    if (cargo == "FCL")
                    {
                        strSQL.Append(" and q.cargo_type=1 ");
                    }
                    else if (cargo == "LCL")
                    {
                        strSQL.Append(" and q.cargo_type=2 ");
                    }
                    else if (cargo == "BBC")
                    {
                        strSQL.Append(" and q.cargo_type=4 ");
                    }
                    strSQL.Append(" and q.status <> 3 AND Q.BIZ_TYPE=2 ");
                    strSQL.Append("               " + condition);

                    strSQL.Append(" union ");

                    strSQL.Append(" select distinct CUST_SHIP.CUSTOMER_NAME, ");
                    strSQL.Append("  Q.QUOTATION_MST_PK,  ");
                    strSQL.Append("  Q.QUOTATION_REF_NO,  ");
                    strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" BKG.BOOKING_REF_NO ");
                    strSQL.Append(" END BOOKING_REF_NO, ");
                    strSQL.Append(" CUST_SHIP.CUSTOMER_NAME SHIPPER_NAME, ");
                    strSQL.Append(" CUST_CONS.CUSTOMER_NAME CONSIGNEE_NAME, ");
                    strSQL.Append(" POL.PORT_NAME POL, ");
                    strSQL.Append(" POD.PORT_NAME POD, ");
                    strSQL.Append(" OPR.OPERATOR_NAME AIRLINE_NAME, ");
                    strSQL.Append(" 'SEA' BIZTYPE, ");
                    strSQL.Append(" DECODE(Q.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE, ");
                    strSQL.Append(" COM.COMMODITY_GROUP_DESC, ");
                    strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled') ");
                    strSQL.Append(" END BKG_STATUS, ");
                    strSQL.Append(" DECODE(Q.STATUS, 1, 'Active', 2, 'Confirm', 4, 'Used') AS STATUS, ");
                    strSQL.Append(" Q.QUOTATION_DATE, ");
                    strSQL.Append(" 0 QUOTATION_TYPE ");
                    //'
                    strSQL.Append("   from QUOTATION_MST_TBL       q, ");
                    strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                    strSQL.Append(" PORT_MST_TBL            pOL, ");
                    strSQL.Append("  PORT_MST_TBL            POD, ");
                    strSQL.Append("       USER_MST_TBL UMT,");
                    strSQL.Append(" customer_mst_tbl        cust_ship, ");
                    strSQL.Append(" BOOKING_TRN   btrn,");
                    strSQL.Append(" booking_mst_tbl           bkg,");
                    strSQL.Append(" customer_mst_tbl          cust_cons,");
                    strSQL.Append(" commodity_group_mst_tbl com,operator_mst_tbl opr ");
                    strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                    strSQL.Append("    AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                    strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                    strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                    strSQL.Append(" and qtrn.commodity_group_fk = com.commodity_group_pk(+) ");
                    strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk(+)");
                    strSQL.Append(" and btrn.trans_ref_no(+) = q.quotation_ref_no");
                    strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+) AND opr.operator_mst_pk(+)=qtrn.CARRIER_MST_FK");
                    if (cargo == "FCL")
                    {
                        strSQL.Append(" and q.cargo_type=1 ");
                    }
                    else if (cargo == "LCL")
                    {
                        strSQL.Append(" and q.cargo_type=2 ");
                    }
                    else if (cargo == "BBC")
                    {
                        strSQL.Append(" and q.cargo_type=4 ");
                    }
                    strSQL.Append(" and q.status <> 3 AND Q.BIZ_TYPE=2  ");
                    strSQL.Append("               " + condition);
                    strSQL.Append(" ORDER BY QUOTATION_DATE DESC");

                    //AIR
                }
                else if (BizType == 1)
                {
                    strSQL.Append(" select distinct CUST_SHIP.CUSTOMER_NAME, ");
                    strSQL.Append("  Q.QUOTATION_MST_PK QUOTPK, ");
                    strSQL.Append(" Q.QUOTATION_REF_NO, ");
                    strSQL.Append(" 'NA' AS BOOKING_REF_NO, ");
                    strSQL.Append(" CUST_SHIP.CUSTOMER_NAME SHIPPER_NAME, ");
                    strSQL.Append("  '' CONSIGNEE_NAME, ");
                    strSQL.Append(" POL.PORT_NAME POL, ");
                    strSQL.Append(" POD.PORT_NAME POD, ");
                    strSQL.Append(" AMT.AIRLINE_NAME, ");
                    strSQL.Append(" 'AIR' BIZTYPE, ");
                    if ((cargo == "KGS"))
                    {
                        strSQL.Append(" 'KGS' CARGO_TYPE, ");
                    }
                    else if ((cargo == "ULD"))
                    {
                        strSQL.Append(" 'ULD' CARGO_TYPE, ");
                    }
                    //strSQL.Append(vbCrLf & " DECODE(SLAB.BASIS, 1, 'KGS', 2, 'ULD') CARGO_TYPE, ")
                    strSQL.Append(" COM.COMMODITY_GROUP_DESC, ");
                    strSQL.Append(" 'NA' BKG_STATUS, ");
                    strSQL.Append(" DECODE(Q.STATUS, 1, 'Active', 2, 'Confirm', 4, 'Used') AS STATUS, ");
                    //'Added By George
                    strSQL.Append(" Q.QUOTATION_DATE, ");
                    strSQL.Append(" Q.QUOTATION_TYPE ");
                    //'
                    strSQL.Append(" from QUOTATION_MST_TBL       q, ");
                    strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                    strSQL.Append(" PORT_MST_TBL            pOL, ");
                    strSQL.Append(" PORT_MST_TBL            POD, ");
                    strSQL.Append(" airline_mst_tbl         amt, ");
                    strSQL.Append(" customer_mst_tbl        cust_ship, ");
                    strSQL.Append(" USER_MST_TBL UMT,");
                    strSQL.Append(" commodity_group_mst_tbl com, ");
                    strSQL.Append(" airfreight_slabs_tbl   slab ");
                    strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                    strSQL.Append(" and qtrn.slab_fk = slab.airfreight_slabs_tbl_pk(+)  ");
                    strSQL.Append(" AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                    strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                    strSQL.Append(" and qtrn.CARRIER_MST_FK = amt.airline_mst_pk(+) ");
                    strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                    strSQL.Append(" and q.commodity_group_mst_fk = com.commodity_group_pk(+) ");
                    strSQL.Append(" and q.status <> 3 AND Q.BIZ_TYPE=1 ");
                    strSQL.Append("               " + condition);
                    if ((cargo == "KGS"))
                    {
                        strSQL.Append(" and q.QUOTATION_TYPE=0 ");
                    }
                    else if ((cargo == "ULD"))
                    {
                        strSQL.Append(" and q.QUOTATION_TYPE=1");
                    }
                    strSQL.Append(" union ");
                    strSQL.Append(" select distinct CUST_SHIP.CUSTOMER_NAME, ");
                    strSQL.Append("  Q.QUOTATION_MST_PK QUOTPK,  ");
                    strSQL.Append(" Q.QUOTATION_REF_NO, ");
                    strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" BKG.BOOKING_REF_NO ");
                    strSQL.Append(" END BOOKING_REF_NO, ");
                    strSQL.Append(" CUST_SHIP.CUSTOMER_NAME SHIPPER_NAME, ");
                    strSQL.Append(" CUST_CONS.CUSTOMER_NAME CONSIGNEE_NAME, ");
                    strSQL.Append(" POL.PORT_NAME POL, ");
                    strSQL.Append(" POD.PORT_NAME POD, ");
                    strSQL.Append(" AMT.AIRLINE_NAME, ");
                    strSQL.Append(" 'AIR' BIZTYPE, ");
                    if ((cargo == "KGS"))
                    {
                        strSQL.Append(" 'KGS' CARGO_TYPE, ");
                    }
                    else if ((cargo == "ULD"))
                    {
                        strSQL.Append(" 'ULD' CARGO_TYPE, ");
                    }
                    strSQL.Append(" COM.COMMODITY_GROUP_DESC, ");
                    strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled', 6, 'Confirm') ");
                    strSQL.Append(" END BKG_STATUS, ");
                    strSQL.Append(" DECODE(Q.STATUS, 1, 'Active', 2, 'Confirm', 4, 'Used') AS STATUS, ");
                    //'Added By George
                    strSQL.Append(" Q.QUOTATION_DATE, ");
                    strSQL.Append(" Q.QUOTATION_TYPE ");
                    //'
                    strSQL.Append(" from QUOTATION_MST_TBL       q, ");
                    strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                    strSQL.Append(" PORT_MST_TBL            POL, ");
                    strSQL.Append(" PORT_MST_TBL            POD, ");
                    strSQL.Append(" airline_mst_tbl         amt, ");
                    strSQL.Append(" BOOKING_TRN         btrn, ");
                    strSQL.Append(" booking_mst_tbl         bkg, ");
                    strSQL.Append(" customer_mst_tbl        cust_ship, ");
                    strSQL.Append(" customer_mst_tbl        cust_cons, ");
                    strSQL.Append("       USER_MST_TBL UMT,");
                    strSQL.Append(" commodity_group_mst_tbl com, ");
                    strSQL.Append(" airfreight_slabs_tbl   slab ");
                    strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                    strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk ");
                    strSQL.Append(" and qtrn.slab_fk = slab.airfreight_slabs_tbl_pk ");
                    strSQL.Append(" AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                    strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                    strSQL.Append(" and qtrn.CARRIER_MST_FK = amt.airline_mst_pk(+) ");
                    strSQL.Append("  and btrn.trans_ref_no(+) = q.quotation_ref_no ");
                    strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                    strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+) ");
                    strSQL.Append(" and q.commodity_group_mst_fk = com.commodity_group_pk(+) ");
                    strSQL.Append(" and q.status <> 3 AND Q.BIZ_TYPE=1 ");
                    strSQL.Append("               " + condition);
                    if ((cargo == "KGS"))
                    {
                        strSQL.Append(" and q.QUOTATION_TYPE=0 ");
                    }
                    else if ((cargo == "ULD"))
                    {
                        strSQL.Append(" and q.QUOTATION_TYPE=1");
                    }

                    strSQL.Append(" union ");
                    strSQL.Append(" select distinct CUST_SHIP.CUSTOMER_NAME, ");
                    strSQL.Append("  Q.QUOTATION_MST_PK QUOTPK,  ");
                    strSQL.Append(" Q.QUOTATION_REF_NO, ");
                    strSQL.Append(" 'NA' AS BOOKING_REF_NO, ");
                    strSQL.Append(" CUST_SHIP.CUSTOMER_NAME SHIPPER_NAME, ");
                    strSQL.Append("  '' CONSIGNEE_NAME, ");
                    strSQL.Append(" POL.PORT_NAME POL, ");
                    strSQL.Append(" POD.PORT_NAME POD, ");
                    strSQL.Append(" AMT.AIRLINE_NAME, ");
                    strSQL.Append(" 'AIR' BIZTYPE, ");
                    if ((cargo == "KGS"))
                    {
                        strSQL.Append(" 'KGS' CARGO_TYPE, ");
                    }
                    else if ((cargo == "ULD"))
                    {
                        strSQL.Append(" 'ULD' CARGO_TYPE, ");
                    }
                    strSQL.Append(" COM.COMMODITY_GROUP_DESC, ");
                    strSQL.Append(" 'NA' BKG_STATUS, ");
                    strSQL.Append(" DECODE(Q.STATUS, 1, 'Active', 2, 'Confirm', 4, 'Used') AS STATUS, ");
                    //'Added By George
                    strSQL.Append(" Q.QUOTATION_DATE, ");
                    strSQL.Append(" Q.QUOTATION_TYPE ");
                    //'
                    strSQL.Append(" FROM QUOTATION_MST_TBL       Q, ");
                    strSQL.Append(" QUOTATION_DTL_TBL    QTRN, ");
                    strSQL.Append(" PORT_MST_TBL            POL, ");
                    strSQL.Append(" PORT_MST_TBL            POD, ");
                    strSQL.Append(" AIRLINE_MST_TBL         AMT, ");
                    strSQL.Append(" CUSTOMER_MST_TBL        CUST_SHIP, ");
                    strSQL.Append("       USER_MST_TBL UMT,");
                    strSQL.Append(" COMMODITY_GROUP_MST_TBL COM ");
                    strSQL.Append("  WHERE (1 = 1) ");
                    strSQL.Append(" AND Q.QUOTATION_MST_PK = QTRN.quotation_mst_Fk ");
                    strSQL.Append(" AND POL.PORT_MST_PK(+) = QTRN.PORT_MST_POL_FK ");
                    strSQL.Append(" AND POD.PORT_MST_PK(+) = QTRN.PORT_MST_POD_FK ");
                    strSQL.Append(" AND QTRN.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+) ");
                    strSQL.Append(" AND Q.CUSTOMER_MST_FK = CUST_SHIP.CUSTOMER_MST_PK(+) ");
                    strSQL.Append(" AND Q.COMMODITY_GROUP_MST_FK = COM.COMMODITY_GROUP_PK(+) ");
                    strSQL.Append(" AND Q.STATUS <> 3 AND Q.BIZ_TYPE=1 ");
                    strSQL.Append("               " + condition);
                    if ((cargo == "KGS"))
                    {
                        strSQL.Append(" and q.QUOTATION_TYPE=0 ");
                    }
                    else if ((cargo == "ULD"))
                    {
                        strSQL.Append(" and q.QUOTATION_TYPE=1");
                    }
                    //'ADDED BY GEORGE
                    strSQL.Append(" ORDER BY QUOTATION_DATE DESC");
                    //'
                    //Sea & Air
                }
                else if (BizType == 0)
                {
                    strSQL.Append(" select distinct CUST_SHIP.CUSTOMER_NAME, ");
                    strSQL.Append("  Q.QUOTATION_MST_PK QUOTPK,  ");
                    strSQL.Append("  Q.QUOTATION_REF_NO,  ");
                    strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" BKG.BOOKING_REF_NO ");
                    strSQL.Append(" END BOOKING_REF_NO, ");
                    strSQL.Append(" CUST_SHIP.CUSTOMER_NAME SHIPPER_NAME, ");
                    strSQL.Append(" CUST_CONS.CUSTOMER_NAME CONSIGNEE_NAME, ");
                    strSQL.Append(" POL.PORT_NAME POL, ");
                    strSQL.Append(" POD.PORT_NAME POD, ");
                    strSQL.Append(" OPR.OPERATOR_NAME AIRLINE_NAME, ");
                    strSQL.Append(" 'SEA' BIZTYPE, ");
                    strSQL.Append(" DECODE(Q.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE, ");
                    strSQL.Append(" COM.COMMODITY_GROUP_DESC, ");
                    strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled') ");
                    strSQL.Append(" END BKG_STATUS, ");
                    strSQL.Append(" DECODE(Q.STATUS, 1, 'Active', 2, 'Confirm', 4, 'Used') AS STATUS, ");
                    //'Added By George
                    strSQL.Append(" Q.QUOTATION_DATE, ");
                    strSQL.Append(" 0 QUOTATION_TYPE ");
                    //'
                    strSQL.Append("   from QUOTATION_MST_TBL       q, ");
                    strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                    strSQL.Append(" PORT_MST_TBL            pOL, ");
                    strSQL.Append("  PORT_MST_TBL            POD, ");
                    strSQL.Append(" BOOKING_TRN         btrn, ");
                    strSQL.Append(" booking_mst_tbl         bkg, ");
                    strSQL.Append(" customer_mst_tbl        cust_ship, ");
                    strSQL.Append(" customer_mst_tbl        cust_cons, ");
                    strSQL.Append("       USER_MST_TBL UMT,");
                    strSQL.Append(" commodity_group_mst_tbl com,operator_mst_tbl opr ");
                    strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                    strSQL.Append("    AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                    strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                    strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk(+) ");
                    strSQL.Append(" and btrn.trans_ref_no(+) = q.quotation_ref_no ");
                    strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                    strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+) ");
                    strSQL.Append(" and qtrn.commodity_group_fk = com.commodity_group_pk(+) AND opr.operator_mst_pk(+)=qtrn.CARRIER_MST_FK");

                    if (cargo == "FCL")
                    {
                        strSQL.Append(" and q.cargo_type=1 ");
                    }
                    else if (cargo == "LCL")
                    {
                        strSQL.Append(" and q.cargo_type=2 ");
                    }
                    else if (cargo == "BBC")
                    {
                        strSQL.Append(" and q.cargo_type=4 ");
                    }
                    strSQL.Append(" and q.status <> 3 AND Q.BIZ_TYPE=2 ");
                    strSQL.Append("               " + condition);
                    if (commodityfk != 0)
                    {
                        strSQL.Append(" AND qtrn.commodity_group_fk = " + commodityfk);
                    }
                    strSQL.Append(" union ");

                    strSQL.Append(" select distinct CUST_SHIP.CUSTOMER_NAME, ");
                    strSQL.Append("  Q.QUOTATION_MST_PK QUOTPK,  ");
                    strSQL.Append("  Q.QUOTATION_REF_NO,  ");
                    strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" BKG.BOOKING_REF_NO ");
                    strSQL.Append(" END BOOKING_REF_NO, ");
                    strSQL.Append(" CUST_SHIP.CUSTOMER_NAME SHIPPER_NAME, ");
                    strSQL.Append(" CUST_CONS.CUSTOMER_NAME CONSIGNEE_NAME, ");
                    strSQL.Append(" POL.PORT_NAME POL, ");
                    strSQL.Append(" POD.PORT_NAME POD, ");
                    strSQL.Append(" OPR.OPERATOR_NAME AIRLINE_NAME, ");
                    strSQL.Append(" 'SEA' BIZTYPE, ");
                    strSQL.Append(" DECODE(Q.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE, ");
                    strSQL.Append(" COM.COMMODITY_GROUP_DESC, ");
                    strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled') ");
                    strSQL.Append(" END BKG_STATUS, ");
                    strSQL.Append(" DECODE(Q.STATUS, 1, 'Active', 2, 'Confirm', 4, 'Used') AS STATUS, ");
                    //'Added By George
                    strSQL.Append(" Q.QUOTATION_DATE, ");
                    strSQL.Append(" 0 QUOTATION_TYPE ");
                    //'
                    strSQL.Append("   from QUOTATION_MST_TBL       q, ");
                    strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                    strSQL.Append(" PORT_MST_TBL            pOL, ");
                    strSQL.Append("  PORT_MST_TBL            POD, ");
                    strSQL.Append("       USER_MST_TBL UMT,");
                    strSQL.Append(" customer_mst_tbl        cust_ship, ");
                    strSQL.Append(" BOOKING_TRN   btrn,");
                    strSQL.Append(" booking_mst_tbl           bkg,");
                    strSQL.Append(" customer_mst_tbl          cust_cons,");
                    strSQL.Append(" commodity_group_mst_tbl com,operator_mst_tbl opr ");
                    strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                    strSQL.Append("    AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                    strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                    strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                    strSQL.Append(" and qtrn.commodity_group_fk = com.commodity_group_pk(+) ");
                    strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk(+)");
                    strSQL.Append(" and btrn.trans_ref_no(+) = q.quotation_ref_no");
                    strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+) AND opr.operator_mst_pk(+)=qtrn.CARRIER_MST_FK");
                    if (cargo == "FCL")
                    {
                        strSQL.Append(" and q.cargo_type=1 ");
                    }
                    else if (cargo == "LCL")
                    {
                        strSQL.Append(" and q.cargo_type=2 ");
                    }
                    else if (cargo == "BBC")
                    {
                        strSQL.Append(" and q.cargo_type=4 ");
                    }
                    strSQL.Append(" and q.status <> 3 AND Q.BIZ_TYPE=2 ");
                    strSQL.Append("               " + condition);
                    if (commodityfk != 0)
                    {
                        strSQL.Append(" AND qtrn.commodity_group_fk = " + commodityfk);
                    }
                    strSQL.Append(" UNION ");
                    //'
                    strSQL.Append(" select distinct CUST_SHIP.CUSTOMER_NAME, ");
                    strSQL.Append("  Q.QUOTATION_MST_PK QUOTPK,  ");
                    strSQL.Append(" Q.QUOTATION_REF_NO, ");
                    strSQL.Append(" 'NA' AS BOOKING_REF_NO, ");
                    strSQL.Append(" CUST_SHIP.CUSTOMER_NAME SHIPPER_NAME, ");
                    strSQL.Append("  '' CONSIGNEE_NAME, ");
                    strSQL.Append(" POL.PORT_NAME POL, ");
                    strSQL.Append(" POD.PORT_NAME POD, ");
                    strSQL.Append(" AMT.AIRLINE_NAME, ");
                    strSQL.Append(" 'AIR' BIZTYPE, ");
                    strSQL.Append(" DECODE(SLAB.BASIS, 1, 'KGS', 2, 'ULD') CARGO_TYPE, ");
                    strSQL.Append(" COM.COMMODITY_GROUP_DESC, ");
                    strSQL.Append(" 'NA' BKG_STATUS, ");
                    strSQL.Append(" DECODE(Q.STATUS, 1, 'Active', 2, 'Confirm', 4, 'Used') AS STATUS, ");
                    //'Added By George
                    strSQL.Append(" Q.QUOTATION_DATE, ");
                    strSQL.Append(" Q.QUOTATION_TYPE ");
                    //'
                    strSQL.Append(" from QUOTATION_MST_TBL       q, ");
                    strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                    strSQL.Append(" PORT_MST_TBL            pOL, ");
                    strSQL.Append(" PORT_MST_TBL            POD, ");
                    strSQL.Append(" airline_mst_tbl         amt, ");
                    strSQL.Append(" customer_mst_tbl        cust_ship, ");
                    strSQL.Append("       USER_MST_TBL UMT,");
                    strSQL.Append(" commodity_group_mst_tbl com, ");
                    strSQL.Append(" airfreight_slabs_tbl   slab ");
                    strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                    strSQL.Append(" and qtrn.slab_fk = slab.airfreight_slabs_tbl_pk(+)  ");
                    strSQL.Append(" AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                    strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                    strSQL.Append(" and qtrn.CARRIER_MST_FK = amt.airline_mst_pk(+) ");
                    strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                    strSQL.Append(" and q.commodity_group_mst_fk = com.commodity_group_pk(+) ");
                    strSQL.Append(" and q.status <> 3 AND Q.BIZ_TYPE=1 ");
                    strSQL.Append("               " + condition);
                    if (commodityfk != 0)
                    {
                        strSQL.Append(" AND q.commodity_group_mst_fk = " + commodityfk);
                    }
                    if ((cargo == "KGS"))
                    {
                        strSQL.Append(" and slab.basis=1 ");
                    }
                    else if ((cargo == "ULD"))
                    {
                        strSQL.Append(" and slab.basis=2 ");
                    }
                    strSQL.Append(" union ");
                    strSQL.Append(" select distinct CUST_SHIP.CUSTOMER_NAME, ");
                    strSQL.Append("  Q.QUOTATION_MST_PK QUOTPK,  ");
                    strSQL.Append(" Q.QUOTATION_REF_NO, ");
                    strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" BKG.BOOKING_REF_NO ");
                    strSQL.Append(" END BOOKING_REF_NO, ");
                    strSQL.Append(" CUST_SHIP.CUSTOMER_NAME SHIPPER_NAME, ");
                    strSQL.Append(" CUST_CONS.CUSTOMER_NAME CONSIGNEE_NAME, ");
                    strSQL.Append(" POL.PORT_NAME POL, ");
                    strSQL.Append(" POD.PORT_NAME POD, ");
                    strSQL.Append(" AMT.AIRLINE_NAME, ");
                    strSQL.Append(" 'AIR' BIZTYPE, ");
                    strSQL.Append(" DECODE(SLAB.BASIS, 1, 'KGS', 2, 'ULD') CARGO_TYPE, ");
                    strSQL.Append(" COM.COMMODITY_GROUP_DESC, ");
                    strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                    strSQL.Append(" 'NA' ELSE ");
                    strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled', 6, 'Confirm') ");
                    strSQL.Append(" END BKG_STATUS, ");
                    strSQL.Append(" DECODE(Q.STATUS, 1, 'Active', 2, 'Confirm', 4, 'Used') AS STATUS, ");
                    //'Added By George
                    strSQL.Append(" Q.QUOTATION_DATE, ");
                    strSQL.Append(" Q.QUOTATION_TYPE ");
                    //'
                    strSQL.Append(" from QUOTATION_MST_TBL       q, ");
                    strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                    strSQL.Append(" PORT_MST_TBL            pOL, ");
                    strSQL.Append(" PORT_MST_TBL            POD, ");
                    strSQL.Append(" airline_mst_tbl         amt, ");
                    strSQL.Append(" BOOKING_TRN         btrn, ");
                    strSQL.Append(" booking_mst_tbl         bkg, ");
                    strSQL.Append(" customer_mst_tbl        cust_ship, ");
                    strSQL.Append(" customer_mst_tbl        cust_cons, ");
                    strSQL.Append("       USER_MST_TBL UMT,");
                    strSQL.Append(" commodity_group_mst_tbl com, ");
                    strSQL.Append(" airfreight_slabs_tbl   slab ");
                    strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                    strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk ");
                    strSQL.Append(" and qtrn.slab_fk = slab.airfreight_slabs_tbl_pk ");
                    strSQL.Append(" AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                    strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                    strSQL.Append(" and qtrn.CARRIER_MST_FK = amt.airline_mst_pk(+) ");
                    strSQL.Append("  and btrn.trans_ref_no(+) = q.quotation_ref_no ");
                    strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                    strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+) ");
                    strSQL.Append(" and q.commodity_group_mst_fk = com.commodity_group_pk(+) ");
                    strSQL.Append(" and q.status <> 3 AND Q.BIZ_TYPE=1 ");
                    strSQL.Append("               " + condition);
                    if (commodityfk != 0)
                    {
                        strSQL.Append(" AND q.commodity_group_mst_fk = " + commodityfk);
                    }
                    if ((cargo == "KGS"))
                    {
                        strSQL.Append(" and slab.basis=1 ");
                    }
                    else if ((cargo == "ULD"))
                    {
                        strSQL.Append(" and slab.basis=2 ");
                    }
                    strSQL.Append(" union ");
                    strSQL.Append(" select distinct CUST_SHIP.CUSTOMER_NAME, ");
                    strSQL.Append("  Q.QUOTATION_MST_PK QUOTPK,  ");
                    strSQL.Append(" Q.QUOTATION_REF_NO, ");
                    strSQL.Append(" 'NA' AS BOOKING_REF_NO, ");
                    strSQL.Append(" CUST_SHIP.CUSTOMER_NAME SHIPPER_NAME, ");
                    strSQL.Append("  '' CONSIGNEE_NAME, ");
                    strSQL.Append(" POL.PORT_NAME POL, ");
                    strSQL.Append(" POD.PORT_NAME POD, ");
                    strSQL.Append(" AMT.AIRLINE_NAME, ");
                    strSQL.Append(" 'AIR' BIZTYPE, ");
                    strSQL.Append(" '' CARGO_TYPE, ");
                    strSQL.Append(" COM.COMMODITY_GROUP_DESC, ");
                    strSQL.Append(" 'NA' BKG_STATUS, ");
                    strSQL.Append(" DECODE(Q.STATUS, 1, 'Active', 2, 'Confirm', 4, 'Used') AS STATUS, ");
                    //'Added By George
                    strSQL.Append(" Q.QUOTATION_DATE, ");
                    strSQL.Append(" Q.QUOTATION_TYPE ");
                    //'
                    strSQL.Append(" from QUOTATION_MST_TBL       q, ");
                    strSQL.Append(" QUOTATION_DTL_TBL    qtrn, ");
                    strSQL.Append(" PORT_MST_TBL            pOL, ");
                    strSQL.Append(" PORT_MST_TBL            POD, ");
                    strSQL.Append(" airline_mst_tbl         amt, ");
                    strSQL.Append(" customer_mst_tbl        cust_ship, ");
                    strSQL.Append(" USER_MST_TBL UMT,");
                    strSQL.Append(" commodity_group_mst_tbl com ");
                    strSQL.Append("  where (1 = 1) ");
                    strSQL.Append(" and q.QUOTATION_MST_PK = qtrn.quotation_mst_Fk ");
                    strSQL.Append(" AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                    strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                    strSQL.Append(" and qtrn.CARRIER_MST_FK = amt.airline_mst_pk(+) ");
                    strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                    strSQL.Append(" and q.commodity_group_mst_fk = com.commodity_group_pk(+) ");
                    strSQL.Append(" and q.status <> 3 AND Q.BIZ_TYPE=1 ");
                    strSQL.Append("               " + condition);
                    if (commodityfk != 0)
                    {
                        strSQL.Append(" AND q.commodity_group_mst_fk = " + commodityfk);
                    }
                    ///'Added By George
                    strSQL.Append(" ORDER BY QUOTATION_DATE DESC");
                    //'
                }
                System.Text.StringBuilder strCount = new System.Text.StringBuilder();
                System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
                System.Text.StringBuilder strSQL3 = new System.Text.StringBuilder();
                strCount.Append(" SELECT COUNT(*)  from  ");
                strCount.Append(("(" + strSQL.ToString() + ""));
                strCount.Append(" )");
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
                strCount.Remove(0, strCount.Length);

                sqlstr2.Append(" Select * from (");
                sqlstr2.Append(" SELECT ROWNUM SL_NO, q.*  FROM ( ");
                sqlstr2.Append("  (" + strSQL.ToString() + " ");
                sqlstr2.Append(" ) q )) ");
                sqlstr2.Append("   WHERE \"SL_NO\"  BETWEEN " + start + " AND " + last + "");
                strSQL3 = sqlstr2;
                return objWF.GetDataSet(strSQL3.ToString());
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

        #endregion "GetQuotationData"

        #region " Commodity Group "

        /// <summary>
        /// Fetches the commodity GRP.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCommodityGrp()
        {
            string sql = null;

            sql += " select 0 COMMODITY_GROUP_PK,";
            sql += " ' ' COMMODITY_GROUP_CODE, ";
            sql += " ' ' COMMODITY_GROUP_DESC, ";
            sql += " 0 VERSION_NO from dual UNION ";
            sql += " SELECT CG.COMMODITY_GROUP_PK,CG.COMMODITY_GROUP_CODE, ";
            sql += " CG.COMMODITY_GROUP_DESC,CG.VERSION_NO ";
            sql += " FROM COMMODITY_GROUP_MST_TBL CG ";
            sql += " WHERE CG.ACTIVE_FLAG=1 ";
            sql += " ORDER BY COMMODITY_GROUP_CODE ";

            try
            {
                return (new WorkFlow()).GetDataSet(sql);
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

        #endregion " Commodity Group "

        #region "GetQuotation Report"

        /// <summary>
        /// Gets the quot report.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="customer">The customer.</param>
        /// <param name="polfk">The polfk.</param>
        /// <param name="podfk">The podfk.</param>
        /// <param name="commodityfk">The commodityfk.</param>
        /// <param name="cargo">The cargo.</param>
        /// <param name="Sector">The sector.</param>
        /// <returns></returns>
        public DataSet GetQuotReport(int BizType, string fromDate, string toDate, int customer = 0, int polfk = 0, int podfk = 0, int commodityfk = 0, string cargo = "", string Sector = "")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSQL = new StringBuilder();
            StringBuilder strCondition = new StringBuilder();
            string condition = null;
            Int32 intLoc = default(Int32);
            intLoc = (Int32)HttpContext.Current.Session["LOGED_IN_LOC_FK"];
            if (!((fromDate == null | string.IsNullOrEmpty(fromDate)) & (toDate == null | string.IsNullOrEmpty(toDate))))
            {
                strCondition.Append(" AND q.quotation_date BETWEEN TO_DATE('" + fromDate + "',dateformat)  AND TO_DATE('" + toDate + "',dateformat) ");
            }
            else if (!(fromDate == null | string.IsNullOrEmpty(fromDate)))
            {
                strCondition.Append(" AND q.quotation_date >= TO_DATE('" + fromDate + "',dateformat) ");
            }
            else if (!(toDate == null | string.IsNullOrEmpty(toDate)))
            {
                strCondition.Append(" AND q.quotation_date >= TO_DATE('" + toDate + "',dateformat) ");
            }
            if (polfk != 0)
            {
                strCondition.Append(" AND qtrn.port_mst_pol_fk = " + polfk);
            }
            if (podfk != 0)
            {
                strCondition.Append(" AND qtrn.port_mst_pod_fk = " + podfk);
            }
            if (commodityfk != 0)
            {
                if (BizType == 2)
                {
                    strCondition.Append(" AND qtrn.commodity_group_fk = " + commodityfk);
                }
                else if (BizType == 1)
                {
                    strCondition.Append(" AND q.commodity_group_mst_fk = " + commodityfk);
                }
            }
            if (customer != 0)
            {
                strCondition.Append(" AND q.customer_mst_fk = " + customer);
            }
            if (!string.IsNullOrEmpty(Sector))
            {
                strCondition.Append(Sector);
            }
            strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + intLoc);
            strCondition.Append(" AND q.CREATED_BY_FK = UMT.USER_MST_PK ");

            condition = strCondition.ToString();

            // SEA
            if (BizType == 2)
            {
                strSQL.Append(" select distinct q.quotation_ref_no AS QT_REF_NO, ");
                strSQL.Append("  q.quotation_date AS Q_DATE, ");
                strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" BKG.BOOKING_REF_NO ");
                strSQL.Append(" END BKG_REF_NO, ");
                //strSQL.Append(vbCrLf & " bkg.booking_ref_no AS BKG_REF_NO, ")
                strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled') ");
                strSQL.Append(" END BKG_STATUS, ");
                strSQL.Append(" decode(q.status, 1, 'Active', 2, 'Confirm', 4, 'Confirm') AS STATUS, ");
                strSQL.Append(" POL.PORT_NAME POL_ID, ");
                strSQL.Append(" POD.PORT_NAME POD_ID, ");
                strSQL.Append(" opr.operator_mst_pk airline_PK, ");
                strSQL.Append(" OPR.OPERATOR_NAME airline_ID, ");
                strSQL.Append(" OPR.OPERATOR_NAME airline_name, ");
                strSQL.Append(" q.customer_mst_fk shipper_PK, ");
                strSQL.Append(" cust_ship.customer_id shipper_ID, ");
                strSQL.Append(" cust_ship.customer_name shipper_name, ");
                strSQL.Append(" bkg.cons_customer_mst_fk consignee_PK, ");
                strSQL.Append(" cust_cons.customer_id consignee_ID, ");
                strSQL.Append("  cust_cons.customer_name consignee_name ,opr.operator_id");
                strSQL.Append("   from QUOTATION_MST_TBL       q, ");
                strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                strSQL.Append(" PORT_MST_TBL            pOL, ");
                strSQL.Append("  PORT_MST_TBL            POD, ");
                //strSQL.Append(vbCrLf & " airline_mst_tbl         amt, ")
                strSQL.Append(" BOOKING_TRN         btrn, ");
                strSQL.Append(" booking_mst_tbl         bkg, ");
                strSQL.Append(" customer_mst_tbl        cust_ship, ");
                strSQL.Append(" customer_mst_tbl        cust_cons, ");
                //strSQL.Append(vbCrLf & " Location_Mst_Tbl        loc, ")
                strSQL.Append("       USER_MST_TBL UMT,");
                strSQL.Append(" commodity_group_mst_tbl com,operator_mst_tbl opr ");
                strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                strSQL.Append("    AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                //strSQL.Append(vbCrLf & " and qtrn.operator_mst_fk = amt.airline_mst_pk(+) ")
                strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk(+) ");
                strSQL.Append(" and btrn.trans_ref_no(+) = q.quotation_ref_no ");
                strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+) ");
                strSQL.Append(" and qtrn.commodity_group_fk = com.commodity_group_pk(+)  AND OPR.OPERATOR_MST_PK = QTRN.CARRIER_MST_FK");

                if (cargo == "FCL")
                {
                    strSQL.Append(" and q.cargo_type=1 ");
                }
                else if (cargo == "LCL")
                {
                    strSQL.Append(" and q.cargo_type=2 ");
                }
                strSQL.Append(" and q.status <> 3 ");
                strSQL.Append("               " + condition);

                strSQL.Append(" union ");

                strSQL.Append(" select distinct q.quotation_ref_no AS QT_REF_NO, ");
                strSQL.Append("  q.quotation_date AS Q_DATE, ");
                strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" BKG.BOOKING_REF_NO ");
                strSQL.Append(" END BKG_REF_NO, ");
                strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled') ");
                strSQL.Append(" END BKG_STATUS, ");
                strSQL.Append(" decode(q.status, 1, 'Active', 2, 'Confirm', 4, 'Confirm') AS STATUS, ");
                strSQL.Append(" POL.PORT_NAME POL_ID, ");
                strSQL.Append(" POD.PORT_NAME POD_ID, ");
                strSQL.Append(" opr.operator_mst_pk airline_PK, ");
                strSQL.Append(" OPR.OPERATOR_NAME airline_ID, ");
                strSQL.Append(" OPR.OPERATOR_NAME airline_name, ");
                strSQL.Append(" q.customer_mst_fk shipper_PK, ");
                strSQL.Append(" cust_ship.customer_id shipper_ID, ");
                strSQL.Append(" cust_ship.customer_name shipper_name, ");
                strSQL.Append(" bkg.cons_customer_mst_fk consignee_PK,");
                strSQL.Append(" cust_cons.customer_id consignee_ID,");
                strSQL.Append(" cust_cons.customer_name consignee_name,opr.operator_id");
                strSQL.Append("   from QUOTATION_MST_TBL       q, ");
                strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                strSQL.Append(" PORT_MST_TBL            pOL, ");
                strSQL.Append("  PORT_MST_TBL            POD, ");
                strSQL.Append("       USER_MST_TBL UMT,");
                strSQL.Append(" customer_mst_tbl        cust_ship, ");
                strSQL.Append(" BOOKING_TRN   btrn,");
                strSQL.Append(" booking_mst_tbl           bkg,");
                strSQL.Append(" customer_mst_tbl          cust_cons,");
                strSQL.Append(" commodity_group_mst_tbl com,operator_mst_tbl opr ");
                strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                strSQL.Append("    AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                strSQL.Append(" and qtrn.commodity_group_fk = com.commodity_group_pk(+) ");
                strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk(+)");
                strSQL.Append(" and btrn.trans_ref_no(+) = q.quotation_ref_no");
                strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+) AND OPR.OPERATOR_MST_PK = QTRN.CARRIER_MST_FK");
                if (cargo == "FCL")
                {
                    strSQL.Append(" and q.cargo_type=1 ");
                }
                else if (cargo == "LCL")
                {
                    strSQL.Append(" and q.cargo_type=2 ");
                }
                strSQL.Append(" and q.status <> 3 ");
                strSQL.Append("               " + condition);

                //AIR
            }
            else if (BizType == 1)
            {
                strSQL.Append(" select distinct q.quotation_ref_no AS QT_REF_NO, ");
                strSQL.Append(" q.quotation_date AS Q_DATE, ");
                strSQL.Append(" 'NA' AS BKG_REF_NO, ");
                strSQL.Append(" 'NA' AS BKG_STATUS, ");
                strSQL.Append(" decode(q.status, 1, 'Active', 2, 'Confirm', 4, 'Confirm') AS STATUS, ");
                strSQL.Append(" POL.PORT_NAME POL_ID, ");
                strSQL.Append(" POD.PORT_NAME POD_ID, ");
                strSQL.Append(" amt.airline_mst_pk airline_PK, ");
                strSQL.Append(" AMT.airline_name airline_ID, ");
                strSQL.Append(" amt.airline_name airline_name, ");
                strSQL.Append(" q.customer_mst_fk shipper_PK, ");
                strSQL.Append(" cust_ship.customer_id shipper_ID, ");
                strSQL.Append(" cust_ship.customer_name shipper_name, ");
                strSQL.Append(" 0 consignee_PK, ");
                strSQL.Append(" '' consignee_ID, ");
                strSQL.Append(" '' consignee_name ");
                strSQL.Append(" from QUOTATION_MST_TBL       q, ");
                strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                strSQL.Append(" PORT_MST_TBL            pOL, ");
                strSQL.Append(" PORT_MST_TBL            POD, ");
                strSQL.Append(" airline_mst_tbl         amt, ");
                strSQL.Append(" customer_mst_tbl        cust_ship, ");
                strSQL.Append("       USER_MST_TBL UMT,");
                strSQL.Append(" commodity_group_mst_tbl com, ");
                strSQL.Append(" airfreight_slabs_tbl   slab ");
                strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                strSQL.Append(" and qtrn.slab_fk = slab.airfreight_slabs_tbl_pk(+)  ");
                strSQL.Append(" AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                strSQL.Append(" AND QTRN.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+) ");
                strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                strSQL.Append(" and q.commodity_group_mst_fk = com.commodity_group_pk(+) ");
                strSQL.Append(" and q.status <> 3 ");
                strSQL.Append("               " + condition);
                if ((cargo == "KGS"))
                {
                    strSQL.Append(" and slab.basis=1 ");
                }
                else if ((cargo == "ULD"))
                {
                    strSQL.Append(" and slab.basis=2 ");
                }
                strSQL.Append(" union ");
                strSQL.Append(" select distinct q.quotation_ref_no AS QT_REF_NO, ");
                strSQL.Append(" q.quotation_date AS Q_DATE, ");
                strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" BKG.BOOKING_REF_NO ");
                strSQL.Append(" END BKG_REF_NO, ");
                strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled', 6, 'Confirm') ");
                strSQL.Append(" END BKG_STATUS, ");
                strSQL.Append(" decode(q.status, 1, 'Active', 2, 'Confirm', 4, 'Confirm') AS STATUS, ");
                strSQL.Append(" POL.PORT_NAME POL_ID, ");
                strSQL.Append(" POD.PORT_NAME POD_ID, ");
                strSQL.Append(" amt.airline_mst_pk airline_PK, ");
                strSQL.Append(" AMT.airline_name airline_ID, ");
                strSQL.Append(" amt.airline_name airline_name, ");
                strSQL.Append(" q.customer_mst_fk shipper_PK, ");
                strSQL.Append(" cust_ship.customer_id shipper_ID, ");
                strSQL.Append(" cust_ship.customer_name shipper_name, ");
                strSQL.Append(" bkg.cons_customer_mst_fk consignee_PK, ");
                strSQL.Append(" cust_cons.customer_id consignee_ID, ");
                strSQL.Append(" cust_cons.customer_name consignee_name ");
                strSQL.Append(" from QUOTATION_MST_TBL       q, ");
                strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                strSQL.Append(" PORT_MST_TBL            pOL, ");
                strSQL.Append(" PORT_MST_TBL            POD, ");
                strSQL.Append(" airline_mst_tbl         amt, ");
                strSQL.Append(" BOOKING_TRN         btrn, ");
                strSQL.Append(" booking_mst_tbl         bkg, ");
                strSQL.Append(" customer_mst_tbl        cust_ship, ");
                strSQL.Append(" customer_mst_tbl        cust_cons, ");
                strSQL.Append("       USER_MST_TBL UMT,");
                strSQL.Append(" commodity_group_mst_tbl com, ");
                strSQL.Append(" airfreight_slabs_tbl   slab ");
                strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk ");
                strSQL.Append(" and qtrn.slab_fk = slab.airfreight_slabs_tbl_pk ");
                strSQL.Append(" AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                strSQL.Append("  AND QTRN.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+) ");
                strSQL.Append("  and btrn.trans_ref_no(+) = q.quotation_ref_no ");
                strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+) ");
                strSQL.Append(" and q.commodity_group_mst_fk = com.commodity_group_pk(+) ");
                strSQL.Append(" and q.status <> 3 ");
                strSQL.Append("               " + condition);
                if ((cargo == "KGS"))
                {
                    strSQL.Append(" and slab.basis=1 ");
                }
                else if ((cargo == "ULD"))
                {
                    strSQL.Append(" and slab.basis=2 ");
                }
                strSQL.Append(" union ");
                strSQL.Append(" select distinct q.quotation_ref_no AS QT_REF_NO, ");
                strSQL.Append(" q.quotation_date AS Q_DATE, ");
                strSQL.Append(" 'NA' as BKG_REF_NO, ");
                strSQL.Append(" 'NA' AS BKG_STATUS, ");
                strSQL.Append(" decode(q.status, 1, 'Active', 2, 'Confirm', 4, 'Confirm') AS STATUS, ");
                strSQL.Append(" POL.PORT_NAME POL_ID, ");
                strSQL.Append(" POD.PORT_NAME POD_ID, ");
                strSQL.Append(" amt.airline_mst_pk airline_PK, ");
                strSQL.Append(" AMT.airline_name airline_ID, ");
                strSQL.Append(" amt.airline_name airline_name, ");
                strSQL.Append(" q.customer_mst_fk shipper_PK, ");
                strSQL.Append(" cust_ship.customer_id shipper_ID, ");
                strSQL.Append(" cust_ship.customer_name shipper_name, ");
                strSQL.Append(" 0 as consignee_PK, ");
                strSQL.Append(" '' as consignee_ID, ");
                strSQL.Append(" '' as consignee_name ");
                strSQL.Append(" from QUOTATION_MST_TBL       q, ");
                strSQL.Append(" QUOTATION_DTL_TBL    qtrn, ");
                strSQL.Append(" PORT_MST_TBL            pOL, ");
                strSQL.Append(" PORT_MST_TBL            POD, ");
                strSQL.Append(" airline_mst_tbl         amt, ");
                strSQL.Append(" customer_mst_tbl        cust_ship, ");
                strSQL.Append("       USER_MST_TBL UMT,");
                strSQL.Append(" commodity_group_mst_tbl com ");
                strSQL.Append("  where (1 = 1) ");
                strSQL.Append(" and q.QUOTATION_MST_PK = qtrn.quotation_mst_Fk ");
                strSQL.Append(" AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                strSQL.Append(" and qtrn.CARRIER_MST_FK = amt.airline_mst_pk(+) ");
                strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                strSQL.Append(" and q.commodity_group_mst_fk = com.commodity_group_pk(+) ");
                strSQL.Append(" and q.status <> 3 ");
                strSQL.Append("               " + condition);
                //Sea & Air
            }
            else if (BizType == 0)
            {
                strSQL.Append(" select distinct q.quotation_ref_no AS QT_REF_NO, ");
                strSQL.Append("  q.quotation_date AS Q_DATE, ");
                strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" BKG.BOOKING_REF_NO ");
                strSQL.Append(" END BKG_REF_NO, ");
                strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled') ");
                strSQL.Append(" END BKG_STATUS, ");
                strSQL.Append(" decode(q.status, 1, 'Active', 2, 'Confirm', 4, 'Confirm') AS STATUS, ");
                strSQL.Append(" POL.PORT_NAME POL_ID, ");
                strSQL.Append(" POD.PORT_NAME POD_ID, ");
                strSQL.Append(" opr.operator_mst_pk airline_PK, ");
                strSQL.Append(" OPR.OPERATOR_NAME airline_ID, ");
                strSQL.Append(" OPR.OPERATOR_NAME airline_name, ");
                strSQL.Append(" q.customer_mst_fk shipper_PK, ");
                strSQL.Append(" cust_ship.customer_id shipper_ID, ");
                strSQL.Append(" cust_ship.customer_name shipper_name, ");
                strSQL.Append(" bkg.cons_customer_mst_fk consignee_PK, ");
                strSQL.Append(" cust_cons.customer_id consignee_ID, ");
                strSQL.Append("  cust_cons.customer_name consignee_name ,opr.operator_id,");
                strSQL.Append(" decode(q.cargo_type,1,'FCL',2,'LCL',4,'BBC')cargo_type, 'SEA' BIZTYPE");
                strSQL.Append("   from QUOTATION_MST_TBL       q, ");
                strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                strSQL.Append(" PORT_MST_TBL            pOL, ");
                strSQL.Append("  PORT_MST_TBL            POD, ");
                strSQL.Append(" BOOKING_TRN         btrn, ");
                strSQL.Append(" booking_mst_tbl         bkg, ");
                strSQL.Append(" customer_mst_tbl        cust_ship, ");
                strSQL.Append(" customer_mst_tbl        cust_cons, ");
                strSQL.Append("       USER_MST_TBL UMT,");
                strSQL.Append(" commodity_group_mst_tbl com,operator_mst_tbl opr ");
                strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                strSQL.Append("    AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk (+)");
                strSQL.Append(" and btrn.trans_ref_no(+) = q.quotation_ref_no ");
                strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+) ");
                strSQL.Append(" and qtrn.commodity_group_fk = com.commodity_group_pk(+)   AND QTRN.CARRIER_MST_FK = OPR.OPERATOR_MST_PK");

                if (cargo == "FCL")
                {
                    strSQL.Append(" and q.cargo_type=1 ");
                }
                else if (cargo == "LCL")
                {
                    strSQL.Append(" and q.cargo_type=2 ");
                }
                strSQL.Append(" and q.status <> 3 ");
                strSQL.Append("               " + condition);
                if (commodityfk != 0)
                {
                    strSQL.Append(" AND qtrn.commodity_group_fk = " + commodityfk);
                }
                strSQL.Append(" union ");

                strSQL.Append(" select distinct q.quotation_ref_no AS QT_REF_NO, ");
                strSQL.Append("  q.quotation_date AS Q_DATE, ");
                strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" BKG.BOOKING_REF_NO ");
                strSQL.Append(" END BKG_REF_NO, ");
                strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled') ");
                strSQL.Append(" END BKG_STATUS, ");
                strSQL.Append(" decode(q.status, 1, 'Active', 2, 'Confirm', 4, 'Confirm') AS STATUS, ");
                strSQL.Append(" POL.PORT_NAME POL_ID, ");
                strSQL.Append(" POD.PORT_NAME POD_ID, ");
                strSQL.Append(" opr.operator_mst_pk airline_PK, ");
                strSQL.Append(" OPR.OPERATOR_NAME airline_ID, ");
                strSQL.Append(" OPR.OPERATOR_NAME airline_name, ");
                strSQL.Append(" q.customer_mst_fk shipper_PK, ");
                strSQL.Append(" cust_ship.customer_id shipper_ID, ");
                strSQL.Append(" cust_ship.customer_name shipper_name, ");
                strSQL.Append(" bkg.cons_customer_mst_fk consignee_PK,");
                strSQL.Append(" cust_cons.customer_id consignee_ID,");
                strSQL.Append(" cust_cons.customer_name consignee_name,opr.operator_id,");
                strSQL.Append(" decode(q.cargo_type,1,'FCL',2,'LCL',4,'BBC')cargo_type, 'SEA' BIZTYPE");
                strSQL.Append("   from QUOTATION_MST_TBL       q, ");
                strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                strSQL.Append(" PORT_MST_TBL            pOL, ");
                strSQL.Append("  PORT_MST_TBL            POD, ");
                strSQL.Append("       USER_MST_TBL UMT,");
                strSQL.Append(" customer_mst_tbl        cust_ship, ");
                strSQL.Append(" BOOKING_TRN   btrn,");
                strSQL.Append(" booking_mst_tbl           bkg,");
                strSQL.Append(" customer_mst_tbl          cust_cons,");
                strSQL.Append(" commodity_group_mst_tbl com,operator_mst_tbl opr ");
                strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                strSQL.Append("    AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                strSQL.Append(" and qtrn.commodity_group_fk = com.commodity_group_pk(+) ");
                strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk(+)");
                strSQL.Append(" and btrn.trans_ref_no(+) = q.quotation_ref_no");
                strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+)  AND OPR.OPERATOR_MST_PK = QTRN.CARRIER_MST_FK");
                if (cargo == "FCL")
                {
                    strSQL.Append(" and q.cargo_type=1 ");
                }
                else if (cargo == "LCL")
                {
                    strSQL.Append(" and q.cargo_type=2 ");
                }
                strSQL.Append(" and q.status <> 3 ");
                strSQL.Append("               " + condition);
                if (commodityfk != 0)
                {
                    strSQL.Append(" AND qtrn.commodity_group_fk = " + commodityfk);
                }
                strSQL.Append(" UNION ");
                //'
                strSQL.Append(" select distinct q.quotation_ref_no AS QT_REF_NO, ");
                strSQL.Append(" q.quotation_date AS Q_DATE, ");
                strSQL.Append(" 'NA' AS BKG_REF_NO, ");
                strSQL.Append(" 'NA' AS BKG_STATUS, ");
                strSQL.Append(" decode(q.status, 1, 'Active', 2, 'Confirm', 4, 'Confirm') AS STATUS, ");
                strSQL.Append(" POL.PORT_NAME POL_ID, ");
                strSQL.Append(" POD.PORT_NAME POD_ID, ");
                strSQL.Append(" amt.airline_mst_pk airline_PK, ");
                strSQL.Append(" AMT.airline_name airline_ID, ");
                strSQL.Append(" amt.airline_name airline_name, ");
                strSQL.Append(" q.customer_mst_fk shipper_PK, ");
                strSQL.Append(" cust_ship.customer_id shipper_ID, ");
                strSQL.Append(" cust_ship.customer_name shipper_name, ");
                strSQL.Append(" 0 consignee_PK, ");
                strSQL.Append(" '' consignee_ID, ");
                strSQL.Append(" '' consignee_name ,");
                strSQL.Append(" '' operator_id,");
                strSQL.Append("  '' cargo_type,'AIR' BIZTYPE");
                strSQL.Append(" from QUOTATION_MST_TBL       q, ");
                strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                strSQL.Append(" PORT_MST_TBL            pOL, ");
                strSQL.Append(" PORT_MST_TBL            POD, ");
                strSQL.Append(" airline_mst_tbl         amt, ");
                strSQL.Append(" customer_mst_tbl        cust_ship, ");
                strSQL.Append("       USER_MST_TBL UMT,");
                strSQL.Append(" commodity_group_mst_tbl com, ");
                strSQL.Append(" airfreight_slabs_tbl   slab ");
                strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                strSQL.Append(" and qtrn.slab_fk = slab.airfreight_slabs_tbl_pk(+)  ");
                strSQL.Append(" AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                strSQL.Append("  AND QTRN.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+) ");
                strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                strSQL.Append(" and q.commodity_group_mst_fk = com.commodity_group_pk(+) ");
                strSQL.Append(" and q.status <> 3 ");
                strSQL.Append("               " + condition);
                if (commodityfk != 0)
                {
                    strSQL.Append(" AND q.commodity_group_mst_fk = " + commodityfk);
                }
                if ((cargo == "KGS"))
                {
                    strSQL.Append(" and slab.basis=1 ");
                }
                else if ((cargo == "ULD"))
                {
                    strSQL.Append(" and slab.basis=2 ");
                }
                strSQL.Append(" union ");
                strSQL.Append(" select distinct q.quotation_ref_no AS QT_REF_NO, ");
                strSQL.Append(" q.quotation_date AS Q_DATE, ");
                strSQL.Append(" CASE WHEN  BKG.BOOKING_REF_NO IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" BKG.BOOKING_REF_NO ");
                strSQL.Append(" END BKG_REF_NO, ");
                strSQL.Append(" CASE WHEN  BKG.STATUS IS NULL THEN ");
                strSQL.Append(" 'NA' ELSE ");
                strSQL.Append(" DECODE(BKG.STATUS, 1, 'Active', 2, 'Confirm', 3, 'Cancelled', 6, 'Confirm') ");
                strSQL.Append(" END BKG_STATUS, ");
                strSQL.Append(" decode(q.status, 1, 'Active', 2, 'Confirm', 4, 'Confirm') AS STATUS, ");
                strSQL.Append(" POL.PORT_NAME POL_ID, ");
                strSQL.Append(" POD.PORT_NAME POD_ID, ");
                strSQL.Append(" amt.airline_mst_pk airline_PK, ");
                strSQL.Append(" AMT.airline_name airline_ID, ");
                strSQL.Append(" amt.airline_name airline_name, ");
                strSQL.Append(" q.customer_mst_fk shipper_PK, ");
                strSQL.Append(" cust_ship.customer_id shipper_ID, ");
                strSQL.Append(" cust_ship.customer_name shipper_name, ");
                strSQL.Append(" bkg.cons_customer_mst_fk consignee_PK, ");
                strSQL.Append(" cust_cons.customer_id consignee_ID, ");
                strSQL.Append(" cust_cons.customer_name consignee_name ,");
                strSQL.Append(" '' operator_id,");
                strSQL.Append("  '' cargo_type,'AIR' BIZTYPE");
                strSQL.Append(" from QUOTATION_MST_TBL       q, ");
                strSQL.Append(" QUOTATION_DTL_TBL       qtrn, ");
                strSQL.Append(" PORT_MST_TBL            pOL, ");
                strSQL.Append(" PORT_MST_TBL            POD, ");
                strSQL.Append(" airline_mst_tbl         amt, ");
                strSQL.Append(" BOOKING_TRN         btrn, ");
                strSQL.Append(" booking_mst_tbl         bkg, ");
                strSQL.Append(" customer_mst_tbl        cust_ship, ");
                strSQL.Append(" customer_mst_tbl        cust_cons, ");
                strSQL.Append("       USER_MST_TBL UMT,");
                strSQL.Append(" commodity_group_mst_tbl com, ");
                strSQL.Append(" airfreight_slabs_tbl   slab ");
                strSQL.Append("  where q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                strSQL.Append(" and btrn.booking_mst_fk = bkg.booking_mst_pk ");
                strSQL.Append(" and qtrn.slab_fk = slab.airfreight_slabs_tbl_pk ");
                strSQL.Append(" AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                strSQL.Append("  AND QTRN.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+) ");
                strSQL.Append("  and btrn.trans_ref_no(+) = q.quotation_ref_no ");
                strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                strSQL.Append(" and bkg.cons_customer_mst_fk = cust_cons.customer_mst_pk(+) ");
                strSQL.Append(" and q.commodity_group_mst_fk = com.commodity_group_pk(+) ");
                strSQL.Append(" and q.status <> 3 ");
                strSQL.Append("               " + condition);
                if (commodityfk != 0)
                {
                    strSQL.Append(" AND q.commodity_group_mst_fk = " + commodityfk);
                }
                if ((cargo == "KGS"))
                {
                    strSQL.Append(" and slab.basis=1 ");
                }
                else if ((cargo == "ULD"))
                {
                    strSQL.Append(" and slab.basis=2 ");
                }
                strSQL.Append(" union ");
                strSQL.Append(" select distinct q.quotation_ref_no AS QT_REF_NO, ");
                strSQL.Append(" q.quotation_date AS Q_DATE, ");
                strSQL.Append(" 'NA' as BKG_REF_NO, ");
                strSQL.Append(" 'NA' AS BKG_STATUS, ");
                strSQL.Append(" decode(q.status, 1, 'Active', 2, 'Confirm', 4, 'Confirm') AS STATUS, ");
                strSQL.Append(" POL.PORT_NAME POL_ID, ");
                strSQL.Append(" POD.PORT_NAME POD_ID, ");
                strSQL.Append(" amt.airline_mst_pk airline_PK, ");
                strSQL.Append(" AMT.airline_name airline_ID, ");
                strSQL.Append(" amt.airline_name airline_name, ");
                strSQL.Append(" q.customer_mst_fk shipper_PK, ");
                strSQL.Append(" cust_ship.customer_id shipper_ID, ");
                strSQL.Append(" cust_ship.customer_name shipper_name, ");
                strSQL.Append(" 0 as consignee_PK, ");
                strSQL.Append(" '' as consignee_ID, ");
                strSQL.Append(" '' as consignee_name, ");
                strSQL.Append(" '' operator_id,");
                strSQL.Append("  '' cargo_type,'AIR' BIZTYPE");
                strSQL.Append(" from QUOTATION_MST_TBL       q, ");
                strSQL.Append(" QUOTATION_DTL_TBL    qtrn, ");
                strSQL.Append(" PORT_MST_TBL            pOL, ");
                strSQL.Append(" PORT_MST_TBL            POD, ");
                strSQL.Append(" airline_mst_tbl         amt, ");
                strSQL.Append(" customer_mst_tbl        cust_ship, ");
                strSQL.Append("       USER_MST_TBL UMT,");
                strSQL.Append(" commodity_group_mst_tbl com ");
                strSQL.Append("  where (1 = 1) ");
                strSQL.Append(" and q.QUOTATION_MST_PK = qtrn.QUOTATION_MST_FK ");
                strSQL.Append(" AND POL.PORT_MST_PK(+) = qtrn.port_mst_pol_fk ");
                strSQL.Append(" AND POD.PORT_MST_PK(+) = qtrn.port_mst_pod_fk ");
                strSQL.Append("  AND QTRN.CARRIER_MST_FK = AMT.AIRLINE_MST_PK(+) ");
                strSQL.Append(" and q.customer_mst_fk = cust_ship.customer_mst_pk(+) ");
                strSQL.Append(" and q.commodity_group_mst_fk = com.commodity_group_pk(+) ");
                strSQL.Append(" and q.status <> 3 ");
                strSQL.Append("               " + condition);
                if (commodityfk != 0)
                {
                    strSQL.Append(" AND q.commodity_group_mst_fk = " + commodityfk);
                }
            }

            try
            {
                return objWF.GetDataSet(strSQL.ToString());
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "GetQuotation Report"

        #region "Find Contract"

        //This fun fetch the contract from the database against the supplied Airline Pk,Pol,Pod,From Date,To Date
        /// <summary>
        /// Finds the contract.
        /// </summary>
        /// <param name="strPolPk">The string pol pk.</param>
        /// <param name="strPodPk">The string pod pk.</param>
        /// <param name="nAirlinePk">The n airline pk.</param>
        /// <param name="ContPk">The cont pk.</param>
        /// <param name="ChargeBasis">The charge basis.</param>
        /// <param name="AirSuchargeToolTip">The air sucharge tool tip.</param>
        /// <returns></returns>
        public DataSet findContract(string strPolPk, string strPodPk, string nAirlinePk, string ContPk, string ChargeBasis, string AirSuchargeToolTip)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsGrid = new DataSet();
            int i = 0;
            System.Text.StringBuilder sqlBuilder = new System.Text.StringBuilder();
            string[] arrPolPk = null;
            string[] arrPodPk = null;
            DataTable dtDataTable = null;
            DataTable dtPort = null;
            DataTable dtOtherCharge = null;
            string strSectorCondition = "";
            Quantum_QFOR.cls_AirlineTariffEntry objATE = new Quantum_QFOR.cls_AirlineTariffEntry(7, 2);

            try
            {
                ContPk = "";
                ChargeBasis = "";
                AirSuchargeToolTip = "";
                //Spliting the POL and POD Pk's
                arrPolPk = strPolPk.Split(Convert.ToChar(","));
                arrPodPk = strPodPk.Split(Convert.ToChar(","));

                for (i = 0; i <= arrPolPk.Length - 1; i++)
                {
                    strSectorCondition += "(" + arrPolPk[i] + "," + arrPodPk[i] + "),";
                }
                strSectorCondition = strSectorCondition.TrimEnd(Convert.ToChar(","));

                //Making query with the condition added
                sqlBuilder.Append(" SELECT HDR.CONT_MAIN_AIR_PK FROM");
                sqlBuilder.Append(" CONT_MAIN_AIR_TBL HDR,");
                sqlBuilder.Append(" CONT_TRN_AIR_LCL TRN,");
                sqlBuilder.Append(" PORT_MST_TBL POL,");
                sqlBuilder.Append(" PORT_MST_TBL POD ");
                sqlBuilder.Append(" WHERE TRN.CONT_MAIN_AIR_FK = HDR.CONT_MAIN_AIR_PK");
                sqlBuilder.Append(" AND HDR.AIRLINE_MST_FK =" + nAirlinePk);
                sqlBuilder.Append(" AND HDR.ACTIVE = 1");
                sqlBuilder.Append(" AND HDR.CONT_APPROVED = 1");
                sqlBuilder.Append(" AND POL.PORT_MST_PK = TRN.PORT_MST_POL_FK");
                sqlBuilder.Append(" AND POD.PORT_MST_PK = TRN.PORT_MST_POD_FK");
                sqlBuilder.Append(" AND (TRN.PORT_MST_POL_FK,TRN.PORT_MST_POD_FK) IN (" + strSectorCondition + ")");
                sqlBuilder.Append(" ORDER BY POL.PORT_ID,POD.PORT_ID");

                dtDataTable = objWF.GetDataTable(sqlBuilder.ToString());

                for (i = 0; i <= dtDataTable.Rows.Count - 1; i++)
                {
                    ContPk += dtDataTable.Rows[i][0].ToString() + ",";
                }
                ContPk = ContPk.TrimEnd(Convert.ToChar(","));

                if (ContPk.Trim().Length > 0)
                {
                    try
                    {
                        Create_Static_Column(dsGrid);

                        //***************************************************************************************'
                        // Fetching Airferight Slabs.
                        // Creating columns and populating child table acc. to the number of air freight slabs.
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("      TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("      FRT_FRT.FREIGHT_ELEMENT_ID AS FRT_FRT,");
                        sqlBuilder.Append("      FRT.CONT_AIR_FREIGHT_PK  AS FRTPK,");
                        sqlBuilder.Append("      FRT.FREIGHT_ELEMENT_MST_FK AS FRT_FRT_FK,");
                        sqlBuilder.Append("      FRT.CURRENCY_MST_FK AS FRT_CURR,");
                        sqlBuilder.Append("      FRT_CURR.CURRENCY_ID AS FRT_CURRID,");
                        sqlBuilder.Append("      FRT.MIN_AMOUNT,");
                        sqlBuilder.Append("      AIR_FRT.BREAKPOINT_ID AS SLABS,");
                        sqlBuilder.Append("      BRK.AIRFREIGHT_SLABS_FK AS SLABS_FK,");
                        sqlBuilder.Append("      BRK.APPROVED_RATE AS SLAB_CURRENT,");
                        sqlBuilder.Append("      BRK.APPROVED_RATE AS SLAB_TARIFF");
                        sqlBuilder.Append(" FROM CONT_MAIN_AIR_TBL       HDR,");
                        sqlBuilder.Append("      CONT_TRN_AIR_LCL        TRN,");
                        sqlBuilder.Append("      CONT_AIR_FREIGHT_TBL    FRT,");
                        sqlBuilder.Append("      CONT_AIR_BREAKPOINTS    BRK,");
                        sqlBuilder.Append("      CURRENCY_TYPE_MST_TBL   FRT_CURR,");
                        sqlBuilder.Append("      FREIGHT_ELEMENT_MST_TBL FRT_FRT,");
                        sqlBuilder.Append("      AIRFREIGHT_SLABS_TBL    AIR_FRT ");
                        sqlBuilder.Append("WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("  AND TRN.CONT_TRN_AIR_PK = FRT.CONT_TRN_AIR_FK");
                        sqlBuilder.Append("  AND FRT.CONT_AIR_FREIGHT_PK = BRK.CONT_AIR_FREIGHT_FK");
                        sqlBuilder.Append("  AND FRT_CURR.CURRENCY_MST_PK = FRT.CURRENCY_MST_FK");
                        sqlBuilder.Append("  AND FRT_FRT.FREIGHT_ELEMENT_MST_PK = FRT.FREIGHT_ELEMENT_MST_FK");
                        sqlBuilder.Append("  AND AIR_FRT.AIRFREIGHT_SLABS_TBL_PK = BRK.AIRFREIGHT_SLABS_FK");
                        sqlBuilder.Append("  AND (TRN.PORT_MST_POL_FK,TRN.PORT_MST_POD_FK) IN (" + strSectorCondition + ")");
                        sqlBuilder.Append("ORDER BY TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("         TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("         FRT_FRT.FREIGHT_ELEMENT_ID,");
                        sqlBuilder.Append("         AIR_FRT.SEQUENCE_NO");

                        dtDataTable = objWF.GetDataTable(sqlBuilder.ToString());
                        Make_Column(dsGrid.Tables["Child"], dtDataTable, true);
                        Populate_Child(dsGrid.Tables["Child"], dtDataTable);

                        //***************************************************************************************'
                        // Fetching Air Surcharges
                        // Creating columns in parent table acc. to the number of air surcharges
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.CONT_TRN_AIR_PK AS TRN_AIR_PK,");
                        sqlBuilder.Append("      SUR_FRT.FREIGHT_ELEMENT_ID ");
                        sqlBuilder.Append(" || DECODE(SUR.CHARGE_BASIS,1,' (%)',2,' (Flat)',3,' (Kgs)') AS SUR,");
                        sqlBuilder.Append("      SUR.FREIGHT_ELEMENT_MST_FK AS SUR_FRT_FK,");
                        sqlBuilder.Append("      SUR.APPROVED_RATE AS SUR_CURRENT,");
                        sqlBuilder.Append("      SUR.APPROVED_RATE AS SUR_TARIFF,");
                        sqlBuilder.Append("      SUR.CHARGE_BASIS AS SUR_CHARGE_BASIS,");
                        sqlBuilder.Append("      SUR_FRT.FREIGHT_ELEMENT_NAME AS SUR_FRT_NAME");
                        sqlBuilder.Append(" FROM CONT_MAIN_AIR_TBL       HDR,");
                        sqlBuilder.Append("      CONT_TRN_AIR_LCL        TRN,");
                        sqlBuilder.Append("      CONT_AIR_SURCHARGE      SUR,");
                        sqlBuilder.Append("      FREIGHT_ELEMENT_MST_TBL SUR_FRT ");
                        sqlBuilder.Append("WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("  AND TRN.CONT_TRN_AIR_PK = SUR.CONT_TRN_AIR_FK");
                        sqlBuilder.Append("  AND SUR_FRT.FREIGHT_ELEMENT_MST_PK = SUR.FREIGHT_ELEMENT_MST_FK");
                        sqlBuilder.Append("  AND (TRN.PORT_MST_POL_FK,TRN.PORT_MST_POD_FK) IN (" + strSectorCondition + ")");
                        sqlBuilder.Append("ORDER BY TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("         TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("         SUR_FRT.FREIGHT_ELEMENT_ID");

                        dtDataTable = objWF.GetDataTable(sqlBuilder.ToString());
                        Make_Column(dsGrid.Tables["Parent"], dtDataTable, false, ChargeBasis, AirSuchargeToolTip);
                        dsGrid.Tables["Parent"].Columns.Add("Oth. Chrg", typeof(string));
                        dsGrid.Tables["Parent"].Columns.Add("Oth_Chrg_Val", typeof(string));

                        //***************************************************************************************'
                        // Fetching Ports and its validity
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.CONT_TRN_AIR_PK AS TRN_AIR_PK,");
                        sqlBuilder.Append("       TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("       POL.PORT_ID AS AOO,");
                        sqlBuilder.Append("       TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("       POD.PORT_ID AS AOD,");
                        sqlBuilder.Append("  FROM CONT_MAIN_AIR_TBL HDR,");
                        sqlBuilder.Append("       CONT_TRN_AIR_LCL  TRN,");
                        sqlBuilder.Append("       PORT_MST_TBL      POL,");
                        sqlBuilder.Append("       PORT_MST_TBL      POD ");
                        sqlBuilder.Append(" WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("   AND POL.PORT_MST_PK = TRN.PORT_MST_POL_FK");
                        sqlBuilder.Append("   AND POD.PORT_MST_PK = TRN.PORT_MST_POD_FK");
                        sqlBuilder.Append("   AND (TRN.PORT_MST_POL_FK,TRN.PORT_MST_POD_FK) IN (" + strSectorCondition + ")");
                        sqlBuilder.Append(" ORDER BY POL.PORT_ID,POD.PORT_ID");

                        dtPort = objWF.GetDataTable(sqlBuilder.ToString());

                        //***************************************************************************************'
                        // Fetching Air Other Charges
                        // Populating parent table with  Ports, Air surcharges and Other Charges
                        //***************************************************************************************'
                        sqlBuilder.Remove(0, sqlBuilder.ToString().Length);
                        sqlBuilder.Append("SELECT TRN.CONT_TRN_AIR_PK AS TRN_AIR_PK,");
                        sqlBuilder.Append("       OTH_CHRG_FRT.FREIGHT_ELEMENT_ID AS OTH_CHRG_FRT,");
                        sqlBuilder.Append("       OTH_CHRG.FREIGHT_ELEMENT_MST_FK AS OTH_CHRG_FRT_FRT_FK,");
                        sqlBuilder.Append("       OTH_CHRG_CURR.CURRENCY_ID AS OTH_CHRG_CURRID,");
                        sqlBuilder.Append("       OTH_CHRG.CURRENCY_MST_FK AS OTH_CHRG_CURR,");
                        sqlBuilder.Append("       OTH_CHRG.APPROVED_RATE AS OTH_CHRG_CURRENT,");
                        sqlBuilder.Append("       OTH_CHRG.APPROVED_RATE AS OTH_CHRG_TARIFF,");
                        sqlBuilder.Append("       OTH_CHRG.CHARGE_BASIS AS OTH_CHRG_BASIS");
                        sqlBuilder.Append("  FROM CONT_MAIN_AIR_TBL       HDR,");
                        sqlBuilder.Append("       CONT_TRN_AIR_LCL        TRN,");
                        sqlBuilder.Append("       CONT_AIR_OTH_CHRG       OTH_CHRG,");
                        sqlBuilder.Append("       CURRENCY_TYPE_MST_TBL   OTH_CHRG_CURR,");
                        sqlBuilder.Append("       FREIGHT_ELEMENT_MST_TBL OTH_CHRG_FRT");
                        sqlBuilder.Append(" WHERE HDR.CONT_MAIN_AIR_PK = TRN.CONT_MAIN_AIR_FK");
                        sqlBuilder.Append("   AND TRN.CONT_TRN_AIR_PK = OTH_CHRG.CONT_TRN_AIR_FK(+)");
                        sqlBuilder.Append("   AND OTH_CHRG_CURR.CURRENCY_MST_PK(+) = OTH_CHRG.CURRENCY_MST_FK");
                        sqlBuilder.Append("   AND OTH_CHRG_FRT.FREIGHT_ELEMENT_MST_PK(+) =");
                        sqlBuilder.Append("       OTH_CHRG.FREIGHT_ELEMENT_MST_FK");
                        sqlBuilder.Append("   AND (TRN.PORT_MST_POL_FK,TRN.PORT_MST_POD_FK) IN (" + strSectorCondition + ")");
                        sqlBuilder.Append(" ORDER BY TRN.PORT_MST_POL_FK,");
                        sqlBuilder.Append("          TRN.PORT_MST_POD_FK,");
                        sqlBuilder.Append("          OTH_CHRG_FRT.FREIGHT_ELEMENT_ID");

                        dtOtherCharge = objWF.GetDataTable(sqlBuilder.ToString());
                        Populate_Parent(dsGrid.Tables["Parent"], dtPort, dtDataTable, dtOtherCharge);

                        //***************************************************************************************'
                        // Creating Relations
                        //***************************************************************************************'
                        DataRelation rel = new DataRelation("rl_POL_POD", new DataColumn[] {
                            dsGrid.Tables["Parent"].Columns["POLPK"],
                            dsGrid.Tables["Parent"].Columns["PODPK"]
                        }, new DataColumn[] {
                            dsGrid.Tables["Child"].Columns["POLPK"],
                            dsGrid.Tables["Child"].Columns["PODPK"]
                        });
                        dsGrid.Relations.Add(rel);
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
                        dtPort.Dispose();
                        dtOtherCharge.Dispose();
                    }
                }
                return dsGrid;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                dtDataTable.Dispose();
                objWF = null;
            }
        }

        #endregion "Find Contract"

        #region " Supporting Functions "

        /// <summary>
        /// Create_s the static_ column.
        /// </summary>
        /// <param name="dsGrid">The ds grid.</param>
        /// Creates Static column to be shown in the grid.

        public void Create_Static_Column(DataSet dsGrid)
        {
            //***************************************************************************************'
            // Creating Parent Table
            //***************************************************************************************'
            dsGrid.Tables.Add("Parent");
            dsGrid.Tables["Parent"].Columns.Add("TRNPK", typeof(long));
            dsGrid.Tables["Parent"].Columns.Add("POLPK", typeof(long));
            dsGrid.Tables["Parent"].Columns.Add("AOO", typeof(string));
            dsGrid.Tables["Parent"].Columns.Add("PODPK", typeof(long));
            dsGrid.Tables["Parent"].Columns.Add("AOD", typeof(string));
            dsGrid.Tables["Parent"].Columns.Add("Valid From", typeof(string));
            dsGrid.Tables["Parent"].Columns.Add("Valid To", typeof(string));

            if (_Static_Col > _AirLine_Tariff_Cols)
            {
                dsGrid.Tables["Parent"].Columns.Add("Expected_Wt", typeof(double));
                dsGrid.Tables["Parent"].Columns.Add("Expected_Vol", typeof(double));
            }
            //Two columns are remainig for Other charges
            //Create these two column at the end of fetcing and creating air surcharges column

            //***************************************************************************************'
            // Creating Child Table
            //***************************************************************************************'
            dsGrid.Tables.Add("Child");
            dsGrid.Tables["Child"].Columns.Add("FRTPK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("POLPK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("PODPK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("FREIGHT_ELEMENT_MST_PK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("Frt. Ele.", typeof(string));
            dsGrid.Tables["Child"].Columns.Add("CURRENCY_MST_PK", typeof(long));
            dsGrid.Tables["Child"].Columns.Add("Curr.", typeof(string));
            dsGrid.Tables["Child"].Columns.Add("Minimum", typeof(double));
        }

        /// <summary>
        /// Make_s the column.
        /// </summary>
        /// <param name="dtMain">The dt main.</param>
        /// <param name="dtTable">The dt table.</param>
        /// <param name="IsSlab">if set to <c>true</c> [is slab].</param>
        /// <param name="ChargeBasis">The charge basis.</param>
        /// <param name="AirSuchargeToolTip">The air sucharge tool tip.</param>
        public void Make_Column(DataTable dtMain, DataTable dtTable, bool IsSlab, string ChargeBasis = "", string AirSuchargeToolTip = "")
        {
            int nRowCnt = 0;
            long nFrt = 0;
            bool bFirstTime = true;

            if (!IsSlab)
            {
                if (dtTable.Rows.Count > 0)
                {
                    nFrt = Convert.ToInt64(dtTable.Rows[0]["SUR_FRT_FK"]);
                }
            }
            else
            {
                if (dtTable.Rows.Count > 0)
                {
                    nFrt = Convert.ToInt64(dtTable.Rows[0]["FRTPK"]);
                }
            }

            //Making dynamic columns
            for (nRowCnt = 0; nRowCnt <= dtTable.Rows.Count - 1; nRowCnt++)
            {
                if (!IsSlab)
                {
                    if (nFrt == Convert.ToInt64(dtTable.Rows[nRowCnt]["SUR_FRT_FK"]) & !bFirstTime)
                    {
                        return;
                    }

                    if (_Col_Incr == 3)
                    {
                        //Current,Requested,Approved
                        CheckColumn(dtMain, dtTable.Rows[nRowCnt]["SUR_FRT_FK"].ToString(), dtTable.Rows[nRowCnt]["SUR_EXTRA"].ToString(), dtTable.Rows[nRowCnt]["SUR"].ToString());
                    }
                    else
                    {
                        CheckColumn(dtMain, dtTable.Rows[nRowCnt]["SUR_FRT_FK"].ToString(), dtTable.Rows[nRowCnt]["SUR"].ToString());
                    }

                    ChargeBasis += dtTable.Rows[nRowCnt]["SUR_CHARGE_BASIS"].ToString() + ",";
                    AirSuchargeToolTip += dtTable.Rows[nRowCnt]["SUR_FRT_NAME"].ToString() + ",";
                    bFirstTime = false;
                }
                else
                {
                    if (nFrt != Convert.ToInt64(dtTable.Rows[nRowCnt]["FRTPK"]))
                    {
                        return;
                    }

                    if (_Col_Incr == 3)
                    {
                        //Current,Requested,Approved
                        CheckColumn(dtMain, dtTable.Rows[nRowCnt]["SLABS_FK"].ToString(), dtTable.Rows[nRowCnt]["SLABS_EXTRA"].ToString(), dtTable.Rows[nRowCnt]["SLABS"].ToString());
                    }
                    else
                    {
                        CheckColumn(dtMain, dtTable.Rows[nRowCnt]["SLABS_FK"].ToString(), dtTable.Rows[nRowCnt]["SLABS"].ToString());

                        AirSuchargeToolTip += dtTable.Rows[nRowCnt]["SLABS"].ToString() + ",";
                    }
                }
            }

            ChargeBasis = ChargeBasis.TrimEnd(Convert.ToChar(","));
            AirSuchargeToolTip = AirSuchargeToolTip.TrimEnd(Convert.ToChar(","));
        }

        /// <summary>
        /// Checks the column.
        /// </summary>
        /// <param name="dtTable">The dt table.</param>
        /// <param name="ColumnName">Name of the column.</param>
        /// Actually creates column in datatabla equal to the number of arguments in paramarray
        public void CheckColumn(DataTable dtTable, params string[] ColumnName)
        {
            try
            {
                int i = 0;
                for (i = 0; i <= ColumnName.Length - 1; i++)
                {
                    dtTable.Columns.Add(ColumnName[i], typeof(double));
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Populate_s the child.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="dtTable">The dt table.</param>
        /// Populates child table for airfreight slabs
        public void Populate_Child(DataTable dsMain, DataTable dtTable)
        {
            int nRowCnt = 0;
            DataRow drRow = dsMain.NewRow();
            int nColPopulated = 0;
            int nTotalCol = dsMain.Columns.Count - 1;
            string strRatesToBeShown = null;
            if (_Col_Incr == 3)
            {
                strRatesToBeShown = "SLAB_APPROVED";
            }
            else
            {
                strRatesToBeShown = "SLAB_TARIFF";
            }
            for (nRowCnt = 0; nRowCnt <= dtTable.Rows.Count - 1; nRowCnt++)
            {
                var _with3 = dtTable.Rows[nRowCnt];

                if (string.IsNullOrEmpty(drRow["FRTPK"].ToString()))
                {
                    drRow["FRTPK"] = _with3["FRTPK"];
                    nColPopulated = 0;
                }

                if (string.IsNullOrEmpty(drRow["POLPK"].ToString()))
                {
                    drRow["POLPK"] = _with3["PORT_MST_POL_FK"];
                    nColPopulated += 1;
                }

                if (string.IsNullOrEmpty(drRow["PODPK"].ToString()))
                {
                    drRow["PODPK"] = _with3["PORT_MST_POD_FK"];
                    nColPopulated += 1;
                }

                if (string.IsNullOrEmpty(drRow["FREIGHT_ELEMENT_MST_PK"].ToString()))
                {
                    drRow["FREIGHT_ELEMENT_MST_PK"] = _with3["FRT_FRT_FK"];
                    nColPopulated += 1;
                }

                if (string.IsNullOrEmpty(drRow["Frt. Ele."].ToString()))
                {
                    drRow["Frt. Ele."] = _with3["FRT_FRT"];
                    nColPopulated += 1;
                }

                if (string.IsNullOrEmpty(drRow["CURRENCY_MST_PK"].ToString()))
                {
                    drRow["CURRENCY_MST_PK"] = _with3["FRT_CURR"];
                    nColPopulated += 1;
                }

                if (string.IsNullOrEmpty(drRow["Curr."].ToString()))
                {
                    drRow["Curr."] = _with3["FRT_CURRID"];
                    nColPopulated += 1;
                }

                if (string.IsNullOrEmpty(drRow["Minimum"].ToString()))
                {
                    drRow["Minimum"] = _with3["MIN_AMOUNT"];
                    nColPopulated += 1;
                    ;
                }

                if (string.IsNullOrEmpty(drRow[_with3["SLABS_FK"].ToString()].ToString()))
                {
                    drRow[_with3["SLABS_FK"].ToString()] = _with3["SLAB_CURRENT"];
                    nColPopulated += 1;
                }

                if (string.IsNullOrEmpty(drRow[_with3["SLABS"].ToString()].ToString()))
                {
                    drRow[_with3["SLABS"].ToString()] = _with3[strRatesToBeShown];
                    nColPopulated += 1;
                }

                if (_Col_Incr == 3)
                {
                    if (string.IsNullOrEmpty(drRow[_with3["SLABS_EXTRA"].ToString()].ToString()))
                    {
                        drRow[_with3["SLABS_EXTRA"].ToString()] = _with3["SLAB_TARIFF"];
                        nColPopulated += 1;
                    }
                }

                if (nTotalCol == nColPopulated)
                {
                    nColPopulated = 0;
                    dsMain.Rows.Add(drRow);
                    drRow = dsMain.NewRow();
                }
            }
        }

        /// <summary>
        /// Populate_s the parent.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="dtPort">The dt port.</param>
        /// <param name="dtAirSurchage">The dt air surchage.</param>
        /// <param name="dtOtherCharges">The dt other charges.</param>
        /// <param name="strContractPk">The string contract pk.</param>
        /// Populates Parent table for sector,air surcharges and other charges
        public void Populate_Parent(DataTable dsMain, DataTable dtPort, DataTable dtAirSurchage, DataTable dtOtherCharges, string strContractPk = null)
        {
            int nPortRowCnt = 0;
            int nAirRowCnt = 0;
            int nOthRowCnt = 0;
            DataRow drMain = null;
            bool boolFirstLoop = true;
            string strRatestoBeShown = null;
            if (_Col_Incr == 3)
            {
                strRatestoBeShown = "SUR_APPROVED";
            }
            else
            {
                strRatestoBeShown = "SUR_TARIFF";
            }
            for (nPortRowCnt = 0; nPortRowCnt <= dtPort.Rows.Count - 1; nPortRowCnt++)
            {
                drMain = dsMain.NewRow();
                var _with4 = dtPort.Rows[nPortRowCnt];
                drMain["TRNPK"] = _with4["TRN_AIR_PK"];
                drMain["POLPK"] = _with4["PORT_MST_POL_FK"];
                drMain["AOO"] = _with4["AOO"];
                drMain["PODPK"] = _with4["PORT_MST_POD_FK"];
                drMain["AOD"] = _with4["AOD"];
                drMain["Valid From"] = (Convert.ToBoolean(_FromDate.TrimEnd().Length > 0) ? _FromDate : _with4["VALID_FROM"]);
                drMain["Valid To"] = (Convert.ToBoolean(_Todate.TrimEnd().Length > 0) ? _Todate : _with4["VALID_TO"]);

                if (_Static_Col > _AirLine_Tariff_Cols & _Use_Extra_Cols)
                {
                    drMain["Expected_Wt"] = _with4["EXPECTED_WEIGHT"];
                    drMain["Expected_Vol"] = _with4["EXPECTED_VOLUME"];
                }
                if (_Static_Col <= _AirLine_Tariff_Cols)
                {
                    if ((strContractPk != null) & boolFirstLoop)
                    {
                        if (!_with4.IsNull("CONT_MAIN_AIR_FK"))
                        {
                            strContractPk = _with4["CONT_MAIN_AIR_FK"].ToString();
                        }
                        boolFirstLoop = false;
                    }
                    else if (!boolFirstLoop)
                    {
                        if (!_with4.IsNull("CONT_MAIN_AIR_FK"))
                        {
                            strContractPk = strContractPk + "," + _with4["CONT_MAIN_AIR_FK"].ToString();
                        }
                    }
                }
                for (nAirRowCnt = 0; nAirRowCnt <= dtAirSurchage.Rows.Count - 1; nAirRowCnt++)
                {
                    var _with5 = dtAirSurchage.Rows[nAirRowCnt];

                    if (Convert.ToInt64(_with5["TRN_AIR_PK"]) == Convert.ToInt64(dtPort.Rows[nPortRowCnt]["TRN_AIR_PK"]))
                    {
                        drMain[_with5["SUR_FRT_FK"].ToString()] = _with5["SUR_CURRENT"];
                        drMain[_with5["SUR"].ToString()] = _with5[strRatestoBeShown];

                        if (_Col_Incr == 3)
                        {
                            drMain[_with5["SUR_EXTRA"].ToString()] = _with5["SUR_TARIFF"];
                        }
                    }
                }
                //Air Surcharge loop
                //FreightElementPk ~ CurrencyPk ~ CurrentRate ~ RequestRate ^
                for (nOthRowCnt = 0; nOthRowCnt <= dtOtherCharges.Rows.Count - 1; nOthRowCnt++)
                {
                    var _with6 = dtOtherCharges.Rows[nOthRowCnt];
                    if (Convert.ToInt64(_with6["TRN_AIR_PK"]) == Convert.ToInt64(dtPort.Rows[nPortRowCnt]["TRN_AIR_PK"]))
                    {
                        if (!(_with6.IsNull("OTH_CHRG_FRT_FRT_FK")))
                        {
                            if (_Col_Incr == 3)
                            {
                                drMain["Oth_Chrg_Val"] = (drMain.IsNull("Oth_Chrg_Val") ? "" : drMain["Oth_Chrg_Val"]).ToString() + _with6["OTH_CHRG_FRT_FRT_FK"].ToString() + "~" + _with6["OTH_CHRG_CURR"].ToString() + "~" + _with6["OTH_CHRG_BASIS"].ToString() + "~" + _with6["OTH_CHRG_CURRENT"].ToString() + "~" + _with6["OTH_CHRG_TARIFF"].ToString() + "~" + _with6["OTH_CHRG_APPROVED"].ToString() + "^";
                            }
                            else
                            {
                                drMain["Oth_Chrg_Val"] = (drMain.IsNull("Oth_Chrg_Val") ? "" : drMain["Oth_Chrg_Val"]).ToString() + _with6["OTH_CHRG_FRT_FRT_FK"].ToString() + "~" + _with6["OTH_CHRG_CURR"].ToString() + "~" + _with6["OTH_CHRG_BASIS"].ToString() + "~" + _with6["OTH_CHRG_CURRENT"].ToString() + "~" + _with6["OTH_CHRG_TARIFF"].ToString() + "^";
                            }
                        }
                    }
                }
                dsMain.Rows.Add(drMain);
            }
        }

        #endregion " Supporting Functions "

        //' PTS -Jul -020
        /// <summary>
        /// Fetches the grid query.
        /// </summary>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcess">The int process.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="customer">The customer.</param>
        /// <param name="Loc">The loc.</param>
        /// <param name="Flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        /// 'This same Query's is using for Grid and Report

        #region "Fetch Grid Query"

        public DataSet FetchGridQuery(int intBizType, int intProcess, string fromDate, string toDate, string customer, Int32 Loc, int Flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            string sql = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            Int32 cust_pk = default(Int32);

            try
            {
                if (!string.IsNullOrEmpty(customer))
                {
                    cust_pk = Convert.ToInt32(objWF.ExecuteScaler("select customer_mst_pk from customer_mst_tbl where customer_id='" + customer + "'"));
                }
                else
                {
                    cust_pk = 0;
                }

                //Sea Export
                if (intBizType == 2 & intProcess == 1)
                {
                    sb.Append(GetReportForSeaExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                //Air Export
                if (intBizType == 1 & intProcess == 1)
                {
                    sb.Append(GetReportForAirExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                //Sea Import
                if (intBizType == 2 & intProcess == 2)
                {
                    sb.Append(GetReportForSeaImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                //Air Import
                if (intBizType == 1 & intProcess == 2)
                {
                    sb.Append(GetReportForAirImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                if (intBizType == 1 & intProcess == 0)
                {
                    ///*******************Air-exp and imp
                    sb.Append(GetReportForAirExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForAirImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                if (intBizType == 2 & intProcess == 0)
                {
                    //'***********************Sea Imp-Exp
                    sb.Append(GetReportForSeaExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForSeaImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                if (intBizType == 3 & intProcess == 1)
                {
                    //'******************************* All Biz Exp
                    sb.Append(GetReportForAirExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForSeaExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                if (intBizType == 3 & intProcess == 2)
                {
                    //'******************************* All Biz Imp
                    sb.Append(GetReportForAirImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForSeaImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                if (intBizType == 3 & intProcess == 0)
                {
                    //'***************************************ALL
                    sb.Append(GetReportForAirExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForSeaExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForAirImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForSeaImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    //'***************************************
                }
                System.Text.StringBuilder strCount = new System.Text.StringBuilder();
                strCount.Append(" SELECT COUNT(*)  from  ");
                strCount.Append((" (" + sb.ToString() + ")"));
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
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
                strCount.Remove(0, strCount.Length);

                System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
                sqlstr2.Append(" SELECT * FROM ( ");
                sqlstr2.Append(" SELECT ROWNUM SLNO, Qry.* FROM ");
                sqlstr2.Append("  (" + sb.ToString() + " ");
                sqlstr2.Append("  ) Qry ) WHERE SLNO  Between " + start + " and " + last + "");

                return objWF.GetDataSet(sqlstr2.ToString());
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

        #endregion "Fetch Grid Query"

        #region "Get Grid Sub Queries"

        /// <summary>
        /// Gets the report for sea exp.
        /// </summary>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcess">The int process.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="cust_pk">The cust_pk.</param>
        /// <param name="Loc">The loc.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public string GetReportForSeaExp(int intBizType, int intProcess, string fromDate, string toDate, int cust_pk, Int32 Loc, int Flag = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT DISTINCT Q.LOCATION_NAME,");
            sb.Append("   Q.CUSTOMER_NAME,");
            sb.Append("   Q.JOB_PK,");
            sb.Append("   Q.JOBCARD_REF_NO,");
            sb.Append("   Q.HBLPK,");
            sb.Append("   Q.HBLNO,");
            sb.Append("   Q.INVOICE_PK,");
            sb.Append("   Q.INVOICE_REF_NO,");
            sb.Append("   Q.INVOICE_DATE,");
            sb.Append("   SUM(Q.AMT) AMT,");
            sb.Append("   Q.COLLECTIONS_TBL_PK,");
            sb.Append("   Q.COLLECTIONS_REF_NO, ");
            sb.Append("   Q.CURRENCY_ID,");
            sb.Append("   SUM(Q.RECIEVED) RECEIVED,");
            sb.Append("   SUM(Q.RECEIVABLE) RECEIVABLE,");
            sb.Append("   Q.BIZPROCES,");
            sb.Append("   Q.CARGO_TYPE");
            sb.Append("  FROM(SELECT DISTINCT LMT1.LOCATION_NAME,");
            sb.Append("           CMT1.CUSTOMER_NAME,");
            sb.Append("           JOB.JOB_CARD_TRN_PK  JOB_PK,");
            sb.Append("           JOB.JOBCARD_REF_NO,");
            sb.Append("   HBL.HBL_EXP_TBL_PK HBLPK,");
            sb.Append("   HBL.HBL_REF_NO HBLNO,");
            sb.Append("           INV1.CONSOL_INVOICE_PK  INVOICE_PK,");
            sb.Append("           INV1.INVOICE_REF_NO,");
            sb.Append("           INV1.INVOICE_DATE,");
            sb.Append("           INV1.NET_RECEIVABLE  AMT,");
            sb.Append("           CLN1.COLLECTIONS_TBL_PK,");
            sb.Append("           CLN1.COLLECTIONS_REF_NO,");
            sb.Append("           CUMT1.CURRENCY_ID,");
            sb.Append("           CTRN1.RECD_AMOUNT_HDR_CURR RECIEVED, ");
            //sb.Append("           (SELECT SUM(NVL(CRN1.RECD_AMOUNT_HDR_CURR,0)) ")
            //sb.Append("              FROM COLLECTIONS_TRN_TBL CRN1")
            //sb.Append("             WHERE CRN1.INVOICE_REF_NR =")
            //sb.Append("                   INV1.INVOICE_REF_NO)*")
            //sb.Append("       GET_EX_RATE(CLN1.CURRENCY_MST_FK,CUMT1.CURRENCY_MST_PK,CLN1.COLLECTIONS_DATE) RECIEVED,")
            sb.Append("           ROUND(NVL(INV1.NET_RECEIVABLE - ");
            sb.Append("           (SELECT SUM(NVL(CRN1.RECD_AMOUNT_HDR_CURR,0))");
            sb.Append("                        FROM COLLECTIONS_TRN_TBL CRN1");
            sb.Append("                       WHERE CRN1.INVOICE_REF_NR =");
            sb.Append("INV1.INVOICE_REF_NO)*");
            sb.Append("       GET_EX_RATE(CLN1.CURRENCY_MST_FK,CUMT1.CURRENCY_MST_PK,CLN1.COLLECTIONS_DATE),");
            sb.Append("                     0),");
            sb.Append("                 2) RECEIVABLE,");
            sb.Append("            'SEAEXP' BIZPROCES,");
            sb.Append("           BKG.CARGO_TYPE");

            sb.Append("                          FROM CONSOL_INVOICE_TBL    INV1,");
            sb.Append("  CONSOL_INVOICE_TRN_TBL INVTRN,");
            sb.Append("   JOB_CARD_TRN   JOB,");
            sb.Append("  booking_mst_tbl         BKG,");
            sb.Append("  CURRENCY_TYPE_MST_TBL CUMT1,");
            sb.Append("  COLLECTIONS_TBL       CLN1,");
            sb.Append("  COLLECTIONS_TRN_TBL   CTRN1,");
            sb.Append("  LOCATION_MST_TBL      LMT1,");
            sb.Append("  CUSTOMER_MST_TBL      CMT1,");
            sb.Append("  CUSTOMER_CONTACT_DTLS CCD,");
            sb.Append("  HBL_EXP_TBL HBL ");
            sb.Append("                         WHERE CLN1.COLLECTIONS_TBL_PK =");
            sb.Append("  CTRN1.COLLECTIONS_TBL_FK");
            sb.Append("                           AND INV1.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
            sb.Append("                           AND INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
            sb.Append("                           AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
            sb.Append("                           AND JOB.booking_mst_fk = BKG.booking_mst_pk");
            sb.Append("                           AND INV1.CURRENCY_MST_FK = CUMT1.CURRENCY_MST_PK");
            sb.Append("                           AND CTRN1.INVOICE_REF_NR = INV1.INVOICE_REF_NO");
            sb.Append("                           AND CLN1.CUSTOMER_MST_FK = CMT1.CUSTOMER_MST_PK                            ");
            sb.Append("                           AND CMT1.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
            sb.Append("                           AND LMT1.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
            sb.Append("                           AND CLN1.BUSINESS_TYPE = 2");
            sb.Append("                           AND CLN1.PROCESS_TYPE = 1");
            if (Loc > 0)
            {
                sb.Append("                           AND LMT1.LOCATION_MST_PK = " + Loc);
            }

            if (cust_pk > 0)
            {
                sb.Append("                           AND CLN1.CUSTOMER_MST_FK = " + cust_pk);
            }

            sb.Append("                           AND (INV1.INVOICE_DATE BETWEEN");
            sb.Append("  TO_DATE('" + fromDate + "', 'dd/mm/yyyy') AND");
            sb.Append("  TO_DATE('" + toDate + "', 'dd/mm/yyyy'))) Q");
            sb.Append("                ");
            sb.Append("                 WHERE Q.RECEIVABLE > 0");
            if (Flag == 0)
            {
                sb.Append("                AND     1= 0");
            }
            sb.Append("                 GROUP BY  Q.LOCATION_NAME,");
            sb.Append("                           Q.CUSTOMER_NAME,");
            sb.Append("                           Q.JOB_PK,");
            sb.Append("                           Q.JOBCARD_REF_NO,");
            sb.Append("                           Q.HBLPK,");
            sb.Append("                           Q.HBLNO,");
            sb.Append("                           Q.INVOICE_PK,");
            sb.Append("                           Q.INVOICE_REF_NO,");
            sb.Append("                           Q.INVOICE_DATE,");
            sb.Append("                           Q.COLLECTIONS_TBL_PK,");
            sb.Append("                           Q.COLLECTIONS_REF_NO,");
            sb.Append("                           Q.CURRENCY_ID,");
            sb.Append("                           Q.BIZPROCES,");
            sb.Append("                           Q.CARGO_TYPE");
            return sb.ToString();
        }

        /// <summary>
        /// Gets the report for sea imp.
        /// </summary>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcess">The int process.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="cust_pk">The cust_pk.</param>
        /// <param name="Loc">The loc.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public string GetReportForSeaImp(int intBizType, int intProcess, string fromDate, string toDate, int cust_pk, Int32 Loc, int Flag = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("     SELECT DISTINCT Q.LOCATION_NAME,");
            sb.Append("                                Q.CUSTOMER_NAME,");
            sb.Append("                                Q.JOB_PK,");
            sb.Append("                                Q.JOBCARD_REF_NO,");
            sb.Append("                                Q.HBLPK,");
            sb.Append("                                Q.HBLNO,");
            sb.Append("                                Q.INVOICE_PK,");
            sb.Append("                                Q.INVOICE_REF_NO,");
            sb.Append("                                Q.INVOICE_DATE,");
            sb.Append("                                SUM(Q.AMT) AMT,");
            sb.Append("                                Q.COLLECTIONS_TBL_PK,");
            sb.Append("                                Q.COLLECTIONS_REF_NO, ");
            sb.Append("                                Q.CURRENCY_ID,");
            sb.Append("                                SUM(Q.RECIEVED) RECEIVED,");
            sb.Append("                                SUM(Q.RECEIVABLE) RECEIVABLE,");
            sb.Append("                                Q.BIZPROCES,");
            sb.Append("                                Q.CARGO_TYPE");

            sb.Append("       FROM(SELECT DISTINCT LMT1.LOCATION_NAME,");
            sb.Append("                                            CMT1.CUSTOMER_NAME,");
            sb.Append("                                             JOB.JOB_CARD_TRN_PK  JOB_PK,");
            sb.Append("                                             JOB.JOBCARD_REF_NO,");
            sb.Append("                                            0 HBLPK,");
            sb.Append("                                            JOB.HBL_HAWB_REF_NO HBLNO,");
            sb.Append("                                             INV1.CONSOL_INVOICE_PK  INVOICE_PK,");
            sb.Append("                                             INV1.INVOICE_REF_NO,");
            sb.Append("                                             INV1.INVOICE_DATE,");
            sb.Append("                                            INV1.NET_RECEIVABLE     AMT,");
            sb.Append("                                             CLN1.COLLECTIONS_TBL_PK,");
            sb.Append("                                              CLN1.COLLECTIONS_REF_NO,");
            sb.Append("                                            CUMT1.CURRENCY_ID,");
            sb.Append("                                        CTRN1.RECD_AMOUNT_HDR_CURR RECIEVED, ");
            //sb.Append("                                            (SELECT SUM(NVL(CRN1.RECD_AMOUNT_HDR_CURR,0))")
            //sb.Append("                                               FROM COLLECTIONS_TRN_TBL CRN1")
            //sb.Append("                                              WHERE CRN1.INVOICE_REF_NR =")
            //sb.Append("                                                    INV1.INVOICE_REF_NO)*")
            //sb.Append("       GET_EX_RATE(CLN1.CURRENCY_MST_FK,CUMT1.CURRENCY_MST_PK,CLN1.COLLECTIONS_DATE) RECIEVED,")
            sb.Append("                                            ROUND(NVL(NVL(INV1.NET_RECEIVABLE,0) - ");
            sb.Append("                                                      (SELECT SUM(NVL(CRN1.RECD_AMOUNT_HDR_CURR,0))");
            sb.Append("                                                         FROM COLLECTIONS_TRN_TBL CRN1");
            sb.Append("                                                        WHERE CRN1.INVOICE_REF_NR =");
            sb.Append("                                                              INV1.INVOICE_REF_NO)*");
            sb.Append("       GET_EX_RATE(CLN1.CURRENCY_MST_FK,CUMT1.CURRENCY_MST_PK,CLN1.COLLECTIONS_DATE), ");
            sb.Append("                                                      0),");
            sb.Append("                                                  2) RECEIVABLE,");
            sb.Append("                                        'SEAIMP' BIZPROCES, ");
            sb.Append("                                         JOB.CARGO_TYPE");

            sb.Append("                              FROM CONSOL_INVOICE_TBL    INV1,");
            sb.Append("                               CONSOL_INVOICE_TRN_TBL INVTRN,");
            sb.Append("                                JOB_CARD_TRN   JOB,");
            sb.Append("                                   CURRENCY_TYPE_MST_TBL CUMT1,");
            sb.Append("                                   COLLECTIONS_TBL       CLN1,");
            sb.Append("                                   COLLECTIONS_TRN_TBL   CTRN1, ");
            sb.Append("                                   LOCATION_MST_TBL      LMT1,");
            sb.Append("                                   CUSTOMER_MST_TBL      CMT1,");
            sb.Append("                                   CUSTOMER_CONTACT_DTLS CCD");
            sb.Append("                             WHERE CLN1.COLLECTIONS_TBL_PK =");
            sb.Append("                                   CTRN1.COLLECTIONS_TBL_FK");
            sb.Append("                               AND INV1.CURRENCY_MST_FK =");
            sb.Append("                                   CUMT1.CURRENCY_MST_PK");
            sb.Append("                           AND INV1.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
            sb.Append("                           AND INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
            sb.Append("                               AND CTRN1.INVOICE_REF_NR = INV1.INVOICE_REF_NO");
            sb.Append("                               AND CLN1.CUSTOMER_MST_FK =");
            sb.Append("                                   CMT1.CUSTOMER_MST_PK   ");
            sb.Append("                               AND CMT1.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
            sb.Append("                               AND LMT1.LOCATION_MST_PK =");
            sb.Append("                                   CCD.ADM_LOCATION_MST_FK");
            sb.Append("                               AND CLN1.BUSINESS_TYPE = 2");
            sb.Append("                               AND CLN1.PROCESS_TYPE = 2");

            if (Loc > 0)
            {
                sb.Append("                               AND LMT1.LOCATION_MST_PK = " + Loc);
            }
            if (cust_pk > 0)
            {
                sb.Append("                               AND CLN1.CUSTOMER_MST_FK = " + cust_pk);
            }
            sb.Append("                               AND (INV1.INVOICE_DATE BETWEEN");
            sb.Append("                                   TO_DATE('" + fromDate + "', 'dd/mm/yyyy') AND");
            sb.Append("                                   TO_DATE('" + toDate + "', 'dd/mm/yyyy'))) Q");
            sb.Append("                     WHERE Q.RECEIVABLE > 0");
            if (Flag == 0)
            {
                sb.Append("                AND           1= 0");
            }
            sb.Append("                 GROUP BY  Q.LOCATION_NAME,");
            sb.Append("                           Q.CUSTOMER_NAME,");
            sb.Append("                           Q.JOB_PK,");
            sb.Append("                           Q.JOBCARD_REF_NO,");
            sb.Append("                                Q.HBLPK,");
            sb.Append("                                Q.HBLNO,");
            sb.Append("                           Q.INVOICE_PK,");
            sb.Append("                           Q.INVOICE_REF_NO,");
            sb.Append("                           Q.INVOICE_DATE,");
            sb.Append("                           Q.COLLECTIONS_TBL_PK,");
            sb.Append("                           Q.COLLECTIONS_REF_NO,");
            sb.Append("                           Q.CURRENCY_ID,");
            sb.Append("                           Q.BIZPROCES,");
            sb.Append("                           Q.CARGO_TYPE");
            return sb.ToString();
        }

        /// <summary>
        /// Gets the report for air exp.
        /// </summary>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcess">The int process.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="cust_pk">The cust_pk.</param>
        /// <param name="Loc">The loc.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public string GetReportForAirExp(int intBizType, int intProcess, string fromDate, string toDate, int cust_pk, Int32 Loc, int Flag = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("            SELECT DISTINCT Q.LOCATION_NAME ,");
            sb.Append("   Q.CUSTOMER_NAME,");
            sb.Append("   Q.JOB_PK,");
            sb.Append("   Q.JOBCARD_REF_NO,");
            sb.Append("   Q.HBLPK,");
            sb.Append("   Q.HBLNO,");
            sb.Append("   Q.INVOICE_PK,");
            sb.Append("   Q.INVOICE_REF_NO,");
            sb.Append("   Q.INVOICE_DATE,");
            sb.Append("   SUM(Q.AMT) AMT,");
            sb.Append("   Q.COLLECTIONS_TBL_PK,");
            sb.Append("   Q.COLLECTIONS_REF_NO, ");
            sb.Append("   Q.CURRENCY_ID,");
            sb.Append("   SUM(Q.RECIEVED) RECEIVED,");
            sb.Append("   SUM(Q.RECEIVABLE) RECEIVABLE,");
            sb.Append("   Q.BIZPROCES,");
            sb.Append("   Q.CARGO_TYPE");

            sb.Append(" FROM(SELECT DISTINCT LMT1.LOCATION_NAME,");
            sb.Append("           CMT1.CUSTOMER_NAME,");
            sb.Append("             JOB.JOB_CARD_TRN_PK  JOB_PK,");
            sb.Append("              JOB.JOBCARD_REF_NO,");
            sb.Append("                           HBL.HAWB_EXP_TBL_PK HBLPK,");
            sb.Append("                           HBL.HAWB_REF_NO HBLNO,");
            sb.Append("              INV1.CONSOL_INVOICE_PK  INVOICE_PK,");
            sb.Append("              INV1.INVOICE_REF_NO,");
            sb.Append("              INV1.INVOICE_DATE,");
            sb.Append("           INV1.NET_RECEIVABLE     AMT,  ");
            sb.Append("              CLN1.COLLECTIONS_TBL_PK,");
            sb.Append("              CLN1.COLLECTIONS_REF_NO,");
            sb.Append("           CUMT1.CURRENCY_ID,");
            sb.Append("         CTRN1.RECD_AMOUNT_HDR_CURR RECIEVED, ");
            //sb.Append("           (SELECT SUM(NVL(CRN1.RECD_AMOUNT_HDR_CURR,0))")
            //sb.Append("              FROM COLLECTIONS_TRN_TBL CRN1")
            //sb.Append("             WHERE CRN1.INVOICE_REF_NR =")
            //sb.Append("                   INV1.INVOICE_REF_NO)*")
            //sb.Append("       GET_EX_RATE(CLN1.CURRENCY_MST_FK,CUMT1.CURRENCY_MST_PK,CLN1.COLLECTIONS_DATE) RECIEVED,")
            sb.Append("           ROUND(NVL(NVL(INV1.NET_RECEIVABLE,0) - ");
            sb.Append("                     (SELECT SUM(NVL(CRN1.RECD_AMOUNT_HDR_CURR,0))");
            sb.Append("                        FROM COLLECTIONS_TRN_TBL CRN1");
            sb.Append("                       WHERE CRN1.INVOICE_REF_NR =");
            sb.Append("                             INV1.INVOICE_REF_NO)*");
            sb.Append("       GET_EX_RATE(CLN1.CURRENCY_MST_FK,CUMT1.CURRENCY_MST_PK,CLN1.COLLECTIONS_DATE), ");
            sb.Append("                     0),");
            sb.Append("                 2) RECEIVABLE,");
            sb.Append("         'AIREXP' BIZPROCES, ");
            sb.Append("          0 CARGO_TYPE");

            sb.Append("                            FROM CONSOL_INVOICE_TBL    INV1,");
            sb.Append("CONSOL_INVOICE_TRN_TBL INVTRN,");
            sb.Append(" JOB_CARD_TRN   JOB,");
            sb.Append("  CURRENCY_TYPE_MST_TBL CUMT1,");
            sb.Append("  COLLECTIONS_TBL       CLN1,");
            sb.Append("  COLLECTIONS_TRN_TBL   CTRN1,");
            sb.Append("  LOCATION_MST_TBL      LMT1,");
            sb.Append("  CUSTOMER_MST_TBL      CMT1,");
            sb.Append("  CUSTOMER_CONTACT_DTLS CCD,");
            sb.Append("  HAWB_EXP_TBL HBL");
            sb.Append("                           WHERE CLN1.COLLECTIONS_TBL_PK =");
            sb.Append("  CTRN1.COLLECTIONS_TBL_FK");
            sb.Append("                           AND INV1.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
            sb.Append("                           AND INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK");
            sb.Append("                             AND JOB.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK(+)");
            sb.Append("                             AND INV1.CURRENCY_MST_FK = CUMT1.CURRENCY_MST_PK");
            sb.Append("                             AND CTRN1.INVOICE_REF_NR = INV1.INVOICE_REF_NO");
            sb.Append("                             AND CLN1.CUSTOMER_MST_FK = CMT1.CUSTOMER_MST_PK");
            sb.Append("                             AND CMT1.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
            sb.Append("                             AND LMT1.LOCATION_MST_PK =");
            sb.Append("  CCD.ADM_LOCATION_MST_FK");
            sb.Append("                             AND LMT1.LOCATION_MST_PK =");
            sb.Append("  CCD.ADM_LOCATION_MST_FK");
            sb.Append("                             AND CLN1.BUSINESS_TYPE = 1");
            sb.Append("                             AND CLN1.PROCESS_TYPE = 1");
            if (Loc > 0)
            {
                sb.Append("                             AND LMT1.LOCATION_MST_PK = " + Loc);
            }
            if (cust_pk > 0)
            {
                sb.Append("                             AND CLN1.CUSTOMER_MST_FK = " + cust_pk);
            }
            sb.Append("                             AND (INV1.INVOICE_DATE BETWEEN");
            sb.Append("  TO_DATE('" + fromDate + "', 'dd/mm/yyyy') AND");
            sb.Append("  TO_DATE('" + toDate + "', 'dd/mm/yyyy'))) Q");
            sb.Append("                   WHERE Q.RECEIVABLE > 0");
            if (Flag == 0)
            {
                sb.Append("              AND             1= 0");
            }
            sb.Append("                 GROUP BY  Q.LOCATION_NAME,");
            sb.Append("                           Q.CUSTOMER_NAME,");
            sb.Append("                           Q.JOB_PK,");
            sb.Append("                           Q.JOBCARD_REF_NO,");
            sb.Append(" Q.HBLPK,");
            sb.Append(" Q.HBLNO,");
            sb.Append("                           Q.INVOICE_PK,");
            sb.Append("                           Q.INVOICE_REF_NO,");
            sb.Append("                           Q.INVOICE_DATE,");
            sb.Append("                           Q.COLLECTIONS_TBL_PK,");
            sb.Append("                           Q.COLLECTIONS_REF_NO,");
            sb.Append("                           Q.CURRENCY_ID,");
            sb.Append("                           Q.BIZPROCES,");
            sb.Append("                           Q.CARGO_TYPE");
            return sb.ToString();
        }

        /// <summary>
        /// Gets the report for air imp.
        /// </summary>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcess">The int process.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="cust_pk">The cust_pk.</param>
        /// <param name="Loc">The loc.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public string GetReportForAirImp(int intBizType, int intProcess, string fromDate, string toDate, int cust_pk, Int32 Loc, int Flag = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("      ");
            sb.Append("                SELECT DISTINCT Q.LOCATION_NAME,");
            sb.Append("                                      Q.CUSTOMER_NAME,");
            sb.Append("                                      Q.JOB_PK,");
            sb.Append("                                      Q.JOBCARD_REF_NO,");
            sb.Append("                                Q.HBLPK,");
            sb.Append("                                Q.HBLNO,");
            sb.Append("                                      Q.INVOICE_PK,");
            sb.Append("                                      Q.INVOICE_REF_NO,");
            sb.Append("                                      Q.INVOICE_DATE,");
            sb.Append("                                      SUM(Q.AMT) AMT,");
            sb.Append("                                      Q.COLLECTIONS_TBL_PK,");
            sb.Append("                                      Q.COLLECTIONS_REF_NO, ");
            sb.Append("                                      Q.CURRENCY_ID,");
            sb.Append("                                      SUM(Q.RECIEVED) RECEIVED,");
            sb.Append("                                      SUM(Q.RECEIVABLE) RECEIVABLE,");
            sb.Append("                                      Q.BIZPROCES,");
            sb.Append("                                      Q.CARGO_TYPE");
            sb.Append("      FROM(SELECT DISTINCT LMT1.LOCATION_NAME,");
            sb.Append("                                          CMT1.CUSTOMER_NAME,");
            sb.Append("                                            JOB.JOB_CARD_TRN_PK  JOB_PK,");
            sb.Append("                                             JOB.JOBCARD_REF_NO,");
            sb.Append("                                           0 HBLPK,");
            sb.Append("                                           JOB.HBL_HAWB_REF_NO HBLNO,");
            sb.Append("                                             INV1.CONSOL_INVOICE_PK  INVOICE_PK,");
            sb.Append("                                             INV1.INVOICE_REF_NO,");
            sb.Append("                                             INV1.INVOICE_DATE,");
            sb.Append("                                              INV1.NET_RECEIVABLE     AMT,");
            sb.Append("                                             CLN1.COLLECTIONS_TBL_PK,");
            sb.Append("                                             CLN1.COLLECTIONS_REF_NO,");
            sb.Append("                                              CUMT1.CURRENCY_ID,");
            sb.Append("                                        CTRN1.RECD_AMOUNT_HDR_CURR RECIEVED, ");
            //sb.Append("                                              (SELECT SUM(NVL(CRN1.RECD_AMOUNT_HDR_CURR,0))")
            //sb.Append("                                                 FROM COLLECTIONS_TRN_TBL CRN1")
            //sb.Append("                                                WHERE CRN1.INVOICE_REF_NR =")
            //sb.Append("                                                      INV1.INVOICE_REF_NO)*")
            //sb.Append("       GET_EX_RATE(CLN1.CURRENCY_MST_FK,CUMT1.CURRENCY_MST_PK,CLN1.COLLECTIONS_DATE) RECIEVED,")
            sb.Append("                                              ROUND(NVL(NVL(INV1.NET_RECEIVABLE,0) - ");
            sb.Append("                                                        (SELECT SUM(NVL(CRN1.RECD_AMOUNT_HDR_CURR,0))");
            sb.Append("                                                           FROM COLLECTIONS_TRN_TBL CRN1");
            sb.Append("                                                          WHERE CRN1.INVOICE_REF_NR =");
            sb.Append("                                                                INV1.INVOICE_REF_NO)*");
            sb.Append("       GET_EX_RATE(CLN1.CURRENCY_MST_FK,CUMT1.CURRENCY_MST_PK,CLN1.COLLECTIONS_DATE),");
            sb.Append("                                                        0),");
            sb.Append("                                                    2) RECEIVABLE,");
            sb.Append("                                        'AIRIMP' BIZPROCES, ");
            sb.Append("                                         0 CARGO_TYPE");

            sb.Append("                                FROM CONSOL_INVOICE_TBL    INV1,");
            sb.Append("                                  CONSOL_INVOICE_TRN_TBL INVTRN,");
            sb.Append("                                     JOB_CARD_TRN   JOB,");
            sb.Append("                                     CURRENCY_TYPE_MST_TBL CUMT1,");
            sb.Append("                                     COLLECTIONS_TBL       CLN1,");
            sb.Append("                                     COLLECTIONS_TRN_TBL   CTRN1,                                 ");
            sb.Append("                                     LOCATION_MST_TBL      LMT1,");
            sb.Append("                                     CUSTOMER_MST_TBL      CMT1,");
            sb.Append("                                     CUSTOMER_CONTACT_DTLS CCD");
            sb.Append("                               WHERE CLN1.COLLECTIONS_TBL_PK =");
            sb.Append("                                     CTRN1.COLLECTIONS_TBL_FK");
            sb.Append("                           AND INV1.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
            sb.Append("                           AND INVTRN.JOB_CARD_FK = JOB.JOB_CARD_TRN_PK(+)");
            sb.Append("                                 AND INV1.CURRENCY_MST_FK =");
            sb.Append("                                     CUMT1.CURRENCY_MST_PK");
            sb.Append("                                 AND CTRN1.INVOICE_REF_NR =");
            sb.Append("                                     INV1.INVOICE_REF_NO");
            sb.Append("                                 AND CLN1.CUSTOMER_MST_FK =");
            sb.Append("                                     CMT1.CUSTOMER_MST_PK");
            sb.Append("                                 AND CMT1.CUSTOMER_MST_PK =");
            sb.Append("                                     CCD.CUSTOMER_MST_FK   ");
            sb.Append("                                 AND LMT1.LOCATION_MST_PK =");
            sb.Append("                                     CCD.ADM_LOCATION_MST_FK");
            sb.Append("                                 AND CLN1.BUSINESS_TYPE = 1");
            sb.Append("                                 AND CLN1.PROCESS_TYPE = 2");

            if (Loc > 0)
            {
                sb.Append("                                 AND LMT1.LOCATION_MST_PK = " + Loc);
            }
            if (cust_pk > 0)
            {
                sb.Append("                                 AND CLN1.CUSTOMER_MST_FK = " + cust_pk);
            }
            sb.Append("                                 AND (INV1.INVOICE_DATE BETWEEN");
            sb.Append("                                     TO_DATE('" + fromDate + "', 'dd/mm/yyyy') AND");
            sb.Append("                                     TO_DATE('" + toDate + "', 'dd/mm/yyyy'))) Q");
            sb.Append("                       WHERE Q.RECEIVABLE > 0");
            if (Flag == 0)
            {
                sb.Append("                        AND   1= 0");
            }
            sb.Append("                 GROUP BY  Q.LOCATION_NAME,");
            sb.Append("                           Q.CUSTOMER_NAME,");
            sb.Append("                           Q.JOB_PK,");
            sb.Append("                           Q.JOBCARD_REF_NO,");
            sb.Append("                                Q.HBLPK,");
            sb.Append("                                Q.HBLNO,");
            sb.Append("                           Q.INVOICE_PK,");
            sb.Append("                           Q.INVOICE_REF_NO,");
            sb.Append("                           Q.INVOICE_DATE,");
            sb.Append("                           Q.COLLECTIONS_TBL_PK,");
            sb.Append("                           Q.COLLECTIONS_REF_NO,");
            sb.Append("                           Q.CURRENCY_ID,");
            sb.Append("                           Q.BIZPROCES,");
            sb.Append("                           Q.CARGO_TYPE");
            return sb.ToString();
        }

        /// <summary>
        /// Conditionvalues the specified va r_ in.
        /// </summary>
        /// <param name="VAR_IN">The va r_ in.</param>
        /// <returns></returns>
        public int CONDITIONVALUE(string VAR_IN)
        {
            if (VAR_IN == "0" | VAR_IN == null)
            {
                return 1;
            }
            else
            {
                return 2;
            }
        }

        #endregion "Get Grid Sub Queries"

        //'

        #region "Fetch Report Query"

        /// <summary>
        /// Fetches the report query.
        /// </summary>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcess">The int process.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="customer">The customer.</param>
        /// <param name="Loc">The loc.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchReportQuery(int intBizType, int intProcess, string fromDate, string toDate, string customer, Int32 Loc, int Flag = 0)
        {
            string sql = null;
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            Int32 cust_pk = default(Int32);

            try
            {
                if (!string.IsNullOrEmpty(customer))
                {
                    cust_pk = Convert.ToInt32(objWF.ExecuteScaler("select customer_mst_pk from customer_mst_tbl where customer_id='" + customer + "'"));
                }
                else
                {
                    cust_pk = 0;
                }

                //Sea Export
                if (intBizType == 2 & intProcess == 1)
                {
                    sb.Append(GetReportForSeaExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                //Air Export
                if (intBizType == 1 & intProcess == 1)
                {
                    sb.Append(GetReportForAirExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                //Sea Import
                if (intBizType == 2 & intProcess == 2)
                {
                    sb.Append(GetReportForSeaImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                //Air Import
                if (intBizType == 1 & intProcess == 2)
                {
                    sb.Append(GetReportForAirImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                if (intBizType == 1 & intProcess == 0)
                {
                    ///*******************Air-exp and imp
                    sb.Append(GetReportForAirExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForAirImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                if (intBizType == 2 & intProcess == 0)
                {
                    //'***********************Sea Imp-Exp
                    sb.Append(GetReportForSeaExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForSeaImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                if (intBizType == 3 & intProcess == 1)
                {
                    //'******************************* All Biz Exp
                    sb.Append(GetReportForAirExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForSeaExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                if (intBizType == 3 & intProcess == 2)
                {
                    //'******************************* All Biz Imp
                    sb.Append(GetReportForAirImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForSeaImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                }
                if (intBizType == 3 & intProcess == 0)
                {
                    //'***************************************ALL
                    sb.Append(GetReportForAirExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForSeaExp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForAirImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    sb.Append(" UNION ");
                    sb.Append(GetReportForSeaImp(intBizType, intProcess, fromDate, toDate, cust_pk, Loc, Flag));
                    //'***************************************
                }
                System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
                sqlstr2.Append(" SELECT * FROM ( ");
                sqlstr2.Append(" SELECT ROWNUM SLNO, Qry.* FROM ");
                sqlstr2.Append("  (" + sb.ToString() + " ");
                sqlstr2.Append("  ) Qry )");

                return objWF.GetDataSet(sqlstr2.ToString());
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

        #endregion "Fetch Report Query"

        #region "GetCollectionData"

        /// <summary>
        /// Gets the col report.
        /// </summary>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcess">The int process.</param>
        /// <param name="fromDate">From date.</param>
        /// <param name="toDate">To date.</param>
        /// <param name="customer">The customer.</param>
        /// <param name="Loc">The loc.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataSet GetColReport(int intBizType, int intProcess, string fromDate, string toDate, string customer, Int32 Loc, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            Int32 cust_pk = default(Int32);

            if (!string.IsNullOrEmpty(customer))
            {
                cust_pk = Convert.ToInt32(objWF.ExecuteScaler("select customer_mst_pk from customer_mst_tbl where customer_id='" + customer + "'"));
            }
            else
            {
                cust_pk = 0;
            }

            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with7 = objWF.MyCommand.Parameters;
                _with7.Add("BIZTYPE_IN", intBizType).Direction = ParameterDirection.Input;
                _with7.Add("PROCESS_IN", intProcess).Direction = ParameterDirection.Input;
                _with7.Add("FROMDATE_IN", fromDate).Direction = ParameterDirection.Input;
                _with7.Add("TODATE_IN", toDate).Direction = ParameterDirection.Input;
                _with7.Add("CUSTOMER_IN", getDefault(cust_pk, "")).Direction = ParameterDirection.Input;
                _with7.Add("Location_IN", getDefault(Loc, "")).Direction = ParameterDirection.Input;
                _with7.Add("COL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWF.GetDataSet("FETCH_COLLECTION_PKG", "FETCH_COL_DATA");
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "GetCollectionData"
    }
}